"""Job `bls`: versioned `bls_observation` rows, `bls_as_of`, reparse (C2, C3, C4).

Responses are the real keyless v1 recording in `tests/fixtures/bls/v1_timeseries.json` (see
test_bls_client), edited per test where a revision is needed. `raw._utcnow` is pinned so each
response's `fetched_at` — which becomes `first_seen_at` — is known.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, jobs, ratelimit, raw, scheduler, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources import bls

FIXTURES = Path(__file__).parent / "fixtures" / "bls"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
HOST = "api.bls.gov"
CPI = "CUSR0000SA0"


# 2026-10-08 13:00 UTC = 09:00 ET (the scheduled time), naive UTC like `fetched_at`
def _naive(*args) -> datetime:
    """A UTC instant as stored: naive."""
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


T1 = _naive(2026, 10, 8, 13, 0)
NOW = T1.replace(tzinfo=UTC)


def _fixture() -> dict:
    return json.loads((FIXTURES / "v1_timeseries.json").read_text("utf-8"))


def _item(doc: dict, series_id: str, year: str, period: str) -> dict:
    series = next(s for s in doc["Results"]["series"] if s["seriesID"] == series_id)
    return next(i for i in series["data"] if i["year"] == year and i["period"] == period)


class FakeApi:
    """Answers every BLS POST with `doc` (or `status`); records request bodies."""

    def __init__(self):
        self.doc = _fixture()
        self.status = 200
        self.bodies: list[dict] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(json.loads(request.content))
        if self.status != 200:
            return httpx.Response(self.status, text="<html>error</html>")
        return httpx.Response(200, json=self.doc)


class Clock:
    """Stands in for `raw._utcnow`: every archived response gets `fetched_at = clock.t`."""

    def __init__(self):
        self.t = T1

    def __call__(self) -> datetime:
        return self.t


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(raw, "_utcnow", c)
    return c


@pytest.fixture
def settings(tmp_path, monkeypatch, clock):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    t = [0.0]
    monkeypatch.setattr(
        ratelimit,
        "LIMITERS",
        ratelimit.Limiters(clock=lambda: t[0], sleep=lambda s: t.__setitem__(0, t[0] + s)),
    )
    monkeypatch.setattr(http, "BACKOFF_BASE", 0.0)
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        bls_api_key=None,
    )


@pytest.fixture
def api(settings):
    fake = FakeApi()
    with respx.mock(assert_all_called=False) as router:
        router.route(host=HOST).mock(side_effect=fake)
        yield fake


def _registry():
    reg = jobs.Registry()
    reg.register(bls.JOB)
    return reg


def _run(settings, now=NOW):
    return jobs.run_job("bls", settings=settings, registry=_registry(), clock=lambda: now)


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _table(settings) -> list[tuple]:
    return _query(
        settings,
        "SELECT series_id, year, period, value, footnotes, first_seen_at, raw_id, source"
        " FROM bls_observation ORDER BY ALL",
    )


def _versions(settings, series_id, year, period) -> list[tuple]:
    return _query(
        settings,
        "SELECT value, footnotes, first_seen_at FROM bls_observation"
        " WHERE series_id = ? AND year = ? AND period = ? ORDER BY first_seen_at",
        [series_id, year, period],
    )


def _total_items(doc) -> int:
    return sum(len(s["data"]) for s in doc["Results"]["series"])


# --- first fetch ---------------------------------------------------------------------------------


def test_first_run_inserts_every_observation_first_seen_at_the_raw_fetch_time(settings, api):
    result = _run(settings)
    assert result.status == "ok", result.error
    assert len(api.bodies) == 1  # one query carries all seven series, whatever the key count
    assert result.rows_upserted == _total_items(api.doc) == 15
    raw_id, fetched_at = _query(
        settings, "SELECT id, fetched_at FROM raw_response WHERE dataset = 'bls_timeseries'"
    )[0]
    assert fetched_at == T1
    rows = _table(settings)
    assert {(r[5], r[6], r[7]) for r in rows} == {(T1, raw_id, "bls")}
    assert _versions(settings, CPI, 2026, "M08") == [(Decimal("334.131"), "[]", T1)]
    ppi = _versions(settings, "WPSFD4", 2026, "M08")[0]
    assert json.loads(ppi[1])[0]["code"] == "P"


def test_a_period_published_as_dash_is_stored_with_a_null_value_and_its_footnotes(settings, api):
    _run(settings)
    [(value, footnotes, seen)] = _versions(settings, CPI, 2025, "M10")
    assert value is None
    assert seen == T1
    assert [n["code"] for n in json.loads(footnotes)] == ["X"]


def test_the_request_covers_this_year_and_last_on_keyless_v1(settings, api):
    _run(settings)
    body = api.bodies[0]
    assert body["startyear"] == "2025" and body["endyear"] == "2026"
    assert "registrationkey" not in body
    assert set(body["seriesid"]) == set(bls.load_series(settings.config_dir))


def test_watermark_per_series_is_its_latest_published_period(settings, api):
    _run(settings)
    with store.session(settings.db_path, wait=0) as con:
        marks = jobs.get_watermarks(con, "bls")
    assert marks == {
        "CUSR0000SA0": "2026-M08",
        "CUSR0000SA0L1E": "2026-M08",
        "WPSFD4": "2026-M08",
        "CES0000000001": "2026-M09",
        "LNS14000000": "2026-M09",
        "CES0500000003": "2026-M09",
        "JTS000000000000000JOL": "2026-M08",
    }


def test_the_job_keeps_bls_series_filled_from_config(settings, api):
    _run(settings)
    rows = dict(_query(settings, "SELECT series_id, decimals FROM bls_series"))
    assert rows[CPI] == 3 and rows["LNS14000000"] == 1 and len(rows) == 7


# --- versions ------------------------------------------------------------------------------------


def test_identical_refetch_inserts_nothing(settings, api, clock):
    _run(settings)
    before = _table(settings)
    clock.t = T1 + timedelta(days=1)
    second = _run(settings, now=NOW + timedelta(days=1))
    assert second.status == "ok"
    assert second.rows_upserted == 0
    assert _table(settings) == before
    assert len(_query(settings, "SELECT id FROM raw_response")) == 2


def test_a_revised_value_inserts_a_new_version_and_keeps_the_old(settings, api, clock):
    _run(settings)
    _item(api.doc, CPI, "2026", "M07")["value"] = "332.900"
    t2 = T1 + timedelta(days=30)
    clock.t = t2
    second = _run(settings, now=NOW + timedelta(days=30))
    assert second.rows_upserted == 1
    assert _versions(settings, CPI, 2026, "M07") == [
        (Decimal("332.813"), "[]", T1),
        (Decimal("332.900"), "[]", t2),
    ]


def test_a_footnote_change_alone_inserts_a_new_version(settings, api, clock):
    _run(settings)
    _item(api.doc, "CES0000000001", "2026", "M08")["footnotes"] = [{}]  # no longer preliminary
    clock.t = T1 + timedelta(days=30)
    assert _run(settings, now=NOW + timedelta(days=30)).rows_upserted == 1
    versions = _versions(settings, "CES0000000001", 2026, "M08")
    assert [v[0] for v in versions] == [Decimal(159015)] * 2
    assert [json.loads(v[1]) for v in versions][1] == []


def test_a_difference_beyond_published_decimals_inserts_nothing(settings, api, clock):
    _run(settings)
    _item(api.doc, CPI, "2026", "M08")["value"] = "334.1312"  # CPI publishes 3 decimals
    _item(api.doc, "LNS14000000", "2026", "M09")["value"] = "4.20"  # UNRATE publishes 1
    clock.t = T1 + timedelta(days=1)
    assert _run(settings, now=NOW + timedelta(days=1)).rows_upserted == 0
    assert len(_versions(settings, CPI, 2026, "M08")) == 1


def test_a_gap_filled_later_is_a_new_version(settings, api, clock):
    _run(settings)
    _item(api.doc, CPI, "2025", "M10")["value"] = "324.500"
    _item(api.doc, CPI, "2025", "M10")["footnotes"] = [{}]
    t2 = T1 + timedelta(days=1)
    clock.t = t2
    assert _run(settings, now=NOW + timedelta(days=1)).rows_upserted == 1
    assert [(v[0], v[2]) for v in _versions(settings, CPI, 2025, "M10")] == [
        (None, T1),
        (Decimal("324.500"), t2),
    ]


def test_a_revision_back_to_an_earlier_value_is_a_new_version(settings, api, clock):
    _run(settings)
    item = _item(api.doc, CPI, "2026", "M07")
    item["value"] = "333.000"
    clock.t = T1 + timedelta(days=1)
    _run(settings, now=NOW + timedelta(days=1))
    item["value"] = "332.813"
    clock.t = T1 + timedelta(days=2)
    assert _run(settings, now=NOW + timedelta(days=2)).rows_upserted == 1
    assert [v[0] for v in _versions(settings, CPI, 2026, "M07")] == [
        Decimal("332.813"),
        Decimal("333.000"),
        Decimal("332.813"),
    ]


# --- bls_as_of -----------------------------------------------------------------------------------


def _as_of(settings, sql_arg, params=()):
    return {
        r[0]: r[1]
        for r in _query(
            settings,
            f"SELECT period, value FROM bls_as_of({sql_arg})"
            " WHERE series_id = 'CUSR0000SA0' AND year = 2026",
            params,
        )
    }


def test_bls_as_of_returns_the_latest_version_first_seen_by_a_timestamp(settings, api, clock):
    _run(settings)
    _item(api.doc, CPI, "2026", "M07")["value"] = "332.900"
    t2 = _naive(2026, 11, 13, 13, 0)
    clock.t = t2
    _run(settings, now=t2.replace(tzinfo=UTC))
    assert _as_of(settings, "?", [T1 - timedelta(seconds=1)]) == {}
    assert _as_of(settings, "?", [T1])["M07"] == Decimal("332.813")
    assert _as_of(settings, "?", [t2 - timedelta(seconds=1)])["M07"] == Decimal("332.813")
    assert _as_of(settings, "?", [t2]) == {
        "M07": Decimal("332.900"),
        "M08": Decimal("334.131"),
    }
    # an aware timestamp is converted to UTC (t2 is 08:00 EST)
    est = "TIMESTAMPTZ '2026-11-13 {}-05:00'"
    assert _as_of(settings, est.format("07:59:59"))["M07"] == Decimal("332.813")
    assert _as_of(settings, est.format("08:00:00"))["M07"] == Decimal("332.900")
    assert _as_of(settings, "?", [t2.replace(tzinfo=UTC)])["M07"] == Decimal("332.900")


def test_bls_as_of_a_date_means_235959_new_york_that_day(settings, api, clock):
    # 2026-10-09 03:30 UTC is 23:30 ET on 10-08; 04:30 UTC is 00:30 ET on 10-09
    _run(settings)
    item = _item(api.doc, CPI, "2026", "M07")
    item["value"] = "333.000"
    clock.t = _naive(2026, 10, 9, 3, 30)
    _run(settings, now=clock.t.replace(tzinfo=UTC))
    item["value"] = "334.000"
    clock.t = _naive(2026, 10, 9, 4, 30)
    _run(settings, now=clock.t.replace(tzinfo=UTC))
    assert _as_of(settings, "?", [date(2026, 10, 7)]) == {}
    assert _as_of(settings, "?", [date(2026, 10, 8)])["M07"] == Decimal("333.000")
    assert _as_of(settings, "DATE '2026-10-09'")["M07"] == Decimal("334.000")
    # winter: EST, 23:59:59 ET = 04:59:59 UTC the next day
    with store.session(settings.db_path, wait=0) as con:
        assert con.execute("SELECT bls_as_of_utc(DATE '2026-01-15')").fetchone()[0] == _naive(
            2026, 1, 16, 4, 59, 59
        )


# --- failures and reparse ------------------------------------------------------------------------


def test_a_failed_request_fails_every_key_once_and_leaves_watermarks(settings, api, clock):
    _run(settings)
    with store.session(settings.db_path, wait=0) as con:
        marks = jobs.get_watermarks(con, "bls")
    api.status = 500
    clock.t = T1 + timedelta(days=1)
    result = _run(settings, now=NOW + timedelta(days=1))
    assert result.status == "failed"
    assert set(result.key_errors) == set(marks)
    assert len(api.bodies) == 1 + 5  # the first run, then one request retried 5 times, once
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, "bls") == marks


def test_a_not_processed_reply_fails_the_run_and_reparse_skips_it(settings, api, clock):
    _run(settings)
    before = _table(settings)
    api.doc = {"status": "REQUEST_NOT_PROCESSED", "message": ["daily threshold"], "Results": {}}
    clock.t = T1 + timedelta(days=1)
    assert _run(settings, now=NOW + timedelta(days=1)).status == "failed"
    with store.session(settings.db_path, wait=0) as con:
        result = jobs.reparse(con, bls.JOB, settings=settings, now=NOW + timedelta(days=1))
    assert len(result.raw_ids) == 2
    assert _table(settings) == before


def test_reparse_deletes_by_raw_id_and_replays_to_an_identical_table(settings, api, clock):
    _run(settings)
    _item(api.doc, CPI, "2026", "M07")["value"] = "332.900"
    clock.t = T1 + timedelta(days=30)
    _run(settings, now=NOW + timedelta(days=30))
    clock.t = T1 + timedelta(days=31)
    _run(settings, now=NOW + timedelta(days=31))  # identical: no rows
    before = _table(settings)
    requests = len(api.bodies)
    with store.session(settings.db_path, wait=0) as con:
        result = jobs.reparse(con, bls.JOB, settings=settings, now=NOW + timedelta(days=32))
    assert len(result.raw_ids) == 3
    assert result.deleted == len(before) == 16
    assert result.rows == 16
    assert _table(settings) == before
    assert len(api.bodies) == requests  # no HTTP


def test_reparse_since_replays_only_later_responses_against_earlier_history(settings, api, clock):
    _run(settings)
    _item(api.doc, CPI, "2026", "M07")["value"] = "332.900"
    clock.t = T1 + timedelta(days=30)
    _run(settings, now=NOW + timedelta(days=30))
    before = _table(settings)
    with store.session(settings.db_path, wait=0) as con:
        result = jobs.reparse(
            con,
            bls.JOB,
            settings=settings,
            now=NOW + timedelta(days=31),
            since=date(2026, 11, 1),
        )
    assert (len(result.raw_ids), result.deleted, result.rows) == (1, 1, 1)
    assert _table(settings) == before


def test_reparse_of_an_emptied_table_rebuilds_it(settings, api, clock):
    _run(settings)
    before = _table(settings)
    _query(settings, "DELETE FROM bls_observation")
    with store.session(settings.db_path, wait=0) as con:
        jobs.reparse(con, bls.JOB, settings=settings, now=NOW)
    assert _table(settings) == before


# --- wiring --------------------------------------------------------------------------------------


def test_bls_validate_stores_the_observations_it_fetched(settings, api, monkeypatch):
    for name, value in (
        ("USFIN_DB", settings.db_path),
        ("USFIN_RAW", settings.raw_dir),
        ("USFIN_CONFIG", settings.config_dir),
    ):
        monkeypatch.setenv(name, str(value))
    monkeypatch.chdir(settings.config_dir.parent)
    out = CliRunner().invoke(app, ["bls", "validate"])
    assert out.exit_code == 0, out.output
    assert len(_table(settings)) == 15
    # the job's next run sees them as already known
    assert _run(settings).rows_upserted == 0


def test_bls_is_registered_and_scheduled_at_0900_on_federal_days():
    assert "bls" in jobs.load_registry()
    assert scheduler.load_schedule(REPO_CONFIG)["bls"] == scheduler.Entry(
        "bls", "0 9 * * *", "federal"
    )
    assert bls.JOB.versioned == ("bls_observation",)
