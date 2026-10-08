"""ALFRED backfill of BLS first-published history (S18, C4).

`tests/fixtures/alfred/UNRATE.json` is a live ALFRED response (2026-10-08,
`fred/series/observations?series_id=UNRATE&realtime_start=1776-07-04&realtime_end=9999-12-31
&file_type=json`) trimmed to observation dates from 2019-01 (`count` adjusted); no API key is in
it. The BLS side is test_bls_versions' keyless v1 recording. `raw._utcnow` is pinned so each
response's `fetched_at` is known.
"""

import copy
import json
import logging
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, jobs, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources import alfred, bls

FIXTURES = Path(__file__).parent / "fixtures"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
FRED_HOST = "api.stlouisfed.org"
BLS_HOST = "api.bls.gov"
UNRATE = "LNS14000000"
KEY = "0123456789abcdef0123456789abcdef"  # made up


def _naive(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


T_BLS = _naive(2026, 10, 8, 13, 0)  # the first BLS fetch: 09:00 ET
T_ALFRED = _naive(2026, 10, 8, 14, 0)  # the backfill's fetch


def _unrate() -> dict:
    return json.loads((FIXTURES / "alfred" / "UNRATE.json").read_text("utf-8"))


def _empty(fred_id: str) -> dict:
    doc = _unrate()
    doc["observations"], doc["count"] = [], 0
    return doc


class Clock:
    def __init__(self):
        self.t = T_BLS

    def __call__(self) -> datetime:
        return self.t


class FakeFred:
    """Answers `fred/series/observations` per `series_id`; records the requests."""

    def __init__(self):
        self.docs = {"UNRATE": _unrate()}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        sid = request.url.params["series_id"]
        return httpx.Response(200, json=self.docs.get(sid) or _empty(sid))


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
    monkeypatch.delenv("FRED_API_KEY", raising=False)
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
        fred_api_key=KEY,
    )


@pytest.fixture
def fred(settings):
    fake = FakeFred()
    bls_doc = json.loads((FIXTURES / "bls" / "v1_timeseries.json").read_text("utf-8"))
    with respx.mock(assert_all_called=False) as router:
        router.route(host=FRED_HOST).mock(side_effect=fake)
        router.route(host=BLS_HOST).mock(return_value=httpx.Response(200, json=bls_doc))
        yield fake


def _run_bls(settings, clock, at=T_BLS):
    clock.t = at
    reg = jobs.Registry()
    reg.register(bls.JOB)
    result = jobs.run_job(
        "bls", settings=settings, registry=reg, clock=lambda: at.replace(tzinfo=UTC)
    )
    assert result.status == "ok", result.error
    return result


def _backfill(settings, clock, at=T_ALFRED):
    clock.t = at
    with (
        store.session(settings.db_path, wait=0) as con,
        http.Client(con, settings=settings) as client,
    ):
        return alfred.backfill(con, settings, client)


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _table(settings) -> list[tuple]:
    return _query(
        settings,
        "SELECT series_id, year, period, value, footnotes, first_seen_at, raw_id, source"
        " FROM bls_observation ORDER BY ALL",
    )


def _versions(settings, year, period, series_id=UNRATE) -> list[tuple]:
    return _query(
        settings,
        "SELECT value, first_seen_at, source FROM bls_observation"
        " WHERE series_id = ? AND year = ? AND period = ? ORDER BY first_seen_at",
        [series_id, year, period],
    )


# --- vintages become rows ------------------------------------------------------------------------


def test_vintages_become_alfred_rows_first_seen_at_realtime_start_midnight_new_york(
    settings, fred, clock
):
    _run_bls(settings, clock)
    _backfill(settings, clock)
    # April 2020: first printed 14.7 on 2020-05-08, then 14.8, 14.7, 14.9, 14.8
    assert _versions(settings, 2020, "M04") == [
        (Decimal("14.7"), _naive(2020, 5, 8, 4, 0), "alfred"),  # EDT: 00:00 ET = 04:00 UTC
        (Decimal("14.8"), _naive(2021, 1, 8, 5, 0), "alfred"),  # EST: 05:00 UTC
        (Decimal("14.7"), _naive(2022, 1, 7, 5, 0), "alfred"),
        (Decimal("14.9"), _naive(2024, 1, 5, 5, 0), "alfred"),
        (Decimal("14.8"), _naive(2024, 1, 10, 5, 0), "alfred"),
    ]
    raw_ids = {
        r[0] for r in _query(settings, "SELECT raw_id FROM bls_observation WHERE source = 'alfred'")
    }
    assert _query(
        settings,
        "SELECT DISTINCT dataset, source FROM raw_response WHERE id IN ("
        + ", ".join(str(i) for i in raw_ids)
        + ")",
    ) == [("alfred_observations", "alfred")]


def test_one_request_per_configured_series_by_its_fred_id(settings, fred, clock):
    _run_bls(settings, clock)
    _backfill(settings, clock)
    sent = [r.url.params for r in fred.requests]
    assert sorted(p["series_id"] for p in sent) == sorted(
        s.fred_id for s in bls.load_series(settings.config_dir).values()
    )
    p = sent[0]
    assert (p["realtime_start"], p["realtime_end"], p["file_type"]) == (
        "1776-07-04",
        "9999-12-31",
        "json",
    )
    assert p["api_key"] == KEY


def test_a_dot_is_stored_as_a_null_version(settings, fred, clock):
    _run_bls(settings, clock)
    _backfill(settings, clock)
    assert _versions(settings, 2025, "M10") == [(None, _naive(2025, 12, 16, 5, 0), "alfred")]


def test_bls_as_of_a_date_returns_the_first_published_value(settings, fred, clock):
    _run_bls(settings, clock)
    _backfill(settings, clock)

    def as_of(day):
        return _query(
            settings,
            f"SELECT value, source FROM bls_as_of(DATE '{day}')"
            f" WHERE series_id = '{UNRATE}' AND year = 2020 AND period = 'M04'",
        )

    assert as_of("2020-05-07") == []
    assert as_of("2020-06-01") == [(Decimal("14.7"), "alfred")]
    assert as_of("2021-06-01") == [(Decimal("14.8"), "alfred")]


# --- the cut-off ---------------------------------------------------------------------------------


def test_only_vintages_before_the_first_bls_fetch_of_the_series_are_kept(settings, fred, clock):
    # the first BLS response is dated 2026-09-04 15:00 UTC (11:00 ET): ALFRED's 2026-09-04
    # vintage (00:00 ET) is kept, its 2026-10-02 one is not — BLS carried that value
    _run_bls(settings, clock, at=_naive(2026, 9, 4, 15, 0))
    _backfill(settings, clock)
    assert _versions(settings, 2026, "M08") == [
        (Decimal("4.1"), _naive(2026, 9, 4, 4, 0), "alfred")
    ]
    assert [r[2] for r in _versions(settings, 2026, "M09")] == ["bls"]
    assert not _query(
        settings,
        "SELECT * FROM bls_observation WHERE source = 'alfred' AND first_seen_at >= ?",
        [_naive(2026, 9, 4, 15, 0)],
    )


def test_without_any_bls_fetch_every_vintage_is_kept(settings, fred, clock):
    _backfill(settings, clock)
    assert _versions(settings, 2026, "M09") == [
        (Decimal("4.2"), _naive(2026, 10, 2, 4, 0), "alfred")
    ]


# --- rounding ------------------------------------------------------------------------------------


def _set(doc, day, realtime_start, value):
    for o in doc["observations"]:
        if o["date"] == day and o["realtime_start"] == realtime_start:
            o["value"] = value
            return
    raise AssertionError((day, realtime_start))


def test_vintages_compare_and_store_at_the_series_decimals(settings, fred, clock):
    doc = fred.docs["UNRATE"]
    # UNRATE publishes 1 decimal: 14.74 is the 14.7 already known, 14.76 is a new 14.8
    _set(doc, "2020-04-01", "2022-01-07", "14.84")  # was 14.7: still a revision, to 14.8
    _set(doc, "2020-04-01", "2024-01-05", "14.76")  # was 14.9: 14.8 again, no new version
    _set(doc, "2020-03-01", "2020-04-03", "4.40")
    _run_bls(settings, clock)
    _backfill(settings, clock)
    assert [(v, s) for v, s, _ in _versions(settings, 2020, "M04")] == [
        (Decimal("14.7"), _naive(2020, 5, 8, 4, 0)),
        (Decimal("14.8"), _naive(2021, 1, 8, 5, 0)),
    ]
    assert _versions(settings, 2020, "M03")[0][0] == Decimal("4.4")


def test_a_bls_value_equal_to_the_last_vintage_at_the_decimals_adds_no_version(
    settings, fred, clock
):
    _run_bls(settings, clock)
    _backfill(settings, clock)
    # the BLS fixture's UNRATE 2026 M08/M09 equal ALFRED's latest vintages: only ALFRED rows
    assert not _query(
        settings,
        f"SELECT * FROM bls_observation WHERE series_id = '{UNRATE}' AND source = 'bls'",
    )
    # CPI has no ALFRED vintages in this test, so its BLS rows stay
    assert (
        _query(
            settings,
            "SELECT count(*) FROM bls_observation WHERE series_id = 'CUSR0000SA0'"
            " AND source = 'bls'",
        )[0][0]
        > 0
    )


# --- reparse and reruns --------------------------------------------------------------------------


def test_reparse_replays_alfred_first_then_bls_to_an_identical_table(settings, fred, clock):
    _run_bls(settings, clock)
    _backfill(settings, clock)
    before = _table(settings)
    requests = len(fred.requests)
    with store.session(settings.db_path, wait=0) as con:
        order = []
        real = bls.JOB.parser_map(settings)

        def recording(dataset, parser):
            def parse(ctx, raw_id):
                order.append(dataset)
                return parser(ctx, raw_id)

            return parse

        spy = replace(bls.JOB, parsers={ds: recording(ds, p) for ds, p in real.items()})
        result = jobs.reparse(con, spy, settings=settings, now=T_ALFRED.replace(tzinfo=UTC))
    assert order == ["alfred_observations"] * 7 + ["bls_timeseries"]
    assert result.deleted == result.rows == len(before)
    assert _table(settings) == before
    assert len(fred.requests) == requests  # no HTTP


def test_a_second_backfill_changes_nothing(settings, fred, clock):
    _run_bls(settings, clock)
    _backfill(settings, clock)
    before = _table(settings)
    _backfill(settings, clock, at=_naive(2026, 10, 9, 14, 0))
    assert _table(settings) == before


def test_a_bls_run_after_the_backfill_compares_with_alfred_versions(settings, fred, clock):
    _run_bls(settings, clock)
    _backfill(settings, clock)
    before = _table(settings)
    assert _run_bls(settings, clock, at=_naive(2026, 10, 9, 13, 0)).rows_upserted == 0
    assert _table(settings) == before


def test_a_vintage_at_an_instant_already_stored_keeps_the_first(settings, fred, clock):
    _run_bls(settings, clock)
    _backfill(settings, clock)
    before = _table(settings)
    fred.docs["UNRATE"] = copy.deepcopy(fred.docs["UNRATE"])
    _set(fred.docs["UNRATE"], "2020-04-01", "2020-05-08", "15.0")  # ALFRED rewrote a vintage
    _backfill(settings, clock, at=_naive(2026, 10, 9, 14, 0))
    assert _table(settings) == before


# --- the key -------------------------------------------------------------------------------------


def test_the_api_key_is_never_archived_or_logged(settings, fred, clock, caplog):
    caplog.set_level(logging.INFO)
    _backfill(settings, clock)
    urls = [
        u
        for (u,) in _query(
            settings, "SELECT url FROM raw_response WHERE dataset = 'alfred_observations'"
        )
    ]
    assert len(urls) == 7
    assert all(KEY not in u and "api_key=REDACTED" in u and "series_id=" in u for u in urls)
    assert KEY not in caplog.text
    assert "api_key=REDACTED" in caplog.text  # httpx's own request log line


def test_a_failed_request_names_the_url_without_the_key(settings, clock):
    with respx.mock() as router:
        router.route(host=FRED_HOST).mock(
            return_value=httpx.Response(
                400, json={"error_code": 400, "error_message": "Bad Request."}
            )
        )
        with pytest.raises(alfred.AlfredError) as err:
            _backfill(settings, clock)
    assert "400" in str(err.value) and KEY not in str(err.value)


def _cli(settings, monkeypatch, *args):
    for name, value in (
        ("USFIN_DB", settings.db_path),
        ("USFIN_RAW", settings.raw_dir),
        ("USFIN_CONFIG", settings.config_dir),
    ):
        monkeypatch.setenv(name, str(value))
    monkeypatch.chdir(settings.config_dir.parent)
    return CliRunner().invoke(app, ["bls", "alfred-backfill", *args])


def test_cli_without_fred_api_key_blocks_with_a_clear_message_and_sends_nothing(
    settings, monkeypatch
):
    with respx.mock(assert_all_called=False) as router:
        route = router.route(host=FRED_HOST)
        out = _cli(settings, monkeypatch)
    assert out.exit_code == 1
    assert "FRED_API_KEY" in out.output
    assert not route.called


def test_cli_backfills_and_reports(settings, fred, clock, monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", KEY)
    out = _cli(settings, monkeypatch)
    assert out.exit_code == 0, out.output
    assert "UNRATE" in out.output and KEY not in out.output
    assert _versions(settings, 2020, "M04")[0][0] == Decimal("14.7")


def test_bls_job_replays_alfred_first():
    assert bls.JOB.parser_map(None).keys() == {"bls_timeseries", "alfred_observations"}
    assert bls.JOB.replay_first == ("alfred_observations",)
