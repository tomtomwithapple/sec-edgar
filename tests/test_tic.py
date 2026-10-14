"""TIC cross-border securities, point in time (S43).

`tests/fixtures/tic/cslt.json` is cut from the live `cslt.zip` (2026-10-14, release transmitted
2026-09-10, file modified 2026-09-16 20:02 GMT): six series with their newest three months —
Indonesia's Treasury holdings and net purchases, US net purchases of Indonesian equities, Japan's
and the grand total's Treasury holdings, and one discontinued series (agency bonds, 10308, ending
2011-12). Notes are shortened; nothing else is edited.

`FakeTic` answers the way ticdata.treasury.gov did live on 2026-10-14: the ZIP with a
`Last-Modified`, and 304 to an `If-Modified-Since` at or after it (an `If-None-Match` is ignored).
"""

import copy
import io
import json
import shutil
import zipfile
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from usfin import http, jobs, ratelimit, raw, scheduler, store
from usfin.settings import load_settings
from usfin.sources import tic
from usfin.web import app as web

FIXTURE = Path(__file__).parent / "fixtures" / "tic" / "cslt.json"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
TIC_HOST = "ticdata.treasury.gov"
IDN_POS = "for_treas_pos_42218"
JUL = date(2026, 7, 1)


def _naive(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


T1 = _naive(2026, 10, 14, 21, 15)  # 17:15 ET, the scheduled fire
T2 = _naive(2026, 10, 15, 21, 15)
T3 = _naive(2026, 10, 16, 21, 15)
MODIFIED = datetime(2026, 9, 16, 20, 2, 1, tzinfo=UTC)


class FakeTic:
    def __init__(self):
        self.doc = json.loads(FIXTURE.read_text("utf-8"))
        self.modified = MODIFIED
        self.status: int | None = None
        """Answer this status instead (e.g. 500)."""
        self.requests: list[httpx.Request] = []

    def release(self, at: datetime, edit) -> None:
        """A new release modified at `at`: `edit(series by id)` changes the document."""
        self.doc = copy.deepcopy(self.doc)
        edit({s["source_id"]: s for s in self.doc["series"]})
        self.modified = at.replace(tzinfo=UTC)

    def body(self) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("cslt.json", json.dumps(self.doc))
        return buf.getvalue()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.url.path.endswith("/Documents/cslt.zip"), request.url
        if self.status:
            return httpx.Response(self.status, text="nope")
        since = request.headers.get("If-Modified-Since")
        if since and parsedate_to_datetime(since) >= self.modified.replace(microsecond=0):
            return httpx.Response(304)
        return httpx.Response(
            200,
            content=self.body(),
            headers={
                "Content-Type": "application/zip",
                "Last-Modified": format_datetime(self.modified, usegmt=True),
            },
        )


class Clock:
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
    )


@pytest.fixture
def api(settings):
    fake = FakeTic()
    with respx.mock(assert_all_called=False) as router:
        router.route(host=TIC_HOST).mock(side_effect=fake)
        yield fake


def _run(settings, clock, at=T1):
    clock.t = at
    reg = jobs.Registry()
    reg.register(tic.JOB)
    return jobs.run_job(
        "tic", settings=settings, registry=reg, clock=lambda: at.replace(tzinfo=UTC)
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _table(settings) -> list[tuple]:
    return _query(
        settings, "SELECT series_id, date, value, first_seen_at FROM tic_observation ORDER BY ALL"
    )


def _versions(settings, sid=IDN_POS, day=JUL) -> list[tuple]:
    return _query(
        settings,
        "SELECT value, first_seen_at FROM tic_observation WHERE series_id = ? AND date = ?"
        " ORDER BY first_seen_at",
        [sid, day],
    )


# --- first load ---------------------------------------------------------------------------


def test_first_run_stores_every_observation_first_seen_when_fetched(settings, api, clock):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    rows = _table(settings)
    assert len(rows) == 6 * 3
    assert _versions(settings) == [(Decimal(37547), T1)]
    assert _versions(settings, "for_treas_pos_42609") == [(Decimal(1103943), T1)]  # Japan
    assert _query(settings, "SELECT key, value FROM watermark WHERE job = 'tic'") == [
        ("cslt", "2026-07-01")
    ]
    assert "If-Modified-Since" not in api.requests[0].headers


def test_catalog_entry_splits_the_series_id(settings, api, clock):
    _run(settings, clock)
    got = _query(
        settings,
        "SELECT series_id, measure, kind, country_code, country, active, units, transmitted_at"
        " FROM tic_series ORDER BY series_id",
    )
    assert len(got) == 6
    assert got[0] == (
        "for_lt_agcy_net_10308", "for_lt_agcy", "net", "10308",
        got[0][4], False, "Millions of Dollars", _naive(2026, 9, 10, 16, 33, 47, 617012),
    )  # fmt: skip
    idn = {r[0]: r for r in got}[IDN_POS]
    assert idn[1:6] == ("for_treas", "pos", "42218", "Indonesia", True)
    assert {r[0]: r for r in got}["us_lt_eqty_net_42218"][1:3] == ("us_lt_eqty", "net")
    title = _query(settings, "SELECT title FROM tic_series WHERE series_id = ?", [IDN_POS])
    expected = (
        "Foreign portfolio holdings of U.S. long-term and short-term Treasury securities: Indonesia"
    )
    assert title == [(expected,)]


def test_split_series_id():
    assert tic.split_series_id("for_treas_pos_42218") == ("for_treas", "pos", "42218")
    assert tic.split_series_id("us_lt_govt_bond_valchg_99996") == (
        "us_lt_govt_bond", "valchg", "99996",
    )  # fmt: skip
    with pytest.raises(ValueError):
        tic.split_series_id("for_treas_42218")


# --- later runs ---------------------------------------------------------------------------


def test_a_later_run_is_conditional_and_a_304_stores_nothing(settings, api, clock):
    _run(settings, clock)
    before = _table(settings)
    api.requests.clear()
    result = _run(settings, clock, T2)
    assert result.status == "ok", result.error
    (req,) = api.requests
    assert parsedate_to_datetime(req.headers["If-Modified-Since"]) == T1.replace(tzinfo=UTC)
    assert _table(settings) == before
    assert _query(settings, "SELECT http_status FROM raw_response ORDER BY id") == [(200,), (304,)]


def test_a_new_release_adds_new_months_and_revisions_only(settings, api, clock):
    _run(settings, clock)

    def edit(by_id):
        idn = by_id[IDN_POS]["observations"]
        idn[0][1] = "37600.0"  # 2026-07 revised
        idn.insert(0, ["2026-08-01", "38000.0"])  # a new month

    api.release(T2, edit)
    assert _run(settings, clock, T3).status == "ok"
    assert _versions(settings) == [(Decimal(37547), T1), (Decimal(37600), T3)]
    assert _versions(settings, day=date(2026, 8, 1)) == [(Decimal(38000), T3)]
    assert len(_table(settings)) == 18 + 2  # nothing else re-stored
    assert _query(settings, "SELECT value FROM watermark WHERE job = 'tic'") == [("2026-08-01",)]


def test_tic_as_of_gives_the_value_known_that_day(settings, api, clock):
    _run(settings, clock)
    api.release(T2, lambda by_id: by_id[IDN_POS]["observations"][0].__setitem__(1, "37600.0"))
    _run(settings, clock, T3)
    as_of = "SELECT value FROM tic_as_of(CAST(? AS DATE)) WHERE series_id = ? AND date = ?"
    assert _query(settings, as_of, ["2026-10-15", IDN_POS, JUL]) == [(Decimal(37547),)]
    assert _query(settings, as_of, ["2026-10-16", IDN_POS, JUL]) == [(Decimal(37600),)]
    assert _query(settings, as_of, ["2026-10-13", IDN_POS, JUL]) == []  # not fetched yet
    latest = "SELECT value FROM tic_observation_latest WHERE series_id = ? AND date = ?"
    assert _query(settings, latest, [IDN_POS, JUL]) == [(Decimal(37600),)]


def test_without_a_watermark_the_request_is_unconditional(settings, api, clock):
    _run(settings, clock)
    _query(settings, "DELETE FROM watermark WHERE job = 'tic'")
    api.requests.clear()
    before = _table(settings)
    assert _run(settings, clock, T2).status == "ok"
    assert "If-Modified-Since" not in api.requests[0].headers
    assert _table(settings) == before  # same values: no new versions


def test_an_error_status_fails_the_run(settings, api, clock):
    api.status = 500
    result = _run(settings, clock)
    assert result.status == "failed"
    assert "500" in result.key_errors["cslt"]
    assert _table(settings) == []


def test_missing_values_are_null(settings, api, clock):
    api.doc["series"][1]["observations"][0][1] = "n.a."
    assert _run(settings, clock).status == "ok"
    sid, day = api.doc["series"][1]["source_id"], api.doc["series"][1]["observations"][0][0]
    assert _versions(settings, sid, date.fromisoformat(day)) == [(None, T1)]


# --- reparse, schedule --------------------------------------------------------------------


def test_reparse_rebuilds_the_same_rows(settings, api, clock):
    _run(settings, clock)
    api.release(T2, lambda by_id: by_id[IDN_POS]["observations"][0].__setitem__(1, "37600.0"))
    _run(settings, clock, T3)
    _run(settings, clock, _naive(2026, 10, 17, 21, 15))  # a 304 in between
    before = _table(settings)
    with store.session(settings.db_path, wait=0) as con:
        result = jobs.reparse(con, tic.JOB, settings=settings)
    assert result.deleted == len(before)
    assert _table(settings) == before


def test_the_job_is_scheduled():
    entry = scheduler.load_schedule(REPO_CONFIG)["tic"]
    assert entry.cron == "15 17 * * *"


def test_the_job_is_in_the_registry():
    assert "tic" in jobs.load_registry()


# --- web ----------------------------------------------------------------------------------


@pytest.fixture
def client(settings, api, clock):
    _run(settings, clock)
    s = scheduler.Scheduler(settings, jobs.Registry(), clock=lambda: T1.replace(tzinfo=UTC))
    yield TestClient(web.create_app(s, manage=False, ui_wait=0.3))
    s.shutdown()


def test_series_index_has_one_tic_row(client):
    page = client.get("/series?format=csv").text
    assert "tic,cslt," in page
    (line,) = [x for x in page.splitlines() if x.startswith("tic,")]
    assert line.endswith(",18,2011-10-01,2026-07-01")


def test_tic_catalog_filters(client):
    rows = client.get("/series/tic?format=csv").text.splitlines()
    assert rows[0] == "series_id,title,measure,kind,country_code,country,active,last,value"
    assert len(rows) == 1 + 6
    idn = client.get("/series/tic?country=indonesia&format=csv").text.splitlines()
    assert len(idn) == 1 + 3
    assert len(client.get("/series/tic?country=42218&kind=pos&format=csv").text.splitlines()) == 2
    assert (
        len(client.get("/series/tic?measure=for_treas&kind=pos&format=csv").text.splitlines()) == 4
    )
    page = client.get("/series/tic?country=Indonesia")
    assert page.status_code == 200 and f"/series/tic/{IDN_POS}" in page.text


def test_tic_series_page_shows_latest_and_as_of(client):
    page = client.get(f"/series/tic/{IDN_POS}")
    assert page.status_code == 200
    assert "Indonesia" in page.text and "<svg" in page.text
    rows = client.get(f"/series/tic/{IDN_POS}?format=csv").text.splitlines()
    assert rows[0] == "date,value,first_seen_at,versions"
    assert rows[1:] == [
        "2026-05-01,35663,2026-10-14 21:15:00Z,1",
        "2026-06-01,32009,2026-10-14 21:15:00Z,1",
        "2026-07-01,37547,2026-10-14 21:15:00Z,1",
    ]
    assert client.get(f"/series/tic/{IDN_POS}?as_of=2026-10-13&format=csv").text.splitlines() == [
        "date,value,first_seen_at,versions"
    ]


def test_tic_page_errors(client):
    assert client.get("/series/tic/nope_pos_1").status_code == 404
    assert client.get(f"/series/tic/{IDN_POS}?as_of=July").status_code == 400
