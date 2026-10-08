"""Job `treasurydirect`: announced + auctioned/search feeds into `tsy_security`.

Fixtures (tests/fixtures/treasurydirect/) are real responses recorded on 2026-10-08 (01:16 UTC,
21:16 ET on 10-07) and trimmed to a few records each, otherwise byte-for-byte as served:
`announced` (two 2026-10-08 bills not yet auctioned, a note and an FRN already auctioned),
`auctioned?days=7`, `search` for 2010-03-01..2010-03-10 and for 2026-09-28..2026-10-08.
Every record in the whole history (11 143 of them, 1979-10-31..2026-10-08) carries the same 120
keys, so 2010 and today need no field-name map, only camelCase -> typed columns.
"""

import copy
import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx

from usfin import alerts, http, jobs, ratelimit, scheduler, store
from usfin.settings import load_settings
from usfin.sources import treasurydirect as td

FIXTURES = Path(__file__).parent / "fixtures" / "treasurydirect"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
HOST = "www.treasurydirect.gov"
NOW = datetime(2026, 10, 8, 18, 0, tzinfo=UTC)  # 14:00 ET, today 2026-10-08
LATER = datetime(2026, 10, 9, 18, 0, tzinfo=UTC)
RESULT_FIELDS = (
    "high_yield",
    "high_discount_rate",
    "high_investment_rate",
    "high_discount_margin",
    "price_per100",
    "bid_to_cover",
    "primary_dealer_accepted",
    "indirect_accepted",
    "direct_accepted",
)


def _fixture(name: str) -> list[dict]:
    return json.loads((FIXTURES / f"{name}.json").read_text("utf-8"))


def _blanked(record: dict, like: dict) -> dict:
    """`record` as a feed shows it before its auction: every key blank in `like` (a real
    not-yet-auctioned record) blanked here too."""
    out = dict(record)
    for key, value in like.items():
        if value == "":
            out[key] = ""
    return out


class FakeApi:
    """Serves `bodies[endpoint]` (announced / auctioned / search); records every request URL."""

    def __init__(self):
        self.bodies = {
            "announced": _fixture("announced"),
            "auctioned": _fixture("auctioned"),
            "search": _fixture("search_current"),
        }
        self.status: dict[str, int] = {}
        self.urls: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.urls.append(str(request.url))
        endpoint = request.url.path.rsplit("/", 1)[1]
        if endpoint in self.status:
            return httpx.Response(self.status[endpoint], text="<html>error</html>")
        return httpx.Response(200, json=self.bodies[endpoint])

    def endpoints(self) -> list[str]:
        return [httpx.URL(u).path.rsplit("/", 1)[1] for u in self.urls]


@pytest.fixture
def settings(tmp_path, monkeypatch):
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
    fake = FakeApi()
    with respx.mock(assert_all_called=False) as router:
        router.route(host=HOST).mock(side_effect=fake)
        yield fake


def _registry():
    reg = jobs.Registry()
    reg.register(td.JOB)
    return reg


def _run(settings, clock=NOW):
    return jobs.run_job(
        "treasurydirect", settings=settings, registry=_registry(), clock=lambda: clock
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _table(settings) -> list[tuple]:
    return sorted(_query(settings, "SELECT * FROM tsy_security"), key=repr)


def _row(settings, cusip: str, auction_date: str) -> dict:
    with store.session(settings.db_path, wait=0) as con:
        cur = con.execute(
            "SELECT * FROM tsy_security WHERE cusip = ? AND auction_date = ?",
            [cusip, date.fromisoformat(auction_date)],
        )
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
    assert row is not None, (cusip, auction_date)
    out = dict(zip(cols, row, strict=True))
    out["payload"] = json.loads(out["payload"])
    return out


def _watermarks(settings) -> dict[str, str]:
    with store.session(settings.db_path, wait=0) as con:
        return jobs.get_watermarks(con, "treasurydirect")


# --- request windows ---------------------------------------------------------------------------


def test_no_watermark_asks_the_auctioned_feed_for_seven_days():
    url = td.results_url(None, date(2026, 10, 8))
    assert url == f"https://{HOST}/TA_WS/securities/auctioned?format=json&days=7"


def test_a_watermark_within_seven_days_uses_the_auctioned_feed():
    assert td.results_url("2026-10-01", date(2026, 10, 8)).endswith("/auctioned?format=json&days=7")
    assert td.results_url("2026-10-07", date(2026, 10, 8)).endswith("/auctioned?format=json&days=7")


def test_an_older_watermark_searches_from_seven_days_before_it_with_camelcase_params():
    url = td.results_url("2026-09-30", date(2026, 10, 8))
    assert url == (
        f"https://{HOST}/TA_WS/securities/search?startDate=2026-09-23&endDate=2026-10-08"
        "&dateFieldName=auctionDate&format=json"
    )
    # lowercase names are ignored upstream and return the whole (37 MB) history
    assert "startdate=" not in url and "enddate=" not in url and "datefieldname" not in url


def test_search_url_builds_any_window():
    url = td.search_url(date(2010, 3, 1), date(2010, 3, 10))
    assert "startDate=2010-03-01&endDate=2010-03-10&dateFieldName=auctionDate" in url


# --- parsing -----------------------------------------------------------------------------------


def test_2010_and_current_records_map_to_the_same_typed_columns():
    old = [td.parse_record(r) for r in _fixture("search_2010")]
    new = [td.parse_record(r) for r in _fixture("search_current")]
    assert {tuple(r) for r in old} == {tuple(r) for r in new} == {tuple(td.COLUMNS)}
    note_2010 = next(r for r in old if r["cusip"] == "912828MP2")
    assert note_2010["auction_date"] == date(2010, 3, 10)
    assert note_2010["issue_date"] == date(2010, 3, 15)
    assert note_2010["maturity_date"] == date(2020, 2, 15)
    assert note_2010["security_type"] == "Note"
    assert note_2010["security_term"] == "9-Year 11-Month"
    assert note_2010["offering_amt"] == Decimal(21000000000)
    assert note_2010["high_yield"] == pytest.approx(3.735)
    assert note_2010["price_per100"] == pytest.approx(99.090493)
    assert note_2010["bid_to_cover"] == pytest.approx(3.45)
    assert note_2010["primary_dealer_accepted"] == Decimal(9857410000)
    assert note_2010["indirect_accepted"] == Decimal(7318990000)
    assert note_2010["direct_accepted"] == Decimal(3653110800)
    assert note_2010["status"] == "auctioned"
    note_now = next(r for r in new if r["cusip"] == "91282CRF0")
    assert note_now["high_yield"] == pytest.approx(5.3)
    assert note_now["status"] == "auctioned"


def test_blank_strings_are_null_and_t00_dates_are_dates():
    bond = td.parse_record(next(r for r in _fixture("search_current") if r["cusip"] == "912810UW6"))
    assert bond["auction_date"] == date(2026, 10, 8)
    assert bond["issue_date"] == date(2026, 10, 15)
    assert all(bond[f] is None for f in RESULT_FIELDS)
    assert bond["status"] == "announced"
    assert bond["offering_amt"] == Decimal(22000000000)


def test_a_bill_is_auctioned_by_its_discount_rate_and_an_frn_by_its_discount_margin():
    cmb = td.parse_record(next(r for r in _fixture("search_2010") if r["cusip"] == "912795U41"))
    assert cmb["high_yield"] is None
    assert cmb["high_discount_rate"] == pytest.approx(0.145)
    assert cmb["high_investment_rate"] == pytest.approx(0.147)
    assert cmb["status"] == "auctioned"
    frn = td.parse_record(next(r for r in _fixture("announced") if r["type"] == "FRN"))
    assert frn["high_yield"] is None and frn["high_discount_rate"] is None
    assert frn["high_discount_margin"] is not None
    assert frn["status"] == "auctioned"


def test_the_payload_keeps_every_upstream_key():
    record = _fixture("search_current")[0]
    assert set(record) == td.KNOWN_FIELDS
    assert json.loads(td.parse_record(record)["payload"]) == record


# --- the job -----------------------------------------------------------------------------------


def test_job_is_registered_and_scheduled_at_14_on_bond_days(monkeypatch):
    monkeypatch.delenv("USFIN_DEMO", raising=False)
    registry = jobs.load_registry()
    assert "treasurydirect" in registry
    job = registry.get("treasurydirect")
    assert set(job.parser_map(load_settings())) == {"td_announced", "td_auctioned", "td_search"}
    entries = scheduler.load_schedule(REPO_CONFIG)
    assert entries["treasurydirect"].cron == "0 14 * * *"
    assert entries["treasurydirect"].calendar == "bond"


def test_first_run_stores_announced_and_auctioned_rows(settings, api):
    result = _run(settings)
    assert result.status == "ok", result.error
    assert api.endpoints() == ["announced", "auctioned"]
    assert api.urls[0] == f"https://{HOST}/TA_WS/securities/announced?format=json"
    pks = {(r["cusip"], r["auctionDate"][:10]) for b in ("announced", "auctioned")
           for r in api.bodies[b]}
    rows = _query(settings, "SELECT cusip, auction_date, status FROM tsy_security")
    assert {(c, d.isoformat()) for c, d, _ in rows} == pks
    assert result.rows_upserted == len(pks)  # the note in both feeds is identical: counted once
    status = {c: s for c, _, s in rows}
    assert status["912797VW4"] == "announced"  # 10-08 bill, not auctioned yet
    assert status["91282CRF0"] == "auctioned"
    assert status["91282CRD5"] == "auctioned"  # FRN
    raw = _query(settings, "SELECT source, dataset FROM raw_response ORDER BY id")
    assert raw == [("treasurydirect", "td_announced"), ("treasurydirect", "td_auctioned")]
    # announced: newest auction date listed (today); results: newest auctioned date
    assert _watermarks(settings) == {"announced": "2026-10-08", "results": "2026-10-07"}


def test_a_second_run_changes_nothing(settings, api):
    _run(settings)
    before = _table(settings)
    result = _run(settings, clock=LATER)
    assert result.status == "ok", result.error
    assert result.rows_upserted == 0
    assert _table(settings) == before


def test_an_old_watermark_searches_and_2010_rows_land_typed(settings, api):
    with store.session(settings.db_path) as con:
        jobs.set_watermark(con, "treasurydirect", "results", "2010-03-08", today=date(2026, 10, 8))
    api.bodies["search"] = _fixture("search_2010")
    result = _run(settings)
    assert result.status == "ok", result.error
    assert api.endpoints() == ["announced", "search"]
    assert api.urls[1] == (
        f"https://{HOST}/TA_WS/securities/search?startDate=2010-03-01&endDate=2026-10-08"
        "&dateFieldName=auctionDate&format=json"
    )
    row = _row(settings, "912828MT4", "2010-03-09")
    assert row["high_yield"] == pytest.approx(1.437)
    assert row["status"] == "auctioned"
    assert _watermarks(settings)["results"] == "2010-03-10"


def test_announced_never_lowers_status_or_blanks_results(settings, api):
    _run(settings)
    before = _row(settings, "91282CRF0", "2026-10-07")
    assert before["status"] == "auctioned" and before["high_yield"] == pytest.approx(5.3)
    pending_like = next(r for r in _fixture("search_current") if r["cusip"] == "912810UW6")
    announced = copy.deepcopy(api.bodies["announced"])
    for i, record in enumerate(announced):
        if record["cusip"] == "91282CRF0":
            announced[i] = _blanked(record, pending_like)
    api.bodies["announced"] = announced
    api.bodies["auctioned"] = []
    result = _run(settings, clock=LATER)
    assert result.status == "ok", result.error
    assert result.rows_upserted == 0
    assert _row(settings, "91282CRF0", "2026-10-07") == before


def test_a_result_arriving_later_raises_the_status(settings, api):
    pending_like = next(r for r in _fixture("search_current") if r["cusip"] == "912810UW6")
    auctioned = next(r for r in _fixture("auctioned") if r["cusip"] == "912797WY9")
    api.bodies["announced"] = [_blanked(auctioned, pending_like)]
    api.bodies["auctioned"] = []
    _run(settings)
    assert _row(settings, "912797WY9", "2026-10-07")["status"] == "announced"
    api.bodies["auctioned"] = [auctioned]
    result = _run(settings, clock=LATER)
    assert result.rows_upserted == 1
    row = _row(settings, "912797WY9", "2026-10-07")
    assert row["status"] == "auctioned"
    assert row["high_discount_rate"] == pytest.approx(4.105)
    assert row["payload"] == auctioned


def test_an_unknown_key_stays_in_payload_and_raises_schema_drift_once(settings, api):
    _run(settings)
    auctioned = copy.deepcopy(api.bodies["auctioned"])
    for record in auctioned:
        record["newFieldPct"] = "1.25"
    api.bodies["auctioned"] = auctioned
    result = _run(settings, clock=LATER)
    assert result.status == "ok", result.error
    row = _row(settings, "91282CRF0", "2026-10-07")
    assert row["payload"]["newFieldPct"] == "1.25"
    with store.session(settings.db_path, wait=0) as con:
        drift = [a for a in alerts.list_alerts(con) if a["rule"] == "schema_drift"]
    assert len(drift) == 1
    assert drift[0]["job"] == "treasurydirect"
    assert drift[0]["subject"] == "tsy_security"
    assert drift[0]["detail"] == {"table": "tsy_security", "fields": ["newFieldPct"]}
    # the key is stored now: a later day does not raise it again
    _run(settings, clock=datetime(2026, 10, 13, 18, 0, tzinfo=UTC))
    with store.session(settings.db_path, wait=0) as con:
        drift = [a for a in alerts.list_alerts(con) if a["rule"] == "schema_drift"]
    assert len(drift) == 1


def test_a_failing_feed_keeps_its_watermark_and_the_other_key_commits(settings, api):
    _run(settings)
    marks = _watermarks(settings)
    api.status["auctioned"] = 500
    result = _run(settings, clock=LATER)
    assert result.status == "failed"
    assert set(result.key_errors) == {"results"}
    assert _watermarks(settings)["results"] == marks["results"]


class FailingTransport(httpx.BaseTransport):
    def handle_request(self, request):
        raise AssertionError(f"reparse sent a request: {request.url}")


def test_reparse_rebuilds_the_identical_table_without_http(settings, api):
    _run(settings)
    api.bodies["search"] = _fixture("search_2010")
    with store.session(settings.db_path) as con:
        jobs.set_watermark(con, "treasurydirect", "results", "2010-03-08", today=date(2026, 10, 9))
    _run(settings, clock=LATER)
    built = _table(settings)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM tsy_security")
        result = jobs.reparse(
            con, td.JOB, settings=settings, now=LATER, transport=FailingTransport()
        )
    assert len(result.raw_ids) == 4
    assert _table(settings) == built
