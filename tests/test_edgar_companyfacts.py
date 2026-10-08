"""EDGAR companyfacts (job `edgar_companyfacts`, table `xbrl_fact`, macro `xbrl_as_of`; C1, C2, C4).

`tests/fixtures/edgar/companyfacts/` holds two real responses fetched 2026-10-08 through the client
(`usfin http get`), trimmed by dropping taxonomies, tags, units and facts only (nothing else
edited; labels and descriptions verbatim):

- `CIK0000320193.json` (Apple; 3 789 099 bytes, dei + us-gaap only, 25 135 facts) cut to 9 facts:
  dei `EntityCommonStockSharesOutstanding` (an instant, 2026-07-17); us-gaap `SalesRevenueNet` for
  FY2009 (2008-09-28..2009-09-26) as first filed in the 10-K of 2009-10-27 (36 537 000 000) and as
  restated by the 10-K/A of 2010-01-25 (42 905 000 000 — the revenue-recognition change) and
  repeated in the 10-Ks of 2010 and 2011 (the 2011 one carries `frame` CY2009); `Assets` at
  2009-09-26 (instant) under the 10-K and the 10-K/A (53 851 vs 47 501 million);
  `EarningsPerShareBasic` (USD/shares, 6.39 vs 9.22).
- `CIK0000019617.json` (JPMorgan; 7 977 054 bytes, taxonomies dei, invest, srt, us-gaap, ffd, ecd)
  cut to 8 facts in dei, invest, srt, ecd and us-gaap: `invest`, `srt` and `ecd` are outside the
  plan's scope and must be dropped; us-gaap `NetIncomeLoss` 2024 includes the DEF 14A fact with
  `fy`/`fp` null.

No request leaves the process.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, jobs, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources.edgar import companyfacts

FIXTURES = Path(__file__).parent / "fixtures" / "edgar" / "companyfacts"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"
APPLE = 320193
JPM = 19617
FY2009 = (date(2008, 9, 28), date(2009, 9, 26))


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(utc(2026, 10, 8, 14, 0))
    monkeypatch.setattr(raw, "_utcnow", c.naive)
    return c


@pytest.fixture
def settings(tmp_path, monkeypatch, clock):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    for key in ("SEC_USER_AGENT", "ALERT_WEBHOOK_URL"):
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
    monkeypatch.setattr(http, "SEC_BLOCK", http.SecBlock())
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        sec_user_agent=UA,
    )


class Fake:
    """Answers data.sec.gov/api/xbrl/companyfacts/<name> from `docs` (404 for an unknown name)."""

    def __init__(self):
        self.docs = {
            p.name: json.loads(p.read_text("utf-8")) for p in sorted(FIXTURES.glob("*.json"))
        }
        self.status: dict[str, int] = {}
        self.requests: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        self.requests.append(name)
        status = self.status.get(name, 200 if name in self.docs else 404)
        if status != 200:
            return httpx.Response(status, text="<html>nope</html>")
        return httpx.Response(200, json=self.docs[name])

    @property
    def apple(self) -> dict:
        return self.docs["CIK0000320193.json"]


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"^https://data\.sec\.gov/api/xbrl/companyfacts/").mock(side_effect=f)
        yield f


def _run(settings, clock, ciks=(APPLE, JPM), when=None, job=None):
    if when is not None:
        clock.t = when
    reg = jobs.Registry()
    reg.register(job or companyfacts.for_ciks(list(ciks)))
    return jobs.run_job(
        "edgar_companyfacts", settings=settings, registry=reg, clock=clock, chain=False
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


COLUMNS = (
    "cik, taxonomy, tag, unit, accn, period_start, period_end, is_instant, value, fy, fp, form,"
    " filed, frame"
)


def _facts(settings):
    return sorted(_query(settings, f"SELECT {COLUMNS} FROM xbrl_fact"), key=repr)


# --- parsing ------------------------------------------------------------------------------------


def test_only_us_gaap_and_dei_facts_are_kept():
    doc = json.loads((FIXTURES / "CIK0000019617.json").read_text("utf-8"))
    assert set(doc["facts"]) == {"dei", "invest", "srt", "ecd", "us-gaap"}
    facts = companyfacts.facts_from_document(doc, cik=JPM)
    assert {f.taxonomy for f in facts} == {"dei", "us-gaap"}
    assert len(facts) == 4


def test_a_document_for_another_cik_is_rejected():
    doc = json.loads((FIXTURES / "CIK0000019617.json").read_text("utf-8"))
    with pytest.raises(companyfacts.CompanyFactsError, match="cik"):
        companyfacts.store_document(None, doc, name="CIK0000320193.json")  # before any SQL


def test_a_fact_without_accn_is_rejected():
    doc = json.loads((FIXTURES / "CIK0000320193.json").read_text("utf-8"))
    del doc["facts"]["us-gaap"]["Assets"]["units"]["USD"][0]["accn"]
    with pytest.raises(companyfacts.CompanyFactsError, match="accn"):
        companyfacts.facts_from_document(doc, cik=APPLE)


# --- job edgar_companyfacts ---------------------------------------------------------------------


def test_facts_land_in_xbrl_fact_with_instants_starting_where_they_end(settings, fake, clock):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert result.rows_upserted == 13  # Apple 9, JPMorgan 4 (us-gaap + dei)
    assert fake.requests == ["CIK0000320193.json", "CIK0000019617.json"]
    rows = {(r[2], r[4], r[5], r[6]): r for r in _facts(settings) if r[0] == APPLE}
    shares = rows["EntityCommonStockSharesOutstanding", "0000320193-26-000020",
                  date(2026, 7, 17), date(2026, 7, 17)]  # fmt: skip
    assert shares == (
        APPLE, "dei", "EntityCommonStockSharesOutstanding", "shares", "0000320193-26-000020",
        date(2026, 7, 17), date(2026, 7, 17), True, 14594180000.0, 2026, "Q3", "10-Q",
        date(2026, 7, 31), "CY2026Q2I",
    )  # fmt: skip
    first = rows["SalesRevenueNet", "0001193125-09-214859", *FY2009]
    assert first[7:] == (False, 36537000000.0, 2009, "FY", "10-K", date(2009, 10, 27), None)
    assert rows["EarningsPerShareBasic", "0001193125-10-012091", *FY2009][3] == "USD/shares"
    assert rows["EarningsPerShareBasic", "0001193125-10-012091", *FY2009][8] == 9.22
    assert _query(settings, "SELECT DISTINCT source, dataset FROM raw_response") == [
        ("edgar", "edgar_companyfacts")
    ]


def test_a_fact_with_no_fiscal_year_or_period_is_kept_with_nulls(settings, fake, clock):
    _run(settings, clock)
    assert _query(
        settings,
        "SELECT value, fy, fp, form FROM xbrl_fact WHERE cik = ? AND accn = ?",
        [JPM, "0000019617-26-000096"],
    ) == [(58500000000.0, None, None, "DEF 14A")]


def test_a_restated_value_under_a_later_accn_is_its_own_row(settings, fake, clock):
    _run(settings, clock)
    revenue = _query(
        settings,
        "SELECT accn, value, form, filed FROM xbrl_fact WHERE cik = ? AND tag = 'SalesRevenueNet'"
        " AND period_start = ? AND period_end = ? ORDER BY filed",
        [APPLE, *FY2009],
    )
    assert revenue == [
        ("0001193125-09-214859", 36537000000.0, "10-K", date(2009, 10, 27)),
        ("0001193125-10-012091", 42905000000.0, "10-K/A", date(2010, 1, 25)),
        ("0001193125-10-238044", 42905000000.0, "10-K", date(2010, 10, 27)),
        ("0001193125-11-282113", 42905000000.0, "10-K", date(2011, 10, 26)),
    ]


def test_xbrl_as_of_returns_the_facts_filed_by_that_date(settings, fake, clock):
    _run(settings, clock)

    def revenue_as_of(d):
        return _query(
            settings,
            "SELECT accn, value FROM xbrl_as_of(?) WHERE cik = ? AND tag = 'SalesRevenueNet'"
            " ORDER BY filed",
            [d, APPLE],
        )

    assert revenue_as_of(date(2009, 10, 26)) == []
    assert revenue_as_of(date(2009, 10, 27)) == [("0001193125-09-214859", 36537000000.0)]
    assert revenue_as_of(date(2010, 1, 25)) == [
        ("0001193125-09-214859", 36537000000.0),
        ("0001193125-10-012091", 42905000000.0),
    ]
    assert len(revenue_as_of(date(2026, 10, 8))) == 4
    assets = _query(
        settings,
        "SELECT value FROM xbrl_as_of(DATE '2009-12-31') WHERE cik = ? AND tag = 'Assets'",
        [APPLE],
    )
    assert assets == [(53851000000.0,)]


def test_the_watermark_is_the_newest_filed_date_per_cik(settings, fake, clock):
    _run(settings, clock)
    assert sorted(
        _query(settings, "SELECT key, value FROM watermark WHERE job = 'edgar_companyfacts'")
    ) == [("0000019617", "2026-04-06"), ("0000320193", "2026-07-31")]


def test_a_second_run_with_the_same_documents_changes_nothing(settings, fake, clock):
    _run(settings, clock)
    before = _facts(settings)
    again = _run(settings, clock, when=utc(2026, 10, 9, 14, 0))
    assert again.status == "ok" and again.rows_upserted == 0
    assert _facts(settings) == before


def test_a_later_document_adds_new_facts_and_updates_a_moved_frame(settings, fake, clock):
    _run(settings, clock)
    usd = fake.apple["facts"]["us-gaap"]["SalesRevenueNet"]["units"]["USD"]
    del usd[3]["frame"]  # SEC moves `frame` to the latest filing of a period
    usd.append(dict(usd[3], accn="0001193125-12-444068", fy=2012, filed="2012-10-31",
                    frame="CY2009"))  # fmt: skip
    again = _run(settings, clock, ciks=(APPLE,), when=utc(2026, 10, 9, 14, 0))
    assert again.status == "ok" and again.rows_upserted == 2
    frames = _query(
        settings,
        "SELECT accn, frame FROM xbrl_fact WHERE tag = 'SalesRevenueNet' AND frame IS NOT NULL",
    )
    assert frames == [("0001193125-12-444068", "CY2009")]
    assert len(_facts(settings)) == 14


def test_a_fact_dropped_from_a_later_document_is_kept(settings, fake, clock):
    _run(settings, clock)
    del fake.apple["facts"]["us-gaap"]["Assets"]
    assert _run(settings, clock, ciks=(APPLE,), when=utc(2026, 10, 9, 14, 0)).rows_upserted == 0
    assert _query(settings, "SELECT count(*) FROM xbrl_fact WHERE tag = 'Assets'") == [(2,)]


def test_a_404_fails_that_key_only(settings, fake, clock):
    fake.status["CIK0000019617.json"] = 404
    result = _run(settings, clock)
    assert result.status == "failed" and "404" in result.error
    assert _query(settings, "SELECT DISTINCT cik FROM xbrl_fact") == [(APPLE,)]
    assert _query(settings, "SELECT key FROM watermark") == [("0000320193",)]


def test_without_sec_user_agent_the_job_fails_and_sends_nothing(settings, fake, clock):
    result = _run(replace(settings, sec_user_agent=None), clock)
    assert result.status == "failed" and "SEC_USER_AGENT" in result.error
    assert fake.requests == []


def test_reparse_rebuilds_the_same_table_without_a_request(settings, fake, clock):
    _run(settings, clock)
    usd = fake.apple["facts"]["us-gaap"]["SalesRevenueNet"]["units"]["USD"]
    del usd[3]["frame"]
    _run(settings, clock, ciks=(APPLE,), when=utc(2026, 10, 9, 14, 0))
    facts = _facts(settings)
    sent = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM xbrl_fact")
        result = jobs.reparse(con, companyfacts.JOB, settings=settings)
    assert len(result.raw_ids) == 3
    assert _facts(settings) == facts
    assert len(fake.requests) == sent


def test_the_registered_job_drains_nothing_until_pending_doc_exists(settings, fake, clock):
    result = _run(settings, clock, job=companyfacts.JOB)
    assert result.status == "ok" and result.rows_upserted == 0
    assert fake.requests == []
    assert "usfin.sources.edgar.companyfacts" in jobs.SOURCE_MODULES


def test_the_registered_job_takes_its_ciks_from_pending_companyfacts_docs(settings, fake, clock):
    with store.session(settings.db_path, wait=0) as con:  # pending_doc (migration 010, S22)
        con.execute(
            "INSERT INTO pending_doc VALUES"
            " ('a1', 'companyfacts', 320193, now(), NULL),"
            " ('a2', 'companyfacts', 320193, now(), NULL),"
            " ('a3', 'submissions', 19617, now(), NULL),"
            " ('a4', 'companyfacts', 19617, now(), now())"
        )
    result = _run(settings, clock, job=companyfacts.JOB)
    assert result.status == "ok" and result.rows_upserted == 9
    assert fake.requests == ["CIK0000320193.json"]


# --- usfin edgar facts --------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    monkeypatch.setenv("SEC_USER_AGENT", UA)


def test_cli_edgar_facts_runs_the_job_for_the_given_cik(settings, fake, cli_env):
    out = CliRunner().invoke(app, ["edgar", "facts", "--cik", "320193"])
    assert out.exit_code == 0, out.output
    assert "edgar_companyfacts #1 ok rows 9" in out.output
    assert "CIK 0000320193  facts 9  tags 4" in out.output
    assert "filed 2009-10-27..2026-07-31" in out.output and "requests 1" in out.output
    assert _query(settings, "SELECT job, trigger FROM job_run") == [
        ("edgar_companyfacts", "manual")
    ]


def test_cli_edgar_facts_rejects_a_cik_that_is_not_a_number(settings, fake, cli_env):
    out = CliRunner().invoke(app, ["edgar", "facts", "--cik", "AAPL"])
    assert out.exit_code == 2
    assert fake.requests == []
