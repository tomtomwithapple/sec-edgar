"""EDGAR submissions (job `edgar_submissions`, tables `edgar_filing`, `edgar_filing_party`,
`edgar_submissions_page`; C1, C2, C14).

`tests/fixtures/edgar/submissions/` holds two real responses fetched 2026-10-08 through the
client, trimmed by dropping array positions only (every column keeps the same positions; nothing
else edited): `CIK0000320193.json` (Apple; 163 743 bytes, `filings.recent` cut from 1000 filings
to 7 — a Form 4, an 8-K/A, two earnings 8-Ks, a SCHEDULE 13G/A, the FY2025 10-K and a 144/A —
with `files[]` verbatim: one page, `CIK0000320193-submissions-001.json`, 1 264 filings
1994-01-26..2015-09-15) and that page (204 857 bytes, cut to 4 filings: a 424B2, the FY2014
10-K, a 4/A and the 1994 10-Q with no primary document). Later documents in these tests are those
edited. No request leaves the process.

`acceptanceDateTime` is not UTC despite its `Z`: the two earnings 8-Ks' filing headers
(`-index-headers.html`, fetched the same day) say ACCEPTANCE-DATETIME 20260730163028 and
20260129163033 (New York), while the JSON says 2026-07-31T00:30:28Z and 2026-01-30T02:30:33Z.
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
from usfin.sources.edgar import filings, submissions

FIXTURES = Path(__file__).parent / "fixtures" / "edgar" / "submissions"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"
APPLE = 320193
MAIN = "https://data.sec.gov/submissions/CIK0000320193.json"
PAGE = "https://data.sec.gov/submissions/CIK0000320193-submissions-001.json"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def naive(*args) -> datetime:
    """Naive UTC, as `edgar_filing.accepted_at` stores it."""
    return utc(*args).replace(tzinfo=None)


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
    """Answers data.sec.gov/submissions/<name> from `docs` (404 for an unknown name)."""

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
    def main(self) -> dict:
        return self.docs["CIK0000320193.json"]


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"^https://data\.sec\.gov/submissions/").mock(side_effect=f)
        yield f


def _registry(job=None):
    reg = jobs.Registry()
    reg.register(job or submissions.for_ciks([APPLE]))
    return reg


def _run(settings, clock, when=None, job=None):
    if when is not None:
        clock.t = when
    return jobs.run_job(
        "edgar_submissions", settings=settings, registry=_registry(job), clock=clock, chain=False
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _filings(settings):
    rows = _query(
        settings,
        "SELECT accession_no, cik, form, filed_at, accepted_at, report_date, primary_doc,"
        " is_amendment, source FROM edgar_filing",
    )
    return {r[0]: r[1:] for r in rows}


def _tables(settings):
    return (
        _filings(settings),
        sorted(_query(settings, "SELECT * FROM edgar_filing_party")),
        sorted(_query(settings, "SELECT * FROM edgar_submissions_page")),
    )


# --- parsing ------------------------------------------------------------------------------------


def test_acceptance_time_is_new_york_wall_time_shifted_so_it_is_read_back_as_such():
    # header 2026-07-30 16:30:28 EDT = 20:30:28 UTC; JSON says 2026-07-31T00:30:28.000Z
    assert submissions.accepted_utc("2026-07-31T00:30:28.000Z") == naive(2026, 7, 30, 20, 30, 28)
    # header 2026-01-29 16:30:33 EST = 21:30:33 UTC; JSON says 2026-01-30T02:30:33.000Z
    assert submissions.accepted_utc("2026-01-30T02:30:33.000Z") == naive(2026, 1, 29, 21, 30, 33)
    assert submissions.accepted_utc("") is None


def test_columns_of_unequal_length_are_rejected():
    cols = json.loads((FIXTURES / "CIK0000320193-submissions-001.json").read_text("utf-8"))
    cols["form"] = cols["form"][:-1]
    with pytest.raises(submissions.SubmissionsError, match="form"):
        submissions.filings_from_columns(cols, cik=APPLE)


# --- job edgar_submissions ----------------------------------------------------------------------


def test_recent_and_the_older_page_become_one_filing_row_each(settings, fake, clock):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert result.rows_upserted == 11
    assert fake.requests == ["CIK0000320193.json", "CIK0000320193-submissions-001.json"]
    rows = _filings(settings)
    assert len(rows) == 11
    assert rows["0000320193-25-000079"] == (
        APPLE,
        "10-K",
        date(2025, 10, 31),
        naive(2025, 10, 31, 10, 1, 26),  # 06:01:26 EDT
        date(2025, 9, 27),
        "aapl-20250927.htm",
        False,
        "submissions",
    )
    assert rows["0000320193-26-000018"][3] == naive(2026, 7, 30, 20, 30, 28)
    sched = rows["0000102909-26-000630"]  # no report date
    assert (sched[1], sched[4], sched[6]) == ("SCHEDULE 13G/A", None, True)
    old = rows["0000320193-94-000002"]  # from the page; no primary document
    assert (old[1], old[2], old[4], old[5]) == ("10-Q", date(1994, 1, 26), date(1993, 12, 31), None)
    assert {a for a, r in rows.items() if r[6]} == {
        "0001140361-26-035325",  # 8-K/A
        "0000102909-26-000630",  # SCHEDULE 13G/A
        "0001959173-25-006313",  # 144/A
        "0001181431-13-036353",  # 4/A
    }
    assert _query(settings, "SELECT DISTINCT source, dataset FROM raw_response") == [
        ("edgar", "edgar_submissions")
    ]


def test_every_filing_lists_the_company_as_a_party_with_role_submissions(settings, fake, clock):
    _run(settings, clock)
    parties = _query(settings, "SELECT accession_no, cik, role FROM edgar_filing_party")
    assert sorted(parties) == sorted((a, APPLE, "submissions") for a in _filings(settings))


def test_the_watermark_is_the_newest_filing_date_per_cik(settings, fake, clock):
    _run(settings, clock)
    assert _query(settings, "SELECT key, value FROM watermark WHERE job = 'edgar_submissions'") == [
        ("0000320193", "2026-10-05")
    ]


def test_a_second_run_skips_the_unchanged_page_and_changes_nothing(settings, fake, clock):
    _run(settings, clock)
    before = _tables(settings)
    again = _run(settings, clock, when=utc(2026, 10, 9, 14, 0))
    assert again.status == "ok" and again.rows_upserted == 0
    assert fake.requests[2:] == ["CIK0000320193.json"]
    assert _tables(settings) == before


def test_a_page_whose_listing_changed_is_fetched_again(settings, fake, clock):
    _run(settings, clock)
    fake.main["filings"]["files"][0]["filingCount"] = 1265
    assert _run(settings, clock, when=utc(2026, 10, 9, 14, 0)).status == "ok"
    assert fake.requests[2:] == ["CIK0000320193.json", "CIK0000320193-submissions-001.json"]
    assert _run(settings, clock, when=utc(2026, 10, 10, 14, 0)).status == "ok"
    assert fake.requests[4:] == ["CIK0000320193.json"]


def test_a_renumbered_page_with_the_same_listing_is_not_fetched_again(settings, fake, clock):
    # heavy filers' pages are numbered newest first, so a new page shifts every name
    _run(settings, clock)
    entry = fake.main["filings"]["files"][0]
    newer = {"name": entry["name"], "filingCount": 1, "filingFrom": "2015-09-16",
             "filingTo": "2015-09-16"}  # fmt: skip
    entry["name"] = "CIK0000320193-submissions-002.json"
    fake.main["filings"]["files"].insert(0, newer)
    fake.docs[entry["name"]] = fake.docs.pop(newer["name"])
    fake.docs[newer["name"]] = {
        k: v[:1] for k, v in fake.main["filings"]["recent"].items()
    }  # a real page's shape: the columns at top level
    assert _run(settings, clock, when=utc(2026, 10, 9, 14, 0)).status == "ok"
    assert fake.requests[2:] == ["CIK0000320193.json", "CIK0000320193-submissions-001.json"]
    pages = _query(
        settings,
        "SELECT name, filing_count, raw_id IS NOT NULL FROM edgar_submissions_page ORDER BY name",
    )
    assert pages == [
        ("CIK0000320193-submissions-001.json", 1, True),
        ("CIK0000320193-submissions-002.json", 1264, True),
    ]


def test_a_later_source_fills_null_columns_and_never_blanks_them(settings, fake, clock):
    with store.session(settings.db_path, wait=0) as con:
        filings.upsert_filings(
            con,
            [
                filings.Filing(
                    accession_no="0000320193-25-000079",
                    cik=APPLE,
                    form="10-K",
                    filed_at=date(2025, 10, 31),
                    accepted_at=naive(2025, 10, 31, 10, 1, 26),
                    report_date=None,
                    primary_doc=None,
                    is_amendment=False,
                    source="feed",
                )
            ],
        )
    _run(settings, clock)
    row = _filings(settings)["0000320193-25-000079"]
    assert row[-1] == "feed"  # the first source stays
    assert (row[4], row[5]) == (date(2025, 9, 27), "aapl-20250927.htm")  # filled
    with store.session(settings.db_path, wait=0) as con:
        blank = filings.Filing(
            "0000320193-25-000079", APPLE, "10-K", date(2025, 10, 31), None, None, None, False,
            "index",
        )  # fmt: skip
        assert filings.upsert_filings(con, [blank]) == 0
    assert _filings(settings)["0000320193-25-000079"] == row


def test_a_404_fails_the_key_and_stores_no_filing(settings, fake, clock):
    fake.status["CIK0000320193.json"] = 404
    result = _run(settings, clock)
    assert result.status == "failed" and "404" in result.error
    assert _filings(settings) == {}
    assert _query(settings, "SELECT count(*) FROM watermark") == [(0,)]


def test_without_sec_user_agent_the_job_fails_and_sends_nothing(settings, fake, clock):
    result = _run(replace(settings, sec_user_agent=None), clock)
    assert result.status == "failed" and "SEC_USER_AGENT" in result.error
    assert fake.requests == []


def test_reparse_rebuilds_the_same_tables_without_a_request(settings, fake, clock):
    _run(settings, clock)
    fake.main["filings"]["files"][0]["filingCount"] = 1265
    _run(settings, clock, when=utc(2026, 10, 9, 14, 0))
    tables = _tables(settings)
    sent = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        for t in ("edgar_filing", "edgar_filing_party", "edgar_submissions_page"):
            con.execute(f"DELETE FROM {t}")
        result = jobs.reparse(con, submissions.JOB, settings=settings)
    assert len(result.raw_ids) == 4
    assert _tables(settings) == tables
    assert len(fake.requests) == sent


def test_the_registered_job_drains_nothing_until_pending_doc_exists(settings, fake, clock):
    result = _run(settings, clock, job=submissions.JOB)
    assert result.status == "ok" and result.rows_upserted == 0
    assert fake.requests == []
    assert "usfin.sources.edgar.submissions" in jobs.SOURCE_MODULES


def test_the_registered_job_takes_its_ciks_from_pending_submissions_docs(settings, fake, clock):
    with store.session(settings.db_path, wait=0) as con:  # pending_doc (migration 010, S22)
        con.execute(
            "INSERT INTO pending_doc VALUES"
            " ('a1', 'submissions', 320193, now(), NULL),"
            " ('a2', 'submissions', 320193, now(), NULL),"
            " ('a3', 'companyfacts', 789019, now(), NULL),"
            " ('a4', 'submissions', 19617, now(), now())"
        )
    result = _run(settings, clock, job=submissions.JOB)
    assert result.status == "ok" and result.rows_upserted == 11
    assert fake.requests[0] == "CIK0000320193.json"


# --- usfin edgar submissions --------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    monkeypatch.setenv("SEC_USER_AGENT", UA)


def test_cli_edgar_submissions_runs_the_job_for_the_given_cik(settings, fake, cli_env):
    out = CliRunner().invoke(app, ["edgar", "submissions", "--cik", "320193"])
    assert out.exit_code == 0, out.output
    assert "edgar_submissions #1 ok rows 11" in out.output
    assert "0000320193" in out.output and "filings 11" in out.output
    assert "pages 1" in out.output and "requests 2" in out.output
    assert _query(settings, "SELECT job, trigger FROM job_run") == [
        ("edgar_submissions", "manual")
    ]


def test_cli_edgar_submissions_rejects_a_cik_that_is_not_a_number(settings, fake, cli_env):
    out = CliRunner().invoke(app, ["edgar", "submissions", "--cik", "AAPL"])
    assert out.exit_code == 2
    assert fake.requests == []


def test_fixture_documents_are_the_columnar_shape_the_parser_reads():
    main = json.loads((FIXTURES / "CIK0000320193.json").read_text("utf-8"))
    page = json.loads((FIXTURES / "CIK0000320193-submissions-001.json").read_text("utf-8"))
    assert list(main["filings"]["recent"]) == list(page)
