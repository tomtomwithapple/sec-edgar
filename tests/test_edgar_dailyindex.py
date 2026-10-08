"""EDGAR daily-index reconcile (job `edgar_reconcile`, tables `edgar_filing`, `edgar_filing_party`,
`pending_doc`; C2, C3, C14).

`tests/fixtures/edgar/dailyindex/master.20261006.idx` is the real
`https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/master.20261006.idx` fetched
2026-10-07 23:14 ET through the client (`usfin http get`, 310 875 bytes, 3 501 lines, 2 370
accessions), trimmed by dropping data lines only (header and kept lines byte for byte, LF, no
newline after the last line, as served) to 13 lines of 8 accessions: a Form 4 listed under three
CIKs (reporting person, trust, issuer; not adjacent, the file sorts CIKs as text), a 4/A, a single-
line Form 4, a SCHEDULE 13G and a SCHEDULE 13D/A (filer + subject), a DRS whose Date Filed is
2026-09-29, a JPMorgan 424B2 and a 6-K (the file's first and last lines). Other days are served
the same header with no lines (a trimmed file, not an invented one). No request leaves the
process.
"""

import shutil
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from usfin import calendars, http, jobs, ratelimit, raw, scheduler, store
from usfin.settings import load_settings
from usfin.sources.edgar import dailyindex, feed, filings

FIXTURES = Path(__file__).parent / "fixtures" / "edgar"
IDX = FIXTURES / "dailyindex" / "master.20261006.idx"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"
F4 = "0001493152-26-045898"  # 4: 1198533 SCOTT JAMES R, 1908712 trust, 860413 issuer
F4A = "0001213900-26-107326"  # 4/A: 1032186 owner, 1861841 Arbe Robotics
F4_ONE = "0000919574-26-006682"  # 4: 1000177 Nordic American Tankers only
G13 = "0001076809-26-000115"  # SCHEDULE 13G: 1076809 Glazer, 2002473 Bowhead
D13A = "0001104659-26-114099"  # SCHEDULE 13D/A: 1048268 IES, 1276922 Tontine
DRS = "0001493152-26-044916"  # Date Filed 20260929
ALL = {F4, F4A, F4_ONE, G13, D13A, DRS, "0001213900-26-106907", "0001232384-26-000053"}


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
    c = Clock(utc(2026, 10, 8, 3, 30))  # 23:30 ET on Wednesday 10-07
    monkeypatch.setattr(raw, "_utcnow", c.naive)
    return c


@pytest.fixture
def settings(tmp_path, monkeypatch, clock):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    for key in ("SEC_USER_AGENT", "ALERT_WEBHOOK_URL", "USFIN_DEMO_FAIL"):
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
    monkeypatch.setattr(feed, "COUNT", 2)
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        sec_user_agent=UA,
    )


def _header_only() -> bytes:
    body = IDX.read_bytes()
    return body[: body.index(b"-" * 80) + 80] + b"\n"


class Fake:
    """Serves master.YYYYMMDD.idx: the fixture for 10-06, 404 for `missing`, else no lines."""

    def __init__(self):
        self.files = {"20261006": IDX.read_bytes()}
        self.missing = {"20261007"}  # today's index is not out at 23:30 ET
        self.requests: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        self.requests.append(request.url.path)
        day = name.removeprefix("master.").removesuffix(".idx")
        if day in self.missing:
            return httpx.Response(404, text="<html>Not Found</html>")
        body = self.files.get(day, _header_only())
        return httpx.Response(
            200, content=body, headers={"content-type": "application/octet-stream"}
        )

    def days(self) -> list[str]:
        return [p.rsplit(".", 2)[-2] for p in self.requests if "/daily-index/" in p]


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"^https://www\.sec\.gov/Archives/edgar/daily-index/").mock(
            side_effect=f
        )
        router.get(url__regex=r"^https://www\.sec\.gov/cgi-bin/browse-edgar").mock(
            side_effect=lambda r: httpx.Response(
                200, content=(FIXTURES / "feed" / "latest.xml").read_bytes()
            )
        )
        yield f


def _registry():
    reg = jobs.Registry()
    reg.register(dailyindex.JOB)
    reg.register(feed.JOB)
    return reg


def _run(settings, clock, *, name="edgar_reconcile", when=None):
    if when is not None:
        clock.t = when
    return jobs.run_job(name, settings=settings, registry=_registry(), clock=clock, chain=False)


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _filings(settings):
    rows = _query(
        settings,
        "SELECT accession_no, cik, form, filed_at, accepted_at, primary_doc, is_amendment, source"
        " FROM edgar_filing",
    )
    return {r[0]: r for r in rows}


def _tables(settings):
    return (
        _filings(settings),
        sorted(_query(settings, "SELECT * FROM edgar_filing_party")),
        sorted(_query(settings, "SELECT accession_no, kind, cik, enqueued_at FROM pending_doc")),
    )


# --- parsing ------------------------------------------------------------------------------------


def test_each_line_is_one_cik_of_one_filing():
    lines = dailyindex.parse_index(IDX.read_bytes())
    assert len(lines) == 13
    assert lines[0] == dailyindex.Line(
        cik=1000177,
        name="NORDIC AMERICAN TANKERS Ltd",
        form="4",
        filed_at=date(2026, 10, 6),
        accession_no=F4_ONE,
    )
    assert [ln.cik for ln in lines if ln.accession_no == F4] == [1198533, 1908712, 860413]
    assert next(ln for ln in lines if ln.accession_no == DRS).filed_at == date(2026, 9, 29)


def test_a_company_name_holding_a_pipe_keeps_the_other_columns():
    body = _header_only() + b"5|A|B CO|8-K|20261006|edgar/data/5/0000000005-26-000001.txt"
    (line,) = dailyindex.parse_index(body)
    assert (line.cik, line.name, line.form, line.accession_no) == (
        5, "A|B CO", "8-K", "0000000005-26-000001",
    )  # fmt: skip


def test_an_index_with_no_lines_parses_to_none():
    assert dailyindex.parse_index(_header_only()) == []


def test_a_body_that_is_not_a_daily_index_is_rejected():
    with pytest.raises(dailyindex.IndexError_):
        dailyindex.parse_index(b"<html><body>Your request was blocked</body></html>")


def test_one_filing_per_accession_with_the_first_listed_cik():
    found = {f.accession_no: f for f in dailyindex.group(dailyindex.parse_index(IDX.read_bytes()))}
    assert set(found) == ALL
    f4 = found[F4]
    assert (f4.cik, f4.form, f4.filed_at, f4.accepted_at, f4.source) == (
        1198533, "4", date(2026, 10, 6), None, "index",
    )  # fmt: skip
    assert found[F4A].is_amendment and found[D13A].is_amendment
    assert not found[G13].is_amendment


def test_the_url_is_by_year_quarter_and_day():
    assert dailyindex.day_url(date(2026, 10, 6)) == (
        "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/master.20261006.idx"
    )
    assert "/2026/QTR1/master.20260331.idx" in dailyindex.day_url(date(2026, 3, 31))


# --- window (C3) --------------------------------------------------------------------------------


CALS = calendars.load_calendars(REPO_CONFIG)


def test_the_window_is_the_last_7_days_of_federal_days_without_a_watermark():
    days = dailyindex.window(CALS, date(2026, 10, 14), None)
    # 10-07..10-14: the weekend and Columbus Day (10-12) are not federal days
    assert days == [date(2026, 10, 7), date(2026, 10, 8), date(2026, 10, 9), date(2026, 10, 13),
                    date(2026, 10, 14)]  # fmt: skip


def test_a_recent_watermark_still_refetches_7_days():
    assert dailyindex.window(CALS, date(2026, 10, 14), date(2026, 10, 13))[0] == date(2026, 10, 7)


def test_an_old_watermark_reaches_back_to_it():
    days = dailyindex.window(CALS, date(2026, 10, 14), date(2026, 9, 21))
    assert days[0] == date(2026, 9, 21) and len(days) == 17


# --- job edgar_reconcile ------------------------------------------------------------------------


def test_every_accession_once_and_every_listed_cik_a_party(settings, fake, clock):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert fake.days() == ["20260930", "20261001", "20261002", "20261005", "20261006", "20261007"]
    rows = _filings(settings)
    assert set(rows) == ALL
    assert result.rows_upserted == 8
    assert rows[DRS][3] == date(2026, 9, 29) and rows[DRS][7] == "index"
    parties = _query(settings, "SELECT accession_no, cik, role FROM edgar_filing_party")
    lines = dailyindex.parse_index(IDX.read_bytes())
    assert sorted(parties) == sorted((ln.accession_no, ln.cik, "index") for ln in lines)
    assert _query(settings, "SELECT DISTINCT source, dataset FROM raw_response") == [
        ("edgar", "edgar_master_idx")
    ]


def test_a_line_repeated_verbatim_is_one_party(settings, fake, clock):
    # live 2026-10-07: 26 (accession, CIK) pairs are listed 2-3 times, e.g. an ABS-15G whose
    # depositor is also its securitizer, a 485 filed for several series of one trust
    line = b"1142786|J.P. Morgan Acceptance Corp II|ABS-15G|20261007|"
    line += b"edgar/data/1142786/0001539497-26-002661.txt"
    fake.files["20261006"] = _header_only() + line + b"\n" + line + b"\n" + line
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert _query(settings, "SELECT accession_no, cik, role FROM edgar_filing_party") == [
        ("0001539497-26-002661", 1142786, "index")
    ]


def test_todays_index_not_yet_published_is_not_an_error(settings, fake, clock):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert _query(settings, "SELECT key, value FROM watermark WHERE job = 'edgar_reconcile'") == [
        ("master", "2026-10-06")
    ]


def test_a_missing_index_for_an_earlier_day_fails_the_key(settings, fake, clock):
    fake.missing.add("20261002")
    result = _run(settings, clock)
    assert result.status == "failed" and "2026-10-02" in result.error
    assert set(_filings(settings)) == ALL  # the days that answered are kept
    assert _query(settings, "SELECT count(*) FROM watermark") == [(0,)]


def test_a_second_run_changes_nothing(settings, fake, clock):
    _run(settings, clock)
    before = _tables(settings)
    again = _run(settings, clock, when=clock.t + timedelta(minutes=5))
    assert again.status == "ok" and again.rows_upserted == 0
    assert _tables(settings) == before


def test_form4_and_schedule_13dg_filings_are_queued(settings, fake, clock):
    _run(settings, clock)
    queued = sorted(_query(settings, "SELECT accession_no, kind, cik FROM pending_doc"))
    assert queued == sorted(
        [
            (F4, "form4", 1198533),  # no feed role: the first listed CIK (S24 reads the issuer)
            (F4A, "form4", 1032186),
            (F4_ONE, "form4", 1000177),
            (G13, "sched13", 1076809),
            (D13A, "sched13", 1048268),
        ]
    )
    enq = _query(settings, "SELECT DISTINCT enqueued_at FROM pending_doc")
    assert enq == [(clock.naive(),)]  # the index response's fetched_at


def test_a_filing_with_feed_roles_keeps_its_issuer_and_its_queue_row(settings, fake, clock):
    with store.session(settings.db_path, wait=0) as con:
        filings.upsert_filings(
            con,
            [filings.Filing(F4, 860413, "4", date(2026, 10, 6), utc(2026, 10, 6, 20, 0).replace(tzinfo=None),
                            None, None, False, "feed")],
        )  # fmt: skip
        filings.add_parties(con, [(F4, 860413, "Issuer"), (F4, 1198533, "Reporting")])
        con.execute(
            "INSERT INTO pending_doc VALUES (?, 'form4', 860413, ?, NULL)", [F4, clock.naive()]
        )
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    row = _filings(settings)[F4]
    assert (row[1], row[7]) == (860413, "feed")
    assert _query(settings, "SELECT cik FROM pending_doc WHERE accession_no = ?", [F4]) == [
        (860413,)
    ]
    roles = sorted(
        r
        for (r,) in _query(
            settings, "SELECT role FROM edgar_filing_party WHERE accession_no = ?", [F4]
        )
    )
    assert roles == ["Issuer", "Reporting", "index", "index", "index"]


def test_an_index_cik_is_replaced_by_the_feed_role_that_arrives_later(settings, fake, clock):
    feed_g13 = "0001104659-26-114475"  # in the feed fixture: Filed by 2131282, Subject 1937073
    body = _header_only() + (
        b"2131282|K-Multi CAPIX Fo1, LP|SCHEDULE 13G|20261007|"
        b"edgar/data/2131282/0001104659-26-114475.txt"
    )
    fake.files["20261006"] = body
    _run(settings, clock)
    assert _filings(settings)[feed_g13][1] == 2131282
    _run(settings, clock, name="edgar_feed")
    assert _filings(settings)[feed_g13][1] == 1937073  # Subject


def test_reparse_rebuilds_the_same_tables_without_a_request(settings, fake, clock):
    _run(settings, clock)
    before = _tables(settings)
    sent = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        for t in ("edgar_filing", "edgar_filing_party", "pending_doc"):
            con.execute(f"DELETE FROM {t}")
        result = jobs.reparse(con, dailyindex.JOB, settings=settings)
    assert len(result.raw_ids) == 6
    assert _tables(settings) == before
    assert len(fake.requests) == sent


def test_inside_the_sec_block_window_the_run_is_skipped(settings, fake, clock):
    http.SEC_BLOCK.block()
    result = _run(settings, clock)
    assert result.status == "skipped" and fake.requests == []


def test_without_sec_user_agent_the_job_fails_and_sends_nothing(settings, fake, clock):
    result = _run(replace(settings, sec_user_agent=None), clock)
    assert result.status == "failed" and "SEC_USER_AGENT" in result.error
    assert fake.requests == []


def test_the_reconcile_is_scheduled_at_23_30_on_federal_days():
    entry = scheduler.load_schedule(REPO_CONFIG)["edgar_reconcile"]
    assert entry.calendar == "federal"
    fires = scheduler.next_fires(entry, CALS, datetime(2026, 10, 9, 23, 0, tzinfo=jobs.ET), 2)
    assert [f.strftime("%m-%d %H:%M") for f in fires] == ["10-09 23:30", "10-13 23:30"]
    assert "usfin.sources.edgar.dailyindex" in jobs.SOURCE_MODULES
