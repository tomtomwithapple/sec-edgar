"""EDGAR latest-filings feed (job `edgar_feed`, tables `edgar_filing`, `edgar_filing_party`,
`pending_doc`; C2, C5, C14) and the SEC block window in the runner.

`tests/fixtures/edgar/feed/` holds four real responses of
`https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&count=…` fetched
2026-10-07 22:57–22:58 ET through the client (`usfin http get`), trimmed by dropping `<entry>`
elements only (headers and kept entries byte for byte, ISO-8859-1 as served):

- `latest.xml` (the unfiltered feed, 100 entries, all `4` / `SCHEDULE 13D|13G` (+/A) accepted
  19:47–21:59 ET) cut to 9 entries of 3 filings: a SCHEDULE 13G listed twice (Filed by, Subject),
  a Form 4 listed twice (Reporting, Issuer) and a Form 4 listed five times (Issuer + 4 Reporting).
- `type_8k.xml` (`&type=8-K&count=10`) cut to an 8-K (Corteva, Filer) and an 8-K/A.
- `type_10q.xml` (`&type=10-Q&count=10`) cut to two 10-Qs (Cintas, Levi Strauss; Filer).
- `type_sc13g_empty.xml` (`&type=SC%2013G&count=10`): no legacy `SC 13G` was in the feed that
  evening, so this is the feed with no entries ("No recent filings"); the legacy form names are
  covered by `pending_kinds` directly.

The fake serves them as consecutive pages (`start=` / `count=`), newest first. No request leaves
the process.
"""

import shutil
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import respx

from usfin import calendars, demo, http, jobs, ratelimit, raw, scheduler, store
from usfin.settings import load_settings
from usfin.sources.edgar import companyfacts, feed, filings, submissions

FIXTURES = Path(__file__).parent / "fixtures" / "edgar"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"
PAGES = ["latest.xml", "type_8k.xml", "type_10q.xml", "type_sc13g_empty.xml"]
G13 = "0001104659-26-114475"  # SCHEDULE 13G: Filed by 2131282, Subject 1937073
F4 = "0002063236-26-000015"  # 4: Reporting 2063236, Issuer 1673743
F4_MULTI = "0001231919-26-001433"  # 4: Issuer 1631574, Reporting ×4
K8 = "0001193125-26-417064"  # 8-K, Corteva (Filer)
CORTEVA = 1755672


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


G13_ACCEPTED = utc(2026, 10, 8, 1, 59, 16).replace(tzinfo=None)  # 21:59:16-04:00, naive UTC


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(utc(2026, 10, 8, 3, 0))  # 23:00 ET on 10-07
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
    monkeypatch.setattr(feed, "COUNT", 2)  # the trimmed pages hold 2..9 entries
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        sec_user_agent=UA,
    )


def _watchlist(settings, ciks):
    (settings.config_dir / "edgar.yaml").write_text(
        "watchlist:\n" + "".join(f"  - {c}\n" for c in ciks), "utf-8"
    )


class Fake:
    """Serves the feed pages by `start` (page n = start / count), and the per-CIK documents."""

    def __init__(self):
        self.pages = [(FIXTURES / "feed" / name).read_bytes() for name in PAGES]
        self.order = list(range(len(PAGES)))
        self.status: dict[str, int] = {}
        self.requests: list[str] = []

    def feed(self, request: httpx.Request) -> httpx.Response:
        q = parse_qs(urlsplit(str(request.url)).query)
        assert q["action"] == ["getcurrent"] and q["output"] == ["atom"]
        n = int(q.get("start", ["0"])[0]) // int(q["count"][0])
        self.requests.append(f"feed:{n}")
        if "feed" in self.status:
            return httpx.Response(self.status["feed"], text="<html>Forbidden</html>")
        body = self.pages[self.order[n]] if n < len(self.order) else self.pages[-1]
        return httpx.Response(200, content=body, headers={"content-type": "application/atom+xml"})

    def docs(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        name = path.rsplit("/", 1)[-1]
        kind = "facts" if "/api/xbrl/companyfacts/" in path else "subs"
        self.requests.append(f"{kind}:{name}")
        folder = FIXTURES / ("companyfacts" if kind == "facts" else "submissions")
        if not (folder / name).is_file():
            return httpx.Response(404, text="<html>nope</html>")
        return httpx.Response(200, content=(folder / name).read_bytes())

    def feed_requests(self) -> list[str]:
        return [r for r in self.requests if r.startswith("feed:")]


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"^https://www\.sec\.gov/cgi-bin/browse-edgar").mock(side_effect=f.feed)
        router.get(url__regex=r"^https://data\.sec\.gov/").mock(side_effect=f.docs)
        yield f


def _registry(*extra):
    reg = jobs.Registry()
    for job in (feed.JOB, submissions.JOB, companyfacts.JOB, *extra):
        reg.register(job)
    return reg


def _run(settings, clock, *, chain=False, name="edgar_feed", registry=None, when=None):
    if when is not None:
        clock.t = when
    return jobs.run_job(
        name, settings=settings, registry=registry or _registry(), clock=clock, chain=chain
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _filings(settings):
    return {
        r[0]: r
        for r in _query(
            settings,
            "SELECT accession_no, cik, form, filed_at, accepted_at, report_date, primary_doc,"
            " is_amendment, source FROM edgar_filing",
        )
    }


def _pending(settings):
    return sorted(
        _query(settings, "SELECT accession_no, kind, cik FROM pending_doc WHERE done_at IS NULL")
    )


# --- parsing ------------------------------------------------------------------------------------


def test_each_entry_is_one_role_of_one_filing():
    entries = feed.parse_feed((FIXTURES / "feed" / "latest.xml").read_bytes())
    assert len(entries) == 9
    first = entries[0]
    assert first == feed.Entry(
        accession_no=G13,
        cik=2131282,
        role="Filed by",
        form="SCHEDULE 13G",
        filed_at=date(2026, 10, 7),
        updated=G13_ACCEPTED,
        name="K-Multi CAPIX Fo1, LP",
    )
    assert [e.role for e in entries if e.accession_no == F4_MULTI] == [
        "Issuer", "Reporting", "Reporting", "Reporting", "Reporting",
    ]  # fmt: skip
    assert entries[1].name == "Calamos Aksia Alternative Credit & Income Fund"


def test_a_feed_with_no_entries_parses_to_none():
    assert feed.parse_feed((FIXTURES / "feed" / "type_sc13g_empty.xml").read_bytes()) == []


def test_a_body_that_is_not_an_atom_feed_is_rejected():
    with pytest.raises(feed.FeedError):
        feed.parse_feed(b"<html><body>Your request was blocked</body></html>")


def test_the_filing_cik_is_the_party_with_the_first_role_in_precedence():
    entries = feed.parse_feed((FIXTURES / "feed" / "latest.xml").read_bytes())
    by_acc = {f.accession_no: f for f in feed.group(entries)}
    assert len(by_acc) == 3
    assert by_acc[G13].cik == 1937073  # Subject before Filed by
    assert by_acc[F4].cik == 1673743  # Issuer before Reporting
    assert by_acc[F4_MULTI].cik == 1631574
    assert by_acc[G13].source == "feed" and by_acc[G13].primary_doc is None
    assert by_acc[G13].accepted_at == G13_ACCEPTED


@pytest.mark.parametrize(
    ("form", "watch", "kinds"),
    [
        ("10-K", False, {"companyfacts"}),
        ("10-Q", False, {"companyfacts"}),
        ("10-K/A", False, {"companyfacts"}),
        ("10-Q/A", False, {"companyfacts"}),
        ("8-K", False, set()),
        ("8-K", True, {"companyfacts", "submissions"}),
        ("10-Q", True, {"companyfacts", "submissions"}),
        ("4", False, {"form4"}),
        ("4/A", False, {"form4"}),
        ("SCHEDULE 13D", False, {"sched13"}),
        ("SCHEDULE 13G/A", False, {"sched13"}),
        ("SC 13D", False, {"sched13"}),
        ("SC 13G", False, {"sched13"}),
        ("SC 13D/A", False, {"sched13"}),
        ("SC 13G/A", False, {"sched13"}),
        ("424B2", False, set()),
        ("424B2", True, {"submissions"}),
        ("SC TO-T", False, set()),
    ],
)
def test_pending_kinds_per_form(form, watch, kinds):
    assert set(feed.pending_kinds(form, on_watchlist=watch)) == kinds


def test_the_repo_watchlist_seeds_apple_jpmorgan_and_microsoft():
    assert {320193, 19617, 789019} <= feed.load_watchlist(REPO_CONFIG)


def test_the_feed_is_scheduled_every_15_minutes_06_to_22_on_federal_days():
    entry = scheduler.load_schedule(REPO_CONFIG)["edgar_feed"]
    assert entry.calendar == "federal"
    fires = scheduler.next_fires(entry, calendars.load_calendars(REPO_CONFIG), datetime(2026, 10, 8, 5, 50, tzinfo=jobs.ET), 3)
    assert [f.strftime("%H:%M") for f in fires] == ["06:00", "06:15", "06:30"]
    assert "usfin.sources.edgar.feed" in jobs.SOURCE_MODULES


# --- job edgar_feed -----------------------------------------------------------------------------


def test_a_first_run_pages_to_the_end_and_stores_one_filing_per_accession(settings, fake, clock):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert fake.feed_requests() == ["feed:0", "feed:1", "feed:2", "feed:3"]  # 3rd page short
    rows = _filings(settings)
    assert len(rows) == 7
    assert rows[G13] == (
        G13, 1937073, "SCHEDULE 13G", date(2026, 10, 7), G13_ACCEPTED,
        None, None, False, "feed",
    )  # fmt: skip
    assert rows[F4][1] == 1673743 and rows[F4_MULTI][1] == 1631574
    assert next(r for r in rows.values() if r[2] == "8-K/A")[7] is True
    assert result.rows_upserted == 7
    parties = _query(settings, "SELECT accession_no, cik, role FROM edgar_filing_party")
    assert len(parties) == 13
    assert sorted(p[1:] for p in parties if p[0] == G13) == [
        (1937073, "Subject"), (2131282, "Filed by"),
    ]  # fmt: skip
    assert sorted(p[2] for p in parties if p[0] == F4_MULTI) == ["Issuer"] + ["Reporting"] * 4
    assert _query(settings, "SELECT DISTINCT source, dataset FROM raw_response") == [
        ("edgar", "edgar_feed")
    ]


def test_the_watermark_is_the_newest_acceptance_time_seen(settings, fake, clock):
    _run(settings, clock)
    assert _query(settings, "SELECT key, value FROM watermark WHERE job = 'edgar_feed'") == [
        ("getcurrent", "2026-10-08T01:59:16")
    ]


def test_paging_stops_at_the_first_entry_not_newer_than_the_watermark(settings, fake, clock):
    with store.session(settings.db_path, wait=0) as con:  # the 8-K/A's acceptance, 17:25:50 ET
        jobs.set_watermark(con, "edgar_feed", "getcurrent", "2026-10-07T21:25:50", today=date.max)
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert fake.feed_requests() == ["feed:0", "feed:1"]
    assert len(_filings(settings)) == 5  # page 2 is parsed whole; the 10-Q page is never asked


def test_paging_stops_after_10_pages(settings, fake, clock):
    fake.order = [0] * 20  # every page is a full page of new entries
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert len(fake.feed_requests()) == 10


def test_a_second_run_asks_one_page_and_changes_nothing(settings, fake, clock):
    _run(settings, clock)
    before = (_filings(settings), _query(settings, "SELECT * FROM edgar_filing_party ORDER BY ALL"))
    fake.requests.clear()
    result = _run(settings, clock, when=clock.t + timedelta(minutes=15))
    assert result.status == "ok" and result.rows_upserted == 0
    assert fake.feed_requests() == ["feed:0"]
    after = (_filings(settings), _query(settings, "SELECT * FROM edgar_filing_party ORDER BY ALL"))
    assert after == before


def test_documents_are_queued_by_kind(settings, fake, clock):
    _watchlist(settings, [CORTEVA])
    _run(settings, clock)
    k8a = next(a for a, r in _filings(settings).items() if r[2] == "8-K/A")
    queued = _pending(settings)
    assert [q for q in queued if q[1] == "form4"] == sorted(
        [(F4, "form4", 1673743), (F4_MULTI, "form4", 1631574)]
    )
    assert [q for q in queued if q[1] == "sched13"] == [(G13, "sched13", 1937073)]
    facts = {(q[0], q[2]) for q in queued if q[1] == "companyfacts"}
    tenq = {(a, r[1]) for a, r in _filings(settings).items() if r[2] == "10-Q"}
    assert len(tenq) == 2 and facts == tenq | {(K8, CORTEVA)}  # not the 8-K/A: off the watchlist
    assert (k8a, CORTEVA) not in facts
    assert [q for q in queued if q[1] == "submissions"] == [(K8, "submissions", CORTEVA)]
    enq = _query(settings, "SELECT DISTINCT enqueued_at FROM pending_doc")
    assert enq == [(clock.naive(),)]  # the feed response's fetched_at


def test_without_a_watchlist_8ks_queue_nothing(settings, fake, clock):
    _watchlist(settings, [])
    _run(settings, clock)
    kinds = {q[1] for q in _pending(settings) if q[0] == K8}
    assert kinds == set()


def test_reparse_rebuilds_the_same_tables_without_a_request(settings, fake, clock):
    _run(settings, clock)
    before = (_filings(settings), _query(settings, "SELECT * FROM edgar_filing_party ORDER BY ALL"))
    queued = _pending(settings)
    sent = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM edgar_filing")
        con.execute("DELETE FROM edgar_filing_party")
        con.execute("DELETE FROM pending_doc")
        result = jobs.reparse(con, feed.JOB, settings=settings)
    assert len(result.raw_ids) == 4
    after = (_filings(settings), _query(settings, "SELECT * FROM edgar_filing_party ORDER BY ALL"))
    assert after == before
    assert _pending(settings) == queued
    assert len(fake.requests) == sent


def test_without_sec_user_agent_the_job_fails_and_sends_nothing(settings, fake, clock):
    result = _run(replace(settings, sec_user_agent=None), clock)
    assert result.status == "failed" and "SEC_USER_AGENT" in result.error
    assert fake.requests == []


# --- edgar_filing.cik precedence across sources -------------------------------------------------


def _submissions_row(con, accession_no, cik, form="SCHEDULE 13G", accepted_at=None):
    filings.upsert_filings(
        con,
        [filings.Filing(accession_no, cik, form, date(2026, 10, 7), accepted_at, None, "x.xml",
                        False, "submissions")],
    )  # fmt: skip
    filings.add_parties(con, [(accession_no, cik, "submissions")])


def test_a_feed_role_replaces_a_cik_first_written_by_a_source_without_roles(settings, fake, clock):
    with store.session(settings.db_path, wait=0) as con:  # the filer's submissions came first
        _submissions_row(con, G13, 2131282)
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    row = _filings(settings)[G13]
    assert row[1] == 1937073  # Subject wins over the submissions company
    assert row[6] == "x.xml" and row[8] == "submissions"  # filled columns and source stay


def test_the_feeds_acceptance_time_replaces_one_a_submissions_json_wrote_first(
    settings, fake, clock
):
    # live 2026-10-07: the submissions JSON wrote a same-day 424B2 as 20:24:53Z, the true UTC
    # (the feed: 16:24:53-04:00), which the S20 conversion stored 4 h early as 16:24:53
    wrong = G13_ACCEPTED - timedelta(hours=4)
    with store.session(settings.db_path, wait=0) as con:
        _submissions_row(con, G13, 2131282, accepted_at=wrong)
    _run(settings, clock)
    assert _filings(settings)[G13][4] == G13_ACCEPTED
    with store.session(settings.db_path, wait=0) as con:  # and a later submissions row cannot
        _submissions_row(con, G13, 2131282, accepted_at=wrong)  # move it back
    assert _filings(settings)[G13][4] == G13_ACCEPTED


def test_a_role_less_source_after_the_feed_never_moves_the_cik(settings, fake, clock):
    _run(settings, clock)
    with store.session(settings.db_path, wait=0) as con:
        _submissions_row(con, G13, 2131282)
        filings.settle_cik(con, [G13])
    assert _filings(settings)[G13][1] == 1937073


def test_a_filing_with_no_feed_role_keeps_its_first_cik(settings):
    with store.session(settings.db_path, wait=0) as con:
        _submissions_row(con, "0000000000-26-000001", 5)
        filings.add_parties(con, [("0000000000-26-000001", 3, "index")])
        filings.settle_cik(con, ["0000000000-26-000001"])
    assert _filings(settings)["0000000000-26-000001"][1] == 5


def test_roles_split_across_two_pages_settle_on_the_best_one(settings, fake, clock):
    fake.pages[0] = (FIXTURES / "feed" / "latest.xml").read_bytes().replace(
        b"(0001937073) (Subject)", b"(0001937073) (Subject-moved)"
    )
    _run(settings, clock)
    assert _filings(settings)[G13][1] == 2131282  # Filed by is the best role seen
    with store.session(settings.db_path, wait=0) as con:
        filings.add_parties(con, [(G13, 1937073, "Subject")])
        filings.settle_cik(con, [G13])
    assert _filings(settings)[G13][1] == 1937073


# --- chain and the document queue --------------------------------------------------------------


APPLE_FACT_ACCN = "0000320193-26-000020"  # in tests/fixtures/edgar/companyfacts/CIK0000320193


def _queue(settings, rows):
    with store.session(settings.db_path, wait=0) as con:
        for acc, kind, cik, enqueued in rows:
            con.execute(
                "INSERT INTO pending_doc VALUES (?, ?, ?, ?, NULL)", [acc, kind, cik, enqueued]
            )


def test_the_chain_drains_submissions_and_companyfacts_once_per_cik(settings, fake, clock):
    fake.order = [3]  # an empty feed: only the queued documents below are drained
    _run(settings, clock)  # creates pending_doc
    now = clock.naive()
    _queue(
        settings,
        [
            (APPLE_FACT_ACCN, "companyfacts", 320193, now),  # in the facts: done
            ("0000320193-26-999999", "companyfacts", 320193, now),  # not yet in the facts: open
            ("0000320193-25-999999", "companyfacts", 320193, now - timedelta(days=2)),  # give up
            ("0000320193-26-000018", "submissions", 320193, now),
            ("0000320193-26-000001", "form4", 320193, now),
        ],
    )
    fake.requests.clear()
    result = _run(settings, clock, chain=True)
    assert result.status == "ok", result.error
    assert [(c.job, c.status) for c in result.chained] == [
        ("edgar_submissions", "ok"), ("edgar_companyfacts", "ok"),
    ]  # fmt: skip
    facts = [r for r in fake.requests if r.startswith("facts:")]
    assert facts == ["facts:CIK0000320193.json"]  # three queued rows, one request
    subs = [r for r in fake.requests if r.startswith("subs:")]
    assert subs[0] == "subs:CIK0000320193.json"
    open_rows = _pending(settings)
    assert ("0000320193-26-999999", "companyfacts", 320193) in open_rows
    assert (APPLE_FACT_ACCN, "companyfacts", 320193) not in open_rows
    assert ("0000320193-25-999999", "companyfacts", 320193) not in open_rows
    assert ("0000320193-26-000001", "form4", 320193) in open_rows  # S24 drains form4


def test_a_submissions_doc_is_done_once_the_company_lists_the_accession(settings, fake, clock):
    fake.order = [3]
    _run(settings, clock)
    _queue(
        settings,
        [
            ("0000320193-26-000018", "submissions", 320193, clock.naive()),  # listed by Apple
            ("0000320193-26-999999", "submissions", 320193, clock.naive()),  # not (yet) listed
        ],
    )
    _run(settings, clock, name="edgar_submissions")
    open_rows = _pending(settings)
    assert ("0000320193-26-999999", "submissions", 320193) in open_rows
    assert ("0000320193-26-000018", "submissions", 320193) not in open_rows


def test_a_failed_feed_run_chains_nothing(settings, fake, clock):
    fake.status["feed"] = 500
    result = _run(settings, clock, chain=True)
    assert result.status == "failed" and result.chained == []


# --- SEC block window (C5) ----------------------------------------------------------------------


def test_a_sec_job_inside_the_block_window_records_skipped_and_sends_nothing(
    settings, fake, clock
):
    http.SEC_BLOCK.block()
    result = _run(settings, clock, chain=True)
    assert result.status == "skipped" and "blocked" in result.error
    assert result.chained == [] and fake.requests == []
    run = _query(settings, "SELECT job, status, error FROM job_run")
    assert run[0][:2] == ("edgar_feed", "skipped") and "blocked" in run[0][2]
    assert _query(settings, "SELECT * FROM watermark") == []


def test_a_403_fails_the_run_blocks_sec_and_the_next_run_is_skipped(settings, fake, clock):
    fake.status["feed"] = 403
    first = _run(settings, clock, chain=True)
    assert first.status == "failed" and "403" in first.error and first.chained == []
    assert http.SEC_BLOCK.blocked()
    fake.status.clear()
    sent = len(fake.requests)
    second = _run(settings, clock, when=clock.t + timedelta(minutes=5))
    assert second.status == "skipped"
    assert len(fake.requests) == sent


def test_a_403_stops_the_remaining_keys_of_the_run(settings, fake, clock):
    calls = []

    def fetch(ctx, key):
        calls.append(key)
        raise http.SecBlocked("SEC answered 403", raw_id=None)

    job = jobs.Job("edgar_probe", keys=("a", "b", "c"), fetch=fetch, parsers={})
    result = _run(settings, clock, name="edgar_probe", registry=_registry(job))
    assert result.status == "failed"
    assert calls == ["a"]
    assert "b" in result.error and "not run" in result.error


def test_the_block_does_not_skip_non_sec_jobs(settings, clock):
    http.SEC_BLOCK.block()
    reg = jobs.Registry()
    reg.register(demo.JOB)
    result = jobs.run_job("_demo", settings=settings, registry=reg, clock=clock)
    assert result.status == "ok"
