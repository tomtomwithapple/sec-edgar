"""Schedule 13D/G (job `edgar_sched13`, tables `ownership_filing`, `ownership_stake`; C2, C13).

Fixtures under `tests/fixtures/edgar/sched13/<accession>/` are real filing-folder `index.json`
listings and `primary_doc.xml` documents fetched 2026-10-08 through the client (`usfin http get`
and a probe on `http.Client`, raw 888–917, dataset `probe_sched13`), byte for byte, untrimmed:

- `0001104659-26-114447` — SCHEDULE 13G (OnKure Therapeutics) by five joint reporting persons.
  The 13G cover pages carry **no CIK per person** (only `reportingPersonName`), so every
  `filer_cik` is NULL; the folder also holds an `.htm` exhibit.
- `0001104659-26-114474` — SCHEDULE 13D (Calamos Aksia fund), one person with
  `reportingPersonCIK`.
- `0001193125-26-416960` — SCHEDULE 13D/A (amendment 7, Mid Penn Bancorp) by three persons, two of
  them `reportingPersonNoCIK` Y. Only the daily index saw it, so `pending_doc` queued it under the
  reporting person 1548007; the subject (879635) comes from the XML.
- `0001905106-26-000171` — SCHEDULE 13G/A (amendment 11) by one person, no `memberGroup`.
- `0000019617-24-000609` — a legacy `SC 13G` (JPMorgan, 2024-10-30, before structured XML was
  required on 2024-12-18): its folder holds only the HTML document, so it is skipped and counted.
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

from usfin import http, jobs, ratelimit, raw, store
from usfin.settings import ConfigError, load_settings
from usfin.sources.edgar import dailyindex, feed, pending, sched13

FIXTURES = Path(__file__).parent / "fixtures" / "edgar" / "sched13"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"

G = "0001104659-26-114447"  # 13G, five persons without CIK
D = "0001104659-26-114474"  # 13D, one person with CIK
D_A = "0001193125-26-416960"  # 13D/A, two persons without CIK; queued under a person
G_A = "0001905106-26-000171"  # 13G/A, one person
LEGACY = "0000019617-24-000609"  # SC 13G, HTML only
FILINGS = [  # (accession, queued cik, form, filed, enqueued minute) — queue order G, D, D_A, G_A, LEGACY
    (G, 1637715, "SCHEDULE 13G", date(2026, 10, 7), 0),
    (D, 1937073, "SCHEDULE 13D", date(2026, 10, 7), 1),
    (D_A, 1548007, "SCHEDULE 13D/A", date(2026, 10, 7), 2),
    (G_A, 1867443, "SCHEDULE 13G/A", date(2026, 10, 7), 3),
    (LEGACY, 19617, "SC 13G", date(2024, 10, 30), 4),
]


def _doc(acc: str) -> bytes:
    return (FIXTURES / acc / "primary_doc.xml").read_bytes()


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(datetime(2026, 10, 8, 14, 30, tzinfo=UTC))  # 10:30 ET on 10-08
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
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        sec_user_agent=UA,
    )


class Fake:
    """Serves `Archives/edgar/data/<any cik>/<nodash>/<name>` from the fixture folders."""

    def __init__(self):
        self.requests: list[str] = []
        self.overrides: dict[str, httpx.Response] = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append(path)
        _, _cik, nodash, name = path.removeprefix("/Archives/edgar/").split("/")
        acc = f"{nodash[:10]}-{nodash[10:12]}-{nodash[12:]}"
        if path in self.overrides:
            return self.overrides[path]
        f = FIXTURES / acc / name
        if not f.is_file():
            return httpx.Response(404, text="<html>Not Found</html>")
        kind = "application/json" if name.endswith(".json") else "text/xml"
        return httpx.Response(200, content=f.read_bytes(), headers={"content-type": kind})


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"^https://www\.sec\.gov/Archives/edgar/data/").mock(side_effect=f)
        yield f


def _registry():
    reg = jobs.Registry()
    reg.register(sched13.JOB)
    return reg


def _run(settings, clock):
    return jobs.run_job(
        "edgar_sched13", settings=settings, registry=_registry(), clock=clock, chain=False
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _queue(settings, clock, rows=FILINGS):
    """What the feed / daily index leave behind: an `edgar_filing` row and a queued document."""
    base = clock.naive()
    with store.session(settings.db_path, wait=0) as con:
        for acc, cik, form, filed, minute in rows:
            con.execute(
                "INSERT INTO edgar_filing VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, 'index')"
                " ON CONFLICT DO NOTHING",
                [acc, cik, form, filed, form.endswith("/A")],
            )
            con.execute(
                "INSERT INTO pending_doc VALUES (?, 'sched13', ?, ?, NULL)",
                [acc, cik, base.replace(minute=minute)],
            )


def _tables(settings):
    return tuple(
        sorted(_query(settings, f"SELECT * FROM {t}"), key=repr)
        for t in ("ownership_filing", "ownership_stake")
    )


def _open(settings):
    return sorted(
        r[0]
        for r in _query(
            settings,
            "SELECT accession_no FROM pending_doc WHERE kind = 'sched13' AND done_at IS NULL",
        )
    )


# --- parsing ------------------------------------------------------------------------------------


def test_a_13g_names_its_persons_without_cik():
    doc = sched13.parse_document(_doc(G))
    assert (doc.form, doc.subject_cik, doc.subject_name, doc.subject_cusips) == (
        "SCHEDULE 13G", 1637715, "OnKure Therapeutics, Inc.", "68277Q105",
    )  # fmt: skip
    assert (doc.class_title, doc.event_date, doc.amendment_no, doc.filer_cik) == (
        "Class A Common Stock, par value $0.0001 per share", date(2026, 10, 1), None, 2068530,
    )  # fmt: skip
    assert [(p.seq, p.cik, p.name) for p in doc.persons] == [
        (1, None, "Coastlands Capital LP"),
        (2, None, "Coastlands Capital Partners LP"),
        (3, None, "Coastlands Capital GP LLC"),
        (4, None, "Coastlands Capital LLC"),
        (5, None, "Matthew D. Perry"),
    ]
    assert doc.persons[0] == sched13.Person(
        seq=1, cik=None, name="Coastlands Capital LP", member_of_group="b", citizenship="DE",
        sole_voting=0.0, shared_voting=2_700_000.0, sole_dispositive=0.0,
        shared_dispositive=2_700_000.0, shares=2_700_000.0, percent_of_class=6.67,
        person_type="IA,PN",
    )  # fmt: skip
    assert doc.persons[4].citizenship == "X1" and doc.persons[4].person_type == "HC,IN"


def test_a_13d_person_carries_a_cik():
    doc = sched13.parse_document(_doc(D))
    assert (doc.form, doc.subject_cik, doc.event_date, doc.subject_cusips) == (
        "SCHEDULE 13D", 1937073, date(2026, 10, 1), "12811A309",
    )  # fmt: skip
    assert doc.subject_name == "Calamos Aksia Alternative Credit & Income Fund"
    assert doc.persons == [
        sched13.Person(
            seq=1, cik=1323271, name="James H. Vos", member_of_group="b", citizenship="X1",
            sole_voting=263_653.39, shared_voting=9_527_838.79, sole_dispositive=263_653.39,
            shared_dispositive=9_527_838.79, shares=9_791_492.19, percent_of_class=7.7,
            person_type="IN",
        )
    ]  # fmt: skip


def test_a_13d_amendment_keeps_persons_without_cik_in_order():
    doc = sched13.parse_document(_doc(D_A))
    assert (doc.form, doc.amendment_no, doc.subject_cik, doc.event_date, doc.filer_cik) == (
        "SCHEDULE 13D/A", 7, 879635, date(2026, 10, 5), 1548007,
    )  # fmt: skip
    assert [(p.seq, p.cik, p.name, p.percent_of_class, p.shares) for p in doc.persons] == [
        (1, None, "GAH Capital, LLC", 9.62, 2_436_379.0),
        (2, None, "GAH Capital Trust", 9.62, 2_436_379.0),
        (3, 1548007, "Philip A. Norcross, individually and in his capacity as Manager and Trustee",
         9.68, 2_451_458.0),
    ]  # fmt: skip


def test_a_13g_amendment_without_a_group_flag():
    doc = sched13.parse_document(_doc(G_A))
    assert (doc.form, doc.amendment_no, doc.subject_cik, doc.event_date) == (
        "SCHEDULE 13G/A", 11, 1867443, date(2026, 9, 30),
    )  # fmt: skip
    (person,) = doc.persons
    assert (person.cik, person.name, person.member_of_group, person.shares) == (
        None, "Meteora Capital, LLC", None, 0.0,
    )  # fmt: skip
    assert person.percent_of_class == 0.0


def test_a_document_that_is_not_a_schedule_13dg_is_refused():
    body = _doc(D).replace(
        b"<submissionType>SCHEDULE 13D<", b"<submissionType>SCHEDULE 13E<"
    )
    with pytest.raises(sched13.Sched13Error, match="submissionType"):
        sched13.parse_document(body)
    with pytest.raises(sched13.Sched13Error):
        sched13.parse_document(b"<html>Not Found</html>")


def test_the_document_is_the_folder_s_primary_doc_xml():
    assert sched13.pick_document((FIXTURES / G / "index.json").read_bytes()) == "primary_doc.xml"
    assert sched13.pick_document((FIXTURES / LEGACY / "index.json").read_bytes()) is None


def test_legacy_forms_are_the_pre_xml_names():
    assert sched13.is_legacy("SC 13G") and sched13.is_legacy("SC 13D/A")
    assert not sched13.is_legacy("SCHEDULE 13G") and not sched13.is_legacy("SCHEDULE 13D/A")


# --- config ------------------------------------------------------------------------------------


def test_the_per_run_cap_comes_from_edgar_yaml(tmp_path):
    assert pending.max_docs(REPO_CONFIG, "sched13") == 300
    (tmp_path / "edgar.yaml").write_text("form4: {max_docs_per_run: 7}\n", "utf-8")
    assert pending.max_docs(tmp_path, "sched13") == 300
    assert pending.max_docs(tmp_path, "form4") == 7
    (tmp_path / "edgar.yaml").write_text("sched13: {max_docs_per_run: 50}\n", "utf-8")
    assert pending.max_docs(tmp_path, "sched13") == 50
    for bad in ("-1", "'x'", "true", "1.5"):
        (tmp_path / "edgar.yaml").write_text(f"sched13: {{max_docs_per_run: {bad}}}\n", "utf-8")
        with pytest.raises(ConfigError):
            pending.max_docs(tmp_path, "sched13")


def test_feed_and_reconcile_chain_sched13():
    assert "edgar_sched13" in feed.JOB.then
    assert "edgar_sched13" in dailyindex.JOB.then
    assert "edgar_sched13" in jobs.load_registry()


# --- job ---------------------------------------------------------------------------------------


def test_the_job_drains_the_queue_into_ownership_tables(settings, fake, clock):
    _run(settings, clock)  # creates the tables; nothing queued, no request
    assert fake.requests == []
    _queue(settings, clock)
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert fake.requests[:2] == [
        "/Archives/edgar/data/1637715/000110465926114447/index.json",
        "/Archives/edgar/data/1637715/000110465926114447/primary_doc.xml",
    ]
    # four structured filings × 2 requests; the legacy one only lists its folder
    assert len(fake.requests) == 9
    assert fake.requests[-1] == "/Archives/edgar/data/19617/000001961724000609/index.json"
    filings = {
        r[0]: r[1:]
        for r in _query(
            settings,
            "SELECT accession_no, form, legacy, subject_cik, amendment_no, filed_at, document,"
            " persons FROM ownership_filing",
        )
    }
    assert filings == {
        G: ("SCHEDULE 13G", False, 1637715, None, date(2026, 10, 7), "primary_doc.xml", 5),
        D: ("SCHEDULE 13D", False, 1937073, None, date(2026, 10, 7), "primary_doc.xml", 1),
        D_A: ("SCHEDULE 13D/A", False, 879635, 7, date(2026, 10, 7), "primary_doc.xml", 3),
        G_A: ("SCHEDULE 13G/A", False, 1867443, 11, date(2026, 10, 7), "primary_doc.xml", 1),
        LEGACY: ("SC 13G", True, None, None, date(2024, 10, 30), None, 0),
    }
    counts = dict(
        _query(settings, "SELECT accession_no, count(*) FROM ownership_stake GROUP BY ALL")
    )
    assert counts == {G: 5, D: 1, D_A: 3, G_A: 1}
    assert result.rows_upserted == 10
    stake = _query(
        settings,
        "SELECT person_seq, filer_cik, filer_name, subject_cik, form, event_date,"
        " percent_of_class, shares, filed_at FROM ownership_stake WHERE accession_no = ?"
        " ORDER BY person_seq",
        [D_A],
    )
    assert stake[0] == (
        1, None, "GAH Capital, LLC", 879635, "SCHEDULE 13D/A", date(2026, 10, 5), 9.62,
        2_436_379.0, date(2026, 10, 7),
    )  # fmt: skip
    assert stake[2][:2] == (3, 1548007)
    assert _query(settings, "SELECT count(*) FROM ownership_stake WHERE filer_cik IS NULL") == [
        (5 + 2 + 1,)
    ]
    assert _query(settings, "SELECT count(*) FROM ownership_filing WHERE legacy") == [(1,)]
    assert _open(settings) == []


def test_a_second_run_and_a_reparse_change_nothing(settings, fake, clock):
    _run(settings, clock)
    _queue(settings, clock)
    _run(settings, clock)
    before = _tables(settings)
    fake.requests.clear()
    again = _run(settings, clock)
    assert (again.status, again.rows_upserted, fake.requests) == ("ok", 0, [])
    with store.session(settings.db_path, wait=0) as con:  # refetch every document
        con.execute("UPDATE pending_doc SET done_at = NULL")
    refetch = _run(settings, clock)
    assert (refetch.status, refetch.rows_upserted, len(fake.requests)) == ("ok", 0, 9)
    assert _tables(settings) == before
    with store.session(settings.db_path, wait=0) as con:
        result = jobs.reparse(con, sched13.JOB, settings=settings)
    assert result.rows == 0 and len(result.raw_ids) == 18
    assert _tables(settings) == before
    with store.session(settings.db_path, wait=0) as con:
        for t in ("ownership_filing", "ownership_stake"):
            con.execute(f"DELETE FROM {t}")
        rebuilt = jobs.reparse(con, sched13.JOB, settings=settings)
    assert rebuilt.rows == 10 and _tables(settings) == before


def test_a_changed_document_replaces_the_accession_s_rows(settings, fake, clock):
    _run(settings, clock)
    _queue(settings, clock, [FILINGS[2]])
    _run(settings, clock)
    changed = _doc(D_A).replace(b"<percentOfClass>9.68<", b"<percentOfClass>9.70<")
    changed = changed.replace(b"<reportingPersonInfo>", b"<!--", 1).replace(
        b"</reportingPersonInfo>", b"-->", 1
    )
    fake.overrides["/Archives/edgar/data/1548007/000119312526416960/primary_doc.xml"] = (
        httpx.Response(200, content=changed)
    )
    with store.session(settings.db_path, wait=0) as con:
        con.execute("UPDATE pending_doc SET done_at = NULL")
    result = _run(settings, clock)
    assert result.rows_upserted == 2  # both remaining persons renumbered; one changed percent
    assert _query(
        settings, "SELECT person_seq, filer_cik, percent_of_class FROM ownership_stake ORDER BY 1"
    ) == [(1, None, 9.62), (2, 1548007, 9.70)]
    assert _query(settings, "SELECT persons FROM ownership_filing") == [(2,)]


def test_the_cap_drains_the_oldest_documents_first(settings, fake, clock):
    (settings.config_dir / "edgar.yaml").write_text(
        "watchlist: [320193]\nsched13:\n  max_docs_per_run: 2\n", "utf-8"
    )
    _run(settings, clock)
    _queue(settings, clock)
    result = _run(settings, clock)
    assert result.status == "ok" and len(fake.requests) == 4
    assert _open(settings) == sorted([D_A, G_A, LEGACY])
    _run(settings, clock)
    _run(settings, clock)
    assert _open(settings) == []


def test_a_structured_filing_without_primary_doc_fails_that_document_only(settings, fake, clock):
    fake.overrides["/Archives/edgar/data/1937073/000110465926114474/index.json"] = (
        httpx.Response(200, json={"directory": {"item": [{"name": f"{D}.txt"}], "name": "/x"}})
    )
    _run(settings, clock)
    _queue(settings, clock)
    result = _run(settings, clock)
    assert result.status == "failed" and list(result.key_errors) == [D]
    assert "primary_doc.xml" in result.key_errors[D]
    assert _open(settings) == [D]
    assert len(_query(settings, "SELECT * FROM ownership_filing")) == 4


def test_a_form4_folder_listing_is_not_a_legacy_skip(settings, fake, clock):
    """`edgar_filing_index` is shared with `edgar_form4`: replaying a Form 4 folder listing in
    `usfin reparse edgar_sched13` must store nothing."""
    _run(settings, clock)
    with store.session(settings.db_path, wait=0) as con:
        con.execute(
            "INSERT INTO edgar_filing VALUES ('0000909832-26-000090', 909832, '4',"
            " DATE '2026-10-07', NULL, NULL, NULL, false, 'index')"
        )
        raw_id = raw.save(
            con, raw_dir=settings.raw_dir, source="edgar", dataset=sched13.INDEX_DATASET,
            url="https://www.sec.gov/Archives/edgar/data/1928633/000090983226000090/index.json",
            http_status=200, body=json.dumps({"directory": {"item": []}}).encode(),
        )  # fmt: skip
        assert raw_id
        result = jobs.reparse(con, sched13.JOB, settings=settings)
    assert result.rows == 0
    assert _query(settings, "SELECT count(*) FROM ownership_filing") == [(0,)]
