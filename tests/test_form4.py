"""Form 4 (job `edgar_form4`, tables `insider_filing`, `insider_owner`, `insider_txn`,
`insider_footnote`; C2, C13).

Fixtures under `tests/fixtures/edgar/form4/<accession>/` are the real filing-folder `index.json`
and ownership XML of four Form 4s fetched 2026-10-07 23:26 ET through the client (`usfin http
get`, raw 776–786), byte for byte, untrimmed:

- `0000909832-26-000090` — non-derivative: one sale of Costco stock by an officer. Only the
  daily index saw it, so `pending_doc` queued it under the owner's CIK 1928633; the folder answers
  under that CIK and the issuer (909832) comes from the XML.
- `0001213900-26-107674` — non-derivative sale plus a `nonDerivativeHolding` held indirectly
  (`natureOfOwnership`).
- `0001193125-26-417218` — derivative: Skyworks officer, 2 non-derivative and 10 derivative
  (RSU) transactions, booleans written `true`/`false`, footnote-only exercise price and dates.
- `0000921895-26-002756` — amendment (4/A, `dateOfOriginalSubmission`) by two reporting owners,
  with non-derivative and derivative transactions, a `derivativeHolding`, footnote-only prices
  and dates. No request leaves the process.
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
from usfin.sources.edgar import dailyindex, feed, form4

FIXTURES = Path(__file__).parent / "fixtures" / "edgar" / "form4"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"

COST = "0000909832-26-000090"  # non-derivative, queued under owner 1928633
STWI = "0001213900-26-107674"  # sale + indirect holding
SWKS = "0001193125-26-417218"  # derivative
BNC_A = "0000921895-26-002756"  # 4/A, two owners
QUEUE = [  # (accession, queued cik, enqueued minute) — oldest first: COST, SWKS, BNC_A, STWI
    (COST, 1928633, 0),
    (SWKS, 4127, 1),
    (BNC_A, 1482541, 2),
    (STWI, 1999261, 3),
]


def _doc(acc: str) -> bytes:
    return next(p for p in (FIXTURES / acc).iterdir() if p.suffix == ".xml").read_bytes()


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(datetime(2026, 10, 8, 3, 30, tzinfo=UTC))  # 23:30 ET on 10-07
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
    reg.register(form4.JOB)
    return reg


def _run(settings, clock):
    return jobs.run_job(
        "edgar_form4", settings=settings, registry=_registry(), clock=clock, chain=False
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _queue(settings, clock, rows=QUEUE):
    base = clock.naive()
    with store.session(settings.db_path, wait=0) as con:
        for acc, cik, minute in rows:
            con.execute(
                "INSERT INTO pending_doc VALUES (?, 'form4', ?, ?, NULL)",
                [acc, cik, base.replace(minute=minute)],
            )


def _tables(settings):
    return tuple(
        sorted(_query(settings, f"SELECT * FROM {t}"), key=repr)
        for t in ("insider_filing", "insider_owner", "insider_txn", "insider_footnote")
    )


def _open(settings):
    return sorted(
        r[0]
        for r in _query(
            settings,
            "SELECT accession_no FROM pending_doc WHERE kind = 'form4' AND done_at IS NULL",
        )
    )


# --- parsing ------------------------------------------------------------------------------------


def test_a_non_derivative_form4_is_one_line_with_the_issuer_from_the_xml():
    doc = form4.parse_document(_doc(COST))
    assert (doc.form, doc.issuer_cik, doc.issuer_name, doc.issuer_ticker) == (
        "4", 909832, "COSTCO WHOLESALE CORP /NEW", "COST",
    )  # fmt: skip
    assert doc.period_of_report == date(2026, 9, 29) and doc.original_filed is None
    assert doc.owners == [
        form4.Owner(
            seq=1, cik=1928633, name="Frates Caton", is_director=False, is_officer=True,
            is_ten_pct_owner=False, is_other=False, officer_title="Sr. Executive Vice President",
            other_text=None,
        )
    ]  # fmt: skip
    assert doc.lines == [
        form4.Line(
            line_no=1, is_derivative=False, is_holding=False, security_title="Common Stock",
            txn_date=date(2026, 9, 29), txn_code="S", shares=800.638, price=915.93,
            acquired_disposed="D", shares_owned_after=7912.0, direct_indirect="D",
            nature_of_ownership=None, exercise_price=None, exercise_date=None,
            expiration_date=None, underlying_title=None, underlying_shares=None, footnotes={},
        )
    ]  # fmt: skip
    assert doc.footnotes == {}


def test_a_holding_is_a_line_without_a_transaction():
    doc = form4.parse_document(_doc(STWI))
    sale, holding = doc.lines
    assert (sale.txn_code, sale.shares, sale.price, sale.footnotes) == (
        "S", 1_000_000.0, 0.138, {"shares": ["F1"]},
    )  # fmt: skip
    assert holding == form4.Line(
        line_no=2, is_derivative=False, is_holding=True, security_title="Common Stock",
        txn_date=None, txn_code=None, shares=None, price=None, acquired_disposed=None,
        shares_owned_after=4_000_000.0, direct_indirect="I",
        nature_of_ownership='By Tourism and Entertainment Group LLC ("TEG LLC")',
        exercise_price=None, exercise_date=None, expiration_date=None, underlying_title=None,
        underlying_shares=None, footnotes={},
    )  # fmt: skip
    assert list(doc.footnotes) == ["F1"]


def test_derivative_lines_follow_the_non_derivative_table():
    doc = form4.parse_document(_doc(SWKS))
    assert [ln.is_derivative for ln in doc.lines] == [False] * 2 + [True] * 10
    assert [ln.line_no for ln in doc.lines] == list(range(1, 13))
    owner = doc.owners[0]  # booleans written true/false here, 1/0 elsewhere
    assert (owner.is_officer, owner.is_director, owner.officer_title) == (
        True, False, "SVP, Gen. Counsel & Secretary",
    )  # fmt: skip
    rsu = doc.lines[2]
    assert rsu == form4.Line(
        line_no=3, is_derivative=True, is_holding=False, security_title="Restricted Stock Units",
        txn_date=date(2026, 10, 5), txn_code="A", shares=1818.0, price=0.0,
        acquired_disposed="A", shares_owned_after=1818.0, direct_indirect="D",
        nature_of_ownership=None, exercise_price=None, exercise_date=None, expiration_date=None,
        underlying_title="Common Stock", underlying_shares=1818.0,
        footnotes={
            "exercise_price": ["F3"], "shares": ["F4"], "exercise_date": ["F5"],
            "expiration_date": ["F5"],
        },
    )  # fmt: skip
    assert doc.lines[-1].underlying_shares == 9219.0
    assert len(doc.footnotes) == 14


def test_an_amendment_by_two_owners_keeps_both_and_footnote_only_values_are_null():
    doc = form4.parse_document(_doc(BNC_A))
    assert (doc.form, doc.issuer_cik, doc.original_filed) == ("4/A", 1482541, date(2026, 9, 21))
    assert [(o.seq, o.cik, o.name, o.is_ten_pct_owner) for o in doc.owners] == [
        (1, 2098271, "YZILabs Management Ltd.", True),
        (2, 1918995, "Zhao Changpeng", True),
    ]
    assert [(ln.is_derivative, ln.is_holding) for ln in doc.lines] == [
        (False, False), (False, False), (True, False), (True, False), (True, True),
    ]  # fmt: skip
    first = doc.lines[0]
    assert (first.shares, first.price, first.txn_code) == (2_180_631.0, None, "X")
    assert first.footnotes == {"price": ["F1"], "direct_indirect": ["F2", "F3"]}
    prefunded = doc.lines[3]
    assert (prefunded.exercise_date, prefunded.expiration_date, prefunded.price) == (None,) * 3
    assert prefunded.footnotes["expiration_date"] == ["F6"]
    stapled = doc.lines[4]
    assert (stapled.exercise_price, stapled.expiration_date, stapled.txn_code) == (
        15.15, date(2028, 6, 28), None,
    )  # fmt: skip
    assert (stapled.shares_owned_after, stapled.underlying_shares) == (9_900_991.0, 9_900_991.0)
    assert sorted(doc.footnotes) == [f"F{i}" for i in range(1, 8)]
    assert doc.footnotes["F1"] == "The exercise price is $0.00001 per share."


def test_the_ownership_xml_is_the_folder_s_xml_document():
    assert form4.pick_document((FIXTURES / COST / "index.json").read_bytes()) == "form4.xml"
    assert (
        form4.pick_document((FIXTURES / BNC_A / "index.json").read_bytes())
        == "form4a14678002_10072026.xml"
    )
    empty = json.dumps({"directory": {"item": [{"name": "x.txt"}], "name": "/x"}}).encode()
    with pytest.raises(form4.Form4Error, match="no XML"):
        form4.pick_document(empty)


def test_a_document_that_is_not_a_form4_is_refused():
    body = _doc(COST).replace(b"<documentType>4<", b"<documentType>3<")
    with pytest.raises(form4.Form4Error, match="documentType"):
        form4.parse_document(body)
    with pytest.raises(form4.Form4Error):
        form4.parse_document(b"<html>Not Found</html>")


# --- config ------------------------------------------------------------------------------------


def test_the_per_run_cap_comes_from_edgar_yaml(tmp_path):
    assert form4.load_max_docs(REPO_CONFIG) == 300
    (tmp_path / "edgar.yaml").write_text("watchlist: [320193]\n", "utf-8")
    assert form4.load_max_docs(tmp_path) == form4.MAX_DOCS_DEFAULT == 300
    (tmp_path / "edgar.yaml").write_text("form4: {max_docs_per_run: 50}\n", "utf-8")
    assert form4.load_max_docs(tmp_path) == 50
    for bad in ("-1", "'x'", "true", "1.5"):
        (tmp_path / "edgar.yaml").write_text(f"form4: {{max_docs_per_run: {bad}}}\n", "utf-8")
        with pytest.raises(ConfigError):
            form4.load_max_docs(tmp_path)


def test_feed_and_reconcile_chain_form4():
    assert "edgar_form4" in feed.JOB.then
    assert "edgar_form4" in dailyindex.JOB.then
    assert "edgar_form4" in jobs.load_registry()


# --- job ---------------------------------------------------------------------------------------


def test_the_job_drains_the_queue_into_insider_tables(settings, fake, clock):
    _run(settings, clock)  # creates the tables; nothing queued, no request
    assert fake.requests == []
    _queue(settings, clock)
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    # the folder is read under the queued CIK — the owner, for an index-only Costco Form 4
    assert fake.requests[:2] == [
        "/Archives/edgar/data/1928633/000090983226000090/index.json",
        "/Archives/edgar/data/1928633/000090983226000090/form4.xml",
    ]
    assert len(fake.requests) == 8
    filings = {r[0]: r for r in _query(settings, "SELECT * FROM insider_filing")}
    assert filings[COST][1:4] == ("4", 909832, "COSTCO WHOLESALE CORP /NEW")
    assert filings[BNC_A][1] == "4/A"
    counts = dict(_query(settings, "SELECT accession_no, count(*) FROM insider_txn GROUP BY ALL"))
    assert counts == {COST: 1, STWI: 2, SWKS: 12, BNC_A: 5}
    assert result.rows_upserted == sum(counts.values())
    txn = _query(
        settings,
        "SELECT issuer_cik, owner_cik, owner_name, is_derivative, txn_code, shares, price,"
        " footnotes FROM insider_txn WHERE accession_no = ? AND line_no = 1",
        [BNC_A],
    )[0]
    assert txn[:7] == (1482541, 2098271, "YZILabs Management Ltd.", False, "X", 2180631.0, None)
    assert json.loads(txn[7]) == {"price": ["F1"], "direct_indirect": ["F2", "F3"]}
    owners = _query(
        settings,
        "SELECT owner_seq, owner_cik FROM insider_owner WHERE accession_no = ? ORDER BY 1",
        [BNC_A],
    )
    assert owners == [(1, 2098271), (2, 1918995)]
    assert _query(settings, "SELECT count(*) FROM insider_footnote")[0][0] == 0 + 1 + 14 + 7
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
    assert (refetch.status, refetch.rows_upserted, len(fake.requests)) == ("ok", 0, 8)
    assert _tables(settings) == before
    with store.session(settings.db_path, wait=0) as con:
        result = jobs.reparse(con, form4.JOB, settings=settings)
    assert result.rows == 0 and len(result.raw_ids) == 16
    assert _tables(settings) == before
    with store.session(settings.db_path, wait=0) as con:
        for t in ("insider_filing", "insider_owner", "insider_txn", "insider_footnote"):
            con.execute(f"DELETE FROM {t}")
        rebuilt = jobs.reparse(con, form4.JOB, settings=settings)
    assert rebuilt.rows == 20 and _tables(settings) == before


def test_a_changed_document_replaces_the_accession_s_rows(settings, fake, clock):
    _run(settings, clock)
    _queue(settings, clock, [(STWI, 1999261, 0)])
    _run(settings, clock)
    changed = _doc(STWI).replace(b"<value>0.138</value>", b"<value>0.139</value>")
    changed = changed.replace(b"<nonDerivativeHolding>", b"<!--").replace(
        b"</nonDerivativeHolding>", b"-->"
    )
    fake.overrides["/Archives/edgar/data/1999261/000121390026107674/ownership.xml"] = (
        httpx.Response(200, content=changed)
    )
    with store.session(settings.db_path, wait=0) as con:
        con.execute("UPDATE pending_doc SET done_at = NULL")
    result = _run(settings, clock)
    assert result.rows_upserted == 1  # the sale's price; the holding is gone
    assert _query(settings, "SELECT line_no, price FROM insider_txn") == [(1, 0.139)]


def test_the_cap_drains_the_oldest_documents_first(settings, fake, clock):
    (settings.config_dir / "edgar.yaml").write_text(
        "watchlist: [320193]\nform4:\n  max_docs_per_run: 2\n", "utf-8"
    )
    _run(settings, clock)
    _queue(settings, clock)
    result = _run(settings, clock)
    assert result.status == "ok" and len(fake.requests) == 4
    assert _open(settings) == sorted([BNC_A, STWI])
    _run(settings, clock)
    assert _open(settings) == []


def test_a_folder_without_xml_fails_that_document_only(settings, fake, clock):
    fake.overrides["/Archives/edgar/data/4127/000119312526417218/index.json"] = httpx.Response(
        200, json={"directory": {"item": [{"name": f"{SWKS}.txt"}], "name": "/x"}}
    )
    _run(settings, clock)
    _queue(settings, clock)
    result = _run(settings, clock)
    assert result.status == "failed" and list(result.key_errors) == [SWKS]
    assert "no XML" in result.key_errors[SWKS]
    assert _open(settings) == [SWKS]
    assert {r[0] for r in _query(settings, "SELECT accession_no FROM insider_filing")} == {
        COST, BNC_A, STWI,
    }  # fmt: skip


def test_a_missing_folder_fails_the_document(settings, fake, clock):
    _run(settings, clock)
    _queue(settings, clock, [("0000000000-26-000001", 1, 0)])
    result = _run(settings, clock)
    assert result.status == "failed" and "404" in result.error
    assert _open(settings) == ["0000000000-26-000001"]
