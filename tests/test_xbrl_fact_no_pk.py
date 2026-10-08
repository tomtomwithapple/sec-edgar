"""`xbrl_fact` without a primary key (plan: How › SEC EDGAR › Bulk, Decisions after S26 (1); S27).

The 7-column PK cost ~70 of 90 bytes per fact (S21, S26), so migration 014 rebuilds the table
without it and uniqueness of (cik, taxonomy, tag, unit, accn, period_start, period_end) becomes
the loader's job. These tests prove the per-CIK job, its reparse and the bulk path still never
store one key twice, and that `xbrl_as_of` survives the rebuild. Fixtures are the real
companyfacts documents of `test_edgar_companyfacts` and the bulk ZIPs of `test_edgar_bulk`.
"""

import copy
import json

import pytest
import test_edgar_bulk as bulk_fx
import test_edgar_companyfacts as cf

from usfin import jobs, store
from usfin.sources.edgar import companyfacts

# the companyfacts job's fixtures and helpers
settings, fake, clock = cf.settings, cf.fake, cf.clock
_run, _query, _facts, utc, APPLE = cf._run, cf._query, cf._facts, cf.utc, cf.APPLE

KEY = ", ".join(companyfacts.KEY)
DUPLICATES = f"SELECT {KEY}, count(*) FROM xbrl_fact GROUP BY ALL HAVING count(*) > 1"


def _no_duplicates(settings):
    assert _query(settings, DUPLICATES) == []


def test_xbrl_fact_has_no_primary_key_and_keeps_its_columns(settings):
    with store.session(settings.db_path, wait=0) as con:
        constraints = con.execute(
            "SELECT constraint_type FROM duckdb_constraints() WHERE table_name = 'xbrl_fact'"
        ).fetchall()
        cols = [r[0] for r in con.execute("DESCRIBE xbrl_fact").fetchall()]
    assert ("PRIMARY KEY",) not in constraints
    assert cols == [
        "cik", "taxonomy", "tag", "unit", "accn", "period_start", "period_end", "is_instant",
        "value", "fy", "fp", "form", "filed", "frame",
    ]  # fmt: skip
    # the taxonomy CHECK and NOT NULLs are kept
    assert ("CHECK",) in constraints and ("NOT NULL",) in constraints


def test_the_migration_keeps_existing_facts_and_xbrl_as_of(tmp_path, monkeypatch):
    path = tmp_path / "old.duckdb"
    every = store.available_migrations()
    monkeypatch.setattr(store, "available_migrations", lambda: [m for m in every if m.version <= 13])
    with store.session(path, wait=0) as con:
        con.execute(
            "INSERT INTO xbrl_fact VALUES (320193, 'us-gaap', 'Assets', 'USD', 'a-1',"
            " '2009-09-26', '2009-09-26', true, 1.0, 2009, 'FY', '10-K', '2009-10-27', NULL)"
        )
    monkeypatch.setattr(store, "available_migrations", lambda: every)
    with store.session(path, wait=0) as con:
        assert store.schema_version(con) == every[-1].version
        assert con.execute("SELECT accn, value FROM xbrl_as_of('2009-10-27')").fetchall() == [
            ("a-1", 1.0)
        ]
        assert con.execute("SELECT count(*) FROM xbrl_as_of(DATE '2009-10-26')").fetchone() == (0,)
        assert con.execute(
            "SELECT count(*) FROM duckdb_constraints()"
            " WHERE table_name = 'xbrl_fact' AND constraint_type = 'PRIMARY KEY'"
        ).fetchone() == (0,)


def test_re_runs_and_a_moved_frame_never_duplicate_a_fact(settings, fake, clock):
    _run(settings, clock)
    _run(settings, clock, when=utc(2026, 10, 9, 14, 0))
    _no_duplicates(settings)
    usd = fake.apple["facts"]["us-gaap"]["SalesRevenueNet"]["units"]["USD"]
    usd[0]["frame"] = "CY2009"  # a changed value column under the same key
    again = _run(settings, clock, when=utc(2026, 10, 10, 14, 0))
    assert again.rows_upserted == 1
    _no_duplicates(settings)
    assert len(_facts(settings)) == 13
    assert _query(
        settings,
        "SELECT frame FROM xbrl_fact WHERE accn = ? AND tag = 'SalesRevenueNet'",
        [usd[0]["accn"]],
    ) == [("CY2009",)]


def test_a_document_listing_one_fact_twice_stores_it_once(settings, fake, clock):
    usd = fake.apple["facts"]["us-gaap"]["Assets"]["units"]["USD"]
    usd.append(copy.deepcopy(usd[0]))
    result = _run(settings, clock, ciks=(APPLE,))
    assert result.status == "ok" and result.rows_upserted == 9
    _no_duplicates(settings)


def test_reparse_twice_never_duplicates(settings, fake, clock):
    _run(settings, clock)
    facts = _facts(settings)
    with store.session(settings.db_path, wait=0) as con:
        jobs.reparse(con, companyfacts.JOB, settings=settings)
        jobs.reparse(con, companyfacts.JOB, settings=settings)
    assert _facts(settings) == facts
    _no_duplicates(settings)


def test_the_bulk_path_and_the_per_cik_job_together_never_duplicate(
    settings, fake, clock, monkeypatch, tmp_path
):
    # the per-CIK job first, then the same documents from companyfacts.zip, twice
    _run(settings, clock)
    monkeypatch.setattr(bulk_fx.raw, "LARGE_BODY_BYTES", 1000)
    from usfin.sources.edgar import bulk

    zip_path = tmp_path / "companyfacts.zip"
    zip_path.write_bytes(bulk_fx.zip_of("companyfacts"))
    with store.session(settings.db_path, wait=0) as con:
        rid = bulk_fx.raw.save(
            con, source="edgar", dataset="bulk_companyfacts", url=bulk.DATASETS["companyfacts"].url,
            body=zip_path.read_bytes(), http_status=200, raw_dir=settings.raw_dir,
        )  # fmt: skip
        first = bulk.load(con, settings, bulk.DATASETS["companyfacts"], rid)
        con.execute("DELETE FROM edgar_bulk_member")  # force every member to parse again
        second = bulk.load(con, settings, bulk.DATASETS["companyfacts"], rid)
    assert (first.parsed, first.rows, second.parsed, second.rows) == (2, 0, 2, 0)
    _no_duplicates(settings)
    assert len(_facts(settings)) == 13


def test_storing_one_company_leaves_another_companys_facts_alone(settings):
    # the diff is limited to the document's CIK
    with store.session(settings.db_path, wait=0) as con:
        con.execute(
            "INSERT INTO xbrl_fact VALUES (1, 'us-gaap', 'Assets', 'USD', 'x', '2020-01-01',"
            " '2020-01-01', true, 5.0, 2020, 'FY', '10-K', '2020-02-01', NULL)"
        )
        doc = cf.Fake().apple
        companyfacts.store_document(con, doc, name="CIK0000320193.json")
        assert con.execute("SELECT count(*) FROM xbrl_fact WHERE cik = 1").fetchone() == (1,)
        assert con.execute(
            "SELECT count(*) FROM xbrl_fact WHERE cik = ?", [APPLE]
        ).fetchone() == (9,)


def test_a_fiscal_year_beyond_smallint_is_stored_as_filed(settings):
    # S27 live: CIRCOR's 2021 10-Qs report fy 44561 (an Excel serial date for 2021-12-31) on
    # every fact; the S21 SMALLINT failed the member. `fixtures/edgar/bulk/CIK0001091883.json`
    # is that companyfacts.zip member trimmed to two facts (labels verbatim).
    path = bulk_fx.FIXTURES / "bulk" / "CIK0001091883.json"
    doc = json.loads(path.read_text("utf-8"))
    with store.session(settings.db_path, wait=0) as con:
        stored = companyfacts.store_document(con, doc, name=path.name)
        rows = con.execute("SELECT accn, fy FROM xbrl_fact ORDER BY accn").fetchall()
        fy_type = con.execute(
            "SELECT data_type FROM duckdb_columns() WHERE table_name = 'xbrl_fact'"
            " AND column_name = 'fy'"
        ).fetchone()
    assert stored.facts == 2
    assert rows == [("0001091883-21-000014", 44561), ("0001091883-22-000053", 2022)]
    assert fy_type == ("INTEGER",)


def test_a_member_without_a_cik_field_takes_the_cik_from_its_name(settings):
    # S27 live: 8 companyfacts.zip members have no `cik` key at all (funds with only vip/cef/oef
    # facts, and two companies with us-gaap facts: Star Mountain 4 588, ZeroStack 6 027).
    # `fixtures/edgar/bulk/CIK0001786835.json` is Star Mountain's member trimmed to 2 facts.
    path = bulk_fx.FIXTURES / "bulk" / "CIK0001786835.json"
    doc = json.loads(path.read_text("utf-8"))
    assert "cik" not in doc
    with store.session(settings.db_path, wait=0) as con:
        stored = companyfacts.store_document(con, doc, name=path.name)
        rows = con.execute("SELECT DISTINCT cik FROM xbrl_fact").fetchall()
    assert (stored.cik, stored.facts, rows) == (1786835, 2, [(1786835,)])
    # a cik that is present and differs from the name is still refused
    doc["cik"] = 1
    with pytest.raises(companyfacts.CompanyFactsError, match="cik"):
        companyfacts.store_document(None, doc, name=path.name)
