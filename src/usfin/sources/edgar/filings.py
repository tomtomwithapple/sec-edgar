"""`edgar_filing` and `edgar_filing_party`: shared by every EDGAR source that lists filings
(submissions S20, feed S22, daily index S23, bulk submissions S26).

A filing row comes from whichever source saw the accession first; a later source only fills the
columns still NULL (`accepted_at`, `report_date`, `primary_doc`) and never blanks or overwrites
one, so the order sources run in cannot lose a value and a re-run changes nothing (C2).
"""

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime

import duckdb

SOURCES = ("feed", "index", "submissions")
FILLABLE = ("accepted_at", "report_date", "primary_doc")


@dataclass(frozen=True)
class Filing:
    accession_no: str
    cik: int
    form: str
    filed_at: date
    accepted_at: datetime | None
    report_date: date | None
    primary_doc: str | None
    is_amendment: bool
    source: str


def is_amendment(form: str) -> bool:
    return form.strip().upper().endswith("/A")


def _iso(v: date | datetime | None) -> str | None:
    return None if v is None else v.isoformat()


def upsert_filings(con: duckdb.DuckDBPyConnection, filings: Sequence[Filing]) -> int:
    """Insert new accessions and fill NULL columns of known ones; return rows inserted or
    filled. Within one call the first filing of an accession wins."""
    if not filings:
        return 0
    bad = {f.source for f in filings} - set(SOURCES)
    if bad:
        raise ValueError(f"unknown filing source(s) {sorted(bad)}")
    rows = [
        [f.accession_no, f.cik, f.form, _iso(f.filed_at), _iso(f.accepted_at),
         _iso(f.report_date), f.primary_doc, f.is_amendment, f.source, i]
        for i, f in enumerate(filings)
    ]  # fmt: skip
    payload = json.dumps(rows)
    # one JSON text parameter, not bound row values (see fiscal.upsert_page)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _filing_stage AS"
        " SELECT r[1] AS accession_no, r[2]::BIGINT AS cik, r[3] AS form,"
        " r[4]::DATE AS filed_at, r[5]::TIMESTAMP AS accepted_at, r[6]::DATE AS report_date,"
        " r[7] AS primary_doc, r[8]::BOOLEAN AS is_amendment, r[9] AS source"
        """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)"""
        " QUALIFY row_number() OVER (PARTITION BY r[1] ORDER BY r[10]::BIGINT) = 1",
        [payload],
    )
    fills = " OR ".join(f"(f.{c} IS NULL AND s.{c} IS NOT NULL)" for c in FILLABLE)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _filing_changed AS SELECT s.* FROM _filing_stage s"
        f" LEFT JOIN edgar_filing f USING (accession_no) WHERE f.accession_no IS NULL OR {fills}"
    )
    changed = con.execute("SELECT count(*) FROM _filing_changed").fetchone()[0]
    if changed:
        sets = ", ".join(f"{c} = coalesce(edgar_filing.{c}, excluded.{c})" for c in FILLABLE)
        con.execute(
            "INSERT INTO edgar_filing SELECT * FROM _filing_changed"
            f" ON CONFLICT (accession_no) DO UPDATE SET {sets}"
        )
    con.execute("DROP TABLE _filing_stage")
    con.execute("DROP TABLE _filing_changed")
    return int(changed)


def add_parties(con: duckdb.DuckDBPyConnection, parties: Iterable[tuple[str, int, str]]) -> int:
    """Store (accession_no, cik, role) rows; return how many were new."""
    rows = [[a, int(c), r] for a, c, r in parties]
    if not rows:
        return 0
    before = con.execute("SELECT count(*) FROM edgar_filing_party").fetchone()[0]
    con.execute(
        "INSERT INTO edgar_filing_party SELECT DISTINCT r[1], r[2]::BIGINT, r[3]"
        """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)"""
        " ON CONFLICT DO NOTHING",
        [json.dumps(rows)],
    )
    return con.execute("SELECT count(*) FROM edgar_filing_party").fetchone()[0] - before
