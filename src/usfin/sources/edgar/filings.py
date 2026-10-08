"""`edgar_filing` and `edgar_filing_party`: shared by every EDGAR source that lists filings
(submissions S20, feed S22, daily index S23, bulk submissions S26).

A filing row comes from whichever source saw the accession first; a later source only fills the
columns still NULL (`accepted_at`, `report_date`, `primary_doc`) and never blanks or overwrites
one, so the order sources run in cannot lose a value and a re-run changes nothing (C2).

`cik` is the one exception (decided in S22): the feed names each party's role, the other sources
do not (submissions: the company whose JSON it is; the daily index: one line per CIK). So once
any party with a feed role is stored, `settle_cik` sets `cik` to the party with the first role in
`ROLE_PRECEDENCE` (Issuer, Subject, Filer, Filed by, Reporting; the lowest CIK on a tie). It reads
the whole party table, so the result does not depend on the order sources ran or pages arrived,
and reparse lands on the same `cik`. A filing with no feed role keeps the CIK first written.

`accepted_at` from the feed also wins (S22): the feed writes it with an explicit New York offset,
while the submissions JSON's `acceptanceDateTime` is true UTC on the filing day and shifted by
the offset later (see `submissions.accepted_utc`), so a value it wrote first may be 4–5 h off.
"""

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime

import duckdb

SOURCES = ("feed", "index", "submissions")
ROLE_PRECEDENCE = ("Issuer", "Subject", "Filer", "Filed by", "Reporting")
"""Feed roles in the order that picks `edgar_filing.cik` (other roles never pick it)."""
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
    fills += (
        " OR (s.source = 'feed' AND s.accepted_at IS NOT NULL"
        " AND f.accepted_at IS DISTINCT FROM s.accepted_at)"
    )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _filing_changed AS SELECT s.* FROM _filing_stage s"
        f" LEFT JOIN edgar_filing f USING (accession_no) WHERE f.accession_no IS NULL OR {fills}"
    )
    changed = con.execute("SELECT count(*) FROM _filing_changed").fetchone()[0]
    if changed:
        sets = {c: f"coalesce(edgar_filing.{c}, excluded.{c})" for c in FILLABLE}
        sets["accepted_at"] = (
            "CASE WHEN excluded.source = 'feed' AND excluded.accepted_at IS NOT NULL"
            f" THEN excluded.accepted_at ELSE {sets['accepted_at']} END"
        )
        sets = ", ".join(f"{c} = {expr}" for c, expr in sets.items())
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


def best_party(parties: Iterable[tuple[int, str]]) -> int | None:
    """The CIK `edgar_filing.cik` should be among (cik, role) parties, or None (no feed role)."""
    ranked = [
        (ROLE_PRECEDENCE.index(role), int(cik)) for cik, role in parties if role in ROLE_PRECEDENCE
    ]
    return min(ranked)[1] if ranked else None


def settle_cik(con: duckdb.DuckDBPyConnection, accession_nos: Iterable[str]) -> int:
    """Point `edgar_filing.cik` of these accessions at their best feed-role party (see the module
    doc); return how many rows changed."""
    accs = sorted(set(accession_nos))
    if not accs:
        return 0
    ranks = " ".join(f"WHEN '{r}' THEN {i}" for i, r in enumerate(ROLE_PRECEDENCE))
    roles = ", ".join(f"'{r}'" for r in ROLE_PRECEDENCE)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _best_party AS"
        " SELECT accession_no, arg_min(cik, (CASE role " + ranks + " END, cik)) AS cik"
        " FROM edgar_filing_party"
        f" WHERE role IN ({roles})"
        "  AND accession_no IN (SELECT unnest(json_transform(?, '[\"VARCHAR\"]')))"
        " GROUP BY accession_no",
        [json.dumps(accs)],
    )
    changed = con.execute(
        "SELECT count(*) FROM edgar_filing f JOIN _best_party b USING (accession_no)"
        " WHERE f.cik <> b.cik"
    ).fetchone()[0]
    if changed:
        con.execute(
            "UPDATE edgar_filing SET cik = b.cik FROM _best_party b"
            " WHERE edgar_filing.accession_no = b.accession_no AND edgar_filing.cik <> b.cik"
        )
    con.execute("DROP TABLE _best_party")
    return int(changed)
