"""`pending_doc`: filing documents queued by the feed (S22) and the daily index (S23) for the job
of each kind (`submissions`, `companyfacts`, `form4` S24, `sched13` S25).

A queued row is `(accession_no, kind, cik, enqueued_at, done_at NULL)`; enqueueing an accession
already queued for that kind changes nothing (a done row stays done). The per-CIK jobs take the
distinct CIKs of open rows as their keys (one request per CIK however many filings queued it) and,
after a key succeeded, mark its rows done when the fetched document carries the accession
(`submissions`: the company lists it, a party row with role `submissions`; `companyfacts`: an
`xbrl_fact` with that `accn`) or when it has waited `GIVE_UP` (SEC builds companyfacts some
minutes after acceptance, and not every 8-K or amendment carries XBRL facts). An open row keeps
its CIK in the next drain.
"""

from collections.abc import Iterable
from datetime import datetime, timedelta

import duckdb

from usfin import store

TABLE = "pending_doc"
KINDS = ("submissions", "companyfacts", "form4", "sched13")
GIVE_UP = timedelta(days=1)

FOUND = {
    "submissions": (
        "EXISTS (SELECT 1 FROM edgar_filing_party x WHERE x.accession_no = p.accession_no"
        " AND x.cik = p.cik AND x.role = 'submissions')"
    ),
    "companyfacts": (
        "EXISTS (SELECT 1 FROM xbrl_fact x WHERE x.cik = p.cik AND x.accn = p.accession_no)"
    ),
}
"""SQL over `pending_doc p`: the fetched document carries this queued accession."""


def enqueue(
    con: duckdb.DuckDBPyConnection,
    rows: Iterable[tuple[str, str, int]],
    *,
    enqueued_at: datetime,
) -> int:
    """Queue (accession_no, kind, cik) rows; return how many were new."""
    rows = list(dict.fromkeys((a, k, int(c)) for a, k, c in rows))
    if not rows:
        return 0
    bad = {k for _, k, _ in rows} - set(KINDS)
    if bad:
        raise ValueError(f"unknown pending_doc kind(s) {sorted(bad)}")
    before = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    con.executemany(
        f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?, NULL) ON CONFLICT DO NOTHING",
        [[a, k, c, enqueued_at] for a, k, c in rows],
    )
    return con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0] - before


def open_ciks(con: duckdb.DuckDBPyConnection, kind: str) -> list[int]:
    """Distinct CIKs with open rows of `kind` (none before the table exists)."""
    if TABLE not in store.table_names(con):
        return []
    rows = con.execute(
        f"SELECT DISTINCT cik FROM {TABLE} WHERE kind = ? AND done_at IS NULL ORDER BY cik",
        [kind],
    ).fetchall()
    return [r[0] for r in rows]


def mark_done(con: duckdb.DuckDBPyConnection, kind: str, cik: int, *, now: datetime) -> int:
    """Close `cik`'s open rows of `kind` whose accession the fetched document carries, or that
    waited `GIVE_UP`; return how many."""
    if TABLE not in store.table_names(con):
        return 0
    found = FOUND[kind]
    return con.execute(
        f"UPDATE {TABLE} p SET done_at = ? WHERE p.kind = ? AND p.cik = ? AND p.done_at IS NULL"
        f" AND ({found} OR p.enqueued_at <= ?)",
        [now, kind, int(cik), now - GIVE_UP],
    ).fetchone()[0]
