"""Cross-check and calendar pages: `/series/auctions/xcheck` (S38) and `/series/releases` (S39).

- **Auction cross-check** reads `auction_xcheck` (job `auction_xcheck`, C15): a summary of every
  result by security type, then the pairs of one result. The default list is every pair that is
  not `matched` (mismatch, pending, one side only), since matched pairs are the bulk and agree;
  `?result=all` lists every pair. A pair's `diff` (`{field: {fiscal, treasurydirect}}`) is shown
  field by field, and the CSV keeps it as JSON.
- **BLS release calendar** reads `bls_release` (job `bls_calendar`, S17) with the series each
  release publishes from `config/bls.yaml`. The default is upcoming releases (New York date from
  today); `?when=past` lists past ones newest first, `?when=all` every one; `?release=` keeps one
  release name.
"""

import json
from collections.abc import Mapping
from datetime import UTC, date
from typing import Any
from zoneinfo import ZoneInfo

import duckdb

from usfin.sources import auction_xcheck, bls, bls_calendar

ET = ZoneInfo("America/New_York")

RESULTS = ("mismatch", "pending", "fd_only", "td_only", "matched")
"""Results in the order the page shows them (a disagreement first)."""
RESULT_PICKS = ("open", "all", *RESULTS)
"""`?result=`: `open` = every result but `matched` (the default)."""
XCHECK_SUMMARY_COLUMNS = ("security_type", *RESULTS, "pairs", "first", "last")
XCHECK_COLUMNS = ("auction_date", "cusip", "security_type", "result", "fields", "diff")

WHEN = ("upcoming", "past", "all")
RELEASE_COLUMNS = ("release_date", "release_name", "released_at", "series")
RELEASE_SUMMARY_COLUMNS = ("release_name", "series", "next", "last", "releases")


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
        ).fetchone()[0]
    )


def _dicts(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


# --- auction cross-check -------------------------------------------------------------------------


def xcheck_summary(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Pairs per result for each security type, then a total row (`security_type` = `all`)."""
    if not _table_exists(con, auction_xcheck.TABLE):
        return []
    counts = ", ".join(f"count(*) FILTER (WHERE result = '{r}') AS {r}" for r in RESULTS)
    tail = f"{counts}, count(*) AS pairs, min(auction_date) AS first, max(auction_date) AS last"
    by_type = _dicts(
        con.execute(
            f"SELECT coalesce(security_type, '(none)') AS security_type, {tail}"
            f" FROM {auction_xcheck.TABLE} GROUP BY 1 ORDER BY 1"
        )
    )
    total = _dicts(con.execute(f"SELECT 'all' AS security_type, {tail} FROM {auction_xcheck.TABLE}"))
    return by_type + total if by_type else []


def _fields(diff: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """`diff` as rows: field, Fiscal Data value, TreasuryDirect value."""
    return [
        {"field": f, "fiscal": v.get("fiscal"), "treasurydirect": v.get("treasurydirect")}
        for f, v in sorted((diff or {}).items())
        if isinstance(v, Mapping)
    ]


def xcheck_rows(con: duckdb.DuckDBPyConnection, result: str = "open") -> list[dict[str, Any]]:
    """Pairs of `result` (`open`: all but matched; `all`), by result order then newest first."""
    if not _table_exists(con, auction_xcheck.TABLE):
        return []
    where, params = "true", []
    if result == "open":
        where = "result <> 'matched'"
    elif result != "all":
        where, params = "result = ?", [result]
    order = "CASE result " + " ".join(f"WHEN '{r}' THEN {i}" for i, r in enumerate(RESULTS))
    rows = _dicts(
        con.execute(
            "SELECT auction_date, cusip, security_type, result, diff"
            f" FROM {auction_xcheck.TABLE} WHERE {where}"
            f" ORDER BY {order} END, auction_date DESC, cusip",
            params,
        )
    )
    for r in rows:
        diff = json.loads(r["diff"]) if isinstance(r["diff"], str) else r["diff"]
        r["diff"] = diff
        r["field_rows"] = _fields(diff)
        r["fields"] = ", ".join(f["field"] for f in r["field_rows"])
    return rows


# --- BLS release calendar ------------------------------------------------------------------------


def release_names(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not _table_exists(con, bls_calendar.TABLE):
        return []
    return [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT release_name FROM {bls_calendar.TABLE} ORDER BY 1"
        ).fetchall()
    ]


def releases(
    con: duckdb.DuckDBPyConnection,
    config_dir,
    *,
    today: date,
    when: str = "upcoming",
    release: str | None = None,
) -> list[dict[str, Any]]:
    """Release events with the configured series each publishes; upcoming soonest first, past
    newest first, all in date order."""
    if not _table_exists(con, bls_calendar.TABLE):
        return []
    where, params = ["true"], []
    if when == "upcoming":
        where.append("release_date >= ?")
        params.append(today)
    elif when == "past":
        where.append("release_date < ?")
        params.append(today)
    if release is not None:
        where.append("release_name = ?")
        params.append(release)
    order = "DESC" if when == "past" else ""
    rows = _dicts(
        con.execute(
            f"SELECT release_date, release_name, released_at FROM {bls_calendar.TABLE}"
            f" WHERE {' AND '.join(where)}"
            f" ORDER BY release_date {order}, released_at {order}, release_name",
            params,
        )
    )
    configured = bls.load_series(config_dir)
    for r in rows:
        r["released_et"] = r["released_at"].replace(tzinfo=UTC).astimezone(ET)
        r["series_ids"] = bls_calendar.series_for(r["release_name"], configured)
        r["series"] = " ".join(r["series_ids"])
    return rows


def release_summary(
    con: duckdb.DuckDBPyConnection, config_dir, *, today: date
) -> list[dict[str, Any]]:
    """One row per release that publishes a configured series: its next and last date."""
    if not _table_exists(con, bls_calendar.TABLE):
        return []
    configured = bls.load_series(config_dir)
    names = sorted({s.release for s in configured.values()})
    rows = []
    for name in names:
        nxt, last, n = con.execute(
            "SELECT min(release_date) FILTER (WHERE release_date >= ?),"
            " max(release_date) FILTER (WHERE release_date < ?), count(*)"
            f" FROM {bls_calendar.TABLE} WHERE release_name = ?",
            [today, today, name],
        ).fetchone()
        ids = bls_calendar.series_for(name, configured)
        rows.append(
            {
                "release_name": name,
                "series_ids": ids,
                "series": " ".join(ids),
                "next": nxt,
                "last": last,
                "releases": n,
            }
        )
    return rows
