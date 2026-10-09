"""Unmatched symbols page (`/symbols/unmatched`) queries (S34, C10).

Reads view `symbol_edgar_match` (S31): one row per open Nasdaq Trader symbol with the active
EDGAR company of the same `ticker_norm`, `cik` NULL when none. The page has two tables:

- **rates** — open symbols and how many match, non-ETF first (EDGAR's ticker map is mostly
  operating companies, so ETFs mostly don't match and that is expected, not a normalization gap),
  then ETF, then all; then per exchange, non-ETF before ETF.
- **symbols** — the unmatched open symbols (or matched / all with `?match=`), non-ETFs first,
  filtered by ETF flag and exchange. An unmatched class/preferred/warrant symbol (its norm has a
  `.`) whose root (the norm before the first `.`) is an active EDGAR company's carries that
  company as a hint (`root_*`): Noble's warrants `NE.A` (EDGAR `NE-WTA`) or a preferred SEC does
  not list (`TCBI$B`) point at the issuer. A plain symbol gets no hint: the Cboe ETF `CRD` is not
  Crawford & Co (`CRD-A`), nor `CTA` EIDP's preferred `CTA-PA` (found in the S34 run).
"""

from collections.abc import Sequence
from typing import Any

import duckdb

from usfin.sources import nasdaqtrader

HTML_ROWS = 500
"""Symbol rows shown in HTML (the CSV has all of them)."""
TABLES = ("unmatched", "rates")
MATCH = ("unmatched", "matched", "all")
ETF = {"": None, "0": False, "1": True}
RATE_COLUMNS = ("kind", "exchange", "symbols", "matched", "unmatched", "matched_pct")
SYMBOL_COLUMNS = (
    "symbol", "exchange", "etf", "name", "ticker_norm", "cik", "edgar_ticker", "edgar_title",
    "root_cik", "root_ticker", "root_title", "file",
)  # fmt: skip


def _kind(etf: bool | None) -> str:
    return "all" if etf is None else "ETF" if etf else "non-ETF"


def rates(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Match rates: non-ETF, ETF, all; then per exchange non-ETF, then per exchange ETF."""
    found = nasdaqtrader.match_rates(con)
    if not found:
        return []
    exchanges = sorted({r.exchange for r in found})
    groups: list[tuple[bool | None, str | None]] = [(False, None), (True, None), (None, None)]
    groups += [(etf, ex) for etf in (False, True) for ex in exchanges]
    out = []
    for etf, ex in groups:
        chosen = [
            r for r in found
            if (etf is None or r.etf == etf) and (ex is None or r.exchange == ex)
        ]  # fmt: skip
        if not chosen:
            continue
        n, m = sum(r.symbols for r in chosen), sum(r.matched for r in chosen)
        out.append(
            {
                "kind": _kind(etf),
                "exchange": ex or "all",
                "symbols": n,
                "matched": m,
                "unmatched": n - m,
                "matched_pct": f"{100 * m / n:.1f}",
                "etf_param": "" if etf is None else str(int(etf)),
                "exchange_param": ex or "",
            }
        )
    return out


def symbols(
    con: duckdb.DuckDBPyConnection,
    *,
    match: str = "unmatched",
    etf: bool | None = None,
    exchange: str | None = None,
) -> list[dict[str, Any]]:
    """Open symbols (unmatched, matched or all), non-ETFs first, then by symbol and exchange."""
    where, params = ["true"], []
    if match == "unmatched":
        where.append("m.cik IS NULL")
    elif match == "matched":
        where.append("m.cik IS NOT NULL")
    if etf is not None:
        where.append("coalesce(m.etf, false) = ?")
        params.append(etf)
    if exchange is not None:
        where.append("m.exchange = ?")
        params.append(exchange)
    cur = con.execute(
        f"""
        WITH roots AS (
            SELECT split_part(ticker_norm, '.', 1) AS root,
                   arg_min(cik, lpad(length(ticker)::VARCHAR, 3, '0') || ticker) AS cik,
                   arg_min(ticker, lpad(length(ticker)::VARCHAR, 3, '0') || ticker) AS ticker,
                   arg_min(title, lpad(length(ticker)::VARCHAR, 3, '0') || ticker) AS title
            FROM edgar_company WHERE active GROUP BY ALL
        )
        SELECT m.symbol, m.exchange, coalesce(m.etf, false) AS etf, m.name, m.ticker_norm,
               m.cik, m.edgar_ticker, m.edgar_title,
               CASE WHEN m.cik IS NULL THEN r.cik END AS root_cik,
               CASE WHEN m.cik IS NULL THEN r.ticker END AS root_ticker,
               CASE WHEN m.cik IS NULL THEN r.title END AS root_title,
               m.file
        FROM {nasdaqtrader.MATCH} m
        LEFT JOIN roots r
            ON contains(m.ticker_norm, '.') AND r.root = split_part(m.ticker_norm, '.', 1)
        WHERE {" AND ".join(where)}
        ORDER BY coalesce(m.etf, false), m.symbol, m.exchange
        """,
        params,
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def table_name(match: str, etf: bool | None, exchange: str | None) -> str:
    """CSV stem of the symbols table: `symbols_unmatched[_etf|_non-etf][_<exchange>]`."""
    parts = ["symbols", match]
    if etf is not None:
        parts.append("etf" if etf else "non-etf")
    if exchange:
        parts.append(exchange)
    return "_".join(parts)


def exchanges(rows: Sequence[dict[str, Any]]) -> list[str]:
    return sorted({r["exchange"] for r in rows if r["exchange"] != "all"})


# --- symbol history (`/symbols/history`, S40) ---------------------------------------------------
#
# Events read off `symbol_history` (S30): a version opened with no version of the same (symbol,
# exchange) closing at that instant is a listing; one closed with none opening is a delisting; a
# close and an open at the same instant is a change (name, ETF flag, market category or financial
# status), shown with the fields that changed and their old values. The first diffed snapshot of
# each file opens every symbol it lists, so its opens are the baseline, not listings, and are left
# out. A symbol that moves exchange is a delisting under the old exchange and a listing under the
# new one, as stored.

SNAPSHOT_ROWS = 20
"""Snapshots shown in HTML (the CSV has all of them)."""
EVENTS = ("listed", "delisted", "changed")
EVENT_PICKS = ("all", *EVENTS)
TRACKED = ("name", "etf", "market_category", "financial_status")
EVENT_COLUMNS = (
    "at", "event", "symbol", "exchange", "file", "name", "etf", "changed", "before",
)  # fmt: skip
VERSION_COLUMNS = (
    "symbol", "exchange", "valid_from", "valid_to", "file", "name", "etf", "market_category",
    "financial_status",
)  # fmt: skip
SNAPSHOT_COLUMNS = (
    "file", "snapshot_at", "rows", "previous_at", "previous_rows", "pct_change", "diffed",
    "opened", "closed",
)  # fmt: skip


def history_baseline(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Each file's first diffed snapshot (the baseline every later event is measured from)."""
    return dict(
        con.execute(
            "SELECT file, min(valid_from) FROM symbol_history GROUP BY file ORDER BY file"
        ).fetchall()
    )


def history_events(
    con: duckdb.DuckDBPyConnection,
    *,
    event: str = "all",
    symbol: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Listings, delistings and changes after each file's baseline, newest first."""
    changed = " || ".join(
        f"CASE WHEN p.{c} IS DISTINCT FROM h.{c} THEN '{c} ' ELSE '' END" for c in TRACKED
    )
    before = " || ".join(
        f"CASE WHEN p.{c} IS DISTINCT FROM h.{c}"
        f" THEN '{c}=' || coalesce(p.{c}::VARCHAR, '') || '; ' ELSE '' END"
        for c in TRACKED
    )
    where, params = ["true"], []
    if event != "all":
        where.append("event = ?")
        params.append(event)
    if symbol is not None:
        where.append("symbol = ?")
        params.append(symbol)
    sql = f"""
        WITH base AS (SELECT file, min(valid_from) AS t0 FROM symbol_history GROUP BY file),
        opened AS (
            SELECT h.valid_from AS "at",
                   CASE WHEN p.symbol IS NULL THEN 'listed' ELSE 'changed' END AS event,
                   h.symbol, h.exchange, h.file, h.name, h.etf,
                   CASE WHEN p.symbol IS NULL THEN NULL ELSE trim({changed}) END AS changed,
                   CASE WHEN p.symbol IS NULL THEN NULL
                        ELSE rtrim(trim({before}), ';') END AS before
            FROM symbol_history h
            JOIN base b ON b.file = h.file AND h.valid_from > b.t0
            LEFT JOIN symbol_history p
                ON p.symbol = h.symbol AND p.exchange = h.exchange AND p.valid_to = h.valid_from
        ),
        closed AS (
            SELECT h.valid_to AS "at", 'delisted' AS event, h.symbol, h.exchange, h.file, h.name,
                   h.etf, NULL AS changed, NULL AS before
            FROM symbol_history h
            WHERE h.valid_to IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM symbol_history n
                WHERE n.symbol = h.symbol AND n.exchange = h.exchange AND n.valid_from = h.valid_to
            )
        )
        SELECT * FROM (SELECT * FROM opened UNION ALL SELECT * FROM closed)
        WHERE {" AND ".join(where)}
        ORDER BY "at" DESC, symbol, exchange, event
    """
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def event_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    found = {e: 0 for e in EVENTS}
    for r in history_events(con):
        found[r["event"]] += 1
    return found


def versions(con: duckdb.DuckDBPyConnection, symbol: str) -> list[dict[str, Any]]:
    """Every stored version of `symbol` on any exchange, newest first."""
    cur = con.execute(
        f"SELECT {', '.join(VERSION_COLUMNS)} FROM symbol_history WHERE symbol = ?"
        " ORDER BY valid_from DESC, exchange",
        [symbol],
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def snapshots(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Stored snapshots with their row-count gate result, newest first."""
    cur = con.execute(
        f"SELECT {', '.join(SNAPSHOT_COLUMNS)} FROM symbol_diff ORDER BY snapshot_at DESC, file"
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
