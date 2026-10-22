"""Prices pages (`/prices`, `/prices/universe`) queries (plan alpaca-market-data: A11, S11).

- **prices** — the daily bars of the security a symbol names today (`alpaca_security_lookup`,
  S9), chained across renames (META: FB's bars to 2022-06-08, then META's), with the adjusted
  close and factors of `alpaca_bar_adjusted_lookup` (S10) and the bar's other columns from the
  newest version of each (symbol, date) (`alpaca_bar_latest`'s rule, applied after the symbol
  filter so only the security's symbols are read). The segments table lists the chain.
- **universe** — the bars universe (`alpaca_universe`, S5) one row per symbol: its asset status
  (`active` when an active listed asset holds it, else `inactive` when an inactive or vanished
  one does, else `none`: known only from a merger, removal or rename), the exchange of that
  asset, its rules (`sources`), `delisted` (not active), its stored bars and, for an active
  symbol only, the active EDGAR company sharing its `ticker_norm` (A8: delisted symbols are not
  joined, `edgar_company` keeps only active pairs).
"""

from typing import Any

import duckdb

HTML_ROWS = 500
"""Rows shown in HTML (the CSV has all of them)."""
PRICE_TABLES = ("bars", "segments")
BAR_COLUMNS = (
    "date", "symbol", "open", "high", "low", "close", "volume", "trade_count", "vwap",
    "adj_close", "split_factor", "tr_factor", "security_key",
)  # fmt: skip
SEGMENT_COLUMNS = ("security_key", "symbol", "valid_from", "valid_to", "bars")
STATUS = ("active", "inactive", "none")
FLAG = {"": None, "0": False, "1": True}
UNIVERSE_TABLES = ("universe",)
UNIVERSE_COLUMNS = (
    "symbol", "status", "exchange", "name", "sources", "delisted", "first_bar", "last_bar",
    "bars", "cik", "edgar_ticker", "edgar_title",
)  # fmt: skip


def _rows(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def segments(
    con: duckdb.DuckDBPyConnection, symbol: str, bar_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """The segments of the security `symbol` names today, oldest first, each with how many of
    `bar_rows` (that security's bars) it holds."""
    found = _rows(
        con.execute(
            "SELECT security_key, symbol, valid_from, valid_to FROM alpaca_security_lookup(?)"
            " ORDER BY valid_from, symbol",
            [symbol],
        )
    )
    for seg in found:
        seg["bars"] = sum(
            1
            for b in bar_rows
            if b["symbol"] == seg["symbol"] and seg["valid_from"] <= b["date"] <= seg["valid_to"]
        )
    return found


def bars(con: duckdb.DuckDBPyConnection, symbol: str) -> list[dict[str, Any]]:
    """The security's bars, oldest first: one a date, unadjusted and adjusted."""
    return _rows(
        con.execute(
            """
            WITH adj AS (SELECT * FROM alpaca_bar_adjusted_lookup(?)),
            latest AS (
                SELECT * FROM alpaca_bar
                WHERE symbol IN (SELECT DISTINCT symbol FROM adj)
                QUALIFY row_number() OVER (
                    PARTITION BY symbol, date ORDER BY first_seen_at DESC, raw_id DESC
                ) = 1
            )
            SELECT a.date, a.symbol, b.open, b.high, b.low, a.close, b.volume, b.trade_count,
                   b.vwap, a.adj_close, a.split_factor, a.tr_factor, a.security_key
            FROM adj a JOIN latest b USING (symbol, date)
            ORDER BY a.date
            """,
            [symbol],
        )
    )


def universe(
    con: duckdb.DuckDBPyConnection,
    exchanges: list[str],
    *,
    status: str | None = None,
    exchange: str | None = None,
    delisted: bool | None = None,
    matched: bool | None = None,
) -> list[dict[str, Any]]:
    """The universe, one row per symbol, by symbol, filtered."""
    where, params = ["true"], []
    if status is not None:
        where.append("status = ?")
        params.append(status)
    if exchange is not None:
        where.append("exchange = ?")
        params.append(exchange)
    if delisted is not None:
        where.append("delisted = ?")
        params.append(delisted)
    if matched is not None:
        where.append(f"cik IS {'NOT ' if matched else ''}NULL")
    return _rows(
        con.execute(
            f"""
            WITH u AS (
                SELECT symbol, string_agg(source, ' ' ORDER BY source) AS sources,
                       bool_or(source = 'asset_active') AS active,
                       bool_or(source = 'asset_inactive') AS inactive
                FROM alpaca_universe GROUP BY symbol
            ),
            asset AS (
                SELECT symbol, name, exchange, status = 'active' AND valid_to IS NULL AS active,
                       valid_from, id
                FROM alpaca_asset_history
                WHERE exchange IN (SELECT unnest(?::VARCHAR[]))
                QUALIFY row_number() OVER (PARTITION BY id ORDER BY valid_from DESC) = 1
            ),
            best AS (
                SELECT * FROM asset
                QUALIFY row_number() OVER (
                    PARTITION BY symbol ORDER BY active DESC, valid_from DESC, id
                ) = 1
            ),
            bar AS (
                SELECT symbol, min(date) AS first_bar, max(date) AS last_bar,
                       count(DISTINCT date) AS bars
                FROM alpaca_bar GROUP BY symbol
            ),
            edgar AS (
                SELECT ticker_norm, min(cik) AS cik, arg_min(ticker, cik) AS ticker,
                       arg_min(title, cik) AS title
                FROM edgar_company WHERE active GROUP BY ticker_norm
            ),
            row AS (
                SELECT u.symbol,
                       CASE WHEN u.active THEN 'active' WHEN u.inactive THEN 'inactive'
                            ELSE 'none' END AS status,
                       a.exchange, a.name, u.sources, NOT u.active AS delisted,
                       b.first_bar, b.last_bar, coalesce(b.bars, 0) AS bars,
                       e.cik, e.ticker AS edgar_ticker, e.title AS edgar_title
                FROM u
                LEFT JOIN best a USING (symbol)
                LEFT JOIN bar b USING (symbol)
                LEFT JOIN edgar e ON u.active AND e.ticker_norm = ticker_norm(u.symbol)
            )
            SELECT * FROM row WHERE {" AND ".join(where)} ORDER BY symbol
            """,
            [exchanges, *params],
        )
    )


def universe_name(
    status: str | None, exchange: str | None, delisted: bool | None, matched: bool | None
) -> str:
    """CSV stem: `prices_universe[_<status>][_<exchange>][_delisted|_listed][_matched|...]`."""
    parts = ["prices_universe"]
    if status:
        parts.append(status)
    if exchange:
        parts.append(exchange)
    if delisted is not None:
        parts.append("delisted" if delisted else "listed")
    if matched is not None:
        parts.append("matched" if matched else "unmatched")
    return "_".join(parts)


def counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    found = {s: 0 for s in STATUS} | {"matched": 0}
    for r in rows:
        found[r["status"]] += 1
        found["matched"] += r["cik"] is not None
    return found
