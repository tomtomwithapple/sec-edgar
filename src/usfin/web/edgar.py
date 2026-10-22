"""EDGAR company page (`/edgar/{cik|ticker}`) and XBRL screen (`/screen`) queries (S33).

A company page reads only that company's rows: `xbrl_fact` has no index (S27 dropped the PK)
but was rebuilt in `cik` order and companyfacts members load one CIK at a time, so DuckDB's
per-row-group min/max (zonemaps) skip all but a few row groups for `cik = ?` — a key-facts
query over 122.7 M facts touches tens of thousands, no index needed. `edgar_filing`,
`insider_txn` and `ownership_stake` are filtered the same way.

**Key facts** use the concept map exactly as S28's `screen` does: a fact counts for a concept
when its taxonomy and unit are the concept's and its tag is in the list; periods are SEC's
`frame` (`CY2025`, `CY2025Q2`, `CY2025Q2I`), so a value on this page is the one the screen
shows for the same period. Per concept and frame the first listed tag wins, then the newest
`filed`. Per concept and kind of frame (annual, quarter, instant) the page shows the newest,
marked `old` when its year is more than a year before the newest key fact's (JPMorgan's newest
quarterly `Revenues` is CY2014Q3: it stopped tagging quarters that way, and the map's other
revenue tags are not what a bank reports).
The chart plots one concept: a duration by annual frame (`CY####`), an instant by quarter-end
(`CY####Q#I`; `CY####I` counts as Q4), each missing period a gap.

**Prices** (plan alpaca-market-data A8, S12): each actively listed bars-universe symbol
(`alpaca_universe` source `asset_active`) whose `ticker_norm` is an active EDGAR ticker of this
company — the company with the lowest CIK when several share it, as `/prices/universe?matched=`
joins — shows the latest close and a 1-year adjusted-close chart of the security it names today
(`alpaca_bar_adjusted_lookup`). Delisted securities are not joined: `edgar_company` keeps only
active pairs.
"""

from collections.abc import Sequence
from datetime import date
from typing import Any

import duckdb
from fastapi.responses import Response

from usfin.sources.edgar.concepts import ConceptMap
from usfin.ticker import ticker_norm
from usfin.web.tables import Table, csv_response

FILINGS_PAGE = 50
"""Filings per page on the company page (the CSV has all of them)."""
RECENT = 50
"""Insider transactions and 13D/G stakes shown on the page (the CSV has all of them)."""
SCREEN_ROWS = 500
"""Screen rows shown in HTML (the CSV has all of them)."""

FILING_COLUMNS = (
    "accession_no", "form", "filed_at", "accepted_at", "report_date", "primary_doc",
    "is_amendment", "source",
)  # fmt: skip
FACT_COLUMNS = (
    "concept", "kind", "period", "value", "unit", "tag", "period_start", "period_end", "form",
    "filed", "accn",
)  # fmt: skip
INSIDER_COLUMNS = (
    "filed_at", "txn_date", "owner_name", "owner_cik", "relationship", "security_title",
    "is_derivative", "txn_code", "acquired_disposed", "shares", "price", "shares_owned_after",
    "direct_indirect", "form", "accession_no", "line_no",
)  # fmt: skip
STAKE_COLUMNS = (
    "filed_at", "event_date", "form", "filer_name", "filer_cik", "percent_of_class", "shares",
    "sole_voting", "shared_voting", "person_type", "accession_no", "person_seq",
)  # fmt: skip
SCREEN_COLUMNS = (
    "rank", "cik", "name", "value", "tag", "period_start", "period_end", "form", "filed", "accn",
)  # fmt: skip
FRAME_COLUMNS = ("rank", "cik", "entity_name", "loc", "value", "period_start", "period_end", "accn")
TABLES = ("filings", "facts", "insider", "stakes", "party", "prices")
PRICE_COLUMNS = (
    "date", "symbol", "close", "adj_close", "split_factor", "tr_factor", "security_key",
)  # fmt: skip
PARTY_COLUMNS = (
    "accession_no", "form", "filed_at", "roles", "filed_under_cik", "filed_under", "source",
)  # fmt: skip
KINDS = ("annual", "quarter", "instant")


def resolve(con: duckdb.DuckDBPyConnection, key: str) -> int | None:
    """The CIK of a ticker in any notation (`brk-b`, `BRK/B` → `BRK.B`), active pairs first."""
    row = con.execute(
        "SELECT cik FROM edgar_company WHERE ticker_norm = ? ORDER BY active DESC, cik LIMIT 1",
        [ticker_norm(key)],
    ).fetchone()
    return row[0] if row else None


def company(con: duckdb.DuckDBPyConnection, cik: int) -> dict[str, Any] | None:
    """Header facts for `cik`, or None when nothing at all is stored for it."""
    tickers = con.execute(
        "SELECT ticker, title, active FROM edgar_company WHERE cik = ? ORDER BY NOT active, ticker",
        [cik],
    ).fetchall()
    n_filings, first_filed, last_filed = con.execute(
        "SELECT count(*), min(filed_at), max(filed_at) FROM edgar_filing WHERE cik = ?", [cik]
    ).fetchone()
    n_facts, last_fact = con.execute(
        "SELECT count(*), max(filed) FROM xbrl_fact WHERE cik = ?", [cik]
    ).fetchone()
    title = tickers[0][1] if tickers else None
    if title is None:
        found = con.execute(
            "SELECT issuer_name FROM insider_filing WHERE issuer_cik = ? AND issuer_name IS NOT NULL"
            " UNION ALL SELECT subject_name FROM ownership_filing"
            " WHERE subject_cik = ? AND subject_name IS NOT NULL LIMIT 1",
            [cik, cik],
        ).fetchone()
        title = found[0] if found else None
    if not (tickers or n_filings or n_facts or title):
        return None
    return {
        "cik": cik,
        "title": title,
        "tickers": [t for t, _, active in tickers if active],
        "inactive_tickers": [t for t, _, active in tickers if not active],
        "filings": n_filings,
        "first_filed": first_filed,
        "last_filed": last_filed,
        "facts": n_facts,
        "last_fact_filed": last_fact,
    }


def _dicts(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def filings(
    con: duckdb.DuckDBPyConnection, cik: int, *, limit: int | None = None, offset: int = 0
) -> list[dict[str, Any]]:
    """The company's filings (`edgar_filing.cik`), newest first."""
    sql = (
        f"SELECT {', '.join(FILING_COLUMNS)} FROM edgar_filing WHERE cik = ?"
        " ORDER BY filed_at DESC, accepted_at DESC NULLS LAST, accession_no DESC"
    )
    params: list[Any] = [cik]
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params += [limit, offset]
    return _dicts(con.execute(sql, params))


def filing_href(cik: int, accession_no: str) -> str:
    """SEC's filing index page (opened by the reader's browser; usfin sends nothing)."""
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no.replace('-', '')}/"
        f"{accession_no}-index.htm"
    )


def frame_kind(frame: str) -> str:
    if frame.endswith("I"):
        return "instant"
    return "quarter" if "Q" in frame else "annual"


def frame_key(frame: str) -> tuple[int, int]:
    """(year, quarter) of a frame period; an annual or `CY####I` frame is quarter 4."""
    body = frame.removesuffix("I")[2:]
    year, _, q = body.partition("Q")
    return int(year), int(q) if q else 4


def frame_values(
    con: duckdb.DuckDBPyConnection, cik: int, cmap: ConceptMap
) -> list[dict[str, Any]]:
    """Per concept and frame: the first listed tag the company reports, then the newest filed."""
    pref = [
        (c.name, c.taxonomy, c.unit, tag, pos)
        for c in cmap.concepts.values()
        for pos, tag in enumerate(c.tags)
    ]
    values = ", ".join("(?, ?, ?, ?, ?)" for _ in pref)
    rows = _dicts(
        con.execute(
            f"""
            WITH pref(concept, taxonomy, unit, tag, pos) AS (VALUES {values}),
            company AS (
                SELECT taxonomy, tag, unit, accn, period_start, period_end, value, form, filed,
                       frame
                FROM xbrl_fact WHERE cik = ? AND frame IS NOT NULL
            )
            SELECT p.concept, f.frame AS period, f.value, f.unit, f.tag, f.period_start,
                   f.period_end, f.form, f.filed, f.accn
            FROM company f JOIN pref p USING (taxonomy, unit, tag)
            QUALIFY row_number() OVER (
                PARTITION BY p.concept, f.frame ORDER BY p.pos, f.filed DESC, f.accn DESC) = 1
            """,
            [v for row in pref for v in row] + [cik],
        )
    )
    for r in rows:
        r["kind"] = frame_kind(r["period"])
    return rows


def key_facts(rows: Sequence[dict[str, Any]], cmap: ConceptMap) -> list[dict[str, Any]]:
    """The newest frame per concept and kind, in concept-map order."""
    newest: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        k = (r["concept"], r["kind"])
        if k not in newest or frame_key(r["period"]) > frame_key(newest[k]["period"]):
            newest[k] = r
    out = [
        newest[(name, kind)] for name in cmap.concepts for kind in KINDS if (name, kind) in newest
    ]
    latest_year = max((frame_key(r["period"])[0] for r in out), default=0)
    for r in out:
        r["stale"] = frame_key(r["period"])[0] < latest_year - 1
    return out


_QUARTER_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}


def _quarter_end(year: int, quarter: int) -> date:
    return date(year, *_QUARTER_END[quarter])


def chart_series(
    rows: Sequence[dict[str, Any]], concept: str
) -> tuple[str, list[tuple[date, float | None]]]:
    """(`annual` | `quarter-end`, points) for one concept, a None for each missing period."""
    mine = [r for r in rows if r["concept"] == concept]
    if any(r["kind"] in ("annual", "quarter") for r in mine):
        how, picked = "annual", [r for r in mine if r["kind"] == "annual"]
        by_key = {frame_key(r["period"]): r["value"] for r in picked}
        if not by_key:
            return how, []
        years = [y for y, _ in by_key]
        keys = [(y, 4) for y in range(min(years), max(years) + 1)]
    else:
        how, picked = "quarter-end", [r for r in mine if r["kind"] == "instant"]
        by_key = {}
        for r in sorted(picked, key=lambda r: not r["period"].endswith("Q4I")):
            by_key.setdefault(frame_key(r["period"]), r["value"])
        if not by_key:
            return how, []
        lo, hi = min(by_key), max(by_key)
        keys = []
        y, q = lo
        while (y, q) <= hi:
            keys.append((y, q))
            y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    return how, [(_quarter_end(y, q), by_key.get((y, q))) for y, q in keys]


_RELATIONSHIP = """
    coalesce(o.officer_title, CASE WHEN o.is_director THEN 'Director'
                                   WHEN o.is_ten_pct_owner THEN '10% owner'
                                   ELSE o.other_text END)
"""


def insider(
    con: duckdb.DuckDBPyConnection, cik: int, *, limit: int | None = None
) -> tuple[int, list[dict[str, Any]]]:
    """(count, rows): Form 4 transactions with the company as issuer (holdings left out)."""
    where = "FROM insider_txn t WHERE t.issuer_cik = ? AND NOT t.is_holding"
    total = con.execute(f"SELECT count(*) {where}", [cik]).fetchone()[0]
    sql = f"""
        SELECT ef.filed_at, t.txn_date, t.owner_name, t.owner_cik, {_RELATIONSHIP} AS relationship,
               t.security_title, t.is_derivative, t.txn_code, t.acquired_disposed, t.shares,
               t.price, t.shares_owned_after, t.direct_indirect, f.form, t.accession_no,
               t.line_no
        FROM insider_txn t
        JOIN insider_filing f USING (accession_no)
        LEFT JOIN insider_owner o ON o.accession_no = t.accession_no AND o.owner_seq = 1
        LEFT JOIN edgar_filing ef ON ef.accession_no = t.accession_no
        WHERE t.issuer_cik = ? AND NOT t.is_holding
        ORDER BY t.txn_date DESC NULLS LAST, ef.filed_at DESC NULLS LAST, t.accession_no DESC,
                 t.line_no
    """
    params: list[Any] = [cik]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return total, _dicts(con.execute(sql, params))


def stakes(
    con: duckdb.DuckDBPyConnection, cik: int, *, limit: int | None = None
) -> tuple[int, list[dict[str, Any]]]:
    """(count, rows): Schedule 13D/G reporting persons with the company as subject."""
    total = con.execute(
        "SELECT count(*) FROM ownership_stake WHERE subject_cik = ?", [cik]
    ).fetchone()[0]
    sql = (
        f"SELECT {', '.join(STAKE_COLUMNS)} FROM ownership_stake WHERE subject_cik = ?"
        " ORDER BY filed_at DESC NULLS LAST, accession_no DESC, person_seq"
    )
    params: list[Any] = [cik]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return total, _dicts(con.execute(sql, params))


def party(
    con: duckdb.DuckDBPyConnection, cik: int, *, limit: int | None = None
) -> tuple[int, list[dict[str, Any]]]:
    """(count, rows): filings stored under another company that name this one as a party
    (`edgar_filing_party`, S41) — e.g. a 13D/G an investor files about an issuer, seen from the
    investor, or a Form 4 seen from the reporting owner. Roles as stored (feed roles, or
    `index` / `submissions` from the role-less sources), comma-joined."""
    base = (
        "FROM edgar_filing_party p JOIN edgar_filing f USING (accession_no)"
        " WHERE p.cik = ? AND f.cik <> ?"
    )
    total = con.execute(
        f"SELECT count(DISTINCT accession_no) {base}", [cik, cik]
    ).fetchone()[0]
    sql = (
        "SELECT f.accession_no, f.form, f.filed_at,"
        " string_agg(DISTINCT p.role, ', ' ORDER BY p.role) AS roles,"
        " f.cik AS filed_under_cik,"
        " (SELECT arg_min(c.title, c.ticker) FROM edgar_company c WHERE c.cik = f.cik)"
        " AS filed_under, f.source"
        f" {base} GROUP BY ALL"
        " ORDER BY f.filed_at DESC, f.accession_no DESC"
    )
    params: list[Any] = [cik, cik]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return total, _dicts(con.execute(sql, params))


def csv_table(con: duckdb.DuckDBPyConnection, cik: int, which: str, cmap: ConceptMap) -> Response:
    """One of the company page's tables (`TABLES`), every row, as `edgar_<cik>_<table>.csv`."""
    name = f"edgar_{cik}_{which}"
    if which == "filings":
        table = Table(name, FILING_COLUMNS, filings(con, cik))
    elif which == "facts":
        table = Table(name, FACT_COLUMNS, key_facts(frame_values(con, cik, cmap), cmap))
    elif which == "insider":
        table = Table(name, INSIDER_COLUMNS, insider(con, cik)[1])
    elif which == "stakes":
        table = Table(name, STAKE_COLUMNS, stakes(con, cik)[1])
    elif which == "prices":
        table = Table(name, PRICE_COLUMNS, prices(con, cik))
    else:
        table = Table(name, PARTY_COLUMNS, party(con, cik)[1])
    return csv_response(table)


def price_symbols(con: duckdb.DuckDBPyConnection, cik: int) -> list[str]:
    """The actively listed Alpaca symbols that are one of the company's active EDGAR tickers."""
    return [
        r[0]
        for r in con.execute(
            """
            WITH e AS (
                SELECT ticker_norm, min(cik) AS cik FROM edgar_company WHERE active
                GROUP BY ticker_norm
            )
            SELECT DISTINCT u.symbol FROM alpaca_universe u
            JOIN e ON e.ticker_norm = ticker_norm(u.symbol)
            WHERE e.cik = ? AND u.source = 'asset_active'
            ORDER BY u.symbol
            """,
            [cik],
        ).fetchall()
    ]


def prices(con: duckdb.DuckDBPyConnection, cik: int) -> list[dict[str, Any]]:
    """The last year of bars (to each security's newest) of `price_symbols`, by symbol and date."""
    out: list[dict[str, Any]] = []
    for symbol in price_symbols(con, cik):
        out += _dicts(
            con.execute(
                f"SELECT {', '.join(PRICE_COLUMNS)} FROM alpaca_bar_adjusted_lookup(?)"
                " QUALIFY date > max(date) OVER () - INTERVAL 1 YEAR ORDER BY date",
                [symbol],
            )
        )
    return out


def num(value: Any) -> str:
    """A number for reading: thousands separators, no `.0` on whole numbers."""
    if value is None:
        return ""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return str(value)
    if float(value).is_integer():
        return f"{value:,.0f}"
    return f"{value:,.4f}".rstrip("0").rstrip(".")
