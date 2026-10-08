"""Job `symbols`: Nasdaq Trader's symbol directory into table `symbol_snapshot` (C2, C9).

Two files, `URL` with `{file}` = `nasdaqlisted` or `otherlisted` (fetched 2026-10-08: 349 272
and 542 931 bytes, `text/plain`, ASCII, CRLF, 5 627 and 7 663 rows, created 03:03 ET). Each is
pipe-delimited: a header line, one line per security, and a trailer
`File Creation Time: mmddyyyyhh:mm|||…` (New York time, padded with one `|` per other column).
The header differs per file (`COLUMNS`): nasdaqlisted is `Symbol|Security Name|Market Category|
Test Issue|Financial Status|Round Lot Size|ETF|NextShares` (all Nasdaq-listed, so exchange `Q`);
otherlisted is `ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|
NASDAQ Symbol` (exchanges A, N, P, Z, V, M). otherlisted spells classes and suffixes with `.`
and preferreds with `$` (`BRK.B`, `AAC.U` unit, `AAC.W` warrant, `AIIA.R` right, `ABR$D`), its
CQS symbol `AAC.WS`/`ABRpD` and its NASDAQ symbol `AAC+`/`ABR-D`; symbols are stored as written.

Every response is archived first (source `nasdaqtrader`, raw datasets `nasdaq_<file>`). A body
without the trailer (cut short) is rejected: the key fails, a `snapshot_rejected` alert is raised
and nothing is stored; reparse skips such a body. Test issues (`Test Issue = Y`: 8 + 29 on
2026-10-08) are dropped. Rows go to `symbol_snapshot` keyed (file, snapshot_at, symbol), so the
same file fetched again inserts nothing. Each file's watermark is its newest `snapshot_at`.

Each new snapshot is then diffed into the type-2 table `symbol_history` (`sync_history`), one
row per (symbol, exchange) version with `valid_from` / `valid_to` = the `snapshot_at` of the
snapshots that opened / closed it. A new symbol opens a row, a gone one is closed, and a change
in any `TRACKED` column (name, ETF flag, market category, financial status) closes the row and
opens another at the same time; round lot (price-driven) and the CQS / NASDAQ spellings are
not tracked. A symbol changing exchange or file is a close under the old exchange plus an open
under the new one. The gate (C9): a snapshot whose row count is more than the job's
`row_count_swing` pct (2) away from the file's previous snapshot is stored but not diffed
(`symbol_diff.diffed = false`; the alert rule raises `row_count_swing`); the next snapshot
within the gate of its predecessor diffs against the open rows, i.e. the last diffed snapshot.
The first snapshot has no check. History is a function of the stored snapshots in
`snapshot_at` order: reparse rebuilds each file's history from scratch, and a snapshot older
than one already diffed rebuilds that file too.
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb

from usfin import alerts, jobs, raw

URL = "https://www.nasdaqtrader.com/dynamic/SymDir/{file}.txt"
SOURCE = "nasdaqtrader"
FILES = ("nasdaqlisted", "otherlisted")
TABLE = "symbol_snapshot"
HISTORY = "symbol_history"
DIFFS = "symbol_diff"
TRACKED = ("name", "etf", "market_category", "financial_status")
"""Columns whose change closes a history row and opens another (besides symbol and exchange)."""
SWING_PCT = 2.0
"""The gate when the job's schedule.yaml entry sets no `row_count_swing` pct for the file."""
JOB_NAME = "symbols"
TRAILER = "File Creation Time:"
ET = ZoneInfo("America/New_York")
log = logging.getLogger("usfin.sources.nasdaqtrader")

COLUMNS: dict[str, dict[str, str]] = {
    # stored column -> header name in that file
    "nasdaqlisted": {
        "symbol": "Symbol",
        "name": "Security Name",
        "market_category": "Market Category",
        "test_issue": "Test Issue",
        "financial_status": "Financial Status",
        "round_lot": "Round Lot Size",
        "etf": "ETF",
    },
    "otherlisted": {
        "symbol": "ACT Symbol",
        "name": "Security Name",
        "exchange": "Exchange",
        "cqs_symbol": "CQS Symbol",
        "etf": "ETF",
        "round_lot": "Round Lot Size",
        "test_issue": "Test Issue",
        "nasdaq_symbol": "NASDAQ Symbol",
    },
}
NASDAQ_EXCHANGE = "Q"
"""nasdaqlisted has no Exchange column: every row is Nasdaq-listed."""


def url(file: str) -> str:
    return URL.format(file=file)


def raw_dataset(file: str) -> str:
    return f"nasdaq_{file}"


class SnapshotError(Exception):
    """A body that is not the symbol directory file it should be."""


class SnapshotRejected(SnapshotError):
    """A file without its `File Creation Time` trailer (cut short): never stored (C9)."""


@dataclass(frozen=True)
class Row:
    symbol: str
    exchange: str
    name: str
    etf: bool | None
    round_lot: int | None
    financial_status: str | None
    market_category: str | None
    cqs_symbol: str | None
    nasdaq_symbol: str | None


@dataclass(frozen=True)
class Snapshot:
    file: str
    snapshot_at: datetime
    """The trailer's New York time as naive UTC."""
    rows: list[Row]
    test_issues: int


def _flag(value: str) -> bool | None:
    return {"Y": True, "N": False}.get(value)


def _trailer_time(line: str) -> datetime:
    stamp = line[len(TRAILER) :].split("|", 1)[0].strip()
    try:
        local = datetime.strptime(stamp, "%m%d%Y%H:%M").replace(tzinfo=ET)
    except ValueError as exc:
        raise SnapshotRejected(f"unreadable {TRAILER!r} trailer {line!r}") from exc
    return local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def parse(body: bytes, file: str) -> Snapshot:
    """The file's rows (test issues dropped) and its creation time; `SnapshotRejected` when the
    trailer is missing, `SnapshotError` when the header lacks a mapped column."""
    if file not in COLUMNS:
        raise ValueError(f"unknown Nasdaq Trader file {file!r} (files: {', '.join(FILES)})")
    lines = [line for line in body.decode("ascii").splitlines() if line.strip()]
    if not lines or not lines[-1].startswith(TRAILER):
        raise SnapshotRejected(
            f"{file}.txt has no {TRAILER!r} trailer ({len(body)} bytes; last line"
            f" {lines[-1][:60] if lines else ''!r}); not stored"
        )
    snapshot_at = _trailer_time(lines[-1])
    header = lines[0].split("|")
    mapping = COLUMNS[file]
    missing = [h for h in mapping.values() if h not in header]
    if missing:
        raise SnapshotError(f"{file}.txt header {header!r} lacks {', '.join(missing)}")
    idx = {col: header.index(h) for col, h in mapping.items()}
    rows: dict[str, Row] = {}
    test_issues = 0
    for n, line in enumerate(lines[1:-1], start=2):
        cells = line.split("|")
        if len(cells) != len(header):
            raise SnapshotError(f"{file}.txt line {n} has {len(cells)} fields, not {len(header)}")

        def get(col: str, cells: list[str] = cells) -> str | None:
            return (cells[idx[col]].strip() or None) if col in idx else None

        if get("test_issue") == "Y":
            test_issues += 1
            continue
        symbol = get("symbol")
        if symbol is None:
            raise SnapshotError(f"{file}.txt line {n} has no symbol")
        exchange = get("exchange")
        if "exchange" in idx and exchange is None:
            raise SnapshotError(f"{file}.txt line {n} ({symbol}) has no exchange")
        lot = get("round_lot")
        rows.setdefault(
            symbol,
            Row(
                symbol=symbol,
                exchange=NASDAQ_EXCHANGE if "exchange" not in idx else exchange,
                name=cells[idx["name"]].strip(),
                etf=_flag(get("etf") or ""),
                round_lot=int(lot) if lot else None,
                financial_status=get("financial_status"),
                market_category=get("market_category"),
                cqs_symbol=get("cqs_symbol"),
                nasdaq_symbol=get("nasdaq_symbol"),
            ),
        )
    return Snapshot(file, snapshot_at, list(rows.values()), test_issues)


def store_snapshot(
    con: duckdb.DuckDBPyConnection, raw_id: int, file: str, *, raw_dir
) -> tuple[Snapshot | None, int]:
    """Insert the response's snapshot; return it and the rows inserted (0 when that snapshot was
    already stored). A non-200 response stores nothing; a rejected body raises."""
    found = con.execute("SELECT http_status FROM raw_response WHERE id = ?", [raw_id]).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    if found[0] != 200:
        return None, 0
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        snap = parse(fh.read(), file)
    before = _count(con, file, snap.snapshot_at)
    if snap.rows:
        values = [
            [r.symbol, r.exchange, r.name, r.etf, r.round_lot, r.financial_status,
             r.market_category, r.cqs_symbol, r.nasdaq_symbol]
            for r in snap.rows
        ]  # fmt: skip
        # one JSON text parameter, not bound row values (see fiscal.upsert_page)
        con.execute(
            f"INSERT INTO {TABLE} (file, snapshot_at, symbol, exchange, name, etf, round_lot,"
            " financial_status, market_category, cqs_symbol, nasdaq_symbol, raw_id)"
            " SELECT ?, ?, r[1], r[2], r[3], r[4]::BOOLEAN, r[5]::INTEGER, r[6], r[7], r[8],"
            " r[9], ?"
            """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)"""
            " ON CONFLICT DO NOTHING",
            [file, snap.snapshot_at, raw_id, json.dumps(values)],
        )
    return snap, _count(con, file, snap.snapshot_at) - before


def _count(con: duckdb.DuckDBPyConnection, file: str, at: datetime) -> int:
    return con.execute(
        f"SELECT count(*) FROM {TABLE} WHERE file = ? AND snapshot_at = ?", [file, at]
    ).fetchone()[0]


# --- history -------------------------------------------------------------------------------


@dataclass(frozen=True)
class Diff:
    file: str
    snapshot_at: datetime
    rows: int
    previous_at: datetime | None
    previous_rows: int | None
    pct_change: float | None
    diffed: bool
    opened: int
    closed: int


def swing_pct(settings, file: str) -> float:
    """The file's gate: the job's `row_count_swing` pct in schedule.yaml, else `SWING_PCT`."""
    rules = alerts.load_rules(settings.config_dir).get(JOB_NAME)
    params = rules.params_for(file).get("row_count_swing") if rules else None
    return float(params["pct"]) if params else SWING_PCT


def _diff_one(con: duckdb.DuckDBPyConnection, file: str, at: datetime, pct: float) -> Diff:
    rows = _count(con, file, at)
    prev = con.execute(
        f"SELECT snapshot_at, count(*) FROM {TABLE} WHERE file = ? AND snapshot_at < ?"
        " GROUP BY 1 ORDER BY 1 DESC LIMIT 1",
        [file, at],
    ).fetchone()
    prev_at, prev_rows = prev if prev else (None, None)
    change = None
    if prev_rows is not None:
        change = (rows - prev_rows) / prev_rows * 100 if prev_rows else float("inf")
    opened = closed = 0
    diffed = change is None or abs(change) <= pct
    if diffed:
        same = " AND ".join(f"s.{c} IS NOT DISTINCT FROM h.{c}" for c in TRACKED)
        closed = con.execute(
            f"UPDATE {HISTORY} h SET valid_to = ? WHERE h.file = ? AND h.valid_to IS NULL"
            f" AND NOT EXISTS (SELECT 1 FROM {TABLE} s WHERE s.file = h.file"
            " AND s.snapshot_at = ? AND s.symbol = h.symbol AND s.exchange = h.exchange"
            f" AND {same})",
            [at, file, at],
        ).fetchone()[0]
        opened = con.execute(
            f"INSERT INTO {HISTORY} (symbol, exchange, valid_from, valid_to, file, "
            f"{', '.join(TRACKED)}) SELECT s.symbol, s.exchange, ?, NULL, s.file, "
            f"{', '.join('s.' + c for c in TRACKED)} FROM {TABLE} s"
            " WHERE s.file = ? AND s.snapshot_at = ?"
            f" AND NOT EXISTS (SELECT 1 FROM {HISTORY} h WHERE h.valid_to IS NULL"
            " AND h.file = s.file AND h.symbol = s.symbol AND h.exchange = s.exchange)",
            [at, file, at],
        ).fetchone()[0]
    diff = Diff(file, at, rows, prev_at, prev_rows, change, diffed, opened, closed)
    con.execute(
        f"INSERT INTO {DIFFS} (file, snapshot_at, rows, previous_at, previous_rows, pct_change,"
        " diffed, opened, closed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [file, at, rows, prev_at, prev_rows, change, diffed, opened, closed],
    )
    return diff


def sync_history(
    con: duckdb.DuckDBPyConnection, file: str, *, pct: float = SWING_PCT, rebuild: bool = False
) -> list[Diff]:
    """Diff every stored snapshot of `file` not yet in `symbol_diff`, oldest first. With
    `rebuild` (or when such a snapshot is older than one already diffed) the file's history and
    diff rows are dropped and every stored snapshot is diffed again in order."""
    done = con.execute(f"SELECT max(snapshot_at) FROM {DIFFS} WHERE file = ?", [file]).fetchone()
    pending = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT snapshot_at FROM {TABLE} WHERE file = ? AND snapshot_at NOT IN"
            f" (SELECT snapshot_at FROM {DIFFS} WHERE file = ?) ORDER BY 1",
            [file, file],
        ).fetchall()
    ]
    if not rebuild and pending and done[0] is not None and pending[0] < done[0]:
        log.warning("symbols %s: snapshot %s is older than %s; history rebuilt", file,
                    pending[0], done[0])  # fmt: skip
        rebuild = True
    if rebuild:
        con.execute(f"DELETE FROM {HISTORY} WHERE file = ?", [file])
        con.execute(f"DELETE FROM {DIFFS} WHERE file = ?", [file])
        pending = [
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT snapshot_at FROM {TABLE} WHERE file = ? ORDER BY 1", [file]
            ).fetchall()
        ]
    return [_diff_one(con, file, at, pct) for at in pending]


# --- join with EDGAR (S31, C10) --------------------------------------------------------------

MATCH = "symbol_edgar_match"
"""View (migration 019): each open `symbol_history` row with the active `edgar_company` pair of
the same `ticker_norm`; `cik` NULL = unmatched. Unmatched symbols stay in it, never dropped."""


@dataclass(frozen=True)
class MatchRate:
    exchange: str
    etf: bool
    symbols: int
    matched: int


def match_rates(con: duckdb.DuckDBPyConnection) -> list[MatchRate]:
    """Open symbols and how many of them meet an EDGAR company, per (exchange, ETF flag)."""
    rows = con.execute(
        f"SELECT exchange, coalesce(etf, false), count(DISTINCT symbol),"
        f" count(DISTINCT symbol) FILTER (WHERE cik IS NOT NULL)"
        f" FROM {MATCH} GROUP BY ALL ORDER BY ALL"
    ).fetchall()
    return [MatchRate(*r) for r in rows]


def unmatched(
    con: duckdb.DuckDBPyConnection, *, etf: bool | None = None, exchange: str | None = None
) -> list[tuple[str, str, bool, str, str]]:
    """(symbol, exchange, etf, ticker_norm, name) of every open symbol no active EDGAR company
    shares a `ticker_norm` with, by symbol; optionally only ETFs / non-ETFs or one exchange."""
    where, params = ["cik IS NULL"], []
    if etf is not None:
        where.append("coalesce(etf, false) = ?")
        params.append(etf)
    if exchange is not None:
        where.append("exchange = ?")
        params.append(exchange.upper())
    return con.execute(
        f"SELECT symbol, exchange, coalesce(etf, false), ticker_norm, name FROM {MATCH}"
        f" WHERE {' AND '.join(where)} ORDER BY symbol, exchange",
        params,
    ).fetchall()


# --- job -------------------------------------------------------------------------------------


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    raw_id, response = ctx.client.get(url(key), source=SOURCE, dataset=raw_dataset(key))
    if response.status_code != 200:
        raise SnapshotError(f"{url(key)} answered {response.status_code}")
    try:
        parse(response.content, key)
    except SnapshotRejected as exc:
        alerts.raise_alert(
            ctx.con, job=ctx.job.name, rule="snapshot_rejected", subject=key,
            detail={"raw_id": raw_id, "bytes": len(response.content), "reason": str(exc)},
            now=ctx.now,
        )  # fmt: skip
        raise
    return [raw_id]


def _parser(file: str):
    def parse_file(ctx: "jobs.JobContext", raw_id: int) -> int:
        try:
            snap, inserted = store_snapshot(ctx.con, raw_id, file, raw_dir=ctx.settings.raw_dir)
        except SnapshotRejected as exc:  # only on reparse: the run rejected it in fetch
            log.warning("symbols: raw_response %s skipped: %s", raw_id, exc)
            return 0
        if snap is None:
            return 0
        log.info(
            "symbols %s: snapshot %s, %s rows (%s new), %s test issues dropped",
            file, snap.snapshot_at, len(snap.rows), inserted, snap.test_issues,
        )  # fmt: skip
        rebuilt = ctx.memo.setdefault("history_rebuilt", set())
        rebuild = ctx.trigger == "reparse" and file not in rebuilt  # from scratch, once per file
        rebuilt.add(file)
        pct = swing_pct(ctx.settings, file)
        for d in sync_history(ctx.con, file, pct=pct, rebuild=rebuild):
            if d.diffed:
                log.info("symbols %s: snapshot %s diffed: %s opened, %s closed",
                         file, d.snapshot_at, d.opened, d.closed)  # fmt: skip
            else:
                log.warning(
                    "symbols %s: snapshot %s not diffed: %s rows vs %s at %s (%+.2f %%, gate %s %%)",
                    file, d.snapshot_at, d.rows, d.previous_rows, d.previous_at, d.pct_change, pct,
                )  # fmt: skip
        known = ctx.watermarks.get(file)
        if known is None or snap.snapshot_at.isoformat() > known:  # never back to an older file
            ctx.advance(file, snap.snapshot_at)
        return inserted

    return parse_file


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name=JOB_NAME,
        keys=FILES,
        fetch=_fetch,
        parsers={raw_dataset(f): _parser(f) for f in FILES},
        observed={
            f: alerts.Observed(TABLE, "snapshot_at", match={"file": [f]}, snapshot=True)
            for f in FILES
        },
    )
)
"""Nasdaq Trader's symbol directory into `symbol_snapshot` and `symbol_history`; 07:00 ET on
NYSE days."""
