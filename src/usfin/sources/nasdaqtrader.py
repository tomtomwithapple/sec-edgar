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
The diff into `symbol_history` and its ±2% gate are S30.
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
        known = ctx.watermarks.get(file)
        if known is None or snap.snapshot_at.isoformat() > known:  # never back to an older file
            ctx.advance(file, snap.snapshot_at)
        return inserted

    return parse_file


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="symbols",
        keys=FILES,
        fetch=_fetch,
        parsers={raw_dataset(f): _parser(f) for f in FILES},
        observed={
            f: alerts.Observed(TABLE, "snapshot_at", match={"file": [f]}, snapshot=True)
            for f in FILES
        },
    )
)
"""Nasdaq Trader's symbol directory into `symbol_snapshot`; 07:00 ET on NYSE days."""
