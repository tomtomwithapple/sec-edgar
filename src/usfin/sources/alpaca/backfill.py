"""Alpaca backfills (plan alpaca-market-data: How > Corporate actions, How > Backfill; A5, A10).

`corp_actions` loads every corporate action processed from `start` (default 2016-01-01) to
today (New York), one calendar month per window on `process_date`. Each window runs in **its own
DB session** (open, archive every page, store, close), so `usfin serve` and the scheduled jobs
get the database between windows. Pages are archived as the daily job's dataset
(`alpaca_corp_actions`) and stored by the job's own `store_page`, so versions dedupe against
what the job stored and `usfin reparse alpaca_corp_actions` rebuilds both.

Progress is the stretch of finished months, watermarks `from` and `through` under the job name
`alpaca_corp_actions_backfill`, which has no `schedule.yaml` entry: no alert rule runs on it,
and the daily job's own watermark is untouched. A rerun skips the months inside the stretch and
refetches the open month (the one holding today), so a stop loses at most one window.

`bars_history` asks the whole bar history of every universe symbol never asked in full (or the
symbols given), `batch_size` symbols a request from `start` (default `backfill_start`) with no
`end`, each batch in its own DB session. One request per batch, not date stretches: the job's
`store_page` then marks the batch's symbols `history` from the archived request (and `invalid`
from an archived 400), so the marks the daily job's catch-up already reads are also this
backfill's to-do list and its resume rule, and reparse rebuilds them with the bars. A stop loses
at most the open batch (its transaction rolls back; its pages stay archived, so a reparse would
store them). A `start` after `backfill_start` asks less than the whole history and marks nothing,
so it is a refetch, not resumable. Progress (the last finished batch's last symbol, and `start`)
is under `alpaca_bars_backfill`, again with no schedule entry, so no alert rule runs on it.
"""

import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import duckdb
import httpx

from usfin import http as usfin_http
from usfin import jobs, store
from usfin.settings import Settings
from usfin.sources import alpaca
from usfin.sources.alpaca import bars
from usfin.sources.alpaca import corp_actions as ca
from usfin.sources.treasurydirect import month_windows

CORP_ACTIONS_START = date(2016, 1, 1)
"""First `process_date` the corporate-action backfill asks for (A5)."""
CORP_JOB = "alpaca_corp_actions_backfill"
"""Progress job name: no schedule entry, so no alert rules (A9)."""
CORP_KEY = "through"
"""Last day of the finished stretch; its first day is under the key `FROM_KEY`."""
FROM_KEY = "from"
BARS_JOB = "alpaca_bars_backfill"
"""Progress job name of the bars backfill: no schedule entry, so no alert rules (A9)."""
SYMBOL_KEY = "symbol"
"""Last symbol of the last finished batch; the `start` it was asked from is under `FROM_KEY`."""


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class Window:
    """One month's outcome, passed to the progress callback after its session has closed."""

    lo: date
    hi: date
    fetched: bool
    pages: int = 0
    actions: int = 0
    inserted: int = 0
    types: dict[str, int] = field(default_factory=dict)
    seconds: float = 0.0


@dataclass
class CorpBackfill:
    windows: int = 0
    fetched: int = 0
    skipped: int = 0
    pages: int = 0
    actions: int = 0
    inserted: int = 0
    types: Counter = field(default_factory=Counter)


def _merge(done: tuple[date, date] | None, lo: date, hi: date) -> tuple[date, date]:
    """The finished stretch after month `lo..hi` finished: joined when they touch, else the
    month alone (a gap would claim months never fetched)."""
    if done is not None and lo <= done[1] + timedelta(days=1) and hi >= done[0] - timedelta(days=1):
        return min(done[0], lo), max(done[1], hi)
    return lo, hi


def corp_actions(
    settings: Settings,
    *,
    start: date = CORP_ACTIONS_START,
    today: date,
    transport: httpx.BaseTransport | None = None,
    progress: Callable[[Window], None] | None = None,
) -> CorpBackfill:
    """Fetch and store every action processed `start`..`today`, month by month, resumable.

    A failing request raises (`HttpError`, `CorpActionError`, or `ConfigError` with a key
    unset, before any request); months finished before it stay stored and recorded.
    """
    with store.session(settings.db_path, wait=settings.lock_wait) as con:
        marks = jobs.get_watermarks(con, CORP_JOB)
    done = (
        (date.fromisoformat(marks[FROM_KEY]), date.fromisoformat(marks[CORP_KEY]))
        if FROM_KEY in marks and CORP_KEY in marks
        else None
    )
    stored = done
    windows = month_windows(start, today)
    result = CorpBackfill(windows=len(windows))
    for lo, hi in windows:
        if stored is not None and stored[0] <= lo and hi <= stored[1] and hi < today:
            done = _merge(done, lo, hi)
            result.skipped += 1
            if progress:
                progress(Window(lo, hi, False))
            continue
        started = time.monotonic()
        types: Counter = Counter()
        actions = inserted = 0
        if hi < today:
            done = _merge(done, lo, hi)
        with store.session(settings.db_path, wait=settings.lock_wait) as con:
            with usfin_http.Client(con, settings=settings, transport=transport) as client:
                raw_ids = ca.fetch_window(client, lo, hi)
            con.execute("BEGIN TRANSACTION")
            try:
                for raw_id in raw_ids:
                    page, n = ca.store_page(con, raw_id, raw_dir=settings.raw_dir)
                    types.update(a["type"] for a in page)
                    actions += len(page)
                    inserted += n
                if done is not None:
                    jobs.set_watermark(con, CORP_JOB, FROM_KEY, done[0], today=today)
                    jobs.set_watermark(con, CORP_JOB, CORP_KEY, done[1], today=today)
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
        result.fetched += 1
        result.pages += len(raw_ids)
        result.actions += actions
        result.inserted += inserted
        result.types.update(types)
        if progress:
            progress(
                Window(lo, hi, True, len(raw_ids), actions, inserted, dict(types),
                       time.monotonic() - started)
            )  # fmt: skip
    return result


# --- bars ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Batch:
    """One batch's outcome, passed to the progress callback after its session has closed."""

    n: int
    of: int
    symbols: tuple[str, ...]
    pages: int = 0
    """Requests archived, invalid-symbol 400s included."""
    bars: int = 0
    inserted: int = 0
    invalid: tuple[str, ...] = ()
    complete: int = 0
    """Symbols now marked `history` (asked in full, bars or none)."""
    seconds: float = 0.0


@dataclass
class BarsBackfill:
    named: int = 0
    """Symbols asked for: the distinct universe, or the `symbols` given."""
    skipped: int = 0
    """Of those, already asked in full or invalid."""
    batches: int = 0
    pages: int = 0
    bars: int = 0
    inserted: int = 0
    invalid: list[str] = field(default_factory=list)
    complete: int = 0


def bars_todo(
    con: duckdb.DuckDBPyConnection, symbols: Sequence[str] | None = None
) -> tuple[list[str], int]:
    """(symbols still to ask, how many were named): the universe's never-asked symbols in the
    daily catch-up's order, or `symbols` (once each, as given) less those already marked."""
    if symbols is None:
        named = con.execute("SELECT count(DISTINCT symbol) FROM alpaca_universe").fetchone()[0]
        return bars.unasked(con), named
    items = list(dict.fromkeys(symbols))
    marked = {
        s
        for (s,) in con.execute(
            f"SELECT DISTINCT symbol FROM {bars.SYMBOLS}"
            " WHERE symbol IN (SELECT unnest(?::VARCHAR[]))",
            [items],
        ).fetchall()
    }
    return [s for s in items if s not in marked], len(items)


def bars_history(
    settings: Settings,
    *,
    today: date,
    start: date | None = None,
    symbols: Sequence[str] | None = None,
    transport: httpx.BaseTransport | None = None,
    progress: Callable[[Batch], None] | None = None,
) -> BarsBackfill:
    """Fetch and store the bars of every symbol still to ask, a batch per DB session, resumable.

    A failing request raises (`HttpError`, `BarsError`, or `ConfigError` with a key unset,
    before any request); batches finished before it stay stored and marked.
    """
    cfg = alpaca.load_config(settings.config_dir)
    start = cfg.backfill_start if start is None else start
    with store.session(settings.db_path, wait=settings.lock_wait) as con:
        todo, named = bars_todo(con, symbols)
    chunks = bars.batches(todo, cfg.batch_size)
    result = BarsBackfill(named=named, skipped=named - len(todo), batches=len(chunks))
    for n, batch in enumerate(chunks, 1):
        started = time.monotonic()
        count = inserted = 0
        with store.session(settings.db_path, wait=settings.lock_wait) as con:
            with usfin_http.Client(con, settings=settings, transport=transport) as client:
                raw_ids, invalid = bars.fetch_batch(client, batch, start)
            con.execute("BEGIN TRANSACTION")
            try:
                for raw_id in raw_ids:
                    page, k = bars.store_page(
                        con, raw_id, raw_dir=settings.raw_dir, backfill_start=cfg.backfill_start
                    )
                    count += len(page)
                    inserted += k
                complete = con.execute(
                    f"SELECT count(*) FROM {bars.SYMBOLS} WHERE status = ?"
                    " AND symbol IN (SELECT unnest(?::VARCHAR[]))",
                    [bars.HISTORY_STATUS, batch],
                ).fetchone()[0]
                jobs.set_watermark(con, BARS_JOB, FROM_KEY, start, today=today)
                jobs.set_watermark(con, BARS_JOB, SYMBOL_KEY, batch[-1], today=today)
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
        result.pages += len(raw_ids)
        result.bars += count
        result.inserted += inserted
        result.invalid += invalid
        result.complete += complete
        if progress:
            progress(
                Batch(n, len(chunks), tuple(batch), len(raw_ids), count, inserted,
                      tuple(invalid), complete, time.monotonic() - started)
            )  # fmt: skip
    return result
