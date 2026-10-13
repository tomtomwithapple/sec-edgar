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
"""

import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import httpx

from usfin import http as usfin_http
from usfin import jobs, store
from usfin.settings import Settings
from usfin.sources.alpaca import corp_actions as ca
from usfin.sources.treasurydirect import month_windows

CORP_ACTIONS_START = date(2016, 1, 1)
"""First `process_date` the corporate-action backfill asks for (A5)."""
CORP_JOB = "alpaca_corp_actions_backfill"
"""Progress job name: no schedule entry, so no alert rules (A9)."""
CORP_KEY = "through"
"""Last day of the finished stretch; its first day is under the key `FROM_KEY`."""
FROM_KEY = "from"


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
