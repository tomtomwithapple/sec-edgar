"""Scheduler for `usfin serve`: cron fires from `config/schedule.yaml`, calendar guard, catch-up.

Each job with a `cron` in `schedule.yaml` (`m h dom mon dow`, America/New_York) becomes an
APScheduler `CronTrigger` job. Every run, scheduled or queued, goes through one worker
(`MAX_WORKERS = 1`, `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600`), so writes
never race. A scheduled fire on a day outside the job's calendar records a `skipped` run.

Run-now and catch-up never run in the caller's thread: they queue a one-shot job on the same
worker. On startup `catchup()` queues each job once (trigger `catchup`) whose most recent fire on
a calendar day is later than its last `ok` run's `started_at`; a fire APScheduler reports missed
(past its grace time, e.g. after the Mac slept) is caught up the same way.

`db_locked`: a run that cannot open the DB (another process held it past the lock wait) cannot
write `job_run` or `alert`. The scheduler keeps it in `pending_locked` (in memory) and stores one
`db_locked` alert per job and New York day of the attempt (subject = job) as soon as a session
opens: before the next run, or from a retry every `FLUSH_RETRY` until the DB is free.

Retry: after every run of a job with a `retry` hook (BLS: a release day whose new period is
missing), the hook says when to run it again today; that becomes a one-shot `DateTrigger` job id
`<job>_retry` (`replace_existing=True`) on the same worker, whose run is `scheduled` and asks the
hook again. A None answer cancels a pending retry (the hook raises its own alerts when it gives
up). The retry job re-adds itself while APScheduler removes the fired one, but both happen under
the scheduler's job-store lock, so the new one is never lost.
"""

import itertools
import json
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import yaml
from apscheduler.events import EVENT_JOB_MISSED, JobExecutionEvent
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from usfin import alerts, calendars, jobs, store
from usfin.settings import ConfigError, Settings

ET = ZoneInfo("America/New_York")
MAX_WORKERS = 1
MISFIRE_GRACE = 3600
CATCHUP_LOOKBACK = timedelta(days=14)
FLUSH_WAIT = 5.0
FLUSH_RETRY = timedelta(minutes=5)
log = logging.getLogger("usfin.scheduler")


# --- schedule.yaml --------------------------------------------------------------------------


@dataclass(frozen=True)
class Entry:
    job: str
    cron: str
    calendar: str

    def trigger(self) -> CronTrigger:
        return CronTrigger.from_crontab(self.cron, timezone=ET)


def load_schedule(config_dir: Path | str) -> dict[str, Entry]:
    """Jobs in `<config_dir>/schedule.yaml` that have a `cron`, by name."""
    path = Path(config_dir) / "schedule.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    out: dict[str, Entry] = {}
    for job, spec in (data.get("jobs") or {}).items():
        spec = spec or {}
        where = f"{path}: {job}"
        cron = spec.get("cron")
        if cron is None:
            continue
        calendar = spec.get("calendar")
        if calendar not in calendars.CALENDARS:
            raise ConfigError(
                f"{where}: calendar must be one of {', '.join(calendars.CALENDARS)},"
                f" got {calendar!r}"
            )
        fields = str(cron).split()
        if len(fields) != 5:
            raise ConfigError(f"{where}: cron needs 5 fields (m h dom mon dow), got {cron!r}")
        if any(ch.isdigit() for ch in fields[4]):
            # APScheduler 3 numbers days of the week from Monday = 0 (cron: Sunday = 0)
            raise ConfigError(
                f"{where}: cron day-of-week must use names (mon..sun), got {fields[4]!r}"
            )
        entry = Entry(str(job), str(cron), calendar)
        try:
            entry.trigger()
        except ValueError as exc:
            raise ConfigError(f"{where}: bad cron {cron!r}: {exc}") from None
        out[entry.job] = entry
    return out


def _fires(trigger: CronTrigger, start: datetime, end: datetime | None, limit: int):
    """Fire times at or after `start` (and at or before `end`), at most `limit`."""
    fire = trigger.get_next_fire_time(None, start)
    for _ in range(limit):
        if fire is None or (end is not None and fire > end):
            return
        yield fire
        fire = trigger.get_next_fire_time(fire, fire + timedelta(seconds=1))


def next_fires(
    entry: Entry, cals: calendars.Calendars, after: datetime, n: int = 5
) -> list[datetime]:
    """The next `n` fires after `after` that fall on the entry's calendar days."""
    out = []
    for fire in _fires(entry.trigger(), after + timedelta(seconds=1), None, 100_000):
        if cals.is_day(entry.calendar, fire.astimezone(ET).date()):
            out.append(fire)
            if len(out) == n:
                break
    return out


def last_due_fire(
    entry: Entry, cals: calendars.Calendars, now: datetime, lookback: timedelta = CATCHUP_LOOKBACK
) -> datetime | None:
    """The most recent fire at or before `now` on a calendar day (None within `lookback`)."""
    last = None
    for fire in _fires(entry.trigger(), now - lookback, now, 100_000):
        if cals.is_day(entry.calendar, fire.astimezone(ET).date()):
            last = fire
    return last


# --- the scheduler --------------------------------------------------------------------------


def _utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


@dataclass(frozen=True)
class Locked:
    """A run that could not open the DB; becomes a `db_locked` alert once the DB is free."""

    job: str
    trigger: str
    at: datetime
    error: str


class Scheduler:
    def __init__(
        self,
        settings: Settings,
        registry: jobs.Registry,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        wait: float | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self.registry = registry
        self.clock = clock
        self.wait = settings.lock_wait if wait is None else wait
        self.transport = transport
        self.cals = calendars.load_calendars(settings.config_dir)
        self.entries: dict[str, Entry] = {}
        for name, entry in load_schedule(settings.config_dir).items():
            if name in registry:
                self.entries[name] = entry
            else:
                log.warning("schedule.yaml: job %s is not registered; not scheduled", name)
        self.pending_locked: list[Locked] = []
        self._cond = threading.Condition()
        self._queued = 0
        self._waiting: set[tuple[str, str]] = set()
        self._ids = itertools.count(1)
        self.aps = BackgroundScheduler(
            timezone=ET,
            executors={"default": ThreadPoolExecutor(MAX_WORKERS)},
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": MISFIRE_GRACE,
            },
        )
        for name, entry in self.entries.items():
            self.aps.add_job(self.fire, entry.trigger(), args=[name], id=name, name=name)
        self.aps.add_listener(self.on_missed, EVENT_JOB_MISSED)

    # lifecycle

    def start(self, *, paused: bool = False) -> None:
        self.aps.start(paused=paused)

    def shutdown(self) -> None:
        if self.aps.running:
            self.aps.shutdown(wait=True)

    @property
    def running(self) -> bool:
        return self.aps.running

    # runs

    def fire(self, name: str) -> jobs.RunResult:
        """The cron target: run `name` (trigger `scheduled`) if today is a calendar day."""
        return self._run(name, "scheduled")

    def run_now(self, name: str, trigger: str = "manual") -> bool:
        """Queue `name` on the worker now; False if the same run is already waiting."""
        self.registry.get(name)  # UnknownJob before anything is queued
        with self._cond:
            if (name, trigger) in self._waiting:
                return False
            self._waiting.add((name, trigger))
            self._queued += 1
        self.aps.add_job(
            self._queued_run,
            "date",
            run_date=datetime.now(ET),
            args=[name, trigger],
            id=f"{trigger}:{name}:{next(self._ids)}",
            name=f"{name} ({trigger})",
            misfire_grace_time=None,
        )
        return True

    def wait_idle(self, timeout: float | None = None) -> bool:
        """Wait until every queued run (run-now, catch-up) has finished."""
        with self._cond:
            return self._cond.wait_for(lambda: self._queued == 0, timeout)

    def _queued_run(self, name: str, trigger: str) -> jobs.RunResult | None:
        with self._cond:
            self._waiting.discard((name, trigger))
        try:
            return self._run(name, trigger)
        except Exception:
            log.exception("job %s (%s) raised", name, trigger)
            return None
        finally:
            with self._cond:
                self._queued -= 1
                self._cond.notify_all()

    def _run(self, name: str, trigger: str) -> jobs.RunResult:
        now = self.clock()
        self.flush_locked()
        if trigger == "scheduled":
            entry = self.entries.get(name)
            calendar = entry.calendar if entry else "any"
            day = now.astimezone(ET).date()
            if not self.cals.is_day(calendar, day):
                return self._skip(name, f"not a {calendar} day ({day.isoformat()})", now)
        result = jobs.run_job(
            name,
            trigger=trigger,
            settings=self.settings,
            registry=self.registry,
            clock=self.clock,
            wait=self.wait,
            transport=self.transport,
        )
        self._note_locked(result, trigger, now)
        if result.run_id is not None:
            self._plan_retry(name)
        return result

    # retry

    def retry(self, name: str) -> jobs.RunResult:
        """The `<job>_retry` target: run `name` again (trigger `scheduled`)."""
        return self._run(name, "scheduled")

    def _plan_retry(self, name: str) -> datetime | None:
        """Ask the job's `retry` hook; (re)schedule `<name>_retry` or cancel it."""
        hook = self.registry.get(name).retry
        if hook is None:
            return None
        retry_id = f"{name}_retry"
        try:
            with store.session(self.settings.db_path, wait=self.wait) as con:
                first = alerts.max_id(con)
                at = hook(con, self.settings, self.clock())
                alerts.notify(con, self.settings, since_id=first)
        except store.DatabaseLocked as exc:
            log.error("job %s: retry not planned, database busy (%s)", name, exc)
            return None
        if at is None:
            if self.aps.get_job(retry_id) is not None:
                self.aps.remove_job(retry_id)
            return None
        log.info("job %s: retry at %s", name, at.astimezone(ET))
        self.aps.add_job(
            self.retry,
            "date",
            run_date=at,
            args=[name],
            id=retry_id,
            name=f"{name} (retry)",
            replace_existing=True,
            misfire_grace_time=MISFIRE_GRACE,
        )
        return at

    def _skip(self, name: str, reason: str, now: datetime) -> jobs.RunResult:
        try:
            with store.session(self.settings.db_path, wait=self.wait) as con:
                before = jobs.get_watermarks(con, name)
                run_id = con.execute(
                    "INSERT INTO job_run (job, started_at, finished_at, status, trigger,"
                    " watermark_before, watermark_after, error)"
                    " VALUES (?, ?, ?, 'skipped', 'scheduled', ?, ?, ?) RETURNING id",
                    [
                        name,
                        _utc_naive(now),
                        _utc_naive(now),
                        json.dumps(before),
                        json.dumps(before),
                        reason,
                    ],
                ).fetchone()[0]
        except store.DatabaseLocked as exc:
            error = f"database busy — another process holds the lock ({exc})"
            log.error("job %s skip not logged: %s", name, error)
            result = jobs.RunResult(name, None, "skipped", 0, reason, locked=True)
            self._note_locked(result, "scheduled", now, error=error)
            return result
        log.info("job %s skipped: %s", name, reason)
        return jobs.RunResult(name, run_id, "skipped", 0, reason)

    # catch-up

    def catchup(self) -> list[str]:
        """Queue each job once whose most recent due fire is later than its last ok run."""
        now = self.clock()
        try:
            with store.session(self.settings.db_path, wait=self.wait) as con:
                last_ok = dict(
                    con.execute(
                        "SELECT job, max(started_at) FROM job_run WHERE status = 'ok' GROUP BY job"
                    ).fetchall()
                )
        except store.DatabaseLocked as exc:
            log.error("catch-up not run: database busy (%s)", exc)
            return []
        queued = []
        for name, entry in sorted(self.entries.items()):
            fire = last_due_fire(entry, self.cals, now)
            if fire is None:
                continue
            ok = last_ok.get(name)
            if ok is None or _utc_naive(fire) > ok:
                log.info("catch-up %s: due fire %s, last ok run %s", name, fire, ok)
                self.run_now(name, "catchup")
                queued.append(name)
        return queued

    def on_missed(self, event: JobExecutionEvent) -> None:
        """A cron fire missed past its grace time (e.g. asleep): catch it up if it was due."""
        entry = self.entries.get(event.job_id)
        if entry is None:
            return
        fire = event.scheduled_run_time
        if self.cals.is_day(entry.calendar, fire.astimezone(ET).date()):
            log.warning("job %s missed its %s fire; catching up", entry.job, fire)
            self.run_now(entry.job, "catchup")

    # db_locked

    def _note_locked(
        self, result: jobs.RunResult, trigger: str, at: datetime, error: str | None = None
    ) -> None:
        found = []

        def walk(r: jobs.RunResult, trig: str) -> None:
            if r.locked:
                found.append(Locked(r.job, trig, at, error or r.error or "database busy"))
            for child in r.chained:
                walk(child, "chained")

        walk(result, trigger)
        if not found:
            return
        with self._cond:
            self.pending_locked.extend(found)
        if self.aps.running:
            self.aps.add_job(
                self._retry_flush,
                "date",
                run_date=datetime.now(ET) + FLUSH_RETRY,
                id="db_locked_flush",
                replace_existing=True,
                misfire_grace_time=None,
            )

    def _retry_flush(self) -> None:
        self.flush_locked()
        if self.pending_locked and self.aps.running:
            self.aps.add_job(
                self._retry_flush,
                "date",
                run_date=datetime.now(ET) + FLUSH_RETRY,
                id="db_locked_flush",
                replace_existing=True,
                misfire_grace_time=None,
            )

    def flush_locked(self) -> int:
        """Store a `db_locked` alert for each pending locked run; return how many were new."""
        with self._cond:
            pending = list(self.pending_locked)
        if not pending:
            return 0
        stored = 0
        try:
            with store.session(self.settings.db_path, wait=min(FLUSH_WAIT, self.wait)) as con:
                first = alerts.max_id(con)
                for item in pending:
                    detail: dict[str, Any] = {
                        "trigger": item.trigger,
                        "at": _utc_naive(item.at).isoformat(),
                        "error": item.error,
                    }
                    new = alerts.raise_alert(
                        con,
                        job=item.job,
                        rule="db_locked",
                        subject=item.job,
                        detail=detail,
                        now=item.at,
                    )
                    stored += new is not None
                alerts.notify(con, self.settings, since_id=first)
        except store.DatabaseLocked:
            log.warning("db_locked alerts pending (%d): database still busy", len(pending))
            return 0
        with self._cond:
            for item in pending:
                self.pending_locked.remove(item)
        return stored
