"""Jobs: registry, runner, run log (`job_run`) and per-key watermarks.

A `Job` names its watermark keys, a `fetch(ctx, key) -> [raw_id]` that stores raw responses
(through `ctx.client` or `raw.save`) and a `parsers` map `{dataset: parse(ctx, raw_id) -> rows}`.
The runner opens a `job_run` row (`running`), loads the job's watermarks, and for each key
fetches, parses every raw response in its own transaction, then writes the key's new watermark
(proposed with `ctx.advance(key, value)`) only if the whole key succeeded. A failing key is
recorded and the next key runs; the run closes `ok`, or `failed` with every key's error.
Watermark dates are capped at today (America/New_York). The runner never consults a calendar:
manual runs ignore it and the scheduler guards scheduled ones.

A derived job (`compute`, e.g. `auction_xcheck`) reads tables other jobs filled instead of
fetching: its `compute(ctx, key)` runs per key in its own transaction and returns rows changed.

A job's `after_key(ctx, key)` runs after a key fully succeeded, in its own transaction (never on
reparse): bookkeeping such as closing the key's `pending_doc` rows.

A job's `keys` or `fetch` may raise `JobSkipped` (job `edgar_bulk` on low disk): the run stops
there and records `skipped` with the reason, unless an earlier key failed.

SEC jobs (named `edgar_*`) record `skipped`, with no request, while `http.SEC_BLOCK` is open (the
10 minutes after a SEC 403); the run that met the 403 records `failed` and runs no further key.

After an `ok` run, each job in `then` runs in turn (its own `job_run`, trigger `chained`, and
its own chain); a failed run chains nothing, and a job runs at most once per chain.
After every recorded run (ok or failed) the job's alert rules run (`usfin.alerts`) and new
alerts go to the webhook; a rule error is logged and never changes the run's status.

`reparse` rebuilds a job's normalized rows from stored raw responses with HTTP disabled: every
unpruned `raw_response` whose dataset is in the job's parser map, in `fetched_at` order, in one
transaction. For a versioned job, rows of its `versioned` tables derived from the selected raw
responses (`raw_id`) are deleted first and replayed, so history is rebuilt, not appended.
Reparse records no `job_run` and never moves a watermark.
"""

import json
import logging
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import httpx

from usfin import alerts, store
from usfin import http as usfin_http
from usfin.settings import Settings, load_settings

ET = ZoneInfo("America/New_York")
TRIGGERS = ("scheduled", "manual", "catchup", "chained")
log = logging.getLogger("usfin.jobs")


class JobSkipped(Exception):
    """Raised by a job's `keys` or `fetch` when the run must not go on (e.g. too little free
    disk for a bulk download): the run stops there and records `skipped` with this message
    (keys done before it keep their work), unless another key already failed."""


class UnknownJob(KeyError):
    """No job is registered under that name."""

    def __str__(self) -> str:
        return f"unknown job {self.args[0]!r}"


Parser = Callable[["JobContext", int], int]


@dataclass(frozen=True)
class Job:
    name: str
    keys: Sequence[str] | Callable[["JobContext"], Sequence[str]]
    fetch: Callable[["JobContext", str], Sequence[int]]
    parsers: Mapping[str, Parser] | Callable[[Settings], Mapping[str, Parser]]
    """`{raw dataset: parser}`, or `parsers(settings)` building it (config-driven jobs)."""
    then: tuple[str, ...] = ()
    versioned: tuple[str, ...] = ()
    """Tables whose rows carry the `raw_id` they came from; reparse deletes and replays them."""
    observed: (
        Mapping[str, alerts.Observed] | Callable[[Settings], Mapping[str, alerts.Observed]] | None
    ) = None
    """`{key: Observed}` (or built from settings): where each key's rows live, for alert rules."""
    compute: Callable[["JobContext", str], int] | None = None
    """A derived job (no source of its own): `compute(ctx, key) -> rows` runs after the key's
    fetch and parses, in its own transaction, and reads tables other jobs filled."""
    after_key: Callable[["JobContext", str], None] | None = None
    """Runs after a key's fetch, parses and compute all succeeded, in its own transaction (never
    on reparse): bookkeeping such as marking the key's `pending_doc` rows done."""
    retry: Callable[[duckdb.DuckDBPyConnection, Settings, datetime], datetime | None] | None = None
    """In `usfin serve`, after each run: `retry(con, settings, now)` returns when to run the job
    again today (a one-shot `<name>_retry`), or None (and raises its own alerts when it gives up).
    BLS: a release day whose new period has not arrived yet."""
    replay_first: tuple[str, ...] = ()
    """Raw datasets reparse replays before the others (each group in `fetched_at` order): BLS
    replays ALFRED's back-dated vintages before its own responses (S18)."""

    def key_list(self, ctx: "JobContext") -> list[str]:
        return list(self.keys(ctx) if callable(self.keys) else self.keys)

    def parser_map(self, settings: Settings) -> Mapping[str, Parser]:
        return self.parsers(settings) if callable(self.parsers) else self.parsers

    def observed_map(self, settings: Settings) -> Mapping[str, alerts.Observed]:
        if self.observed is None:
            return {}
        return self.observed(settings) if callable(self.observed) else self.observed


class Registry:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}

    def register(self, job: Job) -> Job:
        if job.name in self._jobs:
            raise ValueError(f"job {job.name!r} is already registered")
        self._jobs[job.name] = job
        return job

    def get(self, name: str) -> Job:
        try:
            return self._jobs[name]
        except KeyError:
            raise UnknownJob(name) from None

    def names(self) -> list[str]:
        return sorted(self._jobs)

    def __contains__(self, name: object) -> bool:
        return name in self._jobs


REGISTRY = Registry()
"""Process-wide registry; source modules register their jobs here on import."""

SOURCE_MODULES: tuple[str, ...] = (
    "usfin.sources.fiscal",
    "usfin.sources.treasurydirect",
    "usfin.sources.auction_xcheck",
    "usfin.sources.bls",
    "usfin.sources.bls_calendar",
    "usfin.sources.edgar.tickers",
    "usfin.sources.edgar.submissions",
    "usfin.sources.edgar.companyfacts",
    "usfin.sources.edgar.feed",
    "usfin.sources.edgar.dailyindex",
    "usfin.sources.edgar.form4",
    "usfin.sources.edgar.sched13",
    "usfin.sources.edgar.bulk",
    "usfin.sources.nasdaqtrader",
)
"""Modules imported by `load_registry` so their jobs register (sources add themselves)."""


def load_registry() -> Registry:
    """Import every source module; return the registry, plus `_demo` when `USFIN_DEMO=1`."""
    import importlib

    for module in SOURCE_MODULES:
        importlib.import_module(module)
    if os.environ.get("USFIN_DEMO") != "1":
        return REGISTRY
    from usfin import demo

    with_demo = Registry()
    with_demo._jobs = {**REGISTRY._jobs, demo.JOB.name: demo.JOB}
    return with_demo


# --- watermarks ---------------------------------------------------------------------------


def _utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


def watermark_value(value: date | datetime | str, *, today: date) -> str:
    """Serialize a watermark; dates (and ISO date strings) are capped at `today`."""
    if isinstance(value, datetime):
        return _utc_naive(value).isoformat()
    if isinstance(value, date):
        return min(value, today).isoformat()
    if isinstance(value, str):
        try:
            d = date.fromisoformat(value)
        except ValueError:
            return value
        if len(value) == 10:
            return min(d, today).isoformat()
        return value
    raise TypeError(f"watermark value must be a date, datetime or str, got {type(value)}")


def get_watermarks(con: duckdb.DuckDBPyConnection, job: str) -> dict[str, str]:
    rows = con.execute("SELECT key, value FROM watermark WHERE job = ? ORDER BY key", [job])
    return dict(rows.fetchall())


def set_watermark(
    con: duckdb.DuckDBPyConnection,
    job: str,
    key: str,
    value: date | datetime | str,
    *,
    today: date,
) -> str:
    stored = watermark_value(value, today=today)
    con.execute(
        "INSERT INTO watermark VALUES (?, ?, ?, ?)"
        " ON CONFLICT (job, key) DO UPDATE SET value = excluded.value,"
        " updated_at = excluded.updated_at",
        [job, key, stored, _utc_naive(datetime.now(UTC))],
    )
    return stored


# --- runner -------------------------------------------------------------------------------


@dataclass
class JobContext:
    """What a job's fetch and parsers see during one run."""

    job: Job
    con: duckdb.DuckDBPyConnection
    settings: Settings
    today: date
    now: datetime
    trigger: str
    run_id: int | None
    watermarks: dict[str, str]
    transport: httpx.BaseTransport | None = None
    memo: dict[str, Any] = field(default_factory=dict)
    """Per-run scratch shared by fetch and parsers (e.g. one request serving every key)."""
    _proposed: dict[str, date | datetime | str] = field(default_factory=dict)
    _client: usfin_http.Client | None = None

    @property
    def client(self) -> usfin_http.Client:
        """HTTP client on this run's session (opened on first use, closed by the runner)."""
        if self._client is None:
            self._client = usfin_http.Client(
                self.con, settings=self.settings, transport=self.transport
            )
        return self._client

    def advance(self, key: str, value: date | datetime | str) -> None:
        """Propose `key`'s new watermark; written only if the key fully succeeds."""
        self._proposed[key] = value

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None


@dataclass(frozen=True)
class RunResult:
    job: str
    run_id: int | None
    status: str
    rows_upserted: int = 0
    error: str | None = None
    key_errors: dict[str, str] = field(default_factory=dict)
    chained: list["RunResult"] = field(default_factory=list)
    locked: bool = False
    """The DB was held by another process past the lock wait: nothing ran, nothing logged."""


def _run_key(ctx: JobContext, key: str) -> int:
    """Fetch and parse one key; raise on any failure (that key's earlier parses stay)."""
    job, con = ctx.job, ctx.con
    rows = 0
    parsers = job.parser_map(ctx.settings)
    raw_ids = list(job.fetch(ctx, key))
    for raw_id in raw_ids:
        found = con.execute("SELECT dataset FROM raw_response WHERE id = ?", [raw_id]).fetchone()
        if found is None:
            raise LookupError(f"raw_response {raw_id} does not exist")
        parser = parsers.get(found[0])
        if parser is None:
            raise LookupError(f"no parser for dataset {found[0]!r} (raw_response {raw_id})")
        con.execute("BEGIN TRANSACTION")
        try:
            rows += int(parser(ctx, raw_id) or 0)
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    for hook in (job.compute, job.after_key):
        if hook is None:
            continue
        con.execute("BEGIN TRANSACTION")
        try:
            out = hook(ctx, key)
            if hook is job.compute:
                rows += int(out or 0)
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    return rows


SEC_JOB_PREFIX = "edgar_"
"""Jobs named `edgar_*` talk to SEC: inside the SEC block window they record `skipped`."""


def is_sec_job(job: Job) -> bool:
    return job.name.startswith(SEC_JOB_PREFIX)


def _sec_blocked_reason() -> str | None:
    block = usfin_http.SEC_BLOCK
    if not block.blocked():
        return None
    return f"SEC requests blocked for another {block.remaining():.0f}s after a 403; not run"


def _execute(job: Job, con: duckdb.DuckDBPyConnection, ctx_args: dict[str, Any]) -> RunResult:
    now: datetime = ctx_args["now"]
    before = get_watermarks(con, job.name)
    first_alert = alerts.max_id(con)
    run_id = con.execute(
        "INSERT INTO job_run (job, started_at, status, trigger, watermark_before)"
        " VALUES (?, ?, 'running', ?, ?) RETURNING id",
        [job.name, _utc_naive(now), ctx_args["trigger"], json.dumps(before)],
    ).fetchone()[0]
    blocked = _sec_blocked_reason() if is_sec_job(job) else None
    if blocked is not None:
        con.execute(
            "UPDATE job_run SET finished_at = ?, status = 'skipped', rows_upserted = 0,"
            " watermark_after = ?, error = ? WHERE id = ?",
            [_utc_naive(datetime.now(UTC)), json.dumps(before), blocked, run_id],
        )
        log.info("job %s skipped: %s", job.name, blocked)
        return RunResult(job.name, run_id, "skipped", 0, blocked)
    ctx = JobContext(job=job, con=con, run_id=run_id, watermarks=dict(before), **ctx_args)
    rows = 0
    key_errors: dict[str, str] = {}
    skipped: str | None = None
    try:
        keys = job.key_list(ctx)
        for i, key in enumerate(keys):
            ctx._proposed.pop(key, None)
            try:
                rows += _run_key(ctx, key)
                if key in ctx._proposed:
                    set_watermark(con, job.name, key, ctx._proposed.pop(key), today=ctx.today)
            except JobSkipped as exc:
                ctx._proposed.pop(key, None)
                skipped = f"{key}: {exc}"
                log.warning("job %s skipped at key %s: %s", job.name, key, exc)
                break
            except Exception as exc:
                ctx._proposed.pop(key, None)
                key_errors[key] = f"{type(exc).__name__}: {exc}"
                log.error("job %s key %s failed: %s", job.name, key, key_errors[key])
                log.debug("job %s key %s traceback", job.name, key, exc_info=True)
                if isinstance(exc, usfin_http.SecBlocked):
                    for rest in keys[i + 1 :]:  # every SEC request would be refused now
                        key_errors[rest] = "not run: SEC blocked"
                    break
    except JobSkipped as exc:  # keys() itself said not to run
        skipped = str(exc)
        log.warning("job %s skipped: %s", job.name, exc)
    except Exception as exc:  # keys() itself failed
        key_errors["*"] = f"{type(exc).__name__}: {exc}"
        log.error("job %s failed: %s", job.name, key_errors["*"])
        log.debug("job %s traceback", job.name, exc_info=True)
    finally:
        ctx.close()
    status = "failed" if key_errors else "skipped" if skipped else "ok"
    lines = [f"{k}: {v}" for k, v in key_errors.items()]
    if skipped and key_errors:
        lines.append(f"skipped {skipped}")
    error = "\n".join(lines) or skipped
    con.execute(
        "UPDATE job_run SET finished_at = ?, status = ?, rows_upserted = ?,"
        " watermark_after = ?, error = ? WHERE id = ?",
        [
            _utc_naive(datetime.now(UTC)),
            status,
            rows,
            json.dumps(get_watermarks(con, job.name)),
            error,
            run_id,
        ],
    )
    alerts.after_run(
        con, job, settings=ctx.settings, today=ctx.today, now=now, since_id=first_alert
    )
    return RunResult(job.name, run_id, status, rows, error, key_errors)


def run_job(
    name: str,
    *,
    trigger: str = "manual",
    settings: Settings | None = None,
    registry: Registry | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    wait: float | None = None,
    transport: httpx.BaseTransport | None = None,
    chain: bool = True,
) -> RunResult:
    """Run one job in this process and record it in `job_run`, then its `then` chain if ok.

    A DB held by another process past the lock wait returns a `failed` result with no run id
    (the run log cannot be written), logged, with every watermark untouched.
    """
    if trigger not in TRIGGERS:
        raise ValueError(f"trigger must be one of {TRIGGERS}, got {trigger!r}")
    registry = registry or load_registry()
    job = registry.get(name)
    settings = settings or load_settings()
    result = _run_one(job, trigger, settings, clock, wait, transport)
    if chain:
        _run_chain(result, job, {name}, settings, registry, clock, wait, transport)
    return result


def _run_chain(
    result: RunResult,
    job: Job,
    seen: set[str],
    settings: Settings,
    registry: Registry,
    clock: Callable[[], datetime],
    wait: float | None,
    transport: httpx.BaseTransport | None,
) -> None:
    """Run `job.then` after an ok `result`, depth first; each job at most once per chain."""
    if result.status != "ok":
        return
    for name in job.then:
        if name in seen:
            continue
        seen.add(name)
        try:
            child = registry.get(name)
        except UnknownJob as exc:
            log.error("job %s: chained %s", job.name, exc)
            result.chained.append(RunResult(name, None, "failed", 0, str(exc)))
            continue
        child_result = _run_one(child, "chained", settings, clock, wait, transport)
        result.chained.append(child_result)
        _run_chain(child_result, child, seen, settings, registry, clock, wait, transport)


def _run_one(
    job: Job,
    trigger: str,
    settings: Settings,
    clock: Callable[[], datetime],
    wait: float | None,
    transport: httpx.BaseTransport | None,
) -> RunResult:
    name = job.name
    now = clock()
    ctx_args = {
        "settings": settings,
        "today": now.astimezone(ET).date(),
        "now": now,
        "trigger": trigger,
        "transport": transport,
    }
    try:
        with store.session(
            settings.db_path, wait=settings.lock_wait if wait is None else wait
        ) as con:
            return _execute(job, con, ctx_args)
    except store.DatabaseLocked as exc:
        error = f"database busy — another process holds the lock ({exc})"
        log.error("job %s not run: %s", name, error)
        return RunResult(name, None, "failed", 0, error, locked=True)


# --- reparse ------------------------------------------------------------------------------


class NetworkDisabled(RuntimeError):
    """A parser tried to send an HTTP request during reparse."""


class ReparseFailed(Exception):
    """A parser failed during reparse; the whole reparse was rolled back. `__cause__` is why."""


class _NoNetwork(httpx.BaseTransport):
    def handle_request(self, request: httpx.Request) -> httpx.Response:
        raise NetworkDisabled(f"HTTP is disabled during reparse: {request.method} {request.url}")


@dataclass(frozen=True)
class ReparseResult:
    job: str
    raw_ids: list[int]
    skipped: list[int]
    rows: int
    deleted: int


def _since_utc(since: date) -> datetime:
    """Start of `since` in New York as naive UTC (how `fetched_at` is stored)."""
    return _utc_naive(datetime(since.year, since.month, since.day, tzinfo=ET))


def reparse(
    con: duckdb.DuckDBPyConnection,
    job: Job,
    *,
    settings: Settings,
    now: datetime | None = None,
    since: date | None = None,
    transport: httpx.BaseTransport | None = None,
) -> ReparseResult:
    """Rebuild `job`'s normalized rows from its stored raw responses; no HTTP, all or nothing.

    Selects `raw_response` rows whose dataset is in `job.parsers` (fetched on or after `since`,
    a New York date, if given) in `fetched_at` order, `job.replay_first` datasets first. Pruned
    rows are skipped with a warning and their derived rows kept. A parser error rolls the whole reparse back and raises `ReparseFailed`.
    """
    now = now or datetime.now(UTC)
    parsers = job.parser_map(settings)
    datasets = sorted(parsers)
    if not datasets:
        return ReparseResult(job.name, [], [], 0, 0)
    sql = (
        "SELECT id, dataset, pruned_at FROM raw_response"
        f" WHERE dataset IN ({', '.join('?' for _ in datasets)})"
    )
    params: list[Any] = list(datasets)
    if since is not None:
        sql += " AND fetched_at >= ?"
        params.append(_since_utc(since))
    selected = con.execute(sql + " ORDER BY fetched_at, id", params).fetchall()
    selected.sort(key=lambda row: row[1] not in job.replay_first)  # stable: keeps fetched_at order
    skipped = [raw_id for raw_id, _, pruned_at in selected if pruned_at is not None]
    for raw_id in skipped:
        log.warning("reparse %s: raw_response %s was pruned; skipped", job.name, raw_id)
    todo = [(raw_id, dataset) for raw_id, dataset, pruned_at in selected if pruned_at is None]
    ids = [raw_id for raw_id, _ in todo]

    ctx = JobContext(
        job=job,
        con=con,
        settings=settings,
        today=now.astimezone(ET).date(),
        now=now,
        trigger="reparse",
        run_id=None,
        watermarks=get_watermarks(con, job.name),
        transport=transport or _NoNetwork(),
    )
    deleted = rows = 0
    con.execute("BEGIN TRANSACTION")
    try:
        existing = set(store.table_names(con))
        for table in job.versioned:
            if ids and table in existing:
                deleted += con.execute(
                    f'DELETE FROM "{table}" WHERE raw_id IN ({", ".join("?" for _ in ids)})',
                    ids,
                ).fetchone()[0]
        for raw_id, dataset in todo:
            try:
                rows += int(parsers[dataset](ctx, raw_id) or 0)
            except Exception as exc:
                raise ReparseFailed(
                    f"raw_response {raw_id} ({dataset}): {type(exc).__name__}: {exc}"
                ) from exc
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    finally:
        ctx.close()  # proposed watermarks are dropped: reparse never moves them
    return ReparseResult(job.name, ids, skipped, rows, deleted)


# --- listing ------------------------------------------------------------------------------


@dataclass(frozen=True)
class JobStatus:
    job: str
    last_run: dict[str, Any] | None
    watermarks: list[tuple[str, str, datetime]]


def status(con: duckdb.DuckDBPyConnection, names: Sequence[str]) -> list[JobStatus]:
    """Last run and watermarks for `names` plus any job seen in the run log or watermarks."""
    seen = {
        r[0]
        for r in con.execute("SELECT job FROM job_run UNION SELECT job FROM watermark").fetchall()
    }
    out = []
    for name in sorted(set(names) | seen):
        cur = con.execute(
            "SELECT id, status, trigger, started_at, finished_at, rows_upserted, error"
            " FROM job_run WHERE job = ? ORDER BY id DESC LIMIT 1",
            [name],
        )
        row = cur.fetchone()
        last = dict(zip([d[0] for d in cur.description], row, strict=True)) if row else None
        marks = con.execute(
            "SELECT key, value, updated_at FROM watermark WHERE job = ? ORDER BY key", [name]
        ).fetchall()
        out.append(JobStatus(name, last, marks))
    return out
