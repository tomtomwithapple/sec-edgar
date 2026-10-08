"""Data-quality alerts: rules run after every job run, the `alert` table, the webhook.

Rules are configured per job in `config/schedule.yaml` and run per watermark key:

    jobs:
      <job>:
        due: federal                 # due-day rule: a calendar, none, or
                                     # {calendar: federal, nth_of_month: 8}
        alerts:                      # rule -> parameters, for every key
          zero_new_rows: {}
          stale_watermark: {n: 2}
          value_jump: {pct: 25}
          row_count_swing: {pct: 2}
        keys:                        # per-key overrides: due, or a rule's parameters (null = off)
          <key>: {due: none, value_jump: {pct: 50}}

A job tells the rules where a key's rows live with `Job.observed` (`{key: Observed}`).

- `zero_new_rows`: on a due day, no row of the key dated after the key's watermark *as it stood
  before the job's first run that New York day* (so a repeat run the same day does not fire).
- `stale_watermark(n)`: more than `n` due days fall after the key's watermark, through today.
- `value_jump(pct)`: a row dated after that same baseline moved more than `pct` % from the
  previous row of its series (flag only; the row is stored).
- `row_count_swing(pct)`: for full-file snapshots, the newest snapshot's row count differs from
  the previous one by more than `pct` % (the first snapshot has nothing to compare).

A key with no watermark before the day's first run (a first load) has no baseline:
`zero_new_rows`, `value_jump` and `row_count_swing` skip it. Sources raise their own rules
(`schema_drift`, `calendar_stale`, `auction_mismatch`, ...) with `raise_alert`. An alert is stored
once per (job, rule, subject, New York date); each newly stored alert is POSTed as JSON to
`ALERT_WEBHOOK_URL` when set (a failed post is logged, never fatal).
"""

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import duckdb
import httpx
import yaml

from usfin import calendars
from usfin.settings import ConfigError, Settings

if TYPE_CHECKING:
    from usfin.jobs import Job

ET = ZoneInfo("America/New_York")
RULES = ("zero_new_rows", "stale_watermark", "value_jump", "row_count_swing")
_REQUIRED = {"stale_watermark": ("n",), "value_jump": ("pct",)}
_DEFAULTS: dict[str, dict[str, Any]] = {"row_count_swing": {"pct": 2}}
WEBHOOK_TIMEOUT = 10.0
log = logging.getLogger("usfin.alerts")


# --- what a job observes ------------------------------------------------------------------


@dataclass(frozen=True)
class Observed:
    """Where one watermark key's rows live, for the table-based rules."""

    table: str
    date_column: str
    match: Mapping[str, Sequence[Any]] = field(default_factory=dict)
    """Column -> allowed values selecting this key's rows (empty: the whole table)."""
    series: tuple[str, ...] = ()
    """Columns identifying one series within the key's rows (value_jump compares within it)."""
    values: tuple[str, ...] = ()
    """Numeric columns checked by value_jump."""
    order: tuple[str, ...] = ()
    """Tie-breakers after `date_column` when ordering a series."""
    jump_match: Mapping[str, Sequence[Any]] = field(default_factory=dict)
    """Extra column -> values filter for value_jump only (e.g. balances, not daily flows)."""
    snapshot: bool = False
    """Rows are full-file snapshots grouped by `date_column` (row_count_swing applies)."""


# --- due days and rule config -------------------------------------------------------------


@dataclass(frozen=True)
class DueRule:
    """A day is due when it is a `calendar` day (and the `nth_of_month` one, if set)."""

    calendar: str | None
    nth_of_month: int | None = None

    def is_due(self, cals: calendars.Calendars, d: date) -> bool:
        if self.calendar is None or not cals.is_day(self.calendar, d):
            return False
        if self.nth_of_month is None:
            return True
        first = d.replace(day=1)
        nth = sum(
            1
            for i in range((d - first).days + 1)
            if cals.is_day(self.calendar, first + timedelta(days=i))
        )
        return nth == self.nth_of_month

    def describe(self) -> str:
        if self.calendar is None:
            return "none"
        return self.calendar + (f" #{self.nth_of_month} of month" if self.nth_of_month else "")


NEVER = DueRule(None)


@dataclass(frozen=True)
class JobRules:
    due: DueRule
    rules: Mapping[str, Mapping[str, Any]]
    keys: Mapping[str, tuple[DueRule | None, Mapping[str, Mapping[str, Any] | None]]]

    def due_for(self, key: str) -> DueRule:
        override = self.keys.get(key, (None, {}))[0]
        return override if override is not None else self.due

    def params_for(self, key: str) -> dict[str, Mapping[str, Any]]:
        out = dict(self.rules)
        for rule, params in self.keys.get(key, (None, {}))[1].items():
            if params is None:
                out.pop(rule, None)
            else:
                out[rule] = params
        return out


def _due(value: Any, where: str) -> DueRule:
    if value is None or value == "none":
        return NEVER
    if isinstance(value, str):
        if value not in calendars.CALENDARS:
            raise ConfigError(f"{where}: unknown due rule {value!r}")
        return DueRule(value)
    if isinstance(value, Mapping):
        unknown = set(value) - {"calendar", "nth_of_month"}
        cal = value.get("calendar")
        nth = value.get("nth_of_month")
        if unknown or cal not in calendars.CALENDARS:
            raise ConfigError(f"{where}: bad due rule {dict(value)!r}")
        if nth is not None and (not isinstance(nth, int) or not 1 <= nth <= 31):
            raise ConfigError(f"{where}: nth_of_month must be 1..31, got {nth!r}")
        return DueRule(cal, nth)
    raise ConfigError(f"{where}: bad due rule {value!r}")


def _params(rule: str, value: Any, where: str) -> dict[str, Any]:
    if rule not in RULES:
        raise ConfigError(f"{where}: unknown alert rule {rule!r} (rules: {', '.join(RULES)})")
    if value is not None and not isinstance(value, Mapping):
        raise ConfigError(f"{where}: {rule} takes a mapping of parameters, got {value!r}")
    params = {**_DEFAULTS.get(rule, {}), **(value or {})}
    missing = [p for p in _REQUIRED.get(rule, ()) if p not in params]
    if missing:
        raise ConfigError(f"{where}: {rule} needs {', '.join(missing)}")
    return params


def load_rules(config_dir: Path | str) -> dict[str, JobRules]:
    """Read the due-day and alert-rule part of `<config_dir>/schedule.yaml`, by job."""
    path = Path(config_dir) / "schedule.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    out: dict[str, JobRules] = {}
    for job, entry in (data.get("jobs") or {}).items():
        where = f"{path}: {job}"
        entry = entry or {}
        if not isinstance(entry, Mapping):
            raise ConfigError(f"{where}: expected a mapping")
        due = _due(entry.get("due", entry.get("calendar")), where)
        rules = {
            rule: _params(rule, value, where) for rule, value in (entry.get("alerts") or {}).items()
        }
        keys = {}
        for key, override in (entry.get("keys") or {}).items():
            override = dict(override or {})
            key_due = _due(override.pop("due"), f"{where}.{key}") if "due" in override else None
            key_rules = {
                rule: (None if value is None else _params(rule, value, f"{where}.{key}"))
                for rule, value in override.items()
            }
            keys[str(key)] = (key_due, key_rules)
        out[str(job)] = JobRules(due, rules, keys)
    return out


# --- the alert table ----------------------------------------------------------------------


def _utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


def _et_date(dt: datetime) -> date:
    return (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(ET).date()


def raise_alert(
    con: duckdb.DuckDBPyConnection,
    *,
    job: str,
    rule: str,
    subject: str,
    detail: Mapping[str, Any] | None,
    now: datetime,
) -> int | None:
    """Store an alert; return its id, or None if (job, rule, subject, NY date) already exists."""
    row = con.execute(
        "INSERT INTO alert (job, rule, subject, detail, raised_on, raised_at)"
        " VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING id",
        [
            job,
            rule,
            subject,
            None if detail is None else json.dumps(detail, default=str),
            _et_date(now),
            _utc_naive(now),
        ],
    ).fetchone()
    if row is not None:
        log.warning("alert %s/%s %s: %s", job, rule, subject, detail)
    return None if row is None else row[0]


def _rows(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def list_alerts(
    con: duckdb.DuckDBPyConnection, *, include_acked: bool = False, since_id: int | None = None
) -> list[dict[str, Any]]:
    """Alerts newest first (open ones only unless `include_acked`); `detail` decoded."""
    sql = "SELECT * FROM alert WHERE 1 = 1"
    params: list[Any] = []
    if not include_acked:
        sql += " AND acknowledged_at IS NULL"
    if since_id is not None:
        sql += " AND id > ?"
        params.append(since_id)
    rows = _rows(con.execute(sql + " ORDER BY raised_at DESC, id DESC", params))
    for row in rows:
        row["detail"] = None if row["detail"] is None else json.loads(row["detail"])
    return rows


def ack(con: duckdb.DuckDBPyConnection, alert_id: int, *, now: datetime | None = None) -> bool:
    """Acknowledge an alert (idempotent); False if no alert has that id."""
    row = con.execute(
        "UPDATE alert SET acknowledged_at = coalesce(acknowledged_at, ?) WHERE id = ? RETURNING id",
        [_utc_naive(now or datetime.now(UTC)), alert_id],
    ).fetchone()
    return row is not None


def max_id(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute("SELECT coalesce(max(id), 0) FROM alert").fetchone()[0]


# --- webhook ------------------------------------------------------------------------------


def payload(alert: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": alert["id"],
        "job": alert["job"],
        "rule": alert["rule"],
        "subject": alert["subject"],
        "detail": alert["detail"],
        "raised_on": alert["raised_on"].isoformat(),
        "raised_at": alert["raised_at"].isoformat() + "Z",
    }


def notify(con: duckdb.DuckDBPyConnection, settings: Settings, *, since_id: int) -> int:
    """POST every alert stored after `since_id` to the webhook; return how many posts succeeded."""
    url = settings.alert_webhook_url
    if not url:
        return 0
    sent = 0
    for alert in reversed(list_alerts(con, include_acked=True, since_id=since_id)):
        try:
            response = httpx.post(url, json=payload(alert), timeout=WEBHOOK_TIMEOUT)
            response.raise_for_status()
            sent += 1
        except httpx.HTTPError as exc:
            log.warning("alert webhook failed for alert %s: %s", alert["id"], exc)
    return sent


# --- rules --------------------------------------------------------------------------------


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        con.execute(
            "SELECT count(*) FROM information_schema.tables"
            " WHERE table_schema = 'main' AND table_name = ?",
            [table],
        ).fetchone()[0]
    )


def _where(*filters: Mapping[str, Sequence[Any]]) -> tuple[str, list[Any]]:
    parts, params = ["1 = 1"], []
    for filt in filters:
        for col, values in filt.items():
            values = list(values)
            parts.append(f"{_q(col)} IN ({', '.join('?' for _ in values)})")
            params.extend(values)
    return " AND ".join(parts), params


def _as_date(value: str | None) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def day_baseline(con: duckdb.DuckDBPyConnection, job: str, today: date) -> dict[str, str]:
    """The job's watermarks before its first run on `today` (a New York date)."""
    start = _utc_naive(datetime(today.year, today.month, today.day, tzinfo=ET))
    row = con.execute(
        "SELECT watermark_before FROM job_run WHERE job = ? AND started_at >= ?"
        " ORDER BY started_at, id LIMIT 1",
        [job, start],
    ).fetchone()
    return json.loads(row[0]) if row and row[0] else {}


@dataclass
class _Check:
    con: duckdb.DuckDBPyConnection
    job: str
    key: str
    spec: Observed | None
    due: DueRule
    cals: calendars.Calendars
    baseline: str | None
    watermark: str | None
    today: date
    now: datetime

    def alert(self, rule: str, subject: str, detail: Mapping[str, Any]) -> int | None:
        return raise_alert(
            self.con, job=self.job, rule=rule, subject=subject, detail=detail, now=self.now
        )

    def table_ready(self) -> bool:
        return self.spec is not None and _table_exists(self.con, self.spec.table)


def _zero_new_rows(c: _Check, params: Mapping[str, Any]) -> None:
    if c.baseline is None or not c.table_ready() or not c.due.is_due(c.cals, c.today):
        return
    spec = c.spec
    where, args = _where(spec.match)
    newer = c.con.execute(
        f"SELECT count(*) FROM {_q(spec.table)}"
        f" WHERE {where} AND {_q(spec.date_column)} > CAST(? AS TIMESTAMP)",
        [*args, c.baseline],
    ).fetchone()[0]
    if newer == 0:
        c.alert(
            "zero_new_rows",
            c.key,
            {"table": spec.table, "watermark_before": c.baseline, "due": c.due.describe()},
        )


def _stale_watermark(c: _Check, params: Mapping[str, Any]) -> None:
    wm = _as_date(c.watermark)
    if wm is None or c.due.calendar is None:
        return
    n = int(params["n"])
    count, d = 0, wm
    while d < c.today and count <= n:
        d += timedelta(days=1)
        count += c.due.is_due(c.cals, d)
    if count > n:
        c.alert(
            "stale_watermark",
            c.key,
            {"watermark": c.watermark, "due_days_since": count, "n": n, "due": c.due.describe()},
        )


def _value_jump(c: _Check, params: Mapping[str, Any]) -> None:
    if c.baseline is None or not c.table_ready() or not c.spec.values:
        return
    spec, pct = c.spec, float(params["pct"])
    where, args = _where(spec.match, spec.jump_match)
    series = ", ".join(_q(s) for s in spec.series)
    partition = f"PARTITION BY {series} " if series else ""
    order = ", ".join(_q(x) for x in (spec.date_column, *spec.order) if x not in spec.series)
    for col in spec.values:
        x = f"TRY_CAST({_q(col)} AS DOUBLE)"
        rows = c.con.execute(
            f"WITH s AS (SELECT {series + ', ' if series else ''}{_q(spec.date_column)} AS d,"
            f" {x} AS x, lag({x}) OVER ({partition}ORDER BY {order}) AS p"
            f" FROM {_q(spec.table)} WHERE {where} AND {x} IS NOT NULL)"
            " SELECT * FROM s WHERE d > CAST(? AS TIMESTAMP) AND p IS NOT NULL AND p <> 0"
            " AND abs(x - p) / abs(p) * 100 > ? ORDER BY d",
            [*args, c.baseline, pct],
        ).fetchall()
        for row in rows:
            ids, (d, value, previous) = row[: len(spec.series)], row[len(spec.series) :]
            label = "/".join(str(v) for v in ids)
            subject = f"{c.key}:{col}:{label}@{d.isoformat()}" if label else (
                f"{c.key}:{col}@{d.isoformat()}"
            )
            c.alert(
                "value_jump",
                subject,
                {
                    "table": spec.table,
                    "column": col,
                    "series": dict(zip(spec.series, ids, strict=True)),
                    "date": d.isoformat(),
                    "previous": previous,
                    "value": value,
                    "pct_change": (value - previous) / abs(previous) * 100,
                    "threshold_pct": params["pct"],
                },
            )


def _row_count_swing(c: _Check, params: Mapping[str, Any]) -> None:
    if not c.table_ready() or not c.spec.snapshot:
        return
    spec, pct = c.spec, float(params["pct"])
    where, args = _where(spec.match)
    last_two = c.con.execute(
        f"SELECT {_q(spec.date_column)} AS d, count(*) FROM {_q(spec.table)} WHERE {where}"
        " GROUP BY d ORDER BY d DESC LIMIT 2",
        args,
    ).fetchall()
    if len(last_two) < 2:
        return
    (latest, rows), (prev_at, prev_rows) = last_two
    if c.baseline is not None:
        newer = c.con.execute(
            "SELECT CAST(? AS TIMESTAMP) > CAST(? AS TIMESTAMP)", [latest, c.baseline]
        ).fetchone()[0]
        if not newer:
            return
    change = (rows - prev_rows) / prev_rows * 100 if prev_rows else float("inf")
    if abs(change) > pct:
        c.alert(
            "row_count_swing",
            c.key,
            {
                "table": spec.table,
                "snapshot": str(latest),
                "rows": rows,
                "previous_snapshot": str(prev_at),
                "previous_rows": prev_rows,
                "pct_change": change,
                "threshold_pct": params["pct"],
            },
        )


_RULE_FUNCS: dict[str, str] = {r: f"_{r}" for r in RULES}


def evaluate(
    con: duckdb.DuckDBPyConnection,
    job: "Job",
    *,
    settings: Settings,
    today: date,
    now: datetime,
) -> None:
    """Run `job`'s configured rules for every key; a failing rule is logged, never raised."""
    rules = load_rules(settings.config_dir).get(job.name)
    if rules is None:
        return
    observed = job.observed_map(settings)
    cals = calendars.load_calendars(settings.config_dir)
    baseline = day_baseline(con, job.name, today)
    current = dict(
        con.execute("SELECT key, value FROM watermark WHERE job = ?", [job.name]).fetchall()
    )
    for key in sorted(set(observed) | set(current)):
        check = _Check(
            con=con,
            job=job.name,
            key=key,
            spec=observed.get(key),
            due=rules.due_for(key),
            cals=cals,
            baseline=baseline.get(key),
            watermark=current.get(key),
            today=today,
            now=now,
        )
        for rule, params in rules.params_for(key).items():
            func: Callable[[_Check, Mapping[str, Any]], None] = globals()[_RULE_FUNCS[rule]]
            try:
                func(check, params)
            except Exception as exc:
                log.error("alert rule %s for %s/%s failed: %s", rule, job.name, key, exc)
                log.debug("alert rule traceback", exc_info=True)


def after_run(
    con: duckdb.DuckDBPyConnection,
    job: "Job",
    *,
    settings: Settings,
    today: date,
    now: datetime,
    since_id: int,
) -> None:
    """The runner's hook: evaluate the rules, then post every alert stored during the run."""
    try:
        evaluate(con, job, settings=settings, today=today, now=now)
    except Exception as exc:  # e.g. a bad schedule.yaml: the run itself stays as it was
        log.error("alert rules for %s failed: %s", job.name, exc)
        log.debug("alert rules traceback", exc_info=True)
    try:
        notify(con, settings, since_id=since_id)
    except Exception as exc:
        log.error("alert webhook for %s failed: %s", job.name, exc)
        log.debug("alert webhook traceback", exc_info=True)
