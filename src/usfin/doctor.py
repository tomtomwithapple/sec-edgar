"""`usfin doctor`: is this Mac ready to run usfin? (plan: How › Ops; C18; S35).

One line per check, `<status> <name> <detail>`, status `ok`, `info`, `warn` or `FAIL`; the
command exits 1 when any check fails. It reports:

- every env var usfin reads, as set or unset — **never a value** for the secrets
  (`SEC_USER_AGENT`, `BLS_CONTACT_EMAIL`, `BLS_API_KEY`, `FRED_API_KEY`, `ALERT_WEBHOOK_URL`,
  `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`); the Alpaca keys are `info` set or unset (the Alpaca
  jobs refuse without them, the rest of usfin does not need them); paths and numbers (`USFIN_*`) show their effective value. A required one unset fails;
- each config file: present, and loadable by the loader that uses it;
- the DB path, its size and schema version, and whether another process holds its lock — read
  with one read-only open, no lock wait (a held lock is normal while `usfin serve` runs a job, so
  it is `info`, not a failure); a DB not created yet is not created;
- the raw store's size, free disk on the fuller of the raw and DB volumes against
  `USFIN_MIN_FREE_GB`, and the newest EDGAR bulk estimate projected against that free space.
"""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from usfin import alerts, calendars, ratelimit, scheduler, store
from usfin.settings import Settings
from usfin.sources import alpaca, bls, fiscal
from usfin.sources.edgar import bulk, concepts, dailyindex, feed

_PID = re.compile(r"\(PID (\d+)\)")


@dataclass(frozen=True)
class Check:
    status: str  # ok | info | warn | FAIL
    name: str
    detail: str

    def line(self) -> str:
        return f"{self.status:<4}  {self.name:<20} {self.detail}"


@dataclass(frozen=True)
class EnvVar:
    name: str
    secret: bool
    required: bool
    use: str
    default: str | None = None
    status: str | None = None
    """Fixed status whatever the state (the Alpaca keys: always `info`)."""


ENV: tuple[EnvVar, ...] = (
    EnvVar("SEC_USER_AGENT", True, True, "EDGAR jobs: 'Name email' (SEC fair access)"),
    EnvVar("BLS_CONTACT_EMAIL", True, True, "www.bls.gov release calendar (bls_calendar)"),
    EnvVar("BLS_API_KEY", True, False, "BLS v2 (keyless v1 with 15-min retries otherwise)"),
    EnvVar("FRED_API_KEY", True, False, "ALFRED backfill of BLS first prints (usfin bls alfred-backfill)"),
    EnvVar("ALERT_WEBHOOK_URL", True, False, "alerts POSTed as JSON"),
    EnvVar("APCA_API_KEY_ID", True, False, "Alpaca market data jobs (key id)", status="info"),
    EnvVar("APCA_API_SECRET_KEY", True, False, "Alpaca market data jobs (secret)", status="info"),
    EnvVar("USFIN_DB", False, False, "DuckDB file", "data/usfin.duckdb"),
    EnvVar("USFIN_RAW", False, False, "raw responses dir", "raw"),
    EnvVar("USFIN_CONFIG", False, False, "config dir", "config"),
    EnvVar("USFIN_LOCK_WAIT", False, False, "seconds a job waits for the DB", "600"),
    EnvVar("USFIN_MIN_FREE_GB", False, False, "free-space floor for bulk loads, GiB", "10"),
    EnvVar("USFIN_BULK_LIMIT", False, False, "members per ZIP for edgar_bulk", "all"),
)


def _edgar(config_dir: Path) -> None:
    feed.load_watchlist(config_dir)
    dailyindex.load_published_after(config_dir)


def _schedule(config_dir: Path) -> None:
    scheduler.load_schedule(config_dir)
    alerts.load_rules(config_dir)


CONFIG: Mapping[str, Callable[[Path], Any]] = {
    "hosts.yaml": ratelimit.load_hosts,
    "calendars.yaml": calendars.load_calendars,
    "fiscal_data.yaml": fiscal.load_datasets,
    "bls.yaml": bls.load_series,
    "edgar.yaml": _edgar,
    "concepts.yaml": concepts.load_concepts,
    "schedule.yaml": _schedule,
    "alpaca.yaml": alpaca.load_config,
}


def env_checks(env: Mapping[str, str | None]) -> list[Check]:
    out = []
    for var in ENV:
        value = env.get(var.name) or None
        need = "required" if var.required else "optional"
        if var.secret:
            status = var.status or ("ok" if value else ("FAIL" if var.required else "info"))
            state = "set" if value else "unset"
            out.append(Check(status, var.name, f"{state} — {need}: {var.use}"))
        else:
            state = f"set: {value}" if value else f"unset (default {var.default})"
            out.append(Check("ok", var.name, f"{state} — {var.use}"))
    return out


def config_checks(config_dir: Path) -> list[Check]:
    out = []
    for name, loader in CONFIG.items():
        path = config_dir / name
        if not path.is_file():
            out.append(Check("FAIL", name, f"missing in {config_dir}"))
            continue
        try:
            loader(config_dir)
        except Exception as exc:  # noqa: BLE001 — any loader error is the finding
            out.append(Check("FAIL", name, f"does not load: {exc}"))
        else:
            out.append(Check("ok", name, f"{path}"))
    return out


def _size(path: Path) -> str:
    n = path.stat().st_size
    return f"{n / 1e9:.2f} GB" if n >= 1e8 else f"{n} bytes"


def db_checks(settings: Settings, free: int) -> list[Check]:
    path = settings.db_path
    if not path.exists():
        return [Check("info", "database", f"{path} not created yet (the first run creates it)")]
    try:
        con = duckdb.connect(str(path), read_only=True)
    except duckdb.IOException as exc:
        if store._LOCK_MARKER not in str(exc):
            return [Check("FAIL", "database", f"{path}: {exc}")]
        pid = _PID.search(str(exc))
        holder = f" (PID {pid[1]})" if pid else ""
        return [
            Check("ok", "database", f"{path} {_size(path)}; schema not read while it is held"),
            Check("info", "lock", f"held by another process{holder}: usfin serve running a"
                  " job, or an analysis session; jobs wait up to USFIN_LOCK_WAIT"),
            *(Check("info", f"bulk {name}", "not read while the DB is held") for name in bulk.DATASETS),
        ]  # fmt: skip
    try:
        latest = len(store.available_migrations())
        try:
            version = con.execute("SELECT coalesce(max(version), 0) FROM schema_version")
            version = version.fetchone()[0]
        except duckdb.CatalogException:
            version = 0
        status = "ok" if version == latest else "warn"
        pending = "" if version == latest else " (pending migrations apply on the next open)"
        out = [
            Check(
                status, "database", f"{path} {_size(path)}, schema {version} of {latest}{pending}"
            ),
            Check("ok", "lock", "free: no other process holds the DB"),
        ]
        out += bulk_checks(con, settings, free, has_table=version > 0)
        return out
    finally:
        con.close()


def bulk_checks(
    con: duckdb.DuckDBPyConnection, settings: Settings, free: int, *, has_table: bool = True
) -> list[Check]:
    out = []
    for name, spec in bulk.DATASETS.items():
        try:
            est = bulk.latest_estimate(con, spec) if has_table else None
        except duckdb.CatalogException:
            est = None
        if est is None:
            out.append(Check("info", f"bulk {name}", f"no estimate yet (usfin edgar backfill"
                             f" {name} --limit 200 measures one)"))  # fmt: skip
            continue
        projected = max(0, int(est.per_member(est.growth)))
        g = bulk.Guard(free, est.zip_bytes, projected, bulk.floor_bytes(settings))
        out.append(Check(
            "ok" if g.ok else "warn", f"bulk {name}",
            f"estimate {est.measured_at:%Y-%m-%d %H:%M} UTC: a full load grows the DB"
            f" {bulk.gib(projected)}; {g.describe()} → {'fits' if g.ok else 'does not fit'}",
        ))  # fmt: skip
    return out


def raw_checks(settings: Settings) -> list[Check]:
    root = settings.raw_dir
    if not root.is_dir():
        return [Check("info", "raw store", f"{root} not created yet")]
    size = count = 0
    for p in root.rglob("*"):
        if p.is_file():
            size += p.stat().st_size
            count += 1
    return [Check("ok", "raw store", f"{root} {size} bytes ({bulk.gib(size)}) in {count} files")]


def disk_check(settings: Settings, free: int) -> Check:
    floor = settings.min_free_gb
    ok = free >= bulk.floor_bytes(settings)
    return Check(
        "ok" if ok else "FAIL", "disk",
        f"free {bulk.gib(free)} on the fuller of the raw and DB volumes"
        f" {'≥' if ok else '<'} floor {floor:.1f} GiB (USFIN_MIN_FREE_GB)",
    )  # fmt: skip


def report(settings: Settings, env: Mapping[str, str | None]) -> list[Check]:
    free = bulk.free_bytes(settings)
    return [
        *env_checks(env),
        *config_checks(settings.config_dir),
        *db_checks(settings, free),
        *raw_checks(settings),
        disk_check(settings, free),
    ]
