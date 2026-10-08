"""DuckDB store: open-per-use sessions with a cross-process lock wait, and SQL migrations.

DuckDB lets one process hold the file. Nothing keeps a connection between uses: `session()`
opens, yields and closes. In-process opens share one DuckDB instance and never conflict; an open
blocked by another process retries with backoff for up to `wait` seconds, then raises
`DatabaseLocked`.
"""

import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

import duckdb

from usfin.settings import load_settings

_MIGRATION_FILE = re.compile(r"^(\d{3})_(\w+)\.sql$")
_LOCK_MARKER = "Could not set lock on file"
_FIRST_BACKOFF = 0.05
_MAX_BACKOFF = 1.0


class DatabaseLocked(Exception):
    """Another process held the DuckDB file for longer than the lock wait."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def label(self) -> str:
        return f"{self.version:03d}_{self.name}"


def _discover() -> list[Migration]:
    found = []
    for entry in files("usfin.migrations").iterdir():
        m = _MIGRATION_FILE.match(entry.name)
        if m:
            found.append(Migration(int(m.group(1)), m.group(2), entry.read_text("utf-8")))
    found.sort(key=lambda mig: mig.version)
    versions = [mig.version for mig in found]
    if len(set(versions)) != len(versions):
        raise RuntimeError(f"duplicate migration versions: {versions}")
    return found


def available_migrations() -> list[Migration]:
    """Every packaged migration, in version order."""
    return _discover()


def _ensure_version_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "CREATE TABLE IF NOT EXISTS schema_version ("
        " version INTEGER PRIMARY KEY, name VARCHAR NOT NULL, applied_at TIMESTAMP NOT NULL)"
    )


def schema_version(con: duckdb.DuckDBPyConnection) -> int:
    """Highest applied migration version (0 when none)."""
    _ensure_version_table(con)
    return con.execute("SELECT coalesce(max(version), 0) FROM schema_version").fetchone()[0]


def migrate(con: duckdb.DuckDBPyConnection) -> list[int]:
    """Apply pending migrations in order, each in its own transaction; return applied versions."""
    current = schema_version(con)
    applied = []
    for mig in available_migrations():
        if mig.version <= current:
            continue
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(mig.sql)
            con.execute(
                "INSERT INTO schema_version VALUES (?, ?, now()::TIMESTAMP)",
                [mig.version, mig.name],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        applied.append(mig.version)
    return applied


def _connect(path: Path, wait: float) -> duckdb.DuckDBPyConnection:
    deadline = time.monotonic() + wait
    backoff = _FIRST_BACKOFF
    while True:
        try:
            return duckdb.connect(str(path))
        except duckdb.IOException as exc:
            if _LOCK_MARKER not in str(exc):
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DatabaseLocked(
                    f"{path} is held by another process (waited {wait:g}s): {exc}"
                ) from exc
            time.sleep(min(backoff, remaining))
            backoff = min(backoff * 2, _MAX_BACKOFF)


@contextmanager
def session(
    path: Path | str | None = None, *, wait: float | None = None, apply_migrations: bool = True
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the DB (defaults from settings: `USFIN_DB`, `USFIN_LOCK_WAIT`), yield, close.

    Pending migrations are applied on open unless `apply_migrations=False`.
    """
    if path is None or wait is None:
        settings = load_settings()
        path = settings.db_path if path is None else path
        wait = settings.lock_wait if wait is None else wait
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = _connect(path, wait)
    try:
        if apply_migrations:
            migrate(con)
        yield con
    finally:
        con.close()


def table_names(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    return [r[0] for r in rows]
