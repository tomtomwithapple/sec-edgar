from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
import typer

from usfin import __version__, store
from usfin.settings import load_settings

app = typer.Typer(no_args_is_help=True)


@app.callback()
def main() -> None:
    """usfin: fetch, store and serve US public financial data."""


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


db_app = typer.Typer(no_args_is_help=True, help="Database: migrations and info.")
app.add_typer(db_app, name="db")


@contextmanager
def _db_session() -> Iterator[duckdb.DuckDBPyConnection]:
    """Session for CLI commands: a held lock exits 1 with one line instead of a traceback."""
    settings = load_settings()
    try:
        with store.session(
            settings.db_path, wait=settings.lock_wait, apply_migrations=False
        ) as con:
            yield con
    except store.DatabaseLocked as exc:
        typer.echo(f"database busy — another process holds the lock ({exc})", err=True)
        raise typer.Exit(1) from None


@db_app.command("migrate")
def db_migrate() -> None:
    """Apply pending schema migrations."""
    by_version = {m.version: m for m in store.available_migrations()}
    with _db_session() as con:
        applied = store.migrate(con)
    if applied:
        for version in applied:
            typer.echo(f"applied {by_version[version].label}")
    else:
        typer.echo("schema up to date")


@db_app.command("info")
def db_info() -> None:
    """Print the DB path, schema version and tables."""
    with _db_session() as con:
        version = store.schema_version(con)
        tables = store.table_names(con)
    typer.echo(f"path: {load_settings().db_path.resolve()}")
    typer.echo(f"version: {version}")
    typer.echo(f"tables: {', '.join(tables)}")
