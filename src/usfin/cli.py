from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
import typer

from usfin import __version__, raw, store
from usfin import http as usfin_http
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
def _db_session(*, apply_migrations: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Session for CLI commands: a held lock exits 1 with one line instead of a traceback."""
    settings = load_settings()
    try:
        with store.session(
            settings.db_path, wait=settings.lock_wait, apply_migrations=apply_migrations
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


raw_app = typer.Typer(no_args_is_help=True, help="Raw store: stored response bodies.")
app.add_typer(raw_app, name="raw")


@raw_app.command("prune")
def raw_prune(
    keep: int = typer.Option(2, min=0, help="Blobs to keep per bulk dataset."),
    dry_run: bool = typer.Option(False, "--dry-run", help="List candidates, change nothing."),
) -> None:
    """Delete bulk blobs older than the newest KEEP per bulk dataset; mark their rows pruned."""
    with _db_session(apply_migrations=True) as con:
        candidates = raw.prune(con, keep=keep, dry_run=dry_run)
    blobs = {c.blob_path for c in candidates}
    size = sum({c.blob_path: c.bytes for c in candidates}.values())
    for c in candidates:
        typer.echo(f"{c.raw_id}\t{c.dataset}\t{c.fetched_at:%Y-%m-%d %H:%M}\t{c.blob_path}")
    if dry_run:
        typer.echo(f"dry run: {len(candidates)} candidates, {len(blobs)} blobs, {size} bytes")
    else:
        typer.echo(f"pruned {len(candidates)} rows, {len(blobs)} blobs, {size} bytes")


http_app = typer.Typer(no_args_is_help=True, help="HTTP: one archived request through the client.")
app.add_typer(http_app, name="http")


@http_app.command("get")
def http_get(
    url: str,
    source: str = typer.Option("manual", help="Raw store source (blob directory)."),
    dataset: str = typer.Option("http_get", help="Raw response dataset name."),
) -> None:
    """GET URL with rate limit and retries, archive the response, print status, raw_id, bytes."""
    with _db_session(apply_migrations=True) as con, usfin_http.Client(con) as client:
        try:
            raw_id, response = client.get(url, source=source, dataset=dataset)
        except usfin_http.HttpError as exc:
            typer.echo(f"error: {exc}", err=True)
            if exc.raw_id is not None:
                typer.echo(f"raw_id: {exc.raw_id}", err=True)
            raise typer.Exit(1) from None
    typer.echo(f"status: {response.status_code}")
    typer.echo(f"raw_id: {raw_id}")
    typer.echo(f"bytes: {len(response.content)}")
