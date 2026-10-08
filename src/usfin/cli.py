from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date

import duckdb
import typer

from usfin import __version__, calendars, jobs, raw, store
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


@app.command("calendar")
def calendar(day: str = typer.Argument(..., help="Date as YYYY-MM-DD.")) -> None:
    """Print whether DAY is a federal, bond and NYSE business day."""
    try:
        d = date.fromisoformat(day)
    except ValueError:
        raise typer.BadParameter(f"{day!r} is not a YYYY-MM-DD date", param_hint="DAY") from None
    cals = calendars.load_calendars(load_settings().config_dir)
    names = cals.holiday_names(d)
    typer.echo(f"{d:%Y-%m-%d %a}" + (f" ({'; '.join(names)})" if names else ""))
    for name in ("federal", "bond", "nyse"):
        typer.echo(f"{name}: {'open' if cals.is_day(name, d) else 'closed'}")


@app.command("run")
def run(job: str = typer.Argument(..., help="Registered job name.")) -> None:
    """Run JOB now in this process (manual trigger; the calendar is not consulted)."""
    registry = jobs.load_registry()
    try:
        registry.get(job)
    except jobs.UnknownJob as exc:
        known = ", ".join(registry.names()) or "none registered"
        raise typer.BadParameter(f"{exc} (known: {known})", param_hint="JOB") from None
    result = jobs.run_job(job, registry=registry)
    if not _echo_run(result):
        raise typer.Exit(1)


def _echo_run(result: jobs.RunResult, depth: int = 0) -> bool:
    """Print a run and its chained runs; return whether all of them were ok."""
    run_id = f"#{result.run_id}" if result.run_id is not None else "(not logged)"
    suffix = " (chained)" if depth else ""
    typer.echo(
        f"{'  ' * depth}{result.job} {run_id} {result.status} rows {result.rows_upserted}{suffix}"
    )
    ok = result.status == "ok"
    if not ok:
        typer.echo(f"{'  ' * depth}error: {result.error}", err=True)
    for child in result.chained:
        ok = _echo_run(child, depth + 1) and ok
    return ok


@app.command("reparse")
def reparse(
    job: str = typer.Argument(..., help="Registered job name."),
    since: str | None = typer.Option(
        None, help="Only raw responses fetched on or after this New York date (YYYY-MM-DD)."
    ),
) -> None:
    """Rebuild JOB's normalized rows from stored raw responses (no HTTP request is made)."""
    since_date = None
    if since is not None:
        try:
            since_date = date.fromisoformat(since)
        except ValueError:
            raise typer.BadParameter(
                f"{since!r} is not a YYYY-MM-DD date", param_hint="--since"
            ) from None
    registry = jobs.load_registry()
    try:
        target = registry.get(job)
    except jobs.UnknownJob as exc:
        known = ", ".join(registry.names()) or "none registered"
        raise typer.BadParameter(f"{exc} (known: {known})", param_hint="JOB") from None
    settings = load_settings()
    with _db_session(apply_migrations=True) as con:
        try:
            result = jobs.reparse(con, target, settings=settings, since=since_date)
        except jobs.ReparseFailed as exc:
            typer.echo(f"{job} reparse failed, nothing changed: {exc}", err=True)
            raise typer.Exit(1) from None
    if result.skipped:
        typer.echo(f"skipped {len(result.skipped)} pruned: {', '.join(map(str, result.skipped))}")
    typer.echo(
        f"{job} reparsed {len(result.raw_ids)} raw responses, rows {result.rows}"
        + (f", deleted {result.deleted} versioned rows" if target.versioned else "")
    )


@app.command("jobs")
def list_jobs() -> None:
    """List every job with its last run and watermarks."""
    names = jobs.load_registry().names()
    with _db_session(apply_migrations=True) as con:
        statuses = jobs.status(con, names)
    for st in statuses:
        last = st.last_run
        if last is None:
            typer.echo(f"{st.job}\tnever run")
        else:
            finished = f"{last['finished_at']:%Y-%m-%d %H:%M:%S}" if last["finished_at"] else "-"
            typer.echo(
                f"{st.job}\t#{last['id']} {last['status']} ({last['trigger']})"
                f" started {last['started_at']:%Y-%m-%d %H:%M:%S}Z finished {finished}Z"
                f" rows {last['rows_upserted']}"
            )
            if last["error"]:
                for line in last["error"].splitlines():
                    typer.echo(f"  ! {line}")
        for key, value, updated_at in st.watermarks:
            typer.echo(f"  {key} = {value}  (updated {updated_at:%Y-%m-%d %H:%M:%S}Z)")


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
