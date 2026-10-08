import json
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, timedelta

import duckdb
import typer
import uvicorn

from usfin import __version__, alerts, calendars, jobs, raw, scheduler, store
from usfin import http as usfin_http
from usfin.settings import load_settings
from usfin.sources import bls, fiscal, treasurydirect

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


@app.command("schedule")
def schedule(
    at: str | None = typer.Option(
        None, help="List fires after this time (YYYY-MM-DDTHH:MM, New York); default now."
    ),
) -> None:
    """Print each scheduled job's cron, calendar and next 5 fires on its calendar days (ET)."""
    if at is None:
        after = datetime.now(scheduler.ET)
    else:
        try:
            after = datetime.fromisoformat(at)
        except ValueError:
            raise typer.BadParameter(f"{at!r} is not an ISO date-time", param_hint="--at") from None
        if after.tzinfo is None:
            after = after.replace(tzinfo=scheduler.ET)
    settings = load_settings()
    cals = calendars.load_calendars(settings.config_dir)
    registered = set(jobs.load_registry().names())
    for name, entry in scheduler.load_schedule(settings.config_dir).items():
        note = "" if name in registered else "  (not registered)"
        typer.echo(f"{name}  {entry.cron}  {entry.calendar}{note}")
        for fire in scheduler.next_fires(entry, cals, after, n=5):
            typer.echo(f"  {fire.astimezone(scheduler.ET):%Y-%m-%d %H:%M %a}")


@app.command("serve")
def serve(
    host: str = typer.Option("127.0.0.1", help="Bind address (keep it local)."),
    port: int = typer.Option(8660, help="Port."),
) -> None:
    """Run the scheduler (with startup catch-up) and the local web app until stopped."""
    from usfin.web.app import create_app

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    sched = scheduler.Scheduler(load_settings(), jobs.load_registry())
    uvicorn.run(create_app(sched), host=host, port=port)


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


fd_app = typer.Typer(no_args_is_help=True, help="Treasury Fiscal Data.")
app.add_typer(fd_app, name="fd")


@fd_app.command("probe")
def fd_probe(
    name: str = typer.Argument(..., help="Registry dataset name (config/fiscal_data.yaml)."),
    since: str | None = typer.Option(
        None, help="First date (YYYY-MM-DD) on the dataset's date field; default lookback_days before today (ET)."
    ),
) -> None:
    """Fetch NAME from SINCE (all pages, archived as fd_probe) and print rows, columns, types."""
    settings = load_settings()
    datasets = fiscal.load_datasets(settings.config_dir)
    if name not in datasets:
        known = ", ".join(datasets) or "none configured"
        raise typer.BadParameter(f"unknown dataset {name!r} (known: {known})", param_hint="NAME")
    dataset = datasets[name]
    if since is None:
        today = datetime.now(jobs.ET).date()
        start = today - timedelta(days=dataset.lookback_days)
    else:
        try:
            start = date.fromisoformat(since)
        except ValueError:
            raise typer.BadParameter(
                f"{since!r} is not a YYYY-MM-DD date", param_hint="--since"
            ) from None
    with _db_session(apply_migrations=True) as con, usfin_http.Client(con) as client:
        try:
            pages = fiscal.fetch(client, dataset, start, raw_dataset="fd_probe")
        except (usfin_http.HttpError, fiscal.FiscalError) as exc:
            typer.echo(f"error: {exc}", err=True)
            raise typer.Exit(1) from None
    rows = [row for page in pages for row in page.rows]
    types = pages[-1].types
    typer.echo(f"{dataset.name}: {fiscal.page_url(dataset, start, page=1)}")
    typer.echo(f"rows: {len(rows)} (pages {len(pages)}, raw_ids {[p.raw_id for p in pages]})")
    if rows:
        dates = [r[dataset.date_field] for r in rows if r.get(dataset.date_field) is not None]
        if dates:
            typer.echo(f"{dataset.date_field}: {min(dates)} .. {max(dates)}")
    typer.echo(f"columns: {len(types)}")
    for col, fd_type in types.items():
        sample = rows[-1].get(col) if rows else None
        typer.echo(f"{col}\t{fd_type}\t{fiscal.sql_type(fd_type)}\t{sample!r}")


td_app = typer.Typer(no_args_is_help=True, help="TreasuryDirect.")
app.add_typer(td_app, name="td")


@td_app.command("backfill")
def td_backfill(
    start: str = typer.Option(
        treasurydirect.EARLIEST_AUCTION.isoformat(),
        "--from",
        help="First auction date (YYYY-MM-DD); default the earliest one upstream has.",
    ),
) -> None:
    """Load the auction history: one `search` per month from --from to today (resumable)."""
    try:
        first = date.fromisoformat(start)
    except ValueError:
        raise typer.BadParameter(f"{start!r} is not a YYYY-MM-DD date", param_hint="--from") from None
    settings = load_settings()
    years: dict[int, list[int]] = {}

    def progress(lo: date, hi: date, fetched: bool, records: int, rows: int) -> None:
        year = years.setdefault(lo.year, [0, 0, 0, 0])
        year[0 if fetched else 1] += 1
        year[2] += records
        year[3] += rows
        if hi.month == 12 or hi == today:
            f, s, rec, r = year
            typer.echo(f"{lo.year}: fetched {f}, skipped {s}, records {rec}, rows {r}")

    now = treasurydirect._now()
    today = now.astimezone(jobs.ET).date()
    started = time.monotonic()
    with _db_session(apply_migrations=True) as con:
        try:
            result = treasurydirect.backfill(
                con, settings=settings, start=first, now=now, progress=progress
            )
        except (usfin_http.HttpError, treasurydirect.TreasuryDirectError) as exc:
            typer.echo(f"error: {exc} (rerun to resume)", err=True)
            raise typer.Exit(1) from None
    typer.echo(
        f"windows {result.windows} (fetched {result.fetched}, skipped {result.skipped}),"
        f" records {result.records}, rows {result.rows} in {time.monotonic() - started:.1f} s"
    )


bls_app = typer.Typer(no_args_is_help=True, help="BLS: series config and the Public Data API.")
app.add_typer(bls_app, name="bls")


@bls_app.command("validate")
def bls_validate() -> None:
    """Fetch every config/bls.yaml series (this year and last) in one query; check each has data."""
    settings = load_settings()
    series = bls.load_series(settings.config_dir)
    if not series:
        typer.echo("no series in bls.yaml", err=True)
        raise typer.Exit(1)
    key = settings.bls_api_key
    typer.echo(f"api: {'v2 (BLS_API_KEY set)' if key else 'v1 (no BLS_API_KEY)'}")
    year = bls._today().year
    with _db_session(apply_migrations=True) as con, usfin_http.Client(con) as client:
        try:
            raw_id, doc = bls.fetch(client, list(series), year - 1, year, api_key=key)
        except (usfin_http.HttpError, bls.BlsError) as exc:
            typer.echo(f"error: {exc}", err=True)
            raise typer.Exit(1) from None
        checks = bls.validate(doc, list(series))
        bls.upsert_series(con, [series[c.series_id] for c in checks if c.ok])
    typer.echo(f"raw_id: {raw_id}")
    for c in checks:
        if c.ok:
            latest = f"{c.latest[0]} {c.latest[1]}"
            typer.echo(f"{c.series_id}\tok\t{latest}\t{c.count} values")
        else:
            typer.echo(f"{c.series_id}\tFAILED\t{c.message}")
    good = sum(c.ok for c in checks)
    typer.echo(f"{good} of {len(checks)} series ok")
    if good != len(checks):
        raise typer.Exit(1)


alerts_app = typer.Typer(help="Alerts: data-quality alerts raised after job runs.")
app.add_typer(alerts_app, name="alerts")


@alerts_app.callback(invoke_without_command=True)
def alerts_list(
    ctx: typer.Context,
    include_acked: bool = typer.Option(False, "--all", help="Include acknowledged alerts."),
) -> None:
    """List open alerts, newest first (--all: acknowledged ones too)."""
    if ctx.invoked_subcommand is not None:
        return
    with _db_session(apply_migrations=True) as con:
        rows = alerts.list_alerts(con, include_acked=include_acked)
    if not rows:
        typer.echo("no alerts" if include_acked else "no open alerts")
    for a in rows:
        detail = "" if a["detail"] is None else json.dumps(a["detail"], default=str)
        acked = (
            f"\tacked {a['acknowledged_at']:%Y-%m-%d %H:%M}Z" if a["acknowledged_at"] else ""
        )
        typer.echo(
            f"#{a['id']}\t{a['raised_on']}\t{a['job']}\t{a['rule']}\t{a['subject']}"
            f"\t{detail}{acked}"
        )


@alerts_app.command("ack")
def alerts_ack(alert_id: int = typer.Argument(..., help="Alert id (as listed).")) -> None:
    """Acknowledge an alert so it leaves the open list."""
    with _db_session(apply_migrations=True) as con:
        found = alerts.ack(con, alert_id)
    if not found:
        typer.echo(f"no alert #{alert_id}", err=True)
        raise typer.Exit(1)
    typer.echo(f"acknowledged #{alert_id}")
