import json
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from typing import Annotated

import duckdb
import typer
import uvicorn

from usfin import __version__, alerts, calendars, jobs, raw, scheduler, store
from usfin import http as usfin_http
from usfin.settings import ConfigError, load_settings
from usfin.sources import bls, bls_calendar, fiscal, treasurydirect
from usfin.sources.edgar import bulk as edgar_bulk
from usfin.sources.edgar import companyfacts as edgar_companyfacts
from usfin.sources.edgar import concepts as edgar_concepts
from usfin.sources.edgar import frames as edgar_frames
from usfin.sources.edgar import submissions as edgar_submissions
from usfin.sources.edgar import tickers as edgar_tickers

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
        raise typer.BadParameter(
            f"{start!r} is not a YYYY-MM-DD date", param_hint="--from"
        ) from None
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
        # the response is the `bls` job's raw dataset: store its versions too, so the job and
        # `usfin reparse bls` agree on when each value was first seen
        con.execute("BEGIN TRANSACTION")
        try:
            stored = bls.store_observations(con, raw_id, raw_dir=settings.raw_dir, config=series)
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    typer.echo(f"raw_id: {raw_id} ({stored.rows} new observation versions)")
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


@bls_app.command("releases")
def bls_releases(
    next_n: int = typer.Option(5, "--next", min=1, help="How many releases to list."),
    start: str | None = typer.Option(
        None, "--from", help="First New York date (YYYY-MM-DD); default today."
    ),
    every: bool = typer.Option(
        False, "--all", help="Every release, not only those of config/bls.yaml series."
    ),
) -> None:
    """List the next BLS releases stored by job `bls_calendar`, with the series they publish."""
    try:
        first = date.fromisoformat(start) if start else datetime.now(scheduler.ET).date()
    except ValueError:
        raise typer.BadParameter(
            f"{start!r} is not a YYYY-MM-DD date", param_hint="--from"
        ) from None
    series = bls.load_series(load_settings().config_dir)
    with _db_session(apply_migrations=True) as con:
        count, lo, hi = bls_calendar.coverage(con)
        found = bls_calendar.next_releases(con, series, start=first, n=next_n, every=every)
    if not count:
        typer.echo("no releases stored; run `usfin run bls_calendar`", err=True)
        raise typer.Exit(1)
    typer.echo(f"{count} releases stored, {lo}..{hi}")
    for release, ids in found:
        at = release.released_at.replace(tzinfo=UTC).astimezone(scheduler.ET)
        typer.echo(
            f"{release.release_date:%Y-%m-%d %a} {at:%H:%M}  {release.release_name}"
            + (f"  {', '.join(ids)}" if ids else "")
        )


edgar_app = typer.Typer(no_args_is_help=True, help="SEC EDGAR.")
app.add_typer(edgar_app, name="edgar")


@edgar_app.command("company")
def edgar_company(
    query: str = typer.Argument(..., help="Ticker in any spelling (BRK-B, BRK.B) or a CIK."),
) -> None:
    """Show a company's CIK, title and every ticker stored by job `edgar_tickers`."""
    with _db_session(apply_migrations=True) as con:
        rows = edgar_tickers.lookup(con, query)
    if not rows:
        typer.echo(f"no EDGAR company for {query!r} (run `usfin run edgar_tickers`?)", err=True)
        raise typer.Exit(1)
    last = None
    for cik, ticker, title, norm, active, updated_at in rows:
        if cik != last:
            typer.echo(f"CIK {cik:010d}  {title}")
            last = cik
        typer.echo(
            f"  {ticker:<10} {norm:<10} {'active' if active else 'inactive':<8}"
            f"  updated {updated_at:%Y-%m-%d %H:%M:%S}Z"
        )


@edgar_app.command("submissions")
def edgar_submissions_cmd(
    cik: Annotated[
        list[str], typer.Option("--cik", help="Company CIK (digits); repeat for more.")
    ],
) -> None:
    """Run job `edgar_submissions` now for the given CIKs (recent filings and every older page)."""
    ciks = _parse_ciks(cik)
    result, ok, started = _run_for_ciks(edgar_submissions.for_ciks(ciks))
    with _db_session() as con:
        for c in dict.fromkeys(ciks):
            requests = _requests_since(con, result, edgar_submissions.RAW_DATASET, c)
            n, lo, hi = con.execute(
                "SELECT count(*), min(filed_at), max(filed_at) FROM edgar_filing WHERE cik = ?",
                [c],
            ).fetchone()
            pages = con.execute(
                f"SELECT count(*) FROM {edgar_submissions.PAGES} WHERE cik = ?", [c]
            ).fetchone()[0]
            span = f" {lo}..{hi}" if n else ""
            typer.echo(f"CIK {c:010d}  filings {n}{span}  pages {pages}  requests {requests}")
    typer.echo(f"{time.monotonic() - started:.1f} s")
    if not ok:
        raise typer.Exit(1)


@edgar_app.command("facts")
def edgar_facts_cmd(
    cik: Annotated[
        list[str], typer.Option("--cik", help="Company CIK (digits); repeat for more.")
    ],
) -> None:
    """Run job `edgar_companyfacts` now for the given CIKs (us-gaap and dei XBRL facts)."""
    ciks = _parse_ciks(cik)
    result, ok, started = _run_for_ciks(edgar_companyfacts.for_ciks(ciks))
    with _db_session() as con:
        for c in dict.fromkeys(ciks):
            requests = _requests_since(con, result, edgar_companyfacts.RAW_DATASET, c)
            n, tags, lo, hi = con.execute(
                f"SELECT count(*), count(DISTINCT tag), min(filed), max(filed)"
                f" FROM {edgar_companyfacts.TABLE} WHERE cik = ?",
                [c],
            ).fetchone()
            span = f"  filed {lo}..{hi}" if n else ""
            typer.echo(f"CIK {c:010d}  facts {n}  tags {tags}{span}  requests {requests}")
    typer.echo(f"{time.monotonic() - started:.1f} s")
    if not ok:
        raise typer.Exit(1)


@edgar_app.command("backfill")
def edgar_backfill_cmd(
    dataset: str = typer.Argument(..., help="submissions or companyfacts."),
    limit: int | None = typer.Option(
        None, "--limit", min=1, help="Parse at most N members, spread evenly over the ZIP."
    ),
    cik: Annotated[
        list[str] | None, typer.Option("--cik", help="Only this company's members; repeatable.")
    ] = None,
    estimate: bool = typer.Option(
        False, "--estimate", help="Print the full-load projection from the newest sample; no request."
    ),
    check: bool = typer.Option(
        False, "--check", help="HEAD the ZIP and print the free-disk verdict; download nothing."
    ),
    reuse: bool = typer.Option(
        False, "--reuse", help="Parse the newest stored ZIP instead of downloading it again."
    ),
) -> None:
    """Load an EDGAR bulk ZIP member by member (guarded by USFIN_MIN_FREE_GB; resumable)."""
    spec = edgar_bulk.DATASETS.get(dataset)
    if spec is None:
        raise typer.BadParameter(
            f"{dataset!r} is not one of {', '.join(edgar_bulk.DATASETS)}", param_hint="DATASET"
        )
    ciks = _parse_ciks(cik) if cik else None
    settings = load_settings()
    with _db_session(apply_migrations=True) as con:
        if estimate:
            est = edgar_bulk.latest_estimate(con, spec)
            if est is None:
                typer.echo(
                    f"no estimate for {dataset}; sample first:"
                    f" usfin edgar backfill {dataset} --limit 200",
                    err=True,
                )
                raise typer.Exit(1)
            for line in edgar_bulk.estimate_lines(
                est, settings, free=edgar_bulk.free_bytes(settings)
            ):
                typer.echo(line)
            return
        with usfin_http.Client(con, settings=settings) as client:
            download_seconds = None
            if reuse:
                raw_id = edgar_bulk.newest_zip(con, spec)
                if raw_id is None:
                    typer.echo(f"no stored {spec.raw_dataset} ZIP to reuse", err=True)
                    raise typer.Exit(1)
                typer.echo(f"reusing raw {raw_id}")
            else:
                try:
                    size = edgar_bulk.head(client, spec)
                except (usfin_http.HttpError, edgar_bulk.BulkError) as exc:
                    typer.echo(f"error: {exc}", err=True)
                    raise typer.Exit(1) from None
                verdict = edgar_bulk.guard(
                    con, settings, spec, size, limit=len(ciks) if ciks else limit
                )
                typer.echo(f"guard: {verdict.describe()}")
                if check:
                    typer.echo("fits" if verdict.ok else "does not fit")
                    raise typer.Exit(0 if verdict.ok else 1)
                if not verdict.ok:
                    typer.echo(f"disk low: {verdict.describe()}; not downloading", err=True)
                    raise typer.Exit(1)
                started = time.monotonic()
                try:
                    raw_id = edgar_bulk.download(client, spec)
                except (usfin_http.HttpError, edgar_bulk.BulkError) as exc:
                    typer.echo(f"error: {exc}", err=True)
                    raise typer.Exit(1) from None
                download_seconds = time.monotonic() - started
                typer.echo(
                    f"downloaded raw {raw_id}: {edgar_bulk.zip_bytes_of(con, raw_id)} bytes"
                    f" in {download_seconds:.1f} s"
                )
        sample = ciks is None
        before = edgar_bulk.db_bytes(con, settings) if sample else 0

        def progress(done: edgar_bulk.Loaded) -> None:
            if done.parsed % edgar_bulk.GUARD_EVERY == 0:
                typer.echo(f"  {done.parsed} members, rows {done.rows}")

        loaded = edgar_bulk.load(
            con, settings, spec, raw_id, limit=limit, ciks=ciks, progress=progress
        )
        typer.echo(
            f"members {loaded.members_total}, parsed {loaded.parsed}, rows {loaded.rows},"
            f" skipped {loaded.skipped}, empty {loaded.empty}, errors {len(loaded.errors)}"
            f" in {loaded.seconds:.1f} s"
        )
        if spec.companies_only:
            typer.echo(f"not in edgar_company {loaded.filtered} members (not loaded)")
        for err in loaded.errors[:5]:
            typer.echo(f"  error: {err}", err=True)
        if sample and loaded.parsed:
            after = edgar_bulk.db_bytes(con, settings)
            # a --limit sample is copied into scratch files to measure its rows; a full load's
            # rows are the whole table (the copies would need as much disk again): not measured
            scratch = (
                edgar_bulk.measure(con, settings, spec, loaded.ciks)
                if limit is not None
                else (None, None, None)
            )
            est = edgar_bulk.Estimate(
                spec.raw_dataset, edgar_bulk._utcnow(), raw_id,
                edgar_bulk.zip_bytes_of(con, raw_id), loaded.members_total, loaded.parsed,
                loaded.member_bytes_total, loaded.member_bytes_parsed, before, after,
                loaded.rows, loaded.seconds, download_seconds, *scratch,
            )  # fmt: skip
            edgar_bulk.record_estimate(con, est)
            typer.echo(f"estimate recorded (db +{after - before} bytes); see --estimate")
    if loaded.stopped is not None:
        typer.echo(
            f"stopped: {loaded.stopped.describe()} after {loaded.parsed} members"
            " (rerun to resume: usfin edgar backfill … --reuse)",
            err=True,
        )
        raise typer.Exit(1)
    if loaded.errors:
        raise typer.Exit(1)


@edgar_app.command("frame")
def edgar_frame_cmd(
    taxonomy: str = typer.Argument(..., help="us-gaap or dei."),
    tag: str = typer.Argument(..., help="XBRL tag, e.g. Revenues."),
    unit: str = typer.Argument(..., help="Unit, e.g. USD, USD-per-shares, shares."),
    period: str = typer.Argument(..., help="CY2025, CY2025Q2, or with I for an instant."),
    limit: int = typer.Option(20, min=0, help="Points to print, largest first (0 = all)."),
) -> None:
    """One concept for one period across companies, from SEC's frames API (cached 24 h)."""
    started = time.monotonic()
    try:
        with _db_session(apply_migrations=True) as con, usfin_http.Client(con) as client:
            frame = edgar_frames.get_frame(
                con, client, taxonomy=taxonomy, tag=tag, unit=unit, period=period,
                now=datetime.now(UTC), raw_dir=client.settings.raw_dir,
            )  # fmt: skip
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="PERIOD") from None
    except (edgar_frames.FramesError, usfin_http.HttpError, ConfigError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(1) from None
    how = "cached" if frame.cached else "fetched"
    typer.echo(
        f"{edgar_frames.key(taxonomy, tag, unit, period)}  {how} {frame.fetched_at:%Y-%m-%d %H:%M}Z"
        f"  raw_id {frame.raw_id}  points {len(frame.points)} (pts {frame.pts})"
    )
    points = sorted(frame.points, key=lambda p: (-p.value, p.cik))
    for p in points[:limit] if limit else points:
        span = f"{p.period_start or ''}..{p.period_end}"
        typer.echo(f"{p.cik:010d}  {p.value:>20,.0f}  {span}  {p.accn}  {p.entity_name}")
    typer.echo(f"{time.monotonic() - started:.1f} s")


@edgar_app.command("screen")
def edgar_screen_cmd(
    concept: str = typer.Argument(..., help="Concept name in config/concepts.yaml, e.g. revenue."),
    period: str = typer.Argument(..., help="CY2025, CY2025Q2, or with I for an instant."),
    limit: int = typer.Option(20, min=0, help="Companies to print, largest first (0 = all)."),
) -> None:
    """One concept for one period across companies from `xbrl_fact`, with the tag used."""
    settings = load_settings()
    try:
        cmap = edgar_concepts.load_concepts(settings.config_dir)
        spec = cmap[concept]
    except ConfigError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(1) from None
    try:
        with _db_session(apply_migrations=True) as con:
            started = time.monotonic()  # the query alone, not opening the DB
            rows = edgar_concepts.screen(con, spec, period)
            elapsed = time.monotonic() - started
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="PERIOD") from None
    for r in rows[:limit] if limit else rows:
        typer.echo(
            f"{r.cik:010d}  {r.value:>20,.2f}  {r.tag:<55}  {r.period_start}..{r.period_end}"
            f"  {r.form:<6} {r.filed}  {r.accn}  {r.name or ''}"
        )
    by_tag = {t: sum(r.tag == t for r in rows) for t in spec.tags}
    typer.echo(
        f"{concept} (concepts.yaml v{cmap.version}, {spec.taxonomy} {spec.unit}) {period}:"
        f" companies {len(rows)}; "
        + ", ".join(f"{t} {n}" for t, n in by_tag.items())
    )
    typer.echo(f"query {elapsed:.2f} s")


def _parse_ciks(cik: list[str]) -> list[int]:
    try:
        ciks = [int(c) for c in cik]
    except ValueError:
        raise typer.BadParameter(f"CIKs are digits, got {cik!r}", param_hint="--cik") from None
    if any(c <= 0 for c in ciks):
        raise typer.BadParameter(f"CIKs are positive, got {cik!r}", param_hint="--cik")
    return ciks


def _run_for_ciks(job: jobs.Job) -> tuple[jobs.RunResult, bool, float]:
    """Run a per-CIK EDGAR job alone (trigger manual); exit 1 when no run could be logged."""
    registry = jobs.Registry()
    registry.register(job)
    started = time.monotonic()
    result = jobs.run_job(job.name, registry=registry, chain=False)
    ok = _echo_run(result)
    if result.run_id is None:
        raise typer.Exit(1)
    return result, ok, started


def _requests_since(con, result: jobs.RunResult, dataset: str, cik: int) -> int:
    """Responses of `dataset` for `cik` archived since the run started."""
    return con.execute(
        "SELECT count(*) FROM raw_response WHERE dataset = ? AND url LIKE ? AND fetched_at >="
        " (SELECT started_at FROM job_run WHERE id = ?)",
        [dataset, f"%/CIK{cik:010d}%", result.run_id],
    ).fetchone()[0]


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
