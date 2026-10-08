"""FastAPI app for `usfin serve`: the local web UI (jobs, alerts) and its small JSON API.

`create_app(scheduler)` starts the scheduler and runs catch-up in the app's lifespan and shuts it
down on exit (`manage=False` leaves both to the caller). Run-now only queues the job on the
scheduler's single worker; it never runs in the request.

Pages are server-rendered Jinja2 (`templates/`, one stylesheet in `static/style.css`), no
JavaScript. Every table view also answers `?format=csv` (`usfin.web.tables`). Each request opens
its own DB session with a short lock wait (`ui_wait`, default 5 s); past it the page is a 503
"database busy" page. POST endpoints take only path parameters (no form bodies): an HTML form
posting to one (the browser sends `Accept: text/html`) is redirected back to its page with 303,
an API client gets JSON.
"""

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from usfin import __version__, alerts, jobs, store
from usfin.scheduler import ET, Scheduler, next_fires
from usfin.sources import fiscal
from usfin.web import charts, series
from usfin.web.tables import Table, cell, csv_href, respond, wants_csv, wants_html

UI_LOCK_WAIT = 5.0
HTML_ROWS = 500
"""A series page's HTML table shows the newest this many rows; its CSV has every row."""
BUSY = "database busy — another process holds the lock"
HERE = Path(__file__).resolve().parent

JOB_COLUMNS = (
    "job",
    "cron",
    "calendar",
    "next_fire",
    "last_run_id",
    "status",
    "trigger",
    "started_at",
    "finished_at",
    "rows_upserted",
    "error",
    "watermarks",
)
ALERT_COLUMNS = (
    "id",
    "raised_on",
    "raised_at",
    "job",
    "rule",
    "subject",
    "detail",
    "acknowledged_at",
)


def _slug(text: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in text).strip("-")


def make_templates() -> Jinja2Templates:
    templates = Jinja2Templates(directory=HERE / "templates")
    templates.env.filters["cell"] = cell
    return templates


def create_app(
    scheduler: Scheduler, *, manage: bool = True, ui_wait: float = UI_LOCK_WAIT
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if manage:
            scheduler.start()
            scheduler.catchup()
        try:
            yield
        finally:
            if manage:
                scheduler.shutdown()

    app = FastAPI(title="usfin", version=__version__, lifespan=lifespan)
    app.state.scheduler = scheduler
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
    templates = make_templates()
    settings = scheduler.settings

    @contextmanager
    def db() -> Iterator[duckdb.DuckDBPyConnection]:
        with store.session(settings.db_path, wait=ui_wait) as con:
            yield con

    @app.exception_handler(store.DatabaseLocked)
    def database_busy(request: Request, exc: store.DatabaseLocked) -> Response:
        if request.url.path.startswith("/api/"):
            return JSONResponse({"detail": f"{BUSY} ({exc})"}, status_code=503)
        if wants_csv(request):
            return PlainTextResponse(f"{BUSY} ({exc})\n", status_code=503)
        return templates.TemplateResponse(
            request, "busy.html", {"message": BUSY, "error": str(exc)}, status_code=503
        )

    # --- pages ----------------------------------------------------------------------------

    @app.get("/")
    def jobs_page(request: Request) -> Response:
        with db() as con:
            statuses = jobs.status(con, scheduler.registry.names())
        now = scheduler.clock()
        rows = []
        for st in statuses:
            entry = scheduler.entries.get(st.job)
            fires = next_fires(entry, scheduler.cals, now, n=1) if entry else []
            last = st.last_run or {}
            rows.append(
                {
                    "job": st.job,
                    "cron": entry.cron if entry else None,
                    "calendar": entry.calendar if entry else None,
                    "next_fire": fires[0].astimezone(ET) if fires else None,
                    "last_run_id": last.get("id"),
                    "status": last.get("status"),
                    "trigger": last.get("trigger"),
                    "started_at": last.get("started_at"),
                    "finished_at": last.get("finished_at"),
                    "rows_upserted": last.get("rows_upserted"),
                    "error": last.get("error"),
                    "watermarks": "; ".join(f"{k}={v}" for k, v, _ in st.watermarks),
                    "marks": st.watermarks,
                }
            )
        return respond(
            request,
            templates,
            "jobs.html",
            Table("jobs", JOB_COLUMNS, rows),
            title="Jobs",
            nav="jobs",
            csv_href=csv_href(request),
            scheduler_running=scheduler.running,
        )

    @app.get("/alerts")
    def alerts_page(request: Request) -> Response:
        include_acked = request.query_params.get("all") in ("1", "true", "yes")
        with db() as con:
            rows = alerts.list_alerts(con, include_acked=include_acked)
        return respond(
            request,
            templates,
            "alerts.html",
            Table("alerts_all" if include_acked else "alerts", ALERT_COLUMNS, rows),
            title="Alerts",
            nav="alerts",
            csv_href=csv_href(request),
            include_acked=include_acked,
        )

    # --- series browser -------------------------------------------------------------------

    @app.get("/series")
    def series_index(request: Request) -> Response:
        with db() as con:
            rows = series.index(con, settings.config_dir)
        return respond(
            request,
            templates,
            "series_index.html",
            Table("series", series.INDEX_COLUMNS, rows),
            title="Series",
            nav="series",
            csv_href=csv_href(request),
        )

    @app.get("/series/bls/{series_id}")
    def bls_page(series_id: str, request: Request) -> Response:
        raw_as_of = request.query_params.get("as_of") or None
        try:
            as_of = date.fromisoformat(raw_as_of) if raw_as_of else None
        except ValueError:
            raise HTTPException(400, f"as_of must be YYYY-MM-DD, got {raw_as_of!r}") from None
        with db() as con:
            meta = series.bls_series(con, settings.config_dir, series_id)
            if meta is None:
                raise HTTPException(404, f"unknown BLS series {series_id!r}")
            rows = series.bls_rows(con, meta, as_of)
            first_seen = series.bls_first_seen(con, series_id) if not rows else None
        dated = [r for r in rows if r["period_date"] is not None]
        title = f"{meta.title} ({meta.units})" if meta.units else meta.title
        chart = charts.line_chart(
            series.points(dated, "period_date", "number"),
            title=f"{series_id}: {title}" + (f", as of {as_of}" if as_of else ""),
            y_label=meta.units or series_id,
            x_label="Month",
        )
        name = f"bls_{series_id}" + (f"_as_of_{as_of}" if as_of else "")
        return respond(
            request,
            templates,
            "series_bls.html",
            Table(name, series.BLS_COLUMNS, rows),
            title=series_id,
            nav="series",
            csv_href=csv_href(request),
            meta=meta,
            as_of=as_of,
            first_seen=first_seen,
            chart=chart,
            rows_newest=list(reversed(rows)),
        )

    @app.get("/series/fiscal/{name}")
    def fiscal_page(name: str, request: Request) -> Response:
        dataset = fiscal.browsable(settings.config_dir).get(name)
        if dataset is None:
            raise HTTPException(404, f"unknown Fiscal Data dataset {name!r}")
        params = request.query_params
        context = {
            "title": name,
            "nav": "series",
            "dataset": dataset,
            "csv_href": csv_href(request),
        }
        with db() as con:
            shape = series.fiscal_shape(con, dataset)
            if shape is None:
                return respond(
                    request, templates, "series_fiscal.html", Table(name, ()), shape=None, **context
                )
            pick = {d: params[d] for d in shape.dims if d in params}
            if not pick:
                pick = shape.default_pick() or {}
            picked = len(pick) == len(shape.dims)
            field = params.get("field") or shape.default_field()
            if picked and field not in shape.fields:
                raise HTTPException(400, f"{name} has no numeric field {field!r}")
            if not picked:
                rows = series.fiscal_series_list(con, shape)
                for r in rows:
                    r["href"] = f"/series/fiscal/{name}?" + urlencode({d: r[d] for d in shape.dims})
                cols = (
                    *shape.dims,
                    *([shape.label] if shape.label else []),
                    "rows",
                    "first",
                    "last",
                )
                return respond(
                    request,
                    templates,
                    "series_fiscal.html",
                    Table(f"{name}_series", cols, rows),
                    shape=shape,
                    picked=False,
                    **context,
                )
            rows = series.fiscal_rows(con, shape, pick)
        label = " · ".join(pick.values())
        chart = charts.line_chart(
            series.points(rows, shape.date_field, field),
            title=f"{name}{': ' + label if label else ''}: {field}",
            y_label=field,
            x_label=shape.date_field,
        )
        cols = [
            shape.date_field,
            *shape.dims,
            *([shape.label] if shape.label else []),
            *shape.fields,
        ]
        stem = "_".join([name, *(_slug(v) for v in pick.values())])
        return respond(
            request,
            templates,
            "series_fiscal.html",
            Table(stem, cols, rows),
            shape=shape,
            picked=True,
            pick=pick,
            field=field,
            chart=chart,
            field_href=lambda f: f"/series/fiscal/{name}?" + urlencode({**pick, "field": f}),
            html_rows=HTML_ROWS,
            **context,
        )

    @app.get("/series/auctions")
    def auctions_page(request: Request) -> Response:
        params = request.query_params
        typ, term, kind = params.get("type"), params.get("term"), params.get("kind", "nominal")
        context = {"title": "Auctions", "nav": "series", "csv_href": csv_href(request)}
        with db() as con:
            if not (typ and term):
                rows = series.auction_series_list(con)
                for r in rows:
                    r["href"] = "/series/auctions?" + urlencode(
                        {"type": r["security_type"], "term": r["security_term"], "kind": r["kind"]}
                    )
                return respond(
                    request,
                    templates,
                    "series_auctions.html",
                    Table("auctions_series", series.AUCTION_LIST_COLUMNS, rows),
                    picked=False,
                    **context,
                )
            rows = series.auction_rows(con, typ, term, kind)
        field = params.get("field") or series.auction_default_field(typ, kind)
        if field not in series.AUCTION_FIELDS:
            raise HTTPException(400, f"auctions have no field {field!r}")
        label = f"{term} {typ}" + ("" if kind == "nominal" else f" ({kind})")
        chart = charts.line_chart(
            series.points(rows, "auction_date", field),
            title=f"Treasury auctions, {label}: {field}",
            y_label=field,
            x_label="Auction date",
        )
        base = {"type": typ, "term": term, "kind": kind}
        return respond(
            request,
            templates,
            "series_auctions.html",
            Table(f"auctions_{_slug(typ)}_{_slug(term)}_{kind}", series.AUCTION_COLUMNS, rows),
            picked=True,
            label=label,
            field=field,
            fields=series.AUCTION_FIELDS,
            chart=chart,
            field_href=lambda f: "/series/auctions?" + urlencode({**base, "field": f}),
            html_rows=HTML_ROWS,
            **context,
        )

    # --- API ------------------------------------------------------------------------------

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": __version__,
            "scheduler": "running" if scheduler.running else "stopped",
            "scheduled": sorted(scheduler.entries),
            "pending_db_locked": len(scheduler.pending_locked),
        }

    @app.post("/api/jobs/{job}/run", status_code=202, response_model=None)
    def run_now(job: str, request: Request) -> dict[str, Any] | Response:
        try:
            queued = scheduler.run_now(job, "manual")
        except jobs.UnknownJob as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        if wants_html(request):
            return RedirectResponse("/", status_code=303)
        return {"job": job, "enqueued": queued, "trigger": "manual"}

    @app.post("/api/alerts/{alert_id}/ack", response_model=None)
    def ack(alert_id: int, request: Request) -> dict[str, Any] | Response:
        with db() as con:
            found = alerts.ack(con, alert_id)
        if not found:
            raise HTTPException(status_code=404, detail=f"no alert #{alert_id}")
        if wants_html(request):
            return RedirectResponse("/alerts", status_code=303)
        return {"id": alert_id, "acknowledged": True}

    return app
