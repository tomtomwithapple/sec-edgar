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
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from usfin import __version__, alerts, jobs, store
from usfin.scheduler import ET, Scheduler, next_fires
from usfin.web.tables import Table, cell, csv_href, respond, wants_csv, wants_html

UI_LOCK_WAIT = 5.0
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
