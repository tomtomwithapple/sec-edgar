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
from urllib.parse import quote, urlencode

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from usfin import __version__, alerts, jobs, store
from usfin import http as usfin_http
from usfin.scheduler import ET, Scheduler, next_fires
from usfin.settings import ConfigError
from usfin.sources import fiscal
from usfin.sources.edgar import concepts as edgar_concepts
from usfin.sources.edgar import frames as edgar_frames
from usfin.web import charts, series
from usfin.web import edgar as web_edgar
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
    templates.env.filters["num"] = web_edgar.num
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

    # --- EDGAR company and screen ---------------------------------------------------------

    @app.get("/edgar")
    def edgar_lookup(request: Request) -> Response:
        q = (request.query_params.get("q") or "").strip()
        if q:
            return RedirectResponse(f"/edgar/{quote(q, safe='')}", status_code=303)
        return templates.TemplateResponse(
            request, "edgar_index.html", {"title": "EDGAR", "nav": "edgar"}
        )

    @app.get("/edgar/{key:path}")
    def edgar_company(key: str, request: Request) -> Response:
        params = request.query_params
        cmap = edgar_concepts.load_concepts(settings.config_dir)
        which = params.get("table", "filings")
        if wants_csv(request) and which not in web_edgar.TABLES:
            raise HTTPException(400, f"table must be one of {', '.join(web_edgar.TABLES)}")
        chart_concept = params.get("chart", "revenue")
        if chart_concept not in cmap.concepts:
            raise HTTPException(400, f"unknown concept {chart_concept!r}")
        try:
            page = int(params.get("page", "1"))
        except ValueError:
            page = 0
        if page < 1:
            raise HTTPException(400, "page must be a whole number >= 1")
        with db() as con:
            if not key.isdigit():
                cik = web_edgar.resolve(con, key)
                if cik is None:
                    raise HTTPException(404, f"no EDGAR company with ticker {key!r}")
                url = f"/edgar/{cik}" + (f"?{request.url.query}" if request.url.query else "")
                return RedirectResponse(url, status_code=307)
            cik = int(key)
            if wants_csv(request):
                return web_edgar.csv_table(con, cik, which, cmap)
            head = web_edgar.company(con, cik)
            if head is None:
                raise HTTPException(404, f"nothing stored for CIK {cik}")
            offset = (page - 1) * web_edgar.FILINGS_PAGE
            filing_rows = web_edgar.filings(con, cik, limit=web_edgar.FILINGS_PAGE, offset=offset)
            per_frame = web_edgar.frame_values(con, cik, cmap)
            n_insider, insider_rows = web_edgar.insider(con, cik, limit=web_edgar.RECENT)
            n_stakes, stake_rows = web_edgar.stakes(con, cik, limit=web_edgar.RECENT)
        facts = web_edgar.key_facts(per_frame, cmap)
        spec = cmap[chart_concept]
        how, points = web_edgar.chart_series(per_frame, chart_concept)
        chart = charts.line_chart(
            points,
            title=f"{head['title'] or cik}: {chart_concept} ({how}, {spec.unit})",
            y_label=f"{chart_concept}, {spec.unit}",
            x_label="Year" if how == "annual" else "Quarter end",
        )
        for r in filing_rows:
            r["href"] = web_edgar.filing_href(cik, r["accession_no"])
        charted = {r["concept"] for r in per_frame}

        def page_href(n: int) -> str:
            return f"/edgar/{cik}?" + urlencode({**params, "page": n})

        def table_csv(name: str) -> str:
            return f"/edgar/{cik}?" + urlencode({"format": "csv", "table": name})

        pages = max(1, -(-head["filings"] // web_edgar.FILINGS_PAGE))
        return templates.TemplateResponse(
            request,
            "edgar_company.html",
            {
                "title": head["title"] or f"CIK {cik}",
                "nav": "edgar",
                "head": head,
                "cik": cik,
                "filings": filing_rows,
                "page": page,
                "pages": pages,
                "page_href": page_href,
                "facts": facts,
                "chart": chart,
                "chart_concept": chart_concept,
                "chart_how": how,
                "chart_links": [
                    (n, f"/edgar/{cik}?" + urlencode({"chart": n}))
                    for n in cmap.concepts
                    if n in charted
                ],
                "insider": insider_rows,
                "n_insider": n_insider,
                "stakes": stake_rows,
                "n_stakes": n_stakes,
                "table_csv": table_csv,
                "cmap": cmap,
                "screen_href": lambda concept, period: (
                    "/screen?" + urlencode({"concept": concept, "period": period})
                ),
            },
        )

    @app.get("/screen")
    def screen_page(request: Request) -> Response:
        params = request.query_params
        cmap = edgar_concepts.load_concepts(settings.config_dir)
        concept_name = params.get("concept") or ""
        period = (params.get("period") or "").strip().upper()
        source = params.get("source", "xbrl")
        context: dict[str, Any] = {
            "title": "Screen",
            "nav": "screen",
            "cmap": cmap,
            "concept_name": concept_name,
            "period": period,
            "source": source,
            "screen_rows": web_edgar.SCREEN_ROWS,
            "csv_href": csv_href(request),
        }
        if not (concept_name and period):
            return respond(request, templates, "screen.html", Table("screen", ()), **context)
        if concept_name not in cmap.concepts:
            raise HTTPException(400, f"unknown concept {concept_name!r}")
        if source not in ("xbrl", "frames"):
            raise HTTPException(400, "source must be xbrl or frames")
        try:
            edgar_frames.check_period(period)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        spec = cmap[concept_name]
        context["spec"] = spec
        context["frames_href"] = lambda tag: (
            "/screen?"
            + urlencode({"concept": concept_name, "period": period, "source": "frames", "tag": tag})
        )
        if source == "xbrl":
            with db() as con:
                found = edgar_concepts.screen(con, spec, period)
            rows = [{"rank": i, **vars(r)} for i, r in enumerate(found, 1)]
            by_tag = {t: sum(r.tag == t for r in found) for t in spec.tags}
            return respond(
                request,
                templates,
                "screen.html",
                Table(f"screen_{concept_name}_{period}", web_edgar.SCREEN_COLUMNS, rows),
                by_tag=by_tag,
                **context,
            )
        tag = params.get("tag") or spec.tags[0]
        if tag not in spec.tags:
            raise HTTPException(400, f"{tag!r} is not a tag of {concept_name}")
        context["tag"] = tag
        name = f"frames_{tag}_{_slug(spec.unit)}_{period}"
        try:
            with db() as con, usfin_http.Client(con, settings=settings) as client:
                frame = edgar_frames.get_frame(
                    con, client, taxonomy=spec.taxonomy, tag=tag, unit=spec.unit, period=period,
                    now=scheduler.clock(), raw_dir=settings.raw_dir,
                )  # fmt: skip
        except (ConfigError, usfin_http.HttpError, edgar_frames.FramesError) as exc:
            status = 503 if isinstance(exc, ConfigError) else 502
            if isinstance(exc, edgar_frames.FramesError) and " answered 404" in str(exc):
                status = 404
            if wants_csv(request):
                return PlainTextResponse(f"{exc}\n", status_code=status)
            return templates.TemplateResponse(
                request,
                "screen.html",
                {"table": Table(name, ()), "frames_error": str(exc), **context},
                status_code=status,
            )
        points = sorted(frame.points, key=lambda p: (-p.value, p.cik))
        rows = [{"rank": i, **vars(p)} for i, p in enumerate(points, 1)]
        return respond(
            request,
            templates,
            "screen.html",
            Table(name, web_edgar.FRAME_COLUMNS, rows),
            frame=frame,
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
