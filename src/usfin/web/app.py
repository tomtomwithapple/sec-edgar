"""FastAPI app for `usfin serve`: health and run-now (pages come with the UI slices).

`create_app(scheduler)` starts the scheduler and runs catch-up in the app's lifespan and shuts it
down on exit (`manage=False` leaves both to the caller). Run-now only queues the job on the
scheduler's single worker; it never runs in the request.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from usfin import __version__, jobs
from usfin.scheduler import Scheduler


def create_app(scheduler: Scheduler, *, manage: bool = True) -> FastAPI:
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

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": __version__,
            "scheduler": "running" if scheduler.running else "stopped",
            "scheduled": sorted(scheduler.entries),
            "pending_db_locked": len(scheduler.pending_locked),
        }

    @app.post("/api/jobs/{job}/run", status_code=202)
    def run_now(job: str) -> dict[str, Any]:
        try:
            queued = scheduler.run_now(job, "manual")
        except jobs.UnknownJob as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        return {"job": job, "enqueued": queued, "trigger": "manual"}

    return app
