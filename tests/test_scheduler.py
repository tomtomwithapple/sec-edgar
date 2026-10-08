"""Scheduler and `usfin serve` API: cron from `schedule.yaml` in New York time, the calendar
guard, one worker, catch-up after missed fires, run-now through the queue, `db_locked`.
"""

import json
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from apscheduler.events import EVENT_JOB_MISSED, JobExecutionEvent
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from usfin import cli, jobs, raw, scheduler, store
from usfin.cli import app
from usfin.settings import ConfigError, load_settings
from usfin.web import app as web

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"


def et(day: str, hour: int, minute: int = 0) -> datetime:
    """`day` at hour:minute America/New_York, as an aware UTC datetime."""
    local = datetime.fromisoformat(f"{day}T{hour:02d}:{minute:02d}").replace(
        tzinfo=scheduler.ET
    )
    return local.astimezone(UTC)


THU_1700 = et("2026-10-08", 17)
FRI_2300 = et("2026-10-09", 23)
COLUMBUS_1800 = et("2026-10-12", 18)  # federal closed

SCHEDULE = """
jobs:
  fake:
    cron: "0 17 * * *"
    calendar: federal
    alerts: {}
  other:
    cron: "30 9 * * mon-fri"
    calendar: any
  unscheduled:
    calendar: federal
"""


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    (config / "schedule.yaml").write_text(SCHEDULE, "utf-8")
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
    )


class Fake:
    """One-key job: stores one raw body, one row; records threads and overlap."""

    def __init__(self, name="fake", sleep=0.0, tracker=None):
        self.name = name
        self.sleep = sleep
        self.tracker = tracker
        self.threads: list[str] = []
        self.calls = 0

    def fetch(self, ctx, key):
        self.calls += 1
        self.threads.append(threading.current_thread().name)
        if self.tracker is not None:
            self.tracker.enter()
        try:
            time.sleep(self.sleep)
        finally:
            if self.tracker is not None:
                self.tracker.leave()
        return [
            raw.save(
                ctx.con,
                source="fake",
                dataset="fake_items",
                url=f"fake://{self.name}",
                body=b"{}",
                http_status=200,
                raw_dir=ctx.settings.raw_dir,
            )
        ]

    def parse(self, ctx, raw_id):
        ctx.advance("k", ctx.today)
        return 1

    def job(self):
        return jobs.Job(
            name=self.name, keys=("k",), fetch=self.fetch, parsers={"fake_items": self.parse}
        )


class Overlap:
    def __init__(self):
        self.lock = threading.Lock()
        self.now = 0
        self.max = 0

    def enter(self):
        with self.lock:
            self.now += 1
            self.max = max(self.max, self.now)

    def leave(self):
        with self.lock:
            self.now -= 1


def _registry(*fakes):
    reg = jobs.Registry()
    for fake in fakes:
        reg.register(fake.job())
    return reg


def _runs(settings, job=None):
    with store.session(settings.db_path, wait=0) as con:
        sql = "SELECT * FROM job_run" + (" WHERE job = ?" if job else "") + " ORDER BY id"
        cur = con.execute(sql, [job] if job else [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


@pytest.fixture
def make(settings):
    made = []

    def _make(*fakes, clock=lambda: THU_1700, **kwargs):
        sched = scheduler.Scheduler(settings, _registry(*fakes), clock=clock, **kwargs)
        made.append(sched)
        return sched

    yield _make
    for sched in made:
        sched.shutdown()


# --- schedule.yaml --------------------------------------------------------------------------


def test_load_schedule_reads_cron_and_calendar_of_scheduled_jobs(settings):
    entries = scheduler.load_schedule(settings.config_dir)
    assert set(entries) == {"fake", "other"}  # no cron: not scheduled
    assert entries["fake"] == scheduler.Entry("fake", "0 17 * * *", "federal")
    assert entries["other"].calendar == "any"


def test_repo_schedule_has_fiscal_at_1700_federal():
    entries = scheduler.load_schedule(REPO_CONFIG)
    assert entries["fiscal"] == scheduler.Entry("fiscal", "0 17 * * *", "federal")


@pytest.mark.parametrize(
    "entry, match",
    [
        ('{cron: "0 17 * *", calendar: federal}', "cron"),
        ('{cron: "0 17 * * *", calendar: lunar}', "calendar"),
        ('{cron: "0 3 * * 0", calendar: any}', "day-of-week"),  # APScheduler: 0 = Monday
        ('{cron: "0 3 * * 1-5", calendar: any}', "day-of-week"),
    ],
)
def test_bad_schedule_entries_are_config_errors(tmp_path, entry, match):
    (tmp_path / "schedule.yaml").write_text(f"jobs:\n  x: {entry}\n", "utf-8")
    with pytest.raises(ConfigError, match=match):
        scheduler.load_schedule(tmp_path)


def test_cron_jobs_use_new_york_time_one_worker_coalesce_and_misfire_grace(make):
    sched = make(Fake())
    sched.start(paused=True)
    job = sched.aps.get_job("fake")
    assert str(job.trigger.timezone) == "America/New_York"
    assert job.trigger.get_next_fire_time(None, et("2026-10-08", 12)) == et("2026-10-08", 17)
    assert job.misfire_grace_time == 3600
    assert job.coalesce is True
    assert job.max_instances == 1
    assert sched.aps.get_job("other") is None  # in schedule.yaml but not registered
    assert scheduler.MAX_WORKERS == 1


def test_next_fires_skip_non_calendar_days(settings):
    entry = scheduler.load_schedule(settings.config_dir)["fake"]
    cals = scheduler.calendars.load_calendars(settings.config_dir)
    fires = scheduler.next_fires(entry, cals, et("2026-10-08", 18), n=5)
    assert [f.astimezone(scheduler.ET).strftime("%m-%d %H:%M") for f in fires] == [
        "10-09 17:00",
        "10-13 17:00",  # Sat, Sun, Columbus Day skipped
        "10-14 17:00",
        "10-15 17:00",
        "10-16 17:00",
    ]


# --- calendar guard -------------------------------------------------------------------------


def test_fire_on_a_non_calendar_day_records_skipped_and_does_not_run(make, settings):
    fake = Fake()
    sched = make(fake, clock=lambda: COLUMBUS_1800)
    with store.session(settings.db_path, wait=0) as con:
        jobs.set_watermark(con, "fake", "k", "2026-10-09", today=COLUMBUS_1800.date())

    result = sched.fire("fake")

    assert result.status == "skipped"
    assert fake.calls == 0
    [run] = _runs(settings)
    assert run["status"] == "skipped"
    assert run["trigger"] == "scheduled"
    assert "federal" in run["error"] and "2026-10-12" in run["error"]
    assert run["started_at"] == COLUMBUS_1800.replace(tzinfo=None)
    assert json.loads(run["watermark_before"]) == {"k": "2026-10-09"}  # keeps the day baseline


def test_fire_on_a_calendar_day_runs_scheduled(make, settings):
    fake = Fake()
    result = make(fake).fire("fake")
    assert result.status == "ok"
    assert fake.calls == 1
    [run] = _runs(settings)
    assert (run["status"], run["trigger"]) == ("ok", "scheduled")


# --- queue: run-now, one worker -------------------------------------------------------------


def test_run_now_goes_through_the_queue_on_one_worker(make, settings):
    overlap = Overlap()
    slow, other = Fake("fake", 0.3, overlap), Fake("other", 0.3, overlap)
    sched = make(slow, other)
    sched.start()
    caller = threading.current_thread().name

    sched.run_now("fake")
    sched.run_now("other")
    assert sched.wait_idle(10)

    assert overlap.max == 1
    assert slow.calls == other.calls == 1
    assert caller not in slow.threads + other.threads
    assert [(r["job"], r["trigger"]) for r in _runs(settings)] == [
        ("fake", "manual"),
        ("other", "manual"),
    ]


def test_run_now_rejects_an_unknown_job(make):
    with pytest.raises(jobs.UnknownJob):
        make(Fake()).run_now("nope")


# --- catch-up -------------------------------------------------------------------------------


def test_catchup_runs_a_job_once_whose_due_fire_was_missed(settings, make):
    fake = Fake()
    make(fake, clock=lambda: THU_1700).fire("fake")  # Thursday's fire ran

    sched = make(fake, clock=lambda: FRI_2300)  # Mac asleep through Friday 17:00
    sched.start()
    assert sched.catchup() == ["fake"]
    assert sched.wait_idle(10)
    assert sched.catchup() == []  # the catch-up run is later than the missed fire
    assert sched.wait_idle(10)

    runs = _runs(settings)
    assert [(r["status"], r["trigger"]) for r in runs] == [("ok", "scheduled"), ("ok", "catchup")]
    assert runs[1]["started_at"] == FRI_2300.replace(tzinfo=None)


def test_catchup_ignores_fires_on_non_calendar_days(settings, make):
    fake = Fake()
    make(fake, clock=lambda: et("2026-10-09", 17)).fire("fake")  # Friday's fire ran
    sched = make(fake, clock=lambda: COLUMBUS_1800)  # Sat, Sun, Mon holiday: nothing due
    sched.start()
    assert sched.catchup() == []
    assert sched.wait_idle(10)
    assert len(_runs(settings)) == 1


def test_catchup_runs_a_job_that_never_ran_ok(settings, make):
    fake = Fake()
    sched = make(fake, clock=lambda: FRI_2300)
    sched.start()
    assert sched.catchup() == ["fake"]
    assert sched.wait_idle(10)
    assert [r["trigger"] for r in _runs(settings)] == ["catchup"]


def test_a_fire_missed_past_its_grace_time_is_caught_up(settings, make):
    fake = Fake()
    sched = make(fake, clock=lambda: FRI_2300)
    sched.start()
    sched.on_missed(JobExecutionEvent(EVENT_JOB_MISSED, "fake", "default", et("2026-10-09", 17)))
    assert sched.wait_idle(10)
    assert [r["trigger"] for r in _runs(settings)] == ["catchup"]


# --- db_locked ------------------------------------------------------------------------------

HOLDER = """
import sys, duckdb
con = duckdb.connect(sys.argv[1])
print("ready", flush=True)
sys.stdin.readline()
"""


def _alerts(settings):
    with store.session(settings.db_path, wait=0) as con:
        cur = con.execute("SELECT job, rule, subject, detail, raised_on FROM alert ORDER BY id")
        return cur.fetchall()


def test_a_locked_run_raises_db_locked_once_the_db_is_free(settings, make):
    fake = Fake()
    sched = make(fake, clock=lambda: THU_1700, wait=0.3)
    with store.session(settings.db_path, wait=0):
        pass  # migrate before the holder grabs the file
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(settings.db_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        result = sched.fire("fake")
        assert result.status == "failed" and result.run_id is None and result.locked
        assert sched.flush_locked() == 0  # still held: nothing written, still pending
        assert len(sched.pending_locked) == 1
    finally:
        holder.communicate("\n", timeout=10)

    assert sched.flush_locked() == 1
    assert sched.pending_locked == []
    [(job, rule, subject, detail, raised_on)] = _alerts(settings)
    assert (job, rule, subject) == ("fake", "db_locked", "fake")
    detail = json.loads(detail)
    assert detail["trigger"] == "scheduled"
    assert detail["at"].startswith("2026-10-08T21:00")
    assert "database busy" in detail["error"]
    assert str(raised_on) == "2026-10-08"
    assert fake.calls == 0


def test_the_next_run_flushes_pending_db_locked_first(settings, make):
    sched = make(Fake(), clock=lambda: THU_1700)
    sched.pending_locked.append(
        scheduler.Locked("fake", "scheduled", THU_1700 - timedelta(hours=1), "database busy — x")
    )
    sched.fire("fake")
    assert [a[1] for a in _alerts(settings)] == ["db_locked"]
    assert sched.pending_locked == []


# --- web API --------------------------------------------------------------------------------


def test_health_and_run_now_endpoint_enqueue_without_running_in_the_request(settings, make):
    fake = Fake()
    sched = make(fake)
    client = TestClient(web.create_app(sched, manage=False))

    health = client.get("/api/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["scheduler"] == "stopped"

    response = client.post("/api/jobs/fake/run")
    assert response.status_code == 202
    assert response.json() == {"job": "fake", "enqueued": True, "trigger": "manual"}
    assert fake.calls == 0 and _runs(settings) == []  # queued, not run in the request

    sched.start()
    assert sched.wait_idle(10)
    assert fake.calls == 1
    assert [r["trigger"] for r in _runs(settings)] == ["manual"]
    assert client.get("/api/health").json()["scheduler"] == "running"


def test_run_now_endpoint_404s_an_unknown_job(make):
    client = TestClient(web.create_app(make(Fake()), manage=False))
    response = client.post("/api/jobs/nope/run")
    assert response.status_code == 404
    assert "nope" in response.json()["detail"]


def test_app_lifespan_starts_the_scheduler_with_catchup_and_stops_it(settings, make):
    fake = Fake()
    sched = make(fake, clock=lambda: FRI_2300)
    with TestClient(web.create_app(sched)) as client:
        assert client.get("/api/health").json()["scheduler"] == "running"
        assert sched.wait_idle(10)
        assert fake.calls == 1  # never ran ok: caught up on startup
    assert not sched.running


# --- CLI ------------------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    monkeypatch.setattr(jobs, "load_registry", lambda: _registry(Fake(), Fake("other")))
    return settings


def test_cli_schedule_prints_the_next_five_fires_per_job(cli_env):
    result = CliRunner().invoke(app, ["schedule", "--at", "2026-10-08T18:00"])
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    i = lines.index("fake  0 17 * * *  federal")
    assert lines[i + 1 : i + 6] == [
        "  2026-10-09 17:00 Fri",
        "  2026-10-13 17:00 Tue",
        "  2026-10-14 17:00 Wed",
        "  2026-10-15 17:00 Thu",
        "  2026-10-16 17:00 Fri",
    ]
    assert "other  30 9 * * mon-fri  any" in lines


def test_cli_serve_binds_localhost_8660(cli_env, monkeypatch):
    seen = {}

    def fake_run(application, **kwargs):
        seen["app"] = application
        seen.update(kwargs)

    monkeypatch.setattr(cli.uvicorn, "run", fake_run)
    result = CliRunner().invoke(app, ["serve"])
    assert result.exit_code == 0, result.output
    assert (seen["host"], seen["port"]) == ("127.0.0.1", 8660)
    assert seen["app"].state.scheduler.entries.keys() == {"fake", "other"}
