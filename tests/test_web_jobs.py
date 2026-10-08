"""Web UI shell: the jobs page (`/`) and the alerts page (`/alerts`), their CSV export, the HTML
forms that post to the API (run-now, ack), and the "database busy" page.
"""

import csv
import io
import shutil
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from usfin import alerts, jobs, raw, scheduler, store
from usfin.settings import load_settings
from usfin.web import app as web

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
THU_1700 = datetime(2026, 10, 8, 21, 0, tzinfo=UTC)  # 17:00 New York
HTML = {"accept": "text/html,application/xhtml+xml"}

SCHEDULE = """
jobs:
  fake:
    cron: "0 17 * * *"
    calendar: federal
    alerts: {}
"""

HOLDER = """
import sys, duckdb
con = duckdb.connect(sys.argv[1])
print("ready", flush=True)
sys.stdin.readline()
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
    """Job with keys `a` and `b`; each run stores one raw body and advances both keys."""

    def __init__(self, name):
        self.name = name
        self.calls = 0

    def fetch(self, ctx, key):
        self.calls += 1
        return [
            raw.save(
                ctx.con,
                source="fake",
                dataset=f"{self.name}_items",
                url=f"fake://{self.name}/{key}",
                body=key.encode(),
                http_status=200,
                raw_dir=ctx.settings.raw_dir,
            )
        ]

    def parse(self, ctx, raw_id):
        ctx.advance("a", "2026-10-07")
        ctx.advance("b", "2026-10-06")
        return 3

    def job(self):
        return jobs.Job(
            name=self.name,
            keys=("a", "b"),
            fetch=self.fetch,
            parsers={f"{self.name}_items": self.parse},
        )


@pytest.fixture
def fakes():
    return {"fake": Fake("fake"), "never": Fake("never")}


@pytest.fixture
def sched(settings, fakes):
    reg = jobs.Registry()
    for fake in fakes.values():
        reg.register(fake.job())
    s = scheduler.Scheduler(settings, reg, clock=lambda: THU_1700)
    yield s
    s.shutdown()


@pytest.fixture
def client(sched):
    return TestClient(web.create_app(sched, manage=False, ui_wait=0.3))


def _run(settings, sched, name="fake"):
    return jobs.run_job(
        name, settings=settings, registry=sched.registry, clock=lambda: THU_1700, wait=0
    )


def _alert(settings, subject, rule="stale_watermark", job="fake"):
    with store.session(settings.db_path, wait=0) as con:
        return alerts.raise_alert(
            con, job=job, rule=rule, subject=subject, detail={"n": 2}, now=THU_1700
        )


def _csv(response):
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]
    return list(csv.DictReader(io.StringIO(response.text)))


# --- layout ---------------------------------------------------------------------------------


def test_pages_share_the_base_layout_and_one_stylesheet_with_light_and_dark_tokens(client):
    for path in ("/", "/alerts"):
        page = client.get(path)
        assert page.status_code == 200
        assert page.headers["content-type"].startswith("text/html")
        assert '<link rel="stylesheet" href="/static/style.css">' in page.text
        assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in page.text
        assert 'href="/alerts"' in page.text and 'href="/"' in page.text
        assert "<script" not in page.text  # no JS framework, no JS at all
    css = client.get("/static/style.css")
    assert css.status_code == 200
    assert css.headers["content-type"].startswith("text/css")
    assert "prefers-color-scheme: dark" in css.text
    assert "--bg" in css.text and "--fg" in css.text


# --- jobs page ------------------------------------------------------------------------------


def test_jobs_page_lists_every_registered_job_with_last_run_status_and_watermarks(
    client, settings, sched
):
    _run(settings, sched)
    page = client.get("/").text
    assert "fake" in page and "never" in page
    assert "never run" in page
    assert "ok" in page and "manual" in page
    assert "a = 2026-10-07" in page and "b = 2026-10-06" in page
    assert "0 17 * * *" in page and "federal" in page  # schedule shown for scheduled jobs
    assert 'action="/api/jobs/fake/run" method="post"' in page
    assert 'action="/api/jobs/never/run" method="post"' in page


def test_jobs_page_shows_a_failed_runs_error(client, settings, sched, fakes):
    def boom(ctx, key):
        raise RuntimeError("upstream <500>")

    fakes["fake"].fetch = boom
    sched.registry._jobs["fake"] = fakes["fake"].job()
    _run(settings, sched)
    page = client.get("/").text
    assert "failed" in page
    assert "upstream &lt;500&gt;" in page  # escaped


def test_jobs_csv_has_one_row_per_job(client, settings, sched):
    _run(settings, sched)
    rows = _csv(client.get("/?format=csv"))
    assert [r["job"] for r in rows] == ["fake", "never"]
    fake = rows[0]
    assert fake["status"] == "ok" and fake["trigger"] == "manual"
    assert fake["rows_upserted"] == "6"
    assert fake["watermarks"] == "a=2026-10-07; b=2026-10-06"
    assert fake["cron"] == "0 17 * * *" and fake["calendar"] == "federal"
    assert rows[1]["status"] == "" and rows[1]["cron"] == ""


def test_run_now_form_redirects_back_to_the_jobs_page_and_only_queues(client, settings, fakes):
    response = client.post("/api/jobs/fake/run", headers=HTML, follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert fakes["fake"].calls == 0  # queued on the worker, not run in the request


def test_run_now_api_still_answers_json_202(client):
    response = client.post("/api/jobs/fake/run")
    assert response.status_code == 202
    assert response.json() == {"job": "fake", "enqueued": True, "trigger": "manual"}


# --- alerts page ----------------------------------------------------------------------------


def test_alerts_page_lists_open_alerts_with_an_ack_form(client, settings):
    first = _alert(settings, "dts_operating_cash")
    second = _alert(settings, "<b>debt</b>", rule="value_jump")
    page = client.get("/alerts").text
    assert "dts_operating_cash" in page and "stale_watermark" in page
    assert "&lt;b&gt;debt&lt;/b&gt;" in page and "<b>debt</b>" not in page
    assert f'action="/api/alerts/{first}/ack" method="post"' in page
    assert f'action="/api/alerts/{second}/ack" method="post"' in page


def test_ack_form_acknowledges_and_redirects_back(client, settings):
    alert_id = _alert(settings, "dts_operating_cash")
    response = client.post(f"/api/alerts/{alert_id}/ack", headers=HTML, follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/alerts"
    assert "dts_operating_cash" not in client.get("/alerts").text
    everything = client.get("/alerts?all=1").text
    assert "dts_operating_cash" in everything and "acknowledged" in everything


def test_ack_api_answers_json_and_404s_an_unknown_id(client, settings):
    alert_id = _alert(settings, "x")
    response = client.post(f"/api/alerts/{alert_id}/ack")
    assert response.status_code == 200
    assert response.json() == {"id": alert_id, "acknowledged": True}
    assert client.post(f"/api/alerts/{alert_id}/ack").status_code == 200  # idempotent
    assert client.post("/api/alerts/999/ack").status_code == 404


def test_alerts_csv_lists_open_alerts_and_all_with_acked(client, settings):
    first = _alert(settings, "one")
    _alert(settings, "two")
    client.post(f"/api/alerts/{first}/ack")
    rows = _csv(client.get("/alerts?format=csv"))
    assert [r["subject"] for r in rows] == ["two"]
    assert rows[0]["rule"] == "stale_watermark"
    assert rows[0]["detail"] == '{"n": 2}'
    assert rows[0]["raised_on"] == "2026-10-08"
    rows = _csv(client.get("/alerts?format=csv&all=1"))
    assert sorted(r["subject"] for r in rows) == ["one", "two"]
    assert [r["acknowledged_at"] != "" for r in rows if r["subject"] == "one"] == [True]


def test_empty_alerts_page_says_so(client):
    assert "no open alerts" in client.get("/alerts").text


# --- database busy --------------------------------------------------------------------------


def test_pages_render_database_busy_when_another_process_holds_the_db(client, settings):
    with store.session(settings.db_path, wait=0):
        pass
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(settings.db_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        for path in ("/", "/alerts", "/?format=csv", "/alerts?format=csv"):
            response = client.get(path)
            assert response.status_code == 503, path
            assert "database busy — another process holds the lock" in response.text
        page = client.get("/")
        assert page.headers["content-type"].startswith("text/html")
        assert '<link rel="stylesheet" href="/static/style.css">' in page.text
        assert client.post("/api/alerts/1/ack").status_code == 503
    finally:
        holder.communicate("\n", timeout=10)
    assert client.get("/").status_code == 200
