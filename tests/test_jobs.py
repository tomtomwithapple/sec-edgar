import json
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from usfin import calendars, jobs, raw, store
from usfin.cli import app
from usfin.settings import load_settings

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
# 2026-10-08 16:00 UTC = 12:00 ET
NOW = datetime(2026, 10, 8, 16, 0, tzinfo=UTC)


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    base = load_settings(env_file=tmp_path / "no.env")
    return replace(
        base,
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=REPO_CONFIG,
        lock_wait=0,
    )


class Fake:
    """A registrable fake job: one raw body per key, one row per item, advances to max day."""

    def __init__(self, keys=("a", "b"), items=None, fail_fetch=(), fail_parse=(), advance=None):
        self.keys = keys
        self.items = items or {k: [("2026-10-01", 1.0), ("2026-10-02", 2.0)] for k in keys}
        self.fail_fetch = set(fail_fetch)
        self.fail_parse = set(fail_parse)
        self.advance = advance
        self.fetched: list[str] = []

    def fetch(self, ctx, key):
        self.fetched.append(key)
        body = json.dumps({"key": key, "items": self.items[key]}).encode()
        raw_id = raw.save(
            ctx.con,
            source="fake",
            dataset="fake_items",
            url=f"fake://{key}",
            body=body,
            http_status=200,
            raw_dir=ctx.settings.raw_dir,
        )
        if key in self.fail_fetch:
            raise RuntimeError(f"upstream 500 for {key}")
        return [raw_id]

    def parse(self, ctx, raw_id):
        with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
            doc = json.loads(fh.read())
        key = doc["key"]
        ctx.con.execute(
            "CREATE TABLE IF NOT EXISTS fake_item (key VARCHAR, day DATE, value DOUBLE,"
            " PRIMARY KEY (key, day))"
        )
        n = 0
        for day, value in doc["items"]:
            ctx.con.execute(
                "INSERT INTO fake_item VALUES (?, ?, ?)"
                " ON CONFLICT (key, day) DO UPDATE SET value = excluded.value",
                [key, day, value],
            )
            n += 1
        if key in self.fail_parse:
            raise ValueError(f"bad payload for {key}")
        ctx.advance(key, self.advance or date.fromisoformat(max(d for d, _ in doc["items"])))
        return n

    def job(self, name="fake"):
        return jobs.Job(
            name=name, keys=self.keys, fetch=self.fetch, parsers={"fake_items": self.parse}
        )


def _registry(*job_list):
    reg = jobs.Registry()
    for job in job_list:
        reg.register(job)
    return reg


def _run(settings, reg, name="fake", **kwargs):
    return jobs.run_job(name, settings=settings, registry=reg, clock=lambda: NOW, **kwargs)


def _runs(settings):
    with store.session(settings.db_path, wait=0) as con:
        cur = con.execute("SELECT * FROM job_run ORDER BY id")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _watermarks(settings, job="fake"):
    with store.session(settings.db_path, wait=0) as con:
        return jobs.get_watermarks(con, job)


def _set_watermark(settings, key, value, job="fake"):
    with store.session(settings.db_path, wait=0) as con:
        jobs.set_watermark(con, job, key, value, today=NOW.date())


def _count(settings, table="fake_item"):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


# --- registry -----------------------------------------------------------------------------


def test_registry_rejects_duplicates_and_unknown_names():
    reg = _registry(Fake().job())
    with pytest.raises(ValueError, match="already registered"):
        reg.register(Fake().job())
    with pytest.raises(jobs.UnknownJob, match="nope"):
        reg.get("nope")
    assert reg.names() == ["fake"]
    assert reg.get("fake").then == ()


def test_job_keys_may_be_computed_from_the_context(settings):
    fake = Fake(keys=("x",))
    job = jobs.Job(
        name="fake", keys=lambda ctx: ["x"], fetch=fake.fetch, parsers={"fake_items": fake.parse}
    )
    result = _run(settings, _registry(job))
    assert result.status == "ok"
    assert fake.fetched == ["x"]


# --- run log and watermarks ---------------------------------------------------------------


def test_ok_run_records_rows_and_before_after_watermarks(settings):
    reg = _registry(Fake().job())
    _set_watermark(settings, "a", "2026-09-30")

    result = _run(settings, reg)

    assert result.status == "ok"
    assert result.rows_upserted == 4
    [run] = _runs(settings)
    assert run["id"] == result.run_id
    assert run["job"] == "fake"
    assert run["status"] == "ok"
    assert run["trigger"] == "manual"
    assert run["rows_upserted"] == 4
    assert run["error"] is None
    assert run["started_at"] == NOW.replace(tzinfo=None)
    assert run["finished_at"] is not None
    assert json.loads(run["watermark_before"]) == {"a": "2026-09-30"}
    assert json.loads(run["watermark_after"]) == {"a": "2026-10-02", "b": "2026-10-02"}
    assert _watermarks(settings) == {"a": "2026-10-02", "b": "2026-10-02"}
    assert _count(settings) == 4


def test_watermarks_are_visible_to_the_job_at_start(settings):
    seen = {}
    fake = Fake(keys=("a",))

    def fetch(ctx, key):
        seen.update(ctx.watermarks)
        seen["today"] = ctx.today
        return fake.fetch(ctx, key)

    job = jobs.Job(name="fake", keys=("a",), fetch=fetch, parsers={"fake_items": fake.parse})
    _set_watermark(settings, "a", "2026-09-30")
    _run(settings, _registry(job))
    assert seen == {"a": "2026-09-30", "today": date(2026, 10, 8)}


def test_second_run_starts_from_the_first_runs_watermarks(settings):
    reg = _registry(Fake().job())
    _run(settings, reg)
    second = _run(settings, reg, trigger="scheduled")
    runs = _runs(settings)
    assert [r["status"] for r in runs] == ["ok", "ok"]
    assert runs[1]["id"] == second.run_id
    assert runs[1]["trigger"] == "scheduled"
    assert json.loads(runs[1]["watermark_before"]) == json.loads(runs[0]["watermark_after"])
    assert _count(settings) == 4  # upserts: same rows, no duplicates


def test_fetch_failure_records_failed_with_error_and_keeps_that_watermark(settings):
    reg = _registry(Fake(keys=("a",), fail_fetch={"a"}).job())
    _set_watermark(settings, "a", "2026-09-30")

    result = _run(settings, reg)

    assert result.status == "failed"
    [run] = _runs(settings)
    assert run["status"] == "failed"
    assert "upstream 500 for a" in run["error"]
    assert run["finished_at"] is not None
    assert json.loads(run["watermark_after"]) == {"a": "2026-09-30"}
    assert _watermarks(settings) == {"a": "2026-09-30"}
    # raw first: the response saved before the failure is kept, but not parsed
    with store.session(settings.db_path, wait=0) as con:
        assert con.execute("SELECT count(*) FROM raw_response").fetchone()[0] == 1
        assert "fake_item" not in store.table_names(con)


def test_one_failing_key_advances_only_the_others(settings):
    fake = Fake(keys=("a", "b", "c"), fail_parse={"b"})
    reg = _registry(fake.job())
    _set_watermark(settings, "b", "2026-09-30")

    result = _run(settings, reg)

    assert result.status == "failed"
    assert fake.fetched == ["a", "b", "c"]  # a failed key does not stop the others
    [run] = _runs(settings)
    assert "b: " in run["error"] and "bad payload for b" in run["error"]
    assert "a: " not in run["error"]
    assert run["rows_upserted"] == 4  # a and c committed; b's partial insert rolled back
    assert _watermarks(settings) == {"a": "2026-10-02", "b": "2026-09-30", "c": "2026-10-02"}
    with store.session(settings.db_path, wait=0) as con:
        keys = {r[0] for r in con.execute("SELECT DISTINCT key FROM fake_item").fetchall()}
    assert keys == {"a", "c"}


def test_next_run_resumes_a_failed_key_from_its_watermark(settings):
    failing = Fake(keys=("a", "b"), fail_parse={"b"})
    _set_watermark(settings, "b", "2026-09-30")
    _run(settings, _registry(failing.job()))

    seen = {}
    healthy = Fake(keys=("a", "b"))

    def fetch(ctx, key):
        seen[key] = ctx.watermarks.get(key)
        return healthy.fetch(ctx, key)

    job = jobs.Job(name="fake", keys=("a", "b"), fetch=fetch, parsers={"fake_items": healthy.parse})
    result = _run(settings, _registry(job))
    assert result.status == "ok"
    assert seen == {"a": "2026-10-02", "b": "2026-09-30"}
    assert _watermarks(settings) == {"a": "2026-10-02", "b": "2026-10-02"}


def test_raw_response_with_no_parser_fails_that_key(settings):
    fake = Fake(keys=("a",))
    job = jobs.Job(name="fake", keys=("a",), fetch=fake.fetch, parsers={"other": fake.parse})
    result = _run(settings, _registry(job))
    assert result.status == "failed"
    assert "no parser for dataset 'fake_items'" in _runs(settings)[0]["error"]
    assert _watermarks(settings) == {}


def test_watermark_dates_are_capped_at_today(settings):
    reg = _registry(Fake(keys=("a",), advance=date(2030, 1, 1)).job())
    _run(settings, reg)
    assert _watermarks(settings) == {"a": "2026-10-08"}  # today in New York


def test_set_watermark_caps_dates_and_iso_date_strings(settings):
    today = date(2026, 10, 8)
    with store.session(settings.db_path, wait=0) as con:
        jobs.set_watermark(con, "j", "d", date(2027, 1, 1), today=today)
        jobs.set_watermark(con, "j", "s", "2027-01-01", today=today)
        jobs.set_watermark(con, "j", "old", date(2026, 1, 1), today=today)
        jobs.set_watermark(con, "j", "accn", "0000320193-26-000001", today=today)
        jobs.set_watermark(con, "j", "ts", datetime(2026, 10, 8, 14, 30, tzinfo=UTC), today=today)
        assert jobs.get_watermarks(con, "j") == {
            "d": "2026-10-08",
            "s": "2026-10-08",
            "old": "2026-01-01",
            "accn": "0000320193-26-000001",
            "ts": "2026-10-08T14:30:00",
        }
        jobs.set_watermark(con, "j", "d", date(2026, 10, 1), today=today)  # upsert
        assert jobs.get_watermarks(con, "j")["d"] == "2026-10-01"


def test_today_is_the_new_york_date(settings):
    seen = {}
    fake = Fake(keys=("a",))

    def fetch(ctx, key):
        seen["today"] = ctx.today
        return fake.fetch(ctx, key)

    job = jobs.Job(name="fake", keys=("a",), fetch=fetch, parsers={"fake_items": fake.parse})
    # 2026-10-09 02:00 UTC is still 2026-10-08 22:00 in New York
    jobs.run_job(
        "fake",
        settings=settings,
        registry=_registry(job),
        clock=lambda: datetime(2026, 10, 9, 2, 0, tzinfo=UTC),
    )
    assert seen["today"] == date(2026, 10, 8)


def test_run_ignores_the_calendar(settings, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("manual run consulted the calendar")

    monkeypatch.setattr(calendars, "load_calendars", boom)
    reg = _registry(Fake(keys=("a",)).job())
    columbus_day = datetime(2026, 10, 12, 16, 0, tzinfo=UTC)
    result = jobs.run_job("fake", settings=settings, registry=reg, clock=lambda: columbus_day)
    assert result.status == "ok"
    assert _runs(settings)[0]["status"] == "ok"


def test_unknown_job_raises_before_opening_the_db(settings):
    with pytest.raises(jobs.UnknownJob):
        _run(settings, jobs.Registry(), name="nope")
    assert not settings.db_path.exists()


# --- DB lock --------------------------------------------------------------------------------

HOLDER = """
import sys, duckdb
con = duckdb.connect(sys.argv[1])
print("ready", flush=True)
sys.stdin.readline()
"""


def test_db_lock_fails_the_run_without_touching_watermarks(settings, caplog):
    fake = Fake(keys=("a",))
    reg = _registry(fake.job())
    _set_watermark(settings, "a", "2026-09-30")
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(settings.db_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        with caplog.at_level("ERROR", logger="usfin.jobs"):
            result = _run(settings, reg, wait=0.3)
    finally:
        holder.communicate("\n", timeout=10)
    assert result.status == "failed"
    assert result.run_id is None  # the run log could not be written
    assert "database busy" in result.error
    assert "database busy" in caplog.text and "fake" in caplog.text
    assert fake.fetched == []
    assert _runs(settings) == []
    assert _watermarks(settings) == {"a": "2026-09-30"}


# --- CLI ------------------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(REPO_CONFIG))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    return settings


def test_cli_run_and_jobs_list_last_run_and_watermarks(cli_env, monkeypatch):
    reg = _registry(Fake().job(), Fake(keys=("z",)).job("idle"))
    monkeypatch.setattr(jobs, "load_registry", lambda: reg)
    runner = CliRunner()

    before = runner.invoke(app, ["jobs"])
    assert before.exit_code == 0, before.output
    assert "fake" in before.output and "never run" in before.output

    ran = runner.invoke(app, ["run", "fake"])
    assert ran.exit_code == 0, ran.output
    assert "ok" in ran.output and "rows 4" in ran.output

    listed = runner.invoke(app, ["jobs"])
    assert listed.exit_code == 0, listed.output
    lines = listed.output.splitlines()
    fake_line = next(line for line in lines if line.startswith("fake"))
    assert "ok" in fake_line and "manual" in fake_line and "rows 4" in fake_line
    assert any("a = 2026-10-02" in line for line in lines)
    assert any("b = 2026-10-02" in line for line in lines)
    idle_line = next(line for line in lines if line.startswith("idle"))
    assert "never run" in idle_line


def test_cli_run_failed_exits_1_with_the_error(cli_env, monkeypatch):
    reg = _registry(Fake(keys=("a",), fail_fetch={"a"}).job())
    monkeypatch.setattr(jobs, "load_registry", lambda: reg)
    result = CliRunner().invoke(app, ["run", "fake"])
    assert result.exit_code == 1
    assert "failed" in result.output and "upstream 500 for a" in result.output


def test_cli_run_unknown_job_exits_2(cli_env, monkeypatch):
    monkeypatch.setattr(jobs, "load_registry", lambda: jobs.Registry())
    result = CliRunner().invoke(app, ["run", "nope"])
    assert result.exit_code == 2
    assert "unknown job 'nope'" in result.output


def test_demo_job_registers_only_with_usfin_demo(cli_env, monkeypatch):
    assert "_demo" not in jobs.load_registry().names()
    monkeypatch.setenv("USFIN_DEMO", "1")
    assert "_demo" in jobs.load_registry().names()
    runner = CliRunner()
    first = runner.invoke(app, ["run", "_demo"])
    assert first.exit_code == 0, first.output
    second = runner.invoke(app, ["run", "_demo"])
    assert second.exit_code == 0, second.output
    assert _count(cli_env, "demo_item") > 0
    listed = runner.invoke(app, ["jobs"])
    assert "_demo" in listed.output
    with store.session(cli_env.db_path, wait=0) as con:
        # no network: every raw response is a demo:// body
        urls = {r[0] for r in con.execute("SELECT url FROM raw_response").fetchall()}
    assert urls and all(u.startswith("demo://") for u in urls)
