import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from usfin import jobs, raw, store
from usfin.cli import app
from usfin.settings import load_settings

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
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


class FailingTransport(httpx.BaseTransport):
    """Fails the test's expectations on any request: reparse must never reach the network."""

    def __init__(self):
        self.requests: list[httpx.Request] = []

    def handle_request(self, request):
        self.requests.append(request)
        raise AssertionError(f"reparse sent a request: {request.url}")


def _save(con, settings, dataset, body, fetched_at, url="fake://x"):
    return raw.save(
        con,
        source="fake",
        dataset=dataset,
        url=url,
        body=json.dumps(body).encode(),
        http_status=200,
        fetched_at=fetched_at,
        raw_dir=settings.raw_dir,
    )


def _ts(day, hour=12):
    return datetime(2026, 10, day, hour, 0, tzinfo=UTC).replace(tzinfo=None)


class Upsert:
    """Plain (non-versioned) parser: upserts `item(key, value)` and records the call order."""

    def __init__(self):
        self.seen: list[int] = []

    def parse(self, ctx, raw_id):
        self.seen.append(raw_id)
        with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
            doc = json.loads(fh.read())
        ctx.con.execute("CREATE TABLE IF NOT EXISTS item (key VARCHAR PRIMARY KEY, value DOUBLE)")
        ctx.con.execute(
            "INSERT INTO item VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [doc["key"], doc["value"]],
        )
        ctx.advance(doc["key"], "2026-10-08")
        return 1


def _no_fetch(ctx, key):
    raise AssertionError("reparse called fetch")


def _job(parsers, *, name="fake", versioned=(), then=()):
    return jobs.Job(
        name=name, keys=("a",), fetch=_no_fetch, parsers=parsers, versioned=versioned, then=then
    )


# --- selection -------------------------------------------------------------------------------


def test_reparse_selects_the_jobs_datasets_in_fetched_at_order_without_http(settings):
    up = Upsert()
    with store.session(settings.db_path, wait=0) as con:
        late = _save(con, settings, "ds_a", {"key": "k", "value": 3.0}, _ts(5))
        early = _save(con, settings, "ds_b", {"key": "k", "value": 1.0}, _ts(1))
        mid = _save(con, settings, "ds_a", {"key": "k", "value": 2.0}, _ts(3))
        _save(con, settings, "other", {"key": "x", "value": 9.0}, _ts(2))
        transport = FailingTransport()

        result = jobs.reparse(
            con,
            _job({"ds_a": up.parse, "ds_b": up.parse}),
            settings=settings,
            now=NOW,
            transport=transport,
        )

        assert up.seen == [early, mid, late]
        assert result.raw_ids == [early, mid, late]
        assert result.rows == 3
        assert result.skipped == []
        assert transport.requests == []
        # last in fetched_at order wins, and the unrelated dataset was not parsed
        assert con.execute("SELECT key, value FROM item").fetchall() == [("k", 3.0)]


def test_reparse_never_reaches_the_network_by_default(settings):
    def greedy(ctx, raw_id):
        ctx.client.get("https://api.fiscaldata.treasury.gov/x", source="fake", dataset="ds_a")
        return 1

    with store.session(settings.db_path, wait=0) as con:
        _save(con, settings, "ds_a", {"key": "k", "value": 1.0}, _ts(1))
        with pytest.raises(jobs.ReparseFailed, match="NetworkDisabled") as failed:
            jobs.reparse(con, _job({"ds_a": greedy}), settings=settings, now=NOW)
        assert isinstance(failed.value.__cause__, jobs.NetworkDisabled)
        # no response was archived by the attempt
        assert con.execute("SELECT count(*) FROM raw_response").fetchone()[0] == 1


def test_since_filters_by_fetched_at(settings):
    up = Upsert()
    with store.session(settings.db_path, wait=0) as con:
        _save(con, settings, "ds_a", {"key": "k", "value": 1.0}, _ts(1))
        # 2026-10-03 03:00 UTC is still 2026-10-02 in New York: before --since 2026-10-03
        _save(con, settings, "ds_a", {"key": "k", "value": 2.0}, _ts(3, hour=3))
        keep = _save(con, settings, "ds_a", {"key": "k", "value": 3.0}, _ts(3, hour=5))
        result = jobs.reparse(
            con,
            _job({"ds_a": up.parse}),
            settings=settings,
            now=NOW,
            since=date(2026, 10, 3),
            transport=FailingTransport(),
        )
    assert result.raw_ids == [keep]
    assert up.seen == [keep]


def test_pruned_rows_are_skipped_with_a_warning(settings, caplog):
    up = Upsert()
    with store.session(settings.db_path, wait=0) as con:
        gone = _save(con, settings, "ds_a", {"key": "k", "value": 1.0}, _ts(1))
        kept = _save(con, settings, "ds_a", {"key": "k", "value": 2.0}, _ts(2))
        con.execute("UPDATE raw_response SET pruned_at = ? WHERE id = ?", [_ts(4), gone])
        with caplog.at_level("WARNING", logger="usfin.jobs"):
            result = jobs.reparse(
                con,
                _job({"ds_a": up.parse}),
                settings=settings,
                now=NOW,
                transport=FailingTransport(),
            )
    assert result.raw_ids == [kept]
    assert result.skipped == [gone]
    assert up.seen == [kept]
    assert f"raw_response {gone}" in caplog.text and "pruned" in caplog.text


def test_reparse_leaves_watermarks_and_job_run_alone(settings):
    up = Upsert()
    with store.session(settings.db_path, wait=0) as con:
        _save(con, settings, "ds_a", {"key": "a", "value": 1.0}, _ts(1))
        jobs.set_watermark(con, "fake", "a", "2026-09-01", today=NOW.date())
        jobs.reparse(
            con,
            _job({"ds_a": up.parse}),
            settings=settings,
            now=NOW,
            transport=FailingTransport(),
        )
        assert jobs.get_watermarks(con, "fake") == {"a": "2026-09-01"}
        assert con.execute("SELECT count(*) FROM job_run").fetchone()[0] == 0


# --- versioned jobs --------------------------------------------------------------------------


class Versioned:
    """BLS-like parser: a new `obs` row only when the value differs from the latest as of then."""

    def parse(self, ctx, raw_id):
        fetched_at = ctx.con.execute(
            "SELECT fetched_at FROM raw_response WHERE id = ?", [raw_id]
        ).fetchone()[0]
        with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
            doc = json.loads(fh.read())
        ctx.con.execute(
            "CREATE TABLE IF NOT EXISTS obs (period VARCHAR, value DOUBLE,"
            " first_seen_at TIMESTAMP, raw_id BIGINT, PRIMARY KEY (period, first_seen_at))"
        )
        n = 0
        for period, value in doc.items():
            latest = ctx.con.execute(
                "SELECT value FROM obs WHERE period = ? AND first_seen_at <= ?"
                " ORDER BY first_seen_at DESC LIMIT 1",
                [period, fetched_at],
            ).fetchone()
            if latest is None or latest[0] != value:
                ctx.con.execute(
                    "INSERT INTO obs VALUES (?, ?, ?, ?)", [period, value, fetched_at, raw_id]
                )
                n += 1
        return n


def _obs(con):
    return con.execute("SELECT * FROM obs ORDER BY period, first_seen_at").fetchall()


def test_versioned_job_deletes_derived_rows_by_raw_id_and_replays(settings):
    job = _job({"bls": Versioned().parse}, versioned=("obs",))
    with store.session(settings.db_path, wait=0) as con:
        r1 = _save(con, settings, "bls", {"2026M08": 1.0, "2026M09": 2.0}, _ts(1))
        r2 = _save(con, settings, "bls", {"2026M08": 1.0, "2026M09": 2.5}, _ts(2))
        r3 = _save(con, settings, "bls", {"2026M08": 1.1, "2026M09": 2.5}, _ts(3))
        first = jobs.reparse(con, job, settings=settings, now=NOW, transport=FailingTransport())
        built = _obs(con)
        assert first.rows == 4
        assert [(p, v, rid) for p, v, _, rid in built] == [
            ("2026M08", 1.0, r1),
            ("2026M08", 1.1, r3),
            ("2026M09", 2.0, r1),
            ("2026M09", 2.5, r2),
        ]

        # history damaged after the fact: a wrong value and a stray row
        con.execute("UPDATE obs SET value = 9.9 WHERE raw_id = ?", [r2])
        con.execute("INSERT INTO obs VALUES ('2026M09', 7.0, ?, ?)", [_ts(4), r3])
        second = jobs.reparse(con, job, settings=settings, now=NOW, transport=FailingTransport())

        assert second.deleted == 5
        assert _obs(con) == built  # rebuilt, not appended


def test_versioned_reparse_since_keeps_older_history(settings):
    job = _job({"bls": Versioned().parse}, versioned=("obs",))
    with store.session(settings.db_path, wait=0) as con:
        _save(con, settings, "bls", {"2026M09": 2.0}, _ts(1))
        _save(con, settings, "bls", {"2026M09": 2.5}, _ts(3))
        jobs.reparse(con, job, settings=settings, now=NOW, transport=FailingTransport())
        built = _obs(con)
        again = jobs.reparse(
            con,
            job,
            settings=settings,
            now=NOW,
            since=date(2026, 10, 2),
            transport=FailingTransport(),
        )
        assert again.deleted == 1
        assert _obs(con) == built


def test_versioned_reparse_keeps_rows_of_pruned_responses(settings):
    job = _job({"bls": Versioned().parse}, versioned=("obs",))
    with store.session(settings.db_path, wait=0) as con:
        r1 = _save(con, settings, "bls", {"2026M09": 2.0}, _ts(1))
        _save(con, settings, "bls", {"2026M09": 2.5}, _ts(3))
        jobs.reparse(con, job, settings=settings, now=NOW, transport=FailingTransport())
        built = _obs(con)
        con.execute("UPDATE raw_response SET pruned_at = ? WHERE id = ?", [_ts(4), r1])
        again = jobs.reparse(con, job, settings=settings, now=NOW, transport=FailingTransport())
        assert again.skipped == [r1]
        assert _obs(con) == built  # r1's version cannot be replayed, so it is not deleted


def test_failed_reparse_changes_nothing(settings):
    calls = []

    def flaky(ctx, raw_id):
        calls.append(raw_id)
        if len(calls) == 3:
            raise ValueError("parser bug")
        return Versioned().parse(ctx, raw_id)

    with store.session(settings.db_path, wait=0) as con:
        _save(con, settings, "bls", {"2026M09": 2.0}, _ts(1))
        _save(con, settings, "bls", {"2026M09": 2.5}, _ts(3))
        good = _job({"bls": Versioned().parse}, versioned=("obs",))
        jobs.reparse(con, good, settings=settings, now=NOW, transport=FailingTransport())
        built = _obs(con)
        bad = _job({"bls": flaky}, versioned=("obs",))
        jobs.reparse(con, bad, settings=settings, now=NOW, transport=FailingTransport())
        with pytest.raises(jobs.ReparseFailed, match="parser bug"):
            jobs.reparse(con, bad, settings=settings, now=NOW, transport=FailingTransport())
        assert _obs(con) == built  # deletes rolled back with the failed replay


def test_versioned_table_not_yet_created_is_fine(settings):
    job = _job({"bls": Versioned().parse}, versioned=("obs",))
    with store.session(settings.db_path, wait=0) as con:
        _save(con, settings, "bls", {"2026M09": 2.0}, _ts(1))
        result = jobs.reparse(con, job, settings=settings, now=NOW, transport=FailingTransport())
    assert result.deleted == 0 and result.rows == 1


# --- chaining --------------------------------------------------------------------------------


class Step:
    """A one-key job that stores a body and parses it; `fail=True` makes its fetch raise."""

    def __init__(self, name, then=(), fail=False, log=None):
        self.name, self.fail, self.log = name, fail, log if log is not None else []
        self.job = jobs.Job(
            name=name, keys=("k",), fetch=self.fetch, parsers={name: self.parse}, then=then
        )

    def fetch(self, ctx, key):
        self.log.append((self.name, ctx.trigger))
        if self.fail:
            raise RuntimeError(f"{self.name} broke")
        return [_save(ctx.con, ctx.settings, self.name, {"n": 1}, _ts(1))]

    def parse(self, ctx, raw_id):
        return 1


def _registry(*steps):
    reg = jobs.Registry()
    for step in steps:
        reg.register(step.job)
    return reg


def _runs(settings):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute("SELECT id, job, status, trigger FROM job_run ORDER BY id").fetchall()


def test_then_jobs_run_after_success_each_with_its_own_chained_run(settings):
    log: list = []
    reg = _registry(
        Step("parent", then=("child_a", "child_b"), log=log),
        Step("child_a", then=("grandchild",), log=log),
        Step("child_b", log=log),
        Step("grandchild", log=log),
    )
    result = jobs.run_job("parent", settings=settings, registry=reg, clock=lambda: NOW)

    assert result.status == "ok"
    assert log == [
        ("parent", "manual"),
        ("child_a", "chained"),
        ("grandchild", "chained"),
        ("child_b", "chained"),
    ]
    assert [(r.job, r.status) for r in result.chained] == [
        ("child_a", "ok"),
        ("child_b", "ok"),
    ]
    assert [(r.job, r.status) for r in result.chained[0].chained] == [("grandchild", "ok")]
    assert _runs(settings) == [
        (1, "parent", "ok", "manual"),
        (2, "child_a", "ok", "chained"),
        (3, "grandchild", "ok", "chained"),
        (4, "child_b", "ok", "chained"),
    ]


def test_then_jobs_do_not_run_after_a_failure(settings):
    log: list = []
    reg = _registry(Step("parent", then=("child",), fail=True, log=log), Step("child", log=log))
    result = jobs.run_job("parent", settings=settings, registry=reg, clock=lambda: NOW)
    assert result.status == "failed"
    assert result.chained == []
    assert log == [("parent", "manual")]
    assert _runs(settings) == [(1, "parent", "failed", "manual")]


def test_a_failed_chained_job_does_not_stop_its_siblings(settings):
    log: list = []
    reg = _registry(
        Step("parent", then=("bad", "good"), log=log),
        Step("bad", then=("never",), fail=True, log=log),
        Step("good", log=log),
        Step("never", log=log),
    )
    result = jobs.run_job("parent", settings=settings, registry=reg, clock=lambda: NOW)
    assert [(r.job, r.status) for r in result.chained] == [("bad", "failed"), ("good", "ok")]
    assert ("never", "chained") not in log


def test_chain_cycles_and_unknown_names_do_not_loop_or_crash(settings):
    log: list = []
    reg = _registry(
        Step("a", then=("b", "ghost"), log=log),
        Step("b", then=("a",), log=log),
    )
    result = jobs.run_job("a", settings=settings, registry=reg, clock=lambda: NOW)
    assert log == [("a", "manual"), ("b", "chained")]
    assert [(r.job, r.status) for r in result.chained] == [("b", "ok"), ("ghost", "failed")]
    assert result.chained[0].chained == []  # a already ran in this chain
    assert "unknown job 'ghost'" in result.chained[1].error


def test_chain_false_runs_only_the_job(settings):
    log: list = []
    reg = _registry(Step("parent", then=("child",), log=log), Step("child", log=log))
    result = jobs.run_job("parent", settings=settings, registry=reg, clock=lambda: NOW, chain=False)
    assert result.chained == []
    assert log == [("parent", "manual")]


# --- CLI -------------------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(REPO_CONFIG))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    return settings


def test_cli_run_prints_chained_runs_and_exits_1_if_one_failed(cli_env, monkeypatch):
    reg = _registry(Step("parent", then=("child",)), Step("child", fail=True))
    monkeypatch.setattr(jobs, "load_registry", lambda: reg)
    result = CliRunner().invoke(app, ["run", "parent"])
    assert result.exit_code == 1
    assert "parent #1 ok rows 1" in result.output
    assert "child #2 failed rows 0 (chained)" in result.output
    assert "child broke" in result.output


def test_cli_reparse_demo_rebuilds_rows_without_new_raw(cli_env, monkeypatch):
    monkeypatch.setenv("USFIN_DEMO", "1")
    runner = CliRunner()
    assert runner.invoke(app, ["run", "_demo"]).exit_code == 0
    with store.session(cli_env.db_path, wait=0) as con:
        before = con.execute("SELECT * FROM demo_item ORDER BY ALL").fetchall()
        con.execute("DELETE FROM demo_item")
        raws = con.execute("SELECT count(*) FROM raw_response").fetchone()[0]

    result = runner.invoke(app, ["reparse", "_demo"])

    assert result.exit_code == 0, result.output
    assert "_demo reparsed 2 raw responses, rows 6" in result.output
    with store.session(cli_env.db_path, wait=0) as con:
        assert con.execute("SELECT * FROM demo_item ORDER BY ALL").fetchall() == before
        assert con.execute("SELECT count(*) FROM raw_response").fetchone()[0] == raws
        assert con.execute("SELECT count(*) FROM job_run").fetchone()[0] == 1


def test_cli_reparse_since_and_bad_input(cli_env, monkeypatch):
    monkeypatch.setenv("USFIN_DEMO", "1")
    runner = CliRunner()
    assert runner.invoke(app, ["run", "_demo"]).exit_code == 0
    later = runner.invoke(app, ["reparse", "_demo", "--since", "2099-01-01"])
    assert later.exit_code == 0, later.output
    assert "reparsed 0 raw responses" in later.output
    bad_date = runner.invoke(app, ["reparse", "_demo", "--since", "yesterday"])
    assert bad_date.exit_code == 2
    unknown = runner.invoke(app, ["reparse", "nope"])
    assert unknown.exit_code == 2 and "unknown job 'nope'" in unknown.output


def test_cli_reparse_failure_exits_1(cli_env, monkeypatch):
    def broken(ctx, raw_id):
        raise ValueError("parser bug")

    reg = jobs.Registry()
    reg.register(_job({"ds_a": broken}))
    monkeypatch.setattr(jobs, "load_registry", lambda: reg)
    with store.session(cli_env.db_path, wait=0) as con:
        _save(con, cli_env, "ds_a", {"key": "k", "value": 1.0}, _ts(1))
    result = CliRunner().invoke(app, ["reparse", "fake"])
    assert result.exit_code == 1
    assert "parser bug" in result.output
