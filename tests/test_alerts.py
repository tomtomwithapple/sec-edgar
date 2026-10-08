"""Alerts: rules after every run, the `alert` table (one per job, rule, subject, day), webhook, CLI.

Rules run per watermark key from the job's `observed` spec (which table and date column hold
that key's rows) and `config/schedule.yaml` (due days, rule parameters, per-key overrides).
"""

import json
import logging
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import alerts, calendars, jobs, raw, store
from usfin.cli import app
from usfin.settings import ConfigError, load_settings
from usfin.sources import fiscal

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
HOOK = "https://hooks.example.test/usfin"


def at(day: str, hour_utc: int = 21) -> datetime:
    """`day` at 17:00 ET (21:00 UTC, EDT)."""
    d = date.fromisoformat(day)
    return datetime(d.year, d.month, d.day, hour_utc, 0, tzinfo=UTC)


THU = at("2026-10-08")
FRI = at("2026-10-09")
SAT = at("2026-10-10")
COLUMBUS = at("2026-10-12")  # federal closed, NYSE open
TUE = at("2026-10-13")

SCHEDULE = """
jobs:
  fake:
    cron: "0 17 * * *"
    calendar: federal
    due: federal
    alerts:
      zero_new_rows: {}
      stale_watermark: {n: 2}
      value_jump: {pct: 25}
    keys:
      quiet: {due: none}
      wild: {value_jump: {pct: 500}}
  snap:
    calendar: nyse
    due: nyse
    alerts:
      row_count_swing: {pct: 2}
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


class Series:
    """A fake job: key -> [(day, value)]; rows in `obs(key, day, value)`, watermark = max day."""

    def __init__(self, items):
        self.items = {k: list(v) for k, v in items.items()}

    def fetch(self, ctx, key):
        body = json.dumps({"key": key, "items": self.items[key]}).encode()
        return [
            raw.save(
                ctx.con,
                source="fake",
                dataset="fake_obs",
                url=f"fake://{key}",
                body=body,
                http_status=200,
                raw_dir=ctx.settings.raw_dir,
            )
        ]

    def parse(self, ctx, raw_id):
        with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
            doc = json.loads(fh.read())
        ctx.con.execute(
            "CREATE TABLE IF NOT EXISTS obs (key VARCHAR, day DATE, value DOUBLE,"
            " PRIMARY KEY (key, day))"
        )
        for day, value in doc["items"]:
            ctx.con.execute(
                "INSERT INTO obs VALUES (?, ?, ?)"
                " ON CONFLICT (key, day) DO UPDATE SET value = excluded.value",
                [doc["key"], day, value],
            )
        if doc["items"]:
            ctx.advance(doc["key"], max(d for d, _ in doc["items"]))
        return len(doc["items"])

    def job(self):
        def observed(settings):
            return {
                key: alerts.Observed(
                    table="obs",
                    date_column="day",
                    match={"key": (key,)},
                    series=("key",),
                    values=("value",),
                )
                for key in self.items
            }

        return jobs.Job(
            name="fake",
            keys=tuple(self.items),
            fetch=self.fetch,
            parsers={"fake_obs": self.parse},
            observed=observed,
        )


class Snapshots:
    """A fake full-file job: each run stores `count` rows under snapshot day = today."""

    def __init__(self):
        self.count = 100

    def fetch(self, ctx, key):
        body = json.dumps({"day": ctx.today.isoformat(), "n": self.count}).encode()
        return [
            raw.save(
                ctx.con,
                source="fake",
                dataset="fake_snap",
                url="fake://snap",
                body=body,
                http_status=200,
                raw_dir=ctx.settings.raw_dir,
            )
        ]

    def parse(self, ctx, raw_id):
        with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
            doc = json.loads(fh.read())
        ctx.con.execute(
            "CREATE TABLE IF NOT EXISTS snap (file VARCHAR, day DATE, n INTEGER,"
            " PRIMARY KEY (file, day, n))"
        )
        for n in range(doc["n"]):
            ctx.con.execute(
                "INSERT INTO snap VALUES ('listed', ?, ?) ON CONFLICT DO NOTHING", [doc["day"], n]
            )
        ctx.advance("listed", doc["day"])
        return doc["n"]

    def job(self):
        observed = {
            "listed": alerts.Observed(
                table="snap", date_column="day", match={"file": ("listed",)}, snapshot=True
            )
        }
        return jobs.Job(
            name="snap",
            keys=("listed",),
            fetch=self.fetch,
            parsers={"fake_snap": self.parse},
            observed=observed,
        )


def _registry(*job_list):
    reg = jobs.Registry()
    for job in job_list:
        reg.register(job)
    return reg


def _run(settings, job, when, name=None):
    return jobs.run_job(
        name or job.name, settings=settings, registry=_registry(job), clock=lambda: when
    )


def _alerts(settings):
    with store.session(settings.db_path, wait=0) as con:
        cur = con.execute("SELECT * FROM alert ORDER BY id")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _rules(settings, rule):
    return [(a["subject"], a["raised_on"]) for a in _alerts(settings) if a["rule"] == rule]


# --- zero_new_rows ------------------------------------------------------------------------


def test_zero_new_rows_fires_only_on_a_due_day_with_nothing_after_watermark_before(settings):
    fake = Series({"a": [("2026-10-07", 100.0)]})
    job = fake.job()
    _run(settings, job, THU)  # first load: no watermark_before, nothing to compare
    assert _rules(settings, "zero_new_rows") == []

    fake.items["a"].append(("2026-10-08", 101.0))
    assert _run(settings, job, FRI).status == "ok"
    assert _rules(settings, "zero_new_rows") == []  # a row dated after watermark_before

    _run(settings, job, SAT)  # weekend: not a due day
    _run(settings, job, COLUMBUS)  # federal holiday: not a due day
    assert _rules(settings, "zero_new_rows") == []

    result = _run(settings, job, TUE)  # due, and nothing after 2026-10-08
    assert result.status == "ok"
    [alert] = [a for a in _alerts(settings) if a["rule"] == "zero_new_rows"]
    assert (alert["job"], alert["subject"], alert["raised_on"]) == ("fake", "a", date(2026, 10, 13))
    assert json.loads(alert["detail"])["watermark_before"] == "2026-10-08"


def test_zero_new_rows_compares_to_the_days_first_run_so_a_repeat_run_does_not_fire(settings):
    fake = Series({"a": [("2026-10-07", 100.0)]})
    job = fake.job()
    _run(settings, job, THU)
    fake.items["a"].append(("2026-10-08", 101.0))
    _run(settings, job, FRI)
    _run(settings, job, at("2026-10-09", 22))  # same ET day, nothing new since the first run
    assert _rules(settings, "zero_new_rows") == []


def test_zero_new_rows_skips_keys_whose_due_rule_is_none(settings):
    fake = Series({"quiet": [("2026-10-07", 1.0)]})
    job = fake.job()
    _run(settings, job, THU)
    _run(settings, job, FRI)
    assert _rules(settings, "zero_new_rows") == []


# --- stale_watermark ----------------------------------------------------------------------


def test_stale_watermark_counts_due_days_after_the_watermark(settings):
    fake = Series({"a": [("2026-10-05", 1.0)], "b": [("2026-10-06", 1.0)]})
    _run(settings, fake.job(), THU)
    # a: due days after 10-05 through 10-08 are 6, 7, 8 = 3 > 2; b: 7, 8 = 2
    assert _rules(settings, "stale_watermark") == [("a", date(2026, 10, 8))]
    [alert] = [a for a in _alerts(settings) if a["rule"] == "stale_watermark"]
    detail = json.loads(alert["detail"])
    assert detail["watermark"] == "2026-10-05"
    assert detail["due_days_since"] == 3
    assert detail["n"] == 2


def test_stale_watermark_does_not_count_holidays(settings):
    fake = Series({"a": [("2026-10-08", 1.0)]})
    _run(settings, fake.job(), TUE)  # due days after 10-08: 9, 13 (12 is Columbus Day)
    assert _rules(settings, "stale_watermark") == []


def test_due_rules_from_schedule_yaml(settings):
    cals = calendars.load_calendars(settings.config_dir)
    every = alerts.DueRule("federal")
    eighth = alerts.DueRule("federal", nth_of_month=8)
    assert every.is_due(cals, date(2026, 10, 8))
    assert not every.is_due(cals, date(2026, 10, 12))
    # October 2026 federal days: 1, 2, 5, 6, 7, 8, 9, 13 (12 is Columbus Day)
    assert [d for d in range(1, 32) if eighth.is_due(cals, date(2026, 10, d))] == [13]
    assert not alerts.DueRule(None).is_due(cals, date(2026, 10, 8))
    rules = alerts.load_rules(settings.config_dir)
    assert rules["fake"].due_for("a") == every
    assert rules["fake"].due_for("quiet") == alerts.DueRule(None)
    assert rules["fake"].params_for("wild")["value_jump"] == {"pct": 500}
    assert rules["fake"].params_for("a")["value_jump"] == {"pct": 25}
    assert rules["snap"].due_for("listed") == alerts.DueRule("nyse")


@pytest.mark.parametrize(
    "text",
    [
        "jobs: {fake: {alerts: {no_such_rule: {}}}}",
        "jobs: {fake: {due: lunar}}",
        "jobs: {fake: {due: {calendar: federal, nth_of_month: 0}}}",
        "jobs: {fake: {alerts: {stale_watermark: {}}}}",
    ],
)
def test_bad_schedule_yaml_is_a_config_error(tmp_path, text):
    (tmp_path / "schedule.yaml").write_text(text, "utf-8")
    with pytest.raises(ConfigError):
        alerts.load_rules(tmp_path)


def test_repo_schedule_yaml_has_fiscal_due_rules():
    rules = alerts.load_rules(REPO_CONFIG)["fiscal"]
    assert rules.due_for("dts_operating_cash") == alerts.DueRule("federal")
    assert rules.due_for("debt_to_penny") == alerts.DueRule("federal")
    assert rules.due_for("mts_table_1") == alerts.DueRule("federal", nth_of_month=8)
    assert set(rules.params_for("dts_operating_cash")) >= {
        "zero_new_rows",
        "stale_watermark",
        "value_jump",
    }


# --- value_jump ---------------------------------------------------------------------------


def test_value_jump_flags_new_rows_past_the_threshold_and_never_blocks(settings):
    # a historic 200% jump in the first load is not flagged (no baseline yet)
    fake = Series({"a": [("2026-10-06", 100.0), ("2026-10-07", 300.0)]})
    job = fake.job()
    _run(settings, job, THU)
    assert _rules(settings, "value_jump") == []

    fake.items["a"].append(("2026-10-08", 390.0))  # +30% > 25
    result = _run(settings, job, FRI)
    assert result.status == "ok"
    with store.session(settings.db_path, wait=0) as con:
        assert con.execute("SELECT value FROM obs WHERE day = '2026-10-08'").fetchone() == (390.0,)
    [alert] = [a for a in _alerts(settings) if a["rule"] == "value_jump"]
    assert alert["subject"] == "a:value:a@2026-10-08"
    detail = json.loads(alert["detail"])
    assert detail["previous"] == 300.0 and detail["value"] == 390.0
    assert detail["pct_change"] == pytest.approx(30.0)
    assert detail["threshold_pct"] == 25

    fake.items["a"].append(("2026-10-09", 420.0))  # +7.7%
    _run(settings, job, at("2026-10-13"))
    assert len(_rules(settings, "value_jump")) == 1


def test_value_jump_threshold_is_per_key(settings):
    fake = Series({"wild": [("2026-10-07", 100.0)]})
    job = fake.job()
    _run(settings, job, THU)
    fake.items["wild"].append(("2026-10-08", 400.0))  # +300% < 500
    _run(settings, job, FRI)
    assert _rules(settings, "value_jump") == []


# --- row_count_swing ----------------------------------------------------------------------


def test_row_count_swing_fires_outside_2_percent_and_not_on_the_first_snapshot(settings):
    snaps = Snapshots()
    job = snaps.job()
    _run(settings, job, THU)  # first snapshot: no check
    snaps.count = 102  # +2.0%: inside
    _run(settings, job, FRI)
    assert _rules(settings, "row_count_swing") == []
    snaps.count = 99  # -2.9%
    _run(settings, job, COLUMBUS)
    [alert] = [a for a in _alerts(settings) if a["rule"] == "row_count_swing"]
    assert (alert["subject"], alert["raised_on"]) == ("listed", date(2026, 10, 12))
    detail = json.loads(alert["detail"])
    assert (detail["previous_rows"], detail["rows"]) == (102, 99)


# --- the alert table and dedupe -----------------------------------------------------------


def test_one_alert_per_job_rule_subject_and_day(settings):
    with store.session(settings.db_path, wait=0) as con:
        first = alerts.raise_alert(con, job="j", rule="r", subject="s", detail={"x": 1}, now=THU)
        again = alerts.raise_alert(con, job="j", rule="r", subject="s", detail={"x": 2}, now=THU)
        other = alerts.raise_alert(con, job="j", rule="r", subject="t", detail=None, now=THU)
        next_day = alerts.raise_alert(con, job="j", rule="r", subject="s", detail=None, now=FRI)
    assert first is not None and other is not None and next_day is not None
    assert again is None
    rows = _alerts(settings)
    assert [(a["subject"], a["raised_on"]) for a in rows] == [
        ("s", date(2026, 10, 8)),
        ("t", date(2026, 10, 8)),
        ("s", date(2026, 10, 9)),
    ]
    assert json.loads(rows[0]["detail"]) == {"x": 1}
    assert rows[0]["raised_at"] == THU.replace(tzinfo=None)


def test_raised_on_is_the_new_york_date(settings):
    late = datetime(2026, 10, 9, 2, 0, tzinfo=UTC)  # 22:00 ET on 10-08
    with store.session(settings.db_path, wait=0) as con:
        alerts.raise_alert(con, job="j", rule="r", subject="s", detail=None, now=late)
    assert _alerts(settings)[0]["raised_on"] == date(2026, 10, 8)


def test_repeat_runs_on_one_day_keep_one_alert(settings):
    fake = Series({"a": [("2026-10-05", 1.0)]})
    job = fake.job()
    _run(settings, job, THU)
    _run(settings, job, at("2026-10-08", 22))
    assert _rules(settings, "stale_watermark") == [("a", date(2026, 10, 8))]


def test_a_failing_rule_does_not_fail_the_run(settings, monkeypatch, caplog):
    def boom(*args, **kwargs):
        raise RuntimeError("rule exploded")

    monkeypatch.setattr(alerts, "_stale_watermark", boom)
    fake = Series({"a": [("2026-10-05", 1.0)]})
    with caplog.at_level(logging.ERROR, logger="usfin.alerts"):
        result = _run(settings, fake.job(), THU)
    assert result.status == "ok"
    assert "rule exploded" in caplog.text


# --- webhook ------------------------------------------------------------------------------


def test_new_alerts_are_posted_to_the_webhook_once(settings):
    settings = replace(settings, alert_webhook_url=HOOK)
    fake = Series({"a": [("2026-10-05", 1.0)]})
    job = fake.job()
    with respx.mock(assert_all_called=True) as router:
        route = router.post(HOOK).mock(return_value=httpx.Response(204))
        _run(settings, job, THU)
        _run(settings, job, at("2026-10-08", 22))  # same alert again: deduped, not re-posted
    assert route.call_count == 1
    body = json.loads(route.calls[0].request.content)
    assert body["job"] == "fake"
    assert body["rule"] == "stale_watermark"
    assert body["subject"] == "a"
    assert body["raised_on"] == "2026-10-08"
    assert body["detail"]["watermark"] == "2026-10-05"
    assert isinstance(body["id"], int)


@pytest.mark.parametrize(
    "reply", [httpx.Response(500), httpx.ConnectError("refused")], ids=["500", "connect"]
)
def test_webhook_failure_is_logged_not_fatal(settings, caplog, reply):
    settings = replace(settings, alert_webhook_url=HOOK)
    fake = Series({"a": [("2026-10-05", 1.0)]})
    with respx.mock() as router:
        router.post(HOOK).mock(side_effect=[reply])
        with caplog.at_level(logging.WARNING, logger="usfin.alerts"):
            result = _run(settings, fake.job(), THU)
    assert result.status == "ok"
    assert _rules(settings, "stale_watermark") == [("a", date(2026, 10, 8))]
    assert "webhook" in caplog.text


def test_no_webhook_without_the_setting(settings):
    fake = Series({"a": [("2026-10-05", 1.0)]})
    with respx.mock() as router:  # any request would fail as unmatched
        result = _run(settings, fake.job(), THU)
    assert result.status == "ok"
    assert router.calls.call_count == 0


# --- schema_drift (S8's hook) -------------------------------------------------------------


def test_fiscal_schema_drift_lands_as_an_alert(settings):
    dataset = fiscal.Dataset(name="tiny", endpoint="v1/tiny", pk=("record_date",))
    types = {"record_date": "DATE", "amt": "CURRENCY"}
    first = fiscal.Page(1, 1, 1, dict(types), [{"record_date": date(2026, 10, 7), "amt": None}])
    wider = fiscal.Page(
        2,
        1,
        1,
        {**types, "new_pct": "PERCENTAGE"},
        [{"record_date": date(2026, 10, 8), "amt": Decimal("1.5"), "new_pct": 4.25}],
    )
    with store.session(settings.db_path, wait=0) as con:
        ctx = jobs.JobContext(
            job=fiscal.JOB,
            con=con,
            settings=settings,
            today=date(2026, 10, 8),
            now=THU,
            trigger="manual",
            run_id=None,
            watermarks={},
        )
        fiscal.upsert_page(ctx, dataset, first)
        fiscal.upsert_page(ctx, dataset, wider)
    [alert] = _alerts(settings)
    assert (alert["job"], alert["rule"], alert["subject"]) == ("fiscal", "schema_drift", "tiny")
    assert json.loads(alert["detail"]) == {"table": "fd_tiny", "columns": {"new_pct": "PERCENTAGE"}}


def test_fiscal_observed_spec_follows_the_registry(settings):
    observed = fiscal.JOB.observed(settings)
    assert set(observed) == set(fiscal.load_datasets(settings.config_dir))
    dts = observed["dts_operating_cash"]
    assert (dts.table, dts.date_column, dts.series) == (
        "fd_dts_operating_cash",
        "record_date",
        ("account_type",),
    )
    assert dts.values == ("open_today_bal",)
    assert dts.jump_match == {"account_type": ("Treasury General Account (TGA) Closing Balance",)}
    fx = observed["rates_of_exchange"]
    assert fx.series == ("country_currency_desc",)  # src_line_nbr shifts between releases
    assert observed["auctions"].date_column == "auction_date"
    assert observed["auctions"].values == ()


# --- CLI ----------------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    with store.session(settings.db_path, wait=0) as con:
        a = alerts.raise_alert(con, job="fiscal", rule="r1", subject="s1", detail={"k": 1}, now=THU)
        b = alerts.raise_alert(con, job="fiscal", rule="r2", subject="s2", detail=None, now=FRI)
    return a, b


def test_cli_alerts_lists_open_ack_hides_and_all_shows(cli_env):
    a, b = cli_env
    runner = CliRunner()
    listed = runner.invoke(app, ["alerts"])
    assert listed.exit_code == 0, listed.output
    assert f"#{a}" in listed.output and f"#{b}" in listed.output
    assert "r1" in listed.output and "s1" in listed.output and '{"k": 1}' in listed.output

    acked = runner.invoke(app, ["alerts", "ack", str(a)])
    assert acked.exit_code == 0, acked.output
    assert f"#{a}" in acked.output

    open_only = runner.invoke(app, ["alerts"])
    assert f"#{a}" not in open_only.output and f"#{b}" in open_only.output

    everything = runner.invoke(app, ["alerts", "--all"])
    line = next(x for x in everything.output.splitlines() if x.startswith(f"#{a}"))
    assert "acked" in line

    again = runner.invoke(app, ["alerts", "ack", str(a)])
    assert again.exit_code == 0  # acking twice is harmless
    missing = runner.invoke(app, ["alerts", "ack", "999"])
    assert missing.exit_code == 1
    assert "999" in missing.output


def test_cli_alerts_with_none_open(cli_env):
    runner = CliRunner()
    for alert_id in cli_env:
        runner.invoke(app, ["alerts", "ack", str(alert_id)])
    result = runner.invoke(app, ["alerts"])
    assert result.exit_code == 0
    assert "no open alerts" in result.output
