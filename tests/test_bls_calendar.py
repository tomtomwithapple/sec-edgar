"""BLS release calendar (job `bls_calendar`), release days as `bls` due days, `value_jump` on BLS
series, and the release-day retry (`bls_retry`, C6, C8).

`tests/fixtures/bls/bls.ics` is the live BLS release schedule fetched 2026-10-08 from
https://www.bls.gov/schedule/news_release/bls.ics, trimmed to 14 of its 313 events (the
VCALENDAR header, the VTIMEZONE block and the closing lines kept as published). BLS API answers
come from the S15 recording `v1_timeseries.json`; no request leaves the process.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import __version__, alerts, http, jobs, ratelimit, raw, scheduler, store
from usfin.cli import app
from usfin.settings import ConfigError, load_settings
from usfin.sources import bls, bls_calendar

FIXTURES = Path(__file__).parent / "fixtures" / "bls"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
ICS_HOST = "www.bls.gov"
API_HOST = "api.bls.gov"
CPI, CORE, PPI = "CUSR0000SA0", "CUSR0000SA0L1E", "WPSFD4"
EMAIL = "ops@example.test"


def et(day: str, hour: int, minute: int = 0) -> datetime:
    """`day` at hour:minute New York time, as an aware UTC datetime."""
    local = datetime.fromisoformat(f"{day}T{hour:02d}:{minute:02d}").replace(tzinfo=scheduler.ET)
    return local.astimezone(UTC)


def utc(*args) -> datetime:
    """A UTC instant as stored: naive."""
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


def _ics() -> str:
    return (FIXTURES / "bls.ics").read_text("utf-8")


def _doc() -> dict:
    return json.loads((FIXTURES / "v1_timeseries.json").read_text("utf-8"))


class Clock:
    """One instant for the job clock, the scheduler and `raw._utcnow` (`fetched_at`)."""

    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(et("2026-10-08", 6))
    monkeypatch.setattr(raw, "_utcnow", c.naive)
    return c


@pytest.fixture
def settings(tmp_path, monkeypatch, clock):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    for key in ("BLS_API_KEY", "BLS_CONTACT_EMAIL", "ALERT_WEBHOOK_URL"):
        monkeypatch.delenv(key, raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    t = [0.0]
    monkeypatch.setattr(
        ratelimit,
        "LIMITERS",
        ratelimit.Limiters(clock=lambda: t[0], sleep=lambda s: t.__setitem__(0, t[0] + s)),
    )
    monkeypatch.setattr(http, "BACKOFF_BASE", 0.0)
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        bls_api_key=None,
        bls_contact_email=EMAIL,
    )


class Fake:
    """Answers www.bls.gov with `ics` (or `status`) and api.bls.gov with `doc`."""

    def __init__(self):
        self.ics = _ics()
        self.ics_status = 200
        self.doc = _doc()
        self.ics_requests: list[httpx.Request] = []
        self.api_requests: list[httpx.Request] = []

    def calendar(self, request: httpx.Request) -> httpx.Response:
        self.ics_requests.append(request)
        if self.ics_status != 200:
            return httpx.Response(self.ics_status, text="<html>Access Denied</html>")
        return httpx.Response(200, text=self.ics, headers={"Content-Type": "text/calendar"})

    def api(self, request: httpx.Request) -> httpx.Response:
        self.api_requests.append(request)
        return httpx.Response(200, json=self.doc)


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.route(host=ICS_HOST).mock(side_effect=f.calendar)
        router.route(host=API_HOST).mock(side_effect=f.api)
        yield f


def _registry():
    reg = jobs.Registry()
    reg.register(bls.JOB)
    reg.register(bls_calendar.JOB)
    return reg


def _run(settings, name, clock, when=None):
    if when is not None:
        clock.t = when
    return jobs.run_job(name, settings=settings, registry=_registry(), clock=clock, chain=False)


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _alerts(settings, rule=None):
    rows = _query(
        settings,
        "SELECT job, rule, subject, detail FROM alert"
        + (" WHERE rule = ?" if rule else "")
        + " ORDER BY id",
        [rule] if rule else [],
    )
    return [(j, r, s, json.loads(d) if d else None) for j, r, s, d in rows]


def _add_period(doc, series_id, year, period, value):
    series = next(s for s in doc["Results"]["series"] if s["seriesID"] == series_id)
    series["data"].insert(
        0, {"year": year, "period": period, "periodName": "", "value": value, "footnotes": [{}]}
    )


# --- parsing ---------------------------------------------------------------------------------


def test_parse_reads_only_vevent_blocks_with_their_new_york_start_and_summary():
    releases = bls_calendar.parse_ics(_ics())
    # the VCALENDAR's own SUMMARY and the VTIMEZONE's DTSTARTs are not releases
    assert len(releases) == 14
    assert "BLS.gov Economic News Release Schedule" not in {r.release_name for r in releases}
    assert min(r.release_date for r in releases) == date(2025, 9, 25)
    assert max(r.release_date for r in releases) == date(2026, 12, 30)
    cpi = next(
        r
        for r in releases
        if r.release_date == date(2026, 10, 14) and r.release_name == "Consumer Price Index"
    )
    assert cpi.released_at == utc(2026, 10, 14, 12, 30)  # 08:30 EDT, naive UTC
    jolts = next(r for r in releases if r.release_date == date(2026, 11, 3))
    assert jolts.release_name == "Job Openings and Labor Turnover Survey"
    assert jolts.released_at == utc(2026, 11, 3, 15, 0)  # 10:00 EST


def test_parse_strips_trailing_blanks_unfolds_lines_and_unescapes_text():
    names = {r.release_name for r in bls_calendar.parse_ics(_ics())}
    assert "Unpaid Eldercare in the United States" in names  # published with a trailing blank
    folded = (
        "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART;TZID=US-Eastern:20261014T083000\r\n"
        "SUMMARY:Consumer Price\r\n  Index\\, monthly\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
    )
    [r] = bls_calendar.parse_ics(folded)
    assert r.release_name == "Consumer Price Index, monthly"


def test_parse_rejects_an_event_without_a_start_or_with_an_unknown_zone():
    with pytest.raises(ValueError, match="DTSTART"):
        bls_calendar.parse_ics("BEGIN:VEVENT\nSUMMARY:Employment Situation\nEND:VEVENT\n")
    with pytest.raises(ValueError, match="Mars"):
        bls_calendar.parse_ics(
            "BEGIN:VEVENT\nDTSTART;TZID=Mars/Olympus:20261014T083000\nSUMMARY:x\nEND:VEVENT\n"
        )


# --- release names -> series ------------------------------------------------------------------


def test_every_configured_series_maps_to_a_release_the_calendar_publishes():
    series = bls.load_series(REPO_CONFIG)
    assert {s.series_id: s.release for s in series.values()} == {
        CPI: "Consumer Price Index",
        CORE: "Consumer Price Index",
        PPI: "Producer Price Index",
        "CES0000000001": "Employment Situation",
        "LNS14000000": "Employment Situation",
        "CES0500000003": "Employment Situation",
        "JTS000000000000000JOL": "Job Openings and Labor Turnover Survey",
    }
    published = {r.release_name for r in bls_calendar.parse_ics(_ics())}
    assert {s.release for s in series.values()} <= published
    assert bls_calendar.series_for("Consumer Price Index", series) == [CPI, CORE]
    assert bls_calendar.series_for("Real Earnings", series) == []


# --- job bls_calendar ---------------------------------------------------------------------------


def test_the_job_stores_every_release_and_a_refetch_changes_nothing(settings, fake, clock):
    first = _run(settings, "bls_calendar", clock)
    assert first.status == "ok", first.error
    assert first.rows_upserted == 14
    raws = _query(settings, "SELECT source, dataset, url FROM raw_response")
    assert raws == [("bls", "bls_ics", bls_calendar.ICS_URL)]
    rows = _query(settings, "SELECT * FROM bls_release ORDER BY ALL")
    second = _run(settings, "bls_calendar", clock, when=et("2026-10-12", 6))
    assert second.status == "ok"
    assert second.rows_upserted == 0  # C2
    assert _query(settings, "SELECT * FROM bls_release ORDER BY ALL") == rows


def test_a_rescheduled_release_replaces_the_old_date_within_the_files_range(settings, fake, clock):
    _run(settings, "bls_calendar", clock)
    fake.ics = fake.ics.replace(
        "DTSTART;TZID=US-Eastern:20261014T083000\nDURATION:PT0M\nSUMMARY:Consumer Price Index",
        "DTSTART;TZID=US-Eastern:20261016T083000\nDURATION:PT0M\nSUMMARY:Consumer Price Index",
    )
    assert "20261016" in fake.ics
    result = _run(settings, "bls_calendar", clock, when=et("2026-10-12", 6))
    assert result.rows_upserted == 2  # one gone, one new
    cpi = _query(
        settings,
        "SELECT release_date FROM bls_release WHERE release_name = 'Consumer Price Index'"
        " ORDER BY 1",
    )
    assert [d for (d,) in cpi] == [date(2025, 10, 24), date(2026, 10, 16)]


def test_the_request_identifies_with_the_contact_email(settings, fake, clock):
    _run(settings, "bls_calendar", clock)
    [request] = fake.ics_requests
    assert request.headers["User-Agent"] == f"usfin/{__version__} ({EMAIL})"


def test_without_a_contact_email_no_request_is_sent(settings, fake, clock):
    result = jobs.run_job(
        "bls_calendar",
        settings=replace(settings, bls_contact_email=None),
        registry=_registry(),
        clock=clock,
    )
    assert result.status == "failed"
    assert "BLS_CONTACT_EMAIL" in result.error
    assert fake.ics_requests == []


def test_the_contact_email_setting_is_read_from_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("BLS_CONTACT_EMAIL", EMAIL)
    assert load_settings(env_file=tmp_path / "missing.env").bls_contact_email == EMAIL
    monkeypatch.delenv("BLS_CONTACT_EMAIL")
    assert load_settings(env_file=tmp_path / "missing.env").bls_contact_email is None


def test_the_contact_email_must_look_like_one(settings, fake, clock):
    bad = replace(settings, bls_contact_email="https://example.test")
    with (
        store.session(bad.db_path, wait=0) as con,
        http.Client(con, settings=bad) as client,
        pytest.raises(ConfigError, match="BLS_CONTACT_EMAIL"),
    ):
        client.get(bls_calendar.ICS_URL, source="bls", dataset="bls_ics")
    assert fake.ics_requests == []


def test_reparse_rebuilds_the_same_releases_without_a_request(settings, fake, clock):
    _run(settings, "bls_calendar", clock)
    rows = _query(settings, "SELECT * FROM bls_release ORDER BY ALL")
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM bls_release")
        result = jobs.reparse(con, bls_calendar.JOB, settings=settings)
    assert result.raw_ids and result.rows == 14
    assert _query(settings, "SELECT * FROM bls_release ORDER BY ALL") == rows
    assert len(fake.ics_requests) == 1


# --- calendar_stale -------------------------------------------------------------------------------


def test_no_release_dates_beyond_30_days_raise_calendar_stale(settings, fake, clock):
    _run(settings, "bls_calendar", clock)  # 2026-10-08: covered to 2026-12-30
    assert _alerts(settings, "calendar_stale") == []
    _run(settings, "bls_calendar", clock, when=et("2026-12-07", 6))  # 12-30 is < 30 days out
    [(job, _, subject, detail)] = _alerts(settings, "calendar_stale")
    assert (job, subject) == ("bls_calendar", "bls_release")
    assert detail["latest_release"] == "2026-12-30"
    assert detail["horizon_days"] == 30


def test_a_failed_fetch_still_checks_how_far_the_stored_calendar_reaches(settings, fake, clock):
    _run(settings, "bls_calendar", clock)
    fake.ics_status = 403
    result = _run(settings, "bls_calendar", clock, when=et("2026-12-14", 6))
    assert result.status == "failed"
    assert [a[2] for a in _alerts(settings, "calendar_stale")] == ["bls_release"]


def test_bls_calendar_is_registered_and_scheduled_monday_0600():
    assert "bls_calendar" in jobs.load_registry()
    assert scheduler.load_schedule(REPO_CONFIG)["bls_calendar"] == scheduler.Entry(
        "bls_calendar", "0 6 * * mon", "any"
    )


# --- due days = release days ------------------------------------------------------------------


def test_bls_due_days_are_its_series_release_days(settings, fake, clock):
    rules = alerts.load_rules(REPO_CONFIG)["bls"]
    assert rules.due_for(CPI) == alerts.DueRule(None, releases="bls_release")
    _run(settings, "bls_calendar", clock)
    with store.session(settings.db_path, wait=0) as con:
        cpi = alerts.due_days(con, settings, "bls_release", CPI)
        ppi = alerts.due_days(con, settings, "bls_release", PPI)
        unmapped = alerts.due_days(con, settings, "bls_release", "NOT_CONFIGURED")
    assert cpi == {date(2025, 10, 24), date(2026, 10, 14)}
    assert ppi == {date(2026, 10, 15)}
    assert unmapped == frozenset()


def test_a_release_due_rule_in_schedule_yaml_is_checked(tmp_path):
    (tmp_path / "schedule.yaml").write_text(
        "jobs: {x: {due: {releases: bls_release, calendar: federal}}}", "utf-8"
    )
    with pytest.raises(ConfigError):
        alerts.load_rules(tmp_path)


# --- value_jump on BLS series --------------------------------------------------------------------


def test_value_jump_uses_each_series_threshold_from_bls_yaml(settings, fake, clock):
    _run(settings, "bls", clock, when=et("2026-10-13", 9))  # baseline: CPI 2026-M08
    _add_period(fake.doc, CPI, "2026", "M09", "343.000")  # +2.65 % > CPI's 2 %
    _add_period(fake.doc, PPI, "2026", "M09", "160.000")  # +1.64 % < PPI's 3 %
    _run(settings, "bls", clock, when=et("2026-10-14", 9))
    jumps = _alerts(settings, "value_jump")
    assert [(j, s) for j, _, s, _ in jumps] == [("bls", f"{CPI}:value:{CPI}@2026-09-01")]
    detail = jumps[0][3]
    assert detail["threshold_pct"] == 2.0
    assert detail["previous"] == pytest.approx(334.131)


def test_value_jump_skips_a_first_load(settings, fake, clock):
    _add_period(fake.doc, CPI, "2026", "M09", "400.000")
    _run(settings, "bls", clock, when=et("2026-10-14", 9))
    assert _alerts(settings, "value_jump") == []


# --- retry on release days --------------------------------------------------------------------


@pytest.fixture
def make(settings):
    made = []

    def _make(clock, settings=settings):
        sched = scheduler.Scheduler(settings, _registry(), clock=clock)
        sched.start(paused=True)
        made.append(sched)
        return sched

    yield _make
    for sched in made:
        sched.shutdown()


def _ready(settings, clock):
    """Calendar stored, and a `bls` run on 2026-10-13 (no release) set every watermark."""
    _run(settings, "bls_calendar", clock, when=et("2026-10-12", 6))
    _run(settings, "bls", clock, when=et("2026-10-13", 9))


def _retry(sched):
    job = sched.aps.get_job("bls_retry")
    return None if job is None else job.trigger.run_date


def test_a_release_day_without_the_new_period_retries_in_15_minutes_keyless(
    settings, fake, clock, make
):
    _ready(settings, clock)
    clock.t = et("2026-10-14", 9)  # CPI day; the API still serves 2026-M08
    sched = make(clock)
    assert sched.fire("bls").status == "ok"
    assert _retry(sched) == et("2026-10-14", 9, 15)
    assert _alerts(settings, "stale_watermark") == []


def test_with_a_key_the_retry_is_10_minutes_and_replaces_the_pending_one(
    settings, fake, clock, make
):
    _ready(settings, clock)
    keyed = replace(settings, bls_api_key="k")
    clock.t = et("2026-10-14", 9)
    sched = make(clock, settings=keyed)
    sched.fire("bls")
    clock.t = et("2026-10-14", 9, 10)
    sched.fire("bls")
    assert _retry(sched) == et("2026-10-14", 9, 20)
    assert [j.id for j in sched.aps.get_jobs()].count("bls_retry") == 1


def test_the_retry_runs_bls_again_and_stops_once_the_period_arrives(settings, fake, clock, make):
    _ready(settings, clock)
    clock.t = et("2026-10-14", 9)
    sched = make(clock)
    sched.fire("bls")
    assert _retry(sched) is not None
    _add_period(fake.doc, CPI, "2026", "M09", "335.000")
    _add_period(fake.doc, CORE, "2026", "M09", "338.500")
    clock.t = et("2026-10-14", 9, 15)
    calls = len(fake.api_requests)
    result = sched.retry("bls")
    assert result.status == "ok"
    assert len(fake.api_requests) == calls + 1
    assert _retry(sched) is None
    runs = _query(settings, "SELECT trigger FROM job_run WHERE job = 'bls' ORDER BY id")
    assert runs[-1] == ("scheduled",)


def test_a_release_with_one_series_still_missing_keeps_retrying(settings, fake, clock, make):
    _ready(settings, clock)
    _add_period(fake.doc, CPI, "2026", "M09", "335.000")  # core CPI not yet
    clock.t = et("2026-10-14", 9)
    sched = make(clock)
    sched.fire("bls")
    assert _retry(sched) == et("2026-10-14", 9, 15)


def test_no_retry_on_a_day_without_a_release_of_the_series(settings, fake, clock, make):
    _ready(settings, clock)
    clock.t = et("2026-10-16", 9)  # no configured release that day
    sched = make(clock)
    sched.fire("bls")
    assert _retry(sched) is None
    assert _alerts(settings, "stale_watermark") == []


def test_past_noon_the_retries_stop_and_stale_watermark_is_raised(settings, fake, clock, make):
    _ready(settings, clock)
    clock.t = et("2026-10-14", 9)
    sched = make(clock)
    sched.fire("bls")
    clock.t = et("2026-10-14", 11, 45)
    sched.retry("bls")
    assert _retry(sched) == et("2026-10-14", 12, 0)  # 12:00 is the last try
    assert _alerts(settings, "stale_watermark") == []
    clock.t = et("2026-10-14", 12, 0)
    sched.retry("bls")
    assert _retry(sched) is None
    stale = _alerts(settings, "stale_watermark")
    assert sorted(s for _, _, s, _ in stale) == [CPI, CORE]  # not PPI (released 10-15)
    detail = stale[0][3]
    assert detail["release"] == "Consumer Price Index"
    assert detail["release_date"] == "2026-10-14"
    assert detail["watermark"] == "2026-M08"


def test_a_failed_run_on_a_release_day_retries_too(settings, fake, clock, make):
    _ready(settings, clock)
    fake.doc = {"status": "REQUEST_NOT_PROCESSED", "message": ["daily threshold"]}
    clock.t = et("2026-10-14", 9)
    sched = make(clock)
    assert sched.fire("bls").status == "failed"
    assert _retry(sched) == et("2026-10-14", 9, 15)


# --- CLI ----------------------------------------------------------------------------------------


def test_cli_bls_releases_lists_the_next_configured_releases(settings, fake, clock, monkeypatch):
    _run(settings, "bls_calendar", clock)
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    out = CliRunner().invoke(app, ["bls", "releases", "--next", "3", "--from", "2026-10-08"])
    assert out.exit_code == 0, out.output
    lines = out.output.splitlines()
    assert lines[0] == "14 releases stored, 2025-09-25..2026-12-30"
    assert lines[1].startswith("2026-10-14 Wed 08:30  Consumer Price Index")
    assert CPI in lines[1] and CORE in lines[1]
    assert lines[2].startswith("2026-10-15 Thu 08:30  Producer Price Index")
    assert lines[3].startswith("2026-11-03 Tue 10:00  Job Openings and Labor Turnover Survey")
    assert len(lines) == 4
    every = CliRunner().invoke(
        app, ["bls", "releases", "--next", "2", "--from", "2026-10-08", "--all"]
    )
    assert "Real Earnings" in every.output
