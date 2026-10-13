"""FRED series, point in time (S42).

`tests/fixtures/fred/GDPC1_observations.json` is a live ALFRED response (2026-10-13,
`fred/series/observations?series_id=GDPC1&realtime_start=1776-07-04&realtime_end=9999-12-31
&observation_start=2019-10-01&observation_end=2020-07-01&file_type=json`: 33 vintages of four
quarters) and `GDPC1_series.json` the live `fred/series?series_id=GDPC1`; no API key is in either.

`FakeFred` answers the way FRED did live on 2026-10-13: `vintagedates` lists the distinct
`realtime_start`s; `observations` returns every vintage overlapping the requested real-time window
with `realtime_start`/`realtime_end` clipped to it (DGS10 2026-10-05.. gave its 2026-09-28 value
`realtime_start` 2026-10-05), honours `observation_start`, `limit` and `offset`, and refuses (400)
a window holding more than `limit_vintages` vintage dates, counting one more than it lists
("2001 vintage dates" for a window over 2000 listed ones).
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from usfin import http, jobs, ratelimit, raw, scheduler, store
from usfin.settings import ConfigError, load_settings
from usfin.sources import fred
from usfin.web import app as web

FIXTURES = Path(__file__).parent / "fixtures" / "fred"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
FRED_HOST = "api.stlouisfed.org"
KEY = "0123456789abcdef0123456789abcdef"  # made up
Q1 = date(2020, 1, 1)


def _naive(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


T1 = _naive(2026, 10, 13, 22, 30)  # 18:30 ET, the scheduled fire
T2 = _naive(2026, 10, 14, 22, 30)


class FakeFred:
    def __init__(self):
        self.series = json.loads((FIXTURES / "GDPC1_series.json").read_text("utf-8"))
        self.rows = json.loads((FIXTURES / "GDPC1_observations.json").read_text("utf-8"))[
            "observations"
        ]
        self.limit_vintages = 2000
        self.fail_obs: set[int] = set()
        """Observations requests (1-based count) answered 400."""
        self.obs_calls = 0
        self.requests: list[httpx.Request] = []

    def vintages(self) -> list[str]:
        return sorted({r["realtime_start"] for r in self.rows})

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        p = request.url.params
        path = request.url.path.removeprefix("/fred/")
        if path == "series":
            return httpx.Response(200, json=self.series)
        limit, offset = int(p.get("limit", 100000)), int(p.get("offset", 0))
        if path == "series/vintagedates":
            days = self.vintages()
            return httpx.Response(
                200, json={"count": len(days), "vintage_dates": days[offset : offset + limit]}
            )
        assert path == "series/observations", path
        self.obs_calls += 1
        if self.obs_calls in self.fail_obs:
            return httpx.Response(400, json={"error_code": 400, "error_message": "Bad Request."})
        start, end = p["realtime_start"], p["realtime_end"]
        listed = [d for d in self.vintages() if start <= d <= end]
        if len(listed) + 1 > self.limit_vintages:
            return httpx.Response(400, json={"error_code": 400, "error_message": "too many"})
        out = [
            {
                **r,
                "realtime_start": max(r["realtime_start"], start),
                "realtime_end": min(r["realtime_end"], end),
            }
            for r in self.rows
            if r["realtime_start"] <= end
            and r["realtime_end"] >= start
            and r["date"] >= p.get("observation_start", "0000")
        ]
        out.sort(key=lambda r: (r["date"], r["realtime_start"]))
        return httpx.Response(
            200, json={"count": len(out), "observations": out[offset : offset + limit]}
        )


class Clock:
    def __init__(self):
        self.t = T1

    def __call__(self) -> datetime:
        return self.t


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(raw, "_utcnow", c)
    return c


@pytest.fixture
def settings(tmp_path, monkeypatch, clock):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    (config / "fred.yaml").write_text(
        "lookback_days: 1825\nseries:\n  - id: GDPC1\n    jump_pct: 10\n", "utf-8"
    )
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
        fred_api_key=KEY,
    )


@pytest.fixture
def api(settings):
    fake = FakeFred()
    with respx.mock(assert_all_called=False) as router:
        router.route(host=FRED_HOST).mock(side_effect=fake)
        yield fake


def _run(settings, clock, at=T1):
    clock.t = at
    reg = jobs.Registry()
    reg.register(fred.JOB)
    return jobs.run_job("fred", settings=settings, registry=reg, clock=lambda: at.replace(tzinfo=UTC))


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _table(settings) -> list[tuple]:
    return _query(
        settings,
        "SELECT series_id, date, value, realtime_start, first_seen_at FROM fred_observation"
        " ORDER BY ALL",
    )


def _versions(settings, day=Q1) -> list[tuple]:
    return _query(
        settings,
        "SELECT value, realtime_start, first_seen_at FROM fred_observation"
        " WHERE series_id = 'GDPC1' AND date = ? ORDER BY first_seen_at",
        [day],
    )


def _obs_requests(api) -> list:
    return [r.url.params for r in api.requests if r.url.path.endswith("/observations")]


# --- first load ---------------------------------------------------------------------------


def test_first_run_stores_every_vintage_first_seen_at_realtime_start_midnight_new_york(
    settings, api, clock
):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    q1 = _versions(settings)
    # Q1 2020: advance estimate 2020-04-29, then its revisions
    assert q1[:3] == [
        (Decimal("18987.877"), date(2020, 4, 29), _naive(2020, 4, 29, 4, 0)),  # EDT
        (Decimal("18974.702"), date(2020, 5, 28), _naive(2020, 5, 28, 4, 0)),
        (Decimal("18977.363"), date(2020, 6, 25), _naive(2020, 6, 25, 4, 0)),
    ]
    vintages_of_q1 = [r for r in api.rows if r["date"] == "2020-01-01"]
    assert len(q1) == len(vintages_of_q1)
    assert len(_table(settings)) == len(api.rows) == 33
    assert _query(settings, "SELECT key, value FROM watermark WHERE job = 'fred'") == [
        ("GDPC1", "2020-07-01")
    ]


def test_fred_as_of_gives_the_value_known_that_day(settings, api, clock):
    _run(settings, clock)
    as_of = "SELECT value FROM fred_as_of(CAST(? AS DATE)) WHERE series_id = 'GDPC1' AND date = ?"
    assert _query(settings, as_of, ["2020-05-01", Q1]) == [(Decimal("18987.877"),)]
    assert _query(settings, as_of, ["2020-06-01", Q1]) == [(Decimal("18974.702"),)]
    assert _query(settings, as_of, ["2020-04-28", Q1]) == []  # not published yet


def test_catalog_entry_is_stored(settings, api, clock):
    _run(settings, clock)
    assert _query(
        settings, "SELECT series_id, title, units, frequency, seasonal_adjustment FROM fred_series"
    ) == [
        (
            "GDPC1",
            "Real Gross Domestic Product",
            "Billions of Chained 2017 Dollars",
            "Quarterly",
            "Seasonally Adjusted Annual Rate",
        )
    ]


def test_api_key_is_sent_but_never_archived(settings, api, clock):
    _run(settings, clock)
    assert all(r.url.params["api_key"] == KEY for r in api.requests)
    urls = [u for (u,) in _query(settings, "SELECT url FROM raw_response")]
    assert urls and not any(KEY in u for u in urls)
    assert all("api_key=REDACTED" in u for u in urls)


# --- windows ------------------------------------------------------------------------------


def test_windows_hold_at_most_max_vintages_and_cover_everything(monkeypatch):
    monkeypatch.setattr(fred, "MAX_VINTAGES", 2)
    days = ["2020-01-30", "2020-02-27", "2020-03-26", "2020-04-29", "2020-05-28"]
    assert fred.windows(days) == [
        ("1776-07-04", "2020-03-25"),
        ("2020-03-26", "2020-05-27"),
        ("2020-05-28", "9999-12-31"),
    ]
    assert fred.windows([]) == [("1776-07-04", "9999-12-31")]


def test_max_vintages_keeps_two_under_freds_limit():
    # FRED counts one more vintage date than it lists in a window
    assert fred.MAX_VINTAGES + 1 < 2000 + 1


def test_a_chunked_first_load_stores_the_same_rows_as_one_window(settings, api, clock, monkeypatch):
    monkeypatch.setattr(fred, "MAX_VINTAGES", 3)
    api.limit_vintages = 4  # refuses a window listing 4: one more than MAX_VINTAGES is fine
    assert _run(settings, clock).status == "ok"
    sent = _obs_requests(api)
    assert len(sent) == -(-len(api.vintages()) // 3)
    assert sent[0]["realtime_start"] == "1776-07-04"
    assert sent[-1]["realtime_end"] == "9999-12-31"
    expected = [
        ("GDPC1", date.fromisoformat(r["date"]), Decimal(r["value"]),
         date.fromisoformat(r["realtime_start"]), fred.alfred.first_seen(
             date.fromisoformat(r["realtime_start"])))
        for r in api.rows
    ]  # fmt: skip
    assert _table(settings) == sorted(expected)


def test_a_window_over_freds_limit_fails_the_key(settings, api, clock):
    api.limit_vintages = 5  # every vintage date of the fixture in one window is too many
    result = _run(settings, clock)
    assert result.status == "failed"
    assert "FRED answered 400" in result.key_errors["GDPC1"]


def test_pages_are_followed_by_offset(settings, api, clock, monkeypatch):
    monkeypatch.setattr(fred, "PAGE", 10)
    assert _run(settings, clock).status == "ok"
    assert [p["offset"] for p in _obs_requests(api)] == ["0", "10", "20", "30"]
    assert len(_table(settings)) == 33


# --- later runs ---------------------------------------------------------------------------


def test_a_later_run_asks_from_the_newest_vintage_within_lookback(settings, api, clock):
    _run(settings, clock)
    api.requests.clear()
    result = _run(settings, clock, T2)
    assert result.status == "ok"
    assert not [r for r in api.requests if r.url.path.endswith("/vintagedates")]
    (p,) = _obs_requests(api)
    newest = max(r["realtime_start"] for r in api.rows)
    assert (p["realtime_start"], p["realtime_end"]) == (newest, "9999-12-31")
    assert p["observation_start"] == "2021-10-15"  # 2026-10-14 - 1825 days


def test_clipped_unchanged_rows_are_not_new_versions(settings, api, clock):
    _run(settings, clock)
    before = _table(settings)
    _run(settings, clock, T2)  # every row comes back clipped to the newest vintage date
    assert _table(settings) == before


def test_a_revision_is_a_new_version_and_the_old_one_stays(settings, api, clock):
    (settings.config_dir / "fred.yaml").write_text(
        "lookback_days: 10000\nseries:\n  - id: GDPC1\n", "utf-8"
    )  # the fixture's quarters are older than the default 5 years
    _run(settings, clock)
    old = _versions(settings)
    current = next(
        r for r in api.rows if r["date"] == "2020-01-01" and r["realtime_end"] == "9999-12-31"
    )
    current["realtime_end"] = "2026-10-13"
    api.rows.append({**current, "realtime_start": "2026-10-14", "realtime_end": "9999-12-31",
                     "value": "19000.001"})  # fmt: skip
    _run(settings, clock, T2)
    assert _versions(settings) == [
        *old,
        (Decimal("19000.001"), date(2026, 10, 14), _naive(2026, 10, 14, 4, 0)),
    ]


def test_a_first_load_that_failed_half_way_is_fetched_whole_again(
    settings, api, clock, monkeypatch
):
    monkeypatch.setattr(fred, "MAX_VINTAGES", 3)
    api.fail_obs = {2}  # the second window of the first load
    assert _run(settings, clock).status == "failed"
    # every window is fetched before any is parsed: nothing stored, no watermark
    assert _table(settings) == []
    assert _query(settings, "SELECT count(*) FROM watermark WHERE job = 'fred'") == [(0,)]
    assert _run(settings, clock, T2).status == "ok"
    assert len(_table(settings)) == 33


def test_rows_without_a_watermark_mean_the_series_is_loaded_whole(settings, api, clock):
    # e.g. a parse that failed after earlier windows committed: the table alone says "loaded"
    _run(settings, clock)
    before = _table(settings)
    _query(settings, "DELETE FROM watermark WHERE job = 'fred'")
    api.requests.clear()
    assert _run(settings, clock, T2).status == "ok"
    assert [r for r in api.requests if r.url.path.endswith("/vintagedates")]
    assert _obs_requests(api)[0]["realtime_start"] == "1776-07-04"
    assert _table(settings) == before


# --- idempotence, reparse, no key ---------------------------------------------------------


def test_reparse_rebuilds_the_same_rows(settings, api, clock):
    _run(settings, clock)
    _run(settings, clock, T2)
    before = _table(settings)
    with store.session(settings.db_path, wait=0) as con:
        result = jobs.reparse(con, fred.JOB, settings=settings)
    assert result.deleted == len(before)
    assert _table(settings) == before


def test_without_a_key_the_run_is_skipped_and_nothing_is_requested(settings, api, clock):
    result = _run(replace(settings, fred_api_key=None), clock)
    assert result.status == "skipped"
    assert "FRED_API_KEY" in result.error
    assert api.requests == []


# --- config and alerts --------------------------------------------------------------------


def test_repo_config_loads(tmp_path):
    config = fred.load_config(REPO_CONFIG)
    assert config.lookback_days == 1825
    assert {"DGS10", "GDPC1", "SOFR"} <= set(config.series)


@pytest.mark.parametrize(
    "text, message",
    [
        ("series:\n  - id: DGS10\n  - id: dgs10\n", "listed twice"),
        ("series:\n  - jump_pct: 5\n", "needs an id"),
        ("series:\n  - id: GDP\n    jump_pct: lots\n", "jump_pct must be a number"),
        ("lookback_days: 0\nseries: []\n", "at least 1"),
    ],
)
def test_bad_config_is_refused(tmp_path, text, message):
    (tmp_path / "fred.yaml").write_text(text, "utf-8")
    with pytest.raises(ConfigError, match=message):
        fred.load_config(tmp_path)


def test_only_series_with_jump_pct_are_checked_for_value_jump(settings):
    (settings.config_dir / "fred.yaml").write_text(
        "series:\n  - id: GDP\n    jump_pct: 10\n  - id: T10Y2Y\n", "utf-8"
    )
    observed = fred.JOB.observed_map(settings)
    assert (observed["GDP"].values, observed["GDP"].jump_pct) == (("value",), 10.0)
    assert observed["T10Y2Y"].values == ()


def test_the_job_is_scheduled(settings):
    entry = scheduler.load_schedule(REPO_CONFIG)["fred"]
    assert entry.cron == "30 18 * * *"


# --- web ----------------------------------------------------------------------------------


@pytest.fixture
def client(settings, api, clock):
    _run(settings, clock)
    s = scheduler.Scheduler(settings, jobs.Registry(), clock=lambda: T1.replace(tzinfo=UTC))
    yield TestClient(web.create_app(s, manage=False, ui_wait=0.3))
    s.shutdown()


def test_series_index_lists_fred_series(client):
    page = client.get("/series?format=csv").text
    assert "fred,GDPC1,Real Gross Domestic Product,4,2019-10-01,2020-07-01" in page


def test_fred_page_shows_latest_and_as_of(client):
    latest = client.get("/series/fred/GDPC1")
    assert latest.status_code == 200
    assert "Billions of Chained 2017 Dollars" in latest.text and "<svg" in latest.text
    as_of = client.get("/series/fred/GDPC1?as_of=2020-05-01&format=csv").text.splitlines()
    assert as_of[0] == "date,value,realtime_start,first_seen_at,versions"
    assert as_of[1:] == [
        "2019-10-01,19221.97,2020-03-26,2020-03-26 04:00:00Z,3",
        "2020-01-01,18987.877,2020-04-29,2020-04-29 04:00:00Z,1",
    ]


def test_fred_page_errors(client):
    assert client.get("/series/fred/NOPE").status_code == 404
    assert client.get("/series/fred/GDPC1?as_of=May").status_code == 400
