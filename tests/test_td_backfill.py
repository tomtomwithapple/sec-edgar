"""`usfin td backfill`: the auction history through monthly camelCase `search` windows.

The fake API answers `search` like the real one (probed 2026-10-08): records whose auction date
falls in `startDate..endDate`, both ends inclusive, `[]` for an empty window. Its records are the
recorded `search_2010` and `search_current` fixtures.
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

from usfin import http, jobs, ratelimit, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources import treasurydirect as td

FIXTURES = Path(__file__).parent / "fixtures" / "treasurydirect"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
HOST = "www.treasurydirect.gov"
NOW = datetime(2026, 10, 8, 18, 0, tzinfo=UTC)  # 14:00 ET, today 2026-10-08
LATER = datetime(2026, 11, 2, 18, 0, tzinfo=UTC)


def _fixture(name: str) -> list[dict]:
    return json.loads((FIXTURES / f"{name}.json").read_text("utf-8"))


class FakeSearch:
    """`search` over the recorded records, filtered by the window; records every request."""

    def __init__(self):
        self.records = _fixture("search_2010") + _fixture("search_current")
        self.urls: list[httpx.URL] = []
        self.fail_from: date | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.urls.append(request.url)
        assert request.url.path == "/TA_WS/securities/search"
        q = request.url.params
        start, end = date.fromisoformat(q["startDate"]), date.fromisoformat(q["endDate"])
        if self.fail_from is not None and start >= self.fail_from:
            return httpx.Response(404, text="<html>not found</html>")
        hits = [
            r for r in self.records if start <= date.fromisoformat(r["auctionDate"][:10]) <= end
        ]
        return httpx.Response(200, json=hits)

    def windows(self) -> list[tuple[str, str]]:
        return [(u.params["startDate"], u.params["endDate"]) for u in self.urls]


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
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
    )


@pytest.fixture
def api(settings):
    fake = FakeSearch()
    with respx.mock(assert_all_called=False) as router:
        router.route(host=HOST).mock(side_effect=fake)
        yield fake


def _backfill(settings, start, now=NOW):
    with store.session(settings.db_path, wait=0) as con:
        return td.backfill(con, settings=settings, start=start, now=now)


def _table(settings) -> list[tuple]:
    with store.session(settings.db_path, wait=0) as con:
        return sorted(con.execute("SELECT * FROM tsy_security").fetchall(), key=repr)


def _query(settings, sql):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql).fetchall()


# --- windows -----------------------------------------------------------------------------------


def test_month_windows_run_from_the_start_date_to_today():
    assert td.month_windows(date(2010, 2, 15), date(2010, 4, 3)) == [
        (date(2010, 2, 15), date(2010, 2, 28)),
        (date(2010, 3, 1), date(2010, 3, 31)),
        (date(2010, 4, 1), date(2010, 4, 3)),
    ]


def test_month_windows_handle_year_ends_leap_days_and_a_single_day():
    assert td.month_windows(date(1979, 12, 31), date(1980, 3, 1)) == [
        (date(1979, 12, 31), date(1979, 12, 31)),
        (date(1980, 1, 1), date(1980, 1, 31)),
        (date(1980, 2, 1), date(1980, 2, 29)),
        (date(1980, 3, 1), date(1980, 3, 1)),
    ]
    assert td.month_windows(date(2026, 10, 8), date(2026, 10, 8)) == [
        (date(2026, 10, 8), date(2026, 10, 8))
    ]
    assert td.month_windows(date(2026, 10, 9), date(2026, 10, 8)) == []


def test_the_default_start_is_the_probed_earliest_auction():
    assert td.EARLIEST_AUCTION == date(1979, 10, 31)


# --- backfill ----------------------------------------------------------------------------------


def test_each_window_is_one_camelcase_search_request_archived_as_td_search(settings, api):
    result = _backfill(settings, date(2026, 8, 20))
    assert api.windows() == [
        ("2026-08-20", "2026-08-31"),
        ("2026-09-01", "2026-09-30"),
        ("2026-10-01", "2026-10-08"),
    ]
    for url in api.urls:
        assert url.params["dateFieldName"] == "competitiveAuctionCloseDate"
        assert url.params["format"] == "json"
        assert "startdate" not in str(url) and "enddate" not in str(url)
    assert result.windows == 3 and result.fetched == 3 and result.skipped == 0
    datasets = _query(
        settings, "SELECT dataset, http_status, count(*) FROM raw_response GROUP BY ALL"
    )
    assert datasets == [("td_search", 200, 3)]
    current = {(r["cusip"], r["auctionDate"][:10]) for r in _fixture("search_current")}
    stored = {
        (c, d.isoformat())
        for c, d in _query(settings, "SELECT cusip, auction_date FROM tsy_security")
    }
    assert stored == current
    assert result.rows == len(current)


def test_history_lands_typed_and_moves_the_results_watermark_forward_only(settings, api):
    _backfill(settings, date(2010, 3, 1), now=datetime(2010, 3, 20, 18, tzinfo=UTC))
    rows = _query(
        settings,
        "SELECT security_type, high_yield, status FROM tsy_security WHERE cusip = '912828MP2'",
    )
    assert rows == [("Note", pytest.approx(3.735), "auctioned")]
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, "treasurydirect") == {"results": "2010-03-10"}
        jobs.set_watermark(con, "treasurydirect", "results", "2026-10-07", today=NOW.date())
    _backfill(settings, date(2010, 3, 1))
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, "treasurydirect")["results"] == "2026-10-07"


def test_a_rerun_skips_completed_windows_and_refetches_only_the_open_month(settings, api):
    _backfill(settings, date(2026, 8, 20))
    built = _table(settings)
    api.urls.clear()
    result = _backfill(settings, date(2026, 8, 20))
    assert api.windows() == [("2026-10-01", "2026-10-08")]
    assert (result.windows, result.fetched, result.skipped, result.rows) == (3, 1, 2, 0)
    assert _table(settings) == built


def test_the_open_month_is_fetched_whole_once_it_has_ended(settings, api):
    _backfill(settings, date(2026, 9, 1))
    api.urls.clear()
    _backfill(settings, date(2026, 9, 1), now=LATER)
    assert api.windows() == [("2026-10-01", "2026-10-31"), ("2026-11-01", "2026-11-02")]


def test_an_interrupted_backfill_resumes_after_its_last_fetched_window(settings, api):
    api.fail_from = date(2026, 9, 1)
    with pytest.raises(td.TreasuryDirectError):
        _backfill(settings, date(2026, 7, 1))
    assert api.windows() == [
        ("2026-07-01", "2026-07-31"),
        ("2026-08-01", "2026-08-31"),
        ("2026-09-01", "2026-09-30"),
    ]
    api.fail_from = None
    api.urls.clear()
    result = _backfill(settings, date(2026, 7, 1))
    assert api.windows() == [("2026-09-01", "2026-09-30"), ("2026-10-01", "2026-10-08")]
    assert result.skipped == 2


def test_months_archived_under_the_old_date_field_name_count_as_fetched(settings, api):
    _backfill(settings, date(2026, 8, 20))
    with store.session(settings.db_path, wait=0) as con:  # as stored before 2026-10 (S44)
        con.execute(
            "UPDATE raw_response SET url = replace(url, 'competitiveAuctionCloseDate', 'auctionDate')"
        )
    api.urls.clear()
    result = _backfill(settings, date(2026, 8, 20))
    assert api.windows() == [("2026-10-01", "2026-10-08")]
    assert result.skipped == 2


class FailingTransport(httpx.BaseTransport):
    def handle_request(self, request):
        raise AssertionError(f"reparse sent a request: {request.url}")


def test_reparse_treasurydirect_rebuilds_the_backfilled_table(settings, api):
    _backfill(settings, date(2010, 3, 1))
    built = _table(settings)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM tsy_security")
        result = jobs.reparse(con, td.JOB, settings=settings, transport=FailingTransport())
    assert len(result.raw_ids) == len(td.month_windows(date(2010, 3, 1), NOW.date()))
    assert _table(settings) == built


def test_cli_backfill_prints_windows_requests_and_rows(settings, api, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setattr(td, "_now", lambda: NOW)
    out = CliRunner().invoke(app, ["td", "backfill", "--from", "2026-09-15"])
    assert out.exit_code == 0, out.output
    assert "windows 2 (fetched 2, skipped 0)" in out.output
    assert "rows " in out.output
    bad = CliRunner().invoke(app, ["td", "backfill", "--from", "15/09/2026"])
    assert bad.exit_code != 0
