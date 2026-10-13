"""Corporate-action backfill from 2016 (`usfin alpaca backfill corp-actions`; plan
alpaca-market-data S4: A5 from 2016, A10 resume).

The backfill walks calendar-month windows on `process_date` from `--from` to today (New York),
one DB session per window (open, archive and store, close), and records each finished month
under its own job name `alpaca_corp_actions_backfill`, which has no `schedule.yaml` entry, so its
progress raises no alert. Pages are archived as the job's dataset (`alpaca_corp_actions`), so
`usfin reparse alpaca_corp_actions` rebuilds them too. Fixtures are S3's live responses; fake keys
only; no request leaves the process.
"""

import json
import shutil
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import alerts, http, jobs, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources.alpaca import backfill, corp_actions

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca" / "corp_actions"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
TODAY = date(2020, 10, 12)
NOW = datetime(2020, 10, 12, 15, 0, tzinfo=UTC)


def body(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(raw, "_utcnow", lambda: NOW.replace(tzinfo=None))
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
        apca_api_key_id=KEY_ID,
        apca_api_secret_key=SECRET,
    )


class Api:
    """Answers a month window by its `start`: `by_start` maps a start date to its pages (a list
    followed by `next_page_token`); any other window is empty. `fail_from` answers 500 for every
    window starting on or after it."""

    def __init__(self):
        self.by_start: dict[str, list[bytes]] = {
            "2020-08-01": [body("aapl_2020-08.json")],
            "2020-09-01": [body("ctnt_2026-09.json")],
        }
        self.fail_from: str | None = None
        self.stall = 0
        """Requests still to fail with a read timeout (a socket that stopped answering)."""
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.stall:
            self.stall -= 1
            raise httpx.ReadTimeout("stalled", request=request)
        start = request.url.params["start"]
        if self.fail_from is not None and start >= self.fail_from:
            return httpx.Response(500, content=b'{"message":"internal"}')
        pages = self.by_start.get(start, [body("empty.json")])
        token = request.url.params.get("page_token")
        i = 0
        if token is not None:
            while json.loads(pages[i])["next_page_token"] != token:
                i += 1
            i += 1
        return httpx.Response(
            200, content=pages[i], headers={"content-type": "application/json; charset=UTF-8"}
        )

    def windows(self) -> list[tuple[str, str]]:
        seen = [
            (r.url.params["start"], r.url.params["end"])
            for r in self.requests
            if "page_token" not in r.url.params
        ]
        return seen


@pytest.fixture
def api(settings):
    a = Api()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__startswith=corp_actions.URL).mock(side_effect=a)
        yield a


def _backfill(settings, start=date(2020, 7, 1), today=TODAY, **kw):
    return backfill.corp_actions(settings, start=start, today=today, **kw)


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _table(settings):
    return _query(
        settings,
        "SELECT id, type, payload::VARCHAR, first_seen_at, raw_id FROM alpaca_corp_action"
        " ORDER BY id, first_seen_at",
    )


# --- windows ---------------------------------------------------------------------------------


def test_the_default_start_is_2016_01_01():
    assert backfill.CORP_ACTIONS_START == date(2016, 1, 1)


def test_each_month_is_one_window_on_process_date_archived_as_the_jobs_dataset(settings, api):
    result = _backfill(settings)
    assert api.windows() == [
        ("2020-07-01", "2020-07-31"),
        ("2020-08-01", "2020-08-31"),
        ("2020-09-01", "2020-09-30"),
        ("2020-10-01", "2020-10-12"),
    ]
    assert all(r.url.params["limit"] == "1000" for r in api.requests)
    assert all(r.headers["APCA-API-KEY-ID"] == KEY_ID for r in api.requests)
    assert _query(
        settings, "SELECT source, dataset, http_status, count(*) FROM raw_response GROUP BY ALL"
    ) == [("alpaca", "alpaca_corp_actions", 200, 4)]
    assert (result.windows, result.fetched, result.skipped, result.pages) == (4, 4, 0, 4)
    aapl = len(json.loads(body("aapl_2020-08.json"))["corporate_actions"]["cash_dividends"]) + 1
    ctnt = sum(len(v) for v in json.loads(body("ctnt_2026-09.json"))["corporate_actions"].values())
    assert result.actions == result.inserted == aapl + ctnt
    assert _query(settings, "SELECT count(*) FROM alpaca_corp_action") == [(aapl + ctnt,)]


def test_a_window_follows_next_page_token(settings, api):
    api.by_start["2020-08-01"] = [body("aapl_2016-2026_p1.json"), body("aapl_2016-2026_p2.json")]
    result = _backfill(settings, start=date(2020, 8, 1), today=date(2020, 8, 31))
    assert len(api.requests) == 2
    assert (
        api.requests[1].url.params["page_token"]
        == json.loads(api.by_start["2020-08-01"][0])["next_page_token"]
    )
    assert (result.pages, result.inserted) == (2, 44)


def test_the_per_window_callback_reports_pages_actions_and_types(settings, api):
    seen = []
    _backfill(settings, progress=seen.append)
    assert [(w.lo, w.fetched, w.pages) for w in seen] == [
        (date(2020, 7, 1), True, 1),
        (date(2020, 8, 1), True, 1),
        (date(2020, 9, 1), True, 1),
        (date(2020, 10, 1), True, 1),
    ]
    assert seen[1].types == {"cash_dividend": 1, "forward_split": 1}
    assert seen[1].inserted == 2


# --- resume ----------------------------------------------------------------------------------


def test_a_rerun_skips_finished_months_and_refetches_only_the_open_one(settings, api):
    _backfill(settings)
    built = _table(settings)
    api.requests.clear()
    result = _backfill(settings)
    assert api.windows() == [("2020-10-01", "2020-10-12")]
    assert (result.windows, result.fetched, result.skipped, result.inserted) == (4, 1, 3, 0)
    assert _table(settings) == built


def test_the_open_month_is_fetched_whole_once_it_has_ended(settings, api):
    _backfill(settings)
    api.requests.clear()
    _backfill(settings, today=date(2020, 11, 2))
    assert api.windows() == [("2020-10-01", "2020-10-31"), ("2020-11-01", "2020-11-02")]


def test_an_interrupted_backfill_resumes_after_its_last_finished_month(settings, api):
    api.fail_from = "2020-09-01"
    with pytest.raises(http.HttpError):  # a 500 is retried, then raised
        _backfill(settings)
    assert api.windows()[-1] == ("2020-09-01", "2020-09-30")
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, backfill.CORP_JOB)[backfill.CORP_KEY] == "2020-08-31"
    # the finished months' actions stay stored
    assert _query(settings, "SELECT count(*) FROM alpaca_corp_action") == [(2,)]
    api.fail_from = None
    api.requests.clear()
    result = _backfill(settings)
    assert api.windows() == [("2020-09-01", "2020-09-30"), ("2020-10-01", "2020-10-12")]
    assert result.skipped == 2


def test_progress_is_not_reused_for_an_earlier_start(settings, api):
    _backfill(settings, start=date(2020, 9, 1))
    api.requests.clear()
    _backfill(settings, start=date(2020, 7, 1))
    assert api.windows() == [
        ("2020-07-01", "2020-07-31"),
        ("2020-08-01", "2020-08-31"),
        ("2020-10-01", "2020-10-12"),
    ]


# --- sessions and alerts ---------------------------------------------------------------------


def test_each_window_opens_and_closes_its_own_db_session(settings, api, monkeypatch):
    opened, closed = [], []
    real = store.session

    @contextmanager
    def counting(*args, **kwargs):
        opened.append(1)
        with real(*args, **kwargs) as con:
            yield con
        closed.append(1)

    monkeypatch.setattr(backfill.store, "session", counting)
    between = []

    def progress(window):
        between.append((len(opened), len(closed)))
        # nothing holds the DB between windows: another writer gets in
        with real(settings.db_path, wait=0) as con:
            con.execute("CREATE TABLE IF NOT EXISTS other_writer (n INT)")
            con.execute("INSERT INTO other_writer VALUES (?)", [len(between)])

    _backfill(settings, progress=progress)
    # one session reads the progress, then one per window; each closed before the next window
    assert between == [(n + 1, n + 1) for n in range(1, 5)]
    assert _query(settings, "SELECT count(*) FROM other_writer") == [(4,)]


def test_progress_lives_under_its_own_job_name_with_no_alert_rules(settings, api):
    _backfill(settings)
    assert backfill.CORP_JOB == "alpaca_corp_actions_backfill"
    assert backfill.CORP_JOB not in alerts.load_rules(settings.config_dir)
    assert backfill.CORP_JOB not in alerts.load_rules(REPO_CONFIG)
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, backfill.CORP_JOB) == {
            backfill.CORP_KEY: "2020-09-30",
            "from": "2020-07-01",
        }
        # the daily job's watermark and alerts are untouched
        assert jobs.get_watermarks(con, corp_actions.JOB_NAME) == {}
        assert alerts.list_alerts(con) == []
        assert con.execute("SELECT count(*) FROM job_run").fetchone() == (0,)


class FailingTransport(httpx.BaseTransport):
    def handle_request(self, request):
        raise AssertionError(f"reparse sent a request: {request.url}")


def test_reparse_of_the_job_rebuilds_the_backfilled_actions(settings, api):
    _backfill(settings)
    built = _table(settings)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM alpaca_corp_action")
        result = jobs.reparse(
            con, corp_actions.JOB, settings=settings, transport=FailingTransport()
        )
    assert len(result.raw_ids) == 4
    assert _table(settings) == built


def test_no_key_in_the_raw_store_or_table(settings, api):
    _backfill(settings)
    for blob in Path(settings.raw_dir).rglob("*"):
        if blob.is_file():
            data = blob.read_bytes()
            assert KEY_ID.encode() not in data and SECRET.encode() not in data
    dump = repr(_query(settings, "SELECT * FROM raw_response")) + repr(_table(settings))
    assert KEY_ID not in dump and SECRET not in dump


# --- CLI -------------------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    monkeypatch.setenv("APCA_API_KEY_ID", KEY_ID)
    monkeypatch.setenv("APCA_API_SECRET_KEY", SECRET)
    monkeypatch.setattr(backfill, "_now", lambda: NOW)


def test_cli_prints_years_and_totals(settings, api, cli_env):
    out = CliRunner().invoke(app, ["alpaca", "backfill", "corp-actions", "--from", "2020-07-01"])
    assert out.exit_code == 0, out.output
    assert "2020: windows 4 (fetched 4, skipped 0), pages 4" in out.output
    assert "cash_dividend 1" in out.output
    assert "windows 4 (fetched 4, skipped 0), pages 4, actions" in out.output
    assert KEY_ID not in out.output and SECRET not in out.output


def test_cli_rejects_a_bad_date_and_reports_a_failure_with_resume_hint(settings, api, cli_env):
    bad = CliRunner().invoke(app, ["alpaca", "backfill", "corp-actions", "--from", "07/2020"])
    assert bad.exit_code != 0
    api.fail_from = "2020-08-01"
    out = CliRunner().invoke(app, ["alpaca", "backfill", "corp-actions", "--from", "2020-07-01"])
    assert out.exit_code == 1
    assert "rerun to resume" in out.output


def test_cli_without_keys_sends_nothing(settings, api, cli_env, monkeypatch):
    monkeypatch.setenv("APCA_API_KEY_ID", "")  # empty is unset, and wins over ./.env
    out = CliRunner().invoke(app, ["alpaca", "backfill", "corp-actions", "--from", "2020-07-01"])
    assert out.exit_code == 1
    assert "error:" in out.output
    assert api.requests == []


def test_a_stalled_page_times_out_and_is_retried(settings, api, monkeypatch):
    # the Alpaca path uses the shared client's timeout (no override): a read stalled past it
    # raises ReadTimeout, which the client retries like any transport error
    assert http.TIMEOUT.read == 60.0 and http.TIMEOUT.connect == 15.0
    seen = []
    real = http.Client.__init__

    def spy(self, *args, **kwargs):
        real(self, *args, **kwargs)
        seen.append(self._http.timeout)

    monkeypatch.setattr(http.Client, "__init__", spy)
    api.stall = 1
    result = _backfill(settings, start=date(2020, 8, 1), today=date(2020, 8, 31))
    assert seen and all(t == http.TIMEOUT for t in seen)
    assert api.stall == 0 and len(api.requests) == 2  # the stalled try, then its retry
    assert (result.fetched, result.inserted) == (1, 2)
