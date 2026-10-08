"""EDGAR ticker map (job `edgar_tickers`, table `edgar_company`; C2, C5, C10).

`tests/fixtures/edgar/company_tickers.json` is https://www.sec.gov/files/company_tickers.json as
fetched 2026-10-08 (10 434 entries, compact JSON, keys "0".."10433" in SEC's own order), trimmed
to 14 entries kept verbatim with their original keys: two classes of Alphabet and Berkshire, Arbor
Realty's common and three preferreds, a unit (-UN), a warrant (-WT), a right (-RI) and the one
ticker with a dot (`NONE.`). Later files in these tests are that file edited. No request leaves
the process.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, jobs, ratelimit, raw, scheduler, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources.edgar import tickers

FIXTURE = Path(__file__).parent / "fixtures" / "edgar" / "company_tickers.json"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(utc(2026, 10, 8, 11, 15))  # 07:15 EDT
    monkeypatch.setattr(raw, "_utcnow", c.naive)
    return c


@pytest.fixture
def settings(tmp_path, monkeypatch, clock):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    for key in ("SEC_USER_AGENT", "ALERT_WEBHOOK_URL"):
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
    monkeypatch.setattr(http, "SEC_BLOCK", http.SecBlock())
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        sec_user_agent=UA,
    )


class Fake:
    """Answers www.sec.gov/files/company_tickers.json with `doc` (or `status`)."""

    def __init__(self):
        self.doc = json.loads(FIXTURE.read_text("utf-8"))
        self.status = 200
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.status != 200:
            return httpx.Response(self.status, text="<html>nope</html>")
        return httpx.Response(200, json=self.doc)

    def drop(self, ticker: str) -> None:
        self.doc = {k: v for k, v in self.doc.items() if v["ticker"] != ticker}

    def retitle(self, ticker: str, title: str) -> None:
        for v in self.doc.values():
            if v["ticker"] == ticker:
                v["title"] = title


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(tickers.URL).mock(side_effect=f)
        yield f


def _registry():
    reg = jobs.Registry()
    reg.register(tickers.JOB)
    return reg


def _run(settings, clock, when=None):
    if when is not None:
        clock.t = when
    return jobs.run_job(
        "edgar_tickers", settings=settings, registry=_registry(), clock=clock, chain=False
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _company(settings):
    rows = _query(
        settings,
        "SELECT cik, ticker, title, ticker_norm, active, updated_at FROM edgar_company",
    )
    return {(r[0], r[1]): r[2:] for r in rows}


# --- job edgar_tickers --------------------------------------------------------------------------


def test_the_file_becomes_one_row_per_cik_and_ticker(settings, fake, clock):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert result.rows_upserted == 14
    rows = _company(settings)
    assert len(rows) == 14
    first_seen = clock.naive()
    assert rows[(320193, "AAPL")] == ("Apple Inc.", "AAPL", True, first_seen)
    assert rows[(1067983, "BRK-B")] == ("BERKSHIRE HATHAWAY INC", "BRK.B", True, first_seen)
    assert rows[(1253986, "ABR-PD")][1] == "ABR.PD"
    assert rows[(2102713, "KCAC-UN")][1] == "KCAC.UN"
    assert {t for c, t in rows if c == 1652044} == {"GOOGL", "GOOG"}
    [request] = fake.requests
    assert request.headers["User-Agent"] == UA
    assert _query(settings, "SELECT source, dataset, http_status FROM raw_response") == [
        ("edgar", "edgar_tickers", 200)
    ]


def test_a_second_run_of_the_same_file_changes_nothing(settings, fake, clock):
    _run(settings, clock)
    before = _company(settings)
    again = _run(settings, clock, when=utc(2026, 10, 9, 11, 15))
    assert again.status == "ok" and again.rows_upserted == 0
    assert _company(settings) == before


def test_updated_at_moves_only_for_the_row_whose_value_changed(settings, fake, clock):
    _run(settings, clock)
    before = _company(settings)
    fake.retitle("NVDA", "NVIDIA Corp")
    later = utc(2026, 10, 9, 11, 15)
    result = _run(settings, clock, when=later)
    assert result.rows_upserted == 1
    after = _company(settings)
    assert after[(1045810, "NVDA")] == ("NVIDIA Corp", "NVDA", True, later.replace(tzinfo=None))
    assert {k: v for k, v in after.items() if k[1] != "NVDA"} == {
        k: v for k, v in before.items() if k[1] != "NVDA"
    }


def test_a_pair_missing_from_a_later_file_is_kept_inactive_and_comes_back(settings, fake, clock):
    _run(settings, clock)
    fake.drop("KCAC-UN")
    day2 = utc(2026, 10, 9, 11, 15)
    assert _run(settings, clock, when=day2).rows_upserted == 1
    rows = _company(settings)
    assert len(rows) == 14  # never dropped
    assert rows[(2102713, "KCAC-UN")][2:] == (False, day2.replace(tzinfo=None))
    assert sum(r[2] for r in rows.values()) == 13
    assert _run(settings, clock, when=utc(2026, 10, 10, 11, 15)).rows_upserted == 0
    fake.doc = json.loads(FIXTURE.read_text("utf-8"))
    day4 = utc(2026, 10, 11, 11, 15)
    assert _run(settings, clock, when=day4).rows_upserted == 1
    assert _company(settings)[(2102713, "KCAC-UN")][2:] == (True, day4.replace(tzinfo=None))


def test_without_sec_user_agent_the_job_fails_and_sends_nothing(settings, fake, clock):
    result = jobs.run_job(
        "edgar_tickers",
        settings=replace(settings, sec_user_agent=None),
        registry=_registry(),
        clock=clock,
        chain=False,
    )
    assert result.status == "failed"
    assert "SEC_USER_AGENT" in result.error
    assert fake.requests == []
    assert _query(settings, "SELECT count(*) FROM raw_response") == [(0,)]


def test_a_sec_403_fails_the_run_stores_nothing_and_blocks_sec(settings, fake, clock):
    fake.status = 403
    result = _run(settings, clock)
    assert result.status == "failed" and "403" in result.error
    assert http.SEC_BLOCK.blocked()
    assert _query(settings, "SELECT count(*) FROM edgar_company") == [(0,)]
    again = _run(settings, clock)  # inside the block window: refused before sending
    assert again.status == "failed" and len(fake.requests) == 1


def test_reparse_rebuilds_the_same_table_without_a_request(settings, fake, clock):
    _run(settings, clock)
    fake.drop("KCAC-UN")
    _run(settings, clock, when=utc(2026, 10, 9, 11, 15))
    table = _company(settings)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM edgar_company")
        result = jobs.reparse(con, tickers.JOB, settings=settings)
    assert len(result.raw_ids) == 2
    assert _company(settings) == table
    assert len(fake.requests) == 2


def test_the_job_is_scheduled_daily_at_0715_on_any_day(settings):
    entry = scheduler.load_schedule(settings.config_dir)["edgar_tickers"]
    assert (entry.cron, entry.calendar) == ("15 7 * * *", "any")
    assert "usfin.sources.edgar.tickers" in jobs.SOURCE_MODULES


# --- usfin edgar company ------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")


def test_cli_edgar_company_finds_a_company_by_any_ticker_spelling_or_cik(
    settings, fake, clock, cli_env
):
    _run(settings, clock)
    out = CliRunner().invoke(app, ["edgar", "company", "AAPL"])
    assert out.exit_code == 0, out.output
    assert "0000320193" in out.output and "Apple Inc." in out.output
    for spelling in ("BRK.B", "brk/b", "BRK-B"):
        out = CliRunner().invoke(app, ["edgar", "company", spelling])
        assert out.exit_code == 0, out.output
        assert "0001067983" in out.output and "BRK-B" in out.output
        assert "BRK-A" in out.output  # every ticker of the company
    by_cik = CliRunner().invoke(app, ["edgar", "company", "1652044"])
    assert by_cik.exit_code == 0 and "GOOGL" in by_cik.output and "GOOG " in by_cik.output


def test_cli_edgar_company_exits_1_for_an_unknown_ticker(settings, fake, clock, cli_env):
    _run(settings, clock)
    out = CliRunner().invoke(app, ["edgar", "company", "ZZZZ"])
    assert out.exit_code == 1
    assert "ZZZZ" in out.output
