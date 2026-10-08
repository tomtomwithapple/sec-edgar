"""Nasdaq Trader symbols joined to EDGAR companies on `ticker_norm` (view `symbol_edgar_match`,
`usfin symbols unmatched`; C10).

`tests/fixtures/edgar/company_tickers_join.json` is https://www.sec.gov/files/company_tickers.json
as fetched 2026-10-08 (the body S19 archived), trimmed to the 13 entries, verbatim with their
original keys, that belong to the companies in the recorded Nasdaq Trader fixtures (see
test_nasdaq_snapshot): Agilent `A`, Ares Acquisition III `AAC` / `AAC-UN` / `AAC-WT`, AI
Infrastructure Acquisition `AIIA` / `AIIA-RI` / `AIIA-UN`, Arbor `ABR` / `ABR-PD`, `BRK-B`,
`AAPL`, `AACG` and `SPY`. Later files are that file with an entry dropped. No request leaves the
process.
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

from usfin import http, jobs, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources import nasdaqtrader
from usfin.sources.edgar import tickers
from usfin.ticker import ticker_norm

FIXTURES = Path(__file__).parent / "fixtures"
EDGAR_FIXTURE = FIXTURES / "edgar" / "company_tickers_join.json"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def naive(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(utc(2026, 10, 8, 11, 15))
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
    """Answers SEC's ticker map with `doc` and both SymDir files with the recorded fixtures."""

    def __init__(self):
        self.doc = json.loads(EDGAR_FIXTURE.read_text("utf-8"))
        self.symdir = {
            f: (FIXTURES / "nasdaqtrader" / f"{f}.txt").read_bytes() for f in nasdaqtrader.FILES
        }

    def edgar(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=self.doc)

    def nasdaq(self, request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1].removesuffix(".txt")
        return httpx.Response(
            200, content=self.symdir[name], headers={"content-type": "text/plain"}
        )

    def drop(self, ticker: str) -> None:
        self.doc = {k: v for k, v in self.doc.items() if v["ticker"] != ticker}


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(tickers.URL).mock(side_effect=f.edgar)
        for name in nasdaqtrader.FILES:
            router.get(nasdaqtrader.url(name)).mock(side_effect=f.nasdaq)
        yield f


def _run(settings, clock, job):
    reg = jobs.Registry()
    reg.register(tickers.JOB)
    reg.register(nasdaqtrader.JOB)
    result = jobs.run_job(job, settings=settings, registry=reg, clock=clock, chain=False)
    assert result.status == "ok", result.error
    return result


@pytest.fixture
def loaded(settings, clock, fake):
    _run(settings, clock, "edgar_tickers")
    _run(settings, clock, "symbols")
    return settings


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _matches(settings):
    """{symbol: edgar ticker or None} over the view."""
    return dict(_query(settings, "SELECT symbol, edgar_ticker FROM symbol_edgar_match"))


# --- ticker_norm: EDGAR's unit / warrant / right suffixes meet Nasdaq's ---------------------


@pytest.mark.parametrize(
    ("edgar", "nasdaq", "norm"),
    [
        ("AAC-UN", "AAC.U", "AAC.U"),
        ("AAC-WT", "AAC.W", "AAC.W"),
        ("AIIA-RI", "AIIA.R", "AIIA.R"),
        ("BRK-B", "BRK.B", "BRK.B"),
        ("ABR-PD", "ABR$D", "ABR.PD"),
    ],
)
def test_edgar_and_nasdaq_spellings_share_one_norm(edgar, nasdaq, norm):
    assert ticker_norm(edgar) == ticker_norm(nasdaq) == norm


@pytest.mark.parametrize("ticker", ["NE-WTA", "UWMC-RW", "NEE-PU", "ABR-PR", "SKYAW", "PAXGU"])
def test_other_suffixes_and_bare_letters_are_left_alone(ticker):
    # NE-WTA (Nasdaq NE.A), the -RW rights and preferred series U/R are not units, warrants or
    # rights in this sense; Nasdaq-listed symbols carry no separator at all.
    assert ticker_norm(ticker) == ticker.replace("-", ".")


SQL_CASES = [
    "BRK.B", "BRK-B", "BRK/B", "BRK B", " brk-b ", "ABR$D", "ABR-PD", "AAC.U", "AAC-U",
    "AAC-UN", "AAC-WT", "AIIA-RI", "NE-WTA", "UWMC-RW", "NEE-PU", "NONE.", "AAPL", "KCAC-UN",
]  # fmt: skip


def test_sql_macro_agrees_with_python(settings):
    with store.session(settings.db_path, wait=0, apply_migrations=True) as con:
        got = [con.execute("SELECT ticker_norm(?)", [s]).fetchone()[0] for s in SQL_CASES]
    assert got == [ticker_norm(s) for s in SQL_CASES]


# --- view symbol_edgar_match -------------------------------------------------------------------


def test_view_joins_class_preferred_unit_warrant_and_right_forms(loaded):
    m = _matches(loaded)
    assert m["BRK.B"] == "BRK-B"
    assert m["ABR$D"] == "ABR-PD"
    assert m["AAC.U"] == "AAC-UN"
    assert m["AAC.W"] == "AAC-WT"
    assert m["AIIA.R"] == "AIIA-RI"
    assert m["A"] == "A"
    assert m["AAPL"] == "AAPL"  # nasdaqlisted
    assert m["AACG"] == "AACG"
    assert m["SPY"] == "SPY"  # an ETF that files as a company
    row = _query(
        loaded,
        "SELECT exchange, file, etf, ticker_norm, cik, edgar_title FROM symbol_edgar_match"
        " WHERE symbol = 'AAC.U'",
    )
    assert row == [("N", "otherlisted", False, "AAC.U", 2128115, "Ares Acquisition Corp III")]


def test_unmatched_symbols_are_listed_not_dropped(loaded):
    open_rows = _query(loaded, "SELECT count(*) FROM symbol_history WHERE valid_to IS NULL")
    assert _query(loaded, "SELECT count(*) FROM symbol_edgar_match") == open_rows
    m = _matches(loaded)
    assert {s for s, t in m.items() if t is None} == {"AAA", "AAAP", "QQQ"}  # ETFs not in EDGAR


def test_inactive_edgar_pairs_are_ignored(loaded, clock, fake):
    fake.drop("AACG")
    fake.drop("AAC-WT")
    clock.t = utc(2026, 10, 9, 11, 15)
    _run(loaded, clock, "edgar_tickers")
    assert _query(
        loaded, "SELECT ticker FROM edgar_company WHERE NOT active ORDER BY 1"
    ) == [("AAC-WT",), ("AACG",)]
    m = _matches(loaded)
    assert m["AACG"] is None
    assert m["AAC.W"] is None
    assert m["AAC.U"] == "AAC-UN"


def test_closed_history_rows_are_not_joined(loaded):
    with store.session(loaded.db_path, wait=0) as con:
        con.execute(
            "UPDATE symbol_history SET valid_to = TIMESTAMP '2026-10-09 07:03' WHERE symbol = 'A'"
        )
    assert "A" not in _matches(loaded)


# --- migration recomputes edgar_company.ticker_norm -------------------------------------------


def test_migration_rewrites_stored_norms_and_keeps_updated_at(settings):
    with store.session(settings.db_path, wait=0, apply_migrations=False) as con:
        version = next(
            m.version for m in store.available_migrations() if m.name == "symbol_edgar_match"
        )
        store.schema_version(con)  # creates schema_version
        # bring the schema up to just before the migration, then store S19's spelling
        for mig in store.available_migrations():
            if mig.version >= version:
                break
            con.execute(mig.sql)
            con.execute(
                "INSERT INTO schema_version VALUES (?, ?, now()::TIMESTAMP)",
                [mig.version, mig.name],
            )
        con.execute(
            "INSERT INTO edgar_company VALUES"
            " (2128115, 'AAC-UN', 'Ares Acquisition Corp III', 'AAC.UN', true, '2026-10-08 02:27'),"
            " (1067983, 'BRK-B', 'BERKSHIRE HATHAWAY INC', 'BRK.B', true, '2026-10-08 02:27')"
        )
        store.migrate(con)
        rows = con.execute(
            "SELECT ticker, ticker_norm, updated_at FROM edgar_company ORDER BY 1"
        ).fetchall()
    assert rows == [
        ("AAC-UN", "AAC.U", naive(2026, 10, 8, 2, 27)),
        ("BRK-B", "BRK.B", naive(2026, 10, 8, 2, 27)),
    ]


# --- usfin symbols unmatched ---------------------------------------------------------------


@pytest.fixture
def cli_env(loaded, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(loaded.db_path))
    monkeypatch.setenv("USFIN_RAW", str(loaded.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(loaded.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    return loaded


def test_cli_lists_unmatched_with_match_rates(cli_env):
    out = CliRunner().invoke(app, ["symbols", "unmatched"])
    assert out.exit_code == 0, out.output
    listed = [line.split()[0] for line in out.output.splitlines() if line.startswith("  ")]
    assert listed == ["AAA", "AAAP", "QQQ"]
    assert "matched 9 of 12 (75.0%)" in out.output
    assert "non-ETF 8 of 8 (100.0%)" in out.output
    assert "ETF 1 of 4 (25.0%)" in out.output
    assert "N  non-ETF 6 of 6 (100.0%)  ETF 0 of 0 (0.0%)" in out.output
    assert "P  non-ETF 0 of 0 (0.0%)  ETF 1 of 2 (50.0%)" in out.output


def test_cli_filters_by_etf_and_exchange(cli_env):
    out = CliRunner().invoke(app, ["symbols", "unmatched", "--no-etf"])
    assert out.exit_code == 0, out.output
    assert [line for line in out.output.splitlines() if line.startswith("  ")] == []
    out = CliRunner().invoke(app, ["symbols", "unmatched", "--etf", "--exchange", "Q"])
    listed = [line.split()[0] for line in out.output.splitlines() if line.startswith("  ")]
    assert listed == ["AAAP", "QQQ"]
