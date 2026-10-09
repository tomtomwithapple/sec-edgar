"""Unmatched symbols page (`/symbols/unmatched`, S34) and the app-wide CSV rule (C11).

The page reads view `symbol_edgar_match` (S31): the match-rate summary (non-ETF first: ETFs are
mostly not SEC operating companies, S31 measured 3.3 %), then the open symbols no active EDGAR
company shares a `ticker_norm` with, filtered by ETF flag and exchange; an unmatched symbol whose
root (the part before the first `.`) is an EDGAR company's links to it, and `?match=matched|all`
lists matched symbols linked to `/edgar/{cik}`. Two tables, so the CSV picks one with `?table=`.

The last two tests enumerate every GET page route the app has and fetch each one's CSV, so a
page added later without `?format=csv` (or without an entry here) fails. Rows are written by the
test; no request leaves the process.
"""

import csv
import io
import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
import test_web_edgar as edgar_fx
import test_web_series as series_fx
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from usfin import alerts, jobs, scheduler, store
from usfin.settings import load_settings
from usfin.web import app as web
from usfin.web import edgar as web_edgar

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
NOW = datetime(2026, 10, 8, 21, 0, tzinfo=UTC)
SEEN = datetime(2026, 10, 8, 7, 3, tzinfo=UTC).replace(tzinfo=None)  # naive UTC, as stored
APPLE, NOBLE, SPY = 320193, 1458891, 884394


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    (config / "schedule.yaml").write_text("jobs: {}\n", "utf-8")
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
    )


def _symbol(con, symbol, exchange, name, etf, *, closed=False):
    file = "nasdaqlisted" if exchange == "Q" else "otherlisted"
    con.execute(
        "INSERT INTO symbol_history VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
        [symbol, exchange, SEEN, SEEN if closed else None, file, name, etf],
    )


def _symbols(con):
    con.execute(
        "INSERT INTO edgar_company VALUES"
        " (320193, 'AAPL', 'Apple Inc.', 'AAPL', true, '2026-10-08'),"
        " (1067983, 'BRK-B', 'BERKSHIRE HATHAWAY INC', 'BRK.B', true, '2026-10-08'),"
        " (1458891, 'NE', 'Noble Corp plc', 'NE', true, '2026-10-08'),"
        " (1458891, 'NE-WTA', 'Noble Corp plc', 'NE.WTA', true, '2026-10-08'),"
        " (884394, 'SPY', 'SPDR S&P 500 ETF TRUST', 'SPY', true, '2026-10-08'),"
        " (25475, 'CRD-A', 'CRAWFORD & CO', 'CRD.A', true, '2026-10-08'),"
        " (999001, 'GONE', 'Inactive pair Corp', 'GONE', false, '2026-10-08')"
    )
    _symbol(con, "AAPL", "Q", "Apple Inc. - Common Stock", False)
    _symbol(con, "BRK.B", "N", "Berkshire Hathaway Inc. Class B", False)
    _symbol(con, "ASBH", "Q", "ASB Hawaii Common Stock", False)
    _symbol(con, "NE.A", "N", "Noble Corporation plc Tranche 2 Warrants", False)
    _symbol(con, "GONE", "N", "Inactive pair only", None)  # etf unknown counts as non-ETF
    _symbol(con, "SPY", "P", "SPDR S&P 500 ETF Trust", True)
    _symbol(con, "QQQM", "Q", "Invesco NASDAQ 100 ETF", True)
    _symbol(con, "XLKZ", "P", "Some Sector ETF", True)
    _symbol(con, "CRD", "Z", "Corgi CRDO 2x Daily ETF", True)  # plain: no root hint to CRD-A
    _symbol(con, "OLDX", "Q", "Delisted Corp", False, closed=True)  # closed rows never show


@pytest.fixture
def db(settings):
    with store.session(settings.db_path, wait=0) as con:
        _symbols(con)
    return settings


def _client(settings):
    s = scheduler.Scheduler(settings, jobs.Registry(), clock=lambda: NOW)
    return s, TestClient(web.create_app(s, manage=False, ui_wait=0.3))


@pytest.fixture
def client(db):
    s, c = _client(db)
    yield c
    s.shutdown()


def _csv(response):
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]
    return list(csv.DictReader(io.StringIO(response.text)))


# --- the page -------------------------------------------------------------------------------


def test_page_lists_unmatched_non_etfs_first_with_rates_and_nav(client):
    page = client.get("/symbols/unmatched")
    assert page.status_code == 200
    text = page.text
    assert 'href="/symbols/unmatched" aria-current="page">Symbols' in text
    assert "<script" not in text
    # every unmatched open symbol, non-ETFs before ETFs; matched and closed ones absent
    order = [text.index(s) for s in (">ASBH<", ">GONE<", ">NE.A<", ">CRD<", ">QQQM<", ">XLKZ<")]
    assert order == sorted(order)
    for absent in (">AAPL<", ">BRK.B<", ">SPY<", "OLDX"):
        assert absent not in text
    # summary: non-ETF 2 of 5 (40.0 %) before ETF 1 of 4 (25.0 %)
    assert text.index("40.0") < text.index("25.0")
    # NE.A has no EDGAR spelling but its root NE is Noble's: linked as a hint
    assert f'href="/edgar/{NOBLE}"' in text


def test_rates_csv_puts_non_etf_first_then_etf_then_all_then_per_exchange(client):
    rows = _csv(client.get("/symbols/unmatched?format=csv&table=rates"))
    head = [(r["kind"], r["exchange"], r["symbols"], r["matched"], r["unmatched"]) for r in rows]
    assert head[:3] == [
        ("non-ETF", "all", "5", "2", "3"),
        ("ETF", "all", "4", "1", "3"),
        ("all", "all", "9", "3", "6"),
    ]
    assert rows[0]["matched_pct"] == "40.0"
    per_exchange = {(r["kind"], r["exchange"]): r["matched"] for r in rows[3:]}
    assert per_exchange == {
        ("non-ETF", "N"): "1",
        ("non-ETF", "Q"): "1",
        ("ETF", "P"): "1",
        ("ETF", "Q"): "0",
        ("ETF", "Z"): "0",
    }
    assert list(per_exchange)[:2] == [("non-ETF", "N"), ("non-ETF", "Q")]  # non-ETF first


def test_unmatched_csv_is_the_default_table_with_every_row(client):
    response = client.get("/symbols/unmatched?format=csv")
    assert 'filename="symbols_unmatched.csv"' in response.headers["content-disposition"]
    rows = _csv(response)
    assert [r["symbol"] for r in rows] == ["ASBH", "GONE", "NE.A", "CRD", "QQQM", "XLKZ"]
    ne = rows[2]
    assert ne["ticker_norm"] == "NE.A" and ne["etf"] == "False" and ne["cik"] == ""
    assert ne["root_cik"] == str(NOBLE) and ne["root_ticker"] == "NE"
    assert rows[0]["root_cik"] == ""
    assert rows[3]["symbol"] == "CRD" and rows[3]["root_cik"] == ""  # a plain symbol: no hint


def test_filters_by_etf_flag_and_exchange_in_page_and_csv(client):
    non_etf = _csv(client.get("/symbols/unmatched?etf=0&format=csv"))
    assert [r["symbol"] for r in non_etf] == ["ASBH", "GONE", "NE.A"]
    etf = _csv(client.get("/symbols/unmatched?etf=1&format=csv"))
    assert [r["symbol"] for r in etf] == ["CRD", "QQQM", "XLKZ"]
    on_p = _csv(client.get("/symbols/unmatched?exchange=p&format=csv"))
    assert [r["symbol"] for r in on_p] == ["XLKZ"]
    assert (
        'filename="symbols_unmatched_etf_P.csv"'
        in client.get("/symbols/unmatched?etf=1&exchange=P&format=csv").headers[
            "content-disposition"
        ]
    )
    page = client.get("/symbols/unmatched?etf=0&exchange=N").text
    assert ">NE.A<" in page and ">GONE<" in page and ">ASBH<" not in page
    assert 'href="/symbols/unmatched?etf=0&amp;exchange=N&amp;format=csv"' in page


def test_matched_symbols_link_to_their_edgar_company(client):
    page = client.get("/symbols/unmatched?match=matched").text
    assert f'href="/edgar/{APPLE}"' in page and f'href="/edgar/{SPY}"' in page
    assert ">ASBH<" not in page
    rows = _csv(client.get("/symbols/unmatched?match=all&format=csv"))
    assert len(rows) == 9
    aapl = next(r for r in rows if r["symbol"] == "AAPL")
    assert aapl["cik"] == str(APPLE) and aapl["edgar_title"] == "Apple Inc."


@pytest.mark.parametrize(
    "query",
    ["etf=maybe", "exchange=ZZ", "match=some", "format=csv&table=nope", "exchange=Q%27"],
)
def test_bad_parameters_are_400(client, query):
    assert client.get(f"/symbols/unmatched?{query}").status_code == 400


def test_no_symbols_loaded_says_so(settings):
    s, c = _client(settings)
    try:
        page = c.get("/symbols/unmatched")
        assert page.status_code == 200
        assert "usfin run symbols" in page.text
        assert c.get("/symbols/unmatched?format=csv").text.startswith("symbol,exchange,")
    finally:
        s.shutdown()


# --- every table page answers ?format=csv ---------------------------------------------------

CPI = series_fx.CPI
PAGES = {
    "/": ["/"],
    "/alerts": ["/alerts", "/alerts?all=1"],
    "/series": ["/series"],
    "/series/bls/{series_id}": [f"/series/bls/{CPI}", f"/series/bls/{CPI}?as_of=2026-01-31"],
    "/series/fiscal/{name}": ["/series/fiscal/debt_to_penny"],
    "/series/auctions": [
        "/series/auctions",
        "/series/auctions?type=Note&term=10-Year&kind=nominal",
    ],
    "/series/auctions/xcheck": [
        "/series/auctions/xcheck",
        "/series/auctions/xcheck?table=summary",
        "/series/auctions/xcheck?result=all",
    ],
    "/series/releases": [
        "/series/releases",
        "/series/releases?table=summary",
        "/series/releases?when=all&release=Consumer+Price+Index",
    ],
    "/edgar/{key:path}": [f"/edgar/{APPLE}"]
    + [f"/edgar/{APPLE}?table={t}" for t in web_edgar.TABLES],
    "/screen": ["/screen?concept=revenue&period=CY2025"],
    "/symbols/unmatched": [
        "/symbols/unmatched",
        "/symbols/unmatched?table=rates",
        "/symbols/unmatched?table=unmatched&etf=1&exchange=P",
        "/symbols/unmatched?match=all",
    ],
    "/symbols/history": [
        "/symbols/history",
        "/symbols/history?table=snapshots",
        "/symbols/history?symbol=AAPL&table=versions",
        "/symbols/history?event=delisted",
    ],
}
"""Every GET page route → URLs whose CSV is fetched. A new page needs an entry (or a place in
FORMS), so a page without a CSV cannot slip in unnoticed."""
FORMS = {"/edgar"}
"""GET pages with no table: a lookup form only."""


@pytest.fixture(scope="module")
def full(tmp_path_factory):
    """One DB with a row for every page: a job run, an alert, BLS, Fiscal, auctions and their
    cross-check, BLS releases, EDGAR, symbols and their history."""
    tmp = tmp_path_factory.mktemp("full")
    config = tmp / "config"
    shutil.copytree(REPO_CONFIG, config)
    (config / "schedule.yaml").write_text("jobs: {}\n", "utf-8")
    settings = replace(
        load_settings(env_file=tmp / "no.env"),
        db_path=tmp / "usfin.duckdb",
        raw_dir=tmp / "raw",
        config_dir=config,
        lock_wait=0,
    )
    with store.session(settings.db_path, wait=0) as con:
        _symbols(con)
        alerts.raise_alert(con, job="fiscal", rule="zero_new_rows", subject="debt_to_penny",
                           detail={"n": 1}, now=NOW)  # fmt: skip
        con.execute(
            "INSERT INTO job_run (job, started_at, finished_at, status, rows_upserted, trigger)"
            " VALUES ('symbols', '2026-10-08 11:00', '2026-10-08 11:01', 'ok', 9, 'manual')"
        )
        con.execute(
            "INSERT INTO bls_series VALUES (?, 'CPI-U all items', 'S', 'Index 1982-1984=100', 3)",
            [CPI],
        )
        series_fx._obs(con, CPI, 2025, "M08", "323.364", SEEN)
        con.execute(series_fx.PENNY)
        con.execute("INSERT INTO fd_debt_to_penny VALUES ('2026-10-06', 30.1e12, 37.1e12, 1)")
        con.execute(
            "INSERT INTO tsy_security (cusip, auction_date, security_type, security_term,"
            " offering_amt, high_yield, status, payload) VALUES ('91282CAB9', '2026-08-12',"
            " 'Note', '10-Year', 42000000000, 4.125, 'auctioned', ?)",
            [json.dumps({"cusip": "91282CAB9", "tips": "No", "floatingRate": "No"})],
        )
        k25 = "0000320193-25-000079"
        edgar_fx._fact(con, APPLE, edgar_fx.RFCC, 416.161e9, "2024-09-29", "2025-09-27",
                       "CY2025", filed="2025-10-31", accn=k25)  # fmt: skip
        edgar_fx._filing(con, k25, APPLE, "10-K", "2025-10-31")
        con.execute(
            "INSERT INTO auction_xcheck VALUES ('91282CAB9', '2026-08-12', 'mismatch', 'Note',"
            " '{\"high_yield\": {\"fiscal\": \"4.125\", \"treasurydirect\": \"4.130\"}}')"
        )
        con.execute(
            "INSERT INTO bls_release VALUES ('2026-10-15', 'Consumer Price Index',"
            " '2026-10-15 12:30', 1)"
        )
        con.execute(
            "INSERT INTO symbol_diff VALUES ('nasdaqlisted', ?, 2, NULL, NULL, NULL, true, 2, 0)",
            [SEEN],
        )
    s = scheduler.Scheduler(settings, jobs.Registry(), clock=lambda: NOW)
    yield TestClient(web.create_app(s, manage=False, ui_wait=0.3))
    s.shutdown()


def test_every_page_route_is_covered_by_the_csv_test(full):
    routes = {
        r.path
        for r in full.app.routes
        if isinstance(r, APIRoute) and "GET" in r.methods and not r.path.startswith("/api/")
    }
    assert routes == set(PAGES) | FORMS


@pytest.mark.parametrize("url", [u for urls in PAGES.values() for u in urls])
def test_every_table_page_answers_format_csv_with_a_header_row(full, url):
    page = full.get(url)
    assert page.status_code == 200, page.text
    assert page.headers["content-type"].startswith("text/html")
    sep = "&" if "?" in url else "?"
    response = full.get(f"{url}{sep}format=csv")
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/csv")
    assert response.headers["content-disposition"].startswith("attachment; filename=")
    header = next(csv.reader(io.StringIO(response.text)))
    assert header and all(header), f"{url}: empty CSV header {header!r}"
