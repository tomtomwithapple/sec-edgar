"""Prices pages (`/prices`, `/prices/universe`; A11, How > UI, S11).

`/prices?symbol=` reads `alpaca_bar_adjusted_lookup` (S10) and `alpaca_security_lookup` (S9): the
series of the security the symbol names today, chained across renames, as an adjusted or
unadjusted chart, a table and a CSV per table (`?format=csv&table=bars|segments`).
`/prices/universe` lists the bars universe (`alpaca_universe`, S5) one row per symbol, filtered by
asset status, exchange, delisted and matched (an active symbol sharing its `ticker_norm` with an
active EDGAR company).

The store is `test_alpaca_security`'s: the real assets and corporate-actions jobs and the bars
job's fetch and store over whole live responses (FB, IBTA, META, METV, TWTR from 2016, their
actions, the S2 asset list), plus two `edgar_company` rows written here. No request leaves the
process.
"""

import csv
import io
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import test_alpaca_security as security_fx
from fastapi.testclient import TestClient

from usfin import jobs, scheduler, store
from usfin.settings import load_settings
from usfin.web import app as web

NOW = datetime(2026, 10, 19, 21, 0, tzinfo=UTC)
META_CIK, TWTR_CIK = 1326801, 1418091


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> Path:
    tmp = tmp_path_factory.mktemp("prices")
    db_path = security_fx.build(tmp)
    with store.session(db_path, wait=0) as con:
        con.execute(
            "INSERT INTO edgar_company VALUES"
            f" ({META_CIK}, 'META', 'Meta Platforms, Inc.', 'META', true, '2026-10-19'),"
            f" ({TWTR_CIK}, 'TWTR', 'Twitter, Inc.', 'TWTR', false, '2026-10-19')"
        )
    return tmp


@pytest.fixture(scope="module")
def client(built):
    with pytest.MonkeyPatch.context() as mp:
        for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
            mp.delenv(key, raising=False)
        settings = replace(
            load_settings(env_file=built / "no.env"),
            db_path=built / "usfin.duckdb",
            raw_dir=built / "raw",
            config_dir=built / "config",
            lock_wait=0,
        )
    (built / "config" / "schedule.yaml").write_text("jobs: {}\n", "utf-8")
    s = scheduler.Scheduler(settings, jobs.Registry(), clock=lambda: NOW)
    yield TestClient(web.create_app(s, manage=False, ui_wait=0.3))
    s.shutdown()


def _csv(response, name: str) -> list[dict[str, str]]:
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/csv")
    assert response.headers["content-disposition"] == f'attachment; filename="{name}.csv"'
    return list(csv.DictReader(io.StringIO(response.text)))


def _closes(symbol: str) -> dict[date, float]:
    from usfin.sources.alpaca import bars

    return {bars.bar_date(b["t"]): b["c"] for b in security_fx._fixture_bars()[symbol]}


# --- /prices --------------------------------------------------------------------------------


def test_meta_is_fb_bars_to_the_rename_then_meta_as_one_series(client):
    page = client.get("/prices?symbol=META")
    assert page.status_code == 200
    text = page.text
    assert 'href="/prices" aria-current="page">Prices' in text
    assert 'href="/prices/universe"' in text
    assert "<script" not in text
    assert "<svg" in text and "cusip:30303M102" in text
    rows = _csv(client.get("/prices?symbol=META&format=csv"), "prices_META")
    assert len(rows) == 1620 + 1093
    assert (rows[0]["date"], rows[0]["symbol"]) == ("2016-01-04", "FB")
    assert (rows[1619]["date"], rows[1619]["symbol"]) == ("2022-06-08", "FB")
    assert (rows[1620]["date"], rows[1620]["symbol"]) == ("2022-06-09", "META")
    assert (rows[-1]["date"], rows[-1]["symbol"]) == ("2026-10-16", "META")
    fb, meta = _closes("FB"), _closes("META")
    assert float(rows[0]["close"]) == fb[date(2016, 1, 4)]
    assert float(rows[-1]["close"]) == meta[date(2026, 10, 16)]
    for r in (rows[0], rows[1620], rows[-1]):
        assert float(r["adj_close"]) == pytest.approx(float(r["close"]) * float(r["tr_factor"]))
        assert r["open"] and r["high"] and r["low"] and r["volume"]
    # FB-era bars carry Meta's dividends paid under META (S10)
    assert float(rows[0]["tr_factor"]) < 1 and float(rows[-1]["tr_factor"]) == 1


def test_segments_table_and_csv_show_the_chain(client):
    rows = _csv(
        client.get("/prices?symbol=META&format=csv&table=segments"), "prices_META_segments"
    )
    assert [(r["symbol"], r["valid_from"], r["valid_to"]) for r in rows] == [
        ("FB", "2016-01-04", "2022-06-08"),
        ("META", "2022-06-09", "2026-10-16"),
    ]
    assert {r["security_key"] for r in rows} == {"cusip:30303M102"}
    assert rows[0]["bars"] == "1620" and rows[1]["bars"] == "1093"


def test_adjusted_is_the_default_and_unadjusted_a_toggle(client):
    adjusted = client.get("/prices?symbol=META").text
    raw_page = client.get("/prices?symbol=META&adjusted=0").text
    assert "META adjusted close" in adjusted and "META close (unadjusted)" in raw_page
    assert 'href="/prices?symbol=META&amp;adjusted=0"' in adjusted
    assert 'href="/prices?symbol=META"' in raw_page
    # the unadjusted chart's high is the highest raw close; the adjusted one's is lower or equal
    rows = _csv(client.get("/prices?symbol=META&format=csv"), "prices_META")
    high_raw = max(float(r["close"]) for r in rows)
    assert f"high {high_raw:g}" in raw_page or f"high {high_raw}" in raw_page
    # the CSV is the same table whichever chart is shown
    assert client.get("/prices?symbol=META&adjusted=0&format=csv").text == client.get(
        "/prices?symbol=META&format=csv"
    ).text


def test_lower_case_symbol_and_twtr_to_its_merger(client):
    rows = _csv(client.get("/prices?symbol=twtr&format=csv"), "prices_TWTR")
    assert len(rows) == 1718
    assert {r["symbol"] for r in rows} == {"TWTR"}
    assert rows[-1]["date"] == "2022-10-27"
    page = client.get("/prices?symbol=twtr").text
    assert "cusip:90184L102" in page and "2022-10-27" in page


def test_fb_is_only_the_new_fb(client):
    rows = _csv(client.get("/prices?symbol=FB&format=csv"), "prices_FB")
    assert len(rows) == 330 and rows[0]["date"] == "2025-06-26"


def test_html_table_shows_the_newest_rows_and_says_the_csv_has_all(client):
    text = client.get("/prices?symbol=META").text
    assert "2026-10-16" in text
    assert "Newest 500 of 2,713" in text


def test_no_symbol_is_a_search_form_with_an_empty_csv(client):
    page = client.get("/prices")
    assert page.status_code == 200
    assert 'name="symbol"' in page.text and "<svg" not in page.text
    response = client.get("/prices?format=csv")
    assert response.status_code == 200
    assert response.text.splitlines()[0].startswith("date,symbol,")
    assert len(response.text.splitlines()) == 1


def test_an_unknown_symbol_is_404_with_the_form(client):
    page = client.get("/prices?symbol=NOPE")
    assert page.status_code == 404
    assert "no bars stored for NOPE" in page.text and 'name="symbol"' in page.text
    assert client.get("/prices?symbol=NOPE&format=csv").status_code == 404


@pytest.mark.parametrize(
    "query", ["symbol=META&adjusted=maybe", "symbol=META&format=csv&table=nope"]
)
def test_bad_price_parameters_are_400(client, query):
    assert client.get(f"/prices?{query}").status_code == 400


# --- /prices/universe -----------------------------------------------------------------------


def _universe(client, query: str = "", name: str = "prices_universe") -> dict[str, dict]:
    sep = "&" if query else ""
    rows = _csv(client.get(f"/prices/universe?{query}{sep}format=csv"), name)
    return {r["symbol"]: r for r in rows}


def test_universe_lists_every_symbol_once_with_status_and_bars(client):
    rows = _universe(client)
    assert "CYTOF" not in rows  # OTC
    assert {"META", "METV", "FB", "IBTA", "TWTR", "AAPL", "BT"} <= set(rows)
    meta = rows["META"]
    assert (meta["status"], meta["exchange"], meta["delisted"]) == ("active", "NASDAQ", "False")
    assert meta["sources"] == "asset_active renamed"
    assert (meta["first_bar"], meta["last_bar"], meta["bars"]) == ("2021-06-30", "2026-10-16", "1331")
    twtr = rows["TWTR"]
    assert (twtr["status"], twtr["exchange"], twtr["delisted"]) == ("none", "", "True")
    assert twtr["sources"] == "merger" and twtr["last_bar"] == "2022-10-27"
    assert rows["IBTA"]["status"] == "active"  # one active id and one inactive id
    assert rows["BT"]["status"] == "inactive" and rows["BT"]["delisted"] == "True"
    assert rows["AAPL"]["bars"] == "0" and rows["AAPL"]["first_bar"] == ""


def test_universe_page_links_symbols_to_prices_and_matched_to_edgar(client):
    page = client.get("/prices/universe")
    assert page.status_code == 200
    text = page.text
    assert 'href="/prices/universe" aria-current="page"' not in text  # nav marks Prices
    assert 'aria-current="page">Prices' in text
    assert 'href="/prices?symbol=TWTR"' in text
    assert f'href="/edgar/{META_CIK}"' in text
    assert "<script" not in text


def test_universe_filters_status(client):
    active = _universe(client, "status=active", "prices_universe_active")
    assert "META" in active and "TWTR" not in active and "BT" not in active
    inactive = _universe(client, "status=inactive", "prices_universe_inactive")
    assert "BT" in inactive and "META" not in inactive
    none = _universe(client, "status=none", "prices_universe_none")
    assert set(none) == {r for r, v in _universe(client).items() if v["status"] == "none"}
    assert "TWTR" in none


def test_universe_filters_exchange(client):
    arca = _universe(client, "exchange=arca", "prices_universe_ARCA")
    assert {"METV", "SPY"} <= set(arca) and "META" not in arca
    assert {r["exchange"] for r in arca.values()} == {"ARCA"}


def test_universe_filters_delisted(client):
    gone = _universe(client, "delisted=1", "prices_universe_delisted")
    assert {"TWTR", "BT"} <= set(gone) and "META" not in gone
    listed = _universe(client, "delisted=0", "prices_universe_listed")
    assert "META" in listed and "TWTR" not in listed
    assert len(gone) + len(listed) == len(_universe(client))


def test_universe_filters_matched_active_symbols_only(client):
    matched = _universe(client, "matched=1", "prices_universe_matched")
    assert set(matched) == {"META"}
    assert matched["META"]["cik"] == str(META_CIK)
    assert matched["META"]["edgar_title"] == "Meta Platforms, Inc."
    unmatched = _universe(client, "matched=0", "prices_universe_unmatched")
    assert "TWTR" in unmatched and unmatched["TWTR"]["cik"] == ""  # inactive pair, delisted
    assert "META" not in unmatched


def test_universe_filters_combine_in_page_and_csv(client):
    rows = _universe(
        client, "status=active&exchange=NASDAQ&matched=1", "prices_universe_active_NASDAQ_matched"
    )
    assert set(rows) == {"META"}
    page = client.get("/prices/universe?status=active&exchange=NASDAQ").text
    assert ">META<" in page and ">METV<" not in page
    assert 'href="/prices/universe?status=active&amp;exchange=NASDAQ&amp;format=csv' in page


@pytest.mark.parametrize(
    "query",
    ["status=gone", "exchange=OTC", "delisted=maybe", "matched=2", "format=csv&table=nope"],
)
def test_bad_universe_parameters_are_400(client, query):
    assert client.get(f"/prices/universe?{query}").status_code == 400


def test_empty_store_says_so(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    config = tmp_path / "config"
    shutil.copytree(security_fx.REPO_CONFIG, config)
    (config / "schedule.yaml").write_text("jobs: {}\n", "utf-8")
    settings = replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
    )
    s = scheduler.Scheduler(settings, jobs.Registry(), clock=lambda: NOW)
    try:
        c = TestClient(web.create_app(s, manage=False, ui_wait=0.3))
        page = c.get("/prices/universe")
        assert page.status_code == 200 and "usfin run alpaca_assets" in page.text
        assert c.get("/prices/universe?format=csv").text.startswith("symbol,")
    finally:
        s.shutdown()
