"""Series browser (`/series`): BLS series with `?as_of=`, Fiscal Data datasets picked by their
series columns, Treasury auctions by security term; table + inline SVG line chart; CSV export.
"""

import csv
import io
import json
import re
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from usfin import jobs, scheduler, store
from usfin.settings import load_settings
from usfin.web import app as web
from usfin.web import charts

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
NOW = datetime(2026, 10, 8, 21, 0, tzinfo=UTC)
CPI = "CUSR0000SA0"
UNRATE = "LNS14000000"

DTS = """
CREATE TABLE fd_dts_operating_cash (
    record_date DATE, account_type VARCHAR, close_today_bal DECIMAL(24,4),
    open_today_bal DECIMAL(24,4), open_month_bal DECIMAL(24,4), table_nbr VARCHAR,
    src_line_nbr BIGINT, record_fiscal_year BIGINT,
    PRIMARY KEY (record_date, account_type))
"""
FX = """
CREATE TABLE fd_rates_of_exchange (
    record_date DATE, country VARCHAR, currency VARCHAR, country_currency_desc VARCHAR,
    exchange_rate DOUBLE, effective_date DATE, src_line_nbr BIGINT,
    PRIMARY KEY (record_date, src_line_nbr))
"""
PENNY = """
CREATE TABLE fd_debt_to_penny (
    record_date DATE PRIMARY KEY, debt_held_public_amt DECIMAL(24,4),
    tot_pub_debt_out_amt DECIMAL(24,4), src_line_nbr BIGINT)
"""
TGA = "Treasury General Account (TGA) Closing Balance"


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


def _obs(con, sid, year, period, value, seen, footnotes=()):
    con.execute(
        "INSERT INTO bls_observation VALUES (?, ?, ?, ?, ?, ?, 1, 'bls')",
        [sid, year, period, value, json.dumps(list(footnotes)), seen],
    )


@pytest.fixture
def db(settings):
    with store.session(settings.db_path, wait=0) as con:
        con.execute(
            "INSERT INTO bls_series VALUES (?, 'CPI-U all items', 'S', 'Index 1982-1984=100', 3),"
            " (?, 'Unemployment rate', 'S', 'Percent', 1)",
            [CPI, UNRATE],
        )
        jan = datetime(2026, 1, 10, 14, tzinfo=UTC).replace(tzinfo=None)  # naive UTC, as stored
        oct_ = datetime(2026, 10, 8, 1, 42, tzinfo=UTC).replace(tzinfo=None)
        # CPI: 2025 M08 first published 323.364, revised to 323.976 in October
        _obs(con, CPI, 2025, "M08", "323.364", jan)
        _obs(con, CPI, 2025, "M08", "323.976", oct_)
        _obs(con, CPI, 2025, "M09", "324.245", jan)
        gap = [{"code": "X", "text": "Data unavailable due to the 2025 lapse in appropriations"}]
        _obs(con, CPI, 2025, "M10", None, oct_, gap)
        _obs(con, CPI, 2025, "M11", "325.031", oct_)
        _obs(con, CPI, 2025, "M12", "325.722", oct_)
        _obs(con, UNRATE, 2025, "M08", "4.3", oct_)
        con.execute(DTS)
        con.execute(
            "INSERT INTO fd_dts_operating_cash VALUES"
            f" ('2026-09-01', '{TGA}', NULL, 800000, 790000, 'I', 1, 2026),"
            f" ('2026-09-02', '{TGA}', NULL, 812345, 790000, 'I', 1, 2026),"
            " ('2026-09-01', 'Total TGA Deposits (Table II)', NULL, 99999, NULL, 'I', 2, 2026),"
            " ('2026-09-02', 'Total TGA Deposits (Table II)', NULL, 77777, NULL, 'I', 2, 2026)"
        )
        con.execute(FX)
        con.execute(
            "INSERT INTO fd_rates_of_exchange VALUES"
            " ('2026-03-31', 'Euro Zone', 'Euro', 'Euro Zone-Euro', 0.921, '2026-03-31', 1),"
            " ('2026-03-31', 'Japan', 'Yen', 'Japan-Yen', 149.5, '2026-03-31', 2),"
            " ('2026-06-30', 'Japan', 'Yen', 'Japan-Yen', 144.25, '2026-06-30', 3),"
            " ('2026-06-30', 'Euro Zone', 'Euro', 'Euro Zone-Euro', 0.874, '2026-06-30', 4)"
        )
        con.execute(PENNY)
        con.execute(
            "INSERT INTO fd_debt_to_penny VALUES ('2026-10-05', 30e12, 37e12, 1),"
            " ('2026-10-06', 30.1e12, 37.1e12, 1)"
        )
        for cusip, day, typ, term, flags, hy, hir in [
            ("91282CAA1", "2026-05-12", "Note", "10-Year", {}, 4.312, None),
            ("91282CAB9", "2026-08-12", "Note", "10-Year", {}, 4.125, None),
            ("91282CAC7", "2026-07-23", "Note", "10-Year", {"tips": "Yes"}, 1.875, None),
            ("912797AA1", "2026-10-05", "Bill", "13-Week", {}, None, 3.987),
        ]:
            payload = {"cusip": cusip, "tips": "No", "floatingRate": "No"} | flags
            con.execute(
                "INSERT INTO tsy_security (cusip, auction_date, security_type, security_term,"
                " offering_amt, high_yield, high_investment_rate, status, payload)"
                " VALUES (?, ?, ?, ?, 42000000000, ?, ?, 'auctioned', ?)",
                [cusip, day, typ, term, hy, hir, json.dumps(payload)],
            )
    return settings


@pytest.fixture
def client(db):
    s = scheduler.Scheduler(db, jobs.Registry(), clock=lambda: NOW)
    yield TestClient(web.create_app(s, manage=False, ui_wait=0.3))
    s.shutdown()


def _csv(response):
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]
    return list(csv.DictReader(io.StringIO(response.text)))


def _svg(page):
    found = re.findall(r"<svg\b.*?</svg>", page, flags=re.DOTALL)
    assert len(found) == 1, "expected exactly one chart"
    return found[0]


# --- index ----------------------------------------------------------------------------------


def test_series_index_lists_bls_series_fiscal_datasets_and_auctions(client):
    page = client.get("/series")
    assert page.status_code == 200
    text = page.text
    assert 'href="/series"' in text and 'aria-current="page"' in text  # nav entry
    assert f'href="/series/bls/{CPI}"' in text and "CPI-U all items" in text
    for name in ("dts_operating_cash", "rates_of_exchange", "debt_to_penny", "mts_table_1"):
        assert f'href="/series/fiscal/{name}"' in text
    assert 'href="/series/auctions"' in text
    assert "<script" not in text


def test_series_index_csv_has_one_row_per_series_with_counts(client):
    rows = _csv(client.get("/series?format=csv"))
    by_id = {(r["source"], r["id"]): r for r in rows}
    assert by_id[("bls", CPI)]["rows"] == "5"  # periods, not versions
    assert by_id[("bls", CPI)]["first"] == "2025-08-01"
    assert by_id[("fiscal", "dts_operating_cash")]["rows"] == "4"
    assert by_id[("fiscal", "mts_table_1")]["rows"] == "0"  # table not created yet
    assert by_id[("auctions", "tsy_security")]["rows"] == "4"
    assert by_id[("auctions", "tsy_security")]["last"] == "2026-10-05"


# --- BLS ------------------------------------------------------------------------------------


def test_bls_page_renders_table_and_an_accessible_svg_line_with_a_gap_for_null(client):
    page = client.get(f"/series/bls/{CPI}")
    assert page.status_code == 200
    text = page.text
    assert "323.976" in text and "323.364" not in text  # latest version by default
    svg = _svg(text)
    assert 'role="img"' in svg and "<title" in svg and "<desc" in svg
    assert 'viewBox="' in svg
    paths = re.findall(r'<path class="line" d="([^"]+)"', svg)
    assert len(paths) == 1
    # M08, M09 | gap at M10 | M11, M12: two sub-paths, the NULL is not drawn through
    assert paths[0].count("M") == 2
    assert "1 missing" in svg  # the desc says the gap is there
    assert "Data unavailable due to the 2025 lapse" in text  # footnote shown


def test_bls_as_of_changes_values(client):
    page = client.get(f"/series/bls/{CPI}?as_of=2026-01-31").text
    assert "323.364" in page and "323.976" not in page
    assert "325.031" not in page  # not yet known on 2026-01-31
    rows = _csv(client.get(f"/series/bls/{CPI}?as_of=2026-01-31&format=csv"))
    assert [(r["period"], r["value"]) for r in rows] == [("M08", "323.364"), ("M09", "324.245")]
    latest = _csv(client.get(f"/series/bls/{CPI}?format=csv"))
    assert [r["value"] for r in latest] == ["323.976", "324.245", "", "325.031", "325.722"]
    assert latest[0]["versions"] == "2"
    assert latest[2]["footnotes"].startswith("[{")


def test_bls_as_of_before_any_value_says_so_and_bad_input_is_400(client):
    page = client.get(f"/series/bls/{CPI}?as_of=2025-01-01")
    assert page.status_code == 200
    assert "no values known as of 2025-01-01" in page.text
    assert "oldest stored version was first seen 2026-01-10 14:00:00Z" in page.text
    assert "<svg" not in page.text
    assert client.get(f"/series/bls/{CPI}?as_of=yesterday").status_code == 400
    assert client.get("/series/bls/NOPE").status_code == 404


def test_bls_values_are_shown_at_the_series_decimals(client):
    rows = _csv(client.get(f"/series/bls/{UNRATE}?format=csv"))
    assert rows[0]["value"] == "4.3"


# --- Fiscal Data ----------------------------------------------------------------------------


def test_fiscal_page_without_a_pick_lists_the_series_instead_of_plotting_everything(client):
    page = client.get("/series/fiscal/rates_of_exchange").text
    assert "<svg" not in page
    assert "Japan-Yen" in page and "Euro Zone-Euro" in page
    assert "/series/fiscal/rates_of_exchange?country_currency_desc=Japan-Yen" in page
    rows = _csv(client.get("/series/fiscal/rates_of_exchange?format=csv"))
    assert [(r["country_currency_desc"], r["rows"]) for r in rows] == [
        ("Euro Zone-Euro", "2"),
        ("Japan-Yen", "2"),
    ]


def test_fiscal_series_page_plots_one_series_and_field(client):
    url = "/series/fiscal/rates_of_exchange?country_currency_desc=Japan-Yen"
    page = client.get(url).text
    svg = _svg(page)
    assert "exchange_rate" in svg
    assert "144.25" in page and "0.921" not in page
    rows = _csv(client.get(url + "&format=csv"))
    assert [(r["record_date"], r["exchange_rate"]) for r in rows] == [
        ("2026-06-30", "144.25"),
        ("2026-03-31", "149.5"),
    ]


def test_fiscal_default_pick_comes_from_jump_match_and_field_can_change(client):
    page = client.get("/series/fiscal/dts_operating_cash").text
    assert TGA in page and "<svg" in page
    assert "99999" not in page  # deposits are another series
    other = client.get(
        "/series/fiscal/dts_operating_cash",
        params={"account_type": TGA, "field": "open_month_bal"},
    )
    assert "open_month_bal" in _svg(other.text)
    assert client.get("/series/fiscal/dts_operating_cash?field=nope").status_code == 400
    assert client.get("/series/fiscal/nope").status_code == 404


def test_fiscal_dataset_without_series_columns_is_one_series(client):
    page = client.get("/series/fiscal/debt_to_penny").text
    assert "tot_pub_debt_out_amt" in _svg(page)


def test_fiscal_dataset_not_loaded_yet_says_so(client):
    page = client.get("/series/fiscal/mts_table_1")
    assert page.status_code == 200 and "not loaded yet" in page.text


# --- auctions -------------------------------------------------------------------------------


def test_auctions_page_lists_terms_and_plots_high_yield_by_term(client):
    page = client.get("/series/auctions").text
    assert "<svg" not in page
    assert "10-Year" in page and "13-Week" in page
    assert "/series/auctions?type=Note&amp;term=10-Year&amp;kind=nominal" in page
    url = "/series/auctions?type=Note&term=10-Year&kind=nominal"
    svg = _svg(client.get(url).text)
    assert "high_yield" in svg
    rows = _csv(client.get(url + "&format=csv"))
    assert [(r["cusip"], r["high_yield"]) for r in rows] == [
        ("91282CAB9", "4.125"),
        ("91282CAA1", "4.312"),
    ]  # TIPS kept apart


def test_auction_bills_default_to_the_investment_rate(client):
    svg = _svg(client.get("/series/auctions?type=Bill&term=13-Week&kind=nominal").text)
    assert "high_investment_rate" in svg


# --- chart helper ---------------------------------------------------------------------------


def test_line_chart_breaks_at_none_and_marks_lone_points():
    pts = [
        (date(2026, 1, 1), 1.0),
        (date(2026, 2, 1), None),
        (date(2026, 3, 1), 3.0),
        (date(2026, 4, 1), 4.0),
    ]
    svg = charts.line_chart(pts, title="t <x>", y_label="y")
    assert "t &lt;x&gt;" in svg
    d = re.search(r'<path class="line" d="([^"]+)"', svg).group(1)
    assert d.count("M") == 1  # only Mar-Apr is a segment
    assert svg.count('class="dot"') == 1  # Jan stands alone between the start and the gap
    assert charts.line_chart([(date(2026, 1, 1), None)], title="t", y_label="y") == ""
