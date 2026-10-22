"""EDGAR link (A8, How > EDGAR link, S12).

The EDGAR company page (`/edgar/{cik}`) shows, for each **active** bars-universe symbol whose
`ticker_norm` is one of the company's **active** EDGAR tickers, the latest close and a 1-year
adjusted-close chart of the security that symbol names today (`alpaca_bar_adjusted_lookup`,
S10), with the rows as `?format=csv&table=prices`. Delisted securities are not joined:
`edgar_company` keeps only active pairs, and only actively listed Alpaca symbols take part, the
same rule as `/prices/universe?matched=`.

The store is `test_alpaca_security`'s (whole live responses: FB, IBTA, META, METV, TWTR bars
from 2016, their actions, the S2 asset list) plus `edgar_company` rows written here. No request
leaves the process.
"""

import csv
import io
import re
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import test_alpaca_security as security_fx
from fastapi.testclient import TestClient

from usfin import jobs, scheduler, store
from usfin.settings import load_settings
from usfin.web import app as web
from usfin.web.edgar import num

NOW = datetime(2026, 10, 19, 21, 0, tzinfo=UTC)
META_CIK, TWTR_CIK, NOBARS_CIK, LOWER_CIK = 1326801, 1418091, 9999901, 9999902
LAST = date(2026, 10, 16)


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> Path:
    tmp = tmp_path_factory.mktemp("edgar_prices")
    db_path = security_fx.build(tmp)
    with store.session(db_path, wait=0) as con:
        con.execute(
            "INSERT INTO edgar_company VALUES"
            f" ({META_CIK}, 'META', 'Meta Platforms, Inc.', 'META', true, '2026-10-19'),"
            f" ({META_CIK}, 'FB', 'Meta Platforms, Inc.', 'FB', false, '2026-10-19'),"
            f" ({TWTR_CIK}, 'TWTR', 'Twitter, Inc.', 'TWTR', false, '2026-10-19'),"
            f" ({NOBARS_CIK}, 'ZZZZ', 'No Bars Corp', 'ZZZZ', true, '2026-10-19'),"
            # an EDGAR ticker in another notation than Alpaca's still joins through ticker_norm
            f" ({LOWER_CIK}, 'ibta', 'Ibotta, Inc.', 'IBTA', true, '2026-10-19')"
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


def test_active_symbol_joins_its_cik_with_latest_close_and_a_one_year_chart(client):
    page = client.get(f"/edgar/{META_CIK}")
    assert page.status_code == 200
    text = page.text
    close = _closes("META")[LAST]
    assert "Latest close" in text and num(close) in text and "2026-10-16" in text
    assert 'href="/prices?symbol=META"' in text
    svgs = re.findall(r"<svg\b.*?</svg>", text, flags=re.DOTALL)
    assert len(svgs) == 1 and "META adjusted close, 1 year" in svgs[0]  # no facts, no facts chart
    assert 'href="/edgar/1326801?format=csv&amp;table=prices"' in text


def test_prices_csv_is_the_last_year_of_the_joined_security(client):
    rows = _csv(
        client.get(f"/edgar/{META_CIK}?format=csv&table=prices"), f"edgar_{META_CIK}_prices"
    )
    meta = _closes("META")
    year = sorted(d for d in meta if LAST.replace(year=LAST.year - 1) < d <= LAST)
    assert [r["date"] for r in rows] == [d.isoformat() for d in year]
    assert {r["symbol"] for r in rows} == {"META"}
    assert {r["security_key"] for r in rows} == {"cusip:30303M102"}
    assert float(rows[-1]["close"]) == meta[LAST]
    for r in rows:
        assert float(r["adj_close"]) == pytest.approx(float(r["close"]) * float(r["tr_factor"]))


def test_a_ticker_in_another_notation_joins_through_ticker_norm(client):
    rows = _csv(
        client.get(f"/edgar/{LOWER_CIK}?format=csv&table=prices"), f"edgar_{LOWER_CIK}_prices"
    )
    assert rows and {r["symbol"] for r in rows} == {"IBTA"}
    assert 'href="/prices?symbol=IBTA"' in client.get(f"/edgar/{LOWER_CIK}").text


def test_a_delisted_security_is_not_joined(client):
    page = client.get(f"/edgar/{TWTR_CIK}")
    assert page.status_code == 200
    assert "Latest close" not in page.text and "/prices?symbol=TWTR" not in page.text
    assert "<svg" not in page.text
    rows = _csv(
        client.get(f"/edgar/{TWTR_CIK}?format=csv&table=prices"), f"edgar_{TWTR_CIK}_prices"
    )
    assert rows == []


def test_an_active_ticker_alpaca_does_not_list_says_so(client):
    text = client.get(f"/edgar/{NOBARS_CIK}").text
    assert "Latest close" not in text and "No actively listed Alpaca symbol" in text


def test_universe_matched_and_the_company_pages_agree(client):
    universe = _csv(client.get("/prices/universe?matched=1&format=csv"), "prices_universe_matched")
    assert {(r["symbol"], r["cik"]) for r in universe} == {
        ("META", str(META_CIK)),
        ("IBTA", str(LOWER_CIK)),
    }
    for r in universe:
        rows = _csv(
            client.get(f"/edgar/{r['cik']}?format=csv&table=prices"), f"edgar_{r['cik']}_prices"
        )
        assert {p["symbol"] for p in rows} == {r["symbol"]}
    unmatched = _csv(
        client.get("/prices/universe?matched=0&format=csv"), "prices_universe_unmatched"
    )
    assert {"TWTR", "FB", "METV"} <= {r["symbol"] for r in unmatched}
