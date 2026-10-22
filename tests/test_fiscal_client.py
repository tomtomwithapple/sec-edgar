"""Treasury Fiscal Data client: URL grammar, page loop, dataTypes casting.

Fixtures in tests/fixtures/fiscal/ are real responses recorded 2026-10-08 with the exact URLs the
client builds (small `page[size]`); `auctions.json` has its unused `meta.labels` and
`meta.dataFormats` removed to keep it small.
"""

import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import ConfigError, load_settings
from usfin.sources import fiscal

FIXTURES = Path(__file__).parent / "fixtures" / "fiscal"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
PENNY = fiscal.Dataset(name="debt_to_penny", endpoint="v2/accounting/od/debt_to_penny")
AUCTIONS = fiscal.Dataset(
    name="auctions", endpoint="v1/accounting/od/auctions_query", date_field="auction_date"
)
TYPE_FIXTURES = (
    "debt_to_penny_p1.json",
    "dts_operating_cash.json",
    "auctions.json",
    "avg_interest_rates.json",
    "mts_table_1.json",
    "rates_of_exchange.json",
)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text("utf-8"))


def _url(endpoint: str, field: str, since: str, page: int, size: int) -> str:
    return (
        f"{BASE}/{endpoint}?filter={field}:gte:{since}&sort={field}"
        f"&page%5Bnumber%5D={page}&page%5Bsize%5D={size}"
    )


@pytest.fixture
def con(tmp_path):
    with store.session(tmp_path / "usfin.duckdb", wait=0) as c:
        yield c


class FakeClock:
    """Monotonic clock that only moves when the code under test sleeps."""

    def __init__(self):
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def client(con, tmp_path, monkeypatch):
    for key in ("SEC_USER_AGENT", "USFIN_CONFIG", "USFIN_RAW"):
        monkeypatch.delenv(key, raising=False)
    settings = replace(
        load_settings(env_file=tmp_path / "no.env"),
        raw_dir=tmp_path / "raw",
        config_dir=REPO_CONFIG,
    )
    clock = FakeClock()
    with http.Client(
        con,
        settings=settings,
        limiters=ratelimit.Limiters(clock=clock, sleep=clock.sleep),
        clock=clock,
        sleep=clock.sleep,
    ) as c:
        yield c


# --- URL grammar -------------------------------------------------------------------------------


def test_page_url_encodes_brackets_filters_and_sorts_on_the_date_field():
    url = fiscal.page_url(PENNY, date(2026, 10, 2), page=1, page_size=10000)
    assert url == _url("v2/accounting/od/debt_to_penny", "record_date", "2026-10-02", 1, 10000)
    assert "[" not in url and "]" not in url
    assert "fields=" not in url


def test_page_url_uses_the_dataset_date_field():
    url = fiscal.page_url(AUCTIONS, date(2026, 9, 1), page=3, page_size=500)
    assert "filter=auction_date:gte:2026-09-01&sort=auction_date&" in url
    assert url.endswith("page%5Bnumber%5D=3&page%5Bsize%5D=500")


def test_since_backfills_without_watermark_and_looks_back_from_capped_watermark():
    today = date(2026, 10, 8)
    assert fiscal.since(PENNY, None, today) == date(2000, 1, 1)
    early = replace(PENNY, backfill_from=date(1993, 1, 1))
    assert fiscal.since(early, None, today) == date(1993, 1, 1)
    assert fiscal.since(PENNY, "2026-10-06", today) == date(2026, 9, 26)
    # a watermark past today (auction record_date runs ahead) is capped at today first
    assert fiscal.since(PENNY, date(2026, 12, 1), today) == date(2026, 9, 28)


# --- page loop ---------------------------------------------------------------------------------


@respx.mock
def test_fetch_loops_pages_to_total_pages_and_archives_each(client, con):
    ep = "v2/accounting/od/debt_to_penny"
    p1 = respx.get(_url(ep, "record_date", "2026-10-02", 1, 2)).respond(
        200, json=_fixture("debt_to_penny_p1.json")
    )
    p2 = respx.get(_url(ep, "record_date", "2026-10-02", 2, 2)).respond(
        200, json=_fixture("debt_to_penny_p2.json")
    )
    pages = fiscal.fetch(client, PENNY, date(2026, 10, 2), page_size=2)
    assert p1.call_count == 1 and p2.call_count == 1
    assert [p.number for p in pages] == [1, 2]
    assert [r["record_date"] for p in pages for r in p.rows] == [
        date(2026, 10, 2),
        date(2026, 10, 5),
        date(2026, 10, 6),
    ]
    rows = con.execute(
        "SELECT id, source, dataset, http_status FROM raw_response ORDER BY id"
    ).fetchall()
    assert rows == [(pages[0].raw_id, "fiscal", "fd_debt_to_penny", 200),
                    (pages[1].raw_id, "fiscal", "fd_debt_to_penny", 200)]
    with raw.open(con, pages[1].raw_id, raw_dir=client.settings.raw_dir) as fh:
        assert json.loads(fh.read()) == _fixture("debt_to_penny_p2.json")


@respx.mock
def test_fetch_with_zero_total_pages_sends_one_request(client):
    ep = "v1/accounting/mts/mts_table_1"
    route = respx.get(_url(ep, "record_date", "2026-09-01", 1, 2)).respond(
        200, json=_fixture("mts_table_1_empty.json")
    )
    ds = fiscal.Dataset(name="mts_table_1", endpoint=ep)
    pages = fiscal.fetch(client, ds, date(2026, 9, 1), page_size=2)
    assert route.call_count == 1
    assert len(pages) == 1 and pages[0].rows == [] and pages[0].total_pages == 0


@respx.mock
def test_fetch_raises_on_a_non_200_answer_after_archiving_it(client, con):
    ep = "v2/accounting/od/debt_to_penny"
    respx.get(_url(ep, "record_date", "2026-10-02", 1, 2)).respond(
        400, json={"error": "Invalid Query Param", "message": "bad filter"}
    )
    with pytest.raises(fiscal.FiscalError) as err:
        fiscal.fetch(client, PENNY, date(2026, 10, 2), page_size=2)
    assert err.value.raw_id is not None
    assert con.execute("SELECT http_status FROM raw_response").fetchall() == [(400,)]


@respx.mock
def test_fetch_never_sends_fields(client):
    seen: list[httpx.URL] = []

    def answer(request):
        seen.append(request.url)
        return httpx.Response(200, json=_fixture("mts_table_1_empty.json"))

    respx.get(url__startswith=BASE).mock(side_effect=answer)
    fiscal.fetch(client, PENNY, date(2026, 10, 2))
    assert len(seen) == 1
    assert "fields" not in seen[0].params
    assert seen[0].params["page[size]"] == "10000"
    assert seen[0].params["sort"] == "record_date"


# --- casting -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fd_type", "sql"),
    [
        ("DATE", "DATE"),
        ("CURRENCY", "DECIMAL(24,4)"),
        ("CURRENCY0", "DECIMAL(24,4)"),
        ("CURRENCY3", "DECIMAL(24,4)"),
        ("INTEGER", "BIGINT"),
        ("YEAR", "BIGINT"),
        ("QUARTER", "BIGINT"),
        ("MONTH", "BIGINT"),
        ("DAY", "BIGINT"),
        ("NUMBER", "DOUBLE"),
        ("PERCENTAGE", "DOUBLE"),
        ("STRING", "VARCHAR"),
        ("SOMETHING_NEW", "VARCHAR"),
    ],
)
def test_sql_type_map(fd_type, sql):
    assert fiscal.sql_type(fd_type) == sql


def test_cast_debt_to_penny_currency_year_month():
    page = fiscal.cast_page(_fixture("debt_to_penny_p1.json"))
    row = page.rows[0]
    assert row["record_date"] == date(2026, 10, 2)
    assert row["tot_pub_debt_out_amt"] == Decimal("40242446619209.33")
    assert row["record_fiscal_year"] == 2027
    assert row["record_calendar_month"] == 10
    assert row["src_line_nbr"] == 1
    assert page.types["tot_pub_debt_out_amt"] == "CURRENCY"


def test_cast_currency0_and_null_string_in_dts():
    page = fiscal.cast_page(_fixture("dts_operating_cash.json"))
    first = page.rows[0]
    assert page.types["close_today_bal"] == "CURRENCY0"
    assert first["close_today_bal"] is None  # "null" string
    assert first["open_today_bal"] == Decimal(1023554)
    assert first["account_type"] == "Treasury General Account (TGA) Opening Balance"


def test_cast_auctions_number_currency0_strings_and_nulls():
    page = fiscal.cast_page(_fixture("auctions.json"))
    row = page.rows[0]
    assert row["auction_date"] == date(2026, 9, 1)
    assert row["offering_amt"] == Decimal(85000000000)  # CURRENCY0
    assert row["bid_to_cover_ratio"] == 2.85  # NUMBER
    assert row["high_yield"] is None
    assert row["price_per100"] == "99.564250"  # typed STRING upstream, kept as text
    assert row["cusip"] == "912797UK1"


def test_cast_integer_sent_as_a_decimal_string():
    # auctions_query `currently_outstanding` (INTEGER) as "83181000000.000000" since 2026-10-21
    assert fiscal.cast_value("83181000000.000000", "INTEGER") == 83181000000
    assert fiscal.cast_value("-5.0", "INTEGER") == -5
    assert fiscal.cast_value("2026", "YEAR") == 2026
    with pytest.raises(ValueError, match="not an integer"):
        fiscal.cast_value("1.5", "INTEGER")


def test_cast_percentage_and_number():
    rates = fiscal.cast_page(_fixture("avg_interest_rates.json")).rows[0]
    assert rates["avg_interest_rate_amt"] == 3.87
    fx = fiscal.cast_page(_fixture("rates_of_exchange.json")).rows[0]
    assert fx["exchange_rate"] == 65.15
    assert fx["effective_date"] == date(2026, 9, 30)


def test_cast_mts_currency_and_string_ids():
    row = fiscal.cast_page(_fixture("mts_table_1.json")).rows[0]
    assert row["current_month_gross_rcpt_amt"] == Decimal("384862602035.79")
    assert row["parent_id"] == "59308438"


def test_a_column_absent_from_data_types_stays_text():
    doc = {"data": [{"record_date": "2026-10-01", "new_col": "7"}],
           "meta": {"dataTypes": {"record_date": "DATE"}, "total-pages": 1}}
    page = fiscal.cast_page(doc)
    assert page.rows[0]["new_col"] == "7"
    assert page.types["new_col"] == "STRING"


def test_every_type_in_the_six_fixtures_is_mapped_and_lands_in_duckdb(con):
    """Done-when: casting matches the type map for every type seen in the six datasets."""
    seen: set[str] = set()
    for i, name in enumerate(TYPE_FIXTURES):
        page = fiscal.cast_page(_fixture(name))
        assert page.rows, name
        seen |= set(page.types.values())
        cols = list(page.types)
        table = f"t{i}"
        con.execute(
            f"CREATE TABLE {table} ("
            + ", ".join(f'"{c}" {fiscal.sql_type(page.types[c])}' for c in cols)
            + ")"
        )
        con.executemany(
            f"INSERT INTO {table} VALUES ({', '.join('?' for _ in cols)})",
            [[row[c] for c in cols] for row in page.rows],
        )
        back = con.execute(f"SELECT * FROM {table}").fetchall()
        assert [list(r) for r in back] == [[row[c] for c in cols] for row in page.rows], name
    assert seen == {
        "CURRENCY", "CURRENCY0", "DATE", "DAY", "INTEGER", "MONTH", "NUMBER",
        "PERCENTAGE", "QUARTER", "STRING", "YEAR",
    }


# --- registry config + probe -------------------------------------------------------------------


def test_load_datasets_reads_the_repo_registry():
    datasets = fiscal.load_datasets(REPO_CONFIG)
    assert set(datasets) == {
        "dts_operating_cash", "debt_to_penny", "auctions",
        "avg_interest_rates", "mts_table_1", "rates_of_exchange",
    }
    assert datasets["auctions"].date_field == "auction_date"
    assert datasets["debt_to_penny"].endpoint == "v2/accounting/od/debt_to_penny"
    assert datasets["debt_to_penny"].lookback_days == 10
    assert datasets["debt_to_penny"].raw_dataset == "fd_debt_to_penny"


def test_load_datasets_rejects_entries_without_endpoint(tmp_path):
    (tmp_path / "fiscal_data.yaml").write_text("datasets:\n  - name: x\n", "utf-8")
    with pytest.raises(ConfigError):
        fiscal.load_datasets(tmp_path)


@respx.mock
def test_cli_fd_probe_prints_rows_columns_types(tmp_path, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(tmp_path / "usfin.duckdb"))
    monkeypatch.setenv("USFIN_RAW", str(tmp_path / "raw"))
    monkeypatch.setenv("USFIN_CONFIG", str(REPO_CONFIG))
    monkeypatch.chdir(tmp_path)
    ep = "v2/accounting/od/debt_to_penny"
    for n in (1, 2):  # the recorded pages say total-pages 2, so the probe asks for both
        respx.get(_url(ep, "record_date", "2026-10-02", n, 10000)).respond(
            200, json=_fixture(f"debt_to_penny_p{n}.json")
        )
    result = CliRunner().invoke(app, ["fd", "probe", "debt_to_penny", "--since", "2026-10-02"])
    assert result.exit_code == 0, result.output
    assert "rows: 3 (pages 2" in result.output
    assert "tot_pub_debt_out_amt\tCURRENCY\tDECIMAL(24,4)" in result.output
    assert "record_date\tDATE\tDATE" in result.output


def test_cli_fd_probe_unknown_dataset_exits_2(tmp_path, monkeypatch):
    monkeypatch.setenv("USFIN_CONFIG", str(REPO_CONFIG))
    result = CliRunner().invoke(app, ["fd", "probe", "nope"])
    assert result.exit_code == 2
