"""Job `fiscal`: registry-driven tables `fd_<name>`, upserts, watermarks, schema drift, reparse.

Fixtures are the real responses recorded in S7 (tests/fixtures/fiscal/). Most were recorded with a
tiny `page[size]`, so their `meta.total-pages` is > 1; `_one_page` serves them as the only page
(only `meta.total-pages` is changed). `debt_to_penny` is served as its real two pages.
"""

import copy
import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
import yaml

from usfin import http, jobs, ratelimit, store
from usfin.settings import load_settings
from usfin.sources import fiscal

FIXTURES = Path(__file__).parent / "fixtures" / "fiscal"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
NOW = datetime(2026, 10, 8, 21, 0, tzinfo=UTC)  # 17:00 ET, today 2026-10-08
LATER = datetime(2026, 10, 9, 21, 0, tzinfo=UTC)
NAMES = (
    "dts_operating_cash",
    "debt_to_penny",
    "auctions",
    "avg_interest_rates",
    "mts_table_1",
    "rates_of_exchange",
)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text("utf-8"))


def _one_page(doc: dict) -> dict:
    doc = copy.deepcopy(doc)
    doc["meta"]["total-pages"] = 1 if doc["data"] else 0
    return doc


def _pages() -> dict[str, list[dict]]:
    """name -> the pages the fake API serves, in page order."""
    return {
        "dts_operating_cash": [_one_page(_fixture("dts_operating_cash.json"))],
        "debt_to_penny": [_fixture("debt_to_penny_p1.json"), _fixture("debt_to_penny_p2.json")],
        "auctions": [_one_page(_fixture("auctions.json"))],
        "avg_interest_rates": [_one_page(_fixture("avg_interest_rates.json"))],
        "mts_table_1": [_one_page(_fixture("mts_table_1.json"))],
        "rates_of_exchange": [_one_page(_fixture("rates_of_exchange.json"))],
    }


class FakeApi:
    """Serves `pages[name]` by endpoint and `page[number]`; records every request URL."""

    def __init__(self, config_dir: Path):
        self.datasets = fiscal.load_datasets(config_dir)
        self.pages = _pages()
        self.status: dict[str, int] = {}
        self.urls: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.urls.append(url)
        path = request.url.path.split("/fiscal_service/", 1)[1]
        name = next(n for n, ds in self.datasets.items() if ds.endpoint == path)
        if name in self.status:
            return httpx.Response(self.status[name], json={"error": "boom"})
        number = int(request.url.params["page[number]"])
        return httpx.Response(200, json=self.pages[name][number - 1])

    def url_for(self, name: str) -> str:
        endpoint = self.datasets[name].endpoint
        return next(u for u in self.urls if f"/{endpoint}?" in u)


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    # no real sleeping: rate limiter on a fake clock, retries without backoff
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
    fake = FakeApi(settings.config_dir)
    with respx.mock(assert_all_called=False) as router:
        router.route(host="api.fiscaldata.treasury.gov").mock(side_effect=fake)
        yield fake


@pytest.fixture
def drift(monkeypatch):
    calls: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(
        fiscal,
        "on_schema_drift",
        lambda ctx, dataset, columns: calls.append((ctx.job.name, dataset, dict(columns))),
    )
    return calls


def _registry():
    reg = jobs.Registry()
    reg.register(fiscal.JOB)
    return reg


def _run(settings, clock=NOW):
    return jobs.run_job("fiscal", settings=settings, registry=_registry(), clock=lambda: clock)


def _tables(settings) -> dict[str, tuple[list, list]]:
    """Every fd_ table: (columns with types in order, all rows sorted)."""
    out = {}
    with store.session(settings.db_path, wait=0) as con:
        for table in store.table_names(con):
            if not table.startswith("fd_"):
                continue
            cols = con.execute(
                "SELECT column_name, data_type FROM information_schema.columns"
                " WHERE table_name = ? ORDER BY ordinal_position",
                [table],
            ).fetchall()
            rows = con.execute(f'SELECT * FROM "{table}"').fetchall()
            out[table] = (cols, sorted(rows, key=repr))
    return out


def _watermarks(settings) -> dict[str, str]:
    with store.session(settings.db_path, wait=0) as con:
        return jobs.get_watermarks(con, "fiscal")


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


# --- registry ----------------------------------------------------------------------------------


def test_repo_config_lists_six_datasets_each_with_a_pk():
    datasets = fiscal.load_datasets(REPO_CONFIG)
    assert tuple(datasets) == NAMES
    assert datasets["auctions"].pk == ("cusip", "auction_date")
    assert datasets["debt_to_penny"].pk == ("record_date",)
    assert all(ds.pk for ds in datasets.values())


def test_fiscal_job_is_registered_with_one_key_per_registry_entry(monkeypatch):
    monkeypatch.delenv("USFIN_DEMO", raising=False)
    registry = jobs.load_registry()
    assert "fiscal" in registry
    job = registry.get("fiscal")
    assert set(job.parser_map(load_settings())) >= {f"fd_{n}" for n in NAMES}
    assert "fd_probe" not in job.parser_map(load_settings())


# --- first run ---------------------------------------------------------------------------------


def test_first_run_creates_each_table_from_datatypes_with_the_registry_pk(settings, api):
    result = _run(settings)
    assert result.status == "ok", result.error
    datasets = fiscal.load_datasets(settings.config_dir)
    tables = _tables(settings)
    assert set(tables) == {f"fd_{n}" for n in NAMES} | {"fd_mts_monthly"}  # + derived view
    for name in NAMES:
        types = api.pages[name][0]["meta"]["dataTypes"]
        cols, rows = tables[f"fd_{name}"]
        assert cols == [(c, fiscal.sql_type(t)) for c, t in types.items()]
        assert len(rows) == sum(len(p["data"]) for p in api.pages[name])
        pk = _query(
            settings,
            "SELECT constraint_column_names FROM duckdb_constraints()"
            " WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
            [f"fd_{name}"],
        )
        assert pk == [(list(datasets[name].pk),)]
    # backfill: no watermark → backfill_from, sorted on the date field, never fields=
    assert "filter=record_date:gte:2000-01-01&sort=record_date&" in api.url_for("debt_to_penny")
    assert all("fields=" not in u for u in api.urls)
    assert result.rows_upserted == sum(len(r) for _, r in tables.values())
    penny = _query(settings, "SELECT tot_pub_debt_out_amt FROM fd_debt_to_penny ORDER BY 1")
    assert all(isinstance(v, Decimal) for (v,) in penny)


def test_stored_rows_equal_the_cast_rows_exactly(settings, api):
    page = copy.deepcopy(api.pages["avg_interest_rates"][0])
    page["data"][0]["security_desc"] = 'Bills "quoted" \\ back – ünïcode'
    api.pages["avg_interest_rates"] = [page]
    _run(settings)
    for name in NAMES:
        cast = [fiscal.cast_page(p) for p in api.pages[name]]
        cols = list(cast[0].types)
        expected = sorted((tuple(r.get(c) for c in cols) for p in cast for r in p.rows), key=repr)
        assert _tables(settings)[f"fd_{name}"][1] == expected, name


def test_watermark_is_the_newest_date_field_value_per_dataset(settings, api):
    _run(settings)
    marks = _watermarks(settings)
    assert marks["debt_to_penny"] == "2026-10-06"  # newest row is on page 2
    assert marks["dts_operating_cash"] == "2026-09-01"
    assert marks["mts_table_1"] == "2026-08-31"


def test_auctions_filter_and_watermark_use_auction_date(settings, api):
    _run(settings)
    assert "filter=auction_date:gte:2000-01-01&sort=auction_date&" in api.url_for("auctions")
    # the fixture row: record_date 2026-09-03 (issue date), auction_date 2026-09-01
    assert _watermarks(settings)["auctions"] == "2026-09-01"


def test_an_empty_response_creates_the_table_and_sets_no_watermark(settings, api):
    api.pages["mts_table_1"] = [_fixture("mts_table_1_empty.json")]
    result = _run(settings)
    assert result.status == "ok", result.error
    cols, rows = _tables(settings)["fd_mts_table_1"]
    assert rows == [] and len(cols) == len(api.pages["mts_table_1"][0]["meta"]["dataTypes"])
    assert "mts_table_1" not in _watermarks(settings)


# --- C2 / C3 -----------------------------------------------------------------------------------


def test_second_run_with_the_same_data_changes_nothing(settings, api):
    _run(settings)
    before = _tables(settings)
    again = _run(settings, clock=LATER)
    assert again.status == "ok", again.error
    assert again.rows_upserted == 0
    assert _tables(settings) == before


def test_second_run_refetches_the_overlap_and_picks_up_a_revised_value(settings, api):
    _run(settings)
    api.urls.clear()
    revised = copy.deepcopy(api.pages["debt_to_penny"])
    revised[1]["data"][0]["tot_pub_debt_out_amt"] = "38000000000000.01"
    api.pages["debt_to_penny"] = revised
    result = _run(settings, clock=LATER)
    assert result.status == "ok", result.error
    # watermark 2026-10-06 - 10 days lookback
    assert "filter=record_date:gte:2026-09-26&" in api.url_for("debt_to_penny")
    assert result.rows_upserted == 1
    assert _query(
        settings,
        "SELECT tot_pub_debt_out_amt FROM fd_debt_to_penny WHERE record_date = DATE '2026-10-06'",
    ) == [(Decimal("38000000000000.0100"),)]


# --- C16 ---------------------------------------------------------------------------------------


def test_an_unseen_column_is_added_with_alter_table_and_raises_schema_drift(settings, api, drift):
    _run(settings)
    assert drift == []
    page = copy.deepcopy(api.pages["avg_interest_rates"][0])
    page["meta"]["dataTypes"]["new_rate_pct"] = "PERCENTAGE"
    for row in page["data"]:
        row["new_rate_pct"] = "4.25"
    api.pages["avg_interest_rates"] = [page]
    result = _run(settings, clock=LATER)
    assert result.status == "ok", result.error
    cols, rows = _tables(settings)["fd_avg_interest_rates"]
    assert cols[-1] == ("new_rate_pct", "DOUBLE")
    assert {r[-1] for r in rows} == {4.25}
    assert drift == [("fiscal", "avg_interest_rates", {"new_rate_pct": "PERCENTAGE"})]
    _run(settings, clock=LATER)
    assert len(drift) == 1  # the column is known now: no second drift


def test_a_column_missing_from_a_later_response_keeps_its_stored_values(settings, api, drift):
    _run(settings)
    page = copy.deepcopy(api.pages["avg_interest_rates"][0])
    del page["meta"]["dataTypes"]["src_line_nbr"]
    for row in page["data"]:
        del row["src_line_nbr"]
    api.pages["avg_interest_rates"] = [page]
    result = _run(settings, clock=LATER)
    assert result.status == "ok", result.error
    assert result.rows_upserted == 0
    assert _query(
        settings, "SELECT count(*) FROM fd_avg_interest_rates WHERE src_line_nbr IS NULL"
    ) == [(0,)]
    assert drift == []


# --- C7 ----------------------------------------------------------------------------------------


def test_one_dataset_answering_500_fails_its_key_and_the_others_commit(settings, api):
    first = _run(settings)
    assert first.status == "ok"
    before = _watermarks(settings)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM fd_rates_of_exchange")
    api.status["debt_to_penny"] = 500
    revised = copy.deepcopy(api.pages["mts_table_1"])
    revised[0]["data"][0]["current_month_dfct_sur_amt"] = "-1.00"
    api.pages["mts_table_1"] = revised
    result = _run(settings, clock=LATER)
    assert result.status == "failed"
    assert set(result.key_errors) == {"debt_to_penny"}
    assert "500" in result.key_errors["debt_to_penny"]
    marks = _watermarks(settings)
    assert marks["debt_to_penny"] == before["debt_to_penny"]
    # the other keys committed: rows re-inserted, revision applied
    assert _query(settings, "SELECT count(*) FROM fd_rates_of_exchange") == [(2,)]
    assert (Decimal("-1.0000"),) in _query(
        settings, "SELECT current_month_dfct_sur_amt FROM fd_mts_table_1"
    )
    # the 500 was archived (C1) under the dataset's raw name
    assert _query(
        settings,
        "SELECT count(*) FROM raw_response WHERE dataset = 'fd_debt_to_penny'"
        " AND http_status = 500",
    ) == [(1,)]


def test_a_duplicate_pk_within_a_response_fails_that_key_with_a_clear_error(settings, api):
    page = copy.deepcopy(api.pages["avg_interest_rates"][0])
    page["data"][1]["security_desc"] = page["data"][0]["security_desc"]
    page["data"][1]["security_type_desc"] = page["data"][0]["security_type_desc"]
    api.pages["avg_interest_rates"] = [page]
    result = _run(settings)
    assert set(result.key_errors) == {"avg_interest_rates"}
    assert "duplicate" in result.key_errors["avg_interest_rates"]
    # the parse rolled back whole, table creation included; the other datasets committed
    tables = _tables(settings)
    assert "fd_avg_interest_rates" not in tables and "fd_debt_to_penny" in tables


def test_a_dataset_without_pk_fails_its_key(settings, api):
    path = settings.config_dir / "fiscal_data.yaml"
    data = yaml.safe_load(path.read_text("utf-8"))
    for entry in data["datasets"]:
        if entry["name"] == "debt_to_penny":
            entry.pop("pk")
    path.write_text(yaml.safe_dump(data), "utf-8")
    result = _run(settings)
    assert set(result.key_errors) == {"debt_to_penny"}
    assert "pk" in result.key_errors["debt_to_penny"]


# --- C1 ----------------------------------------------------------------------------------------


class FailingTransport(httpx.BaseTransport):
    def handle_request(self, request):
        raise AssertionError(f"reparse sent a request: {request.url}")


def test_reparse_rebuilds_identical_tables_without_http(settings, api, drift):
    _run(settings)
    revised = copy.deepcopy(api.pages["debt_to_penny"])
    revised[1]["data"][0]["tot_pub_debt_out_amt"] = "38000000000000.01"
    api.pages["debt_to_penny"] = revised
    page = copy.deepcopy(api.pages["avg_interest_rates"][0])
    page["meta"]["dataTypes"]["new_rate_pct"] = "PERCENTAGE"
    for row in page["data"]:
        row["new_rate_pct"] = "4.25"
    api.pages["avg_interest_rates"] = [page]
    _run(settings, clock=LATER)
    built = _tables(settings)
    marks = _watermarks(settings)

    with store.session(settings.db_path, wait=0) as con:
        for table in built:
            kind = "VIEW" if table == "fd_mts_monthly" else "TABLE"  # the derived view (S37)
            con.execute(f'DROP {kind} "{table}"')
        result = jobs.reparse(
            con, fiscal.JOB, settings=settings, now=LATER, transport=FailingTransport()
        )
    assert result.raw_ids  # 7 pages per run, two runs
    assert len(result.raw_ids) == 14
    assert _tables(settings) == built
    assert _watermarks(settings) == marks


def test_reparse_ignores_probe_responses(settings, api):
    _run(settings)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("UPDATE raw_response SET dataset = 'fd_probe' WHERE id = 1")
        result = jobs.reparse(
            con, fiscal.JOB, settings=settings, now=LATER, transport=FailingTransport()
        )
    assert 1 not in result.raw_ids
