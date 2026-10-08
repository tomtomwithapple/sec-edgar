"""Monthly Treasury Statement table 1 as a true monthly series (`fd_mts_monthly`).

`line_code_nbr` is a row's position within one release, not a series (S32): every release lists
the prior fiscal year's twelve months, the current fiscal year's months to date and a
Year-to-Date line per block, each block under a header row `FY <year>` (`data_type_cd` S). A
monthly line (`record_type_cd` MTH) is a calendar month given by its header's fiscal year and its
`classification_desc` (the month name); the release's own month (`record_date`) is the current-month
line of the `FY <record_fiscal_year>` block.

Fixtures are two consecutive real releases (2026-07-31 and 2026-08-31, recorded 2026-10-08 with
`filter=record_date:eq:<date>`), trimmed to both headers, the prior-year June and Year-to-Date
lines, and the current year's October, June, July (revised by the August release), August and
Year-to-Date lines.
"""

import copy
import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
import yaml
from fastapi.testclient import TestClient

from usfin import alerts, http, jobs, ratelimit, scheduler, store
from usfin.settings import ConfigError, load_settings
from usfin.sources import fiscal
from usfin.web import app as web

FIXTURES = Path(__file__).parent / "fixtures" / "fiscal"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
JUL = "mts_table_1_2026-07-31.json"
AUG = "mts_table_1_2026-08-31.json"
NOW = datetime(2026, 10, 8, 21, 0, tzinfo=UTC)
LATER = datetime(2026, 10, 9, 21, 0, tzinfo=UTC)
MTS_ENDPOINT = "v1/accounting/mts/mts_table_1"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text("utf-8"))


def _pages(*names: str) -> list[dict]:
    """The releases as the pages of one response (`meta.total-pages` = how many)."""
    out = []
    for name in names:
        doc = copy.deepcopy(_fixture(name))
        doc["meta"]["total-pages"] = len(names)
        out.append(doc)
    return out


class MtsApi:
    """Serves `pages` for MTS table 1 and an empty page for every other dataset."""

    def __init__(self):
        self.pages: list[dict] = _pages(JUL)
        self.empty = _fixture("mts_table_1_empty.json")

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if not request.url.path.endswith(MTS_ENDPOINT):
            doc = {"data": [], "meta": {"total-pages": 0, "dataTypes": {"record_date": "DATE"}}}
            if "auctions_query" in request.url.path:
                doc["meta"]["dataTypes"] = {"cusip": "STRING", "auction_date": "DATE"}
            elif "dts" in request.url.path:
                doc["meta"]["dataTypes"]["account_type"] = "STRING"
            elif "avg_interest" in request.url.path:
                doc["meta"]["dataTypes"] |= {"security_type_desc": "STRING", "security_desc": "STRING"}
            elif "rates_of_exchange" in request.url.path:
                doc["meta"]["dataTypes"]["src_line_nbr"] = "INTEGER"
            return httpx.Response(200, json=doc)
        number = int(request.url.params["page[number]"])
        return httpx.Response(200, json=self.pages[number - 1])


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
    fake = MtsApi()
    with respx.mock(assert_all_called=False) as router:
        router.route(host="api.fiscaldata.treasury.gov").mock(side_effect=fake)
        yield fake


def _run(settings, clock=NOW):
    reg = jobs.Registry()
    reg.register(fiscal.JOB)
    result = jobs.run_job(
        "fiscal", settings=settings, registry=reg, clock=lambda: clock, chain=False
    )
    assert result.status == "ok", result.error
    return result


def _monthly(settings) -> list[dict]:
    with store.session(settings.db_path, wait=0) as con:
        cur = con.execute("SELECT * FROM fd_mts_monthly ORDER BY record_date")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _two_releases(settings, api):
    _run(settings)  # the July release alone
    api.pages = _pages(JUL, AUG)  # next day: the overlap re-reads July, August is new
    _run(settings, clock=LATER)


# --- config -------------------------------------------------------------------------------


def test_repo_registry_derives_mts_monthly_from_mts_table_1():
    datasets = fiscal.load_datasets(REPO_CONFIG)
    assert datasets["mts_table_1"].derived == "mts_monthly"
    derived = fiscal.derived_dataset(datasets["mts_table_1"])
    assert (derived.name, derived.table, derived.pk, derived.date_field) == (
        "mts_monthly",
        "fd_mts_monthly",
        ("record_date",),
        "record_date",
    )
    assert derived.jump_fields == ("current_month_dfct_sur_amt",)
    assert [ds.name for ds in fiscal.browsable(REPO_CONFIG).values()] == [
        "dts_operating_cash",
        "debt_to_penny",
        "auctions",
        "avg_interest_rates",
        "mts_table_1",
        "mts_monthly",
        "rates_of_exchange",
    ]


def test_unknown_derived_series_is_a_config_error(tmp_path):
    (tmp_path / "fiscal_data.yaml").write_text(
        "datasets: [{name: x, endpoint: v1/x, pk: [record_date], derived: nope}]\n", "utf-8"
    )
    with pytest.raises(ConfigError, match="nope"):
        fiscal.load_datasets(tmp_path)


# --- the derived view ---------------------------------------------------------------------


def test_one_row_per_month_from_monthly_lines_headers_and_year_to_date_excluded(settings, api):
    _run(settings)
    rows = _monthly(settings)
    # July release: prior-year June (line 100) is June 2025, not a second June 2026; the FY
    # headers and both Year-to-Date lines are not months
    assert [r["record_date"] for r in rows] == [
        date(2025, 6, 30),
        date(2025, 10, 31),
        date(2026, 6, 30),
        date(2026, 7, 31),
    ]
    june_2025, oct_2025 = rows[0], rows[1]
    assert june_2025["current_month_dfct_sur_amt"] == Decimal("-27009650010.0100")
    assert (june_2025["record_fiscal_year"], june_2025["classification_desc"]) == (2025, "June")
    assert (oct_2025["record_fiscal_year"], oct_2025["record_calendar_year"]) == (2026, 2025)
    assert oct_2025["record_calendar_month"] == 10
    july = rows[3]  # the release's current-month line
    assert july["first_release_date"] == july["release_date"] == date(2026, 7, 31)
    assert july["current_month_dfct_sur_amt"] == Decimal("432307874621.4800")
    assert july["current_month_gross_rcpt_amt"] == Decimal("334009875555.7900")
    assert july["first_dfct_sur_amt"] == Decimal("432307874621.4800")


def test_a_later_release_revising_a_month_is_a_new_value_not_a_duplicate(settings, api):
    _two_releases(settings, api)
    rows = _monthly(settings)
    dates = [r["record_date"] for r in rows]
    assert dates == sorted(set(dates))  # each month once
    assert dates[-2:] == [date(2026, 7, 31), date(2026, 8, 31)]
    july, august = rows[-2], rows[-1]
    # July as revised by the August release; its first print is kept beside it
    assert july["current_month_dfct_sur_amt"] == Decimal("432285727964.6000")
    assert july["first_dfct_sur_amt"] == Decimal("432307874621.4800")
    assert (july["first_release_date"], july["release_date"]) == (
        date(2026, 7, 31),
        date(2026, 8, 31),
    )
    assert august["current_month_dfct_sur_amt"] == Decimal("166796952277.3800")
    assert august["first_release_date"] == august["release_date"] == date(2026, 8, 31)


def test_view_is_unchanged_by_a_repeat_run_and_by_reparse(settings, api):
    _two_releases(settings, api)
    before = _monthly(settings)
    _run(settings, clock=LATER)
    assert _monthly(settings) == before
    with store.session(settings.db_path, wait=0) as con:
        jobs.reparse(con, fiscal.JOB, settings=settings)
    assert _monthly(settings) == before


# --- alerts -------------------------------------------------------------------------------


def test_value_jump_for_mts_runs_on_the_monthly_series(settings, api):
    observed = fiscal.JOB.observed(settings)["mts_table_1"]
    assert (observed.table, observed.date_column, observed.series) == (
        "fd_mts_monthly",
        "record_date",
        (),
    )
    assert observed.values == ("current_month_dfct_sur_amt",)
    # a threshold low enough that a line-position view would flag the revised July line and
    # both Year-to-Date lines: on the monthly series only August (new) is checked, against July
    path = settings.config_dir / "schedule.yaml"
    sched = yaml.safe_load(path.read_text("utf-8"))
    sched["jobs"]["fiscal"]["keys"]["mts_table_1"]["value_jump"] = {"pct": 0.001}
    path.write_text(yaml.safe_dump(sched), "utf-8")
    _two_releases(settings, api)
    with store.session(settings.db_path, wait=0) as con:
        found = [
            a
            for a in alerts.list_alerts(con, include_acked=True)
            if a["rule"] == "value_jump" and a["subject"].startswith("mts_table_1:")
        ]
    assert [a["subject"] for a in found] == ["mts_table_1:current_month_dfct_sur_amt@2026-08-31"]
    detail = found[0]["detail"]
    detail = json.loads(detail) if isinstance(detail, str) else detail
    assert detail["table"] == "fd_mts_monthly"
    assert detail["previous"] == pytest.approx(432285727964.60)
    assert detail["value"] == pytest.approx(166796952277.38)


# --- series browser -----------------------------------------------------------------------


@pytest.fixture
def client(settings, api):
    _two_releases(settings, api)
    s = scheduler.Scheduler(settings, jobs.Registry(), clock=lambda: LATER)
    yield TestClient(web.create_app(s, manage=False, ui_wait=0.3))
    s.shutdown()


def test_series_browser_lists_and_plots_the_monthly_series(client):
    index = client.get("/series")
    assert 'href="/series/fiscal/mts_monthly"' in index.text
    page = client.get("/series/fiscal/mts_monthly")
    assert page.status_code == 200
    assert "<svg" in page.text and "current_month_dfct_sur_amt" in page.text
    assert "Pick a series" not in page.text
    rows = client.get("/series/fiscal/mts_monthly?format=csv").text.strip().splitlines()[1:]
    months = [r.split(",")[0] for r in rows]
    assert months == [
        "2026-08-31",
        "2026-07-31",
        "2026-06-30",
        "2025-10-31",
        "2025-06-30",
    ]  # one point per month, newest first
    assert "432285727964.6000" in rows[1]
