"""BLS client: request bodies (v1 keyless / v2 with key + catalog), parsing, `bls validate`.

`tests/fixtures/bls/v1_timeseries.json` is a real keyless v1 response recorded 2026-10-08 for the
seven configured IDs plus the made-up `XXX000BOGUS` (startyear 2025, endyear 2026), trimmed to the
first two observations per series plus CUSR0000SA0's 2025 M10, which BLS publishes as value "-"
with footnote X (the 2025 lapse in appropriations). No v2 response is recorded: no key was set.
"""

import json
import shutil
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, ratelimit, store
from usfin.cli import app
from usfin.settings import ConfigError, load_settings
from usfin.sources import bls

FIXTURES = Path(__file__).parent / "fixtures" / "bls"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
V1 = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
V2 = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SEVEN = {
    "CUSR0000SA0": "CPIAUCSL",
    "CUSR0000SA0L1E": "CPILFESL",
    "WPSFD4": "PPIFIS",
    "CES0000000001": "PAYEMS",
    "LNS14000000": "UNRATE",
    "CES0500000003": "CES0500000003",
    "JTS000000000000000JOL": "JTSJOL",
}


def _fixture() -> dict:
    return json.loads((FIXTURES / "v1_timeseries.json").read_text("utf-8"))


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
    for key in ("BLS_API_KEY", "USFIN_CONFIG", "USFIN_RAW"):
        monkeypatch.delenv(key, raising=False)
    settings = replace(
        load_settings(env_file=tmp_path / "no.env"),
        raw_dir=tmp_path / "raw",
        config_dir=REPO_CONFIG,
    )
    clock = FakeClock()
    limiters = ratelimit.Limiters(clock=clock, sleep=clock.sleep)
    with http.Client(con, settings=settings, limiters=limiters, clock=clock, sleep=clock.sleep) as c:
        yield c


# --- config ------------------------------------------------------------------------------------


def test_bls_yaml_lists_the_seven_series_with_fred_ids_decimals_and_thresholds():
    series = bls.load_series(REPO_CONFIG)
    assert {s.series_id: s.fred_id for s in series.values()} == SEVEN
    assert list(series) == list(SEVEN)
    for s in series.values():
        assert s.title and s.units and s.seasonality in ("S", "U")
        assert s.decimals >= 0 and s.jump_pct > 0
    assert series["LNS14000000"].decimals == 1
    assert series["CUSR0000SA0"].decimals == 3


def test_load_series_rejects_duplicates_and_missing_fields(tmp_path):
    (tmp_path / "bls.yaml").write_text(
        "series:\n  - {id: A, fred: X, title: t, units: u, decimals: 1, jump_pct: 2}\n"
        "  - {id: A, fred: Y, title: t, units: u, decimals: 1, jump_pct: 2}\n"
    )
    with pytest.raises(ConfigError, match="twice"):
        bls.load_series(tmp_path)
    (tmp_path / "bls.yaml").write_text("series:\n  - {id: A, title: t}\n")
    with pytest.raises(ConfigError, match="A"):
        bls.load_series(tmp_path)


# --- request -----------------------------------------------------------------------------------


def test_v2_request_with_key_sends_registrationkey_and_catalog():
    url, body = bls.request(["CUSR0000SA0", "LNS14000000"], 2025, 2026, api_key="k3y")
    assert url == V2
    assert body == {
        "seriesid": ["CUSR0000SA0", "LNS14000000"],
        "startyear": "2025",
        "endyear": "2026",
        "registrationkey": "k3y",
        "catalog": True,
    }


def test_v1_request_without_key_has_no_key_and_no_catalog():
    url, body = bls.request(["CUSR0000SA0"], 2025, 2026, api_key=None)
    assert url == V1
    assert body == {"seriesid": ["CUSR0000SA0"], "startyear": "2025", "endyear": "2026"}


def test_request_enforces_the_api_limits():
    with pytest.raises(ValueError, match="25"):
        bls.request([f"S{i}" for i in range(26)], 2025, 2026, api_key=None)
    with pytest.raises(ValueError, match="10"):
        bls.request(["S"], 2016, 2026, api_key=None)
    bls.request([f"S{i}" for i in range(50)], 2007, 2026, api_key="k")  # v2: 50 series, 20 years


@respx.mock
def test_fetch_posts_json_and_archives_as_bls_timeseries(client, con):
    route = respx.post(V1).respond(200, json=_fixture())
    raw_id, doc = bls.fetch(client, list(SEVEN), 2025, 2026, api_key=None)
    sent = json.loads(route.calls.last.request.content)
    assert sent == {"seriesid": list(SEVEN), "startyear": "2025", "endyear": "2026"}
    assert doc["status"] == "REQUEST_SUCCEEDED"
    row = con.execute(
        "SELECT source, dataset, url, http_status FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    assert row == ("bls", "bls_timeseries", V1, 200)


@respx.mock
def test_fetch_raises_on_http_error_and_on_a_request_bls_did_not_process(client):
    not_processed = {**_fixture(), "status": "REQUEST_NOT_PROCESSED"}
    respx.post(V1).mock(
        side_effect=[httpx.Response(400, text="bad"), httpx.Response(200, json=not_processed)]
    )
    with pytest.raises(bls.BlsError) as err:
        bls.fetch(client, ["CUSR0000SA0"], 2025, 2026, api_key=None)
    assert err.value.raw_id is not None
    with pytest.raises(bls.BlsError, match="REQUEST_NOT_PROCESSED"):
        bls.fetch(client, ["CUSR0000SA0"], 2025, 2026, api_key=None)


# --- parse -------------------------------------------------------------------------------------


def test_parse_yields_series_year_period_value_footnotes():
    obs = bls.parse(_fixture())
    by_key = {(o.series_id, o.year, o.period): o for o in obs}
    cpi = by_key[("CUSR0000SA0", 2026, "M08")]
    assert cpi.value == Decimal("334.131")
    assert cpi.footnotes == ()
    ppi = by_key[("WPSFD4", 2026, "M08")]
    assert ppi.value == Decimal("157.411")
    assert ppi.footnotes[0]["code"] == "P"
    assert by_key[("CES0000000001", 2026, "M09")].value == Decimal(159044)
    assert by_key[("LNS14000000", 2026, "M09")].value == Decimal("4.2")
    # 2025 lapse in appropriations: BLS publishes "-" with footnote X
    gap = by_key[("CUSR0000SA0", 2025, "M10")]
    assert gap.value is None
    assert gap.footnotes == ({"code": "X", "text": gap.footnotes[0]["text"]},)
    assert "lapse in appropriations" in gap.footnotes[0]["text"]
    assert not any(o.series_id == "XXX000BOGUS" for o in obs)
    assert len(obs) == 15


# --- validate ----------------------------------------------------------------------------------


def test_validate_flags_an_unknown_id_and_reports_latest_periods():
    checks = {c.series_id: c for c in bls.validate(_fixture(), [*SEVEN, "XXX000BOGUS", "NOPE"])}
    assert all(checks[s].ok for s in SEVEN)
    assert checks["CUSR0000SA0"].latest == (2026, "M08")
    assert checks["LNS14000000"].latest == (2026, "M09")
    bogus = checks["XXX000BOGUS"]
    assert not bogus.ok and "Series does not exist" in bogus.message
    assert not checks["NOPE"].ok and "not in the response" in checks["NOPE"].message


def test_bls_series_table_exists(con):
    cols = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'bls_series'"
        " ORDER BY ordinal_position"
    ).fetchall()
    assert [c[0] for c in cols] == ["series_id", "title", "seasonality", "units", "decimals"]


def _cli_env(tmp_path, monkeypatch, config_dir):
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    monkeypatch.setenv("USFIN_DB", str(tmp_path / "usfin.duckdb"))
    monkeypatch.setenv("USFIN_RAW", str(tmp_path / "raw"))
    monkeypatch.setenv("USFIN_CONFIG", str(config_dir))
    monkeypatch.chdir(tmp_path)


@respx.mock
def test_cli_bls_validate_uses_v1_without_key_and_stores_bls_series(tmp_path, monkeypatch):
    _cli_env(tmp_path, monkeypatch, REPO_CONFIG)
    monkeypatch.setattr(bls, "_today", lambda: date(2026, 10, 8))
    route = respx.post(V1).respond(200, json=_fixture())
    result = CliRunner().invoke(app, ["bls", "validate"])
    assert result.exit_code == 0, result.output
    assert route.call_count == 1
    sent = json.loads(route.calls.last.request.content)
    assert sent["startyear"] == "2025" and sent["endyear"] == "2026"
    assert "v1 (no BLS_API_KEY)" in result.output
    assert "CUSR0000SA0\tok\t2026 M08" in result.output
    assert "7 of 7 series ok" in result.output
    with store.session(tmp_path / "usfin.duckdb", wait=0) as c:
        rows = c.execute("SELECT series_id, decimals FROM bls_series ORDER BY 1").fetchall()
    assert dict(rows) == {s.series_id: s.decimals for s in bls.load_series(REPO_CONFIG).values()}


@respx.mock
def test_cli_bls_validate_flags_an_unknown_id_and_exits_1(tmp_path, monkeypatch):
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    with (config / "bls.yaml").open("a") as f:
        f.write(
            "  - {id: XXX000BOGUS, fred: NONE, title: bogus, seasonality: U, units: none,"
            " decimals: 0, jump_pct: 1}\n"
        )
    _cli_env(tmp_path, monkeypatch, config)
    respx.post(V1).respond(200, json=_fixture())
    result = CliRunner().invoke(app, ["bls", "validate"])
    assert result.exit_code == 1, result.output
    assert "XXX000BOGUS\tFAILED\tSeries does not exist for Series XXX000BOGUS" in result.output
    assert "7 of 8 series ok" in result.output


@respx.mock
def test_cli_bls_validate_uses_v2_with_key(tmp_path, monkeypatch):
    _cli_env(tmp_path, monkeypatch, REPO_CONFIG)
    monkeypatch.setenv("BLS_API_KEY", "k3y")
    route = respx.post(V2).respond(200, json=_fixture())
    result = CliRunner().invoke(app, ["bls", "validate"])
    assert result.exit_code == 0, result.output
    sent = json.loads(route.calls.last.request.content)
    assert sent["registrationkey"] == "k3y" and sent["catalog"] is True
    assert "v2" in result.output and "k3y" not in result.output
