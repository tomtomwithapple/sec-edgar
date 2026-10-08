"""EDGAR frames (cache `frames_cache`, TTL 24 h) and the concept map screen (S28).

`tests/fixtures/edgar/frames/us-gaap_Revenues_USD_CY2025Q2.json` is the real response of
`https://data.sec.gov/api/xbrl/frames/us-gaap/Revenues/USD/CY2025Q2.json` fetched 2026-10-08
through the client (`usfin http get`, 296 137 bytes, 1 939 data points), trimmed to 3 of its
`data` entries (ACME United, Air Products, Walmart); every other key verbatim, so `pts` still says
1 939. The screen tests store the Apple companyfacts fixture (S21) and a few `xbrl_fact` rows
written here. No request leaves the process.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import ConfigError, load_settings
from usfin.sources.edgar import companyfacts, concepts, frames

FIXTURES = Path(__file__).parent / "fixtures" / "edgar"
FRAME = FIXTURES / "frames" / "us-gaap_Revenues_USD_CY2025Q2.json"
FRAME_URL = "https://data.sec.gov/api/xbrl/frames/us-gaap/Revenues/USD/CY2025Q2.json"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"
T0 = datetime(2026, 10, 8, 14, 0, tzinfo=UTC).replace(tzinfo=None)  # naive UTC, as stored


@pytest.fixture
def settings(tmp_path, monkeypatch):
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
    monkeypatch.setattr(raw, "_utcnow", lambda: T0)
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        sec_user_agent=UA,
    )


class Fake:
    """Answers data.sec.gov/api/xbrl/frames/… with the fixture (404 for any other frame)."""

    def __init__(self):
        self.body = FRAME.read_text("utf-8")
        self.requests: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        if str(request.url) != FRAME_URL:
            return httpx.Response(404, text="<html>not found</html>")
        return httpx.Response(200, text=self.body, headers={"content-type": "application/json"})


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"^https://data\.sec\.gov/api/xbrl/frames/").mock(side_effect=f)
        yield f


@pytest.fixture
def con(settings):
    with store.session(settings.db_path, wait=0) as c:
        yield c


def _frame(con, settings, now, period="CY2025Q2", tag="Revenues"):
    with http.Client(con, settings=settings) as client:
        return frames.get_frame(
            con, client, taxonomy="us-gaap", tag=tag, unit="USD", period=period, now=now,
            raw_dir=settings.raw_dir,
        )  # fmt: skip


# --- frames ------------------------------------------------------------------------------------


def test_a_frame_is_fetched_once_and_parsed(con, settings, fake):
    frame = _frame(con, settings, T0)
    assert fake.requests == [FRAME_URL]
    assert not frame.cached
    assert (frame.taxonomy, frame.tag, frame.unit, frame.period) == (
        "us-gaap", "Revenues", "USD", "CY2025Q2"
    )  # fmt: skip
    assert frame.pts == 1939 and len(frame.points) == 3
    walmart = next(p for p in frame.points if p.cik == 104169)
    assert walmart == frames.Point(
        cik=104169, entity_name="WALMART INC.", loc="US-AR", accn="0000104169-26-000154",
        period_start=date(2025, 5, 1), period_end=date(2025, 7, 31), value=177402000000.0,
    )  # fmt: skip
    assert con.execute("SELECT key, fetched_at, raw_id FROM frames_cache").fetchall() == [
        ("us-gaap/Revenues/USD/CY2025Q2", T0, frame.raw_id)
    ]
    assert con.execute(
        "SELECT source, dataset, url FROM raw_response WHERE id = ?", [frame.raw_id]
    ).fetchall() == [("edgar", "edgar_frames", FRAME_URL)]


def test_within_24_hours_the_cache_answers_without_a_request(con, settings, fake):
    first = _frame(con, settings, T0)
    again = _frame(con, settings, T0 + timedelta(hours=23, minutes=59))
    assert fake.requests == [FRAME_URL]
    assert again.cached and again.raw_id == first.raw_id and again.points == first.points


def test_after_24_hours_the_frame_is_fetched_again_and_the_cache_row_moves(con, settings, fake):
    first = _frame(con, settings, T0)
    later = T0 + timedelta(hours=24)
    again = _frame(con, settings, later)
    assert fake.requests == [FRAME_URL, FRAME_URL]
    assert not again.cached and again.raw_id != first.raw_id
    assert con.execute("SELECT fetched_at, raw_id FROM frames_cache").fetchall() == [
        (later, again.raw_id)
    ]


def test_a_frame_never_touches_xbrl_fact(con, settings, fake):
    _frame(con, settings, T0)
    _frame(con, settings, T0 + timedelta(days=2))
    assert con.execute("SELECT count(*) FROM xbrl_fact").fetchone() == (0,)


def test_a_missing_frame_raises_and_is_not_cached(con, settings, fake):
    with pytest.raises(frames.FramesError, match="404"):
        _frame(con, settings, T0, tag="NoSuchTag")
    assert con.execute("SELECT count(*) FROM frames_cache").fetchone() == (0,)
    assert con.execute("SELECT count(*) FROM raw_response").fetchone() == (1,)  # archived


@pytest.mark.parametrize("period", ["CY2025Q2", "CY2025", "CY2025Q2I", "CY2025I"])
def test_frame_periods_are_accepted(period):
    assert frames.check_period(period) == period


@pytest.mark.parametrize("period", ["2025Q2", "CY2025Q5", "cy2025q2", "CY25Q2", "FY2025"])
def test_other_periods_are_rejected_before_any_request(period, con, settings, fake):
    with pytest.raises(ValueError, match="period"):
        _frame(con, settings, T0, period=period)
    assert fake.requests == []


def test_without_sec_user_agent_nothing_is_sent(con, settings, fake):
    no_ua = replace(settings, sec_user_agent=None)
    with http.Client(con, settings=no_ua) as client, pytest.raises(ConfigError):
        frames.get_frame(
            con, client, taxonomy="us-gaap", tag="Revenues", unit="USD",
            period="CY2025Q2", now=T0, raw_dir=settings.raw_dir,
        )  # fmt: skip
    assert fake.requests == []


# --- concept map -------------------------------------------------------------------------------


def test_the_repo_concept_map_loads_with_a_version_and_revenue_first_tags():
    cmap = concepts.load_concepts(REPO_CONFIG)
    assert cmap.version >= 1
    revenue = cmap["revenue"]
    assert (revenue.taxonomy, revenue.unit) == ("us-gaap", "USD")
    assert revenue.tags[:3] == (
        "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"
    )  # fmt: skip


@pytest.mark.parametrize(
    "text, match",
    [
        ("concepts: {revenue: {tags: [Revenues]}}\n", "version"),
        ("version: 1\nconcepts: {revenue: {tags: []}}\n", "tags"),
        ("version: 1\nconcepts: {revenue: {tags: [Revenues], colour: red}}\n", "colour"),
        ("version: 1\nconcepts: {revenue: {tags: [Revenues, Revenues]}}\n", "twice"),
        ("version: 1\nconcepts: {revenue: {taxonomy: ifrs-full, tags: [Revenue]}}\n", "taxonomy"),
    ],
)
def test_a_bad_concept_map_is_a_config_error(tmp_path, text, match):
    (tmp_path / "concepts.yaml").write_text(text, "utf-8")
    with pytest.raises(ConfigError, match=match):
        concepts.load_concepts(tmp_path)


def test_an_unknown_concept_is_a_config_error():
    with pytest.raises(ConfigError, match="profit"):
        concepts.load_concepts(REPO_CONFIG)["profit"]


# --- screen ------------------------------------------------------------------------------------


def _insert(con, cik, tag, value, *, frame="CY2025Q2", unit="USD", accn=None, filed=None,
            start=date(2025, 4, 1), end=date(2025, 6, 30), taxonomy="us-gaap"):  # fmt: skip
    con.execute(
        "INSERT INTO xbrl_fact VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 2025, 'Q2', '10-Q', ?, ?)",
        [cik, taxonomy, tag, unit, accn or f"{cik:010d}-25-000001", start, end, start == end,
         value, filed or date(2025, 8, 1), frame],
    )  # fmt: skip


REVENUE = concepts.Concept(
    name="revenue", taxonomy="us-gaap", unit="USD",
    tags=("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"),
)  # fmt: skip


def test_the_screen_picks_the_first_tag_each_company_reports_and_says_which(con):
    _insert(con, 1, "Revenues", 100.0)
    _insert(con, 1, "RevenueFromContractWithCustomerExcludingAssessedTax", 90.0)
    _insert(con, 2, "RevenueFromContractWithCustomerExcludingAssessedTax", 50.0)
    _insert(con, 2, "SalesRevenueNet", 49.0)
    _insert(con, 3, "SalesRevenueNet", 7.0)
    rows = concepts.screen(con, REVENUE, "CY2025Q2")
    assert [(r.cik, r.tag, r.value) for r in rows] == [
        (1, "Revenues", 100.0),
        (2, "RevenueFromContractWithCustomerExcludingAssessedTax", 50.0),
        (3, "SalesRevenueNet", 7.0),
    ]
    assert rows[0].period_start == date(2025, 4, 1) and rows[0].period_end == date(2025, 6, 30)


def test_the_screen_matches_the_frame_column_not_the_period_dates(con):
    _insert(con, 1, "Revenues", 100.0, frame=None)  # same dates, SEC put the frame elsewhere
    _insert(con, 1, "Revenues", 101.0, frame="CY2025Q2", accn="0000000001-26-000009",
            filed=date(2026, 8, 1))  # fmt: skip
    _insert(con, 2, "Revenues", 5.0, frame="CY2025Q1", start=date(2025, 1, 1),
            end=date(2025, 3, 31))  # fmt: skip
    _insert(con, 3, "Revenues", 6.0, frame="CY2025Q2", unit="CAD")
    _insert(con, 4, "Revenues", 7.0, frame="CY2025Q2", taxonomy="dei")
    _insert(con, 5, "OtherTag", 8.0)
    rows = concepts.screen(con, REVENUE, "CY2025Q2")
    assert [(r.cik, r.value, r.accn, r.filed) for r in rows] == [
        (1, 101.0, "0000000001-26-000009", date(2026, 8, 1))
    ]


def test_an_instant_period_needs_its_i_suffix(con):
    assets = concepts.Concept(name="assets", taxonomy="us-gaap", unit="USD", tags=("Assets",))
    d = date(2025, 6, 30)
    _insert(con, 1, "Assets", 10.0, frame="CY2025Q2I", start=d, end=d)
    assert concepts.screen(con, assets, "CY2025Q2") == []
    assert [r.value for r in concepts.screen(con, assets, "CY2025Q2I")] == [10.0]


def test_the_screen_reads_stored_companyfacts(con):
    doc = json.loads((FIXTURES / "companyfacts" / "CIK0000320193.json").read_text("utf-8"))
    companyfacts.store_document(con, doc, name="CIK0000320193.json")
    rows = concepts.screen(con, REVENUE, "CY2009")
    # Apple FY2009 as repeated in the 10-K of 2011, the filing SEC hangs frame CY2009 on
    assert [(r.cik, r.tag, r.value, r.form) for r in rows] == [
        (320193, "SalesRevenueNet", 42905000000.0, "10-K")
    ]


def test_the_screen_rejects_a_bad_period(con):
    with pytest.raises(ValueError, match="period"):
        concepts.screen(con, REVENUE, "2025Q2")


def test_the_screen_shows_the_company_name_when_known(con):
    _insert(con, 320193, "Revenues", 1.0)
    con.execute(
        "INSERT INTO edgar_company VALUES (320193, 'AAPL', 'Apple Inc.', 'AAPL', true, ?)", [T0]
    )
    _insert(con, 2, "Revenues", 2.0)
    names = {r.cik: r.name for r in concepts.screen(con, REVENUE, "CY2025Q2")}
    assert names == {320193: "Apple Inc.", 2: None}


# --- CLI ---------------------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    monkeypatch.setenv("SEC_USER_AGENT", UA)


def test_cli_edgar_frame_fetches_then_answers_from_the_cache(settings, fake, cli_env):
    args = ["edgar", "frame", "us-gaap", "Revenues", "USD", "CY2025Q2"]
    out = CliRunner().invoke(app, args)
    assert out.exit_code == 0, out.output
    assert "fetched" in out.output and "points 3 (pts 1939)" in out.output
    assert "WALMART INC." in out.output
    out = CliRunner().invoke(app, args)
    assert out.exit_code == 0, out.output
    assert "cached" in out.output
    assert fake.requests == [FRAME_URL]


def test_cli_edgar_frame_404_exits_1(settings, fake, cli_env):
    out = CliRunner().invoke(app, ["edgar", "frame", "us-gaap", "Nope", "USD", "CY2025Q2"])
    assert out.exit_code == 1
    assert "404" in out.output


def test_cli_edgar_screen_prints_the_tag_used_per_company(settings, fake, cli_env):
    with store.session(settings.db_path, wait=0) as c:
        _insert(c, 1, "Revenues", 100.0)
        _insert(c, 2, "SalesRevenueNet", 7.0)
    out = CliRunner().invoke(app, ["edgar", "screen", "revenue", "CY2025Q2"])
    assert out.exit_code == 0, out.output
    lines = out.output.splitlines()
    assert any("0000000001" in ln and "Revenues" in ln and "100" in ln for ln in lines)
    assert any("0000000002" in ln and "SalesRevenueNet" in ln for ln in lines)
    assert "companies 2" in out.output
    assert "Revenues 1" in out.output and "SalesRevenueNet 1" in out.output
    assert fake.requests == []


def test_cli_edgar_screen_unknown_concept_exits_1(settings, fake, cli_env):
    out = CliRunner().invoke(app, ["edgar", "screen", "profit", "CY2025Q2"])
    assert out.exit_code == 1
    assert "profit" in out.output
