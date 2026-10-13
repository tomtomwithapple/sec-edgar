"""Alpaca universe: the symbols the bars jobs request (view `alpaca_universe`; A3, How > Universe).

Built from the S2/S3 fixtures, loaded through the real jobs: the asset list
`tests/fixtures/alpaca/assets.json` (49 real assets, listed and OTC, active and inactive) and the
corporate-action responses in `tests/fixtures/alpaca/corp_actions/` (TWTR cash merger, ATAI
stock-and-cash merger, SIVBQ worthless removal, FB -> META name change, CTNT reverse split, AAPL
dividend and split). Fake keys only; no request leaves the process.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx
import yaml

from usfin import http, jobs, ratelimit, raw, store
from usfin.settings import load_settings
from usfin.sources.alpaca import assets, corp_actions

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
SOURCES = {"asset_active", "asset_inactive", "merger", "removal", "renamed"}
ACTIONS = (
    "twtr_2022-10.json",
    "atai_2026-09.json",
    "sivbq_2023-2026.json",
    "fb_meta_2022-06.json",
    "ctnt_2026-09.json",
    "aapl_2020-08.json",
)
D1, D2 = (datetime(2026, 10, d, 11, 30, tzinfo=UTC) for d in (12, 13))


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(D1)
    monkeypatch.setattr(raw, "_utcnow", c.naive)
    return c


@pytest.fixture
def settings(tmp_path, monkeypatch, clock):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
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
        apca_api_key_id=KEY_ID,
        apca_api_secret_key=SECRET,
    )


class Fake:
    """The asset list (`objects`, editable) and one corporate-actions page (`page`)."""

    def __init__(self):
        self.objects: list[dict] = json.loads((FIXTURES / "assets.json").read_text("utf-8"))
        self.page: bytes = (FIXTURES / "corp_actions" / "empty.json").read_bytes()

    @staticmethod
    def _json(content: bytes) -> httpx.Response:
        return httpx.Response(
            200, content=content, headers={"content-type": "application/json; charset=UTF-8"}
        )

    def assets(self, request: httpx.Request) -> httpx.Response:
        return self._json(json.dumps(self.objects).encode())

    def actions(self, request: httpx.Request) -> httpx.Response:
        return self._json(self.page)


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(assets.URL).mock(side_effect=f.assets)
        router.get(url__startswith=corp_actions.URL).mock(side_effect=f.actions)
        yield f


def _run(settings, clock, name, when=D1):
    reg = jobs.Registry()
    reg.register(assets.JOB)
    reg.register(corp_actions.JOB)
    clock.t = when
    result = jobs.run_job(name, settings=settings, registry=reg, clock=clock, chain=False)
    assert result.status == "ok", result


def _actions(settings, fake, clock, pages):
    for page in pages:
        fake.page = page
        _run(settings, clock, "alpaca_corp_actions")


def _fixture(name: str) -> bytes:
    return (FIXTURES / "corp_actions" / name).read_bytes()


@pytest.fixture
def loaded(settings, fake, clock):
    _run(settings, clock, "alpaca_assets")
    _actions(settings, fake, clock, [_fixture(n) for n in ACTIONS])
    return settings


def _universe(settings) -> list[tuple[str, str]]:
    with store.session(settings.db_path, wait=0) as con:
        return con.execute("SELECT symbol, source FROM alpaca_universe ORDER BY 1, 2").fetchall()


def _sources(settings, symbol) -> set[str]:
    return {s for sym, s in _universe(settings) if sym == symbol}


def test_listed_active_and_inactive_assets_with_their_status(loaded):
    assert _sources(loaded, "AAPL") == {"asset_active"}
    assert _sources(loaded, "SPY") == {"asset_active"}
    assert _sources(loaded, "RLJE") == {"asset_inactive"}
    assert _sources(loaded, "MFCB") == {"asset_inactive"}


def test_a_symbol_with_an_active_and_an_inactive_asset_id_has_both(loaded):
    assert _sources(loaded, "IBTA") == {"asset_active", "asset_inactive"}


def test_every_listed_exchange_of_the_config_and_no_otc_asset(loaded):
    exchanges = yaml.safe_load((REPO_CONFIG / "alpaca.yaml").read_text())["exchanges"]
    objects = json.loads((FIXTURES / "assets.json").read_text("utf-8"))
    listed = {o["symbol"] for o in objects if o["exchange"] in exchanges}
    otc = {o["symbol"] for o in objects if o["exchange"] == "OTC"}
    symbols = {sym for sym, s in _universe(loaded) if s.startswith("asset_")}
    assert {o["exchange"] for o in objects} - {"OTC"} == set(exchanges)
    assert symbols == listed
    assert not symbols & otc
    assert {"OTGLF", "OMCC", "CYTOF", "KUKEY", "NVOS"} <= otc


def test_merger_acquirees_are_in_and_acquirers_are_not(loaded):
    assert _sources(loaded, "TWTR") == {"merger"}  # cash merger; TWTR is not in the asset list
    assert _sources(loaded, "ATAI") == {"merger"}  # stock and cash merger
    assert _sources(loaded, "046CVR015") == set()  # ATAI's acquirer (a CVR)


def test_a_worthless_removal_symbol_is_in(loaded):
    assert _sources(loaded, "SIVBQ") == {"removal"}


def test_sivb_is_the_documented_survivorship_gap(loaded):
    assert _sources(loaded, "SIVB") == set()


def test_a_renamed_old_symbol_is_in_and_the_new_one_only_by_its_asset(loaded):
    assert _sources(loaded, "FB") == {"asset_active", "renamed"}  # the new FB (BATS) too
    assert _sources(loaded, "META") == {"asset_active"}


def test_other_action_types_add_no_symbol(loaded):
    assert _sources(loaded, "CTNT") == set()  # reverse split only


def test_an_action_symbol_known_only_as_an_otc_asset_is_out(settings, fake, clock):
    _run(settings, clock, "alpaca_assets")
    removal = _fixture("sivbq_2023-2026.json")
    _actions(settings, fake, clock, [
        removal.replace(b'"SIVBQ"', b'"NVOS"'),  # an OTC asset
        removal.replace(b'"SIVBQ"', b'"RLJE"').replace(b"70d10d5c", b"70d10d5d"),  # listed
        _fixture("twtr_2022-10.json").replace(b'"TWTR"', b'"OTGLF"'),  # an OTC asset
    ])  # fmt: skip
    assert _sources(settings, "NVOS") == set()
    assert _sources(settings, "OTGLF") == set()
    assert _sources(settings, "RLJE") == {"asset_inactive", "removal"}


def test_a_listed_asset_that_left_the_list_stays_in_as_inactive(settings, fake, clock):
    _run(settings, clock, "alpaca_assets")
    otc = next(o for o in fake.objects if o["symbol"] == "OTGLF")
    fake.objects = [o for o in fake.objects if o["symbol"] != "JBDI"]
    fake.objects.append(dict(otc, id="00000000-0000-4000-8000-0000000000aa", symbol="ZZZZF"))
    _run(settings, clock, "alpaca_assets", D2)  # 49 vs 49: diffed, JBDI's row closed
    assert _sources(settings, "JBDI") == {"asset_inactive"}


def test_rows_are_distinct_symbol_source_pairs_with_a_known_source(loaded):
    rows = _universe(loaded)
    assert rows and len(rows) == len(set(rows))
    assert {s for _, s in rows} == SOURCES
    assert all(sym for sym, _ in rows)


def test_the_view_follows_the_newest_action_version(settings, fake, clock):
    _run(settings, clock, "alpaca_assets")
    twtr = _fixture("twtr_2022-10.json")
    _actions(settings, fake, clock, [twtr])
    _run_later = twtr.replace(b'"TWTR"', b'"TWTX"')  # same id, a corrected symbol
    fake.page = _run_later
    _run(settings, clock, "alpaca_corp_actions", D2)
    assert _sources(settings, "TWTR") == set()
    assert _sources(settings, "TWTX") == {"merger"}


def test_an_empty_store_has_an_empty_universe(settings):
    assert _universe(settings) == []
