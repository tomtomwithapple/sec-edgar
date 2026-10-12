"""Alpaca asset list: daily snapshot, diff and type-2 history (job `alpaca_assets`; A1, A4, A9).

`tests/fixtures/alpaca/assets.json` is GET https://paper-api.alpaca.markets/v2/assets
?asset_class=us_equity as fetched 2026-10-12 (33 564 assets, 14.6 MB, compact JSON), trimmed to
49 of its objects kept byte for byte in their original order: four per (exchange, status) pair
(NASDAQ, NYSE, ARCA, BATS, AMEX, OTC x active, inactive), AAPL, MSFT, KO, SPY, BRK.B, META, FB,
METV, both IBTA asset ids (one active, one inactive, same name); 16 of them have an empty name.
Later snapshots in these tests are that list edited: an object dropped, a status, name or
exchange changed. Fake keys only; no request leaves the process.
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

from usfin import alerts, http, jobs, ratelimit, raw, store
from usfin.settings import load_settings
from usfin.sources.alpaca import assets

FIXTURE = Path(__file__).parent / "fixtures" / "alpaca" / "assets.json"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
AAPL = "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415"
IBTA_ACTIVE = "8536ce46-9ded-45f3-805f-f103a1d1419f"
OTC = "eb4efa31-64e8-4a57-9b4b-cd5adc06b853"  # OTGLF, OTC inactive


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def naive(t: datetime) -> datetime:
    return t.replace(tzinfo=None)


D1, D2, D3, D4 = (utc(2026, 10, d, 11, 30) for d in (12, 13, 14, 15))  # 07:30 EDT


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return naive(self.t)


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
    """Answers the asset list with `objects` (the fixture, edited by the tests)."""

    def __init__(self):
        self.objects: list[dict] = json.loads(FIXTURE.read_text("utf-8"))
        self.body: bytes | None = FIXTURE.read_bytes()  # the recorded bytes until edited
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        body = self.body if self.body is not None else json.dumps(self.objects).encode()
        return httpx.Response(
            200, content=body, headers={"content-type": "application/json; charset=UTF-8"}
        )

    def drop(self, symbol: str) -> None:
        self.objects = [o for o in self.objects if o["symbol"] != symbol]
        self.body = None

    def set(self, asset_id: str, **changes) -> None:
        for o in self.objects:
            if o["id"] == asset_id:
                o.update(changes)
        self.body = None


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(assets.URL).mock(side_effect=f)
        yield f


def _registry():
    reg = jobs.Registry()
    reg.register(assets.JOB)
    return reg


def _run(settings, clock, when):
    clock.t = when
    result = jobs.run_job(
        "alpaca_assets", settings=settings, registry=_registry(), clock=clock, chain=False
    )
    assert result.status == "ok", result
    return result


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _snapshots(settings):
    return _query(
        settings, "SELECT snapshot_at, count(*) FROM alpaca_asset_snapshot GROUP BY 1 ORDER BY 1"
    )


def _history(settings):
    return _query(
        settings,
        "SELECT id, symbol, name, exchange, status, tradable, class, valid_from, valid_to"
        " FROM alpaca_asset_history ORDER BY id, valid_from",
    )


def _rows(settings, asset_id):
    return [r for r in _history(settings) if r[0] == asset_id]


def _diffs(settings):
    return _query(
        settings,
        "SELECT snapshot_at, rows, previous_rows, diffed, opened, closed FROM alpaca_asset_diff"
        " ORDER BY snapshot_at",
    )


# --- request and snapshot -------------------------------------------------------------------


def test_one_call_for_the_whole_us_equity_list_with_the_keys(settings, fake, clock):
    _run(settings, clock, D1)
    assert len(fake.requests) == 1
    req = fake.requests[0]
    assert req.url.host == "paper-api.alpaca.markets"
    assert req.url.path == "/v2/assets"
    assert dict(req.url.params) == {"asset_class": "us_equity"}  # no status: inactive too
    assert req.headers["APCA-API-KEY-ID"] == KEY_ID
    stored = _query(settings, "SELECT source, dataset, http_status FROM raw_response")
    assert stored == [("alpaca", "alpaca_assets", 200)]


def test_the_first_snapshot_keeps_the_tracked_columns_of_every_asset(settings, fake, clock):
    result = _run(settings, clock, D1)
    assert result.rows_upserted == 49
    assert _snapshots(settings) == [(naive(D1), 49)]  # snapshot_at = the response's fetched_at
    row = _query(
        settings,
        "SELECT symbol, name, exchange, status, tradable, class FROM alpaca_asset_snapshot"
        " WHERE id = ?",
        [AAPL],
    )
    assert row == [("AAPL", "Apple Inc. Common Stock", "NASDAQ", "active", True, "us_equity")]
    # OTC is kept, inactive is kept, an empty name is kept as written
    assert _query(
        settings, "SELECT symbol, exchange, status, tradable FROM alpaca_asset_snapshot"
        " WHERE id = ?", [OTC],
    ) == [("OTGLF", "OTC", "inactive", False)]  # fmt: skip
    assert _query(settings, "SELECT count(*) FROM alpaca_asset_snapshot WHERE name = ''") == [(16,)]
    # churning fields stay in the raw JSON only
    columns = {r[0] for r in _query(settings, "DESCRIBE alpaca_asset_snapshot")}
    assert columns == {
        "snapshot_at", "id", "symbol", "name", "exchange", "status", "tradable", "class",
        "raw_id",
    }  # fmt: skip
    assert _query(settings, "SELECT value FROM watermark WHERE job = 'alpaca_assets'") == [
        (naive(D1).isoformat(),)
    ]


def test_the_first_snapshot_opens_a_history_row_per_asset_id(settings, fake, clock):
    _run(settings, clock, D1)
    history = _history(settings)
    assert len(history) == 49
    assert all(r[7] == naive(D1) and r[8] is None for r in history)
    # IBTA: two asset ids under one symbol, two rows
    assert sorted(r[4] for r in history if r[1] == "IBTA") == ["active", "inactive"]
    assert _diffs(settings) == [(naive(D1), 49, None, True, 49, 0)]


def test_a_second_identical_snapshot_inserts_nothing(settings, fake, clock):
    _run(settings, clock, D1)
    built = _history(settings), _diffs(settings)
    result = _run(settings, clock, D2)
    assert result.rows_upserted == 0
    assert _snapshots(settings) == [(naive(D1), 49)]
    assert (_history(settings), _diffs(settings)) == built
    # the list was confirmed at D2: the watermark moves, so stale_watermark stays quiet
    assert _query(settings, "SELECT value FROM watermark WHERE job = 'alpaca_assets'") == [
        (naive(D2).isoformat(),)
    ]
    assert len(_query(settings, "SELECT id FROM raw_response")) == 2  # both archived


def test_a_changed_status_or_name_closes_and_opens_rows_by_id(settings, fake, clock):
    _run(settings, clock, D1)
    fake.set(IBTA_ACTIVE, status="inactive", tradable=False)
    fake.set(AAPL, name="Apple Inc.")
    result = _run(settings, clock, D2)
    assert result.rows_upserted == 49  # a changed list is a whole new snapshot
    assert _snapshots(settings) == [(naive(D1), 49), (naive(D2), 49)]
    assert [(r[2], r[7], r[8]) for r in _rows(settings, AAPL)] == [
        ("Apple Inc. Common Stock", naive(D1), naive(D2)),
        ("Apple Inc.", naive(D2), None),
    ]
    ibta = _rows(settings, IBTA_ACTIVE)
    assert [(r[4], r[5], r[7], r[8]) for r in ibta] == [
        ("active", True, naive(D1), naive(D2)),
        ("inactive", False, naive(D2), None),
    ]
    assert _diffs(settings)[-1] == (naive(D2), 49, 49, True, 2, 2)


def test_gone_new_and_moved_assets_close_and_open_rows(settings, fake, clock):
    _run(settings, clock, D1)
    original = json.loads(FIXTURE.read_text("utf-8"))
    rlje = next(o for o in original if o["symbol"] == "RLJE")["id"]
    fake.drop("RLJE")
    new = dict(
        next(o for o in original if o["id"] == AAPL), id="00000000-0000-4000-8000-0000000000aa"
    )
    fake.objects.append(dict(new, symbol="AAPX", name="A copy of AAPL under another id"))
    fake.set(AAPL, exchange="NYSE")
    _run(settings, clock, D2)  # 49 vs 49: diffed
    assert [(r[7], r[8]) for r in _rows(settings, rlje)] == [(naive(D1), naive(D2))]
    assert [(r[1], r[7], r[8]) for r in _rows(settings, new["id"])] == [("AAPX", naive(D2), None)]
    assert [(r[3], r[7], r[8]) for r in _rows(settings, AAPL)] == [
        ("NASDAQ", naive(D1), naive(D2)),
        ("NYSE", naive(D2), None),
    ]
    assert _diffs(settings)[-1] == (naive(D2), 49, 49, True, 2, 2)
    # an id that comes back opens a new row
    fake.objects.append(next(o for o in original if o["symbol"] == "RLJE"))
    fake.drop("AAPX")
    _run(settings, clock, D3)
    assert [(r[7], r[8]) for r in _rows(settings, rlje)] == [
        (naive(D1), naive(D2)),
        (naive(D3), None),
    ]


# --- the swing gate (C9) ----------------------------------------------------------------------


def test_a_swing_beyond_2_pct_is_stored_but_not_diffed_and_alerts(settings, fake, clock):
    _run(settings, clock, D1)
    before = _history(settings)
    fake.drop("OTGLF")
    fake.drop("RLJE")  # 49 -> 47: -4.08 %
    fake.set(AAPL, name="Apple Inc.")
    _run(settings, clock, D2)
    assert _snapshots(settings) == [(naive(D1), 49), (naive(D2), 47)]
    assert _history(settings) == before  # nothing closed, nothing renamed
    assert _diffs(settings)[-1] == (naive(D2), 47, 49, False, 0, 0)
    assert _query(settings, "SELECT rule, subject FROM alert") == [("row_count_swing", assets.KEY)]
    # the next snapshot within the gate of its predecessor diffs against the open rows (D1)
    _run(settings, clock, D3)  # same list as D2: identical, not stored
    fake.drop("SPY")  # 47 -> 46: -2.13 %: gated again
    _run(settings, clock, D4)
    assert _diffs(settings)[-1][3] is False


def test_the_snapshot_after_a_swing_diffs_against_the_last_diffed_one(settings, fake, clock):
    _run(settings, clock, D1)
    fake.drop("OTGLF")
    fake.drop("RLJE")
    _run(settings, clock, D2)  # gated
    fake.set(AAPL, name="Apple Inc.")
    _run(settings, clock, D3)  # 47 vs 47: diffed against the D1 history
    assert _diffs(settings)[-1] == (naive(D3), 47, 47, True, 1, 3)
    assert [(r[7], r[8]) for r in _rows(settings, OTC)] == [(naive(D1), naive(D3))]


def test_the_gate_uses_the_jobs_row_count_swing_pct(settings, fake, clock):
    path = settings.config_dir / "schedule.yaml"
    config = yaml.safe_load(path.read_text())
    config["jobs"]["alpaca_assets"]["alerts"]["row_count_swing"] = {"pct": 10}
    path.write_text(yaml.safe_dump(config))
    _run(settings, clock, D1)
    fake.drop("OTGLF")
    fake.drop("RLJE")  # -4.08 %, inside 10 %
    _run(settings, clock, D2)
    assert _diffs(settings)[-1] == (naive(D2), 47, 49, True, 0, 2)


# --- bad responses --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [b"[]", b'{"message": "forbidden"}', b'[{"id": "x", "symbol": "A"}]'],
    ids=["empty", "not-a-list", "missing-columns"],
)
def test_a_body_that_is_not_the_asset_list_fails_and_stores_nothing(settings, fake, clock, body):
    fake.body = body
    clock.t = D1
    result = jobs.run_job(
        "alpaca_assets", settings=settings, registry=_registry(), clock=clock, chain=False
    )
    assert result.status == "failed"
    assert _query(settings, "SELECT count(*) FROM alpaca_asset_snapshot") == [(0,)]
    assert _query(settings, "SELECT count(*) FROM raw_response") == [(1,)]  # archived anyway


# --- reparse (A1) ---------------------------------------------------------------------------


def _three_days(settings, fake, clock):
    _run(settings, clock, D1)
    fake.set(AAPL, name="Apple Inc.")
    _run(settings, clock, D2)
    _run(settings, clock, D3)  # identical to D2
    fake.drop("OTGLF")
    fake.drop("RLJE")
    _run(settings, clock, D4)  # gated


def test_reparse_rebuilds_snapshot_and_history_with_no_request(settings, fake, clock):
    _three_days(settings, fake, clock)
    built = _snapshots(settings), _history(settings), _diffs(settings)
    assert [s[0] for s in built[0]] == [naive(D1), naive(D2), naive(D4)]
    n = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        jobs.reparse(con, assets.JOB, settings=settings)
    assert (_snapshots(settings), _history(settings), _diffs(settings)) == built
    with store.session(settings.db_path, wait=0) as con:
        for table in ("alpaca_asset_history", "alpaca_asset_diff", "alpaca_asset_snapshot"):
            con.execute(f"DELETE FROM {table}")
        jobs.reparse(con, assets.JOB, settings=settings)
    assert (_snapshots(settings), _history(settings), _diffs(settings)) == built
    assert len(fake.requests) == n


# --- schedule and alert rules (A9) ----------------------------------------------------------


def test_schedule_entry_daily_0730_with_swing_and_stale_rules():
    entry = yaml.safe_load((REPO_CONFIG / "schedule.yaml").read_text())["jobs"]["alpaca_assets"]
    assert entry["cron"] == "30 7 * * *"
    assert entry["calendar"] == "any"
    assert entry["alerts"] == {"row_count_swing": {"pct": 2}, "stale_watermark": {"n": 1}}
    rules = alerts.load_rules(REPO_CONFIG)["alpaca_assets"]
    assert rules.params_for(assets.KEY)["row_count_swing"] == {"pct": 2}


def test_the_job_is_in_the_registry_and_its_raw_dataset_is_parsed():
    registry = jobs.load_registry()
    assert "alpaca_assets" in registry
    job = registry.get("alpaca_assets")
    assert set(job.parsers) == {"alpaca_assets"}
    assert job.key_list(None) == [assets.KEY]
