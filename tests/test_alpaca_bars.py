"""Alpaca daily bars: request, versioned insert, as-of (job `alpaca_bars`; A1, A2).

Fixtures in `tests/fixtures/alpaca/bars/` are whole responses of GET
https://data.alpaca.markets/v2/stocks/bars?timeframe=1Day&feed=sip&adjustment=raw&asof=- as
fetched 2026-10-19, byte for byte:

- `aapl_2016-01.json`     AAPL 2015-12-28..2016-01-06: the SIP window starts 2016-01-04; `t` at
                          05:00Z (EST)
- `aapl_2020-08.json`     AAPL 2020-08-26..09-02: `t` at 04:00Z (EDT); the raw 4:1 split
                          (08-28 close 499.23, 08-31 close 129.04)
- `multi_p{1,2}.json`     AAPL,MSFT 2026-10-01..10-16 with `limit=15`: page 1 holds MSFT's first
                          3 bars and AAPL's 12 and a `next_page_token`, page 2 MSFT's other 9
- `meta_fb_2022.json`     META,FB,METV 2022-01-26..02-02 with `asof=-`: FB is Meta, META the
                          Roundhill ETF (flat 12.31 from 01-28), METV from its rename on 01-31
- `invalid_symbol.json`   the 400 for a batch holding `005CVR014`
- `empty.json`            a window with no bars

Fake keys only; no request leaves the process.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx

from usfin import http, jobs, ratelimit, raw, store
from usfin.settings import load_settings
from usfin.sources.alpaca import bars

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca" / "bars"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
D1, D2, D3 = (datetime(2026, 10, d, 0, 30, tzinfo=UTC) for d in (17, 20, 21))  # 20:30 EDT
D2B = datetime(2026, 10, 20, 0, 31, tzinfo=UTC)


def body(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def naive(t: datetime) -> datetime:
    return t.replace(tzinfo=None)


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
    """Answers by request: `pages` (first page, then the page whose predecessor's
    `next_page_token` was sent); a request naming a symbol in `invalid` gets that 400."""

    def __init__(self):
        self.pages: list[bytes] = [body("empty.json")]
        self.invalid: set[str] = set()
        self.status = 200
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        asked = request.url.params["symbols"].split(",")
        bad = [s for s in asked if s in self.invalid]
        if bad:
            content, status = json.dumps({"message": f"invalid symbol: {bad[0]}"}).encode(), 400
        else:
            token = request.url.params.get("page_token")
            i = 0
            if token is not None:
                while json.loads(self.pages[i])["next_page_token"] != token:
                    i += 1
                i += 1
            content, status = self.pages[i], self.status
        return httpx.Response(
            status, content=content, headers={"content-type": "application/json; charset=UTF-8"}
        )


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__startswith=bars.URL).mock(side_effect=f)
        yield f


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _load(settings, symbols, start=date(2016, 1, 4), end=date(2026, 10, 16)):
    """Fetch and store `symbols` the way the job and backfill do; return (raw ids, invalid)."""
    with store.session(settings.db_path, wait=0) as con:
        store.migrate(con)
        client = http.Client(con, settings=settings)
        try:
            raw_ids, invalid = bars.fetch_batch(client, symbols, start, end)
        finally:
            client.close()
        for raw_id in raw_ids:
            bars.store_page(con, raw_id, raw_dir=settings.raw_dir)
    return raw_ids, invalid


def _registry():
    reg = jobs.Registry()
    reg.register(bars.JOB)
    return reg


def _run(settings, clock, when, *, ok=True):
    clock.t = when
    result = jobs.run_job(
        "alpaca_bars", settings=settings, registry=_registry(), clock=clock, chain=False
    )
    if ok:
        assert result.status == "ok", result
    return result


def _bars(settings, symbol):
    return _query(
        settings,
        "SELECT date, open, high, low, close, volume, trade_count, vwap FROM alpaca_bar"
        " WHERE symbol = ? ORDER BY date, first_seen_at",
        [symbol],
    )


def _all(settings):
    return _query(
        settings,
        "SELECT symbol, date, close, first_seen_at, raw_id FROM alpaca_bar"
        " ORDER BY symbol, date, first_seen_at",
    )


def _edit(name, symbol, day, **change) -> bytes:
    data = json.loads(body(name))
    for bar in data["bars"][symbol]:
        if bar["t"].startswith(day):
            bar.update(change)
    return json.dumps(data).encode()


# --- request --------------------------------------------------------------------------------


def test_the_request_pins_sip_raw_asof_and_the_page_limit(settings, fake):
    fake.pages = [body("aapl_2016-01.json")]
    _load(settings, ["AAPL", "MSFT"], date(2016, 1, 4), date(2026, 10, 16))
    (req,) = fake.requests
    assert req.url.host == "data.alpaca.markets"
    assert req.url.path == "/v2/stocks/bars"
    assert dict(req.url.params) == {
        "timeframe": "1Day", "feed": "sip", "adjustment": "raw", "asof": "-", "limit": "10000",
        "symbols": "AAPL,MSFT", "start": "2016-01-04", "end": "2026-10-16",
    }  # fmt: skip
    assert req.headers["APCA-API-KEY-ID"] == KEY_ID
    assert _query(settings, "SELECT source, dataset, http_status FROM raw_response") == [
        ("alpaca", "alpaca_bars", 200)
    ]


def test_pages_are_followed_by_next_page_token_and_a_symbol_split_across_them(settings, fake):
    fake.pages = [body("multi_p1.json"), body("multi_p2.json")]
    raw_ids, _ = _load(settings, ["AAPL", "MSFT"])
    assert len(raw_ids) == 2
    first, second = (dict(r.url.params) for r in fake.requests)
    assert second == {**first, "page_token": json.loads(fake.pages[0])["next_page_token"]}
    assert _query(
        settings, "SELECT symbol, count(*), count(DISTINCT raw_id) FROM alpaca_bar GROUP BY 1"
        " ORDER BY 1",
    ) == [("AAPL", 12, 1), ("MSFT", 12, 2)]  # fmt: skip


def test_batches_split_symbols_in_order_without_repeats():
    assert bars.batches(["A", "B", "A", "C", "D", "E"], 2) == [["A", "B"], ["C", "D"], ["E"]]
    assert bars.batches([], 100) == []


def test_an_invalid_symbol_is_dropped_and_the_batch_asked_again(settings, fake):
    fake.pages = [body("aapl_2016-01.json")]
    fake.invalid = {"005CVR014", "BF/B"}
    raw_ids, invalid = _load(settings, ["AAPL", "005CVR014", "BF/B"])
    assert invalid == ["005CVR014", "BF/B"]
    assert [r.url.params["symbols"] for r in fake.requests] == [
        "AAPL,005CVR014,BF/B", "AAPL,BF/B", "AAPL",
    ]  # fmt: skip
    # the 400s stay archived (raw first) and parse to nothing
    assert _query(settings, "SELECT http_status FROM raw_response ORDER BY id") == [
        (400,),
        (400,),
        (200,),
    ]
    assert len(raw_ids) == 3
    assert _query(settings, "SELECT count(*) FROM alpaca_bar") == [(3,)]


def test_the_invalid_symbol_message_is_read_from_the_real_400():
    assert bars.invalid_symbol(400, body("invalid_symbol.json")) == "005CVR014"
    assert bars.invalid_symbol(200, body("invalid_symbol.json")) is None
    assert bars.invalid_symbol(400, b'{"message":"forbidden"}') is None
    assert bars.invalid_symbol(400, b"not json") is None


def test_a_batch_of_only_invalid_symbols_stores_nothing_and_does_not_fail(settings, fake):
    fake.invalid = {"09175M705"}
    raw_ids, invalid = _load(settings, ["09175M705"])
    assert invalid == ["09175M705"] and len(raw_ids) == 1


@pytest.mark.parametrize("status", [403, 422])
def test_any_other_non_200_fails_the_batch(settings, fake, status):
    fake.status = status
    with pytest.raises(bars.BarsError, match=str(status)):
        _load(settings, ["AAPL"])


def test_a_5xx_is_retried_by_the_client_and_then_fails(settings, fake):
    fake.status = 500
    with pytest.raises(http.HttpError, match="500"):
        _load(settings, ["AAPL"])
    assert len(fake.requests) == http.MAX_TRIES


@pytest.mark.parametrize("bad", [b"<html>", b'{"message":"x"}', b'{"bars":[]}'])
def test_a_body_that_is_not_the_bar_list_fails(settings, fake, bad):
    fake.pages = [bad]
    with pytest.raises(bars.BarsError):
        _load(settings, ["AAPL"])


def test_a_fractional_volume_fails_loudly():
    data = json.loads(body("aapl_2016-01.json"))
    data["bars"]["AAPL"][0]["v"] = 1.5
    with pytest.raises(bars.BarsError, match="whole number"):
        bars.parse(json.dumps(data).encode())


# --- rows -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("t", "day"),
    [
        ("2016-01-04T05:00:00Z", date(2016, 1, 4)),  # EST
        ("2020-08-31T04:00:00Z", date(2020, 8, 31)),  # EDT
        ("2026-03-09T04:00:00Z", date(2026, 3, 9)),  # first Monday after the switch to EDT
        ("2026-11-02T05:00:00Z", date(2026, 11, 2)),  # first Monday back on EST
    ],
)
def test_t_is_the_new_york_midnight_of_the_bar_date(t, day):
    assert bars.bar_date(t) == day


def test_bars_load_unadjusted_with_every_column(settings, fake):
    fake.pages = [body("aapl_2016-01.json")]
    _load(settings, ["AAPL"])
    assert _bars(settings, "AAPL")[0] == (
        date(2016, 1, 4),
        102.61,
        105.368,
        102.0,
        105.35,
        71935339,
        351452,
        104.00725,
    )
    assert [d for d, *_ in _bars(settings, "AAPL")] == [
        date(2016, 1, 4),
        date(2016, 1, 5),
        date(2016, 1, 6),
    ]


def test_the_2020_split_is_stored_raw(settings, fake):
    fake.pages = [body("aapl_2020-08.json")]
    _load(settings, ["AAPL"])
    close = {d: c for d, _, _, _, c, *_ in _bars(settings, "AAPL")}
    assert close[date(2020, 8, 28)] == 499.23
    assert close[date(2020, 8, 31)] == 129.04


def test_asof_dash_keeps_each_symbol_as_it_traded_then(settings, fake):
    fake.pages = [body("meta_fb_2022.json")]
    _load(settings, ["META", "FB", "METV"])
    close = {
        (s, d): c for s, d, c in _query(settings, "SELECT symbol, date, close FROM alpaca_bar")
    }
    assert close[("FB", date(2022, 1, 31))] == 313.26  # Meta, then under FB
    assert close[("META", date(2022, 1, 31))] == 12.31  # the Roundhill ETF's flat close
    assert ("METV", date(2022, 1, 28)) not in close  # METV starts at its rename
    assert close[("METV", date(2022, 1, 31))] == 13


def test_first_seen_at_is_the_responses_fetched_at(settings, fake, clock):
    fake.pages = [body("aapl_2016-01.json")]
    _load(settings, ["AAPL"])
    assert _query(settings, "SELECT DISTINCT first_seen_at FROM alpaca_bar") == [(naive(D1),)]
    assert _query(
        settings, "SELECT DISTINCT b.raw_id = r.id FROM alpaca_bar b, raw_response r"
    ) == [(True,)]


# --- versions -------------------------------------------------------------------------------


def test_an_identical_refetch_inserts_nothing(settings, fake, clock):
    fake.pages = [body("multi_p1.json"), body("multi_p2.json")]
    _load(settings, ["AAPL", "MSFT"])
    before = _all(settings)
    clock.t = D2
    _load(settings, ["AAPL", "MSFT"])
    assert _all(settings) == before
    assert _query(settings, "SELECT count(*) FROM raw_response") == [(4,)]


@pytest.mark.parametrize(
    ("field", "value"),
    [("o", 1.0), ("h", 1.0), ("l", 1.0), ("c", 1.0), ("v", 1), ("n", 1), ("vw", 1.0), ("vw", None)],
)
def test_a_change_in_any_compared_column_inserts_a_version(settings, fake, clock, field, value):
    fake.pages = [body("aapl_2016-01.json")]
    _load(settings, ["AAPL"])
    clock.t = D2
    fake.pages = [_edit("aapl_2016-01.json", "AAPL", "2016-01-05", **{field: value})]
    _load(settings, ["AAPL"])
    rows = _all(settings)
    assert len(rows) == 4
    changed = [r for r in rows if r[1] == date(2016, 1, 5)]
    assert [r[3] for r in changed] == [naive(D1), naive(D2)]


def test_a_value_that_changes_back_is_a_third_version(settings, fake, clock):
    for when, page in [
        (D1, body("aapl_2016-01.json")),
        (D2, _edit("aapl_2016-01.json", "AAPL", "2016-01-05", c=1.0)),
        (D3, body("aapl_2016-01.json")),
    ]:
        clock.t = when
        fake.pages = [page]
        _load(settings, ["AAPL"])
    closes = _query(
        settings,
        "SELECT close, first_seen_at FROM alpaca_bar WHERE date = '2016-01-05' ORDER BY 2",
    )
    assert closes == [(102.71, naive(D1)), (1.0, naive(D2)), (102.71, naive(D3))]
    assert _query(settings, "SELECT close FROM alpaca_bar_latest WHERE date = '2016-01-05'") == [
        (102.71,)
    ]


def test_latest_and_as_of(settings, fake, clock):
    fake.pages = [body("aapl_2016-01.json")]
    _load(settings, ["AAPL"])
    clock.t = D2
    fake.pages = [_edit("aapl_2016-01.json", "AAPL", "2016-01-05", c=103.0)]
    _load(settings, ["AAPL"])

    def as_of(arg):
        return _query(
            settings,
            f"SELECT date, close FROM alpaca_bar_as_of({arg}) ORDER BY date",
        )

    first = [(date(2016, 1, 4), 105.35), (date(2016, 1, 5), 102.71), (date(2016, 1, 6), 100.7)]
    second = [first[0], (date(2016, 1, 5), 103.0), first[2]]
    assert as_of(f"TIMESTAMP '{naive(D1)}'") == first
    assert as_of(f"TIMESTAMP '{naive(D2)}'") == second
    # a DATE means 23:59:59 New York that day: D1 is 2026-10-16 20:30 EDT, D2 2026-10-19
    assert as_of("DATE '2026-10-16'") == first
    assert as_of("DATE '2026-10-19'") == second
    assert as_of("DATE '2026-10-15'") == []
    assert _query(settings, "SELECT date, close FROM alpaca_bar_latest ORDER BY date") == second


def test_no_duplicate_symbol_date_first_seen_after_any_sequence_of_runs(settings, fake, clock):
    sequence = [
        (D1, [body("multi_p1.json"), body("multi_p2.json")]),
        (D1, [body("multi_p1.json"), body("multi_p2.json")]),  # same instant again
        (D2, [_edit("multi_p1.json", "AAPL", "2026-10-02", c=1.0), body("multi_p2.json")]),
        (D2B, [body("multi_p1.json"), body("multi_p2.json")]),
        (D3, [body("multi_p1.json"), _edit("multi_p2.json", "MSFT", "2026-10-16", v=1)]),
        (D3, [body("multi_p1.json"), _edit("multi_p2.json", "MSFT", "2026-10-16", v=1)]),
    ]
    for when, pages in sequence:
        clock.t = when
        fake.pages = pages
        _load(settings, ["AAPL", "MSFT"])
    assert (
        _query(
            settings,
            "SELECT symbol, date, first_seen_at, count(*) FROM alpaca_bar GROUP BY ALL"
            " HAVING count(*) > 1",
        )
        == []
    )
    # versions: 24 bars + AAPL 10-02 changed and changed back (+2) + MSFT 10-16 once (+1)
    assert _query(settings, "SELECT count(*) FROM alpaca_bar") == [(27,)]


def test_the_table_has_no_primary_key(settings):
    with store.session(settings.db_path, wait=0) as con:
        store.migrate(con)
        assert con.execute(
            "SELECT count(*) FROM duckdb_constraints() WHERE table_name = 'alpaca_bar'"
            " AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')"
        ).fetchone() == (0,)


# --- job and reparse ------------------------------------------------------------------------


def _active(settings, *symbols):
    """Listed active assets, so the universe (and the daily job) holds them; see
    test_alpaca_bars_job.py for which symbols a run asks."""
    with store.session(settings.db_path, wait=0) as con:
        for s in symbols:
            con.execute(
                "INSERT INTO alpaca_asset_history (id, symbol, exchange, status, valid_from)"
                " VALUES (?, ?, 'NASDAQ', 'active', TIMESTAMP '2026-10-12 10:00')",
                [f"id-{s}", s],
            )


def test_the_watermark_never_moves_back(settings, fake, clock):
    fake.pages = [body("multi_p1.json"), body("multi_p2.json")]
    _load(settings, ["AAPL", "MSFT"])
    _active(settings, "AAPL", "MSFT")
    _run(settings, clock, D2)
    fake.pages = [body("aapl_2020-08.json")]
    _run(settings, clock, D3)
    assert _query(
        settings, "SELECT value FROM watermark WHERE job = 'alpaca_bars' AND key = 'date'"
    ) == [("2026-10-16",)]


def test_reparse_rebuilds_every_version_with_no_request(settings, fake, clock):
    fake.pages = [body("aapl_2016-01.json")]
    _load(settings, ["AAPL"])
    clock.t = D2
    fake.pages = [_edit("aapl_2016-01.json", "AAPL", "2016-01-05", c=103.0)]
    _load(settings, ["AAPL"])
    fake.invalid = {"005CVR014"}
    _load(settings, ["005CVR014"])
    before = _all(settings)
    asked = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM alpaca_bar")
        jobs.reparse(con, bars.JOB, settings=settings)
    assert _all(settings) == before
    assert len(fake.requests) == asked


def test_the_job_is_in_the_registry_and_its_raw_dataset_is_parsed():
    registry = jobs.load_registry()
    assert "alpaca_bars" in registry
    assert set(registry.get("alpaca_bars").parsers) == {"alpaca_bars"}
    assert registry.get("alpaca_bars").versioned == ("alpaca_bar", "alpaca_bar_symbol")
