"""The daily bars job: which symbols a run asks for, catch-up, invalid symbols, schedule and
alerts (job `alpaca_bars`; A2, A9, How > Bars).

A run asks (a) every active universe symbol over watermark − `lookback_days` .. now and (b) up to
`new_symbol_cap` universe symbols whose whole history was never asked, from `backfill_start`
(active first, then by symbol). What the requests learnt about a symbol (`alpaca_bar_symbol`:
`invalid`, `history`) is rebuilt from the raw store by reparse.

The universe is written straight into `alpaca_asset_history` / `alpaca_corp_action` (the view is
tested in test_alpaca_universe.py). The fake answers each request from `Fake.held`, the bars it
holds per symbol, cut to the request's `start`..`end`; AAPL and MSFT 2026-10-01..10-16 are the
real bars of the S6 fixtures `multi_p{1,2}.json`. Fake keys only; no request leaves the process.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx
import yaml

from usfin import alerts, http, jobs, ratelimit, raw, store
from usfin.settings import load_settings
from usfin.sources.alpaca import bars

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca" / "bars"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
START = "2016-01-04"


def at(day: int, hour: int = 20, minute: int = 30) -> datetime:
    """`hour:minute` New York (EDT, UTC−4) on 2026-10-`day`."""
    return datetime(2026, 10, day, hour, minute, tzinfo=bars.ET).astimezone(UTC)


MON = at(19)  # Monday 2026-10-19 20:30 EDT, a NYSE day


def body(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _real() -> dict[str, list[dict]]:
    held: dict[str, list[dict]] = {}
    for name in ("multi_p1.json", "multi_p2.json"):
        for symbol, items in json.loads(body(name))["bars"].items():
            held.setdefault(symbol, []).extend(items)
    return held


def _bar(day: str, close: float = 10.0) -> dict:
    return {"t": f"{day}T04:00:00Z", "o": close, "h": close, "l": close, "c": close, "v": 100,
            "n": 10, "vw": close}  # fmt: skip


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.astimezone(UTC).replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(MON)
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
    s = replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        apca_api_key_id=KEY_ID,
        apca_api_secret_key=SECRET,
    )
    with store.session(s.db_path, wait=0) as con:
        store.migrate(con)
    return s


class Fake:
    """Answers from `held` cut to the request's dates, in one page; or serves `pages` in turn
    like test_alpaca_bars.Fake; a request naming a symbol in `invalid` gets that 400."""

    def __init__(self):
        self.held: dict[str, list[dict]] = _real()
        self.pages: list[bytes] | None = None
        self.invalid: set[str] = set()
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        params = request.url.params
        asked = params["symbols"].split(",")
        bad = [s for s in asked if s in self.invalid]
        status = 200
        if bad:
            content, status = json.dumps({"message": f"invalid symbol: {bad[0]}"}).encode(), 400
        elif self.pages is not None:
            token, i = params.get("page_token"), 0
            if token is not None:
                while json.loads(self.pages[i])["next_page_token"] != token:
                    i += 1
                i += 1
            content = self.pages[i]
        else:
            lo, hi = params["start"], params.get("end", "9999-12-31")
            cut = {
                s: [b for b in self.held.get(s, []) if lo <= bars.bar_date(b["t"]).isoformat() <= hi]
                for s in asked
            }
            content = json.dumps(
                {"bars": {s: v for s, v in cut.items() if v}, "next_page_token": None}
            ).encode()
        return httpx.Response(
            status, content=content, headers={"content-type": "application/json; charset=UTF-8"}
        )

    def asked(self) -> list[tuple[str, str, str | None]]:
        """(symbols, start, end) of every first-page request, in order."""
        return [
            (r.url.params["symbols"], r.url.params["start"], r.url.params.get("end"))
            for r in self.requests
            if "page_token" not in r.url.params
        ]


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__startswith=bars.URL).mock(side_effect=f)
        yield f


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _universe(settings, active=(), inactive=(), merged=()):
    """Listed assets (active / inactive) and cash-merger acquirees, as the universe reads them."""
    with store.session(settings.db_path, wait=0) as con:
        for status, symbols in (("active", active), ("inactive", inactive)):
            for s in symbols:
                con.execute(
                    "INSERT INTO alpaca_asset_history (id, symbol, name, exchange, status,"
                    " tradable, class, valid_from) VALUES (?, ?, ?, 'NASDAQ', ?, true,"
                    " 'us_equity', TIMESTAMP '2026-10-12 10:00')",
                    [f"id-{s}", s, s, status],
                )
        for s in merged:
            con.execute(
                "INSERT INTO alpaca_corp_action (id, type, acquiree_symbol, process_date, payload,"
                " first_seen_at, raw_id) VALUES (?, 'cash_merger', ?, DATE '2022-10-28', '{}',"
                " TIMESTAMP '2026-10-12 12:00', 0)",
                [f"ca-{s}", s],
            )


def _cap(settings, n: int) -> None:
    path = settings.config_dir / "alpaca.yaml"
    data = yaml.safe_load(path.read_text())
    data["new_symbol_cap"] = n
    path.write_text(yaml.safe_dump(data))


def _watermark(settings, value: str) -> None:
    with store.session(settings.db_path, wait=0) as con:
        jobs.set_watermark(con, "alpaca_bars", "date", value, today=date(2026, 10, 19))


def _history(settings, clock, fake, symbols, *, through: str | None = None):
    """Ask `symbols`' whole history the way the catch-up (and S8's backfill) does, at 20:30 on
    `through` with the bars Alpaca held then (default: now, all of them); the requests are then
    forgotten, so `fake.asked()` shows the job's only."""
    saved, held = clock.t, fake.held
    if through:
        clock.t = at(int(through[-2:]))
        fake.held = {s: [b for b in v if b["t"][:10] <= through] for s, v in held.items()}
    try:
        with store.session(settings.db_path, wait=0) as con:
            client = http.Client(con, settings=settings)
            try:
                raw_ids, _ = bars.fetch_batch(client, symbols, date.fromisoformat(START))
            finally:
                client.close()
            for raw_id in raw_ids:
                bars.store_page(con, raw_id, raw_dir=settings.raw_dir)
    finally:
        clock.t, fake.held = saved, held
    fake.requests.clear()


def _registry():
    reg = jobs.Registry()
    reg.register(bars.JOB)
    return reg


def _run(settings, clock, when=MON, *, ok=True):
    clock.t = when
    result = jobs.run_job(
        "alpaca_bars", settings=settings, registry=_registry(), clock=clock, chain=False
    )
    if ok:
        assert result.status == "ok", result
    return result


def _marks(settings, status):
    return [
        s
        for (s,) in _query(
            settings, "SELECT symbol FROM alpaca_bar_symbol WHERE status = ? ORDER BY 1", [status]
        )
    ]


def _wm(settings):
    return _query(
        settings, "SELECT value FROM watermark WHERE job = 'alpaca_bars' AND key = 'date'"
    )


def _alerts(settings):
    return _query(settings, "SELECT rule, subject FROM alert WHERE job = 'alpaca_bars' ORDER BY 1")


# --- which symbols a run asks for -----------------------------------------------------------


def test_a_run_refreshes_active_symbols_and_catches_up_the_rest_in_full(settings, fake, clock):
    _universe(settings, active=["AAPL", "MSFT", "NEWCO", "ZETA"], inactive=["GONE"],
              merged=["TWTR"])  # fmt: skip
    _history(settings, clock, fake, ["AAPL", "MSFT", "GONE"])
    _watermark(settings, "2026-10-16")
    _run(settings, clock)
    assert fake.asked() == [
        # (b) never asked in full: active first, then the rest, by symbol; from the SIP start
        ("NEWCO,ZETA,TWTR", START, None),
        # (a) the other active symbols: watermark 10-16 − lookback_days 5; never an `end`
        ("AAPL,MSFT", "2026-10-11", None),
    ]
    assert _marks(settings, "history") == ["AAPL", "GONE", "MSFT", "NEWCO", "TWTR", "ZETA"]


def test_the_catch_up_takes_at_most_new_symbol_cap_a_run_in_a_stable_order(settings, fake, clock):
    _cap(settings, 2)
    _universe(settings, inactive=["E", "D", "C", "B", "A"])
    _watermark(settings, "2026-10-16")
    for day in (19, 20, 21, 22):
        _run(settings, clock, at(day))
    assert fake.asked() == [("A,B", START, None), ("C,D", START, None), ("E", START, None)]


def test_a_symbol_with_no_bars_is_asked_in_full_once(settings, fake, clock):
    # a well-formed symbol Alpaca has no bars for (a right, a warrant, TWTR today) is absent
    # from the answer: once asked, it must not take a catch-up place every day
    _universe(settings, inactive=["XYZ.RT"], active=["AAPL"])
    _history(settings, clock, fake, ["AAPL"])
    _watermark(settings, "2026-10-16")
    _run(settings, clock, at(19))
    _run(settings, clock, at(20))
    assert fake.asked() == [
        ("XYZ.RT", START, None),
        ("AAPL", "2026-10-11", None),
        ("AAPL", "2026-10-11", None),
    ]
    assert _marks(settings, "history") == ["AAPL", "XYZ.RT"]


def test_a_symbol_caught_up_is_refreshed_daily_once_active(settings, fake, clock):
    fake.held["NEWCO"] = [_bar("2026-10-15"), _bar("2026-10-16"), _bar("2026-10-19")]
    _universe(settings, active=["NEWCO"])
    _watermark(settings, "2026-10-16")
    _run(settings, clock, at(19))
    _run(settings, clock, at(20))
    assert fake.asked() == [("NEWCO", START, None), ("NEWCO", "2026-10-14", None)]
    assert _query(settings, "SELECT count(*) FROM alpaca_bar WHERE symbol = 'NEWCO'") == [(3,)]


def test_with_no_watermark_the_daily_part_starts_lookback_days_before_today(
    settings, fake, clock
):
    # a first run must not turn every active symbol into a full backfill (S8 does that, a
    # batch per DB session)
    _universe(settings, active=["AAPL", "MSFT"])
    _history(settings, clock, fake, ["AAPL", "MSFT"])
    _run(settings, clock, at(19))
    assert fake.asked() == [("AAPL,MSFT", "2026-10-14", None)]
    assert _wm(settings) == [("2026-10-16",)]


def test_with_an_empty_universe_nothing_is_asked(settings, fake, clock):
    result = _run(settings, clock)
    assert result.rows_upserted == 0 and fake.requests == []


def test_a_missed_day_is_caught_up_by_the_lookback(settings, fake, clock):
    fake.held["AAPL"].append(_bar("2026-10-19", 250.0))
    _universe(settings, active=["AAPL"])
    _history(settings, clock, fake, ["AAPL"], through="2026-10-14")
    _watermark(settings, "2026-10-14")
    # no run on 10-15 or 10-16 (the Mac was off); Monday's run covers them
    result = _run(settings, clock, at(19))
    assert fake.asked() == [("AAPL", "2026-10-09", None)]
    assert result.rows_upserted == 3
    days = _query(
        settings, "SELECT date FROM alpaca_bar WHERE symbol = 'AAPL' AND date > '2026-10-14'"
        " ORDER BY 1"
    )  # fmt: skip
    assert days == [(date(2026, 10, 15),), (date(2026, 10, 16),), (date(2026, 10, 19),)]
    assert _wm(settings) == [("2026-10-19",)]


# --- invalid symbols ------------------------------------------------------------------------


def test_invalid_symbols_are_remembered_and_never_asked_again(settings, fake, clock):
    fake.invalid = {"005CVR014", "BF/B", "ABC-U"}
    _universe(settings, active=["AAPL", "ABC-U"], inactive=["005CVR014"], merged=["BF/B"])
    _history(settings, clock, fake, ["AAPL"])
    _watermark(settings, "2026-10-16")
    _run(settings, clock, at(19))
    assert fake.asked() == [
        ("ABC-U,005CVR014,BF/B", START, None),
        ("005CVR014,BF/B", START, None),
        ("BF/B", START, None),
        ("AAPL", "2026-10-11", None),
    ]
    assert _marks(settings, "invalid") == ["005CVR014", "ABC-U", "BF/B"]
    assert "ABC-U" not in _marks(settings, "history")
    fake.requests.clear()
    _run(settings, clock, at(20))
    assert fake.asked() == [("AAPL", "2026-10-11", None)]


def test_reparse_rebuilds_the_symbol_marks_with_no_request(settings, fake, clock):
    fake.invalid = {"005CVR014"}
    _universe(settings, active=["AAPL", "MSFT"], inactive=["005CVR014", "XYZ.RT"])
    _watermark(settings, "2026-10-16")
    _run(settings, clock, at(19))
    _run(settings, clock, at(20))
    sql = "SELECT symbol, status, first_seen_at, raw_id FROM alpaca_bar_symbol ORDER BY ALL"
    before = _query(settings, sql)
    assert {(s, st) for s, st, *_ in before} == {
        ("005CVR014", "invalid"), ("AAPL", "history"), ("MSFT", "history"),
        ("XYZ.RT", "history"),
    }  # fmt: skip
    bars_before = _query(settings, "SELECT * FROM alpaca_bar ORDER BY ALL")
    asked = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM alpaca_bar_symbol")
        jobs.reparse(con, bars.JOB, settings=settings)
    assert _query(settings, sql) == before
    assert _query(settings, "SELECT * FROM alpaca_bar ORDER BY ALL") == bars_before
    assert len(fake.requests) == asked


def test_no_symbol_status_is_stored_twice(settings, fake, clock):
    fake.invalid = {"005CVR014"}
    _universe(settings, active=["AAPL"], inactive=["005CVR014"])
    for day in (19, 20):
        _run(settings, clock, at(day))
    _history(settings, clock, fake, ["AAPL"])  # asked in full again (S8 backfill)
    fake.invalid = set()
    with store.session(settings.db_path, wait=0) as con:
        jobs.reparse(con, bars.JOB, settings=settings)
        jobs.reparse(con, bars.JOB, settings=settings)
    assert _query(
        settings, "SELECT symbol, status FROM alpaca_bar_symbol GROUP BY ALL HAVING count(*) > 1"
    ) == []
    assert _query(settings, "SELECT symbol, status FROM alpaca_bar_symbol ORDER BY ALL") == [
        ("005CVR014", "invalid"), ("AAPL", "history"),
    ]  # fmt: skip


# --- when a request counts as the whole history ---------------------------------------------


def _fetch(settings, symbols, start, end=None):
    with store.session(settings.db_path, wait=0) as con:
        client = http.Client(con, settings=settings)
        try:
            raw_ids, _ = bars.fetch_batch(client, symbols, start, end)
        finally:
            client.close()
        marks = []
        for raw_id in raw_ids:
            bars.store_page(con, raw_id, raw_dir=settings.raw_dir)
            marks.append(
                con.execute(
                    "SELECT count(*) FROM alpaca_bar_symbol WHERE status = 'history'"
                ).fetchone()[0]
            )
    return marks


def test_history_is_marked_on_the_last_page_only(settings, fake):
    fake.pages = [body("multi_p1.json"), body("multi_p2.json")]
    assert _fetch(settings, ["AAPL", "MSFT"], date(2016, 1, 4)) == [0, 2]


@pytest.mark.parametrize(
    ("start", "end"),
    [(date(2016, 1, 5), None), (date(2026, 10, 1), None), (date(2016, 1, 4), date(2026, 10, 16))],
)
def test_a_later_start_or_an_end_is_not_the_whole_history(settings, fake, start, end):
    assert _fetch(settings, ["AAPL", "MSFT"], start, end) == [0]


def test_an_earlier_start_than_backfill_start_is_the_whole_history(settings, fake):
    assert _fetch(settings, ["AAPL", "MSFT"], date(2015, 12, 28)) == [2]


# --- schedule and alerts (A9) ---------------------------------------------------------------


def test_schedule_entry_2030_on_nyse_days_with_a9_rules_and_no_value_jump():
    entry = yaml.safe_load((REPO_CONFIG / "schedule.yaml").read_text())["jobs"]["alpaca_bars"]
    assert entry["cron"] == "30 20 * * *"
    assert entry["calendar"] == "nyse"
    assert entry["due"] == "nyse"
    assert entry["alerts"] == {"zero_new_rows": {"after": "20:30"}, "stale_watermark": {"n": 1}}
    rules = alerts.load_rules(REPO_CONFIG)["alpaca_bars"]
    assert rules.params_for(bars.KEY) == {
        "zero_new_rows": {"after": "20:30"}, "stale_watermark": {"n": 1},
    }  # fmt: skip


def test_a_2030_run_with_no_new_bar_raises_zero_new_rows_and_stale_watermark(
    settings, fake, clock
):
    _universe(settings, active=["AAPL"])
    _history(settings, clock, fake, ["AAPL"], through="2026-10-15")
    _watermark(settings, "2026-10-15")
    fake.held = {"AAPL": [b for b in fake.held["AAPL"] if b["t"][:10] <= "2026-10-15"]}
    _run(settings, clock, at(19))
    # 10-16 and 10-19 are NYSE days after the watermark: more than n = 1
    assert _alerts(settings) == [("stale_watermark", "date"), ("zero_new_rows", "date")]


def test_a_run_before_2030_judges_no_zero_new_rows(settings, fake, clock):
    _universe(settings, active=["AAPL"])
    _history(settings, clock, fake, ["AAPL"])
    _watermark(settings, "2026-10-16")
    _run(settings, clock, at(19, 18, 0))  # a manual run at 18:00 brings nothing for 10-19
    assert _alerts(settings) == []


def test_a_2030_run_with_the_days_bar_raises_nothing(settings, fake, clock):
    fake.held["AAPL"].append(_bar("2026-10-19", 250.0))
    _universe(settings, active=["AAPL"])
    _history(settings, clock, fake, ["AAPL"], through="2026-10-16")
    _watermark(settings, "2026-10-16")
    _run(settings, clock, at(19))
    assert _alerts(settings) == []
    assert _wm(settings) == [("2026-10-19",)]


def test_the_job_reparses_both_tables():
    job = jobs.load_registry().get("alpaca_bars")
    assert job.versioned == ("alpaca_bar", "alpaca_bar_symbol")
    assert set(job.parsers) == {"alpaca_bars"}
