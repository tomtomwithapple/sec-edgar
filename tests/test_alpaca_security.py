"""Alpaca security identity: a price series per security, not per symbol (views `alpaca_security`,
`alpaca_security_symbol`, macro `alpaca_security_lookup`; A7, How > Identity).

The store is built through the real jobs from whole live responses, fetched 2026-10-19 with
`usfin http get` (archived as raw 883 and 884 of the worktree; sha256 of each body equals its
`raw_response.sha256`):

- `bars/five_2016.json.gz`  GET .../v2/stocks/bars?timeframe=1Day&feed=sip&adjustment=raw&asof=-
                            &limit=10000&symbols=FB,IBTA,META,METV,TWTR&start=2016-01-04, one page,
                            byte for byte (gzip only for size: 826 462 bytes, 7 850 bars). FB 1 950
                            (Meta to 2022-06-08, the new FB from 2025-06-26), META 1 331 (the
                            Roundhill ETF to 2022-01-28, a flat 12.31 with volume 0 to 06-08, then
                            Meta), METV 1 183, TWTR 1 718 (to 2022-10-27), IBTA 1 668 (the iShares
                            ETF from 2020-02-28, a flat 25.35 with volume 0 from late 2021, Ibotta
                            from its IPO on 2024-04-18)
- `corp_actions/five_2016-2026.json`  GET .../v1/corporate-actions?symbols=FB,IBTA,META,METV,TWTR
                            &start=2016-01-01&end=2026-10-19&limit=1000: the FB -> META and
                            META -> METV name changes, the TWTR cash merger, 32 cash dividends
                            (CUSIPs of Meta, METV, the new FB and the iShares IBTA)
- `assets.json` (S2)        the asset list: META, METV, the new FB, IBTA twice (one active and one
                            inactive id, both "Ibotta, Inc.")

Fake keys only; no request leaves the process.
"""

import gzip
import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import httpx
import pytest
import respx

from usfin import http, jobs, ratelimit, raw, store
from usfin.calendars import load_calendars
from usfin.settings import load_settings
from usfin.sources.alpaca import assets, bars, corp_actions, load_config

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
FIVE = ("FB", "IBTA", "META", "METV", "TWTR")
LAST = date(2026, 10, 16)  # the fixture's last session
GAP = load_config(REPO_CONFIG).gap_sessions
WHEN = datetime(2026, 10, 19, 5, 30, tzinfo=UTC)


def _bars_body() -> bytes:
    return gzip.decompress((FIXTURES / "bars" / "five_2016.json.gz").read_bytes())


def _fixture_bars() -> dict[str, list[dict]]:
    return json.loads(_bars_body())["bars"]


def _json(content: bytes) -> httpx.Response:
    return httpx.Response(
        200, content=content, headers={"content-type": "application/json; charset=UTF-8"}
    )


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> Path:
    return build(tmp_path_factory.mktemp("security"))


def build(tmp: Path) -> Path:
    """A DB holding the five symbols' bars, their corporate actions and the asset list, loaded
    by the real jobs (assets, corporate actions) and the bars job's fetch and store (also the
    store of `test_web_prices`)."""
    config = tmp / "config"
    shutil.copytree(REPO_CONFIG, config)
    with pytest.MonkeyPatch.context() as mp:
        for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
            mp.delenv(key, raising=False)
        mp.delenv("ALERT_WEBHOOK_URL", raising=False)
        mp.setattr(raw, "_utcnow", lambda: WHEN.replace(tzinfo=None))
        t = [0.0]
        mp.setattr(
            ratelimit,
            "LIMITERS",
            ratelimit.Limiters(clock=lambda: t[0], sleep=lambda s: t.__setitem__(0, t[0] + s)),
        )
        mp.setattr(http, "BACKOFF_BASE", 0.0)
        settings = replace(
            load_settings(env_file=tmp / "no.env"),
            db_path=tmp / "usfin.duckdb",
            raw_dir=tmp / "raw",
            config_dir=config,
            lock_wait=0,
            apca_api_key_id=KEY_ID,
            apca_api_secret_key=SECRET,
        )
        objects = (FIXTURES / "assets.json").read_bytes()
        actions = (FIXTURES / "corp_actions" / "five_2016-2026.json").read_bytes()
        with respx.mock(assert_all_called=True) as router:
            router.get(assets.URL).mock(return_value=_json(objects))
            router.get(url__startswith=corp_actions.URL).mock(return_value=_json(actions))
            router.get(url__startswith=bars.URL).mock(return_value=_json(_bars_body()))
            reg = jobs.Registry()
            reg.register(assets.JOB)
            reg.register(corp_actions.JOB)
            for name in ("alpaca_assets", "alpaca_corp_actions"):
                result = jobs.run_job(
                    name, settings=settings, registry=reg, clock=lambda: WHEN, chain=False
                )
                assert result.status == "ok", result
            with store.session(settings.db_path, wait=0) as con:
                client = http.Client(con, settings=settings)
                try:
                    raw_ids, invalid = bars.fetch_batch(client, FIVE, date(2016, 1, 4))
                finally:
                    client.close()
                assert invalid == []
                for raw_id in raw_ids:
                    bars.store_page(con, raw_id, raw_dir=settings.raw_dir)
    return settings.db_path


@pytest.fixture
def con(built):
    c = duckdb.connect(str(built), read_only=True)
    yield c
    c.close()


@pytest.fixture
def scratch(built, tmp_path):
    """A writable copy of the built DB, for tests that change bars or actions."""
    path = tmp_path / "usfin.duckdb"
    shutil.copy(built, path)
    c = duckdb.connect(str(path))
    yield c
    c.close()


def _lookup(con, symbol) -> list[tuple]:
    return con.execute(
        "SELECT symbol, valid_from, valid_to FROM alpaca_security_lookup(?) ORDER BY valid_from",
        [symbol],
    ).fetchall()


def _lookup_key(con, symbol) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "SELECT security_key FROM alpaca_security_lookup(?)", [symbol]
        ).fetchall()
    }


def _segments(con, symbol) -> list[tuple]:
    return con.execute(
        "SELECT symbol, valid_from, valid_to FROM alpaca_security WHERE symbol = ?"
        " ORDER BY valid_from",
        [symbol],
    ).fetchall()


def _key(con, symbol, valid_from) -> str:
    return con.execute(
        "SELECT security_key FROM alpaca_security WHERE symbol = ? AND valid_from = ?",
        [symbol, valid_from],
    ).fetchone()[0]


def _lookup_bars(con, symbol) -> list[tuple]:
    """The lookup joined to the bars, the way the adjusted prices and the UI read it."""
    return con.execute(
        "SELECT b.symbol, b.date FROM alpaca_security_lookup(?) s JOIN alpaca_bar_latest b"
        " ON b.symbol = s.symbol AND b.date BETWEEN s.valid_from AND s.valid_to ORDER BY b.date",
        [symbol],
    ).fetchall()


def _expected(*parts: tuple[str, date, date]) -> list[tuple]:
    fixture = _fixture_bars()
    out = []
    for symbol, lo, hi in parts:
        for bar in fixture[symbol]:
            d = bars.bar_date(bar["t"])
            if lo <= d <= hi:
                out.append((symbol, d))
    return sorted(out, key=lambda r: r[1])


# --- A7: lookups by today's symbol ---------------------------------------------------------------


def test_meta_is_fb_bars_to_the_rename_then_meta(con):
    assert _lookup(con, "META") == [
        ("FB", date(2016, 1, 4), date(2022, 6, 8)),
        ("META", date(2022, 6, 9), LAST),
    ]
    got = _lookup_bars(con, "META")
    assert got == _expected(("FB", date(2016, 1, 4), date(2022, 6, 8)),
                            ("META", date(2022, 6, 9), LAST))  # fmt: skip
    assert len(got) == len({d for _, d in got})  # one bar a day


def test_fb_is_only_the_new_fb_from_2025_06_26(con):
    assert _lookup(con, "FB") == [("FB", date(2025, 6, 26), LAST)]
    assert _lookup_bars(con, "FB") == _expected(("FB", date(2025, 6, 26), LAST))


def test_metv_is_metv_plus_the_roundhill_meta_bars(con):
    assert _lookup(con, "METV") == [
        ("META", date(2021, 6, 30), date(2022, 1, 28)),
        ("METV", date(2022, 1, 31), LAST),
    ]
    assert _lookup_bars(con, "METV") == _expected(
        ("META", date(2021, 6, 30), date(2022, 1, 28)), ("METV", date(2022, 1, 31), LAST)
    )


def test_twtr_is_twtr_to_its_merger(con):
    assert _lookup(con, "TWTR") == [("TWTR", date(2016, 1, 4), date(2022, 10, 27))]
    assert len(_lookup_bars(con, "TWTR")) == 1718


def test_ibta_two_asset_ids_with_one_name_are_one_security(con):
    ids = {
        r[0]
        for r in con.execute(
            "SELECT DISTINCT id FROM alpaca_asset_history WHERE symbol = 'IBTA'"
        ).fetchall()
    }
    assert len(ids) == 2
    assert _lookup(con, "IBTA") == [("IBTA", date(2024, 4, 18), LAST)]
    assert _lookup_key(con, "IBTA") == {f"asset:{min(ids)}"}  # the name group's smallest id
    keys = con.execute(
        "SELECT DISTINCT security_key FROM alpaca_security WHERE security_key LIKE 'asset:%'"
    ).fetchall()
    assert [k for (k,) in keys if k.removeprefix("asset:") in ids] == [f"asset:{min(ids)}"]


def test_an_unknown_symbol_finds_nothing(con):
    assert _lookup(con, "ZZZZ") == []


# --- How > Identity: segments and keys ----------------------------------------------------------


def test_the_flat_meta_stretch_between_the_renames_is_its_own_key(con):
    assert _segments(con, "META") == [
        ("META", date(2021, 6, 30), date(2022, 1, 28)),
        ("META", date(2022, 1, 31), date(2022, 6, 8)),
        ("META", date(2022, 6, 9), LAST),
    ]
    flat = _key(con, "META", date(2022, 1, 31))
    assert flat == "symbol:META:2022-01-31"
    assert con.execute(
        "SELECT count(*) FROM alpaca_security WHERE security_key = ?", [flat]
    ).fetchone()[0] == 1
    for symbol in ("META", "METV", "FB"):
        assert flat not in _lookup_key(con, symbol)


def test_keys_are_the_cusip_of_the_actions_when_there_is_one(con):
    assert _lookup_key(con, "META") == {"cusip:30303M102"}  # the rename's, then Meta's dividends
    assert _lookup_key(con, "METV") == {"cusip:53656F417"}
    assert _lookup_key(con, "FB") == {"cusip:74349Y613"}  # the new FB's dividends
    assert _lookup_key(con, "TWTR") == {"cusip:90184L102"}  # the merger's acquiree_cusip


def test_a_reused_symbol_splits_at_its_gap_and_its_quiet_stretch(con):
    assert _segments(con, "FB") == [
        ("FB", date(2016, 1, 4), date(2022, 6, 8)),
        ("FB", date(2025, 6, 26), LAST),
    ]
    assert _segments(con, "IBTA") == [
        ("IBTA", date(2020, 2, 28), date(2021, 11, 30)),  # the iShares ETF's last trade
        ("IBTA", date(2021, 12, 1), date(2024, 4, 17)),  # flat 25.35, volume 0
        ("IBTA", date(2024, 4, 18), LAST),  # Ibotta's IPO
    ]
    assert _key(con, "IBTA", date(2020, 2, 28)) == "cusip:46436E700"  # iShares dividends
    assert _key(con, "IBTA", date(2021, 12, 1)) == "symbol:IBTA:2021-12-01"
    assert _key(con, "IBTA", date(2024, 4, 18)).startswith("asset:")


def test_segments_cover_every_bar_once(con):
    rows = con.execute(
        "SELECT b.symbol, b.date, count(s.symbol) FROM alpaca_bar_latest b"
        " LEFT JOIN alpaca_security s ON s.symbol = b.symbol"
        " AND b.date BETWEEN s.valid_from AND s.valid_to GROUP BY ALL"
    ).fetchall()
    assert len(rows) == 7850
    assert {n for _, _, n in rows} == {1}
    assert con.execute(
        "SELECT count(*), count(DISTINCT (symbol, valid_from)) FROM alpaca_security"
    ).fetchone() == (10, 10)


def test_every_symbol_has_one_current_key(con):
    rows = con.execute(
        "SELECT symbol, security_key FROM alpaca_security_symbol ORDER BY symbol"
    ).fetchall()
    assert [s for s, _ in rows] == sorted(FIVE)
    assert dict(rows)["META"] == "cusip:30303M102"


def test_gap_sessions_matches_the_config(scratch):
    """A stretch of exactly `gap_sessions` NYSE sessions with no TWTR bar keeps one security;
    one session more splits it (the view writes the value; config/alpaca.yaml holds it)."""
    cal = load_calendars(REPO_CONFIG)
    first = date(2018, 3, 1)
    days = [first]
    while len(days) < GAP + 1:
        days.append(cal.next_day("nyse", days[-1]))
    scratch.execute(
        "DELETE FROM alpaca_bar WHERE symbol = 'TWTR' AND date BETWEEN ? AND ?",
        [days[0], days[GAP - 1]],
    )
    assert len(_segments(scratch, "TWTR")) == 1
    scratch.execute("DELETE FROM alpaca_bar WHERE symbol = 'TWTR' AND date = ?", [days[GAP]])
    segs = _segments(scratch, "TWTR")
    assert len(segs) == 2
    assert segs[0][2] == cal.prev_day("nyse", first)
    assert segs[1][1] == cal.next_day("nyse", days[GAP])
    assert _lookup(scratch, "TWTR") == [segs[1]]  # the merger's CUSIP names the later part
    assert _key(scratch, "TWTR", date(2016, 1, 4)) == "symbol:TWTR:2016-01-04"


def test_a_quiet_stretch_longer_than_gap_sessions_is_its_own_security(scratch):
    """Bars with volume 0 count as no trade: Alpaca pads a symbol it has no trades for with flat
    bars (META 2022-01-31..06-08, IBTA 2021..2024), so a gap is sessions without a traded bar."""
    cal = load_calendars(REPO_CONFIG)
    days = [date(2018, 3, 1)]
    while len(days) < GAP + 1:
        days.append(cal.next_day("nyse", days[-1]))
    scratch.execute(
        "UPDATE alpaca_bar SET volume = 0 WHERE symbol = 'TWTR' AND date BETWEEN ? AND ?",
        [days[0], days[GAP - 1]],
    )
    assert len(_segments(scratch, "TWTR")) == 1
    scratch.execute(
        "UPDATE alpaca_bar SET volume = 0 WHERE symbol = 'TWTR' AND date = ?", [days[GAP]]
    )
    segs = _segments(scratch, "TWTR")
    assert [(s[1], s[2]) for s in segs][1] == (days[0], days[GAP])
    assert len(segs) == 3
    assert len({_key(scratch, "TWTR", s[1]) for s in segs}) == 3


def test_gap_sessions_of_the_views_is_the_config(con):
    assert con.execute("SELECT alpaca_gap_sessions()").fetchone() == (GAP,)


def test_one_cusip_rejoins_a_security_split_by_a_gap(scratch):
    """Meta's bars with a hole longer than the gap in 2023: two META segments, no rename between
    them, but the rename's new CUSIP (first part) and Meta's dividends (second part) name both,
    so they stay one security."""
    cal = load_calendars(REPO_CONFIG)
    first, last = date(2023, 3, 1), date(2023, 3, 1)
    for _ in range(GAP):
        last = cal.next_day("nyse", last)
    scratch.execute(
        "DELETE FROM alpaca_bar WHERE symbol = 'META' AND date BETWEEN ? AND ?", [first, last]
    )
    assert _lookup(scratch, "META") == [
        ("FB", date(2016, 1, 4), date(2022, 6, 8)),
        ("META", date(2022, 6, 9), cal.prev_day("nyse", first)),
        ("META", cal.next_day("nyse", last), LAST),
    ]
    assert _lookup_key(scratch, "META") == {"cusip:30303M102"}


def test_a_lookup_reads_only_linked_symbols_and_equals_the_view(con):
    for symbol in FIVE:
        key = con.execute(
            "SELECT security_key FROM alpaca_security_symbol WHERE symbol = ?", [symbol]
        ).fetchone()[0]
        view = con.execute(
            "SELECT * FROM alpaca_security WHERE security_key = ? ORDER BY valid_from", [key]
        ).fetchall()
        got = con.execute(
            "SELECT * FROM alpaca_security_lookup(?) ORDER BY valid_from", [symbol]
        ).fetchall()
        assert got == view
    linked = con.execute(
        "SELECT DISTINCT symbol FROM alpaca_security_of(['FB', 'META', 'METV'])"
    ).fetchall()
    assert sorted(s for (s,) in linked) == ["FB", "META", "METV"]


def test_nyse_sessions_are_the_calendar(con):
    cal = load_calendars(REPO_CONFIG)
    rows = con.execute("SELECT date, n FROM nyse_session ORDER BY date").fetchall()
    assert [n for _, n in rows] == list(range(1, len(rows) + 1))
    got = [d for d, _ in rows]
    assert got[0] == date(2016, 1, 4) and got[-1].year == 2050
    want, d = [], date(2016, 1, 1)
    while d <= date(2050, 12, 31):
        if cal.is_nyse_day(d):
            want.append(d)
        d = date.fromordinal(d.toordinal() + 1)
    assert got == want


def test_an_empty_store_has_no_security(tmp_path):
    with store.session(tmp_path / "usfin.duckdb", wait=0) as c:
        assert c.execute("SELECT count(*) FROM alpaca_security").fetchone() == (0,)
        assert c.execute("SELECT count(*) FROM alpaca_security_symbol").fetchone() == (0,)
        assert c.execute("SELECT count(*) FROM alpaca_security_lookup('META')").fetchone() == (0,)
