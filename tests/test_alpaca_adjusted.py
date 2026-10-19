"""Alpaca adjusted prices: split and total-return factors per security, chained across renames
(view `alpaca_bar_adjusted`, macro `alpaca_bar_adjusted_lookup`; A6, How > Adjusted prices).

The store is built by the corporate-actions and bars jobs' fetch and store from whole live
responses, byte for byte:

- `bars/five_2016.json.gz`, `corp_actions/five_2016-2026.json` (S9): FB, IBTA, META, METV, TWTR
  from 2016 with `asof=-`; the FB -> META and META -> METV renames, Meta's 11 dividends (all paid
  under META, from 2024), the new FB's and METV's and the iShares IBTA's dividends
- `bars/aapl_2020-08-03..09-04.json`  GET .../v2/stocks/bars?timeframe=1Day&feed=sip
                            &adjustment=raw&asof=-&limit=10000&symbols=AAPL&start=2020-08-03
                            &end=2020-09-04, fetched 2026-10-19 with `usfin http get` (raw 886 of
                            the worktree, sha256 equal): 25 bars across the 0.82 dividend (ex
                            2020-08-07) and the 4:1 split (ex 2020-08-31; 499.23 -> 129.04)
- `corp_actions/aapl_2020-08.json` (S3): that dividend and that split
- `bars/ctnt_2026-09.json`  the same request for CTNT 2026-09-14..2026-10-02 (raw 887): 15 bars
                            across its 1:150 reverse split (0.0315 -> 3.60 on 2026-09-28)
- `corp_actions/ctnt_2026-09.json` (S3): that reverse split

Fake keys only; no request leaves the process, except in the live test (`-m live`), which reads
the worktree DB and asks Alpaca for `adjustment=all` directly (not archived).
"""

import gzip
import json
import math
import shutil
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime
from itertools import pairwise
from pathlib import Path

import duckdb
import httpx
import pytest
import respx

from usfin import http, ratelimit, raw, store
from usfin.settings import load_settings
from usfin.sources.alpaca import bars, corp_actions

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
FIVE = ("FB", "IBTA", "META", "METV", "TWTR")
WHEN = datetime(2026, 10, 19, 5, 30, tzinfo=UTC)
COLUMNS = "security_key, date, symbol, close, adj_close, split_factor, tr_factor"

BAR_BODIES = {
    FIVE: gzip.decompress((FIXTURES / "bars" / "five_2016.json.gz").read_bytes()),
    ("AAPL",): (FIXTURES / "bars" / "aapl_2020-08-03..09-04.json").read_bytes(),
    ("CTNT",): (FIXTURES / "bars" / "ctnt_2026-09.json").read_bytes(),
}
ACTION_FILES = ("five_2016-2026.json", "aapl_2020-08.json", "ctnt_2026-09.json")


def _closes(symbol: str) -> dict[date, float]:
    for body in BAR_BODIES.values():
        found = json.loads(body)["bars"].get(symbol)
        if found:
            return {bars.bar_date(b["t"]): b["c"] for b in found}
    raise KeyError(symbol)


def _json(content: bytes) -> httpx.Response:
    return httpx.Response(
        200, content=content, headers={"content-type": "application/json; charset=UTF-8"}
    )


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> Path:
    """A DB holding the fixtures' bars and corporate actions, archived and stored by the jobs'
    own fetch and store functions."""
    tmp = tmp_path_factory.mktemp("adjusted")
    config = tmp / "config"
    shutil.copytree(REPO_CONFIG, config)
    with pytest.MonkeyPatch.context() as mp:
        for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
            mp.delenv(key, raising=False)
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

        def answer_bars(request: httpx.Request) -> httpx.Response:
            symbols = tuple(request.url.params["symbols"].split(","))
            return _json(BAR_BODIES[symbols])

        actions = [_json((FIXTURES / "corp_actions" / f).read_bytes()) for f in ACTION_FILES]
        with respx.mock(assert_all_called=True) as router:
            router.get(url__startswith=corp_actions.URL).mock(side_effect=actions)
            router.get(url__startswith=bars.URL).mock(side_effect=answer_bars)
            with store.session(settings.db_path, wait=0) as con:
                client = http.Client(con, settings=settings)
                try:
                    action_ids = [
                        rid
                        for _ in ACTION_FILES
                        for rid in corp_actions.fetch_window(
                            client, date(2016, 1, 1), date(2026, 10, 19)
                        )
                    ]
                    bar_ids = []
                    for symbols in BAR_BODIES:
                        ids, invalid = bars.fetch_batch(client, symbols, date(2016, 1, 4))
                        assert invalid == []
                        bar_ids += ids
                finally:
                    client.close()
                for raw_id in action_ids:
                    corp_actions.store_page(con, raw_id, raw_dir=settings.raw_dir)
                for raw_id in bar_ids:
                    bars.store_page(con, raw_id, raw_dir=settings.raw_dir)
    return settings.db_path


@pytest.fixture
def con(built):
    c = duckdb.connect(str(built), read_only=True)
    yield c
    c.close()


@pytest.fixture
def scratch(built, tmp_path):
    """A writable copy of the built DB, for tests that add actions or remove bars."""
    path = tmp_path / "usfin.duckdb"
    shutil.copy(built, path)
    c = duckdb.connect(str(path))
    yield c
    c.close()


def _adjusted(con, symbol) -> dict[date, tuple]:
    """date -> (symbol, close, adj_close, split_factor, tr_factor) of the security `symbol`
    names today."""
    rows = con.execute(
        "SELECT date, symbol, close, adj_close, split_factor, tr_factor"
        " FROM alpaca_bar_adjusted_lookup(?) ORDER BY date",
        [symbol],
    ).fetchall()
    out = {r[0]: r[1:] for r in rows}
    assert len(out) == len(rows)  # one row a date
    return out


def _tr(con, symbol) -> dict[date, float]:
    return {d: r[4] for d, r in _adjusted(con, symbol).items()}


def _add_action(con, type_, symbol, ex_date, **values) -> None:
    """Insert an action version the way the corporate-actions job would store it."""
    row = {"id": str(uuid.uuid4()), "type": type_, "symbol": symbol, "ex_date": ex_date,
           "process_date": ex_date, **values}  # fmt: skip
    cols = ", ".join(f'"{c}"' for c in row)
    con.execute(
        f"INSERT INTO alpaca_corp_action ({cols}, payload, first_seen_at, raw_id)"
        f" VALUES ({', '.join('?' * len(row))}, ?, ?, ?)",
        [*row.values(), json.dumps(row, default=str), WHEN.replace(tzinfo=None), 0],
    )


SPLIT_DAY, EX_DAY = date(2020, 8, 31), date(2020, 8, 7)


# --- A6: AAPL across the 2020-08-31 split -------------------------------------------------------


def test_aapl_has_no_jump_across_the_2020_08_31_split(con):
    got = _adjusted(con, "AAPL")
    closes = _closes("AAPL")
    assert set(got) == set(closes)
    before, on = got[date(2020, 8, 28)], got[SPLIT_DAY]
    assert (before[1], on[1]) == (499.23, 129.04)  # raw: -74 %
    assert before[3] == 0.25 and on[3] == 1.0
    assert before[2] == pytest.approx(499.23 / 4)
    assert on[2] / before[2] - 1 == pytest.approx(129.04 / (499.23 / 4) - 1)  # +3.4 %, the day's move
    for d, (_, close, adj, split, tr) in got.items():
        assert split == (0.25 if d < SPLIT_DAY else 1.0)
        assert adj == pytest.approx(close * tr)
        if d >= EX_DAY:
            assert tr == split  # no dividend left after it
    days = sorted(got)
    worst = max(abs(got[b][2] / got[a][2] - 1) for a, b in pairwise(days))
    assert worst < 0.1  # AAPL's real moves (-8 % on 2020-09-03); the raw split day is -74 %


def test_a_reverse_split_is_applied(con):
    got = _adjusted(con, "CTNT")
    assert set(got) == set(_closes("CTNT"))
    split_day = date(2026, 9, 28)
    for d, (symbol, close, adj, split, tr) in got.items():
        assert symbol == "CTNT"
        assert split == tr == (150.0 if d < split_day else 1.0)  # old_rate 150 / new_rate 1
        assert adj == pytest.approx(close * split)
    assert got[date(2026, 9, 25)][2] == pytest.approx(0.0315 * 150)
    assert got[split_day][2] == 3.6


# --- How > Adjusted prices: dividends ------------------------------------------------------------


def test_the_dividend_factor_uses_the_last_close_before_the_ex_date(con):
    got = _tr(con, "AAPL")
    closes = _closes("AAPL")
    factor = 1 - 0.82 / closes[date(2020, 8, 6)]  # 455.61, not the ex-date's 444.45
    assert got[date(2020, 8, 6)] == pytest.approx(0.25 * factor, rel=1e-12)
    assert got[date(2020, 8, 3)] == got[date(2020, 8, 6)]
    assert got[EX_DAY] == 0.25  # the ex-date's own bar is not adjusted for it
    assert got[date(2020, 8, 6)] != pytest.approx(0.25 * (1 - 0.82 / closes[EX_DAY]), rel=1e-6)


def test_the_last_close_before_the_ex_date_skips_a_missing_bar(scratch):
    scratch.execute("DELETE FROM alpaca_bar WHERE symbol = 'AAPL' AND date = '2020-08-06'")
    got = _tr(scratch, "AAPL")
    assert date(2020, 8, 6) not in got
    assert got[date(2020, 8, 5)] == pytest.approx(0.25 * (1 - 0.82 / 440.25), rel=1e-12)


def test_special_and_foreign_dividends_count_at_their_stated_rate(scratch):
    _add_action(scratch, "cash_dividend", "AAPL", date(2020, 8, 20), rate=5.0, special=True,
                foreign=False, cusip="037833100")  # fmt: skip
    _add_action(scratch, "cash_dividend", "AAPL", date(2020, 8, 24), rate=2.0, special=False,
                foreign=True, cusip="037833100")  # fmt: skip
    got = _tr(scratch, "AAPL")
    closes = _closes("AAPL")
    special = 1 - 5.0 / closes[date(2020, 8, 19)]
    foreign = 1 - 2.0 / closes[date(2020, 8, 21)]
    assert got[date(2020, 8, 21)] == pytest.approx(0.25 * foreign, rel=1e-12)
    assert got[date(2020, 8, 19)] == pytest.approx(0.25 * special * foreign, rel=1e-12)
    assert got[date(2020, 8, 6)] == pytest.approx(
        0.25 * special * foreign * (1 - 0.82 / closes[date(2020, 8, 6)]), rel=1e-12
    )


@pytest.mark.parametrize(
    "type_", ["spin_off", "rights_distribution", "cash_merger", "stock_merger",
              "stock_and_cash_merger", "stock_dividend", "unit_split"],
)  # fmt: skip
def test_spin_offs_rights_and_mergers_are_not_applied(con, scratch, type_):
    _add_action(scratch, type_, "AAPL", date(2020, 8, 20), rate=0.5, old_rate=1.0, new_rate=2.0,
                cash_rate=10.0, cusip="037833100")  # fmt: skip
    assert _adjusted(scratch, "AAPL") == _adjusted(con, "AAPL")


def test_one_dividend_listed_twice_counts_once(con, scratch):
    """Alpaca lists some dividends under two ids (two CUSIPs, or a special and a regular copy);
    the same amount on the same ex-date is one dividend, a different amount another."""
    _add_action(scratch, "cash_dividend", "AAPL", EX_DAY, rate=0.82, special=True,
                foreign=False, cusip="")  # fmt: skip
    assert _adjusted(scratch, "AAPL") == _adjusted(con, "AAPL")
    _add_action(scratch, "cash_dividend", "AAPL", EX_DAY, rate=0.1, special=True,
                foreign=False, cusip="")  # fmt: skip
    closes = _closes("AAPL")
    assert _tr(scratch, "AAPL")[date(2020, 8, 6)] == pytest.approx(
        0.25 * (1 - 0.82 / closes[date(2020, 8, 6)]) * (1 - 0.1 / closes[date(2020, 8, 6)]),
        rel=1e-12,
    )


def test_an_action_after_the_last_bar_adjusts_nothing_yet(con, scratch):
    """Announced actions arrive up to 90 days ahead (A5): they count once their ex-date has a
    bar, the way Alpaca's own adjustment does."""
    _add_action(scratch, "cash_dividend", "AAPL", date(2020, 9, 8), rate=0.205, special=False,
                foreign=False, cusip="037833100")  # fmt: skip
    _add_action(scratch, "forward_split", "AAPL", date(2020, 9, 8), old_rate=1.0, new_rate=2.0,
                cusip="037833100")  # fmt: skip
    assert _adjusted(scratch, "AAPL") == _adjusted(con, "AAPL")


def test_the_newest_version_of_an_action_counts(con, scratch):
    """A corrected rate is a new version of the same id (A5); only the newest counts."""
    action_id = scratch.execute(
        "SELECT id FROM alpaca_corp_action WHERE symbol = 'AAPL' AND type = 'cash_dividend'"
    ).fetchone()[0]
    scratch.execute(
        "INSERT INTO alpaca_corp_action SELECT * REPLACE (0.41 AS rate,"
        " first_seen_at + INTERVAL 1 DAY AS first_seen_at) FROM alpaca_corp_action WHERE id = ?",
        [action_id],
    )
    closes = _closes("AAPL")
    assert _tr(scratch, "AAPL")[date(2020, 8, 6)] == pytest.approx(
        0.25 * (1 - 0.41 / closes[date(2020, 8, 6)]), rel=1e-12
    )


# --- Chained across renames, per security --------------------------------------------------------


def _meta_dividends() -> list[tuple[date, float]]:
    actions = json.loads((FIXTURES / "corp_actions" / "five_2016-2026.json").read_bytes())
    return sorted(
        (date.fromisoformat(a["ex_date"]), a["rate"])
        for a in actions["corporate_actions"]["cash_dividends"]
        if a["symbol"] == "META"
        and date.fromisoformat(a["ex_date"]) >= date(2022, 6, 9)  # not the Roundhill ETF's
    )


def test_factors_chain_across_the_fb_meta_rename(con):
    got = _adjusted(con, "META")
    meta = _closes("META")
    divs = _meta_dividends()
    assert len(divs) == 11  # all paid under META, from 2024
    days = sorted(meta)
    whole = 1.0
    for ex, rate in divs:
        whole *= 1 - rate / meta[max(d for d in days if d < ex)]
    fb_last, meta_first = got[date(2022, 6, 8)], got[date(2022, 6, 9)]
    assert (fb_last[0], meta_first[0]) == ("FB", "META")
    assert fb_last[4] == pytest.approx(whole, rel=1e-12)  # FB-era bars carry META's dividends
    assert meta_first[4] == fb_last[4]
    assert got[date(2016, 1, 4)][4] == fb_last[4]  # no action under FB while it was Meta
    assert {r[3] for r in got.values()} == {1.0}  # Meta never split
    assert got[date(2016, 1, 4)][2] == pytest.approx(got[date(2016, 1, 4)][1] * whole)


def test_a_reused_symbols_dividends_stay_with_their_own_security(con):
    """FB's 2025-2026 dividends belong to the ProShares ETF that holds FB now, not to Meta; META's
    2021 bars are the Roundhill ETF's (now METV), whose dividends are METV's."""
    meta = _adjusted(con, "META")
    fb = _adjusted(con, "FB")
    assert min(fb) == date(2025, 6, 26)
    assert fb[date(2025, 6, 26)][4] < 1  # its own five dividends
    assert date(2025, 6, 26) in meta and meta[date(2025, 6, 26)][0] == "META"
    metv = _adjusted(con, "METV")
    assert metv[date(2021, 6, 30)][0] == "META"
    closes = _closes("METV")
    want = 1.0
    for ex, rate in [(date(2022, 12, 13), 0.0063376), (date(2023, 12, 27), 0.0191304),
                     (date(2025, 12, 30), 0.033679)]:  # fmt: skip
        want *= 1 - rate / closes[max(d for d in closes if d < ex)]
    assert metv[date(2021, 6, 30)][4] == pytest.approx(want, rel=1e-12)


def test_a_quiet_segment_takes_no_action(con):
    """The flat META stretch between the two renames is its own security (A7); nothing adjusts
    it."""
    rows = con.execute(
        "SELECT DISTINCT split_factor, tr_factor FROM alpaca_bar_adjusted"
        " WHERE security_key = 'symbol:META:2022-01-31'"
    ).fetchall()
    assert rows == [(1.0, 1.0)]


# --- View and lookup ------------------------------------------------------------------------------


def test_the_view_holds_every_bar_once_and_equals_the_lookups(con):
    total = con.execute("SELECT count(*) FROM alpaca_bar_latest").fetchone()[0]
    assert con.execute(
        "SELECT count(*), count(DISTINCT (security_key, date)), count(DISTINCT (symbol, date))"
        " FROM alpaca_bar_adjusted"
    ).fetchone() == (total, total, total)
    for symbol in (*FIVE, "AAPL", "CTNT"):
        key = con.execute(
            "SELECT security_key FROM alpaca_security_symbol WHERE symbol = ?", [symbol]
        ).fetchone()[0]
        view = con.execute(
            f"SELECT {COLUMNS} FROM alpaca_bar_adjusted WHERE security_key = ? ORDER BY date",
            [key],
        ).fetchall()
        got = con.execute(
            f"SELECT {COLUMNS} FROM alpaca_bar_adjusted_lookup(?) ORDER BY date", [symbol]
        ).fetchall()
        assert got == view
        assert view


def test_columns(con):
    cols = [d[0] for d in con.execute("SELECT * FROM alpaca_bar_adjusted LIMIT 0").description]
    assert cols == [c.strip() for c in COLUMNS.split(",")]


def test_an_unknown_symbol_finds_nothing(con):
    assert _adjusted(con, "ZZZZ") == {}


def test_an_empty_store_has_no_adjusted_bar(tmp_path):
    with store.session(tmp_path / "usfin.duckdb", wait=0) as c:
        assert c.execute("SELECT count(*) FROM alpaca_bar_adjusted").fetchone() == (0,)
        assert c.execute(
            "SELECT count(*) FROM alpaca_bar_adjusted_lookup('AAPL')"
        ).fetchone() == (0,)


# --- Live: Alpaca's own adjustment=all ------------------------------------------------------------

LIVE_SYMBOLS = ("AAPL", "MSFT", "KO")


@pytest.mark.live
def test_live_matches_alpacas_adjustment_all():
    """Every day AAPL, MSFT and KO have in the worktree DB and in Alpaca's `adjustment=all`
    (fetched now, directly: not through a job, not archived) agrees within 0.5 % (A6). Needs
    their whole history in the DB (`usfin alpaca backfill bars --symbols MSFT,KO`) and the
    corporate actions from 2016."""
    settings = load_settings()
    if not (settings.apca_api_key_id and settings.apca_api_secret_key):
        pytest.skip("APCA_API_KEY_ID / APCA_API_SECRET_KEY not set")
    headers = {
        "APCA-API-KEY-ID": settings.apca_api_key_id,
        "APCA-API-SECRET-KEY": settings.apca_api_secret_key,
    }
    # no `end`: the free plan refuses a date `end` of today (How > Bars, amended in S6)
    params = {"symbols": ",".join(LIVE_SYMBOLS), "timeframe": "1Day", "feed": "sip",
              "adjustment": "all", "start": "2016-01-04", "limit": "10000"}  # fmt: skip
    alpaca: dict[str, dict[date, float]] = {s: {} for s in LIVE_SYMBOLS}
    token = None
    with httpx.Client(timeout=60) as client:
        while True:
            page = {**params, "page_token": token} if token else params
            response = client.get(bars.URL, params=page, headers=headers)
            assert response.status_code == 200, response.text[:200]
            found, token = bars.parse(response.content)
            for bar in found:
                alpaca[bar["symbol"]][date.fromisoformat(bar["date"])] = bar["close"]
            if token is None:
                break
    report = []
    with store.session(settings.db_path) as con:
        for symbol in LIVE_SYMBOLS:
            ours = _adjusted(con, symbol)
            assert min(ours) == date(2016, 1, 4), f"{symbol}: backfill its whole history first"
            both = sorted(set(ours) & set(alpaca[symbol]))
            assert len(both) >= 2700
            err = {d: ours[d][2] / alpaca[symbol][d] - 1 for d in both}
            worst = max(err, key=lambda d: abs(err[d]))
            mean = sum(abs(e) for e in err.values()) / len(err)
            report.append(
                f"{symbol}: {len(both)} days, max {abs(err[worst]):.4%} on {worst}"
                f" (ours {ours[worst][2]:.4f}, Alpaca {alpaca[symbol][worst]}), mean {mean:.5%},"
                f" tr_factor 2016-01-04 {ours[date(2016, 1, 4)][4]:.6f}"
            )
            assert all(math.isfinite(e) for e in err.values())
            assert abs(err[worst]) <= 0.005, report[-1]
    print("\n" + "\n".join(report))
