"""Type-2 symbol history diffed from Nasdaq Trader snapshots (job `symbols`; C2, C9).

Every snapshot here is `tests/fixtures/nasdaqtrader/{nasdaqlisted,otherlisted}.txt` (recorded
2026-10-08, see test_nasdaq_snapshot) with rows edited: a later `File Creation Time`, a row
dropped, a real row copied under another symbol, a name, flag or exchange changed. Nothing else
about the format is made up. No request leaves the process.
"""

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
from usfin.sources import nasdaqtrader

FIXTURES = Path(__file__).parent / "fixtures" / "nasdaqtrader"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def naive(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


T1 = naive(2026, 10, 8, 7, 3)  # 10/08 03:03 EDT, the recorded files
T2 = naive(2026, 10, 9, 7, 2)
T3 = naive(2026, 10, 12, 7, 4)
T4 = naive(2026, 10, 13, 7, 3)
STAMP = {T1: "1008202603:03", T2: "1009202603:02", T3: "1012202603:04", T4: "1013202603:03"}
RUN_AT = {
    T1: utc(2026, 10, 8, 11, 0),
    T2: utc(2026, 10, 9, 11, 0),
    T3: utc(2026, 10, 12, 11, 0),
    T4: utc(2026, 10, 13, 11, 0),
}


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(RUN_AT[T1])
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
    )


class Fake:
    """Answers both SymDir URLs with `body[file]`."""

    def __init__(self):
        self.body = {f: fixture(f"{f}.txt").decode("ascii") for f in nasdaqtrader.FILES}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        name = request.url.path.rsplit("/", 1)[-1].removesuffix(".txt")
        return httpx.Response(
            200, content=self.body[name].encode("ascii"), headers={"content-type": "text/plain"}
        )

    def stamp(self, at: datetime) -> None:
        """Both files as created at `at` (the recorded files' trailer re-dated)."""
        for f in nasdaqtrader.FILES:
            self.body[f] = _restamp(self.body[f], STAMP[at])

    def drop(self, file: str, symbol: str) -> None:
        self.body[file] = "".join(
            line
            for line in self.body[file].splitlines(keepends=True)
            if not line.startswith(f"{symbol}|")
        )

    def set(self, file: str, symbol: str, column: str, value: str) -> None:
        lines = self.body[file].splitlines(keepends=True)
        header = lines[0].rstrip("\r\n").split("|")
        col = header.index(column)
        for i, line in enumerate(lines):
            if line.startswith(f"{symbol}|"):
                cells = line.rstrip("\r\n").split("|")
                cells[col] = value
                lines[i] = "|".join(cells) + "\r\n"
        self.body[file] = "".join(lines)

    def copy(self, file: str, symbol: str, new_symbol: str, after: str | None = None) -> None:
        """Add a row: `symbol`'s real line under `new_symbol`, placed after line `after`."""
        lines = self.body[file].splitlines(keepends=True)
        line = next(x for x in lines if x.startswith(f"{symbol}|"))
        at = next(i for i, x in enumerate(lines) if x.startswith(f"{after or symbol}|"))
        lines.insert(at + 1, new_symbol + line[len(symbol) :])
        self.body[file] = "".join(lines)

    def move(self, symbol: str, src: str, dst: str, exchange: str) -> None:
        """`symbol` leaves `src` and appears in `dst`, its other cells from `dst`'s first row."""
        lines = self.body[src].splitlines(keepends=True)
        name = next(x for x in lines if x.startswith(f"{symbol}|")).split("|")[1]
        self.drop(src, symbol)
        dlines = self.body[dst].splitlines(keepends=True)
        header = dlines[0].rstrip("\r\n").split("|")
        cells = dlines[1].rstrip("\r\n").split("|")
        for col, value in (
            ("ACT Symbol", symbol), ("Security Name", name), ("Exchange", exchange),
            ("CQS Symbol", symbol), ("NASDAQ Symbol", symbol),
        ):  # fmt: skip
            cells[header.index(col)] = value
        dlines.insert(len(dlines) - 1, "|".join(cells) + "\r\n")
        self.body[dst] = "".join(dlines)


def _restamp(body: str, stamp: str) -> str:
    head, _, tail = body.rpartition("File Creation Time: ")
    return head + "File Creation Time: " + stamp + tail[len(stamp) :]


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        for name in nasdaqtrader.FILES:
            router.get(nasdaqtrader.url(name)).mock(side_effect=f)
        yield f


def _registry():
    reg = jobs.Registry()
    reg.register(nasdaqtrader.JOB)
    return reg


def _run(settings, clock, at: datetime, fake: Fake | None = None):
    if fake is not None:
        fake.stamp(at)
    clock.t = RUN_AT[at]
    result = jobs.run_job("symbols", settings=settings, registry=_registry(), clock=clock)
    assert result.status == "ok", result.error
    return result


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


HISTORY = (
    "symbol, exchange, valid_from, valid_to, file, name, etf, market_category, financial_status"
)


def _history(settings):
    return _query(settings, f"SELECT {HISTORY} FROM symbol_history ORDER BY ALL")


def _rows(settings, symbol):
    """(exchange, valid_from, valid_to, name, etf, market_category, financial_status) per row."""
    return [
        (r[1], r[2], r[3], r[5], r[6], r[7], r[8])
        for r in _query(
            settings,
            f"SELECT {HISTORY} FROM symbol_history WHERE symbol = ? ORDER BY valid_from",
            [symbol],
        )
    ]


def _diffs(settings):
    return _query(
        settings,
        "SELECT file, snapshot_at, rows, previous_rows, diffed, opened, closed FROM symbol_diff"
        " ORDER BY file, snapshot_at",
    )


def _set_swing_pct(settings, pct):
    path = settings.config_dir / "schedule.yaml"
    data = yaml.safe_load(path.read_text("utf-8"))
    data["jobs"]["symbols"]["alerts"]["row_count_swing"] = {"pct": pct}
    path.write_text(yaml.safe_dump(data), "utf-8")


# --- first snapshot -------------------------------------------------------------------------


def test_the_first_snapshot_opens_a_row_per_symbol_with_no_swing_check(settings, fake, clock):
    _run(settings, clock, T1)
    history = _history(settings)
    assert len(history) == 4 + 8
    assert {r[2] for r in history} == {T1} and {r[3] for r in history} == {None}
    assert _rows(settings, "AAPL") == [
        ("Q", T1, None, "Apple Inc. - Common Stock", False, "Q", "N")
    ]
    assert _rows(settings, "SPY") == [
        ("P", T1, None, "State Street SPDR S&P 500 ETF Trust", True, None, None)
    ]
    assert _diffs(settings) == [
        ("nasdaqlisted", T1, 4, None, True, 4, 0),
        ("otherlisted", T1, 8, None, True, 8, 0),
    ]
    assert _query(settings, "SELECT rule FROM alert") == []


def test_the_same_snapshot_again_changes_no_history(settings, fake, clock):
    _run(settings, clock, T1)
    before = (_history(settings), _diffs(settings))
    clock.t = utc(2026, 10, 8, 15, 0)
    again = jobs.run_job("symbols", settings=settings, registry=_registry(), clock=clock)
    assert again.status == "ok" and again.rows_upserted == 0
    assert (_history(settings), _diffs(settings)) == before


# --- changes --------------------------------------------------------------------------------


def test_add_remove_rename_and_etf_change_make_type2_rows(settings, fake, clock):
    _run(settings, clock, T1, fake)
    # otherlisted keeps 8 rows (one gone, one new: no swing); nasdaqlisted keeps 4
    fake.drop("otherlisted", "AAA")
    fake.copy("otherlisted", "BRK.B", "BRK.A")
    fake.set("otherlisted", "A", "Security Name", "Agilent Technologies Inc. Common Stock")
    fake.set("otherlisted", "SPY", "ETF", "N")
    fake.set("otherlisted", "AAC.U", "Exchange", "A")  # another exchange: close + open
    fake.set("nasdaqlisted", "AACG", "Financial Status", "N")
    fake.set("nasdaqlisted", "QQQ", "Market Category", "Q")
    fake.set("nasdaqlisted", "AAPL", "Round Lot Size", "100")  # not a change
    fake.set("otherlisted", "AAC.W", "CQS Symbol", "AAC WS")  # not a change
    _run(settings, clock, T2, fake)

    assert _rows(settings, "AAA")[0][1:3] == (T1, T2)  # removed: closed
    assert _rows(settings, "BRK.A") == [
        ("N", T2, None, "Berkshire Hathaway Inc. New Common Stock", False, None, None)
    ]
    assert [(r[1], r[2], r[3]) for r in _rows(settings, "A")] == [
        (T1, T2, "Agilent Technologies, Inc. Common Stock"),
        (T2, None, "Agilent Technologies Inc. Common Stock"),
    ]
    assert [(r[1], r[2], r[4]) for r in _rows(settings, "SPY")] == [
        (T1, T2, True), (T2, None, False),
    ]  # fmt: skip
    assert [(r[0], r[1], r[2]) for r in _rows(settings, "AAC.U")] == [
        ("N", T1, T2), ("A", T2, None),
    ]  # fmt: skip
    assert [(r[1], r[2], r[6]) for r in _rows(settings, "AACG")] == [
        (T1, T2, "D"), (T2, None, "N"),
    ]  # fmt: skip
    assert [(r[1], r[2], r[5]) for r in _rows(settings, "QQQ")] == [
        (T1, T2, "G"), (T2, None, "Q"),
    ]  # fmt: skip
    for unchanged in ("AAPL", "AAC.W", "BRK.B", "ABR$D"):
        assert [(r[1], r[2]) for r in _rows(settings, unchanged)] == [(T1, None)]
    # opened: BRK.A + 3 changed (A, SPY, AAC.U on A); closed: AAA + the 3 changed
    assert ("otherlisted", T2, 8, 8, True, 4, 4) in _diffs(settings)
    assert ("nasdaqlisted", T2, 4, 4, True, 2, 2) in _diffs(settings)
    open_now = _query(
        settings, "SELECT file, count(*) FROM symbol_history WHERE valid_to IS NULL GROUP BY 1"
    )
    assert sorted(open_now) == [("nasdaqlisted", 4), ("otherlisted", 8)]


def test_a_symbol_moving_between_files_closes_and_opens(settings, fake, clock):
    _set_swing_pct(settings, 50)  # one row out of four is a 25 % swing in these small files
    _run(settings, clock, T1, fake)
    fake.move("AAPL", "nasdaqlisted", "otherlisted", "N")
    _run(settings, clock, T2, fake)
    assert [(r[0], r[1], r[2]) for r in _rows(settings, "AAPL")] == [
        ("Q", T1, T2), ("N", T2, None),
    ]  # fmt: skip
    assert _query(
        settings, "SELECT file FROM symbol_history WHERE symbol = 'AAPL' ORDER BY valid_from"
    ) == [("nasdaqlisted",), ("otherlisted",)]


def test_a_symbol_that_comes_back_opens_a_new_row(settings, fake, clock):
    _run(settings, clock, T1, fake)
    original = fake.body["otherlisted"]
    fake.drop("otherlisted", "AAA")
    fake.copy("otherlisted", "BRK.B", "BRK.A")
    _run(settings, clock, T2, fake)
    fake.body["otherlisted"] = original
    _run(settings, clock, T3, fake)
    assert [(r[1], r[2]) for r in _rows(settings, "AAA")] == [(T1, T2), (T3, None)]
    assert [(r[1], r[2]) for r in _rows(settings, "BRK.A")] == [(T2, T3)]


# --- the ±2 % gate --------------------------------------------------------------------------


def test_a_swing_snapshot_is_stored_but_not_diffed_and_alerts(settings, fake, clock):
    _run(settings, clock, T1, fake)
    before = _history(settings)
    fake.drop("otherlisted", "SPY")  # 8 -> 7 rows: -12.5 %
    fake.set("otherlisted", "A", "Security Name", "Agilent Technologies Inc. Common Stock")
    _run(settings, clock, T2, fake)

    assert _query(
        settings, "SELECT count(*) FROM symbol_snapshot WHERE file = 'otherlisted' AND"
        " snapshot_at = ?", [T2],
    ) == [(7,)]  # fmt: skip
    assert _history(settings) == before  # nothing closed, nothing renamed
    assert ("otherlisted", T2, 7, 8, False, 0, 0) in _diffs(settings)
    assert ("nasdaqlisted", T2, 4, 4, True, 0, 0) in _diffs(settings)
    assert _query(settings, "SELECT rule, subject FROM alert") == [
        ("row_count_swing", "otherlisted")
    ]

    # the next snapshot within ±2 % of its predecessor diffs against the last diffed one (T1):
    # SPY closes and A is renamed at T3, the swing snapshot leaves no trace in history
    _run(settings, clock, T3, fake)
    assert ("otherlisted", T3, 7, 7, True, 1, 2) in _diffs(settings)
    assert [(r[1], r[2]) for r in _rows(settings, "SPY")] == [(T1, T3)]
    assert [(r[1], r[2]) for r in _rows(settings, "A")] == [(T1, T3), (T3, None)]


def test_a_snapshot_after_a_bad_one_is_gated_against_the_bad_one(settings, fake, clock):
    """The gate compares with the previous snapshot (C9): after a cut-down file, the restored
    file swings too and waits a day; nothing is lost, the diff base is still T1."""
    _run(settings, clock, T1, fake)
    original = fake.body["otherlisted"]
    fake.drop("otherlisted", "SPY")
    fake.drop("otherlisted", "AAA")
    _run(settings, clock, T2, fake)  # 6 vs 8: gated
    fake.body["otherlisted"] = original
    _run(settings, clock, T3, fake)  # 8 vs 6: gated
    _run(settings, clock, T4, fake)  # 8 vs 8: diffed against T1, no change
    other = [d for d in _diffs(settings) if d[0] == "otherlisted"]
    assert [(d[1], d[4]) for d in other] == [(T1, True), (T2, False), (T3, False), (T4, True)]
    assert {(r[2], r[3]) for r in _history(settings) if r[4] == "otherlisted"} == {(T1, None)}


def test_the_gate_uses_the_jobs_row_count_swing_pct(settings, fake, clock):
    _set_swing_pct(settings, 20)
    _run(settings, clock, T1, fake)
    fake.drop("otherlisted", "SPY")  # -12.5 %, inside 20 %
    _run(settings, clock, T2, fake)
    assert ("otherlisted", T2, 7, 8, True, 0, 1) in _diffs(settings)
    assert _rows(settings, "SPY")[0][2] == T2


# --- reparse --------------------------------------------------------------------------------


def _three_days(settings, fake, clock):
    _run(settings, clock, T1, fake)
    fake.drop("otherlisted", "SPY")
    fake.set("nasdaqlisted", "QQQ", "ETF", "N")
    _run(settings, clock, T2, fake)  # otherlisted gated, nasdaqlisted diffed
    fake.set("otherlisted", "A", "Security Name", "Agilent Technologies Inc. Common Stock")
    _run(settings, clock, T3, fake)


def test_reparse_rebuilds_the_same_history_from_stored_snapshots(settings, fake, clock):
    _three_days(settings, fake, clock)
    built = (_history(settings), _diffs(settings))
    assert len(built[1]) == 6
    n = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        jobs.reparse(con, nasdaqtrader.JOB, settings=settings)
    assert (_history(settings), _diffs(settings)) == built
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM symbol_history")
        con.execute("DELETE FROM symbol_diff")
        con.execute("DELETE FROM symbol_snapshot")
        jobs.reparse(con, nasdaqtrader.JOB, settings=settings)
    assert (_history(settings), _diffs(settings)) == built
    assert len(fake.requests) == n


def test_reparse_since_a_day_still_rebuilds_the_whole_file_in_order(settings, fake, clock):
    _three_days(settings, fake, clock)
    built = (_history(settings), _diffs(settings))
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM symbol_history WHERE valid_from > ?", [T1])
        con.execute("UPDATE symbol_history SET valid_to = NULL")
        result = jobs.reparse(con, nasdaqtrader.JOB, settings=settings, since=RUN_AT[T3].date())
    assert len(result.raw_ids) == 2
    assert (_history(settings), _diffs(settings)) == built
