"""Bars backfill (`usfin alpaca backfill bars`; plan alpaca-market-data S8: A10, How > Backfill).

The backfill asks every universe symbol whose whole history was never asked (no `history` or
`invalid` mark in `alpaca_bar_symbol`), or the `--symbols` given, `batch_size` symbols a request,
from `--from` (default `backfill_start`) with no `end`. Each batch is one DB session (open,
archive every page, store, close); pages are archived as the job's dataset (`alpaca_bars`) and
stored by the job's `store_page`, so the marks it writes are the to-do list a rerun resumes from
and `usfin reparse alpaca_bars` rebuilds everything. Progress (the last finished batch) is under
the job name `alpaca_bars_backfill`, which has no `schedule.yaml` entry, so it raises no alert.

The universe is written straight into `alpaca_asset_history` / `alpaca_corp_action` as in
test_alpaca_bars_job.py. The fake answers each request from `Fake.held` cut to its dates, in
pages of `page_size` bars; AAPL and MSFT are the real bars of the S6 fixtures
`multi_p{1,2}.json`. Fake keys only; no request leaves the process.
"""

import json
import shutil
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx
import yaml
from typer.testing import CliRunner

from usfin import alerts, http, jobs, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources.alpaca import backfill, bars

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca" / "bars"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
START = "2016-01-04"
NOW = datetime(2026, 10, 19, 23, 0, tzinfo=UTC)  # 19:00 New York
TODAY = date(2026, 10, 19)


def _real() -> dict[str, list[dict]]:
    held: dict[str, list[dict]] = {}
    for name in ("multi_p1.json", "multi_p2.json"):
        for symbol, items in json.loads((FIXTURES / name).read_bytes())["bars"].items():
            held.setdefault(symbol, []).extend(items)
    return held


def _bar(day: str, close: float = 10.0) -> dict:
    # 05:00Z is midnight New York in winter and 01:00 in summer: the same date either way
    return {"t": f"{day}T05:00:00Z", "o": close, "h": close, "l": close, "c": close, "v": 100,
            "n": 10, "vw": close}  # fmt: skip


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(raw, "_utcnow", lambda: NOW.replace(tzinfo=None))
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
    """Answers from `held` cut to the request's dates, `page_size` bars a page (in symbol order,
    a symbol's bars may be cut in two, like Alpaca's); a request naming a symbol in `invalid`
    gets that 400, one naming a symbol in `fail` a 500."""

    def __init__(self):
        self.held: dict[str, list[dict]] = _real()
        self.page_size = 10_000
        self.invalid: set[str] = set()
        self.fail: set[str] = set()
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        params = request.url.params
        asked = params["symbols"].split(",")
        status = 200
        if bad := [s for s in asked if s in self.invalid]:
            content, status = json.dumps({"message": f"invalid symbol: {bad[0]}"}).encode(), 400
        elif [s for s in asked if s in self.fail]:
            content, status = b'{"message":"internal"}', 500
        else:
            lo, hi = params["start"], params.get("end", "9999-12-31")
            flat = [
                (s, b)
                for s in sorted(asked)
                for b in self.held.get(s, [])
                if lo <= bars.bar_date(b["t"]).isoformat() <= hi
            ]
            i = int(params.get("page_token", "0"))
            page = flat[i : i + self.page_size]
            out: dict[str, list[dict]] = {}
            for s, b in page:
                out.setdefault(s, []).append(b)
            more = i + self.page_size < len(flat)
            content = json.dumps(
                {"bars": out, "next_page_token": str(i + self.page_size) if more else None}
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


def _batch_size(settings, n: int) -> None:
    path = settings.config_dir / "alpaca.yaml"
    data = yaml.safe_load(path.read_text())
    data["batch_size"] = n
    path.write_text(yaml.safe_dump(data))


def _backfill(settings, **kw):
    return backfill.bars_history(settings, today=TODAY, **kw)


def _marks(settings, status):
    return [
        s
        for (s,) in _query(
            settings, "SELECT symbol FROM alpaca_bar_symbol WHERE status = ? ORDER BY 1", [status]
        )
    ]


def _table(settings):
    return _query(settings, "SELECT * FROM alpaca_bar ORDER BY ALL")


def _symbol_table(settings):
    return _query(settings, "SELECT * FROM alpaca_bar_symbol ORDER BY ALL")


@pytest.fixture
def five(settings, fake):
    """Five universe symbols, two of them active; batches of two."""
    for s in ("GONE", "OLD", "TWTR"):
        fake.held[s] = [_bar("2016-01-04"), _bar("2016-01-05"), _bar("2022-10-27")]
    _universe(settings, active=["MSFT", "AAPL"], inactive=["GONE", "OLD"], merged=["TWTR"])
    _batch_size(settings, 2)


# --- what it asks ----------------------------------------------------------------------------


def test_it_batches_the_universe_active_first_from_backfill_start_with_no_end(
    settings, fake, five
):
    result = _backfill(settings)
    # the daily job's catch-up order: active first, then by symbol; whole history, no `end`
    assert fake.asked() == [("AAPL,MSFT", START, None), ("GONE,OLD", START, None),
                            ("TWTR", START, None)]  # fmt: skip
    assert all(r.headers["APCA-API-KEY-ID"] == KEY_ID for r in fake.requests)
    assert all(r.url.params["asof"] == "-" for r in fake.requests)
    assert _query(
        settings, "SELECT source, dataset, http_status, count(*) FROM raw_response GROUP BY ALL"
    ) == [("alpaca", "alpaca_bars", 200, 3)]
    held = sum(len(v) for k, v in fake.held.items() if k in {"AAPL", "MSFT", "GONE", "OLD",
                                                              "TWTR"})  # fmt: skip
    assert (result.named, result.skipped, result.batches, result.pages) == (5, 0, 3, 3)
    assert result.bars == result.inserted == held
    assert result.complete == 5
    assert _marks(settings, "history") == ["AAPL", "GONE", "MSFT", "OLD", "TWTR"]


def test_symbols_replaces_the_universe(settings, fake, five):
    result = _backfill(settings, symbols=["OLD", "AAPL", "OLD", "ZZZZ"])
    # as given, once each; a symbol outside the universe is asked too (and marked: asked in full)
    assert fake.asked() == [("OLD,AAPL", START, None), ("ZZZZ", START, None)]
    assert (result.named, result.batches) == (3, 2)
    assert _marks(settings, "history") == ["AAPL", "OLD", "ZZZZ"]


def test_a_batch_follows_next_page_token_and_marks_history_on_its_last_page(
    settings, fake, five
):
    fake.page_size = 7
    seen = []
    result = _backfill(settings, symbols=["AAPL", "MSFT"], progress=seen.append)
    aapl_msft = len(fake.held["AAPL"]) + len(fake.held["MSFT"])
    pages = -(-aapl_msft // 7)
    assert len(fake.requests) == pages > 1
    assert (result.pages, result.bars, result.inserted) == (pages, aapl_msft, aapl_msft)
    assert [(b.n, b.of, b.symbols, b.pages, b.bars, b.complete) for b in seen] == [
        (1, 1, ("AAPL", "MSFT"), pages, aapl_msft, 2)
    ]
    assert _marks(settings, "history") == ["AAPL", "MSFT"]


def test_a_later_from_asks_from_it_and_marks_nothing(settings, fake, five):
    _backfill(settings, start=date(2026, 10, 1), symbols=["AAPL"])
    assert fake.asked() == [("AAPL", "2026-10-01", None)]
    # not the whole history: the daily catch-up (and a default backfill) still ask it in full
    assert _marks(settings, "history") == []
    assert _query(settings, "SELECT count(*) FROM alpaca_bar WHERE symbol = 'AAPL'") == [
        (len([b for b in fake.held["AAPL"] if b["t"][:10] >= "2026-10-01"]),)
    ]


# --- resume ----------------------------------------------------------------------------------


def test_a_rerun_asks_nothing_and_inserts_nothing(settings, fake, five):
    _backfill(settings)
    built, marks = _table(settings), _symbol_table(settings)
    fake.requests.clear()
    result = _backfill(settings)
    assert fake.requests == []
    assert (result.named, result.skipped, result.batches, result.inserted) == (5, 5, 0, 0)
    assert _table(settings) == built and _symbol_table(settings) == marks


def test_symbols_already_asked_in_full_are_skipped(settings, fake, five):
    # by the daily job's catch-up, an earlier backfill, or an earlier `--symbols` run
    _backfill(settings, symbols=["MSFT", "OLD"])
    fake.requests.clear()
    result = _backfill(settings)
    assert fake.asked() == [("AAPL,GONE", START, None), ("TWTR", START, None)]
    assert (result.named, result.skipped) == (5, 2)


def test_an_interrupted_backfill_rolls_back_only_its_open_batch_and_resumes(
    settings, fake, five
):
    fake.fail = {"OLD"}
    with pytest.raises(http.HttpError):  # a 500 is retried, then raised
        _backfill(settings)
    assert fake.asked()[0] == ("AAPL,MSFT", START, None)
    assert fake.asked()[-1] == ("GONE,OLD", START, None)
    # the finished batch stays stored and marked; the failed one left nothing
    assert _marks(settings, "history") == ["AAPL", "MSFT"]
    assert _query(settings, "SELECT DISTINCT symbol FROM alpaca_bar ORDER BY 1") == [
        ("AAPL",), ("MSFT",)
    ]  # fmt: skip
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, backfill.BARS_JOB) == {"from": START, "symbol": "MSFT"}
    fake.fail = set()
    fake.requests.clear()
    result = _backfill(settings)
    assert fake.asked() == [("GONE,OLD", START, None), ("TWTR", START, None)]
    assert (result.skipped, result.batches) == (2, 2)
    assert _marks(settings, "history") == ["AAPL", "GONE", "MSFT", "OLD", "TWTR"]


def test_a_failure_while_storing_rolls_back_the_batch(settings, fake, five, monkeypatch):
    real = bars.store_page
    calls = []

    def failing(con, raw_id, **kw):
        calls.append(raw_id)
        out = real(con, raw_id, **kw)
        if len(calls) == 2:
            raise bars.BarsError("broken page")
        return out

    monkeypatch.setattr(backfill.bars, "store_page", failing)
    with pytest.raises(bars.BarsError):
        _backfill(settings)
    # batch 2 stored its page, then failed: none of its bars or marks remain
    assert _marks(settings, "history") == ["AAPL", "MSFT"]
    assert _query(settings, "SELECT count(*) FROM alpaca_bar WHERE symbol IN ('GONE', 'OLD')") == [
        (0,)
    ]  # fmt: skip
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, backfill.BARS_JOB)["symbol"] == "MSFT"


# --- invalid symbols -------------------------------------------------------------------------


def test_invalid_symbols_are_marked_and_not_asked_again(settings, fake):
    fake.invalid = {"005CVR014", "BF/B"}
    _universe(settings, active=["AAPL"], inactive=["005CVR014"], merged=["BF/B"])
    seen = []
    result = _backfill(settings, progress=seen.append)
    assert fake.asked() == [
        ("AAPL,005CVR014,BF/B", START, None),
        ("AAPL,BF/B", START, None),
        ("AAPL", START, None),
    ]
    assert result.invalid == ["005CVR014", "BF/B"]
    assert seen[0].invalid == ("005CVR014", "BF/B")
    assert _marks(settings, "invalid") == ["005CVR014", "BF/B"]
    assert _marks(settings, "history") == ["AAPL"]
    fake.requests.clear()
    assert _backfill(settings).batches == 0
    assert _backfill(settings, symbols=["BF/B", "AAPL"]).batches == 0
    assert fake.requests == []


# --- sessions, progress and alerts -----------------------------------------------------------


def test_each_batch_opens_and_closes_its_own_db_session(settings, fake, five, monkeypatch):
    opened, closed = [], []
    real = store.session

    @contextmanager
    def counting(*args, **kwargs):
        opened.append(1)
        with real(*args, **kwargs) as con:
            yield con
        closed.append(1)

    monkeypatch.setattr(backfill.store, "session", counting)
    between = []

    def progress(batch):
        between.append((len(opened), len(closed)))
        # nothing holds the DB between batches: another writer gets in
        with real(settings.db_path, wait=0) as con:
            con.execute("CREATE TABLE IF NOT EXISTS other_writer (n INT)")
            con.execute("INSERT INTO other_writer VALUES (?)", [len(between)])

    _backfill(settings, progress=progress)
    # one session reads the to-do list, then one per batch; each closed before the next batch
    assert between == [(n + 1, n + 1) for n in range(1, 4)]
    assert _query(settings, "SELECT count(*) FROM other_writer") == [(3,)]


def test_progress_lives_under_its_own_job_name_with_no_alert_rules(settings, fake, five):
    _backfill(settings)
    assert backfill.BARS_JOB == "alpaca_bars_backfill"
    assert backfill.BARS_JOB not in alerts.load_rules(settings.config_dir)
    assert backfill.BARS_JOB not in alerts.load_rules(REPO_CONFIG)
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, backfill.BARS_JOB) == {"from": START, "symbol": "TWTR"}
        # the daily job's watermark and alerts are untouched; no job run is recorded
        assert jobs.get_watermarks(con, bars.JOB_NAME) == {}
        assert alerts.list_alerts(con) == []
        assert con.execute("SELECT count(*) FROM job_run").fetchone() == (0,)


class FailingTransport(httpx.BaseTransport):
    def handle_request(self, request):
        raise AssertionError(f"reparse sent a request: {request.url}")


def test_reparse_of_the_job_rebuilds_the_bars_and_marks(settings, fake, five):
    fake.invalid = {"OLD"}
    fake.page_size = 9
    _backfill(settings)
    built, marks = _table(settings), _symbol_table(settings)
    assert ("OLD", "invalid") in {(s, st) for s, st, *_ in marks}
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM alpaca_bar")
        con.execute("DELETE FROM alpaca_bar_symbol")
        jobs.reparse(con, bars.JOB, settings=settings, transport=FailingTransport())
    assert _table(settings) == built
    assert _symbol_table(settings) == marks


def test_no_key_in_the_raw_store_or_tables(settings, fake, five):
    _backfill(settings)
    for blob in Path(settings.raw_dir).rglob("*"):
        if blob.is_file():
            data = blob.read_bytes()
            assert KEY_ID.encode() not in data and SECRET.encode() not in data
    dump = repr(_query(settings, "SELECT * FROM raw_response")) + repr(_table(settings))
    assert KEY_ID not in dump and SECRET not in dump


# --- CLI -------------------------------------------------------------------------------------


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    monkeypatch.setenv("APCA_API_KEY_ID", KEY_ID)
    monkeypatch.setenv("APCA_API_SECRET_KEY", SECRET)
    monkeypatch.setattr(backfill, "_now", lambda: NOW)


def test_cli_prints_each_batch_and_totals(settings, fake, five, cli_env):
    fake.invalid = {"OLD"}
    out = CliRunner().invoke(app, ["alpaca", "backfill", "bars"])
    assert out.exit_code == 0, out.output
    assert "batch 1/3 AAPL..MSFT: pages 1, bars" in out.output
    assert "invalid OLD" in out.output
    assert "symbols 5 (skipped 0), batches 3, pages 4, bars" in out.output
    assert KEY_ID not in out.output and SECRET not in out.output


def test_cli_takes_symbols_and_from(settings, fake, five, cli_env):
    out = CliRunner().invoke(
        app, ["alpaca", "backfill", "bars", "--symbols", "aapl, OLD", "--from", "2016-01-05"]
    )
    assert out.exit_code == 0, out.output
    # upper-cased and trimmed, like a symbol typed on the command line
    assert fake.asked() == [("AAPL,OLD", "2016-01-05", None)]


def test_cli_rejects_a_bad_date_and_reports_a_failure_with_resume_hint(
    settings, fake, five, cli_env
):
    bad = CliRunner().invoke(app, ["alpaca", "backfill", "bars", "--from", "01/2016"])
    assert bad.exit_code != 0
    fake.fail = {"GONE"}
    out = CliRunner().invoke(app, ["alpaca", "backfill", "bars"])
    assert out.exit_code == 1
    assert "batch 1/3 AAPL..MSFT" in out.output
    assert "rerun to resume" in out.output


def test_cli_without_keys_sends_nothing(settings, fake, five, cli_env, monkeypatch):
    monkeypatch.setenv("APCA_API_KEY_ID", "")  # empty is unset, and wins over ./.env
    out = CliRunner().invoke(app, ["alpaca", "backfill", "bars"])
    assert out.exit_code == 1
    assert "error:" in out.output
    assert fake.requests == []
