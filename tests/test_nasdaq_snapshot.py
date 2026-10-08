"""Nasdaq Trader symbol directory snapshots (job `symbols`, table `symbol_snapshot`; C2, C9).

`tests/fixtures/nasdaqtrader/{nasdaqlisted,otherlisted}.txt` are
https://www.nasdaqtrader.com/dynamic/SymDir/{nasdaqlisted,otherlisted}.txt as fetched 2026-10-08
(5 627 and 7 663 rows, created 10/08/2026 03:03 ET), trimmed to the header line, a few rows kept
byte for byte (CRLF) and the `File Creation Time` trailer exactly. nasdaqlisted keeps two ETFs
(AAAP, QQQ), a deficient issuer (AACG, financial status D), AAPL (round lot 40) and two test
issues (ZAZZT, ZXYZ.A); otherlisted keeps an ETF (AAA, SPY), a unit (AAC.U), a warrant (AAC.W),
a preferred (ABR$D), a right (AIIA.R), a class (BRK.B) and a test issue (ATEST).
`nasdaqlisted_truncated.txt` is the first 300 bytes of the real nasdaqlisted.txt: cut mid-row,
no trailer. Later files in these tests are those files edited. No request leaves the process.
"""

import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from usfin import alerts, http, jobs, ratelimit, raw, scheduler, store
from usfin.settings import load_settings
from usfin.sources import nasdaqtrader

FIXTURES = Path(__file__).parent / "fixtures" / "nasdaqtrader"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
SNAPSHOT_AT = datetime(2026, 10, 8, 7, 3, tzinfo=UTC).replace(tzinfo=None)  # 03:03 EDT


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


class Clock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def naive(self) -> datetime:
        return self.t.replace(tzinfo=None)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(utc(2026, 10, 8, 11, 0))  # Thu 07:00 EDT, an NYSE day
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
        self.body = {f: fixture(f"{f}.txt") for f in nasdaqtrader.FILES}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        name = request.url.path.rsplit("/", 1)[-1].removesuffix(".txt")
        return httpx.Response(200, content=self.body[name], headers={"content-type": "text/plain"})

    def edit(self, file: str, fn) -> None:
        self.body[file] = fn(self.body[file].decode("ascii")).encode("ascii")


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


def _run(settings, clock, when=None):
    if when is not None:
        clock.t = when
    return jobs.run_job(
        "symbols", settings=settings, registry=_registry(), clock=clock, chain=False
    )


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


COLUMNS = (
    "file, snapshot_at, symbol, exchange, name, etf, round_lot, financial_status,"
    " market_category, cqs_symbol, nasdaq_symbol"
)


def _snapshot(settings):
    rows = _query(settings, f"SELECT {COLUMNS} FROM symbol_snapshot ORDER BY ALL")
    return {(r[0], r[1], r[2]): r[3:] for r in rows}


# --- parsing --------------------------------------------------------------------------------


def test_nasdaqlisted_parses_with_its_column_map():
    snap = nasdaqtrader.parse(fixture("nasdaqlisted.txt"), "nasdaqlisted")
    assert snap.snapshot_at == SNAPSHOT_AT
    assert snap.test_issues == 2  # ZAZZT, ZXYZ.A
    by = {r.symbol: r for r in snap.rows}
    assert list(by) == ["AAAP", "AACG", "AAPL", "QQQ"]
    aaap = by["AAAP"]
    assert (aaap.exchange, aaap.name, aaap.etf, aaap.round_lot) == (
        "Q", "Pacer Barings CLO Market Flex ETF", True, 100,
    )  # fmt: skip
    assert (aaap.market_category, aaap.financial_status) == ("G", "N")
    assert (aaap.cqs_symbol, aaap.nasdaq_symbol) == (None, None)
    assert by["AACG"].financial_status == "D"
    assert by["AAPL"].round_lot == 40 and by["AAPL"].etf is False
    assert by["QQQ"].etf is True


def test_otherlisted_parses_with_its_column_map():
    snap = nasdaqtrader.parse(fixture("otherlisted.txt"), "otherlisted")
    assert snap.snapshot_at == SNAPSHOT_AT
    assert snap.test_issues == 1  # ATEST
    by = {r.symbol: r for r in snap.rows}
    assert list(by) == ["A", "AAA", "AAC.U", "AAC.W", "ABR$D", "AIIA.R", "BRK.B", "SPY"]
    abr = by["ABR$D"]
    assert (abr.exchange, abr.cqs_symbol, abr.nasdaq_symbol) == ("N", "ABRpD", "ABR-D")
    assert (abr.market_category, abr.financial_status) == (None, None)
    assert (by["AAC.W"].cqs_symbol, by["AAC.W"].nasdaq_symbol) == ("AAC.WS", "AAC+")
    assert by["AIIA.R"].nasdaq_symbol == "AIIA^"
    assert by["SPY"].exchange == "P" and by["SPY"].etf is True and by["SPY"].round_lot == 40
    assert by["BRK.B"].name == "Berkshire Hathaway Inc. New Common Stock"


def test_a_file_without_its_trailer_is_rejected():
    with pytest.raises(nasdaqtrader.SnapshotRejected, match="File Creation Time"):
        nasdaqtrader.parse(fixture("nasdaqlisted_truncated.txt"), "nasdaqlisted")
    full = fixture("otherlisted.txt").decode("ascii")
    cut = full[: full.index("File Creation Time")]  # every row there, only the trailer gone
    with pytest.raises(nasdaqtrader.SnapshotRejected):
        nasdaqtrader.parse(cut.encode("ascii"), "otherlisted")


def test_a_header_missing_a_mapped_column_is_an_error():
    body = fixture("otherlisted.txt").replace(b"|CQS Symbol|", b"|CQS Sym|")
    with pytest.raises(nasdaqtrader.SnapshotError, match="CQS Symbol"):
        nasdaqtrader.parse(body, "otherlisted")


# --- job symbols ----------------------------------------------------------------------------


def test_the_job_stores_both_files_as_snapshots(settings, fake, clock):
    result = _run(settings, clock)
    assert result.status == "ok", result.error
    assert result.rows_upserted == 4 + 8
    rows = _snapshot(settings)
    assert len(rows) == 12
    assert {(f, s) for f, _, s in rows if s in ("ZAZZT", "ZXYZ.A", "ATEST")} == set()
    assert {at for _, at, _ in rows} == {SNAPSHOT_AT}
    assert rows[("nasdaqlisted", SNAPSHOT_AT, "AAPL")] == (
        "Q", "Apple Inc. - Common Stock", False, 40, "N", "Q", None, None,
    )  # fmt: skip
    assert rows[("otherlisted", SNAPSHOT_AT, "ABR$D")][4:] == (None, None, "ABRpD", "ABR-D")
    assert _query(
        settings, "SELECT source, dataset, http_status FROM raw_response ORDER BY dataset"
    ) == [
        ("nasdaqtrader", "nasdaq_nasdaqlisted", 200),
        ("nasdaqtrader", "nasdaq_otherlisted", 200),
    ]
    with store.session(settings.db_path, wait=0) as con:
        assert jobs.get_watermarks(con, "symbols") == {
            "nasdaqlisted": "2026-10-08T07:03:00",
            "otherlisted": "2026-10-08T07:03:00",
        }
    assert {r.headers["User-Agent"].split("/")[0] for r in fake.requests} == {"usfin"}


def test_a_second_run_of_the_same_snapshot_changes_nothing(settings, fake, clock):
    _run(settings, clock)
    before = _snapshot(settings)
    again = _run(settings, clock, when=utc(2026, 10, 8, 15, 0))
    assert again.status == "ok" and again.rows_upserted == 0
    assert _snapshot(settings) == before
    assert _query(settings, "SELECT rule FROM alert") == []


def test_a_newer_snapshot_is_stored_beside_the_old_one(settings, fake, clock):
    _run(settings, clock)
    for f in nasdaqtrader.FILES:
        fake.edit(f, lambda s: s.replace("1008202603:03", "1009202603:02"))
    result = _run(settings, clock, when=utc(2026, 10, 9, 11, 0))
    assert result.status == "ok" and result.rows_upserted == 12
    times = _query(settings, "SELECT DISTINCT snapshot_at FROM symbol_snapshot ORDER BY 1")
    assert times == [(SNAPSHOT_AT,), (utc(2026, 10, 9, 7, 2).replace(tzinfo=None),)]


def test_a_file_without_its_trailer_fails_its_key_alerts_and_stores_nothing(settings, fake, clock):
    fake.body["nasdaqlisted"] = fixture("nasdaqlisted_truncated.txt")
    result = _run(settings, clock)
    assert result.status == "failed"
    assert set(result.key_errors) == {"nasdaqlisted"}
    assert "File Creation Time" in result.key_errors["nasdaqlisted"]
    assert _query(settings, "SELECT DISTINCT file FROM symbol_snapshot") == [("otherlisted",)]
    with store.session(settings.db_path, wait=0) as con:
        assert set(jobs.get_watermarks(con, "symbols")) == {"otherlisted"}
        [alert] = alerts.list_alerts(con)
    assert (alert["job"], alert["rule"], alert["subject"]) == (
        "symbols", "snapshot_rejected", "nasdaqlisted",
    )  # fmt: skip
    assert alert["detail"]["bytes"] == 300
    # raw first: the rejected body is archived all the same
    assert ("nasdaq_nasdaqlisted",) in _query(settings, "SELECT dataset FROM raw_response")


def test_reparse_rebuilds_the_same_snapshots_without_a_request(settings, fake, clock):
    fake.body["nasdaqlisted"] = fixture("nasdaqlisted_truncated.txt")
    _run(settings, clock)
    fake.body["nasdaqlisted"] = fixture("nasdaqlisted.txt")
    _run(settings, clock, when=utc(2026, 10, 8, 15, 0))
    table = _snapshot(settings)
    assert len(table) == 12
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM symbol_snapshot")
        result = jobs.reparse(con, nasdaqtrader.JOB, settings=settings)
    assert len(result.raw_ids) == 4  # the rejected body is skipped, not a reparse failure
    assert _snapshot(settings) == table
    assert len(fake.requests) == 4


def test_a_row_count_swing_between_snapshots_alerts_but_not_the_first(settings, fake, clock):
    _run(settings, clock)
    assert _query(settings, "SELECT rule FROM alert") == []
    fake.edit(
        "otherlisted",
        lambda s: "".join(
            line for line in s.splitlines(keepends=True) if not line.startswith("SPY|")
        ).replace("1008202603:03", "1009202603:02"),
    )
    fake.edit("nasdaqlisted", lambda s: s.replace("1008202603:03", "1009202603:02"))
    assert _run(settings, clock, when=utc(2026, 10, 9, 11, 0)).status == "ok"
    assert _query(settings, "SELECT rule, subject FROM alert") == [
        ("row_count_swing", "otherlisted")
    ]


def test_the_job_is_scheduled_at_0700_on_nyse_days(settings):
    entry = scheduler.load_schedule(settings.config_dir)["symbols"]
    assert (entry.cron, entry.calendar) == ("0 7 * * *", "nyse")
    assert "usfin.sources.nasdaqtrader" in jobs.SOURCE_MODULES
