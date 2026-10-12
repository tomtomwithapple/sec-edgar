"""Alpaca corporate actions: versioned actions over a forward window (job `alpaca_corp_actions`;
A1, A5, A9).

Fixtures in `tests/fixtures/alpaca/corp_actions/` are whole responses of GET
https://data.alpaca.markets/v1/corporate-actions as fetched 2026-10-12, byte for byte, each
narrowed by `symbols` so it stays small:

- `aapl_2020-08.json`     AAPL 2020-08-01..09-30: cash dividend (ex 2020-08-07, processed on its
                          payable date 08-13) and the 4:1 forward split (process 2020-08-31)
- `ctnt_2026-09.json`     CTNT reverse split 150:1 with a new CUSIP
- `fb_meta_2022-06.json`  the FB -> META name change (process_date only)
- `twtr_2022-10.json`     the TWTR cash merger (only reachable as `acquiree_symbol`)
- `sivbq_2023-2026.json`  SIVBQ worthless removal
- `atai_2026-09.json`     ATAI stock-and-cash merger (acquirer and acquiree fields, cash rate)
- `aapl_2016-2026_p{1,2}.json`  AAPL 2016-01-01..2026-10-12 with `limit=40`: page 1 (39
                          dividends + the split) carries a `next_page_token`, page 2 (4) none
- `empty.json`            a window with no actions

The response groups actions by type (`cash_dividends`, `forward_splits`, ...); an action object
has no type field of its own. Fake keys only; no request leaves the process.
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
from usfin.sources.alpaca import corp_actions

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca" / "corp_actions"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
KEY_ID, SECRET = "PKTESTKEYID0000000000", "test-secret-0000000000000000000000000000"
AAPL_DIV = "d09386ae-0c2b-4280-8aa0-295e545354b1"
AAPL_SPLIT = "d209b6c2-9231-474c-a80e-1a79dacadbb2"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def naive(t: datetime) -> datetime:
    return t.replace(tzinfo=None)


D1, D2, D3 = (utc(2026, 10, d, 11, 45) for d in (12, 13, 14))  # 07:45 EDT


def body(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


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
    """Answers a window: the first request (no `page_token`) with `pages[0]`, a request whose
    `page_token` is the previous page's `next_page_token` with the next page."""

    def __init__(self):
        self.pages: list[bytes] = [body("empty.json")]
        self.status = 200
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        token = request.url.params.get("page_token")
        i = 0
        while token is not None and json.loads(self.pages[i])["next_page_token"] != token:
            i += 1
        if token is not None:
            i += 1
        return httpx.Response(
            self.status,
            content=self.pages[i],
            headers={"content-type": "application/json; charset=UTF-8"},
        )


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.get(url__startswith=corp_actions.URL).mock(side_effect=f)
        yield f


def _registry():
    reg = jobs.Registry()
    reg.register(corp_actions.JOB)
    return reg


def _run(settings, clock, when, *, ok=True):
    clock.t = when
    result = jobs.run_job(
        "alpaca_corp_actions", settings=settings, registry=_registry(), clock=clock, chain=False
    )
    if ok:
        assert result.status == "ok", result
    return result


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _one(settings, action_id, columns):
    rows = _query(
        settings,
        f"SELECT {', '.join(c if '::' in c else f'"{c}"' for c in columns)}"
        " FROM alpaca_corp_action WHERE id = ?",
        [action_id],
    )
    assert len(rows) == 1, rows
    return dict(zip(columns, rows[0], strict=True))


def _watermark(settings):
    return _query(
        settings, "SELECT value FROM watermark WHERE job = 'alpaca_corp_actions' AND key = ?",
        [corp_actions.KEY],
    )  # fmt: skip


def _all(settings):
    return _query(
        settings,
        "SELECT id, type, payload::VARCHAR, first_seen_at, raw_id FROM alpaca_corp_action"
        " ORDER BY id, first_seen_at",
    )


# --- request and window ---------------------------------------------------------------------


def test_the_first_run_asks_today_minus_30_to_today_plus_90_with_the_keys(settings, fake, clock):
    _run(settings, clock, D1)
    assert len(fake.requests) == 1
    req = fake.requests[0]
    assert req.url.host == "data.alpaca.markets"
    assert req.url.path == "/v1/corporate-actions"
    # no `types`: every type; start/end filter process_date
    assert dict(req.url.params) == {"start": "2026-09-12", "end": "2027-01-10", "limit": "1000"}
    assert req.headers["APCA-API-KEY-ID"] == KEY_ID
    stored = _query(settings, "SELECT source, dataset, http_status FROM raw_response")
    assert stored == [("alpaca", "alpaca_corp_actions", 200)]


def test_later_runs_start_30_days_before_the_watermark(settings, fake, clock):
    fake.pages = [body("ctnt_2026-09.json")]  # process 2026-09-28
    _run(settings, clock, D1)
    assert _watermark(settings) == [("2026-09-28",)]
    _run(settings, clock, D3)
    assert dict(fake.requests[-1].url.params) == {
        "start": "2026-08-29", "end": "2027-01-12", "limit": "1000",
    }  # fmt: skip


def test_the_window_function():
    today = date(2026, 10, 12)
    assert corp_actions.window(today, None) == (date(2026, 9, 12), date(2027, 1, 10))
    assert corp_actions.window(today, "2026-10-01") == (date(2026, 9, 1), date(2027, 1, 10))


def test_pages_are_followed_by_next_page_token(settings, fake, clock):
    fake.pages = [body("aapl_2016-2026_p1.json"), body("aapl_2016-2026_p2.json")]
    result = _run(settings, clock, D1)
    assert len(fake.requests) == 2
    first, second = (dict(r.url.params) for r in fake.requests)
    assert "page_token" not in first
    token = json.loads(fake.pages[0])["next_page_token"]
    assert second == {**first, "page_token": token}
    assert result.rows_upserted == 44
    assert _query(settings, "SELECT count(*) FROM raw_response") == [(2,)]
    assert _query(settings, "SELECT count(DISTINCT raw_id), count(*) FROM alpaca_corp_action") == [
        (2, 44)
    ]
    assert _query(
        settings, "SELECT type, count(*) FROM alpaca_corp_action GROUP BY 1 ORDER BY 1"
    ) == [("cash_dividend", 43), ("forward_split", 1)]


# --- promoted columns per type --------------------------------------------------------------


COLS = (
    "type", "symbol", "old_symbol", "new_symbol", "acquiree_symbol", "acquirer_symbol",
    "cusip", "old_cusip", "new_cusip", "acquiree_cusip", "acquirer_cusip",
    "ex_date", "record_date", "payable_date", "process_date", "effective_date",
    "rate", "old_rate", "new_rate", "cash_rate", "acquiree_rate", "acquirer_rate",
    "special", "foreign",
)  # fmt: skip


def _load(settings, fake, clock, name):
    fake.pages = [body(name)]
    _run(settings, clock, D1)
    return json.loads(body(name))["corporate_actions"]


def _expect(**set_):
    return {c: set_.get(c) for c in COLS}


def test_a_cash_dividend_and_a_forward_split(settings, fake, clock):
    _load(settings, fake, clock, "aapl_2020-08.json")
    assert _one(settings, AAPL_DIV, COLS) == _expect(
        type="cash_dividend", symbol="AAPL", cusip="037833100",
        ex_date=date(2020, 8, 7), record_date=date(2020, 8, 10),
        payable_date=date(2020, 8, 13), process_date=date(2020, 8, 13),
        rate=0.82, special=False, foreign=False,
    )  # fmt: skip
    assert _one(settings, AAPL_SPLIT, COLS) == _expect(
        type="forward_split", symbol="AAPL", cusip="037833100",
        ex_date=date(2020, 8, 31), record_date=date(2020, 8, 24),
        payable_date=date(2020, 8, 28), process_date=date(2020, 8, 31),
        old_rate=1.0, new_rate=4.0,
    )  # fmt: skip


def test_a_reverse_split_with_a_new_cusip(settings, fake, clock):
    _load(settings, fake, clock, "ctnt_2026-09.json")
    assert _one(settings, "09f25252-61c2-4a7b-81aa-022181904ea1", COLS) == _expect(
        type="reverse_split", symbol="CTNT", old_cusip="16307X301", new_cusip="16307X400",
        ex_date=date(2026, 9, 28), record_date=date(2026, 9, 28),
        payable_date=date(2026, 9, 28), process_date=date(2026, 9, 28),
        old_rate=150.0, new_rate=1.0,
    )  # fmt: skip


def test_a_name_change(settings, fake, clock):
    _load(settings, fake, clock, "fb_meta_2022-06.json")
    assert _one(settings, "ed6f16b1-fc46-443e-8dba-898ed9007a7a", COLS) == _expect(
        type="name_change", old_symbol="FB", new_symbol="META",
        old_cusip="30303M102", new_cusip="30303M102", process_date=date(2022, 6, 9),
    )  # fmt: skip


def test_a_cash_merger(settings, fake, clock):
    _load(settings, fake, clock, "twtr_2022-10.json")
    assert _one(settings, "0cfca8cd-6187-47d5-8ee0-45fde44d03e5", COLS) == _expect(
        type="cash_merger", acquiree_symbol="TWTR", acquiree_cusip="90184L102",
        effective_date=date(2022, 10, 28), process_date=date(2022, 10, 28), rate=54.2,
    )  # fmt: skip


def test_a_worthless_removal(settings, fake, clock):
    _load(settings, fake, clock, "sivbq_2023-2026.json")
    assert _one(settings, "70d10d5c-037a-425f-a8d5-f8785a9bfbf4", COLS) == _expect(
        type="worthless_removal", symbol="SIVBQ", cusip="78486Q101",
        process_date=date(2024, 11, 8),
    )  # fmt: skip


def test_a_stock_and_cash_merger(settings, fake, clock):
    _load(settings, fake, clock, "atai_2026-09.json")
    assert _one(settings, "fa73619f-6ad3-4c6c-922f-4ca90970a957", COLS) == _expect(
        type="stock_and_cash_merger",
        acquiree_symbol="ATAI", acquiree_cusip="04650F101", acquiree_rate=1.0,
        acquirer_symbol="046CVR015", acquirer_cusip="046CVR015", acquirer_rate=1.0,
        cash_rate=6.75, effective_date=date(2026, 9, 11),
        payable_date=date(2026, 9, 14), process_date=date(2026, 9, 14),
    )  # fmt: skip


def test_payload_is_the_full_action_object_and_first_seen_at_the_fetch(settings, fake, clock):
    groups = _load(settings, fake, clock, "aapl_2020-08.json")
    split = groups["forward_splits"][0]
    row = _one(settings, AAPL_SPLIT, ("payload::VARCHAR", "first_seen_at", "raw_id"))
    assert json.loads(row["payload::VARCHAR"]) == split  # due_bill_redemption_date kept too
    assert row["first_seen_at"] == naive(D1)  # the raw response's fetched_at
    assert row["raw_id"] == 1


# --- versions -------------------------------------------------------------------------------


def test_an_identical_refetch_inserts_nothing(settings, fake, clock):
    fake.pages = [body("aapl_2020-08.json")]
    assert _run(settings, clock, D1).rows_upserted == 2
    before = _all(settings)
    assert _run(settings, clock, D2).rows_upserted == 0
    assert _all(settings) == before
    assert _query(settings, "SELECT count(*) FROM raw_response") == [(2,)]  # archived anyway


def _with_rate(rate) -> bytes:
    data = json.loads(body("aapl_2020-08.json"))
    data["corporate_actions"]["cash_dividends"][0]["rate"] = rate
    return json.dumps(data).encode()


def test_a_changed_payload_inserts_a_version_and_latest_picks_it(settings, fake, clock):
    fake.pages = [body("aapl_2020-08.json")]
    _run(settings, clock, D1)
    fake.pages = [_with_rate(0.83)]
    assert _run(settings, clock, D2).rows_upserted == 1  # the split is unchanged
    versions = _query(
        settings,
        "SELECT rate, first_seen_at FROM alpaca_corp_action WHERE id = ? ORDER BY first_seen_at",
        [AAPL_DIV],
    )
    assert versions == [(0.82, naive(D1)), (0.83, naive(D2))]
    latest = _query(
        settings, "SELECT id, rate, first_seen_at FROM alpaca_corp_action_latest ORDER BY id"
    )
    assert latest == [(AAPL_DIV, 0.83, naive(D2)), (AAPL_SPLIT, None, naive(D1))]
    # back to the first payload: it differs from the newest version, so it is a third version
    fake.pages = [body("aapl_2020-08.json")]
    assert _run(settings, clock, D3).rows_upserted == 1
    assert _query(
        settings, "SELECT rate FROM alpaca_corp_action_latest WHERE id = ?", [AAPL_DIV]
    ) == [(0.82,)]


def test_no_duplicate_id_and_payload_after_any_sequence_of_runs(settings, fake, clock):
    for when, pages in (
        (D1, [body("aapl_2016-2026_p1.json"), body("aapl_2016-2026_p2.json")]),
        (D2, [body("aapl_2020-08.json")]),
        (D3, [_with_rate(0.83)]),
    ):
        fake.pages = pages
        _run(settings, clock, when)
    dupes = _query(
        settings,
        "SELECT id, payload::VARCHAR, count(*) FROM alpaca_corp_action GROUP BY 1, 2"
        " HAVING count(*) > 1",
    )
    assert dupes == []


# --- watermark ------------------------------------------------------------------------------


def test_the_watermark_is_the_newest_process_date_up_to_today(settings, fake, clock):
    fake.pages = [body("aapl_2020-08.json")]  # dividend processed 08-13, split 08-31
    _run(settings, clock, utc(2020, 8, 20, 11, 45))
    assert _watermark(settings) == [("2020-08-13",)]  # the split is future-dated: stored anyway
    assert _query(settings, "SELECT count(*) FROM alpaca_corp_action") == [(2,)]
    assert dict(fake.requests[-1].url.params)["end"] == "2020-11-18"
    _run(settings, clock, utc(2020, 9, 5, 11, 45))
    assert _watermark(settings) == [("2020-08-31",)]


def test_a_window_with_nothing_up_to_today_keeps_the_watermark(settings, fake, clock):
    fake.pages = [body("ctnt_2026-09.json")]
    _run(settings, clock, D1)
    fake.pages = [body("empty.json")]
    assert _run(settings, clock, D2).rows_upserted == 0
    assert _watermark(settings) == [("2026-09-28",)]


def test_the_watermark_never_moves_back(settings, fake, clock):
    fake.pages = [body("ctnt_2026-09.json")]
    _run(settings, clock, D1)
    fake.pages = [body("atai_2026-09.json")]  # process 2026-09-14
    _run(settings, clock, D2)
    assert _watermark(settings) == [("2026-09-28",)]


# --- failures -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    [b"not json", b"[]", b'{"next_page_token": null}', b'{"corporate_actions": []}',
     b'{"corporate_actions": {"cash_dividends": [{"symbol": "X"}]}, "next_page_token": null}'],
)  # fmt: skip
def test_a_body_that_is_not_the_action_list_fails_and_stores_nothing(settings, fake, clock, bad):
    fake.pages = [bad]
    result = _run(settings, clock, D1, ok=False)
    assert result.status == "failed"
    assert _query(settings, "SELECT count(*) FROM alpaca_corp_action") == [(0,)]
    assert _query(settings, "SELECT count(*) FROM raw_response") == [(1,)]  # archived anyway
    assert _watermark(settings) == []


def test_a_non_200_answer_fails_the_run(settings, fake, clock):
    fake.status = 422
    fake.pages = [b'{"message": "invalid start"}']
    result = _run(settings, clock, D1, ok=False)
    assert result.status == "failed"
    assert "422" in result.error
    assert _watermark(settings) == []


def test_a_failed_second_page_stores_nothing_from_the_first(settings, fake, clock):
    fake.pages = [body("aapl_2016-2026_p1.json"), b"oops"]
    result = _run(settings, clock, D1, ok=False)
    assert result.status == "failed"
    assert _query(settings, "SELECT count(*) FROM raw_response") == [(2,)]
    assert _watermark(settings) == []


# --- reparse (A1) ---------------------------------------------------------------------------


def test_reparse_rebuilds_every_version_with_no_request(settings, fake, clock):
    fake.pages = [body("aapl_2016-2026_p1.json"), body("aapl_2016-2026_p2.json")]
    _run(settings, clock, D1)
    fake.pages = [_with_rate(0.83)]
    _run(settings, clock, D2)
    fake.pages = [body("aapl_2020-08.json")]
    _run(settings, clock, D3)
    built = _all(settings)
    assert len(built) == 46  # 44 + the 0.83 version + the 0.82 version again
    n = len(fake.requests)
    with store.session(settings.db_path, wait=0) as con:
        jobs.reparse(con, corp_actions.JOB, settings=settings)
    assert _all(settings) == built
    with store.session(settings.db_path, wait=0) as con:
        con.execute("DELETE FROM alpaca_corp_action")
        jobs.reparse(con, corp_actions.JOB, settings=settings)
    assert _all(settings) == built
    assert len(fake.requests) == n


# --- table, schedule, registry (A9) ---------------------------------------------------------


def test_the_table_has_no_primary_key(settings):
    with store.session(settings.db_path, wait=0) as con:
        pk = con.execute(
            "SELECT count(*) FROM duckdb_constraints() WHERE table_name = 'alpaca_corp_action'"
            " AND constraint_type = 'PRIMARY KEY'"
        ).fetchone()
    assert pk == (0,)


def test_schedule_entry_daily_0745_on_nyse_days_with_stale_rule():
    entry = yaml.safe_load((REPO_CONFIG / "schedule.yaml").read_text())["jobs"][
        "alpaca_corp_actions"
    ]
    assert entry["cron"] == "45 7 * * *"
    assert entry["calendar"] == "nyse"
    assert entry["due"] == "nyse"
    assert entry["alerts"] == {"stale_watermark": {"n": 2}}
    rules = alerts.load_rules(REPO_CONFIG)["alpaca_corp_actions"]
    assert rules.params_for(corp_actions.KEY) == {"stale_watermark": {"n": 2}}


def test_the_job_is_in_the_registry_and_its_raw_dataset_is_parsed():
    registry = jobs.load_registry()
    assert "alpaca_corp_actions" in registry
    job = registry.get("alpaca_corp_actions")
    assert set(job.parsers) == {"alpaca_corp_actions"}
    assert job.key_list(None) == [corp_actions.KEY]
    assert job.versioned == ("alpaca_corp_action",)
