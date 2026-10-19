"""Job `alpaca_bars`: Alpaca's unadjusted daily bars into `alpaca_bar`, versioned (plan
alpaca-market-data: How > Bars; A1, A2).

GET `URL?symbols&start&end` with `PARAMS` pinned (`feed=sip`: from 2016-01-04; `adjustment=raw`;
`asof=-`: what traded under the symbol then, so META before 2022-06-09 is the Roundhill ETF, not
Meta) returns `{"bars": {"AAPL": [{"t", "o", "h", "l", "c", "v", "n", "vw"}, ...], ...},
"next_page_token": ...}`; a page holds at most `limit` bars across its symbols and may cut a
symbol's bars in two, and the next page is the same request plus `page_token`. Symbols go in
batches of `batch_size` (`config/alpaca.yaml`). A symbol Alpaca has no bars for (TWTR today,
`ZZZZQQ`) is simply absent, but one it calls invalid (CUSIP-like `09175M705`, `BF/B`, `ABC-U`)
fails the whole request with 400 `{"message": "invalid symbol: X"}`: the batch is asked again
without X (the 400 stays archived; its parse holds no bars), and the dropped symbols are
returned.

Every page is archived first (source `alpaca`, dataset `alpaca_bars`). A non-200 page other than
an invalid symbol, or a body that is not the bar list, fails the key before anything is parsed.
A bar is one row: `date` = the New York date of `t` (04:00Z or 05:00Z), `COMPARED` its values.
A bar equal in every compared column (exact, NULL = NULL) to the newest stored version of its
(symbol, date) inserts nothing; any other inserts a version with `first_seen_at` = the page's
`fetched_at`. Reparse replays every page in `fetched_at` order, so the versions are rebuilt as
they were.

What a request learnt about a symbol goes into `alpaca_bar_symbol`, from the archived responses
so reparse rebuilds it: `invalid` from the 400's body, `history` for every symbol of a request
from `backfill_start` (or earlier) with no `end` whose last page (no `next_page_token`) arrived,
bars or none.

The daily run (S7) asks two sets, `batch_size` symbols a request, never with an `end`:
(a) every active universe symbol (`asset_active`) from watermark − `lookback_days` (today −
`lookback_days` with no watermark yet, so a first run is never a whole backfill), which also
catches up days the job missed; (b) up to `new_symbol_cap` universe symbols whose history was
never asked, from `backfill_start`: active ones first, then by symbol, so new listings come before
the delisted tail and every run moves on through the rest. "Never asked", not "no bars": (a)
gives every active symbol a few days of bars, and a well-formed symbol Alpaca has nothing for
(`X.RT`, TWTR today) is absent from every answer, so "no bars" would never let (b) reach
the former and would ask the latter every day. A symbol of (b) is left out of this run's (a); a
symbol marked `invalid` is in neither. The watermark is the newest bar date seen, never moving
back. S8's backfill (`backfill.bars_history`) asks the same to-do list (`unasked`) all at once,
a batch per DB session.
"""

import json
import logging
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timedelta
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

import duckdb

from usfin import alerts, jobs, raw
from usfin import http as usfin_http
from usfin.sources import alpaca

URL = "https://data.alpaca.markets/v2/stocks/bars"
SOURCE = "alpaca"
DATASET = "alpaca_bars"
JOB_NAME = "alpaca_bars"
KEY = "date"
TABLE = "alpaca_bar"
SYMBOLS = "alpaca_bar_symbol"
INVALID_STATUS, HISTORY_STATUS = "invalid", "history"
PARAMS = {"timeframe": "1Day", "feed": "sip", "adjustment": "raw", "asof": "-", "limit": "10000"}
MAX_PAGES = 1000
"""A batch that pages on past this is a loop, not data (100 symbols × 10.8 y ≈ 28 pages)."""
COMPARED = ("open", "high", "low", "close", "volume", "trade_count", "vwap")
FIELDS = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "n": "trade_count",
          "vw": "vwap"}  # fmt: skip
ET = ZoneInfo("America/New_York")
INVALID = "invalid symbol: "
log = logging.getLogger("usfin.sources.alpaca.bars")


class BarsError(Exception):
    """A response that is not Alpaca's bar list."""


def bar_date(t: str) -> date:
    """The New York date of a bar's `t` (`2016-01-04T05:00:00Z` → 2016-01-04)."""
    try:
        return datetime.fromisoformat(t).astimezone(ET).date()
    except (TypeError, ValueError):
        raise BarsError(f"bar time is not ISO 8601: {t!r}") from None


def parse(body: bytes) -> tuple[list[dict], str | None]:
    """The page's bars as rows (`symbol`, `date`, `COMPARED`) and its next page token. A
    (symbol, date) listed twice on a page is kept once (its first bar)."""
    try:
        data = json.loads(body)
    except ValueError as exc:
        raise BarsError(f"bars page is not JSON ({len(body)} bytes)") from exc
    per_symbol = data.get("bars") if isinstance(data, dict) else None
    if not isinstance(per_symbol, dict):
        raise BarsError(f"not a bars page: {body[:80]!r}")
    rows: dict[tuple[str, date], dict] = {}
    for symbol, bars in per_symbol.items():
        if not isinstance(bars, list):
            raise BarsError(f"{symbol}: expected a list, got {type(bars).__name__}")
        for n, bar in enumerate(bars):
            if not isinstance(bar, dict) or "t" not in bar:
                raise BarsError(f"{symbol}[{n}] lacks t: {bar!r:.80}")
            for f in ("v", "n"):
                if bar.get(f) is not None and (
                    isinstance(bar[f], bool) or not isinstance(bar[f], int)
                ):
                    raise BarsError(f"{symbol}[{n}] {f} is not a whole number: {bar[f]!r}")
            d = bar_date(bar["t"])
            row = {"symbol": symbol, "date": d.isoformat()}
            row.update({col: bar.get(f) for f, col in FIELDS.items()})
            rows.setdefault((symbol, d), row)
    return list(rows.values()), data.get("next_page_token") or None


def invalid_symbol(status: int, body: bytes) -> str | None:
    """The symbol a 400 names as invalid, else None."""
    if status != 400:
        return None
    try:
        message = json.loads(body).get("message", "")
    except (ValueError, AttributeError):
        return None
    return message.removeprefix(INVALID) if message.startswith(INVALID) else None


_SCHEMA = json.dumps(
    [
        {
            "symbol": "VARCHAR",
            "date": "DATE",
            **{c: "DOUBLE" for c in ("open", "high", "low", "close", "vwap")},
            "volume": "BIGINT",
            "trade_count": "BIGINT",
        }
    ]
)


def note_symbols(
    con: duckdb.DuckDBPyConnection, symbols: Iterable[str], status: str, at, raw_id: int
) -> int:
    """Mark `symbols` `status` in `alpaca_bar_symbol` where not marked yet; return rows added."""
    items = list(dict.fromkeys(symbols))
    if not items:
        return 0
    return con.execute(
        f"INSERT INTO {SYMBOLS} SELECT t.symbol, ?, ?, ? FROM unnest(?::VARCHAR[]) t(symbol)"
        f" WHERE NOT EXISTS (SELECT 1 FROM {SYMBOLS} k WHERE k.symbol = t.symbol"
        " AND k.status = ?)",
        [status, at, raw_id, items, status],
    ).fetchone()[0]


def whole_history(url: str, backfill_start: date) -> list[str]:
    """The symbols of a request asking their whole history (from `backfill_start` or earlier,
    no `end`), else []."""
    params = parse_qs(urlsplit(url).query)
    start = params.get("start", [""])[0]
    if "end" in params or not start or date.fromisoformat(start) > backfill_start:
        return []
    return [s for s in params.get("symbols", [""])[0].split(",") if s]


def store_page(
    con: duckdb.DuckDBPyConnection,
    raw_id: int,
    *,
    raw_dir,
    backfill_start: date = alpaca.AlpacaConfig.backfill_start,
) -> tuple[list[dict], int]:
    """Insert the page's new bar versions; return (its bars, rows inserted). An invalid-symbol
    400 holds no bars and marks its symbol `invalid`; the last page of a whole-history request
    marks its symbols `history`."""
    found = con.execute(
        "SELECT http_status, fetched_at, url FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    status, at, url = found
    if status != 200:
        if status == 400:
            with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
                bad = invalid_symbol(status, fh.read())
            if bad is not None:
                note_symbols(con, [bad], INVALID_STATUS, at, raw_id)
        return [], 0
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        bars, token = parse(fh.read())
    if token is None:
        note_symbols(con, whole_history(url, backfill_start), HISTORY_STATUS, at, raw_id)
    if not bars:
        return bars, 0
    cols = ("symbol", "date", *COMPARED)
    same = " AND ".join(f"c.{c} IS NOT DISTINCT FROM n.{c}" for c in COMPARED)
    # one JSON text parameter, not bound row values (see fiscal.upsert_page); the newest versions
    # are read for this page's symbols and dates only, not the whole ~30 M-row table
    inserted = con.execute(
        f"INSERT INTO {TABLE} ({', '.join(cols)}, first_seen_at, raw_id)"
        " WITH n AS (SELECT unnest(json_transform(?, ?), recursive := true)),"
        f" c AS (SELECT * FROM {TABLE}"
        "  WHERE symbol IN (SELECT DISTINCT symbol FROM n)"
        "  AND date BETWEEN (SELECT min(date) FROM n) AND (SELECT max(date) FROM n)"
        "  QUALIFY row_number() OVER ("
        "   PARTITION BY symbol, date ORDER BY first_seen_at DESC, raw_id DESC) = 1)"
        f" SELECT {', '.join(f'n.{c}' for c in cols)}, ?, ? FROM n"
        f" WHERE NOT EXISTS (SELECT 1 FROM c WHERE c.symbol = n.symbol AND c.date = n.date"
        f" AND {same})"
        " ORDER BY n.symbol, n.date",
        [json.dumps(bars), _SCHEMA, at, raw_id],
    ).fetchone()[0]
    return bars, inserted


def batches(symbols: Iterable[str], size: int) -> list[list[str]]:
    items = list(dict.fromkeys(symbols))
    return [items[i : i + size] for i in range(0, len(items), size)]


def fetch_batch(
    client: usfin_http.Client, symbols: Sequence[str], start: date, end: date | None = None
) -> tuple[list[int], list[str]]:
    """Archive every page of `symbols`' bars `start`..`end`; return (raw ids, the symbols
    Alpaca called invalid, which were dropped and the batch asked again without them).

    `end` None sends no `end`: Alpaca then answers up to the free plan's delay. A date `end` of
    today is refused at any hour (403 "subscription does not permit querying recent SIP data":
    a date means through that day, which is less than 15 minutes old), so the job sends none."""
    left = list(symbols)
    raw_ids: list[int] = []
    invalid: list[str] = []
    while left:
        params = {**PARAMS, "symbols": ",".join(left), "start": start.isoformat()}
        if end is not None:
            params["end"] = end.isoformat()
        token: str | None = None
        pages = 0
        while True:
            raw_id, response = client.get(
                URL,
                source=SOURCE,
                dataset=DATASET,
                params={**params, "page_token": token} if token else params,
            )
            raw_ids.append(raw_id)
            pages += 1
            bad = invalid_symbol(response.status_code, response.content)
            if bad is not None and token is None and bad in left:
                log.info("alpaca_bars: %s is an invalid symbol, asking again without it", bad)
                invalid.append(bad)
                left.remove(bad)
                break
            if response.status_code != 200:
                raise BarsError(
                    f"{URL} {left[0]}..({len(left)} symbols) {start}..{end} answered"
                    f" {response.status_code}: {response.content[:120]!r}"
                )
            _, token = parse(response.content)
            if token is None:
                return raw_ids, invalid
            if pages >= MAX_PAGES:
                raise BarsError(f"{left[0]}..: still paging after {MAX_PAGES} pages")
    return raw_ids, invalid


# --- job -------------------------------------------------------------------------------------


def unasked(con: duckdb.DuckDBPyConnection, cap: int | None = None) -> list[str]:
    """Universe symbols with no mark (whole history never asked, not invalid), active first,
    then by symbol; at most `cap`. The daily catch-up's (b) and the backfill's to-do list."""
    return [
        s
        for (s,) in con.execute(
            "SELECT symbol FROM alpaca_universe"
            f" WHERE symbol NOT IN (SELECT symbol FROM {SYMBOLS})"
            " GROUP BY symbol ORDER BY bool_or(source = 'asset_active') DESC, symbol"
            + (" LIMIT ?" if cap is not None else ""),
            [cap] if cap is not None else [],
        ).fetchall()
    ]


def select(con: duckdb.DuckDBPyConnection, cap: int) -> tuple[list[str], list[str]]:
    """(a) the active universe symbols a run refreshes, (b) at most `cap` universe symbols whose
    history was never asked (active first, then by symbol); neither holds an invalid symbol and
    (a) leaves out (b)'s."""
    new = unasked(con, cap)
    daily = [
        s
        for (s,) in con.execute(
            "SELECT DISTINCT symbol FROM alpaca_universe WHERE source = 'asset_active'"
            f" AND symbol NOT IN (SELECT symbol FROM {SYMBOLS} WHERE status = ?)"
            " AND symbol NOT IN (SELECT unnest(?::VARCHAR[])) ORDER BY symbol",
            [INVALID_STATUS, new],
        ).fetchall()
    ]
    return daily, new


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    cfg = alpaca.load_config(ctx.settings.config_dir)
    known = ctx.watermarks.get(key)
    since = date.fromisoformat(known) if known else ctx.today
    start = since - timedelta(days=cfg.lookback_days)
    daily, new = select(ctx.con, cfg.new_symbol_cap)
    raw_ids: list[int] = []
    dropped: list[str] = []
    for symbols, begin in ((new, cfg.backfill_start), (daily, start)):
        for batch in batches(symbols, cfg.batch_size):
            ids, invalid = fetch_batch(ctx.client, batch, begin)
            raw_ids += ids
            dropped += invalid
    if dropped:
        log.warning(
            "alpaca_bars: %s invalid symbol(s) dropped and remembered: %s", len(dropped),
            ", ".join(dropped),
        )  # fmt: skip
    log.info(
        "alpaca_bars: %s symbol(s) from %s, %s new symbol(s) from %s, %s page(s)", len(daily),
        start, len(new), cfg.backfill_start, len(raw_ids),
    )  # fmt: skip
    return raw_ids


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    cfg = ctx.memo.get("config") or alpaca.load_config(ctx.settings.config_dir)
    ctx.memo["config"] = cfg
    bars, inserted = store_page(
        ctx.con, raw_id, raw_dir=ctx.settings.raw_dir, backfill_start=cfg.backfill_start
    )
    log.info(
        "alpaca_bars: raw %s, %s bars of %s symbol(s), %s new version(s)", raw_id, len(bars),
        len({b["symbol"] for b in bars}), inserted,
    )  # fmt: skip
    newest = max((b["date"] for b in bars), default=None)
    known = ctx.memo.get("watermark") or ctx.watermarks.get(KEY)
    if newest is not None and (known is None or newest > known):
        ctx.memo["watermark"] = newest
        ctx.advance(KEY, date.fromisoformat(newest))
    return inserted


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name=JOB_NAME,
        keys=(KEY,),
        fetch=_fetch,
        parsers={DATASET: _parse},
        versioned=(TABLE, SYMBOLS),
        observed={KEY: alerts.Observed(TABLE, "date")},
    )
)
"""Alpaca daily bars: active universe symbols from watermark − `lookback_days`, plus up to
`new_symbol_cap` symbols' whole history; 20:30 ET on NYSE days."""
