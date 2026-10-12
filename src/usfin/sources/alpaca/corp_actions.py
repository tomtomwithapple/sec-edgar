"""Job `alpaca_corp_actions`: Alpaca's corporate actions into `alpaca_corp_action`, versioned
(plan alpaca-market-data: How > Corporate actions; A1, A5, A9).

GET `URL?start&end&limit=1000` returns every action type, grouped by type
(`{"corporate_actions": {"cash_dividends": [...], "forward_splits": [...], ...},
"next_page_token": ...}`; an empty window is `{"corporate_actions": {}}`), ordered by symbol
across the whole window, and the next page is the same request plus `page_token`. `start` and
`end` filter `process_date`, which for a dividend is its payable date, weeks after the ex-date,
so the daily window is watermark − `BACK_DAYS` .. today + `FORWARD_DAYS`: announced actions
arrive before their ex-date. Sep 2026 had 6 160 actions in 14 types over 7 pages; actions are
processed on weekdays only (2026-10-12).

Every page is archived first (source `alpaca`, dataset `alpaca_corp_actions`). A non-200 page or
a body that is not the action list fails the key before anything is parsed. Each action is one
row: `type` (the group name in the singular), the promoted columns (`COLUMNS`) and `payload`, the
object as returned in canonical JSON. An action whose payload equals its newest stored version
inserts nothing; any other payload inserts a version with `first_seen_at` = the page's
`fetched_at`. The watermark is the newest `process_date` up to today among the window's actions
(future-dated actions are stored anyway) and never moves back. Reparse replays every page in
`fetched_at` order, so the versions are rebuilt as they were.
"""

import json
import logging
from collections import Counter
from collections.abc import Sequence
from datetime import date, timedelta

import duckdb

from usfin import alerts, jobs, raw
from usfin import http as usfin_http

URL = "https://data.alpaca.markets/v1/corporate-actions"
SOURCE = "alpaca"
DATASET = "alpaca_corp_actions"
JOB_NAME = "alpaca_corp_actions"
KEY = "process_date"
TABLE = "alpaca_corp_action"
LATEST = "alpaca_corp_action_latest"
BACK_DAYS = 30
FORWARD_DAYS = 90
PAGE_LIMIT = 1000
MAX_PAGES = 500
"""A window that pages on past this is a loop, not data (a month is ~7 pages)."""
SYMBOLS = ("symbol", "old_symbol", "new_symbol", "acquiree_symbol", "acquirer_symbol")
CUSIPS = ("cusip", "old_cusip", "new_cusip", "acquiree_cusip", "acquirer_cusip")
DATES = ("ex_date", "record_date", "payable_date", "process_date", "effective_date")
RATES = ("rate", "old_rate", "new_rate", "cash_rate", "acquiree_rate", "acquirer_rate")
FLAGS = ("special", "foreign")
COLUMNS = (*SYMBOLS, *CUSIPS, *DATES, *RATES, *FLAGS)
"""Action fields promoted to columns (NULL where the type has none); the rest is in `payload`."""
log = logging.getLogger("usfin.sources.alpaca.corp_actions")


class CorpActionError(Exception):
    """A response that is not Alpaca's corporate-action list."""


def window(today: date, watermark: str | None) -> tuple[date, date]:
    """The daily request's (start, end) on `process_date`: watermark (today when none) − 30 d to
    today + 90 d."""
    since = date.fromisoformat(watermark) if watermark else today
    return since - timedelta(days=BACK_DAYS), today + timedelta(days=FORWARD_DAYS)


def parse(body: bytes) -> tuple[list[dict], str | None]:
    """The page's actions as rows (`id`, `type`, `COLUMNS`, `payload`) and its next page token.
    An id listed twice on a page is kept once (its first object)."""
    try:
        data = json.loads(body)
    except ValueError as exc:
        raise CorpActionError(f"corporate actions page is not JSON ({len(body)} bytes)") from exc
    groups = data.get("corporate_actions") if isinstance(data, dict) else None
    if not isinstance(groups, dict):
        raise CorpActionError(f"not a corporate actions page: {body[:80]!r}")
    token = data.get("next_page_token") or None
    rows: dict[str, dict] = {}
    for group, items in groups.items():
        if not isinstance(items, list):
            raise CorpActionError(f"{group}: expected a list, got {type(items).__name__}")
        kind = group.removesuffix("s")
        for n, obj in enumerate(items):
            if not isinstance(obj, dict) or not obj.get("id") or not obj.get("process_date"):
                raise CorpActionError(f"{group}[{n}] lacks id or process_date: {obj!r:.80}")
            row = {"id": obj["id"], "type": kind, **{c: obj.get(c) for c in COLUMNS}}
            row["payload"] = json.dumps(obj, sort_keys=True, separators=(",", ":"))
            rows.setdefault(obj["id"], row)
    return list(rows.values()), token


_SCHEMA = json.dumps(
    [
        {
            "id": "VARCHAR",
            "type": "VARCHAR",
            **{c: "VARCHAR" for c in (*SYMBOLS, *CUSIPS)},
            **{c: "DATE" for c in DATES},
            **{c: "DOUBLE" for c in RATES},
            **{c: "BOOLEAN" for c in FLAGS},
            "payload": "VARCHAR",
        }
    ]
)


def store_page(
    con: duckdb.DuckDBPyConnection, raw_id: int, *, raw_dir
) -> tuple[list[dict], int]:
    """Insert the page's new action versions; return (its actions, rows inserted)."""
    found = con.execute(
        "SELECT http_status, fetched_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    status, at = found
    if status != 200:
        return [], 0
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        actions, _ = parse(fh.read())
    if not actions:
        return actions, 0
    cols = ("id", "type", *COLUMNS)
    picked = ", ".join(f'r."{c}"' for c in cols)
    # one JSON text parameter, not bound row values (see fiscal.upsert_page)
    inserted = con.execute(
        f"INSERT INTO {TABLE} ({', '.join(f'"{c}"' for c in cols)}, payload, first_seen_at,"
        f" raw_id) SELECT {picked}, r.payload::JSON, ?, ?"
        " FROM (SELECT unnest(json_transform(?, ?)) AS r)"
        f" WHERE NOT EXISTS (SELECT 1 FROM {LATEST} l"
        " WHERE l.id = r.id AND l.payload::VARCHAR = r.payload)",
        [at, raw_id, json.dumps(actions), _SCHEMA],
    ).fetchone()[0]
    return actions, inserted


def fetch_window(client: usfin_http.Client, start: date, end: date) -> list[int]:
    """Archive every page of the actions processed `start`..`end`; return their raw ids."""
    params = {"start": start.isoformat(), "end": end.isoformat(), "limit": str(PAGE_LIMIT)}
    raw_ids: list[int] = []
    token: str | None = None
    while True:
        raw_id, response = client.get(
            URL,
            source=SOURCE,
            dataset=DATASET,
            params={**params, "page_token": token} if token else params,
        )
        raw_ids.append(raw_id)
        if response.status_code != 200:
            raise CorpActionError(f"{URL} {start}..{end} answered {response.status_code}")
        _, token = parse(response.content)
        if token is None:
            return raw_ids
        if len(raw_ids) >= MAX_PAGES:
            raise CorpActionError(f"{start}..{end}: still paging after {MAX_PAGES} pages")


# --- job -------------------------------------------------------------------------------------


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    start, end = window(ctx.today, ctx.watermarks.get(key))
    raw_ids = fetch_window(ctx.client, start, end)
    log.info("alpaca_corp_actions: %s..%s, %s page(s)", start, end, len(raw_ids))
    return raw_ids


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    actions, inserted = store_page(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir)
    per = Counter(a["type"] for a in actions)
    log.info(
        "alpaca_corp_actions: raw %s, %s actions (%s), %s new version(s)", raw_id, len(actions),
        ", ".join(f"{t} {n}" for t, n in sorted(per.items())) or "none", inserted,
    )  # fmt: skip
    today = ctx.today.isoformat()
    newest = max((a["process_date"] for a in actions if a["process_date"] <= today), default=None)
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
        versioned=(TABLE,),
        observed={KEY: alerts.Observed(TABLE, "process_date")},
    )
)
"""Alpaca corporate actions, watermark − 30 d .. today + 90 d; daily 07:45 ET on NYSE days."""
