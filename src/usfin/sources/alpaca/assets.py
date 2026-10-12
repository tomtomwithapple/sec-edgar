"""Job `alpaca_assets`: Alpaca's asset list into `alpaca_asset_snapshot` and its type-2 history
(plan alpaca-market-data: How > Assets; A1, A4, A9).

One GET `URL` (`asset_class=us_equity`, no `status`) returns every asset, active and inactive,
OTC included: 33 564 objects, 14.6 MB of compact JSON in ~23-31 s (2026-10-12). Each object
has `id` (a UUID, one per asset; a symbol can have several, e.g. IBTA active + inactive),
`class`, `exchange` (NASDAQ, NYSE, ARCA, BATS, AMEX, OTC), `symbol`, `name` (537 are empty),
`status`, `tradable` and churning fields (margin, shortable, easy_to_borrow, borrow_status,
fractionable, attributes) that are kept in the raw JSON only. The list has no date, so a
snapshot's `snapshot_at` is the raw response's `fetched_at` (naive UTC).

Every response is archived first (source `alpaca`, dataset `alpaca_assets`). A body that is not
a non-empty list of objects with the `TRACKED` fields fails the run and stores nothing. A list
identical in every tracked column to the newest stored snapshot before it is not stored again
(so a rerun inserts 0), yet still moves the watermark: the watermark is the newest
`snapshot_at` that confirmed the list, stored or not.

Each stored snapshot is diffed into `alpaca_asset_history` keyed by asset `id`, the
`nasdaqtrader` pattern: a new id opens a row, a gone one is closed, a change in any tracked
column closes the row and opens another at the same `snapshot_at`. The gate (C9): a snapshot
whose row count is more than the job's `row_count_swing` pct (2) away from the previous stored
snapshot is stored but not diffed (`alpaca_asset_diff.diffed = false`; the alert rule raises
`row_count_swing`); the next snapshot within the gate diffs against the open rows. Reparse
rebuilds the snapshots from the raw store and the history from scratch, in `fetched_at` order.
"""

import json
import logging
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

import duckdb

from usfin import alerts, jobs, raw

URL = "https://paper-api.alpaca.markets/v2/assets?asset_class=us_equity"
SOURCE = "alpaca"
DATASET = "alpaca_assets"
JOB_NAME = "alpaca_assets"
KEY = "us_equity"
TABLE = "alpaca_asset_snapshot"
HISTORY = "alpaca_asset_history"
DIFFS = "alpaca_asset_diff"
TRACKED = ("symbol", "name", "exchange", "status", "tradable", "class")
"""Columns stored per asset; a change in any of them is a new history version."""
SWING_PCT = 2.0
"""The gate when schedule.yaml sets no `row_count_swing` pct for the job."""
log = logging.getLogger("usfin.sources.alpaca.assets")


class AssetListError(Exception):
    """A body that is not Alpaca's asset list."""


Asset = tuple[str, str, str | None, str | None, str | None, bool | None, str | None]
"""(id, *TRACKED)."""


def parse(body: bytes) -> list[Asset]:
    """The list's assets as (id, *TRACKED); `AssetListError` unless it is a non-empty list of
    objects that all carry `id` and the tracked fields."""
    try:
        data = json.loads(body)
    except ValueError as exc:
        raise AssetListError(f"asset list is not JSON ({len(body)} bytes)") from exc
    if not isinstance(data, list) or not data:
        raise AssetListError(f"asset list is not a non-empty JSON list: {body[:80]!r}")
    out: dict[str, Asset] = {}
    for n, obj in enumerate(data):
        if not isinstance(obj, dict):
            raise AssetListError(f"asset {n} is not an object")
        missing = [k for k in ("id", *TRACKED) if k not in obj]
        if missing or not obj["id"] or not obj["symbol"]:
            raise AssetListError(f"asset {n} ({obj.get('symbol')!r}) lacks {', '.join(missing)}")
        out.setdefault(obj["id"], (obj["id"], *(obj[k] for k in TRACKED)))
    return list(out.values())


def _snapshot_rows(con: duckdb.DuckDBPyConnection, at: datetime) -> set[tuple]:
    return set(
        con.execute(
            f"SELECT id, {', '.join(TRACKED)} FROM {TABLE} WHERE snapshot_at = ?", [at]
        ).fetchall()
    )


def store_snapshot(
    con: duckdb.DuckDBPyConnection, raw_id: int, *, raw_dir
) -> tuple[datetime | None, list[Asset], int]:
    """Store the response's snapshot; return (snapshot_at, assets, rows inserted). Nothing is
    inserted when that snapshot is already stored or equals the newest stored one before it.
    A non-200 response returns (None, [], 0); a body that is not the list raises."""
    found = con.execute(
        "SELECT http_status, fetched_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    status, at = found
    if status != 200:
        return None, [], 0
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        assets = parse(fh.read())
    if con.execute(f"SELECT 1 FROM {TABLE} WHERE snapshot_at = ? LIMIT 1", [at]).fetchone():
        return at, assets, 0
    prev = con.execute(f"SELECT max(snapshot_at) FROM {TABLE} WHERE snapshot_at < ?", [at])
    prev_at = prev.fetchone()[0]
    if prev_at is not None and _snapshot_rows(con, prev_at) == set(assets):
        return at, assets, 0
    # one JSON text parameter, not bound row values (see fiscal.upsert_page)
    con.execute(
        f"INSERT INTO {TABLE} (snapshot_at, id, {', '.join(TRACKED)}, raw_id)"
        " SELECT ?, r[1], r[2], r[3], r[4], r[5], r[6]::BOOLEAN, r[7], ?"
        """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)""",
        [at, raw_id, json.dumps(assets)],
    )
    return at, assets, len(assets)


# --- history -------------------------------------------------------------------------------


@dataclass(frozen=True)
class Diff:
    snapshot_at: datetime
    rows: int
    previous_at: datetime | None
    previous_rows: int | None
    pct_change: float | None
    diffed: bool
    opened: int
    closed: int


def swing_pct(settings) -> float:
    """The gate: the job's `row_count_swing` pct in schedule.yaml, else `SWING_PCT`."""
    rules = alerts.load_rules(settings.config_dir).get(JOB_NAME)
    params = rules.params_for(KEY).get("row_count_swing") if rules else None
    return float(params["pct"]) if params else SWING_PCT


def _count(con: duckdb.DuckDBPyConnection, at: datetime) -> int:
    return con.execute(f"SELECT count(*) FROM {TABLE} WHERE snapshot_at = ?", [at]).fetchone()[0]


def _diff_one(con: duckdb.DuckDBPyConnection, at: datetime, pct: float) -> Diff:
    rows = _count(con, at)
    prev = con.execute(
        f"SELECT snapshot_at, count(*) FROM {TABLE} WHERE snapshot_at < ?"
        " GROUP BY 1 ORDER BY 1 DESC LIMIT 1",
        [at],
    ).fetchone()
    prev_at, prev_rows = prev if prev else (None, None)
    change = None
    if prev_rows is not None:
        change = (rows - prev_rows) / prev_rows * 100 if prev_rows else float("inf")
    opened = closed = 0
    diffed = change is None or abs(change) <= pct
    if diffed:
        same = " AND ".join(f"s.{c} IS NOT DISTINCT FROM h.{c}" for c in TRACKED)
        closed = con.execute(
            f"UPDATE {HISTORY} h SET valid_to = ? WHERE h.valid_to IS NULL"
            f" AND NOT EXISTS (SELECT 1 FROM {TABLE} s WHERE s.snapshot_at = ? AND s.id = h.id"
            f" AND {same})",
            [at, at],
        ).fetchone()[0]
        opened = con.execute(
            f"INSERT INTO {HISTORY} (id, {', '.join(TRACKED)}, valid_from, valid_to)"
            f" SELECT s.id, {', '.join('s.' + c for c in TRACKED)}, ?, NULL FROM {TABLE} s"
            " WHERE s.snapshot_at = ?"
            f" AND NOT EXISTS (SELECT 1 FROM {HISTORY} h WHERE h.valid_to IS NULL AND h.id = s.id)",
            [at, at],
        ).fetchone()[0]
    con.execute(
        f"INSERT INTO {DIFFS} (snapshot_at, rows, previous_at, previous_rows, pct_change, diffed,"
        " opened, closed) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [at, rows, prev_at, prev_rows, change, diffed, opened, closed],
    )
    return Diff(at, rows, prev_at, prev_rows, change, diffed, opened, closed)


def sync_history(
    con: duckdb.DuckDBPyConnection, *, pct: float = SWING_PCT, rebuild: bool = False
) -> list[Diff]:
    """Diff every stored snapshot not yet in `alpaca_asset_diff`, oldest first. With `rebuild`
    (or when such a snapshot is older than one already diffed) history and diff rows are dropped
    and every stored snapshot is diffed again in order."""
    done = con.execute(f"SELECT max(snapshot_at) FROM {DIFFS}").fetchone()[0]
    pending = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT snapshot_at FROM {TABLE}"
            f" WHERE snapshot_at NOT IN (SELECT snapshot_at FROM {DIFFS}) ORDER BY 1"
        ).fetchall()
    ]
    if not rebuild and pending and done is not None and pending[0] < done:
        log.warning("alpaca_assets: snapshot %s is older than %s; history rebuilt",
                    pending[0], done)  # fmt: skip
        rebuild = True
    if rebuild:
        con.execute(f"DELETE FROM {HISTORY}")
        con.execute(f"DELETE FROM {DIFFS}")
        pending = [
            r[0]
            for r in con.execute(f"SELECT DISTINCT snapshot_at FROM {TABLE} ORDER BY 1").fetchall()
        ]
    return [_diff_one(con, at, pct) for at in pending]


# --- job -------------------------------------------------------------------------------------


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    raw_id, response = ctx.client.get(URL, source=SOURCE, dataset=DATASET)
    if response.status_code != 200:
        raise AssetListError(f"{URL} answered {response.status_code}")
    return [raw_id]


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    at, assets, inserted = store_snapshot(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir)
    if at is None:
        return 0
    if inserted:
        per = Counter((a[3], a[4]) for a in assets)
        log.info(
            "alpaca_assets: snapshot %s, %s assets (%s)", at, len(assets),
            ", ".join(f"{ex} {st} {n}" for (ex, st), n in sorted(per.items())),
        )  # fmt: skip
    else:
        log.info("alpaca_assets: snapshot %s, %s assets, unchanged: not stored", at, len(assets))
    rebuild = ctx.trigger == "reparse" and not ctx.memo.get("history_rebuilt")
    ctx.memo["history_rebuilt"] = True
    pct = swing_pct(ctx.settings)
    for d in sync_history(ctx.con, pct=pct, rebuild=rebuild):
        if d.diffed:
            log.info("alpaca_assets: snapshot %s diffed: %s opened, %s closed",
                     d.snapshot_at, d.opened, d.closed)  # fmt: skip
        else:
            log.warning(
                "alpaca_assets: snapshot %s not diffed: %s rows vs %s at %s (%+.2f %%, gate %s %%)",
                d.snapshot_at, d.rows, d.previous_rows, d.previous_at, d.pct_change, pct,
            )  # fmt: skip
    known = ctx.watermarks.get(KEY)
    if known is None or at.isoformat() > known:
        ctx.advance(KEY, at)
    return inserted


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name=JOB_NAME,
        keys=(KEY,),
        fetch=_fetch,
        parsers={DATASET: _parse},
        versioned=(TABLE,),
        observed={KEY: alerts.Observed(TABLE, "snapshot_at", snapshot=True)},
    )
)
"""Alpaca's asset list into `alpaca_asset_snapshot` and `alpaca_asset_history`; daily 07:30 ET."""
