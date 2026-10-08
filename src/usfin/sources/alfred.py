"""ALFRED backfill of BLS first-published history (S18, C4).

BLS's own API only serves the current numbers, so the vintages before this store's first BLS
fetch come from ALFRED (St. Louis Fed, archival FRED): one request per configured series, by its
FRED ID (`fred` in config/bls.yaml), for every real-time period —
`fred/series/observations?series_id=…&realtime_start=1776-07-04&realtime_end=9999-12-31
&file_type=json&api_key=…` — archived as raw dataset `alfred_observations` (source `alfred`; the
key is redacted from the archived URL by `http.redact`). Each observation row is one vintage:
`date` (the period, first of the month), `value` (`.` when that vintage had none, stored NULL) and
`realtime_start`, the day it was first published.

A vintage becomes a `bls_observation` row with `source='alfred'`, `first_seen_at` = its
`realtime_start` at 00:00 New York, the value rounded to the series' published `decimals`, and no
footnotes — only when it is earlier than the first BLS response that carried the series (from
then on BLS's own responses are the record) and only when it differs, at those decimals, from the
version before it. A vintage at an instant already stored keeps the stored row. The rows are
versioned by the job `bls`: its reparse replays every ALFRED response first, then BLS's in
`fetched_at` order, so `usfin bls alfred-backfill` fetches and then reparses `bls`, and a BLS value
equal to the last pre-fetch vintage is no new version.
"""

import bisect
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any

import duckdb
import httpx

from usfin import http, jobs, raw
from usfin.settings import ConfigError, Settings
from usfin.sources import bls

URL = "https://api.stlouisfed.org/fred/series/observations"
SOURCE = "alfred"
RAW_DATASET = bls.ALFRED_DATASET
REALTIME = ("1776-07-04", "9999-12-31")
"""ALFRED's whole real-time range: every vintage."""


class AlfredError(Exception):
    """ALFRED answered non-200 or a partial page (`raw_id` is the archived reply)."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Vintage:
    year: int
    period: str
    value: Decimal | None
    realtime_start: date


def params(fred_id: str, api_key: str) -> dict[str, str]:
    return {
        "series_id": fred_id,
        "realtime_start": REALTIME[0],
        "realtime_end": REALTIME[1],
        "file_type": "json",
        "api_key": api_key,
    }


def _require_key(api_key: str | None) -> str:
    if not api_key:
        raise ConfigError(
            "FRED_API_KEY is not set; the ALFRED backfill needs a free key from "
            "https://fred.stlouisfed.org/docs/api/api_key.html in the environment or .env"
        )
    return api_key


def fetch(client: http.Client, fred_id: str, *, api_key: str | None) -> tuple[int, dict]:
    """GET every vintage of one series, archived first; return (raw_id, parsed body)."""
    key = _require_key(api_key)
    raw_id, response = client.get(
        URL, source=SOURCE, dataset=RAW_DATASET, params=params(fred_id, key)
    )
    if response.status_code != 200:
        raise AlfredError(
            f"ALFRED answered {response.status_code} for {fred_id}: "
            f"{http.redact(response.text[:200])}",
            raw_id=raw_id,
        )
    doc = response.json()
    _check(doc, fred_id, raw_id)
    return raw_id, doc


def _check(doc: Any, fred_id: str, raw_id: int) -> list[dict]:
    observations = doc.get("observations") if isinstance(doc, dict) else None
    if not isinstance(observations, list):
        raise AlfredError(f"ALFRED sent no observations for {fred_id}", raw_id=raw_id)
    if int(doc.get("count", len(observations))) > len(observations):
        raise AlfredError(
            f"ALFRED sent {len(observations)} of {doc['count']} observations for {fred_id}"
            " (more than one page: not supported)",
            raw_id=raw_id,
        )
    return observations


def parse(doc: Mapping[str, Any]) -> list[Vintage]:
    """Every vintage in a response, monthly periods as BLS writes them (`M01`..`M12`)."""
    out = []
    for o in doc.get("observations") or []:
        day = date.fromisoformat(o["date"])
        if day.day != 1:
            raise ValueError(f"not a monthly observation date: {o['date']}")
        try:
            value = Decimal(o["value"])
        except (InvalidOperation, TypeError):
            value = None  # ".": no value in that vintage
        out.append(
            Vintage(day.year, f"M{day.month:02d}", value, date.fromisoformat(o["realtime_start"]))
        )
    return out


def first_seen(realtime_start: date) -> datetime:
    """`realtime_start` 00:00 New York as naive UTC (how `first_seen_at` is stored)."""
    return (
        datetime.combine(realtime_start, time(0), tzinfo=jobs.ET)
        .astimezone(UTC)
        .replace(tzinfo=None)
    )


def bls_cutoffs(con: duckdb.DuckDBPyConnection, raw_dir) -> dict[str, datetime]:
    """Per BLS series ID, the `fetched_at` of the first stored BLS response that carried it."""
    out: dict[str, datetime] = {}
    for raw_id, fetched_at in con.execute(
        "SELECT id, fetched_at FROM raw_response WHERE dataset = ? AND http_status = 200"
        " AND pruned_at IS NULL ORDER BY fetched_at, id",
        [bls.RAW_DATASET],
    ).fetchall():
        doc = bls._read(con, raw_id, raw_dir)
        for series in ((doc or {}).get("Results") or {}).get("series") or []:
            if series.get("data"):
                out.setdefault(series["seriesID"], fetched_at)
    return out


def _round(value: Decimal | None, decimals: int | None) -> Decimal | None:
    if value is None or decimals is None:
        return value
    return value.quantize(Decimal(1).scaleb(-decimals), ROUND_HALF_EVEN)


def store(
    con: duckdb.DuckDBPyConnection,
    raw_id: int,
    *,
    raw_dir,
    config: Mapping[str, bls.Series],
    cutoffs: Mapping[str, datetime],
) -> int:
    """Write the versions one `alfred_observations` response adds; return how many.

    The caller owns the transaction. A response for a FRED ID no longer in config, or not 200,
    stores nothing.
    """
    url, status, fetched_at = con.execute(
        "SELECT url, http_status, fetched_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    fred_id = httpx.URL(url).params.get("series_id")
    series = next((s for s in config.values() if s.fred_id == fred_id), None)
    if status != 200 or series is None:
        return 0
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        doc = json.loads(fh.read())
    _check(doc, fred_id, raw_id)
    sid = series.series_id
    decimals = bls._decimals(con, config).get(sid)
    limit = cutoffs.get(sid, fetched_at)
    history: dict[tuple[int, str], list[tuple[datetime, Decimal | None, str]]] = {}
    for year, period, seen, value, notes in con.execute(
        f"SELECT year, period, first_seen_at, value, footnotes FROM {bls.TABLE}"
        " WHERE series_id = ? ORDER BY first_seen_at",
        [sid],
    ).fetchall():
        history.setdefault((year, period), []).append((seen, value, notes))
    new: list[list[Any]] = []
    for v in sorted(parse(doc), key=lambda v: (v.year, v.period, v.realtime_start)):
        seen = first_seen(v.realtime_start)
        if seen >= limit:
            continue  # BLS's own responses are the record from here on
        versions = history.setdefault((v.year, v.period), [])
        at = bisect.bisect_right([row[0] for row in versions], seen)
        if at and versions[at - 1][0] == seen:
            continue  # this instant is stored already: the first record keeps it
        value = _round(v.value, decimals)
        if at:
            _, old, notes = versions[at - 1]
            if bls._same_value(value, old, decimals) and json.loads(notes) == []:
                continue
        versions.insert(at, (seen, value, "[]"))
        new.append([sid, v.year, v.period, value, "[]", seen, raw_id])
    if new:
        con.executemany(
            f"INSERT INTO {bls.TABLE} (series_id, year, period, value, footnotes, first_seen_at,"
            " raw_id, source) VALUES (?, ?, ?, ?, ?, ?, ?, 'alfred')",
            new,
        )
    return len(new)


def parse_raw(ctx: "jobs.JobContext", raw_id: int) -> int:
    """Job `bls`'s parser for `alfred_observations` (reparse only: the job never fetches it)."""
    cutoffs = ctx.memo.get("alfred_cutoffs")
    if cutoffs is None:
        cutoffs = ctx.memo["alfred_cutoffs"] = bls_cutoffs(ctx.con, ctx.settings.raw_dir)
    return store(
        ctx.con,
        raw_id,
        raw_dir=ctx.settings.raw_dir,
        config=bls.load_series(ctx.settings.config_dir),
        cutoffs=cutoffs,
    )


@dataclass(frozen=True)
class Backfill:
    fetched: dict[str, tuple[int, int]]
    """FRED ID -> (raw_id, observations in the response)."""
    reparse: "jobs.ReparseResult"


def backfill(con: duckdb.DuckDBPyConnection, settings: Settings, client: http.Client) -> Backfill:
    """Fetch every configured series' vintages, then rebuild `bls_observation` by reparse."""
    key = _require_key(settings.fred_api_key)
    fetched: dict[str, tuple[int, int]] = {}
    for s in bls.load_series(settings.config_dir).values():
        raw_id, doc = fetch(client, s.fred_id, api_key=key)
        fetched[s.fred_id] = (raw_id, len(doc["observations"]))
    return Backfill(fetched, jobs.reparse(con, bls.JOB, settings=settings))
