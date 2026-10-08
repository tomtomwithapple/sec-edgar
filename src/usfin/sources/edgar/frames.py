"""EDGAR frames: one concept for one period across every company, on demand, cached 24 h.

`https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json` (live 2026-10-08,
`us-gaap/Revenues/USD/CY2025Q2`: 296 137 bytes, 1 939 points) is `{taxonomy, tag, ccp, uom,
label, description, pts, data: [{accn, cik, entityName, loc, start?, end, val}]}`: `ccp` echoes
the period, `uom` the unit, `pts` counts `data`; one point per company, the fact SEC hangs the
frame on (its latest filing for that period, so a restated value wins). An unknown
tag/unit/period answers 404.

Periods are SEC's calendar frames: `CY####` (annual duration, ~365 days), `CY####Q#` (quarterly
duration, ~91 days), each with an `I` suffix for the instant at the period's end (`CY2025Q2I`).
SEC fits each fact to the nearest calendar period, so a fiscal quarter ending 2025-07-31 (Walmart)
lands in CY2025Q2.

Every response is archived first (raw dataset `edgar_frames`, source `edgar`, through
`usfin.http.Client`: no `SEC_USER_AGENT` → `ConfigError`, SEC block honoured). `frames_cache`
(key `<taxonomy>/<tag>/<unit>/<period>`, `fetched_at`, `raw_id`) remembers the latest 200 response
per frame: younger than `TTL` it answers from the raw store with no request. A frame is never
written to `xbrl_fact` (the screen in `concepts` reads `xbrl_fact`; frames are SEC's view).
"""

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import duckdb

from usfin import http, raw

BASE = "https://data.sec.gov/api/xbrl/frames/"
SOURCE = "edgar"
RAW_DATASET = "edgar_frames"
TABLE = "frames_cache"
TTL = timedelta(hours=24)
PERIOD = re.compile(r"CY\d{4}(Q[1-4])?I?")


class FramesError(Exception):
    """SEC answered something other than 200, or a body that is not a frame."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


def check_period(period: str) -> str:
    """`period` if it is a frame period (`CY2025`, `CY2025Q2`, `CY2025Q2I`, `CY2025I`)."""
    if not PERIOD.fullmatch(period):
        raise ValueError(
            f"period {period!r} is not a frame period: CY#### or CY####Q#, with I for an instant"
        )
    return period


def key(taxonomy: str, tag: str, unit: str, period: str) -> str:
    return f"{taxonomy}/{tag}/{unit}/{period}"


def url(taxonomy: str, tag: str, unit: str, period: str) -> str:
    return BASE + "/".join(quote(p, safe="") for p in (taxonomy, tag, unit, period)) + ".json"


@dataclass(frozen=True)
class Point:
    cik: int
    entity_name: str
    loc: str | None
    accn: str
    period_start: date | None
    """None for an instant frame (the point has no `start`)."""
    period_end: date
    value: float


@dataclass(frozen=True)
class Frame:
    taxonomy: str
    tag: str
    unit: str
    period: str
    label: str | None
    pts: int
    points: tuple[Point, ...]
    raw_id: int
    fetched_at: datetime
    cached: bool
    """True when the cache answered (no request sent)."""


def parse(doc: Any) -> tuple[str | None, int, tuple[Point, ...]]:
    """(label, pts, points) of a frames document."""
    if not isinstance(doc, dict) or not isinstance(doc.get("data"), list):
        raise FramesError("a frame is an object with a `data` array")
    points = []
    for d in doc["data"]:
        try:
            start = d.get("start")
            points.append(
                Point(
                    cik=int(d["cik"]), entity_name=str(d["entityName"]), loc=d.get("loc"),
                    accn=str(d["accn"]),
                    period_start=date.fromisoformat(start) if start else None,
                    period_end=date.fromisoformat(d["end"]), value=float(d["val"]),
                )
            )  # fmt: skip
        except (KeyError, TypeError, ValueError) as exc:
            raise FramesError(f"frame point is not {{accn, cik, entityName, end, val}}: {d!r}") from exc
    return doc.get("label"), int(doc.get("pts", len(points))), tuple(points)


def _naive_utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


def get_frame(
    con: duckdb.DuckDBPyConnection,
    client: http.Client,
    *,
    taxonomy: str,
    tag: str,
    unit: str,
    period: str,
    now: datetime,
    raw_dir: Path | str | None = None,
) -> Frame:
    """The frame, from the cache when its response is younger than `TTL`, else fetched."""
    check_period(period)
    now = _naive_utc(now)
    k = key(taxonomy, tag, unit, period)
    hit = con.execute(f"SELECT fetched_at, raw_id FROM {TABLE} WHERE key = ?", [k]).fetchone()
    if hit is not None and now - hit[0] < TTL:
        fetched_at, raw_id = hit
        with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
            doc = json.loads(fh.read())
        cached = True
    else:
        target = url(taxonomy, tag, unit, period)
        raw_id, response = client.get(target, source=SOURCE, dataset=RAW_DATASET)
        if response.status_code != 200:
            raise FramesError(f"{target} answered {response.status_code}", raw_id=raw_id)
        doc = response.json()
        fetched_at, cached = now, False
    label, pts, points = parse(doc)
    if not cached:
        con.execute(
            f"INSERT INTO {TABLE} (key, fetched_at, raw_id) VALUES (?, ?, ?)"
            " ON CONFLICT (key) DO UPDATE SET fetched_at = excluded.fetched_at,"
            " raw_id = excluded.raw_id",
            [k, fetched_at, raw_id],
        )
    return Frame(taxonomy, tag, unit, period, label, pts, points, raw_id, fetched_at, cached)
