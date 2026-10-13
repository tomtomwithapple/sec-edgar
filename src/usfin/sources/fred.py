"""FRED series, point in time (S42).

Job `fred`, one watermark key per series in config/fred.yaml (the FRED series ID). Each run of a
key asks FRED (api.stlouisfed.org, `FRED_API_KEY`) for:

- `fred/series` — the catalog entry (title, units, frequency, seasonal adjustment, last update),
  raw dataset `fred_series`, upserted into `fred_series`;
- `fred/series/observations` with real-time periods, raw dataset `fred_observations`. A series
  with no stored vintage is loaded whole: its vintage dates (`fred/series/vintagedates`, raw
  dataset `fred_vintagedates`, not parsed) cut in chunks of `MAX_VINTAGES`, because FRED refuses
  a request spanning more than 2000 (DGS10 has 5 127), each chunk one real-time window
  `[chunk's first, next chunk's first - 1 day]`, the last one open to 9999-12-31. A series counts
  as loaded once its key has a watermark (written only when every window was fetched and parsed),
  so a first load that failed half way is fetched whole again (stored instants are kept). Later runs ask
  from the newest stored `realtime_start` to 9999-12-31, for observation dates from
  `today - lookback_days`. Every response is paged by `offset` when `count` exceeds the page.

FRED clips a row's `realtime_start` to the requested start: an observation unchanged since an
earlier vintage comes back dated at the window's start. So a row is a new version only when its
value differs from the version stored before that instant, and a row at an instant already stored
keeps the stored one (as in `usfin.sources.alfred`). `first_seen_at` is `realtime_start` at 00:00
New York. The rows are versioned: reparse deletes them and replays the responses in `fetched_at`
order, which is the order the windows were fetched in.

The watermark is the newest observation date stored (value_jump compares observations dated after
the day's first watermark); where the next fetch starts comes from the table, not the watermark.
Without `FRED_API_KEY` the run is `skipped`.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb
import httpx
import yaml

from usfin import alerts, http, jobs, raw
from usfin.settings import ConfigError
from usfin.sources import alfred

BASE = "https://api.stlouisfed.org/fred"
SOURCE = "fred"
SERIES_DATASET = "fred_series"
OBS_DATASET = "fred_observations"
VINTAGES_DATASET = "fred_vintagedates"
TABLE = "fred_observation"
LATEST_VIEW = "fred_observation_latest"
MAX_VINTAGES = 1998
"""Vintage dates per real-time window. FRED refuses a window holding more than 2000, and counts
one more than `vintagedates` lists in it (a window over 2000 listed dates was refused as "2001
vintage dates", live 2026-10-13), so a window keeps 2 under the limit."""
PAGE = 100_000
"""FRED's largest `limit` for observations."""
VINTAGE_PAGE = 10_000
"""FRED's largest `limit` for vintage dates."""
OPEN_END = "9999-12-31"
EARLIEST = "1776-07-04"
DECIMALS = Decimal("0.000001")
"""`fred_observation.value` is DECIMAL(24, 6): values are compared at that precision."""


class FredError(Exception):
    """FRED answered non-200 or a body without the expected list (`raw_id` is the reply)."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Series:
    series_id: str
    jump_pct: float | None


@dataclass(frozen=True)
class Config:
    lookback_days: int
    series: Mapping[str, Series]


def load_config(config_dir: Path | str) -> Config:
    """Read `<config_dir>/fred.yaml` (`lookback_days`, `series: [{id, jump_pct?}]`)."""
    path = Path(config_dir) / "fred.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    try:
        lookback = int(data.get("lookback_days", 1825))
    except (TypeError, ValueError):
        raise ConfigError(f"{path}: lookback_days must be a whole number of days") from None
    if lookback < 1:
        raise ConfigError(f"{path}: lookback_days must be at least 1")
    out: dict[str, Series] = {}
    for entry in data.get("series") or []:
        if not isinstance(entry, Mapping) or not entry.get("id"):
            raise ConfigError(f"{path}: every series needs an id, got {entry!r}")
        sid = str(entry["id"]).strip().upper()
        if sid in out:
            raise ConfigError(f"{path}: series {sid!r} is listed twice")
        try:
            jump = float(entry["jump_pct"]) if entry.get("jump_pct") is not None else None
        except (TypeError, ValueError):
            raise ConfigError(f"{path}: series {sid!r}: jump_pct must be a number") from None
        out[sid] = Series(sid, jump)
    return Config(lookback, out)


# --- requests -----------------------------------------------------------------------------


def _get(ctx: "jobs.JobContext", path: str, dataset: str, params: Mapping[str, str]):
    raw_id, response = ctx.client.get(
        f"{BASE}/{path}",
        source=SOURCE,
        dataset=dataset,
        params={**params, "file_type": "json", "api_key": ctx.settings.fred_api_key},
    )
    if response.status_code != 200:
        raise FredError(
            f"FRED answered {response.status_code} for {path} {params.get('series_id')}: "
            f"{http.redact(response.text[:200])}",
            raw_id=raw_id,
        )
    return raw_id, response.json()


def _paged(
    ctx: "jobs.JobContext", path: str, dataset: str, params: Mapping[str, str], field: str, page: int
) -> tuple[list[int], list[Any]]:
    """Every page of one listing (`offset` until `count` rows came); raw ids and the rows."""
    raw_ids: list[int] = []
    rows: list[Any] = []
    while True:
        raw_id, doc = _get(
            ctx, path, dataset, {**params, "limit": str(page), "offset": str(len(rows))}
        )
        raw_ids.append(raw_id)
        got = doc.get(field) if isinstance(doc, dict) else None
        if not isinstance(got, list):
            raise FredError(f"FRED sent no {field} for {params.get('series_id')}", raw_id=raw_id)
        rows += got
        if not got or len(rows) >= int(doc.get("count", len(rows))):
            return raw_ids, rows


def windows(vintages: Sequence[str]) -> list[tuple[str, str]]:
    """Real-time windows holding at most `MAX_VINTAGES` vintage dates each, oldest first."""
    days = sorted(set(vintages))
    if not days:
        return [(EARLIEST, OPEN_END)]
    out = []
    for i in range(0, len(days), MAX_VINTAGES):
        nxt = days[i + MAX_VINTAGES] if i + MAX_VINTAGES < len(days) else None
        end = (date.fromisoformat(nxt) - timedelta(days=1)).isoformat() if nxt else OPEN_END
        out.append((days[i] if i else EARLIEST, end))
    return out


def newest_vintage(con: duckdb.DuckDBPyConnection, series_id: str) -> date | None:
    return con.execute(
        f"SELECT max(realtime_start) FROM {TABLE} WHERE series_id = ?", [series_id]
    ).fetchone()[0]


def _require_key(ctx: "jobs.JobContext") -> None:
    if not ctx.settings.fred_api_key:
        raise jobs.JobSkipped(
            "FRED_API_KEY is not set; get a free key at "
            "https://fred.stlouisfed.org/docs/api/api_key.html and add it to .env"
        )


def _keys(ctx: "jobs.JobContext") -> list[str]:
    _require_key(ctx)
    return list(load_config(ctx.settings.config_dir).series)


def _fetch(ctx: "jobs.JobContext", key: str) -> list[int]:
    _require_key(ctx)
    config = load_config(ctx.settings.config_dir)
    meta_id, _ = _get(ctx, "series", SERIES_DATASET, {"series_id": key})
    out = [meta_id]
    since = newest_vintage(ctx.con, key) if key in ctx.watermarks else None
    if since is None:
        _, vintages = _paged(
            ctx,
            "series/vintagedates",
            VINTAGES_DATASET,
            {"series_id": key, "realtime_start": EARLIEST, "realtime_end": OPEN_END},
            "vintage_dates",
            VINTAGE_PAGE,
        )
        spans = [({"realtime_start": s, "realtime_end": e}) for s, e in windows(vintages)]
    else:
        start = ctx.today - timedelta(days=config.lookback_days)
        spans = [
            {
                "realtime_start": since.isoformat(),
                "realtime_end": OPEN_END,
                "observation_start": start.isoformat(),
            }
        ]
    for span in spans:
        ids, _ = _paged(
            ctx, "series/observations", OBS_DATASET, {"series_id": key, **span},
            "observations", PAGE,
        )
        out += ids
    return out


# --- parsers ------------------------------------------------------------------------------


def _doc(con: duckdb.DuckDBPyConnection, raw_id: int, raw_dir) -> tuple[str | None, int, Any]:
    url, status = con.execute(
        "SELECT url, http_status FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if status != 200:
        return None, status, None
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        return httpx.URL(url).params.get("series_id"), status, json.loads(fh.read())


def _value(text: Any) -> Decimal | None:
    try:
        return Decimal(str(text)).quantize(DECIMALS)
    except (InvalidOperation, TypeError, ValueError):
        return None  # ".": no value in that vintage


def _parse_series(ctx: "jobs.JobContext", raw_id: int) -> int:
    sid, _, doc = _doc(ctx.con, raw_id, ctx.settings.raw_dir)
    entries = (doc or {}).get("seriess") or []
    if not sid or not entries:
        return 0
    s = entries[0]
    ctx.con.execute(
        "INSERT INTO fred_series VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (series_id) DO UPDATE"
        " SET title = excluded.title, units = excluded.units, frequency = excluded.frequency,"
        " seasonal_adjustment = excluded.seasonal_adjustment,"
        " last_updated = excluded.last_updated, raw_id = excluded.raw_id",
        [
            sid,
            s.get("title") or sid,
            s.get("units"),
            s.get("frequency"),
            s.get("seasonal_adjustment"),
            s.get("last_updated"),
            raw_id,
        ],
    )
    return 1


def store(con: duckdb.DuckDBPyConnection, raw_id: int, *, raw_dir) -> tuple[str | None, int]:
    """Write the versions one `fred_observations` response adds; return (series, how many).

    The caller owns the transaction. A non-200 response stores nothing.
    """
    sid, _, doc = _doc(con, raw_id, raw_dir)
    observations = (doc or {}).get("observations") if isinstance(doc, dict) else None
    if not sid or not isinstance(observations, list):
        return sid, 0
    history: dict[date, list[tuple[datetime, Decimal | None]]] = {}
    for day, seen, value in con.execute(
        f"SELECT date, first_seen_at, value FROM {TABLE} WHERE series_id = ?"
        " ORDER BY date, first_seen_at",
        [sid],
    ).fetchall():
        history.setdefault(day, []).append((seen, value))
    new: list[list[Any]] = []
    for o in sorted(observations, key=lambda o: (o["date"], o["realtime_start"])):
        day = date.fromisoformat(o["date"])
        realtime = date.fromisoformat(o["realtime_start"])
        seen = alfred.first_seen(realtime)
        value = _value(o.get("value"))
        versions = history.setdefault(day, [])
        before = [v for v in versions if v[0] <= seen]
        if before and before[-1][0] == seen:
            continue  # this instant is stored already: the first record keeps it
        if before and before[-1][1] == value:
            continue  # unchanged (FRED clips realtime_start to the window's start)
        versions.append((seen, value))
        versions.sort(key=lambda v: v[0])
        new.append([sid, day, value, realtime, seen, raw_id])
    if new:
        con.executemany(
            f"INSERT INTO {TABLE} (series_id, date, value, realtime_start, first_seen_at, raw_id)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            new,
        )
    return sid, len(new)


def _parse_observations(ctx: "jobs.JobContext", raw_id: int) -> int:
    sid, rows = store(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir)
    if sid:
        newest = ctx.con.execute(
            f"SELECT max(date) FROM {TABLE} WHERE series_id = ?", [sid]
        ).fetchone()[0]
        if newest is not None:
            ctx.advance(sid, newest)
    return rows


def _observed(settings) -> dict[str, alerts.Observed]:
    return {
        sid: alerts.Observed(
            table=LATEST_VIEW,
            date_column="date",
            match={"series_id": (sid,)},
            series=("series_id",),
            values=("value",) if s.jump_pct is not None else (),
            jump_pct=s.jump_pct,
        )
        for sid, s in load_config(settings.config_dir).series.items()
    }


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="fred",
        keys=_keys,
        fetch=_fetch,
        parsers={SERIES_DATASET: _parse_series, OBS_DATASET: _parse_observations},
        versioned=(TABLE,),
        observed=_observed,
    )
)
"""One key per series in fred.yaml; versioned rows in `fred_observation`."""
