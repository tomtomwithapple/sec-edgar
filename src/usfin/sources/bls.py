"""BLS Public Data API client: series config, request bodies, parsing, validation.

One POST carries every configured series (`seriesid`, `startyear`, `endyear`). With `BLS_API_KEY`
set it goes to v2 with `registrationkey` and `catalog: true` (50 series, 20 years per query);
without, to v1 (25 series, 10 years, 25 queries a day, no catalog). Every response is archived as
raw dataset `bls_timeseries` (source `bls`) before it is read. BLS answers HTTP 200 with
`status` `REQUEST_SUCCEEDED` even when some IDs do not exist: such a series comes back with empty
`data` and a `message` line "Series does not exist for Series <id>". Values are strings; BLS
writes `-` for an observation it could not publish (2025 M10, footnote X: the lapse in
appropriations), parsed as `None`. Empty footnote objects (`[{}]`) are dropped.

Titles, seasonality, units and decimals come from `config/bls.yaml` and are written to table
`bls_series` by `usfin bls validate` (and later the `bls` job).
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb
import yaml

from usfin import http, jobs
from usfin.settings import ConfigError

V1_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SOURCE = "bls"
RAW_DATASET = "bls_timeseries"
LIMITS = {"v1": (25, 10), "v2": (50, 20)}
"""(series per query, years per query) for each API version."""
_OK = "REQUEST_SUCCEEDED"


class BlsError(Exception):
    """BLS answered non-200 or did not process the request (`raw_id` is the archived reply)."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Series:
    """One `config/bls.yaml` entry."""

    series_id: str
    fred_id: str
    title: str
    seasonality: str | None
    units: str
    decimals: int
    jump_pct: float


@dataclass(frozen=True)
class Observation:
    series_id: str
    year: int
    period: str
    value: Decimal | None
    footnotes: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class Check:
    """`validate` verdict for one series ID."""

    series_id: str
    ok: bool
    count: int = 0
    latest: tuple[int, str] | None = None
    message: str = ""


def load_series(config_dir: Path | str) -> dict[str, Series]:
    """Read `<config_dir>/bls.yaml` (`series: [{id, fred, title, units, decimals, ...}]`), by ID."""
    path = Path(config_dir) / "bls.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    out: dict[str, Series] = {}
    for entry in data.get("series") or []:
        if not isinstance(entry, Mapping) or not entry.get("id"):
            raise ConfigError(f"{path}: every series needs an id, got {entry!r}")
        sid = str(entry["id"])
        if sid in out:
            raise ConfigError(f"{path}: series {sid!r} is listed twice")
        missing = [k for k in ("fred", "title", "units", "decimals", "jump_pct") if k not in entry]
        if missing:
            raise ConfigError(f"{path}: series {sid!r} lacks {', '.join(missing)}")
        try:
            out[sid] = Series(
                series_id=sid,
                fred_id=str(entry["fred"]),
                title=str(entry["title"]),
                seasonality=str(entry["seasonality"]) if entry.get("seasonality") else None,
                units=str(entry["units"]),
                decimals=int(entry["decimals"]),
                jump_pct=float(entry["jump_pct"]),
            )
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"{path}: series {sid!r}: {exc}") from None
    return out


def request(
    series_ids: Sequence[str], start_year: int, end_year: int, *, api_key: str | None
) -> tuple[str, dict[str, Any]]:
    """URL and JSON body for one query: v2 + key + catalog with a key, else keyless v1."""
    version = "v2" if api_key else "v1"
    max_series, max_years = LIMITS[version]
    if len(series_ids) > max_series:
        raise ValueError(f"BLS {version} takes at most {max_series} series per query")
    if end_year - start_year + 1 > max_years:
        raise ValueError(f"BLS {version} takes at most {max_years} years per query")
    body: dict[str, Any] = {
        "seriesid": list(series_ids),
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if api_key:
        body |= {"registrationkey": api_key, "catalog": True}
    return (V2_URL if api_key else V1_URL), body


def fetch(
    client: http.Client,
    series_ids: Sequence[str],
    start_year: int,
    end_year: int,
    *,
    api_key: str | None,
    raw_dataset: str = RAW_DATASET,
) -> tuple[int, dict[str, Any]]:
    """POST one query, archived first; return (raw_id, parsed body)."""
    url, body = request(series_ids, start_year, end_year, api_key=api_key)
    raw_id, response = client.post(url, source=SOURCE, dataset=raw_dataset, json=body)
    if response.status_code != 200:
        raise BlsError(
            f"{url} answered {response.status_code}: {response.text[:200]}", raw_id=raw_id
        )
    doc = response.json()
    if doc.get("status") != _OK:
        raise BlsError(
            f"BLS {doc.get('status')}: {'; '.join(doc.get('message') or [])}", raw_id=raw_id
        )
    return raw_id, doc


def _value(text: str) -> Decimal | None:
    try:
        return Decimal(text)
    except (InvalidOperation, TypeError):
        return None  # "-": not published


def parse(doc: Mapping[str, Any]) -> list[Observation]:
    """Every observation in a response, in response order."""
    out: list[Observation] = []
    for series in (doc.get("Results") or {}).get("series") or []:
        sid = series["seriesID"]
        for item in series.get("data") or []:
            notes = tuple(
                {str(k): str(v) for k, v in note.items()}
                for note in item.get("footnotes") or []
                if note
            )
            out.append(
                Observation(
                    series_id=sid,
                    year=int(item["year"]),
                    period=item["period"],
                    value=_value(item.get("value")),
                    footnotes=notes,
                )
            )
    return out


def validate(doc: Mapping[str, Any], series_ids: Sequence[str]) -> list[Check]:
    """One check per ID: ok when the response carries at least one published value for it."""
    messages = [str(m) for m in doc.get("message") or []]
    by_id: dict[str, list[Observation]] = {}
    present = {s["seriesID"] for s in (doc.get("Results") or {}).get("series") or []}
    for obs in parse(doc):
        by_id.setdefault(obs.series_id, []).append(obs)
    checks: list[Check] = []
    for sid in series_ids:
        published = [o for o in by_id.get(sid, []) if o.value is not None]
        if published:
            latest = max((o.year, o.period) for o in published)
            checks.append(Check(sid, True, count=len(published), latest=latest))
            continue
        said = [m for m in messages if m.endswith(f" {sid}")]
        if said:
            message = "; ".join(said)
        elif sid in present:
            message = "no published values in the requested years"
        else:
            message = "not in the response"
        checks.append(Check(sid, False, message=message))
    return checks


def upsert_series(con: duckdb.DuckDBPyConnection, series: Sequence[Series]) -> int:
    """Write `bls_series` rows from config; return how many were new or changed."""
    changed = 0
    for s in series:
        row = (s.title, s.seasonality, s.units, s.decimals)
        old = con.execute(
            "SELECT title, seasonality, units, decimals FROM bls_series WHERE series_id = ?",
            [s.series_id],
        ).fetchone()
        if old == row:
            continue
        con.execute(
            "INSERT INTO bls_series VALUES (?, ?, ?, ?, ?) ON CONFLICT (series_id) DO UPDATE SET"
            " title = excluded.title, seasonality = excluded.seasonality,"
            " units = excluded.units, decimals = excluded.decimals",
            [s.series_id, *row],
        )
        changed += 1
    return changed


def _today() -> date:
    """Today in New York (the query covers this year and the one before)."""
    return datetime.now(jobs.ET).date()
