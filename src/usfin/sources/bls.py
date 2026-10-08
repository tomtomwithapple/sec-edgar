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
`bls_series` by `usfin bls validate` and the `bls` job.

Job `bls` (09:00 ET, federal days): one query for every configured series, this year and last
(C3's overlap), archived as `bls_timeseries`; watermark key per series = its latest published
period (`YYYY-Mnn`, never moved back). Its keys are the series IDs but one request serves them
all: the first key's fetch posts and every key reuses that raw response (`ctx.memo`), so a failed
request is sent once and fails every key. `store_observations` versions `bls_observation` (C4): a
row is written only when the value (compared at the series' published `decimals`) or the footnotes
differ from the latest row for that period as of the response's `fetched_at`, which becomes the
row's `first_seen_at`. A "-" period is stored with `value` NULL. Every `bls_timeseries` response is
parsed the same way — `bls validate`'s included — so `usfin reparse bls` rebuilds the same table;
a response that is not HTTP 200 + `REQUEST_SUCCEEDED` stores nothing.

Release days (S17): each series names the BLS release that publishes it (`release` in bls.yaml,
the SUMMARY of its event in the release calendar that job `bls_calendar` stores in
`bls_release`). Those days are the series' due days (`due: {releases: bls_release}` in
schedule.yaml) and drive the release-day retry: in `usfin serve`, after a `bls` run on a release
day of a series whose watermark has not moved since the day's first run, the job runs again in
10 minutes (15 without `BLS_API_KEY`, to stay under v1's 25 queries a day) as long as that is no
later than 12:00 New York; past that, each such series raises `stale_watermark`. `value_jump`
checks each new period against the previous one, at the series' `jump_pct`, on the dated view
`bls_observation_dated` (`period_date` = the first day of a monthly period).
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb
import yaml

from usfin import alerts, http, jobs, raw
from usfin.settings import ConfigError

V1_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SOURCE = "bls"
RAW_DATASET = "bls_timeseries"
ALFRED_DATASET = "alfred_observations"
"""ALFRED's vintages of the same series (S18, `usfin.sources.alfred`), replayed before BLS."""
TABLE = "bls_observation"
LIMITS = {"v1": (25, 10), "v2": (50, 20)}
"""(series per query, years per query) for each API version."""
_OK = "REQUEST_SUCCEEDED"
DATED_VIEW = "bls_observation_dated"
RELEASE_TABLE = "bls_release"
DAY_SOURCE = "bls_release"
RETRY_UNTIL = time(12, 0)
RETRY_EVERY = timedelta(minutes=10)
RETRY_EVERY_KEYLESS = timedelta(minutes=15)


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
    release: str | None = None
    """The BLS release (calendar SUMMARY) that publishes this series."""


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
                release=str(entry["release"]).strip() if entry.get("release") else None,
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


# --- versioned observations (job `bls`) ------------------------------------------------------


@dataclass(frozen=True)
class Stored:
    """What `store_observations` did with one raw response."""

    rows: int
    latest: dict[str, str]
    """Series ID -> its latest published period in the response, `YYYY-Mnn`."""


def _footnotes_json(notes: Sequence[Mapping[str, str]]) -> str:
    return json.dumps([dict(n) for n in notes], sort_keys=True, separators=(",", ":"))


def _same_value(a: Decimal | None, b: Decimal | None, decimals: int | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if decimals is None:
        return a == b
    quantum = Decimal(1).scaleb(-decimals)
    return a.quantize(quantum, ROUND_HALF_EVEN) == b.quantize(quantum, ROUND_HALF_EVEN)


def _decimals(con: duckdb.DuckDBPyConnection, config: Mapping[str, Series]) -> dict[str, int]:
    """Published decimals per series: config first, else what `bls_series` recorded."""
    out = {
        sid: int(d)
        for sid, d in con.execute("SELECT series_id, decimals FROM bls_series").fetchall()
    }
    return out | {sid: s.decimals for sid, s in config.items()}


def _read(con: duckdb.DuckDBPyConnection, raw_id: int, raw_dir) -> dict[str, Any] | None:
    """The response body, or None when it carries no data (non-200 or not processed)."""
    status = con.execute("SELECT http_status FROM raw_response WHERE id = ?", [raw_id]).fetchone()
    if status is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    if status[0] != 200:
        return None
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        try:
            doc = json.loads(fh.read())
        except ValueError:
            return None
    return doc if isinstance(doc, dict) and doc.get("status") == _OK else None


def store_observations(
    con: duckdb.DuckDBPyConnection,
    raw_id: int,
    *,
    raw_dir,
    config: Mapping[str, Series],
) -> Stored:
    """Write the new versions one `bls_timeseries` response carries; return what it did.

    The caller owns the transaction. Versions are compared with the latest row for each period
    first seen at or before this response's `fetched_at` (so a reparse replaying responses in
    order rebuilds the same history).
    """
    doc = _read(con, raw_id, raw_dir)
    if doc is None:
        return Stored(0, {})
    observations = parse(doc)
    upsert_series(
        con, [config[sid] for sid in sorted({o.series_id for o in observations} & set(config))]
    )
    fetched_at = con.execute(
        "SELECT fetched_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()[0]
    ids = sorted({o.series_id for o in observations})
    known: dict[tuple[str, int, str], tuple[Decimal | None, str]] = {}
    if ids:
        for sid, year, period, value, notes in con.execute(
            f"SELECT series_id, year, period, value, footnotes FROM {TABLE}"
            f" WHERE series_id IN ({', '.join('?' for _ in ids)}) AND first_seen_at <= ?"
            " QUALIFY row_number() OVER (PARTITION BY series_id, year, period"
            " ORDER BY first_seen_at DESC) = 1",
            [*ids, fetched_at],
        ).fetchall():
            known[(sid, year, period)] = (value, notes)
    decimals = _decimals(con, config)
    new: list[list[Any]] = []
    latest: dict[str, str] = {}
    for o in observations:
        if o.value is not None:
            period = f"{o.year}-{o.period}"
            latest[o.series_id] = max(latest.get(o.series_id, period), period)
        notes = _footnotes_json(o.footnotes)
        old = known.get((o.series_id, o.year, o.period))
        if old is not None and (
            _same_value(o.value, old[0], decimals.get(o.series_id))
            and json.loads(old[1]) == json.loads(notes)
        ):
            continue
        known[(o.series_id, o.year, o.period)] = (o.value, notes)
        new.append([o.series_id, o.year, o.period, o.value, notes, fetched_at, raw_id])
    if new:
        con.executemany(
            f"INSERT INTO {TABLE} (series_id, year, period, value, footnotes, first_seen_at,"
            " raw_id, source) VALUES (?, ?, ?, ?, ?, ?, ?, 'bls')",
            new,
        )
    return Stored(len(new), latest)


def _keys(ctx: "jobs.JobContext") -> list[str]:
    return list(load_series(ctx.settings.config_dir))


def _fetch(ctx: "jobs.JobContext", key: str) -> list[int]:
    """One query for every series, sent on the first key and reused by the others."""
    got = ctx.memo.get("bls_raw_id")
    if got is None:
        series = list(load_series(ctx.settings.config_dir))
        try:
            got, _ = fetch(
                ctx.client,
                series,
                ctx.today.year - 1,
                ctx.today.year,
                api_key=ctx.settings.bls_api_key,
            )
        except (http.HttpError, BlsError, ValueError) as exc:  # every key fails; sent once
            got = exc
        ctx.memo["bls_raw_id"] = got
    if isinstance(got, Exception):
        raise got
    return [got]


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    done: dict[int, Stored] = ctx.memo.setdefault("bls_parsed", {})
    rows = 0
    if raw_id not in done:
        done[raw_id] = store_observations(
            ctx.con,
            raw_id,
            raw_dir=ctx.settings.raw_dir,
            config=load_series(ctx.settings.config_dir),
        )
        rows = done[raw_id].rows
    for sid, period in done[raw_id].latest.items():
        current = ctx.watermarks.get(sid)
        ctx.advance(sid, max(period, current) if current else period)
    return rows


# --- release days, alerts and the release-day retry --------------------------------------------


def period_date(watermark: str) -> str | None:
    """`2026-M08` -> `2026-08-01` (the date `bls_observation_dated` gives that period)."""
    try:
        year, month = watermark.split("-M")
        return date(int(year), int(month), 1).isoformat()
    except ValueError:
        return None


def release_days(con: duckdb.DuckDBPyConnection, settings, series_id: str) -> frozenset[date]:
    """Days the release that publishes `series_id` comes out, as stored in `bls_release`."""
    series = load_series(settings.config_dir).get(series_id)
    if series is None or series.release is None:
        return frozenset()
    exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [RELEASE_TABLE]
    ).fetchone()[0]
    if not exists:
        return frozenset()
    rows = con.execute(
        f"SELECT release_date FROM {RELEASE_TABLE} WHERE release_name = ?", [series.release]
    ).fetchall()
    return frozenset(d for (d,) in rows)


alerts.DAY_SOURCES[DAY_SOURCE] = release_days


def _observed(settings) -> dict[str, alerts.Observed]:
    return {
        sid: alerts.Observed(
            table=DATED_VIEW,
            date_column="period_date",
            match={"series_id": (sid,)},
            series=("series_id",),
            values=("value",),
            jump_pct=s.jump_pct,
            watermark_date=period_date,
        )
        for sid, s in load_series(settings.config_dir).items()
    }


def waiting(con: duckdb.DuckDBPyConnection, settings, today: date) -> list[Series]:
    """Series released on `today` whose watermark has not moved since the day's first run."""
    baseline = alerts.day_baseline(con, "bls", today)
    current = jobs.get_watermarks(con, "bls")
    return [
        s
        for sid, s in load_series(settings.config_dir).items()
        if sid in baseline  # no baseline: a first load, nothing to wait for
        and current.get(sid) == baseline[sid]
        and today in release_days(con, settings, sid)
    ]


def retry(con: duckdb.DuckDBPyConnection, settings, now: datetime) -> datetime | None:
    """When to run `bls` again today, or None; past 12:00 ET raises `stale_watermark` instead."""
    local = now.astimezone(jobs.ET)
    late = waiting(con, settings, local.date())
    if not late:
        return None
    at = now + (RETRY_EVERY if settings.bls_api_key else RETRY_EVERY_KEYLESS)
    if at <= datetime.combine(local.date(), RETRY_UNTIL, tzinfo=jobs.ET):
        return at
    for s in late:
        alerts.raise_alert(
            con,
            job="bls",
            rule="stale_watermark",
            subject=s.series_id,
            detail={
                "watermark": jobs.get_watermarks(con, "bls").get(s.series_id),
                "release": s.release,
                "release_date": local.date().isoformat(),
                "retried_until": RETRY_UNTIL.strftime("%H:%M"),
            },
            now=now,
        )
    return None


def _parse_alfred(ctx: "jobs.JobContext", raw_id: int) -> int:
    from usfin.sources import alfred  # alfred builds on this module

    return alfred.parse_raw(ctx, raw_id)


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="bls",
        keys=_keys,
        fetch=_fetch,
        parsers={RAW_DATASET: _parse, ALFRED_DATASET: _parse_alfred},
        versioned=(TABLE,),
        replay_first=(ALFRED_DATASET,),
        observed=_observed,
        retry=retry,
    )
)
"""All configured series in one query; versioned rows in `bls_observation`. Reparse replays the
ALFRED backfill's responses (`usfin bls alfred-backfill`) first, then BLS's in `fetched_at` order."""


def _today() -> date:
    """Today in New York (the query covers this year and the one before)."""
    return datetime.now(jobs.ET).date()
