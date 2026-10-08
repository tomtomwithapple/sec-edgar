"""Treasury Fiscal Data client: dataset registry, page fetch loop, `meta.dataTypes` casting.

Every dataset shares one grammar: `filter=<date_field>:gte:<since>&sort=<date_field>` with
`page[number]`/`page[size]` (brackets URL-encoded), looped to `meta.total-pages`. `fields=` is
never sent (a partial field list makes the API aggregate rows). Unsorted, the API answers
oldest-first, so `sort=` is always sent. Every value arrives as a string; `cast_page` types them
by `meta.dataTypes` and turns the string `"null"` into `None`.

Job `fiscal` runs every registry entry as one watermark key (the entry's name). Each page is
archived as raw dataset `fd_<name>`, then parsed from the raw store into table `fd_<name>`: the
table is created from the page's `dataTypes` with the entry's `pk` on first parse (an empty page
still creates it), a column it has not seen is added with `ALTER TABLE ADD COLUMN` and reported
through `on_schema_drift`, and rows are upserted on `pk`, counting only rows that are new or whose
values changed. A column missing from a later response keeps its stored values. The key's
watermark is the newest `date_field` value parsed (capped at today by the runner).
"""

import json
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from usfin import http, jobs, raw
from usfin.settings import ConfigError, Settings

BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
SOURCE = "fiscal"
PAGE_SIZE = 10000
LOOKBACK_DAYS = 10
BACKFILL_FROM = date(2000, 1, 1)

_CURRENCY = re.compile(r"CURRENCY\d*")
_INTEGER_TYPES = frozenset({"INTEGER", "YEAR", "QUARTER", "MONTH", "DAY"})
_FLOAT_TYPES = frozenset({"NUMBER", "PERCENTAGE"})
_STAGE = "_fd_stage"
log = logging.getLogger("usfin.fiscal")


class FiscalError(Exception):
    """Fiscal Data answered something other than 200 (the response is archived as `raw_id`)."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Dataset:
    """One `config/fiscal_data.yaml` registry entry."""

    name: str
    endpoint: str
    pk: tuple[str, ...] = ()
    date_field: str = "record_date"
    lookback_days: int = LOOKBACK_DAYS
    backfill_from: date = BACKFILL_FROM
    jump_fields: tuple[str, ...] = ()

    @property
    def raw_dataset(self) -> str:
        return f"fd_{self.name}"

    @property
    def table(self) -> str:
        return f"fd_{self.name}"


@dataclass
class Page:
    """One fetched page: its raw response id and its rows cast by `meta.dataTypes`."""

    raw_id: int | None
    number: int
    total_pages: int
    types: dict[str, str]
    rows: list[dict[str, Any]] = field(default_factory=list)


def load_datasets(config_dir: Path | str) -> dict[str, Dataset]:
    """Read `<config_dir>/fiscal_data.yaml` (`datasets: [{name, endpoint, ...}]`), by name."""
    path = Path(config_dir) / "fiscal_data.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    out: dict[str, Dataset] = {}
    for entry in data.get("datasets") or []:
        if not isinstance(entry, Mapping) or not entry.get("name") or not entry.get("endpoint"):
            raise ConfigError(f"{path}: every dataset needs a name and an endpoint, got {entry!r}")
        name = str(entry["name"])
        if name in out:
            raise ConfigError(f"{path}: dataset {name!r} is listed twice")
        try:
            backfill = entry.get("backfill_from", BACKFILL_FROM)
            out[name] = Dataset(
                name=name,
                endpoint=str(entry["endpoint"]).strip("/"),
                pk=tuple(str(c) for c in entry.get("pk") or ()),
                date_field=str(entry.get("date_field") or "record_date"),
                lookback_days=int(entry.get("lookback_days", LOOKBACK_DAYS)),
                backfill_from=(
                    backfill if isinstance(backfill, date) else date.fromisoformat(str(backfill))
                ),
                jump_fields=tuple(str(c) for c in entry.get("jump_fields") or ()),
            )
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"{path}: dataset {name!r}: {exc}") from None
    return out


def since(dataset: Dataset, watermark: date | str | None, today: date) -> date:
    """First date to request: `backfill_from` with no watermark, else min(wm, today) - lookback."""
    if watermark is None:
        return dataset.backfill_from
    wm = watermark if isinstance(watermark, date) else date.fromisoformat(str(watermark)[:10])
    return min(wm, today) - timedelta(days=dataset.lookback_days)


def page_url(dataset: Dataset, start: date, *, page: int, page_size: int = PAGE_SIZE) -> str:
    """The request URL for one page; brackets are pre-encoded, `fields=` is never added."""
    f = dataset.date_field
    return (
        f"{BASE}/{dataset.endpoint}?filter={f}:gte:{start.isoformat()}&sort={f}"
        f"&page%5Bnumber%5D={page}&page%5Bsize%5D={page_size}"
    )


def fetch(
    client: http.Client,
    dataset: Dataset,
    start: date,
    *,
    page_size: int = PAGE_SIZE,
    raw_dataset: str | None = None,
) -> list[Page]:
    """Fetch every page from `start` (pages 1..`meta.total-pages`); each is archived first."""
    pages: list[Page] = []
    number = 1
    while True:
        url = page_url(dataset, start, page=number, page_size=page_size)
        raw_id, response = client.get(
            url, source=SOURCE, dataset=raw_dataset or dataset.raw_dataset
        )
        if response.status_code != 200:
            raise FiscalError(
                f"{dataset.name}: {url} answered {response.status_code}: {response.text[:200]}",
                raw_id=raw_id,
            )
        page = cast_page(response.json(), raw_id=raw_id, number=number)
        pages.append(page)
        if number >= page.total_pages:
            return pages
        number += 1


def sql_type(fd_type: str) -> str:
    """DuckDB column type for a Fiscal Data `dataTypes` value (unknown types are text)."""
    if fd_type == "DATE":
        return "DATE"
    if _CURRENCY.fullmatch(fd_type):
        return "DECIMAL(24,4)"
    if fd_type in _INTEGER_TYPES:
        return "BIGINT"
    if fd_type in _FLOAT_TYPES:
        return "DOUBLE"
    return "VARCHAR"


def cast_value(value: Any, fd_type: str) -> Any:
    """Cast one string value; `"null"` (and JSON null) → `None`."""
    if value is None or value == "null":
        return None
    sql = sql_type(fd_type)
    if sql == "DATE":
        return date.fromisoformat(value)
    if sql == "DECIMAL(24,4)":
        return Decimal(value)
    if sql == "BIGINT":
        return int(value)
    if sql == "DOUBLE":
        return float(value)
    return value


def cast_page(doc: Mapping[str, Any], *, raw_id: int | None = None, number: int = 1) -> Page:
    """Type a parsed response body; columns absent from `dataTypes` are kept as `STRING`."""
    meta = doc.get("meta") or {}
    types = {str(k): str(v) for k, v in (meta.get("dataTypes") or {}).items()}
    rows: list[dict[str, Any]] = []
    for record in doc.get("data") or []:
        for col in record:
            types.setdefault(col, "STRING")
        rows.append({col: cast_value(v, types[col]) for col, v in record.items()})
    return Page(
        raw_id=raw_id,
        number=number,
        total_pages=int(meta.get("total-pages") or 0),
        types=types,
        rows=rows,
    )



# --- job `fiscal` -------------------------------------------------------------------------------


def _log_schema_drift(ctx: "jobs.JobContext", dataset: str, columns: Mapping[str, str]) -> None:
    log.warning("schema_drift: %s gained columns %s", dataset, dict(columns))


on_schema_drift: Callable[["jobs.JobContext", str, Mapping[str, str]], None] = _log_schema_drift
"""Called with (ctx, dataset name, {column: dataType}) after new columns were added.

A stub that logs until the alert framework (S9) points it at the `schema_drift` alert rule."""


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _columns(con, table: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position",
        [table],
    ).fetchall()
    return [r[0] for r in rows]


def _ensure_table(ctx: "jobs.JobContext", dataset: Dataset, types: Mapping[str, str]) -> None:
    """Create `fd_<name>` from `types` with the registry pk, or add the columns it lacks."""
    con = ctx.con
    missing_pk = [c for c in dataset.pk if c not in types]
    if missing_pk:
        raise ValueError(f"{dataset.name}: pk columns {missing_pk} are not in the response")
    existing = _columns(con, dataset.table)
    if not existing:
        cols = ", ".join(f"{_q(c)} {sql_type(t)}" for c, t in types.items())
        pk = ", ".join(_q(c) for c in dataset.pk)
        con.execute(f"CREATE TABLE {_q(dataset.table)} ({cols}, PRIMARY KEY ({pk}))")
        return
    added = {c: t for c, t in types.items() if c not in existing}
    for col, fd_type in added.items():
        con.execute(f"ALTER TABLE {_q(dataset.table)} ADD COLUMN {_q(col)} {sql_type(fd_type)}")
    if added:
        on_schema_drift(ctx, dataset.name, added)


def upsert_page(ctx: "jobs.JobContext", dataset: Dataset, page: Page) -> int:
    """Store one cast page in `fd_<name>`; return how many rows were new or changed."""
    if not dataset.pk:
        raise ConfigError(f"fiscal dataset {dataset.name!r} has no pk in fiscal_data.yaml")
    con = ctx.con
    _ensure_table(ctx, dataset, page.types)
    if not page.rows:
        return 0
    cols = list(page.types)
    table, stage = _q(dataset.table), _q(_STAGE)
    col_list = ", ".join(_q(c) for c in cols)
    con.execute(f"DROP TABLE IF EXISTS {stage}")
    con.execute(f"CREATE TEMP TABLE {stage} AS SELECT {col_list} FROM {table} LIMIT 0")
    try:
        # One JSON text parameter, not bound row values: DuckDB 1.5.6 binds large parameter
        # lists at ~30 µs a value (30 s for a 10 000-row auctions page) but parses JSON in
        # well under a second. Values are already cast by `cast_page`; dates and decimals
        # travel as their exact text and are cast back to the column type.
        text = json.dumps([[row.get(c) for c in cols] for row in page.rows], default=str)
        casts = ", ".join(f"r[{i}]::{sql_type(page.types[c])}" for i, c in enumerate(cols, 1))
        con.execute(
            f"INSERT INTO {stage} ({col_list}) SELECT {casts}"
            """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)""",
            [text],
        )
        pk_list = ", ".join(_q(c) for c in dataset.pk)
        total, distinct = con.execute(
            f"SELECT count(*), count(DISTINCT ({pk_list})) FROM {stage}"
        ).fetchone()
        if total != distinct:
            raise ValueError(
                f"{dataset.name}: {total - distinct} duplicate pk ({', '.join(dataset.pk)})"
                f" rows in raw_response {page.raw_id}"
            )
        same = " AND ".join(f"t.{_q(c)} IS NOT DISTINCT FROM s.{_q(c)}" for c in cols)
        changed = f"NOT EXISTS (SELECT 1 FROM {table} t WHERE {same})"
        count = con.execute(f"SELECT count(*) FROM {stage} s WHERE {changed}").fetchone()[0]
        if count:
            others = [c for c in cols if c not in dataset.pk]
            action = (
                "DO UPDATE SET " + ", ".join(f"{_q(c)} = excluded.{_q(c)}" for c in others)
                if others
                else "DO NOTHING"
            )
            con.execute(
                f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {stage} s"
                f" WHERE {changed} ON CONFLICT ({pk_list}) {action}"
            )
    finally:
        con.execute(f"DROP TABLE IF EXISTS {stage}")
    return count


def _registry(ctx: "jobs.JobContext") -> dict[str, Dataset]:
    return load_datasets(ctx.settings.config_dir)


def _keys(ctx: "jobs.JobContext") -> list[str]:
    return list(_registry(ctx))


def _fetch(ctx: "jobs.JobContext", key: str) -> list[int]:
    dataset = _registry(ctx)[key]
    start = since(dataset, ctx.watermarks.get(key), ctx.today)
    return [page.raw_id for page in fetch(ctx.client, dataset, start)]


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    found = ctx.con.execute("SELECT dataset FROM raw_response WHERE id = ?", [raw_id]).fetchone()
    name = found[0].removeprefix("fd_")
    datasets = _registry(ctx)
    if name not in datasets:
        raise ConfigError(f"raw_response {raw_id}: {name!r} is not in fiscal_data.yaml")
    dataset = datasets[name]
    with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
        page = cast_page(json.loads(fh.read()), raw_id=raw_id)
    rows = upsert_page(ctx, dataset, page)
    dates = [r[dataset.date_field] for r in page.rows if r.get(dataset.date_field) is not None]
    if dates:
        # pages come sorted on date_field, so the last page parsed holds the newest date
        ctx.advance(name, max(dates))
    return rows


def _parsers(settings: Settings) -> dict[str, "jobs.Parser"]:
    return {ds.raw_dataset: _parse for ds in load_datasets(settings.config_dir).values()}


JOB = jobs.REGISTRY.register(jobs.Job(name="fiscal", keys=_keys, fetch=_fetch, parsers=_parsers))
"""Every registry entry, one watermark key each. S14 adds `then: (auction_xcheck,)`."""
