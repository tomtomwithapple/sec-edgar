"""Treasury Fiscal Data client: dataset registry, page fetch loop, `meta.dataTypes` casting.

Every dataset shares one grammar: `filter=<date_field>:gte:<since>&sort=<date_field>` with
`page[number]`/`page[size]` (brackets URL-encoded), looped to `meta.total-pages`. `fields=` is
never sent (a partial field list makes the API aggregate rows). Unsorted, the API answers
oldest-first, so `sort=` is always sent. Every value arrives as a string; `cast_page` types them
by `meta.dataTypes` and turns the string `"null"` into `None`.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from usfin import http
from usfin.settings import ConfigError

BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
SOURCE = "fiscal"
PAGE_SIZE = 10000
LOOKBACK_DAYS = 10
BACKFILL_FROM = date(2000, 1, 1)

_CURRENCY = re.compile(r"CURRENCY\d*")
_INTEGER_TYPES = frozenset({"INTEGER", "YEAR", "QUARTER", "MONTH", "DAY"})
_FLOAT_TYPES = frozenset({"NUMBER", "PERCENTAGE"})


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

