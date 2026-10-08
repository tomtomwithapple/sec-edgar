"""Series browser queries (`/series`): BLS series point in time, Fiscal Data datasets, auctions.

Every function takes an open connection and returns plain rows for a `usfin.web.tables.Table`;
the page handlers in `usfin.web.app` add the chart. Identifiers that reach SQL text (Fiscal Data
column names) are checked against the table's own columns first and quoted.

- **BLS**: `bls_as_of(DATE)` (C4: 23:59:59 New York that day) or, without `as_of`, the latest
  version; one row per period with how many versions were known by then; NULL values (a period BLS
  published as "-") stay rows and are gaps in the chart.
- **Fiscal Data**: one series = one value of the registry's `series` columns (default pk minus
  the date field), e.g. a DTS account type or an exchange-rate country-currency; without a pick the
  page lists the series (a single `jump_match` value per series column is the default pick).
  The plotted field is any numeric column (`?field=`; default the first `jump_fields`).
- **Auctions** (`tsy_security`, the source of truth): one series = security type × term as
  auctioned × kind (nominal / TIPS / FRN / CMB, from the payload flags) — a 10-year TIPS is not a
  10-year note, and a reopening ("9-Year 10-Month") is its own term. The plotted field defaults to
  the high yield (notes, bonds, TIPS), the investment rate for bills (they have no yield, and it
  is the bond-equivalent rate) and the discount margin for FRNs.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import duckdb

from usfin.sources import bls, fiscal
from usfin.sources.fiscal import Dataset

LATEST = datetime(9999, 12, 31, tzinfo=UTC).replace(tzinfo=None)
"""As-of instant meaning "every version stored" (naive UTC, as stored)."""
NUMERIC = ("DECIMAL", "DOUBLE", "BIGINT", "INTEGER", "FLOAT", "HUGEINT", "SMALLINT", "REAL")
NOT_VALUES = ("src_line_nbr",)
AUCTION_KIND = (
    "CASE WHEN payload->>'tips' = 'Yes' THEN 'TIPS'"
    " WHEN payload->>'floatingRate' = 'Yes' THEN 'FRN'"
    " WHEN payload->>'cashManagementBillCMB' = 'Yes' THEN 'CMB' ELSE 'nominal' END"
)
AUCTION_FIELDS = (
    "high_yield",
    "high_investment_rate",
    "high_discount_rate",
    "high_discount_margin",
    "bid_to_cover",
    "offering_amt",
)
AUCTION_COLUMNS = (
    "auction_date",
    "cusip",
    "security_type",
    "security_term",
    "kind",
    "status",
    *AUCTION_FIELDS,
    "issue_date",
    "maturity_date",
)
INDEX_COLUMNS = ("source", "id", "title", "rows", "first", "last")


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main'"
            " AND table_name = ?",
            [table],
        ).fetchone()
    )


def _dicts(rel: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row, strict=True)) for row in rel.fetchall()]


# --- index ----------------------------------------------------------------------------------


def index(con: duckdb.DuckDBPyConnection, config_dir) -> list[dict[str, Any]]:
    """One row per BLS series, Fiscal Data dataset and the auctions table, with counts."""
    rows: list[dict[str, Any]] = []
    for r in _dicts(
        con.execute(
            "SELECT s.series_id AS id, s.title, count(o.period_date) AS rows,"
            " min(o.period_date) AS first, max(o.period_date) AS last"
            " FROM bls_series s LEFT JOIN bls_observation_dated o USING (series_id)"
            " GROUP BY ALL ORDER BY s.series_id"
        )
    ):
        rows.append({"source": "bls", **r, "href": f"/series/bls/{r['id']}"})
    for ds in fiscal.load_datasets(config_dir).values():
        stats = {"rows": 0, "first": None, "last": None}
        if _table_exists(con, ds.table):
            col = _q(ds.date_field)
            n, first, last = con.execute(
                f"SELECT count(*), min({col}), max({col}) FROM {_q(ds.table)}"
            ).fetchone()
            stats = {"rows": n, "first": first, "last": last}
        rows.append(
            {
                "source": "fiscal",
                "id": ds.name,
                "title": ds.endpoint
                + (
                    " (cross-check copy; browse by term under auctions)"
                    if ds.name == "auctions"
                    else ""
                ),
                **stats,
                "href": f"/series/fiscal/{ds.name}",
            }
        )
    n, first, last = con.execute(
        "SELECT count(*), min(auction_date), max(auction_date) FROM tsy_security"
    ).fetchone()
    rows.append(
        {
            "source": "auctions",
            "id": "tsy_security",
            "title": "Treasury auctions by security term (TreasuryDirect)",
            "rows": n,
            "first": first,
            "last": last,
            "href": "/series/auctions",
        }
    )
    return rows


# --- BLS ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class BlsSeries:
    series_id: str
    title: str
    units: str | None
    decimals: int


def bls_series(con: duckdb.DuckDBPyConnection, config_dir, series_id: str) -> BlsSeries | None:
    found = con.execute(
        "SELECT title, units, decimals FROM bls_series WHERE series_id = ?", [series_id]
    ).fetchone()
    if found:
        return BlsSeries(series_id, found[0], found[1], found[2])
    configured = bls.load_series(config_dir).get(series_id)
    if configured:
        return BlsSeries(series_id, configured.title, configured.units, configured.decimals)
    return None


BLS_COLUMNS = (
    "year",
    "period",
    "period_date",
    "value",
    "footnotes",
    "first_seen_at",
    "source",
    "versions",
)


def bls_rows(
    con: duckdb.DuckDBPyConnection, series: BlsSeries, as_of: date | None
) -> list[dict[str, Any]]:
    """Per period, the version known as of `as_of` (a New York date) or the latest one."""
    ts: date | datetime = as_of if as_of is not None else LATEST
    rows = _dicts(
        con.execute(
            "SELECT a.year, a.period,"
            " CASE WHEN a.period LIKE 'M%' AND a.period <> 'M13'"
            "  THEN make_date(a.year, CAST(substr(a.period, 2) AS INTEGER), 1) END AS period_date,"
            " a.value, CAST(a.footnotes AS VARCHAR) AS footnotes, a.first_seen_at, a.source,"
            " (SELECT count(*) FROM bls_observation o WHERE o.series_id = a.series_id"
            "   AND o.year = a.year AND o.period = a.period"
            "   AND o.first_seen_at <= a_utc.ts) AS versions"
            " FROM bls_as_of(?) a, (SELECT bls_as_of_utc(?) AS ts) a_utc"
            " WHERE a.series_id = ? ORDER BY a.year, a.period",
            [ts, ts, series.series_id],
        )
    )
    for r in rows:
        v = r["value"]
        r["number"] = None if v is None else float(v)
        r["value"] = None if v is None else f"{Decimal(v):.{series.decimals}f}"
        r["notes"] = [f.get("text") or f.get("code") for f in _json_list(r["footnotes"])]
        if r["footnotes"] == "[]":
            r["footnotes"] = None
    return rows


def bls_first_seen(con: duckdb.DuckDBPyConnection, series_id: str) -> datetime | None:
    """When the oldest stored version of the series was first seen (naive UTC)."""
    return con.execute(
        "SELECT min(first_seen_at) FROM bls_observation WHERE series_id = ?", [series_id]
    ).fetchone()[0]


def _json_list(text: str | None) -> list[Mapping[str, Any]]:
    if not text:
        return []
    value = json.loads(text)
    return [v for v in value if isinstance(v, Mapping)] if isinstance(value, list) else []


# --- Fiscal Data ----------------------------------------------------------------------------


@dataclass(frozen=True)
class FiscalShape:
    """What the page needs to know about a dataset's stored table."""

    dataset: Dataset
    columns: Mapping[str, str]  # name -> DuckDB type
    dims: tuple[str, ...]
    label: str | None
    fields: tuple[str, ...]

    @property
    def date_field(self) -> str:
        return self.dataset.date_field

    def default_pick(self) -> dict[str, str] | None:
        match = self.dataset.jump_match
        if self.dims and all(len(match.get(d, ())) == 1 for d in self.dims):
            return {d: match[d][0] for d in self.dims}
        return None

    def default_field(self) -> str | None:
        for f in self.dataset.jump_fields:
            if f in self.fields:
                return f
        return self.fields[0] if self.fields else None


def fiscal_shape(con: duckdb.DuckDBPyConnection, ds: Dataset) -> FiscalShape | None:
    """The dataset's columns, series columns, label column and numeric fields; None if unloaded."""
    cols = dict(
        con.execute(
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position",
            [ds.table],
        ).fetchall()
    )
    if not cols:
        return None
    dims = tuple(ds.series or (c for c in ds.pk if c != ds.date_field))
    label = next(
        (c for c, t in cols.items() if c.endswith("_desc") and c not in dims and t == "VARCHAR"),
        None,
    )
    fields = tuple(
        c
        for c, t in cols.items()
        if t.startswith(NUMERIC)
        and c not in dims
        and c not in ds.pk
        and c not in NOT_VALUES
        and not c.startswith("record_")
    )
    return FiscalShape(ds, cols, dims, label, fields)


def fiscal_series_list(con: duckdb.DuckDBPyConnection, shape: FiscalShape) -> list[dict]:
    """One row per series (distinct values of the series columns) with its count and range."""
    date_col = _q(shape.date_field)
    dims = ", ".join(_q(d) for d in shape.dims)
    label = f", arg_max({_q(shape.label)}, {date_col}) AS {_q(shape.label)}" if shape.label else ""
    # numeric codes (MTS line_code_nbr) in number order, text alphabetically
    order = ", ".join(f"TRY_CAST({_q(d)} AS DOUBLE) NULLS LAST, {_q(d)}" for d in shape.dims)
    return _dicts(
        con.execute(
            f"SELECT {dims}{label}, count(*) AS rows, min({date_col}) AS first,"
            f" max({date_col}) AS last FROM {_q(shape.dataset.table)}"
            f" GROUP BY {dims} ORDER BY {order}"
        )
    )


def fiscal_rows(
    con: duckdb.DuckDBPyConnection, shape: FiscalShape, pick: Mapping[str, str]
) -> list[dict]:
    """The picked series' rows, newest first: date, series columns, label, numeric fields."""
    cols = [shape.date_field, *shape.dims, *([shape.label] if shape.label else []), *shape.fields]
    where = " AND ".join(f"{_q(d)} = ?" for d in shape.dims) or "TRUE"
    return _dicts(
        con.execute(
            f"SELECT {', '.join(_q(c) for c in cols)} FROM {_q(shape.dataset.table)}"
            f" WHERE {where} ORDER BY {_q(shape.date_field)} DESC",
            [pick[d] for d in shape.dims],
        )
    )


# --- auctions -------------------------------------------------------------------------------


AUCTION_LIST_COLUMNS = ("security_type", "security_term", "kind", "auctions", "first", "last")


def auction_series_list(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """One row per (type, term as auctioned, kind), most auctioned first."""
    return _dicts(
        con.execute(
            f"SELECT security_type, security_term, {AUCTION_KIND} AS kind, count(*) AS auctions,"
            " min(auction_date) AS first, max(auction_date) AS last FROM tsy_security"
            " GROUP BY ALL ORDER BY auctions DESC, security_type, security_term, kind"
        )
    )


def auction_rows(
    con: duckdb.DuckDBPyConnection, security_type: str, term: str, kind: str
) -> list[dict]:
    cols = ", ".join(c if c != "kind" else f"{AUCTION_KIND} AS kind" for c in AUCTION_COLUMNS)
    return _dicts(
        con.execute(
            f"SELECT {cols} FROM tsy_security WHERE security_type = ? AND security_term = ?"
            f" AND {AUCTION_KIND} = ? ORDER BY auction_date DESC, cusip",
            [security_type, term, kind],
        )
    )


def auction_default_field(security_type: str, kind: str) -> str:
    if kind == "FRN":
        return "high_discount_margin"
    if security_type == "Bill":
        return "high_investment_rate"
    return "high_yield"


def points(rows: Sequence[Mapping[str, Any]], date_key: str, value_key: str) -> list:
    """(date, float | None) pairs in date order for `charts.line_chart`."""
    out = [
        (r[date_key], None if r[value_key] is None else float(r[value_key]))
        for r in rows
        if r[date_key] is not None
    ]
    return sorted(out, key=lambda p: p[0])
