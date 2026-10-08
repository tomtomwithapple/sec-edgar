"""Table views shared by every UI page: one `Table` feeds both the HTML template and `?format=csv`.

A page handler builds a `Table` (name, columns, rows of plain values) and returns
`respond(request, templates, "page.html", table, **context)`: with `?format=csv` that is the
table as a CSV download (`<name>.csv`), otherwise the template rendered with `table` and the
context. Cells are formatted the same way in both (`cell`): None → empty, timestamps (stored
naive UTC) → `YYYY-MM-DD HH:MM:SSZ`, dates ISO, dicts/lists as JSON.
"""

import csv
import io
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from urllib.parse import urlencode

from fastapi import Request
from fastapi.responses import Response
from fastapi.templating import Jinja2Templates


@dataclass(frozen=True)
class Table:
    """A table view: `name` is the CSV file stem; `rows` are mappings keyed by `columns`."""

    name: str
    columns: Sequence[str]
    rows: Sequence[Mapping[str, Any]] = field(default_factory=list)


def cell(value: Any) -> str:
    """One value as text, identical in HTML and CSV."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        stamp = f"{value:%Y-%m-%d %H:%M:%S}"
        return stamp + ("Z" if value.tzinfo is None else f" {value:%Z}")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping | list | tuple):
        return json.dumps(value, default=str, ensure_ascii=False)
    return str(value)


def wants_csv(request: Request) -> bool:
    return request.query_params.get("format") == "csv"


def wants_html(request: Request) -> bool:
    """A browser (form post) rather than an API client such as curl."""
    return "text/html" in request.headers.get("accept", "")


def to_csv(table: Table) -> str:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(table.columns)
    for row in table.rows:
        writer.writerow([cell(row.get(col)) for col in table.columns])
    return out.getvalue()


def csv_response(table: Table) -> Response:
    return Response(
        to_csv(table),
        media_type="text/csv; charset=utf-8",
        headers={"content-disposition": f'attachment; filename="{table.name}.csv"'},
    )


def respond(
    request: Request,
    templates: Jinja2Templates,
    template: str,
    table: Table,
    **context: Any,
) -> Response:
    """CSV of `table` for `?format=csv`, else `template` rendered with `table` and `context`."""
    if wants_csv(request):
        return csv_response(table)
    return templates.TemplateResponse(request, template, {"table": table, **context})


def csv_href(request: Request) -> str:
    """The current URL with `format=csv` added (other query parameters kept)."""
    params = dict(request.query_params)
    params["format"] = "csv"
    return f"{request.url.path}?{urlencode(params)}"
