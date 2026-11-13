"""Table `edgar_company_profile` (S45): a company's metadata from its submissions main document.

SEC's ticker map (`edgar_company`) has no industry; the submissions JSON (`CIK##########.json`,
fetched per CIK by job `edgar_submissions` or read from bulk submissions.zip) carries `sic`,
`sicDescription`, `ownerOrg`, `entityType`, `category`, `fiscalYearEnd`, `stateOfIncorporation`
and `exchanges` at its top level. `submissions.store_document` calls `store` for every main
document, so both the per-CIK job and the weekly bulk reconcile keep it current; `backfill`
fills it from the stored responses without a request (`usfin edgar profiles`).

`sic` is a string in the JSON ("3571"), "" when SEC lists none; stored as an integer or NULL.
A row is replaced only by a document seen at the same time or later (`seen_at`, the response's
`fetched_at`; a bulk member takes its ZIP's), so replaying an older response changes nothing.
"""

import json
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import duckdb

from usfin import raw

TABLE = "edgar_company_profile"
COLUMNS = (
    "cik", "name", "sic", "sic_description", "owner_org", "entity_type", "category",
    "fiscal_year_end", "state_of_incorporation", "exchanges", "seen_at", "raw_id",
)  # fmt: skip


class ProfileError(ValueError):
    """A main document whose metadata is not the shape SEC serves."""


def _text(doc: Mapping[str, Any], key: str) -> str | None:
    value = doc.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ProfileError(f"{key} is {type(value).__name__}, not a string")
    value = value.strip()
    return value or None


def sic_code(value: Any) -> int | None:
    """`"3571"` → 3571; `""` or None → None; anything else is an error."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if not text.isdigit() or len(text) > 4:
        raise ProfileError(f"sic {value!r} is not a SIC code")
    return int(text)


def row(cik: int, doc: Mapping[str, Any], *, seen_at: datetime | None, raw_id: int | None) -> tuple:
    exchanges = doc.get("exchanges")
    if exchanges is not None and not (
        isinstance(exchanges, list) and all(isinstance(x, str | None) for x in exchanges)
    ):
        raise ProfileError("exchanges is not a list of strings")
    return (
        cik,
        _text(doc, "name"),
        sic_code(doc.get("sic")),
        _text(doc, "sicDescription"),
        _text(doc, "ownerOrg"),
        _text(doc, "entityType"),
        _text(doc, "category"),
        _text(doc, "fiscalYearEnd"),
        _text(doc, "stateOfIncorporation"),
        None if exchanges is None else [x for x in exchanges if x],
        seen_at,
        raw_id,
    )


def seen_at_of(con: duckdb.DuckDBPyConnection, raw_id: int | None) -> datetime | None:
    if raw_id is None:
        return None
    found = con.execute("SELECT fetched_at FROM raw_response WHERE id = ?", [raw_id]).fetchone()
    return None if found is None else found[0]


def store(
    con: duckdb.DuckDBPyConnection,
    cik: int,
    doc: Mapping[str, Any],
    *,
    seen_at: datetime | None = None,
    raw_id: int | None = None,
) -> int:
    """Upsert the company's profile from main document `doc`; 1 if the row was written, else 0
    (a stored row seen later wins). `seen_at` defaults to the `fetched_at` of `raw_id`."""
    if seen_at is None:
        seen_at = seen_at_of(con, raw_id)
    values = row(cik, doc, seen_at=seen_at, raw_id=raw_id)
    before = con.execute(f"SELECT seen_at FROM {TABLE} WHERE cik = ?", [cik]).fetchone()
    if before is not None and before[0] is not None and (seen_at is None or seen_at < before[0]):
        return 0
    sets = ", ".join(f"{c} = excluded.{c}" for c in COLUMNS[1:])
    con.execute(
        f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES ({', '.join('?' for _ in COLUMNS)})"
        f" ON CONFLICT (cik) DO UPDATE SET {sets}",
        list(values),
    )
    return 1


# --- backfill from stored responses -------------------------------------------------------------


@dataclass
class Backfilled:
    zip_raw_id: int | None = None
    members: int = 0
    responses: int = 0
    written: int = 0
    errors: list[str] = field(default_factory=list)


def backfill(
    con: duckdb.DuckDBPyConnection,
    *,
    raw_dir,
    zip_raw_id: int | None,
    ciks: set[int],
    name_re,
    progress: Callable[[Backfilled], None] | None = None,
) -> Backfilled:
    """Profiles for `ciks` from the main members of bulk ZIP `zip_raw_id` (if any), then from
    every stored per-CIK main response (`edgar_submissions`), newer winning. No request."""
    out = Backfilled(zip_raw_id=zip_raw_id)
    if zip_raw_id is not None:
        seen = seen_at_of(con, zip_raw_id)
        with raw.open(con, zip_raw_id, raw_dir=raw_dir) as fh, zipfile.ZipFile(fh) as zf:
            con.execute("BEGIN TRANSACTION")
            for info in zf.infolist():
                m = name_re.fullmatch(info.filename)
                if m is None or "-submissions-" in info.filename or int(m.group(1)) not in ciks:
                    continue
                try:
                    with zf.open(info) as member:
                        doc = json.load(member)
                    if doc:
                        out.written += store(con, int(m.group(1)), doc, seen_at=seen,
                                             raw_id=zip_raw_id)  # fmt: skip
                except (ProfileError, ValueError) as exc:
                    out.errors.append(f"{info.filename}: {exc}")
                    continue
                out.members += 1
                if progress is not None and out.members % 1000 == 0:
                    progress(out)
            con.execute("COMMIT")
    rows = con.execute(
        "SELECT id, url, fetched_at FROM raw_response WHERE dataset = 'edgar_submissions'"
        " AND http_status = 200 AND pruned_at IS NULL ORDER BY fetched_at, id"
    ).fetchall()
    for raw_id, url, fetched_at in rows:
        name = url.rsplit("/", 1)[-1]
        m = name_re.fullmatch(name)
        if m is None or "-submissions-" in name:
            continue
        try:
            with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
                doc = json.loads(fh.read())
            out.written += store(con, int(m.group(1)), doc, seen_at=fetched_at, raw_id=raw_id)
        except (ProfileError, ValueError) as exc:
            out.errors.append(f"raw {raw_id}: {exc}")
            continue
        out.responses += 1
    return out
