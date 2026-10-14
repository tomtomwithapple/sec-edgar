"""TIC cross-border securities, point in time (S43).

Job `tic`, one watermark key `cslt`: the Fed/Treasury CSLT file
(`ticdata.treasury.gov/.../Documents/cslt.zip`, raw dataset `cslt`), one `cslt.json` holding
every series — foreign holdings (`_pos`), net transactions (`_net`) and valuation changes
(`_valchg`) of US securities by country (`for_*`), and the same for US holdings of foreign
securities (`us_*`), monthly, $ millions, from 1984-12. The old TIC text files (`mfh.txt`,
`slt1d_globl.csv`) stopped at 2023-01.

Every release re-sends the whole history, revisions included, so a row is a new version only when
its value differs from the version stored before; `first_seen_at` is the response's `fetched_at`
(the file says when it was transmitted, not when it was published). Once the key has a watermark,
a run asks with `If-Modified-Since` (the newest 200's `fetched_at`): the server answers 304 when
nothing changed (it ignores `If-None-Match`), and a 304 stores nothing. Without a watermark the
request is unconditional, so a load that failed is fetched whole again.

The JSON (122 MB) is read by DuckDB (`read_json` + `unnest`, ~3 s) from a scratch copy beside the
raw store. The watermark is the newest observation month.
"""

import re
import tempfile
import zipfile
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path

from usfin import alerts, jobs, raw

URL = "https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/cslt.zip"
SOURCE = "tic"
DATASET = "cslt"
KEY = "cslt"
MEMBER = "cslt.json"
TABLE = "tic_observation"
LATEST_VIEW = "tic_observation_latest"
MAX_JSON = 1 << 30
"""`read_json`'s `maximum_object_size`: the file is one 122 MB object (2026-09)."""
_ID = re.compile(r"^(?P<measure>(?:for|us)_[a-z_]+?)_(?P<kind>pos|net|valchg)_(?P<code>\d+)$")
_SERIES = "STRUCT(source_id VARCHAR, metadata JSON, observations VARCHAR[][])[]"


class TicError(Exception):
    """The server answered neither 200 nor 304, or the ZIP lacks `cslt.json`."""


def split_series_id(series_id: str) -> tuple[str, str, str]:
    """`for_treas_pos_42218` -> (`for_treas`, `pos`, `42218`)."""
    m = _ID.match(series_id)
    if not m:
        raise ValueError(f"not a CSLT series id: {series_id!r}")
    return m["measure"], m["kind"], m["code"]


def _last_ok(ctx: "jobs.JobContext") -> datetime | None:
    return ctx.con.execute(
        "SELECT max(fetched_at) FROM raw_response WHERE source = ? AND dataset = ?"
        " AND http_status = 200",
        [SOURCE, DATASET],
    ).fetchone()[0]


def _fetch(ctx: "jobs.JobContext", key: str) -> list[int]:
    headers = {}
    since = _last_ok(ctx) if key in ctx.watermarks else None
    if since is not None:
        headers["If-Modified-Since"] = format_datetime(since.replace(tzinfo=UTC), usegmt=True)
    raw_id, response = ctx.client.get(
        URL, source=SOURCE, dataset=DATASET, headers=headers, stream=True
    )
    if response.status_code not in (200, 304):
        raise TicError(f"TIC answered {response.status_code} for {URL}")
    return [raw_id]


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    con = ctx.con
    status, seen = con.execute(
        "SELECT http_status, fetched_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if status != 200:
        return 0  # 304: nothing changed
    scratch = Path(ctx.settings.raw_dir)
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".scratch-tic-", dir=scratch) as tmp:
        path = Path(tmp) / MEMBER
        with raw.open(con, raw_id, raw_dir=ctx.settings.raw_dir) as fh, zipfile.ZipFile(fh) as zf:
            if MEMBER not in zf.namelist():
                raise TicError(f"raw_response {raw_id}: no {MEMBER} in {zf.namelist()}")
            with zf.open(MEMBER) as src, path.open("wb") as dst:
                while chunk := src.read(raw.CHUNK_SIZE):
                    dst.write(chunk)
        con.execute(
            "CREATE OR REPLACE TEMP TABLE tic_doc AS SELECT"
            " TRY_CAST(transmissionDt AS TIMESTAMP) AS transmitted_at, unnest(series) AS s"
            f" FROM read_json(?, maximum_object_size = {MAX_JSON},"
            f" columns = {{'transmissionDt': 'VARCHAR', 'series': '{_SERIES}'}})",
            [str(path)],
        )
    try:
        return _store(ctx, raw_id, seen)
    finally:
        con.execute("DROP TABLE IF EXISTS tic_doc")
        con.execute("DROP TABLE IF EXISTS tic_new")


def _store(ctx: "jobs.JobContext", raw_id: int, seen: datetime) -> int:
    con = ctx.con
    ids = [sid for (sid,) in con.execute("SELECT s.source_id FROM tic_doc").fetchall()]
    parts = [(sid, *split_series_id(sid)) for sid in ids]
    con.execute(
        "CREATE OR REPLACE TEMP TABLE tic_ids"
        " (series_id VARCHAR, measure VARCHAR, kind VARCHAR, country_code VARCHAR)"
    )
    if parts:
        con.executemany("INSERT INTO tic_ids VALUES (?, ?, ?, ?)", parts)
    con.execute(
        "INSERT INTO tic_series SELECT d.s.source_id, d.s.metadata->>'$.title', i.measure,"
        " i.kind, i.country_code, d.s.metadata->>'$.additional.geography.name',"
        " coalesce(d.s.metadata->>'$.additional.status', 'A') <> 'D',"
        " d.s.metadata->>'$.units', d.s.metadata->>'$.frequency', d.transmitted_at, ?"
        " FROM tic_doc d JOIN tic_ids i ON i.series_id = d.s.source_id"
        " ON CONFLICT (series_id) DO UPDATE SET title = excluded.title,"
        " measure = excluded.measure, kind = excluded.kind,"
        " country_code = excluded.country_code, country = excluded.country,"
        " active = excluded.active, units = excluded.units, frequency = excluded.frequency,"
        " transmitted_at = excluded.transmitted_at, raw_id = excluded.raw_id",
        [raw_id],
    )
    con.execute("DROP TABLE tic_ids")
    con.execute(
        "CREATE OR REPLACE TEMP TABLE tic_new AS SELECT s.source_id AS series_id,"
        " CAST(ob[1] AS DATE) AS date, TRY_CAST(ob[2] AS DECIMAL(24, 6)) AS value"
        " FROM (SELECT s, unnest(s.observations) AS ob FROM tic_doc)"
    )
    # the version stored at or before this instant; a row is new when there is none, or when
    # its value differs and the instant itself is not stored yet (the first record keeps it)
    added = con.execute(
        f"INSERT INTO {TABLE} (series_id, date, value, first_seen_at, raw_id)"
        " SELECT n.series_id, n.date, n.value, ?, ? FROM tic_new n"
        " LEFT JOIN ("
        f"   SELECT series_id, date, value, first_seen_at FROM {TABLE}"
        "   WHERE first_seen_at <= ?"
        "   QUALIFY row_number() OVER (PARTITION BY series_id, date"
        "     ORDER BY first_seen_at DESC) = 1"
        " ) p USING (series_id, date)"
        " WHERE p.series_id IS NULL"
        "   OR (p.value IS DISTINCT FROM n.value AND p.first_seen_at < ?)",
        [seen, raw_id, seen, seen],
    ).fetchone()[0]
    newest = con.execute(f"SELECT max(date) FROM {TABLE}").fetchone()[0]
    if newest is not None:
        ctx.advance(KEY, newest)
    return added


def _observed(settings) -> dict[str, alerts.Observed]:
    # monthly, no release calendar: no due days; value_jump off (4 260 series, flows flip sign)
    return {
        KEY: alerts.Observed(
            table=LATEST_VIEW, date_column="date", match={}, series=("series_id",), values=()
        )
    }


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="tic",
        keys=(KEY,),
        fetch=_fetch,
        parsers={DATASET: _parse},
        versioned=(TABLE,),
        observed=_observed,
    )
)
"""One key, the whole CSLT file; versioned rows in `tic_observation`."""
