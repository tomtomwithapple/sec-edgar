"""EDGAR bulk backfill: submissions.zip and companyfacts.zip, member by member (C1, C17).

`usfin edgar backfill {submissions|companyfacts}` reads the ZIP's `Content-Length` (HEAD) and the
free space of the raw and DB volumes first (`guard`): if free − ZIP − projected DB growth would
fall below `USFIN_MIN_FREE_GB` (GiB) it downloads nothing. Otherwise the ZIP is streamed into the
raw store (`http.Client(stream=True)` → `raw.save`, kept uncompressed as `<sha>.zip`, raw datasets
`bulk_submissions` / `bulk_companyfacts`), and its members are read straight out of that blob
(`zipfile`, no extraction to disk; one member's JSON in memory at a time) and stored with the same
`store_document(con, doc, name=)` the per-CIK jobs use (a member that is exactly `{}` — 62 in
companyfacts.zip on 2026-10-08 — has nothing to store and is only recorded). Each member commits on its own together
with its CRC-32 in `edgar_bulk_member`, so a member whose CRC is already stored is skipped: a load
stopped by the guard (re-checked every `GUARD_EVERY` members) or a failure resumes where it left
off. `--limit N` parses N members spread evenly over the ZIP (the first N CIKs are the oldest
registrants, not a fair sample); submissions members are ordered main document before its pages,
so a page lands on the listing its main document wrote.

A load of the whole ZIP (not a `--cik` pick) records its cost in `edgar_bulk_estimate`;
`usfin edgar backfill --estimate` projects the full load from the newest row: DB growth and
parse time per member × members, and per uncompressed member byte × member bytes. For
companyfacts the sampled facts are also copied into scratch DuckDB files with and without the
`xbrl_fact` primary key (S21: 91 vs 21 bytes per fact), the user's lever if the full load does not
fit. Acceptance times of bulk submissions members are read as S20 did (`fetched_at` not passed;
see the plan's Submissions note).

*(S27.)* Submissions members are loaded only for CIKs in `edgar_company` (active or not), pages
included (`Spec.companies_only`; the ~985 000 other filers stay out). Only a `--limit` sample is
copied into scratch files; a full load records its cost without them. Job `edgar_bulk` (Sun
03:00 ET, calendar any) re-downloads both ZIPs through the guard — `disk_low` alert and a
`skipped` run when one does not fit — re-parses the members whose CRC changed (at most
`USFIN_BULK_LIMIT` per ZIP when set), then prunes the bulk blobs to the newest 2 per dataset.
"""

import json
import logging
import re
import tempfile
import time
import zipfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from shutil import disk_usage
from typing import Any

import duckdb

from usfin import alerts, http, jobs, raw
from usfin.settings import Settings
from usfin.sources.edgar import companyfacts, submissions

SOURCE = "edgar"
HEAD_DATASET = "edgar_bulk_head"
MEMBERS = "edgar_bulk_member"
ESTIMATES = "edgar_bulk_estimate"
GUARD_EVERY = 1000
GIB = 2**30
log = logging.getLogger("usfin.edgar.bulk")


@dataclass(frozen=True)
class Spec:
    name: str
    url: str
    raw_dataset: str
    store: Callable[..., Any]
    name_re: Any
    companies_only: bool = False
    """Load only members whose CIK is in `edgar_company` (Decisions after S26 (2))."""

    def cik_of(self, member: str) -> int | None:
        m = self.name_re.fullmatch(member)
        return int(m.group(1)) if m else None


DATASETS: Mapping[str, Spec] = {
    "submissions": Spec(
        "submissions",
        "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
        "bulk_submissions",
        lambda con, doc, name, raw_id: submissions.store_document(
            con, doc, name=name, raw_id=raw_id
        ).filings,
        submissions.NAME,
        companies_only=True,
    ),
    "companyfacts": Spec(
        "companyfacts",
        "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        "bulk_companyfacts",
        lambda con, doc, name, raw_id: companyfacts.store_document(
            con, doc, name=name, raw_id=raw_id
        ).facts,
        companyfacts.NAME,
    ),
}


MEMBER_ERRORS = (
    companyfacts.CompanyFactsError,
    submissions.SubmissionsError,
    ValueError,  # json.JSONDecodeError, bad dates and numbers
    KeyError,
    TypeError,
    zipfile.BadZipFile,
    duckdb.Error,
)
"""What a malformed member can raise; it is rolled back, listed and the load goes on."""


class BulkError(Exception):
    """The bulk ZIP could not be sized or fetched."""


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def gib(n: float) -> str:
    return f"{n / GIB:.2f} GiB"


# --- free-disk guard -----------------------------------------------------------------------------


def _existing(path: Path) -> Path:
    path = path.resolve()
    while not path.exists() and path != path.parent:
        path = path.parent
    return path


def free_bytes(settings: Settings) -> int:
    """Free bytes on the fuller of the raw and DB volumes."""
    paths = [settings.raw_dir, settings.db_path.parent]
    return min(disk_usage(str(_existing(Path(p)))).free for p in paths)


@dataclass(frozen=True)
class Guard:
    free: int
    zip_bytes: int
    projected: int
    floor: int

    @property
    def left(self) -> int:
        return self.free - self.zip_bytes - self.projected

    @property
    def ok(self) -> bool:
        return self.left >= self.floor

    def describe(self) -> str:
        return (
            f"free {gib(self.free)} − zip {self.zip_bytes} bytes ({gib(self.zip_bytes)})"
            f" − projected {gib(self.projected)} = {gib(self.left)}"
            f" {'≥' if self.ok else '<'} floor {self.floor / GIB:.1f} GiB"
        )


def floor_bytes(settings: Settings) -> int:
    return int(settings.min_free_gb * GIB)


class DiskLow(Exception):
    """The streamed ZIP's `Content-Length` does not fit the guard: nothing was written."""

    def __init__(self, g: Guard):
        super().__init__(f"disk low: {g.describe()}; not downloading")
        self.guard = g


_CONTENT_RANGE = re.compile(r"bytes \d+-\d+/(\d+)")


def head(client: http.Client, spec: Spec) -> int:
    """The ZIP's size: the HEAD's `Content-Length`, else a 1-byte ranged GET's total.

    S35: SEC refused a HEAD on an Archives file whose GET worked (S36), while HEADs of both bulk
    ZIPs answered 200 with their size (S26, S27, S35). So a refused HEAD (the client opens no SEC
    block for it) or one without a size falls back to `GET` with `Range: bytes=0-0` and reads
    `Content-Range: bytes 0-0/<size>` (live 206 on 2026-10-08). A server ignoring the Range (200,
    whole body) is refused before the body is read; any other answer raises `BulkError`, a 403
    blocks SEC as usual — the guard is never skipped. Every answer is archived as
    `edgar_bulk_head`."""
    raw_id, response = client.request("HEAD", spec.url, source=SOURCE, dataset=HEAD_DATASET)
    length = response.headers.get("Content-Length")
    if response.status_code == 200 and length and length.isdigit():
        return int(length)
    why = (
        f"HEAD {spec.url} answered {response.status_code} (raw {raw_id})"
        if response.status_code != 200
        else f"HEAD {spec.url} gave no Content-Length (raw {raw_id})"
    )
    log.info("%s; sizing with a ranged GET", why)

    def partial_only(r) -> None:
        if r.status_code == 200:
            raise BulkError(
                f"{why}, and a ranged GET ignored its Range (200, whole body);"
                " size unknown, not downloading"
            )

    raw_id, response = client.request(
        "GET", spec.url, source=SOURCE, dataset=HEAD_DATASET, headers={"Range": "bytes=0-0"},
        stream=True, inspect=partial_only,
    )  # fmt: skip
    m = _CONTENT_RANGE.fullmatch(response.headers.get("Content-Range", "").strip())
    if response.status_code != 206 or m is None:
        raise BulkError(
            f"{why}; the ranged GET answered {response.status_code} without a size"
            f" (raw {raw_id}); size unknown, not downloading"
        )
    return int(m[1])


def guard(
    con: duckdb.DuckDBPyConnection, settings: Settings, spec: Spec, zip_bytes: int,
    *, limit: int | None = None,
) -> Guard:  # fmt: skip
    """Free space vs ZIP + projected growth (from the newest estimate, if any) vs the floor."""
    projected = 0
    est = latest_estimate(con, spec)
    if est is not None and est.members_sampled:
        members = est.members_total if limit is None else min(limit, est.members_total)
        projected = max(0, int(est.growth / est.members_sampled * members))
    return Guard(free_bytes(settings), zip_bytes, projected, floor_bytes(settings))


# --- download and members -----------------------------------------------------------------------


def download(
    client: http.Client, spec: Spec, *, fits: Callable[[int], Guard] | None = None
) -> int:
    """Stream the ZIP into the raw store; return its raw id.

    With `fits`, the GET's own `Content-Length` goes through the guard before a byte is written
    (S35): a ZIP larger than the HEAD said raises `DiskLow` and nothing is archived."""

    def check(response) -> None:
        length = response.headers.get("Content-Length", "")
        if fits is not None and response.status_code == 200 and length.isdigit():
            g = fits(int(length))
            if not g.ok:
                raise DiskLow(g)

    raw_id, response = client.get(
        spec.url, source=SOURCE, dataset=spec.raw_dataset, stream=True, inspect=check
    )
    if response.status_code != 200:
        raise BulkError(f"GET {spec.url} answered {response.status_code} (raw {raw_id})")
    return raw_id


def newest_zip(con: duckdb.DuckDBPyConnection, spec: Spec) -> int | None:
    row = con.execute(
        "SELECT id FROM raw_response WHERE dataset = ? AND http_status = 200 AND pruned_at IS NULL"
        " ORDER BY fetched_at DESC, id DESC LIMIT 1",
        [spec.raw_dataset],
    ).fetchone()
    return None if row is None else row[0]


def _order(name: str) -> tuple:
    stem = name.removesuffix(".json")
    base, _, page = stem.partition("-submissions-")
    return (base, page != "", page, name)


def select_members(
    infos: Sequence[zipfile.ZipInfo],
    *,
    done: Mapping[str, int],
    limit: int | None,
    ciks: Iterable[int] | None,
    spec: Spec | None = None,
) -> list[zipfile.ZipInfo]:
    """Members still to parse (CRC not stored), main documents before their pages; with `ciks`
    only those companies; with `limit` that many spread evenly over the ZIP."""
    todo = sorted(
        (i for i in infos if not i.is_dir() and done.get(i.filename) != i.CRC),
        key=lambda i: _order(i.filename),
    )
    if ciks is not None and spec is not None:
        wanted = set(ciks)
        todo = [i for i in todo if spec.cik_of(i.filename) in wanted]
    if limit is None or limit >= len(todo):
        return todo
    if limit <= 0:
        return []
    step = len(todo) / limit
    return [todo[int(k * step)] for k in range(limit)]


def company_ciks(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Every CIK in `edgar_company`, active or not."""
    return {r[0] for r in con.execute("SELECT DISTINCT cik FROM edgar_company").fetchall()}


@dataclass
class Loaded:
    members_total: int = 0
    """Members a full load would parse: the ZIP's, less those `filtered`."""
    member_bytes_total: int = 0
    filtered: int = 0
    """Members left out because their CIK is not in `edgar_company` (submissions only)."""
    parsed: int = 0
    member_bytes_parsed: int = 0
    rows: int = 0
    skipped: int = 0
    empty: int = 0
    """Members that are exactly `{}` (companyfacts.zip has 62): parsed, no rows."""
    errors: list[str] | None = None
    stopped: Guard | None = None
    seconds: float = 0.0
    ciks: list[int] | None = None


def load(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    spec: Spec,
    raw_id: int,
    *,
    limit: int | None = None,
    ciks: Sequence[int] | None = None,
    progress: Callable[[Loaded], None] | None = None,
) -> Loaded:
    """Parse the ZIP's members in place, each in its own transaction with its CRC."""
    out = Loaded(errors=[], ciks=[])
    done = dict(
        con.execute(f"SELECT name, crc FROM {MEMBERS} WHERE dataset = ?", [spec.raw_dataset])
        .fetchall()
    )
    started = time.monotonic()
    with raw.open(con, raw_id, raw_dir=settings.raw_dir) as fh, zipfile.ZipFile(fh) as zf:
        infos = [i for i in zf.infolist() if not i.is_dir()]
        if spec.companies_only:
            wanted = company_ciks(con)
            kept = [i for i in infos if spec.cik_of(i.filename) in wanted]
            out.filtered = len(infos) - len(kept)
            infos = kept
        out.members_total = len(infos)
        out.member_bytes_total = sum(i.file_size for i in infos)
        for info in select_members(infos, done=done, limit=limit, ciks=ciks, spec=spec):
            cik = spec.cik_of(info.filename)
            if cik is None:
                out.skipped += 1
                continue
            con.execute("BEGIN TRANSACTION")
            try:
                with zf.open(info) as member:
                    doc = json.load(member)
                if doc == {}:
                    out.empty += 1
                    rows = 0
                else:
                    rows = spec.store(con, doc, info.filename, raw_id)
                con.execute(
                    f"INSERT INTO {MEMBERS} VALUES (?, ?, ?, ?) ON CONFLICT (dataset, name)"
                    " DO UPDATE SET crc = excluded.crc, raw_id = excluded.raw_id",
                    [spec.raw_dataset, info.filename, info.CRC, raw_id],
                )
                con.execute("COMMIT")
            except MEMBER_ERRORS as exc:  # one bad member must not sink the whole load
                con.execute("ROLLBACK")
                out.errors.append(f"{info.filename}: {exc}")
                continue
            out.parsed += 1
            out.member_bytes_parsed += info.file_size
            out.rows += rows
            out.ciks.append(cik)
            if progress is not None:
                progress(out)
            if out.parsed % GUARD_EVERY == 0:
                g = Guard(free_bytes(settings), 0, 0, floor_bytes(settings))
                if not g.ok:
                    out.stopped = g
                    break
    out.seconds = time.monotonic() - started
    return out


# --- estimate ------------------------------------------------------------------------------------


def db_bytes(con: duckdb.DuckDBPyConnection, settings: Settings) -> int:
    """The DB file's size after a CHECKPOINT (WAL folded in)."""
    con.execute("CHECKPOINT")
    path = Path(settings.db_path)
    wal = path.with_name(path.name + ".wal")
    return path.stat().st_size + (wal.stat().st_size if wal.exists() else 0)


_SAMPLE_ROWS = {
    # table -> rows that came from the sampled companies' members
    "bulk_companyfacts": {
        companyfacts.TABLE: "cik IN (SELECT cik FROM _sample_ciks)",
    },
    "bulk_submissions": {
        "edgar_filing": "accession_no IN (SELECT accession_no FROM edgar_filing_party"
        " WHERE role = 'submissions' AND cik IN (SELECT cik FROM _sample_ciks))",
        "edgar_filing_party": "role = 'submissions' AND cik IN (SELECT cik FROM _sample_ciks)",
        submissions.PAGES: "cik IN (SELECT cik FROM _sample_ciks)",
    },
}


def _ddl(con: duckdb.DuckDBPyConnection, table: str, *, pk: bool) -> str:
    sql = con.execute("SELECT sql FROM duckdb_tables() WHERE table_name = ?", [table]).fetchone()[0]
    if not pk and ", PRIMARY KEY" in sql:
        sql = sql[: sql.index(", PRIMARY KEY")] + ");"
    return sql.replace("CREATE TABLE ", "CREATE TABLE scratch.", 1)


def measure(
    con: duckdb.DuckDBPyConnection, settings: Settings, spec: Spec, ciks: Sequence[int]
) -> tuple[int, int, int | None]:
    """Copy the rows the given companies' members produced into a fresh DuckDB file with the
    real schema, and for companyfacts also without the `xbrl_fact` primary key; return (rows of
    the dataset's main table, bytes with PKs, bytes without or None). Files are deleted.

    A fresh file measures what the rows cost; the live DB file's own growth over a few hundred
    small transactions is mostly allocation churn (S26)."""
    tables = _SAMPLE_ROWS[spec.raw_dataset]
    main = next(iter(tables))
    variants = [True, False] if spec.name == "companyfacts" else [True]
    folder = _existing(Path(settings.db_path).parent)
    sizes: list[int] = []
    rows = 0
    con.execute("CREATE OR REPLACE TEMP TABLE _sample_ciks (cik BIGINT)")
    con.execute("INSERT INTO _sample_ciks SELECT unnest(?::BIGINT[])", [list(ciks)])
    try:
        with tempfile.TemporaryDirectory(prefix=".scratch-", dir=folder) as tmp:
            for pk in variants:
                path = Path(tmp) / f"{'pk' if pk else 'nopk'}.duckdb"
                con.execute(f"ATTACH '{path}' AS scratch")
                try:
                    for table, where in tables.items():
                        con.execute(_ddl(con, table, pk=pk))
                        con.execute(
                            f"INSERT INTO scratch.{table} SELECT * FROM {table} WHERE {where}"
                        )
                    rows = con.execute(f"SELECT count(*) FROM scratch.{main}").fetchone()[0]
                    con.execute("CHECKPOINT scratch")
                finally:
                    con.execute("DETACH scratch")
                sizes.append(path.stat().st_size)
    finally:
        con.execute("DROP TABLE _sample_ciks")
    return rows, sizes[0], sizes[1] if len(sizes) > 1 else None


@dataclass(frozen=True)
class Estimate:
    dataset: str
    measured_at: datetime
    raw_id: int
    zip_bytes: int
    members_total: int
    members_sampled: int
    member_bytes_total: int
    member_bytes_sampled: int
    db_bytes_before: int
    db_bytes_after: int
    rows_inserted: int
    seconds: float
    download_seconds: float | None
    scratch_rows: int | None
    scratch_bytes_pk: int | None
    scratch_bytes_no_pk: int | None

    @property
    def growth(self) -> int:
        return self.db_bytes_after - self.db_bytes_before

    def per_member(self, value: float) -> float:
        return value / self.members_sampled * self.members_total if self.members_sampled else 0.0

    def per_byte(self, value: float) -> float:
        if not self.member_bytes_sampled:
            return 0.0
        return value / self.member_bytes_sampled * self.member_bytes_total


_FIELDS = tuple(Estimate.__dataclass_fields__)


def record_estimate(con: duckdb.DuckDBPyConnection, est: Estimate) -> None:
    con.execute(
        f"INSERT INTO {ESTIMATES} ({', '.join(_FIELDS)}) VALUES ({', '.join('?' * len(_FIELDS))})",
        [getattr(est, f) for f in _FIELDS],
    )


def latest_estimate(con: duckdb.DuckDBPyConnection, spec: Spec) -> Estimate | None:
    row = con.execute(
        f"SELECT {', '.join(_FIELDS)} FROM {ESTIMATES} WHERE dataset = ?"
        " ORDER BY measured_at DESC LIMIT 1",
        [spec.raw_dataset],
    ).fetchone()
    return None if row is None else Estimate(*row)


def zip_bytes_of(con: duckdb.DuckDBPyConnection, raw_id: int) -> int:
    return con.execute("SELECT bytes FROM raw_response WHERE id = ?", [raw_id]).fetchone()[0]


def estimate_lines(est: Estimate, settings: Settings, free: int | None = None) -> list[str]:
    """`usfin edgar backfill --estimate`: the projection of a full load from one sample."""
    name = est.dataset.removeprefix("bulk_")
    share = est.member_bytes_sampled / est.member_bytes_total if est.member_bytes_total else 0
    lines = [
        (
            f"{name}: measured {est.measured_at:%Y-%m-%d %H:%M}Z from raw {est.raw_id},"
            f" zip {est.zip_bytes} bytes ({gib(est.zip_bytes)})"
        ),
        (
            f"  members {est.members_total}, sampled {est.members_sampled}"
            f" ({share:.2%} of {gib(est.member_bytes_total)} uncompressed),"
            f" rows {est.rows_inserted}"
        ),
        f"  db {est.db_bytes_before} → {est.db_bytes_after} bytes (+{est.growth})",
        (
            f"  projected db growth {gib(est.per_member(est.growth))} per member,"
            f" {gib(est.per_byte(est.growth))} by member bytes"
        ),
    ]
    if est.scratch_bytes_pk is not None and est.scratch_rows and est.scratch_bytes_no_pk is None:
        pk = est.scratch_bytes_pk
        lines.append(
            f"  scratch {est.scratch_rows} filings (+ parties, pages): {pk} bytes"
            f" ({pk / est.scratch_rows:.0f}/filing)"
        )
        lines.append(
            f"  projected scratch (with PKs) {gib(est.per_member(pk))} per member,"
            f" {gib(est.per_byte(pk))} by member bytes"
        )
    elif est.scratch_bytes_pk is not None and est.scratch_rows:
        pk, nopk = est.scratch_bytes_pk, est.scratch_bytes_no_pk or 0
        lines.append(
            f"  scratch {est.scratch_rows} facts: PK {pk} bytes ({pk / est.scratch_rows:.0f}/fact),"
            f" no-PK {nopk} bytes ({nopk / est.scratch_rows:.0f}/fact)"
        )
        lines.append(
            f"  projected xbrl_fact: PK {gib(est.per_member(pk))},"
            f" no-PK {gib(est.per_member(nopk))} per member;"
            f" PK {gib(est.per_byte(pk))}, no-PK {gib(est.per_byte(nopk))} by member bytes"
        )
    rate = est.members_sampled / est.seconds if est.seconds else 0
    dl = "" if est.download_seconds is None else f", download {est.download_seconds:.0f} s"
    lines.append(
        f"  projected time {est.per_member(est.seconds) / 60:.1f} min per member,"
        f" {est.per_byte(est.seconds) / 60:.1f} min by member bytes"
        f" (sample {est.seconds:.1f} s, {rate:.1f} members/s{dl})"
    )
    if free is not None:
        worst = max(est.per_member(est.growth), est.per_byte(est.growth))
        g = Guard(free, est.zip_bytes, int(worst), floor_bytes(settings))
        lines.append(f"  full load: {g.describe()} → {'fits' if g.ok else 'does not fit'}")
    return lines



# --- job edgar_bulk (weekly reconcile) -----------------------------------------------------------

PRUNE_KEY = "prune"
KEEP = 2


def _job_fetch(ctx: "jobs.JobContext", key: str) -> list[int]:
    """One ZIP: HEAD, guard, download, parse the members whose CRC changed. Or, for key
    `prune`, the raw store's bulk blobs down to the newest `KEEP` per dataset."""
    if key == PRUNE_KEY:
        ctx.memo[key] = raw.prune(ctx.con, keep=KEEP, raw_dir=ctx.settings.raw_dir)
        return []
    spec = DATASETS[key]
    limit = ctx.settings.bulk_limit
    g = guard(ctx.con, ctx.settings, spec, head(ctx.client, spec), limit=limit)
    if not g.ok:
        _disk_low(ctx, spec, g)
        raise jobs.JobSkipped(f"disk low: {g.describe()}; not downloading {spec.name}.zip")
    try:
        raw_id = download(
            ctx.client, spec, fits=lambda n: guard(ctx.con, ctx.settings, spec, n, limit=limit)
        )
    except DiskLow as exc:
        _disk_low(ctx, spec, exc.guard)
        raise jobs.JobSkipped(str(exc)) from None
    loaded = load(ctx.con, ctx.settings, spec, raw_id, limit=limit)
    ctx.memo[key] = loaded
    log.info(
        "edgar_bulk %s: raw %s, members %s, parsed %s (CRC changed), rows %s, filtered %s,"
        " errors %s in %.1f s",
        key, raw_id, loaded.members_total, loaded.parsed, loaded.rows, loaded.filtered,
        len(loaded.errors or []), loaded.seconds,
    )  # fmt: skip
    if loaded.stopped is not None:
        _disk_low(ctx, spec, loaded.stopped)
        raise jobs.JobSkipped(
            f"disk low: {loaded.stopped.describe()}; stopped after {loaded.parsed} members"
        )
    if loaded.errors:
        listed = "; ".join(loaded.errors[:5])
        raise BulkError(f"{len(loaded.errors)} malformed members (others committed): {listed}")
    ctx.advance(key, ctx.today)
    return []


def _disk_low(ctx: "jobs.JobContext", spec: Spec, g: Guard) -> None:
    alerts.raise_alert(
        ctx.con, job=ctx.job.name, rule="disk_low", subject=spec.raw_dataset,
        detail={"free": g.free, "zip_bytes": g.zip_bytes, "projected": g.projected,
                "floor": g.floor, "left": g.left, "verdict": g.describe()},
        now=ctx.now,
    )  # fmt: skip


def _job_rows(ctx: "jobs.JobContext", key: str) -> int:
    loaded = ctx.memo.get(key)
    return loaded.rows if isinstance(loaded, Loaded) else 0


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="edgar_bulk",
        keys=("companyfacts", "submissions", PRUNE_KEY),
        fetch=_job_fetch,
        parsers={},
        compute=_job_rows,
    )
)
"""Weekly reconcile (Sun 03:00 ET, calendar any): companyfacts.zip, then submissions.zip, each
through the free-disk guard and parsed where a member's CRC changed, then `raw prune --keep 2`.
The members are parsed while the key is fetched (each member commits on its own, which a parser
inside the runner's transaction could not), so the job has no parsers: `usfin edgar backfill
… --reuse` replays a stored ZIP. Watermark per dataset = the New York date of the last ZIP
fully reconciled."""
