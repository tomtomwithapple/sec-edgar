"""Content-addressed raw store: every response body on disk, one `raw_response` row per fetch.

Blobs live under the raw dir (`USFIN_RAW`) at `<source>/<sha[:2]>/<sha>.gz`, gzipped, where sha
is the SHA-256 of the uncompressed body. Identical bodies share one blob. A body larger than
`LARGE_BODY_BYTES` (EDGAR bulk ZIPs) is kept uncompressed as `<sha>.zip`. Bodies are streamed to
a temp file in chunks while hashing, so no body is ever held whole in memory.

Only bulk datasets are ever pruned (`prune`); everything else is kept forever.
"""

import gzip
import os
import shutil
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, Protocol

import duckdb

from usfin.settings import load_settings

LARGE_BODY_BYTES = 64 * 1024 * 1024
CHUNK_SIZE = 1024 * 1024
BULK_DATASETS = ("bulk_submissions", "bulk_companyfacts")


class RawPruned(Exception):
    """The raw response's blob was deleted by `prune`."""


class _Readable(Protocol):
    def read(self, size: int = ..., /) -> bytes: ...


Body = bytes | _Readable | Iterable[bytes]


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _raw_dir(raw_dir: Path | str | None) -> Path:
    return Path(raw_dir) if raw_dir is not None else load_settings().raw_dir


def _chunks(body: Body) -> Iterator[bytes]:
    if isinstance(body, bytes | bytearray | memoryview):
        view = memoryview(body)
        for start in range(0, len(view), CHUNK_SIZE):
            yield bytes(view[start : start + CHUNK_SIZE])
    elif hasattr(body, "read"):
        while chunk := body.read(CHUNK_SIZE):
            yield chunk
    else:
        yield from body


def _write_blob(root: Path, source: str, body: Body) -> tuple[str, str, int]:
    """Store the body; return (sha256, blob_path relative to root, uncompressed size)."""
    tmp_dir = root / source
    tmp_dir.mkdir(parents=True, exist_ok=True)
    spool = tmp_dir / f".tmp-{uuid.uuid4().hex}"
    digest = sha256()
    size = 0
    try:
        with spool.open("wb") as out:
            for chunk in _chunks(body):
                digest.update(chunk)
                out.write(chunk)
                size += len(chunk)
        sha = digest.hexdigest()
        ext = "zip" if size > LARGE_BODY_BYTES else "gz"
        rel = f"{source}/{sha[:2]}/{sha}.{ext}"
        final = root / rel
        if final.exists():
            return sha, rel, size
        final.parent.mkdir(parents=True, exist_ok=True)
        if ext == "zip":
            os.replace(spool, final)
        else:
            packed = spool.with_suffix(".gz")
            try:
                with spool.open("rb") as src, gzip.open(packed, "wb") as dst:
                    shutil.copyfileobj(src, dst, CHUNK_SIZE)
                os.replace(packed, final)
            finally:
                packed.unlink(missing_ok=True)
        return sha, rel, size
    finally:
        spool.unlink(missing_ok=True)


def save(
    con: duckdb.DuckDBPyConnection,
    *,
    source: str,
    dataset: str,
    url: str,
    body: Body,
    http_status: int,
    content_type: str | None = None,
    request_sha256: str | None = None,
    fetched_at: datetime | None = None,
    raw_dir: Path | str | None = None,
) -> int:
    """Store `body` (bytes, a binary stream or an iterable of chunks); return the new raw id."""
    sha, rel, size = _write_blob(_raw_dir(raw_dir), source, body)
    return con.execute(
        "INSERT INTO raw_response (source, dataset, url, request_sha256, fetched_at,"
        " http_status, sha256, blob_path, bytes, content_type)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
        [
            source,
            dataset,
            url,
            request_sha256,
            fetched_at or _utcnow(),
            http_status,
            sha,
            rel,
            size,
            content_type,
        ],
    ).fetchone()[0]


def open(
    con: duckdb.DuckDBPyConnection, raw_id: int, *, raw_dir: Path | str | None = None
) -> BinaryIO:
    """Readable binary stream of the stored (uncompressed) body. Close it when done."""
    row = con.execute(
        "SELECT blob_path, pruned_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if row is None:
        raise KeyError(f"no raw_response with id {raw_id}")
    rel, pruned_at = row
    if pruned_at is not None:
        raise RawPruned(f"raw_response {raw_id} was pruned at {pruned_at}")
    path = _raw_dir(raw_dir) / rel
    if rel.endswith(".gz"):
        return gzip.open(path, "rb")
    return path.open("rb")


@dataclass(frozen=True)
class PruneCandidate:
    raw_id: int
    dataset: str
    fetched_at: datetime
    blob_path: str
    bytes: int


def prune(
    con: duckdb.DuckDBPyConnection,
    *,
    keep: int = 2,
    dry_run: bool = False,
    raw_dir: Path | str | None = None,
) -> list[PruneCandidate]:
    """Prune bulk-dataset rows whose blob is older than the newest `keep` blobs per dataset.

    Blobs are ranked by the latest fetch that produced them, so an unchanged ZIP downloaded twice
    counts once. Candidate rows get `pruned_at`; their blobs are deleted unless an unpruned row
    still uses them. With `dry_run` nothing changes. Returns the candidate rows.
    """
    if keep < 0:
        raise ValueError(f"keep must be >= 0, got {keep}")
    placeholders = ", ".join("?" for _ in BULK_DATASETS)
    rows = con.execute(
        f"""
        WITH live AS (
            SELECT * FROM raw_response
            WHERE dataset IN ({placeholders}) AND pruned_at IS NULL
        ),
        blobs AS (
            SELECT dataset, blob_path,
                   row_number() OVER (
                       PARTITION BY dataset ORDER BY max(fetched_at) DESC, max(id) DESC
                   ) AS rank
            FROM live GROUP BY dataset, blob_path
        )
        SELECT l.id, l.dataset, l.fetched_at, l.blob_path, l.bytes
        FROM live l JOIN blobs b USING (dataset, blob_path)
        WHERE b.rank > ?
        ORDER BY l.dataset, l.fetched_at, l.id
        """,
        [*BULK_DATASETS, keep],
    ).fetchall()
    candidates = [PruneCandidate(*r) for r in rows]
    if dry_run or not candidates:
        return candidates

    root = _raw_dir(raw_dir)
    ids = [c.raw_id for c in candidates]
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            "UPDATE raw_response SET pruned_at = ? WHERE id IN (SELECT unnest(?::BIGINT[]))",
            [_utcnow(), ids],
        )
        still_used = {
            r[0]
            for r in con.execute(
                "SELECT DISTINCT blob_path FROM raw_response"
                " WHERE pruned_at IS NULL AND blob_path IN (SELECT unnest(?::VARCHAR[]))",
                [sorted({c.blob_path for c in candidates})],
            ).fetchall()
        }
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    for blob in sorted({c.blob_path for c in candidates} - still_used):
        (root / blob).unlink(missing_ok=True)
    return candidates
