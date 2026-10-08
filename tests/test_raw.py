import gzip
import hashlib
import io
from datetime import UTC, datetime, timedelta

import pytest
from typer.testing import CliRunner

from usfin import raw, store
from usfin.cli import app


@pytest.fixture
def con(tmp_path):
    with store.session(tmp_path / "usfin.duckdb", wait=0) as c:
        yield c


@pytest.fixture
def raw_dir(tmp_path):
    return tmp_path / "raw"


def _save(con, raw_dir, body, *, source="fiscal", dataset="fd_x", fetched_at=None, **kw):
    return raw.save(
        con,
        source=source,
        dataset=dataset,
        url=f"https://example.test/{dataset}",
        body=body,
        http_status=200,
        content_type="application/json",
        fetched_at=fetched_at,
        raw_dir=raw_dir,
        **kw,
    )


def _row(con, raw_id):
    cur = con.execute("SELECT * FROM raw_response WHERE id = ?", [raw_id])
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, cur.fetchone(), strict=True))


def test_save_gzips_body_to_content_addressed_path_and_writes_row(con, raw_dir):
    body = b'{"data": [1, 2, 3]}'
    sha = hashlib.sha256(body).hexdigest()
    raw_id = _save(con, raw_dir, body, request_sha256="req123")

    row = _row(con, raw_id)
    assert row["blob_path"] == f"fiscal/{sha[:2]}/{sha}.gz"
    assert row["sha256"] == sha
    assert row["bytes"] == len(body)
    assert row["source"] == "fiscal" and row["dataset"] == "fd_x"
    assert row["url"] == "https://example.test/fd_x"
    assert row["http_status"] == 200
    assert row["content_type"] == "application/json"
    assert row["request_sha256"] == "req123"
    assert row["fetched_at"] is not None and row["fetched_at"].tzinfo is None
    assert row["pruned_at"] is None

    blob = raw_dir / row["blob_path"]
    assert gzip.decompress(blob.read_bytes()) == body
    assert not [p for p in raw_dir.rglob("*") if p.is_file() and p != blob]  # no temp left


def test_identical_bodies_share_one_blob_but_get_two_rows(con, raw_dir):
    a = _save(con, raw_dir, b"same body")
    b = _save(con, raw_dir, b"same body")
    assert a != b
    assert _row(con, a)["blob_path"] == _row(con, b)["blob_path"]
    blobs = [p for p in raw_dir.rglob("*") if p.is_file()]
    assert len(blobs) == 1


class ChunkReader:
    """A binary stream that refuses unbounded reads and records how it was read."""

    def __init__(self, data: bytes):
        self._buf = io.BytesIO(data)
        self.reads = 0

    def read(self, size=-1):
        assert size is not None and 0 < size <= raw.CHUNK_SIZE, "read the whole body at once"
        self.reads += 1
        return self._buf.read(size)


def test_large_stream_is_stored_uncompressed_as_zip_while_hashing(con, raw_dir, monkeypatch):
    monkeypatch.setattr(raw, "LARGE_BODY_BYTES", 1000)
    monkeypatch.setattr(raw, "CHUNK_SIZE", 256)
    body = bytes(range(256)) * 20  # 5120 bytes > threshold
    sha = hashlib.sha256(body).hexdigest()
    reader = ChunkReader(body)

    raw_id = _save(con, raw_dir, reader, source="edgar", dataset="bulk_submissions")

    assert reader.reads >= len(body) // 256
    row = _row(con, raw_id)
    assert row["blob_path"] == f"edgar/{sha[:2]}/{sha}.zip"
    assert row["sha256"] == sha and row["bytes"] == len(body)
    assert (raw_dir / row["blob_path"]).read_bytes() == body  # stored as-is, not gzipped


def test_iterable_of_chunks_is_accepted(con, raw_dir, monkeypatch):
    monkeypatch.setattr(raw, "LARGE_BODY_BYTES", 10)
    chunks = [b"abcdef", b"ghijkl", b"mnop"]
    raw_id = _save(con, raw_dir, iter(chunks))
    assert _row(con, raw_id)["blob_path"].endswith(".zip")
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        assert fh.read() == b"".join(chunks)


def test_small_stream_is_gzipped(con, raw_dir):
    body = b"x" * 3000
    raw_id = _save(con, raw_dir, ChunkReader(body))
    assert _row(con, raw_id)["blob_path"].endswith(".gz")


@pytest.mark.parametrize("big", [False, True])
def test_open_round_trips(con, raw_dir, monkeypatch, big):
    if big:
        monkeypatch.setattr(raw, "LARGE_BODY_BYTES", 4)
    body = b"round trip body \x00\xff"
    raw_id = _save(con, raw_dir, body)
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        assert fh.read() == body


def test_open_unknown_id_raises(con, raw_dir):
    with pytest.raises(KeyError):
        raw.open(con, 12345, raw_dir=raw_dir)


def test_raw_dir_defaults_to_settings(con, tmp_path, monkeypatch):
    monkeypatch.setenv("USFIN_RAW", str(tmp_path / "envraw"))
    raw_id = raw.save(
        con, source="bls", dataset="bls_timeseries", url="https://x", body=b"b", http_status=200
    )
    assert (tmp_path / "envraw" / _row(con, raw_id)["blob_path"]).is_file()
    with raw.open(con, raw_id) as fh:
        assert fh.read() == b"b"


T0 = datetime(2026, 1, 4, 3, 0, tzinfo=UTC).replace(tzinfo=None)  # stored naive UTC


def _bulk_history(con, raw_dir):
    """Four weekly downloads per bulk dataset (distinct bodies) plus one non-bulk row."""
    ids = {}
    for ds in raw.BULK_DATASETS:
        ids[ds] = [
            _save(
                con,
                raw_dir,
                f"{ds}-{week}".encode(),
                source="edgar",
                dataset=ds,
                fetched_at=T0 + timedelta(weeks=week),
            )
            for week in range(4)
        ]
    old_other = _save(
        con, raw_dir, b"old fiscal", dataset="fd_x", fetched_at=T0 - timedelta(days=900)
    )
    return ids, old_other


def test_prune_keeps_newest_n_per_bulk_dataset(con, raw_dir):
    ids, other = _bulk_history(con, raw_dir)

    result = raw.prune(con, keep=2, raw_dir=raw_dir)

    expected = sorted(i for ds in raw.BULK_DATASETS for i in ids[ds][:2])
    assert sorted(c.raw_id for c in result) == expected
    for ds in raw.BULK_DATASETS:
        old, new = ids[ds][:2], ids[ds][2:]
        for rid in old:
            row = _row(con, rid)
            assert row["pruned_at"] is not None
            assert not (raw_dir / row["blob_path"]).exists()
        for rid in new:
            row = _row(con, rid)
            assert row["pruned_at"] is None
            assert (raw_dir / row["blob_path"]).is_file()
    other_row = _row(con, other)
    assert other_row["pruned_at"] is None and (raw_dir / other_row["blob_path"]).is_file()

    with pytest.raises(raw.RawPruned):
        raw.open(con, ids["bulk_submissions"][0], raw_dir=raw_dir)

    assert raw.prune(con, keep=2, raw_dir=raw_dir) == []  # already pruned: nothing new


def test_prune_counts_blobs_not_rows(con, raw_dir):
    """An unchanged ZIP downloaded twice is one blob; the newest 2 blobs survive."""
    ds = "bulk_companyfacts"
    a = _save(con, raw_dir, b"v1", source="edgar", dataset=ds, fetched_at=T0)
    b = _save(con, raw_dir, b"v2", source="edgar", dataset=ds, fetched_at=T0 + timedelta(weeks=1))
    c = _save(con, raw_dir, b"v3", source="edgar", dataset=ds, fetched_at=T0 + timedelta(weeks=2))
    d = _save(con, raw_dir, b"v3", source="edgar", dataset=ds, fetched_at=T0 + timedelta(weeks=3))

    pruned = raw.prune(con, keep=2, raw_dir=raw_dir)

    assert [p.raw_id for p in pruned] == [a]
    for rid in (b, c, d):
        assert _row(con, rid)["pruned_at"] is None
        assert (raw_dir / _row(con, rid)["blob_path"]).is_file()


def test_prune_dry_run_deletes_nothing(con, raw_dir):
    _bulk_history(con, raw_dir)
    files_before = sorted(p for p in raw_dir.rglob("*") if p.is_file())

    result = raw.prune(con, keep=2, dry_run=True, raw_dir=raw_dir)

    assert len(result) == 2 * len(raw.BULK_DATASETS)
    assert sorted(p for p in raw_dir.rglob("*") if p.is_file()) == files_before
    assert (
        con.execute("SELECT count(*) FROM raw_response WHERE pruned_at IS NOT NULL").fetchone()[0]
        == 0
    )


def test_prune_rejects_negative_keep(con, raw_dir):
    with pytest.raises(ValueError):
        raw.prune(con, keep=-1, raw_dir=raw_dir)


def test_cli_raw_prune(tmp_path, monkeypatch):
    db = tmp_path / "cli.duckdb"
    raw_dir = tmp_path / "raw"
    monkeypatch.setenv("USFIN_DB", str(db))
    monkeypatch.setenv("USFIN_RAW", str(raw_dir))
    runner = CliRunner()

    empty = runner.invoke(app, ["raw", "prune", "--dry-run"])
    assert empty.exit_code == 0, empty.output
    assert "0 candidates" in empty.output

    with store.session(db, wait=0) as con:
        _bulk_history(con, raw_dir)

    dry = runner.invoke(app, ["raw", "prune", "--dry-run", "--keep", "3"])
    assert dry.exit_code == 0, dry.output
    assert "dry run: 2 candidates" in dry.output

    real = runner.invoke(app, ["raw", "prune"])
    assert real.exit_code == 0, real.output
    assert "pruned 4" in real.output
    with store.session(db, wait=0) as con:
        assert (
            con.execute("SELECT count(*) FROM raw_response WHERE pruned_at IS NOT NULL").fetchone()[
                0
            ]
            == 4
        )
