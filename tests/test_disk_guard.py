"""Free-disk guard and sample estimate for EDGAR bulk loads (plan: How › SEC EDGAR › Free-disk
guard, Measure before the full load; C17). `shutil.disk_usage` is patched; the ZIPs are the
fixture ZIPs of `test_edgar_bulk` (real member JSON). No request leaves the process.
"""

import io
import zipfile

import pytest
import test_edgar_bulk as fixtures

from usfin import store
from usfin.settings import ConfigError, load_settings
from usfin.sources.edgar import bulk

# the bulk tests' fixtures and helpers
settings, cli_env, disk, sec = fixtures.settings, fixtures.cli_env, fixtures.disk, fixtures.sec
GIB, _backfill, _query = fixtures.GIB, fixtures._backfill, fixtures._query


def test_min_free_gb_defaults_to_10_and_reads_the_env(tmp_path, monkeypatch):
    monkeypatch.delenv("USFIN_MIN_FREE_GB", raising=False)
    assert load_settings(env_file=tmp_path / "none").min_free_gb == 10.0
    monkeypatch.setenv("USFIN_MIN_FREE_GB", "12.5")
    assert load_settings(env_file=tmp_path / "none").min_free_gb == 12.5
    monkeypatch.setenv("USFIN_MIN_FREE_GB", "lots")
    with pytest.raises(ConfigError, match="USFIN_MIN_FREE_GB"):
        load_settings(env_file=tmp_path / "none")


def test_too_little_free_disk_sends_no_get_and_exits_1_with_the_numbers(
    settings, sec, disk, cli_env
):
    size = len(sec.bodies["companyfacts.zip"])
    disk.free = 10 * GIB + size - 1  # one byte short of the floor once the ZIP lands
    out = _backfill("companyfacts")
    assert out.exit_code == 1
    assert sec.requests == ["HEAD companyfacts.zip"]  # no GET
    assert "not downloading" in out.output
    assert f"zip {size} bytes" in out.output and "floor 10.0 GiB" in out.output
    assert _query(settings, "SELECT count(*) FROM raw_response WHERE dataset LIKE 'bulk_%'") == [
        (0,)
    ]
    # both volumes were checked (raw dir and DB dir)
    assert str(settings.raw_dir) in disk.paths
    assert str(settings.db_path.parent) in disk.paths


def test_exactly_the_floor_left_is_allowed(settings, sec, disk, cli_env):
    disk.free = 10 * GIB + len(sec.bodies["companyfacts.zip"])
    out = _backfill("companyfacts")
    assert out.exit_code == 0, out.output


def test_a_head_without_content_length_downloads_nothing(settings, sec, disk, cli_env):
    sec.content_length = False
    out = _backfill("submissions")
    assert out.exit_code == 1
    assert sec.requests == ["HEAD submissions.zip"]
    assert "Content-Length" in out.output


def test_check_only_heads_and_prints_the_verdict(settings, sec, disk, cli_env):
    out = _backfill("companyfacts", "--check")
    assert out.exit_code == 0, out.output
    assert sec.requests == ["HEAD companyfacts.zip"]
    assert "fits" in out.output


def test_floor_crossed_mid_load_stops_after_the_current_member(
    settings, sec, disk, cli_env, monkeypatch
):
    monkeypatch.setattr(bulk, "GUARD_EVERY", 1)
    # HEAD guard (raw, db), then the re-check after member 1 (raw, db) finds the floor crossed
    disk.script = [18 * GIB, 18 * GIB, 18 * GIB, 9 * GIB]
    out = _backfill("companyfacts")
    assert out.exit_code == 1
    assert "stopped" in out.output and "rerun to resume" in out.output
    # member 1 (JPMorgan, CIK 19617 sorts first) is committed with its CRC; member 2 never parsed
    assert _query(settings, "SELECT name FROM edgar_bulk_member") == [("CIK0000019617.json",)]
    assert _query(settings, "SELECT DISTINCT cik FROM xbrl_fact") == [(19617,)]
    disk.script = []
    out = _backfill("companyfacts", "--reuse")
    assert out.exit_code == 0, out.output
    assert "parsed 1," in out.output


def test_the_sample_run_records_an_estimate_and_estimate_prints_the_projection(
    settings, sec, disk, cli_env
):
    out = _backfill("companyfacts", "--limit", "1")
    assert out.exit_code == 0, out.output
    (row,) = _query(
        settings,
        "SELECT dataset, zip_bytes, members_total, members_sampled, member_bytes_total,"
        " member_bytes_sampled, db_bytes_before, db_bytes_after, rows_inserted, seconds,"
        " download_seconds, scratch_rows, scratch_bytes_pk, scratch_bytes_no_pk"
        " FROM edgar_bulk_estimate",
    )
    (ds, zbytes, total, sampled, mtotal, msampled, before, after, rows, secs, dl,
     srows, spk, snopk) = row  # fmt: skip
    assert ds == "bulk_companyfacts" and zbytes == len(sec.bodies["companyfacts.zip"])
    assert (total, sampled, rows) == (2, 1, 4)  # JPMorgan first: 4 us-gaap/dei facts
    assert 0 < msampled < mtotal
    assert before > 0 and after >= before and secs >= 0 and dl is not None
    # the sampled facts measured in scratch DuckDB files, with and without the xbrl_fact PK
    assert srows == 4 and spk > 0 and snopk > 0
    assert not list(settings.db_path.parent.glob("*scratch*"))  # scratch files deleted

    out = _backfill("companyfacts", "--estimate")
    assert out.exit_code == 0, out.output
    growth = (after - before) / sampled * total
    assert f"projected db growth {growth / GIB:.2f} GiB" in out.output
    assert f"no-PK {snopk / sampled * total / GIB:.2f} GiB" in out.output
    assert "projected time" in out.output
    assert sec.requests.count("GET companyfacts.zip") == 1  # --estimate sends nothing


def test_estimate_without_a_sample_says_so(settings, sec, disk, cli_env):
    out = _backfill("submissions", "--estimate")
    assert out.exit_code == 1
    assert "no estimate" in out.output
    assert sec.requests == []


def test_a_known_projection_counts_against_the_floor(settings, sec, disk, cli_env):
    size = len(sec.bodies["companyfacts.zip"])
    with store.session(settings.db_path, wait=0) as con:  # a sample that grew the DB by 1 GiB
        con.execute(
            "INSERT INTO edgar_bulk_estimate (dataset, measured_at, raw_id, zip_bytes,"
            " members_total, members_sampled, member_bytes_total, member_bytes_sampled,"
            " db_bytes_before, db_bytes_after, rows_inserted, seconds)"
            " VALUES ('bulk_companyfacts', '2026-10-08 12:00', 1, ?, 2, 1, 200, 100, ?, ?, 4, 1)",
            [size, GIB, 2 * GIB],
        )
    disk.free = 10 * GIB + size + GIB  # room for the ZIP, not for 2 members x 1 GiB
    out = _backfill("companyfacts")
    assert out.exit_code == 1
    assert "projected 2.00 GiB" in out.output and "not downloading" in out.output
    assert sec.requests == ["HEAD companyfacts.zip"]
    disk.free = 10 * GIB + size + 2 * GIB
    assert _backfill("companyfacts").exit_code == 0


def test_a_submissions_sample_is_measured_in_a_scratch_file_too(settings, sec, disk, cli_env):
    # the DB file's own growth over a few hundred small transactions is mostly allocation churn
    # (S26 live: +23.6 MB for 4 190 filings), so the sampled rows are copied into a fresh file
    fixtures._company(settings, 320193, "AAPL")
    out = _backfill("submissions", "--limit", "2")
    assert out.exit_code == 0, out.output
    ((srows, spk, snopk),) = _query(
        settings,
        "SELECT scratch_rows, scratch_bytes_pk, scratch_bytes_no_pk FROM edgar_bulk_estimate",
    )
    assert srows == 11 and spk > 0 and snopk is None
    out = _backfill("submissions", "--estimate")
    assert "projected scratch (with PKs)" in out.output


def test_a_full_load_records_its_cost_but_copies_nothing_to_scratch(settings, sec, disk, cli_env):
    # S27: a full load's rows are the whole table; copying them twice into scratch files would
    # need as much disk again, so only a --limit sample is measured in scratch files
    out = _backfill("companyfacts")
    assert out.exit_code == 0, out.output
    assert _query(
        settings,
        "SELECT members_total, members_sampled, scratch_rows, scratch_bytes_pk"
        " FROM edgar_bulk_estimate",
    ) == [(2, 2, None, None)]


def test_a_submissions_estimate_counts_only_members_of_edgar_company(
    settings, sec, disk, cli_env
):
    fixtures._company(settings, 320193, "AAPL")
    # Apple's 2 members plus one of a CIK not in edgar_company (a real 2-byte member, never read)
    buf = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(sec.bodies["submissions.zip"])) as src, zipfile.ZipFile(
        buf, "w"
    ) as dst:
        for info in src.infolist():
            dst.writestr(info.filename, src.read(info))
        other = fixtures.FIXTURES / "bulk" / "CIK0000022370.json"
        dst.writestr(other.name, other.read_bytes())
    sec.bodies["submissions.zip"] = buf.getvalue()
    out = _backfill("submissions", "--limit", "1")
    assert out.exit_code == 0, out.output
    # members_total is what a full load would parse: Apple's 2, not the ZIP's 3
    assert _query(settings, "SELECT members_total, members_sampled FROM edgar_bulk_estimate") == [
        (2, 1)
    ]
