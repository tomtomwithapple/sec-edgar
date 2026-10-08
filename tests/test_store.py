import subprocess
import sys
import time

import duckdb
import pytest
from typer.testing import CliRunner

from usfin import store
from usfin.cli import app
from usfin.store import DatabaseLocked

CORE_TABLES = {"raw_response", "job_run", "watermark", "alert", "schema_version"}


@pytest.fixture
def db(tmp_path):
    return tmp_path / "sub" / "usfin.duckdb"


def _tables(con) -> set[str]:
    rows = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
    return {r[0] for r in rows}


def test_migrations_are_discovered_in_order():
    found = store.available_migrations()
    versions = [m.version for m in found]
    assert versions == sorted(versions)
    assert found[0].version == 1 and found[0].name == "core"


def test_first_session_applies_migrations_and_records_version(db):
    with store.session(db, wait=0, apply_migrations=False) as con:
        assert store.schema_version(con) == 0
        applied = store.migrate(con)
        assert applied == [m.version for m in store.available_migrations()]
        rows = con.execute("SELECT version, name, applied_at FROM schema_version").fetchall()
        assert [r[0] for r in rows] == applied
        assert rows[0][1] == "core" and rows[0][2] is not None
        assert CORE_TABLES <= _tables(con)
        assert store.schema_version(con) == applied[-1]
    assert db.exists()  # parent directory was created


def test_reopen_applies_nothing(db):
    with store.session(db, wait=0) as con:
        store.migrate(con)
    with store.session(db, wait=0) as con:
        assert store.migrate(con) == []
        assert con.execute("SELECT count(*) FROM schema_version").fetchone()[0] == len(
            store.available_migrations()
        )


def test_session_migrates_by_default(db):
    with store.session(db, wait=0) as con:
        assert CORE_TABLES <= _tables(con)


def test_migration_failure_rolls_back(db, monkeypatch):
    bad = store.Migration(version=999, name="broken", sql="CREATE TABLE ok_part(x INT); NOPE;")
    monkeypatch.setattr(store, "available_migrations", lambda: [*store._discover(), bad])
    with pytest.raises(duckdb.Error), store.session(db, wait=0) as con:
        pass
    monkeypatch.undo()
    with store.session(db, wait=0, apply_migrations=False) as con:
        assert "ok_part" not in _tables(con)
        assert 999 not in [
            r[0] for r in con.execute("SELECT version FROM schema_version").fetchall()
        ]


def test_core_table_keys(db):
    with store.session(db, wait=0) as con:
        rid = con.execute(
            "INSERT INTO raw_response (source, dataset, url, fetched_at, http_status, sha256,"
            " blob_path, bytes) VALUES ('fiscal', 'fd_x', 'https://x', now(), 200, 'ab',"
            " 'fiscal/ab/ab.gz', 3) RETURNING id"
        ).fetchone()[0]
        rid2 = con.execute(
            "INSERT INTO raw_response (source, dataset, url, fetched_at, http_status, sha256,"
            " blob_path, bytes) VALUES ('fiscal', 'fd_x', 'https://x', now(), 200, 'ab',"
            " 'fiscal/ab/ab.gz', 3) RETURNING id"
        ).fetchone()[0]
        assert rid2 > rid  # ids come from a sequence; identical rows are allowed

        run_id = con.execute(
            "INSERT INTO job_run (job, started_at, status, trigger, watermark_before)"
            " VALUES ('fiscal', now(), 'ok', 'manual', '{\"a\": \"2026-01-01\"}') RETURNING id"
        ).fetchone()[0]
        assert run_id >= 1
        with pytest.raises(duckdb.ConstraintException):
            con.execute(
                "INSERT INTO job_run (job, started_at, status, trigger)"
                " VALUES ('fiscal', now(), 'bogus', 'manual')"
            )
        with pytest.raises(duckdb.ConstraintException):
            con.execute(
                "INSERT INTO job_run (job, started_at, status, trigger)"
                " VALUES ('fiscal', now(), 'ok', 'whenever')"
            )

        con.execute("INSERT INTO watermark VALUES ('fiscal', 'debt_to_penny', '2026-10-01', now())")
        with pytest.raises(duckdb.ConstraintException):
            con.execute(
                "INSERT INTO watermark VALUES ('fiscal', 'debt_to_penny', '2026-10-02', now())"
            )

        insert_alert = (
            "INSERT INTO alert (job, rule, subject, detail, raised_on, raised_at)"
            " VALUES ('fiscal', 'zero_new_rows', 'dts', '{}', DATE '2026-10-08', now())"
        )
        con.execute(insert_alert)
        with pytest.raises(duckdb.ConstraintException):
            con.execute(insert_alert)
        assert con.execute("SELECT acknowledged_at FROM alert").fetchone()[0] is None


def test_in_process_double_open(db):
    with store.session(db, wait=0) as a:
        a.execute("INSERT INTO watermark VALUES ('j', 'k', 'v', now())")
        with store.session(db, wait=0) as b:
            assert b.execute("SELECT value FROM watermark").fetchone()[0] == "v"


HOLDER = """
import sys, time, duckdb
con = duckdb.connect(sys.argv[1])
print("ready", flush=True)
sys.stdin.readline()
"""


def test_other_process_holding_file_raises_database_locked_after_wait(db):
    with store.session(db, wait=0):
        pass
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(db)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        start = time.monotonic()
        with pytest.raises(DatabaseLocked) as exc, store.session(db, wait=1):
            pass
        elapsed = time.monotonic() - start
        assert 0.9 <= elapsed < 3.0
        assert str(db) in str(exc.value)
    finally:
        holder.communicate("\n", timeout=10)
    # once released, the file opens fine and nothing was corrupted
    with store.session(db, wait=0) as con:
        assert store.migrate(con) == []


def test_session_waits_for_lock_release(db):
    with store.session(db, wait=0):
        pass
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(db)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert holder.stdout.readline().strip() == "ready"

    def release_soon():
        time.sleep(0.5)
        holder.communicate("\n", timeout=10)

    import threading

    t = threading.Thread(target=release_soon)
    t.start()
    try:
        with store.session(db, wait=10) as con:
            assert con.execute("SELECT 1").fetchone()[0] == 1
    finally:
        t.join()


def test_session_default_wait_comes_from_settings(db, monkeypatch, tmp_path):
    monkeypatch.setenv("USFIN_DB", str(db))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    monkeypatch.chdir(tmp_path)
    with store.session() as con:
        assert CORE_TABLES <= _tables(con)
    assert db.exists()


def test_cli_db_migrate_and_info(db, monkeypatch, tmp_path):
    monkeypatch.setenv("USFIN_DB", str(db))
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    first = runner.invoke(app, ["db", "migrate"])
    assert first.exit_code == 0, first.output
    assert "001_core" in first.output
    again = runner.invoke(app, ["db", "migrate"])
    assert again.exit_code == 0
    assert "up to date" in again.output
    info = runner.invoke(app, ["db", "info"])
    assert info.exit_code == 0, info.output
    assert str(db) in info.output
    assert "version: 1" in info.output
    for table in CORE_TABLES:
        assert table in info.output


def test_cli_reports_database_locked_cleanly(db, monkeypatch, tmp_path):
    monkeypatch.setenv("USFIN_DB", str(db))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0.2")
    monkeypatch.chdir(tmp_path)
    with store.session(db, wait=0):
        pass
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(db)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        result = CliRunner().invoke(app, ["db", "info"])
    finally:
        holder.communicate("\n", timeout=10)
    assert result.exit_code == 1
    assert "database busy" in result.output
    assert not isinstance(result.exception, DatabaseLocked)
