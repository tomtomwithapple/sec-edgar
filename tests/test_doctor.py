"""`usfin doctor` and the launchd example (plan: How › Ops; C18; S35).

Doctor reports every env var as set or unset — never its value —, the config files, the DB path
and whether another process holds its lock (it never waits for it), the raw store's size, free
disk against `USFIN_MIN_FREE_GB`, and the newest EDGAR bulk projection. It runs in a temp working
directory, so neither the user's `.env` nor `data/` is read. `shutil.disk_usage` is patched.
"""

import plistlib
import re
import shutil
import subprocess
import sys
import time
from collections import namedtuple
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from usfin import store
from usfin.cli import app
from usfin.sources.edgar import bulk

REPO = Path(__file__).resolve().parents[1]
PLIST = REPO / "deploy" / "com.tuntun.usfin.serve.plist.example"
GIB = 2**30
Usage = namedtuple("Usage", "total used free")
ENV_KEYS = (
    "SEC_USER_AGENT", "BLS_API_KEY", "BLS_CONTACT_EMAIL", "FRED_API_KEY", "ALERT_WEBHOOK_URL",
    "USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_MIN_FREE_GB",
    "USFIN_BULK_LIMIT", "USFIN_DEMO",
)  # fmt: skip
SECRET_UA = "Secret Person s3cret-ua@example.test"
SECRET_MAIL = "hidden-mail@example.test"
SECRET_HOOK = "https://hooks.example.test/T0KEN-abc123"


@pytest.fixture
def home(tmp_path, monkeypatch):
    """A working directory with the repo's config, an empty raw dir and a `.env` with secrets."""
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    shutil.copytree(REPO / "config", tmp_path / "config")
    (tmp_path / "raw").mkdir()
    (tmp_path / ".env").write_text(
        f"SEC_USER_AGENT={SECRET_UA}\nBLS_CONTACT_EMAIL={SECRET_MAIL}\n", "utf-8"
    )
    monkeypatch.chdir(tmp_path)
    return tmp_path


class Disk:
    def __init__(self, free):
        self.free = free

    def __call__(self, path):
        return Usage(228 * GIB, 228 * GIB - int(self.free), int(self.free))


@pytest.fixture
def disk(monkeypatch):
    d = Disk(30 * GIB)
    monkeypatch.setattr(bulk, "disk_usage", d)
    return d


def _doctor():
    return CliRunner().invoke(app, ["doctor"])


def _line(output: str, name: str) -> str:
    found = [ln for ln in output.splitlines() if re.search(rf"\b{re.escape(name)}\b", ln)]
    assert found, f"no line for {name!r} in:\n{output}"
    return found[0]


def _db(home: Path) -> Path:
    path = home / "data" / "usfin.duckdb"
    with store.session(path, wait=0):
        pass
    return path


# --- env -----------------------------------------------------------------------------------


def test_every_env_var_is_reported_set_or_unset_and_no_value_is_printed(home, disk, monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", SECRET_HOOK)
    monkeypatch.setenv("USFIN_MIN_FREE_GB", "12")
    _db(home)
    out = _doctor()
    assert out.exit_code == 0, out.output
    for key in ENV_KEYS[:-1]:  # USFIN_DEMO is a test switch, not configuration
        _line(out.output, key)
    assert re.search(r"\bset\b", _line(out.output, "SEC_USER_AGENT"))
    assert re.search(r"\bset\b", _line(out.output, "BLS_CONTACT_EMAIL"))
    assert re.search(r"\bset\b", _line(out.output, "ALERT_WEBHOOK_URL"))
    assert "unset" in _line(out.output, "FRED_API_KEY")
    assert "unset" in _line(out.output, "BLS_API_KEY")
    for secret in (SECRET_UA, "s3cret", SECRET_MAIL, "hidden-mail", SECRET_HOOK, "T0KEN"):
        assert secret not in out.output
    # what each optional key unlocks is said, so an unset one is not a mystery
    assert "ALFRED" in _line(out.output, "FRED_API_KEY")
    # paths and numbers are not secrets: the effective value is shown
    assert "12" in _line(out.output, "USFIN_MIN_FREE_GB")
    assert "unset" in _line(out.output, "USFIN_LOCK_WAIT") and "600" in _line(
        out.output, "USFIN_LOCK_WAIT"
    )


def test_a_required_env_var_unset_fails_the_doctor(home, disk):
    (home / ".env").write_text(f"BLS_CONTACT_EMAIL={SECRET_MAIL}\n", "utf-8")
    _db(home)
    out = _doctor()
    assert out.exit_code == 1
    line = _line(out.output, "SEC_USER_AGENT")
    assert "unset" in line and "required" in line and line.lstrip().startswith("FAIL")


# --- config --------------------------------------------------------------------------------


def test_config_files_are_listed_and_a_missing_one_fails(home, disk):
    _db(home)
    out = _doctor()
    for name in ("hosts.yaml", "calendars.yaml", "fiscal_data.yaml", "bls.yaml", "edgar.yaml",
                 "concepts.yaml", "schedule.yaml"):  # fmt: skip
        assert _line(out.output, name).lstrip().startswith("ok")
    (home / "config" / "edgar.yaml").unlink()
    out = _doctor()
    assert out.exit_code == 1
    assert "missing" in _line(out.output, "edgar.yaml")


def test_a_schedule_that_does_not_load_fails(home, disk):
    _db(home)
    (home / "config" / "schedule.yaml").write_text(
        'jobs:\n  fiscal: {cron: "0 3 * * 0", calendar: federal}\n', "utf-8"
    )
    out = _doctor()
    assert out.exit_code == 1
    assert "FAIL" in _line(out.output, "schedule.yaml")


# --- database ------------------------------------------------------------------------------


HOLDER = """
import sys, duckdb
con = duckdb.connect(sys.argv[1])
print("ready", flush=True)
sys.stdin.readline()
"""


def test_db_path_and_lock_state_free_then_held_by_a_second_process(home, disk, monkeypatch):
    monkeypatch.setenv("USFIN_LOCK_WAIT", "30")  # doctor must not wait for the lock
    db = _db(home)
    out = _doctor()
    assert out.exit_code == 0, out.output
    assert str(Path("data") / "usfin.duckdb") in out.output
    lock = _line(out.output, "lock")
    assert "free" in lock
    assert f"schema {len(store.available_migrations())}" in _line(out.output, "database")

    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(db)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )  # fmt: skip
    try:
        assert holder.stdout.readline().strip() == "ready"
        start = time.monotonic()
        out = _doctor()
        assert time.monotonic() - start < 5
    finally:
        holder.communicate("\n", timeout=10)
    assert out.exit_code == 0, out.output  # a held lock is normal while serve runs a job
    lock = _line(out.output, "lock")
    assert "held by another process" in lock and f"PID {holder.pid}" in lock


def test_a_db_not_created_yet_is_reported_and_not_created(home, disk):
    out = _doctor()
    assert "not created yet" in _line(out.output, "database")
    assert not (home / "data" / "usfin.duckdb").exists()


# --- raw store, disk, bulk projection ------------------------------------------------------


def test_raw_store_size_counts_every_blob(home, disk):
    _db(home)
    (home / "raw" / "fiscal" / "ab").mkdir(parents=True)
    (home / "raw" / "fiscal" / "ab" / "x.gz").write_bytes(b"x" * 1000)
    (home / "raw" / "edgar" / "cd").mkdir(parents=True)
    (home / "raw" / "edgar" / "cd" / "y.zip").write_bytes(b"y" * 2500)
    line = _line(_doctor().output, "raw store")
    assert "3500 bytes" in line and "2 files" in line


def test_free_disk_is_judged_against_the_floor(home, disk, monkeypatch):
    _db(home)
    disk.free = 12 * GIB
    out = _doctor()
    line = _line(out.output, "disk")
    assert out.exit_code == 0 and line.lstrip().startswith("ok")
    assert "12.00 GiB" in line and "floor 10.0 GiB" in line
    disk.free = 9 * GIB
    out = _doctor()
    assert out.exit_code == 1
    assert _line(out.output, "disk").lstrip().startswith("FAIL")


def test_the_newest_bulk_estimate_is_projected_against_free_disk(home, disk):
    db = _db(home)
    out = _doctor()
    assert "no estimate" in _line(out.output, "companyfacts")
    with store.session(db, wait=0) as con:
        bulk.record_estimate(
            con,
            bulk.Estimate(
                dataset="bulk_companyfacts", measured_at=datetime(2026, 10, 8, 9, 0, tzinfo=UTC).replace(tzinfo=None),
                raw_id=1, zip_bytes=GIB, members_total=1000, members_sampled=100,
                member_bytes_total=10 * GIB, member_bytes_sampled=GIB,
                db_bytes_before=0, db_bytes_after=GIB // 10, rows_inserted=1,
                seconds=60.0, download_seconds=None, scratch_rows=None,
                scratch_bytes_pk=None, scratch_bytes_no_pk=None,
            ),
        )  # fmt: skip
    line = _line(_doctor().output, "companyfacts")
    assert "2026-10-08" in line and "1.00 GiB" in line and "fits" in line  # 0.1 GiB × 10


# --- launchd -------------------------------------------------------------------------------


def test_the_launchd_example_runs_usfin_serve_from_the_repo_with_keepalive():
    with PLIST.open("rb") as fh:
        plist = plistlib.load(fh)
    assert plist["Label"] == "com.tuntun.usfin.serve"
    args = plist["ProgramArguments"]
    assert Path(args[0]).name == "uv" and Path(args[0]).is_absolute()
    assert args[1:] == ["run", "usfin", "serve"]
    assert Path(plist["WorkingDirectory"]).is_absolute()  # so config/ and .env resolve
    assert plist["KeepAlive"] and plist["RunAtLoad"]
    for key in ("StandardOutPath", "StandardErrorPath"):
        assert Path(plist[key]).is_absolute()
    # never a secret in the plist: they live in .env under WorkingDirectory
    env = plist.get("EnvironmentVariables", {})
    assert not set(env) & {"SEC_USER_AGENT", "BLS_CONTACT_EMAIL", "FRED_API_KEY", "BLS_API_KEY"}


@pytest.mark.skipif(shutil.which("plutil") is None, reason="plutil is macOS only")
def test_the_launchd_example_passes_plutil_lint():
    out = subprocess.run(
        ["plutil", "-lint", str(PLIST)], capture_output=True, text=True, check=False
    )
    assert out.returncode == 0, out.stdout + out.stderr


INSTALL = REPO / "scripts" / "install.sh"


def test_install_renders_the_example_for_this_checkout():
    # --print-plist only renders: no uv sync, no .env, no launchctl
    out = subprocess.run(
        ["bash", str(INSTALL), "--print-plist"], capture_output=True, check=True, cwd="/"
    )
    plist = plistlib.loads(out.stdout)
    uv = shutil.which("uv")
    assert plist["ProgramArguments"] == [uv, "run", "usfin", "serve"]
    assert plist["WorkingDirectory"] == str(REPO)
    for key in ("StandardOutPath", "StandardErrorPath"):
        assert plist[key] == str(REPO / "data" / "usfin-serve.log")
    assert plist["EnvironmentVariables"]["PATH"].split(":")[0] == str(Path(uv).parent)
    assert b"tomtomtomtom" not in out.stdout


def test_install_rejects_an_unknown_option():
    out = subprocess.run(
        ["bash", str(INSTALL), "--bogus"], capture_output=True, text=True, check=False
    )
    assert out.returncode != 0 and "unknown option" in out.stderr
