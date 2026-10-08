from pathlib import Path

import pytest

from usfin.settings import ConfigError, load_settings

ENV_KEYS = ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path):
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)


def test_defaults_without_env_or_dotenv(tmp_path):
    s = load_settings(env_file=tmp_path / "missing.env")
    assert s.db_path == Path("data/usfin.duckdb")
    assert s.raw_dir == Path("raw")
    assert s.config_dir == Path("config")
    assert s.lock_wait == 600.0


def test_environment_overrides(monkeypatch, tmp_path):
    monkeypatch.setenv("USFIN_DB", str(tmp_path / "x.duckdb"))
    monkeypatch.setenv("USFIN_RAW", str(tmp_path / "blobs"))
    monkeypatch.setenv("USFIN_CONFIG", str(tmp_path / "cfg"))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "5")
    s = load_settings(env_file=tmp_path / "missing.env")
    assert s.db_path == tmp_path / "x.duckdb"
    assert s.raw_dir == tmp_path / "blobs"
    assert s.config_dir == tmp_path / "cfg"
    assert s.lock_wait == 5.0


def test_dotenv_file_is_read(tmp_path):
    env = tmp_path / ".env"
    env.write_text("USFIN_DB=from_dotenv.duckdb\nUSFIN_LOCK_WAIT=30\n")
    s = load_settings(env_file=env)
    assert s.db_path == Path("from_dotenv.duckdb")
    assert s.lock_wait == 30.0
    assert s.raw_dir == Path("raw")


def test_default_dotenv_is_cwd(tmp_path):
    (tmp_path / ".env").write_text("USFIN_RAW=cwd_raw\n")
    assert load_settings().raw_dir == Path("cwd_raw")


def test_real_env_beats_dotenv(monkeypatch, tmp_path):
    env = tmp_path / ".env"
    env.write_text("USFIN_DB=from_dotenv.duckdb\n")
    monkeypatch.setenv("USFIN_DB", "from_env.duckdb")
    assert load_settings(env_file=env).db_path == Path("from_env.duckdb")


def test_loading_does_not_mutate_process_env(tmp_path):
    import os

    env = tmp_path / ".env"
    env.write_text("USFIN_DB=from_dotenv.duckdb\n")
    load_settings(env_file=env)
    assert "USFIN_DB" not in os.environ


def test_secrets_are_optional_and_read(monkeypatch, tmp_path):
    s = load_settings(env_file=tmp_path / "missing.env")
    assert s.sec_user_agent is None
    assert s.bls_api_key is None
    monkeypatch.setenv("SEC_USER_AGENT", "Tommy tommy@example.com")
    assert load_settings(env_file=tmp_path / "missing.env").sec_user_agent == (
        "Tommy tommy@example.com"
    )


def test_bad_lock_wait_raises_config_error(monkeypatch, tmp_path):
    monkeypatch.setenv("USFIN_LOCK_WAIT", "soon")
    with pytest.raises(ConfigError, match="USFIN_LOCK_WAIT"):
        load_settings(env_file=tmp_path / "missing.env")
