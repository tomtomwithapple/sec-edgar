"""Environment and path settings, read from the process env with `.env` as fallback."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values


class ConfigError(Exception):
    """Missing or invalid configuration."""


@dataclass(frozen=True)
class Settings:
    db_path: Path
    raw_dir: Path
    config_dir: Path
    lock_wait: float
    sec_user_agent: str | None
    bls_api_key: str | None
    bls_contact_email: str | None
    fred_api_key: str | None
    alert_webhook_url: str | None
    min_free_gb: float = 10.0
    """`USFIN_MIN_FREE_GB`: free-disk floor for EDGAR bulk downloads (GiB)."""
    bulk_limit: int | None = None
    """`USFIN_BULK_LIMIT`: job `edgar_bulk` parses at most this many changed members per ZIP
    (spread evenly, as `usfin edgar backfill --limit`); unset = every changed member."""
    apca_api_key_id: str | None = None
    """`APCA_API_KEY_ID`: Alpaca key id, sent as a header to the Alpaca hosts only."""
    apca_api_secret_key: str | None = None
    """`APCA_API_SECRET_KEY`: Alpaca secret, sent as a header to the Alpaca hosts only."""


def _merged(env_file: Path | None) -> Mapping[str, str | None]:
    path = Path(".env") if env_file is None else env_file
    file_values = dotenv_values(path) if path.is_file() else {}
    return {**file_values, **os.environ}


def environment(env_file: Path | None = None) -> dict[str, str | None]:
    """The merged environment settings are read from (real env over `env_file`, default
    `./.env`); `usfin doctor` reads which keys are set from it."""
    return dict(_merged(env_file))


def load_settings(env_file: Path | None = None) -> Settings:
    """Resolve settings; real environment variables win over `env_file` (default `./.env`)."""
    env = _merged(env_file)

    def get(key: str) -> str | None:
        value = env.get(key)
        return value if value else None

    raw_wait = get("USFIN_LOCK_WAIT") or "600"
    try:
        lock_wait = float(raw_wait)
    except ValueError:
        raise ConfigError(f"USFIN_LOCK_WAIT must be a number of seconds, got {raw_wait!r}") from None

    raw_floor = get("USFIN_MIN_FREE_GB") or "10"
    try:
        min_free_gb = float(raw_floor)
    except ValueError:
        raise ConfigError(f"USFIN_MIN_FREE_GB must be a number of GiB, got {raw_floor!r}") from None

    raw_limit = get("USFIN_BULK_LIMIT")
    try:
        bulk_limit = None if raw_limit is None else int(raw_limit)
    except ValueError:
        raise ConfigError(f"USFIN_BULK_LIMIT must be a whole number, got {raw_limit!r}") from None
    if bulk_limit is not None and bulk_limit < 1:
        raise ConfigError(f"USFIN_BULK_LIMIT must be at least 1, got {raw_limit!r}")

    return Settings(
        db_path=Path(get("USFIN_DB") or "data/usfin.duckdb"),
        raw_dir=Path(get("USFIN_RAW") or "raw"),
        config_dir=Path(get("USFIN_CONFIG") or "config"),
        lock_wait=lock_wait,
        sec_user_agent=get("SEC_USER_AGENT"),
        bls_api_key=get("BLS_API_KEY"),
        bls_contact_email=get("BLS_CONTACT_EMAIL"),
        fred_api_key=get("FRED_API_KEY"),
        alert_webhook_url=get("ALERT_WEBHOOK_URL"),
        min_free_gb=min_free_gb,
        bulk_limit=bulk_limit,
        apca_api_key_id=get("APCA_API_KEY_ID"),
        apca_api_secret_key=get("APCA_API_SECRET_KEY"),
    )
