"""Alpaca US market data (plan alpaca-market-data): settings in `config/alpaca.yaml`.

`load_config(config_dir)` reads the file every Alpaca job and backfill shares; a missing key
takes the default below, a bad value is a `ConfigError` (`usfin doctor` checks it). The API key
and secret are not here: `http.Client` sends them from Settings (`APCA_API_KEY_ID`,
`APCA_API_SECRET_KEY`) to the Alpaca hosts only.
"""

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

from usfin.settings import ConfigError


@dataclass(frozen=True)
class AlpacaConfig:
    exchanges: tuple[str, ...] = ("NYSE", "NASDAQ", "AMEX", "ARCA", "BATS")
    """Listed exchanges whose asset-list symbols are in the universe (A3); the rest is OTC."""
    batch_size: int = 100
    """Symbols per bars request."""
    lookback_days: int = 5
    """The daily bars job refetches active symbols from watermark − this many days."""
    backfill_start: date = date(2016, 1, 4)
    """First bar date requested (the free SIP window starts here)."""
    new_symbol_cap: int = 200
    """Universe symbols whose history was never asked that one daily bars run asks in full."""
    gap_sessions: int = 60
    """NYSE sessions without a bar that split a symbol into two securities (A7)."""


_INTS = ("batch_size", "lookback_days", "new_symbol_cap", "gap_sessions")


def load_config(config_dir: Path | str) -> AlpacaConfig:
    """Read `<config_dir>/alpaca.yaml`; a missing file means every default."""
    path = Path(config_dir) / "alpaca.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a mapping, got {type(data).__name__}")
    unknown = set(data) - {"exchanges", "backfill_start", *_INTS}
    if unknown:
        raise ConfigError(f"{path}: unknown keys {', '.join(sorted(unknown))}")
    default = AlpacaConfig()
    values: dict = {}
    for name in _INTS:
        value = data.get(name, getattr(default, name))
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ConfigError(f"{path}: {name} must be a whole number ≥ 1, got {value!r}")
        values[name] = value
    exchanges = data.get("exchanges", list(default.exchanges))
    if not isinstance(exchanges, list) or not exchanges or not all(
        isinstance(e, str) and e for e in exchanges
    ):
        raise ConfigError(f"{path}: exchanges must be a non-empty list of names, got {exchanges!r}")
    start = data.get("backfill_start", default.backfill_start)
    if isinstance(start, str):
        try:
            start = date.fromisoformat(start)
        except ValueError:
            raise ConfigError(f"{path}: backfill_start must be YYYY-MM-DD, got {start!r}") from None
    if not isinstance(start, date):
        raise ConfigError(f"{path}: backfill_start must be YYYY-MM-DD, got {start!r}")
    return AlpacaConfig(exchanges=tuple(exchanges), backfill_start=start, **values)
