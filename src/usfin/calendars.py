"""Business-day calendars for scheduled jobs (C6).

- `federal`: weekday, not a US federal holiday (`holidays.US(observed=True)`) — Fiscal Data, BLS,
  EDGAR feed/reconcile.
- `bond`: a federal day that is not Good Friday — TreasuryDirect.
- `nyse`: weekday, not an NYSE holiday (`holidays.NYSE()`) — Nasdaq Trader.
- `any`: every day.

`config/calendars.yaml` adds one-off overrides per calendar (`<calendar>: {closed: [...], open:
[...]}`), e.g. SIFMA early-close-turned-closure or an NYSE day of mourning. A `federal` override
also moves `bond` (bond is federal minus Good Friday); a `bond` override wins over both.
"""

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import holidays
import yaml

from usfin.settings import ConfigError

CALENDARS = ("federal", "bond", "nyse", "any")
_OVERRIDABLE = ("federal", "bond", "nyse")
_SEARCH_LIMIT = 30  # no calendar has 30 closed days in a row


def easter(year: int) -> date:
    """Gregorian Easter Sunday (anonymous Gregorian algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    m = (32 + 2 * e + 2 * i - h - k) % 7
    n = (a + 11 * h + 22 * m) // 451
    month, day = divmod(h + m - 7 * n + 114, 31)
    return date(year, month, day + 1)


def is_good_friday(d: date) -> bool:
    return d == easter(d.year) - timedelta(days=2)


@dataclass(frozen=True)
class Override:
    closed: frozenset[date] = frozenset()
    open: frozenset[date] = frozenset()

    def apply(self, d: date, verdict: bool) -> bool:
        if d in self.open:
            return True
        if d in self.closed:
            return False
        return verdict


@dataclass
class Calendars:
    overrides: dict[str, Override] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._us = holidays.US(observed=True)
        self._nyse = holidays.NYSE()

    def _override(self, name: str) -> Override:
        return self.overrides.get(name) or Override()

    def is_federal_day(self, d: date) -> bool:
        verdict = d.weekday() < 5 and d not in self._us
        return self._override("federal").apply(d, verdict)

    def is_bond_day(self, d: date) -> bool:
        verdict = self.is_federal_day(d) and not is_good_friday(d)
        return self._override("bond").apply(d, verdict)

    def is_nyse_day(self, d: date) -> bool:
        verdict = d.weekday() < 5 and d not in self._nyse
        return self._override("nyse").apply(d, verdict)

    def is_day(self, calendar: str, d: date) -> bool:
        if calendar == "federal":
            return self.is_federal_day(d)
        if calendar == "bond":
            return self.is_bond_day(d)
        if calendar == "nyse":
            return self.is_nyse_day(d)
        if calendar == "any":
            return True
        raise ValueError(f"unknown calendar {calendar!r}; expected one of {', '.join(CALENDARS)}")

    def holiday_names(self, d: date) -> list[str]:
        """Holiday names on `d` from both sources (for display), deduplicated, in order."""
        names = [self._us.get(d), self._nyse.get(d), "Good Friday" if is_good_friday(d) else None]
        out: list[str] = []
        for name in names:
            for part in (name or "").split("; "):
                if part and part not in out:
                    out.append(part)
        return out

    def _step(self, calendar: str, d: date, step: int) -> date:
        cur = d
        for _ in range(_SEARCH_LIMIT):
            cur += timedelta(days=step)
            if self.is_day(calendar, cur):
                return cur
        raise ValueError(f"no {calendar} day within {_SEARCH_LIMIT} days of {d}")

    def next_day(self, calendar: str, d: date) -> date:
        """First `calendar` day strictly after `d`."""
        return self._step(calendar, d, 1)

    def prev_day(self, calendar: str, d: date) -> date:
        """Last `calendar` day strictly before `d`."""
        return self._step(calendar, d, -1)

    def days_back(self, calendar: str, d: date, n: int) -> date:
        """The `n`-th `calendar` day before `d` (`n=0` is `d` itself)."""
        if n < 0:
            raise ValueError(f"n must be >= 0, got {n}")
        cur = d
        for _ in range(n):
            cur = self.prev_day(calendar, cur)
        return cur


def _as_date(value: object, where: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        raise ConfigError(f"{where}: {value!r} is not a YYYY-MM-DD date") from None


def load_calendars(config_dir: Path | str) -> Calendars:
    """Read `<config_dir>/calendars.yaml` overrides; a missing or empty file means none."""
    path = Path(config_dir) / "calendars.yaml"
    data = yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None
    if data is None:
        return Calendars()
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a mapping of calendar -> {{closed, open}}")
    overrides: dict[str, Override] = {}
    for name, spec in data.items():
        if name not in _OVERRIDABLE:
            raise ConfigError(f"{path}: unknown calendar {name!r}")
        spec = spec or {}
        if not isinstance(spec, dict) or set(spec) - {"closed", "open"}:
            raise ConfigError(f"{path}: {name} takes only 'closed' and 'open' lists")
        where = f"{path}: {name}"
        closed = frozenset(_as_date(v, where) for v in spec.get("closed") or [])
        opened = frozenset(_as_date(v, where) for v in spec.get("open") or [])
        if both := closed & opened:
            raise ConfigError(f"{where}: {sorted(both)} both closed and open")
        overrides[name] = Override(closed=closed, open=opened)
    return Calendars(overrides)
