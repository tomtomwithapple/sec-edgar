"""Job `bls_calendar`: the BLS release schedule (iCalendar) into table `bls_release`.

`ICS_URL` is the "Economic News Release Schedule" calendar linked from
https://www.bls.gov/schedule/news_release/ (fetched 2026-10-08: 313 events, 2025-01-03 to
2026-12-30, `text/calendar`, LF line ends, no folded lines). www.bls.gov answers 403 unless the
User-Agent carries a contact email (see `usfin.http`, `BLS_CONTACT_EMAIL`). Every response is
archived as raw dataset `bls_ics` (source `bls`) before it is read.

Only `VEVENT` blocks are read (the calendar's own SUMMARY and the VTIMEZONE's DTSTARTs are not
releases): `DTSTART;TZID=US-Eastern:YYYYMMDDTHHMMSS` (US-Eastern = America/New_York; a `Z` time is
UTC, a DATE value midnight New York) and `SUMMARY` (unfolded, unescaped, stripped: some carry
trailing blanks). The release name is the SUMMARY; `config/bls.yaml` maps each series to one
(`release`). Each file is authoritative for its own date range: rows in that range that the file
no longer lists (a moved release) are deleted, new ones inserted, a changed time updated, so a
refetch of the same file changes nothing (C2) and `usfin reparse bls_calendar` rebuilds the table.

After each run — even a failed fetch — `calendar_stale` is raised (subject `bls_release`) when no
stored release is more than `HORIZON_DAYS` days ahead: BLS publishes the next year's schedule
late in the year, and release days are the `bls` job's due days.
"""

import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import duckdb

from usfin import alerts, jobs, raw
from usfin.sources import bls

ICS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
SOURCE = "bls"
RAW_DATASET = "bls_ics"
TABLE = "bls_release"
HORIZON_DAYS = 30
ET = ZoneInfo("America/New_York")
ZONES = {"US-Eastern": "America/New_York"}
"""Non-IANA TZIDs the BLS file uses."""
_STAMP = re.compile(r"(\d{4})(\d{2})(\d{2})(?:T(\d{2})(\d{2})(\d{2})(Z?))?")
log = logging.getLogger("usfin.bls_calendar")


class CalendarError(Exception):
    """www.bls.gov answered something other than 200 (`raw_id` is the archived reply)."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Release:
    release_date: date
    """New York date of the release."""
    release_name: str
    released_at: datetime
    """Release time, naive UTC."""


# --- parsing ---------------------------------------------------------------------------------


def _unfold(text: str) -> list[str]:
    out: list[str] = []
    for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if line[:1] in (" ", "\t") and out:
            out[-1] += line[1:]
        else:
            out.append(line)
    return out


def _unescape(value: str) -> str:
    return re.sub(r"\\([\\;,nN])", lambda m: "\n" if m[1] in "nN" else m[1], value)


def _utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None)


def _start(params: Mapping[str, str], value: str) -> tuple[date, datetime]:
    """(New York date, naive UTC instant) of a DTSTART."""
    m = _STAMP.fullmatch(value)
    if m is None:
        raise ValueError(f"DTSTART {value!r} is not a DATE or DATE-TIME")
    year, month, day, hh, mm, ss, z = m.groups()
    if hh is None or params.get("VALUE") == "DATE":
        d = date(int(year), int(month), int(day))
        return d, _utc_naive(datetime(d.year, d.month, d.day, tzinfo=ET))
    if z:
        zone = UTC
    else:
        tzid = params.get("TZID")
        try:
            zone = ZoneInfo(ZONES.get(tzid, tzid)) if tzid else ET  # floating: New York
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError(f"DTSTART has an unknown TZID {tzid!r}") from None
    aware = datetime(int(year), int(month), int(day), int(hh), int(mm), int(ss), tzinfo=zone)
    return aware.astimezone(ET).date(), _utc_naive(aware)


def parse_ics(text: str) -> list[Release]:
    """Every VEVENT as a `Release`, in file order."""
    out: list[Release] = []
    event: dict[str, tuple[dict[str, str], str]] | None = None
    for line in _unfold(text):
        if line == "BEGIN:VEVENT":
            event = {}
        elif line == "END:VEVENT" and event is not None:
            if "DTSTART" not in event:
                raise ValueError(f"VEVENT without DTSTART: {event}")
            if "SUMMARY" not in event:
                raise ValueError(f"VEVENT without SUMMARY: {event}")
            day, at = _start(*event["DTSTART"])
            out.append(Release(day, _unescape(event["SUMMARY"][1]).strip(), at))
            event = None
        elif event is not None and ":" in line:
            name, _, value = line.partition(":")
            prop, *raw_params = name.split(";")
            params = dict(p.partition("=")[::2] for p in raw_params)
            if prop.upper() in ("DTSTART", "SUMMARY"):
                event.setdefault(prop.upper(), ({k.upper(): v for k, v in params.items()}, value))
    return out


def series_for(release_name: str, series: Mapping[str, bls.Series]) -> list[str]:
    """Configured series IDs the release publishes, in config order."""
    return [sid for sid, s in series.items() if s.release == release_name]


# --- storing ---------------------------------------------------------------------------------


def store_releases(con: duckdb.DuckDBPyConnection, raw_id: int, *, raw_dir) -> int:
    """Make `bls_release` equal the response's events within its date range; return rows
    inserted, updated or deleted. A non-200 response stores nothing."""
    status = con.execute("SELECT http_status FROM raw_response WHERE id = ?", [raw_id]).fetchone()
    if status is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    if status[0] != 200:
        return 0
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        releases = parse_ics(fh.read().decode("utf-8", errors="replace"))
    if not releases:
        return 0
    found: dict[tuple[date, str], datetime] = {}
    for r in releases:
        key = (r.release_date, r.release_name)
        if key in found:
            log.warning("bls_calendar: %s listed twice on %s; keeping the first", *key[::-1])
            continue
        found[key] = r.released_at
    lo, hi = min(d for d, _ in found), max(d for d, _ in found)
    stored = {
        (d, name): at
        for d, name, at in con.execute(
            f"SELECT release_date, release_name, released_at FROM {TABLE}"
            " WHERE release_date BETWEEN ? AND ?",
            [lo, hi],
        ).fetchall()
    }
    gone = [key for key in stored if key not in found]
    changed = [(key, at) for key, at in found.items() if stored.get(key) != at]
    for d, name in gone:
        con.execute(f"DELETE FROM {TABLE} WHERE release_date = ? AND release_name = ?", [d, name])
    if changed:
        con.executemany(
            f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?) ON CONFLICT (release_date, release_name)"
            " DO UPDATE SET released_at = excluded.released_at, raw_id = excluded.raw_id",
            [[d, name, at, raw_id] for (d, name), at in changed],
        )
    return len(gone) + len(changed)


def check_stale(con: duckdb.DuckDBPyConnection, *, today: date, now: datetime) -> int | None:
    """Raise `calendar_stale` when no stored release is more than `HORIZON_DAYS` days ahead."""
    latest = con.execute(f"SELECT max(release_date) FROM {TABLE}").fetchone()[0]
    if latest is not None and latest > today + timedelta(days=HORIZON_DAYS):
        return None
    return alerts.raise_alert(
        con,
        job="bls_calendar",
        rule="calendar_stale",
        subject=TABLE,
        detail={
            "latest_release": None if latest is None else latest.isoformat(),
            "today": today.isoformat(),
            "horizon_days": HORIZON_DAYS,
        },
        now=now,
    )


def next_releases(
    con: duckdb.DuckDBPyConnection,
    series: Mapping[str, bls.Series],
    *,
    start: date,
    n: int,
    every: bool = False,
) -> list[tuple[Release, list[str]]]:
    """The next `n` releases on or after `start` (only those of configured series unless
    `every`), each with the series it publishes."""
    names = sorted({s.release for s in series.values() if s.release})
    sql = f"SELECT release_date, release_name, released_at FROM {TABLE} WHERE release_date >= ?"
    params: list = [start]
    if not every:
        if not names:
            return []
        sql += f" AND release_name IN ({', '.join('?' for _ in names)})"
        params += names
    rows = con.execute(sql + " ORDER BY released_at, release_name LIMIT ?", [*params, n])
    return [(Release(*row), series_for(row[1], series)) for row in rows.fetchall()]


def coverage(con: duckdb.DuckDBPyConnection) -> tuple[int, date | None, date | None]:
    """(releases stored, first date, last date)."""
    return con.execute(
        f"SELECT count(*), min(release_date), max(release_date) FROM {TABLE}"
    ).fetchone()


# --- job -------------------------------------------------------------------------------------


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    try:
        raw_id, response = ctx.client.get(ICS_URL, source=SOURCE, dataset=RAW_DATASET)
        if response.status_code != 200:
            raise CalendarError(f"{ICS_URL} answered {response.status_code}", raw_id=raw_id)
    except Exception:
        # the stored calendar still ages: check it before the key fails
        check_stale(ctx.con, today=ctx.today, now=ctx.now)
        raise
    return [raw_id]


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    return store_releases(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir)


def _compute(ctx: "jobs.JobContext", key: str) -> int:
    check_stale(ctx.con, today=ctx.today, now=ctx.now)
    return 0


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="bls_calendar",
        keys=("schedule",),
        fetch=_fetch,
        parsers={RAW_DATASET: _parse},
        compute=_compute,
    )
)
"""The BLS release schedule into `bls_release`; Mondays 06:00 ET."""
