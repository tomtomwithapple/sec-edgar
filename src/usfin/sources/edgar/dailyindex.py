"""Job `edgar_reconcile`: EDGAR's daily index into `edgar_filing`, `edgar_filing_party` and the
document queue `pending_doc`, so every accession of a day lands exactly once (C14).

`https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx` (fetched
2026-10-07 23:14 ET for 10-06: `application/octet-stream`, ASCII, LF, no newline after the last
line, 310 875 bytes, 3 501 lines, 2 370 accessions) is a header (`Description:`, `Last Data
Received:`, …, the column line `CIK|Company Name|Form Type|Date Filed|File Name` and a line of 80
dashes) then one line per (CIK, filing), sorted by CIK as text: an accession filed under several
CIKs (a Form 4's issuer and owners, a 13D/G's filer and subject) is listed once per CIK, not
adjacent, with no role. `Date Filed` is `YYYYMMDD` and need not be the index's day (a DRS of
2026-09-29 and EFFECT/FOCUSN notices of earlier days sit in 10-06's file); `File Name` is
`edgar/data/<cik>/<accession>.txt`.

Each accession becomes one `edgar_filing` row (`source='index'`, `cik` = its first CIK in the
file, `accepted_at`/`report_date`/`primary_doc` NULL — a later source fills them) and each line an
`edgar_filing_party` row (role `index`); `filings.settle_cik` then points `cik` at the best feed
role when the feed saw the filing (an index-only filing keeps its first CIK). Form 4 (4, 4/A) and
Schedule 13D/G (new and legacy names) accessions are queued in `pending_doc` (kinds `form4`,
`sched13`, CIK = the filing's settled `cik`, `enqueued_at` = the response's `fetched_at`); a row the
feed queued first stays as it is.

Window (C3): the federal days from min(watermark, today − 7 days) to today — 7 days of overlap, or
back to the watermark after a longer outage — where the watermark (key `master`) is the newest day
whose index parsed. Today's index is requested only from `published_after` (New York time,
`config/edgar.yaml` `daily_index:`, default 22:30): SEC writes each day's file about 22:02 ET that
evening (Last-Modified of 10-01, 10-02, 10-06, 10-07, probed 2026-10-08), and before that it
answers 403 — S32's 03:47 ET catch-up asked for master.20261008.idx and opened the SEC block (S36).
After that time a 404 or 403 for today (SEC late; the client does not block on a daily-index 403
for today or later) is normal; a 404 for an earlier federal day fails the key after the other days
are parsed, so the gap stays visible and the watermark stays put (a 403 for one is SEC refusing us,
C5: the block opens and the run fails). Every response is archived first (raw dataset `edgar_master_idx`, source
`edgar`). Chained: `edgar_form4` (S24) and `edgar_sched13` (S25), as for the feed.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

import yaml

from usfin import calendars, jobs, raw
from usfin.settings import ConfigError
from usfin.sources.edgar import feed, pending
from usfin.sources.edgar import filings as edgar_filings

BASE = "https://www.sec.gov/Archives/edgar/daily-index/"
SOURCE = "edgar"
RAW_DATASET = "edgar_master_idx"
KEY = "master"
ROLE = "index"
LOOKBACK = timedelta(days=7)
CALENDAR = "federal"
COLUMNS = "CIK|Company Name|Form Type|Date Filed|File Name"
SEPARATOR = re.compile(r"^-{20,}$")
FILE_NAME = re.compile(r"edgar/data/\d+/(\d{10}-\d{2}-\d{6})\.txt")
URL_DAY = re.compile(r"/master\.(\d{8})\.idx$")
PUBLISHED_AFTER_DEFAULT = time(22, 30)
"""New York time from which today's index is requested; SEC wrote 10-01..10-07's at 22:01–22:04."""


class IndexError_(Exception):
    """SEC answered something other than 200, or a body that is not a daily index."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Line:
    cik: int
    name: str
    form: str
    filed_at: date
    accession_no: str


def parse_index(body: bytes) -> list[Line]:
    """The index's data lines in file order."""
    text = body.decode("latin-1")
    lines = text.split("\n")
    try:
        start = next(i for i, ln in enumerate(lines) if SEPARATOR.match(ln.rstrip("\r")))
    except StopIteration:
        raise IndexError_("no header separator: not a daily index") from None
    if COLUMNS not in (ln.rstrip("\r") for ln in lines[:start]):
        raise IndexError_(f"header lacks the column line {COLUMNS!r}")
    out = []
    for ln in lines[start + 1 :]:
        ln = ln.rstrip("\r")
        if not ln:
            continue
        fields = ln.split("|")
        if len(fields) < 5:
            raise IndexError_(f"line {ln!r} has {len(fields)} fields, not 5")
        cik, form, filed, file_name = fields[0], fields[-3], fields[-2], fields[-1]
        m = FILE_NAME.fullmatch(file_name)
        if not cik.isdigit() or m is None or not (len(filed) == 8 and filed.isdigit()):
            raise IndexError_(f"line {ln!r} is not CIK|name|form|YYYYMMDD|edgar/data/…")
        out.append(
            Line(
                cik=int(cik),
                name="|".join(fields[1:-3]),
                form=form.strip(),
                filed_at=date(int(filed[:4]), int(filed[4:6]), int(filed[6:])),
                accession_no=m[1],
            )
        )
    return out


def group(lines: Sequence[Line]) -> list[edgar_filings.Filing]:
    """One filing per accession (first-seen order), `cik` = its first listed CIK."""
    first: dict[str, Line] = {}
    for ln in lines:
        first.setdefault(ln.accession_no, ln)
    return [
        edgar_filings.Filing(
            accession_no=acc,
            cik=ln.cik,
            form=ln.form,
            filed_at=ln.filed_at,
            accepted_at=None,
            report_date=None,
            primary_doc=None,
            is_amendment=edgar_filings.is_amendment(ln.form),
            source="index",
        )
        for acc, ln in first.items()
    ]


def queue_kind(form: str) -> str | None:
    """The `pending_doc` kind the index queues for `form` (Form 4 and Schedule 13D/G only)."""
    form = form.strip().upper()
    if form in feed.FORM4_FORMS:
        return "form4"
    if form in feed.SCHED13_FORMS:
        return "sched13"
    return None


def day_url(d: date) -> str:
    return f"{BASE}{d.year}/QTR{(d.month - 1) // 3 + 1}/master.{d:%Y%m%d}.idx"


def load_published_after(config_dir: Path | str) -> time:
    """`daily_index: published_after: "HH:MM"` (New York) in `<config_dir>/edgar.yaml`."""
    path = Path(config_dir) / "edgar.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a mapping")
    section = data.get("daily_index") or {}
    if not isinstance(section, dict):
        raise ConfigError(f"{path}: daily_index must be a mapping")
    value = section.get("published_after")
    if value is None:
        return PUBLISHED_AFTER_DEFAULT
    if isinstance(value, int) and not isinstance(value, bool):  # YAML 1.1 reads 22:30 as 1350
        value = f"{value // 60:02d}:{value % 60:02d}"
    try:
        return time.fromisoformat(str(value))
    except ValueError:
        raise ConfigError(f"{path}: daily_index.published_after {value!r} is not HH:MM") from None


def published(now: datetime, published_after: time) -> bool:
    """Whether today's (New York) index may exist at `now`."""
    return now.astimezone(jobs.ET).time() >= published_after


def window(
    cals: calendars.Calendars, today: date, watermark: date | None, *, with_today: bool = True
) -> list[date]:
    """Federal days from min(watermark, today − 7 days) to today (to yesterday without
    `with_today`)."""
    start = today - LOOKBACK
    if watermark is not None:
        start = min(start, watermark)
    last = today if with_today else today - timedelta(days=1)
    days, d = [], start
    while d <= last:
        if cals.is_day(CALENDAR, d):
            days.append(d)
        d += timedelta(days=1)
    return days


# --- job -------------------------------------------------------------------------------------


def _watermark(ctx: "jobs.JobContext") -> date | None:
    value = ctx.watermarks.get(KEY)
    return date.fromisoformat(value) if value else None


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    cals = calendars.load_calendars(ctx.settings.config_dir)
    with_today = published(ctx.now, load_published_after(ctx.settings.config_dir))
    out, missing = [], []
    for d in window(cals, ctx.today, _watermark(ctx), with_today=with_today):
        url = day_url(d)
        raw_id, response = ctx.client.get(url, source=SOURCE, dataset=RAW_DATASET)
        if response.status_code == 404 or (response.status_code == 403 and d >= ctx.today):
            if d < ctx.today:  # today's may be late; 403 for a past day never gets here (C5)
                missing.append(d)
        elif response.status_code != 200:
            raise IndexError_(f"{url} answered {response.status_code}", raw_id=raw_id)
        out.append(raw_id)
    ctx.memo["missing"] = missing
    return out


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    found = ctx.con.execute(
        "SELECT url, http_status, fetched_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    url, status, fetched_at = found
    if status != 200:
        return 0
    with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
        lines = parse_index(fh.read())
    con = ctx.con
    found_filings = group(lines)
    rows = edgar_filings.upsert_filings(con, found_filings)
    edgar_filings.add_parties(con, ((ln.accession_no, ln.cik, ROLE) for ln in lines))
    accs = [f.accession_no for f in found_filings]
    rows += edgar_filings.settle_cik(con, accs)
    queued = [(f.accession_no, queue_kind(f.form)) for f in found_filings]
    queued = [(a, k) for a, k in queued if k is not None]
    if queued:
        ciks = dict(
            con.execute(
                "SELECT accession_no, cik FROM edgar_filing WHERE accession_no IN"
                " (SELECT unnest(?::VARCHAR[]))",
                [[a for a, _ in queued]],
            ).fetchall()
        )
        pending.enqueue(con, ((a, k, ciks[a]) for a, k in queued), enqueued_at=fetched_at)
    m = URL_DAY.search(url)
    if m is None:
        raise IndexError_(f"{url} is not a master.YYYYMMDD.idx URL")
    day = date(int(m[1][:4]), int(m[1][4:6]), int(m[1][6:]))
    mark = max(filter(None, (_watermark(ctx), ctx.memo.get("newest"), day)))
    ctx.memo["newest"] = mark
    ctx.advance(KEY, mark)
    return rows


def _check_gaps(ctx: "jobs.JobContext", key: str) -> int:
    """Fail the key (after the other days parsed) when an earlier day's index was missing."""
    missing = ctx.memo.get("missing") or []
    if missing:
        days = ", ".join(d.isoformat() for d in missing)
        raise IndexError_(f"no daily index for federal day(s) {days} (404)")
    return 0


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="edgar_reconcile",
        keys=(KEY,),
        fetch=_fetch,
        parsers={RAW_DATASET: _parse},
        compute=_check_gaps,
        then=("edgar_form4", "edgar_sched13"),
    )
)
"""Daily-index reconcile at 23:30 ET on federal days (schedule.yaml)."""
