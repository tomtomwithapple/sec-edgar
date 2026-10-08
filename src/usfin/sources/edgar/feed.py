"""Job `edgar_feed`: EDGAR's latest-filings Atom feed into `edgar_filing`, `edgar_filing_party`
and the document queue `pending_doc`.

`https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&count=100&start=N` lists
the newest filings first (fetched 2026-10-07 22:57 ET: `application/atom+xml`, ISO-8859-1, 100
entries, 57 KB a page). It has **one entry per party of a filing**: title
`<form> - <name> (<10-digit CIK>) (<role>)` with role Issuer, Subject, Filer, Filed by or
Reporting (a Form 4 by four owners is five entries); `<updated>` is the acceptance time with its
New York offset; the summary carries `Filed: <date>` and `AccNo: <accession>`; `<category
term>` is the form; `<id>` ends `accession-number=<accession>`. A feed with nothing to list has
no `<entry>` at all.

Entries are grouped by accession: every (cik, role) becomes an `edgar_filing_party` row and the
filing one `edgar_filing` row (`source='feed'`, `cik` = the party with the first role in
`filings.ROLE_PRECEDENCE`, which also overrides a `cik` a role-less source wrote first — see
`filings.settle_cik`). Each filing queues its documents in `pending_doc` (`pending_kinds`):
`submissions` when a party is on the watchlist (`config/edgar.yaml`), `companyfacts` for 10-K /
10-Q (+/A) filers and watchlist 8-K (+/A) filers (decision C), `form4` for 4 and 4/A (CIK =
issuer), `sched13` for SCHEDULE 13D/13G and legacy SC 13D/13G (+/A) (CIK = subject).

Paging (`start=` by `COUNT`) stops at the first page holding an entry not newer than the
watermark (the newest `updated` seen, naive UTC), at a short page (the feed's end), or after
`MAX_PAGES`. Every page is archived first (raw dataset `edgar_feed`, source `edgar`) and parsed
alone, so a filing whose entries straddle two pages settles its `cik` when the second arrives.
Chained: `then` = `edgar_submissions`, `edgar_companyfacts` (each drains its kind); S24 and S25
add `edgar_form4` and `edgar_sched13` here when they add those jobs.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import yaml
from defusedxml import ElementTree

from usfin import jobs, raw
from usfin.settings import ConfigError
from usfin.sources.edgar import filings as edgar_filings
from usfin.sources.edgar import pending

BASE = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom"
COUNT = 100
MAX_PAGES = 10
SOURCE = "edgar"
RAW_DATASET = "edgar_feed"
KEY = "getcurrent"
ATOM = "{http://www.w3.org/2005/Atom}"
TITLE = re.compile(r"(?P<form>.+?) - (?P<name>.*) \((?P<cik>\d{10})\) \((?P<role>[^()]+)\)")
FILED = re.compile(r"Filed:</b>\s*(\d{4}-\d{2}-\d{2})")
ACCESSION = re.compile(r"accession-number=(\d{10}-\d{2}-\d{6})$")

COMPANYFACTS_FORMS = frozenset({"10-K", "10-Q", "10-K/A", "10-Q/A"})
WATCHLIST_FACTS_FORMS = frozenset({"8-K", "8-K/A"})
FORM4_FORMS = frozenset({"4", "4/A"})
SCHED13_FORMS = frozenset(
    f"{prefix} 13{x}{amend}"
    for prefix in ("SCHEDULE", "SC")
    for x in ("D", "G")
    for amend in ("", "/A")
)


class FeedError(Exception):
    """SEC answered something other than 200, or a body that is not the Atom feed."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Entry:
    accession_no: str
    cik: int
    role: str
    form: str
    filed_at: date
    updated: datetime
    """Acceptance time, naive UTC."""
    name: str


def _text(el, tag: str) -> str:
    child = el.find(ATOM + tag)
    if child is None or child.text is None:
        raise FeedError(f"entry without <{tag}>")
    return child.text.strip()


def parse_feed(body: bytes) -> list[Entry]:
    """The feed's entries in feed order (newest first)."""
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError as exc:
        raise FeedError(f"not XML: {exc}") from exc
    if root.tag != ATOM + "feed":
        raise FeedError(f"not an Atom feed: root <{root.tag}>")
    out = []
    for el in root.findall(ATOM + "entry"):
        title = _text(el, "title")
        m = TITLE.fullmatch(title)
        if m is None:
            raise FeedError(f"title {title!r} is not '<form> - <name> (<cik>) (<role>)'")
        acc = ACCESSION.search(_text(el, "id"))
        filed = FILED.search(_text(el, "summary"))
        if acc is None or filed is None:
            raise FeedError(f"entry {title!r} lacks an accession number or a filing date")
        category = el.find(ATOM + "category")
        form = (category.get("term") if category is not None else None) or m["form"]
        updated = datetime.fromisoformat(_text(el, "updated"))
        if updated.tzinfo is None:
            raise FeedError(f"entry {title!r}: <updated> has no UTC offset")
        out.append(
            Entry(
                accession_no=acc[1],
                cik=int(m["cik"]),
                role=m["role"].strip(),
                form=form.strip(),
                filed_at=date.fromisoformat(filed[1]),
                updated=updated.astimezone(UTC).replace(tzinfo=None),
                name=m["name"].strip(),
            )
        )
    return out


def _by_accession(entries: Sequence[Entry]) -> dict[str, list[Entry]]:
    groups: dict[str, list[Entry]] = {}
    for e in entries:
        groups.setdefault(e.accession_no, []).append(e)
    return groups


def group(entries: Sequence[Entry]) -> list[edgar_filings.Filing]:
    """One filing per accession (first-seen order), `cik` by role precedence."""
    out = []
    for acc, items in _by_accession(entries).items():
        first = items[0]
        cik = edgar_filings.best_party((e.cik, e.role) for e in items) or first.cik
        out.append(
            edgar_filings.Filing(
                accession_no=acc,
                cik=cik,
                form=first.form,
                filed_at=first.filed_at,
                accepted_at=min(e.updated for e in items),
                report_date=None,
                primary_doc=None,
                is_amendment=edgar_filings.is_amendment(first.form),
                source="feed",
            )
        )
    return out


def pending_kinds(form: str, *, on_watchlist: bool) -> tuple[str, ...]:
    """The `pending_doc` kinds a filing of `form` queues (see the module doc)."""
    form = form.strip().upper()
    kinds = []
    if on_watchlist:
        kinds.append("submissions")
    if form in COMPANYFACTS_FORMS or (on_watchlist and form in WATCHLIST_FACTS_FORMS):
        kinds.append("companyfacts")
    if form in FORM4_FORMS:
        kinds.append("form4")
    if form in SCHED13_FORMS:
        kinds.append("sched13")
    return tuple(kinds)


def load_watchlist(config_dir: Path | str) -> frozenset[int]:
    """CIKs in `<config_dir>/edgar.yaml` `watchlist:` (none when the file or key is absent)."""
    path = Path(config_dir) / "edgar.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a mapping")
    items = data.get("watchlist") or []
    if not isinstance(items, list):
        raise ConfigError(f"{path}: watchlist must be a list of CIKs")
    out = set()
    for item in items:
        if isinstance(item, bool) or not isinstance(item, int | str) or not str(item).isdigit():
            raise ConfigError(f"{path}: watchlist entry {item!r} is not a CIK")
        out.add(int(item))
    return frozenset(out)


def queue_rows(
    entries: Sequence[Entry], watchlist: frozenset[int]
) -> list[tuple[str, str, int]]:
    """(accession_no, kind, cik) rows the entries' filings queue."""
    rows = []
    for acc, items in _by_accession(entries).items():
        parties = [(e.cik, e.role) for e in items]
        cik = edgar_filings.best_party(parties) or items[0].cik
        watched = [(c, r) for c, r in parties if c in watchlist]
        watched_cik = (edgar_filings.best_party(watched) or watched[0][0]) if watched else None
        form = items[0].form.strip().upper()
        for kind in pending_kinds(form, on_watchlist=watched_cik is not None):
            if kind == "submissions" or (kind == "companyfacts" and form in WATCHLIST_FACTS_FORMS):
                rows.append((acc, kind, watched_cik))
            else:
                rows.append((acc, kind, cik))
    return rows


# --- job -------------------------------------------------------------------------------------


def page_url(start: int) -> str:
    url = f"{BASE}&count={COUNT}"
    return url if start == 0 else f"{url}&start={start}"


def _watermark(ctx: "jobs.JobContext") -> datetime | None:
    value = ctx.watermarks.get(KEY)
    return datetime.fromisoformat(value) if value else None


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    mark = _watermark(ctx)
    out = []
    for n in range(MAX_PAGES):
        url = page_url(n * COUNT)
        raw_id, response = ctx.client.get(url, source=SOURCE, dataset=RAW_DATASET)
        if response.status_code != 200:
            raise FeedError(f"{url} answered {response.status_code}", raw_id=raw_id)
        out.append(raw_id)
        entries = parse_feed(response.content)
        if len(entries) < COUNT or (mark is not None and any(e.updated <= mark for e in entries)):
            break
    return out


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    found = ctx.con.execute(
        "SELECT http_status, fetched_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    status, fetched_at = found
    if status != 200:
        return 0
    with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
        entries = parse_feed(fh.read())
    if "watchlist" not in ctx.memo:
        ctx.memo["watchlist"] = load_watchlist(ctx.settings.config_dir)
    found_filings = group(entries)
    rows = edgar_filings.upsert_filings(ctx.con, found_filings)
    edgar_filings.add_parties(ctx.con, ((e.accession_no, e.cik, e.role) for e in entries))
    rows += edgar_filings.settle_cik(ctx.con, (f.accession_no for f in found_filings))
    pending.enqueue(ctx.con, queue_rows(entries, ctx.memo["watchlist"]), enqueued_at=fetched_at)
    if entries:
        newest = max(e.updated for e in entries)
        mark = max(filter(None, (_watermark(ctx), ctx.memo.get("newest"), newest)))
        ctx.memo["newest"] = mark
        ctx.advance(KEY, mark)
    return rows


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="edgar_feed",
        keys=(KEY,),
        fetch=_fetch,
        parsers={RAW_DATASET: _parse},
        then=("edgar_submissions", "edgar_companyfacts"),
    )
)
"""Latest filings every 15 minutes 06:00–22:00 ET on federal days (schedule.yaml)."""
