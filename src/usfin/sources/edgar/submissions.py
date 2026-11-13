"""Job `edgar_submissions`: a company's filing list from data.sec.gov into `edgar_filing`.

`https://data.sec.gov/submissions/CIK##########.json` carries company metadata and
`filings.recent`: parallel arrays (`accessionNumber`, `filingDate`, `reportDate`,
`acceptanceDateTime`, `form`, `primaryDocument`, …) of the newest filings — 1 000, or one year
for very heavy filers (JPMorgan: 2025-10-07 on, 4.6 MB). Older filings are in pages listed in
`filings.files[]` as `{name, filingCount, filingFrom, filingTo}`, each page the same arrays at
its top level, at `https://data.sec.gov/submissions/<name>`. Pages are numbered newest first
(`-submissions-001.json` is the newest), so a new page renames every older one, and
`filingFrom`/`filingTo` are not the page's first and last filing dates (Apple's page says
1994-01-26..2015-09-15, its newest filing is 2015-09-11). A page is therefore known by its
listing (cik, filingFrom, filingTo, filingCount) in `edgar_submissions_page`, and fetched only
when no stored response has been parsed for that listing.

`acceptanceDateTime` ends in `Z` but is not UTC: it is the New York acceptance time pushed
forward by the UTC offset once more (2026-07-30 16:30:28 EDT is written 2026-07-31T00:30:28Z;
2026-01-29 16:30:33 EST 2026-01-30T02:30:33Z; checked against the filings' index headers on
2026-10-08), so `accepted_utc` reads it as UTC, converts to New York and keeps the wall time.
*(Found in S22, decided in S23.)* That shift is not on every filing: a filing whose
`filingDate` is the New York date the response was fetched carries the true UTC (JPMorgan,
fetched 2026-10-07 22:37 and 23:07 ET: the six 424B2s the feed also listed match its
offset-bearing times to the second, and 0001213900-26-107349 reads 2026-10-07T01:11:28Z where its
index header says ACCEPTANCE-DATETIME 20261006211128, FILED AS OF DATE 20261007 — accepted after
17:30, so dated the next day: a rule on the acceptance date would get it wrong). So
`store_document(fetched_at=)` reads the time as UTC for a filing of the fetch day and converts the
others; the choice uses the stored response's `fetched_at`, so reparse is deterministic. A
corrected value replaces one submissions wrote before (`filings.upsert_filings`) unless a feed
role is stored for the filing (the feed's time wins). Without `fetched_at` (S26's bulk members)
every time is converted, as S20 did.

Each filing becomes an `edgar_filing` row (`source='submissions'`, `cik` = the company) and an
`edgar_filing_party` row (the company, role `submissions`: the JSON names no role). Every
response is archived first (raw dataset `edgar_submissions`, source `edgar`). Watermark key = the
10-digit CIK, value = its newest `filingDate`. The registered job drains the CIKs of open
`pending_doc` rows of kind `submissions` (queued by the feed for watchlist companies) and closes
the rows whose accession the company now lists (`pending.mark_done`); `usfin edgar submissions --cik` runs it for given CIKs (`for_ciks`).
*(S45.)* A main document also upserts the company's `edgar_company_profile` row (SIC, owner org,
entity type…; `profile.store`), for bulk members too.
"""

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import duckdb

from usfin import jobs, raw
from usfin.sources.edgar import filings as edgar_filings
from usfin.sources.edgar import pending
from usfin.sources.edgar import profile as edgar_profile

BASE = "https://data.sec.gov/submissions/"
SOURCE = "edgar"
RAW_DATASET = "edgar_submissions"
ROLE = "submissions"
PAGES = "edgar_submissions_page"
ET = ZoneInfo("America/New_York")
NAME = re.compile(r"CIK(\d{10})(?:-submissions-\d+)?\.json")
REQUIRED = ("accessionNumber", "filingDate", "form")


class SubmissionsError(Exception):
    """SEC answered something other than 200, or a body that is not a submissions document."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


def cik_key(cik: int) -> str:
    return f"{int(cik):010d}"


def main_url(cik: int) -> str:
    return f"{BASE}CIK{cik_key(cik)}.json"


def accepted_utc(value: str | None, *, true_utc: bool = False) -> datetime | None:
    """`acceptanceDateTime` as naive UTC: as written when `true_utc` (a filing of the fetch day),
    else converted back from SEC's shift (see the module doc)."""
    if not value:
        return None
    parsed = datetime.fromisoformat(value).astimezone(UTC)
    if true_utc:
        return parsed.replace(tzinfo=None)
    return parsed.astimezone(ET).replace(tzinfo=None)


def _date(value: Any) -> date | None:
    return date.fromisoformat(value) if value else None


def filings_from_columns(
    cols: Mapping[str, Any], *, cik: int, fetched_on: date | None = None
) -> list[edgar_filings.Filing]:
    """The filings of one columnar block (`filings.recent` or a page); `fetched_on` is the New
    York date the response was fetched (filings of that date carry the true UTC)."""
    if not isinstance(cols, Mapping):
        raise SubmissionsError(f"expected columnar filings, got {type(cols).__name__}")
    missing = [c for c in REQUIRED if not isinstance(cols.get(c), list)]
    if missing:
        raise SubmissionsError(f"columnar filings lack {missing}")
    n = len(cols["accessionNumber"])
    uneven = sorted(k for k, v in cols.items() if isinstance(v, list) and len(v) != n)
    if uneven:
        raise SubmissionsError(f"columns {uneven} are not {n} long like accessionNumber")

    def col(name: str) -> list:
        v = cols.get(name)
        return v if isinstance(v, list) else [None] * n

    out = []
    for acc, filed, form, report, accepted, doc in zip(
        cols["accessionNumber"], cols["filingDate"], cols["form"], col("reportDate"),
        col("acceptanceDateTime"), col("primaryDocument"), strict=True,
    ):  # fmt: skip
        if not acc or not filed or not form:
            raise SubmissionsError(f"filing {acc!r} lacks accessionNumber, filingDate or form")
        filed_at = date.fromisoformat(filed)
        out.append(
            edgar_filings.Filing(
                accession_no=acc,
                cik=cik,
                form=form,
                filed_at=filed_at,
                accepted_at=accepted_utc(accepted, true_utc=filed_at == fetched_on),
                report_date=_date(report),
                primary_doc=doc or None,
                is_amendment=edgar_filings.is_amendment(form),
                source="submissions",
            )
        )
    return out


@dataclass(frozen=True)
class Page:
    name: str
    filing_from: date
    filing_to: date
    filing_count: int


def page_listing(doc: Mapping[str, Any]) -> list[Page]:
    files = (doc.get("filings") or {}).get("files") or []
    try:
        return [
            Page(f["name"], date.fromisoformat(f["filingFrom"]), date.fromisoformat(f["filingTo"]),
                 int(f["filingCount"]))
            for f in files
        ]  # fmt: skip
    except (KeyError, TypeError, ValueError) as exc:
        raise SubmissionsError(f"filings.files is not [{{name, filingCount, …}}]: {exc}") from exc


@dataclass(frozen=True)
class Stored:
    cik: int
    filings: int
    """`edgar_filing` rows inserted or filled."""
    parties: int
    newest: date | None
    """The newest `filingDate` of a main document (None for a page)."""


def store_document(
    con: duckdb.DuckDBPyConnection,
    doc: Any,
    *,
    name: str,
    raw_id: int | None = None,
    fetched_at: datetime | None = None,
) -> Stored:
    """Store one submissions document named `name` (`CIK##########.json` or a page
    `CIK##########-submissions-NNN.json`, as served and as bulk ZIP members are named);
    `fetched_at` (naive UTC) is when it was fetched, which decides how acceptance times are read."""
    m = NAME.fullmatch(name)
    if m is None:
        raise SubmissionsError(f"{name!r} is not a submissions document name")
    cik = int(m.group(1))
    if not isinstance(doc, Mapping):
        raise SubmissionsError(f"{name}: expected an object, got {type(doc).__name__}")
    is_page = "-submissions-" in name
    fetched_on = (
        None if fetched_at is None else fetched_at.replace(tzinfo=UTC).astimezone(ET).date()
    )
    if is_page:
        found = filings_from_columns(doc, cik=cik, fetched_on=fetched_on)
        newest = None
    else:
        if str(doc.get("cik", "")).lstrip("0") != str(cik):
            raise SubmissionsError(f"{name}: cik is {doc.get('cik')!r}")
        recent = (doc.get("filings") or {}).get("recent")
        found = filings_from_columns(recent, cik=cik, fetched_on=fetched_on)
        newest = max((f.filed_at for f in found), default=None)
        _store_listing(con, cik, page_listing(doc))
        edgar_profile.store(con, cik, doc, seen_at=fetched_at, raw_id=raw_id)
    rows = edgar_filings.upsert_filings(con, found)
    parties = edgar_filings.add_parties(con, ((f.accession_no, cik, ROLE) for f in found))
    if is_page and raw_id is not None:
        con.execute(
            f"UPDATE {PAGES} SET raw_id = ? WHERE cik = ? AND name = ?", [raw_id, cik, name]
        )
    return Stored(cik, rows, parties, newest)


def _store_listing(con: duckdb.DuckDBPyConnection, cik: int, pages: Sequence[Page]) -> None:
    """Make the company's page listing equal `pages`; a listing seen before keeps its raw_id."""
    stored = {
        (f, t, c): name
        for f, t, c, name in con.execute(
            f"SELECT filing_from, filing_to, filing_count, name FROM {PAGES} WHERE cik = ?", [cik]
        ).fetchall()
    }
    listed = {(p.filing_from, p.filing_to, p.filing_count): p.name for p in pages}
    for key in stored.keys() - listed.keys():
        con.execute(
            f"DELETE FROM {PAGES} WHERE cik = ? AND filing_from = ? AND filing_to = ?"
            " AND filing_count = ?",
            [cik, *key],
        )
    for key, name in listed.items():
        if key not in stored:
            con.execute(f"INSERT INTO {PAGES} VALUES (?, ?, ?, ?, ?, NULL)", [cik, *key, name])
        elif stored[key] != name:
            con.execute(
                f"UPDATE {PAGES} SET name = ? WHERE cik = ? AND filing_from = ? AND filing_to = ?"
                " AND filing_count = ?",
                [name, cik, *key],
            )


def pages_to_fetch(con: duckdb.DuckDBPyConnection, cik: int, doc: Mapping[str, Any]) -> list[Page]:
    """The listed pages no parsed response exists for (by listing, not by name)."""
    have = {
        tuple(r)
        for r in con.execute(
            f"SELECT filing_from, filing_to, filing_count FROM {PAGES}"
            " WHERE cik = ? AND raw_id IS NOT NULL",
            [cik],
        ).fetchall()
    }
    return [
        p for p in page_listing(doc) if (p.filing_from, p.filing_to, p.filing_count) not in have
    ]


# --- job -------------------------------------------------------------------------------------


def _get(ctx: "jobs.JobContext", url: str) -> tuple[int, Any]:
    raw_id, response = ctx.client.get(url, source=SOURCE, dataset=RAW_DATASET)
    if response.status_code != 200:
        raise SubmissionsError(f"{url} answered {response.status_code}", raw_id=raw_id)
    return raw_id, response.json()


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    cik = int(key)
    raw_id, doc = _get(ctx, main_url(cik))
    out = [raw_id]
    for page in pages_to_fetch(ctx.con, cik, doc):
        out.append(_get(ctx, BASE + page.name)[0])
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
        doc = json.loads(fh.read())
    stored = store_document(
        ctx.con, doc, name=url.rsplit("/", 1)[-1], raw_id=raw_id, fetched_at=fetched_at
    )
    if stored.newest is not None:
        ctx.advance(cik_key(stored.cik), stored.newest)
    return stored.filings


def pending_ciks(ctx: "jobs.JobContext") -> list[str]:
    """CIKs with open `pending_doc` rows of kind `submissions`."""
    return [cik_key(c) for c in pending.open_ciks(ctx.con, "submissions")]


def _done(ctx: "jobs.JobContext", key: str) -> None:
    """Close the key's queued `submissions` docs the fetched document carries (`pending.mark_done`)."""
    pending.mark_done(ctx.con, "submissions", int(key), now=jobs._utc_naive(ctx.now))


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="edgar_submissions",
        keys=pending_ciks,
        fetch=_fetch,
        parsers={RAW_DATASET: _parse},
        after_key=_done,
    )
)
"""Per-CIK submissions into `edgar_filing`; chained after the feed (S22), no schedule of its own."""


def for_ciks(ciks: Sequence[int]) -> jobs.Job:
    """`JOB` with fixed keys: the given CIKs instead of the pending queue."""
    return replace(JOB, keys=tuple(dict.fromkeys(cik_key(c) for c in ciks)))
