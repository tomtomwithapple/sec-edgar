"""Job `edgar_sched13`: Schedule 13D/G beneficial-ownership filings into `ownership_filing` and
`ownership_stake` (C13).

Every SCHEDULE 13D/13G (+/A) and legacy SC 13D/13G (+/A) accession the feed (S22) or the daily
index (S23) saw is queued in `pending_doc` (kind `sched13`). The queued CIK is the subject when
the feed saw the filing, but may be a reporting person for one only the index saw; the folder
answers under any party's CIK, so it serves the URL, and the subject is always read from the XML.
Per accession (the job's keys — the oldest open rows, at most `sched13: max_docs_per_run` in
`config/edgar.yaml`, default `pending.MAX_DOCS_DEFAULT`):

1. `https://www.sec.gov/Archives/edgar/data/<cik>/<accession without dashes>/index.json` (raw
   dataset `edgar_filing_index`, shared with `edgar_form4`).
2. Its `primary_doc.xml` (raw dataset `edgar_sched13_xml`), an `<edgarSubmission>` in namespace
   `http://www.sec.gov/edgar/schedule13D` or `…/schedule13g` (schema X0202 in every 2026 file
   seen). Structured XML is required since 2024-12-18; a filing whose `edgar_filing.form` is a
   legacy name (`SC 13D`, `SC 13G`, +/A: HTML or text) fetches only its folder listing, and the
   listing's parser records it as skipped: an `ownership_filing` row with `legacy` true and no
   persons (counted with `SELECT count(*) FROM ownership_filing WHERE legacy`).

Element mapping (`parse_document`; namespaces are ignored, dates are `MM/DD/YYYY`):
`headerData/submissionType` → form (SCHEDULE 13D/13G, +/A; anything else refused);
`headerData/filerInfo/filer/filerCredentials/cik` → filer_cik (the first filer);
`formData/coverPageHeader`: `amendmentNo`, `securitiesClassTitle` → class_title,
`dateOfEvent` (13D) or `eventDateRequiresFilingThisStatement` (13G) → event_date,
`issuerInfo/issuerCIK` (13D) or `issuerCik` (13G) → subject_cik, `issuerInfo/issuerName`,
`issuerInfo/issuerCusips/issuerCusipNumber` → subject_cusips. Reporting persons, in order:
13D `formData/reportingPersons/reportingPersonInfo` with `reportingPersonCIK` (absent when
`reportingPersonNoCIK` is Y), `reportingPersonName`, `memberOfGroup`, `citizenshipOrOrganization`,
`soleVotingPower`, `sharedVotingPower`, `soleDispositivePower`, `sharedDispositivePower`,
`aggregateAmountOwned` → shares, `percentOfClass`, `typeOfReportingPerson` (repeated);
13G `formData/coverPageHeaderReportingPersonDetails` with no CIK element at all,
`reportingPersonName`, `memberGroup`, `citizenshipOrOrganization`,
`reportingPersonBeneficiallyOwnedNumberOfShares/soleVotingPower|sharedVotingPower|
soleDispositivePower|sharedDispositivePower`,
`reportingPersonBeneficiallyOwnedAggregateNumberOfShares` → shares, `classPercent`,
`typeOfReportingPerson` (repeated). Not kept: addresses, comments, the narrative items, signatures.

A document's rows are compared with the stored ones of its accession; when anything differs both
tables' rows of the accession are replaced (`form4.replace_rows`), so a re-run or reparse
changes nothing (C2). Rows counted = `ownership_stake` rows inserted or changed. After an
accession is stored its `pending_doc` row is closed (`pending.close`).
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import duckdb
from defusedxml import ElementTree

from usfin import jobs, raw
from usfin.sources.edgar import feed, form4, pending

SOURCE = "edgar"
INDEX_DATASET = form4.INDEX_DATASET
XML_DATASET = "edgar_sched13_xml"
KIND = "sched13"
DOCUMENT = "primary_doc.xml"
FORMS = ("SCHEDULE 13D", "SCHEDULE 13D/A", "SCHEDULE 13G", "SCHEDULE 13G/A")
LEGACY_FORMS = frozenset(feed.SCHED13_FORMS) - set(FORMS)


class Sched13Error(Exception):
    """SEC answered something other than 200, or a body that is not a Schedule 13D/G."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Person:
    seq: int
    cik: int | None
    name: str | None
    member_of_group: str | None
    citizenship: str | None
    sole_voting: float | None
    shared_voting: float | None
    sole_dispositive: float | None
    shared_dispositive: float | None
    shares: float | None
    percent_of_class: float | None
    person_type: str | None


@dataclass(frozen=True)
class Document:
    form: str
    amendment_no: int | None
    filer_cik: int | None
    subject_cik: int
    subject_name: str | None
    subject_cusips: str | None
    class_title: str | None
    event_date: date | None
    persons: list[Person]


def is_legacy(form: str | None) -> bool:
    """A pre-XML form name (`SC 13D`, `SC 13G`, +/A)."""
    return (form or "").strip().upper() in LEGACY_FORMS


# --- parsing -----------------------------------------------------------------------------------

PERSON_PATHS = {
    # column: (13D path, 13G path) under the person element
    "cik": ("reportingPersonCIK", None),
    "name": ("reportingPersonName", "reportingPersonName"),
    "member_of_group": ("memberOfGroup", "memberGroup"),
    "citizenship": ("citizenshipOrOrganization", "citizenshipOrOrganization"),
    "sole_voting": (
        "soleVotingPower", "reportingPersonBeneficiallyOwnedNumberOfShares/soleVotingPower",
    ),
    "shared_voting": (
        "sharedVotingPower", "reportingPersonBeneficiallyOwnedNumberOfShares/sharedVotingPower",
    ),
    "sole_dispositive": (
        "soleDispositivePower",
        "reportingPersonBeneficiallyOwnedNumberOfShares/soleDispositivePower",
    ),
    "shared_dispositive": (
        "sharedDispositivePower",
        "reportingPersonBeneficiallyOwnedNumberOfShares/sharedDispositivePower",
    ),
    "shares": (
        "aggregateAmountOwned", "reportingPersonBeneficiallyOwnedAggregateNumberOfShares",
    ),
    "percent_of_class": ("percentOfClass", "classPercent"),
}  # fmt: skip
NUMBERS = ("sole_voting", "shared_voting", "sole_dispositive", "shared_dispositive", "shares",
           "percent_of_class")  # fmt: skip


def _strip_namespaces(root) -> None:
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]


def _text(el) -> str | None:
    if el is None or el.text is None:
        return None
    return el.text.strip() or None


def _number(text: str | None, what: str) -> float | None:
    if text is None:
        return None
    try:
        return float(text.replace(",", "").removesuffix("%"))
    except ValueError:
        raise Sched13Error(f"{what}: {text!r} is not a number") from None


def _cik(text: str | None, what: str) -> int | None:
    if text is None:
        return None
    if not text.isdigit():
        raise Sched13Error(f"{what}: {text!r} is not a CIK")
    return int(text)


def _date(text: str | None, what: str) -> date | None:
    if text is None:
        return None
    month, _, rest = text.partition("/")
    day, _, year = rest.partition("/")
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        raise Sched13Error(f"{what}: {text!r} is not a MM/DD/YYYY date") from None


def _person(el, seq: int, *, is_13d: bool) -> Person:
    values: dict[str, Any] = {}
    for column, paths in PERSON_PATHS.items():
        path = paths[0] if is_13d else paths[1]
        values[column] = _text(el.find(path)) if path else None
    for column in NUMBERS:
        values[column] = _number(values[column], f"person {seq} {column}")
    values["cik"] = _cik(values["cik"], f"person {seq} reportingPersonCIK")
    types = [t for t in (_text(x) for x in el.findall("typeOfReportingPerson")) if t]
    return Person(seq=seq, person_type=",".join(types) or None, **values)


def parse_document(body: bytes) -> Document:
    """One Schedule 13D/G `primary_doc.xml` (see the module doc for the element mapping)."""
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError as exc:
        raise Sched13Error(f"not XML: {exc}") from exc
    _strip_namespaces(root)
    if root.tag != "edgarSubmission":
        raise Sched13Error(f"not a Schedule 13D/G document: root <{root.tag}>")
    form = _text(root.find("headerData/submissionType"))
    if form not in FORMS:
        raise Sched13Error(f"submissionType {form!r} is not one of {FORMS}")
    is_13d = form.startswith("SCHEDULE 13D")
    cover = root.find("formData/coverPageHeader")
    if cover is None:
        raise Sched13Error("no formData/coverPageHeader")
    issuer = "issuerInfo/issuerCIK" if is_13d else "issuerInfo/issuerCik"
    subject = _cik(_text(cover.find(issuer)), issuer)
    if subject is None:
        raise Sched13Error(f"no {issuer}")
    event = "dateOfEvent" if is_13d else "eventDateRequiresFilingThisStatement"
    amendment = _text(cover.find("amendmentNo"))
    if amendment is not None and not amendment.isdigit():
        raise Sched13Error(f"amendmentNo {amendment!r} is not a number")
    cusips = [c for c in (_text(x) for x in cover.findall("issuerInfo/issuerCusips/*")) if c]
    path = "formData/reportingPersons/reportingPersonInfo" if is_13d else (
        "formData/coverPageHeaderReportingPersonDetails"
    )
    persons = [_person(el, i, is_13d=is_13d) for i, el in enumerate(root.findall(path), 1)]
    if not persons:
        raise Sched13Error("no reporting person")
    return Document(
        form=form,
        amendment_no=int(amendment) if amendment is not None else None,
        filer_cik=_cik(
            _text(root.find("headerData/filerInfo/filer/filerCredentials/cik")), "filer cik"
        ),
        subject_cik=subject,
        subject_name=_text(cover.find("issuerInfo/issuerName")),
        subject_cusips=",".join(cusips) or None,
        class_title=_text(cover.find("securitiesClassTitle")),
        event_date=_date(_text(cover.find(event)), event),
        persons=persons,
    )


def pick_document(body: bytes) -> str | None:
    """`primary_doc.xml` when the folder's `index.json` lists it, else None."""
    try:
        items = json.loads(body)["directory"]["item"]
    except (ValueError, KeyError, TypeError) as exc:
        raise Sched13Error(f"not a filing-folder index.json: {exc!r}") from None
    names = {str(item.get("name", "")) for item in items}
    return DOCUMENT if DOCUMENT in names else None


# --- storage -----------------------------------------------------------------------------------

TABLES = ("ownership_filing", "ownership_stake")


def _filed(con: duckdb.DuckDBPyConnection, accession_no: str) -> tuple[str | None, date | None]:
    row = con.execute(
        "SELECT form, filed_at FROM edgar_filing WHERE accession_no = ?", [accession_no]
    ).fetchone()
    return (row[0], row[1]) if row else (None, None)


def store_document(
    con: duckdb.DuckDBPyConnection, accession_no: str, doc: Document, *, document: str
) -> int:
    """Replace the accession's rows when the document differs; return `ownership_stake` rows
    inserted or changed (0 when nothing differs)."""
    _, filed = _filed(con, accession_no)
    rows = {
        "ownership_filing": [
            [accession_no, doc.form, False, doc.subject_cik, doc.subject_name,
             doc.subject_cusips, doc.class_title, doc.event_date, doc.amendment_no,
             doc.filer_cik, filed, document, len(doc.persons)]
        ],
        "ownership_stake": [
            [accession_no, p.seq, p.cik, p.name, doc.subject_cik, doc.form, doc.event_date,
             p.percent_of_class, p.shares, filed, p.sole_voting, p.shared_voting,
             p.sole_dispositive, p.shared_dispositive, p.member_of_group, p.citizenship,
             p.person_type]
            for p in doc.persons
        ],
    }  # fmt: skip
    return form4.replace_rows(con, accession_no, rows, counted="ownership_stake")


def store_legacy(con: duckdb.DuckDBPyConnection, accession_no: str) -> int:
    """Record a legacy (pre-XML) filing as skipped; return 0 (no person rows)."""
    form, filed = _filed(con, accession_no)
    rows = {
        "ownership_filing": [
            [accession_no, form, True, None, None, None, None, None, None, None, filed, None, 0]
        ],
        "ownership_stake": [],
    }
    return form4.replace_rows(con, accession_no, rows, counted="ownership_stake")


# --- job ---------------------------------------------------------------------------------------


def queued(ctx: "jobs.JobContext") -> list[str]:
    """The oldest open `sched13` accessions, at most `max_docs_per_run`."""
    rows = pending.open_accessions(
        ctx.con, KIND, limit=pending.max_docs(ctx.settings.config_dir, KIND)
    )
    ctx.memo["cik"] = dict(rows)
    return [acc for acc, _ in rows]


def _get(ctx: "jobs.JobContext", url: str, dataset: str):
    raw_id, response = ctx.client.get(url, source=SOURCE, dataset=dataset)
    if response.status_code != 200:
        raise Sched13Error(f"{url} answered {response.status_code}", raw_id=raw_id)
    return raw_id, response


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    folder = form4.folder_url(ctx.memo["cik"][key], key)
    index_id, response = _get(ctx, folder + "index.json", INDEX_DATASET)
    form, _ = _filed(ctx.con, key)
    if is_legacy(form):
        return [index_id]  # its parser records the skip
    name = pick_document(response.content)
    if name is None:
        raise Sched13Error(f"{key} ({form}): no {DOCUMENT} in the filing folder", raw_id=index_id)
    xml_id, _ = _get(ctx, folder + name, XML_DATASET)
    return [index_id, xml_id]


def _response(ctx: "jobs.JobContext", raw_id: int) -> tuple[str, int]:
    found = ctx.con.execute(
        "SELECT url, http_status FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    return found


def _parse_index(ctx: "jobs.JobContext", raw_id: int) -> int:
    """A folder listing records a legacy filing's skip; any other listing (a structured
    Schedule 13D/G, or a Form 4's — the dataset is shared) stores nothing."""
    url, status = _response(ctx, raw_id)
    if status != 200:
        return 0
    accession_no = form4._accession(url)
    form, _ = _filed(ctx.con, accession_no)
    if not is_legacy(form):
        return 0
    return store_legacy(ctx.con, accession_no)


def _parse_xml(ctx: "jobs.JobContext", raw_id: int) -> int:
    url, status = _response(ctx, raw_id)
    if status != 200:
        return 0
    with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
        doc = parse_document(fh.read())
    accession_no = form4._accession(url)
    return store_document(ctx.con, accession_no, doc, document=url.rsplit("/", 1)[-1])


def _done(ctx: "jobs.JobContext", key: str) -> None:
    pending.close(ctx.con, KIND, key, now=jobs._utc_naive(ctx.now))


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="edgar_sched13",
        keys=queued,
        fetch=_fetch,
        parsers={INDEX_DATASET: _parse_index, XML_DATASET: _parse_xml},
        after_key=_done,
    )
)
"""Drains `pending_doc` kind `sched13`; chained after `edgar_feed` and `edgar_reconcile`."""
