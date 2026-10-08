"""Job `edgar_form4`: Form 4 ownership documents into `insider_filing`, `insider_owner`,
`insider_txn` and `insider_footnote` (C13).

Every Form 4 / 4/A the feed (S22) or the daily index (S23) saw is queued in `pending_doc` (kind
`form4`). The queued CIK is the issuer when the feed saw the filing, but may be a reporting owner
for one only the index saw; the filing folder answers under any party's CIK, so it serves the URL,
and the issuer is always read from the XML. Per accession (the job's keys — the oldest open rows,
at most `max_docs_per_run` from `config/edgar.yaml` `form4:`, default `MAX_DOCS_DEFAULT`):

1. `https://www.sec.gov/Archives/edgar/data/<cik>/<accession without dashes>/index.json` (raw
   dataset `edgar_filing_index`): `{"directory": {"item": [{name, type, size, last-modified}],
   "name", "parent-dir"}}`; the folder holds `<acc>-index-headers.html`, `<acc>-index.html`,
   `<acc>.txt` and one ownership XML whose name varies (`form4.xml`, `ownership.xml`,
   `wk-form4_<n>.xml`, `primarydocument.xml`, …): the first `.xml` item (`pick_document`).
2. That XML (raw dataset `edgar_form4_xml`), an `<ownershipDocument>` (schema X0609 in 2026).

Element mapping (`parse_document`; a field is `<x><value>…</value><footnoteId id=…/></x>`, and a
value given only as a footnote is NULL with the id kept in the line's `footnotes`):
`documentType` → form (4 or 4/A, anything else refused); `periodOfReport`;
`dateOfOriginalSubmission` → original_filed; `issuer/issuerCik|issuerName|issuerTradingSymbol`;
each `reportingOwner` → `insider_owner` (`reportingOwnerId/rptOwnerCik|rptOwnerName`,
`reportingOwnerRelationship/isDirector|isOfficer|isTenPercentOwner|isOther` written `1`/`0` or
`true`/`false`, `officerTitle`, `otherText`); the children of `nonDerivativeTable` then
`derivativeTable` in document order → `insider_txn` lines from 1 (`*Transaction` or `*Holding`):
`securityTitle`, `transactionDate`, `transactionCoding/transactionCode`,
`transactionAmounts/transactionShares|transactionPricePerShare|transactionAcquiredDisposedCode`,
`postTransactionAmounts/sharesOwnedFollowingTransaction`,
`ownershipNature/directOrIndirectOwnership|natureOfOwnership`, and for derivatives
`conversionOrExercisePrice`, `exerciseDate`, `expirationDate`,
`underlyingSecurity/underlyingSecurityTitle|underlyingSecurityShares`; `footnotes/footnote[@id]`
→ `insider_footnote`. A line's `owner_cik` is the first reporting owner (a joint filing does not
split its lines between owners; all of them are in `insider_owner`).

A document's rows are compared with the stored ones of its accession; when anything differs, the
accession's rows are replaced in all four tables, so a re-run or reparse changes nothing (C2).
Rows counted = `insider_txn` lines inserted or changed. After an accession's document is stored
its `pending_doc` row is closed (`pending.close`).
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import yaml
from defusedxml import ElementTree

from usfin import jobs, raw
from usfin.settings import ConfigError
from usfin.sources.edgar import pending

BASE = "https://www.sec.gov/Archives/edgar/data/"
SOURCE = "edgar"
INDEX_DATASET = "edgar_filing_index"
XML_DATASET = "edgar_form4_xml"
KIND = "form4"
FORMS = ("4", "4/A")
MAX_DOCS_DEFAULT = 300
TABLES = ("insider_filing", "insider_owner", "insider_txn", "insider_footnote")


class Form4Error(Exception):
    """SEC answered something other than 200, or a body that is not a Form 4 folder/document."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Owner:
    seq: int
    cik: int
    name: str | None
    is_director: bool
    is_officer: bool
    is_ten_pct_owner: bool
    is_other: bool
    officer_title: str | None
    other_text: str | None


@dataclass(frozen=True)
class Line:
    line_no: int
    is_derivative: bool
    is_holding: bool
    security_title: str | None
    txn_date: date | None
    txn_code: str | None
    shares: float | None
    price: float | None
    acquired_disposed: str | None
    shares_owned_after: float | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    exercise_price: float | None
    exercise_date: date | None
    expiration_date: date | None
    underlying_title: str | None
    underlying_shares: float | None
    footnotes: dict[str, list[str]] = field(default_factory=dict)
    """Column → footnote ids attached to it in the XML."""


@dataclass(frozen=True)
class Document:
    form: str
    issuer_cik: int
    issuer_name: str | None
    issuer_ticker: str | None
    period_of_report: date | None
    original_filed: date | None
    owners: list[Owner]
    lines: list[Line]
    footnotes: dict[str, str]


# --- parsing -----------------------------------------------------------------------------------

LINE_FIELDS: dict[str, tuple[str, str]] = {
    # column: (path under the line element, type)
    "security_title": ("securityTitle", "text"),
    "txn_date": ("transactionDate", "date"),
    "txn_code": ("transactionCoding", "code"),
    "shares": ("transactionAmounts/transactionShares", "number"),
    "price": ("transactionAmounts/transactionPricePerShare", "number"),
    "acquired_disposed": ("transactionAmounts/transactionAcquiredDisposedCode", "text"),
    "shares_owned_after": ("postTransactionAmounts/sharesOwnedFollowingTransaction", "number"),
    "direct_indirect": ("ownershipNature/directOrIndirectOwnership", "text"),
    "nature_of_ownership": ("ownershipNature/natureOfOwnership", "text"),
    "exercise_price": ("conversionOrExercisePrice", "number"),
    "exercise_date": ("exerciseDate", "date"),
    "expiration_date": ("expirationDate", "date"),
    "underlying_title": ("underlyingSecurity/underlyingSecurityTitle", "text"),
    "underlying_shares": ("underlyingSecurity/underlyingSecurityShares", "number"),
}
TABLE_ELEMENTS = (("nonDerivativeTable", False), ("derivativeTable", True))


def _text(el) -> str | None:
    if el is None or el.text is None:
        return None
    return el.text.strip() or None


def _field(line, path: str, kind: str) -> tuple[Any, list[str]]:
    el = line.find(path)
    if el is None:
        return None, []
    notes = [f.get("id") for f in el.findall("footnoteId") if f.get("id")]
    if kind == "code":
        text = _text(el.find("transactionCode"))
    elif el.find("value") is not None:
        text = _text(el.find("value"))
    else:
        text = _text(el) if len(el) == 0 else None
    if text is None:
        return None, notes
    try:
        if kind == "number":
            return float(text.replace(",", "")), notes
        if kind == "date":
            return date.fromisoformat(text[:10]), notes
    except ValueError:
        raise Form4Error(f"{path}: {text!r} is not a {kind}") from None
    return text, notes


def _flag(el) -> bool:
    return (_text(el) or "").lower() in ("1", "true")


def _line(el, line_no: int, *, is_derivative: bool) -> Line:
    values: dict[str, Any] = {}
    notes: dict[str, list[str]] = {}
    for column, (path, kind) in LINE_FIELDS.items():
        values[column], ids = _field(el, path, kind)
        if ids:
            notes[column] = ids
    return Line(
        line_no=line_no,
        is_derivative=is_derivative,
        is_holding=el.tag.endswith("Holding"),
        footnotes=notes,
        **values,
    )


def _date(el) -> date | None:
    text = _text(el)
    if text is None:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        raise Form4Error(f"<{el.tag}> {text!r} is not a date") from None


def parse_document(body: bytes) -> Document:
    """One ownership XML (see the module doc for the element mapping)."""
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError as exc:
        raise Form4Error(f"not XML: {exc}") from exc
    if root.tag != "ownershipDocument":
        raise Form4Error(f"not an ownership document: root <{root.tag}>")
    form = _text(root.find("documentType"))
    if form not in FORMS:
        raise Form4Error(f"documentType {form!r} is not one of {FORMS}")
    issuer_cik = _text(root.find("issuer/issuerCik"))
    if issuer_cik is None or not issuer_cik.isdigit():
        raise Form4Error(f"issuerCik {issuer_cik!r} is not a CIK")
    owners = []
    for seq, el in enumerate(root.findall("reportingOwner"), start=1):
        cik = _text(el.find("reportingOwnerId/rptOwnerCik"))
        if cik is None or not cik.isdigit():
            raise Form4Error(f"reporting owner {seq}: rptOwnerCik {cik!r} is not a CIK")
        rel = "reportingOwnerRelationship/{}"
        owners.append(
            Owner(
                seq=seq,
                cik=int(cik),
                name=_text(el.find("reportingOwnerId/rptOwnerName")),
                is_director=_flag(el.find(rel.format("isDirector"))),
                is_officer=_flag(el.find(rel.format("isOfficer"))),
                is_ten_pct_owner=_flag(el.find(rel.format("isTenPercentOwner"))),
                is_other=_flag(el.find(rel.format("isOther"))),
                officer_title=_text(el.find(rel.format("officerTitle"))),
                other_text=_text(el.find(rel.format("otherText"))),
            )
        )
    if not owners:
        raise Form4Error("no reportingOwner")
    lines = []
    for table, is_derivative in TABLE_ELEMENTS:
        for el in root.findall(table + "/*"):
            lines.append(_line(el, len(lines) + 1, is_derivative=is_derivative))
    footnotes = {
        f.get("id"): " ".join((f.text or "").split()) or None
        for f in root.findall("footnotes/footnote")
        if f.get("id")
    }
    return Document(
        form=form,
        issuer_cik=int(issuer_cik),
        issuer_name=_text(root.find("issuer/issuerName")),
        issuer_ticker=_text(root.find("issuer/issuerTradingSymbol")),
        period_of_report=_date(root.find("periodOfReport")),
        original_filed=_date(root.find("dateOfOriginalSubmission")),
        owners=owners,
        lines=lines,
        footnotes=footnotes,
    )


def pick_document(body: bytes) -> str:
    """The ownership XML's name in a filing folder's `index.json` (its first `.xml` item)."""
    try:
        items = json.loads(body)["directory"]["item"]
    except (ValueError, KeyError, TypeError) as exc:
        raise Form4Error(f"not a filing-folder index.json: {exc!r}") from None
    for item in items:
        name = str(item.get("name", ""))
        if name.lower().endswith(".xml") and "/" not in name:
            return name
    raise Form4Error("no XML document in the filing folder")


# --- storage -----------------------------------------------------------------------------------


def _rows(acc: str, doc: Document, document: str) -> dict[str, list[list[Any]]]:
    first = doc.owners[0]
    return {
        "insider_filing": [
            [acc, doc.form, doc.issuer_cik, doc.issuer_name, doc.issuer_ticker,
             doc.period_of_report, doc.original_filed, document, len(doc.owners), len(doc.lines)]
        ],
        "insider_owner": [
            [acc, o.seq, o.cik, o.name, o.is_director, o.is_officer, o.is_ten_pct_owner,
             o.is_other, o.officer_title, o.other_text]
            for o in doc.owners
        ],
        "insider_txn": [
            [acc, ln.line_no, doc.issuer_cik, first.cik, first.name, ln.is_derivative,
             ln.is_holding, ln.security_title, ln.txn_date, ln.txn_code, ln.shares, ln.price,
             ln.acquired_disposed, ln.shares_owned_after, ln.direct_indirect,
             ln.nature_of_ownership, ln.exercise_price, ln.exercise_date, ln.expiration_date,
             ln.underlying_title, ln.underlying_shares,
             json.dumps(ln.footnotes, sort_keys=True) if ln.footnotes else None]
            for ln in doc.lines
        ],
        "insider_footnote": [[acc, k, v] for k, v in doc.footnotes.items()],
    }  # fmt: skip


def store_document(
    con: duckdb.DuckDBPyConnection, accession_no: str, doc: Document, *, document: str
) -> int:
    """Replace the accession's rows when the document differs from them; return `insider_txn`
    lines inserted or changed (0 when nothing differs)."""
    rows = _rows(accession_no, doc, document)
    differs = False
    for table in TABLES:
        stage = f"_stage_{table}"
        con.execute(f"CREATE OR REPLACE TEMP TABLE {stage} AS SELECT * FROM {table} LIMIT 0")
        if rows[table]:
            marks = ", ".join("?" for _ in rows[table][0])
            con.executemany(f"INSERT INTO {stage} VALUES ({marks})", rows[table])
        stored = f"(SELECT * FROM {table} WHERE accession_no = ?)"
        n = con.execute(
            f"SELECT count(*) FROM ((SELECT * FROM {stage} EXCEPT SELECT * FROM {stored})"
            f" UNION ALL (SELECT * FROM {stored} EXCEPT SELECT * FROM {stage}))",
            [accession_no, accession_no],
        ).fetchone()[0]
        differs = differs or n > 0
    changed = con.execute(
        "SELECT count(*) FROM (SELECT * FROM _stage_insider_txn EXCEPT"
        " SELECT * FROM insider_txn WHERE accession_no = ?)",
        [accession_no],
    ).fetchone()[0]
    if differs:
        for table in TABLES:
            con.execute(f"DELETE FROM {table} WHERE accession_no = ?", [accession_no])
            con.execute(f"INSERT INTO {table} SELECT * FROM _stage_{table}")
    for table in TABLES:
        con.execute(f"DROP TABLE _stage_{table}")
    return int(changed) if differs else 0


# --- config ------------------------------------------------------------------------------------


def load_max_docs(config_dir: Path | str) -> int:
    """`form4: max_docs_per_run` in `<config_dir>/edgar.yaml` (default `MAX_DOCS_DEFAULT`)."""
    path = Path(config_dir) / "edgar.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a mapping")
    section = data.get("form4") or {}
    if not isinstance(section, dict):
        raise ConfigError(f"{path}: form4 must be a mapping")
    value = section.get("max_docs_per_run", MAX_DOCS_DEFAULT)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigError(f"{path}: form4.max_docs_per_run {value!r} is not a count >= 0")
    return value


# --- job ---------------------------------------------------------------------------------------


def folder_url(cik: int, accession_no: str) -> str:
    return f"{BASE}{int(cik)}/{accession_no.replace('-', '')}/"


def _accession(url: str) -> str:
    nodash = url.rstrip("/").split("/")[-2]
    if len(nodash) != 18 or not nodash.isdigit():
        raise Form4Error(f"{url} is not a filing-folder URL")
    return f"{nodash[:10]}-{nodash[10:12]}-{nodash[12:]}"


def queued(ctx: "jobs.JobContext") -> list[str]:
    """The oldest open `form4` accessions, at most `max_docs_per_run`."""
    rows = pending.open_accessions(ctx.con, KIND, limit=load_max_docs(ctx.settings.config_dir))
    ctx.memo["cik"] = dict(rows)
    return [acc for acc, _ in rows]


def _get(ctx: "jobs.JobContext", url: str, dataset: str):
    raw_id, response = ctx.client.get(url, source=SOURCE, dataset=dataset)
    if response.status_code != 200:
        raise Form4Error(f"{url} answered {response.status_code}", raw_id=raw_id)
    return raw_id, response


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    folder = folder_url(ctx.memo["cik"][key], key)
    index_id, response = _get(ctx, folder + "index.json", INDEX_DATASET)
    name = pick_document(response.content)
    xml_id, _ = _get(ctx, folder + name, XML_DATASET)
    return [index_id, xml_id]


def _parse_index(ctx: "jobs.JobContext", raw_id: int) -> int:
    """The folder listing only names the XML (used during fetch); nothing to store."""
    return 0


def _parse_xml(ctx: "jobs.JobContext", raw_id: int) -> int:
    found = ctx.con.execute(
        "SELECT url, http_status FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    url, status = found
    if status != 200:
        return 0
    with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
        doc = parse_document(fh.read())
    return store_document(ctx.con, _accession(url), doc, document=url.rsplit("/", 1)[-1])


def _done(ctx: "jobs.JobContext", key: str) -> None:
    pending.close(ctx.con, KIND, key, now=jobs._utc_naive(ctx.now))


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="edgar_form4",
        keys=queued,
        fetch=_fetch,
        parsers={INDEX_DATASET: _parse_index, XML_DATASET: _parse_xml},
        after_key=_done,
    )
)
"""Drains `pending_doc` kind `form4`; chained after `edgar_feed` and `edgar_reconcile`."""
