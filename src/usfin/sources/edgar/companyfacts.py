"""Job `edgar_companyfacts`: a company's XBRL facts from data.sec.gov into `xbrl_fact`.

`https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json` is `{cik, entityName, facts:
{taxonomy: {tag: {label, description, units: {unit: [fact, …]}}}}}`, every fact
`{start?, end, val, accn, fy, fp, form, filed, frame?}`: `start` is absent for an instant, `fy`/`fp`
are null for facts from forms with no fiscal period (DEF 14A, 424B2), `frame` sits on one filing
per period (SEC moves it to the latest). Apple's document is 3.8 MB (dei + us-gaap, 25 135 facts),
JPMorgan's 8.0 MB (also invest, srt, ffd, ecd; 49 946 us-gaap + dei facts) on 2026-10-08.

Only `us-gaap` and `dei` are kept (plan: What › out of scope). Each fact becomes one row keyed
(cik, taxonomy, tag, unit, accn, period_start, period_end); a restatement is a new `accn`, hence a
new row, so `xbrl_as_of(d)` (facts with `filed <= d`) answers what was public on a date (C4). A
known fact is updated only when a column changed (in practice `frame`), so a re-run reports 0
rows (C2); a fact missing from a later document is kept.

Every response is archived first (raw dataset `edgar_companyfacts`, source `edgar`). Watermark
key = the 10-digit CIK, value = its newest `filed`. The registered job drains the CIKs of open
`pending_doc` rows of kind `companyfacts` (that table arrives with the feed, S22); `usfin edgar
facts --cik` runs it for given CIKs (`for_ciks`). `store_document(con, doc, name=)` takes the
document name (`CIK##########.json`, as served and as companyfacts.zip members are named), so S26's
bulk load reuses it as is.
"""

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

import duckdb

from usfin import jobs, raw, store

BASE = "https://data.sec.gov/api/xbrl/companyfacts/"
SOURCE = "edgar"
RAW_DATASET = "edgar_companyfacts"
KIND = "companyfacts"
TABLE = "xbrl_fact"
TAXONOMIES = ("us-gaap", "dei")
NAME = re.compile(r"CIK(\d{10})\.json")
KEY = ("cik", "taxonomy", "tag", "unit", "accn", "period_start", "period_end")
VALUES = ("is_instant", "value", "fy", "fp", "form", "filed", "frame")


class CompanyFactsError(Exception):
    """SEC answered something other than 200, or a body that is not a companyfacts document."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


def cik_key(cik: int) -> str:
    return f"{int(cik):010d}"


def url(cik: int) -> str:
    return f"{BASE}CIK{cik_key(cik)}.json"


@dataclass(frozen=True)
class Fact:
    cik: int
    taxonomy: str
    tag: str
    unit: str
    accn: str
    period_start: date
    period_end: date
    is_instant: bool
    value: float
    fy: int | None
    fp: str | None
    form: str
    filed: date


@dataclass(frozen=True)
class _Row:
    fact: Fact
    frame: str | None


def _fact_rows(doc: Mapping[str, Any], *, cik: int) -> list[_Row]:
    facts = doc.get("facts")
    if not isinstance(facts, Mapping):
        raise CompanyFactsError(f"CIK {cik}: `facts` is not an object")
    out = []
    for taxonomy in TAXONOMIES:
        for tag, concept in (facts.get(taxonomy) or {}).items():
            units = concept.get("units") if isinstance(concept, Mapping) else None
            if not isinstance(units, Mapping):
                raise CompanyFactsError(f"{taxonomy}:{tag} has no `units` object")
            for unit, items in units.items():
                for f in items:
                    missing = [k for k in ("end", "val", "accn", "form", "filed") if f.get(k) is None]
                    if missing:
                        raise CompanyFactsError(f"{taxonomy}:{tag} {unit} fact lacks {missing}")
                    end = date.fromisoformat(f["end"])
                    start = f.get("start")
                    fy = f.get("fy")
                    out.append(
                        _Row(
                            Fact(
                                cik=cik, taxonomy=taxonomy, tag=tag, unit=unit, accn=f["accn"],
                                period_start=date.fromisoformat(start) if start else end,
                                period_end=end, is_instant=not start, value=float(f["val"]),
                                fy=None if fy is None else int(fy), fp=f.get("fp") or None,
                                form=f["form"], filed=date.fromisoformat(f["filed"]),
                            ),
                            f.get("frame") or None,
                        )
                    )  # fmt: skip
    return out


def facts_from_document(doc: Mapping[str, Any], *, cik: int) -> list[Fact]:
    """The us-gaap and dei facts of one companyfacts document."""
    return [r.fact for r in _fact_rows(doc, cik=cik)]


@dataclass(frozen=True)
class Stored:
    cik: int
    facts: int
    """`xbrl_fact` rows inserted or changed."""
    newest: date | None
    """The newest `filed` among the document's kept facts."""


def store_document(
    con: duckdb.DuckDBPyConnection, doc: Any, *, name: str, raw_id: int | None = None
) -> Stored:
    """Store one companyfacts document named `name` (`CIK##########.json`)."""
    m = NAME.fullmatch(name)
    if m is None:
        raise CompanyFactsError(f"{name!r} is not a companyfacts document name")
    cik = int(m.group(1))
    if not isinstance(doc, Mapping):
        raise CompanyFactsError(f"{name}: expected an object, got {type(doc).__name__}")
    if str(doc.get("cik", "")).lstrip("0") != str(cik):
        raise CompanyFactsError(f"{name}: cik is {doc.get('cik')!r}")
    rows = _fact_rows(doc, cik=cik)
    newest = max((r.fact.filed for r in rows), default=None)
    return Stored(cik, upsert_facts(con, rows), newest)


def upsert_facts(con: duckdb.DuckDBPyConnection, rows: Sequence[_Row]) -> int:
    """Insert new facts and update changed ones; return how many rows were inserted or changed."""
    if not rows:
        return 0
    payload = json.dumps(
        [
            [f.cik, f.taxonomy, f.tag, f.unit, f.accn, f.period_start.isoformat(),
             f.period_end.isoformat(), f.is_instant, repr(f.value), f.fy, f.fp, f.form,
             f.filed.isoformat(), r.frame]
            for r in rows
            for f in (r.fact,)
        ]
    )  # fmt: skip
    # one JSON text parameter, not bound row values (see fiscal.upsert_page)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _fact_stage AS SELECT r[1]::BIGINT AS cik,"
        " r[2] AS taxonomy, r[3] AS tag, r[4] AS unit, r[5] AS accn, r[6]::DATE AS period_start,"
        " r[7]::DATE AS period_end, r[8]::BOOLEAN AS is_instant, r[9]::DOUBLE AS value,"
        " r[10]::SMALLINT AS fy, r[11] AS fp, r[12] AS form, r[13]::DATE AS filed, r[14] AS frame"
        """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)""",
        [payload],
    )
    on = " AND ".join(f"s.{c} = t.{c}" for c in KEY)
    differs = " OR ".join(f"s.{c} IS DISTINCT FROM t.{c}" for c in VALUES)
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _fact_changed AS SELECT s.* FROM _fact_stage s"
        f" LEFT JOIN {TABLE} t ON {on} WHERE t.cik IS NULL OR {differs}"
    )
    changed = con.execute("SELECT count(*) FROM _fact_changed").fetchone()[0]
    if changed:
        sets = ", ".join(f"{c} = excluded.{c}" for c in VALUES)
        con.execute(
            f"INSERT INTO {TABLE} SELECT * FROM _fact_changed"
            f" ON CONFLICT ({', '.join(KEY)}) DO UPDATE SET {sets}"
        )
    con.execute("DROP TABLE _fact_stage")
    con.execute("DROP TABLE _fact_changed")
    return int(changed)


# --- job -------------------------------------------------------------------------------------


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    raw_id, response = ctx.client.get(url(int(key)), source=SOURCE, dataset=RAW_DATASET)
    if response.status_code != 200:
        raise CompanyFactsError(f"{url(int(key))} answered {response.status_code}", raw_id=raw_id)
    return [raw_id]


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    found = ctx.con.execute(
        "SELECT url, http_status FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    doc_url, status = found
    if status != 200:
        return 0
    with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
        doc = json.loads(fh.read())
    stored = store_document(ctx.con, doc, name=doc_url.rsplit("/", 1)[-1], raw_id=raw_id)
    if stored.newest is not None:
        ctx.advance(cik_key(stored.cik), stored.newest)
    return stored.facts


def pending_ciks(ctx: "jobs.JobContext") -> list[str]:
    """CIKs with open `pending_doc` rows of kind `companyfacts` (none before S22 adds the table)."""
    if "pending_doc" not in store.table_names(ctx.con):
        return []
    rows = ctx.con.execute(
        "SELECT DISTINCT cik FROM pending_doc WHERE kind = ? AND done_at IS NULL ORDER BY cik",
        [KIND],
    ).fetchall()
    return [cik_key(r[0]) for r in rows]


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="edgar_companyfacts",
        keys=pending_ciks,
        fetch=_fetch,
        parsers={RAW_DATASET: _parse},
    )
)
"""Per-CIK XBRL facts into `xbrl_fact`; chained after the feed (S22), no schedule of its own."""


def for_ciks(ciks: Sequence[int]) -> jobs.Job:
    """`JOB` with fixed keys: the given CIKs instead of the pending queue."""
    return replace(JOB, keys=tuple(dict.fromkeys(cik_key(c) for c in ciks)))
