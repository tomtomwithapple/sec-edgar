"""TreasuryDirect securities: job `treasurydirect` → table `tsy_security`.

Base `https://www.treasurydirect.gov/TA_WS/securities/`. Two watermark keys:

- `announced`: `announced?format=json` (raw dataset `td_announced`) — the latest 250 announced
  securities, already-auctioned ones included; watermark = newest auction date listed (capped at
  today).
- `results`: `auctioned?format=json&days=7` (`td_auctioned`) while the watermark — the newest
  auction date stored *with results* — is within 7 days of today (or unset; history is the
  backfill's job), otherwise `search?startDate=<wm-7d>&endDate=<today>&dateFieldName=auctionDate
  &format=json` (`td_search`). The parameter names must be camelCase: lowercase ones are ignored
  and the whole history (37 MB) comes back.

Every response is a JSON list of records, each with the same 120 string-valued keys across the
whole history (1979 to now, checked 2026-10-08): dates read `YYYY-MM-DDT00:00:00`, a missing
value is `""`. `parse_record` maps the keys other jobs use to typed columns (`""` → NULL) and
keeps the whole record in `payload`. A security counts as `auctioned` once any of its high yield,
discount rate, investment rate or (FRNs) discount margin is set. Upserts never lower `status` and
never overwrite a stored value with a blank one, typed or in `payload`, since a feed may list an
auctioned security as it stood before its auction. A key outside `KNOWN_FIELDS` that no stored
payload has yet raises `schema_drift` (subject `tsy_security`); it is kept in `payload`.
"""

import json
import logging
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from usfin import alerts, jobs, raw

BASE = "https://www.treasurydirect.gov/TA_WS/securities"
SOURCE = "treasurydirect"
TABLE = "tsy_security"
WINDOW_DAYS = 7
DATASET_KEY = {"td_announced": "announced", "td_auctioned": "results", "td_search": "results"}
log = logging.getLogger("usfin.treasurydirect")

# column -> (upstream key, kind); kinds: str, date, amount (DECIMAL(24,4)), rate (DOUBLE)
FIELD_MAP: dict[str, tuple[str, str]] = {
    "cusip": ("cusip", "str"),
    "auction_date": ("auctionDate", "date"),
    "security_type": ("securityType", "str"),
    "security_term": ("securityTerm", "str"),
    "offering_amt": ("offeringAmount", "amount"),
    "high_yield": ("highYield", "rate"),
    "high_discount_rate": ("highDiscountRate", "rate"),
    "high_investment_rate": ("highInvestmentRate", "rate"),
    "high_discount_margin": ("highDiscountMargin", "rate"),
    "price_per100": ("pricePer100", "rate"),
    "bid_to_cover": ("bidToCoverRatio", "rate"),
    "primary_dealer_accepted": ("primaryDealerAccepted", "amount"),
    "indirect_accepted": ("indirectBidderAccepted", "amount"),
    "direct_accepted": ("directBidderAccepted", "amount"),
    "issue_date": ("issueDate", "date"),
    "maturity_date": ("maturityDate", "date"),
}
RESULT_COLUMNS = ("high_yield", "high_discount_rate", "high_investment_rate", "high_discount_margin")
TYPED = tuple(FIELD_MAP)
COLUMNS = (*TYPED, "status", "payload")
PK = ("cusip", "auction_date")
_SQL = {"str": "VARCHAR", "date": "DATE", "amount": "DECIMAL(24,4)", "rate": "DOUBLE"}

_KNOWN_FIELDS = """
    cusip issueDate securityType securityTerm maturityDate interestRate refCpiOnIssueDate
    refCpiOnDatedDate announcementDate auctionDate auctionDateYear datedDate
    accruedInterestPer1000 accruedInterestPer100 adjustedAccruedInterestPer1000 adjustedPrice
    allocationPercentage allocationPercentageDecimals announcedCusip auctionFormat
    averageMedianDiscountRate averageMedianInvestmentRate averageMedianPrice
    averageMedianDiscountMargin averageMedianYield backDated backDatedDate bidToCoverRatio
    callDate callable calledDate cashManagementBillCMB closingTimeCompetitive
    closingTimeNoncompetitive competitiveAccepted competitiveBidDecimals competitiveTendered
    competitiveTendersAccepted corpusCusip cpiBaseReferencePeriod currentlyOutstanding
    directBidderAccepted directBidderTendered
    estimatedAmountOfPubliclyHeldMaturingSecuritiesByType fimaIncluded
    fimaNoncompetitiveAccepted fimaNoncompetitiveTendered firstInterestPeriod
    firstInterestPaymentDate floatingRate frnIndexDeterminationDate frnIndexDeterminationRate
    highDiscountRate highInvestmentRate highPrice highDiscountMargin highYield
    indexRatioOnIssueDate indirectBidderAccepted indirectBidderTendered interestPaymentFrequency
    lowDiscountRate lowInvestmentRate lowPrice lowDiscountMargin lowYield maturingDate
    maximumCompetitiveAward maximumNoncompetitiveAward maximumSingleBid minimumBidAmount
    minimumStripAmount minimumToIssue multiplesToBid multiplesToIssue nlpExclusionAmount
    nlpReportingThreshold noncompetitiveAccepted noncompetitiveTendersAccepted offeringAmount
    originalCusip originalDatedDate originalIssueDate originalSecurityTerm
    pdfFilenameAnnouncement pdfFilenameCompetitiveResults pdfFilenameNoncompetitiveResults
    pdfFilenameSpecialAnnouncement pricePer100 primaryDealerAccepted primaryDealerTendered
    reopening securityTermDayMonth securityTermWeekYear series somaAccepted somaHoldings
    somaIncluded somaTendered spread standardInterestPaymentPer1000 strippable term
    tiinConversionFactorPer1000 tips totalAccepted totalTendered treasuryRetailAccepted
    treasuryRetailTendersAccepted type unadjustedAccruedInterestPer1000 unadjustedPrice
    updatedTimestamp xmlFilenameAnnouncement xmlFilenameCompetitiveResults
    xmlFilenameSpecialAnnouncement tintCusip1 tintCusip2 tintCusip1DueDate tintCusip2DueDate
"""
KNOWN_FIELDS = frozenset(_KNOWN_FIELDS.split())
"""The 120 keys every record carried from 1979-10-31 to 2026-10-08."""


class TreasuryDirectError(Exception):
    """TreasuryDirect answered something other than a 200 JSON list (archived as `raw_id`)."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


# --- requests ----------------------------------------------------------------------------------


def announced_url() -> str:
    return f"{BASE}/announced?format=json"


def auctioned_url(days: int = WINDOW_DAYS) -> str:
    return f"{BASE}/auctioned?format=json&days={days}"


def search_url(start: date, end: date) -> str:
    """Auctions dated `start`..`end`; camelCase names, or upstream returns the whole history."""
    return (
        f"{BASE}/search?startDate={start.isoformat()}&endDate={end.isoformat()}"
        "&dateFieldName=auctionDate&format=json"
    )


def results_url(watermark: date | str | None, today: date) -> str:
    """`auctioned` for the last 7 days when the watermark is unset or within them, else a
    `search` from 7 days before the watermark to today."""
    if watermark is None:
        return auctioned_url()
    wm = watermark if isinstance(watermark, date) else date.fromisoformat(str(watermark)[:10])
    wm = min(wm, today)
    if (today - wm).days <= WINDOW_DAYS:
        return auctioned_url()
    return search_url(wm - timedelta(days=WINDOW_DAYS), today)


# --- parsing -----------------------------------------------------------------------------------


def _cast(value: Any, kind: str) -> Any:
    if value is None or value == "":
        return None
    if kind == "date":
        return date.fromisoformat(str(value)[:10])  # "2026-10-08T00:00:00"
    if kind == "amount":
        return Decimal(str(value))
    if kind == "rate":
        return float(value)
    return str(value)


def _status(row: Mapping[str, Any]) -> str:
    return "auctioned" if any(row.get(c) is not None for c in RESULT_COLUMNS) else "announced"


def parse_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """One upstream record → typed columns, `status` and `payload` (the record as JSON text)."""
    row = {col: _cast(record.get(key), kind) for col, (key, kind) in FIELD_MAP.items()}
    if row["cusip"] is None or row["auction_date"] is None:
        raise ValueError(f"record without cusip/auctionDate: {dict(record)!r:.200}")
    row["status"] = _status(row)
    row["payload"] = json.dumps(record)
    return row


def merge(old: Mapping[str, Any] | None, new: Mapping[str, Any]) -> dict[str, Any]:
    """`new` over `old`, except that a blank never replaces a stored value (typed or payload)
    and so `status` never goes back to `announced`."""
    if old is None:
        return dict(new)
    out = {c: new[c] if new[c] is not None else old[c] for c in TYPED}
    payload = json.loads(old["payload"])
    for key, value in json.loads(new["payload"]).items():
        if value != "" or key not in payload:
            payload[key] = value
    out["status"] = _status(out)
    out["payload"] = json.dumps(payload)
    return out


def _same(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    return all(a[c] == b[c] for c in (*TYPED, "status")) and json.loads(
        a["payload"]
    ) == json.loads(b["payload"])


# --- storing -----------------------------------------------------------------------------------


def _new_fields(con, records: Sequence[Mapping[str, Any]]) -> list[str]:
    """Keys outside KNOWN_FIELDS that no stored payload carries yet."""
    unknown = sorted({k for r in records for k in r} - KNOWN_FIELDS)
    path = '$."{}"'
    return [
        key
        for key in unknown
        if not con.execute(
            f"SELECT count(*) FROM {TABLE} WHERE json_exists(payload, ?)",
            [path.format(key.replace('"', '\\"'))],
        ).fetchone()[0]
    ]


def _existing(con, rows: Sequence[Mapping[str, Any]]) -> dict[tuple, dict[str, Any]]:
    keys = json.dumps([[r["cusip"], r["auction_date"].isoformat()] for r in rows])
    cur = con.execute(
        f"SELECT t.* FROM {TABLE} t JOIN (SELECT unnest(json_transform(?, '[[\"VARCHAR\"]]'))"
        " AS k) ON t.cusip = k[1] AND t.auction_date = k[2]::DATE",
        [keys],
    )
    cols = [d[0] for d in cur.description]
    out = {}
    for values in cur.fetchall():
        row = dict(zip(cols, values, strict=True))
        out[(row["cusip"], row["auction_date"])] = row
    return out


def upsert(ctx: "jobs.JobContext", records: Sequence[Mapping[str, Any]]) -> int:
    """Store records in `tsy_security`; return how many rows were new or changed."""
    con = ctx.con
    fields = _new_fields(con, records)
    if fields:
        log.warning("schema_drift: TreasuryDirect records gained keys %s", fields)
        alerts.raise_alert(
            con,
            job=ctx.job.name,
            rule="schema_drift",
            subject=TABLE,
            detail={"table": TABLE, "fields": fields},
            now=ctx.now,
        )
    incoming: dict[tuple, dict[str, Any]] = {}
    for record in records:
        row = parse_record(record)
        pk = (row["cusip"], row["auction_date"])
        incoming[pk] = merge(incoming.get(pk), row)
    if not incoming:
        return 0
    stored = _existing(con, list(incoming.values()))
    changed = []
    for pk, row in incoming.items():
        merged = merge(stored.get(pk), row)
        if pk not in stored or not _same(merged, stored[pk]):
            changed.append(merged)
    if not changed:
        return 0
    # one JSON text parameter, not bound row values (see fiscal.upsert_page)
    text = json.dumps([[r[c] for c in COLUMNS] for r in changed], default=str)
    types = [_SQL[kind] for _, kind in FIELD_MAP.values()] + ["VARCHAR", "JSON"]
    casts = ", ".join(f"r[{i}]::{t}" for i, t in enumerate(types, 1))
    updates = ", ".join(f"{c} = excluded.{c}" for c in COLUMNS if c not in PK)
    con.execute(
        f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) SELECT {casts}"
        """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)"""
        f" ON CONFLICT ({', '.join(PK)}) DO UPDATE SET {updates}",
        [text],
    )
    return len(changed)


# --- job `treasurydirect` ------------------------------------------------------------------------


def _get(ctx: "jobs.JobContext", url: str, dataset: str) -> int:
    raw_id, response = ctx.client.get(url, source=SOURCE, dataset=dataset)
    if response.status_code != 200:
        raise TreasuryDirectError(
            f"{url} answered {response.status_code}: {response.text[:200]}", raw_id=raw_id
        )
    return raw_id


def _fetch(ctx: "jobs.JobContext", key: str) -> list[int]:
    if key == "announced":
        return [_get(ctx, announced_url(), "td_announced")]
    url = results_url(ctx.watermarks.get("results"), ctx.today)
    return [_get(ctx, url, "td_search" if "/search?" in url else "td_auctioned")]


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    dataset = ctx.con.execute(
        "SELECT dataset FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()[0]
    with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
        records = json.loads(fh.read())
    if not isinstance(records, list):
        raise TreasuryDirectError(
            f"raw_response {raw_id}: expected a JSON list, got {type(records).__name__}",
            raw_id=raw_id,
        )
    rows = upsert(ctx, records)
    key = DATASET_KEY[dataset]
    parsed = [parse_record(r) for r in records]
    if key == "results":
        parsed = [r for r in parsed if r["status"] == "auctioned"]
    dates = [r["auction_date"] for r in parsed]
    current = ctx.watermarks.get(key)
    if current:
        dates.append(date.fromisoformat(current[:10]))
    if dates:
        ctx.advance(key, max(dates))
    return rows


_OBSERVED = {
    "announced": alerts.Observed(table=TABLE, date_column="auction_date"),
    "results": alerts.Observed(
        table=TABLE, date_column="auction_date", match={"status": ("auctioned",)}
    ),
}

JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="treasurydirect",
        keys=("announced", "results"),
        fetch=_fetch,
        parsers={dataset: _parse for dataset in DATASET_KEY},
        observed=_OBSERVED,
    )
)
"""Announced, then auction results; S14 compares `tsy_security` with `fd_auctions`."""

