"""Job `auction_xcheck`: Fiscal Data `fd_auctions` vs TreasuryDirect `tsy_security` (C15).

A derived job (no source, no raw responses): chained after `fiscal`, it joins the two tables on
(cusip, auction_date) and compares the offering amount and the four auction results. Rules, set
from the full history in data/usfin.duckdb on 2026-10-08 (7 894 Fiscal Data auctions since
2000-01-03, every one also in TreasuryDirect, all agreeing):

- The key is (cusip, auction_date): a reopening shares its CUSIP with the original issue, so the
  CUSIP alone would pair a reopening with the wrong auction (2 214 CUSIPs were auctioned more
  than once).
- Like is compared with like, so no unit conversion is needed: both store rates in percent and
  amounts in dollars. A bill has a discount rate and an investment rate and no yield; an FRN has
  only a discount margin (Fiscal Data `high_discnt_margin`); notes, bonds and TIPS a yield. Each of
  `offering_amt`, `high_yield`, `high_discount_rate`, `high_investment_rate`,
  `high_discount_margin` is compared on its own.
- Amounts compare exactly (both DECIMAL). Rates are written at different precision upstream
  (TreasuryDirect "5.3000", Fiscal Data "5.300000"), so they compare rounded to 6 decimals —
  a real difference is at least 0.001 (results are published to 3 decimals).
- A field set on one side only is not a disagreement but `pending` (Fiscal Data may publish a
  result after TreasuryDirect has it); it is compared once both have it.
- Rows on one side only are counted (`fd_only`, `td_only`), never alerted, within the overlap of
  the two tables (auction dates from the later of their earliest dates: TreasuryDirect reaches back
  to 1979, the Fiscal Data registry to 2000).

Each pair with a disagreement raises one `auction_mismatch` alert per New York day (subject
`<cusip>@<auction_date>`, detail: the pair and `fields: {field: {fiscal, treasurydirect}}`); a
disagreement that persists raises again the next day. Every compared pair lands in table
`auction_xcheck` with its result; rows change only when a result or diff changes, so the run's
row count is the number of pairs whose verdict changed.
"""

import json
import logging
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from typing import Any

from usfin import alerts, jobs, store

TABLE = "auction_xcheck"
FD_TABLE = "fd_auctions"
TD_TABLE = "tsy_security"
FIELDS = {  # tsy_security column -> fd_auctions column
    "offering_amt": "offering_amt",
    "high_yield": "high_yield",
    "high_discount_rate": "high_discnt_rate",
    "high_investment_rate": "high_investment_rate",
    "high_discount_margin": "high_discnt_margin",
}
RATE_DECIMALS = 6
log = logging.getLogger("usfin.auction_xcheck")


def _norm(value: Any) -> Any:
    if value is None or isinstance(value, Decimal):
        return value
    return round(float(value), RATE_DECIMALS)


def _plain(value: Any) -> Any:
    return str(value) if isinstance(value, Decimal) else value


def compare_pair(td: Mapping[str, Any], fd: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Fields set on both sides that disagree: `{field: {fiscal, treasurydirect}}`.

    Both mappings are keyed by `tsy_security` column names."""
    out = {}
    for name in FIELDS:
        a, b = td.get(name), fd.get(name)
        if a is None or b is None:
            continue
        if _norm(a) != _norm(b):
            out[name] = {"fiscal": b, "treasurydirect": a}
    return out


def one_sided(td: Mapping[str, Any], fd: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Fields set on exactly one side."""
    return {
        name: {"fiscal": fd.get(name), "treasurydirect": td.get(name)}
        for name in FIELDS
        if (td.get(name) is None) != (fd.get(name) is None)
    }


def mismatch_detail(pair: Mapping[str, Any], fields: Mapping[str, Mapping[str, Any]]) -> dict:
    return {
        "cusip": pair["cusip"],
        "auction_date": pair["auction_date"].isoformat(),
        "security_type": pair.get("security_type"),
        "security_term": pair.get("security_term"),
        "fields": {f: {k: _plain(v) for k, v in sides.items()} for f, sides in fields.items()},
    }


def _pairs(con) -> list[dict[str, Any]]:
    """Full outer join of the two tables over their overlapping auction dates."""
    td_cols = ", ".join(f"t.{c} AS td_{c}" for c in FIELDS)
    fd_cols = ", ".join(f"f.{f} AS fd_{c}" for c, f in FIELDS.items())
    cur = con.execute(
        f"""
        WITH lo AS (
            SELECT greatest((SELECT min(auction_date) FROM {FD_TABLE}),
                            (SELECT min(auction_date) FROM {TD_TABLE})) AS d
        )
        SELECT coalesce(t.cusip, f.cusip) AS cusip,
               coalesce(t.auction_date, f.auction_date) AS auction_date,
               coalesce(t.security_type, f.security_type) AS security_type,
               coalesce(t.security_term, f.security_term) AS security_term,
               t.cusip IS NOT NULL AS in_td, f.cusip IS NOT NULL AS in_fd,
               {td_cols}, {fd_cols}
        FROM (SELECT * FROM {TD_TABLE} WHERE auction_date >= (SELECT d FROM lo)) t
        FULL OUTER JOIN (SELECT * FROM {FD_TABLE} WHERE auction_date >= (SELECT d FROM lo)) f
          ON t.cusip = f.cusip AND t.auction_date = f.auction_date
        ORDER BY auction_date, cusip
        """
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def _verdict(pair: Mapping[str, Any]) -> tuple[str, dict | None]:
    if not pair["in_fd"]:
        return "td_only", None
    if not pair["in_td"]:
        return "fd_only", None
    td = {c: pair[f"td_{c}"] for c in FIELDS}
    fd = {c: pair[f"fd_{c}"] for c in FIELDS}
    fields = compare_pair(td, fd)
    if fields:
        return "mismatch", mismatch_detail(pair, fields)["fields"]
    sided = one_sided(td, fd)
    if sided:
        return "pending", mismatch_detail(pair, sided)["fields"]
    return "matched", None


def _store(con, verdicts: dict[tuple, tuple[str, str | None, dict | None]]) -> int:
    """Make `auction_xcheck` equal `verdicts`; return how many rows were added, changed or
    removed."""
    stored = {
        (cusip, day): (result, typ, json.loads(diff) if diff else None)
        for cusip, day, result, typ, diff in con.execute(
            f"SELECT cusip, auction_date, result, security_type, diff FROM {TABLE}"
        ).fetchall()
    }
    changed = [(pk, v) for pk, v in verdicts.items() if stored.get(pk) != v]
    gone = [pk for pk in stored if pk not in verdicts]
    if gone:
        con.execute(
            f"DELETE FROM {TABLE} t USING (SELECT unnest(json_transform(?, '[[\"VARCHAR\"]]'))"
            " AS k) WHERE t.cusip = k[1] AND t.auction_date = k[2]::DATE",
            [json.dumps([[c, d.isoformat()] for c, d in gone])],
        )
    if changed:
        # one JSON text parameter, not bound row values (see fiscal.upsert_page)
        text = json.dumps(
            [
                [c, d.isoformat(), r, t, None if diff is None else json.dumps(diff)]
                for (c, d), (r, t, diff) in changed
            ]
        )
        con.execute(
            f"INSERT INTO {TABLE} SELECT r[1], r[2]::DATE, r[3], r[4], r[5]::JSON"
            """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)"""
            " ON CONFLICT (cusip, auction_date) DO UPDATE SET result = excluded.result,"
            " security_type = excluded.security_type, diff = excluded.diff",
            [text],
        )
    return len(changed) + len(gone)


def check(ctx: "jobs.JobContext", key: str) -> int:
    con = ctx.con
    if FD_TABLE not in store.table_names(con):
        log.info("auction_xcheck: no %s table yet, nothing compared", FD_TABLE)
        return 0
    verdicts: dict[tuple[str, date], tuple[str, str | None, dict | None]] = {}
    counts: dict[str, int] = {}
    for pair in _pairs(con):
        result, diff = _verdict(pair)
        verdicts[(pair["cusip"], pair["auction_date"])] = (result, pair["security_type"], diff)
        counts[result] = counts.get(result, 0) + 1
        if result == "mismatch":
            alerts.raise_alert(
                con,
                job=ctx.job.name,
                rule="auction_mismatch",
                subject=f"{pair['cusip']}@{pair['auction_date'].isoformat()}",
                detail=mismatch_detail(pair, diff),
                now=ctx.now,
            )
    log.info("auction_xcheck: %s", ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    return _store(con, verdicts)


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="auction_xcheck",
        keys=("auctions",),
        fetch=lambda ctx, key: [],
        parsers={},
        compute=check,
    )
)
"""Derived: compares `fd_auctions` with `tsy_security`; chained after `fiscal`."""
