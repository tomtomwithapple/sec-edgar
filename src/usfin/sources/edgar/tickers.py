"""Job `edgar_tickers`: SEC's ticker↔CIK map into table `edgar_company`.

`URL` (fetched 2026-10-08: 798 727 bytes, `application/json`, 10 434 entries for 8 016 CIKs) is
one object keyed "0".."n-1" in SEC's own order, each value `{cik_str: int, ticker, title}`. No
ticker repeats across CIKs; a CIK can list many (Alphabet GOOGL/GOOG, one fund 45). Tickers are
uppercase with `-` before a class or suffix letter (BRK-B, ABR-PD, KCAC-UN, SLND-WT, OCAC-RI);
one carries a dot (`NONE.`).

Every response is archived first (raw dataset `edgar_tickers`, source `edgar`) through
`usfin.http.Client`, which refuses without `SEC_USER_AGENT` and honours the SEC block window.
The file is authoritative: each pair in it is stored active (inserted, or updated when its title
or `ticker_norm` changed or it was inactive); each stored active pair the file lacks becomes
inactive — never deleted. `updated_at` is the response's `fetched_at`, set only on such a change,
so a refetch changes nothing (C2) and `usfin reparse edgar_tickers` rebuilds the same table.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import duckdb

from usfin import jobs, raw
from usfin.ticker import ticker_norm

URL = "https://www.sec.gov/files/company_tickers.json"
SOURCE = "edgar"
RAW_DATASET = "edgar_tickers"
TABLE = "edgar_company"


class TickersError(Exception):
    """SEC answered something other than 200, or a body that is not the ticker map."""

    def __init__(self, message: str, *, raw_id: int | None = None):
        super().__init__(message)
        self.raw_id = raw_id


@dataclass(frozen=True)
class Company:
    cik: int
    ticker: str
    title: str
    ticker_norm: str


def parse(doc: Any) -> dict[tuple[int, str], Company]:
    """The file's pairs by (cik, ticker); the first entry wins if a pair repeats."""
    if not isinstance(doc, Mapping) or not doc:
        raise TickersError(f"expected a non-empty object of entries, got {type(doc).__name__}")
    out: dict[tuple[int, str], Company] = {}
    for key, entry in doc.items():
        try:
            cik, ticker, title = int(entry["cik_str"]), str(entry["ticker"]), str(entry["title"])
        except (KeyError, TypeError, ValueError) as exc:
            raise TickersError(f"entry {key!r} is not {{cik_str, ticker, title}}: {entry!r}") from exc
        ticker = ticker.strip()
        out.setdefault((cik, ticker), Company(cik, ticker, title.strip(), ticker_norm(ticker)))
    return out


def store_companies(con: duckdb.DuckDBPyConnection, raw_id: int, *, raw_dir) -> int:
    """Make `edgar_company` agree with the response; return rows inserted, changed or
    deactivated. A non-200 response stores nothing."""
    found = con.execute(
        "SELECT http_status, fetched_at FROM raw_response WHERE id = ?", [raw_id]
    ).fetchone()
    if found is None:
        raise LookupError(f"raw_response {raw_id} does not exist")
    status, fetched_at = found
    if status != 200:
        return 0
    with raw.open(con, raw_id, raw_dir=raw_dir) as fh:
        companies = parse(json.loads(fh.read()))
    stored = {
        (cik, ticker): (title, norm, active)
        for cik, ticker, title, norm, active in con.execute(
            f"SELECT cik, ticker, title, ticker_norm, active FROM {TABLE}"
        ).fetchall()
    }
    upserts = [
        [c.cik, c.ticker, c.title, c.ticker_norm, True]
        for pk, c in companies.items()
        if stored.get(pk) != (c.title, c.ticker_norm, True)
    ]
    gone = [
        [cik, ticker, title, norm, False]
        for (cik, ticker), (title, norm, active) in stored.items()
        if active and (cik, ticker) not in companies
    ]
    changed = upserts + gone
    if changed:
        # one JSON text parameter, not bound row values (see fiscal.upsert_page)
        con.execute(
            f"INSERT INTO {TABLE} (cik, ticker, title, ticker_norm, active, updated_at)"
            " SELECT r[1]::BIGINT, r[2], r[3], r[4], r[5]::BOOLEAN, ?"
            """ FROM (SELECT unnest(json_transform(?, '[["VARCHAR"]]')) AS r)"""
            " ON CONFLICT (cik, ticker) DO UPDATE SET title = excluded.title,"
            " ticker_norm = excluded.ticker_norm, active = excluded.active,"
            " updated_at = excluded.updated_at",
            [fetched_at, json.dumps(changed)],
        )
    return len(changed)


def lookup(con: duckdb.DuckDBPyConnection, query: str) -> list[tuple]:
    """Every (cik, ticker, title, ticker_norm, active, updated_at) of the companies matching
    `query`: a CIK (digits) or a ticker in any spelling (`ticker_norm`)."""
    q = query.strip()
    if q.isdigit():
        where, param = "cik = ?", int(q)
    else:
        where, param = f"cik IN (SELECT cik FROM {TABLE} WHERE ticker_norm = ?)", ticker_norm(q)
    return con.execute(
        f"SELECT cik, ticker, title, ticker_norm, active, updated_at FROM {TABLE}"
        f" WHERE {where} ORDER BY cik, NOT active, ticker",
        [param],
    ).fetchall()


# --- job -------------------------------------------------------------------------------------


def _fetch(ctx: "jobs.JobContext", key: str) -> Sequence[int]:
    raw_id, response = ctx.client.get(URL, source=SOURCE, dataset=RAW_DATASET)
    if response.status_code != 200:
        raise TickersError(f"{URL} answered {response.status_code}", raw_id=raw_id)
    return [raw_id]


def _parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    return store_companies(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir)


JOB = jobs.REGISTRY.register(
    jobs.Job(
        name="edgar_tickers",
        keys=("company_tickers",),
        fetch=_fetch,
        parsers={RAW_DATASET: _parse},
    )
)
"""SEC's ticker map into `edgar_company`; daily 07:15 ET."""
