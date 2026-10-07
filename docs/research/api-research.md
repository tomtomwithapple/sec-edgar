# US Financial Data Ingestion App — API Research & Fetch Plan

Oct 7, 2026 · @tuntuntuntun

## Overview

One ingestion app pulls six US public data sources on a schedule, lands every response raw, then normalizes it into PostgreSQL for analysis. The six sources: SEC EDGAR, US Treasury Fiscal Data, FINRA TRACE, BLS, TreasuryDirect and the Nasdaq Trader symbol directory.

Design rules, applied to every fetcher:

- **Raw first.** Every HTTP response is stored byte-for-byte (gzip) with URL, timestamp and SHA-256 before any parsing. Re-parsing never needs a re-fetch. Bodies over a few MB (the EDGAR bulk ZIPs) go to disk or object storage; Postgres keeps only the path and hash.
- **Idempotent upserts.** Each normalized table has a natural key from the source (accession number, CUSIP + auction date, series ID + period). Re-running a job changes nothing.
- **Watermarks with a lookback.** Each job stores the last record date it saw, then re-fetches an overlap window behind it on every run (per source below), so late revisions and restatements are caught. Upserts absorb the overlap.
- **Point-in-time.** Revisions (BLS, EDGAR amendments) are kept as new versions with a `first_seen_at`, never overwritten, so backtests see only what was known then.
- **One rate limiter per host.** Limits are set per source below and enforced centrally, not per job.
- **Two holiday calendars.** Fiscal Data, TreasuryDirect and FINRA follow the federal/SIFMA bond calendar; Nasdaq Trader and EDGAR market-hours polling follow the NYSE calendar. They differ on days such as Columbus Day and Veterans Day.

Assumed stack: Java 21 + Spring Boot for workers and API, PostgreSQL 16 for storage, a small web UI for job status and browsing. Swap freely; the plan is stack-neutral.

## Architecture

&#91;embedded content: ingestion architecture · 6 stages\]

Data flows one way: scheduler, fetcher, raw store, parser, normalized tables, UI. Because the raw layer keeps every response, a parser bug is fixed by re-parsing, never by re-fetching. The run log holds each dataset's watermark, so a failed run simply resumes from the last date it saw.

Core tables shared by every source: `raw_response` (id, source, url, fetched\_at, http\_status, sha256, body\_gz or blob\_path) and `job_run` (job, started\_at, finished\_at, status, rows\_upserted, watermark\_before, watermark\_after, error).

Data-quality alerts run after each job and post to the UI and a notification channel:

- **Zero rows** on a day the calendar says data is due.
- **Stale watermark:** no new data after N expected periods (e.g. 2 business days for DTS, 1 release for BLS).
- **Value jump** beyond a per-series threshold, flagged for review rather than blocked.
- **Row-count swing** of more than ±2% versus the previous snapshot for full-file sources (Nasdaq Trader).

## SEC EDGAR

Backfill from the nightly bulk ZIPs, then keep current with the per-company JSON APIs. No key; every request needs a User-Agent with a contact email, capped at 10 requests per second ([Accessing EDGAR Data](https://www.sec.gov/os/accessing-edgar-data)). Run the limiter at 8 rps to leave headroom.

| Fetch | Endpoint | Cadence | Lands in |
| --- | --- | --- | --- |
| Ticker ↔ CIK map | `www.sec.gov/files/company_tickers.json` | Daily | `edgar_company` |
| Filing history, all companies | `Archives/edgar/daily-index/bulkdata/submissions.zip` | Once (backfill), then weekly reconcile | `edgar_filing` |
| XBRL facts, all companies | `Archives/edgar/daily-index/xbrl/companyfacts.zip` | Once (backfill), then weekly reconcile | `xbrl_fact` |
| What filed today | EDGAR latest-filings feed or the daily form index | Every 15 min, 06:00–22:00 ET, NYSE days | `edgar_filing` |
| Filing detail for CIKs that just filed | `data.sec.gov/submissions/CIK##########.json` | Only for CIKs in the feed and on the watchlist | `edgar_filing` |
| Facts for a company that just filed | `data.sec.gov/api/xbrl/companyfacts/CIK##########.json` | On new 10-K/10-Q/8-K | `xbrl_fact` |
| Cross-section, one tag one period | `data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{CY2024Q1I}.json` | On demand from UI | query cache only |
| Form 4, 13D/G documents | `Archives/edgar/data/{cik}/{accession}/` | On new filing of those forms | `insider_txn`, `ownership_stake` |

The APIs update in real time as filings go public, and the bulk ZIP is republished nightly ([SEC press release 2021-159](https://www.sec.gov/news/press-release/2021-159)). CIKs are 10 digits, zero-padded. data.sec.gov has no CORS, so the browser never calls it directly; all calls go through the backend ([EDGAR APIs](https://www.sec.gov/page/sec-api-documentation)).

Table keys:

- `edgar_filing`: PK `accession_no`; columns `cik`, `form`, `filed_at`, `report_date`, `primary_doc`, `is_amendment`.
- `xbrl_fact`: PK (`cik`, `taxonomy`, `tag`, `unit`, `accn`, `period_start`, `period_end`); columns `value`, `fy`, `fp`, `form`, `filed`, `frame`. Keeping `accn` + `filed` in the key preserves restated values as separate rows, which keeps backtests point-in-time.
- `insider_txn`: PK (`accession_no`, `line_no`); parsed from the Form 4 XML.

Limits to design around:

- **Feed first, then per-CIK.** One feed poll every 15 minutes replaces hundreds of per-company calls; the per-CIK APIs are hit only for companies that actually filed.
- **Submissions are paged for heavy filers.** The per-company JSON holds recent filings inline and links older ones in extra files; the backfill must follow those links.
- **No segments in XBRL facts.** `companyfacts` carries only non-dimensional facts, so revenue by segment or geography is not available from this API. Company-level screens are fine.
- **Lookback:** reconcile the last 7 days of the daily index each night to catch filings the 15-minute poll missed.

Open question: 13F holdings are easier to load from the SEC's quarterly 13F data sets than from per-filing XML; confirm the current download location before building.

## US Treasury Fiscal Data

One generic fetcher covers every dataset: same base URL, same query grammar, same response shape. No key or account is needed, and the data is free to reuse commercially ([API documentation](https://fiscaldata.treasury.gov/api-documentation)).

Base URL: `https://api.fiscaldata.treasury.gov/services/api/fiscal_service`

| Dataset | Endpoint | Cadence | Why it matters |
| --- | --- | --- | --- |
| Daily Treasury Statement, operating cash | `/v1/accounting/dts/operating_cash_balance` | Daily, \~16:00 ET | TGA balance, a core liquidity input |
| Debt to the Penny | `/v2/accounting/od/debt_to_penny` | Daily | Total public debt outstanding |
| Treasury auctions | `/v1/accounting/od/auctions_query` | Daily | Auction results, bid-to-cover |
| Average interest rates | `/v2/accounting/od/avg_interest_rates` | Monthly | Cost of debt by security class |
| Monthly Treasury Statement | `/v1/accounting/mts/mts_table_1` (+ other tables) | Monthly | Receipts, outlays, deficit |
| Rates of exchange | `/v1/accounting/od/rates_of_exchange` | Quarterly | Official FX, incl. IDR |

Endpoint versions (v1 vs v2) differ by dataset; confirm each on its dataset page before coding.

Fetch pattern:

1. `filter=record_date:gte:{watermark minus 10 days}&sort=record_date&page[size]=10000` (default page is 100; URL-encode the brackets). The 10-day overlap catches revised rows.
2. Loop while `page[number]` < `meta.total-pages`.
3. Cast using `meta.dataTypes`; every value arrives as a string, and nulls arrive as the string `"null"`.
4. Upsert on (`record_date` + the dataset's row identifiers, e.g. `account_type`).
5. Schedule on the federal/SIFMA calendar; DTS for a given day publishes the next business day.

Gotcha: passing a partial `fields=` list can make the API aggregate and sum rows. Omit `fields=` to get every column unaggregated.

Tables: one table per endpoint (`fd_dts_operating_cash`, `fd_debt_to_penny`, `fd_auction`, ...), each with `record_date` plus typed columns. A `fd_dataset` registry holds endpoint, version, primary-key columns and watermark, so adding a dataset is a config row, not code.

## FINRA TRACE

Plan for the free aggregates only; trade-by-trade TRACE needs a licensed entitlement and stays out of phase 1. An individual can create a Public Credential, which reaches publicly available data at no cost ([FINRA API support](https://developer.finra.org/support)).

The free Fixed Income API covers three families: Treasury trading-volume aggregates, market breadth and sentiment, and capped volume for corporate/agency and structured products ([Fixed Income datasets](https://developer.finra.org/node/1171)).

| Dataset | Cadence | Lands in | Use |
| --- | --- | --- | --- |
| Treasury daily aggregates | Daily | `trace_tsy_agg` | Treasury liquidity by subtype and venue |
| Treasury monthly aggregates | Monthly | `trace_tsy_agg` | Longer-run volume trend |
| Treasury weekly aggregates (history only) | Once | `trace_tsy_agg` | Backfill 2019 to Feb 2023 |
| Corporate / agency / 144A breadth and sentiment | Daily | `trace_breadth` | Advances, declines, highs, lows in credit |
| Corporate / agency / securitized capped volume | Daily | `trace_capped_volume` | Credit trading activity |

The weekly Treasury format ended in February 2023 and was replaced by daily and monthly aggregates ([About Treasury Aggregate Statistics](https://www.finra.org/finra-data/browse-catalog/about-treasury)). Aggregates group volume by Bills, FRN, nominal coupons and TIPS, then by "ATS and Interdealer", "Dealer-to-Customer" and "Total" ([weekly statistics](https://www.finra.org/finra-data/browse-catalog/about-treasury/weekly-data)).

Fetch pattern:

1. OAuth2 client-credentials: exchange client ID + secret for a bearer token, cache it, refresh before expiry.
2. Query each dataset by group and name on `api.finra.org`, filtering on the trade/report date column > watermark.
3. Upsert on (date, product subtype, category, maturity bucket).

Open question: confirm exact dataset names, the per-request record cap, and which datasets a Public Credential actually reaches, in the API Console catalog before coding.

## BLS

A starter set of about 10 series fits in one request per run, so the 500-query daily quota is never a constraint. Registered v2 allows 500 queries a day, 50 series and 20 years per query, and 50 requests per 10 seconds; the key must be renewed yearly ([BLS API FAQ](https://www.bls.gov/developers/api_faqs.htm)).

Endpoint: `POST https://api.bls.gov/publicAPI/v2/timeseries/data/` with JSON body `{seriesid: [...], startyear, endyear, registrationkey, catalog: true}`.

| Series | Series ID | Release |
| --- | --- | --- |
| CPI-U all items, SA | `CUSR0000SA0` | Monthly |
| CPI-U core (ex food and energy), SA | `CUSR0000SA0L1E` | Monthly |
| PPI final demand | `WPSFD4` | Monthly |
| Nonfarm payrolls, SA | `CES0000000001` | Monthly |
| Unemployment rate, SA | `LNS14000000` | Monthly |
| Avg hourly earnings, private, SA | `CES0500000003` | Monthly |
| JOLTS job openings | `JTS000000000000000JOL` | Monthly |

Series IDs are from BLS conventions; validate each with one `catalog: true` call before the first scheduled run.

Fetch pattern:

1. Run at 09:00 ET on each release day from the BLS release calendar (releases land at 08:30 ET), not daily polling.
2. If the new period is not in the response yet, retry every 10 minutes until it appears or 12:00 ET, then alert.
3. Request the current and prior year; BLS revises recent months (payrolls twice).
4. Upsert into `bls_observation` keyed on (`series_id`, `year`, `period`, `first_seen_at`); write a new row only when the value or footnote (e.g. preliminary) changed.

Tables: `bls_series` (ID, title, seasonality, units, from the catalog) and `bls_observation`. The BLS API returns only current values, so vintages accumulate from the day the job starts. For history as first published, backfill the same series once from ALFRED (St. Louis Fed's vintage archive) into `bls_observation` with the vintage date as `first_seen_at`.

## TreasuryDirect

Make TreasuryDirect the source of truth for auctions, one row per security, and keep Fiscal Data's `auctions_query` only as a daily cross-check. Both describe the same auctions; storing both as primary tables invites conflicts. No key or auth is needed.

Base: `https://www.treasurydirect.gov/TA_WS/securities/`

| Fetch | Endpoint | Cadence | Lands in |
| --- | --- | --- | --- |
| Upcoming auctions | `announced?format=json` | Daily | `tsy_security` (status = announced) |
| Recent results | `auctioned?format=json&days=7` | Daily, after 13:30 ET | `tsy_security` (status = auctioned) |
| History by date range | `search?startdate=YYYY-MM-DD&enddate=YYYY-MM-DD&format=json` | Once, monthly windows back to the earliest date the feed returns (confirm with one old query) | `tsy_security` |
| One security | `{cusip}/{issueDate}?format=json` | On demand | `tsy_security` |

The announced feed carries term, type, CMB/reopening flag, CUSIP, offering amount, auction and issue dates ([data.gov: Upcoming Auctions](https://catalog-old.data.gov/harvest/object/a979bccd-a2e0-44f6-b146-85431cb176b7)). The results feed adds high yield, rate or discount margin, and price per $100 ([data.gov: Auction Results](https://catalog.data.gov/dataset/auction-results)). The date-range search is the backfill route ([data.gov: Auction Query](https://catalog-old.data.gov/harvest/object/35bd9cd1-031b-43f5-ad11-5b8809df387d)).

Table `tsy_security`: PK (`cusip`, `auction_date`), because reopenings reuse a CUSIP. Store the full JSON row in a `jsonb` column plus typed columns for the analysis fields: `security_type`, `term`, `offering_amt`, `high_yield`, `bid_to_cover`, `primary_dealer_accepted`, `indirect_accepted`, `direct_accepted`. Announced rows are updated in place to auctioned; the raw layer keeps both versions.

Open question: field names change over the years in this feed; map them against a 2010 and a current response before writing the parser.

## Nasdaq Trader symbol directory

Snapshot two files daily and diff them, so the app builds its own listing history: adds, delistings, renames and ETF flags, dated. The files only show today, so this history starts the day the job starts; it cannot be backfilled from this source.

| File | URL | Covers |
| --- | --- | --- |
| `nasdaqlisted.txt` | `https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt` | Nasdaq-listed securities |
| `otherlisted.txt` | `https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt` | NYSE, NYSE American, Cboe and other venues |

The same files sit on `ftp.nasdaqtrader.com/symboldirectory`, alongside `nasdaqtraded.txt` and others ([FINRA OATS page](https://www.finra.org/filing-reporting/oats/oats-reportable-securities)). Fields include symbol, security name, market category, test-issue flag, financial status, round lot size and ETF flag ([datahub.io field list](https://datahub.io/core/nasdaq-listings)).

Fetch pattern:

1. Download at 07:00 ET on NYSE trading days; both files are pipe-delimited.
2. Drop the trailing `File Creation Time` line and store its timestamp as the snapshot time. A file without that line is truncated: reject it.
3. Sanity-check the row count against the previous snapshot; outside ±2%, store the snapshot but skip the diff and alert.
4. Drop rows with Test Issue = Y.
5. Compare with the previous snapshot and write changes to `symbol_history` as type-2 rows (`valid_from`, `valid_to`).

Tables: `symbol_snapshot` (raw rows per day) and `symbol_history` (PK `symbol`, `exchange`, `valid_from`).

Joining to EDGAR needs a normalized ticker. `otherlisted.txt` carries three symbol columns (ACT, CQS, Nasdaq), and class shares are written differently across sources (e.g. BRK.B vs BRK-B). Store a `ticker_norm` (uppercase, class separator mapped to one character) on both `symbol_history` and `edgar_company`, join on that, and list unmatched symbols in the UI rather than dropping them.

## Schedule, build order and risks

All jobs fit in one scheduler with eight cron entries. Times are US Eastern; WIB is ET + 11 h during US daylight time, + 12 h otherwise, so store everything in UTC. "Bond days" follow the federal/SIFMA calendar, "trading days" the NYSE calendar.

| Job | When (ET) | Source |
| --- | --- | --- |
| Symbol snapshot | 07:00, trading days | Nasdaq Trader |
| Ticker ↔ CIK map | 07:15 daily | EDGAR |
| Filing poll, watchlist | Every 15 min, 06:00–22:00 | EDGAR |
| Auction results | 14:00 bond days | TreasuryDirect |
| DTS, Debt to the Penny, auctions cross-check | 17:00 bond days | Fiscal Data |
| TRACE aggregates | 18:00 bond days | FINRA |
| BLS release pull | 09:00 on release days, retry to 12:00 | BLS |
| Bulk reconcile (companyfacts, submissions) | Sunday 03:00 | EDGAR |

Build order, each phase usable on its own:

1. **Core:** raw store, run log, rate limiter, scheduler, job-status page. Prove it with Fiscal Data (simplest API, no auth).
2. **Macro:** BLS and TreasuryDirect, with vintage handling.
3. **Fundamentals:** EDGAR bulk backfill, then incremental polling and the Nasdaq symbol join.
4. **Credit:** FINRA OAuth and aggregates.
5. **Analysis UI:** series browser, cross-section screens from `xbrl_fact`, CSV export.

Risks:

- **EDGAR blocking** if the User-Agent is missing or the rate spikes. Mitigation: one shared limiter, backoff on 403/429, feed-first polling.
- **Schema drift** in TreasuryDirect and Fiscal Data versions. Mitigation: raw store plus parser tests against saved responses.
- **FINRA entitlements** narrower than expected on a Public Credential. Mitigation: FINRA is phase 4, nothing depends on it.
- **XBRL tag inconsistency** across companies (e.g. several revenue tags). Mitigation: a mapping table from standard concept to candidate tags, versioned.
- **Storage growth** from `companyfacts.zip` (many GB uncompressed). Mitigation: raw ZIPs on disk, load only `us-gaap` and `dei`, partition `xbrl_fact` by fiscal year.
- **False delistings** from a failed or truncated symbol file. Mitigation: truncation check and the ±2% row-count gate.
- **Silent ticker mismatches** between Nasdaq Trader and EDGAR. Mitigation: `ticker_norm` join and an unmatched-symbols list.
