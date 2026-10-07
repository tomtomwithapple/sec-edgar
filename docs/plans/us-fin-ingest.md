# US financial data ingestion (usfin)

Status: planning — next: review
Goal: One local app that fetches five US public data sources (SEC EDGAR, Treasury Fiscal Data,
BLS, TreasuryDirect, Nasdaq Trader) on schedule, stores every response raw, normalizes into DuckDB
point-in-time, and shows job status, alerts and the data in a small local web UI.

Source research: [docs/research/api-research.md](../research/api-research.md) (endpoints, cadences,
table keys, gotchas). This plan adapts it to the stack below; where they differ, this plan wins.

## Why

Tommy wants US macro, Treasury and company-fundamental data in one local store for analysis and
backtests. Today it lives in five APIs with different shapes, limits, calendars and revision
habits. Backtests need values *as they were known on a date*, not today's revised numbers, and a
parser bug must never mean re-downloading gigabytes from SEC.

When this is done, these are true (each one is covered by at least one slice's test):

- **C1 Raw first.** Every HTTP response is stored gzip-compressed on disk, content-addressed by
  SHA-256, with a `raw_response` row (source, dataset, url, fetched_at, http_status, sha256,
  blob_path, bytes) written *before* parsing. `usfin reparse <job>` rebuilds that job's
  normalized rows from stored raw responses with the network disabled.
- **C2 Idempotent.** Running any job twice against the same upstream data leaves every
  normalized table's row count and content unchanged on the second run.
- **C3 Watermark + lookback.** Each job reads its watermark from its last successful `job_run`
  and re-fetches its overlap window (Fiscal Data 10 days, EDGAR daily index 7 days, BLS current +
  prior year, TreasuryDirect 7 days). A value revised inside the window is picked up.
- **C4 Point-in-time.** BLS observations and EDGAR XBRL facts keep every distinct published value
  with `first_seen_at` (BLS) or `accn`/`filed` (XBRL); an `as_of(date)` query returns what was
  known on that date.
- **C5 Polite fetching.** One rate limiter per host, shared by all jobs in the process; SEC hosts
  never exceed 8 requests/second. Every SEC request carries `SEC_USER_AGENT`; with it unset,
  EDGAR jobs refuse to start (no request is sent).
- **C6 Calendars.** Jobs run only on their calendar's days: bond days (US federal holidays + Good
  Friday closed) for Fiscal Data, TreasuryDirect, BLS; NYSE trading days for Nasdaq Trader and the
  EDGAR intraday poll. On 2026-10-12 (Columbus Day) bond jobs skip and NYSE jobs run.
- **C7 Resume.** A failed run records the error, leaves the watermark where it was, and the next
  run resumes from it.
- **C8 Alerts.** After each job the four data-quality rules run (zero rows on a due day, stale
  watermark, value jump past a per-series threshold, row-count swing > ±2% on full-file
  snapshots); alerts land in an `alert` table, the UI, and an optional webhook.
- **C9 Safe symbol history.** A Nasdaq Trader file missing its `File Creation Time` trailer is
  rejected; a snapshot whose row count moves more than ±2% is stored but not diffed, and alerts.
- **C10 Ticker join.** `ticker_norm` joins Nasdaq Trader symbols to EDGAR companies across class
  separators (`BRK.B`, `BRK-B`, `BRK/B` → one form); unmatched symbols are listed, never dropped.
- **C11 UI.** `usfin serve` runs the scheduler and a local web UI at http://127.0.0.1:8660 showing
  job runs and watermarks (with run-now), alerts, a series browser (BLS, Fiscal Data, Treasury
  auctions), EDGAR company filings/facts, an XBRL cross-section screen, unmatched symbols, and
  CSV export of every table view.
- **C12 Gate.** `uv run ruff check . && uv run pytest -q && uv build -q` is green at every commit.

## What

In scope:

- Core: raw store, run log with watermarks, per-host rate limiter, HTTP client with retries,
  calendars, job framework with `run` / `reparse`, data-quality alerts, scheduler.
- **Treasury Fiscal Data**: one generic fetcher driven by a dataset registry — DTS operating cash
  balance, Debt to the Penny, auctions (cross-check only), average interest rates, MTS table 1,
  rates of exchange.
- **TreasuryDirect**: announced and auctioned securities daily, one-off history backfill,
  `tsy_security` as the source of truth for auctions, cross-check against Fiscal Data auctions.
- **BLS**: seven starter series (research doc table), versioned observations, release-calendar
  driven pulls with retry to 12:00 ET, ALFRED backfill of first-published history (needs
  `FRED_API_KEY`).
- **SEC EDGAR**: ticker↔CIK map, submissions (per-CIK incl. paged history, and bulk ZIP backfill),
  XBRL companyfacts (per-CIK and bulk ZIP, `us-gaap` + `dei` only), 15-minute latest-filings poll,
  nightly 7-day daily-index reconcile, Form 4 → `insider_txn`, Schedule 13D/G → `ownership_stake`,
  frames cross-section with cache, a small versioned concept→tags map for screens.
- **Nasdaq Trader**: daily `nasdaqlisted.txt` + `otherlisted.txt` snapshots, type-2
  `symbol_history`, `ticker_norm` join to EDGAR, unmatched list.
- **UI**: the pages in C11, server-rendered, read-mostly, bound to 127.0.0.1.
- **Ops**: `usfin doctor`, a launchd plist example for `usfin serve`, README run docs.

**Out of scope:**

- FINRA TRACE (all of it — credentials and dataset names unconfirmed). Can be a later plan.
- SEC 13F holdings, XBRL segment/dimensional data, taxonomies other than `us-gaap` and `dei`,
  full-text search, filing documents other than Form 4 and 13D/G XML.
- PostgreSQL, Docker, cloud deployment, multi-user access, UI authentication (localhost only).
- Notification channels beyond one generic JSON webhook (no Slack/Telegram/email integrations).
- Derived indicators, analytics or backtesting code — this plan only lands and serves data.
- Back-filling Nasdaq Trader history (the source has none) or BLS vintages before the ALFRED
  backfill's coverage.
- Mobile layout polish beyond "readable at phone width".

## Who

- **User:** Tommy Yohanes — runs it locally, reads the UI, queries DuckDB for analysis.
- **Owner/reviewer:** Tommy reviews the branch and decides on the PR.
- **Doers:** one fresh Claude subagent per slice, dispatched sequentially by the planning session,
  which verifies each slice against git; a fresh reviewer subagent critiques this plan first.

## Where

Push target: remote `origin` → `https://github.com/tomtomtomdev/sec-edgar.git` (private, created
for this plan with `gh repo create` on the user's yes), branch `feat/us-fin-ingest` (new; `main`
holds only the initial commit). `git ls-remote --heads origin` answered 2026-10-07.

Repo: `/Users/tomtomtomtom/Projects/sec-edgar` — empty except README, `.gitignore`,
`docs/research/api-research.md`. Everything below is new:

```
pyproject.toml, uv.lock, .python-version, .env.example
config/
  hosts.yaml          per-host rate limits + headers
  calendars.yaml      manual holiday overrides (bond / nyse)
  fiscal_data.yaml    Fiscal Data dataset registry
  bls.yaml            BLS series list + jump thresholds
  edgar.yaml          watchlist CIKs, forms to follow
  concepts.yaml       standard concept → candidate XBRL tags (versioned)
  schedule.yaml       cron entries (ET) + calendar per job
src/usfin/
  cli.py              typer app `usfin`
  settings.py         env + paths
  store.py            DuckDB connection, migrations runner
  migrations/NNN_*.sql
  raw.py              content-addressed raw store
  http.py             client, retries, archive-every-response
  ratelimit.py        per-host token bucket
  calendars.py        bond / nyse day logic
  jobs.py             Job protocol, run log, watermarks, run/reparse
  alerts.py           DQ rules, alert table, webhook
  scheduler.py        APScheduler wiring
  sources/fiscal.py, treasurydirect.py, bls.py, alfred.py,
          edgar/{tickers,submissions,companyfacts,feed,dailyindex,form4,sched13,frames}.py,
          nasdaqtrader.py
  web/app.py, web/templates/*.html, web/static/style.css
tests/  test_*.py, fixtures/<source>/...
deploy/com.tuntun.usfin.serve.plist.example
```

Data and raw blobs live outside git: `data/usfin.duckdb` (`USFIN_DB`), `raw/` (`USFIN_RAW`).
Runs on this Mac (macOS, Apple Silicon) only; no CI in this plan.

## When

Order follows the research doc's build order, each phase usable on its own:

1. **Core** (scaffold, store, raw, HTTP + limiter, jobs, calendars) — everything depends on it.
2. **Fiscal Data** — proves the core end to end on the simplest no-auth API.
3. **Alerts, scheduler, UI shell** — the app becomes a running service with a job-status page.
4. **TreasuryDirect**, then the auction cross-check (needs Fiscal auctions).
5. **BLS** — versioning, calendar, retry; ALFRED last (key-gated).
6. **EDGAR** — tickers, submissions, companyfacts, then bulk backfills, then feed/reconcile,
   then Form 4, 13D/G, frames.
7. **Nasdaq Trader** — snapshot, diff, then the ticker join (needs EDGAR tickers).
8. **UI pages** for series, EDGAR and symbols; then ops (doctor, launchd, README).

New sources register their own schedule entry in the slice that adds their job, so the
scheduler is never edited for more than one source at a time. No deadline.

## How

### Tech stack

| Layer | Choice | Version | Source |
|-------|--------|---------|--------|
| Language / toolchain | Python, uv, uv_build backend | 3.13 (`requires-python >=3.12`) / uv 0.12.2 | proposed; matches sibling repos (ausdata, singstat); uv + CPython 3.13 installed |
| CLI | typer | 0.27.3 | proposed; used in ausdata |
| HTTP | httpx (sync client) | 0.28.1 | proposed; used in all siblings |
| Storage | DuckDB (single file) | 1.5.6 | proposed; used in all siblings; user chose it over Postgres |
| Config / env | pyyaml, python-dotenv | 6.0.3 / 1.2.4 | proposed; used in ausdata / worldbankscape |
| Calendars | holidays (`holidays.US`, `holidays.NYSE`) | 0.106 | proposed, approved |
| Scheduler | APScheduler (BackgroundScheduler, CronTrigger tz America/New_York) | 3.11.3 | proposed, approved |
| Web UI | FastAPI + uvicorn + Jinja2, server-rendered, inline SVG charts, no JS build | 0.142.2 / 0.54.0 / 3.1.6 | proposed, approved |
| XML | defusedxml (ElementTree API) | 0.7.1 | proposed, approved |
| Tests | pytest, respx (httpx mocking), FastAPI TestClient | 9.1.1 / 0.23.1 | proposed; used in siblings |
| Lint | ruff, line length 100 | 0.16.10 | proposed; used in siblings |

Versions are the ones the S0 `uv.lock` must pin; a slice that needs a different version stops
and reports it.

**New dependencies:** this is a new project, so all of the above are new to it. duckdb, httpx,
typer, pyyaml, python-dotenv, pytest, respx and ruff are already used in the sibling repos.
holidays, apscheduler, fastapi, uvicorn, jinja2 and defusedxml are new to this user's projects
and were **approved by the user on 2026-10-07**. No pandas: CSV export uses `csv`, charts are
hand-built SVG.

### Approach

**Conventions (every slice).**
- Package `usfin` under `src/usfin/`, tests in `tests/`, fixtures in `tests/fixtures/<source>/`.
- TDD. Unit tests never touch the network: `respx` mocks HTTP, fixtures are recorded from the
  real endpoint and trimmed small (a few KB; never invented). Tests that hit real endpoints are
  `@pytest.mark.live` and skipped by default (`uv run pytest -m live`).
- Tests use a temp DuckDB (`tmp_path`) and temp raw dir; never `data/` or `raw/`.
- All timestamps stored as naive UTC `TIMESTAMP`; schedules and calendars reason in
  `America/New_York` (`zoneinfo`). Dates from sources stay `DATE`.
- Secrets only in `.env` (see `.env.example`): `SEC_USER_AGENT` (required for EDGAR),
  `BLS_API_KEY` (optional), `FRED_API_KEY` (needed by ALFRED slice only), `ALERT_WEBHOOK_URL`
  (optional). Paths: `USFIN_DB` (default `data/usfin.duckdb`), `USFIN_RAW` (default `raw/`).
- Commit style follows the sibling repos: `S<n>: <what the slice found>`; plan-only commits
  `plan(us-fin-ingest): …`.

**Storage and concurrency.** DuckDB allows one read-write process. `usfin serve` is that process:
it holds the connection, runs the scheduler and the UI. Jobs run in a **single-worker** executor
(APScheduler `max_workers=1`, `max_instances=1`, `coalesce=True`), so writes never race; each
job uses its own `conn.cursor()`. Long jobs (EDGAR bulk reconcile) only delay later jobs, which
coalesce. CLI commands that write (`usfin run`, `reparse`, backfills) open the DB themselves; if
the file is locked by `serve`, they print "server running — use `usfin trigger <job>`", and
`usfin trigger` POSTs to `http://127.0.0.1:8660/api/jobs/<job>/run`. The UI reads through the
same connection (cursor per request).

**Schema migrations.** Plain SQL files `src/usfin/migrations/NNN_name.sql`, applied in order and
recorded in `schema_version`. Each slice that adds tables adds a migration; none edits an old one.

**Core tables.**
- `raw_response(id BIGINT PK, source, dataset, url, request_body_sha256 NULL, fetched_at,
  http_status, sha256, blob_path, bytes, content_type)`.
- `job_run(id PK, job, started_at, finished_at, status ok|failed|skipped, rows_upserted,
  watermark_before, watermark_after, error, raw_ids)` — watermark stored as text (a date, a
  timestamp or an accession), interpreted by the job.
- `alert(id PK, job, rule, subject, detail JSON, raised_at, acknowledged_at NULL)`.

**Raw store (C1).** Body is gzipped to `raw/<source>/<sha[:2]>/<sha>.gz` (sha of the uncompressed
body); identical bodies share one blob, but every fetch gets its own `raw_response` row. Bodies
larger than 64 MB (EDGAR bulk ZIPs) are stored uncompressed as `<sha>.zip` and streamed to disk
while hashing, never held in memory. `raw.open(raw_id)` returns a readable stream.

**HTTP (C5).** `usfin.http.Client.get/post(url, source=, dataset=, …)`: looks up the host in
`config/hosts.yaml` for rate and headers (`www.sec.gov` and `data.sec.gov` share one 8 rps
bucket, others default 4 rps), acquires a token from a thread-safe token bucket, sends, retries
429/5xx/transport errors with exponential backoff (honouring `Retry-After`, max 5 tries), treats
SEC 403 as "blocked": backs off 10 minutes and fails the job. Every final response — success or
error status — is archived through the raw store before returning `(raw_id, response)`. SEC hosts
require `SEC_USER_AGENT`; missing → `ConfigError` before any socket opens.

**Calendars (C6).** `is_bond_day(d)`: weekday, not in `holidays.US(observed=True)` federal
holidays, not Good Friday; `is_nyse_day(d)`: weekday, not in `holidays.NYSE()`. Both apply
`config/calendars.yaml` overrides (`add_closed`, `add_open`) for SIFMA one-offs. `prev/next
business day` helpers per calendar.

**Jobs (C2, C3, C7).** A job is `fetch(ctx, watermark) -> list[raw_id]` plus
`parse(ctx, raw_id) -> rows upserted`, plus `next_watermark`. The runner opens a `job_run`, reads
the last `ok` run's watermark, fetches, parses, upserts inside one transaction per raw response,
then closes the run `ok` with the new watermark — or `failed` with the error and
`watermark_after = watermark_before`. `usfin run <job>`, `usfin jobs` (last run per job),
`usfin reparse <job> [--since DATE]` (calls `parse` over that job's `raw_response` rows; a test
asserts no HTTP happens). Upserts are `INSERT … ON CONFLICT (pk) DO UPDATE` on natural keys;
versioned tables (BLS) insert-on-change instead.

**Alerts (C8).** Rules run after each job, configured per job: `zero_rows_on_due_day`,
`stale_watermark(n_periods)` (e.g. DTS 2 bond days, BLS 1 release), `value_jump(series, pct)`
(flag, never block), `row_count_swing(pct=2)`. An alert is inserted once per (job, rule, subject,
day) and POSTed as JSON to `ALERT_WEBHOOK_URL` if set (failure to post is logged, not fatal).
`usfin alerts [--all]`, `usfin alerts ack <id>`.

**Treasury Fiscal Data.** Base `https://api.fiscaldata.treasury.gov/services/api/fiscal_service`.
`config/fiscal_data.yaml` entries: `name, endpoint, pk: [record_date, …], date_field,
lookback_days: 10, jump_fields`. Initial six datasets per research doc; each entry's endpoint
version and pk columns are confirmed against a recorded live response in the slice that adds it.
Fetch: `filter=record_date:gte:<watermark-10d>`, `sort=record_date`, `page[size]=10000`
(brackets URL-encoded), loop pages to `meta.total-pages`; **never send `fields=`** (it
aggregates). Parse: cast via `meta.dataTypes` (`"null"` string → NULL; DATE, NUMBER→DOUBLE,
CURRENCY→DECIMAL(20,2), PERCENTAGE→DOUBLE, INTEGER→BIGINT, else VARCHAR). Each dataset gets
table `fd_<name>` created from its dataTypes on first parse; an unseen column is added with
`ALTER TABLE ADD COLUMN` and logged (schema drift). Job `fiscal` runs every registry entry; one
dataset failing fails the run but the others still commit (partial watermark per dataset is
stored in `fd_watermark(name, watermark)`). First run with no watermark backfills from
`backfill_from` in the registry (default 2000-01-01).

**TreasuryDirect.** Base `https://www.treasurydirect.gov/TA_WS/securities/`. Job
`treasurydirect`: `announced?format=json` and `auctioned?format=json&days=7`. Backfill command
`usfin td backfill [--from YYYY-MM-DD]` walks monthly `search?startdate&enddate&format=json`
windows; the earliest returned date is probed once and recorded in this plan. Table
`tsy_security` PK (`cusip`, `auction_date`); typed columns `security_type, security_term,
offering_amt, high_yield, high_discount_rate, high_investment_rate, price_per100,
bid_to_cover, primary_dealer_accepted, indirect_accepted, direct_accepted, issue_date,
maturity_date, status announced|auctioned` plus `payload JSON`. A field map handles old vs new
field names (verified against a 2010 and a current fixture). `announced` rows become `auctioned`
in place. Cross-check job `auction_xcheck` compares `fd_auctions` to `tsy_security` on
(cusip, auction_date) for high yield/rate and offering amount; mismatches raise alerts.

**BLS (C4).** `config/bls.yaml`: series IDs from the research doc + jump thresholds. Client:
v2 `POST https://api.bls.gov/publicAPI/v2/timeseries/data/` with `registrationkey` and
`catalog: true` when `BLS_API_KEY` is set; otherwise v1 (no key: 25 queries/day, 25 series,
10 years — enough for 7 series in one query; no catalog). `usfin bls validate` checks every
configured ID returns data. Tables `bls_series(series_id PK, title, seasonality, units)` and
`bls_observation(series_id, year, period, value, footnotes, first_seen_at)`, PK
(`series_id, year, period, first_seen_at`); a new row is written only when value or footnotes
differ from the latest row for that period. `as_of(series, date)` view/function returns the
latest row with `first_seen_at <= date` per period. Release days come from the BLS release
schedule iCalendar feed (URL confirmed in its slice; hand-parsed `VEVENT` `DTSTART`/`SUMMARY`),
cached weekly into `bls_release(date, release_name)`. Job `bls` is scheduled 09:00 ET on bond
days; on a release day where the expected new period is absent it reschedules itself every 10 min
until 12:00 ET, then raises `stale_watermark`. Not a release day → one cheap pull anyway (catches
unscheduled revisions). If the calendar can't be fetched, alert and treat today as a non-release
day.
ALFRED: `usfin bls alfred-backfill` maps each BLS series to its FRED ID in `bls.yaml`
(e.g. CUSR0000SA0→CPIAUCSL, LNS14000000→UNRATE, CES0000000001→PAYEMS), pulls
`fred/series/observations?realtime_start=1776-07-04&realtime_end=9999-12-31` and inserts each
vintage as a `bls_observation` row with `first_seen_at = realtime_start`. Blocks without
`FRED_API_KEY`.

**SEC EDGAR.** All requests via `http.Client` with the SEC bucket. CIK always 10-digit
zero-padded in URLs, stored as BIGINT.
- Tickers: `https://www.sec.gov/files/company_tickers.json` → `edgar_company(cik, ticker,
  title, ticker_norm, updated_at)` (one row per cik+ticker; PK (`cik`, `ticker`)). Job
  `edgar_tickers` daily 07:15 ET.
- Submissions: `data.sec.gov/submissions/CIK##########.json`; parse `filings.recent` columnar
  arrays and follow `filings.files[]` (older pages) → `edgar_filing(accession_no PK, cik, form,
  filed_at, accepted_at NULL, report_date, primary_doc, is_amendment, source)`.
- Companyfacts: `data.sec.gov/api/xbrl/companyfacts/CIK##########.json` →
  `xbrl_fact(cik, taxonomy, tag, unit, accn, period_start NULL, period_end, value DOUBLE, fy,
  fp, form, filed, frame NULL)`, PK (`cik, taxonomy, tag, unit, accn, period_start,
  period_end`) — DuckDB PKs disallow NULL, so instant facts store `period_start = period_end`
  with a boolean `is_instant`. Only `us-gaap` and `dei`.
- Bulk: `Archives/edgar/daily-index/bulkdata/submissions.zip` and
  `Archives/edgar/daily-index/xbrl/companyfacts.zip` downloaded once to the raw store (streamed),
  then parsed member by member without extracting to disk; `usfin edgar backfill
  {submissions|companyfacts} [--limit N] [--cik …]`. The slice run uses `--limit`; the full
  load is an operation documented in README with its measured time and DB size. Weekly
  reconcile job (Sunday 03:00 ET) re-downloads and re-parses only members whose bytes changed
  (member CRC vs stored `edgar_bulk_member(name, crc, raw_id)`).
- Feed: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&count=100`
  paged with `start=` until an accession already seen; job `edgar_feed` every 15 min 06:00–22:00
  ET on NYSE days. Every entry lands in `edgar_filing` (`source='feed'`). Then: CIKs on the
  watchlist (`config/edgar.yaml`) get a submissions refresh; any 10-K/10-Q filer, and 8-K filers
  on the watchlist, get a companyfacts refresh; every Form 4 and SC 13D/G (incl. /A) is queued
  for its document parser. Watermark: newest accepted timestamp seen.
- Daily index reconcile: `Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx` for the last
  7 NYSE days (404 for a not-yet-published day is normal) → `edgar_filing` (`source='index'`);
  job `edgar_reconcile` nightly 23:30 ET.
- Form 4: filing folder `Archives/edgar/data/{cik}/{accession_nodash}/index.json` → the `.xml`
  ownership document → `insider_txn(accession_no, line_no, issuer_cik, owner_cik, owner_name,
  is_derivative, security_title, txn_date, txn_code, shares, price, acquired_disposed,
  shares_owned_after, direct_indirect)`, PK (`accession_no`, `line_no`).
- Schedule 13D/G: structured XML (required since 2024-12-18) → `ownership_stake(accession_no,
  filer_cik, subject_cik, form, event_date, percent_of_class, shares, filed_at)`, PK
  (`accession_no`, `filer_cik`). Pre-XML (HTML/text) 13D/G filings are skipped and counted.
- Frames: `data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json` on demand →
  `frames_cache(key PK, fetched_at, raw_id)`, TTL 24 h; never written to `xbrl_fact`.
- Concept map `config/concepts.yaml` (e.g. `revenue: [Revenues,
  RevenueFromContractWithCustomerExcludingAssessedTax, SalesRevenueNet]`), with a `version:`;
  screens pick the first tag a company reports for that period.

**Nasdaq Trader (C9, C10).** `https://www.nasdaqtrader.com/dynamic/SymDir/{nasdaqlisted,otherlisted}.txt`,
pipe-delimited, job `symbols` 07:00 ET NYSE days. Trailer `File Creation Time: mmddyyyyhh:mm|…`
→ snapshot time; no trailer → reject (failed run, alert). Drop `Test Issue = Y`. Store
`symbol_snapshot(snapshot_at, file, symbol, exchange, name, etf, round_lot, financial_status,
market_category, cqs_symbol, act_symbol)`. Row count vs previous snapshot of that file outside
±2% → keep the snapshot, skip the diff, alert. Diff → `symbol_history(symbol, exchange,
valid_from, valid_to NULL, name, etf, …)` PK (`symbol, exchange, valid_from`): new symbol opens a
row, gone symbol closes it, changed name/ETF flag closes and opens. `ticker_norm(s)`: uppercase,
strip, map `.`, `-`, `/`, ` ` class separators to `.`; NASDAQ-style suffix forms are matched
as-is. View `symbol_edgar_match` joins open history rows to `edgar_company` on `ticker_norm`;
`usfin symbols unmatched`.

**Scheduler (C6).** `config/schedule.yaml` lists every job with cron (ET), calendar
(`bond`|`nyse`|`any`) and window. `usfin serve` builds APScheduler CronTriggers in
`America/New_York`; a guard skips (and logs a `skipped` job_run) on non-calendar days.
`usfin schedule` prints the next 5 fire times per job. Final schedule:

| Job | When (ET) | Calendar |
|---|---|---|
| symbols | 07:00 | nyse |
| edgar_tickers | 07:15 | any |
| edgar_feed | every 15 min 06:00–22:00 | nyse |
| treasurydirect | 14:00 | bond |
| fiscal (+ auction_xcheck after) | 17:00 | bond |
| bls | 09:00, retry to 12:00 on release days | bond |
| edgar_reconcile | 23:30 | any |
| edgar_bulk_reconcile | Sun 03:00 | any |
| bls_calendar | Mon 06:00 | any |

**Web UI (C11).** FastAPI app in `usfin.web.app`, Jinja2 templates, one CSS file with
light/dark tokens, no JS framework (a few lines of vanilla JS at most). Pages: `/` jobs
(last run, status, watermark, rows, run-now button → `POST /api/jobs/{job}/run`), `/alerts`
(ack), `/series` (BLS series, Fiscal datasets, auctions; table + inline SVG line chart; as-of
date picker for BLS), `/edgar/{cik}` (filings, key facts via concept map), `/screen` (one
concept × one period across companies from `xbrl_fact`, or frames on demand), `/symbols/unmatched`.
Every table view has `?format=csv`. Bound to 127.0.0.1:8660.

**Risks and how each is retired.**
- *EDGAR blocking* (missing UA, rate spikes) → one shared bucket at 8 rps, UA required by code,
  403 back-off; retired by S-tests on the limiter and the UA guard, and the first live EDGAR run.
- *Schema drift* (Fiscal Data, TreasuryDirect) → raw store + add-column + recorded fixtures from
  old and new responses; retired by the TreasuryDirect 2010-vs-current fixture test.
- *Bulk size* (companyfacts.zip multi-GB, tens of millions of facts) → stream members, `us-gaap`
  + `dei` only, `--limit` runs in slices; full load measured once and recorded in README. If the
  full load exceeds what this Mac handles comfortably, that is reported, not worked around.
- *DuckDB single-writer* → one serving process, single-worker executor, `usfin trigger` for CLI
  while serving; retired by a test that a second write-open reports the lock cleanly.
- *Endpoint facts not yet verified* (Fiscal pk columns, TD earliest date, BLS iCal URL, EDGAR
  Atom paging, 13D/G XML element names) → each is confirmed against a live response in the slice
  that uses it, and the plan is amended in place if it was wrong.
- *XBRL tag inconsistency* → versioned `concepts.yaml`; screens show which tag was used.
- *False delistings* → trailer check + ±2% gate (C9).
- *Ticker mismatches* → `ticker_norm` + unmatched list (C10).

### Commands

- Install: `uv sync`
- **Test:** `uv run pytest -q` (live tests excluded by the default `-m 'not live'`; run them with
  `uv run pytest -m live`)
- **Build (gate):** `uv run ruff check . && uv run pytest -q && uv build -q`
- **Run:** `uv run usfin <command>` — e.g. `uv run usfin run fiscal`, `uv run usfin jobs`,
  `uv run usfin serve` (scheduler + UI on http://127.0.0.1:8660).
