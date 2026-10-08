# US financial data ingestion (usfin)

Status: S4 done — next: S5
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

- **C1 Raw first.** Every final HTTP response (after retries; retried 429/5xx attempts carry no data and are not kept) is stored on disk, content-addressed by SHA-256, with a
  `raw_response` row (source, dataset, url, fetched_at, http_status, sha256, blob_path, bytes)
  written *before* parsing. `usfin reparse <job>` rebuilds that job's normalized rows from stored
  raw responses with no HTTP request made.
- **C2 Idempotent.** Running any job twice against the same upstream data leaves every
  normalized table's row count and content unchanged on the second run. Bookkeeping tables are
  excluded by name: `job_run`, `raw_response`, `watermark`, `alert`, `frames_cache`,
  `pending_doc`, `schema_version`.
- **C3 Watermark + lookback.** Each job keeps a watermark per (job, key) in the `watermark` table
  (key = dataset, series group or file), capped at today, and re-fetches its overlap window
  (Fiscal Data 10 days; TreasuryDirect and EDGAR daily index max(7 days, back to the
  watermark); BLS current + prior year). A value revised inside the window is picked up.
- **C4 Point-in-time.** BLS observations keep every distinct published value with
  `first_seen_at` = the `fetched_at` of the raw response that first carried it; XBRL facts keep
  every `accn`/`filed`. `bls_as_of(ts)` returns, per series and period, the latest row with
  `first_seen_at <= ts`; a date argument means 23:59:59 America/New_York that day. `xbrl_as_of(d)`
  returns facts with `filed <= d`. Reparse never rewrites history.
- **C5 Polite fetching.** One rate limiter per host, shared by all jobs in the process; SEC hosts
  never exceed 8 requests/second. Every SEC request carries `SEC_USER_AGENT`; with it unset,
  EDGAR jobs refuse to start (no request is sent). A SEC 403 blocks SEC jobs for 10 minutes
  without stalling other jobs.
- **C6 Calendars.** Scheduled jobs run only on their calendar's days. Calendars: `federal`
  (weekday, not a US federal holiday) for Fiscal Data, BLS and EDGAR feed/reconcile; `bond`
  (federal + Good Friday closed) for TreasuryDirect; `nyse` for Nasdaq Trader. On 2026-10-12
  (Columbus Day) federal and bond jobs skip and NYSE jobs run; on 2026-04-03 (Good Friday)
  federal jobs run and bond and NYSE jobs skip.
- **C7 Resume.** A failed run (or a failed key within a run) records the error, leaves that
  key's watermark unchanged, and the next run resumes from it. Missed runs while the Mac slept
  are caught up once on wake or startup.
- **C8 Alerts.** After each job the rules run: zero new rows (no row dated after
  `watermark_before`) on a due day, stale watermark, value jump past a per-series threshold,
  row-count swing > ±2% on full-file snapshots, plus `calendar_stale`, `auction_mismatch`,
  `schema_drift`. Alerts land in an `alert` table (one per job, rule, subject and day), the UI,
  and an optional webhook.
- **C9 Safe symbol history.** A Nasdaq Trader file missing its `File Creation Time` trailer is
  rejected; a snapshot whose row count moves more than ±2% from the previous one is stored but
  not diffed, and alerts. The first snapshot has no swing check.
- **C10 Ticker join.** `ticker_norm` joins Nasdaq Trader symbols to EDGAR companies across class
  and preferred notations (`BRK.B`/`BRK-B`/`BRK/B` → `BRK.B`; `ABR$D`/`ABR-PD` → `ABR.PD`);
  unmatched symbols are listed, never dropped.
- **C11 UI.** `usfin serve` runs the scheduler and a local web UI at http://127.0.0.1:8660 showing
  job runs and watermarks (with run-now), alerts (with ack), a series browser (BLS with as-of,
  Fiscal Data, Treasury auctions), EDGAR company filings/facts, an XBRL cross-section screen,
  unmatched symbols, and CSV export of every table view.
- **C12 Gate.** `uv run ruff check . && uv run pytest -q && uv build -q` is green at every commit.
- **C13 Filing documents.** Recorded Form 4 and Schedule 13D/G XML fixtures parse to the
  stated `insider_txn` / `ownership_stake` rows, including a 13D/G reporting person with no CIK.
- **C14 Filing completeness.** Feed polls plus the daily-index reconcile land every accession in
  a day's `master.idx` exactly once in `edgar_filing`, with every listed CIK in
  `edgar_filing_party`.
- **C15 Auction cross-check.** A Fiscal Data vs TreasuryDirect difference in high yield/rate or
  offering amount for the same (cusip, auction_date) raises exactly one `auction_mismatch` alert.
- **C16 Schema drift.** A Fiscal Data or TreasuryDirect column the parser has not seen is added to
  its table (`ALTER TABLE ADD COLUMN`) or kept in `payload`, and raises `schema_drift`; nothing
  is dropped.
- **C17 Bulk + retention.** EDGAR bulk ZIPs load member by member without extracting to disk; the
  weekly reconcile re-parses only members whose CRC changed; `usfin raw prune` keeps the last 2
  blobs per bulk dataset and marks older `raw_response` rows `pruned_at`.
- **C18 Ops.** `usfin doctor` reports env vars, config, DB path and whether another process holds
  the DB lock, raw dir size and free disk; the launchd plist example passes `plutil -lint`.

## What

In scope:

- Core: raw store (with bulk prune), run log, watermarks, per-host rate limiter, HTTP client with
  retries, three calendars, job framework with `run` / `reparse`, data-quality alerts,
  scheduler with catch-up.
- **Treasury Fiscal Data**: one generic fetcher driven by a dataset registry — DTS operating cash
  balance, Debt to the Penny, auctions (cross-check only), average interest rates, MTS table 1,
  rates of exchange.
- **TreasuryDirect**: announced and auctioned securities daily, one-off history backfill,
  `tsy_security` as the source of truth for auctions, cross-check against Fiscal Data auctions.
- **BLS**: seven starter series, versioned observations, release-calendar driven pulls with
  retry to 12:00 ET, ALFRED backfill of first-published history (needs `FRED_API_KEY`).
- **SEC EDGAR**: ticker↔CIK map, submissions (per-CIK incl. paged history, and bulk ZIP backfill),
  XBRL companyfacts (per-CIK and bulk ZIP, `us-gaap` + `dei` only), 15-minute latest-filings poll,
  nightly daily-index reconcile, Form 4 → `insider_txn`, Schedule 13D/G → `ownership_stake`,
  frames cross-section with cache, a small versioned concept→tags map for screens.
- **Nasdaq Trader**: daily `nasdaqlisted.txt` + `otherlisted.txt` snapshots, type-2
  `symbol_history`, `ticker_norm` join to EDGAR, unmatched list.
- **UI**: the pages in C11, server-rendered, read-mostly, bound to 127.0.0.1.
- **Ops**: `usfin doctor`, a launchd plist example for `usfin serve`, README run docs.

**Out of scope:**

- FINRA TRACE (all of it — credentials and dataset names unconfirmed). Can be a later plan.
- SEC 13F holdings, XBRL segment/dimensional data, taxonomies other than `us-gaap` and `dei`,
  full-text search, filing documents other than Form 4 and 13D/G XML, pre-XML (HTML/text)
  13D/G filings (skipped and counted).
- PostgreSQL, Docker, cloud deployment, CI, multi-user access, UI authentication (localhost only).
- A read replica, SQL endpoint or other analysis access path beyond opening the DuckDB file
  between jobs (decision A).
- Power management (pmset wake schedules, caffeinate) — catch-up on wake instead (decision E).
- Notification channels beyond one generic JSON webhook (no Slack/Telegram/email integrations).
- Derived indicators, analytics or backtesting code — this plan only lands and serves data.
- Back-filling Nasdaq Trader history (the source has none) or BLS vintages before the ALFRED
  backfill's coverage.
- Mobile layout polish beyond "readable at phone width".

## Who

- **User:** Tommy Yohanes — runs it locally, reads the UI, queries DuckDB for analysis.
- **Owner/reviewer:** Tommy reviews the branch and decides on the PR.
- **Doers:** one fresh Claude subagent per slice, dispatched sequentially by the planning session,
  which verifies each slice against git; a fresh reviewer subagent critiqued this plan first.

## Where

Push target: remote `origin` → `https://github.com/tomtomtomdev/sec-edgar.git` (private, created
for this plan with `gh repo create` on the user's yes), branch `feat/us-fin-ingest` (new; `main`
holds only the initial commit). `git ls-remote --heads origin` answered 2026-10-07.

Repo: `/Users/tomtomtomtom/Projects/sec-edgar` — empty except README, `.gitignore`,
`docs/research/api-research.md`. Everything below is new:

```
pyproject.toml, uv.lock, .python-version, .env.example
config/                      (USFIN_CONFIG, default ./config; not shipped in the wheel)
  hosts.yaml          per-host rate limits + headers
  calendars.yaml      manual holiday overrides (federal / bond / nyse)
  fiscal_data.yaml    Fiscal Data dataset registry
  bls.yaml            BLS series list, FRED IDs, jump thresholds
  edgar.yaml          watchlist CIKs
  concepts.yaml       standard concept → candidate XBRL tags (versioned)
  schedule.yaml       cron entries (ET), calendar, due-day rule per job
src/usfin/
  cli.py              typer app `usfin`
  settings.py         env + paths
  store.py            DuckDB sessions (open per use, lock wait), migrations runner
  migrations/NNN_*.sql
  raw.py              content-addressed raw store + prune
  http.py             client, retries, SEC block, archive-every-response
  ratelimit.py        per-host token bucket
  calendars.py        federal / bond / nyse day logic
  jobs.py             Job protocol, registry, run log, watermarks, run/reparse, chaining
  alerts.py           DQ rules, alert table, webhook
  scheduler.py        APScheduler wiring, calendar guard, catch-up
  doctor.py
  sources/fiscal.py, treasurydirect.py, bls.py, bls_calendar.py, alfred.py, nasdaqtrader.py
  sources/edgar/tickers.py, submissions.py, companyfacts.py, bulk.py, feed.py,
                dailyindex.py, form4.py, sched13.py, frames.py, concepts.py
  web/app.py, web/templates/*.html, web/static/style.css
tests/  test_*.py, fixtures/<source>/...
deploy/com.tuntun.usfin.serve.plist.example
```

Data and raw blobs live outside git: `data/usfin.duckdb` (`USFIN_DB`), `raw/` (`USFIN_RAW`).
Runs on this Mac (macOS, Apple Silicon) only.

## When

Order follows the research doc's build order, each phase usable on its own:

1. **Core** (scaffold, store, raw, HTTP + limiter, jobs, calendars) — everything depends on it.
2. **Fiscal Data** — proves the core end to end on the simplest no-auth API.
3. **Alerts, scheduler, UI shell** — the app becomes a running service with a job-status page.
4. **TreasuryDirect**, then the auction cross-check (needs Fiscal auctions).
5. **BLS** — versioning, calendar, retry; ALFRED last (key-gated).
6. **EDGAR** — tickers, submissions, companyfacts, then feed + document queue, reconcile,
   Form 4, 13D/G, bulk backfills + weekly reconcile + prune, frames.
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
| Calendars | holidays (`holidays.US(observed=True)`, `holidays.NYSE()`) | 0.106 | proposed, approved |
| Scheduler | APScheduler (BackgroundScheduler, CronTrigger/DateTrigger tz America/New_York) | 3.11.3 | proposed, approved |
| Web UI | FastAPI + uvicorn + Jinja2, server-rendered, inline SVG charts, no JS build | 0.142.2 / 0.54.0 / 3.1.6 | proposed, approved |
| XML | defusedxml (ElementTree API) | 0.7.1 | proposed, approved |
| Tests | pytest, respx (httpx mocking), FastAPI TestClient | 9.1.1 / 0.23.1 | proposed; used in siblings |
| Lint | ruff, line length 100 | 0.16.10 | proposed; used in siblings |

Versions are the ones the S0 `uv.lock` must pin (the review resolved all of them together on
CPython 3.13 and ran the gate in a scaffold copy); a slice that needs a different version or an
unlisted package stops and reports it.

**New dependencies:** this is a new project, so all of the above are new to it. duckdb, httpx,
typer, pyyaml, python-dotenv, pytest, respx and ruff are already used in the sibling repos.
holidays, apscheduler, fastapi, uvicorn, jinja2 and defusedxml are new to this user's projects
and were **approved by the user on 2026-10-07**. No pandas (CSV export uses `csv`, charts are
hand-built SVG). No `python-multipart`: every POST endpoint takes only path parameters.

### Approach

**Conventions (every slice).**
- Package `usfin` under `src/usfin/`, tests in `tests/`, fixtures in `tests/fixtures/<source>/`.
- TDD. Unit tests never touch the network: `respx` mocks HTTP, fixtures are recorded from the
  real endpoint and trimmed small (a few KB; never invented). Tests that hit real endpoints are
  `@pytest.mark.live` and skipped by default (`uv run pytest -m live`).
- Tests use a temp DuckDB (`tmp_path`), temp raw dir and a temp/fixture config dir; never
  `data/`, `raw/` or the user's `.env`.
- All timestamps stored as naive UTC `TIMESTAMP`; schedules and calendars reason in
  `America/New_York` (`zoneinfo`). Dates from sources stay `DATE`.
- Secrets and paths only via env / `.env` (see `.env.example`): `SEC_USER_AGENT` (required for
  EDGAR; the user sets it), `BLS_API_KEY` (optional), `FRED_API_KEY` (ALFRED slice only),
  `ALERT_WEBHOOK_URL` (optional), `USFIN_DB` (default `data/usfin.duckdb`), `USFIN_RAW`
  (default `raw/`), `USFIN_CONFIG` (default `config/`), `USFIN_LOCK_WAIT` (seconds, default 600).
- Non-SEC hosts get `User-Agent: usfin/<version> (+https://github.com/tomtomtomdev/sec-edgar)`.
- Commit style follows the sibling repos: `S<n>: <what the slice found>`; plan-only commits
  `plan(us-fin-ingest): …`.

**Storage and concurrency (decision A: open per use).** DuckDB allows one process to hold the
file (a second process can't even open it read-only while a writer holds it). So nothing holds
the DB between uses: `store.session()` opens a connection, yields it, closes it. Within one
process every connection uses the same config, so DuckDB shares one instance and in-process
opens never conflict. Across processes, opening retries with backoff: jobs and write CLI
commands wait up to `USFIN_LOCK_WAIT` seconds, then fail with `DatabaseLocked` (the job_run is
recorded as failed when the run log can be written, otherwise logged — watermarks untouched,
C7). UI requests wait up to 5 s, then render "database busy — another process holds the lock".
The user's analysis process can open the file between jobs; a long-held external connection
makes jobs wait and fail visibly, never corrupts. In `usfin serve` jobs run in a
**single-worker** executor (`max_workers=1`, `max_instances=1`, `coalesce=True`), so writes never
race. Run-now (UI or `usfin run` while serving is not special) enqueues into the scheduler with
`next_run_time=now`, never in the request thread. `usfin run <job>` from a shell runs the job in
that process directly (lock wait applies) and ignores the calendar guard (manual intent).

**Schema migrations.** Plain SQL files `src/usfin/migrations/NNN_name.sql`, applied in order and
recorded in `schema_version`. Each slice that adds tables adds a migration; none edits an old one.

**Core tables.**
- `raw_response(id BIGINT PK, source, dataset, url, request_sha256 NULL, fetched_at,
  http_status, sha256, blob_path, bytes, content_type, pruned_at NULL)`.
- `job_run(id PK, job, started_at, finished_at, status running|ok|failed|skipped, rows_upserted,
  watermark_before JSON, watermark_after JSON, error, trigger scheduled|manual|catchup|chained)`.
- `watermark(job, key, value VARCHAR, updated_at)` PK (`job`, `key`) — value is a date,
  timestamp or accession, interpreted by the job; capped at today for dates.
- `alert(id PK, job, rule, subject, detail JSON, raised_on DATE, raised_at, acknowledged_at
  NULL)`, unique (`job, rule, subject, raised_on`).
- `id` columns are `BIGINT PRIMARY KEY DEFAULT nextval('<table>_id_seq')`; `status` and
  `trigger` are `CHECK`-constrained. `job_run.status` also allows `running` (amended in S1: the
  runner opens the row before the job finishes, so it needs a non-final status).
  `schema_version(version PK, name, applied_at)`. `store.session()` applies pending migrations on
  open (`apply_migrations=False` to skip); `usfin db migrate` / `usfin db info` exit 1 with
  "database busy — …" on `DatabaseLocked`.

**Raw store (C1, C17).** Body is gzipped to `raw/<source>/<sha[:2]>/<sha>.gz` (sha of the
uncompressed body); identical bodies share one blob, every fetch gets its own `raw_response`
row. Bodies larger than 64 MB (EDGAR bulk ZIPs) are streamed to disk while hashing, stored
uncompressed as `<sha>.zip`, never held in memory. `raw.open(con, raw_id)` returns a readable
stream (amended in S2: it takes the session, like every store call; a pruned row raises
`RawPruned`). `usfin raw prune [--keep 2] [--dry-run]` deletes bulk blobs (datasets
`bulk_submissions`, `bulk_companyfacts`) older than the newest N per dataset and sets `pruned_at`;
blobs are ranked by their latest fetch, so an unchanged ZIP downloaded twice counts as one blob
(clarified in S2). Nothing else is ever pruned (decision B). Reparse skips pruned rows with a
warning.

**HTTP (C5).** `usfin.http.Client.get/post(url, source=, dataset=, …)`: looks up the host in
`config/hosts.yaml` for rate and headers (`www.sec.gov` and `data.sec.gov` share one 8 rps
bucket, others default 4 rps), acquires a token from a thread-safe token bucket, sends, retries
429/5xx/transport errors with exponential backoff (honouring `Retry-After`, max 5 tries). A SEC
403 sets a process-wide `sec_blocked_until = now + 10 min` and raises; SEC jobs check it at start
and record `skipped` until then — no sleeping in the worker. Every final response (success or
error status) is archived through the raw store before returning `(raw_id, response)`. SEC hosts
require `SEC_USER_AGENT`; missing → `ConfigError` before any socket opens.
Amended in S3: `Client(con, …)` takes the session, like `raw.save`/`raw.open`. The bucket is a
token bucket of capacity 1 (no banked burst: grants ≥ 1/rate apart, so ≤ rate in any 1 s window).
`hosts.yaml` is `default_rate` + named `buckets: {name: {hosts, rate, headers}}`; a SEC bucket
above 8 rps is a `ConfigError` on load; which hosts are SEC (`sec.gov`, `*.sec.gov`) is decided by
code, not config. Only the final attempt is archived (retried 429/5xx bodies are not); a 5xx still
failing after 5 tries is archived and raises `HttpError(raw_id, response)`, a transport failure
raises `HttpError` with no `raw_id`; a SEC 403 is archived, not retried, and raises `SecBlocked`
(a `HttpError`). The process-wide block is `http.SEC_BLOCK` (`.blocked()`, `.remaining()`) and
the limiters `ratelimit.LIMITERS`. `usfin http get URL [--source] [--dataset]` is the manual probe.

**Calendars (C6).** `is_federal_day(d)`: weekday, not in `holidays.US(observed=True)`;
`is_bond_day(d)`: federal day and not Good Friday; `is_nyse_day(d)`: weekday, not in
`holidays.NYSE()`. All apply `config/calendars.yaml` overrides (`<calendar>: {closed: […], open:
[…]}`) for SIFMA/NYSE one-offs. `prev_day/next_day(calendar, d)` and `days_back(calendar, d, n)`.
*(Amended after S4.)* The functions are methods of a `Calendars` object built by
`calendars.load_calendars(config_dir)` (the overrides travel with it): `is_federal_day`,
`is_bond_day`, `is_nyse_day`, `is_day(calendar, d)` with calendar `federal|bond|nyse|any` (`any` =
every day, no overrides), `prev_day`/`next_day` strictly before/after `d`, `days_back(cal, d, 0)`
= `d`. Good Friday is computed (Gregorian Easter − 2), not looked up. A `federal` override moves
`bond` too (bond = federal minus Good Friday); a `bond` override wins over both. Bad override
files (unknown calendar or key, non-date, a date both closed and open) are `ConfigError`.
`holidays.NYSE()` already carries NYSE days of mourning (2018-12-05, 2025-01-09) but
`holidays.US` carries no one-off executive-order federal closures (2025-01-09) — those go in
`calendars.yaml` by hand. `usfin calendar YYYY-MM-DD` prints the holiday names and the three
verdicts.

**Jobs (C2, C3, C7).** A job is registered in `jobs.REGISTRY` by name with:
`fetch(ctx) -> list[raw_id]`, `parsers: {dataset: parse(ctx, raw_id) -> rows}`, watermark keys,
and optional `then: [job names]` (chained jobs run after it succeeds, each with its own
`job_run`, trigger `chained`). The runner opens a `job_run`, loads the job's watermarks, fetches,
parses each raw response in its own transaction, advances each key's watermark only when that
key fully succeeded, and closes the run `ok`, or `failed` with the error (keys that succeeded
keep their advance; failed keys keep theirs). Upserts are `INSERT … ON CONFLICT (pk) DO UPDATE`
on natural keys and only touch `updated_at`-style columns when a value changed.
`usfin reparse <job> [--since DATE]` selects `raw_response` rows whose `dataset` is in the job's
parser map, in `fetched_at` order, and calls the parsers with HTTP disabled (a test injects a
transport that fails on any request). Versioned jobs (BLS) first delete the rows derived from
the selected raw responses (`raw_id`) and replay in order, so history is rebuilt, not appended.
Commands: `usfin run <job>`, `usfin jobs` (last run + watermarks per job), `usfin reparse`.

**Job names (complete list).** `fiscal`, `auction_xcheck`, `treasurydirect`, `bls`,
`bls_calendar`, `edgar_tickers`, `edgar_feed`, `edgar_submissions`, `edgar_companyfacts`,
`edgar_form4`, `edgar_sched13`, `edgar_reconcile`, `edgar_bulk` (weekly reconcile; backfill
uses the same parsers via CLI), `symbols`. Raw `dataset` values are named in each section below.

**Alerts (C8).** Rules run after each job, configured per job in `schedule.yaml`:
`zero_new_rows` (no normalized row dated after `watermark_before` on a *due day*),
`stale_watermark(n)` (watermark older than n due days), `value_jump(series, pct)` (flag, never
block), `row_count_swing(pct=2)`, and source-raised `calendar_stale`, `auction_mismatch`,
`schema_drift`, `db_locked`. Due days per job: `fiscal` every federal day (DTS); MTS only the 8th
federal day of the month; `bls` BLS release days only; `symbols` NYSE days; `edgar_feed` federal
days. An alert is inserted once per (job, rule, subject, raised_on) and POSTed as JSON to
`ALERT_WEBHOOK_URL` if set (failure to post is logged, not fatal). `usfin alerts [--all]`,
`usfin alerts ack <id>`.

**Treasury Fiscal Data.** Base `https://api.fiscaldata.treasury.gov/services/api/fiscal_service`
(all six endpoint versions in the research doc verified by the review). `config/fiscal_data.yaml`
entries: `name, endpoint, pk: [...], date_field (default record_date), lookback_days: 10,
backfill_from, jump_fields`. Registry names (tables `fd_<name>`, raw dataset `fd_<name>`):
`dts_operating_cash`, `debt_to_penny`, `auctions` (`date_field: auction_date`, since its
`record_date` is the issue date and runs ahead), `avg_interest_rates`, `mts_table_1`,
`rates_of_exchange`. Each entry's pk columns are confirmed against a recorded live response in
the slice that adds it. Fetch: `filter=<date_field>:gte:<min(watermark, today) - 10d>`,
`sort=<date_field>`, `page[size]=10000` (brackets URL-encoded), loop pages to
`meta.total-pages`; **never send `fields=`** (it aggregates). Parse: cast via `meta.dataTypes`
(`"null"` string → NULL): `DATE` → DATE; `CURRENCY\d*` → DECIMAL(24,4); `INTEGER|YEAR|QUARTER|
MONTH|DAY` → BIGINT; `NUMBER|PERCENTAGE` → DOUBLE; anything else → VARCHAR. Each dataset's table
is created from its dataTypes on first parse; an unseen column is added with `ALTER TABLE ADD
COLUMN` and raises `schema_drift` (C16). Job `fiscal` runs every registry entry; watermark key =
registry name; one dataset failing fails the run but the others commit. First run with no
watermark backfills from `backfill_from` (default 2000-01-01). Chained: `then: [auction_xcheck]`.

**TreasuryDirect.** Base `https://www.treasurydirect.gov/TA_WS/securities/`. Job
`treasurydirect` (raw datasets `td_announced`, `td_auctioned`, `td_search`):
`announced?format=json`, then `auctioned?format=json&days=7` when the watermark (latest
auction_date stored) is within 7 days, otherwise
`search?startDate=<watermark-7d>&endDate=<today>&dateFieldName=auctionDate&format=json`
(camelCase parameters — lowercase ones are ignored and return the whole history). Backfill:
`usfin td backfill [--from YYYY-MM-DD]` walks monthly `search` windows; the earliest date the
feed returns is probed once and recorded in this plan. Parse: `…T00:00:00` → DATE, `""` → NULL.
Table `tsy_security` PK (`cusip`, `auction_date`); typed columns `security_type, security_term,
offering_amt, high_yield, high_discount_rate, high_investment_rate, price_per100, bid_to_cover,
primary_dealer_accepted, indirect_accepted, direct_accepted, issue_date, maturity_date, status
announced|auctioned` plus `payload JSON` (unknown fields stay in payload; a new key raises
`schema_drift`). A field map handles old vs new field names, verified against a 2010 and a
current fixture. `status = auctioned` when any of highYield / highDiscountRate /
highInvestmentRate is non-empty. An upsert never lowers `status` and never overwrites a non-NULL
result field with NULL (the `announced` feed carries already-auctioned rows with blanks).
Job `auction_xcheck` compares `fd_auctions` to `tsy_security` on (cusip, auction_date) for high
yield/rate and offering amount; each mismatch raises one `auction_mismatch` alert (C15).

**BLS (C4).** `config/bls.yaml`: series ID, FRED ID, jump threshold:
`CUSR0000SA0`→`CPIAUCSL`, `CUSR0000SA0L1E`→`CPILFESL`, `WPSFD4`→`PPIFIS`,
`CES0000000001`→`PAYEMS`, `LNS14000000`→`UNRATE`, `CES0500000003`→`CES0500000003`,
`JTS000000000000000JOL`→`JTSJOL`. Client (raw dataset `bls_timeseries`): v2
`POST https://api.bls.gov/publicAPI/v2/timeseries/data/` with `registrationkey` and
`catalog: true` when `BLS_API_KEY` is set; otherwise v1 (no key: 25 queries/day, 25 series,
10 years — 7 series fit one query; no catalog, so `bls_series` titles come from `bls.yaml`).
`usfin bls validate` checks every configured ID returns data. Tables `bls_series(series_id PK,
title, seasonality, units, decimals)` and `bls_observation(series_id, year, period, value,
footnotes, first_seen_at, raw_id, source bls|alfred)` PK (`series_id, year, period,
first_seen_at`). `first_seen_at` = `raw_response.fetched_at`, never parse time. A new row is
written only when value (compared at the series' published `decimals`) or footnotes differ from
the latest row for that period as of that raw response. Table macros `bls_as_of(ts)` and
`xbrl_as_of(d)` (C4). Release days: job `bls_calendar` (Mon 06:00 ET, raw dataset `bls_ics`)
fetches the BLS release-schedule iCalendar file (URL recorded in its slice; the review fetched
it on 2026-10-07 and it covered dates to 2026-12-30), parses only `VEVENT` blocks
(`DTSTART;TZID=US-Eastern:` + `SUMMARY`) into `bls_release(date, release_name)`, mapping release
names to series in `bls.yaml`. No future release dates beyond 30 days → `calendar_stale` alert.
Job `bls` runs 09:00 ET on federal days (watermark key per series = latest period). On a release
day whose expected period is absent, it schedules a one-shot `DateTrigger` job id `bls_retry`
(`replace_existing=True`) for now + 10 min (+ 15 min without a key, to stay under v1's daily
cap) while ET time < 12:00; then raises `stale_watermark`. Not a release day → one pull anyway.
ALFRED (`usfin bls alfred-backfill`, raw dataset `alfred_observations`):
`https://api.stlouisfed.org/fred/series/observations?series_id=…&realtime_start=1776-07-04&realtime_end=9999-12-31&file_type=json`,
each vintage becomes a row with `source='alfred'`, `first_seen_at = realtime_start` (00:00 ET),
only for vintages before the first BLS raw response for that series; reparse replays ALFRED rows
first, then BLS raw in `fetched_at` order. Blocks without `FRED_API_KEY`.

**SEC EDGAR.** All requests via `http.Client` with the SEC bucket. CIK always 10-digit
zero-padded in URLs, stored as BIGINT.
- Tickers (job `edgar_tickers`, 07:15 ET daily, raw dataset `edgar_tickers`):
  `https://www.sec.gov/files/company_tickers.json` → `edgar_company(cik, ticker, title,
  ticker_norm, active, updated_at)` PK (`cik`, `ticker`); `updated_at` changes only when a value
  changes; a pair missing from the latest file gets `active=false`.
- Filings: `edgar_filing(accession_no PK, cik, form, filed_at, accepted_at NULL, report_date
  NULL, primary_doc NULL, is_amendment, source feed|index|submissions)` and
  `edgar_filing_party(accession_no, cik, role)` PK (`accession_no, cik, role`). `edgar_filing.cik`
  is the first present of Issuer, Subject, Filer, Filed by, Reporting; on conflict a later source
  fills NULL columns, never blanks them.
- Submissions (job `edgar_submissions`, raw dataset `edgar_submissions`):
  `data.sec.gov/submissions/CIK##########.json`; parse `filings.recent` columnar arrays and follow
  `filings.files[]` (older pages) → `edgar_filing` (`source='submissions'`).
- Companyfacts (job `edgar_companyfacts`, raw dataset `edgar_companyfacts`):
  `data.sec.gov/api/xbrl/companyfacts/CIK##########.json` → `xbrl_fact(cik, taxonomy, tag, unit,
  accn, period_start, period_end, is_instant, value DOUBLE, fy, fp, form, filed, frame NULL)`, PK
  (`cik, taxonomy, tag, unit, accn, period_start, period_end`); instant facts store
  `period_start = period_end`, `is_instant = true` (DuckDB PKs reject NULL). Only `us-gaap` and
  `dei`.
- Feed (job `edgar_feed`, every 15 min 06:00–22:00 ET on federal days, raw dataset
  `edgar_feed`): `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&count=100`
  paged with `start=` until an entry's `updated` is ≤ the watermark (newest `updated` seen) or 10
  pages. The feed has one entry per role per filing: entries are grouped by accession, every
  (cik, role) goes to `edgar_filing_party`, one row to `edgar_filing` (`source='feed'`). Then
  `pending_doc(accession_no, kind, cik, enqueued_at, done_at NULL)` PK (`accession_no, kind`)
  gets: `submissions` for watchlist CIKs (`config/edgar.yaml`); `companyfacts` for every 10-K,
  10-Q (and /A) filer and for 8-K filers on the watchlist (decision C); `form4` for `4`, `4/A`;
  `sched13` for `SCHEDULE 13D`, `SCHEDULE 13G` (+`/A`) and legacy `SC 13D`, `SC 13G` (+`/A`).
  Chained: `then: [edgar_submissions, edgar_companyfacts, edgar_form4, edgar_sched13]`, each
  draining its `pending_doc` kind (deduped by CIK for submissions/companyfacts).
- Daily index reconcile (job `edgar_reconcile`, 23:30 ET on federal days, raw dataset
  `edgar_master_idx`): `Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx` for federal
  days from max(watermark − 7 days, today − 7 days) to today (404 for a not-yet-published day is
  normal) → `edgar_filing` (`source='index'`) and `edgar_filing_party` (role `index`); missing
  Form 4 / 13D/G accessions are enqueued in `pending_doc` and drained by the same chain (C14).
- Form 4 (job `edgar_form4`, raw datasets `edgar_filing_index`, `edgar_form4_xml`): filing folder
  `Archives/edgar/data/{cik}/{accession_nodash}/index.json` → the ownership `.xml` →
  `insider_txn(accession_no, line_no, issuer_cik, owner_cik, owner_name, is_derivative,
  security_title, txn_date, txn_code, shares, price, acquired_disposed, shares_owned_after,
  direct_indirect)` PK (`accession_no`, `line_no`).
- Schedule 13D/G (job `edgar_sched13`, raw datasets `edgar_filing_index`, `edgar_sched13_xml`):
  structured XML (required since 2024-12-18) → `ownership_stake(accession_no, person_seq,
  filer_cik NULL, filer_name, subject_cik, form, event_date, percent_of_class, shares, filed_at)`
  PK (`accession_no`, `person_seq`). Pre-XML filings are skipped and counted.
- Bulk (raw datasets `bulk_submissions`, `bulk_companyfacts`):
  `Archives/edgar/daily-index/bulkdata/submissions.zip` and
  `Archives/edgar/daily-index/xbrl/companyfacts.zip` streamed into the raw store, then parsed
  member by member without extracting, with the same parsers as the per-CIK jobs; member CRCs
  kept in `edgar_bulk_member(dataset, name, crc, raw_id)`. `usfin edgar backfill
  {submissions|companyfacts} [--limit N] [--cik …]`. Job `edgar_bulk` (Sun 03:00 ET) re-downloads
  both ZIPs, re-parses only members whose CRC changed, then runs `raw prune --keep 2`. The slice
  run uses `--limit`; the full load is an operation documented in README with its measured time
  and DB size; if it exceeds what this Mac handles comfortably, that is reported, not worked
  around.
- Frames (raw dataset `edgar_frames`): `data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json`
  on demand → `frames_cache(key PK, fetched_at, raw_id)`, TTL 24 h; never written to `xbrl_fact`.
- Concept map `config/concepts.yaml` (e.g. `revenue: [Revenues,
  RevenueFromContractWithCustomerExcludingAssessedTax, SalesRevenueNet]`) with `version:`;
  screens pick the first tag a company reports for that period and show which tag was used.

**Nasdaq Trader (C9, C10).** Job `symbols` (07:00 ET NYSE days, raw datasets
`nasdaq_nasdaqlisted`, `nasdaq_otherlisted`):
`https://www.nasdaqtrader.com/dynamic/SymDir/{nasdaqlisted,otherlisted}.txt`, pipe-delimited.
Trailer `File Creation Time: mmddyyyyhh:mm||…` → `snapshot_at` (ET → UTC); no trailer → reject
(failed run for that file, alert). Drop `Test Issue = Y`. Columns differ per file: `symbol` =
`Symbol` (nasdaqlisted) or `ACT Symbol` (otherlisted); `exchange` = `Exchange` (otherlisted) or
`Q` (nasdaqlisted); `cqs_symbol`, `nasdaq_symbol` kept from otherlisted. Table
`symbol_snapshot(file, snapshot_at, symbol, exchange, name, etf, round_lot, financial_status,
market_category, cqs_symbol, nasdaq_symbol)` PK (`file, snapshot_at, symbol`). Row count vs the
previous snapshot of that file outside ±2% → keep the snapshot, skip the diff, alert; first
snapshot → no check. Diff → `symbol_history(symbol, exchange, valid_from, valid_to NULL, name,
etf, …)` PK (`symbol, exchange, valid_from`): new symbol opens a row, gone symbol closes it,
changed name/ETF flag closes and opens. `ticker_norm(s)`: uppercase, strip; `$` → `.P`; `.`,
`-`, `/`, space → `.` (so `BRK-B`, `BRK/B` → `BRK.B`; `ABR$D`, `ABR-PD` → `ABR.PD`; `AAC.U`,
`AAC-U` → `AAC.U`). View `symbol_edgar_match` joins open history rows to active `edgar_company`
rows on `ticker_norm`; `usfin symbols unmatched`.

**Scheduler (C6, C7).** `config/schedule.yaml` lists every job with cron (ET), calendar
(`federal`|`bond`|`nyse`|`any`), due-day rule and alert rules. `usfin serve` builds APScheduler
CronTriggers in `America/New_York` with `misfire_grace_time=3600`; a guard records a `skipped`
job_run on non-calendar days. On startup (and therefore after wake, when a missed fire is past
its grace time) a catch-up pass runs each job once (trigger `catchup`) whose most recent due fire
is later than its last `ok` run's `started_at` (decision E). `usfin schedule` prints the next 5
fire times per job. Final schedule:

| Job | When (ET) | Calendar |
|---|---|---|
| symbols | 07:00 | nyse |
| edgar_tickers | 07:15 | any |
| edgar_feed (+ submissions, companyfacts, form4, sched13 chained) | every 15 min 06:00–22:00 | federal |
| bls | 09:00, retry to 12:00 on release days | federal |
| treasurydirect | 14:00 | bond |
| fiscal (+ auction_xcheck chained) | 17:00 | federal |
| edgar_reconcile (+ form4, sched13 chained) | 23:30 | federal |
| edgar_bulk | Sun 03:00 | any |
| bls_calendar | Mon 06:00 | any |

**Web UI (C11).** FastAPI app in `usfin.web.app`, Jinja2 templates, one CSS file with
light/dark tokens, no JS framework (a few lines of vanilla JS at most). Pages: `/` jobs
(last run, status, watermarks, rows, run-now → `POST /api/jobs/{job}/run`), `/alerts`
(`POST /api/alerts/{id}/ack`), `/series` (BLS series, Fiscal datasets, auctions; table + inline
SVG line chart; `?as_of=` for BLS), `/edgar/{cik}` (filings, key facts via concept map),
`/screen` (one concept × one period across companies from `xbrl_fact`, or frames on demand),
`/symbols/unmatched`. Every table view has `?format=csv`. Bound to 127.0.0.1:8660.

**Ops (C18).** `usfin doctor` (env, config files present, DB path, lock state, raw size, free
disk). `deploy/com.tuntun.usfin.serve.plist.example` runs `uv run usfin serve` with
`WorkingDirectory` set to the repo (so `config/` and `.env` resolve) and `KeepAlive`. README:
setup, `.env`, first backfills, schedule, analysis access between jobs, the full bulk load's
measured time and size.

**Risks and how each is retired.**
- *EDGAR blocking* → shared 8 rps bucket, UA required by code, 403 sets a block window; retired
  by the limiter/UA/403 tests and the first live EDGAR run.
- *Schema drift* (Fiscal Data, TreasuryDirect) → raw store, add-column / payload, `schema_drift`
  alert; retired by the TreasuryDirect 2010-vs-current fixture test and the add-column test.
- *Bulk size* → streamed members, `us-gaap` + `dei` only, `--limit` in slices, prune keeps 2;
  full load measured once and recorded in README.
- *DuckDB single-process lock* → open per use, lock wait, `DatabaseLocked` surfaced as a failed
  run + `db_locked` alert; retired by a two-process contention test.
- *Mac asleep at fire time* → 1 h misfire grace + startup catch-up; retired by a catch-up test
  with a fake clock.
- *BLS v1 daily cap* → one query for all series, 15-min retries without a key (≤ 13 queries on a
  release day); a key lifts it.
- *Endpoint facts not yet verified* (Fiscal pk columns, TD earliest date, BLS iCal URL and
  release-name mapping, 13D/G XML element names) → confirmed against a live response in the
  slice that uses it; the plan is amended in place if wrong.
- *XBRL tag inconsistency* → versioned `concepts.yaml`; screens show the tag used.
- *False delistings* → trailer check + ±2% gate (C9). *Ticker mismatches* → `ticker_norm` +
  unmatched list (C10).

### Commands

- Install: `uv sync`
- **Test:** `uv run pytest -q` (live tests excluded by the default `-m 'not live'`; run them with
  `uv run pytest -m live`)
- **Build (gate):** `uv run ruff check . && uv run pytest -q && uv build -q`
- **Run:** `uv run usfin <command>` — e.g. `uv run usfin run fiscal`, `uv run usfin jobs`,
  `uv run usfin serve` (scheduler + UI on http://127.0.0.1:8660).

## Review

Independent reviewer subagent, 2026-10-07 (25 findings, 13 live requests, local checks on
DuckDB 1.5.6 / holidays 0.106 / APScheduler 3.11.3). Decisions by the user 2026-10-08.

1. TreasuryDirect `search` needs camelCase `startDate/endDate` + `dateFieldName` — fixed.
2. `announced` feed would downgrade auctioned rows — fixed (never lower status / never blank results).
3. EDGAR, BLS, Fiscal Data follow federal days, not bond/NYSE (Good Friday payrolls) — fixed (third calendar, C6 restated).
4. Second process can't open DuckDB while serve holds it — decided: A = open per use (user chose "open per job only"); fixed in Storage and concurrency.
5. Reparse on versioned tables rewrote history — fixed (`raw_id`, `first_seen_at` = fetched_at, delete + ordered replay).
6. Feed has one entry per role; 13D/G form names changed — fixed (`edgar_filing_party`, dedupe, new + legacy form names).
7. Document-parser ownership and job names undefined — fixed (complete job list, `pending_doc`, chaining, reparse by dataset).
8. Fiscal Data type map missed CURRENCY0/YEAR/… — fixed.
9. `auctions_query` record_date runs ahead — fixed (`date_field: auction_date`, watermark capped at today).
10. Watermark semantics inconsistent, 7-day windows ignore outages — fixed (`watermark` table per key, windows reach back to the watermark).
11. Zero-rows alert could never fire — fixed (no row after `watermark_before` on a due day; due days per job).
12. BLS retry would block the single worker — fixed (one-shot DateTrigger; 15 min without key).
13. SEC 403 back-off would stall all jobs — fixed (block window, SEC jobs skip).
14. Mac sleep drops fires silently — fixed (misfire grace 3600, catch-up) — decided: E = catch up on wake, no power management.
15. `as_of` underspecified — fixed (`bls_as_of(ts)`, date = 23:59:59 ET, `xbrl_as_of(d)`).
16. Some tables not idempotent — fixed (snapshot PK, change-only `updated_at`, `active` flag, C2 exclusions named).
17. 13D/G person without CIK breaks PK — fixed (PK accession_no + person_seq).
18. Nasdaq column mapping ambiguous — fixed (ACT symbol, exchange Q, `$` → `.P`, first snapshot rule).
19. *What* items without criteria — fixed (C13–C18 added).
20. ALFRED rounding/ordering — fixed (compare at published decimals, only pre-live vintages, replay order, full FRED map).
21. `config/` not in the wheel — fixed (`USFIN_CONFIG`, plist `WorkingDirectory`).
22. FastAPI forms need python-multipart; default UA for all hosts — fixed (path params only; usfin UA).
23. Fiscal table names inconsistent — fixed (six registry names listed).
24. No slices yet; calendar guard on manual runs unclear — fixed (slices follow; manual `run` ignores the guard).
25. BLS calendar coverage/parsing — fixed (`calendar_stale` on no future dates; VEVENT + TZID parsing).
- Decision B (raw retention): keep the last 2 bulk ZIPs per dataset, everything else forever — decided by user.
- Decision C (companyfacts scope): every 10-K/10-Q filer + watchlist 8-K filers — decided by user.
- Decision D (BLS key): keyless v1 with 15-min retries; key optional — decided by user earlier (keyless fallbacks, keys optional).

## Slices

Every slice closes with the gate (`uv run ruff check . && uv run pytest -q && uv build -q`), its
Run step, its progress row, one commit `S<n>: <finding>`, and a push to `origin feat/us-fin-ingest`.
"Run" steps that need `SEC_USER_AGENT`, `BLS_API_KEY` or `FRED_API_KEY` and find it unset report
a block instead of guessing.

### S0 — Scaffold
Depends on: none
Red: `tests/test_cli.py` — `usfin version` prints the package version; `tests/test_settings.py` — env/`.env` resolution of `USFIN_DB`, `USFIN_RAW`, `USFIN_CONFIG`, `USFIN_LOCK_WAIT` defaults and overrides.
Green: `pyproject.toml` (deps pinned per Tech stack, uv_build, `usfin` script, pytest `live` marker + `addopts = "-m 'not live'"`, ruff 100), `.python-version` 3.13, `uv.lock`, `.env.example`, `src/usfin/{__init__,cli,settings}.py`, empty `config/` files (How › Conventions).
Run: `uv sync && uv run usfin version`
Done when: gate green; `uv.lock` pins exactly the Tech stack versions (C12).

### S1 — Store, migrations, lock wait
Depends on: S0
Red: `tests/test_store.py` — migrations apply once in order and record `schema_version`; re-open applies nothing; core tables `raw_response`, `job_run`, `watermark`, `alert` exist with their PKs/uniques; a second *process* holding the file makes `session(wait=1)` raise `DatabaseLocked` after ~1 s; in-process double open works.
Green: `store.py` session + migration runner, `migrations/001_core.sql` (How › Storage and concurrency, Core tables).
Run: `uv run usfin db migrate && uv run usfin db info` (prints path, version, tables).
Done when: lock contention surfaces as `DatabaseLocked`, never a hang or corruption.

### S2 — Raw store
Depends on: S1
Red: `tests/test_raw.py` — save gzips to `<source>/<sha[:2]>/<sha>.gz` and writes a `raw_response` row; identical bodies share one blob but get two rows; a stream > threshold (threshold patched small) is stored as `<sha>.zip` uncompressed while hashing without reading fully into memory; `open(raw_id)` round-trips; prune keeps newest N per bulk dataset, sets `pruned_at`, deletes blobs, leaves non-bulk untouched; `--dry-run` deletes nothing.
Green: `raw.py`, `usfin raw prune` (How › Raw store).
Run: `uv run usfin raw prune --dry-run` on the empty store (prints 0 candidates).
Done when: C1 storage half and C17 prune half covered.

### S3 — Rate limiter and HTTP client
Depends on: S2
Red: `tests/test_ratelimit.py` — token bucket with fake clock: 8 rps host never exceeds 8 grants in any 1 s window across 4 threads; `www.sec.gov` and `data.sec.gov` share a bucket. `tests/test_http.py` (respx) — every response incl. 500/404 archived before return; 429 with `Retry-After` retried and honoured; 5 failures → error; SEC host without `SEC_USER_AGENT` raises `ConfigError` with zero requests sent; SEC UA header sent when set; non-SEC hosts get the usfin UA; SEC 403 sets the block window and later SEC calls raise `SecBlocked` without sending.
Green: `ratelimit.py`, `http.py`, `config/hosts.yaml` (How › HTTP).
Run: `uv run usfin http get https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny?page%5Bsize%5D=1` prints status, raw_id, bytes.
Done when: C5 covered by tests.

### S4 — Calendars
Depends on: S0
Red: `tests/test_calendars.py` — 2026-10-12: federal False, bond False, nyse True; 2026-04-03: federal True, bond False, nyse False; 2026-11-11 Veterans Day: federal False, nyse True; weekend False everywhere; `calendars.yaml` overrides add a closure and an opening; `prev_day/next_day/days_back`.
Green: `calendars.py`, `config/calendars.yaml` (How › Calendars).
Run: `uv run usfin calendar 2026-10-12` and `… 2026-04-03` print the three verdicts.
Done when: C6 day logic covered.

### S5 — Job runner, run log, watermarks
Depends on: S1, S2, S3
Red: `tests/test_jobs.py` — a fake registered job: run records `job_run` ok with rows and before/after watermarks; a failure records `failed` + error and leaves that key's watermark; multi-key job where one key fails advances only the others; watermark dates capped at today; `usfin jobs` lists last run + watermarks; `run` ignores calendar; DB lock → failed run (or logged) without watermark change.
Green: `jobs.py` (registry, runner, watermark API), `usfin run`, `usfin jobs` (How › Jobs).
Run: `uv run usfin run _demo` (a no-network demo job registered only when `USFIN_DEMO=1`) then `uv run usfin jobs`.
Done when: C7 failure/resume covered.

### S6 — Reparse and chaining
Depends on: S5
Red: `tests/test_reparse.py` — reparse selects raw rows by the job's parser datasets in `fetched_at` order with an HTTP transport that fails on any request; `--since` filters; pruned rows skipped with warning; a versioned job's derived rows are deleted by `raw_id` and replayed; `then:` chains run after success only, each with its own `job_run` (trigger `chained`).
Green: reparse + chaining in `jobs.py`, `usfin reparse` (How › Jobs).
Run: `uv run usfin reparse _demo` with `USFIN_DEMO=1`.
Done when: C1 reparse half covered.

### S7 — Fiscal Data client
Depends on: S3
Red: `tests/test_fiscal_client.py` with recorded fixtures (`tests/fixtures/fiscal/`, two pages of a real response trimmed): filter/sort/page[size] URL with encoded brackets, no `fields=`; page loop to `total-pages`; casting per `dataTypes` incl. `CURRENCY0`, `YEAR`, `"null"`.
Green: `sources/fiscal.py` client + caster (How › Treasury Fiscal Data).
Run: `uv run usfin fd probe debt_to_penny --since 2026-09-01` prints row count, columns, types.
Done when: casting matches the type map for every type seen in the six datasets' fixtures.

### S8 — Fiscal Data registry job
Depends on: S6, S7
Red: `tests/test_fiscal_job.py` — table `fd_<name>` created from dataTypes with registry pk; second run with the same fixtures leaves tables identical (C2); a revised value inside the 10-day overlap is updated (C3); `auctions` uses `auction_date`; an extra column in a later response triggers `ALTER TABLE ADD COLUMN` (alert hook stubbed until S9) (C16); one dataset 500 → others committed, that key's watermark unchanged (C7); reparse rebuilds identical tables (C1).
Green: `config/fiscal_data.yaml` (six entries; pk columns confirmed against live responses and recorded here), job `fiscal`.
Run: `uv run usfin run fiscal` (live, full backfill), then again — second run prints 0 changed rows; `uv run usfin jobs`.
Done when: C2/C3/C16 for Fiscal Data covered; live backfill row counts per dataset recorded in the row.

### S9 — Alerts
Depends on: S8
Red: `tests/test_alerts.py` — `zero_new_rows` fires only on a due day with nothing after `watermark_before`; `stale_watermark(n)`; `value_jump` flags; `row_count_swing` ±2%; dedupe per (job, rule, subject, day); webhook POST via respx and failure tolerated; `usfin alerts`, `ack`; `schema_drift` from S8 now lands as an alert.
Green: `alerts.py`, `migrations` for `alert` if not in S1, rules wired after every job, `config/schedule.yaml` due-day rules for `fiscal` (How › Alerts).
Run: `uv run usfin run fiscal && uv run usfin alerts --all`.
Done when: C8 rules covered.

### S10 — Scheduler and `usfin serve` (API only)
Depends on: S4, S9
Red: `tests/test_scheduler.py` — cron entries from `schedule.yaml` in America/New_York; guard records `skipped` on non-calendar days; single worker, coalesce, misfire 3600; catch-up with a fake clock runs a job once whose due fire was missed; `POST /api/jobs/{job}/run` enqueues (not in request thread); `usfin schedule` prints next 5 fires.
Green: `scheduler.py`, minimal `web/app.py` (health + run endpoint), `usfin serve`, `usfin schedule` (How › Scheduler).
Run: `uv run usfin serve` in background, `curl -XPOST 127.0.0.1:8660/api/jobs/fiscal/run`, watch `usfin jobs`; stop server.
Done when: C6 scheduling and C7 catch-up covered.

### S11 — UI shell: jobs and alerts pages
Depends on: S10
Red: `tests/test_web_jobs.py` (TestClient) — `/` lists every registered job with last run, status, watermarks, run-now form posting to the API; `/alerts` lists open alerts, ack works; `?format=csv` on both; "database busy" page when lock wait exceeded.
Green: templates, `style.css` (light/dark tokens), CSV helper (How › Web UI).
Run: `uv run usfin serve`, `curl -s 127.0.0.1:8660/ | head`, `curl -s '127.0.0.1:8660/alerts?format=csv'`.
Done when: C11 jobs/alerts part covered.

### S12 — TreasuryDirect job
Depends on: S9
Red: `tests/test_treasurydirect.py` — fixtures: announced (incl. already-auctioned rows with blanks), auctioned, a 2010 `search` result and a current one: field map gives identical typed columns; `T00:00:00` → DATE, `""` → NULL; announced never lowers status or blanks results; unknown key → payload + `schema_drift`; window logic picks `auctioned&days=7` vs camelCase `search`; idempotent re-run.
Green: `sources/treasurydirect.py`, job `treasurydirect`, schedule entry 14:00 bond (How › TreasuryDirect).
Run: `uv run usfin run treasurydirect` live; query `tsy_security` counts by status.
Done when: C2/C3/C16 for TreasuryDirect covered.

### S13 — TreasuryDirect backfill
Depends on: S12
Red: `tests/test_td_backfill.py` — monthly windows generated from `--from` to today; each window one `search` request with camelCase params; resumable (skips windows already fetched).
Green: `usfin td backfill`.
Run: probe the earliest date live (record it in How › TreasuryDirect), then `uv run usfin td backfill --from <earliest>`; report rows and time.
Done when: full auction history loaded; earliest date recorded in the plan.

### S14 — Auction cross-check
Depends on: S8, S12
Red: `tests/test_auction_xcheck.py` — matching rows → no alert; a yield or offering difference → exactly one `auction_mismatch` per (cusip, auction_date, day); rows only on one side are counted, not alerted.
Green: job `auction_xcheck`, chained after `fiscal`.
Run: `uv run usfin run fiscal` (chain runs) and `uv run usfin alerts`.
Done when: C15 covered; live mismatch count recorded.

### S15 — BLS client and series config
Depends on: S5
Red: `tests/test_bls_client.py` — v2 request body with key + catalog; v1 without key (no catalog); response parse to (series, year, period, value, footnotes); `bls validate` flags an unknown ID.
Green: `sources/bls.py` client, `config/bls.yaml` (seven series, FRED IDs, thresholds), `bls_series` migration, `usfin bls validate` (How › BLS).
Run: `uv run usfin bls validate` live (keyless v1 if no key).
Done when: all seven IDs validated live, or a wrong one corrected in the plan.

### S16 — BLS versioned observations
Depends on: S6, S15
Red: `tests/test_bls_versions.py` — first fetch inserts rows with `first_seen_at` = raw fetched_at; identical refetch inserts nothing (C2); revised value inserts a new version; value differing only beyond published decimals inserts nothing; `bls_as_of(ts)` and date-means-23:59:59-ET semantics (C4); reparse deletes by raw_id and replays to an identical table.
Green: `bls_observation` migration, job `bls` (current + prior year), `bls_as_of` macro, schedule entry 09:00 federal.
Run: `uv run usfin run bls` twice; second inserts 0; query `bls_as_of(now())` for CPI.
Done when: C4 BLS half covered.

### S17 — BLS release calendar and retry
Depends on: S10, S16
Red: `tests/test_bls_calendar.py` — VEVENT-only parsing with `DTSTART;TZID=US-Eastern:`; release names mapped to series; no future dates within 30 days → `calendar_stale`; on a release day with the period absent, a one-shot `bls_retry` DateTrigger is (re)scheduled at +10 min (+15 without key) until 12:00 ET, then `stale_watermark`; due days = release days.
Green: `sources/bls_calendar.py`, job `bls_calendar`, retry wiring in `scheduler.py`.
Run: `uv run usfin run bls_calendar` live; `uv run usfin bls releases --next 5`.
Done when: the iCal URL and release-name mapping are recorded in How › BLS.

### S18 — ALFRED backfill
Depends on: S16
Red: `tests/test_alfred.py` — fixture vintages become `source='alfred'` rows with `first_seen_at` = realtime_start 00:00 ET; only vintages before the first BLS raw fetch; rounding to the series' decimals; reparse replays ALFRED first then BLS.
Green: `sources/alfred.py`, `usfin bls alfred-backfill` (blocks with a clear message without `FRED_API_KEY`).
Run: `uv run usfin bls alfred-backfill` live (needs `FRED_API_KEY`); `bls_as_of('2020-06-01')` for UNRATE shows the first-published April 2020 value.
Done when: C4 historic vintages available; blocked if no key.

### S19 — EDGAR tickers and ticker_norm
Depends on: S6
Red: `tests/test_edgar_tickers.py` — fixture → `edgar_company` rows; `updated_at` changes only on value change; pair missing from a later file → `active=false`; job refuses without `SEC_USER_AGENT` (no request). `tests/test_ticker_norm.py` — `BRK.B/BRK-B/BRK/B`→`BRK.B`, `ABR$D/ABR-PD`→`ABR.PD`, `AAC.U/AAC-U`→`AAC.U`.
Green: `sources/edgar/tickers.py`, `ticker_norm` in a shared module, job `edgar_tickers`, schedule 07:15 any.
Run: `uv run usfin run edgar_tickers` live; count rows; `usfin edgar company AAPL`.
Done when: C10 normalization covered; C5 UA guard exercised live.

### S20 — EDGAR submissions
Depends on: S19
Red: `tests/test_edgar_submissions.py` — columnar `filings.recent` → `edgar_filing`; `filings.files[]` pages followed; `source` precedence fills NULLs never blanks; idempotent.
Green: `sources/edgar/submissions.py`, `edgar_filing` + `edgar_filing_party` migration, job `edgar_submissions` (drains `pending_doc` kind `submissions`; for now also `usfin edgar submissions --cik`).
Run: `uv run usfin edgar submissions --cik 320193` live (Apple) and a heavy filer with paged files; counts.
Done when: paged history verified live.

### S21 — EDGAR companyfacts
Depends on: S20
Red: `tests/test_edgar_companyfacts.py` — fixture → `xbrl_fact` us-gaap + dei only; instant facts `period_start = period_end`, `is_instant`; restated value under a new accn is a separate row; `xbrl_as_of(d)`; idempotent.
Green: `sources/edgar/companyfacts.py`, migration, job `edgar_companyfacts`, `usfin edgar facts --cik`.
Run: `uv run usfin edgar facts --cik 320193` live; Revenue rows as of two dates differ only by filings.
Done when: C4 XBRL half covered.

### S22 — EDGAR feed and document queue
Depends on: S10, S21
Red: `tests/test_edgar_feed.py` — fixture Atom pages: entries grouped by accession, parties stored per role, `edgar_filing.cik` precedence; paging stops at watermark or 10 pages; `pending_doc` gets the right kinds (10-K/10-Q any filer, 8-K watchlist only, Form 4, new + legacy 13D/G names); chain drains submissions/companyfacts deduped by CIK; SEC block → skipped.
Green: `sources/edgar/feed.py`, `pending_doc` migration, `config/edgar.yaml`, job `edgar_feed` with chain, schedule every 15 min 06–22 federal.
Run: `uv run usfin run edgar_feed` live; counts of filings, parties, pending by kind.
Done when: feed half of C14 covered.

### S23 — EDGAR daily-index reconcile
Depends on: S22
Red: `tests/test_edgar_dailyindex.py` — `master.idx` fixture parse; window from max(watermark−7d, today−7d) over federal days; 404 for today is not an error; every accession once in `edgar_filing`, all CIKs in parties; missing Form 4/13D/G enqueued.
Green: `sources/edgar/dailyindex.py`, job `edgar_reconcile` (chain form4, sched13), schedule 23:30 federal.
Run: `uv run usfin run edgar_reconcile` live; compare one day's idx accession count to `edgar_filing`.
Done when: C14 covered (live day matches exactly).

### S24 — Form 4
Depends on: S22
Red: `tests/test_form4.py` — recorded `index.json` + ownership XML fixtures (one non-derivative, one derivative, one amendment) → expected `insider_txn` rows; idempotent.
Green: `sources/edgar/form4.py`, migration, job `edgar_form4` draining `form4`.
Run: `uv run usfin run edgar_form4` live after a feed run; rows parsed vs pending drained.
Done when: C13 Form 4 half covered.

### S25 — Schedule 13D/G
Depends on: S22
Red: `tests/test_sched13.py` — recorded XML fixtures for 13D and 13G (incl. a reporting person without CIK) → `ownership_stake` rows with `person_seq`; pre-XML filing skipped and counted.
Green: `sources/edgar/sched13.py`, migration, job `edgar_sched13`; XML element names recorded in How › SEC EDGAR.
Run: `uv run usfin run edgar_sched13` live.
Done when: C13 13D/G half covered.

### S26 — EDGAR bulk backfill
Depends on: S21, S2
Red: `tests/test_edgar_bulk.py` — a small fixture ZIP (2 members each) streams to raw as `.zip`; members parsed in place with the per-CIK parsers; `edgar_bulk_member` CRCs stored; `--limit`, `--cik`.
Green: `sources/edgar/bulk.py`, `usfin edgar backfill {submissions|companyfacts}`.
Run: live download of both ZIPs, `--limit 200` each; record download sizes and parse rate.
Done when: C17 streaming half covered.

### S27 — Weekly bulk reconcile and prune
Depends on: S26
Red: `tests/test_edgar_bulk_reconcile.py` — second ZIP with one changed member → only that member reparsed; prune keeps 2 per dataset after the job.
Green: job `edgar_bulk`, schedule Sun 03:00 any.
Run: `uv run usfin run edgar_bulk` live with `--limit` env cap; then the **full** companyfacts + submissions load once — record time and DB size in README and here.
Done when: C17 covered; full-load numbers recorded (or the limit reported if this Mac can't).

### S28 — Frames and concept map
Depends on: S21
Red: `tests/test_frames.py` — frames fixture cached in `frames_cache` with TTL 24 h (fake clock); never touches `xbrl_fact`; `concepts.yaml` picks the first reported tag and reports which.
Green: `sources/edgar/frames.py`, `sources/edgar/concepts.py`, `config/concepts.yaml`, `usfin edgar frame`, `usfin edgar screen revenue CY2025Q2`.
Run: both commands live.
Done when: screen output shows tag used per company.

### S29 — Nasdaq Trader snapshot
Depends on: S9
Red: `tests/test_nasdaq_snapshot.py` — both fixture files parse with the per-file column map; trailer → snapshot_at; missing trailer rejected + alert; test issues dropped; idempotent (PK file, snapshot_at, symbol).
Green: `sources/nasdaqtrader.py` snapshot part, migration, job `symbols`, schedule 07:00 nyse.
Run: `uv run usfin run symbols` live; counts per file.
Done when: C9 trailer half covered.

### S30 — Symbol history diff
Depends on: S29
Red: `tests/test_symbol_history.py` — first snapshot opens rows, no swing check; add/remove/rename/ETF change produce correct type-2 rows; ±2% swing stores snapshot, skips diff, alerts.
Green: diff in `sources/nasdaqtrader.py`.
Run: `uv run usfin run symbols` twice live (second makes no history changes).
Done when: C9 covered.

### S31 — Ticker join and unmatched list
Depends on: S19, S30
Red: `tests/test_symbol_match.py` — view `symbol_edgar_match` joins on `ticker_norm` incl. class and preferred forms; unmatched listed; inactive EDGAR pairs ignored.
Green: view migration, `usfin symbols unmatched`.
Run: live; record match rate.
Done when: C10 covered.

### S32 — UI: series browser
Depends on: S11, S14, S16
Red: `tests/test_web_series.py` — `/series` lists BLS series, Fiscal datasets, auctions; series page renders table + SVG path; `?as_of=` changes BLS values; CSV export.
Green: series templates and SVG helper.
Run: `uv run usfin serve`; fetch `/series/bls/CUSR0000SA0?as_of=2026-01-01` and a Fiscal page.
Done when: C11 series part covered.

### S33 — UI: EDGAR company and screen
Depends on: S28, S32
Red: `tests/test_web_edgar.py` — `/edgar/{cik}` shows filings and key facts by concept; `/screen` concept × period across companies with tag used; CSV.
Green: templates.
Run: serve; open `/edgar/320193` and `/screen?concept=revenue&period=CY2025Q2`.
Done when: C11 EDGAR part covered.

### S34 — UI: unmatched symbols
Depends on: S31, S32
Red: `tests/test_web_symbols.py` — `/symbols/unmatched` lists them with CSV; every table page in the app answers `?format=csv` (one parametrized test over all routes).
Green: template.
Run: serve; fetch page and CSV.
Done when: C11 fully covered.

### S35 — Ops: doctor, launchd, README, live smoke
Depends on: S33, S34
Red: `tests/test_doctor.py` — reports each env var set/unset (never values), config present, DB lock state (with a second process), raw size, free disk.
Green: `doctor.py`, `deploy/com.tuntun.usfin.serve.plist.example`, README (setup, `.env`, backfills, schedule, analysis access between jobs, bulk numbers).
Run: `uv run usfin doctor`; `plutil -lint deploy/*.plist.example`; `usfin serve` for one scheduled feed fire, then `usfin jobs`.
Done when: C18 covered; every job has at least one ok live run.

## Progress

| Slice | Status | Date | Commit | Tests | Run showed |
|-------|--------|------|--------|-------|------------|
| S0 Scaffold | done | 2026-10-08 | 70b5a87 | 10 new (test_cli 2, test_settings 8); suite 10 passed | `uv sync` resolved 43 packages on CPython 3.13.14; `usfin version` printed `0.1.0`; uv.lock pins all 14 Tech stack versions exactly (deps pinned with `==` in pyproject); `uv build` made sdist + wheel. Real env wins over `.env`; `.env` is read without mutating `os.environ`; bad `USFIN_LOCK_WAIT` → `ConfigError`. No plan change. |
| S1 Store, migrations, lock wait | done | 2026-10-08 | 352a7c4 | 12 new (test_store); suite 22 passed | `db migrate` on a fresh `data/usfin.duckdb` printed `applied 001_core`, re-run `schema up to date`; `db info` printed path, `version: 1`, tables `alert, job_run, raw_response, schema_version, watermark`. With a second python process holding the real file, `USFIN_LOCK_WAIT=1 usfin db info` exited 1 after ~1 s with `database busy — …` (DuckDB `IOException` "Could not set lock on file … Conflicting lock is held in … (PID n)"); after release it opened clean. Each migration runs in its own transaction (a broken one rolls back fully). Plan amended (Core tables): `job_run.status` gains `running`; ids from sequences. |
| S2 Raw store | done | 2026-10-08 | d5475d2 | 14 new (test_raw); suite 36 passed | `usfin raw prune --dry-run` on the real (empty) store printed `dry run: 0 candidates, 0 blobs, 0 bytes`, exit 0; `db info` unchanged (version 1). Every body (bytes, stream or chunk iterable) is spooled to a temp file in `CHUNK_SIZE` reads while hashing, then gzipped or renamed to `.zip` — never read whole, no temp left behind. Prune ranks distinct blobs per dataset by latest fetch (an unchanged ZIP fetched twice is one blob), marks rows in one transaction, deletes blobs only no unpruned row still uses. Plan amended (Raw store): `raw.open(con, raw_id)` takes the session; blob-vs-row ranking stated. |
| S3 Rate limiter and HTTP client | done | 2026-10-08 | c6cfb02 | 27 new (test_ratelimit 9, test_http 18); suite 63 passed | `usfin http get …/debt_to_penny?page%5Bsize%5D=1` (live, keyless) printed `status: 200`, `raw_id: 1`, `bytes: 1765`, exit 0; the `raw_response` row has source `manual`, dataset `http_get`, the encoded URL, `application/json`, a request sha, and `raw.open` returns the JSON (decoded: httpx un-gzips before archiving). A second run made raw_id 2 in ~1.4 s. Unsorted, Fiscal Data returns the oldest record first (1993-04-01) — S7 must send `sort=`. No SEC request sent (SEC tested with respx only). Plan amended (HTTP): `Client(con, …)`, capacity-1 bucket, `hosts.yaml` shape, SEC hosts decided by code, only the final attempt archived, error/`SecBlocked` carry `raw_id`. |
| S4 Calendars | done | 2026-10-08 | | 25 new (test_calendars); suite 88 passed | `usfin calendar 2026-10-12` printed `Mon (Columbus Day)` federal closed, bond closed, nyse open; `… 2026-04-03` `Fri (Good Friday)` federal open, bond closed, nyse closed; `… 2026-11-11` Veterans Day federal/bond closed, nyse open; a bad date exits 2. `holidays.NYSE()` already has the 2018/2025 mourning closures, `holidays.US` has no executive-order federal closures (2025-01-09) — overrides file is where those go. Plan amended (Calendars): `Calendars` object from `load_calendars`, `any` calendar, override precedence (federal moves bond, bond wins), strict prev/next, computed Good Friday, `usfin calendar`. |
| S5 Job runner, run log, watermarks | todo | | | | |
| S6 Reparse and chaining | todo | | | | |
| S7 Fiscal Data client | todo | | | | |
| S8 Fiscal Data registry job | todo | | | | |
| S9 Alerts | todo | | | | |
| S10 Scheduler and usfin serve | todo | | | | |
| S11 UI shell: jobs and alerts | todo | | | | |
| S12 TreasuryDirect job | todo | | | | |
| S13 TreasuryDirect backfill | todo | | | | |
| S14 Auction cross-check | todo | | | | |
| S15 BLS client and series config | todo | | | | |
| S16 BLS versioned observations | todo | | | | |
| S17 BLS release calendar and retry | todo | | | | |
| S18 ALFRED backfill | todo | | | | |
| S19 EDGAR tickers and ticker_norm | todo | | | | |
| S20 EDGAR submissions | todo | | | | |
| S21 EDGAR companyfacts | todo | | | | |
| S22 EDGAR feed and document queue | todo | | | | |
| S23 EDGAR daily-index reconcile | todo | | | | |
| S24 Form 4 | todo | | | | |
| S25 Schedule 13D/G | todo | | | | |
| S26 EDGAR bulk backfill | todo | | | | |
| S27 Weekly bulk reconcile and prune | todo | | | | |
| S28 Frames and concept map | todo | | | | |
| S29 Nasdaq Trader snapshot | todo | | | | |
| S30 Symbol history diff | todo | | | | |
| S31 Ticker join and unmatched list | todo | | | | |
| S32 UI: series browser | todo | | | | |
| S33 UI: EDGAR company and screen | todo | | | | |
| S34 UI: unmatched symbols | todo | | | | |
| S35 Ops: doctor, launchd, README, live smoke | todo | | | | |
