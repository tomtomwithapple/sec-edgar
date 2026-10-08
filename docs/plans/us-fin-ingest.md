# US financial data ingestion (usfin)

Status: S29 done — S18 blocked (FRED_API_KEY) — next: S30
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
- **C8 Alerts.** After each job the rules run: zero new rows (no row dated after the watermark
  before the job's first run that day — amended in S9) on a due day, stale watermark, value jump past a per-series threshold,
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
  blobs per bulk dataset and marks older `raw_response` rows `pruned_at`. A bulk download or
  reconcile that would leave less than `USFIN_MIN_FREE_GB` free on the raw or DB volume does not
  start: it records a `skipped` run and raises `disk_low`. The full load runs only after a
  sample run has projected its size and the projection fits under that guard.
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
*(Found in S10.)* starlette 1.7.0 (pulled by fastapi 0.142.2) warns that `TestClient` over httpx is
deprecated in favour of `httpx2`; httpx2 is not in the stack, so pytest filters that one warning
(`pyproject.toml` `filterwarnings`) and tests keep using the pinned httpx.

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
  `BLS_CONTACT_EMAIL` (required for www.bls.gov, the release calendar — added in S17),
  `ALERT_WEBHOOK_URL` (optional), `USFIN_DB` (default `data/usfin.duckdb`), `USFIN_RAW`
  (default `raw/`), `USFIN_CONFIG` (default `config/`), `USFIN_LOCK_WAIT` (seconds, default 600),
  `USFIN_MIN_FREE_GB` (default 10; free-disk floor for bulk downloads), `USFIN_BULK_LIMIT`
  (optional; members per ZIP job `edgar_bulk` parses — added in S27).
- Non-SEC hosts get `User-Agent: usfin/<version> (+https://github.com/tomtomtomdev/sec-edgar)`,
  except www.bls.gov: `usfin/<version> (<BLS_CONTACT_EMAIL>)` (amended in S17: it 403s the URL form).
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
and record `skipped` until then — no sleeping in the worker. *(Wired in S22.)* The runner treats
every job named `edgar_*` as a SEC job (`jobs.is_sec_job`): inside the window its run records
`skipped` with no request (error "SEC requests blocked for another Ns after a 403; not run") and
chains nothing; the run that met the 403 records `failed` and runs no further key (they are listed
"not run: SEC blocked"). Non-SEC jobs are unaffected. Every final response (success or
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
*(Amended in S5.)* Fetch is per key, not per job: a `jobs.Job(name, keys, fetch, parsers,
then=())` has `keys` (a list, or `keys(ctx)` for config-driven ones such as Fiscal registry names)
and `fetch(ctx, key) -> [raw_id]`; that is what lets one key fail while the others commit. A
parser returns its row count and proposes the key's new watermark with `ctx.advance(key, value)`
(dates and ISO date strings capped at `ctx.today`, the America/New_York date; datetimes stored as
naive-UTC ISO; anything else verbatim); the runner writes it only after every raw response of that
key parsed. A raw response whose dataset has no parser fails its key. `ctx` also carries `con`,
`settings`, `now`, `trigger`, `run_id`, `watermarks` (as loaded at start) and a lazy `ctx.client`
(`http.Client` on the run's session; `run_job(transport=)` injects a transport). One session spans
the run, so a DB held past the lock wait means no `job_run` row: `run_job` logs it and returns a
`failed` result with no run id (CLI exit 1), watermarks untouched. `jobs.load_registry()` imports
`SOURCE_MODULES` (each source slice adds its module) and adds the no-network `_demo` job (module
`usfin.demo`, keys `alpha`, `beta`; `USFIN_DEMO_FAIL=<key>` fails that key) only when
`USFIN_DEMO=1`. The runner never consults a calendar (scheduler guard is S10).
*(Amended in S6.)* A job is versioned by naming its derived tables: `Job(…, versioned=(tables))`,
each with a `raw_id` column. `jobs.reparse(con, job, since=)` runs in **one transaction**: delete
the versioned rows of the selected raw ids, replay every parser in `fetched_at, id` order, commit;
a parser error rolls everything back (deletes included) and raises `ReparseFailed` (naming the
raw id; `__cause__` is the parser's error) — CLI exit 1 "nothing changed". HTTP is off by
default (`ctx.client` gets a transport raising `NetworkDisabled`, not an httpx `TransportError`,
so the client does not retry it). Reparse writes no `job_run` (the trigger CHECK has no
`reparse`; `ctx.trigger = "reparse"`, `run_id = None`) and drops `ctx.advance` proposals:
watermarks track what was fetched, not parsed. `--since` is a New York date (00:00 ET, like
`bls_as_of`). A pruned row is skipped with a warning **and its derived versioned rows are kept**
(deleting rows that cannot be replayed would lose history). Chaining: after an `ok` run,
`run_job` runs each `then` job depth first (child, its chain, then the next sibling), each
`_run_one` opening its own session and `job_run` (trigger `chained`); a failed run chains
nothing, a failed child does not stop its siblings, a job runs at most once per chain (cycles
stop), an unknown name becomes a `failed` result with no run id. `RunResult.chained` holds the
children; `usfin run` prints them indented with `(chained)` and exits 1 if any run failed;
`run_job(chain=False)` runs the job alone.
*(Amended in S8.)* `Job.parsers` may also be `parsers(settings) -> {dataset: parse}` for
config-driven jobs (Fiscal's raw datasets come from `fiscal_data.yaml`); the runner and reparse
call `job.parser_map(settings)`. `SOURCE_MODULES` starts with `usfin.sources.fiscal`.
*(Amended in S14.)* A derived job — one that reads tables other jobs filled and has no source of
its own (`auction_xcheck`) — sets `Job(…, compute=fn)`: `fn(ctx, key) -> rows` runs after the
key's fetch and parses, in its own transaction, and its rows count toward `rows_upserted`; such a
job has `fetch` returning `[]` and `parsers={}`, so reparse has nothing to replay for it.
*(Amended in S16.)* `JobContext.memo` is a per-run dict shared by fetch and parsers, for a job
whose one request serves several watermark keys (BLS: seven series keys, one query).
*(Amended in S27.)* A job's `keys` or `fetch` may raise `jobs.JobSkipped(reason)`: the run stops
at that key and records `skipped` with the reason (keys done before it keep their work and
watermarks), or `failed` if an earlier key failed. `edgar_bulk` uses it for the free-disk guard.
*(Amended in S22.)* `Job(…, after_key=fn)`: `fn(ctx, key)` runs after a key's fetch, parses and
compute all succeeded, in its own transaction, never on reparse — bookkeeping such as closing the
key's `pending_doc` rows. SEC jobs skip inside the SEC block window (How › HTTP).

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
*(Amended in S9.)* `zero_new_rows` compares to the key's watermark **before the job's first run
that New York day** (`alerts.day_baseline`), not the run's own `watermark_before`: with the latter
every repeat run on a due day fires, since the first run already moved the watermark. A key with no
such baseline (first load) skips `zero_new_rows`, `value_jump` and `row_count_swing`, so a backfill
never floods alerts with history; `stale_watermark` needs only the current watermark. A job tells
the rules where each key's rows live with `Job(…, observed={key: alerts.Observed(table,
date_column, match, series, values, order, jump_match, snapshot)})` (or `observed(settings)`).
`stale_watermark(n)` fires when more than `n` due days fall in (watermark, today];
`value_jump(pct)` compares each row dated after the baseline with the previous row of its series
(`|Δ|/|prev| > pct`, subject `<key>:<column>:<series>@<date>`); `row_count_swing(pct=2)` compares
the newest snapshot (`snapshot=True`, grouped by `date_column`) with the previous one, only when
the newest is after the baseline. `config/schedule.yaml` shape: `jobs: {<job>: {cron, calendar,
due, alerts: {rule: params}, keys: {<key>: {due, <rule>: params | null}}}}`; `due` is a calendar
name, `none`, or `{calendar, nth_of_month}`, default the job's calendar; unknown rules, bad due
rules and missing `n`/`pct` are `ConfigError`. Rules run in `jobs._execute` after every recorded
run (ok or failed); a rule error is logged and never changes the run. `fiscal` due days: DTS and
Debt to the Penny every federal day, MTS the 8th federal day (`stale_watermark n: 1`), auctions /
avg_interest_rates / rates_of_exchange none (irregular releases: no `zero_new_rows` or
`stale_watermark`, `value_jump` still runs). Thresholds, set from the year to 2026-10 of real data:
25 % default, `debt_to_penny` 5 %, `mts_table_1` 100 %. `fiscal_data.yaml` gains `series` (value_jump
series columns; `rates_of_exchange` uses `country_currency_desc`, since `src_line_nbr` shifts between
releases) and `jump_match` (`dts_operating_cash` checks only the TGA closing balance: the
deposits/withdrawals rows are daily flows that move > 25 % on 9 days in 10). `fiscal.on_schema_drift`
raises `schema_drift` (subject = dataset, detail `{table, columns}`) inside the parse's transaction.
Alerts stored during a run (id above the run's starting max) are POSTed after the run with plain
`httpx.post` (not the archiving client: a webhook reply is not source data), body `{id, job, rule,
subject, detail, raised_on, raised_at}`. `usfin alerts` lists open alerts newest first, `--all`
adds acknowledged ones, `usfin alerts ack <id>` is idempotent and exits 1 for an unknown id.
`db_locked` is not raised yet: a run that cannot open the DB cannot write the `alert` table either
(S5: no `job_run` row); the scheduler (S10) owns raising it once a session opens.
*(Amended in S10.)* In `usfin serve` a run that hits `DatabaseLocked` (`RunResult.locked`, also on a
chained child or a guard skip that could not be logged) is kept in the scheduler's in-memory
`pending_locked`; the next session the scheduler opens — before its next run, or a one-shot retry
every 5 min (`db_locked_flush`, lock wait 5 s) — stores one `db_locked` alert per job and New York
day of the attempt (subject = job, detail `{trigger, at, error}`) and posts it to the webhook.
`/api/health` shows `pending_db_locked`. Pending alerts are lost if serve stops before the DB frees;
`usfin run` from a shell still only exits 1 (it has no later session to raise it in).

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
*(Amended in S7.)* Client API in `usfin.sources.fiscal`: `load_datasets(config_dir)` reads
`fiscal_data.yaml` as `datasets: [{name, endpoint, date_field, lookback_days, backfill_from, pk,
jump_fields}]` into `Dataset`s (S7 lists the six names + endpoints + `auctions`' date field; S8
adds `pk`/`jump_fields`); `since(ds, watermark, today)` = `backfill_from` with no watermark, else
min(wm, today) − lookback; `page_url` builds `filter=…&sort=…&page%5Bnumber%5D=n&page%5Bsize%5D=s`;
`fetch(client, ds, start) -> [Page(raw_id, number, total_pages, types, rows)]` archives every page
(source `fiscal`, dataset `fd_<name>`) and raises `FiscalError(raw_id)` on a non-200;
`total-pages: 0` (no rows yet, e.g. MTS before its release) is one request and still carries the
full `dataTypes`, so S8 can create a table from an empty page; `cast_page(doc)` /
`sql_type(fd_type)` implement the type map; a column missing from `dataTypes` is kept as
`STRING`. The six datasets use exactly `CURRENCY, CURRENCY0, DATE, DAY, INTEGER, MONTH, NUMBER,
PERCENTAGE, QUARTER, STRING, YEAR`; some auction columns typed `STRING` upstream hold numbers
(`price_per100`, `high_price`) and stay VARCHAR. `usfin fd probe <name> [--since]` fetches and
prints rows, columns and types; it archives as raw dataset `fd_probe`, so `reparse fiscal` never
replays probes.
*(Amended in S8.)* Registry pks, each confirmed on 2026-10-08 against the full live history from
2000-01-01 (non-null in every row, unique): `dts_operating_cash` (record_date, account_type),
`debt_to_penny` (record_date), `auctions` (cusip, auction_date), `avg_interest_rates`
(record_date, security_type_desc, security_desc), `mts_table_1` (record_date, line_code_nbr —
`classification_id` is a per-row id, new every month), `rates_of_exchange` (record_date,
src_line_nbr): upstream repeats some (record_date, country, currency) rows (e.g. 2018-12-31 Czech
Republic-Koruna twice; mid-quarter amendments with another `effective_date`), so only the line
number is unique per release. `jump_fields` are set for S9 (none for `auctions`). Job `fiscal`
lives in `sources/fiscal.py` (`fiscal.JOB`): parsers read each page back from the raw store,
`upsert_page` creates `fd_<name>` from the page's `dataTypes` with the registry pk (an empty page
creates it too; a missing pk is a `ConfigError` for that key, a duplicate pk within a page fails
the key), adds unseen columns with `ALTER TABLE ADD COLUMN` and calls
`fiscal.on_schema_drift(ctx, name, {column: dataType})` — a logging stub S9 points at the
`schema_drift` alert. A column absent from a later response keeps its stored values (only the
response's columns are updated). Rows are staged and upserted on pk only when new or changed
(`IS NOT DISTINCT FROM` on every column), so `rows_upserted` counts changes and a repeat run
reports 0. Staging passes the cast rows as **one JSON text parameter** (`json_transform`), not
bound row values: DuckDB 1.5.6 binds large parameter lists at ~30 µs a value (30 s for one
10 000-row auctions page) — that took the live backfill from 97 s to 20 s and a full reparse from
72 s to 1.5 s. The key's watermark is the newest `date_field` value parsed; a response with no
rows leaves it unset. `then: [auction_xcheck]` was left unset in S8 (chaining a job that does not
exist would fail every run) and set in S14 with the job. `auctions_query` already carries
announced auctions dated after today (null results); the cap keeps the watermark at today and
the lookback refetches them.

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
*(Amended in S12.)* There are no old vs new field names: all 11 143 records of the whole history
(the 37.6 MB a lowercase-parameter `search` returns, auction dates 1979-10-31..2026-10-08) carry
the same 120 string-valued keys, so the "field map" is only camelCase → typed column
(`usfin.sources.treasurydirect.FIELD_MAP`); the 2010 and current fixtures map identically. FRNs
report their result only as `highDiscountMargin` (155 FRNs, never a high yield/rate), so
`tsy_security` gains `high_discount_margin DOUBLE` and `status = auctioned` when any of highYield /
highDiscountRate / highInvestmentRate / highDiscountMargin is non-empty (the plan's rule left every
FRN `announced` forever). `bid_to_cover` ← `bidToCoverRatio`, `indirect_accepted` ←
`indirectBidderAccepted`, `direct_accepted` ← `directBidderAccepted`; amounts DECIMAL(24,4), rates
and prices DOUBLE. `announced` lists the latest 250 securities (2026-03-17..2026-10-08 on 10-07 ET),
240 of them already auctioned and carrying the same results as `auctioned`/`search` — no blanks
today; the never-lower / never-blank merge stays as a guard and applies to `payload` key by key
too. `payload` holds the whole upstream record; a key outside the known 120 that no stored payload
has yet raises `schema_drift` once (subject `tsy_security`, detail `{table, fields}`). Watermark
keys: `announced` (newest auction date listed, capped at today) and `results` (newest auction date
*with results*, never moved back); `results` with no watermark asks `auctioned&days=7` (history is
S13's backfill). Table in migration `002_treasurydirect.sql`. Alerts: due days `bond`; `results`
has `stale_watermark n: 3` (since 2015 at most 3 bond days in a row passed without an auction,
since 2023 at most 1) and no `zero_new_rows`, since some bond days have no auction. One 2001-09-11
4-week bill has no results at all and stays `announced`.
*(Recorded in S13.)* **Earliest date: 1979-10-31.** Probed live 2026-10-08: `search` for
1900-01-01..1979-10-30 returns `[]` (200, `application/json`), 1900-01-01..1980-12-31 returns 171
records starting 1979-10-31 (`treasurydirect.EARLIEST_AUCTION`, the default `--from`). `endDate` is
inclusive and a month holds at most 51 records (2020-04; 2026-03: 37, 131 KB), so one request per calendar
month needs no paging. `usfin td backfill [--from]` (`treasurydirect.backfill`) archives each window
as raw dataset `td_search` — the job's own dataset, so `usfin reparse treasurydirect` replays the
backfill with the same parser — and parses it in its own transaction right after the fetch. It
writes no `job_run` (a CLI load, like `edgar backfill`); it moves the `results` watermark only
forward. Resume: a month that has ended and whose exact URL already has a 200 `td_search` response
is skipped; the open month (its window ends at today) is fetched on every run, and whole once it has
ended. A parse error after a successful fetch leaves that month counted as fetched: fix the parser
and `usfin reparse treasurydirect`. The host gets the default 4 rps bucket (`hosts.yaml`).
Job `auction_xcheck` compares `fd_auctions` to `tsy_security` on (cusip, auction_date) for high
yield/rate and offering amount; each mismatch raises one `auction_mismatch` alert (C15).
*(Amended in S14.)* Comparison rules, set against the full history in `data/usfin.duckdb` on
2026-10-08 (7 894 Fiscal Data auctions 2000-01-03..2026-10-08, every one also in `tsy_security`,
none the other way round inside that range): the key must include `auction_date` — 2 214 CUSIPs
were auctioned more than once (reopenings). Like is compared with like, no unit conversion: both
sources store rates in percent and amounts in dollars; a bill has a discount and an investment
rate and no yield, an FRN only a discount margin (`high_discnt_margin` ↔ `high_discount_margin`),
notes/bonds/TIPS a yield. The fields are `offering_amt` (exact, both DECIMAL), `high_yield`,
`high_discount_rate`, `high_investment_rate`, `high_discount_margin` (rounded to 6 decimals:
upstream writes "5.3000" vs "5.300000"; results are published to 3 decimals). A field set on one
side only is **`pending`**, not a mismatch (Fiscal Data may publish a result after TreasuryDirect
has it, and the job runs at 17:00 after TreasuryDirect's 14:00); rows on one side only are
`fd_only` / `td_only`, counted within the two tables' overlap (auction dates from the later of
their earliest dates — TreasuryDirect reaches 1979, the Fiscal Data registry 2000). A pair with
any disagreeing field raises one `auction_mismatch` per New York day (subject
`<cusip>@<auction_date>`, detail `{cusip, auction_date, security_type, security_term, fields:
{field: {fiscal, treasurydirect}}}`); a disagreement that persists raises again the next day.
Every compared pair is kept in table `auction_xcheck(cusip, auction_date, result
matched|mismatch|pending|fd_only|td_only, security_type, diff JSON)` (migration
`003_auction_xcheck.sql`), changed only when a verdict or diff changes, so `rows_upserted` is the
number of pairs whose verdict changed and the counts are `SELECT result, count(*) FROM
auction_xcheck GROUP BY result`. The job has one key (`auctions`), no watermark, no cron (it runs
chained after `fiscal`) and no `schedule.yaml` rules. Module `usfin.sources.auction_xcheck`.

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

*(Amended in S15.)* `config/bls.yaml` is `series: [{id, fred, title, seasonality S|U, units,
decimals, jump_pct}]` (decimals = the precision the live responses carry: CPI/PPI 3, AHE 2, UNRATE 1,
payrolls/JOLTS 0); `bls.load_series`, `bls.request` (v1 25 series/10 years, v2 50/20, checked),
`bls.fetch` (one POST, source `bls`, raw dataset `bls_timeseries`), `bls.parse` →
`Observation(series_id, year, period, value Decimal|None, footnotes)`, `bls.validate`.
`bls_series` is migration `004_bls_series.sql`, filled from `bls.yaml` by `usfin bls validate`
(for IDs that returned data). Live v1 facts (2026-10-08): HTTP 200 with
`Content-Type: text/plain;charset=ISO-8859-1` (the client reads it as JSON regardless); an unknown
ID does **not** fail the request — `status` stays `REQUEST_SUCCEEDED`, the series comes back with
empty `data` and `message` gains "Series does not exist for Series <id>"; v1 also adds "Unable to
get Catalog Data for series JTS000000000000000JOL" though no catalog was asked (harmless);
footnotes are `[{}]` when empty; **2025 M10 is published as value `"-"`** (footnote X/9, the 2025
lapse in appropriations) for CPI, core CPI and UNRATE — parsed as `None`, so S16 must store or skip
a no-value period deliberately. Any `status` other than `REQUEST_SUCCEEDED` (e.g. v1's daily cap) is
a `BlsError`. No v2 response recorded yet (no key); v2 is tested on the request body only.
*(Amended in S16.)* **A period BLS publishes as `"-"` is stored** (decision from the planning
session, 2026-10-08): a `bls_observation` row with `value` NULL and its footnotes (e.g. 2025 M10,
code X/9 "Data unavailable due to the 2025 lapse in appropriations"), so the gap is visible point in
time and a later fill is a new version. Table `bls_observation(series_id, year, period, value
DECIMAL(18,6) NULL, footnotes JSON, first_seen_at, raw_id, source bls|alfred)` and macros
`bls_as_of_utc(ts)` / `bls_as_of(ts)` are migration `005_bls_observation.sql`; `footnotes` is the
JSON list of `{code, text}` (`[]` when BLS sends `[{}]`), compared as parsed JSON. `bls_as_of(ts)`
takes a `DATE` (23:59:59 New York that day), a `TIMESTAMPTZ` (converted to UTC, so `now()` works) or a
plain `TIMESTAMP` (already naive UTC); a date written as a string is read as a timestamp at midnight
UTC, so pass `DATE '…'`. Job `bls` (`bls.JOB`, `SOURCE_MODULES` gains `usfin.sources.bls`): keys
are the configured series IDs (watermark = latest published period `YYYY-Mnn`, never moved back; a
`-` period does not count), but **one request serves every key**: the first key's fetch posts the
single query (this year and last) and the others reuse its raw id through the new per-run
`JobContext.memo`; a failed request is sent once (5 tries) and fails every key with the same error,
watermarks untouched. `bls.store_observations(con, raw_id, …)` writes a version when the value at the
series' `decimals` or the footnotes differ from the latest row first seen at or before the response's
`fetched_at`; a non-200 or non-`REQUEST_SUCCEEDED` raw response stores nothing (so reparse skips it,
not fails). The job also upserts `bls_series`. **`usfin bls validate` archives under the job's own
raw dataset `bls_timeseries`, so it now stores observations too** — otherwise a run after a validate
would date values later than `usfin reparse bls`, which replays the validate response first. Schedule:
`bls` `0 9 * * *` federal, `due: none` until S17 supplies release days; `value_jump` (`jump_pct`) is
not wired yet: `bls_observation` has no date column for `alerts.Observed` (S17 decides).
*(Amended in S17.)* **iCal URL: `https://www.bls.gov/schedule/news_release/bls.ics`** ("Economic
News Release Schedule", linked from bls.gov/schedule/news_release/). Fetched 2026-10-08: 313 VEVENTs,
2025-01-03..2026-12-30, `text/calendar`, LF line ends, no folded lines, every release at 08:30 or
10:00 ET (`DTSTART;TZID=US-Eastern:`), some SUMMARYs with trailing blanks, no (date, name)
repeats. **www.bls.gov answers 403 to the plan's `usfin/<version> (+https://…)` User-Agent** —
with or without browser-style `Accept`/`Accept-Language`/`Sec-Fetch-*` headers or a `From:` header,
and to any UA that carries a URL, even next to an email; it answers 200 to a UA that carries a
contact email and no URL (`usfin/0.1.0 (contact@example.com)` passed), or to a full Chrome header
set (not used: impersonation). api.bls.gov still takes the usfin UA (S15). So requests to
`www.bls.gov` (code-decided host list `http.BLS_WWW_HOSTS`) send `usfin/<version> (<email>)` from the
new env var **`BLS_CONTACT_EMAIL`**; unset or not an email → `ConfigError`, no request (like
`SEC_USER_AGENT`). Release-name mapping (`release` per series in `bls.yaml`, all four present in the
file): CPI and core CPI ← "Consumer Price Index"; PPI final demand ← "Producer Price Index"; payrolls,
UNRATE and AHE ← "Employment Situation"; JOLTS ← "Job Openings and Labor Turnover Survey" (released
at **10:00**, after the 09:00 `bls` run, so its release days always need a retry). Job `bls_calendar`
(`usfin.sources.bls_calendar`, Mon 06:00 `any`, raw dataset `bls_ics`, key `schedule`, no
watermark: a full file) parses VEVENT blocks only (unfolded, unescaped, stripped; `US-Eastern` →
America/New_York, a `Z` time UTC, an unknown TZID or an event without DTSTART/SUMMARY fails the key)
into `bls_release(release_date, release_name, released_at naive UTC, raw_id)` PK (`release_date,
release_name`) (migration `006_bls_release.sql`); each file is authoritative for its own date range
(a moved release loses its old date), a refetch changes nothing, `usfin reparse bls_calendar`
rebuilds it. `calendar_stale` (subject `bls_release`) fires when no stored release is more than 30
days ahead — also after a failed fetch, since the stored calendar still ages. **Due days = release
days:** schedule.yaml `bls: due: {releases: bls_release}`, a new due-rule kind: `alerts.DAY_SOURCES`
maps a name to `fn(con, settings, key) -> days` (`bls.release_days`: the dates of the key's release in
`bls_release`), resolved per key when the rules run. **`value_jump` wired** via view
`bls_observation_dated` (migration 006: the current version of each monthly period with
`period_date` = first of the month, M13 left out) and two new `alerts.Observed` fields: `jump_pct`
(per-key threshold, from `bls.yaml`, overriding the rule's `pct: 25`) and `watermark_date` (the
baseline `2026-M08` → `2026-08-01`). bls has no `zero_new_rows`/generic `stale_watermark` rule: the
watermark is a period, not a date, and the retry decides on release days. **Retry:** `Job.retry(con,
settings, now) -> datetime | None`, asked by the scheduler after every logged run in `usfin serve`;
bls answers "now + 10 min (15 without `BLS_API_KEY`)" while some series released today has the same
watermark as before the day's first run and that time is ≤ 12:00 ET; past it, each such series
raises `stale_watermark` (subject = series, detail `{watermark, release, release_date,
retried_until}`) and no retry. The scheduler (re)adds one-shot `DateTrigger` id `bls_retry`
(`replace_existing=True`, same single worker, run trigger `scheduled`, guard applies); a None answer
removes a pending one. `usfin run bls` from a shell never retries. `usfin bls releases [--next 5]
[--from DATE] [--all]` lists stored releases of configured series (`--all`: every release).

**SEC EDGAR.** All requests via `http.Client` with the SEC bucket. CIK always 10-digit
zero-padded in URLs, stored as BIGINT.
- Tickers (job `edgar_tickers`, 07:15 ET daily, raw dataset `edgar_tickers`):
  `https://www.sec.gov/files/company_tickers.json` → `edgar_company(cik, ticker, title,
  ticker_norm, active, updated_at)` PK (`cik`, `ticker`); `updated_at` changes only when a value
  changes; a pair missing from the latest file gets `active=false`.
  *(Amended in S19.)* Live 2026-10-08: one compact JSON object keyed `"0".."10433"` (SEC's order),
  values `{cik_str: int, ticker, title}`, 798 727 bytes `application/json`; 10 434 pairs, 8 016 CIKs
  (one fund lists 45 tickers), no ticker under two CIKs, no two tickers with the same `ticker_norm`.
  Tickers are uppercase with `-` before a class/suffix: preferreds `-PA`..`-PZ`, classes `-A`/`-B`,
  **units `-UN` (50), warrants `-WT` (61), rights `-RI` (21)**, one `NONE.`. `updated_at` is the
  `fetched_at` of the response that changed the row (title, `ticker_norm` or `active`), not the run
  clock, so reparse rebuilds the identical table (key `company_tickers`, no watermark: a full file;
  an empty or malformed file fails the key instead of deactivating everything; a non-200 stores
  nothing). Migration `007_edgar_company.sql`; `ticker_norm` lives in `usfin.ticker`; `usfin edgar
  company <ticker|cik>` lists every ticker of the matching companies. A SEC 403 or open block window
  makes the run `failed` (the client raises `SecBlocked`), not `skipped` — the plan's "SEC jobs
  record skipped" (How › HTTP) is not wired in the runner yet; S22 (the 15-minute feed) should.
  *(Done in S22: a run inside the window is `skipped`, the run that met the 403 `failed`.)*
- Filings: `edgar_filing(accession_no PK, cik, form, filed_at, accepted_at NULL, report_date
  NULL, primary_doc NULL, is_amendment, source feed|index|submissions)` and
  `edgar_filing_party(accession_no, cik, role)` PK (`accession_no, cik, role`). `edgar_filing.cik`
  is the first present of Issuer, Subject, Filer, Filed by, Reporting; on conflict a later source
  fills NULL columns, never blanks them.
  *(Decided in S22.)* The role precedence **does override** a `cik` a role-less source wrote first
  (submissions: the company whose JSON it is; the daily index: one line per CIK, so its first CIK
  is arbitrary): `filings.settle_cik` sets `cik` to the stored party with the best feed role (the
  lowest CIK on a tie), reading the whole party table, so the result does not depend on source or
  page order and reparse lands on the same value; a filing with no feed role keeps its first CIK.
  The feed's `accepted_at` also replaces one written first (it carries an explicit New York offset;
  the submissions value can be 4–5 h off, see Submissions); every other column stays fill-only.
  Live 2026-10-07 the feed also names roles **Depositor** and **Securitizer** (ABS-15G); they are
  stored as parties and never pick `cik`.
  *(Amended in S23.)* A submissions `accepted_at` also replaces a stored one when the filing has no
  feed-role party (`filings.NON_FEED_ROLES` = `submissions`, `index`): S23's same-day rule (see
  Submissions) makes every submissions response yield the same time, so a later fetch or a reparse
  corrects what S20 wrote; once the feed's time is there it is never moved.
- Submissions (job `edgar_submissions`, raw dataset `edgar_submissions`):
  `data.sec.gov/submissions/CIK##########.json`; parse `filings.recent` columnar arrays and follow
  `filings.files[]` (older pages) → `edgar_filing` (`source='submissions'`).
  *(Amended in S20.)* Migration `008_edgar_filing.sql`; `filed_at` is a DATE (EDGAR's filing date),
  `accepted_at` naive UTC; the fill rule is shared code (`usfin.sources.edgar.filings`:
  `upsert_filings` fills only `accepted_at`/`report_date`/`primary_doc` when NULL — `cik`, `form`,
  `filed_at`, `source` stay as the first source wrote them; `add_parties`). Submissions writes
  `cik` = the company whose JSON it is and one party row per filing with **role `submissions`** (the
  JSON names no role); S22 decides whether a feed role (Issuer…) may replace a `cik` first written
  by submissions or the index, since "fills NULLs" alone never would. Live facts (2026-10-08):
  **`acceptanceDateTime` is not UTC despite its `Z`** — it is New York wall time pushed forward by
  the UTC offset once more (Apple 8-K header ACCEPTANCE-DATETIME 20260730163028 EDT is written
  `2026-07-31T00:30:28.000Z`, 20260129163033 EST `2026-01-30T02:30:33.000Z`, pre-2001 filings
  `T10:00:00Z` in winter = midnight ET), so it is read as UTC, converted to New York and the wall
  time stored as UTC. **Contradicted in part by S22:** on the filing day the JSON carries the
  *true* UTC — JPMorgan's 424B2 0001213900-26-107586 reads `2026-10-07T20:24:53Z` in its JSON and
  `16:24:53-04:00` in the feed, and all 78 of JPMorgan's 2026-10-07 `recent` entries fall 06:00–21:59
  ET read as UTC while every earlier day's sit about 4 h later (12:00–01:00) — so the shift appears
  only after the filing day, and the S20 conversion stores a same-day filing 4–5 h early. The feed's
  `accepted_at` now replaces it (above); a filing the feed never saw keeps the S20 value. Deciding
  per filing (e.g. filing date vs the response's New York fetch date) is left to S23/S26 or the
  user; it was not changed silently.
  *(Decided in S23.)* The parser reads `acceptanceDateTime` as true UTC when the filing's
  `filingDate` equals the New York date of the response's `fetched_at`, and converts it (S20)
  otherwise; `store_document(…, fetched_at=)` carries the time, `_parse` takes it from
  `raw_response`, so reparse is deterministic (without it — S26's bulk members — every time is
  converted, as before; S26 checked bulk against this rule: consistent, see Bulk). Evidence (2026-10-07, JPMorgan fetched
  22:37/23:07 ET, Apple 22:37 ET): the 6 accessions the feed also listed match its offset-bearing
  times to the second; 0001213900-26-107349 (424B2) reads `2026-10-07T01:11:28Z` and its index
  header says ACCEPTANCE-DATETIME 20261006211128, FILED AS OF DATE 20261007 — accepted after 17:30
  and dated the next day, so the rule must key on `filingDate`, not the acceptance date (6 such
  filings); earlier filing dates sit 4 h later as S20 found. `usfin reparse edgar_submissions` on
  `data/usfin.duckdb` changed **72 rows**, each +4 h (all JPMorgan filings dated 2026-10-07 the feed
  had not seen: 48 `0001918704-…`, 24 `0001213900-…`), Apple none; a second reparse 0. Not known:
  when SEC rebuilds the JSON — a fetch just after midnight ET may still carry the previous day's
  true UTC and be converted wrongly; a later fetch or the feed corrects it. `recent` is the newest 1 000 filings, or one year for very heavy filers
  (JPMorgan 26 311 filings, 4.6 MB). Pages hold the same arrays at their top level, are **numbered
  newest first** (JPMorgan `-001` = 2025-09-03..2025-10-05, `-070` = 1994..2004), and a listing's
  `filingFrom`/`filingTo` are not its first/last filing dates (Apple's page lists ..2015-09-15, its
  newest filing is 2015-09-11). So pages are tracked by listing, not name, in
  `edgar_submissions_page(cik, filing_from, filing_to, filing_count, name, raw_id)` (filled by the
  parsers, so reparse rebuilds it), and a run fetches only listings with no parsed response: a
  re-run is one request per CIK. Watermark key = 10-digit CIK, value = newest `filingDate`. The
  registered job's keys are the CIKs of open `pending_doc` rows of kind `submissions` (none until
  S22 creates the table; marking rows done is S22's); `usfin edgar submissions --cik N [--cik …]`
  runs the same job (`submissions.for_ciks`, trigger manual) for given CIKs. `store_document(con,
  doc, name=)` takes the member name, so S26's bulk ZIP members use it as is. schedule.yaml lists
  the job with no cron (`federal`, `due: none`).
- Companyfacts (job `edgar_companyfacts`, raw dataset `edgar_companyfacts`):
  `data.sec.gov/api/xbrl/companyfacts/CIK##########.json` → `xbrl_fact(cik, taxonomy, tag, unit,
  accn, period_start, period_end, is_instant, value DOUBLE, fy, fp, form, filed, frame NULL)`, PK
  (`cik, taxonomy, tag, unit, accn, period_start, period_end`); instant facts store
  `period_start = period_end`, `is_instant = true` (DuckDB PKs reject NULL). Only `us-gaap` and
  `dei`.
  *(Amended in S21.)* Live 2026-10-08: the document is `{cik, entityName, facts: {taxonomy: {tag:
  {label, description, units: {unit: [fact]}}}}}`, a fact `{start?, end, val, accn, fy, fp, form,
  filed, frame?}`; Apple 3.8 MB (dei + us-gaap only, 25 135 facts), JPMorgan 8.0 MB with `invest`,
  `srt`, `ffd`, `ecd` besides (dropped; 49 946 us-gaap + dei facts kept). The planned PK is unique in
  both (no instant/one-day-duration clash). `fy`/`fp` are **null** on facts from forms without a
  fiscal period (JPMorgan's DEF 14A pay-versus-performance `NetIncomeLoss`), so both are nullable;
  forms seen: 10-K, 10-Q, 8-K, 10-K/A, 10-Q/A, DEF 14A. `frame` sits on one filing per period and
  SEC moves it to the latest, so it is the column a refetch updates (a fact absent from a later
  document is kept). Types: `cik` BIGINT, `fy` SMALLINT, dates DATE, `value` DOUBLE (no |val| above
  2^53 in either company), strings VARCHAR (DuckDB dictionary-compresses them). **Wrong for the
  full ZIP (found in S27):** `fy` is INTEGER, not SMALLINT — CIRCOR International (CIK 1091883)
  files fy `44561` (an Excel serial date for 2021-12-31) on its 2021 10-Q facts, which failed the
  whole member (migration `015_xbrl_fact_fy_integer.sql`); and the `cik` key is **absent** from
  8 companyfacts.zip members (funds with only `vip`/`cef`/`oef` facts, plus Star Mountain Lower
  Middle-Market Capital 4 588 and ZeroStack 6 027 facts), so the member name decides when `cik`
  is missing (a present, different `cik` is still refused). No `raw_id`: facts
  are not versioned rows, every accn is its own row. Migration `009_xbrl_fact.sql` with macro
  `xbrl_as_of(d)` = `SELECT * FROM xbrl_fact WHERE filed <= CAST(d AS DATE)`. Module
  `usfin.sources.edgar.companyfacts`: `store_document(con, doc, name=)` (name
  `CIK##########.json`, as companyfacts.zip members are named) for S26, `upsert_facts` stages
  through one JSON text parameter (S8) and counts only new or changed rows; registered keys = CIKs of
  open `pending_doc` kind `companyfacts` (S22), `usfin edgar facts --cik N` (`for_ciks`), watermark
  key = 10-digit CIK, value = newest `filed`. **Storage: 91 bytes per fact with the PK, 21 without**
  (75 081 facts copied into fresh DuckDB files, checkpointed): the 7-column PK's ART index is about
  three quarters of the table. S26's projection must use the with-PK figure; if the full load does
  not fit, dropping the PK (the upsert already finds changes by anti-join; only its `ON CONFLICT`
  needs it) is the first lever — the user's call. *(Decided after S26, built in S27: no PK, see
  Bulk › Decisions after S26.)*
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
  *(Amended in S22.)* Live 2026-10-07 22:57 ET: `application/atom+xml`, ISO-8859-1, 57 KB per
  100-entry page; entry title `<form> - <name> (<10-digit CIK>) (<role>)`, `<updated>` = the
  acceptance time with its New York offset (stored naive UTC), the summary's `Filed:` date and
  `AccNo:`, `<category term>` = form, `<id>` ending `accession-number=…`; a feed with nothing to
  list has no `<entry>`. After 17:30 ET the feed lists only `4` and Schedule 13D/G filings; 10
  pages (1 000 entries, 628 accessions) reached back only to 16:15 ET that day, so a first run or
  a long outage relies on the daily-index reconcile (S23) for the rest. Paging also stops at a
  short page. Module `usfin.sources.edgar.feed`, key `getcurrent`, watermark = newest `updated`
  (never moved back). `pending_doc` is migration `010_pending_doc.sql` (kind CHECKed; `enqueued_at`
  = the queuing response's `fetched_at`, so reparse queues the same rows; a done row is never
  reopened); queue rules in `feed.pending_kinds`/`queue_rows`: `submissions` (any form) for the
  watchlist party, `companyfacts` 10-K/10-Q (+/A) → filing CIK and 8-K (+/A) only for a watchlist
  party → that CIK, `form4` → issuer, `sched13` → subject; module `usfin.sources.edgar.pending`.
  **Draining:** a per-CIK job's `after_key` (Jobs) closes a CIK's rows whose accession the fetched
  document carries (`submissions`: the company lists it; `companyfacts`: an `xbrl_fact` with that
  `accn`) or that waited 1 day (`pending.GIVE_UP`: SEC builds companyfacts minutes after
  acceptance and not every 8-K or amendment has XBRL); an open row keeps its CIK in the next
  drain. **Chain:** `then` is `edgar_submissions, edgar_companyfacts` only — chaining a job that
  does not exist yet would fail every run (as with `auction_xcheck` in S8/S14), so S24 and S25 add
  `edgar_form4` / `edgar_sched13` to `feed.JOB.then` with their jobs; until then `form4`/`sched13`
  rows wait in the queue. *(S24: `edgar_form4` added; `form4` drains per accession, see Form 4. S25: `edgar_sched13` added the same way, see Schedule 13D/G.)* `config/edgar.yaml` is `watchlist: [CIK, …]`, seeded with Apple 320193,
  JPMorgan 19617, Microsoft 789019. Schedule `*/15 6-22 * * *` federal (the 22:00–22:45 polls
  catch 21:45–22:00 acceptances), `due: federal`, `stale_watermark n: 1` (no `zero_new_rows`: the
  06:00 poll usually has nothing new).
- Daily index reconcile (job `edgar_reconcile`, 23:30 ET on federal days, raw dataset
  `edgar_master_idx`): `Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx` for federal
  days from max(watermark − 7 days, today − 7 days) to today (404 for a not-yet-published day is
  normal) → `edgar_filing` (`source='index'`) and `edgar_filing_party` (role `index`); missing
  Form 4 / 13D/G accessions are enqueued in `pending_doc` and drained by the same chain (C14).
  *(Amended in S23.)* **The window was wrong here and in the S23 entry:** max(watermark − 7 days,
  today − 7 days) is today − 7 days for any watermark not in the future, so it ignored outages,
  against C3 ("max(7 days, back to the watermark)") and review finding 10. Built: the federal days
  from **min(watermark, today − 7 days)** to today; watermark key `master` = the newest day whose
  index parsed (`dailyindex.window`). A 404 for today is normal; a 404 for an earlier federal day
  fails the key after the other days parsed (the `compute` hook), so the gap stays visible and the
  watermark does not move — an unscheduled SEC closure goes in `calendars.yaml`. Live 2026-10-07
  23:14 ET: `application/octet-stream`, ASCII, LF, no newline after the last line; header lines
  (`Description:`, `Last Data Received:`, …), the column line `CIK|Company Name|Form Type|Date
  Filed|File Name`, 80 dashes, then one line per CIK of a filing **sorted by CIK as text** (an
  accession's CIKs are not adjacent) with no role; `File Name` is `edgar/data/<cik>/<acc>.txt`;
  `Date Filed` (`YYYYMMDD`, stored as `filed_at`) need not be the index's day (DRS, EFFECT and
  FOCUSN of earlier dates); some (accession, CIK) lines repeat verbatim 2–3 times (26 pairs on
  10-07: ABS-15G depositor = securitizer, 485s per series), so parties are distinct pairs; no
  accession sat in two days' files (18 592 over 6 days). `cik` is the first listed CIK until
  `settle_cik` finds a feed role; Form 4 (`4`, `4/A`) and 13D/G (`feed.SCHED13_FORMS`) accessions
  are queued (kinds `form4`/`sched13`, CIK = the settled `cik` — for an index-only Form 4 that may be
  an owner, so **S24 must read the issuer from the XML**; any party CIK serves the folder URL);
  a row the feed queued first stays. The 10-07 index was already out at 23:21 ET (`Last Data
  Received: Oct 7`) and held all 628 feed accessions. Module `usfin.sources.edgar.dailyindex`, raw
  dataset `edgar_master_idx`, schedule `30 23 * * *` federal, `due: federal`, `stale_watermark n: 1`.
  **Chain:** none yet — S24/S25 add `edgar_form4`/`edgar_sched13` to **both** `feed.JOB.then` and
  `dailyindex.JOB.then`. *(S24: `edgar_form4` is in both; the feed's `then` is now `edgar_submissions,
  edgar_companyfacts, edgar_form4`, the reconcile's `edgar_form4`. S25: both end with `edgar_sched13`.)*
- Form 4 (job `edgar_form4`, raw datasets `edgar_filing_index`, `edgar_form4_xml`): filing folder
  `Archives/edgar/data/{cik}/{accession_nodash}/index.json` → the ownership `.xml` →
  `insider_txn(accession_no, line_no, issuer_cik, owner_cik, owner_name, is_derivative,
  security_title, txn_date, txn_code, shares, price, acquired_disposed, shares_owned_after,
  direct_indirect)` PK (`accession_no`, `line_no`).
  *(Amended in S24.)* The planned single table could not hold a joint filing's owners, holdings,
  derivative terms or footnote-only values, so migration `011_insider.sql` has four tables, all
  keyed by accession and filled from the one XML: `insider_filing(accession_no PK, form, issuer_cik,
  issuer_name, issuer_ticker, period_of_report, original_filed NULL, document, owners, lines)`,
  `insider_owner(accession_no, owner_seq, owner_cik, owner_name, is_director, is_officer,
  is_ten_pct_owner, is_other, officer_title, other_text)` PK (`accession_no, owner_seq`),
  `insider_txn` with the planned columns plus `is_holding, nature_of_ownership, exercise_price,
  exercise_date, expiration_date, underlying_title, underlying_shares, footnotes JSON`, and
  `insider_footnote(accession_no, footnote_id, text)`. **Multiple owners:** a joint Form 4 does not
  split its lines between owners (BNB Standard's 4/A: YZi Labs and Changpeng Zhao report the same
  lines), so `insider_txn.owner_cik/owner_name` is the first `reportingOwner` and every owner is in
  `insider_owner` (one row per line × owner would double-count shares). **Amendments:** a 4/A is
  its own accession with `form = '4/A'` and `original_filed` = `dateOfOriginalSubmission`; its
  lines stand beside the original's (the amendment may restate all of them or none — live, BNB's
  only added the second owner), nothing is deleted from the original. **Footnote-only values**
  (`<transactionPricePerShare><footnoteId id="F1"/></…>`, RSU exercise/expiration dates) are NULL
  and `footnotes` maps each column to its ids (`{"price": ["F1"]}`), text in `insider_footnote`.
  **XML element mapping** (`ownershipDocument`, schema X0609 in every 2026 file seen; a field is
  `<x><value>…</value><footnoteId id/>…</x>`, flags written `1`/`0` or `true`/`false`):
  `documentType` → `form` (only `4`, `4/A`; anything else fails the key); `periodOfReport`;
  `dateOfOriginalSubmission` → `original_filed`; `issuer/issuerCik|issuerName|issuerTradingSymbol`
  → `issuer_*` (**the issuer is always read here**, never from the queue); each `reportingOwner`
  → `insider_owner` (`reportingOwnerId/rptOwnerCik|rptOwnerName`,
  `reportingOwnerRelationship/isDirector|isOfficer|isTenPercentOwner|isOther|officerTitle|otherText`);
  the children of `nonDerivativeTable`, then `derivativeTable`, in document order → `insider_txn`
  lines from 1 (`*Transaction`, or `*Holding` with `is_holding` and NULL transaction columns):
  `securityTitle` → security_title, `transactionDate` → txn_date (first 10 chars),
  `transactionCoding/transactionCode` → txn_code, `transactionAmounts/transactionShares|
  transactionPricePerShare|transactionAcquiredDisposedCode` → shares/price/acquired_disposed,
  `postTransactionAmounts/sharesOwnedFollowingTransaction` → shares_owned_after,
  `ownershipNature/directOrIndirectOwnership|natureOfOwnership`, and for derivatives
  `conversionOrExercisePrice`, `exerciseDate`, `expirationDate`,
  `underlyingSecurity/underlyingSecurityTitle|underlyingSecurityShares`; `footnotes/footnote[@id]`
  → `insider_footnote` (whitespace collapsed). Not kept: addresses, `deemedExecutionDate`,
  `transactionFormType`, `equitySwapInvolved`, `transactionTimeliness`, `aff10b5One`, remarks,
  signatures, `valueOwnedFollowingTransaction`. **Fetch:** per queued accession, the folder
  `Archives/edgar/data/<queued cik>/<acc without dashes>/index.json` — the folder answers under
  any party's CIK (verified live: Costco's index-only 0000909832-26-000090 under owner 1928633) —
  then its first `.xml` item, whose name varies (`ownership.xml` 19 and `form4.xml` 12 of 50 live,
  else `form4-<date>_<n>.xml`, `wk-form4_<n>.xml`, `primarydocument.xml`, `form4a…xml`); a
  folder lists only that XML plus `-index.html`, `-index-headers.html` and `.txt`. The
  `edgar_filing_index` parser stores nothing (the listing only names the XML; S25 shares the
  dataset). **Idempotence:** a document's rows are compared with the stored ones (`EXCEPT` both
  ways per table); any difference replaces the accession's rows in all four tables, otherwise
  nothing is written; rows counted = `insider_txn` lines inserted or changed. **Queue:** keys are
  the open `form4` accessions oldest first (`enqueued_at, accession_no`), **at most
  `form4.max_docs_per_run` in `config/edgar.yaml` (default 300)** — S23 left ~4 500 open rows and
  each costs 2 requests, which would stall the 15-min feed chain; a row closes
  (`pending.close`) once `insider_filing` has its accession; there is no give-up (a failing
  accession fails only its key and stays open, visible in the run log). `edgar_filing.cik` is
  not touched (for an index-only Form 4 it may stay an owner; `insider_filing.issuer_cik` is
  authoritative). Live 2026-10-07 23:34 ET, the 50 oldest: **~0.7 s per document** (100 requests
  in 34.6 s, ~3 rps — SEC transfer time, well under the 8 rps cap), so the default cap of 300 is
  ~3.5 min and the 4 449 left ~52 min, ~15 runs of 300.
- Schedule 13D/G (job `edgar_sched13`, raw datasets `edgar_filing_index`, `edgar_sched13_xml`):
  structured XML (required since 2024-12-18) → `ownership_stake(accession_no, person_seq,
  filer_cik NULL, filer_name, subject_cik, form, event_date, percent_of_class, shares, filed_at)`
  PK (`accession_no`, `person_seq`). Pre-XML filings are skipped and counted.
  *(Amended in S25.)* **Schedule 13G cover pages carry no CIK for any reporting person** (only
  `reportingPersonName`; 13D has `reportingPersonCIK`, absent when `reportingPersonNoCIK` is `Y`),
  so `filer_cik` is NULL for every 13G person, not just the odd one — live, 134 of 162 persons;
  the filing's login CIK (`headerData/filerInfo/filer/filerCredentials/cik`) is kept per filing
  but cannot be matched to a person (Berkshire's 13G/A logs in as Berkshire, Buffett is person 1).
  Migration `012_ownership.sql`: `ownership_filing(accession_no PK, form, legacy, subject_cik,
  subject_name, subject_cusips, class_title, event_date, amendment_no, filer_cik, filed_at,
  document, persons)` beside the planned `ownership_stake` (+ `sole_voting, shared_voting,
  sole_dispositive, shared_dispositive, member_of_group, citizenship, person_type`); `filed_at`
  is `edgar_filing.filed_at` (the XML has no filing date). **Legacy skip:** a queued accession
  whose `edgar_filing.form` is `SC 13D`/`SC 13G` (+/A) fetches only its folder `index.json` (no
  XML in it — JPMorgan's SC 13G 0000019617-24-000609 lists only its `.htm`), whose parser writes
  an `ownership_filing` row with `legacy` true and 0 persons: counted by `SELECT count(*) FROM
  ownership_filing WHERE legacy`, and the row closes the queue entry. Legacy names in
  `edgar_filing`: newest 2024-12-04 (JPMorgan SC 13G/A), none in the 491 queued. The
  `edgar_filing_index` parser thus does work here, but only for legacy accessions (a Form 4's or a
  structured filing's listing stores nothing), so the shared dataset replays harmlessly in both
  jobs' reparses. **Fetch:** per queued accession, the folder under the queued CIK (for an
  index-only filing that may be a reporting person — 1 of 50 live; the subject is read from the
  XML) then `primary_doc.xml` (always that name; folders also hold `.htm`/`.pdf` exhibits); a
  structured form without it fails its key. **Queue:** as Form 4 — oldest first, at most
  `sched13.max_docs_per_run` in `config/edgar.yaml` (default 300; `pending.max_docs(config,
  kind)` now serves both), closed once `ownership_filing` has the accession, no give-up; rows
  are compared and an accession's rows replaced in both tables only when they differ
  (`form4.replace_rows`), rows counted = `ownership_stake` rows inserted or changed. Chained
  after `edgar_feed` and `edgar_reconcile`.
  **XML element mapping** (`edgarSubmission`, namespace `http://www.sec.gov/edgar/schedule13D` or
  `…/schedule13g`, schema `X0202` in all 50 + 23 files seen; namespaces ignored; dates
  `MM/DD/YYYY`; numbers like `2700000.00`): `headerData/submissionType` → form (`SCHEDULE 13D`,
  `SCHEDULE 13D/A`, `SCHEDULE 13G`, `SCHEDULE 13G/A`; anything else fails the key);
  `formData/coverPageHeader`: `amendmentNo`, `securitiesClassTitle` → class_title, 13D
  `dateOfEvent` / 13G `eventDateRequiresFilingThisStatement` → event_date, 13D
  `issuerInfo/issuerCIK` / 13G `issuerInfo/issuerCik` → subject_cik, `issuerInfo/issuerName`,
  `issuerInfo/issuerCusips/issuerCusipNumber` (comma-joined). Persons in document order from 1:
  13D `formData/reportingPersons/reportingPersonInfo` — `reportingPersonCIK`,
  `reportingPersonName`, `memberOfGroup`, `citizenshipOrOrganization`, `soleVotingPower`,
  `sharedVotingPower`, `soleDispositivePower`, `sharedDispositivePower`, `aggregateAmountOwned` →
  shares, `percentOfClass`, `typeOfReportingPerson` (repeats, comma-joined); 13G
  `formData/coverPageHeaderReportingPersonDetails` — `reportingPersonName`, `memberGroup`
  (optional), `citizenshipOrOrganization`, `reportingPersonBeneficiallyOwnedNumberOfShares/
  soleVotingPower|sharedVotingPower|soleDispositivePower|sharedDispositivePower`,
  `reportingPersonBeneficiallyOwnedAggregateNumberOfShares` → shares, `classPercent`,
  `typeOfReportingPerson`. Not kept: addresses, `authorizedPersons`, `commentContent`/`comments`,
  `fundType`, `legalProceedings`, the narrative `items`/`items1To7`, exhibits, signatures. Live
  2026-10-08 (50 oldest): **~0.77 s per document** (100 requests in 38.6 s), so the default cap
  300 is ~4 min and the 441 left ~5.7 min.
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
  **Free-disk guard (added 2026-10-08 at the user's request; the Mac had 22 GiB free of 228).**
  Before any bulk download (`usfin edgar backfill`, job `edgar_bulk`) the code reads the ZIP's
  `Content-Length` (HEAD request) and `shutil.disk_usage` of the `USFIN_RAW` and `USFIN_DB`
  volumes; if free − ZIP size (+ projected DB growth, when known) < `USFIN_MIN_FREE_GB`
  (default 10), it does not download: the CLI exits 1 with the numbers, the job records
  `skipped` and raises `disk_low` (subject = dataset). The guard is re-checked every 1 000
  parsed members during a full load and stops cleanly (committed members stay, CRCs make the
  next run resume) if free space falls below the floor.
  **Decisions after S26 (user, 2026-10-08).** (1) `xbrl_fact` has **no primary key**: uniqueness
  is the loader's job (per-document diff/replace), saving ~8 GiB on the full load (20 vs 90
  bytes/fact); a migration rebuilds the table without it, and tests prove re-runs, reparse and the
  bulk path still produce no duplicate (cik, taxonomy, tag, unit, accn, period_start, period_end).
  (2) The submissions bulk backfill and weekly reconcile load only members whose CIK is in
  `edgar_company` (active or not), including their paged `-submissions-NNN.json` members; the
  ~985 000 other filers (people, funds) are not loaded from the ZIP — feed, nightly index and
  per-CIK submissions still record their new filings. (3) Keep the last **2** bulk ZIPs per dataset
  (as planned).
  **Measure before the full load.** The S26 sample run (`--limit 200` per dataset) records, in
  `edgar_bulk_estimate(dataset, measured_at, zip_bytes, members_total, members_sampled,
  db_bytes_before, db_bytes_after, rows_inserted, seconds)`, and `usfin edgar backfill --estimate`
  prints the projection: `projected_db_growth = (db_bytes_after − db_bytes_before) /
  members_sampled × members_total` and the time likewise. S27 runs the full load only when ZIP
  sizes + projected growth fit under the guard; otherwise it reports the numbers and stops as a
  block for the user (move `USFIN_RAW`/`USFIN_DB`, keep 1 ZIP, or narrow scope).
  *(Amended in S26.)* Module `usfin.sources.edgar.bulk`, migration `013_edgar_bulk.sql`
  (`edgar_bulk_member` PK (`dataset, name`); `edgar_bulk_estimate` carries the planned columns plus
  `raw_id`, `member_bytes_total/sampled` (uncompressed), `download_seconds` and
  `scratch_rows/scratch_bytes_pk/scratch_bytes_no_pk`). CLI: `usfin edgar backfill
  {submissions|companyfacts} [--limit N] [--cik …] [--check] [--reuse] [--estimate]` — `--check`
  HEADs and prints the guard verdict only, `--reuse` parses the newest stored ZIP instead of
  downloading again. HEADs are archived as raw dataset `edgar_bulk_head` (not a bulk dataset, so
  prune ignores them). `USFIN_MIN_FREE_GB` is GiB (2^30, as `df -h` shows); free = the fuller of
  the raw and DB volumes; projected growth in the guard = the newest estimate's DB-delta per member
  × the members to parse. `http.Client.get(…, stream=True)` streams the body into `raw.save` in
  chunks; **a body cut off mid-stream escaped the client before S26** (the first live companyfacts
  download died after ~10 min with nothing archived, cause unrecorded), so it is now retried like
  a transport error and raises `HttpError` after 5 tries. `--limit N` takes N members **spread
  evenly** over the ZIP, not the first N (those are the oldest, smallest registrants); members are
  ordered main document before its `-submissions-NNN` pages, so a page member links its listing
  (`edgar_submissions_page.raw_id` = the ZIP's raw id) and the per-CIK job will not refetch it.
  Each member commits alone with its CRC; a member already stored with the same CRC is skipped
  (resume); a malformed member is rolled back, listed and the load goes on (exit 1 at the end).
  **companyfacts.zip has 62 members that are exactly `{}`** (no `cik`, no `facts`; e.g.
  `CIK0000022370.json`) — recorded with their CRC, no rows. An estimate row is written by every
  load of the whole ZIP that parsed a member (not by a `--cik` pick).
  **The DB-file delta is not a reliable size projection for submissions**: two 200-member samples
  of the same ZIP grew the file +23.6 MB and +3.7 MB (projections 136 and 17 GiB), allocation
  churn of 200 small transactions, not row cost. So every sample is also copied into a fresh
  DuckDB file (`bulk.measure`: companyfacts with and without the `xbrl_fact` PK; submissions
  `edgar_filing` + parties + pages with their PKs), but a 200-member submissions sample is only
  ~9 blocks, so its fixed overhead inflates that too (421 B/filing); calibrated on the 198 415
  filings already stored, a filing costs **165 bytes** with its party row and PKs. The guard still
  uses the DB-delta projection (conservative); S27 should judge the full load with the calibrated
  figures in the S26 progress row. Bulk submissions acceptance times: the ZIP is built just after
  midnight (members dated 2026-10-07 00:20, newest `filingDate` 10-06) and its strings equal the
  per-CIK JSON fetched later that day for every shared filing (JPMorgan 26 233, Apple 1 000,
  incl. 102 dated 10-06), so converting every time (no `fetched_at`) agrees with the S23 rule.
  *(Amended in S27.)* **No PK:** migration `014_xbrl_fact_no_pk.sql` rebuilds `xbrl_fact` (same
  columns, NOT NULLs and CHECK; DuckDB cannot drop a PK in place) and recreates `xbrl_as_of`;
  `companyfacts.upsert_facts` keeps the 7-column key unique itself: a document's facts are
  deduplicated by key (first listed wins), diffed against the stored rows **of that CIK only**
  (`WHERE cik = ?`, so DuckDB skips other companies' row groups), and every changed key is
  deleted and inserted again (a duplicate already stored heals the same way). **Submissions
  filter:** `Spec.companies_only` — a submissions member (main or `-submissions-NNN` page) loads
  only when its CIK is in `edgar_company`; `Loaded.members_total` and the estimate count only
  those, so the guard projects the filtered load (the CLI prints `not in edgar_company N
  members`). Only a `--limit` sample is copied into scratch files; a full load records its cost
  (an estimate row with `scratch_*` NULL) without them. **Job `edgar_bulk`** (module `bulk`,
  keys `companyfacts`, `submissions`, `prune`, no parsers): per dataset HEAD → guard (projection
  from the newest estimate, capped by `USFIN_BULK_LIMIT`) → download → `load` (members whose CRC
  changed, each committing alone, at most `USFIN_BULK_LIMIT` spread evenly when set) — the members
  are parsed while the key is fetched, since a parser runs inside the runner's transaction and a
  member commits on its own; `compute` reports the rows. A guard that does not fit (before the
  download or crossed mid-load) raises `disk_low` (subject = raw dataset, detail = the numbers)
  and `jobs.JobSkipped`, which stops the run as `skipped` (keys done before it keep their work).
  Malformed members fail that dataset's key after the others committed. Key `prune` runs
  `raw.prune(keep=2)`. Watermark per dataset = the New York date of the last ZIP fully
  reconciled. Reparse has nothing to replay for the job: `usfin edgar backfill … --reuse`
  replays a stored ZIP. Schedule `0 3 * * sun`, `any`, `due: none`.
- Frames (raw dataset `edgar_frames`): `data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json`
  on demand → `frames_cache(key PK, fetched_at, raw_id)`, TTL 24 h; never written to `xbrl_fact`.
  *(Amended in S28.)* Live 2026-10-08 (`us-gaap/Revenues/USD/CY2025Q2`, 296 137 bytes): `{taxonomy,
  tag, ccp, uom, label, description, pts, data: [{accn, cik, entityName, loc, start?, end, val}]}`,
  one point per company (1 939); an unknown frame is 404 (archived, raises, not cached). Module
  `usfin.sources.edgar.frames`: `get_frame(con, client, taxonomy=, tag=, unit=, period=, now=)`,
  cache key `<taxonomy>/<tag>/<unit>/<period>`, migration `016_frames_cache.sql`; periods are
  `CY####` or `CY####Q#`, `I` suffix for instants, anything else is refused before a request.
  CLI `usfin edgar frame TAXONOMY TAG UNIT PERIOD [--limit N]`. **Frames are fresher than the
  bulk companyfacts:** for Revenues USD CY2025Q2 the API and `xbrl_fact` (bulk ZIP of 2026-10-08)
  share 1 932 CIKs with the same period dates, but in 42 the API's frame sits on a 2026 filing the
  stored companyfacts document does not carry (e.g. UDR, HCA: the ZIP member's newest fact is filed
  2026-04), 2 values differ (Tiptree 92 000 vs 528.75 M, Sun Communities 495.9 vs 623.5 M, both
  after the frame moved), 7 CIKs only in the API (stale or factless members: OGE Energy's newest
  stored fact is filed 2020-11, Highwoods has none) and 3 only in `xbrl_fact`. So some
  companyfacts.zip members lag SEC by months; the per-CIK chain refreshes a company when it next
  files a 10-K/10-Q.
- Concept map `config/concepts.yaml` (e.g. `revenue: [Revenues,
  RevenueFromContractWithCustomerExcludingAssessedTax, SalesRevenueNet]`) with `version:`;
  screens pick the first tag a company reports for that period and show which tag was used.
  *(Amended in S28.)* A list of tags is not enough: facts and frames are per unit (revenue has
  USD and CAD facts in CY2025Q2) and `dei` concepts exist, so each concept is `{taxonomy?
  (default us-gaap), unit? (default USD), tags: [...]}` under `concepts:`, plus `version:`
  (version 1: revenue, net_income, operating_income, eps_diluted, assets, liabilities,
  stockholders_equity, cash, operating_cash_flow, shares_outstanding). Module
  `usfin.sources.edgar.concepts`: `load_concepts(config_dir)` (bad file → `ConfigError`) and
  `screen(con, concept, period)` (S33 reuses it). **Period → rows:** a frame period maps to
  `xbrl_fact` rows by the `frame` column, exactly (`frame = 'CY2025Q2'`, taxonomy and unit of the
  concept, tag in its list), never by period dates: `frame` is SEC's own fit (the same one the
  frames API serves) of a duration to the nearest calendar period — CY2025Q2 durations span 46–136
  days and end 2025-05-16..2025-08-05, CY2025 320–401 days — and `I` marks an instant period,
  which SEC also gives to short durations (1 169 CY2025Q2I facts are 5–39-day durations), so
  `is_instant` is not consulted and a balance-sheet concept needs the `I` period (`assets
  CY2025Q2I`). SEC hangs a frame on one filing per company and period, the latest (Apple's
  Apr–Jun 2025 revenue carries CY2025Q2 on the 10-Q of 2026-07-31, not the 10-Q of 2025-08-01),
  so the screen is a current view of the latest reported value, showing `accn`, `form`, `filed`;
  per company the first listed tag wins, then the newest `filed` (a fact dropped from a later
  document keeps its old frame). A point-in-time screen cannot use `frame` (it moves): that needs
  `xbrl_as_of` and period dates, not built here. Company names come from `edgar_company` (ticker
  companies only). **Speed on 122.7 M facts, no index:** the screen is one filtered scan of
  `frame`/`tag` — 1.6–1.8 s cold (first query of a process), 0.37–0.49 s warm; a plain `frame =`
  count is 0.35 s. No index added (size cost 0); an index on `frame` was not built or measured —
  the only ART measured, the 7-column PK, cost ~70 B/fact (S21), and the screen is already sub-2 s.
  CLI `usfin edgar screen CONCEPT PERIOD [--limit N]` prints per company value, tag used, period,
  form, filed, accn, name, then the count per tag.

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
`AAC-U` → `AAC.U`). *(Found in S19.)* EDGAR spells units `-UN`, warrants `-WT`, rights `-RI`
(`KCAC-UN` → `KCAC.UN`), so a Nasdaq `.U` unit will not meet EDGAR's `.UN` under this rule; S31
checks the Nasdaq Trader spellings and decides (suffix map or unmatched). View `symbol_edgar_match` joins open history rows to active `edgar_company`
rows on `ticker_norm`; `usfin symbols unmatched`.
*(Amended in S29.)* Live 2026-10-08: both files are ASCII, CRLF, `text/plain` (349 272 and
542 931 bytes; 5 627 and 7 663 rows), created 03:03 ET (`Last-Modified` 07:03 GMT agrees, so the
trailer is New York time). Trailer `File Creation Time: 1008202603:03|||||||` — padded with one `|`
per other column. nasdaqlisted's 8th column `NextShares` is not stored; otherlisted's `Exchange`
codes seen are A, N, P, Z, F (and M, V on test issues only). otherlisted already spells symbols
with `.` and `$` (`AAC.U`, `AAC.W`, `AIIA.R`, `BRK.B`, `ABR$D`; CQS `AAC.WS`/`ABRpD`, NASDAQ
`AAC+`/`ABR-D`); symbols are stored as written. `symbol_snapshot` also carries `raw_id` (the first
response that carried the snapshot; `ON CONFLICT DO NOTHING`) and `round_lot INTEGER`, `etf
BOOLEAN`. Watermark per file (`nasdaqlisted`, `otherlisted`) = newest `snapshot_at` (naive UTC
ISO, never moved back to an older file). The trailer check happens in `fetch`, after the body is
archived: a rejected body fails its key and raises source alert `snapshot_rejected` (subject =
file, detail `{raw_id, bytes, reason}`); its parser never runs in the run, and on reparse it skips
that body with a warning (a rejected body must not make reparse fail). A header missing a mapped
column, or a row with the wrong field count, is a `SnapshotError` (key fails). Alerts
(`schedule.yaml`, due nyse): `zero_new_rows`, `stale_watermark n: 1`, `row_count_swing pct: 2` via
`Observed(symbol_snapshot, snapshot_at, match file, snapshot=True)` — S29 alerts on a swing; S30
adds the diff and its skip.

**Scheduler (C6, C7).** `config/schedule.yaml` lists every job with cron (ET), calendar
(`federal`|`bond`|`nyse`|`any`), due-day rule and alert rules. `usfin serve` builds APScheduler
CronTriggers in `America/New_York` with `misfire_grace_time=3600`; a guard records a `skipped`
job_run on non-calendar days. On startup (and therefore after wake, when a missed fire is past
its grace time) a catch-up pass runs each job once (trigger `catchup`) whose most recent due fire
is later than its last `ok` run's `started_at` (decision E). `usfin schedule` prints the next 5
fire times per job.
*(Amended in S10.)* `usfin.scheduler`: `load_schedule(config_dir)` takes every `schedule.yaml` job
with a `cron` (no cron = not scheduled; `calendar` required); a job in the file but not registered
is logged and not scheduled. **Day of week must be named (`mon..sun`)**: APScheduler 3's
`CronTrigger.from_crontab` numbers Monday 0 where cron numbers Sunday 0 (`"0 3 * * 0"` fires on
Mondays), so a digit in the dow field is a `ConfigError` (edgar_bulk is `0 3 * * sun`, bls_calendar
`0 6 * * mon`). `Scheduler(settings, registry, clock=)` wraps a `BackgroundScheduler` (tz New York,
`ThreadPoolExecutor(1)`, job defaults `coalesce`, `max_instances=1`, `misfire_grace_time=3600`); the
cron target `fire(job)` applies the guard on the clock's New York date and otherwise calls
`jobs.run_job(trigger='scheduled')` (chains included). The `skipped` row carries the current
watermarks as `watermark_before` so the day's alert baseline survives a skip. `run_now(job,
trigger)` adds a one-shot date job with a unique id and `misfire_grace_time=None` on the same worker
(a second identical request while one is still waiting is dropped, `enqueued: false`). Catch-up
looks back 14 days for the latest fire on a calendar day and compares it with `max(started_at)` of
`ok` runs (any trigger; a job never run ok is caught up); APScheduler's in-memory store forgets
fires from before startup, so catch-up is the only path for them, and an `EVENT_JOB_MISSED` (fire
missed past the grace while serve was running, e.g. asleep) queues a catch-up when that fire was on
a calendar day. API (`usfin.web.app.create_app(scheduler)`; the lifespan starts the scheduler, runs
catch-up and shuts it down, waiting for the running job): `GET /api/health`, `POST
/api/jobs/{job}/run` → 202 `{job, enqueued, trigger: manual}`, 404 for an unknown job. `usfin serve
[--host 127.0.0.1] [--port 8660]`, `usfin schedule [--at YYYY-MM-DDTHH:MM]` (fires on calendar days
only). Final schedule:

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
*(Amended in S11.)* An HTML form can only post a body or navigate to the response, and a JSON
202 is a dead end in a browser; since POSTs take path parameters only (no `python-multipart`), the
API endpoints negotiate on `Accept`: a form post (browser sends `text/html`) gets 303 back to its
page (`/` or `/alerts`), curl and other clients keep JSON (`POST /api/jobs/{job}/run` 202,
`POST /api/alerts/{id}/ack` 200 `{id, acknowledged}`, idempotent, 404 unknown). Layout:
`web/templates/base.html` (nav, viewport meta, `/static/style.css`, no `<script>`) that every page
extends, `busy.html`, `jobs.html`, `alerts.html`; `web/static/style.css` with `--bg/--fg/…` tokens
redefined under `prefers-color-scheme: dark`, tables in a horizontal-scroll wrapper at phone
width. Shared table helper `usfin.web.tables` for S32–S34: a page builds `Table(name, columns,
rows)` (rows are mappings; extra keys are template-only) and returns `respond(request, templates,
"page.html", table, **context)` — `?format=csv` gives `<name>.csv` as an attachment, otherwise the
template; `cell()` formats both alike (None empty, naive-UTC timestamps `YYYY-MM-DD HH:MM:SSZ`, aware
ones with their zone, dicts as JSON) and is the templates' `cell` filter; `csv_href(request)` keeps
the page's other query parameters. `create_app(scheduler, ui_wait=5.0)`: each request opens its own
session; `DatabaseLocked` → 503 (the busy page, plain text for CSV, JSON under `/api/`). `/` lists
every registered job plus any in the run log (cron, calendar, next fire in ET, last run, error,
watermarks; CSV `watermarks` = `k=v; k=v`), `/alerts` open alerts newest first, `?all=1` adds
acknowledged ones (CSV `alerts_all.csv`).

**Ops (C18).** `usfin doctor` (env, config files present, DB path, lock state, raw size, free
disk vs `USFIN_MIN_FREE_GB`, and the latest bulk projection from `edgar_bulk_estimate`). `deploy/com.tuntun.usfin.serve.plist.example` runs `uv run usfin serve` with
`WorkingDirectory` set to the repo (so `config/` and `.env` resolve) and `KeepAlive`. README:
setup, `.env`, first backfills, schedule, analysis access between jobs, the full bulk load's
measured time and size.

**Risks and how each is retired.**
- *EDGAR blocking* → shared 8 rps bucket, UA required by code, 403 sets a block window; retired
  by the limiter/UA/403 tests and the first live EDGAR run.
- *Schema drift* (Fiscal Data, TreasuryDirect) → raw store, add-column / payload, `schema_drift`
  alert; retired by the TreasuryDirect 2010-vs-current fixture test and the add-column test.
- *Bulk size / low disk* (22 GiB free on 2026-10-08) → streamed members, `us-gaap` + `dei` only,
  `--limit` in slices, prune keeps 2, free-disk guard with `disk_low` alert, sample-based
  projection before the full load; retired by the guard tests (S26, S27) and the S26 projection.
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
Red: `tests/test_edgar_dailyindex.py` — `master.idx` fixture parse; window from min(watermark, today−7d) over federal days *(amended in S23: was max(watermark−7d, today−7d), which ignores outages against C3)*; 404 for today is not an error; every accession once in `edgar_filing`, all CIKs in parties; missing Form 4/13D/G enqueued.
Green: `sources/edgar/dailyindex.py`, job `edgar_reconcile` (chain form4, sched13), schedule 23:30 federal.
Run: `uv run usfin run edgar_reconcile` live; compare one day's idx accession count to `edgar_filing`.
Done when: C14 covered (live day matches exactly).

### S24 — Form 4
Depends on: S22
Red: `tests/test_form4.py` — recorded `index.json` + ownership XML fixtures (one non-derivative, one derivative, one amendment) → expected `insider_txn` rows; idempotent. *(S24: plus a holding/indirect one; the amendment has two reporting owners; per-run cap, oldest first.)*
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
Red: `tests/test_edgar_bulk.py` — a small fixture ZIP (2 members each) streams to raw as `.zip`; members parsed in place with the per-CIK parsers; `edgar_bulk_member` CRCs stored; `--limit`, `--cik`. `tests/test_disk_guard.py` — with `disk_usage` patched: free − Content-Length below `USFIN_MIN_FREE_GB` → no GET sent, CLI exits 1 with the numbers; the floor crossed mid-load stops after the current member with earlier members committed; the sample run writes an `edgar_bulk_estimate` row and `--estimate` prints the projected growth from it.
Green: `sources/edgar/bulk.py`, `usfin edgar backfill {submissions|companyfacts} [--estimate]`, guard + estimate table (How › SEC EDGAR › Free-disk guard, Measure before the full load).
Run: check free disk, then live download of both ZIPs (only if the guard allows), `--limit 200` each; record ZIP sizes, parse rate and the projected full-load DB growth and time in the row.
Done when: C17 streaming half and the guard covered; a projection for both datasets is recorded.

### S27 — Weekly bulk reconcile and prune
Depends on: S26
Red: `tests/test_edgar_bulk_reconcile.py` — second ZIP with one changed member → only that member reparsed; prune keeps 2 per dataset after the job; low free disk → `skipped` run + one `disk_low` alert, nothing downloaded.
Green: job `edgar_bulk`, schedule Sun 03:00 any, guard wired into the job.
Also (decisions after S26): migration dropping the `xbrl_fact` PK with no-duplicate tests; submissions bulk filtered to `edgar_company` CIKs (+ their paged members) with a test.
Run: `uv run usfin run edgar_bulk` live with `--limit` env cap; then, **only if the projection (recomputed for no PK and ticker-CIK submissions) fits under the guard**, the **full** companyfacts + submissions load once — record time and DB size in README and here. If it does not fit, stop and report it as a block with the numbers.
Done when: C17 covered; full-load numbers recorded, or the block reported with the projection.

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
| S4 Calendars | done | 2026-10-08 | ecfb40c | 25 new (test_calendars); suite 88 passed | `usfin calendar 2026-10-12` printed `Mon (Columbus Day)` federal closed, bond closed, nyse open; `… 2026-04-03` `Fri (Good Friday)` federal open, bond closed, nyse closed; `… 2026-11-11` Veterans Day federal/bond closed, nyse open; a bad date exits 2. `holidays.NYSE()` already has the 2018/2025 mourning closures, `holidays.US` has no executive-order federal closures (2025-01-09) — overrides file is where those go. Plan amended (Calendars): `Calendars` object from `load_calendars`, `any` calendar, override precedence (federal moves bond, bond wins), strict prev/next, computed Good Friday, `usfin calendar`. |
| S5 Job runner, run log, watermarks | done | 2026-10-08 | e92cd24 | 19 new (test_jobs); suite 107 passed | On the real store, `USFIN_DEMO=1 usfin run _demo` printed `_demo #1 ok rows 6` and `usfin jobs` listed run #1 and `alpha`/`beta` = 2026-10-07 — the Mac clock read 2026-10-08 00:22 UTC, i.e. 20:22 ET on the 7th, so the ET-date cap held. With `USFIN_DEMO_FAIL=beta`: `#2 failed rows 3`, exit 1, error `beta: RuntimeError: …`; a later failing run moved `alpha`'s `updated_at` but left `beta`'s from the run before (C7). Four runs left 6 `demo_item` rows (upserts) and 8 `raw_response` rows over 2 blobs. With another python process holding `data/usfin.duckdb`, `USFIN_LOCK_WAIT=1 usfin run _demo` exited 1 after ~1.3 s with `database busy — …`, `(not logged)`, and no new `job_run` or watermark change. Without `USFIN_DEMO`, `usfin run _demo` exits 2 `unknown job`. Plan amended (Jobs): fetch is `fetch(ctx, key)` per watermark key (a per-job `fetch(ctx)` could not fail one key alone), `ctx.advance`, ET today, lock → no run row, `load_registry`/`_demo`. |
| S6 Reparse and chaining | done | 2026-10-08 | af1bf5d | 19 new (test_reparse); suite 126 passed | On the real store (8 `_demo` raw rows from S5), `USFIN_DEMO=1 usfin reparse _demo` printed `_demo reparsed 8 raw responses, rows 24`, exit 0; `raw_response` (10), `job_run` (4), both watermarks and their `updated_at` unchanged, `demo_item` identical. After `DELETE FROM demo_item` the same command rebuilt all 6 rows. `--since 2026-10-08` reparsed 0 and `--since 2026-10-07` all 8: the blobs were fetched 2026-10-08 00:22 UTC = 10-07 ET, so `--since` reasons in New York dates. With another process holding the DB, `USFIN_LOCK_WAIT=1 usfin reparse _demo` exited 1 after ~1 s with `database busy — …`. Plan amended (Jobs): `Job.versioned` tables, one-transaction all-or-nothing reparse with `ReparseFailed`, `NetworkDisabled` transport, no `job_run` and no watermark move on reparse, pruned rows keep their derived history, depth-first chain run once per chain, failed child does not stop siblings, `usfin run` exits 1 if a chained run failed. |
| S7 Fiscal Data client | done | 2026-10-08 | 8981c36 | 31 new (test_fiscal_client); suite 157 passed | `usfin fd probe debt_to_penny --since 2026-09-01` (live) printed 25 rows in 1 page, record_date 2026-09-01 .. 2026-10-06, 11 columns (`CURRENCY`→DECIMAL(24,4), `YEAR/QUARTER/MONTH/DAY/INTEGER`→BIGINT, `DATE`), exit 0 in ~3.5 s; `auctions` 48 rows with auction_date up to today 2026-10-08 while record_date reads 2026-10-13 (ahead, as planned) and today's auction has null results (the lookback picks them up); `mts_table_1 --since 2026-09-01` returned 0 rows, `total-pages: 0`, but full `dataTypes` (Sept MTS not out yet); an unknown name exits 2 listing the six. A one-off scan of the first 10 000 rows of every dataset from 1990 cast with no failure. Fixtures: 8 real responses (debt_to_penny 2 pages, the other five, MTS empty) recorded with the client's exact URLs; `auctions.json` lost only its unused `meta.labels`/`dataFormats`. No plan contradiction; How › Treasury Fiscal Data gains the client API, the registry YAML shape (endpoints now, pk in S8) and the `fd_probe` raw dataset. |
| S8 Fiscal Data registry job | done | 2026-10-08 | 0ad6102 | 16 new (test_fiscal_job); suite 173 passed | Live full backfill on `data/usfin.duckdb` (`usfin run fiscal`, from 2000-01-01, 14 pages): `fiscal #7 ok rows 58575` in 19.7 s — `fd_dts_operating_cash` 16 674 (2005-10-03..2026-10-06), `fd_debt_to_penny` 6 718 (2000-01-03..2026-10-06), `fd_auctions` 7 894 (auction_date 2000-01-03..2026-10-08), `fd_avg_interest_rates` 5 025 (2001-01-31..2026-09-30), `fd_mts_table_1` 3 117 (2015-03-31..2026-08-31), `fd_rates_of_exchange` 19 147 (2001-03-31..2026-09-30). Second run `fiscal #8 ok rows 0` in 5.4 s (7 overlap requests). Watermarks: auctions 2026-10-07 (capped: the Mac read 20:52 ET on 10-07 and the feed already lists 2026-10-08 auctions with null results), dts/penny 2026-10-06, avg_rates/rates_of_exchange 2026-09-30, MTS 2026-08-31. `usfin reparse fiscal` replayed 14 raw responses with 0 changes in 1.5 s. After: `data/usfin.duckdb` 13.4 MB, `raw/` 2.4 MB (28 fiscal raw rows, 94 MB of JSON bodies; identical re-fetched pages share blobs). The first live run with per-row bound parameters took 97 s (reparse 72 s); staging through one JSON parameter fixed it and the tables were dropped and re-backfilled with the final code. Plan amended (Jobs: `parsers(settings)`; Treasury Fiscal Data: confirmed pks — `rates_of_exchange` needs `src_line_nbr` because upstream repeats rows —, JSON staging, `on_schema_drift` stub, `then: auction_xcheck` deferred to S14 since chaining a missing job would fail every run). |
| S9 Alerts | done | 2026-10-08 | 0f65700 | 26 new (test_alerts); suite 199 passed | `usfin run fiscal` on `data/usfin.duckdb` at 21:01 ET on 10-07 printed `fiscal #9 ok rows 0` (5.5 s) and `usfin alerts --all` printed `no alerts` — correct: the day's first fiscal run was S8's backfill (#5, empty `watermark_before`), so no key had a baseline, and no watermark was stale (DTS/penny 10-06 is 1 federal day back, MTS 08-31 has 1 due day since). On a scratch copy of the real DB the rules then fired as designed: a simulated Fri 10-09 run with nothing new raised `zero_new_rows` and `stale_watermark` (3 due days > 2) for `dts_operating_cash` and `debt_to_penny` only; with the baseline a year back, `value_jump` raised 14 (9 rates_of_exchange, 5 mts_table_1, 0 for TGA/penny/avg rates) — among them the 2026-09-30 exchange-rate release, where some rows carry their neighbour's rate (Nepal-Rupee 151.46 → 1.771 ≈ NZD's rate, NZ-Dollar 1.767 → 0.881), an upstream error the rule exists for. No webhook configured, none sent. Plan amended (Alerts): `zero_new_rows` baseline is the watermark before the day's first run — the plan's `watermark_before` fires on every repeat run; first loads skip the baseline rules; `Job.observed`, `schedule.yaml` shape, fiscal due days and thresholds, `series`/`jump_match` in `fiscal_data.yaml` (`src_line_nbr` is no series key: 553 of 681 jumps > 10 % were line shifts); `db_locked` left to S10. |
| S10 Scheduler and usfin serve | done | 2026-10-08 | 503ed48 | 23 new (test_scheduler); suite 222 passed | `usfin schedule` printed `fiscal  0 17 * * *  federal` and 10-08, 10-09, 10-13, 10-14, 10-15 17:00 (Columbus Day 10-12 skipped). `usfin serve` on `data/usfin.duckdb` at 21:08 ET on 10-07: `/api/health` `scheduler: running`, no catch-up queued (last ok fiscal run 21:01 ET is after the 17:00 fire — correct). `POST /api/jobs/fiscal/run` answered 202 in 9 ms; the worker ran `fiscal #10 ok (manual) rows 0` in 5 s (6 Fiscal requests), `usfin jobs` showed it with unchanged watermarks; `POST /api/jobs/nope/run` → 404. While serve ran a job, `USFIN_LOCK_WAIT=1 usfin jobs` exited 1 `database busy` (decision A as designed), and printed #11 once it finished. Restarted with `USFIN_LOCK_WAIT=2` and another process holding the DB: run-now logged `job fiscal not run: database busy`, health showed `pending_db_locked: 1`; the next run-now stored alert #1 `fiscal/db_locked/fiscal` (raised_on 2026-10-07, the NY date) before running `#12 ok`; the induced alert was acked. Ctrl-C shut the scheduler and uvicorn down cleanly; no process left, port 8660 free. Plan amended (Scheduler: APScheduler numbers Monday 0 in crontab dow, so named days are required; module API, catch-up rules, skipped row keeps watermark_before; Alerts: how `db_locked` is raised; Tech stack: starlette TestClient `httpx2` warning filtered). |
| S11 UI shell: jobs and alerts | done | 2026-10-08 | a999ead | 12 new (test_web_jobs); suite 234 passed | `usfin serve` on `data/usfin.duckdb` (Mac clock 21:13 ET on 10-07): `/` listed `fiscal` (`0 17 * * *` federal, next 2026-10-08 17:00 EDT, #12 ok manual, six watermarks) and `_demo` (not registered without `USFIN_DEMO`, shown from the run log with its #4 failed error); `/?format=csv` gave the same two rows; `/alerts` said `no open alerts`, `/alerts?format=csv&all=1` returned `alerts_all.csv` with S10's acked `db_locked` alert #1 (detail JSON, `acknowledged_at`); `/static/style.css` 200 `text/css`. `curl -XPOST -H 'Accept: text/html' /api/jobs/fiscal/run` answered 303 → `/` in 2 ms and the worker ran `fiscal #13 ok (manual) rows 0` in 4 s. With another process holding the DB, `/` waited the 5 s UI lock wait and rendered the 503 busy page (DuckDB's lock message under Details); a CSV request started 7 s into a 12 s hold got 200 once the holder let go inside its wait; `/` answered 200 after release. SIGINT shut the scheduler and uvicorn down; no process left, port 8660 free. Wheel and sdist carry `web/templates/*.html` and `web/static/style.css`. Plan amended (Web UI): form posts get 303 back by `Accept` negotiation (a JSON 202 strands a browser and POST bodies are out), the `usfin.web.tables` helper, `ui_wait`, busy responses, `?all=1`. |
| S12 TreasuryDirect job | done | 2026-10-08 | bd72e5e | 17 new (test_treasurydirect); suite 251 passed (test_store's `version: 1` now reads the latest migration) | `usfin run treasurydirect` on `data/usfin.duckdb` (Mac clock 21:21 ET on 10-07): `treasurydirect #14 ok rows 250` in 5.4 s — 2 requests (`announced` 885 KB, `auctioned&days=7` 32 KB), migration 002 applied; `tsy_security` 247 `auctioned` (auction_date 2026-03-17..2026-10-07) + 3 `announced` (the 2026-10-08 4-/8-week bills and the 30-year bond reopening); watermarks `announced` and `results` both 2026-10-07 (the 10-08 auctions capped at the ET date); no alerts. Second run `#15 ok rows 0` in 5.4 s, table unchanged. Live probes for the fixtures: camelCase `search` 2010-03-01..03-10 gave 11 records (39 KB), lowercase names gave the whole history (37.6 MB, 11 143 records, earliest auction 1979-10-31 — S13 still probes the earliest window). Plan amended (TreasuryDirect): no old/new field names exist (one 120-key shape since 1979), FRN results live only in `highDiscountMargin` so the status rule and a `high_discount_margin` column include it, `announced` carries full results today, keys `announced`/`results`, drift once per new key, alert rules. |
| S13 TreasuryDirect backfill | done | 2026-10-08 | ea07743 | 10 new (test_td_backfill); suite 261 passed | Probe: `search` 1900-01-01..1979-10-30 → `[]`, ..1980-12-31 → 171 records from **1979-10-31** (recorded in How › TreasuryDirect). `usfin td backfill --from 1979-10-31` on `data/usfin.duckdb` (Mac clock 20:26 ET on 10-07, so today = 10-07): 565 monthly windows, 565 requests, 11 140 records, 10 893 rows new (the other 247 were S12's, unchanged) in 210 s (~2.7 req/s under the 4 rps bucket). `tsy_security` now 11 143 rows (11 139 `auctioned` 1979-10-31..2026-10-07, 4 `announced`: the three 2026-10-08 auctions and the 2001-09-11 4-week bill that never got results) — 8 352 bills, 2 337 notes, 454 bonds; the same 11 143 records S12's lowercase `search` returned; no alerts, watermarks unchanged (`results` 2026-10-07). Re-run: 1 request (the open October window), 564 skipped, rows 0 in 3.3 s. `usfin reparse treasurydirect` replayed 570 raw responses with rows 0 (table hash unchanged) in 3.4 s; on a scratch copy with `tsy_security` emptied it rebuilt all 11 143 rows to the identical hash in 4.8 s. After: `data/usfin.duckdb` 16.0 → 43.3 MB (the JSON payloads), `raw/` 2.5 → 5.8 MB (566 `td_search` bodies, 37.6 MB uncompressed, 3.4 MB of gzip blobs under `raw/treasurydirect`). Plan held; How › TreasuryDirect gained the earliest date and the backfill's dataset, resume and watermark rules (no `job_run` for the CLI load). |
| S14 Auction cross-check | done | 2026-10-08 | f6b4cde | 14 new (test_auction_xcheck); suite 275 passed | Before defining a mismatch, the real data: `fd_auctions` 7 894 rows (2000-01-03..2026-10-08) join `tsy_security` 1:1 on (cusip, auction_date), 0 one-sided either way in the overlap; every one of the five fields agrees on every pair (nulls included — the 3 2026-10-08 auctions and the 2001-09-11 bill have no results on either side); FD writes rates "3.735000", TD "5.3000"; 2 214 CUSIPs are reopened, so the CUSIP alone is no key. `usfin run fiscal` on `data/usfin.duckdb` (Mac clock 21:35 ET on 10-07): `fiscal #16 ok rows 0` then `auction_xcheck #17 ok rows 7894 (chained)` in 0.09 s, migration 003 applied; `auction_xcheck` = **7 894 matched, 0 mismatch, 0 pending, 0 fd_only, 0 td_only**; `usfin alerts` showed no `auction_mismatch` (live mismatch count 0). `usfin run auction_xcheck` again: `#18 ok rows 0`. On a scratch copy with a TD yield set to 5.31 (91282CRF0 10-07), an FD offering +1 bn (FRN reopening 91282CRD5 09-23), one bill's FD rates nulled and one bill CUSIP's 3 FD auctions deleted: `rows 6`, exactly 2 `auction_mismatch` alerts (one per pair, the right fields), 1 pending, 3 td_only, 7 888 matched; a second run raised no more and changed 0 rows. Plan amended: Jobs gains `Job.compute` for derived jobs (a no-source job has no raw responses for parsers, so the runner needed a hook); How › TreasuryDirect gains the comparison rules (pending for one-sided fields, overlap for one-sided rows, 6-decimal rate rounding, re-alert per day) and table `auction_xcheck`; Fiscal Data's `then: [auction_xcheck]` is now set. |
| S15 BLS client and series config | done | 2026-10-08 | bffa8ed | 13 new (test_bls_client); suite 288 passed | No `BLS_API_KEY` (no `.env`, unset in env), so keyless **v1**; 2 live requests in all (one curl with the client's User-Agent to record the fixture — the seven IDs plus a made-up `XXX000BOGUS`, 2025–2026 — and the Run). No 403: the `usfin/<version>` UA was enough, no extra header. `usfin bls validate` on `data/usfin.duckdb` (migration 004 applied, raw_id 648, 13 916 bytes, `text/plain`): all **7 of 7 ok** — CUSR0000SA0 2026 M08 (19 values), CUSR0000SA0L1E 2026 M08 (19), WPSFD4 2026 M08 (20), CES0000000001 2026 M09 (21), LNS14000000 2026 M09 (20), CES0500000003 2026 M09 (21), JTS000000000000000JOL 2026 M08 (20); exit 0; `bls_series` 7 rows. The 19/20 counts are the 2025 M10 gap: BLS publishes `"-"` with footnote X/9 (lapse in appropriations) for CPI, core CPI and UNRATE. In the recording `XXX000BOGUS` came back with empty data and "Series does not exist for Series XXX000BOGUS" under a `REQUEST_SUCCEEDED` status, which is how validate flags it. All seven plan IDs were right. Plan amended (How › BLS): bls.yaml shape and decimals, client API, migration 004, the live v1 response facts (unknown IDs don't fail the request, `-` values, stray catalog message, text/plain); v2 is not recorded (no key). |
| S16 BLS versioned observations | done | 2026-10-08 | 38691e1 | 20 new (test_bls_versions); suite 308 passed | No `BLS_API_KEY`, keyless v1; 2 live requests (the two runs). On `data/usfin.duckdb` (Mac clock 21:49 ET on 10-07): migration 005 applied; S15's `bls validate` response (raw 648, 21:42 ET) is already raw dataset `bls_timeseries`, so `usfin reparse bls` first stored it: **143 rows** (140 published values + the 3 `-` 2025 M10 gaps for CPI, core CPI, UNRATE, value NULL with footnote X/9), all `first_seen_at` 2026-10-08 01:42:35 UTC. `usfin run bls` → `bls #19 ok rows 0`, again → `#20 ok rows 0` (raw 649, 650: one request each for the seven keys, byte-identical 13 916-byte bodies, so even the first run had nothing new); watermarks CPI/core CPI/PPI/JOLTS 2026-M08, payrolls/UNRATE/AHE 2026-M09; no alerts. `bls_as_of(now())` for CPI: 2026 M08 334.131, M07 332.813, M06 332.568, M05 333.979; `bls_as_of(DATE '2026-10-07')` returns all 143 (21:42 ET is that New York day), `DATE '2026-10-06'` none. `usfin reparse bls` replayed 3 raw responses, deleted 143 and rebuilt 143, table md5 unchanged (960bd8ec…). `usfin schedule`: bls 09:00 on 10-08, 10-09, 10-13 (Columbus Day skipped). Plan amended (How › BLS): `-` periods stored as NULL versions (planning decision), migration 005 and the macro's argument types, one request for seven keys via the new `JobContext.memo` (Jobs), non-OK replies store nothing, **`bls validate` now stores observations** — it archives under the job's dataset, so without that a run after a validate dates values later than reparse does; `due: none` until S17, `value_jump` not wired (no date column). |
| S17 BLS release calendar and retry | done | 2026-10-08 | 2136670 | 26 new (test_bls_calendar); suite 334 passed | www.bls.gov 403s the plan's `usfin/<version> (+https://…)` User-Agent (also with browser Accept/Sec-Fetch headers or `From:`, and any UA carrying a URL); a UA with a contact email and no URL gets 200, so www.bls.gov now gets `usfin/<version> (<BLS_CONTACT_EMAIL>)` — new env var, set by the user in `.env` (was a block until then; a first run without it on `data/usfin.duckdb` failed #21 with ConfigError, no request, and raised alert #2 `calendar_stale`). Live with it set (Mac clock 2026-10-07 ET evening; migration 006 applied): `usfin run bls_calendar` → `#22 ok rows 313` (raw 651, 80 672 bytes `text/calendar`), again → `#23 ok rows 0` (C2); `usfin reparse bls_calendar` replayed 2 responses, 0 changes. **313 releases, 2025-01-03..2026-12-30**; 92 events map to configured series. `usfin bls releases --next 5`: 2026-10-14 08:30 CPI (CUSR0000SA0, CUSR0000SA0L1E); 10-15 08:30 PPI (WPSFD4); 11-03 **10:00** JOLTS; 11-06 08:30 Employment Situation (CES0000000001, LNS14000000, CES0500000003); 11-10 08:30 CPI. Calendar reaches 84 days ahead (> 30), so alert #2 was acked. `usfin schedule`: bls_calendar Mon 06:00 (10-12, 10-19, …). No api.bls.gov data request in this slice; retry tested with fakes. Plan amended (How › BLS, Conventions): iCal URL, the bls.gov User-Agent rule + `BLS_CONTACT_EMAIL`, release mapping (JOLTS releases at 10:00, after the 09:00 run, so its days always retry), `bls_release` + `bls_observation_dated` (migration 006), due days = release days (`due: {releases: bls_release}`, `alerts.DAY_SOURCES`), value_jump wired with per-series `jump_pct` and a period→date baseline (`Observed.jump_pct`, `watermark_date`), no generic zero_new_rows/stale_watermark for bls (period watermark; the retry raises `stale_watermark` past 12:00 ET), `Job.retry` + one-shot `bls_retry`. |
| S18 ALFRED backfill | blocked: FRED_API_KEY not set in .env (needed to record fixtures and run); add it and re-run `/plan-build docs/plans/us-fin-ingest.md --slice S18` | | | | |
| S19 EDGAR tickers and ticker_norm | done | 2026-10-08 | 141d84b | 21 new (test_edgar_tickers 10, test_ticker_norm 11); suite 355 passed | First live SEC traffic, 3 requests in all, no 403, `SEC_USER_AGENT` from `.env`: one `usfin http get` of company_tickers.json to record the fixture (raw 653, dataset `http_get`, 798 727 bytes, trimmed to 14 verbatim entries, 1 KB), then the Run. `usfin run edgar_tickers` on `data/usfin.duckdb` (Mac clock 22:27 ET on 10-07; migration 007 applied) → `#24 ok rows 10434` in 1.3 s; again → `#25 ok rows 0` (C2; same sha256 body); `usfin reparse edgar_tickers` replayed 2 responses, rows 0. `edgar_company` 10 434 rows, 8 016 CIKs, all active, 10 434 distinct `ticker_norm`. `usfin edgar company AAPL` → CIK 0000320193 Apple Inc., AAPL; `brk.b` → 0001067983 BRK-A/BRK-B (norm BRK.A/BRK.B); `ZZZZQ` exit 1. `usfin schedule`: edgar_tickers 07:15 daily (any). No alerts. Plan amended (How › SEC EDGAR, Nasdaq Trader): the file's shape and counts, `updated_at` = the changing response's `fetched_at` (so reparse is identical), EDGAR suffixes `-UN`/`-WT`/`-RI` (units won't meet Nasdaq `.U` under `ticker_norm` — S31 decides), and a SEC 403/block makes the run `failed`, not `skipped` (runner not wired; left to S22). |
| S20 EDGAR submissions | done | 2026-10-08 | a679aac | 17 new (test_edgar_submissions); suite 372 passed | Live, no 403, `SEC_USER_AGENT` from `.env`. Probes via `usfin http get` (7 requests): Apple's JSON and its one page (fixtures, trimmed to 7 + 4 filings), two Apple 8-K `-index-headers.html` (proved `acceptanceDateTime`'s `Z` is false: header 16:30:28 EDT vs JSON 00:30:28Z next day, 16:30:33 EST vs 02:30:33Z), Berkshire (1 page, so not heavy enough) and JPMorgan (70 pages). On `data/usfin.duckdb` (migration 008 applied): `usfin edgar submissions --cik 320193` → `#26 ok rows 2264` (1 000 recent + 1 264 on page 001, 1994-01-26..2026-10-05), 2 requests, 3.8 s; `--cik 19617` (JPMorgan, the paged-history check) → `#27 ok rows 168590` (26 311 recent — one year — + 142 279 on 70 pages, exactly the listed counts, 1994-01-20..2026-10-07; 130 136 424B2), 71 requests, 29.8 MB of JSON, 108 s, nearly all of it SEC transfer time (parsing all 75 responses takes 2.8 s); both again → `#28 ok rows 0`, 1 request per CIK (pages skipped by listing), 30.5 s (JPMorgan's 4.6 MB main JSON); `usfin reparse edgar_submissions` replayed 75 responses, rows 0; on a scratch copy with the three tables emptied it rebuilt 170 854 filings, 170 854 parties and 71 pages to the identical md5 in 3.8 s. `usfin run edgar_submissions` (no `pending_doc` yet) → `#29 ok rows 0`, no request. Watermarks 0000320193 = 2026-10-05, 0000019617 = 2026-10-07; no alerts; `raw/` 9.9 MB, DB 80 MB. Plan amended (How › SEC EDGAR, Submissions): false-UTC acceptance times, `filed_at` DATE, fill rule as shared code, party role `submissions` (S22 decides cik precedence over it), pages numbered newest first with listing dates ≠ filing dates so tracked by listing in new `edgar_submissions_page`, keys from `pending_doc`, `--cik` CLI. |
| S21 EDGAR companyfacts | done | 2026-10-08 | 1d73942 | 18 new (test_edgar_companyfacts); suite 390 passed | Live, no 403, `SEC_USER_AGENT` from `.env`, 6 requests in all. Probes via `usfin http get` (raw 737/738, dataset `http_get`): Apple companyfacts 3 789 099 bytes, JPMorgan 7 977 054 bytes (taxonomies dei, us-gaap, invest, srt, ffd, ecd); fixtures trimmed to 9 + 8 verbatim facts (instant, duration, Apple FY2009 `SalesRevenueNet` 36 537 M in the 10-K of 2009-10-27 restated to 42 905 M by the 10-K/A of 2010-01-25, USD/shares, a DEF 14A fact with null `fy`/`fp`, and `invest`/`srt`/`ecd` to be dropped). On `data/usfin.duckdb` (migration 009 applied): `usfin edgar facts --cik 320193` → `#30 ok rows 25135` (505 tags, filed 2009-07-22..2026-07-31), 1 request, 5.2 s; `--cik 19617` → `#31 ok rows 49946` (920 tags, filed 2009-08-10..2026-08-06), 5.7 s — nearly all transfer; both again → `#32 ok rows 0` (same sha256 bodies), 17.2 s of SEC transfer; `usfin reparse edgar_companyfacts` replayed 4 responses in 1.7 s, rows 0; on a scratch copy with `xbrl_fact` emptied, reparse rebuilt 75 081 facts to the identical md5 in 1.6 s (~95 000 facts/s parsed). `xbrl_as_of`: Apple revenue tags as of 2025-10-30 vs 2025-10-31 = 325 vs 328 rows, nothing removed, the 3 added all accn 0000320193-25-000079 (10-K filed 2025-10-31); as of 2009-10-26 vs 2010-01-25 = 4 vs 12, the 8 added from the 10-K, 10-Q and 10-K/A filed in between. Storage: JSON gzips to 6.5 % in `raw/` (256 KB + 503 KB, shared by the 3 fetches each); **xbrl_fact 91 bytes/fact with the PK, 21 without** (fresh files, checkpointed; the DB file itself barely moved, 71.6 → 72.1 MB, as freed blocks were reused). Plan amended (How › SEC EDGAR › Companyfacts): document shape, nullable `fy`/`fp`, `frame` moves, types, `xbrl_as_of` definition, the PK's storage cost as a lever for S26. |
| S22 EDGAR feed and document queue | done | 2026-10-08 | 5a97746 | 45 new (test_edgar_feed, 18 of them form→kind cases); 3 changed (tickers: a run inside the block is `skipped`; submissions/companyfacts: `pending_doc` now comes from migration 010); suite 435 passed | Live, no 403, `SEC_USER_AGENT` from `.env`. Fixtures: 4 probes via `usfin http get` (raw 743–746: the unfiltered feed and `type=10-Q`, `8-K`, `SC 13G`), trimmed to 9 + 2 + 2 + 0 verbatim entries (13G Filed by/Subject, Form 4 Reporting/Issuer, a Form 4 with 4 owners, 8-K, 8-K/A, 10-Qs); **no legacy `SC 13G` was in the feed** (that query: "No recent filings", kept as the empty-feed fixture), so legacy names are tested on `pending_kinds` only. `usfin run edgar_feed` on `data/usfin.duckdb` (Mac clock 23:07 ET on 10-07; migration 010 applied): `#33 ok rows 625` — 10 pages (the cap; 1 000 entries back to 16:15 ET), **622 filings** (189 `4`, 96 `424B2`, 43 `8-K`, 49 Schedule 13D/G (+/A), 3 `10-Q` …), parties Filer 353 / Reporting 263 / Issuer 215 / Subject 108 / Filed by 51 / Depositor 1 / Securitizer 1, every Form 4 `cik` = its Issuer; pending: form4 193 (112 CIKs), sched13 49 (46), companyfacts 3 (3 10-Q filers), submissions 6 (JPMorgan 424B2s); chained `edgar_submissions #34 ok rows 0` (1 request, JPMorgan 4.6 MB, pages skipped) and `edgar_companyfacts #35 ok rows 50 777` (3 requests, 7.7 MB); all 9 submissions/companyfacts rows closed (each document carried its accession); **14 requests, 13.4 s**. Those 6 JPMorgan 424B2s exposed the S20 time bug: submissions had stored 16:24:53 for a filing the feed accepted 16:24:53-04:00 (20:24:53Z) — same-day JSON times are true UTC — so the feed's `accepted_at` now wins; `usfin reparse edgar_feed` replayed the 10 pages and fixed the 6 (rows 6), again rows 0. Second run `#36 ok rows 0`: **1 request** (stopped at watermark 2026-10-08T01:59:16), chain `#37`/`#38` rows 0, no request. No alerts. Plan amended: How › HTTP (SEC skip wired), Jobs (`after_key`), SEC EDGAR › Filings (feed roles override `cik`, feed `accepted_at` wins), Submissions (the S20 time conversion is wrong for same-day filings — contradiction recorded, rule left to S23/S26/user), Feed (live shape, 10 pages ≈ 6 h of an evening, queue/drain rules, chain without form4/sched13 until S24/S25, watchlist seed, schedule). |
| S23 EDGAR daily-index reconcile | done | 2026-10-08 | 9a5fd8f | 25 new (21 test_edgar_dailyindex, 4 test_edgar_submissions for the same-day time rule); suite 460 passed | Live, no 403, `SEC_USER_AGENT` from `.env`. Probes via `usfin http get`: `master.20261006.idx` (raw 762; 310 875 bytes, 3 501 lines, 2 370 accessions; fixture trimmed to 13 lines / 8 accessions: a Form 4 under 3 non-adjacent CIKs, a 4/A, a one-line Form 4, a SCHEDULE 13G and 13D/A, a DRS dated 09-29, a JPMorgan 424B2, the first and last lines) and one `-index-headers.html` (raw 763) for the time rule. **Loose end (submissions times):** decided per filing by `filingDate` = the response's New York fetch date → true UTC, else converted; evidence: the 6 feed-overlap 424B2s equal to the second, and 0001213900-26-107349 JSON 01:11:28Z = header 20261006211128 ET, FILED AS OF 20261007 (6 such evening filings dated the next day — an acceptance-date rule would get them wrong); a submissions time now replaces a stored one unless a feed role exists. `usfin reparse edgar_submissions` (76 responses) → **rows 72**, all +4 h (JPMorgan filings of 10-07 the feed had not seen), again → rows 0; `reparse edgar_feed` rows 0. `usfin run edgar_reconcile` at 23:21 ET on 10-07 (window 09-30..10-07, 6 federal days, 6 requests, all 200 — 10-07's index was already out) → `#39 ok rows 17237`; per day master.idx accessions vs `edgar_filing`: 09-30 2 627/2 627, 10-01 3 898/3 898, 10-02 4 413/4 413, 10-05 3 037/3 037, 10-06 2 370/2 370, **10-07 2 247/2 247 (622 feed + 1 547 index + 78 submissions)**, each exactly once, every distinct (accession, CIK) pair in `edgar_filing_party` (0 missing); all 628 feed accessions were in 10-07's file; 26 pairs listed 2–3 times verbatim; no accession in two days. Watermark `master` = 2026-10-07; `edgar_filing` 171 476 → 188 713, `index` parties 27 516; `pending_doc` form4 4 499 / sched13 491 open (4 748 queued by the index; drained by S24/S25). Second run `#40 ok rows 0` (6 requests, 7.8 s), tables hash-identical; `reparse edgar_reconcile` (12 responses) rows 0; no alerts. Requests in all: 14 (2 probes + 2 × 6 days). Plan amended: How › SEC EDGAR › Daily index reconcile (window was max(wm−7d, today−7d), which ignores outages against C3 — built min(watermark, today−7d), here and in the S23 entry; 404 rules; live format; queue CIK; chain left to S24/S25 on both jobs), Submissions (same-day rule decided, 72 rows), Filings (submissions time replaces unless a feed role). |
| S24 Form 4 | done | 2026-10-08 | 6786edc | 14 new (test_form4); 1 changed (test_edgar_feed: the feed chain now runs `edgar_form4` too, so the test no longer queues a form4 row it expects left open); suite 474 passed | Live, no 403, `SEC_USER_AGENT` from `.env`. Probes via `usfin http get` (raw 776–787, 12 requests, dataset `probe_form4`): 6 folder `index.json` + 6 XMLs; fixtures are 4 of them byte for byte, untrimmed — Costco 0000909832-26-000090 (non-derivative; index-only, queued under **owner** 1928633, and the folder answered under that CIK), StageWise 0001213900-26-107674 (sale + indirect `nonDerivativeHolding`), Skyworks 0001193125-26-417218 (2 non-derivative + 10 derivative RSU lines, flags `true`/`false`, footnote-only exercise price/dates; 27 KB, kept whole rather than cut) and BNB Standard 0000921895-26-002756 (**4/A by two reporting owners**, derivative transactions + `derivativeHolding`, footnote-only prices/dates). No smaller derivative-only Form 4 was among the 4 sampled. `usfin run edgar_form4` on `data/usfin.duckdb` with a scratch config capping 50 (migration 011 applied, 23:34 ET on 10-07): `#41 ok rows 220` in 36 s — **100 requests (50 × 2), all 200, 34.6 s, 0.51 MB of XML**; 50 `insider_filing` (all `4`: the 50 oldest were feed-queued at 03:06Z, so their queued CIK was the issuer; 0 mismatches between queued CIK, `edgar_filing.cik` and the XML issuer), 54 owners (2 joint filings), 220 lines (118 non-derivative transactions, 16 non-derivative holdings, 71 derivative transactions, 15 derivative holdings; codes M 55, A 53, S 50, F 16, C 8, J 6, P 1), 202 footnotes; 10 transactions with NULL price, every one footnote-only; pending `form4` 4 499 → 4 449 open. `usfin reparse edgar_form4` replayed 100 responses, **rows 0**, the four tables hash-identical, 1.1 s. **Projection:** ~0.7 s/document → the 4 449 left ≈ 52 min of requests, ~15 runs at the default cap 300 (~3.5 min each, inside the 15-min feed cycle). Plan amended (How › SEC EDGAR › Form 4: one table became four — owners, holdings, derivative terms and footnote-only values did not fit `insider_txn` alone —, element mapping, multiple-owner and amendment rules, fetch path under the queued CIK, per-accession drain with the `max_docs_per_run` cap; Feed and Daily index: chain now includes `edgar_form4`). |
| S25 Schedule 13D/G | done | 2026-10-08 | 9205e4b | 15 new (test_sched13); 1 changed (test_edgar_feed: the feed chain now runs `edgar_sched13` too); suite 489 passed | Live, no 403, `SEC_USER_AGENT` from `.env`. Probes (raw 888–917, 30 requests, dataset `probe_sched13`): 7 folder `index.json` + 23 `primary_doc.xml`; fixtures are 5 filings byte for byte, untrimmed — OnKure SCHEDULE 13G 0001104659-26-114447 (5 joint persons, **no CIK element on any 13G person**), Calamos Aksia SCHEDULE 13D 0001104659-26-114474 (one person with `reportingPersonCIK`), Mid Penn SCHEDULE 13D/A 0001193125-26-416960 (amendment 7, 2 of 3 persons `reportingPersonNoCIK` Y; index-only, queued under reporting person 1548007, subject 879635 from the XML), Meteora SCHEDULE 13G/A 0001905106-26-000171 (amendment 11, no `memberGroup`) and JPMorgan's legacy **SC 13G** 0000019617-24-000609 (folder lists only its `.htm`). Structured XML since 2024-12-18 confirmed: newest legacy `SC 13D/G` name in `edgar_filing` is 2024-12-04, all 491 queued rows are `SCHEDULE 13D/G` (+/A). `usfin run edgar_sched13` on `data/usfin.duckdb` with a scratch config capping 50 (migration 012 applied, 23:46 ET on 10-07): `#42 ok rows 162` in 39.6 s — **100 requests (50 × 2), all 200, 38.6 s, 0.55 MB of XML** (~0.77 s/doc); 50 `ownership_filing` (13D 6, 13D/A 9, 13G 13, 13G/A 22; 31 amendments, up to no. 26; 47 subjects; 1 to 12 persons each), 162 `ownership_stake` rows — 105 13G persons all without CIK, 57 13D persons of which 29 without —; percent 0–79.3, every person with shares, percent and event date; **legacy skipped 0** (none queued; the skip is covered by the fixture); 1 of 50 queued under a non-subject CIK; pending `sched13` 491 → 441 open. `usfin reparse edgar_sched13` replayed 150 responses (incl. the 50 Form 4 folder listings of the shared `edgar_filing_index`), **rows 0**, both tables and the insider tables hash-identical, 0.9 s. **Projection:** ~0.77 s/doc → the 441 left ≈ 5.7 min of requests, 2 runs at the default cap 300. Plan amended (How › SEC EDGAR › Schedule 13D/G: 13G persons never carry a CIK, so `filer_cik` is NULL for all of them, not an edge case; `ownership_filing` added for subject/CUSIPs/amendment/legacy count; legacy skip = folder listing + `legacy` row; element mapping; fetch, queue and cap as Form 4; Feed and Daily index: chains now end with `edgar_sched13`). |
| S26 EDGAR bulk backfill | done | 2026-10-08 | 5f6e57d | 20 new (test_edgar_bulk 8, test_disk_guard 10, test_http 2 for a stream cut mid-body); suite 509 passed | Live, no 403, `SEC_USER_AGENT` from `.env`. Free disk at start 19.6 GiB (`df` 20.6 M KB; DB 99 MB, raw 13 MB); `--check` HEADs: **companyfacts.zip 1 410 110 606 bytes (1.31 GiB), submissions.zip 1 567 815 592 bytes (1.46 GiB)** → 19.6 − 2.77 = 16.85 GiB ≥ floor 10, both allowed. The first companyfacts download died after ~10 min with no blob or row (cause not captured; an httpx read error mid-body escaped the client) → client now retries a cut body. Then companyfacts **618 s** (~2.2 MB/s), submissions **650 s**, each downloaded once and parsed with `--reuse` after. Free disk moved outside usfin during the run (19.6 → ~63 GiB mid-run, something else on the Mac freed space); `df` after: 52.0 GiB free (54 509 176 KB), DB 219 MB, raw 2.8 GB (both ZIPs kept for S27). **companyfacts**: 20 438 members (17.99 GiB uncompressed); sample of 200 spread members: 199 parsed, 1 154 272 facts in 14.7 s (13.5 members/s, ~78 500 facts/s); 1 member was `{}` (62 such in the ZIP: now recorded, no rows); DB 99.4 → 192.7 MB. **Projection with the PK: DB-delta 8.93 GiB per member / 9.05 GiB by member bytes; scratch xbrl_fact 90 B/fact → 9.93 / 10.07 GiB (~118 M facts). Without the PK: 20 B/fact → 2.21 / 2.24 GiB.** Parse time ~25 min (+ ~10 min download). **submissions**: 993 246 members (5.36 GiB uncompressed); two 200-member samples: 4 190 filings in 17.6 s (11.4 members/s, DB +23.6 MB) and 5 521 in 13.3 s (15.1 members/s, DB +3.7 MB) — the DB-delta projection swung 136 → 17 GiB (allocation churn, not row cost; plan amended); scratch of the sample 421 B/filing (≈ 9 blocks, overhead-bound) → 10.6–11.0 GiB; **calibrated on the 198 415 filings already stored: 165 B/filing (party + PKs) × ~24 M filings ≈ 3.7 GiB**. **Parse time ~18–24 h** (1 064–1 453 min, ~70–90 ms per member on a 200 MB DB) — the full submissions load is a day-long run, which S27 must plan for. **Fit:** full load = companyfacts + submissions + 2.8 GiB of ZIPs (5.5 GiB once prune keeps 2 per dataset): with the PK ≈ 10.1 + 3.7 + 2.8 = 16.6 GiB, without ≈ 2.2 + 3.7 + 2.8 = 8.7 GiB. Against the ~18–19.6 GiB free at slice start, **neither leaves the 10 GiB floor** (≈ 1–3 GiB left with PK, ≈ 9.3–10.9 GiB without, at or just under the floor); against today's 52 GiB both fit. `--estimate` prints `fits` for both (guard uses the newest DB-delta: companyfacts 9.05, submissions 16.97 GiB). The PK decision (S21 lever) and whether the free space stays at 52 GiB are the user's call before S27. Bulk acceptance times checked against S23's rule: ZIP built 2026-10-07 00:20, newest filingDate 10-06; strings equal the per-CIK JSON for all shared filings (JPMorgan 26 233, Apple 1 000) → converting all is right. Plan amended (How › SEC EDGAR › Bulk: CLI flags, HEAD dataset, GiB, spread sample, `{}` members, stream-cut retry, DB-delta unreliable + scratch/calibrated measure; Submissions note). |
| S27 Weekly bulk reconcile and prune | done | 2026-10-08 | 605b2e2 | 23 new (test_edgar_bulk_reconcile 10, test_xbrl_fact_no_pk 9, test_edgar_bulk 2, test_disk_guard 2); suite 532 passed | Live, no 403, `SEC_USER_AGENT` from `.env`. Free disk at start 47.9 GiB (`df` 50 182 532 KB), DB 219 MB, raw 2.8 GB. Migration 014 (no PK) on the real DB in 1.6 s: 1 280 130 facts kept, 0 duplicate keys, `xbrl_as_of` works. **Run 1** `USFIN_BULK_LIMIT=200 usfin run edgar_bulk` (#43, 16 min 52 s, almost all download): new ZIPs (companyfacts 1 410 169 309 B, submissions 1 568 085 395 B, SEC rebuilt both since S26), guard passed, 200 members each; **companyfacts key failed on one member: CIRCOR (CIK 1091883) files `fy` 44561** (an Excel serial date) → S21's SMALLINT was wrong, migration 015 makes `fy` INTEGER; submissions ok; prune kept 2 per dataset (nothing pruned). **Projection recomputed** (no PK, ticker CIKs): companyfacts 20 B/fact × ~118 M ≈ 2.2 GiB; submissions only 10 757 of 993 246 members (8 016 CIKs, 19.7 % of member bytes) ≈ 4.7 M filings × 165 B ≈ 0.8 GiB; + 2.8 GiB for the second ZIP pair ≈ 5.8 GiB vs 43.0 GiB free − 10 floor → fits (even the guard's conservative DB-delta figures, 9.05 + 16.97 GiB, fit). **Full load** (`--reuse` on the job's ZIPs): companyfacts 28 min 2 s (parse 1 674 s, ~12 members/s, ~72 000 facts/s), 20 034 parsed, **120 221 183 facts**, 61 `{}`, **8 errors: members with no `cik` key** (funds with only vip/cef/oef facts; Star Mountain 4 588 and ZeroStack 6 027 facts) → the name now decides when `cik` is absent; the 8 reloaded with `--cik` (9 811 facts); DB 0.21 → 2.57 GB (+2.36 GB, ~20 B/fact as projected). Submissions 24 min 8 s (parse 1 444 s), 10 555 of 10 759 wanted members (the rest stored by the samples), **6 601 482 filings**, 982 713 members not in `edgar_company` skipped, 0 errors; DB → 4.47 GB (+1.90 GB, ~288 B/filing incl. parties and pages, above the 165 B calibrated — churn of 10 k transactions). Totals: `xbrl_fact` 122 683 478 facts / 17 133 CIKs, **0 duplicate keys**; `edgar_filing` 6 880 951 (13 693 CIKs), parties 6 969 828, pages 2 744. **Run 2** uncapped (#44, 12 min 48 s): same SHA-256 for both ZIPs (one blob each, nothing new on disk), **0 members changed, rows 0**, watermarks companyfacts/submissions = 2026-10-08, prune nothing. End: free 38.3 GiB (40 188 000 KB), DB 4.47 GB, raw 5.6 GB. Plan amended: Companyfacts types (`fy` INTEGER, absent `cik`), Bulk (no-PK upsert, filter, job, scratch only for samples), Jobs (`JobSkipped`), env `USFIN_BULK_LIMIT`. |
| S28 Frames and concept map | done | 2026-10-08 | 8808d7d | 32 new (test_frames: cache/TTL with a fake clock, 404 not cached, no `xbrl_fact` writes, period check before any request, no UA → no request, concept-map validation, first-tag pick and tag shown, `frame` column not dates, `I` periods, CLI); suite 564 passed | Live, no 403, `SEC_USER_AGENT` from `.env`, **2 requests** in all: the fixture probe (`usfin http get`, raw 1033, 296 137 bytes, trimmed to 3 of 1 939 points) and `usfin edgar frame us-gaap Revenues USD CY2025Q2` on `data/usfin.duckdb` (migration 016 applied): `fetched` raw 1034, points 1 939 (pts 1 939), top Walmart 177.4 B (fiscal May–Jul fitted to CY2025Q2), UnitedHealth, CVS, Alphabet, Berkshire; 4.0 s; again → `cached`, no request, 0.3 s. `usfin edgar screen revenue CY2025Q2`: **4 224 companies** (3 736 with a ticker name) — tag used: Revenues 1 935, RevenueFromContractWithCustomerExcludingAssessedTax 2 003, RevenueFromContractWithCustomerIncludingAssessedTax 286, SalesRevenueNet 0, SalesRevenueGoodsNet 0 (814 companies report more than one candidate, the first listed wins; Amazon and McKesson show the ASC 606 tag next to Walmart's `Revenues`); query **1.76–1.82 s cold** in the CLI process (whole command 2.1–2.6 s), **0.37–0.49 s warm**, no index (none needed; nothing added to the DB but the empty `frames_cache`). Screen vs frames API for Revenues USD CY2025Q2: 1 939 API points vs 1 935 `xbrl_fact` rows, 1 932 CIKs in both with identical periods; 42 differ in `accn` (the API's frame moved to a 2026 filing the bulk companyfacts member lacks — UDR, HCA, CTS, Lazard; members' newest facts filed 2026-04), 2 in value (Tiptree, Sun Communities), 7 only in the API (stale/factless members: OGE newest fact 2020, Highwoods none), 3 only in `xbrl_fact`. Plan amended (How › SEC EDGAR › Frames: document shape, module, CLI, frames fresher than companyfacts.zip; Concept map: per-concept taxonomy/unit — the plan's tag-list format could not express units or dei — period → rows by the `frame` column, `I` also on short durations, current-view only, query speed). |
| S29 Nasdaq Trader snapshot | done | 2026-10-08 | e5a4631 | 11 new (test_nasdaq_snapshot); suite 575 passed | Fixtures recorded live (curl, 2026-10-08 07:19 GMT), trimmed to header + rows + trailer byte for byte, plus `nasdaqlisted_truncated.txt` = the real file's first 300 bytes (cut mid-row). `usfin run symbols` on `data/usfin.duckdb` (Mac 14:24 WIB = 03:24 ET on 10-08; migration 017 applied) → `#45 ok rows 13253` in 5.5 s: nasdaqlisted 5 619 rows (8 test issues dropped, 1 293 ETFs, all exchange Q), otherlisted 7 634 (29 test issues dropped, 4 474 ETFs; N 2 900, P 2 753, Z 1 656, A 309, F 16); both snapshot_at 2026-10-08 07:03 UTC (03:03 ET), raw 1035/1036 (349 272 / 542 931 bytes); watermarks `2026-10-08T07:03:00` per file. Second run → `#46 ok rows 0` (same bodies, C2); `usfin reparse symbols` replayed 4 responses, rows 0. No alerts (first snapshot: no swing check). Plan amended (How › Nasdaq Trader): file shape, trailer padding and ET, `NextShares` dropped, exchange codes, `raw_id` column, per-file watermark, trailer check in fetch with alert `snapshot_rejected` (the plan named no rule) and reparse skipping rejected bodies, the symbols alert rules. |
| S30 Symbol history diff | todo | | | | |
| S31 Ticker join and unmatched list | todo | | | | |
| S32 UI: series browser | todo | | | | |
| S33 UI: EDGAR company and screen | todo | | | | |
| S34 UI: unmatched symbols | todo | | | | |
| S35 Ops: doctor, launchd, README, live smoke | todo | | | | |
- Decisions after S26 (user, 2026-10-08): drop the `xbrl_fact` PK (~2.2 vs ~10.1 GiB); submissions bulk only for `edgar_company` CIKs (full ZIP would take ~1 day for 993k filers); keep the last 2 bulk ZIPs.
