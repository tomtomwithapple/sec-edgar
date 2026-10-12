# Alpaca market data (usfin)

Status: review folded in — next: slicing
Goal: Add free Alpaca US market data (daily bars, the asset list, corporate actions) to usfin as
new jobs next to EDGAR: archived raw first, stored point-in-time in `data/usfin.duckdb`,
scheduled, and shown in the existing web UI.

Builds on [us-fin-ingest.md](us-fin-ingest.md) (status done): its core (raw store, HTTP client
and limiter, jobs, watermarks, reparse, calendars, alerts, scheduler, UI) is reused unchanged
except where this plan says so.

## Why

usfin has US fundamentals (EDGAR XBRL facts, filings), Treasury and BLS data, but **no prices**,
so nothing in it can be backtested or valued. It is the US leg of a cross-market investment
system (home market IDX; US, EU, Japan and Singapore are candidates) that must use free data
only. The user has a free Alpaca paper account, whose market data plan covers it.

Measured live on 2026-10-12 with the user's paper key (free plan; planner and reviewer):

- Daily bars, `feed=sip`: from **2016-01-04** to the last close (AAPL 2 708 bars in one
  request, 1.4-8.5 s); `feed=iex` only from 2020-07-27. Multi-symbol requests work. A 10 000-bar
  page takes 1.8-7.2 s and is ~1.1 MB. Header `X-Ratelimit-Limit: 200` (per minute) and
  `X-Ratelimit-Reset`; no `Retry-After`.
- `adjustment=raw` is unadjusted (AAPL 2020-08-28 499.23 → 2020-08-31 129.04 at the 4:1 split).
  Bar `t` is 04:00Z or 05:00Z (midnight New York).
- **Symbol mapping (`asof`).** By default Alpaca maps a symbol to its current security's whole
  history: `symbols=META` returns 2 708 bars from 2016 (Meta's years as FB included). With
  `asof=-` it returns what traded under that symbol then: META 1 326 bars from 2021-06-30 (the
  Roundhill ETF until 2022-01-28, renamed METV on 2022-01-31; then a flat 12.31 until 2022-06-08;
  then Meta); FB 1 945 bars (Meta until 2022-06-08, a gap, the new FB from 2025-06-26).
- Assets: one `/v2/assets?asset_class=us_equity` call without `status` returns all 33 575 rows
  in ~23 s (14.6 MB); 13 240 active and 2 875 inactive are on a listed exchange, the rest OTC.
  240 listed symbols have ≥ 2 asset ids (224 of them one active and one inactive, often the
  same name, e.g. IBTA, HAO, JDIV). The asset list has no dates.
- Delisted symbols keep their bars but are mostly not in the asset list: TWTR is only reachable
  as `acquiree_symbol` of its cash merger; SIVB is in neither the list nor any action under that
  symbol (its only action is a `worthless_removal` of the OTC symbol SIVBQ).
- Corporate actions `/v1/corporate-actions` go back to at least 2016 for the whole market;
  `start`/`end` filter on **`process_date`**, which for a dividend is its payable date, weeks
  after the ex-date (AAPL's 2020-08-07 ex-date dividend appears only in a query covering
  2020-08-13). Future-dated actions exist (> 1 000 over the next 5 months). Sep 2026 had 6 160
  actions over 7 pages of 1 000. Types include cash dividends, forward/reverse splits, name
  changes (old → new symbol and CUSIP, `process_date` only), cash/stock mergers
  (`acquiree_symbol`/`acquiree_cusip`/`acquirer_*`/rates), worthless removals, spin-offs, rights.
- Adjustment check (reviewer): split factor `old_rate/new_rate` and dividend factor
  `1 − rate / close of the last bar before ex_date` reproduce Alpaca's `adjustment=all` within
  0.023 % (AAPL), 0.194 % (MSFT), 0.017 % (KO).

When this is done, these are true (each is covered by at least one slice's test):

- **A1 Raw first.** Every Alpaca response a job or the backfill makes is archived (source
  `alpaca`) before parsing, and `usfin reparse <job>` rebuilds that job's tables from the raw
  store with no request made; derived objects (universe, security map, adjusted prices) are
  views/macros or rebuilt by the parsers, so they follow. The API key and secret never appear in
  the raw store, `raw_response`, `request_sha`, captured logs, error messages or the UI.
- **A2 Bars.** `alpaca_bar` holds unadjusted daily bars (open, high, low, close, volume,
  trade count, VWAP) keyed by (symbol, date, `first_seen_at`), fetched with `asof=-` from
  2016-01-04; `date` is the New York date of `t`. A second run over the same window writes
  nothing (C2). A bar whose compared columns (o, h, l, c, v, n, vw — exact equality) differ from
  its newest stored version inserts a new version with `first_seen_at` = the raw response's
  `fetched_at`; `alpaca_bar_as_of(ts)` returns, per (symbol, date), the newest version with
  `first_seen_at <= ts` (C4, same convention as `bls_as_of`). A dedupe test proves no duplicate
  (symbol, date, first_seen_at) exists after any sequence of runs.
- **A3 Universe.** The universe is every symbol of the asset list on NYSE, NASDAQ, AMEX, ARCA or
  BATS (active or inactive), plus every `acquiree_symbol` of a cash/stock/stock-and-cash merger,
  the symbol of a worthless removal, and the `old_symbol` of a name change. TWTR (via its cash
  merger) is in it; OTC-only symbols are not. SIVB is **not** and is the documented example of
  the survivorship gap (see Out of scope); the universe slice reports how many symbols came from
  each rule.
- **A4 Assets history.** Each daily asset snapshot (one call, one `snapshot_at`) keeps the
  tracked columns (symbol, name, exchange, status, tradable, class) for every asset (OTC
  included), and is diffed into a type-2 table keyed by the Alpaca asset `id`, so an asset's
  status, exchange, name and tradability on a past date can be read, like `symbol_history`. A
  row-count swing beyond 2 % stores the snapshot but does not diff it (C9 pattern).
- **A5 Corporate actions.** `alpaca_corp_action` keeps every action with `process_date` from
  2016-01-01 on, with type, symbols, CUSIPs, dates and rates (merger fields included) and its
  full JSON; an action whose `id` comes back with an identical payload inserts nothing, a
  changed payload inserts a new version (`first_seen_at`). Daily runs query
  watermark − 30 days to today + 90 days, so announced actions arrive before their ex-date.
- **A6 Adjusted prices.** `alpaca_bar_adjusted` gives, per security (A7) and date, close,
  adjusted close, split factor and total-return factor from A2 + A5, chained across renames.
  AAPL across the 2020-08-31 split has no jump, and AAPL, MSFT and KO match Alpaca's own
  `adjustment=all` close within 0.5 % on every checked day (live test).
- **A7 Security identity.** A price series is read per security, not per symbol. Lookups by
  today's symbol return: META → FB bars 2016-01-04..2022-06-08 plus META bars from 2022-06-09;
  FB → only bars from 2025-06-26; METV → METV bars plus META bars 2021-06-30..2022-01-28; TWTR →
  TWTR to 2022-10-27; IBTA (two asset ids, same name) → one security.
- **A8 EDGAR link.** The current symbol of an **active** security joins to its EDGAR company
  (CIK) through the existing `ticker_norm`; the EDGAR company page shows the latest close and a
  1-year chart. `/prices/universe` can filter matched/unmatched. Delisted securities are not
  joined (`edgar_company` keeps only active pairs).
- **A9 Schedule and alerts.** Each job has a `schedule.yaml` entry with explicit rules:
  `alpaca_bars` 20:30 ET on NYSE days (`zero_new_rows {after: "20:30"}`,
  `stale_watermark {n: 1}`, no `value_jump`); `alpaca_assets` daily 07:30 ET
  (`row_count_swing {pct: 2}`, `stale_watermark {n: 1}`); `alpaca_corp_actions` daily 07:45 ET
  (`stale_watermark {n: 2}`). Missed runs catch up (C6, C7, C8). Backfill progress raises no
  alert.
- **A10 Rate limits and backfill.** The two Alpaca hosts each have their own `hosts.yaml` bucket
  at ≤ 3 requests/s (180/min, under 200/min). A 429 is retried by the existing client, waiting
  until `X-Ratelimit-Reset` when present. The full bars backfill (~16 k symbols, 2016 on) resumes
  per batch after a stop and opens a DB session per batch, so `usfin serve` jobs are not locked
  out for hours.
- **A11 UI.** `usfin serve` gains `/prices` (symbol search, adjusted/unadjusted chart, table,
  CSV) and `/prices/universe` (status, exchange, delisted, matched filters), both in the nav and
  in the app-wide page test; every table has a CSV via `?format=csv&table=<name>`.
- **A12 Ops.** `usfin doctor` reports `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` as `info`
  set/unset (never the value) and checks `config/alpaca.yaml`; with either key unset the Alpaca
  jobs and `usfin http get` to an Alpaca host raise `ConfigError` and send nothing. README and
  `.env.example` document them.

## What

In scope:

- Alpaca HTTP access: two hosts (`data.alpaca.markets`, `paper-api.alpaca.markets`), key
  headers injected by `http.Client`, rate limits, 429 handling, archiving.
- Jobs `alpaca_assets`, `alpaca_corp_actions`, `alpaca_bars`, and the backfill commands
  `usfin alpaca backfill bars|corp-actions`.
- Tables + migrations: asset snapshots, diffs and history; corporate actions (versioned); bars
  (versioned); views/macros for universe, security map, adjusted prices, `alpaca_bar_as_of`.
- Schedule entries, alert rules, doctor checks, reparse support.
- UI pages `/prices`, `/prices/universe`, and the price block on the EDGAR company page.

**Out of scope:**

- Orders, positions, fills, paper trading — a later, separate execution service.
- Any paid Alpaca plan, real-time/streaming data, intraday bars, quotes and trades, options,
  crypto, news; Alpaca's `/v2/calendar` (usfin's own `nyse` calendar stays the schedule).
- OTC bars (OTC assets are kept in the asset snapshot only).
- Delisted symbols in neither the asset list nor a corporate action under a listed symbol, e.g.
  SIVB (survivorship gap; Nasdaq Data Link `SHARADAR/TICKERS` could close it later).
- Spin-off adjustment (spin-offs are stored, not applied to adjusted prices).
- History before 2016-01-04 (the free SIP window).
- Changing EDGAR, BLS, Fiscal, TreasuryDirect or Nasdaq Trader jobs, except the read-only join
  in A8.
- A shared query layer across repos; consumers read `data/usfin.duckdb` directly.

## Who

- **User:** Tommy Yohanes — runs it locally, reads the UI, queries DuckDB for backtests.
- **Owner/reviewer:** Tommy reviews the branch and decides on the PR.
- **Doers:** one fresh Claude subagent per slice, dispatched sequentially by the planning
  session, which verifies each slice against git; a fresh reviewer subagent critiqued this plan.

## Where

Push target: remote `origin` → `https://github.com/tomtomtomdev/sec-edgar.git`, branch
`feat/alpaca-market-data` (new, from `origin/main` 625cac8; resolved as "the only remote", branch
named `feat/<slug>` because the current branch is `main`). `git ls-remote --heads origin`
answered 2026-10-12. The branch was created tracking `origin/main`, so the **first push is
`git push -u origin feat/alpaca-market-data`**, which sets its own upstream; later pushes are
`git push origin feat/alpaca-market-data`.

Work happens in the **worktree `/Users/tommy/Projects/sec-edgar-alpaca`**, not the main checkout
`/Users/tommy/Projects/sec-edgar`, which is running the first EDGAR bulk backfill on `main` and
holds the lock on its `data/usfin.duckdb`. The worktree has its own git-ignored `.env` (copy of
the main one: `SEC_USER_AGENT`, `BLS_CONTACT_EMAIL`, the Alpaca keys) and its own `data/` and
`raw/`; live runs in slices write there. Slices never touch the main checkout.

Files (new unless marked):

```
config/hosts.yaml                (edit) buckets alpaca_data, alpaca_trading
config/schedule.yaml             (edit) alpaca_assets, alpaca_corp_actions, alpaca_bars
config/alpaca.yaml               exchanges, batch_size, lookback_days, backfill_start,
                                 new_symbol_cap, gap_sessions
.env.example, README.md          (edit)
src/usfin/settings.py            (edit) APCA_API_KEY_ID, APCA_API_SECRET_KEY
src/usfin/http.py                (edit) Alpaca auth headers by host; X-Ratelimit-Reset wait
src/usfin/doctor.py              (edit) key checks (info), CONFIG entry for alpaca.yaml
src/usfin/jobs.py                (edit) add the alpaca modules to the import list
src/usfin/cli.py                 (edit) `usfin alpaca backfill bars|corp-actions`
src/usfin/sources/alpaca/__init__.py, assets.py, corp_actions.py, bars.py, backfill.py
src/usfin/migrations/020_alpaca_asset.sql, 021_alpaca_corp_action.sql, 022_alpaca_bar.sql,
                     023_alpaca_universe.sql, 024_alpaca_security.sql, 025_alpaca_adjusted.sql
                     (views/macros; numbers follow the last migration at the time — 019 now)
src/usfin/web/prices.py, web/app.py (edit), web/edgar.py (edit),
web/templates/prices.html, prices_universe.html, base.html (edit, nav), edgar template (edit)
tests/test_alpaca_*.py, tests/fixtures/alpaca/...,
tests/test_web_symbols.py        (edit) PAGES dict gets the new routes
```

Runs on this Mac (macOS, Apple Silicon) only.

## When

1. Settings, auth headers, host buckets, doctor (everything depends on it).
2. Assets snapshot + diff + history.
3. Corporate actions (job, then backfill).
4. Universe view (needs 2 + 3).
5. Bars: job and versioning, then the backfill command, then new-symbol catch-up.
6. Security identity, then adjusted prices.
7. Schedule + alerts per job, added in the slice that adds the job (never one big edit).
8. UI pages, EDGAR link (needs `edgar_tickers` run once in the worktree), README.

No deadline. The live full bars backfill runs after its slice, in the worktree; the main
checkout's DB gets Alpaca data only after the branch is merged.

## How

### Tech stack

| Layer | Choice | Version | Source |
|-------|--------|---------|--------|
| Language / toolchain | Python, uv (installed 0.13.0), uv_build | 3.13 (`requires-python >=3.12`) / uv_build >=0.12.2,<0.13 | `.python-version`, `pyproject.toml` |
| CLI | typer | 0.27.3 | `pyproject.toml` (pinned) |
| HTTP | httpx sync client via `usfin.http` + `usfin.ratelimit` (5 tries, 1-2-4-8 s backoff, `Retry-After`) | 0.28.1 | `pyproject.toml`, existing code |
| Storage | DuckDB single file, SQL migrations `src/usfin/migrations/NNN_*.sql` (last: 019) | 1.5.6 | `pyproject.toml`, existing code |
| Config / env | pyyaml, python-dotenv, `config/*.yaml` | 6.0.3 / 1.2.4 | `pyproject.toml` |
| Calendars | holidays (`nyse`) via `usfin.calendars` | 0.106 | `pyproject.toml` |
| Scheduler | APScheduler via `usfin.scheduler`, `config/schedule.yaml` | 3.11.3 | `pyproject.toml` |
| Web UI | FastAPI + uvicorn + Jinja2 templates in `web/templates`, routes in `create_app` (`web/app.py`), query helpers per module, hand-built SVG (`web/charts.py`) | 0.142.2 / 0.54.0 / 3.1.6 | `pyproject.toml`, existing code |
| Jobs | `usfin.jobs.Job` registered in `jobs.REGISTRY`, watermarks, chaining, reparse via parsers | — | existing code |
| Tests | pytest, respx, FastAPI TestClient; `live` marker | 9.1.1 / 0.23.1 | `pyproject.toml` |
| Lint | ruff, line length 100 | 0.16.10 | `pyproject.toml` |

**New dependencies:** none. No Alpaca SDK (`alpaca-py`): the three endpoints are plain JSON over
httpx, and an SDK would bring a second HTTP stack and pydantic.

### Approach

**Auth and secrets.** Alpaca authenticates by headers (`APCA-API-KEY-ID`,
`APCA-API-SECRET-KEY`). `http.Client` injects them for requests to the two Alpaca hosts from
`Settings`, the way it already adds the SEC user agent and the BLS contact email; unset keys →
`ConfigError` before any request (this also covers `usfin http get`). `hosts.yaml` is committed
and never holds them. `raw.save` stores the redacted URL, body, status and content type, and
`request_sha` covers method, URL and body only, so headers are never archived; a test still
asserts the key and secret strings appear nowhere in the raw store, `raw_response`, captured logs
or error messages (A1, A12).

**Rate limit.** `hosts.yaml` buckets `alpaca_data` (data.alpaca.markets) and `alpaca_trading`
(paper-api.alpaca.markets), each 3 requests/s. The client's existing 429 retry additionally
honours `X-Ratelimit-Reset` (epoch seconds) when Alpaca sends it instead of `Retry-After` (A10).

**Assets (`alpaca_assets`).** One GET `/v2/assets?asset_class=us_equity` (~23 s, 33.6 k rows).
`alpaca_asset_snapshot` (snapshot_at, id, symbol, name, exchange, status, tradable, class,
raw_id) — only the tracked columns; churning fields (`easy_to_borrow`, `shortable`, margin
fields, `attributes`) stay in the raw JSON only. ~12 M rows a year, narrow. Diffed into
`alpaca_asset_history` (id, symbol, name, exchange, status, tradable, class, valid_from,
valid_to) with an `alpaca_asset_diff` gate table, the `symbol_history` pattern; a swing beyond
2 % stores but does not diff (A4). Watermark = newest `snapshot_at`.

**Corporate actions (`alpaca_corp_actions`).** GET `/v1/corporate-actions?start&end&limit=1000`
(all types, follow `next_page_token`). `start`/`end` filter `process_date`. Daily window:
watermark − 30 d to today + 90 d; the watermark is the newest `process_date` ≤ today (capped at
today by `watermark_value`; future-dated actions are stored anyway). Backfill: month windows
from 2016-01-01 via `usfin alpaca backfill corp-actions`. Table `alpaca_corp_action`: `id`,
`type`, `symbol`, `old_symbol`, `new_symbol`, `acquiree_symbol`, `acquirer_symbol`, `cusip`,
`old_cusip`, `new_cusip`, `acquiree_cusip`, `acquirer_cusip`, `ex_date`, `record_date`,
`payable_date`, `process_date`, `effective_date`, `rate`, `old_rate`, `new_rate`, `cash_rate`,
`acquiree_rate`, `acquirer_rate`, `special`, `foreign`, `payload` (JSON), `first_seen_at`,
`raw_id`; no PK; insert where no row with the same `id` and identical `payload` exists (A5).
`alpaca_corp_action_latest` view = newest version per `id`.

**Universe (A3).** View `alpaca_universe(symbol, source)`: rules as in A3, `source` naming the
rule (asset_active, asset_inactive, merger, removal, renamed). Every universe symbol requests
bars 2016-01-04..today; Alpaca returns what exists.

**Bars (`alpaca_bars`).** GET `/v2/stocks/bars?timeframe=1Day&feed=sip&adjustment=raw&asof=-
&limit=10000`, symbols in batches (`batch_size`, start 100; measure and record). `date` = `t`
converted to America/New_York. Table `alpaca_bar` (symbol, date, open, high, low, close, volume,
trade_count, vwap, first_seen_at, raw_id) **with no PK** (migration 014 pattern: an ART index on
~30 M rows would be ~2 GB): rows load in symbol order, and insert is one set-based statement that
unnests the parsed batch as JSON (the `nasdaqtrader` style, not `executemany`) and anti-joins
against the newest version per (symbol, date) on the compared columns. A dedupe test guards it.
Daily job at 20:30 ET (after extended hours): (a) every active universe symbol over
watermark − `lookback_days` (5) .. today; (b) up to `new_symbol_cap` (200) universe symbols with
no bars yet, full history — so new delistings, inactive assets and uplistings get backfilled
over the following days. The bars slice measures how many bars change between an 18:00 and a
20:30 fetch and between days, and records it (A2). `alpaca_bar_as_of(ts)` is a macro on the
`bls_as_of_utc` pattern; `alpaca_bar_latest` view = newest version per (symbol, date).

**Backfill.** `usfin alpaca backfill bars [--from 2016-01-04] [--symbols …]`: walks the universe
in batches; each batch is archived and parsed like the job, in **its own DB session** (open,
write, close), and its progress is a watermark key under the job name `alpaca_bars_backfill`,
which has no `schedule.yaml` entry, so no alert rules run on it (A9, A10). Expected: ~16.1 k
listed asset rows + action symbols; 25-35 M bars; ~3 000 pages at 1.8-7.2 s each → 2-6 h,
latency-bound (~15 requests/min, far under the cap); raw store +~3.3 GB before gzip; DB growth
measured and recorded in the slice.

**Identity (A7).** View/macro `alpaca_security(security_key, symbol, valid_from, valid_to)`:

1. Split each symbol's bar dates into segments at: name-change `process_date` (the old symbol's
   segment ends the day before, the new symbol's starts on it), removal/merger effective dates,
   and gaps of more than `gap_sessions` (60) NYSE sessions with no bar.
2. Chain segments across a name change (old segment → new segment) under one key.
3. Key = CUSIP from the actions touching the segment when there is one; else the active asset
   currently holding the symbol, for the segment that reaches today; else the inactive asset
   ids holding the symbol, merged when their names are equal.
4. A segment between a rename and a later reuse with a flat price (META 2022-01-31..06-08) is
   kept as its own key, not chained.

Lookup by today's symbol = the security whose segment holds that symbol today (or, for a
delisted symbol, its last segment), with all chained segments; A7 lists the expected answers,
which are the fixture tests (FB/META/METV, TWTR, IBTA).

**Adjusted prices (A6).** View `alpaca_bar_adjusted(security_key, date, symbol, close,
adj_close, split_factor, tr_factor)` over `alpaca_bar_latest` joined to `alpaca_security`, so
factors chain across renames. Backward-cumulative split factor: product of `old_rate/new_rate`
for splits (forward and reverse) with `ex_date` after the bar's date. Dividend factor at each
cash-dividend `ex_date`: `1 − rate / close` of the last bar before `ex_date`, cumulated
backward; special and foreign dividends included at their stated rate; spin-offs, rights and
mergers not applied (Out of scope). The live test fetches `adjustment=all` for AAPL, MSFT and
KO directly (not through a job; not archived) and compares (A6).

**EDGAR link (A8).** The current symbol of active securities goes through `ticker_norm` to
`edgar_company`; the EDGAR company page reads the latest close and a 1-year `adj_close` chart.
`edgar_tickers` must be run once in the worktree before the slice's live run (needs
`SEC_USER_AGENT`, present in the worktree `.env`).

**UI (A11).** `web/prices.py` query helpers; routes in `create_app`; templates `prices.html`,
`prices_universe.html`; nav links in `base.html`; CSV via `?format=csv&table=<name>`; the new
routes go into `PAGES` in `tests/test_web_symbols.py` so the app-wide CSV test covers them.

**Risks and how each is retired.**

- *Bar volume and DuckDB size.* No PK from the start (014 pattern); the bars slice measures rows,
  DB growth and insert time per batch and records them.
- *Free-plan limits change.* A live test asserts the 2016 SIP start, `asof=-` behaviour on
  FB/META, and the rate-limit headers; it fails loudly if Alpaca changes them.
- *Symbol reuse and renames.* Retired by A7's fixture tests from real FB/META/METV, TWTR and IBTA
  responses.
- *Bar revisions after the close.* Measured in the bars slice; the job runs at 20:30 ET.
- *Corporate actions processed after their ex-date.* Retired by the + 90 d forward window.
- *Delisted gap.* Documented (SIVB); the universe slice reports counts per rule.
- *The main checkout's DB is locked by the EDGAR backfill.* Retired by working in the worktree
  with its own DB; the backfill opens a session per batch so it never does the same to `serve`.

### Commands

- Install: `uv sync`
- **Test:** `uv run pytest -q` (760 passed on 625cac8; live tests excluded by the default
  `-m 'not live'`; Alpaca live tests: `uv run pytest -m live tests/test_alpaca_*.py`)
- **Build (gate):** `uv run ruff check . && uv run pytest -q && uv build -q`
- **Run:** `uv run usfin <command>` from the worktree — e.g. `uv run usfin run alpaca_assets`,
  `uv run usfin alpaca backfill bars --symbols AAPL,TWTR`, `uv run usfin serve`
  (scheduler + UI on http://127.0.0.1:8660).

## Review

Independent reviewer, 2026-10-12 (read the touched code, ran the gate: 760 passed, ruff and
build ok; probed the live API).

1. Bars `asof` unspecified, changes the data (META/FB/METV) — fixed: `asof=-` pinned (How › Bars,
   A2), behaviour in Why, live test in Risks.
2. A3 claimed SIVB is in the universe; it is not reachable — fixed: TWTR via merger
   `acquiree_symbol` is the test, SIVB documented as the gap (A3, Out of scope).
3. Identity under-specified; asset id ≠ security (240 multi-id symbols) — fixed: algorithm in
   How › Identity, expected lookups in A7.
4. Corporate-action window on `process_date`, missing merger columns, amendments lost — fixed:
   − 30 d / + 90 d window, merger columns, payload versioning (A5, How).
5. Backfill progress keys would raise alerts — fixed: separate job name
   `alpaca_bars_backfill` with no schedule entry.
6. Decide the PK now — fixed: no PK, 014 pattern, set-based anti-join insert, dedupe test.
7. Backfill would hold the DB lock for hours — fixed: session per batch (A10).
8. Volume/timing numbers stale — fixed: 16.1 k symbols, 3 000 pages, 2-6 h, 3.3 GB raw.
9. Assets: one call (~23 s), no job timeout exists, explicit tracked columns — fixed.
10. Where auth headers go; `X-Ratelimit-Reset`; buckets — fixed: injected in `http.Client` from
    Settings, Reset honoured, two buckets.
11. Daily job never backfills new universe symbols — fixed: `new_symbol_cap` catch-up.
12. Bar revision semantics and timing — fixed: compared columns named, exact equality,
    20:30 ET, measured in the slice.
13. Alert rules per job missing; `value_jump` misfires on splits — fixed: explicit rules in A9,
    no `value_jump` on bars.
14. Reparse doesn't run compute; derived tables wouldn't rebuild — fixed: views/macros,
    `first_seen_at` = `fetched_at`, `bls_as_of_utc` pattern.
15. Adjusted prices: per security, spin-offs/special/foreign, symbols checked — fixed (How › 
    Adjusted, A6); `adjustment=all` only in the live test, not archived.
16. A8 hidden `edgar_tickers` dependency, inactive pairs don't join, no unmatched route —
    fixed: A8 limited to active, matched filter, dependency noted.
17. Missing files in Where (templates, nav, PAGES, doctor CONFIG, diff table, security
    migration) — fixed.
18. CSV convention is `?format=csv&table=` — fixed.
19. Doctor would fail on unset optional keys — fixed: `info`, jobs refuse instead.
20. Stale measurements in Why — fixed.
21. `/v2/calendar` measured but unused — fixed: dropped, listed in Out of scope.
22. Inactive assets have no removal date — fixed: every symbol requests 2016..today.
23. Bar dates are 04:00Z/05:00Z — fixed: New York date.
24. `-m live` runs other sources' live tests — fixed: path-restricted command.
25. "One limiter per host" wording — fixed: per bucket, two buckets.
26. Corp-action watermark capped at today vs future actions — fixed: documented in How.
