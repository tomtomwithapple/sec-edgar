# Alpaca market data (usfin)

Status: S9 done — next: S10
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
src/usfin/migrations/022_alpaca_asset.sql, 023_alpaca_corp_action.sql,
                     024_alpaca_universe.sql, 025_alpaca_bar.sql, 026_alpaca_bar_symbol.sql,
                     027_alpaca_security.sql, 028_alpaca_adjusted.sql (views/macros; numbers
                     follow the last migration at the time — amended in S5: the universe, built
                     first, took the next number; amended after S5: rebased onto main, whose
                     020_fred / 021_tic came first, so every alpaca migration moved up by 2;
                     amended in S7: 026 remembers invalid and fully asked symbols, security and
                     adjusted moved up by 1)
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
2 % stores but does not diff (A4). `snapshot_at` = the raw response's `fetched_at` (the list has
no date). *Amended in S2:* a list identical in every tracked column to the newest stored
snapshot is not stored again (so a rerun inserts 0; the table grows only on days the list
changes, at most the ~12 M rows a year above), and the watermark = the newest `snapshot_at` that
confirmed the list, stored or not, so a quiet day raises no `stale_watermark`. The snapshot table
has no PK (014 pattern; a snapshot is written whole by one statement); history PK (id,
valid_from). The list changes within minutes in the early morning ET (S2 Run: 06:38 vs 06:39 ET
differed by 69 ids gone, 2 new, 48 changed), so the 07:30 ET run sees the day's refresh.

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
`alpaca_corp_action_latest` view = newest version per `id`. *Amended in S3:* the insert
anti-joins against the **newest** version per `id` (a payload that changes and later changes back
is a third version, so `_latest` stays right); `type` is the response's group key in the
singular (`cash_dividends` → `cash_dividend`; the action objects carry no type field); `payload`
is canonical JSON (sorted keys, compact). The response has 15 groups as of 2026-10-12 (the A5
list plus `capital_gains_distribution`, `partial_call`, `reorganization`, `stock_dividend`,
`unit_split`); fields outside the promoted columns (`sub_type`, due bills, `source_*` of
spin-offs and rights, `stock_movements`, capital-gains rates) are in `payload` only. Actions
are processed on weekdays only, so the job's calendar and due days are `nyse`. Pages are ordered
by symbol across the whole window and are not exactly 1 000 long (999 seen).
*Amended in S4:* the backfill (`usfin alpaca backfill corp-actions [--from 2016-01-01]`,
`sources/alpaca/backfill.py`) archives each month as the job's dataset `alpaca_corp_actions` and
stores it with the job's `store_page`, so versions dedupe against the job's and `usfin reparse
alpaca_corp_actions` rebuilds both. Its progress is the finished stretch of months, watermarks
`from` and `through` under the job name `alpaca_corp_actions_backfill` (no schedule entry, no
alert rules; the daily job's watermark is untouched). A rerun skips months inside the stretch and
refetches the open month; a stretch from a later `--from` is reused when an earlier `--from` walks
into it. History holds a 16th group, `redemption` (16 actions, 2023), not in the 2026-10-12 forward window.

**Universe (A3).** View `alpaca_universe(symbol, source)`: rules as in A3, `source` naming the
rule (asset_active, asset_inactive, merger, removal, renamed). Every universe symbol requests
bars 2016-01-04..today; Alpaca returns what exists.
*Amended in S5* (migration `024_alpaca_universe.sql`, 022 before the rebase): one row per distinct (symbol, source), so
a symbol reached by several rules has several rows (FB: asset_active + renamed; IBTA:
asset_active + asset_inactive); read distinct symbols for requests. Assets are read from each
asset id's newest `alpaca_asset_history` row: `asset_active` = active and still open,
`asset_inactive` = inactive or gone from the list since (so a delisting never drops a symbol).
Actions are read from `alpaca_corp_action_latest`; an action symbol whose only assets are OTC
is left out (512 live), one in no asset at all (TWTR) is in. The listed exchanges are written
into the view (a view cannot read `config/alpaca.yaml`); a test keeps them equal to `exchanges`.
SIVB is the acquirer (not the acquiree) of BPFH's 2023 stock-and-cash merger, so it stays out.
Live 2026-10-13: 24 737 rows, **23 061 distinct symbols** (15 870 from assets, 7 191 only from
action rules). The rule-only symbols come almost entirely from actions of 2019 on (first action
year: 4 before 2019, 77 in 2019, ~700-1 250 a year from 2020), so delistings of 2016-2019 are
mostly missing — a wider survivorship gap than SIVB alone. Not every symbol is a plain ticker:
441 rows are CUSIP-like (`09175M705`, `005CVR014`, `478ESC015`: escrows, CVRs, rights; 289 of
them inactive *listed* assets, so the asset list itself carries them) and dotted suffixes
(`.WS` warrants, `.U` units, `.RT` rights, `.WI` when-issued, `.PRA` preferreds) are common;
the view keeps them all, the bars slices see what Alpaca returns for them.

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
*Amended in S6* (migration `025_alpaca_bar.sql`, `sources/alpaca/bars.py`): (1) **no `end` for
"up to now"**: the free plan refuses a date `end` of today at any hour (403 "subscription does
not permit querying recent SIP data"; a date means through that day) and any timestamp less
than 15 minutes old; with `end` omitted Alpaca answers up to its delay, so the job sends none
and the backfill sends none for its last stretch. (2) **Invalid symbols fail the whole
request**: a CUSIP-like (`005CVR014`, `09175M705`) or `-`/`/` symbol (`ABC-U`, `BF/B`) answers
400 `{"message":"invalid symbol: X"}` for every symbol in the batch, while a well-formed
symbol with no bars (TWTR today, `X.RT`, `SPAC.WS`) is just absent. `fetch_batch` archives the
400, drops X, asks again and returns the dropped symbols; S7/S8 should remember them so the
441 CUSIP-like universe rows do not cost a request each day. (3) The page's insert reads the
newest versions for its own symbols and date range only (not a window over the whole table).
(4) `volume`/`trade_count` are BIGINT and a non-integer value fails the page loudly. (5) The
S6 job refreshes only the symbols already in `alpaca_bar`, from watermark − `lookback_days`
(from `backfill_start` with no watermark); S7 replaces `bars.symbols(ctx)` with the universe.
*Amended in S7* (migration `026_alpaca_bar_symbol.sql`, `bars.select`): (1) **Symbol marks.**
Table `alpaca_bar_symbol(symbol, status, first_seen_at, raw_id)`, one row per (symbol, status),
the first raw response that showed it, no PK: `invalid` when a 400 names the symbol (read from
the archived body), `history` for every symbol of a request from `backfill_start` or earlier with
no `end` whose last page (no `next_page_token`) arrived — whole history asked, bars or none. The
bars parser writes both, so it is a `versioned` table of the job and `usfin reparse alpaca_bars`
rebuilds it (the request's symbols and `start` are read from `raw_response.url`). (2) **What a
run asks.** The spec's literal (a)/(b) does not work: (a) gives every active symbol a few days of
bars, so "no bars" would never let (b) backfill its history; and a well-formed symbol Alpaca has
nothing for (`X.RT`, `SPAC.WS`, TWTR today) is absent from every answer, so under "no bars" it
would hold a catch-up place every day, and once more than `new_symbol_cap` of those exist a new
listing would never be reached. The rule kept: (a) every `asset_active` universe symbol not
marked `invalid`, watermark − `lookback_days` .. now (today − `lookback_days` with no watermark,
so a first run on an empty DB is never a whole backfill in one session); (b) up to
`new_symbol_cap` universe symbols with no mark (neither `invalid` nor `history`), active first,
then by symbol, from `backfill_start`; (b)'s symbols are left out of the same run's (a). So the
daily job covers every listed symbol from its first run, (b) walks the rest 200 a day in a stable
order (new listings ahead of the delisted tail) and asks each one in full once, and an invalid
symbol costs one 400 ever. (b) runs before (a), both with no `end`. A missed day is inside
watermark − 5 d (the scheduler's catch-up runs the job once; the window covers the gap). (3) S8's
backfill can use "no `history` mark" as its to-do list and its resume rule; it marks `history`
only if each symbol's last stretch is one request from `backfill_start` with no `end` (else it
must write the mark itself, or the daily (b) will ask those symbols in full once more —
harmless, 200 a day). (4) Schedule (A9): `alpaca_bars` 20:30 ET, `nyse` calendar and due days,
`zero_new_rows {after: "20:30"}`, `stale_watermark {n: 1}`, no `value_jump` (raw prices jump at
splits).

**Backfill.** `usfin alpaca backfill bars [--from 2016-01-04] [--symbols …]`: walks the universe
in batches; each batch is archived and parsed like the job, in **its own DB session** (open,
write, close), and its progress is a watermark key under the job name `alpaca_bars_backfill`,
which has no `schedule.yaml` entry, so no alert rules run on it (A9, A10). (Corporate
actions do the same per month under `alpaca_corp_actions_backfill`; see How › Corporate
actions.) Expected: ~16.1 k
listed asset rows + action symbols (*amended in S5:* the universe has 23 061 distinct symbols,
15 870 from assets and 7 191 only from actions, many of those CUSIP-like or rights with no bars); 25-35 M bars; ~3 000 pages at 1.8-7.2 s each → 2-6 h,
latency-bound (~15 requests/min, far under the cap); raw store +~3.3 GB before gzip; DB growth
measured and recorded in the slice.
*Amended in S8* (`backfill.bars_history`, `usfin alpaca backfill bars [--from] [--symbols]`):
(1) **One request per batch, no stretches.** Each batch is one `fetch_batch` from `--from`
(default `backfill_start`) with no `end`, archived as the job's dataset `alpaca_bars` and stored
by the job's `store_page` in one transaction. So the S7 marks come for free: the last page marks
the batch's symbols `history`, a 400 marks its symbol `invalid`, versions dedupe against the
daily job's bars, and `usfin reparse alpaca_bars` rebuilds all of it. (2) **To-do list = resume
rule = the marks.** The backfill asks the universe symbols with neither mark (`bars.unasked`,
shared with the daily (b): active first, then by symbol), or the `--symbols` given (upper-cased,
once each) less the marked ones. A rerun therefore skips whatever any earlier run finished
(backfill, `--symbols` run or the daily catch-up), and a stop loses at most the open batch, whose
transaction rolls back. A `--from` after `backfill_start` asks less than the whole history, marks
nothing and so is a refetch, not resumable. (3) **Progress record**: watermarks `from` and
`symbol` (last symbol of the last finished batch) under `alpaca_bars_backfill`, written in each
batch's transaction; no `schedule.yaml` entry, so no alert, no `job_run` row, and the daily
job's `date` watermark untouched. (4) **A DB session per batch** holds the lock for the batch's
fetch and store, ~30-50 s live (100 symbols' full history), well under `USFIN_LOCK_WAIT`'s
600 s. (5) **Reparse after a mid-batch stop**: `raw.save` commits each page as it arrives, so the
stopped batch's pages stay archived though never stored; the live run stores the rerun's copies,
while reparse replays every archived page in `fetched_at` order and gives those bars the earlier
page's `first_seen_at` / `raw_id`. Values and marks are identical; only the attribution moves
(seconds to minutes earlier), and from then on reparse is a fixed point. The same holds for any
failed daily run or corp-action window; left as is (raw first; the spec wants the failed batch
rolled back). (6) **Invalid symbols are more than CUSIPs**: live also `H037108`, `R004277`
(letter + 6 digits), `M53RGT011`, `FCE.A_DELISTED`, `AET_DELISTED`, `NOEA:XETR`; `fetch_batch`
drops whatever the 400 names, so no pattern list is needed. **Projection of the full run**
(S13; from the S8 sample's density per kind, 22 160 symbols still unmarked on 2026-10-19:
12 669 active, 2 172 inactive, 6 967 action-only, 352 invalid-looking): ~**21.7 M bars**
(active ~1 240 bars a symbol, inactive ~820, action-only ~600; ~22.6 M in the table after),
222 batches, ~2 300 bar pages at ~9 400 bars and ~4.1 s each plus ~500 invalid 400s at ~0.5 s,
so **~2.8 h awake time** (latency-bound, ~15 requests/min); DB **+~0.5 GB** (~22 bytes a bar),
raw store **+~0.5 GB gzip** (~0.21 MB a page; ~2.2 GB of bodies). Run it under `caffeinate -s`.

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
*Amended in S9* (migration `027_alpaca_security.sql`; views `nyse_session`, `alpaca_security`,
`alpaca_security_symbol`, macros `alpaca_gap_sessions()`, `alpaca_security_of(symbols)`,
`alpaca_security_lookup(sym)`, all new objects for the parent's adapter): (1) **A gap is sessions
without a *traded* bar.** Alpaca does not leave a dead symbol empty under `asof=-`: it pads it with
flat bars of volume 0 at the last price, not only META 2022-01-31..06-08 (12.31) but IBTA from the
iShares ETF's last trade (2021-11-30) to Ibotta's IPO (2024-04-18): 25.35 for 597 sessions, no
missing bar at all, so step 1's "gap with no bar" never splits it and IBTA would be one segment
2020-2026 keyed by the iShares CUSIP. A bar with volume 0 now counts as no trade; an untraded run of
more than `gap_sessions` sessions (to the symbol's first or last bar at either end) is a segment of
its own, flagged `quiet`, while shorter ones (a thin ETF's quiet days; IBTA had 87 in 2020) stay
in their segment. Live this caught real reuses beyond the five (ACI: Arch Coal, then Albertsons from
2020-06-26; ADT: old ADT, then the 2018 IPO). (2) **Step 4 needs no rule of its own**: cutting at
both renames (META → METV on 2022-01-31 and FB → META on 06-09) leaves the flat META stretch
between them unchained, and it is `quiet` besides; a quiet segment is never chained and takes no
CUSIP or asset, so its key is `symbol:META:2022-01-31`. (3) **Chaining** links the old symbol's
segment ending before the rename's `process_date` to the new symbol's segment starting on or after
it, each within `gap_sessions` sessions of it and neither quiet; any number of renames chain.
(4) **Key** (step 3 made concrete): `cusip:<CUSIP>` of the newest action naming one of the chain's
segments (dividends, splits: the segment holding the symbol on the ex-date; a rename's old CUSIP
for the old segment and new CUSIP for the new one; a merger's acquiree/acquirer CUSIP; a worthless
removal), else `asset:<id>` when the chain ends in its symbol's current segment and assets hold the
symbol (ids with the same name are one group, the smallest id of the group with an active id,
else of the only group: IBTA's two "Ibotta, Inc." ids → one key), else
`symbol:<symbol>:<first date>`. Chains sharing a CUSIP are one security without a rename between
them, which rejoins a security cut by a stray action or a gap (AIEQ, AESR, TBLU, PQLC live; SNH →
DHC, whose rename falls in a gap only because DHC's history is not backfilled yet). (5)
**Sessions**: the views count NYSE sessions with `nyse_session(date, n)`, weekdays less the
`holidays.NYSE()` dates 2016-2050 written into the migration; a test keeps it equal to
`usfin.calendars` (with `config/calendars.yaml`) and `alpaca_gap_sessions()` equal to
`gap_sessions` (S5's pattern for `exchanges`). Alpaca has a bar on exactly those 2 713 sessions to
2026-10-16. (6) **Shape**: `alpaca_security(security_key, symbol, valid_from, valid_to, quiet)`,
one row per segment, `valid_from`/`valid_to` its first and last bar dates (inclusive), every bar in
exactly one segment; a security's bars are `alpaca_bar_latest` joined on symbol and `date BETWEEN
valid_from AND valid_to`. `alpaca_security_symbol(symbol, security_key)` = each symbol's current
security (its newest non-quiet segment). `alpaca_security_lookup(sym)` returns that security's
segments; it reads only the bars of the symbols linked to `sym` by renames and shared CUSIPs
(`alpaca_security_of(list)`, exact for a list closed under those links), because the whole view
ranks every bar (0.5 s on 956 k bars, so ~11 s on the ~22 M after S13) while a lookup stays ~0.2-0.3 s.
(7) **Left as is**: a symbol reused right after its old security left, with no action, no gap of
missing or untraded bars, cannot be told from a price jump and stays one segment; and one CUSIP
named under two symbols at once would give its security overlapping segments (none live).

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

## Slices

### S1 — Alpaca settings, auth headers, host buckets, doctor
Depends on: none
Red: `tests/test_alpaca_client.py` — requests to both Alpaca hosts carry `APCA-API-KEY-ID` /
`APCA-API-SECRET-KEY` from Settings and other hosts don't; unset keys raise `ConfigError` before
any request (job path and `usfin http get`); a 429 with `X-Ratelimit-Reset` waits until it; the
key and secret strings appear nowhere in the raw store, `raw_response`, logs or errors after an
archived request; doctor shows the keys as `info` set/unset and checks `config/alpaca.yaml`.
Green: How › Auth and secrets, How › Rate limit; `settings.py`, `http.py`, `hosts.yaml`,
`doctor.py`, `config/alpaca.yaml` (all keys of How with their defaults), `.env.example`.
Run: `uv run usfin doctor` (keys `info` set) and `uv run usfin http get
"https://paper-api.alpaca.markets/v2/clock"` answers 200 with the keys and is archived.
Done when: A12 (keys, ConfigError, doctor) and the secret part of A1 hold; A10's limiter/429 part.

### S2 — `alpaca_assets` job: snapshot, diff, history
Depends on: S1
Red: `tests/test_alpaca_assets.py` — a trimmed real fixture loads tracked columns into
`alpaca_asset_snapshot`; a second identical snapshot inserts nothing; a changed status/name opens
and closes `alpaca_asset_history` rows keyed by `id`; a > 2 % swing stores but doesn't diff;
reparse rebuilds snapshot and history with no request.
Green: How › Assets; migration `022_alpaca_asset.sql`; `sources/alpaca/assets.py`; register in
`jobs.py`; `schedule.yaml` entry per A9.
Run: `uv run usfin run alpaca_assets` live: rows per status/exchange, time; rerun inserts 0.
Done when: A4, A1 (reparse) for assets, A9 entry for assets.

### S3 — `alpaca_corp_actions` job: versioned actions, forward window
Depends on: S1
Red: `tests/test_alpaca_corp_actions.py` — fixtures for dividend, forward/reverse split, name
change, cash merger (TWTR), worthless removal load into the promoted columns + `payload`;
pagination via `next_page_token`; identical refetch inserts nothing; a changed payload inserts a
version; the daily window is watermark − 30 d .. today + 90 d; watermark capped at today;
`alpaca_corp_action_latest`; reparse.
Green: How › Corporate actions (job part); migration `023_alpaca_corp_action.sql`;
`sources/alpaca/corp_actions.py`; `schedule.yaml` entry per A9.
Run: `uv run usfin run alpaca_corp_actions` live: actions per type in the window, pages, time.
Done when: A5 for the daily job.

### S4 — Corporate-action backfill from 2016
Depends on: S3
Red: `tests/test_alpaca_backfill_corp.py` — `usfin alpaca backfill corp-actions --from` walks
month windows, resumes from its progress key after a stop, opens a DB session per window,
and its progress raises no alert.
Green: How › Corporate actions (backfill), How › Backfill (session per batch, separate job name
`alpaca_corp_actions_backfill`); `cli.py`, `sources/alpaca/backfill.py`.
Run: live backfill 2016-01-01..today; record actions per year/type, pages, time, DB growth.
Done when: A5 from 2016, A10 resume for corp actions.

### S5 — Universe view
Depends on: S2, S4
Red: `tests/test_alpaca_universe.py` — `alpaca_universe` holds listed active/inactive asset
symbols, merger `acquiree_symbol`s (TWTR), worthless-removal symbols and renamed `old_symbol`s,
each with its `source`; OTC-only symbols excluded; SIVB absent (documented gap).
Green: How › Universe; migration `024_alpaca_universe.sql` (view; amended in S5, then +2 after the rebase onto main).
Run: count per `source` on the live worktree DB; TWTR in, SIVB out.
Done when: A3.

### S6 — `alpaca_bars` core: request, versioned insert, as-of
Depends on: S1
Red: `tests/test_alpaca_bars.py` — request pins `feed=sip&adjustment=raw&asof=-&limit=10000`,
batches symbols and follows `next_page_token`; `t` 04:00Z/05:00Z → NY date; identical refetch
inserts 0; a changed compared column inserts a version with `first_seen_at` = `fetched_at`;
`alpaca_bar_as_of(ts)` and `alpaca_bar_latest`; dedupe test after mixed runs; no PK in the
migration; reparse rebuilds.
Green: How › Bars (table, insert, as-of); migration `025_alpaca_bar.sql` (amended in S5, then +2 after the rebase onto main);
`sources/alpaca/bars.py` with a `--symbols`-style key list for tests.
Run: live fetch for AAPL, FB, META, METV, TWTR from 2016: counts match Why (FB 1 945, META 1 326
with `asof=-`); rerun inserts 0.
Done when: A2.

### S7 — Daily bars job: schedule, alerts, new-symbol catch-up, revision check
Depends on: S5, S6
Red: `tests/test_alpaca_bars_job.py` — the job takes active universe symbols over watermark − 5
.. today plus ≤ `new_symbol_cap` universe symbols with no bars (full history); schedule entry
20:30 ET `nyse` with A9's rules and no `value_jump`; catch-up after a missed day; invalid
symbols remembered and skipped, rebuilt by reparse (*amended in S7:* "no bars" became "whole
history never asked", see How › Bars).
Green: How › Bars (daily job); `schedule.yaml`; migration `026_alpaca_bar_symbol.sql`.
Run: live `uv run usfin run alpaca_bars`; then measure and record bars changed between two
fetches of the same day (and an 18:00 vs 20:30 fetch if the slice runs across them).
Done when: A9 for bars, A2 revisions measured.

### S8 — Bars backfill command
Depends on: S7
Red: `tests/test_alpaca_backfill_bars.py` — `usfin alpaca backfill bars [--from] [--symbols]`
batches the universe, one DB session per batch (another connection can write between batches),
progress under `alpaca_bars_backfill` (no alerts), resumes after a stop.
Green: How › Backfill; `cli.py`, `sources/alpaca/backfill.py`.
Run: live backfill of a 500-symbol sample: pages, time per page, rows, DB and raw growth;
project the full run and record it.
Done when: A10.

### S9 — Security identity
Depends on: S4, S6
Red: `tests/test_alpaca_security.py` — fixture-built bars + actions + assets: lookups return
exactly A7's answers for META, FB, METV, TWTR, IBTA; gap split at > `gap_sessions`; the flat
META 2022-01-31..06-08 segment is its own key.
Green: How › Identity; migration `027_alpaca_security.sql` (views/macros; 026 before S7).
Run: live lookups for the same five on the worktree DB.
Done when: A7.

### S10 — Adjusted prices
Depends on: S9
Red: `tests/test_alpaca_adjusted.py` — AAPL fixture across 2020-08-31 has no jump; reverse split;
dividend factor uses the last close before ex_date; factors chain across the FB → META rename;
live test (`-m live`) compares AAPL, MSFT, KO with `adjustment=all` within 0.5 %.
Green: How › Adjusted prices; migration `028_alpaca_adjusted.sql` (027 before S7).
Run: live check output (max relative error per symbol).
Done when: A6.

### S11 — UI: /prices and /prices/universe
Depends on: S10
Red: `tests/test_web_prices.py` — `/prices?symbol=META` renders the chained series with an
adjusted/unadjusted toggle, table and CSV; `/prices/universe` filters status, exchange, delisted,
matched; nav links; both routes in `PAGES` of `tests/test_web_symbols.py`.
Green: How › UI; `web/prices.py`, `web/app.py`, templates, `base.html`.
Run: `uv run usfin serve`, open both pages for META, TWTR; CSV downloads.
Done when: A11.

### S12 — EDGAR link
Depends on: S11
Red: `tests/test_web_edgar_prices.py` — an active security's symbol joins via `ticker_norm` to
its CIK; the EDGAR company page shows latest close and a 1-year chart; delisted securities are
not joined; `/prices/universe?matched=` works.
Green: How › EDGAR link; `web/edgar.py`, the EDGAR template.
Run: `uv run usfin run edgar_tickers` once in the worktree, then the AAPL EDGAR page.
Done when: A8.

### S13 — Full backfill, docs, close-out
Depends on: S8, S12
Red: `tests/test_readme_alpaca.py` (or the repo's doc-check pattern if one exists) — README
documents keys, jobs, backfill commands and pages; `.env.example` lists the keys.
Green: README, `.env.example`; run the full bars backfill in the worktree.
Run: full `usfin alpaca backfill bars`: total rows, symbols, time, DB and raw size; `usfin jobs`
shows the three jobs ok; the gate passes.
Done when: A10 full run recorded, A12 docs; every criterion A1-A12 checked.

## Progress

| Slice | Status | Date | Commit | Tests | Run showed |
|-------|--------|------|--------|-------|------------|
| S1 Settings, auth, buckets, doctor | done | 2026-10-12 | 1126804 | 19 new (`tests/test_alpaca_client.py`), 2 edited (`test_doctor.py`: APCA keys isolated, `alpaca.yaml` listed); suite 779 passed | `usfin doctor` exit 0: `APCA_API_KEY_ID`/`APCA_API_SECRET_KEY` `info set`, `hosts.yaml` and `alpaca.yaml` ok. `usfin http get https://paper-api.alpaca.markets/v2/clock --source alpaca` → 200, raw_id 1, 148 bytes (is_open, next_open, next_close, timestamp); key id and secret found in neither `raw_response` nor the blob. Finding: `usfin http get` let a `ConfigError` escape as a traceback (SEC host without `SEC_USER_AGENT` too); it now prints one `error:` line and exits 1. A 429 whose `X-Ratelimit-Reset` has already passed retries with no wait of its own (no 1 s backoff); `Retry-After` still wins when both are sent. |
| S2 alpaca_assets | done | 2026-10-12 | e44db40 | 15 new (`tests/test_alpaca_assets.py`, fixture `tests/fixtures/alpaca/assets.json`: 49 objects byte for byte from the live list); suite 794 passed | Live `usfin run alpaca_assets`: one call, 14.6 MB, 33 564 assets, run 38 s (fetch ~23-31 s); first snapshot opened 33 564 history rows. Per exchange active/inactive (first snapshot): NASDAQ 5 626/1 326, NYSE 2 907/843, ARCA 2 754/471, BATS 1 659/137, AMEX 291/98, OTC 1 157/16 295. Finding: a rerun 29 s later (06:39 ET) got a *different* list, 33 497 assets (−0.20 %, inside the gate): 69 ids gone (55 OTC, 14 listed, all active-untradable), 2 new (PCG.PRX, WIZEY), 48 changed (tradable flips, renames); diffed: 50 opened, 117 closed. Alpaca refreshes the list in the early morning; 07:30 ET stays. A third run (28 s) found it identical: **inserted 0**, watermark moved. `usfin reparse alpaca_assets`: 3 raw responses, 67 061 rows rebuilt identically in 2.3 s, no request. Plan amended (How › Assets): an identical list is not stored again, watermark = newest confirming fetch, no snapshot PK. First live attempt failed with `ConnectError` (the local resolver briefly returned ISP block-page IPs for paper-api.alpaca.markets; resolved by itself within a minute). |
| S3 alpaca_corp_actions | done | 2026-10-12 | 0478ca7 | 28 new (`tests/test_alpaca_corp_actions.py`, 9 fixtures in `tests/fixtures/alpaca/corp_actions/`: whole live responses byte for byte, narrowed by `symbols` — AAPL 2020-08 dividend + 4:1 split, CTNT 150:1 reverse split, FB→META name change, TWTR cash merger, SIVBQ worthless removal, ATAI stock-and-cash merger, AAPL 2016-2026 at `limit=40` as two pages, an empty window); suite 822 passed | Live `usfin run alpaca_corp_actions` (2026-10-12 08:55 ET): window 2026-09-12..2027-01-10, 9 pages (1 000, 1 000, 999, 1 000, 1 000, 1 000, 1 000, 999, 216; ~1.1-1.5 s each), run 13.4 s, **8 214 actions** inserted, process_date 2026-09-14..2027-01-08, 2 122 future-dated; watermark `2026-10-12` (newest process_date ≤ today). Per type: cash_dividend 7 751, capital_gains_distribution 144, reverse_split 118, name_change 53, forward_split 26, partial_call 24, worthless_removal 23, cash_merger 22, stock_merger 18, reorganization 11, spin_off 9, stock_dividend 5, stock_and_cash_merger 4, rights_distribution 4, unit_split 2. Rerun 7 min later: 9 pages, **inserted 0**. `usfin reparse alpaca_corp_actions`: 18 raw responses, 8 214 rows rebuilt in 0.6 s, no request. Key id and secret in neither the table, `raw_response` nor the raw files. No id appears twice within or across pages (the 999-long pages are Alpaca's). Plan amended (How › Corporate actions): versions compare with the newest per id, `type` = singular group key, 15 types, `nyse` calendar. Gate note: pytest reports 75 s but `uv run pytest -q` takes ~5 min wall clock on this Mac (outside the session timer; not from this slice's tests, which take 3.5 s). |
| S4 Corp-action backfill | done | 2026-10-13 | c00846f | 16 new (`tests/test_alpaca_backfill_corp.py`: month windows on `process_date`, `next_page_token`, resume after a stop, open month refetched, an earlier `--from` reuses a later stretch, one DB session per window with another writer between windows, progress under `alpaca_corp_actions_backfill` with no rules/alerts and the job's watermark untouched, reparse rebuilds, no key stored, CLI output/bad date/no keys, a stalled read times out and is retried); suite 838 passed | Live `usfin alpaca backfill corp-actions` 2016-01-01..2026-10-13: 130 month windows, **461 pages**, ~379.6 k actions new (table now 387 849 with S3's window; 6 092 of 2026's backfilled actions matched S3's rows and inserted nothing). Per process_date year: 2016 24 401, 2017 25 331, 2018 26 435, 2019 27 119, 2020 34 024, 2021 36 375, 2022 37 579, 2023 39 626, 2024 42 435, 2025 48 540, 2026 45 976 (incl. future-dated from S3), 2027 8. Per type: cash_dividend 370 017, reverse_split 5 626, name_change 3 692, stock_merger 3 223, cash_merger 1 999, forward_split 972, unit_split 509, worthless_removal 472, stock_dividend 311, stock_and_cash_merger 250, capital_gains_distribution 247, spin_off 241, rights_distribution 121, reorganization 93, partial_call 60, **redemption 16** (a 16th group, 2023 only, not in S3's window). Before 2019 Alpaca has almost only dividends and splits (2016: 3 types; mergers/renames from 2017, numerous from 2020), so A3's merger/rename universe rules reach back mainly from 2020. FB->META rename and TWTR cash merger present. DB 25.96 MB -> 107.49 MB (+81.5 MB); raw store 3.7 MB -> 19.6 MB (gzip; 88.97 MB of bodies over 482 corp-action responses). Time: 75 pages in 113 s (~1.5 s/page) while the Mac was awake; the whole run took ~8 h wall clock over four runs because the Mac (on battery) idle-slept between brief DarkWakes. **Resume, three real stops**: (1) interrupted by me during Dec 2021 -> rerun skipped 71 windows and started at 2021-12; (2) killed by the planning session after a read hung 45+ min across sleep at Oct 2024 -> rerun started at 2024-10; (3) a `ConnectError` (connection refused, the DNS blip) at Aug 2025 page 2 -> rerun started at 2025-08 and finished. Each stop rolled back only its open window. A final rerun fetched only Oct 2026 (3 pages, 2 025 actions, **inserted 0**, 4.3 s). 0 alerts; no duplicate (id, payload); the secret is in no raw or DB file. **Finding (hang):** the shared client's `httpx.Timeout(60, connect=15)` does apply to Alpaca (no override; a `ReadTimeout` is retried; test added), but it is counted on the monotonic clock, which on macOS stops while the system sleeps: a socket half-open across sleep only times out after 60 s of *awake* time, and between 05:12 and 05:58 the Mac woke only for seconds at a time (DarkWakes 05:27, 05:42, 05:56). Not a code bug and not fixed here; run long backfills on AC power or under `caffeinate -s`. **Finding (slow suite):** same cause: pytest's 78 s are awake time; the 5-25 min wall clock is system sleep (the final gate, run while the Mac stayed awake, took 81 s wall for 80 s reported). Plan amended (How > Corporate actions: backfill semantics, the `redemption` group; How > Backfill: corp actions use the same per-window session pattern). |
| S5 Universe view | done | 2026-10-13 | dd50996 | 13 new (`tests/test_alpaca_universe.py`, built from the S2 asset fixture and S3 action fixtures loaded through the real jobs: listed active/inactive, IBTA both, every config exchange and no OTC asset, TWTR and ATAI mergers in and the acquirer CVR out, SIVBQ removal, SIVB absent, FB renamed + asset, CTNT split adds nothing, an action symbol known only as OTC is out, a listed id gone from the list stays as inactive, newest action version, distinct pairs with a known source, empty store); suite 851 passed | Migration `022_alpaca_universe.sql` (view) applied to the worktree DB (schema 22). Live per source: asset_active 13 224, asset_inactive 2 871, merger 4 776, removal 367, renamed 3 499; 24 737 rows, **23 061 distinct symbols** (15 870 from assets, 7 191 only from action rules; 1 564 symbols reached by 2-4 rules). TWTR in (merger), SIVB out; SIVBQ in (removal; it is in no asset row, so the OTC filter cannot see it); META is also `renamed` (Roundhill's META -> METV, 2022). 512 action symbols dropped as OTC-only. **Findings:** (1) the rule-only symbols come from 2019 on (first action year: 4 before 2019, 77 in 2019, 700-1 250 a year from 2020), so 2016-2019 delistings are mostly absent: the survivorship gap is wider than SIVB. (2) Not all symbols are tickers: 441 rows CUSIP-like (`09175M705` is an inactive *listed* asset; `005CVR014`, `478ESC015` removals; 289 of the 441 come from the asset list itself), plus `.WS` (217 renamed), `.U`, `.RT` (37 removals), `.WI`, `.PRx`; kept, bars slices will see what Alpaca returns. (3) SIVB appears only as the *acquirer* of BPFH (2023). (4) The universe is ~23.1 k symbols, not ~16.1 k: the bars backfill estimate grows ~45 %. Query 0.3 s. Plan amended: migration numbers (universe 022, bars 023), How › Universe (row grain, asset/OTC rules, counts, the pre-2019 gap, odd symbols), How › Backfill (symbol count). |
| S6 Bars core | done | 2026-10-19 | 967ea66 | 39 new (`tests/test_alpaca_bars.py`, 7 fixtures in `tests/fixtures/alpaca/bars/`: whole live responses byte for byte — AAPL 2015-12-28..2016-01-06 (first bar 2016-01-04 at 05:00Z), AAPL across the 2020-08-31 split raw, AAPL,MSFT at `limit=15` as two pages with MSFT split across them, META/FB/METV around the 2022-01-31 rename with `asof=-`, the real invalid-symbol 400, an empty window); suite 938 passed | Rebased onto main first (FRED/TIC); migrations now 022-025. Live on the worktree DB, one request for AAPL,FB,META,METV,TWTR from 2016-01-04 with no `end`: **8 895 bars**, 10.0 s fetch, 0.07 s insert; AAPL 2 713, FB 1 950 (2016-01-04..), META 1 331 (from 2021-06-30), METV 1 183 (from 2022-01-31), TWTR 1 718 (to 2022-10-27) — the Why counts of 2026-10-12 (2 708 / 1 945 / 1 326) plus the five sessions since. Rerun: **inserted 0**. `usfin run alpaca_bars` twice: rows 0 both (11.4 s from 2016 with no watermark, then 2.4 s from watermark 2026-10-16 − 5 d). `usfin reparse alpaca_bars`: 6 raw responses (two of them the 403s below), 8 895 rows rebuilt in 0.5 s, md5 unchanged, no request. Key id and secret in neither the raw store nor the DB file. **Findings:** (1) the first live attempt sent `end=2026-10-19` (today) and got 403 "subscription does not permit querying recent SIP data"; probed: a date end of today is refused, a timestamp 16 min old is accepted and one 14 min old refused, no `end` is accepted — the job now sends none. (2) A CUSIP-like or `-`/`/` symbol fails the whole batch with a 400 naming it (well-formed unknown symbols are just absent); handled by drop-and-retry, see How › Bars. DB size unchanged at 107.5 MB (8.9 k rows fit existing blocks). |
| S7 Daily bars job | done | 2026-10-19 | 0ec32cc | 20 new (`tests/test_alpaca_bars_job.py`: universe written into asset history / corp actions, a fake answering from the S6 AAPL/MSFT bars cut to each request's dates — (a)/(b) split with (b) active first then by symbol and left out of (a), `new_symbol_cap` walking a stable order over runs, a symbol with no bars asked in full once, a caught-up symbol refreshed daily, no watermark = today − 5 not 2016, empty universe asks nothing, a missed 10-15/10-16 caught up by Monday's window, invalid symbols (`005CVR014`, `BF/B`, active `ABC-U`) remembered and never asked again, reparse rebuilds the marks and the bars with no request, no (symbol, status) twice, `history` marked on the last page only and not for a later start or an `end`, schedule entry, `zero_new_rows` + `stale_watermark` raised by a 20:30 run with no new bar, nothing at 18:00 or with the day's bar, `versioned` both tables); 3 S6 job tests adapted to the universe (one superseded test removed, `versioned`, an empty universe); suite 956 passed | Migration `026_alpaca_bar_symbol.sql` applied (schema 26); `usfin reparse alpaca_bars` first marked S6's five symbols `history` from their archived 2016 requests. Live `usfin run alpaca_bars` (Mon 2026-10-19 00:34 ET, watermark 2026-10-16): **165 s, 155 requests, 0 failed**: (a) 131 requests (the 13 024 active symbols outside (b); 13 009 got bars over 10-12..10-16, ~0.55 s a request), 64 964 bars; (b) 2 batches of 100 from 2016 (A..ADCT, the first 200 active symbols never asked), 24 pages (22 with a `page_token`, ~3.6 s a page, up to 7.2 s), 221 663 bars; **286 627 bars inserted**, 13 209 symbols; 200 symbols caught up, **0 invalid symbols found** (no active symbol is CUSIP-like or has `-`/`/`: they sit in the inactive/action tail that (b) reaches after the ~13 k active symbols, or S8); every caught-up symbol had bars; 11 active symbols had no bar since 10-12. Watermark stays 2026-10-16 (Monday's session not traded yet); 0 alerts. Second run 3.5 min later: 188 s, 158 requests (131 daily + 27 pages for ADDS..AINF), 256 325 bars inserted, all from the 200 new symbols: **0 of the 64 964 refetched bars (10-12..10-16) changed**, nor any of S6's 8 895 bars fetched 00:22 ET. A2's 18:00 vs 20:30 comparison was not possible (the slice ran 00:30-00:45 ET on a Monday, after a weekend; nothing was trading) and is left for S8 or later, on a weekday evening. `usfin reparse alpaca_bars`: 319 raw responses, 551 847 rows rebuilt in 7.7 s, md5 of `alpaca_bar` and of `alpaca_bar_symbol` (405 `history` marks) unchanged, no request. Key id and secret in neither the raw store nor the DB file. DB file unchanged at 107.5 MB (DuckDB reused free blocks: 276 of 410 blocks used after run 1, 295 after run 2); `alpaca_bar` is 47 blocks (~11.8 MiB) for 551 847 rows, **~22 bytes a bar**, so 30 M bars ≈ 650 MB; raw store 19.9 → 35.3 MB gzip (+15.4 MB over 2 runs; 62.4 MB of bodies, ~0.11 MB per symbol's full history). **Decisions:** (1) (a)/(b) as How › Bars *Amended in S7*: (a) every active symbol, not only those holding bars, so the daily job covers the listed market from now on; (b) "whole history never asked" instead of "no bars" (with "no bars" a symbol Alpaca has nothing for would take a catch-up place every day and starve new listings). Steady cost of (a): ~131 requests, ~70 s. (2) Invalid and fully asked symbols live in `alpaca_bar_symbol`, written by the parser from the archived 400 bodies and request URLs, so reparse rebuilds them. |
| S8 Bars backfill command | done | 2026-10-19 | 9cdb952 | 17 new (`tests/test_alpaca_backfill_bars.py`: the universe in the catch-up order from `backfill_start` with no `end`, archived as `alpaca_bars`; `--symbols` replaces it (once each, outside symbols too); `next_page_token` with `history` on the last page; a later `--from` marks nothing; a rerun asks and inserts nothing; symbols marked by any earlier run skipped; a 500 mid-run keeps the finished batch and resumes at the failed one; a store failure rolls back only its batch; invalid symbols marked and never asked again (universe or `--symbols`); one DB session per batch with another writer between batches; progress `from`/`symbol` under `alpaca_bars_backfill`, no rules, alerts or `job_run`, daily watermark untouched; reparse rebuilds bars and marks with no request; no key stored; CLI output, `--symbols`/`--from`, bad date, resume hint, no keys); `bars.select`'s query became `bars.unasked`, shared; suite 973 passed | Live `usfin alpaca backfill bars --symbols <500>` (00:51-00:56 ET; sample by hash per kind: META, FB, METV, TWTR, IBTA + 150 active, 120 inactive, 110 merger-only, 64 renamed-only, 30 removal-only, 20 CUSIP-like, `OPP-C`): META/FB/METV/TWTR already had `history` from S6, so **496 asked, 5 batches**; **466 marked `history`** (399 with bars, 67 with none), **30 `invalid`** (20 CUSIP-like, `OPP-C`, and 9 more found live: `M53RGT011`, `H037108`, `R004277`, `F023181`, `R002986`, `FCE.A_DELISTED`, `AET_DELISTED`, `COL_DELISTED`, `NOEA:XETR`); **43 bar pages + 30 invalid 400s** (+4 pages of the stopped batch), **405 019 bars, 404 264 inserted** (755 were the daily job's 10-12..10-16 bars, identical, so not stored again); bars per symbol: active 1 243, inactive 821, action-only 596. Time: batches 47.4, 51.1, 31.7, 32.1, 35.3 s (197.6 s); a bar page 3.72 s median, 4.11 s mean, 8.42 s max (~0.95 MB, ~9 400 bars); a 400 0.34 s median. `alpaca_bar` 551 847 → 956 111; DB file unchanged at 107.5 MB (used blocks 295 → 328 after reparse, ~21 bytes a bar); raw store 35.3 → 44.9 MB gzip (+9.6 MB; 44.5 MB of bodies). **Resume, two real stops**: (1) SIGINT right after batch 2/5 committed (batch 3 had archived nothing) → progress `symbol = PJH`, 581 `history`; rerun named 500, skipped 204, 3 batches left. (2) SIGINT 20 s into that rerun's batch 2/3 (DVIN…), after 4 of its pages were archived: none stored, progress still `IQRAX`; the next rerun skipped 304 and asked exactly DVIN… and the last batch (2 batches, 14 pages, 116 633 bars, 67.4 s). A final rerun: **batches 0, inserted 0**. `usfin reparse alpaca_bars`: 396 raw responses in 11 s, no request; `alpaca_bar_symbol` md5 unchanged and `alpaca_bar` values md5 unchanged, but the full `alpaca_bar` md5 changed: exactly the 40 000 bars of the 4 archived-not-stored pages (raw 865-868) now carry those pages' `first_seen_at` (~40 s earlier) instead of the rerun's (raw 869-872) — see How › Backfill *Amended in S8* (5); a second reparse left every md5 unchanged. 0 alerts; daily watermark still 2026-10-16; key id and secret in neither `raw/` nor the DB file. **Projection** for the 22 160 symbols still unmarked: ~21.7 M bars, ~2 300 bar pages + ~500 400s, ~2.8 h awake, DB +~0.5 GB, raw +~0.5 GB gzip (How › Backfill). **Decisions:** one request per batch so the S7 marks are the to-do list and the resume rule (no stretches, no mark written by the backfill itself); the watermarks are a progress record only. |
| S9 Security identity | done | 2026-10-19 | 10c699a | 18 new (`tests/test_alpaca_security.py`, the store built by the real assets and corp-actions jobs and the bars job's fetch/store from two whole live responses fetched 2026-10-19 by `usfin http get` (raw 883/884 of the worktree, sha256 equal): `bars/five_2016.json.gz` (FB,IBTA,META,METV,TWTR from 2016-01-04 with `asof=-`, one page, 826 462 bytes gzipped for size, 7 850 bars) and `corp_actions/five_2016-2026.json` (both renames, the TWTR merger, 32 dividends) plus S2's `assets.json` — A7's five lookups as segments and as the bars they join to, exactly; the flat META stretch its own `symbol:` key in no lookup; keys by CUSIP (META, METV, the new FB, TWTR) and by the IBTA name group; FB split by its gap, IBTA by its quiet stretch into iShares / padding / Ibotta; every bar in one segment; one current key per symbol; a hole of exactly `gap_sessions` NYSE sessions keeps one segment and one more splits it, the same for volume-0 bars (3 segments); `alpaca_gap_sessions()` = config; one CUSIP rejoins META split by a gap; the lookup equals the view's rows and reads only linked symbols; `nyse_session` = the `nyse` calendar 2016-2050; empty store); suite 991 passed | Migration 027 applied to the worktree DB (schema 27). **A7 holds live** (956 111 bars, 13 462 symbols): META → `cusip:30303M102` FB 2016-01-04..2022-06-08 (1 620 bars) + META 2022-06-09..2026-10-16 (1 093); FB → `cusip:74349Y613` FB 2025-06-26..2026-10-16 (330); METV → `cusip:53656F417` META 2021-06-30..2022-01-28 (148) + METV 2022-01-31..2026-10-16 (1 183); TWTR → `cusip:90184L102` 2016-01-04..2022-10-27 (1 718); IBTA → `asset:00b64222…` (the smaller of its two "Ibotta, Inc." ids) 2024-04-18..2026-10-16 (627). META's other segments: `cusip:53656F417` (Roundhill, in METV's chain) and the quiet `symbol:META:2022-01-31` (90 bars, close 12.31, volume 0); IBTA's: `cusip:46436E700` (iShares, 2020-02-28..2021-11-30) and the quiet `symbol:IBTA:2021-12-01` (597 bars). Time: a lookup 219-280 ms, with its bars joined 288-382 ms (it reads only the linked symbols' bars); the whole view 485 ms (a first version reading `alpaca_bar_latest` and evaluating `alpaca_corp_action_latest` per reference took 3-6 s a lookup). Over all bars: **13 629 segments, 13 606 securities** (12 206 `asset:`, 1 284 `cusip:` over 1 307 segments, 116 `symbol:`), **13 multi-symbol chains** (FB → META, META → METV, IPV → AEVA, ACIC → ACHR, ACSG → EMCR, AFIN → RTL, HUSA → AGIG, ALTS → AIFC, SHOTW → BNKKW, NWTNW → AIIOW, RSF.RTWI → RSF.RT, and by CUSIP across a gap SNH → DHC and CMII → IPXG, whose new symbols have only S7's 5 days), 9 more securities of one symbol in several segments rejoined by CUSIP (AIEQ, AESR, AFIF, TBLU, INBX, ASPA, VHNA, PQLC, HIX.RT), 103 symbols in more than one segment (99 with more than one traded segment), **54 quiet segments** over 40 symbols (27 111 days; ACI 2016-01-12..2020-06-25 between Arch Coal and Albertsons, ADT 2016-05-02..2018-01-18 between the two ADTs, NEWP, AIB, BITE, TBLU, …). Most of the 13 462 symbols hold only S7's 5 days; over the 804 with their whole history asked: 950 securities. No overlapping segments within a security; every bar in exactly one segment; for 370 symbols (every multi-segment security's plus every 40th) the lookup equals the view's rows. **Findings:** (1) Alpaca pads a dead symbol with volume-0 flat bars, so the plan's "gap with no bar" never split IBTA: the gap is now counted in sessions without a *traded* bar, and such a run is its own `quiet` segment (How › Identity *Amended in S9*). (2) Step 4 follows from cutting at both renames. (3) The whole view must rank every bar (DuckDB does not push a symbol filter through `alpaca_bar_latest`'s window): 0.5 s now, ~11 s projected at ~22 M bars, so the lookup works on the linked symbols only. Two live requests made for the fixtures (none for the job tables, which are unchanged). |
| S10 Adjusted prices | todo | | | | |
| S11 UI pages | todo | | | | |
| S12 EDGAR link | todo | | | | |
| S13 Full backfill, docs | todo | | | | |
