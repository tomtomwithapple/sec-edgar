# Alpaca market data (usfin)

Status: planning — next: review
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

Measured live on 2026-10-12 with the user's paper key (free plan):

- Daily bars, `feed=sip`: AAPL from **2016-01-04** to the last close (2 708 bars, one request,
  8.5 s); `feed=iex` only from 2020-07-27. Multi-symbol requests work (5 symbols × 28 bars in
  3.1 s). Header `X-Ratelimit-Limit: 200` (per minute).
- `adjustment=raw` is unadjusted (AAPL 2020-08-28 499.23 → 2020-08-31 129.04 at the 4:1 split).
- Delisted symbols keep their bars (TWTR to 2022-10-27, SIVB to 2023-03-09), but they are **not**
  in `/v2/assets?status=inactive` (19 170 inactive, 16 295 of them OTC; 14 405 active). Each
  assets call takes ~135-145 s.
- **Symbols are reused:** `FB` before 2022-06-09 is Meta; `FB` in 2026 is another security
  (~$45). Bars are keyed by the symbol *as it was then*.
- Corporate actions `/v1/corporate-actions` go back to at least 2016 for the whole market,
  paginated (1 000 per page): cash dividends, forward/reverse splits, name changes (old → new
  symbol and CUSIP), cash/stock mergers, worthless removals, spin-offs, rights and more (AAPL
  2016-2019 16 dividends; TWTR a cash merger; META 2 name changes; Sep 2026 ~1 000 actions).
- The trading calendar `/v2/calendar` answers (22 sessions in Oct 2026).

When this is done, these are true (each is covered by at least one slice's test):

- **A1 Raw first.** Every Alpaca response is archived (source `alpaca`) before parsing, and
  `usfin reparse` rebuilds every Alpaca table from the raw store with no request made. The API
  key and secret never appear in the raw store, `raw_response`, logs, errors or the UI.
- **A2 Bars.** `alpaca_bar` holds unadjusted daily bars (open, high, low, close, volume,
  trade count, VWAP) keyed by (symbol, date) for the universe, from 2016-01-04. A second run
  over the same window writes nothing (C2); a bar Alpaca corrects inside the lookback window is
  kept as a new version with its `first_seen_at`, and `alpaca_bar_as_of(ts)` returns what was
  known at `ts` (C4).
- **A3 Universe.** The universe is every symbol in the asset list on a US exchange (NYSE,
  NASDAQ, AMEX, ARCA, BATS; not OTC), active or inactive, plus every symbol that a corporate
  action removed (cash/stock merger, worthless removal) or renamed (old symbol). TWTR and SIVB
  are in it; OTC-only symbols are not.
- **A4 Assets history.** Each daily asset snapshot is diffed into a type-2 table keyed by the
  Alpaca asset `id` (stable across renames), so a symbol's status, exchange, name and
  tradability on a past date can be read, like `symbol_history`.
- **A5 Corporate actions.** `alpaca_corp_action` keeps every action from 2016-01-01 on with its
  type, symbols, CUSIPs, dates and rates, keyed by Alpaca's action `id`; re-fetching inserts
  nothing.
- **A6 Adjusted prices.** `alpaca_bar_adjusted` (view or table) gives split- and
  dividend-adjusted closes and total-return factors computed from A2 + A5; for AAPL across the
  2020-08-31 split the adjusted series has no jump, and it matches Alpaca's own
  `adjustment=all` close within 0.5 % on the checked days.
- **A7 Security identity.** A price series is read per security, not per symbol: name changes
  link old and new symbols (FB → META on 2022-06-09), and the reused `FB` after that date is a
  different security. A lookup by today's symbol returns the history of today's security only.
- **A8 EDGAR link.** A US-listed price series joins to its EDGAR company (CIK) through the
  existing `ticker_norm`; the EDGAR company page shows the latest close and a price chart.
- **A9 Schedule and alerts.** `alpaca_bars` runs every NYSE day after the close (18:00 ET),
  `alpaca_assets` daily, `alpaca_corp_actions` daily; each has a schedule entry with alert
  rules (zero new rows on a due day, stale watermark, value jump), and missed runs catch up
  (C6, C7, C8).
- **A10 Rate limits.** One limiter per Alpaca host stays under 200 requests/min; a 429 backs
  off and retries; the full backfill (≈ 13 k symbols × 2016-on) resumes per batch after a stop.
- **A11 UI.** `usfin serve` gains a `/prices` page (symbol search, adjusted/unadjusted chart,
  table, CSV) and a `/prices/universe` page (assets with status, exchange, delisted ones); every
  table has a CSV, like the existing pages.
- **A12 Ops.** `usfin doctor` reports `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` set or unset
  (never the value); with either unset the Alpaca jobs refuse to start and send nothing. README
  and `.env.example` document them.

## What

In scope:

- Alpaca HTTP access: two hosts (`data.alpaca.markets`, `paper-api.alpaca.markets`), key
  headers, rate limits, 429 handling, archiving.
- Jobs `alpaca_assets`, `alpaca_corp_actions`, `alpaca_bars`, and the backfill command
  `usfin alpaca backfill bars|corp-actions`.
- Tables + migrations: asset snapshots and history, corporate actions, bars (versioned), the
  adjusted view, the security-identity mapping.
- Schedule entries, alert rules, doctor checks, reparse support.
- UI pages `/prices`, `/prices/universe`, and the price block on the EDGAR company page.

**Out of scope:**

- Orders, positions, fills, paper trading — a later, separate execution service.
- Any paid Alpaca plan, real-time/streaming data, intraday bars, quotes and trades, options,
  crypto, news.
- OTC symbols.
- Delisted symbols that are in neither the asset list nor a corporate action (a known
  survivorship gap; Nasdaq Data Link `SHARADAR/TICKERS` could close it later).
- History before 2016-01-04 (the free SIP window).
- Changing EDGAR, BLS, Fiscal, TreasuryDirect or Nasdaq Trader jobs, except the read-only join
  in A8.
- A shared query layer across repos; consumers read `data/usfin.duckdb` directly.

## Who

- **User:** Tommy Yohanes — runs it locally, reads the UI, queries DuckDB for backtests.
- **Owner/reviewer:** Tommy reviews the branch and decides on the PR.
- **Doers:** one fresh Claude subagent per slice, dispatched sequentially by the planning
  session, which verifies each slice against git; a fresh reviewer subagent critiques this plan
  first.

## Where

Push target: remote `origin` → `https://github.com/tomtomtomdev/sec-edgar.git`, branch
`feat/alpaca-market-data` (new, from `origin/main` 625cac8; resolved as "the only remote", branch
named `feat/<slug>` because the current branch is `main`). `git ls-remote --heads origin`
answered 2026-10-12. The branch was created tracking `origin/main`, so the **first push is
`git push -u origin feat/alpaca-market-data`**, which sets its own upstream.

Work happens in the **worktree `/Users/tommy/Projects/sec-edgar-alpaca`**, not the main checkout
`/Users/tommy/Projects/sec-edgar`, which is running the first EDGAR bulk backfill on `main` and
holds the lock on its `data/usfin.duckdb`. The worktree has its own git-ignored `.env` (copy of
the main one, with the Alpaca keys) and its own `data/` and `raw/`; live runs in slices write
there. Slices never touch the main checkout.

Files (new unless marked):

```
config/hosts.yaml                (edit) data.alpaca.markets, paper-api.alpaca.markets
config/schedule.yaml             (edit) alpaca_assets, alpaca_corp_actions, alpaca_bars
config/alpaca.yaml               universe exchanges, batch size, lookback, backfill start
.env.example, README.md          (edit)
src/usfin/settings.py            (edit) APCA_API_KEY_ID, APCA_API_SECRET_KEY
src/usfin/http.py                (edit only if needed) per-host auth headers, 429 retry
src/usfin/doctor.py              (edit)
src/usfin/jobs.py                (edit) register the alpaca module for import
src/usfin/cli.py                 (edit) `usfin alpaca backfill …`
src/usfin/sources/alpaca/__init__.py, client.py, assets.py, corp_actions.py, bars.py,
                         adjust.py, identity.py
src/usfin/migrations/020_alpaca_asset.sql, 021_alpaca_corp_action.sql, 022_alpaca_bar.sql,
                     023_alpaca_adjusted.sql (numbers follow the last migration at the time)
src/usfin/web/prices.py, web/app.py (edit), web/edgar.py (edit), templates as the app has them
tests/test_alpaca_*.py, tests/fixtures/alpaca/...
```

Runs on this Mac (macOS, Apple Silicon) only.

## When

1. Client + settings + doctor (everything depends on it).
2. Assets snapshot + history (defines the universe's listed part).
3. Corporate actions (completes the universe; needed by identity and adjustment).
4. Bars: daily job, versioning, backfill.
5. Identity (renames, symbol reuse) and adjusted prices.
6. Schedule + alerts per job, as each job lands (never one big scheduler edit).
7. UI pages, EDGAR link, README.

No deadline. The live backfill of all bars runs after the bars slice, in the worktree; the
main checkout's DB gets Alpaca data only after the branch is merged.

## How

### Tech stack

| Layer | Choice | Version | Source |
|-------|--------|---------|--------|
| Language / toolchain | Python, uv, uv_build | 3.13 (`requires-python >=3.12`) / uv_build >=0.12.2,<0.13 | `.python-version`, `pyproject.toml` |
| CLI | typer | 0.27.3 | `pyproject.toml` (pinned) |
| HTTP | httpx sync client via `usfin.http` + `usfin.ratelimit` | 0.28.1 | `pyproject.toml`, existing code |
| Storage | DuckDB single file, SQL migrations `src/usfin/migrations/NNN_*.sql` | 1.5.6 | `pyproject.toml`, existing code |
| Config / env | pyyaml, python-dotenv, `config/*.yaml` | 6.0.3 / 1.2.4 | `pyproject.toml` |
| Calendars | holidays (`nyse`) via `usfin.calendars` | 0.106 | `pyproject.toml` |
| Scheduler | APScheduler via `usfin.scheduler`, `config/schedule.yaml` | 3.11.3 | `pyproject.toml` |
| Web UI | FastAPI + uvicorn + Jinja2, server-rendered, hand-built SVG charts (`web/charts.py`) | 0.142.2 / 0.54.0 / 3.1.6 | `pyproject.toml`, existing code |
| Jobs | `usfin.jobs.Job` registered in `jobs.REGISTRY`, watermarks, chaining, reparse | — | existing code |
| Tests | pytest, respx, FastAPI TestClient; `live` marker for network tests | 9.1.1 / 0.23.1 | `pyproject.toml` |
| Lint | ruff, line length 100 | 0.16.10 | `pyproject.toml` |

**New dependencies:** none. No Alpaca SDK (`alpaca-py`): the three endpoints are plain JSON over
httpx, and an SDK would bring a second HTTP stack and pydantic.

### Approach

**Auth and secrets.** Alpaca authenticates by headers (`APCA-API-KEY-ID`,
`APCA-API-SECRET-KEY`), not query parameters. The client adds them per request for the two
Alpaca hosts only. Whatever `usfin.http` archives about a request (URL, params, headers if any)
must exclude them; a test asserts the key and secret strings appear nowhere in the raw store,
`raw_response`, captured logs or error messages (A1). Missing keys → the job raises the same
"refuses to start" error EDGAR uses for `SEC_USER_AGENT` (A12).

**Rate limit.** `hosts.yaml`: both Alpaca hosts at ≤ 3 requests/s (180/min, under the measured
200/min). A 429 is retried with backoff by the existing client if it already does so for other
hosts; otherwise add it there (A10).

**Assets (`alpaca_assets`).** GET `/v2/assets?status=active|inactive&asset_class=us_equity`
from `paper-api` (two slow requests, ~140 s each: the job's timeout must allow ≥ 300 s). Stored
as `alpaca_asset_snapshot` (snapshot_at, id, symbol, name, exchange, status, tradable, class,
attributes…), then diffed into `alpaca_asset_history` (type-2, `valid_from`/`valid_to`) keyed by
asset `id` — the same pattern as `symbol_history` (A4). A row-count swing gate like C9 guards
the diff. Watermark = newest snapshot.

**Corporate actions (`alpaca_corp_actions`).** GET `/v1/corporate-actions` by date window
(`start`/`end`, all types, `limit=1000`, follow `next_page_token`). Each type has different
fields, so storage is one row per action: `id` (PK), `type`, `symbol`, `old_symbol`,
`new_symbol`, `cusip`/`old_cusip`/`new_cusip`, `ex_date`, `record_date`, `payable_date`,
`process_date`, `effective_date`, `rate`, `old_rate`, `new_rate`, `cash_rate`, `special`,
`foreign`, plus the action's full JSON in a `payload` column for anything not promoted. Watermark
= newest `process_date`; daily runs re-fetch a 30-day lookback window (actions are announced
ahead and amended). Backfill walks month windows from 2016-01-01 (A5).

**Universe.** `alpaca_universe` (view): asset symbols whose exchange is in `config/alpaca.yaml`
`exchanges` (NYSE, NASDAQ, AMEX, ARCA, BATS), active or inactive, union the acquiree/old symbols
of merger, worthless-removal and name-change actions. Each universe symbol carries the date
range it needs bars for: from 2016-01-04 (or its first action date) to today if active, or to
its removal date + 5 days if not (A3).

**Bars (`alpaca_bars`).** GET `/v2/stocks/bars?timeframe=1Day&feed=sip&adjustment=raw`, symbols
in batches (`config/alpaca.yaml` `batch_size`, start at 100 and measure; URL length and
`limit=10000` pagination both bound it). Daily job: every universe symbol that is active, window
= watermark − `lookback_days` (5) to today; the free plan's 15-minute delay is irrelevant at
18:00 ET. Table `alpaca_bar` (symbol, date, open, high, low, close, volume, trade_count, vwap,
first_seen_at, raw_id), PK (symbol, date, first_seen_at); a fetched bar equal to the newest
stored version for its key inserts nothing, a different one inserts a new version (A2).
`alpaca_bar_as_of(ts)` = per (symbol, date) the newest version with `first_seen_at <= ts`
(the same shape as `bls_as_of`). Backfill: `usfin alpaca backfill bars [--from 2016-01-04]
[--symbols …]` walks the universe in batches; progress per batch is a watermark key, so a stop
resumes (A10). Expected size ≈ 13 k symbols × ≤ 2 700 bars ≈ 25-35 M rows; measure in the slice
and record it.

**Identity (A7).** `alpaca_security` maps (symbol, valid_from, valid_to) → security key. The
security key is the Alpaca asset `id` where the asset list knows the symbol on that date;
name-change actions chain old → new symbol under one key from the action's date; a symbol
reused after a rename starts a new key. Delisted symbols with no asset row get a synthetic key
from their CUSIP (from the action). The `/prices` lookup and the EDGAR join go through it.

**Adjusted prices (A6).** From `alpaca_bar` (newest versions) + splits and cash dividends:
backward-cumulative split factor (old_rate/new_rate at each ex_date) and dividend factor
(1 − dividend / previous close) on ex_dates, as `alpaca_bar_adjusted` (symbol, date, close,
adj_close, split_factor, tr_factor). Checked against Alpaca's own `adjustment=all` close for a
few symbols in a live test (within 0.5 %); a mismatch is reported, not tuned away.

**EDGAR link (A8).** Alpaca symbols go through the existing `ticker_norm` and its EDGAR ticker
map to a CIK; unmatched ones are listed (like `/symbols/unmatched`), never dropped. The EDGAR
company page reads the latest close and a 1-year chart for the company's security.

**UI (A11).** `web/prices.py`: `/prices?symbol=` (adjusted/unadjusted toggle, SVG chart via
`web/charts.py`, table, CSV via `?table=`), `/prices/universe` (filters: status, exchange,
delisted). The app-wide test that fetches every GET page's CSV must pass with the new routes.

**Risks and how each is retired.**

- *Bar volume and DuckDB size.* Measured in the bars slice (rows, DB growth, time); if the
  versioned PK makes inserts slow, the slice records it and switches to the `xbrl_fact`
  no-PK pattern (migration 014) with a dedupe check.
- *Free-plan limits change.* A live test asserts the 2016 SIP start and the rate-limit header;
  it fails loudly if Alpaca changes them.
- *Assets endpoint slowness (~140 s).* The job timeout and the test use the measured time; the
  job runs once a day.
- *Symbol reuse and renames.* Retired by A7's tests built from the real FB/META and TWTR
  fixtures.
- *Delisted gap.* Documented as out of scope; the universe slice reports how many removed
  symbols came from corporate actions versus the asset list.
- *The main checkout's DB is locked by the EDGAR backfill.* Retired by working in the worktree
  with its own DB.

### Commands

- Install: `uv sync`
- **Test:** `uv run pytest -q` (live tests excluded by the default `-m 'not live'`; run them with
  `uv run pytest -m live`)
- **Build (gate):** `uv run ruff check . && uv run pytest -q && uv build -q`
- **Run:** `uv run usfin <command>` — e.g. `uv run usfin run alpaca_assets`,
  `uv run usfin alpaca backfill bars --symbols AAPL,TWTR`, `uv run usfin serve`
  (scheduler + UI on http://127.0.0.1:8660). All from the worktree.
