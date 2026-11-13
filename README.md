# sec-edgar (usfin)

US public financial data ingestion: SEC EDGAR, Treasury Fiscal Data, BLS, TreasuryDirect, Nasdaq
Trader symbol directory, FRED, TIC, and Alpaca daily prices (free plan). Every response is stored raw first, normalized into one DuckDB file
point-in-time, and served by a small local web UI with job status and alerts.

Plan and progress: [docs/plans/us-fin-ingest.md](docs/plans/us-fin-ingest.md); Alpaca market data:
[docs/plans/alpaca-market-data.md](docs/plans/alpaca-market-data.md).

## Setup

macOS (Apple Silicon), Python 3.13 via [uv](https://docs.astral.sh/uv/):

```sh
uv sync                       # installs the locked versions (uv.lock)
cp .env.example .env          # then fill in the two required keys below
uv run usfin doctor           # env, config, DB lock, raw size, free disk — exits 1 on a problem
uv run pytest -q              # tests never touch the network, data/ or .env
```

### `.env`

Real environment variables win over `.env`. `usfin doctor` shows each key as set or unset and
never prints a value.

| Key | Required | What for |
|---|---|---|
| `SEC_USER_AGENT` | **yes** (EDGAR) | `Name email@example.com`, sent on every SEC request (fair-access policy). Unset: EDGAR jobs refuse to start, no request is sent. |
| `BLS_CONTACT_EMAIL` | **yes** (BLS calendar) | www.bls.gov answers 403 unless the User-Agent carries a contact email; usfin sends `usfin/<version> (<email>)`. |
| `BLS_API_KEY` | no | BLS v2 API. Without it the keyless v1 API is used (daily cap; release-day retries every 15 min instead of 10). |
| `FRED_API_KEY` | no | job `fred` (`config/fred.yaml` series) and `usfin bls alfred-backfill` (BLS first-published history from ALFRED); without it `fred` records `skipped`; never archived or logged. |
| `APCA_API_KEY_ID` | **yes** (Alpaca) | Alpaca key id from a free paper account (<https://app.alpaca.markets> → API keys). Sent only as a header to `data.alpaca.markets` and `paper-api.alpaca.markets`; never archived, logged or shown. Unset: the Alpaca jobs, backfills and `usfin http get` to an Alpaca host raise `ConfigError` and send nothing. |
| `APCA_API_SECRET_KEY` | **yes** (Alpaca) | The Alpaca secret for that key id, same rules (unset: `ConfigError`, nothing sent). |
| `ALERT_WEBHOOK_URL` | no | Each new alert is POSTed here as JSON. |
| `USFIN_DB` | no | DuckDB file, default `data/usfin.duckdb`. |
| `USFIN_RAW` | no | Raw store, default `raw/`. |
| `USFIN_CONFIG` | no | Config dir, default `config/` (not in the wheel: run from the repo). |
| `USFIN_LOCK_WAIT` | no | Seconds a job or write command waits for the DB lock, default 600. |
| `USFIN_MIN_FREE_GB` | no | Free-disk floor in GiB for EDGAR bulk downloads/loads, default 10. |
| `USFIN_BULK_LIMIT` | no | Members per ZIP the weekly `edgar_bulk` job parses (unset = all changed ones). |

`.env`, `data/` and `raw/` are git-ignored.

## First backfills

Run once, in this order, from the repo (`uv run usfin …`). Times measured on this Mac on
2026-10-08.

| Source | Command | Measured |
|---|---|---|
| Fiscal Data (6 datasets from 2000) | `usfin run fiscal` | 14 pages, 58 575 rows in 20 s; repeat runs ~5 s, 0 rows. Chains `auction_xcheck`. |
| TreasuryDirect current | `usfin run treasurydirect` | 2 requests, 250 rows in 5 s |
| TreasuryDirect history | `usfin td backfill` (from 1979-10-31, resumable) | 565 monthly requests, 10 893 rows in 210 s |
| BLS series (7, keyless) | `usfin run bls` | one query, 143 observations |
| BLS release calendar | `usfin run bls_calendar` | 313 release dates (needs `BLS_CONTACT_EMAIL`) |
| FRED series (34, every vintage) | `usfin run fred` | first load of the 18 S42 series: 21 vintage-date lists + 45 observation pages, 193 751 rows in ~3 min; the 4 added later (DFII10, DTWEXBGS, NFCI, DCOILBRENTEU) 967 719 rows in 9 min, 928 189 of them NFCI (every weekly vintage re-estimates the whole history); the 12 after that (net liquidity, EM OAS, nowcasts, breakevens, term premium, stress, policy uncertainty, IDR) 518 372 rows in 5.6 min, 313 457 of them STLFSI4; repeat runs ~40 s (needs `FRED_API_KEY`) |
| TIC cross-border securities (keyless) | `usfin run tic` | one 8.9 MB ZIP (122 MB JSON), 4 260 series, 1 130 857 rows in 29 s; repeat runs are a 304 (~10 s, 0 rows) until the next monthly release |
| EDGAR ticker map | `usfin run edgar_tickers` | 10 434 tickers / 8 016 CIKs in 1.3 s — run before the bulk submissions load, which keeps only these companies |
| Nasdaq Trader symbols | `usfin run symbols` | 13 253 symbols in 5.5 s |
| EDGAR bulk | see below | ~1 h in all |
| Alpaca asset list | `usfin run alpaca_assets` | one call, 33 564 assets (14.6 MB) in ~30 s |
| Alpaca corporate actions | `usfin run alpaca_corp_actions`, then `usfin alpaca backfill corp-actions` (from 2016-01-01, resumable) | daily window 8 214 actions in 13 s; backfill 130 month windows, 461 pages, ~380 k actions, ~1.5 s a page |
| Alpaca daily bars | `usfin alpaca backfill bars` (after the two above; resumable) | see *Alpaca market data* |

### EDGAR full bulk load (measured 2026-10-08, S26/S27)

Both bulk ZIPs are loaded member by member from the raw store, guarded by `USFIN_MIN_FREE_GB`
(see *Disk guard*). Sample first, check the projection, then load the whole ZIP:

```sh
uv run usfin edgar backfill companyfacts --check      # size the ZIP, print the free-disk verdict
uv run usfin edgar backfill companyfacts --limit 200  # sample: downloads the ZIP, records an estimate
uv run usfin edgar backfill companyfacts --estimate   # projection from the newest sample
uv run usfin edgar backfill companyfacts --reuse      # parse the stored ZIP in full
uv run usfin edgar backfill submissions --limit 200   # then the same for submissions
uv run usfin edgar backfill submissions --reuse       # edgar_company CIKs only (+ their pages)
```

Measured on this Mac (Apple Silicon, DuckDB 1.5.6), `xbrl_fact` without a primary key:

| Dataset | ZIP | Members parsed | Rows | Time | DB growth |
|---|---|---|---|---|---|
| companyfacts | 1.31 GiB, 20 440 members | 20 034 (+ 8 after a fix; 61 are `{}`; 199 from the S26 sample) | 120 221 183 facts (+ 9 811) | 28 min (1 674 s parse) | +2.36 GB (~20 B/fact) |
| submissions | 1.46 GiB, 993 472 members; 10 759 of `edgar_company` CIKs | 10 555 (+ S26/S27 samples); 982 713 not loaded | 6 601 482 filings | 24 min (1 444 s parse) | +1.90 GB (~288 B/filing) |

Each download took ~10–11 min (~2.2 MB/s). After both: `xbrl_fact` 122.7 M facts (17 133
companies, no duplicate key), `edgar_filing` 6.88 M filings, DB file 4.47 GB (from 0.21 GB), raw
store 5.6 GB (two ZIPs per dataset kept by `raw prune --keep 2`), free disk 47.9 → 38.3 GiB. The
weekly job `edgar_bulk` (Sun 03:00 ET) re-downloads both ZIPs (~13–17 min) and re-parses only
members whose CRC changed; a run against an unchanged ZIP parses nothing (0 rows).

### Company profiles and SIC (S45)

`edgar_company_profile` holds one row per CIK from the top of its submissions JSON: `sic`,
`sic_description`, `owner_org`, `entity_type`, `category`, `fiscal_year_end`,
`state_of_incorporation`, `exchanges`. The view `edgar_company_sic` adds `sic_major` (two-digit
major group) and `sic_division` / `sic_division_name` (A–K). Every submissions main document the
per-CIK job or the weekly bulk reconcile parses keeps it current; to fill it from what is already
stored (no request):

```sh
uv run usfin edgar profiles   # newest stored submissions.zip, then per-CIK responses; newer wins
```

Measured 2026-11-13: 8 116 profiles in 70 s, 6 991 with a SIC (funds and trusts have none);
`/prices/universe?status=active&matched=1` carries `sic`, `sic_description`, `sic_major`,
`sic_division`, set on 6 735 of 7 182 rows.

## Running

```sh
uv run usfin serve            # scheduler + web UI on http://127.0.0.1:8660 (localhost only)
uv run usfin schedule         # next 5 fires per job (ET)
uv run usfin jobs             # last run and watermarks per job
uv run usfin alerts           # open alerts; `usfin alerts ack <id>`
uv run usfin run <job>        # one job now, in this process (ignores the calendar)
uv run usfin reparse <job>    # rebuild a job's rows from stored raw responses, no HTTP
```

### Schedule (America/New_York)

| Job | When (ET) | Calendar |
|---|---|---|
| symbols | 07:00 | nyse |
| edgar_tickers | 07:15 | any |
| edgar_feed (+ submissions, companyfacts, form4, sched13 chained) | every 15 min 06:00–22:45 | federal |
| bls | 09:00, retry every 15 min to 12:00 on release days | federal |
| treasurydirect | 14:00 | bond |
| fiscal (+ auction_xcheck chained) | 17:00 | federal |
| tic | 17:15 | federal |
| fred | 18:30 | federal |
| edgar_reconcile (+ form4, sched13 chained) | 23:30 | federal |
| edgar_bulk | Sun 03:00 | any |
| bls_calendar | Mon 06:00 | any |
| alpaca_assets | 07:30 | any |
| alpaca_corp_actions | 07:45 | nyse |
| alpaca_bars | 20:30 | nyse |

`federal` = weekday and not a US federal holiday; `bond` = federal minus Good Friday; `nyse` =
NYSE trading day. On another day a fire records a `skipped` run. Fires missed while the Mac slept
are caught up once at wake or startup (1 h misfire grace, then the startup catch-up). The
`zero_new_rows` alert is judged only after each job's publication time (`after:` in
`config/schedule.yaml`: fiscal 17:00, symbols 07:00), so a manual or catch-up run earlier in the
day does not raise it. Alerts: `/alerts` in the UI, `usfin alerts`, and the webhook if set.

### Start at login (launchd)

One step: `scripts/install.sh` runs `uv sync`, checks `.env` (creates it from `.env.example` and
stops if `SEC_USER_AGENT` or `BLS_CONTACT_EMAIL` is empty; warns on an empty `FRED_API_KEY`),
`usfin db migrate` and `usfin doctor`, then installs the agent below with this checkout's paths,
starts it and waits for http://127.0.0.1:8660. Rerun it to reinstall; `--uninstall` stops and
removes the agent (data kept), `--no-agent` stops before launchd. serve's startup catch-up runs
every job whose last due fire is later than its last ok run, so a first install fills in by
itself (the EDGAR bulk load above stays a manual step).

[`deploy/com.tuntun.usfin.serve.plist.example`](deploy/com.tuntun.usfin.serve.plist.example) runs
`uv run usfin serve` with `WorkingDirectory` = the repo (so `config/` and `.env` resolve),
`KeepAlive` and a log at `data/usfin-serve.log`. By hand instead: edit its paths, then:

```sh
cp deploy/com.tuntun.usfin.serve.plist.example ~/Library/LaunchAgents/com.tuntun.usfin.serve.plist
plutil -lint ~/Library/LaunchAgents/com.tuntun.usfin.serve.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tuntun.usfin.serve.plist
launchctl bootout gui/$(id -u)/com.tuntun.usfin.serve   # to stop it
```

### Start at logon (Windows Task Scheduler)

`scripts\schedule.ps1` is the Windows counterpart of `install.sh`: the same steps (`uv sync`,
`.env` check, `usfin db migrate`, `usfin doctor`), then a task `usfin-serve` that runs
`uv run usfin serve` at logon with no window (through `scripts\schedule-hidden.js`), restarts it
if it exits and appends to `data\usfin-serve.log`. The job times stay in `config/schedule.yaml`,
kept by serve itself.

```powershell
powershell -ExecutionPolicy Bypass -File scripts\schedule.ps1            # install (or reinstall) and start
powershell -ExecutionPolicy Bypass -File scripts\schedule.ps1 status     # task state, last result, port 8660
powershell -ExecutionPolicy Bypass -File scripts\schedule.ps1 run        # start the task if serve is down
powershell -ExecutionPolicy Bypass -File scripts\schedule.ps1 uninstall  # stop serve, remove the task (data kept)
```

## Analysis access between jobs

DuckDB lets one process hold the file at a time, and a second process cannot even open it
read-only while a writer holds it. So usfin holds nothing between uses: every job, CLI command
and UI request opens the DB, works, and closes it. Your own analysis (Python, the `duckdb` CLI, a
notebook) can open `data/usfin.duckdb` whenever no job is running:

```python
import duckdb
con = duckdb.connect("data/usfin.duckdb", read_only=True)
con.sql("SELECT * FROM bls_as_of(DATE '2026-10-08') LIMIT 5")
con.close()   # close it: a job waits up to USFIN_LOCK_WAIT (600 s) for the lock, then fails
```

While you hold it, jobs wait up to `USFIN_LOCK_WAIT` seconds and then fail visibly (a failed run
and a `db_locked` alert; watermarks untouched, so the next run resumes) — never a corrupt file.
UI pages say "database busy" after 5 s. `usfin doctor` shows whether, and which PID, holds the
lock. Keep sessions short, or copy the file (`cp data/usfin.duckdb /somewhere/snapshot.duckdb`
between jobs) for long work. Point-in-time views: `bls_as_of(ts)` (BLS values as known at `ts`)
and `xbrl_as_of(d)` (XBRL facts filed by `d`).

## Disk guard

EDGAR bulk ZIPs are 1.3–1.5 GiB each and the full load grows the DB by ~4 GB. Before any bulk
download (`usfin edgar backfill`, job `edgar_bulk`) usfin sizes the ZIP — a HEAD, or, if SEC
refuses the HEAD (it did for an Archives file in S36), a 1-byte ranged GET — and checks free space
on the raw and DB volumes: if free − ZIP − projected DB growth would leave less than
`USFIN_MIN_FREE_GB` (default 10 GiB), nothing is downloaded: the CLI exits 1 with the numbers, the
job records `skipped` and raises a `disk_low` alert. The streamed download's own `Content-Length`
is checked again before a byte is written, and the guard is re-checked every 1 000 members during
a load (a stop is clean; the next run resumes by member CRC). `usfin raw prune --keep 2` (run by
`edgar_bulk`) keeps the newest two ZIPs per dataset. `usfin doctor` prints free space against the
floor and the newest bulk projection.

## FRED series

`config/fred.yaml` lists the FRED series job `fred` keeps point in time (rates, spreads, VIX, Fed
balance sheet, TGA and reverse repo (net liquidity = WALCL − WTREGEN − RRPONTSYD), real yield,
broad dollar, financial conditions and stress, EM corporate OAS (FRED keeps only 3 years of ICE
data), GDPNow and the NY Fed WEI, breakevens, term premium, policy uncertainty, IDR per USD,
Brent, GDP, PCE prices, industrial production, retail sales, housing starts, sentiment, M2);
add or remove an `id` and the next run picks it up. A series' first run loads every ALFRED
vintage, in real-time windows of at most 1 998 vintage dates (FRED refuses more than 2 000, and
counts one more than it lists); later runs ask only for vintages since the newest stored one, for
observation dates within `lookback_days` (5 years). Rows are `fred_observation`, one per distinct
published value of a date, first seen at its vintage date 00:00 New York; `fred_as_of(DATE)` gives
the values known that day, e.g.
`SELECT * FROM fred_as_of(DATE '2020-05-01') WHERE series_id = 'GDPC1'` has Q1 2020 real GDP as
first estimated (18 987.877). `/series/fred/<id>?as_of=YYYY-MM-DD` shows it. A series with
`jump_pct` in `fred.yaml` raises `value_jump` past that change; the others are not checked.

## TIC cross-border securities

Job `tic` keeps the Fed/Treasury CSLT file (`cslt.zip` on ticdata.treasury.gov) point in time:
4 260 monthly series from 1984-12, $ millions, by country. `for_*` series are foreign holdings
(`_pos`), net purchases (`_net`) and valuation changes (`_valchg`) of US Treasuries, agencies,
corporates and equities; `us_*` are the same for US investors in foreign bonds and equities. The
series id is `<measure>_<kind>_<country code>`, e.g. `for_treas_pos_42218` (Indonesia's holdings
of Treasuries) or `us_lt_eqty_net_42218` (US net purchases of Indonesian equities); `tic_series`
has the parts, the country name and whether the series is discontinued. Every release re-sends
the whole history, so `tic_observation` keeps a row only when a value changed, first seen when it
was fetched; `tic_as_of(DATE)` and `tic_observation_latest` read it like FRED. A run asks with
`If-Modified-Since` and stores nothing on a 304. `/series/tic?country=indonesia` lists a
country's series, `/series/tic/<id>?as_of=YYYY-MM-DD` shows one (both take `?format=csv`).

## ALFRED backfill (BLS history)

BLS's API serves only today's numbers, so the values as they were first published (and every
revision since) come from ALFRED, the St. Louis Fed's archive of FRED vintages. With a free key
from <https://fred.stlouisfed.org/docs/api/api_key.html> as `FRED_API_KEY=<key>` in `.env`:

```sh
uv run usfin bls alfred-backfill
```

sends one request per `config/bls.yaml` series (its `fred` ID, every vintage), archives each as raw
dataset `alfred_observations` (the key is replaced by `REDACTED` in the stored URL and in logs), and
rebuilds `bls_observation` by reparsing `bls`: ALFRED vintages first, as `source='alfred'` rows
first seen at their `realtime_start` 00:00 New York, only those before the first BLS response for
the series and rounded to its published decimals; then BLS's own responses, which add a version only
where they differ. On 2026-10-08 that was 26 051 ALFRED rows (payrolls back to 1939, vintages from
1955) and 13 BLS rows (footnotes ALFRED does not carry), in about 20 s; a rerun changes nothing.
Without `FRED_API_KEY` the command exits 1 and sends nothing. Then, e.g.,
`SELECT * FROM bls_as_of(DATE '2020-06-01') WHERE series_id = 'LNS14000000'` gives April 2020's
unemployment rate as first printed (14.7), and `/series/bls/<id>?as_of=YYYY-MM-DD` shows it.

## Alpaca market data

Unadjusted US daily bars from 2016-01-04 (the free SIP window), the asset list and corporate
actions, from a free Alpaca paper account (`APCA_API_KEY_ID` / `APCA_API_SECRET_KEY`; settings in
`config/alpaca.yaml`). Both Alpaca hosts have their own bucket in `config/hosts.yaml` at 3
requests/s (Alpaca allows 200/min); a 429 waits until `X-Ratelimit-Reset`. Raw first: every
response is archived (source `alpaca`) before it is parsed, and `usfin reparse alpaca_assets`,
`usfin reparse alpaca_corp_actions` and `usfin reparse alpaca_bars` rebuild their tables with no
request.

| Job | What it keeps |
|---|---|
| `alpaca_assets` | `alpaca_asset_snapshot` (the whole list, OTC included, when it changed), `alpaca_asset_history` (type 2 per Alpaca asset id: name, exchange, status, tradable, class). A row-count swing over 2 % is stored but not diffed. |
| `alpaca_corp_actions` | `alpaca_corp_action` (every type, versioned by payload; `alpaca_corp_action_latest`), `process_date` from watermark − 30 days to today + 90 days, so announced splits and dividends arrive before their ex-date. |
| `alpaca_bars` | `alpaca_bar` (`feed=sip`, `adjustment=raw`, `asof=-`; versioned by `first_seen_at`): every active symbol from watermark − 5 days, plus up to `new_symbol_cap` (200) universe symbols whose history was never asked, in full. Symbols Alpaca refuses (CUSIP-like, `-`/`/`) are remembered in `alpaca_bar_symbol` and never asked again. |

The bars universe (`alpaca_universe`) is every asset-list symbol on NYSE, NASDAQ, AMEX, ARCA or
BATS (active or inactive) plus every symbol a merger, worthless removal or name change names.
**Survivorship gap:** a delisted symbol in neither (e.g. SIVB) has no bars, and Alpaca's merger
and rename actions are sparse before 2019, so 2016–2019 delistings are mostly missing.

### Bars backfill

```sh
uv run usfin run alpaca_assets                     # the universe needs the asset list
uv run usfin alpaca backfill corp-actions          # and the actions (from 2016-01-01)
caffeinate -s uv run usfin alpaca backfill bars    # every universe symbol's history, ~3.5 h
uv run usfin alpaca backfill bars --symbols AAPL,MSFT   # just these (upper-cased)
```

The bars backfill asks 100 symbols per request from `--from` (default `backfill_start`,
2016-01-04) with no end date, and stores each batch in its own DB session, so `usfin serve`'s
jobs are never locked out for long. A stop loses only the open batch: rerun the same command and
it skips every symbol already asked in full (by any earlier run or the daily job's catch-up). A
`--from` later than 2016-01-04 refetches that stretch and marks nothing. Progress is under
`alpaca_bars_backfill` in `usfin jobs` and raises no alert. Run it on AC power under
`caffeinate -s`: macOS stops the request timeouts while the Mac sleeps, so a socket left open
across sleep hangs.

Measured 2026-10-22 (one uninterrupted run, on AC power): 22 027 symbols left to ask of the
23 130 in the universe, 221 batches, 2 921 pages, **23.9 M bars** in 3 h 33 min (active symbols
~90 s a batch, the inactive and action-only tail ~10-30 s); 21 606 symbols asked in full (2 493
of all marked have no bars) and 421 refused as invalid; `alpaca_bar` 25.1 M bars over 20 190
symbols. DB 0.11 → 0.65 GB (~23 bytes a bar), raw store 0.06 → 0.61 GB gzip. A rerun asks
nothing (0 batches, 0.3 s). Run during a session, it also stores that day's bar so far; the
20:30 job's fetch replaces it with a new version.

### Reading prices

- `alpaca_bar_latest`, `alpaca_bar_as_of(ts)`: the newest version per (symbol, date), or the one
  known at `ts`.
- `alpaca_security_lookup('META')`: the segments of the security a symbol names today (META → FB
  2016-01-04..2022-06-08 + META from 2022-06-09; FB → the new FB from 2025-06-26); view
  `alpaca_security` holds every segment. A security is keyed by CUSIP where actions give one.
- `alpaca_bar_adjusted_lookup('AAPL')` (view `alpaca_bar_adjusted`): close, `adj_close`,
  `split_factor`, `tr_factor` per date, chained across renames; splits and cash dividends applied
  (spin-offs and mergers not). Within 0.03 % of Alpaca's own `adjustment=all` for AAPL, MSFT, KO.

Pages in `usfin serve`: `/prices?symbol=META` (the chained series, adjusted chart by default,
`?adjusted=0` raw; bars and segments tables), `/prices/universe` (every universe symbol with
status, exchange, delisted, bars and its EDGAR company; filters `status`, `exchange`, `delisted`,
`matched`), and a Price block on each EDGAR company page (latest close, 1-year adjusted chart) for
its actively listed symbols. Every table downloads with `?format=csv&table=<name>`.
