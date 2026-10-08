# sec-edgar (usfin)

US public financial data ingestion: SEC EDGAR, Treasury Fiscal Data, BLS, TreasuryDirect, Nasdaq
Trader symbol directory. Every response is stored raw first, normalized into one DuckDB file
point-in-time, and served by a small local web UI with job status and alerts.

Plan and progress: [docs/plans/us-fin-ingest.md](docs/plans/us-fin-ingest.md).

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
| `FRED_API_KEY` | no | `usfin bls alfred-backfill` (BLS first-published history from ALFRED); never archived or logged. |
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
| EDGAR ticker map | `usfin run edgar_tickers` | 10 434 tickers / 8 016 CIKs in 1.3 s — run before the bulk submissions load, which keeps only these companies |
| Nasdaq Trader symbols | `usfin run symbols` | 13 253 symbols in 5.5 s |
| EDGAR bulk | see below | ~1 h in all |

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
| edgar_reconcile (+ form4, sched13 chained) | 23:30 | federal |
| edgar_bulk | Sun 03:00 | any |
| bls_calendar | Mon 06:00 | any |

`federal` = weekday and not a US federal holiday; `bond` = federal minus Good Friday; `nyse` =
NYSE trading day. On another day a fire records a `skipped` run. Fires missed while the Mac slept
are caught up once at wake or startup (1 h misfire grace, then the startup catch-up). The
`zero_new_rows` alert is judged only after each job's publication time (`after:` in
`config/schedule.yaml`: fiscal 17:00, symbols 07:00), so a manual or catch-up run earlier in the
day does not raise it. Alerts: `/alerts` in the UI, `usfin alerts`, and the webhook if set.

### Start at login (launchd)

[`deploy/com.tuntun.usfin.serve.plist.example`](deploy/com.tuntun.usfin.serve.plist.example) runs
`uv run usfin serve` with `WorkingDirectory` = the repo (so `config/` and `.env` resolve),
`KeepAlive` and a log at `data/usfin-serve.log`. Edit its paths, then:

```sh
cp deploy/com.tuntun.usfin.serve.plist.example ~/Library/LaunchAgents/com.tuntun.usfin.serve.plist
plutil -lint ~/Library/LaunchAgents/com.tuntun.usfin.serve.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tuntun.usfin.serve.plist
launchctl bootout gui/$(id -u)/com.tuntun.usfin.serve   # to stop it
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
