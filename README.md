# sec-edgar (usfin)

US public financial data ingestion: SEC EDGAR, Treasury Fiscal Data, BLS, TreasuryDirect, Nasdaq Trader symbol directory.

Plan and progress: [docs/plans/us-fin-ingest.md](docs/plans/us-fin-ingest.md).

## EDGAR full bulk load (measured 2026-10-08, S27)

Both bulk ZIPs are loaded member by member from the raw store, guarded by `USFIN_MIN_FREE_GB`
(see the plan, How › SEC EDGAR › Bulk). Check the projection first, then load from the stored
ZIP:

```sh
uv run usfin edgar backfill companyfacts --estimate   # projection from the newest sample
uv run usfin edgar backfill companyfacts --reuse      # parse the newest stored ZIP
uv run usfin edgar backfill submissions --reuse       # edgar_company CIKs only (+ their pages)
```

Measured on this Mac (Apple Silicon, DuckDB 1.5.6), `xbrl_fact` without a primary key:

| Dataset | ZIP | Members parsed | Rows | Time | DB growth |
|---|---|---|---|---|---|
| companyfacts | 1.31 GiB, 20 440 members | 20 034 (+ 8 after a fix; 61 are `{}`; 199 from the S26 sample) | 120 221 183 facts (+ 9 811) | 28 min (1 674 s parse) | +2.36 GB (~20 B/fact) |
| submissions | 1.46 GiB, 993 472 members; 10 759 of `edgar_company` CIKs | 10 555 (+ S26/S27 samples); 982 713 not loaded | 6 601 482 filings | 24 min (1 444 s parse) | +1.90 GB (~288 B/filing) |

After both: `xbrl_fact` 122.7 M facts (17 133 companies, no duplicate key), `edgar_filing`
6.88 M filings, DB file 4.47 GB (from 0.21 GB), raw store 5.6 GB (two ZIPs per dataset kept by
`raw prune --keep 2`), free disk 47.9 → 38.3 GiB. The weekly job `edgar_bulk` (Sun 03:00 ET)
re-downloads both ZIPs (~13–17 min at ~2 MB/s) and re-parses only members whose CRC changed; a
run against an unchanged ZIP parses nothing (0 rows).
