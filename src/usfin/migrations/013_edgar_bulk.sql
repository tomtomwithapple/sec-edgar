-- EDGAR bulk ZIPs (plan: How > SEC EDGAR > Bulk, S26; C17). `edgar_bulk_member` records, per
-- dataset and member name, the CRC-32 of the member last parsed and the ZIP's raw response, so a
-- later load (resume after a guard stop, S27's weekly reconcile) parses only members whose CRC
-- changed. Bookkeeping, not normalized data.
CREATE TABLE edgar_bulk_member (
    dataset VARCHAR NOT NULL CHECK (dataset IN ('bulk_submissions', 'bulk_companyfacts')),
    name    VARCHAR NOT NULL,
    crc     BIGINT NOT NULL,
    raw_id  BIGINT NOT NULL,
    PRIMARY KEY (dataset, name)
);

-- One row per bulk load that parsed members of the whole ZIP (not a --cik pick): what it cost, so
-- `usfin edgar backfill --estimate` can project the full load (Measure before the full load).
-- `db_bytes_*` are the DB file sizes after a CHECKPOINT; `member_bytes_*` uncompressed member
-- sizes; `seconds` the parse time, `download_seconds` the ZIP's transfer (NULL when reused).
-- `scratch_*` (companyfacts only): the sampled facts copied into fresh DuckDB files with and
-- without the xbrl_fact primary key (S21: the 7-column PK's index is most of the table).
CREATE TABLE edgar_bulk_estimate (
    dataset              VARCHAR NOT NULL,
    measured_at          TIMESTAMP NOT NULL,
    raw_id               BIGINT NOT NULL,
    zip_bytes            BIGINT NOT NULL,
    members_total        BIGINT NOT NULL,
    members_sampled      BIGINT NOT NULL,
    member_bytes_total   BIGINT NOT NULL,
    member_bytes_sampled BIGINT NOT NULL,
    db_bytes_before      BIGINT NOT NULL,
    db_bytes_after       BIGINT NOT NULL,
    rows_inserted        BIGINT NOT NULL,
    seconds              DOUBLE NOT NULL,
    download_seconds     DOUBLE,
    scratch_rows         BIGINT,
    scratch_bytes_pk     BIGINT,
    scratch_bytes_no_pk  BIGINT,
    PRIMARY KEY (dataset, measured_at)
);
