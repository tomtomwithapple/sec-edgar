-- `xbrl_fact` without its primary key (plan: How > SEC EDGAR > Bulk, Decisions after S26 (1); S27).
-- The 7-column PK's ART index was ~70 of 90 bytes per fact (S21, S26): ~8 GiB on the full
-- companyfacts load. Uniqueness of (cik, taxonomy, tag, unit, accn, period_start, period_end) is
-- now the loader's job (`companyfacts.upsert_facts`: per-document diff, delete + insert of the
-- changed keys). DuckDB cannot drop a PK in place, so the table is rebuilt with the same columns,
-- NOT NULLs and CHECK, and the `xbrl_as_of` macro recreated over it.
CREATE TABLE xbrl_fact_rebuilt (
    cik          BIGINT NOT NULL,
    taxonomy     VARCHAR NOT NULL CHECK (taxonomy IN ('us-gaap', 'dei')),
    tag          VARCHAR NOT NULL,
    unit         VARCHAR NOT NULL,
    accn         VARCHAR NOT NULL,
    period_start DATE NOT NULL,
    period_end   DATE NOT NULL,
    is_instant   BOOLEAN NOT NULL,
    value        DOUBLE NOT NULL,
    fy           SMALLINT,
    fp           VARCHAR,
    form         VARCHAR NOT NULL,
    filed        DATE NOT NULL,
    frame        VARCHAR
);
INSERT INTO xbrl_fact_rebuilt SELECT * FROM xbrl_fact ORDER BY cik, taxonomy, tag, unit, accn;
DROP MACRO TABLE xbrl_as_of;
DROP TABLE xbrl_fact;
ALTER TABLE xbrl_fact_rebuilt RENAME TO xbrl_fact;

-- Facts filed on or before `d` (a DATE, or anything that casts to one): what was public that day.
CREATE MACRO xbrl_as_of(d) AS TABLE
SELECT * FROM xbrl_fact WHERE filed <= CAST(d AS DATE);
