-- XBRL company facts (plan: How > SEC EDGAR > Companyfacts, S21; C4). One row per fact as a
-- filing reported it: the same (tag, unit, period) restated by a later filing is another row under
-- that filing's `accn`, so history is kept and `xbrl_as_of(d)` can cut it at a filing date. Only
-- the `us-gaap` and `dei` taxonomies. An instant fact (no `start` upstream) stores
-- `period_start = period_end` and `is_instant = true` (a PK column cannot be NULL). `frame` is
-- SEC's calendar-period label, which SEC moves to the latest filing of a period, so it is the one
-- column a refetch routinely updates. Facts missing from a later document are kept.
CREATE TABLE xbrl_fact (
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
    frame        VARCHAR,
    PRIMARY KEY (cik, taxonomy, tag, unit, accn, period_start, period_end)
);

-- Facts filed on or before `d` (a DATE, or anything that casts to one): what was public that day.
CREATE MACRO xbrl_as_of(d) AS TABLE
SELECT * FROM xbrl_fact WHERE filed <= CAST(d AS DATE);
