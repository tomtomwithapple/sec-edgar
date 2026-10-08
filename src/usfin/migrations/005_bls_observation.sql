-- BLS versioned observations (plan: How > BLS, C4). One row per distinct published version of a
-- (series, year, period): `first_seen_at` is the `fetched_at` of the raw response that first
-- carried it (naive UTC), `raw_id` that response. A period BLS publishes as "-" (e.g. 2025 M10,
-- the lapse in appropriations) is a row with `value` NULL and its footnotes; a later fill is a new
-- version. `footnotes` is a JSON list of {code, text} objects ([] when none).
CREATE TABLE bls_observation (
    series_id     VARCHAR NOT NULL,
    year          INTEGER NOT NULL,
    period        VARCHAR NOT NULL,
    value         DECIMAL(18, 6),
    footnotes     JSON NOT NULL,
    first_seen_at TIMESTAMP NOT NULL,
    raw_id        BIGINT NOT NULL,
    source        VARCHAR NOT NULL DEFAULT 'bls' CHECK (source IN ('bls', 'alfred')),
    PRIMARY KEY (series_id, year, period, first_seen_at)
);

-- The as-of instant as naive UTC: a DATE means 23:59:59 America/New_York that day, a TIMESTAMPTZ
-- is converted to UTC, a plain TIMESTAMP is already naive UTC (how every timestamp is stored).
CREATE MACRO bls_as_of_utc(ts) AS CASE
    WHEN typeof(ts) = 'DATE' THEN timezone('UTC', timezone('America/New_York',
        CAST(CAST(ts AS DATE) AS TIMESTAMP) + INTERVAL '23 hours 59 minutes 59 seconds'))
    WHEN typeof(ts) = 'TIMESTAMP WITH TIME ZONE' THEN timezone('UTC', CAST(ts AS TIMESTAMPTZ))
    ELSE CAST(ts AS TIMESTAMP)
END;

-- Per series and period, the latest version first seen at or before `ts`.
CREATE MACRO bls_as_of(ts) AS TABLE
SELECT series_id, year, period, value, footnotes, first_seen_at, raw_id, source
FROM bls_observation
WHERE first_seen_at <= bls_as_of_utc(ts)
QUALIFY row_number() OVER (
    PARTITION BY series_id, year, period ORDER BY first_seen_at DESC
) = 1;
