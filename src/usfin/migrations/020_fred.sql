-- FRED series (S42, plan: How > FRED). `fred_series` is the latest catalog entry per configured
-- series (fred/series). `fred_observation` keeps one row per distinct published version of a
-- (series, date): `realtime_start` is the vintage date FRED gives it, `first_seen_at` that day at
-- 00:00 America/New_York (naive UTC), `raw_id` the response it came from. A value FRED sends as
-- "." is NULL.
CREATE TABLE fred_series (
    series_id           VARCHAR PRIMARY KEY,
    title               VARCHAR NOT NULL,
    units               VARCHAR,
    frequency           VARCHAR,
    seasonal_adjustment VARCHAR,
    last_updated        VARCHAR,
    raw_id              BIGINT NOT NULL
);

CREATE TABLE fred_observation (
    series_id      VARCHAR NOT NULL,
    date           DATE NOT NULL,
    value          DECIMAL(24, 6),
    realtime_start DATE NOT NULL,
    first_seen_at  TIMESTAMP NOT NULL,
    raw_id         BIGINT NOT NULL,
    PRIMARY KEY (series_id, date, first_seen_at)
);

-- Per series and date, the latest version first seen at or before `ts` (as in bls_as_of).
CREATE MACRO fred_as_of(ts) AS TABLE
SELECT series_id, date, value, realtime_start, first_seen_at, raw_id
FROM fred_observation
WHERE first_seen_at <= bls_as_of_utc(ts)
QUALIFY row_number() OVER (PARTITION BY series_id, date ORDER BY first_seen_at DESC) = 1;

-- The latest version of every (series, date): what value_jump checks.
CREATE VIEW fred_observation_latest AS
SELECT series_id, date, value, realtime_start, first_seen_at, raw_id
FROM fred_observation
QUALIFY row_number() OVER (PARTITION BY series_id, date ORDER BY first_seen_at DESC) = 1;
