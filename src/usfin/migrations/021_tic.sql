-- TIC cross-border securities (S43, plan: Slices > S43). `tic_series` is the latest catalog
-- entry per series of the CSLT file (`cslt.zip`): the series id `<measure>_<kind>_<country code>`
-- split into its parts, `active` false for a discontinued series, `transmitted_at` the release's
-- `transmissionDt`. `tic_observation` keeps one row per distinct value of a (series, month):
-- every release re-sends the whole history, so a row is new only when its value changed;
-- `first_seen_at` is when the response was fetched (naive UTC). Values are $ millions; "n.a." is
-- NULL.
CREATE TABLE tic_series (
    series_id      VARCHAR PRIMARY KEY,
    title          VARCHAR NOT NULL,
    measure        VARCHAR NOT NULL,
    kind           VARCHAR NOT NULL,
    country_code   VARCHAR NOT NULL,
    country        VARCHAR,
    active         BOOLEAN NOT NULL,
    units          VARCHAR,
    frequency      VARCHAR,
    transmitted_at TIMESTAMP,
    raw_id         BIGINT NOT NULL
);

CREATE TABLE tic_observation (
    series_id     VARCHAR NOT NULL,
    date          DATE NOT NULL,
    value         DECIMAL(24, 6),
    first_seen_at TIMESTAMP NOT NULL,
    raw_id        BIGINT NOT NULL,
    PRIMARY KEY (series_id, date, first_seen_at)
);

-- Per series and month, the latest version first seen at or before `ts` (as in fred_as_of).
CREATE MACRO tic_as_of(ts) AS TABLE
SELECT series_id, date, value, first_seen_at, raw_id
FROM tic_observation
WHERE first_seen_at <= bls_as_of_utc(ts)
QUALIFY row_number() OVER (PARTITION BY series_id, date ORDER BY first_seen_at DESC) = 1;

CREATE VIEW tic_observation_latest AS
SELECT series_id, date, value, first_seen_at, raw_id
FROM tic_observation
QUALIFY row_number() OVER (PARTITION BY series_id, date ORDER BY first_seen_at DESC) = 1;
