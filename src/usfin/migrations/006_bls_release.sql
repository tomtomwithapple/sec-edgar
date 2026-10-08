-- BLS release calendar (plan: How > BLS, S17). One row per release event of the BLS release
-- schedule (https://www.bls.gov/schedule/news_release/bls.ics): its New York date, its name (the
-- event SUMMARY, matched by `release` in config/bls.yaml), the release time as naive UTC, and the
-- raw response that first carried it. Job `bls_calendar` makes the table equal each file within
-- the file's date range, so a moved release loses its old date.
CREATE TABLE bls_release (
    release_date DATE NOT NULL,
    release_name VARCHAR NOT NULL,
    released_at  TIMESTAMP NOT NULL,
    raw_id       BIGINT NOT NULL,
    PRIMARY KEY (release_date, release_name)
);

-- The current version of each monthly BLS observation with a date for the alert rules:
-- `period_date` is the first day of the period's month (M13, the annual average, is left out).
CREATE VIEW bls_observation_dated AS
SELECT series_id, year, period,
       make_date(year, CAST(substr(period, 2) AS INTEGER), 1) AS period_date,
       value, first_seen_at
FROM bls_observation
WHERE period LIKE 'M%' AND period <> 'M13'
QUALIFY row_number() OVER (
    PARTITION BY series_id, year, period ORDER BY first_seen_at DESC
) = 1;
