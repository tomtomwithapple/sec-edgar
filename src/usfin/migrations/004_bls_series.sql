-- BLS series metadata (plan: How > BLS). One row per configured series; titles, seasonality and
-- units come from config/bls.yaml (the keyless v1 API sends no catalog); `decimals` is the
-- published precision that S16's versioned observations are compared at.
CREATE TABLE bls_series (
    series_id   VARCHAR PRIMARY KEY,
    title       VARCHAR NOT NULL,
    seasonality VARCHAR,
    units       VARCHAR,
    decimals    INTEGER NOT NULL
);
