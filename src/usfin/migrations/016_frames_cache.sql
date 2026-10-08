-- EDGAR frames cache (plan: How > SEC EDGAR > Frames, S28). One row per frame
-- (`<taxonomy>/<tag>/<unit>/<period>`): the raw response that answers it and when it was fetched
-- (naive UTC). A row younger than 24 h answers without a request; an older one is replaced by the
-- next fetch. Bookkeeping (C2 excludes it by name); frames never write `xbrl_fact`.
CREATE TABLE frames_cache (
    key        VARCHAR PRIMARY KEY,
    fetched_at TIMESTAMP NOT NULL,
    raw_id     BIGINT NOT NULL
);
