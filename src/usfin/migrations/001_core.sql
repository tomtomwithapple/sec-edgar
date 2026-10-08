-- Core bookkeeping tables (plan: How > Core tables).
-- All timestamps are naive UTC.

CREATE SEQUENCE raw_response_id_seq;
CREATE TABLE raw_response (
    id             BIGINT PRIMARY KEY DEFAULT nextval('raw_response_id_seq'),
    source         VARCHAR NOT NULL,
    dataset        VARCHAR NOT NULL,
    url            VARCHAR NOT NULL,
    request_sha256 VARCHAR,
    fetched_at     TIMESTAMP NOT NULL,
    http_status    INTEGER NOT NULL,
    sha256         VARCHAR NOT NULL,
    blob_path      VARCHAR NOT NULL,
    bytes          BIGINT NOT NULL,
    content_type   VARCHAR,
    pruned_at      TIMESTAMP
);

CREATE SEQUENCE job_run_id_seq;
CREATE TABLE job_run (
    id               BIGINT PRIMARY KEY DEFAULT nextval('job_run_id_seq'),
    job              VARCHAR NOT NULL,
    started_at       TIMESTAMP NOT NULL,
    finished_at      TIMESTAMP,
    status           VARCHAR NOT NULL CHECK (status IN ('running', 'ok', 'failed', 'skipped')),
    rows_upserted    BIGINT NOT NULL DEFAULT 0,
    watermark_before JSON,
    watermark_after  JSON,
    error            VARCHAR,
    trigger          VARCHAR NOT NULL
                     CHECK (trigger IN ('scheduled', 'manual', 'catchup', 'chained'))
);

CREATE TABLE watermark (
    job        VARCHAR NOT NULL,
    key        VARCHAR NOT NULL,
    value      VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (job, key)
);

CREATE SEQUENCE alert_id_seq;
CREATE TABLE alert (
    id              BIGINT PRIMARY KEY DEFAULT nextval('alert_id_seq'),
    job             VARCHAR NOT NULL,
    rule            VARCHAR NOT NULL,
    subject         VARCHAR NOT NULL,
    detail          JSON,
    raised_on       DATE NOT NULL,
    raised_at       TIMESTAMP NOT NULL,
    acknowledged_at TIMESTAMP,
    UNIQUE (job, rule, subject, raised_on)
);
