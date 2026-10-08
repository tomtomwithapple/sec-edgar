-- Filing documents queued for the per-CIK and per-filing jobs (plan: How > SEC EDGAR, S22).
-- The feed (S22) and the daily-index reconcile (S23) enqueue; the job of each kind drains it:
-- `submissions` / `companyfacts` (edgar_submissions / edgar_companyfacts, one request per CIK),
-- `form4` (S24), `sched13` (S25). `enqueued_at` is the fetched_at of the response that queued it;
-- `done_at` is set by the draining job. Bookkeeping, not normalized data (C2 excludes it).
CREATE TABLE pending_doc (
    accession_no VARCHAR NOT NULL,
    kind         VARCHAR NOT NULL CHECK (kind IN ('submissions', 'companyfacts', 'form4', 'sched13')),
    cik          BIGINT NOT NULL,
    enqueued_at  TIMESTAMP NOT NULL,
    done_at      TIMESTAMP,
    PRIMARY KEY (accession_no, kind)
);
