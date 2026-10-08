-- EDGAR filings (plan: How > SEC EDGAR, S20). One row per accession number, from whichever source
-- saw it first (`submissions` per-CIK JSON now; `feed` S22, `index` S23). A later source only fills
-- the NULL columns (accepted_at, report_date, primary_doc), never blanks or overwrites a value.
-- `filed_at` is EDGAR's filing date; `accepted_at` the acceptance time as naive UTC.
CREATE TABLE edgar_filing (
    accession_no VARCHAR NOT NULL PRIMARY KEY,
    cik          BIGINT NOT NULL,
    form         VARCHAR NOT NULL,
    filed_at     DATE NOT NULL,
    accepted_at  TIMESTAMP,
    report_date  DATE,
    primary_doc  VARCHAR,
    is_amendment BOOLEAN NOT NULL,
    source       VARCHAR NOT NULL CHECK (source IN ('feed', 'index', 'submissions'))
);

-- Every CIK a source lists for a filing, with the role it gives: the feed's roles (Issuer,
-- Subject, Filer, Filed by, Reporting; S22), `index` for the daily index (S23), `submissions` for
-- the company whose submissions JSON lists the filing (that JSON names no role).
CREATE TABLE edgar_filing_party (
    accession_no VARCHAR NOT NULL,
    cik          BIGINT NOT NULL,
    role         VARCHAR NOT NULL,
    PRIMARY KEY (accession_no, cik, role)
);

-- The older-filings pages a company's submissions JSON lists (`filings.files[]`), keyed by their
-- listing, not their name: heavy filers' pages are numbered newest first, so a new page renames
-- every older one. `raw_id` is the stored page response parsed for the listing (NULL until one
-- is); the job fetches only listings without one.
CREATE TABLE edgar_submissions_page (
    cik          BIGINT NOT NULL,
    filing_from  DATE NOT NULL,
    filing_to    DATE NOT NULL,
    filing_count BIGINT NOT NULL,
    name         VARCHAR NOT NULL,
    raw_id       BIGINT,
    PRIMARY KEY (cik, filing_from, filing_to, filing_count)
);
