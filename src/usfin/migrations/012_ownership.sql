-- Schedule 13D/G beneficial-ownership filings (plan: How > SEC EDGAR > Schedule 13D/G, S25; C13).
-- Filled by job `edgar_sched13` from the filing's structured `primary_doc.xml` (required since
-- 2024-12-18); every row of an accession comes from that one document, so a refetch that changed
-- it replaces the accession's rows in both tables. An amendment (/A) is its own accession.
-- A legacy filing (form `SC 13D`, `SC 13G`, +/A: HTML or text, no XML) is skipped and counted:
-- one `ownership_filing` row with `legacy` true, no persons.
CREATE TABLE ownership_filing (
    accession_no   VARCHAR PRIMARY KEY,
    form           VARCHAR NOT NULL,       -- submissionType, or the legacy form name
    legacy         BOOLEAN NOT NULL,
    subject_cik    BIGINT,                 -- issuerCik / issuerCIK from the XML (NULL if legacy)
    subject_name   VARCHAR,
    subject_cusips VARCHAR,                -- issuerCusipNumber values, comma-joined
    class_title    VARCHAR,
    event_date     DATE,
    amendment_no   INTEGER,
    filer_cik      BIGINT,                 -- headerData filer credentials (the filing login)
    filed_at       DATE,                   -- edgar_filing.filed_at
    document       VARCHAR,                -- the XML's file name (NULL if legacy)
    persons        SMALLINT NOT NULL
);

-- One row per reporting person on the cover pages, in document order (`person_seq` from 1).
-- A person may have no CIK: Schedule 13G cover pages carry none at all, a 13D person may be
-- `reportingPersonNoCIK` Y. `form`, `subject_cik`, `event_date`, `filed_at` repeat the filing's.
CREATE TABLE ownership_stake (
    accession_no       VARCHAR NOT NULL,
    person_seq         SMALLINT NOT NULL,
    filer_cik          BIGINT,
    filer_name         VARCHAR,
    subject_cik        BIGINT NOT NULL,
    form               VARCHAR NOT NULL,
    event_date         DATE,
    percent_of_class   DOUBLE,
    shares             DOUBLE,             -- aggregate amount beneficially owned
    filed_at           DATE,
    sole_voting        DOUBLE,
    shared_voting      DOUBLE,
    sole_dispositive   DOUBLE,
    shared_dispositive DOUBLE,
    member_of_group    VARCHAR,            -- a / b as filed
    citizenship        VARCHAR,
    person_type        VARCHAR,            -- typeOfReportingPerson codes, comma-joined
    PRIMARY KEY (accession_no, person_seq)
);
