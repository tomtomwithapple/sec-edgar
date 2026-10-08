-- Form 4 ownership documents (plan: How > SEC EDGAR > Form 4, S24; C13). Filled by job
-- `edgar_form4` from the filing's ownership XML; every row of an accession comes from that one
-- document, so a refetch that changed it replaces the accession's rows in all four tables.
-- An amendment (4/A) is its own accession: its rows stand beside the original's, and
-- `insider_filing.original_filed` carries the XML's dateOfOriginalSubmission.
CREATE TABLE insider_filing (
    accession_no     VARCHAR PRIMARY KEY,
    form             VARCHAR NOT NULL,          -- documentType: 4 or 4/A
    issuer_cik       BIGINT NOT NULL,           -- from the XML, never the queued CIK
    issuer_name      VARCHAR,
    issuer_ticker    VARCHAR,
    period_of_report DATE,
    original_filed   DATE,                      -- 4/A only
    document         VARCHAR NOT NULL,          -- the XML's file name in the filing folder
    owners           SMALLINT NOT NULL,
    lines            INTEGER NOT NULL
);

-- Every reporting owner, in document order (a joint filing names several; their transactions
-- are not split between them).
CREATE TABLE insider_owner (
    accession_no     VARCHAR NOT NULL,
    owner_seq        SMALLINT NOT NULL,
    owner_cik        BIGINT NOT NULL,
    owner_name       VARCHAR,
    is_director      BOOLEAN NOT NULL,
    is_officer       BOOLEAN NOT NULL,
    is_ten_pct_owner BOOLEAN NOT NULL,
    is_other         BOOLEAN NOT NULL,
    officer_title    VARCHAR,
    other_text       VARCHAR,
    PRIMARY KEY (accession_no, owner_seq)
);

-- One row per line of the non-derivative table, then the derivative table, in document order
-- (`line_no` from 1); holdings (no transaction) have `is_holding` and NULL transaction columns.
-- `owner_cik`/`owner_name` are the first reporting owner (the rest in `insider_owner`). A value
-- given only as a footnote is NULL; `footnotes` maps each column to its footnote ids (JSON).
CREATE TABLE insider_txn (
    accession_no        VARCHAR NOT NULL,
    line_no             INTEGER NOT NULL,
    issuer_cik          BIGINT NOT NULL,
    owner_cik           BIGINT NOT NULL,
    owner_name          VARCHAR,
    is_derivative       BOOLEAN NOT NULL,
    is_holding          BOOLEAN NOT NULL,
    security_title      VARCHAR,
    txn_date            DATE,
    txn_code            VARCHAR,
    shares              DOUBLE,
    price               DOUBLE,
    acquired_disposed   VARCHAR,
    shares_owned_after  DOUBLE,
    direct_indirect     VARCHAR,
    nature_of_ownership VARCHAR,
    exercise_price      DOUBLE,
    exercise_date       DATE,
    expiration_date     DATE,
    underlying_title    VARCHAR,
    underlying_shares   DOUBLE,
    footnotes           JSON,
    PRIMARY KEY (accession_no, line_no)
);

CREATE TABLE insider_footnote (
    accession_no VARCHAR NOT NULL,
    footnote_id  VARCHAR NOT NULL,
    text         VARCHAR,
    PRIMARY KEY (accession_no, footnote_id)
);
