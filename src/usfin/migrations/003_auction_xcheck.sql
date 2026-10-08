-- Fiscal Data vs TreasuryDirect auction cross-check (plan: How > TreasuryDirect, C15). One row
-- per (cusip, auction_date) in the two sources' overlap, rebuilt by job `auction_xcheck`:
-- matched | mismatch (a result or the offering amount differs) | pending (a field set on one
-- side only, e.g. results not yet published by Fiscal Data) | fd_only | td_only.
CREATE TABLE auction_xcheck (
    cusip         VARCHAR NOT NULL,
    auction_date  DATE NOT NULL,
    result        VARCHAR NOT NULL
                  CHECK (result IN ('matched', 'mismatch', 'pending', 'fd_only', 'td_only')),
    security_type VARCHAR,
    diff          JSON,
    PRIMARY KEY (cusip, auction_date)
);
