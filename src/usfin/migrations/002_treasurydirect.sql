-- TreasuryDirect securities (plan: How > TreasuryDirect). One row per (cusip, auction_date):
-- typed columns for the fields other jobs use, the full upstream record in `payload`.
CREATE TABLE tsy_security (
    cusip                   VARCHAR NOT NULL,
    auction_date            DATE NOT NULL,
    security_type           VARCHAR,
    security_term           VARCHAR,
    offering_amt            DECIMAL(24,4),
    high_yield              DOUBLE,
    high_discount_rate      DOUBLE,
    high_investment_rate    DOUBLE,
    high_discount_margin    DOUBLE,
    price_per100            DOUBLE,
    bid_to_cover            DOUBLE,
    primary_dealer_accepted DECIMAL(24,4),
    indirect_accepted       DECIMAL(24,4),
    direct_accepted         DECIMAL(24,4),
    issue_date              DATE,
    maturity_date           DATE,
    status                  VARCHAR NOT NULL CHECK (status IN ('announced', 'auctioned')),
    payload                 JSON NOT NULL,
    PRIMARY KEY (cusip, auction_date)
);
