-- EDGAR ticker map (plan: How > SEC EDGAR, S19), from https://www.sec.gov/files/company_tickers.json.
-- One row per (cik, ticker) ever listed. `ticker_norm` is `usfin.ticker.ticker_norm(ticker)`, the
-- join key to Nasdaq Trader symbols (S31). A pair missing from the latest file is kept with
-- `active = false`, never deleted. `updated_at` is the `fetched_at` of the file that last changed
-- a value of the row (title, ticker_norm, active), so a refetch or a reparse changes nothing.
CREATE TABLE edgar_company (
    cik         BIGINT NOT NULL,
    ticker      VARCHAR NOT NULL,
    title       VARCHAR NOT NULL,
    ticker_norm VARCHAR NOT NULL,
    active      BOOLEAN NOT NULL,
    updated_at  TIMESTAMP NOT NULL,
    PRIMARY KEY (cik, ticker)
);
