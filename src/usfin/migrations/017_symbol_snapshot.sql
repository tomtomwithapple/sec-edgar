-- Nasdaq Trader symbol directory snapshots (plan: How > Nasdaq Trader, S29), from
-- https://www.nasdaqtrader.com/dynamic/SymDir/{nasdaqlisted,otherlisted}.txt. One row per
-- (file, snapshot, symbol); `snapshot_at` is the file's `File Creation Time` trailer (ET) as
-- naive UTC, so a refetch of the same file inserts nothing (C2). Test issues are not stored.
-- Columns differ per file: `exchange` is 'Q' for nasdaqlisted; `market_category` and
-- `financial_status` exist only in nasdaqlisted, `cqs_symbol` and `nasdaq_symbol` only in
-- otherlisted (NULL in the other file). `raw_id` is the first raw response that carried it.
CREATE TABLE symbol_snapshot (
    file             VARCHAR NOT NULL,
    snapshot_at      TIMESTAMP NOT NULL,
    symbol           VARCHAR NOT NULL,
    exchange         VARCHAR NOT NULL,
    name             VARCHAR NOT NULL,
    etf              BOOLEAN,
    round_lot        INTEGER,
    financial_status VARCHAR,
    market_category  VARCHAR,
    cqs_symbol       VARCHAR,
    nasdaq_symbol    VARCHAR,
    raw_id           BIGINT NOT NULL,
    PRIMARY KEY (file, snapshot_at, symbol)
);
