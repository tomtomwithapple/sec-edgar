-- Type-2 symbol history diffed from `symbol_snapshot` (plan: How > Nasdaq Trader, S30).
-- One row per (symbol, exchange) version: open from `valid_from` until `valid_to` (NULL = still
-- listed), both the `snapshot_at` of the diffed snapshots that opened and closed it (half-open
-- [valid_from, valid_to)). A changed name, ETF flag, market category or financial status closes
-- the row and opens a new one at the same time; round lot and the CQS / NASDAQ spellings are
-- not tracked. A symbol that changes exchange or file is closed under the old exchange and
-- opened under the new one. Derived entirely from `symbol_snapshot`, so reparse rebuilds it.
CREATE TABLE symbol_history (
    symbol           VARCHAR NOT NULL,
    exchange         VARCHAR NOT NULL,
    valid_from       TIMESTAMP NOT NULL,
    valid_to         TIMESTAMP,
    file             VARCHAR NOT NULL,
    name             VARCHAR NOT NULL,
    etf              BOOLEAN,
    market_category  VARCHAR,
    financial_status VARCHAR,
    PRIMARY KEY (symbol, exchange, valid_from)
);

-- One row per stored snapshot: its row count against the file's previous snapshot and whether
-- it was diffed into `symbol_history` (`diffed = false`: outside the row_count_swing gate, C9).
-- The next diffed snapshot diffs against the open history rows, i.e. the last diffed snapshot.
CREATE TABLE symbol_diff (
    file           VARCHAR NOT NULL,
    snapshot_at    TIMESTAMP NOT NULL,
    rows           INTEGER NOT NULL,
    previous_at    TIMESTAMP,
    previous_rows  INTEGER,
    pct_change     DOUBLE,
    diffed         BOOLEAN NOT NULL,
    opened         INTEGER NOT NULL,
    closed         INTEGER NOT NULL,
    PRIMARY KEY (file, snapshot_at)
);
