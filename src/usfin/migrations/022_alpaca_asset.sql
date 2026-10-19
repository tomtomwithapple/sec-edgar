-- Alpaca asset list snapshots and their type-2 history (plan alpaca-market-data: How > Assets,
-- S2, A4), from GET https://paper-api.alpaca.markets/v2/assets?asset_class=us_equity (one call,
-- every asset: active and inactive, OTC included). Only the tracked columns are stored; the
-- churning ones (easy_to_borrow, shortable, borrow_status, margin fields, fractionable,
-- attributes) stay in the raw JSON. `snapshot_at` is the raw response's `fetched_at` (the list
-- carries no date). A list identical in every tracked column to the newest stored snapshot is
-- not stored again, so a rerun inserts nothing. No PK (an ART index on ~12 M rows a year of
-- UUIDs buys nothing): a snapshot is written whole, once, by one statement.
CREATE TABLE alpaca_asset_snapshot (
    snapshot_at  TIMESTAMP NOT NULL,
    id           VARCHAR NOT NULL,
    symbol       VARCHAR NOT NULL,
    name         VARCHAR,
    exchange     VARCHAR,
    status       VARCHAR,
    tradable     BOOLEAN,
    class        VARCHAR,
    raw_id       BIGINT NOT NULL
);

-- One row per asset id version: open from `valid_from` until `valid_to` (NULL = in the newest
-- diffed snapshot), both the `snapshot_at` of the diffed snapshots that opened and closed it
-- (half-open). A change in symbol, name, exchange, status, tradable or class closes the row and
-- opens a new one at the same time; an id that leaves the list is closed. Derived entirely from
-- `alpaca_asset_snapshot`, so reparse rebuilds it.
CREATE TABLE alpaca_asset_history (
    id           VARCHAR NOT NULL,
    symbol       VARCHAR NOT NULL,
    name         VARCHAR,
    exchange     VARCHAR,
    status       VARCHAR,
    tradable     BOOLEAN,
    class        VARCHAR,
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP,
    PRIMARY KEY (id, valid_from)
);

-- One row per stored snapshot: its row count against the previous stored snapshot and whether it
-- was diffed into `alpaca_asset_history` (`diffed = false`: outside the row_count_swing gate,
-- C9). The next diffed snapshot diffs against the open history rows, i.e. the last diffed one.
CREATE TABLE alpaca_asset_diff (
    snapshot_at    TIMESTAMP PRIMARY KEY,
    rows           INTEGER NOT NULL,
    previous_at    TIMESTAMP,
    previous_rows  INTEGER,
    pct_change     DOUBLE,
    diffed         BOOLEAN NOT NULL,
    opened         INTEGER NOT NULL,
    closed         INTEGER NOT NULL
);
