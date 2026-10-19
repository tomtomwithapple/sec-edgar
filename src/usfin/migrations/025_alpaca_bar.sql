-- Alpaca daily bars, unadjusted and versioned (plan alpaca-market-data: How > Bars, S6, A2),
-- from GET https://data.alpaca.markets/v2/stocks/bars?timeframe=1Day&feed=sip&adjustment=raw
-- &asof=-&limit=10000 (`asof=-`: what traded under the symbol then, not today's security's whole
-- history). `date` is the New York date of the bar's `t` (04:00Z or 05:00Z, midnight New York);
-- open/high/low/close/volume/trade_count/vwap are the bar's o/h/l/c/v/n/vw.
--
-- One row per version of a (symbol, date): a bar whose compared columns (all seven, exact
-- equality, NULL equal to NULL) equal its newest stored version inserts nothing, a different bar
-- inserts a new version with `first_seen_at` = the raw response's `fetched_at` (naive UTC).
-- No PK (014 pattern: an ART index on ~30 M rows would be ~2 GB); rows load in symbol order and
-- the loader's set-based anti-join against the newest version per (symbol, date) is the
-- uniqueness rule.
CREATE TABLE alpaca_bar (
    symbol         VARCHAR NOT NULL,
    date           DATE NOT NULL,
    open           DOUBLE,
    high           DOUBLE,
    low            DOUBLE,
    close          DOUBLE,
    volume         BIGINT,
    trade_count    BIGINT,
    vwap           DOUBLE,
    first_seen_at  TIMESTAMP NOT NULL,
    raw_id         BIGINT NOT NULL
);

-- The newest version of every (symbol, date) (ties on `first_seen_at`: the later raw response).
CREATE VIEW alpaca_bar_latest AS
SELECT * FROM alpaca_bar
QUALIFY row_number() OVER (
    PARTITION BY symbol, date ORDER BY first_seen_at DESC, raw_id DESC
) = 1;

-- Per (symbol, date), the newest version first seen at or before `ts` (a DATE means 23:59:59
-- New York that day; see bls_as_of_utc).
CREATE MACRO alpaca_bar_as_of(ts) AS TABLE
SELECT * FROM alpaca_bar
WHERE first_seen_at <= bls_as_of_utc(ts)
QUALIFY row_number() OVER (
    PARTITION BY symbol, date ORDER BY first_seen_at DESC, raw_id DESC
) = 1;
