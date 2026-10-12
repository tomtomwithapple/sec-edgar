-- Alpaca corporate actions, versioned (plan alpaca-market-data: How > Corporate actions, S3, A5),
-- from GET https://data.alpaca.markets/v1/corporate-actions?start&end&limit=1000 (all types;
-- `start`/`end` filter `process_date`). The response groups actions by type (`cash_dividends`,
-- `forward_splits`, `name_changes`, `cash_mergers`, ...); `type` is that group's name in the
-- singular (`cash_dividend`, `forward_split`, ...), as Alpaca's `types` parameter spells it.
-- The fields other code reads are promoted to columns (NULL where the type has none); the action
-- object as returned is kept whole in `payload` (canonical JSON text: sorted keys, compact), so
-- fields of other types (sub_type, due bills, source_* of spin-offs and rights, stock_movements,
-- capital-gains rates, partial-call lotteries) are kept too.
--
-- One row per version of an action `id`: an action whose payload equals its newest stored version
-- inserts nothing, a different payload inserts a new version with `first_seen_at` = the raw
-- response's `fetched_at` (naive UTC). No PK (014 pattern; ~6 k actions a month): the loader's
-- set-based anti-join against the newest version per id is the uniqueness rule.
CREATE TABLE alpaca_corp_action (
    id               VARCHAR NOT NULL,
    type             VARCHAR NOT NULL,
    symbol           VARCHAR,
    old_symbol       VARCHAR,
    new_symbol       VARCHAR,
    acquiree_symbol  VARCHAR,
    acquirer_symbol  VARCHAR,
    cusip            VARCHAR,
    old_cusip        VARCHAR,
    new_cusip        VARCHAR,
    acquiree_cusip   VARCHAR,
    acquirer_cusip   VARCHAR,
    ex_date          DATE,
    record_date      DATE,
    payable_date     DATE,
    process_date     DATE NOT NULL,
    effective_date   DATE,
    rate             DOUBLE,
    old_rate         DOUBLE,
    new_rate         DOUBLE,
    cash_rate        DOUBLE,
    acquiree_rate    DOUBLE,
    acquirer_rate    DOUBLE,
    special          BOOLEAN,
    "foreign"        BOOLEAN,
    payload          JSON NOT NULL,
    first_seen_at    TIMESTAMP NOT NULL,
    raw_id           BIGINT NOT NULL
);

-- The newest version of every action id (ties on `first_seen_at`: the later raw response).
CREATE VIEW alpaca_corp_action_latest AS
SELECT * FROM alpaca_corp_action
QUALIFY row_number() OVER (PARTITION BY id ORDER BY first_seen_at DESC, raw_id DESC) = 1;
