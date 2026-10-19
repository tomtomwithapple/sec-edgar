-- The bars universe (plan alpaca-market-data: How > Universe, S5, A3): every symbol the bars jobs
-- request, one row per (symbol, source) - a symbol reached by several rules has several rows.
--
--   asset_active    an asset on a listed exchange, active in the newest diffed asset list
--   asset_inactive  an asset on a listed exchange, inactive, or gone from the list since
--                   (its last history row; kept so a delisting does not drop its symbol)
--   merger          the acquiree_symbol of a cash, stock or stock-and-cash merger
--   removal         the symbol of a worthless removal
--   renamed         the old_symbol of a name change
--
-- Listed exchanges are config/alpaca.yaml `exchanges` (a view cannot read it; a test keeps the
-- two equal); every other asset is OTC. An action symbol whose only assets are OTC is left out
-- (OTC bars are out of scope); one that is in no asset at all (TWTR) is in. Actions are read in
-- their newest version. A delisted symbol reached by none of the rules is not here: SIVB (its
-- only action is the removal of the OTC symbol SIVBQ; it is the acquirer, not the acquiree, of
-- BPFH) is the documented survivorship gap.
CREATE VIEW alpaca_universe AS
WITH asset AS (
    SELECT symbol, exchange IN ('NYSE', 'NASDAQ', 'AMEX', 'ARCA', 'BATS') AS listed,
           status = 'active' AND valid_to IS NULL AS active
    FROM alpaca_asset_history
    QUALIFY row_number() OVER (PARTITION BY id ORDER BY valid_from DESC) = 1
),
otc_only AS (
    SELECT symbol FROM asset GROUP BY symbol HAVING NOT bool_or(listed)
),
action AS (
    SELECT acquiree_symbol AS symbol, 'merger' AS source FROM alpaca_corp_action_latest
    WHERE type IN ('cash_merger', 'stock_merger', 'stock_and_cash_merger')
    UNION ALL
    SELECT symbol, 'removal' FROM alpaca_corp_action_latest WHERE type = 'worthless_removal'
    UNION ALL
    SELECT old_symbol, 'renamed' FROM alpaca_corp_action_latest WHERE type = 'name_change'
)
SELECT DISTINCT symbol, CASE WHEN active THEN 'asset_active' ELSE 'asset_inactive' END AS source
FROM asset WHERE listed
UNION
SELECT DISTINCT symbol, source FROM action
WHERE symbol IS NOT NULL AND symbol <> '' AND symbol NOT IN (SELECT symbol FROM otc_only);
