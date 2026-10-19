-- Adjusted prices (plan alpaca-market-data: How > Adjusted prices, S10, A6): per security (A7) and
-- date, the unadjusted close, the split-adjusted and total-return factors and the adjusted close,
-- back-adjusted to today the way Alpaca's own `adjustment=all` is (S10 compared them live: AAPL,
-- MSFT and KO within half a cent, i.e. Alpaca's rounding to the cent, on every day from 2016; at
-- most 0.023 %). Views and macros only, over the newest bar versions (`alpaca_bar_latest`'s rule),
-- `alpaca_corp_action_latest`'s rule and `alpaca_security`, so `usfin reparse` of the bars and
-- corporate-action jobs rebuilds them.
--
-- An action counts for the security whose segment holds its symbol on its `ex_date` (a bar date of
-- that segment or a day between two of them): so a dividend paid under FB in 2019 adjusts Meta, one
-- paid under FB in 2025 adjusts the ProShares ETF, and one paid under META in 2021 the Roundhill
-- ETF (now METV). An `ex_date` after the segment's last bar (announced, not yet traded: the
-- forward window keeps ~3 months of them), in a gap between segments, or in a quiet segment adjusts
-- nothing. Factors then multiply backward over the whole security, across its symbols, so FB-era
-- bars carry Meta's dividends paid under META.
--
-- - Split (`forward_split`, `reverse_split`): `old_rate / new_rate` for every bar before
--   `ex_date` (AAPL 4:1 on 2020-08-31: 1/4; CTNT 1:150 on 2026-09-28: 150).
-- - Cash dividend: `1 - rate / close`, `close` the security's last bar *before* `ex_date` (not on
--   it), raw like the rate, for every bar before `ex_date`. Special and foreign dividends count at
--   their stated rate. One amount on one `ex_date` counts once: Alpaca sometimes lists the same
--   dividend under two ids (two CUSIPs, or a special and a regular copy; 662 (symbol, ex_date, rate)
--   repeats in S10's live store); different amounts on one day all count. A dividend with no bar
--   before it, or not below that close (a data error; a factor <= 0), adjusts nothing.
-- - Spin-offs, rights, mergers, stock dividends, unit splits, capital-gains distributions and the
--   other types are not applied (plan: Out of scope).
--
-- Columns: `split_factor` = product of the split factors after the bar; `tr_factor` = that times
-- the dividend factors after it (the total-return factor); `adj_close = close * tr_factor`, the
-- price `adjustment=all` reports; `close * split_factor` is `adjustment=split`. Both factors are 1
-- on and after the last action.

-- `alpaca_bar_adjusted_lookup(sym)`: the adjusted bars of the security `sym` names today
-- (alpaca_security_lookup's answer, all its symbols included), one row a date; NULL: every
-- security (the view below). A symbol reads only its linked symbols' bars and actions (0.14 s on
-- S10's 962 k bars, 0.17-0.21 s measured on 22 M), because the whole view ranks every bar of the
-- table (DuckDB does not push a symbol filter through alpaca_bar_latest's window). The branch for
-- the other case is pruned when `sym` is a constant or a bound parameter, so a lookup never
-- builds the whole security view.
CREATE MACRO alpaca_bar_adjusted_lookup(sym) AS TABLE
WITH
seg AS MATERIALIZED (
    SELECT security_key, symbol, valid_from, valid_to, quiet FROM alpaca_security
    WHERE sym IS NULL
    UNION ALL
    SELECT security_key, symbol, valid_from, valid_to, quiet FROM alpaca_security_lookup(sym)
    WHERE sym IS NOT NULL
),
-- alpaca_bar_latest's rule, applied after the symbol filter
latest AS (
    SELECT symbol, date, close FROM alpaca_bar
    WHERE symbol IN (SELECT symbol FROM seg)
    QUALIFY row_number() OVER (
        PARTITION BY symbol, date ORDER BY first_seen_at DESC, raw_id DESC
    ) = 1
),
bar AS MATERIALIZED (
    SELECT g.security_key, b.symbol, b.date, b.close
    FROM latest b
    JOIN seg g ON b.symbol = g.symbol AND b.date BETWEEN g.valid_from AND g.valid_to
),
-- alpaca_corp_action_latest's rule, applied after the type and symbol filter; the segment holding
-- the symbol on the ex-date (the newest starting on or before it, if it reaches the ex-date)
action AS (
    SELECT DISTINCT g.security_key, a.type, a.ex_date, a.rate, a.old_rate, a.new_rate
    FROM (
        SELECT * FROM alpaca_corp_action
        WHERE type IN ('forward_split', 'reverse_split', 'cash_dividend')
          AND symbol IN (SELECT symbol FROM seg)
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY first_seen_at DESC, raw_id DESC) = 1
    ) a
    ASOF JOIN seg g ON a.symbol = g.symbol AND a.ex_date >= g.valid_from
    WHERE a.ex_date <= g.valid_to AND NOT g.quiet
),
event AS (
    SELECT a.security_key, a.ex_date,
           CASE WHEN a.type = 'cash_dividend' THEN 1.0 ELSE a.old_rate / a.new_rate END AS split,
           CASE WHEN a.type = 'cash_dividend' THEN 1 - a.rate / p.close ELSE 1.0 END AS dividend
    FROM action a
    ASOF LEFT JOIN bar p ON a.security_key = p.security_key AND a.ex_date > p.date
),
-- one factor per (security, ex_date); then cumulated from the newest ex-date backward
day AS (
    SELECT security_key, ex_date, product(split) AS split, product(split * dividend) AS total
    FROM event WHERE split > 0 AND dividend > 0
    GROUP BY security_key, ex_date
),
cum AS (
    SELECT security_key, ex_date,
           product(split) OVER w AS split_factor, product(total) OVER w AS tr_factor
    FROM day
    WINDOW w AS (
        PARTITION BY security_key ORDER BY ex_date DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)
-- each bar takes the factor of the first ex-date after it
SELECT b.security_key, b.date, b.symbol, b.close,
       b.close * coalesce(c.tr_factor, 1) AS adj_close,
       coalesce(c.split_factor, 1) AS split_factor,
       coalesce(c.tr_factor, 1) AS tr_factor
FROM bar b
ASOF LEFT JOIN cum c ON b.security_key = c.security_key AND b.date < c.ex_date;

-- Every security's adjusted bars (A6's `alpaca_bar_adjusted`): 0.8 s on S10's 962 k bars, ~16 s
-- measured on 22 M; read one security through alpaca_bar_adjusted_lookup.
CREATE VIEW alpaca_bar_adjusted AS
SELECT * FROM alpaca_bar_adjusted_lookup(NULL::VARCHAR);
