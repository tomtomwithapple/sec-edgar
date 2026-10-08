-- Nasdaq Trader symbols joined to EDGAR companies (plan: How > Nasdaq Trader, S31; C10).
--
-- `ticker_norm(s)` is `usfin.ticker.ticker_norm` in SQL (tests keep the two equal): uppercase,
-- trim, `$` -> `.P`, `-` `/` space -> `.`, then EDGAR's unit / warrant / right suffixes `.UN`,
-- `.WT`, `.RI` at the end take Nasdaq's `.U`, `.W`, `.R`.
CREATE MACRO ticker_norm(s) AS regexp_replace(regexp_replace(regexp_replace(
    translate(replace(upper(trim(s)), '$', '.P'), '-/ ', '...'),
    '\.UN$', '.U'), '\.WT$', '.W'), '\.RI$', '.R');

-- S19 stored `-UN`/`-WT`/`-RI` tickers as `.UN`/`.WT`/`.RI`: rewrite them to the new rule.
-- `updated_at` is kept, so the table equals what a reparse from scratch builds (the key is
-- derived; the upstream values did not change).
UPDATE edgar_company SET ticker_norm = ticker_norm(ticker)
WHERE ticker_norm <> ticker_norm(ticker);

-- One row per open `symbol_history` row (every listed symbol, matched or not), with the active
-- `edgar_company` pair sharing its `ticker_norm`; `cik` NULL = unmatched. Inactive pairs and
-- closed history rows never join.
CREATE VIEW symbol_edgar_match AS
SELECT
    h.symbol,
    h.exchange,
    h.file,
    h.name,
    h.etf,
    ticker_norm(h.symbol) AS ticker_norm,
    e.cik,
    e.ticker AS edgar_ticker,
    e.title AS edgar_title
FROM symbol_history h
LEFT JOIN edgar_company e
    ON e.active AND e.ticker_norm = ticker_norm(h.symbol)
WHERE h.valid_to IS NULL;
