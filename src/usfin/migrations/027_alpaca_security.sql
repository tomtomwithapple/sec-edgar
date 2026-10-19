-- Security identity (plan alpaca-market-data: How > Identity, S9, A7): a price series is read per
-- security, not per symbol. Bars are stored per symbol as Alpaca's `asof=-` returns them, i.e. what
-- traded under the symbol on each day, so one symbol's bars can belong to several securities (FB:
-- Meta to 2022-06-08, the ProShares ETF from 2025-06-26) and one security's bars to several
-- symbols (Meta: FB, then META from 2022-06-09). Views and macros only, over the newest bar
-- versions (`alpaca_bar_latest`'s rule), `alpaca_corp_action_latest` and `alpaca_asset_history`,
-- so `usfin reparse` of the three jobs rebuilds them.

-- How many NYSE sessions without a traded bar split a symbol's history (config/alpaca.yaml
-- `gap_sessions`; a view cannot read it, so the value is written here and a test keeps the two
-- equal).
CREATE MACRO alpaca_gap_sessions() AS 60;

-- NYSE sessions 2016-01-04..2050-12-31 numbered 1, 2, ... (`n`), so the sessions between two
-- dates are a difference of `n`: weekdays less the NYSE holidays of `holidays.NYSE()` (the `nyse`
-- calendar of usfin.calendars, days of mourning included; a test keeps the two equal, overrides
-- of config/calendars.yaml included). Alpaca has a bar on exactly these days (2 713 sessions to
-- 2026-10-16, measured in S9).
CREATE VIEW nyse_session AS
SELECT date, row_number() OVER (ORDER BY date) AS n
FROM (
    SELECT CAST(d AS DATE) AS date
    FROM generate_series(TIMESTAMP '2016-01-01', TIMESTAMP '2050-12-31', INTERVAL 1 DAY) t(d)
)
WHERE isodow(date) <= 5 AND date NOT IN (SELECT unnest([
    '2016-01-01', '2016-01-18', '2016-02-15', '2016-03-25', '2016-05-30', '2016-07-04', '2016-09-05',
    '2016-11-24', '2016-12-26', '2017-01-02', '2017-01-16', '2017-02-20', '2017-04-14', '2017-05-29',
    '2017-07-04', '2017-09-04', '2017-11-23', '2017-12-25', '2018-01-01', '2018-01-15', '2018-02-19',
    '2018-03-30', '2018-05-28', '2018-07-04', '2018-09-03', '2018-11-22', '2018-12-05', '2018-12-25',
    '2019-01-01', '2019-01-21', '2019-02-18', '2019-04-19', '2019-05-27', '2019-07-04', '2019-09-02',
    '2019-11-28', '2019-12-25', '2020-01-01', '2020-01-20', '2020-02-17', '2020-04-10', '2020-05-25',
    '2020-07-03', '2020-09-07', '2020-11-26', '2020-12-25', '2021-01-01', '2021-01-18', '2021-02-15',
    '2021-04-02', '2021-05-31', '2021-07-05', '2021-09-06', '2021-11-25', '2021-12-24', '2022-01-17',
    '2022-02-21', '2022-04-15', '2022-05-30', '2022-06-20', '2022-07-04', '2022-09-05', '2022-11-24',
    '2022-12-26', '2023-01-02', '2023-01-16', '2023-02-20', '2023-04-07', '2023-05-29', '2023-06-19',
    '2023-07-04', '2023-09-04', '2023-11-23', '2023-12-25', '2024-01-01', '2024-01-15', '2024-02-19',
    '2024-03-29', '2024-05-27', '2024-06-19', '2024-07-04', '2024-09-02', '2024-11-28', '2024-12-25',
    '2025-01-01', '2025-01-09', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26', '2025-06-19',
    '2025-07-04', '2025-09-01', '2025-11-27', '2025-12-25', '2026-01-01', '2026-01-19', '2026-02-16',
    '2026-04-03', '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
    '2027-01-01', '2027-01-18', '2027-02-15', '2027-03-26', '2027-05-31', '2027-06-18', '2027-07-05',
    '2027-09-06', '2027-11-25', '2027-12-24', '2028-01-17', '2028-02-21', '2028-04-14', '2028-05-29',
    '2028-06-19', '2028-07-04', '2028-09-04', '2028-11-23', '2028-12-25', '2029-01-01', '2029-01-15',
    '2029-02-19', '2029-03-30', '2029-05-28', '2029-06-19', '2029-07-04', '2029-09-03', '2029-11-22',
    '2029-12-25', '2030-01-01', '2030-01-21', '2030-02-18', '2030-04-19', '2030-05-27', '2030-06-19',
    '2030-07-04', '2030-09-02', '2030-11-28', '2030-12-25', '2031-01-01', '2031-01-20', '2031-02-17',
    '2031-04-11', '2031-05-26', '2031-06-19', '2031-07-04', '2031-09-01', '2031-11-27', '2031-12-25',
    '2032-01-01', '2032-01-19', '2032-02-16', '2032-03-26', '2032-05-31', '2032-06-18', '2032-07-05',
    '2032-09-06', '2032-11-25', '2032-12-24', '2033-01-17', '2033-02-21', '2033-04-15', '2033-05-30',
    '2033-06-20', '2033-07-04', '2033-09-05', '2033-11-24', '2033-12-26', '2034-01-02', '2034-01-16',
    '2034-02-20', '2034-04-07', '2034-05-29', '2034-06-19', '2034-07-04', '2034-09-04', '2034-11-23',
    '2034-12-25', '2035-01-01', '2035-01-15', '2035-02-19', '2035-03-23', '2035-05-28', '2035-06-19',
    '2035-07-04', '2035-09-03', '2035-11-22', '2035-12-25', '2036-01-01', '2036-01-21', '2036-02-18',
    '2036-04-11', '2036-05-26', '2036-06-19', '2036-07-04', '2036-09-01', '2036-11-27', '2036-12-25',
    '2037-01-01', '2037-01-19', '2037-02-16', '2037-04-03', '2037-05-25', '2037-06-19', '2037-07-03',
    '2037-09-07', '2037-11-26', '2037-12-25', '2038-01-01', '2038-01-18', '2038-02-15', '2038-04-23',
    '2038-05-31', '2038-06-18', '2038-07-05', '2038-09-06', '2038-11-25', '2038-12-24', '2039-01-17',
    '2039-02-21', '2039-04-08', '2039-05-30', '2039-06-20', '2039-07-04', '2039-09-05', '2039-11-24',
    '2039-12-26', '2040-01-02', '2040-01-16', '2040-02-20', '2040-03-30', '2040-05-28', '2040-06-19',
    '2040-07-04', '2040-09-03', '2040-11-22', '2040-12-25', '2041-01-01', '2041-01-21', '2041-02-18',
    '2041-04-19', '2041-05-27', '2041-06-19', '2041-07-04', '2041-09-02', '2041-11-28', '2041-12-25',
    '2042-01-01', '2042-01-20', '2042-02-17', '2042-04-04', '2042-05-26', '2042-06-19', '2042-07-04',
    '2042-09-01', '2042-11-27', '2042-12-25', '2043-01-01', '2043-01-19', '2043-02-16', '2043-03-27',
    '2043-05-25', '2043-06-19', '2043-07-03', '2043-09-07', '2043-11-26', '2043-12-25', '2044-01-01',
    '2044-01-18', '2044-02-15', '2044-04-15', '2044-05-30', '2044-06-20', '2044-07-04', '2044-09-05',
    '2044-11-24', '2044-12-26', '2045-01-02', '2045-01-16', '2045-02-20', '2045-04-07', '2045-05-29',
    '2045-06-19', '2045-07-04', '2045-09-04', '2045-11-23', '2045-12-25', '2046-01-01', '2046-01-15',
    '2046-02-19', '2046-03-23', '2046-05-28', '2046-06-19', '2046-07-04', '2046-09-03', '2046-11-22',
    '2046-12-25', '2047-01-01', '2047-01-21', '2047-02-18', '2047-04-12', '2047-05-27', '2047-06-19',
    '2047-07-04', '2047-09-02', '2047-11-28', '2047-12-25', '2048-01-01', '2048-01-20', '2048-02-17',
    '2048-04-03', '2048-05-25', '2048-06-19', '2048-07-03', '2048-09-07', '2048-11-26', '2048-12-25',
    '2049-01-01', '2049-01-18', '2049-02-15', '2049-04-16', '2049-05-31', '2049-06-18', '2049-07-05',
    '2049-09-06', '2049-11-25', '2049-12-24', '2050-01-17', '2050-02-21', '2050-04-08', '2050-05-30',
    '2050-06-20', '2050-07-04', '2050-09-05', '2050-11-24', '2050-12-26'
]::DATE[]));

-- One row per segment: a stretch of one symbol's bars that belongs to one security, from its first
-- bar (`valid_from`) to its last (`valid_to`), both bar dates and inclusive, so a segment's bars
-- are `alpaca_bar_latest` with that symbol and `date BETWEEN valid_from AND valid_to`. Every bar
-- is in exactly one segment. `quiet` marks a segment of untraded padding (4. below).
--
-- 1. Segments. A symbol's bars are split where
--    - a name change's `process_date` falls (for its old and its new symbol: the old symbol's
--      security leaves it that day, the new symbol's arrives), or a merger's `effective_date`
--      (its acquiree) or a worthless removal's `process_date`;
--    - more than alpaca_gap_sessions() NYSE sessions pass without a *traded* bar (volume > 0).
--      Alpaca does not leave a dead symbol empty: under `asof=-` it pads it with flat bars of
--      volume 0 at the last price (META 12.31 from the Roundhill rename on 2022-01-31 to Meta's
--      arrival on 06-09; IBTA 25.35 from the iShares ETF's last trade in 2021 to Ibotta's IPO on
--      2024-04-18), so a gap of missing bars alone would never split those. A run of untraded
--      bars longer than that (`quiet`) is a segment of its own; a shorter one (a thin ETF's quiet
--      days) stays in its segment.
-- 2. Chains. A name change links the old symbol's segment that ends before its `process_date` to
--    the new symbol's segment that starts on or after it (each within alpaca_gap_sessions()
--    sessions of it, neither quiet); linked segments are one security, across any number of
--    renames (FB -> META; META -> METV).
-- 3. Key, per chain: `cusip:<CUSIP>` of the newest action that names one of its segments with a
--    CUSIP (a dividend or split on its symbol, the segment holding the symbol on the ex-date; the
--    old or new CUSIP of a rename; the acquiree or acquirer CUSIP of a merger; a worthless
--    removal); else `asset:<id>` when the chain ends in its symbol's current segment and an asset
--    holds the symbol: the asset ids of one symbol with the same name are one security (IBTA's
--    active and inactive "Ibotta, Inc." ids), the smallest id of the name group with an active id,
--    else of the only name group; else `symbol:<symbol>:<first valid_from>` of the chain.
--    Chains with one CUSIP are one security even with no rename between them.
-- 4. A quiet segment is never chained and never takes a CUSIP or an asset: it keeps its own
--    `symbol:` key (META 2022-01-31..06-08 is `symbol:META:2022-01-31`).
--
-- A symbol reused right after its old security left, with no action, no gap and no quiet stretch
-- between them, cannot be told apart from a price jump and stays one segment.
--
-- `alpaca_security_of(symbols)` computes this over the bars of `symbols` (VARCHAR[]; NULL: every
-- symbol). Its rows for a symbol depend only on the symbols linked to it by renames and shared
-- CUSIPs, so a list closed under those links (what alpaca_security_lookup passes) gives exactly
-- the rows `alpaca_security` has for them, without reading every symbol's bars.
CREATE MACRO alpaca_security_of(symbols) AS TABLE
WITH RECURSIVE
action AS MATERIALIZED (
    SELECT type, symbol, old_symbol, new_symbol, acquiree_symbol, acquirer_symbol, cusip,
           old_cusip, new_cusip, acquiree_cusip, acquirer_cusip, ex_date, process_date,
           effective_date
    FROM alpaca_corp_action_latest
),
-- alpaca_bar_latest's rule, applied after the symbol filter (DuckDB does not push the filter
-- through the view's window, so a lookup would rank every bar of the table)
latest AS (
    SELECT symbol, date, volume FROM alpaca_bar
    WHERE symbols IS NULL OR symbol IN (SELECT unnest(symbols))
    QUALIFY row_number() OVER (
        PARTITION BY symbol, date ORDER BY first_seen_at DESC, raw_id DESC
    ) = 1
),
bar AS (
    SELECT b.symbol, b.date, s.n, coalesce(b.volume, 0) > 0 AS traded
    FROM latest b ASOF LEFT JOIN nyse_session s ON b.date >= s.date
),
run AS (
    SELECT symbol, date, n, traded,
           max(CASE WHEN traded THEN n END) OVER (
               PARTITION BY symbol ORDER BY date
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS traded_before,
           min(CASE WHEN traded THEN n END) OVER (
               PARTITION BY symbol ORDER BY date
               ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
           ) AS traded_after,
           min(n) OVER (PARTITION BY symbol) AS first_n,
           max(n) OVER (PARTITION BY symbol) AS last_n
    FROM bar
),
-- an untraded bar is quiet when the sessions without a traded bar around it (to the symbol's
-- first or last bar at either end) are more than the gap
quiet AS (
    SELECT symbol, date, n,
           NOT traded AND coalesce(traded_after, last_n + 1)
               - coalesce(traded_before, first_n - 1) - 1 > alpaca_gap_sessions() AS quiet
    FROM run
),
cut AS (
    SELECT symbol, date, row_number() OVER (PARTITION BY symbol ORDER BY date) AS k
    FROM (
        SELECT old_symbol AS symbol, process_date AS date
        FROM action WHERE type = 'name_change'
        UNION
        SELECT new_symbol, process_date FROM action WHERE type = 'name_change'
        UNION
        SELECT acquiree_symbol, coalesce(effective_date, process_date)
        FROM action
        WHERE type IN ('cash_merger', 'stock_merger', 'stock_and_cash_merger')
        UNION
        SELECT symbol, process_date
        FROM action WHERE type = 'worthless_removal'
    )
    WHERE symbol IS NOT NULL
),
started AS (
    SELECT q.symbol, q.date, q.n, q.quiet,
           CASE WHEN lag(q.date) OVER w IS NULL
                  OR q.quiet <> lag(q.quiet) OVER w
                  OR coalesce(c.k, 0) <> lag(coalesce(c.k, 0)) OVER w
                  OR q.n - lag(q.n) OVER w - 1 > alpaca_gap_sessions()
                THEN 1 ELSE 0 END AS start
    FROM quiet q ASOF LEFT JOIN cut c ON q.symbol = c.symbol AND q.date >= c.date
    WINDOW w AS (PARTITION BY q.symbol ORDER BY q.date)
),
seg AS MATERIALIZED (
    SELECT symbol, min(date) AS valid_from, max(date) AS valid_to,
           min(n) AS first_n, max(n) AS last_n, bool_or(quiet) AS quiet
    FROM (SELECT *, sum(start) OVER (PARTITION BY symbol ORDER BY date) AS no FROM started)
    GROUP BY symbol, no
),
rename AS (
    SELECT DISTINCT r.old_symbol, r.new_symbol, r.process_date, s.n
    FROM action r ASOF JOIN nyse_session s ON r.process_date <= s.date
    WHERE r.type = 'name_change' AND r.old_symbol IS NOT NULL AND r.new_symbol IS NOT NULL
),
edge AS MATERIALIZED (
    SELECT DISTINCT o.symbol AS old_symbol, o.valid_from AS old_from,
                    w.symbol AS new_symbol, w.valid_from AS new_from
    FROM rename r
    ASOF JOIN seg o ON r.old_symbol = o.symbol AND r.process_date > o.valid_to
    ASOF JOIN seg w ON r.new_symbol = w.symbol AND r.process_date <= w.valid_from
    WHERE NOT o.quiet AND NOT w.quiet
      AND r.n - o.last_n - 1 <= alpaca_gap_sessions()
      AND w.first_n - r.n <= alpaca_gap_sessions()
),
chain(root_symbol, root_from, symbol, valid_from) AS (
    SELECT symbol, valid_from, symbol, valid_from FROM seg
    WHERE NOT EXISTS (
        SELECT 1 FROM edge e WHERE e.new_symbol = seg.symbol AND e.new_from = seg.valid_from
    )
    UNION ALL
    SELECT c.root_symbol, c.root_from, e.new_symbol, e.new_from
    FROM chain c JOIN edge e ON e.old_symbol = c.symbol AND e.old_from = c.valid_from
),
-- a segment reached from two roots (two renames into it) joins the earlier root's chain
member AS MATERIALIZED (
    SELECT * FROM chain
    QUALIFY row_number() OVER (
        PARTITION BY symbol, valid_from ORDER BY root_from, root_symbol
    ) = 1
),
-- (symbol, date, cusip) an action ties to the segment holding the symbol on that date (`next`:
-- to the segment starting on or after it, the new symbol of a rename)
evidence AS (
    SELECT symbol, coalesce(ex_date, process_date) AS date, cusip, false AS next
    FROM action WHERE type <> 'worthless_removal'
    UNION ALL
    SELECT symbol, process_date - 1, cusip, false
    FROM action WHERE type = 'worthless_removal'
    UNION ALL
    SELECT old_symbol, process_date - 1, old_cusip, false
    FROM action WHERE type = 'name_change'
    UNION ALL
    SELECT new_symbol, process_date, new_cusip, true
    FROM action WHERE type = 'name_change'
    UNION ALL
    SELECT acquiree_symbol, coalesce(effective_date, process_date) - 1, acquiree_cusip, false
    FROM action
    WHERE type IN ('cash_merger', 'stock_merger', 'stock_and_cash_merger')
    UNION ALL
    SELECT acquirer_symbol, coalesce(effective_date, process_date), acquirer_cusip, false
    FROM action
    WHERE type IN ('stock_merger', 'stock_and_cash_merger')
),
named AS (
    SELECT s.symbol, s.valid_from, e.date, e.cusip
    FROM (SELECT * FROM evidence WHERE NOT next) e
    ASOF JOIN seg s ON e.symbol = s.symbol AND e.date >= s.valid_from
    WHERE e.cusip <> '' AND NOT s.quiet
    UNION ALL
    SELECT s.symbol, s.valid_from, e.date, e.cusip
    FROM (SELECT * FROM evidence WHERE next) e
    ASOF JOIN nyse_session p ON e.date <= p.date
    ASOF JOIN seg s ON e.symbol = s.symbol AND e.date <= s.valid_from
    WHERE e.cusip <> '' AND NOT s.quiet AND s.first_n - p.n <= alpaca_gap_sessions()
),
chain_cusip AS (
    SELECT m.root_symbol, m.root_from, max_by(n.cusip, strftime(n.date, '%Y-%m-%d') || n.cusip)
           AS cusip
    FROM named n JOIN member m USING (symbol, valid_from)
    GROUP BY m.root_symbol, m.root_from
),
asset AS (
    SELECT id, symbol, name, status = 'active' AND valid_to IS NULL AS active
    FROM alpaca_asset_history
    QUALIFY row_number() OVER (PARTITION BY id ORDER BY valid_from DESC) = 1
),
asset_group AS (
    SELECT symbol, name, min(id) AS id, bool_or(active) AS active
    FROM asset GROUP BY symbol, name
),
asset_key AS (
    SELECT symbol,
           coalesce(min(id) FILTER (WHERE active), CASE WHEN count(*) = 1 THEN min(id) END) AS id
    FROM asset_group GROUP BY symbol
),
-- the segment a symbol names today: its newest one that is not quiet (else its newest)
current AS (
    SELECT symbol, valid_from FROM seg
    QUALIFY row_number() OVER (PARTITION BY symbol ORDER BY quiet, valid_from DESC) = 1
),
chain_asset AS (
    SELECT l.root_symbol, l.root_from, k.id
    FROM (
        SELECT root_symbol, root_from, max_by(symbol, valid_from) AS symbol,
               max(valid_from) AS valid_from
        FROM member GROUP BY root_symbol, root_from
    ) l
    JOIN current c USING (symbol, valid_from)
    JOIN seg s USING (symbol, valid_from)
    JOIN asset_key k USING (symbol)
    WHERE NOT s.quiet AND k.id IS NOT NULL
)
SELECT coalesce('cusip:' || cc.cusip, 'asset:' || ca.id,
                'symbol:' || m.root_symbol || ':' || strftime(m.root_from, '%Y-%m-%d'))
           AS security_key,
       m.symbol, m.valid_from, s.valid_to, s.quiet
FROM member m
JOIN seg s USING (symbol, valid_from)
LEFT JOIN chain_cusip cc USING (root_symbol, root_from)
LEFT JOIN chain_asset ca USING (root_symbol, root_from);

CREATE VIEW alpaca_security AS
SELECT * FROM alpaca_security_of(NULL::VARCHAR[]);

-- The security each symbol names today: the key of its newest segment that is not quiet (else of
-- its newest), so a delisted symbol names its last security (TWTR) and a reused one its newest
-- (FB: the ProShares ETF, not Meta).
CREATE VIEW alpaca_security_symbol AS
SELECT symbol, security_key FROM alpaca_security
QUALIFY row_number() OVER (PARTITION BY symbol ORDER BY quiet, valid_from DESC) = 1;

-- Lookup by today's symbol: every segment of the security `sym` names today, all its symbols
-- included. Its bars: JOIN alpaca_bar_latest b ON b.symbol = s.symbol AND b.date BETWEEN
-- s.valid_from AND s.valid_to (one bar a date while the security's segments do not overlap,
-- which only one CUSIP named under two symbols at once could break; none did in S9's live data).
-- Only the symbols linked to `sym` through renames and shared CUSIPs (found from the actions
-- alone) have their bars read: ~0.2 s, against ~0.5 s for the whole view on 956 k bars.
CREATE MACRO alpaca_security_lookup(sym) AS TABLE
WITH RECURSIVE
action AS MATERIALIZED (SELECT * FROM alpaca_corp_action_latest),
named AS (
    SELECT symbol, cusip FROM action
    UNION SELECT old_symbol, old_cusip FROM action
    UNION SELECT new_symbol, new_cusip FROM action
    UNION SELECT acquiree_symbol, acquiree_cusip FROM action
    UNION SELECT acquirer_symbol, acquirer_cusip FROM action
),
link(a, b) AS (
    SELECT x.symbol, y.symbol FROM named x JOIN named y USING (cusip)
    WHERE x.symbol <> y.symbol AND cusip <> ''
    UNION SELECT old_symbol, new_symbol FROM action WHERE type = 'name_change'
    UNION SELECT new_symbol, old_symbol FROM action WHERE type = 'name_change'
),
linked(symbol) AS (
    SELECT sym
    UNION
    SELECT l.b FROM linked k JOIN link l ON l.a = k.symbol WHERE l.b IS NOT NULL
),
s AS MATERIALIZED (SELECT * FROM alpaca_security_of((SELECT list(symbol) FROM linked)))
SELECT * FROM s
WHERE security_key = (
    SELECT security_key FROM s WHERE symbol = sym
    QUALIFY row_number() OVER (ORDER BY quiet, valid_from DESC) = 1
);
