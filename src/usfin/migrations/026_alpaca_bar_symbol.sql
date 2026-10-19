-- What the bars requests have learnt about a symbol (plan alpaca-market-data: How > Bars, S7):
-- one row per (symbol, status), the first raw response that showed it.
--
--   invalid  Alpaca answered 400 {"message":"invalid symbol: X"} for a batch holding it
--            (CUSIP-like `005CVR014`, `BF/B`, `ABC-U`): no request asks for it again
--   history  a request from `backfill_start` (or earlier) with no `end` reached its last page
--            with the symbol in it, so its whole history up to that day was asked, whether or not
--            Alpaca had bars for it: the daily job's catch-up does not ask it in full again
--
-- Filled by the bars parser from the archived responses (the 400 body; the 200 page's URL and
-- its missing next_page_token), so `usfin reparse alpaca_bars` rebuilds it with the bars.
-- Small (one row per universe symbol at most twice); no PK, like the other alpaca tables, the
-- loader inserts a (symbol, status) only when none is stored.
CREATE TABLE alpaca_bar_symbol (
    symbol         VARCHAR NOT NULL,
    status         VARCHAR NOT NULL,
    first_seen_at  TIMESTAMP NOT NULL,
    raw_id         BIGINT NOT NULL
);
