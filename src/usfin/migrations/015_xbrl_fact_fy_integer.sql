-- `xbrl_fact.fy` as INTEGER (S27). S21 chose SMALLINT from two companies; the first live
-- `edgar_bulk` run met CIRCOR International (CIK 1091883), whose 2021 10-Q facts carry fy 44561
-- (an Excel serial date for 2021-12-31, the filer's error) — out of SMALLINT range, so the whole
-- member failed. `fy` is stored as filed.
ALTER TABLE xbrl_fact ALTER fy TYPE INTEGER;
