-- EDGAR company profile (plan us-fin-ingest: S45): the company metadata at the top of a
-- submissions main document (`CIK##########.json`, per CIK or a bulk submissions.zip member), so
-- the parent can group companies by industry. One row per CIK; pages carry no metadata.
--
-- `sic` is SEC's Standard Industrial Classification code (NULL when SEC lists none: funds, trusts,
-- many foreign filers), `sic_description` its title as SEC writes it, `owner_org` the SEC review
-- office ("06 Technology"). `seen_at` is the `fetched_at` (naive UTC) of the response the row came
-- from, a bulk ZIP's for a member: a row is replaced only by a response seen at the same time or
-- later, so a reparse or an older ZIP never rolls a profile back. `raw_id` is that response.
CREATE TABLE edgar_company_profile (
    cik                    BIGINT NOT NULL PRIMARY KEY,
    name                   VARCHAR,
    sic                    INTEGER,
    sic_description        VARCHAR,
    owner_org              VARCHAR,
    entity_type            VARCHAR,
    category               VARCHAR,
    fiscal_year_end        VARCHAR,
    state_of_incorporation VARCHAR,
    exchanges              VARCHAR[],
    seen_at                TIMESTAMP,
    raw_id                 BIGINT
);

-- Each profile with its SIC hierarchy: `sic_major` the two-digit major group (industry group,
-- e.g. 35 Industrial machinery and computer equipment), `sic_division` the letter and title of
-- the SIC division (A..K, the coarsest level, the nearest SIC has to a sector). Ranges are the
-- 1987 SIC manual's (K = 99 Nonclassifiable). SEC's own non-operating codes 8880, 8888 and 9995
-- (American depositary receipts, foreign governments, non-operating establishments) get division
-- NULL; 6770 (blank checks) stays in H as SEC files it.
CREATE VIEW edgar_company_sic AS
SELECT
    cik,
    name,
    sic,
    sic_description,
    sic // 100 AS sic_major,
    CASE
        WHEN sic BETWEEN 100 AND 999 THEN 'A'
        WHEN sic BETWEEN 1000 AND 1499 THEN 'B'
        WHEN sic BETWEEN 1500 AND 1799 THEN 'C'
        WHEN sic BETWEEN 2000 AND 3999 THEN 'D'
        WHEN sic BETWEEN 4000 AND 4999 THEN 'E'
        WHEN sic BETWEEN 5000 AND 5199 THEN 'F'
        WHEN sic BETWEEN 5200 AND 5999 THEN 'G'
        WHEN sic BETWEEN 6000 AND 6799 THEN 'H'
        WHEN sic BETWEEN 7000 AND 8999 AND sic NOT IN (8880, 8888) THEN 'I'
        WHEN sic BETWEEN 9100 AND 9729 THEN 'J'
        WHEN sic BETWEEN 9900 AND 9999 AND sic <> 9995 THEN 'K'
    END AS sic_division,
    CASE
        WHEN sic BETWEEN 100 AND 999 THEN 'Agriculture, Forestry and Fishing'
        WHEN sic BETWEEN 1000 AND 1499 THEN 'Mining'
        WHEN sic BETWEEN 1500 AND 1799 THEN 'Construction'
        WHEN sic BETWEEN 2000 AND 3999 THEN 'Manufacturing'
        WHEN sic BETWEEN 4000 AND 4999
            THEN 'Transportation, Communications, Electric, Gas and Sanitary Services'
        WHEN sic BETWEEN 5000 AND 5199 THEN 'Wholesale Trade'
        WHEN sic BETWEEN 5200 AND 5999 THEN 'Retail Trade'
        WHEN sic BETWEEN 6000 AND 6799 THEN 'Finance, Insurance and Real Estate'
        WHEN sic BETWEEN 7000 AND 8999 AND sic NOT IN (8880, 8888) THEN 'Services'
        WHEN sic BETWEEN 9100 AND 9729 THEN 'Public Administration'
        WHEN sic BETWEEN 9900 AND 9999 AND sic <> 9995 THEN 'Nonclassifiable'
    END AS sic_division_name,
    owner_org,
    entity_type,
    seen_at
FROM edgar_company_profile;
