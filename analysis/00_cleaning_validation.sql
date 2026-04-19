-- 00_cleaning_validation.sql
-- --------------------------
-- Validates the responses_clean view produced by cleaning.py.
-- Run this after cleaning.py to confirm all normalization logic is correct.
-- Each block answers a specific data quality question.


-- ============================================================
-- 1. ROW COUNTS BY YEAR
--    Confirm no rows were lost vs the raw responses table.
-- ============================================================
SELECT
    r.survey_year,
    COUNT(*) AS raw_rows,
    COUNT(c.response_id) AS clean_rows
FROM responses r
LEFT JOIN responses_clean c USING (response_id, survey_year)
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- 2. SALARY OUTLIER REMOVAL
--    Show how many rows were nulled out per year.
--    Expect small numbers (hundreds) — large numbers suggest
--    the $1k-$1M threshold needs revisiting.
-- ============================================================
SELECT
    survey_year,
    COUNT(salary_usd_yearly)       AS raw_with_salary,
    COUNT(salary_usd_yearly_clean) AS clean_with_salary,
    raw_with_salary
        - clean_with_salary        AS outliers_removed,
    ROUND(
        100.0 * (raw_with_salary - clean_with_salary)
        / NULLIF(raw_with_salary, 0), 2
    )                              AS pct_removed
FROM responses_clean
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- 3. REMOTE WORK NORMALIZATION
--    Check no values fell through to NULL unexpectedly.
--    The 'Unmatched raw values' query shows what we missed.
-- ============================================================
SELECT
    survey_year,
    remote_work_clean,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY survey_year), 1) AS pct_of_year
FROM responses_clean
WHERE remote_work IS NOT NULL        -- only rows where the question was asked
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- Values in remote_work that did NOT match any CASE branch
SELECT
    survey_year,
    remote_work AS unmatched_raw_value,
    COUNT(*) AS n
FROM responses_clean
WHERE remote_work IS NOT NULL
  AND remote_work_clean IS NULL
GROUP BY 1, 2
ORDER BY 3 DESC;


-- ============================================================
-- 4. EDUCATION LEVEL NORMALIZATION
--    Verify the 5-tier collapse looks proportional.
--    A NULL here means a raw value didn't match any branch.
-- ============================================================
SELECT
    ed_level_clean,
    COUNT(*)                                             AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS pct_total
FROM responses_clean
GROUP BY 1
ORDER BY 2 DESC;

-- Raw ed_level values that fell through to NULL
SELECT
    ed_level AS unmatched_raw_value,
    COUNT(*) AS n
FROM responses_clean
WHERE ed_level IS NOT NULL
  AND ed_level_clean IS NULL
GROUP BY 1
ORDER BY 2 DESC;


-- ============================================================
-- 5. EXPERIENCE BRACKET DISTRIBUTION
--    ORDER BY uses MIN(experience_years) to get natural order,
--    not alphabetical. Expect a right-skewed distribution.
-- ============================================================
SELECT
    exp_bracket,
    COUNT(*)                                             AS n,
    ROUND(AVG(experience_years), 1)                     AS avg_yrs,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS pct_total
FROM responses_clean
WHERE exp_bracket IS NOT NULL
GROUP BY 1
ORDER BY MIN(experience_years);


-- ============================================================
-- 6. EXPERIENCE SOURCE: years_code_pro vs work_exp
--    In 2025 years_code_pro is NULL for all rows — work_exp
--    should cover all non-NULL experience_years for that year.
-- ============================================================
SELECT
    survey_year,
    COUNT(years_code_pro)  AS has_years_code_pro,
    COUNT(work_exp)        AS has_work_exp,
    COUNT(experience_years) AS has_experience_years
FROM responses_clean
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- 7. AI USER FLAG
--    Expect NULL for 2021-2022, increasing TRUE rate 2023-2025.
-- ============================================================
SELECT
    survey_year,
    ai_user,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY survey_year), 1) AS pct_of_year
FROM responses_clean
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================
-- 8. EMPLOYMENT TYPE NORMALIZATION
--    'Other' bucket should be small — if it's large, add more
--    ILIKE branches in cleaning.py.
-- ============================================================
SELECT
    employment_clean,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_total
FROM responses_clean
GROUP BY 1
ORDER BY 2 DESC;

-- Raw employment values landing in 'Other'
SELECT
    employment AS raw_value,
    COUNT(*) AS n
FROM responses_clean
WHERE employment_clean = 'Other'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;


-- ============================================================
-- 9. ORG SIZE SORT KEY COVERAGE
--    NULL org_size_order means a raw org_size value wasn't
--    matched in the CASE. Should be near zero.
-- ============================================================
SELECT
    org_size         AS raw_value,
    org_size_order,
    COUNT(*)         AS n
FROM responses_clean
WHERE org_size IS NOT NULL
GROUP BY 1, 2
ORDER BY org_size_order NULLS LAST;