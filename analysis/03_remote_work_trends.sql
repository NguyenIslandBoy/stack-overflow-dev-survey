-- 03_remote_work_trends.sql
-- -------------------------
-- Analyses remote work adoption and its relationship with salary
-- and developer type across 2022-2025.
-- (2021 excluded: RemoteWork question not present in that year's survey.)
--
-- Questions answered:
--   1. How has the Remote / Hybrid / In-person split shifted 2022-2025?
--   2. Does remote work correlate with higher or lower salary?
--   3. Which developer types are most vs. least remote?
--   4. How does the remote/in-person salary gap change over time?
--
-- Window functions used:
--   LAG()               — YoY shift in remote work share per category
--   PERCENTILE_CONT()   — median salary by work arrangement
--   RANK()              — rank dev types by remote adoption rate


-- ============================================================
-- QUERY A: Remote work share by year
--          Shows the overall Remote/Hybrid/In-person split
--          and YoY percentage point shift per category.
-- ============================================================
WITH remote_base AS (
    SELECT
        survey_year,
        remote_work_clean,
        COUNT(*) AS n
    FROM responses_clean
    WHERE remote_work_clean IS NOT NULL
      AND employment_clean IN ('Employed FT', 'Self-employed')
    GROUP BY survey_year, remote_work_clean
),

with_totals AS (
    SELECT
        survey_year,
        remote_work_clean,
        n,
        SUM(n) OVER (PARTITION BY survey_year)          AS year_total,
        ROUND(100.0 * n / SUM(n) OVER (PARTITION BY survey_year), 1) AS pct_of_year
    FROM remote_base
),

with_lag AS (
    SELECT
        survey_year,
        remote_work_clean,
        n,
        year_total,
        pct_of_year,
        LAG(pct_of_year) OVER (
            PARTITION BY remote_work_clean
            ORDER BY survey_year
        )                                               AS prev_year_pct,
        ROUND(
            pct_of_year -
            LAG(pct_of_year) OVER (
                PARTITION BY remote_work_clean
                ORDER BY survey_year
            )
        , 1)                                            AS yoy_change_pp
    FROM with_totals
)

SELECT
    survey_year,
    remote_work_clean,
    n,
    pct_of_year,
    prev_year_pct,
    yoy_change_pp
FROM with_lag
ORDER BY survey_year, pct_of_year DESC;


-- ============================================================
-- QUERY B: Salary by remote work arrangement per year
--          Does working remotely actually pay more?
--          Controlled to full-time employed only to avoid
--          mixing in self-employed/contractor rate variation.
-- ============================================================
WITH salary_remote AS (
    SELECT
        survey_year,
        remote_work_clean,
        salary_usd_yearly_clean AS salary
    FROM responses_clean
    WHERE salary_usd_yearly_clean IS NOT NULL
      AND remote_work_clean IS NOT NULL
      AND employment_clean = 'Employed FT'
),

stats AS (
    SELECT
        survey_year,
        remote_work_clean,
        COUNT(*)                                                            AS n,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary))        AS p25,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary))        AS median_salary,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary))        AS p75
    FROM salary_remote
    GROUP BY survey_year, remote_work_clean
)

SELECT
    survey_year,
    remote_work_clean,
    n,
    p25,
    median_salary,
    p75,
    -- Salary gap vs in-person workers in same year
    ROUND(
        median_salary -
        FIRST_VALUE(median_salary) OVER (
            PARTITION BY survey_year
            ORDER BY CASE remote_work_clean
                WHEN 'In-person' THEN 1
                WHEN 'Hybrid'    THEN 2
                WHEN 'Remote'    THEN 3
            END
        )
    )                                                                       AS gap_vs_inperson,
    LAG(median_salary) OVER (
        PARTITION BY remote_work_clean
        ORDER BY survey_year
    )                                                                       AS prev_year_median,
    ROUND(
        median_salary -
        LAG(median_salary) OVER (
            PARTITION BY remote_work_clean
            ORDER BY survey_year
        )
    )                                                                       AS yoy_salary_change
FROM stats
ORDER BY survey_year, median_salary DESC;


-- ============================================================
-- QUERY C: Remote work rate by developer type (2022-2025)
--          Which roles went most remote? Which stayed in-office?
--          Uses RANK() to surface most and least remote dev types.
-- ============================================================
WITH devtype_remote AS (
    SELECT
        survey_year,
        dev_type_primary,
        remote_work_clean,
        COUNT(*) AS n
    FROM responses_clean
    WHERE remote_work_clean IS NOT NULL
      AND dev_type_primary IS NOT NULL
      AND dev_type_primary != ''
      AND employment_clean IN ('Employed FT', 'Self-employed')
    GROUP BY survey_year, dev_type_primary, remote_work_clean
),

devtype_totals AS (
    SELECT
        survey_year,
        dev_type_primary,
        SUM(n) AS total_n,
        SUM(CASE WHEN remote_work_clean = 'Remote' THEN n ELSE 0 END)    AS remote_n,
        SUM(CASE WHEN remote_work_clean = 'Hybrid' THEN n ELSE 0 END)    AS hybrid_n,
        SUM(CASE WHEN remote_work_clean = 'In-person' THEN n ELSE 0 END) AS inperson_n
    FROM devtype_remote
    GROUP BY survey_year, dev_type_primary
    HAVING SUM(n) >= 100        -- minimum sample per dev type per year
)

SELECT
    survey_year,
    dev_type_primary,
    total_n,
    ROUND(100.0 * remote_n   / total_n, 1) AS pct_remote,
    ROUND(100.0 * hybrid_n   / total_n, 1) AS pct_hybrid,
    ROUND(100.0 * inperson_n / total_n, 1) AS pct_inperson,
    RANK() OVER (
        PARTITION BY survey_year
        ORDER BY 100.0 * remote_n / total_n DESC
    )                                       AS rank_most_remote,
    RANK() OVER (
        PARTITION BY survey_year
        ORDER BY 100.0 * inperson_n / total_n DESC
    )                                       AS rank_most_inperson
FROM devtype_totals
ORDER BY survey_year, pct_remote DESC;