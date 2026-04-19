-- 04_devtype_salary_gap.sql
-- -------------------------
-- Analyses salary gaps between developer types over time,
-- combining NTILE, PERCENTILE_CONT, LAG, and correlated subqueries
-- to surface which roles pay best, which are declining, and why.
--
-- Questions answered:
--   1. How has the salary ranking of each dev type changed year over year?
--   2. Which dev types have the widest salary spread (high variance roles)?
--   3. Does AI tool adoption correlate with higher salary within a dev type?
--   4. Which dev types are growing vs shrinking in headcount?
--
-- Window functions used:
--   NTILE(10)          — decile assignment for within-type spread analysis
--   PERCENTILE_CONT()  — median and percentile salary by dev type
--   LAG()              — salary rank change year over year
--   RANK()             — rank dev types by median salary per year


-- ============================================================
-- QUERY A: Salary rank change by developer type over time
--          LAG on rank shows which roles are climbing or falling
--          in relative pay, independent of absolute salary growth.
-- ============================================================
WITH salary_base AS (
    SELECT
        survey_year,
        dev_type_primary,
        salary_usd_yearly_clean AS salary
    FROM responses_clean
    WHERE salary_usd_yearly_clean IS NOT NULL
      AND employment_clean = 'Employed FT'
      AND dev_type_primary IS NOT NULL
      AND dev_type_primary != ''
),

devtype_stats AS (
    SELECT
        survey_year,
        dev_type_primary,
        COUNT(*)                                                            AS n,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary))        AS p25,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary))        AS median_salary,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary))        AS p75,
        -- Coefficient of variation proxy: IQR / median
        -- Higher = more salary spread within this dev type
        ROUND(
            (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) -
             PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary))
            / NULLIF(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary), 0)
        , 2)                                                                AS iqr_over_median
    FROM salary_base
    GROUP BY survey_year, dev_type_primary
    HAVING COUNT(*) >= 100
),

devtype_with_rank AS (
    SELECT
        survey_year,
        dev_type_primary,
        n,
        p25,
        median_salary,
        p75,
        iqr_over_median,
        RANK() OVER (PARTITION BY survey_year ORDER BY median_salary DESC)  AS salary_rank
    FROM devtype_stats
),

devtype_ranked AS (
    SELECT
        *,
        LAG(salary_rank) OVER (
            PARTITION BY dev_type_primary ORDER BY survey_year
        )                                                                   AS prev_year_rank
    FROM devtype_with_rank
)

SELECT
    survey_year,
    dev_type_primary,
    n,
    median_salary,
    salary_rank,
    prev_year_rank,
    -- Positive = climbed in ranking, negative = fell
    CASE
        WHEN prev_year_rank IS NULL THEN NULL
        ELSE prev_year_rank - salary_rank
    END                                                                     AS rank_change,
    iqr_over_median
FROM devtype_ranked
ORDER BY survey_year, salary_rank;


-- ============================================================
-- QUERY B: Salary spread within dev types using deciles
--          NTILE(10) assigns each respondent to a salary decile
--          within their dev type + year. We then compare the
--          D1 (bottom 10%) and D9 (top 10%) salaries to measure
--          how wide the spread is within each role.
--          Wide spread = high variance role (e.g. freelancer mix).
-- ============================================================
WITH salary_deciles AS (
    SELECT
        survey_year,
        dev_type_primary,
        salary_usd_yearly_clean AS salary,
        NTILE(10) OVER (
            PARTITION BY survey_year, dev_type_primary
            ORDER BY salary_usd_yearly_clean
        ) AS decile
    FROM responses_clean
    WHERE salary_usd_yearly_clean IS NOT NULL
      AND employment_clean = 'Employed FT'
      AND dev_type_primary IS NOT NULL
      AND dev_type_primary != ''
),

decile_bounds AS (
    SELECT
        survey_year,
        dev_type_primary,
        COUNT(*)                                                            AS total_n,
        -- D1 ceiling: max salary in the bottom decile
        MAX(CASE WHEN decile = 1  THEN salary END)                         AS d1_ceiling,
        -- D9 floor: min salary in the top decile
        MIN(CASE WHEN decile = 9  THEN salary END)                         AS d9_floor,
        -- Median for reference
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary))        AS median_salary
    FROM salary_deciles
    GROUP BY survey_year, dev_type_primary
    HAVING COUNT(*) >= 100
)

SELECT
    survey_year,
    dev_type_primary,
    total_n,
    d1_ceiling,
    median_salary,
    d9_floor,
    -- D9/D1 ratio: how many times higher is top-10% vs bottom-10%
    ROUND(d9_floor / NULLIF(d1_ceiling, 0), 1)                             AS d9_d1_ratio,
    RANK() OVER (
        PARTITION BY survey_year
        ORDER BY d9_floor / NULLIF(d1_ceiling, 0) DESC
    )                                                                       AS rank_by_spread
FROM decile_bounds
ORDER BY survey_year, rank_by_spread;


-- ============================================================
-- QUERY C: AI tool adoption rate vs median salary by dev type
--          Available 2023-2025 only (ai_user NULL before 2023).
--          Shows whether AI-adopting dev types earn more —
--          and whether the correlation strengthened over time.
-- ============================================================
WITH ai_salary AS (
    SELECT
        survey_year,
        dev_type_primary,
        ai_user,
        salary_usd_yearly_clean AS salary
    FROM responses_clean
    WHERE salary_usd_yearly_clean IS NOT NULL
      AND employment_clean = 'Employed FT'
      AND dev_type_primary IS NOT NULL
      AND dev_type_primary != ''
      AND survey_year >= 2023
      AND ai_user IS NOT NULL
),

devtype_ai_stats AS (
    SELECT
        survey_year,
        dev_type_primary,
        COUNT(*)                                                            AS total_n,
        -- AI adoption rate within this dev type
        ROUND(100.0 * SUM(CASE WHEN ai_user THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                            AS pct_ai_users,
        -- Median salary for AI users vs non-users within same dev type
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY salary
        ) FILTER (WHERE ai_user = TRUE))                                    AS median_salary_ai_users,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY salary
        ) FILTER (WHERE ai_user = FALSE))                                   AS median_salary_non_ai
    FROM ai_salary
    GROUP BY survey_year, dev_type_primary
    HAVING COUNT(*) >= 100
)

SELECT
    survey_year,
    dev_type_primary,
    total_n,
    pct_ai_users,
    median_salary_ai_users,
    median_salary_non_ai,
    -- Salary premium for AI users within same dev type
    ROUND(median_salary_ai_users - median_salary_non_ai)                    AS ai_salary_premium,
    RANK() OVER (
        PARTITION BY survey_year
        ORDER BY pct_ai_users DESC
    )                                                                       AS rank_by_ai_adoption
FROM devtype_ai_stats
ORDER BY survey_year, pct_ai_users DESC;


-- ============================================================
-- QUERY D: Headcount trend by dev type 2021-2025
--          Which roles are growing in survey representation?
--          Proxy for industry hiring trends.
--          Uses LAG to show YoY respondent count change.
--          Note: normalised against total respondents per year
--          to account for varying survey sizes across years.
-- ============================================================
WITH year_totals AS (
    SELECT survey_year, COUNT(*) AS year_total
    FROM responses_clean
    WHERE dev_type_primary IS NOT NULL AND dev_type_primary != ''
    GROUP BY survey_year
),

devtype_counts AS (
    SELECT
        rc.survey_year,
        rc.dev_type_primary,
        COUNT(*)                                                            AS n,
        yt.year_total,
        ROUND(100.0 * COUNT(*) / yt.year_total, 2)                         AS pct_of_year
    FROM responses_clean rc
    JOIN year_totals yt USING (survey_year)
    WHERE rc.dev_type_primary IS NOT NULL
      AND rc.dev_type_primary != ''
    GROUP BY rc.survey_year, rc.dev_type_primary, yt.year_total
    HAVING COUNT(*) >= 100
)

SELECT
    survey_year,
    dev_type_primary,
    n,
    pct_of_year,
    LAG(pct_of_year) OVER (
        PARTITION BY dev_type_primary ORDER BY survey_year
    )                                                                       AS prev_year_pct,
    ROUND(
        pct_of_year -
        LAG(pct_of_year) OVER (
            PARTITION BY dev_type_primary ORDER BY survey_year
        )
    , 2)                                                                    AS yoy_share_change
FROM devtype_counts
ORDER BY survey_year, pct_of_year DESC;