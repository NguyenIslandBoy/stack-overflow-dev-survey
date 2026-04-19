-- 02_salary_percentiles.sql
-- -------------------------
-- Salary distribution analysis by country, experience, and developer type.
--
-- Questions answered:
--   1. What are the salary percentiles (P25/median/P75) by country and year?
--   2. How does salary scale with experience within each country?
--   3. Which countries have the highest salary growth over time?
--   4. How does salary vary across developer types?
--
-- Tables used:
--   responses_clean — canonical view with salary_usd_yearly_clean, exp_bracket
--
-- Window functions used:
--   PERCENTILE_CONT()  — continuous percentile estimation
--   RANK()             — rank countries by median salary within each year
--   LAG()              — year-over-year median salary change per country
--
-- Note: salary data is NULL for ~2-3% of rows (outlier removal in cleaning.py).
--       2024 has a smaller salary sample (~22k vs ~47k in 2023) due to SO
--       making compensation questions optional that year. Interpret 2024
--       salary figures with caution.
--
-- Salary filter: respondents who answered ConvertedCompYearly and passed
--       the $1k-$1M plausibility filter applied in cleaning.py.
--       We additionally filter to full-time employed respondents to avoid
--       mixing student/part-time salaries into country comparisons.


-- ============================================================
-- QUERY A: Salary percentiles by country and year
--          Only countries with 100+ salary responses per year
--          to ensure stable percentile estimates.
--          LAG shows year-over-year median change per country.
-- ============================================================
WITH salary_base AS (
    SELECT
        survey_year,
        country,
        salary_usd_yearly_clean AS salary
    FROM responses_clean
    WHERE salary_usd_yearly_clean IS NOT NULL
      AND employment_clean = 'Employed FT'     -- exclude students, part-time
      AND country IS NOT NULL
),

country_year_stats AS (
    SELECT
        survey_year,
        country,
        COUNT(*)                                                                AS n_respondents,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary))            AS p25,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary))            AS median_salary,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary))            AS p75,
        ROUND(AVG(salary))                                                      AS mean_salary,
        -- IQR as a measure of salary spread within country
        ROUND(
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) -
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary)
        )                                                                       AS iqr
    FROM salary_base
    GROUP BY survey_year, country
    HAVING COUNT(*) >= 100      -- minimum sample for stable percentiles
),

country_with_rank_and_lag AS (
    SELECT
        survey_year,
        country,
        n_respondents,
        p25,
        median_salary,
        p75,
        mean_salary,
        iqr,
        -- Rank countries by median salary within each year
        RANK() OVER (PARTITION BY survey_year ORDER BY median_salary DESC)      AS rank_by_median,
        -- YoY change in median salary for this country
        LAG(median_salary) OVER (PARTITION BY country ORDER BY survey_year)     AS prev_year_median,
        ROUND(
            median_salary -
            LAG(median_salary) OVER (PARTITION BY country ORDER BY survey_year)
        )                                                                       AS yoy_median_change
    FROM country_year_stats
)

SELECT
    survey_year,
    country,
    n_respondents,
    p25,
    median_salary,
    p75,
    iqr,
    rank_by_median,
    prev_year_median,
    yoy_median_change
FROM country_with_rank_and_lag
ORDER BY survey_year, rank_by_median;


-- ============================================================
-- QUERY B: Salary by experience bracket within top countries
--          Shows how salary scales with seniority.
--          PARTITION BY country lets us compare the experience
--          premium across different markets.
-- ============================================================
WITH salary_base AS (
    SELECT
        survey_year,
        country,
        exp_bracket,
        experience_years,
        salary_usd_yearly_clean AS salary
    FROM responses_clean
    WHERE salary_usd_yearly_clean IS NOT NULL
      AND employment_clean = 'Employed FT'
      AND exp_bracket IS NOT NULL
      AND country IN (
          -- Top countries by total salary responses across all years
          SELECT country
          FROM responses_clean
          WHERE salary_usd_yearly_clean IS NOT NULL
            AND employment_clean = 'Employed FT'
          GROUP BY country
          HAVING COUNT(*) >= 500
          ORDER BY COUNT(*) DESC
          LIMIT 10
      )
),

exp_stats AS (
    SELECT
        survey_year,
        country,
        exp_bracket,
        COUNT(*)                                                            AS n,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary))        AS median_salary,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary))        AS p25,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary))        AS p75
    FROM salary_base
    GROUP BY survey_year, country, exp_bracket
    HAVING COUNT(*) >= 30       -- minimum for a stable bracket estimate
)

SELECT
    survey_year,
    country,
    exp_bracket,
    n,
    p25,
    median_salary,
    p75,
    -- Salary premium vs the 0-1 yrs bracket in the same country+year
    ROUND(
        median_salary -
        FIRST_VALUE(median_salary) OVER (
            PARTITION BY survey_year, country
            ORDER BY CASE exp_bracket
                WHEN '0-1 yrs'   THEN 1
                WHEN '2-4 yrs'   THEN 2
                WHEN '5-9 yrs'   THEN 3
                WHEN '10-19 yrs' THEN 4
                WHEN '20+ yrs'   THEN 5
            END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    ) AS premium_vs_entry_level,
    CASE exp_bracket
        WHEN '0-1 yrs'   THEN 1
        WHEN '2-4 yrs'   THEN 2
        WHEN '5-9 yrs'   THEN 3
        WHEN '10-19 yrs' THEN 4
        WHEN '20+ yrs'   THEN 5
    END AS bracket_order
FROM exp_stats
ORDER BY survey_year, country, bracket_order;


-- ============================================================
-- QUERY C: Developer type salary distribution
--          Uses NTILE(4) to bucket respondents into salary
--          quartiles, then shows which dev types concentrate
--          in the top quartile vs bottom quartile.
--          Restricted to 2023-2025 for cleaner dev_type data.
-- ============================================================
WITH salary_quartiles AS (
    SELECT
        survey_year,
        response_id,
        dev_type_primary,
        salary_usd_yearly_clean AS salary,
        -- Assign each respondent to a global salary quartile that year
        NTILE(4) OVER (
            PARTITION BY survey_year
            ORDER BY salary_usd_yearly_clean
        ) AS salary_quartile       -- 1=bottom 25%, 4=top 25%
    FROM responses_clean
    WHERE salary_usd_yearly_clean IS NOT NULL
      AND employment_clean = 'Employed FT'
      AND dev_type_primary IS NOT NULL
      AND dev_type_primary != ''
      AND survey_year >= 2023
),

devtype_stats AS (
    SELECT
        survey_year,
        dev_type_primary,
        COUNT(*)                                                    AS total_n,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary)) AS median_salary,
        -- Pct in top quartile (Q4)
        ROUND(100.0 * SUM(CASE WHEN salary_quartile = 4 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                    AS pct_in_top_quartile,
        -- Pct in bottom quartile (Q1)
        ROUND(100.0 * SUM(CASE WHEN salary_quartile = 1 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                    AS pct_in_bottom_quartile
    FROM salary_quartiles
    GROUP BY survey_year, dev_type_primary
    HAVING COUNT(*) >= 50
)

SELECT
    survey_year,
    dev_type_primary,
    total_n,
    median_salary,
    pct_in_top_quartile,
    pct_in_bottom_quartile,
    RANK() OVER (PARTITION BY survey_year ORDER BY median_salary DESC) AS rank_by_median
FROM devtype_stats
ORDER BY survey_year, rank_by_median;