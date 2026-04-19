-- 01_language_trends.sql
-- ----------------------
-- Tracks programming language adoption trends across 2021-2025.
--
-- Questions answered:
--   1. Which languages grew or declined the most over 5 years?
--   2. What is each language's year-over-year adoption change?
--   3. Which languages are consistently rising vs. peaking vs. fading?
--   4. Is there a gap between languages developers USE vs. WANT to use?
--
-- Tables used:
--   lang_exploded   — one row per respondent per language (built in ingest.py)
--   responses_clean — for respondent counts per year (denominator)
--
-- Window functions used:
--   LAG()  — compare current year adoption to previous year
--   LEAD() — look ahead to next year (identifies early trend signals)
--   RANK() — rank languages by adoption within each year
--
-- Note: each query is self-contained (CTEs do not persist between statements).


-- ============================================================
-- QUERY A: Full trend table — adoption rate + YoY change
--          LAG/LEAD per language per year, for languages with
--          5%+ adoption in at least one year across 3+ years.
-- ============================================================
WITH respondents_per_year AS (
    SELECT survey_year, COUNT(*) AS total_respondents
    FROM responses_clean
    WHERE lang_worked IS NOT NULL
    GROUP BY survey_year
),

lang_counts AS (
    SELECT le.survey_year, le.language, COUNT(DISTINCT le.response_id) AS user_count
    FROM lang_exploded le
    WHERE le.language IS NOT NULL AND TRIM(le.language) != ''
    GROUP BY le.survey_year, le.language
),

lang_rates AS (
    SELECT
        lc.survey_year,
        lc.language,
        lc.user_count,
        rpy.total_respondents,
        ROUND(100.0 * lc.user_count / rpy.total_respondents, 2) AS adoption_pct
    FROM lang_counts lc
    JOIN respondents_per_year rpy USING (survey_year)
),

lang_with_trends AS (
    SELECT
        survey_year,
        language,
        user_count,
        total_respondents,
        adoption_pct,
        LAG(adoption_pct)  OVER (PARTITION BY language ORDER BY survey_year) AS prev_year_pct,
        LEAD(adoption_pct) OVER (PARTITION BY language ORDER BY survey_year) AS next_year_pct,
        ROUND(
            adoption_pct -
            LAG(adoption_pct) OVER (PARTITION BY language ORDER BY survey_year)
        , 2) AS yoy_change_pp,
        RANK() OVER (PARTITION BY survey_year ORDER BY adoption_pct DESC) AS rank_in_year
    FROM lang_rates
)

SELECT
    survey_year,
    language,
    user_count,
    adoption_pct,
    prev_year_pct,
    yoy_change_pp,
    next_year_pct,
    rank_in_year
FROM lang_with_trends
WHERE language IN (
    -- Only languages with 5%+ adoption in at least one year, present in 3+ years
    SELECT language
    FROM lang_rates
    WHERE adoption_pct >= 5
    GROUP BY language
    HAVING COUNT(DISTINCT survey_year) >= 3
)
ORDER BY language, survey_year;


-- ============================================================
-- QUERY B: Biggest risers and fallers (2021 vs 2025)
--          Compares endpoints directly to show 5-year movement.
--          Self-contained: redefines lang_rates locally.
-- ============================================================
WITH respondents_per_year AS (
    SELECT survey_year, COUNT(*) AS total_respondents
    FROM responses_clean
    WHERE lang_worked IS NOT NULL
    GROUP BY survey_year
),

lang_counts AS (
    SELECT le.survey_year, le.language, COUNT(DISTINCT le.response_id) AS user_count
    FROM lang_exploded le
    WHERE le.language IS NOT NULL AND TRIM(le.language) != ''
    GROUP BY le.survey_year, le.language
),

lang_rates AS (
    SELECT
        lc.survey_year,
        lc.language,
        ROUND(100.0 * lc.user_count / rpy.total_respondents, 2) AS adoption_pct
    FROM lang_counts lc
    JOIN respondents_per_year rpy USING (survey_year)
),

endpoints AS (
    SELECT
        language,
        MAX(CASE WHEN survey_year = 2021 THEN adoption_pct END) AS pct_2021,
        MAX(CASE WHEN survey_year = 2025 THEN adoption_pct END) AS pct_2025
    FROM lang_rates
    GROUP BY language
    HAVING pct_2021 IS NOT NULL AND pct_2025 IS NOT NULL
)

SELECT
    language,
    pct_2021,
    pct_2025,
    ROUND(pct_2025 - pct_2021, 2) AS total_change_pp,
    CASE
        WHEN pct_2025 > pct_2021 THEN 'Rising'
        WHEN pct_2025 < pct_2021 THEN 'Declining'
        ELSE 'Stable'
    END AS trend_direction
FROM endpoints
WHERE pct_2021 >= 3 OR pct_2025 >= 3
ORDER BY total_change_pp DESC;


-- ============================================================
-- QUERY C: Desire gap — languages developers WANT vs USE
--          High positive gap = aspirational language gaining
--          mindshare before adoption catches up (expect Rust).
--          UNNEST in FROM clause per DuckDB syntax requirement.
-- ============================================================
WITH respondents_per_year AS (
    SELECT survey_year, COUNT(*) AS total
    FROM responses_clean
    WHERE lang_worked IS NOT NULL
    GROUP BY survey_year
),

used AS (
    SELECT survey_year, language, COUNT(DISTINCT response_id) AS used_count
    FROM lang_exploded
    WHERE language IS NOT NULL AND TRIM(language) != ''
    GROUP BY survey_year, language
),

wanted AS (
    -- UNNEST must be in FROM clause in DuckDB; alias as tok(language)
    SELECT
        rc.survey_year,
        TRIM(tok.language) AS language,
        COUNT(DISTINCT rc.response_id) AS want_count
    FROM responses_clean rc,
         UNNEST(STRING_SPLIT(rc.lang_want, ';')) AS tok(language)
    WHERE rc.lang_want IS NOT NULL
    GROUP BY rc.survey_year, TRIM(tok.language)
)

SELECT
    u.survey_year,
    u.language,
    ROUND(100.0 * u.used_count / r.total, 1) AS pct_use,
    ROUND(100.0 * w.want_count / r.total, 1)  AS pct_want,
    ROUND(
        (100.0 * w.want_count / r.total) -
        (100.0 * u.used_count / r.total)
    , 1) AS desire_gap_pp,
    RANK() OVER (
        PARTITION BY u.survey_year
        ORDER BY (100.0 * w.want_count / r.total) - (100.0 * u.used_count / r.total) DESC
    ) AS desire_gap_rank
FROM used u
JOIN wanted w ON u.survey_year = w.survey_year AND u.language = w.language
JOIN respondents_per_year r ON u.survey_year = r.survey_year
WHERE u.used_count + w.want_count > 500
ORDER BY u.survey_year, desire_gap_pp DESC;