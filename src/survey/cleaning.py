"""
cleaning.py
-----------
Builds the `responses_clean` view in survey.duckdb.

Keeps raw `responses` table untouched — the view is always rerunnable.
All validation and distribution checks live in analysis/00_cleaning_validation.sql.

Cleaning operations applied:
  1. salary_usd_yearly_clean  — NULL out outliers outside $1k-$1M
  2. experience_years         — COALESCE(years_code_pro, work_exp), capped at 50
  3. exp_bracket              — labeled experience tiers for GROUP BY / partitions
  4. remote_work_clean        — 3 canonical values (Remote / Hybrid / In-person)
  5. ed_level_clean           — 5 canonical education tiers
  6. employment_clean         — 5 canonical employment types
  7. dev_type_primary         — first dev type from semicolon-separated list
  8. org_size_order           — integer sort key for org size
  9. ai_user                  — boolean flag, NULL for 2021-2022 (question absent)
"""

import duckdb
from pathlib import Path

DB_PATH = Path("survey.duckdb")


def build_clean_view(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DROP VIEW IF EXISTS responses_clean")
    con.execute("""
    CREATE VIEW responses_clean AS
    SELECT
        response_id,
        survey_year,
        country,
        age,

        -- 1. Salary: NULL out implausible values
        CASE
            WHEN salary_usd_yearly BETWEEN 1000 AND 1000000
            THEN salary_usd_yearly
        END AS salary_usd_yearly_clean,

        -- 2. Experience: prefer years_code_pro, fall back to work_exp; cap at 50
        CASE
            WHEN COALESCE(years_code_pro, work_exp) BETWEEN 0 AND 50
            THEN COALESCE(years_code_pro, work_exp)
        END AS experience_years,

        -- 3. Experience bracket
        CASE
            WHEN COALESCE(years_code_pro, work_exp) < 2   THEN '0-1 yrs'
            WHEN COALESCE(years_code_pro, work_exp) < 5   THEN '2-4 yrs'
            WHEN COALESCE(years_code_pro, work_exp) < 10  THEN '5-9 yrs'
            WHEN COALESCE(years_code_pro, work_exp) < 20  THEN '10-19 yrs'
            WHEN COALESCE(years_code_pro, work_exp) <= 50 THEN '20+ yrs'
            ELSE NULL
        END AS exp_bracket,

        -- 4. Remote work: normalize 2025's verbose labels to 3 canonical values
        CASE
            WHEN remote_work ILIKE '%remote%' AND remote_work NOT ILIKE '%hybrid%'
                THEN 'Remote'
            WHEN remote_work ILIKE '%hybrid%'
            OR remote_work ILIKE '%your choice%'
            OR remote_work ILIKE '%flexible%'
                THEN 'Hybrid'
            WHEN remote_work ILIKE '%person%' AND remote_work NOT ILIKE '%hybrid%'
                THEN 'In-person'
            ELSE NULL
        END AS remote_work_clean,

        -- 5. Education: collapse to 5 tiers
        CASE
            WHEN ed_level ILIKE '%primary%'
              OR ed_level ILIKE '%secondary%'
              OR ed_level ILIKE 'Something else'   THEN 'No formal degree'
            WHEN ed_level ILIKE '%associate%'      THEN 'Associate degree'
            WHEN ed_level ILIKE '%bachelor%'       THEN 'Bachelor''s degree'
            WHEN ed_level ILIKE '%master%'         THEN 'Master''s degree'
            WHEN ed_level ILIKE '%doctoral%'
              OR ed_level ILIKE '%phd%'
              OR ed_level ILIKE '%doctor%'         THEN 'PhD or higher'
            WHEN ed_level ILIKE '%professional%'   THEN 'Professional degree'
            WHEN ed_level ILIKE '%some college%'
            OR ed_level ILIKE '%without earning%'  THEN 'Some college'
            ELSE NULL
        END AS ed_level_clean,

        -- 6. Employment: collapse to 5 canonical types
        CASE
            WHEN employment ILIKE '%student%'      THEN 'Student'
            WHEN employment ILIKE '%full-time%'
              OR employment ILIKE '%full time%'
              OR employment = 'Employed'           THEN 'Employed FT'
            WHEN employment ILIKE '%part-time%'
              OR employment ILIKE '%part time%'    THEN 'Employed PT'
            WHEN employment ILIKE '%self-employed%'
              OR employment ILIKE '%freelance%'
              OR employment ILIKE '%contractor%'
              OR employment ILIKE '%independent%'  THEN 'Self-employed'
            WHEN employment ILIKE '%not employed%'
              OR employment ILIKE '%retired%'      THEN 'Not employed'
            ELSE 'Other'
        END AS employment_clean,

        -- 7. Primary dev type: first entry before the first semicolon
        SPLIT_PART(dev_type, ';', 1) AS dev_type_primary,

        -- 8. Org size sort key so ORDER BY org_size works correctly
        CASE org_size
            WHEN 'Just me - I am a freelancer, sole proprietor, etc.' THEN 1
            WHEN 'Just me - I am a freelancer, sole proprietor, etc'  THEN 1
            WHEN '2 to 9 employees'                                    THEN 2
            WHEN '10 to 19 employees'                                  THEN 3
            WHEN '20 to 99 employees'                                  THEN 4
            WHEN '100 to 499 employees'                                THEN 5
            WHEN '500 to 999 employees'                                THEN 6
            WHEN '1,000 to 4,999 employees'                            THEN 7
            WHEN '5,000 to 9,999 employees'                            THEN 8
            WHEN '10,000 or more employees'                            THEN 9
            ELSE NULL
        END AS org_size_order,

        -- 9. AI user flag — NULL for 2021-2022 (question not in survey)
        CASE
            WHEN survey_year < 2023      THEN NULL
            WHEN ai_select ILIKE '%yes%' THEN TRUE
            WHEN ai_select ILIKE '%no%'  THEN FALSE
            ELSE NULL
        END AS ai_user,

        -- Raw pass-through columns needed by analysis queries
        lang_worked,
        lang_want,
        lang_admired,
        db_worked,
        db_want,
        platform_worked,
        tools_worked,
        dev_envs_worked,
        dev_type,
        ed_level,
        employment,
        remote_work,
        org_size,
        ai_select,
        industry,
        years_code,
        years_code_pro,
        work_exp,
        salary_usd_yearly,
        main_branch

    FROM responses
    """)


def main():
    print(f"Building responses_clean view in {DB_PATH} ...")
    con = duckdb.connect(str(DB_PATH))
    build_clean_view(con)
    row_count = con.execute("SELECT COUNT(*) FROM responses_clean").fetchone()[0]
    con.close()
    print(f"Done. responses_clean: {row_count:,} rows")
    print("Run analysis/00_cleaning_validation.sql to verify cleaning logic.")


if __name__ == "__main__":
    main()