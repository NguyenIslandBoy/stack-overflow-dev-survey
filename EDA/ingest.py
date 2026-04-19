"""
ingest.py
---------
Load Stack Overflow Developer Survey CSVs (2021-2025) into a DuckDB database.

Responsibilities:
  - Rename columns to canonical names across years
  - Add a `survey_year` column
  - Write a single `responses` table to survey.duckdb
  - Write a `lang_exploded` table (one row per respondent per language)
  - Write a `db_exploded` table (one row per respondent per database)

Known schema breaks handled here:
  - 2024: ConvertedCompYearly is fully NULL (SO dropped comp questions that year)
  - 2025: YearsCodePro dropped; use WorkExp as proxy
  - 2025: NEWCollabToolsHaveWorkedWith renamed to DevEnvsHaveWorkedWith
  - RemoteWork values differ slightly in 2025 (normalized in cleaning.sql)
"""

import duckdb
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")          # folder containing the 5 CSVs
DB_PATH = Path("survey.duckdb")

# ---------------------------------------------------------------------------
# Canonical column mapping: {canonical_name: {year: source_column_or_None}}
# None means the column did not exist that year.
# ---------------------------------------------------------------------------
COLUMN_MAP = {
    "response_id":          {2021: "ResponseId",                2022: "ResponseId",                2023: "ResponseId",                2024: "ResponseId",                2025: "ResponseId"},
    "main_branch":          {2021: "MainBranch",                2022: "MainBranch",                2023: "MainBranch",                2024: "MainBranch",                2025: "MainBranch"},
    "employment":           {2021: "Employment",                2022: "Employment",                2023: "Employment",                2024: "Employment",                2025: "Employment"},
    "country":              {2021: "Country",                   2022: "Country",                   2023: "Country",                   2024: "Country",                   2025: "Country"},
    "ed_level":             {2021: "EdLevel",                   2022: "EdLevel",                   2023: "EdLevel",                   2024: "EdLevel",                   2025: "EdLevel"},
    "age":                  {2021: "Age",                       2022: "Age",                       2023: "Age",                       2024: "Age",                       2025: "Age"},
    "years_code":           {2021: "YearsCode",                 2022: "YearsCode",                 2023: "YearsCode",                 2024: "YearsCode",                 2025: "YearsCode"},
    # YearsCodePro dropped in 2025; WorkExp is the closest proxy (available 2022+)
    "years_code_pro":       {2021: "YearsCodePro",              2022: "YearsCodePro",              2023: "YearsCodePro",              2024: "YearsCodePro",              2025: None},
    "work_exp":             {2021: None,                        2022: "WorkExp",                   2023: "WorkExp",                   2024: "WorkExp",                   2025: "WorkExp"},
    "dev_type":             {2021: "DevType",                   2022: "DevType",                   2023: "DevType",                   2024: "DevType",                   2025: "DevType"},
    "org_size":             {2021: "OrgSize",                   2022: "OrgSize",                   2023: "OrgSize",                   2024: "OrgSize",                   2025: "OrgSize"},
    "currency":             {2021: "Currency",                  2022: "Currency",                  2023: "Currency",                  2024: "Currency",                  2025: "Currency"},
    "comp_total":           {2021: "CompTotal",                 2022: "CompTotal",                 2023: "CompTotal",                 2024: "CompTotal",                 2025: "CompTotal"},
    "comp_freq":            {2021: "CompFreq",                  2022: "CompFreq",                  2023: None,                        2024: None,                        2025: None},
    # ConvertedCompYearly: ALL NULL in 2024 (SO removed comp questions that year)
    "salary_usd_yearly":    {2021: "ConvertedCompYearly",       2022: "ConvertedCompYearly",       2023: "ConvertedCompYearly",       2024: "ConvertedCompYearly",       2025: "ConvertedCompYearly"},
    "remote_work":          {2021: None,                        2022: "RemoteWork",                2023: "RemoteWork",                2024: "RemoteWork",                2025: "RemoteWork"},
    "industry":             {2021: None,                        2022: None,                        2023: "Industry",                  2024: "Industry",                  2025: "Industry"},
    "ai_select":            {2021: None,                        2022: None,                        2023: "AISelect",                  2024: "AISelect",                  2025: "AISelect"},
    # Multi-value tech columns (semicolon-separated strings)
    "lang_worked":          {2021: "LanguageHaveWorkedWith",    2022: "LanguageHaveWorkedWith",    2023: "LanguageHaveWorkedWith",    2024: "LanguageHaveWorkedWith",    2025: "LanguageHaveWorkedWith"},
    "lang_want":            {2021: "LanguageWantToWorkWith",    2022: "LanguageWantToWorkWith",    2023: "LanguageWantToWorkWith",    2024: "LanguageWantToWorkWith",    2025: "LanguageWantToWorkWith"},
    "lang_admired":         {2021: None,                        2022: None,                        2023: None,                        2024: "LanguageAdmired",           2025: "LanguageAdmired"},
    "db_worked":            {2021: "DatabaseHaveWorkedWith",    2022: "DatabaseHaveWorkedWith",    2023: "DatabaseHaveWorkedWith",    2024: "DatabaseHaveWorkedWith",    2025: "DatabaseHaveWorkedWith"},
    "db_want":              {2021: "DatabaseWantToWorkWith",    2022: "DatabaseWantToWorkWith",    2023: "DatabaseWantToWorkWith",    2024: "DatabaseWantToWorkWith",    2025: "DatabaseWantToWorkWith"},
    "platform_worked":      {2021: "PlatformHaveWorkedWith",   2022: "PlatformHaveWorkedWith",    2023: "PlatformHaveWorkedWith",    2024: "PlatformHaveWorkedWith",    2025: "PlatformHaveWorkedWith"},
    "tools_worked":         {2021: "ToolsTechHaveWorkedWith",  2022: "ToolsTechHaveWorkedWith",   2023: "ToolsTechHaveWorkedWith",   2024: "ToolsTechHaveWorkedWith",   2025: None},  # merged into platform in 2025
    # Dev environments: renamed in 2025
    "dev_envs_worked":      {2021: "NEWCollabToolsHaveWorkedWith", 2022: "NEWCollabToolsHaveWorkedWith", 2023: "NEWCollabToolsHaveWorkedWith", 2024: "NEWCollabToolsHaveWorkedWith", 2025: "DevEnvsHaveWorkedWith"},
}

YEARS = [2021, 2022, 2023, 2024, 2025]


def load_year(year: int) -> pd.DataFrame:
    path = DATA_DIR / f"survey_results_public_{year}.csv"
    print(f"  Loading {path} ...", end=" ")
    raw = pd.read_csv(path, low_memory=False)
    print(f"{len(raw):,} rows, {len(raw.columns)} cols")

    canonical = {"survey_year": year}
    for canon_col, year_map in COLUMN_MAP.items():
        src = year_map.get(year)
        if src and src in raw.columns:
            canonical[canon_col] = raw[src]
        else:
            canonical[canon_col] = pd.NA

    return pd.DataFrame(canonical)


def explode_multival(df: pd.DataFrame, src_col: str, out_col: str) -> pd.DataFrame:
    """
    Expand a semicolon-separated column into one row per value.
    Keeps survey_year and response_id for joins.
    """
    subset = df[["survey_year", "response_id", src_col]].dropna(subset=[src_col])
    subset = subset.copy()
    subset[out_col] = subset[src_col].str.split(";")
    exploded = subset.explode(out_col)
    exploded[out_col] = exploded[out_col].str.strip()
    return exploded[["survey_year", "response_id", out_col]].reset_index(drop=True)


def main():
    print("=== Stack Overflow Survey Ingestion ===\n")

    frames = []
    for year in YEARS:
        frames.append(load_year(year))

    responses = pd.concat(frames, ignore_index=True)
    print(f"\nCombined: {len(responses):,} rows, {len(responses.columns)} columns")

    # Coerce numeric columns
    for col in ["years_code", "years_code_pro", "work_exp", "comp_total", "salary_usd_yearly"]:
        responses[col] = pd.to_numeric(responses[col], errors="coerce")

    # Exploded tables
    lang_exploded = explode_multival(responses, "lang_worked", "language")
    db_exploded   = explode_multival(responses, "db_worked",   "database")

    print(f"lang_exploded: {len(lang_exploded):,} rows")
    print(f"db_exploded:   {len(db_exploded):,} rows")

    # Write to DuckDB
    print(f"\nWriting to {DB_PATH} ...")
    con = duckdb.connect(str(DB_PATH))
    con.execute("DROP TABLE IF EXISTS responses")
    con.execute("DROP TABLE IF EXISTS lang_exploded")
    con.execute("DROP TABLE IF EXISTS db_exploded")
    con.register("_responses",     responses)
    con.register("_lang_exploded", lang_exploded)
    con.register("_db_exploded",   db_exploded)
    con.execute("CREATE TABLE responses     AS SELECT * FROM _responses")
    con.execute("CREATE TABLE lang_exploded AS SELECT * FROM _lang_exploded")
    con.execute("CREATE TABLE db_exploded   AS SELECT * FROM _db_exploded")

    # Sanity checks
    print("\n=== Sanity Checks ===")
    print(con.execute("SELECT survey_year, COUNT(*) AS n FROM responses GROUP BY 1 ORDER BY 1").df().to_string(index=False))
    print(con.execute("""
        SELECT survey_year,
               ROUND(100.0 * SUM(CASE WHEN salary_usd_yearly IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null
        FROM responses GROUP BY 1 ORDER BY 1
    """).df().to_string(index=False))

    con.close()
    print(f"\nDone. Database written to {DB_PATH}")


if __name__ == "__main__":
    main()