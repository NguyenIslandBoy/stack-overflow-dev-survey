# Stack Overflow Developer Survey

End-to-end SQL analytics project using Stack Overflow Developer Survey data (2021–2025).
Demonstrates advanced SQL across a real, messy, multi-year dataset: window functions,
percentile aggregates, CTEs, cross-year normalization, and honest handling of schema breaks.

Built as a portfolio project targeting Data Analyst and Data Engineer roles.

---

## Project Structure

```
stack_overflow_dev_survey/
├── data/                          # Raw CSVs (not committed — see Data section)
├── notebooks/
│   ├── eda.ipynb                  # Some basic exploratory analysis before ETL
├── src/
│   ├── ingest.py                  # Load CSVs → DuckDB (responses, lang_exploded, db_exploded)
│   └── cleaning.py                # Build responses_clean view with normalized columns
├── analysis/
│   ├── 00_cleaning_validation.sql # Data quality checks — null rates, distribution checks
│   ├── 01_language_trends.sql     # Language adoption trends with LAG/LEAD
│   ├── 02_salary_percentiles.sql  # Salary by country, experience, and dev type
│   ├── 03_remote_work_trends.sql  # Remote work shift 2022–2025
│   └── 04_devtype_salary_gap.sql  # Salary ranking, spread, and AI adoption by role
├── survey.duckdb                  # DuckDB database (generated — not committed)
├── requirements.txt
└── README.md
```

---

## Data

Download the public survey CSVs from [Stack Overflow Insights](https://insights.stackoverflow.com/survey)
for years 2021–2025. Place them in `data/` with the naming convention:

```
data/survey_results_public_2021.csv
data/survey_results_public_2022.csv
...
data/survey_results_public_2025.csv
```

**Total dataset**: ~360,000 respondents across 5 years.

---

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

Run the pipeline from the project root:

```bash
python EDA/ingest.py
python EDA/cleaning.py
```

Run any analysis file with the DuckDB CLI:

```bash
duckdb survey.duckdb < analysis/01_language_trends.sql
```

---

## Schema Design

`ingest.py` maps ~170 source columns (varying by year) to 27 canonical columns,
handling renames and column drops across years. Three tables are written to `survey.duckdb`:

| Table | Description |
|---|---|
| `responses` | Raw canonical columns, one row per respondent |
| `lang_exploded` | One row per respondent per language (exploded from semicolon-separated) |
| `db_exploded` | One row per respondent per database |

`cleaning.py` builds a `responses_clean` view on top of `responses` with normalized
categorical values. The raw table is never modified.

### Key schema breaks handled

| Issue | Detail |
|---|---|
| 2024 salary | `ConvertedCompYearly` is ~64% null — SO made compensation optional that year. Included in analysis with caveat. |
| `YearsCodePro` | Dropped in 2025. `WorkExp` used as proxy via `COALESCE(years_code_pro, work_exp)`. |
| `Bash/Shell` | Not a distinct language option until 2023. Absent from 2021–2022 language trends. |
| Remote work (2025) | Added verbose label `"Your choice (very flexible...)"` — normalized to Hybrid. |
| Dev type names | SO renamed several roles between years (e.g. `"DevOps specialist"` → `"DevOps engineer or professional"`). YoY LAG comparisons show NULL for renamed roles. |
| `ToolsTechHaveWorkedWith` | Merged into `PlatformHaveWorkedWith` in 2025. Tools analysis limited to 2021–2024. |

---

## Analysis Queries

### 01 — Language Adoption Trends (2021–2025)

**Window functions**: `LAG()`, `LEAD()`, `RANK()`

Three queries:

**Query A** — Full trend table with year-over-year change per language.
`LAG(adoption_pct)` gives the previous year's rate; `LEAD(adoption_pct)` surfaces
languages already declining before it becomes obvious in the rankings.

**Query B** — 2021 vs 2025 endpoint comparison. Key findings:

| Language | 2021 | 2025 | Change |
|---|---|---|---|
| TypeScript | 30.3% | 43.8% | +13.5pp |
| SQL | 47.2% | 58.8% | +11.7pp |
| Python | 48.3% | 58.1% | +9.8pp |
| Rust | 7.0% | 14.9% | +7.9pp |
| Java | 35.4% | 29.6% | −5.9pp |
| PHP | 22.0% | 18.9% | −3.1pp |

**Query C** — Desire gap (languages developers *want* to use minus languages they *use*).
Rust tops every year (+12pp in 2021, still positive in 2025). By 2025, JavaScript,
HTML/CSS, and SQL show the largest *negative* desire gaps — widely used but not wanted.

---

### 02 — Salary Percentiles by Country and Experience

**Window functions**: `PERCENTILE_CONT()`, `RANK()`, `FIRST_VALUE()`, `LAG()`

Three queries covering country-level salary distribution, experience brackets, and
developer type salary quartiles.

**Salary caveats:**
- Outliers outside $1k–$1M removed (~2–3% of salary rows per year).
- Full-time employed respondents only to avoid mixing student/contractor rates.
- 2024 has ~22k salary responses vs ~47k in 2023 — interpret with caution.

**Key findings:**

- US median salary for full-time developers: $125k (2021) → $150k (2022) → $172k (2025).
- Experience premium in the US: $80k entry level → $172k at 20+ years (+$92k).
- `Data or business analyst` ranks last by median salary in 2025 ($57k), with 42% of
  respondents in the bottom salary quartile — the clearest signal it is an entry point,
  not a destination role.
- Remote workers earn a ~$45k median premium over in-person workers in 2025, but this
  reflects geography (remote workers skew US/Western Europe) more than work arrangement.

---

### 03 — Remote Work Trends (2022–2025)

**Window functions**: `LAG()`, `RANK()`, `FIRST_VALUE()`

**Key findings:**

- Remote peaked at 43.3% in 2022 and has declined every year since, reaching 32.6% in 2025.
- Hybrid absorbed the shift, jumping +8.1pp in 2025 alone (now 49.9%).
- In-person never recovered: 14.5% in 2022 → 17.5% in 2025, well below pre-pandemic levels.
- SRE and cloud engineers are consistently the most remote roles; system administrators
  and academic researchers are the most in-person.
- `Data or business analyst` ranks 23rd out of 29 dev types for remote adoption in 2025.

---

### 04 — Developer Type Salary Gap

**Window functions**: `NTILE(10)`, `PERCENTILE_CONT()`, `LAG()`, `RANK()`

Four queries covering salary rank changes, within-role salary spread, AI adoption
vs salary, and headcount trends.

**Key findings:**

- Senior executive and engineering manager have held the top 2 salary ranks consistently
  across all years.
- `"Other (please specify)"` fell from rank 3 (2021) to rank 17 (2025) — a data artifact:
  SO added more specific role categories over time, splitting this bucket.
- **AI adoption vs salary**: `ai_salary_premium` is negative for most roles in both 2023
  and 2025. AI tool adoption does not correlate with higher individual pay within the same
  role. The more notable finding is the adoption rate itself: from ~40–60% across roles in
  2023 to 80%+ by 2025.
- Mobile developers and data/business analysts have the highest within-role salary spread
  (D9/D1 ratio 10–13×), reflecting a mix of high-paying Western respondents and low-paying
  emerging market respondents in the same category.

---

## Honest Limitations

- **Self-selected sample**: respondents are Stack Overflow users, skewing toward
  engaged English-speaking developers. Not representative of all developers globally.
- **Currency conversion**: `ConvertedCompYearly` uses SO's own USD conversion at survey
  time. Exchange rate fluctuations affect year-over-year salary comparisons.
- **Dev type renaming**: SO changed role labels between years. Cross-year LAG on dev type
  ranks produces NULL for renamed roles — flagged in query output.
- **2024 salary sample**: smaller and potentially self-selected (only respondents who
  chose to answer compensation questions). Treat 2024 salary figures as indicative only.
- **Primary dev type only**: respondents can select multiple dev types; this analysis
  uses only the first listed value for aggregation.

![CI](https://github.com/NguyenIslandBoy/stack-overflow-dev-survey/actions/workflows/ci.yml/badge.svg)
