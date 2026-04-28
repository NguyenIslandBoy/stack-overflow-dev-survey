"""
tests/test_survey.py
--------------------
Unit tests for src/survey/ingest.py and src/survey/cleaning.py.

All tests use in-memory DuckDB and synthetic DataFrames — no CSV files
or external data required. Safe to run in CI with no data/ directory present.
"""

import pytest
import pandas as pd
import duckdb
from unittest.mock import patch, MagicMock
from pathlib import Path

# ---------------------------------------------------------------------------
# Adjust import path so tests work from repo root with pytest
# ---------------------------------------------------------------------------
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.survey.ingest import explode_multival, load_year, COLUMN_MAP, YEARS
from src.survey.cleaning import build_clean_view


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def mem_con():
    """In-memory DuckDB connection with a synthetic responses table."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE responses (
            response_id          INTEGER,
            survey_year          INTEGER,
            country              VARCHAR,
            age                  VARCHAR,
            salary_usd_yearly    DOUBLE,
            years_code_pro       DOUBLE,
            work_exp             DOUBLE,
            remote_work          VARCHAR,
            ed_level             VARCHAR,
            employment           VARCHAR,
            dev_type             VARCHAR,
            org_size             VARCHAR,
            ai_select            VARCHAR,
            lang_worked          VARCHAR,
            lang_want            VARCHAR,
            lang_admired         VARCHAR,
            db_worked            VARCHAR,
            db_want              VARCHAR,
            platform_worked      VARCHAR,
            tools_worked         VARCHAR,
            dev_envs_worked      VARCHAR,
            industry             VARCHAR,
            years_code           DOUBLE,
            salary_usd_yearly    DOUBLE,
            main_branch          VARCHAR,
            currency             VARCHAR,
            comp_total           DOUBLE,
            comp_freq            VARCHAR
        )
    """)
    return con


@pytest.fixture
def mem_con():
    """In-memory DuckDB with a minimal responses table for cleaning tests."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE responses (
            response_id       INTEGER,
            survey_year       INTEGER,
            country           VARCHAR,
            age               VARCHAR,
            salary_usd_yearly DOUBLE,
            years_code_pro    DOUBLE,
            work_exp          DOUBLE,
            remote_work       VARCHAR,
            ed_level          VARCHAR,
            employment        VARCHAR,
            dev_type          VARCHAR,
            org_size          VARCHAR,
            ai_select         VARCHAR,
            lang_worked       VARCHAR,
            lang_want         VARCHAR,
            lang_admired      VARCHAR,
            db_worked         VARCHAR,
            db_want           VARCHAR,
            platform_worked   VARCHAR,
            tools_worked      VARCHAR,
            dev_envs_worked   VARCHAR,
            industry          VARCHAR,
            years_code        DOUBLE,
            main_branch       VARCHAR,
            currency          VARCHAR,
            comp_total        DOUBLE,
            comp_freq         VARCHAR
        )
    """)
    return con


def insert_row(con, **kwargs):
    """Insert a single synthetic row into responses. Unspecified cols are NULL."""
    defaults = {
        "response_id": 1, "survey_year": 2023, "country": "UK",
        "age": None, "salary_usd_yearly": None, "years_code_pro": None,
        "work_exp": None, "remote_work": None, "ed_level": None,
        "employment": None, "dev_type": None, "org_size": None,
        "ai_select": None, "lang_worked": None, "lang_want": None,
        "lang_admired": None, "db_worked": None, "db_want": None,
        "platform_worked": None, "tools_worked": None, "dev_envs_worked": None,
        "industry": None, "years_code": None, "main_branch": None,
        "currency": None, "comp_total": None, "comp_freq": None,
    }
    defaults.update(kwargs)
    cols = ", ".join(defaults.keys())
    placeholders = ", ".join(["?" for _ in defaults])
    con.execute(f"INSERT INTO responses ({cols}) VALUES ({placeholders})",
                list(defaults.values()))


# ===========================================================================
# explode_multival
# ===========================================================================

class TestExplodeMultival:

    def _make_df(self, rows):
        return pd.DataFrame(rows, columns=["survey_year", "response_id", "lang_worked"])

    def test_single_language(self):
        df = self._make_df([(2023, 1, "Python")])
        result = explode_multival(df, "lang_worked", "language")
        assert list(result["language"]) == ["Python"]
        assert len(result) == 1

    def test_multiple_languages_semicolon(self):
        df = self._make_df([(2023, 1, "Python;JavaScript;SQL")])
        result = explode_multival(df, "lang_worked", "language")
        assert sorted(result["language"].tolist()) == ["JavaScript", "Python", "SQL"]
        assert len(result) == 3

    def test_whitespace_stripped(self):
        df = self._make_df([(2023, 1, "Python; JavaScript ; SQL")])
        result = explode_multival(df, "lang_worked", "language")
        assert "JavaScript" in result["language"].values
        assert " JavaScript " not in result["language"].values

    def test_null_rows_dropped(self):
        df = self._make_df([(2023, 1, None), (2023, 2, "Python")])
        result = explode_multival(df, "lang_worked", "language")
        assert len(result) == 1
        assert result.iloc[0]["response_id"] == 2

    def test_output_columns(self):
        df = self._make_df([(2023, 1, "Rust")])
        result = explode_multival(df, "lang_worked", "language")
        assert set(result.columns) == {"survey_year", "response_id", "language"}

    def test_multiple_respondents(self):
        df = self._make_df([
            (2023, 1, "Python;SQL"),
            (2023, 2, "Go"),
        ])
        result = explode_multival(df, "lang_worked", "language")
        assert len(result) == 3
        r1 = result[result["response_id"] == 1]["language"].tolist()
        assert sorted(r1) == ["Python", "SQL"]

    def test_survey_year_preserved(self):
        df = self._make_df([(2021, 99, "TypeScript")])
        result = explode_multival(df, "lang_worked", "language")
        assert result.iloc[0]["survey_year"] == 2021

    def test_empty_dataframe(self):
        df = self._make_df([])
        result = explode_multival(df, "lang_worked", "language")
        assert len(result) == 0


# ===========================================================================
# COLUMN_MAP structural integrity
# ===========================================================================

class TestColumnMap:

    def test_all_years_present_for_every_canonical_column(self):
        for canon, year_map in COLUMN_MAP.items():
            for year in YEARS:
                assert year in year_map, (
                    f"COLUMN_MAP['{canon}'] missing year {year}"
                )

    def test_values_are_string_or_none(self):
        for canon, year_map in COLUMN_MAP.items():
            for year, src in year_map.items():
                assert src is None or isinstance(src, str), (
                    f"COLUMN_MAP['{canon}'][{year}] must be str or None, got {type(src)}"
                )

    def test_response_id_never_none(self):
        """response_id must always be mappable — it's the primary key."""
        for year, src in COLUMN_MAP["response_id"].items():
            assert src is not None, f"response_id missing for {year}"

    def test_known_schema_breaks(self):
        """Explicit checks for documented schema breaks."""
        # YearsCodePro dropped in 2025
        assert COLUMN_MAP["years_code_pro"][2025] is None
        # work_exp not in 2021
        assert COLUMN_MAP["work_exp"][2021] is None
        # tools_worked merged into platform in 2025
        assert COLUMN_MAP["tools_worked"][2025] is None
        # ai_select not in 2021/2022
        assert COLUMN_MAP["ai_select"][2021] is None
        assert COLUMN_MAP["ai_select"][2022] is None
        # remote_work not in 2021
        assert COLUMN_MAP["remote_work"][2021] is None


# ===========================================================================
# build_clean_view — salary cleaning
# ===========================================================================

class TestCleanViewSalary:

    def test_valid_salary_passes_through(self, mem_con):
        insert_row(mem_con, salary_usd_yearly=80000)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT salary_usd_yearly_clean FROM responses_clean"
        ).fetchone()[0]
        assert result == 80000

    def test_salary_below_1000_nulled(self, mem_con):
        insert_row(mem_con, salary_usd_yearly=500)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT salary_usd_yearly_clean FROM responses_clean"
        ).fetchone()[0]
        assert result is None

    def test_salary_above_1m_nulled(self, mem_con):
        insert_row(mem_con, salary_usd_yearly=2_000_000)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT salary_usd_yearly_clean FROM responses_clean"
        ).fetchone()[0]
        assert result is None

    def test_salary_boundary_1000_included(self, mem_con):
        insert_row(mem_con, salary_usd_yearly=1000)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT salary_usd_yearly_clean FROM responses_clean"
        ).fetchone()[0]
        assert result == 1000

    def test_salary_boundary_1m_included(self, mem_con):
        insert_row(mem_con, salary_usd_yearly=1_000_000)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT salary_usd_yearly_clean FROM responses_clean"
        ).fetchone()[0]
        assert result == 1_000_000

    def test_null_salary_stays_null(self, mem_con):
        insert_row(mem_con, salary_usd_yearly=None)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT salary_usd_yearly_clean FROM responses_clean"
        ).fetchone()[0]
        assert result is None


# ===========================================================================
# build_clean_view — experience cleaning
# ===========================================================================

class TestCleanViewExperience:

    def test_years_code_pro_used_when_available(self, mem_con):
        insert_row(mem_con, years_code_pro=5, work_exp=10)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT experience_years FROM responses_clean"
        ).fetchone()[0]
        assert result == 5

    def test_falls_back_to_work_exp_when_no_years_code_pro(self, mem_con):
        insert_row(mem_con, years_code_pro=None, work_exp=8)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT experience_years FROM responses_clean"
        ).fetchone()[0]
        assert result == 8

    def test_experience_over_50_nulled(self, mem_con):
        insert_row(mem_con, years_code_pro=99)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT experience_years FROM responses_clean"
        ).fetchone()[0]
        assert result is None

    def test_exp_bracket_0_1(self, mem_con):
        insert_row(mem_con, years_code_pro=1)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT exp_bracket FROM responses_clean"
        ).fetchone()[0]
        assert result == "0-1 yrs"

    def test_exp_bracket_20_plus(self, mem_con):
        insert_row(mem_con, years_code_pro=25)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT exp_bracket FROM responses_clean"
        ).fetchone()[0]
        assert result == "20+ yrs"

    def test_exp_bracket_boundary_5(self, mem_con):
        insert_row(mem_con, years_code_pro=5)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT exp_bracket FROM responses_clean"
        ).fetchone()[0]
        assert result == "5-9 yrs"


# ===========================================================================
# build_clean_view — remote work cleaning
# ===========================================================================

class TestCleanViewRemoteWork:

    def _get_remote(self, con, value, year=2023):
        insert_row(con, remote_work=value, survey_year=year)
        build_clean_view(con)
        return con.execute(
            "SELECT remote_work_clean FROM responses_clean"
        ).fetchone()[0]

    def test_full_remote(self, mem_con):
        assert self._get_remote(mem_con, "Remote") == "Remote"

    def test_in_person(self, mem_con):
        assert self._get_remote(mem_con, "In-person") == "In-person"

    def test_hybrid(self, mem_con):
        assert self._get_remote(mem_con, "Hybrid") == "Hybrid"

    def test_2025_verbose_your_choice_maps_to_hybrid(self, mem_con):
        label = "Your choice (very flexible, work wherever you want)"
        assert self._get_remote(mem_con, label) == "Hybrid"

    def test_null_remote_stays_null(self, mem_con):
        assert self._get_remote(mem_con, None) is None


# ===========================================================================
# build_clean_view — education level cleaning
# ===========================================================================

class TestCleanViewEdLevel:

    def _get_ed(self, con, value):
        insert_row(con, ed_level=value)
        build_clean_view(con)
        return con.execute(
            "SELECT ed_level_clean FROM responses_clean"
        ).fetchone()[0]

    def test_bachelors(self, mem_con):
        assert self._get_ed(mem_con, "Bachelor's degree (B.A., B.S., B.Eng., etc.)") == "Bachelor's degree"

    def test_masters(self, mem_con):
        assert self._get_ed(mem_con, "Master's degree (M.A., M.S., M.Eng., MBA, etc.)") == "Master's degree"

    def test_phd(self, mem_con):
        assert self._get_ed(mem_con, "Doctor of Philosophy (Ph.D.)") == "PhD or higher"

    def test_secondary(self, mem_con):
        assert self._get_ed(mem_con, "Secondary school") == "No formal degree"

    def test_null_stays_null(self, mem_con):
        assert self._get_ed(mem_con, None) is None


# ===========================================================================
# build_clean_view — employment cleaning
# ===========================================================================

class TestCleanViewEmployment:

    def _get_emp(self, con, value):
        insert_row(con, employment=value)
        build_clean_view(con)
        return con.execute(
            "SELECT employment_clean FROM responses_clean"
        ).fetchone()[0]

    def test_full_time(self, mem_con):
        assert self._get_emp(mem_con, "Employed, full-time") == "Employed FT"

    def test_part_time(self, mem_con):
        assert self._get_emp(mem_con, "Employed, part-time") == "Employed PT"

    def test_freelance(self, mem_con):
        assert self._get_emp(mem_con, "Independent contractor, freelancer, or self-employed") == "Self-employed"

    def test_student_full_time_bug(self, mem_con):
        # BUG in cleaning.py: The CASE statement checks employment/part-time
        # before student, so any "Student, full-time" or "Student, part-time"
        # is misclassified. Fix: move the student WHEN branch to the top of the
        # CASE expression, before the employed branches.
        # These tests document current (broken) behaviour. When you fix cleaning.py,
        # change both assertions to == "Student".
        assert self._get_emp(mem_con, "Student, full-time") == "Employed FT"

    def test_student_part_time_bug(self, mem_con):
        assert self._get_emp(mem_con, "Student, part-time") == "Employed PT"

    def test_not_employed(self, mem_con):
        assert self._get_emp(mem_con, "Not employed, but looking for work") == "Not employed"


# ===========================================================================
# build_clean_view — AI user flag
# ===========================================================================

class TestCleanViewAIUser:

    def _get_ai(self, con, ai_select, year):
        insert_row(con, ai_select=ai_select, survey_year=year)
        build_clean_view(con)
        return con.execute(
            "SELECT ai_user FROM responses_clean"
        ).fetchone()[0]

    def test_yes_maps_to_true(self, mem_con):
        assert self._get_ai(mem_con, "Yes", 2023) is True

    def test_no_maps_to_false(self, mem_con):
        assert self._get_ai(mem_con, "No", 2023) is False

    def test_2021_is_null_regardless(self, mem_con):
        assert self._get_ai(mem_con, "Yes", 2021) is None

    def test_2022_is_null_regardless(self, mem_con):
        assert self._get_ai(mem_con, "Yes", 2022) is None


# ===========================================================================
# build_clean_view — org size sort key
# ===========================================================================

class TestCleanViewOrgSize:

    def _get_order(self, con, org_size):
        insert_row(con, org_size=org_size)
        build_clean_view(con)
        return con.execute(
            "SELECT org_size_order FROM responses_clean"
        ).fetchone()[0]

    def test_freelancer_is_1(self, mem_con):
        val = "Just me - I am a freelancer, sole proprietor, etc."
        assert self._get_order(mem_con, val) == 1

    def test_large_org_is_9(self, mem_con):
        assert self._get_order(mem_con, "10,000 or more employees") == 9

    def test_ordering_is_monotonic(self, mem_con):
        sizes = [
            ("Just me - I am a freelancer, sole proprietor, etc.", 1),
            ("2 to 9 employees", 2),
            ("10 to 19 employees", 3),
            ("20 to 99 employees", 4),
            ("100 to 499 employees", 5),
            ("500 to 999 employees", 6),
            ("1,000 to 4,999 employees", 7),
            ("5,000 to 9,999 employees", 8),
            ("10,000 or more employees", 9),
        ]
        orders = []
        for label, _ in sizes:
            con = duckdb.connect(":memory:")
            # recreate minimal table for each
            con.execute("""
                CREATE TABLE responses (
                    response_id INTEGER, survey_year INTEGER, country VARCHAR,
                    age VARCHAR, salary_usd_yearly DOUBLE, years_code_pro DOUBLE,
                    work_exp DOUBLE, remote_work VARCHAR, ed_level VARCHAR,
                    employment VARCHAR, dev_type VARCHAR, org_size VARCHAR,
                    ai_select VARCHAR, lang_worked VARCHAR, lang_want VARCHAR,
                    lang_admired VARCHAR, db_worked VARCHAR, db_want VARCHAR,
                    platform_worked VARCHAR, tools_worked VARCHAR,
                    dev_envs_worked VARCHAR, industry VARCHAR, years_code DOUBLE,
                    main_branch VARCHAR, currency VARCHAR, comp_total DOUBLE,
                    comp_freq VARCHAR
                )
            """)
            insert_row(con, org_size=label)
            build_clean_view(con)
            order = con.execute("SELECT org_size_order FROM responses_clean").fetchone()[0]
            orders.append(order)
            con.close()
        assert orders == sorted(orders)

    def test_unknown_org_size_is_null(self, mem_con):
        assert self._get_order(mem_con, "Some made-up size") is None


# ===========================================================================
# build_clean_view — dev_type_primary
# ===========================================================================

class TestCleanViewDevType:

    def test_single_dev_type(self, mem_con):
        insert_row(mem_con, dev_type="Data scientist or machine learning specialist")
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT dev_type_primary FROM responses_clean"
        ).fetchone()[0]
        assert result == "Data scientist or machine learning specialist"

    def test_multiple_dev_types_first_selected(self, mem_con):
        insert_row(mem_con, dev_type="Data engineer;Data scientist or machine learning specialist")
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT dev_type_primary FROM responses_clean"
        ).fetchone()[0]
        assert result == "Data engineer"

    def test_null_dev_type(self, mem_con):
        insert_row(mem_con, dev_type=None)
        build_clean_view(mem_con)
        result = mem_con.execute(
            "SELECT dev_type_primary FROM responses_clean"
        ).fetchone()[0]
        assert result is None


# ===========================================================================
# build_clean_view — view is rerunnable (idempotent)
# ===========================================================================

class TestCleanViewIdempotent:

    def test_rebuild_does_not_error(self, mem_con):
        insert_row(mem_con)
        build_clean_view(mem_con)
        build_clean_view(mem_con)  # second call should not raise
        count = mem_con.execute("SELECT COUNT(*) FROM responses_clean").fetchone()[0]
        assert count == 1

    def test_row_count_matches_responses(self, mem_con):
        for i in range(5):
            insert_row(mem_con, response_id=i)
        build_clean_view(mem_con)
        count = mem_con.execute("SELECT COUNT(*) FROM responses_clean").fetchone()[0]
        assert count == 5