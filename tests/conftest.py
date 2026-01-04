"""
Pytest Configuration for SQL Exercises

Provides fixtures for database connections, solution loading, and query validation.
"""

import pytest
import duckdb
import hashlib
import json
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATABASES_DIR = PROJECT_ROOT / "data" / "databases"
SOLUTIONS_DIR = PROJECT_ROOT / "solutions"
EXPECTED_DIR = PROJECT_ROOT / "tests" / "expected_results"


@pytest.fixture(scope="session")
def db_connection():
    """
    Session-scoped database connection.

    Returns a read-only connection to the practice database.
    The same connection is reused across all tests in a session.
    """
    db_path = DATABASES_DIR / "practice.duckdb"

    if not db_path.exists():
        pytest.skip(
            f"Database not found at {db_path}. "
            f"Run 'python data/scripts/init_database.py' first."
        )

    conn = duckdb.connect(str(db_path), read_only=True)
    yield conn
    conn.close()


@pytest.fixture
def db(db_connection):
    """
    Alias for db_connection for convenience.

    Example:
        def test_query(db):
            result = db.execute("SELECT * FROM employees").fetchdf()
    """
    return db_connection


@pytest.fixture
def execute_query(db_connection):
    """
    Factory fixture to execute queries and return DataFrames.

    Example:
        def test_something(execute_query):
            df = execute_query("SELECT * FROM employees WHERE salary > 50000")
            assert len(df) > 0
    """
    def _execute(query: str):
        return db_connection.execute(query).fetchdf()
    return _execute


@pytest.fixture
def load_expected():
    """
    Factory fixture to load expected results for a notebook.

    Example:
        def test_exercise(load_expected):
            expected = load_expected("01_select_basics")
            ex_01 = expected["ex_01"]
    """
    def _load(notebook_name: str) -> dict:
        expected_file = EXPECTED_DIR / f"{notebook_name}.json"
        if not expected_file.exists():
            return {}
        with open(expected_file) as f:
            return json.load(f)
    return _load


class QueryValidator:
    """
    Helper class for validating SQL queries in tests.

    Provides various assertion methods for testing query results.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def execute(self, query: str):
        """Execute query and return DataFrame."""
        return self.conn.execute(query).fetchdf()

    def hash_result(self, query: str) -> str:
        """Get hash of query result for comparison."""
        df = self.execute(query)
        if df.empty:
            return hashlib.sha256(b"empty").hexdigest()[:16]

        try:
            df_sorted = df.sort_values(by=list(df.columns)).reset_index(drop=True)
        except TypeError:
            df_sorted = df.astype(str).sort_values(by=list(df.columns)).reset_index(drop=True)

        content = df_sorted.to_csv(index=False)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def assert_row_count(self, query: str, expected: int, msg: str = None):
        """Assert query returns expected number of rows."""
        df = self.execute(query)
        assert len(df) == expected, msg or f"Expected {expected} rows, got {len(df)}"

    def assert_columns(self, query: str, expected_columns: list, msg: str = None):
        """Assert query returns expected columns in order."""
        df = self.execute(query)
        assert list(df.columns) == expected_columns, \
            msg or f"Expected columns {expected_columns}, got {list(df.columns)}"

    def assert_column_exists(self, query: str, column: str, msg: str = None):
        """Assert a specific column exists in results."""
        df = self.execute(query)
        assert column in df.columns, msg or f"Column '{column}' not found in results"

    def assert_contains_value(self, query: str, column: str, value, msg: str = None):
        """Assert a specific value exists in a column."""
        df = self.execute(query)
        assert value in df[column].values, \
            msg or f"Value '{value}' not found in column '{column}'"

    def assert_not_contains_value(self, query: str, column: str, value, msg: str = None):
        """Assert a specific value does NOT exist in a column."""
        df = self.execute(query)
        assert value not in df[column].values, \
            msg or f"Value '{value}' should not be in column '{column}'"

    def assert_all_values_satisfy(self, query: str, column: str, condition, msg: str = None):
        """Assert all values in a column satisfy a condition."""
        df = self.execute(query)
        assert condition(df[column]).all(), \
            msg or f"Not all values in '{column}' satisfy the condition"

    def assert_no_duplicates(self, query: str, columns: list = None, msg: str = None):
        """Assert no duplicate rows (or in specified columns)."""
        df = self.execute(query)
        if columns:
            has_dupes = df[columns].duplicated().any()
        else:
            has_dupes = df.duplicated().any()
        assert not has_dupes, msg or "Duplicate rows found"

    def assert_sorted(self, query: str, column: str, ascending: bool = True, msg: str = None):
        """Assert results are sorted by column."""
        df = self.execute(query)
        if ascending:
            is_sorted = df[column].is_monotonic_increasing
        else:
            is_sorted = df[column].is_monotonic_decreasing
        direction = "ascending" if ascending else "descending"
        assert is_sorted, msg or f"Results not sorted by '{column}' {direction}"

    def assert_no_nulls(self, query: str, column: str, msg: str = None):
        """Assert no NULL values in a column."""
        df = self.execute(query)
        assert not df[column].isna().any(), \
            msg or f"NULL values found in column '{column}'"

    def assert_all_nulls(self, query: str, column: str, msg: str = None):
        """Assert all values in a column are NULL."""
        df = self.execute(query)
        assert df[column].isna().all(), \
            msg or f"Non-NULL values found in column '{column}'"

    def assert_empty(self, query: str, msg: str = None):
        """Assert query returns no rows."""
        df = self.execute(query)
        assert len(df) == 0, msg or f"Expected empty result, got {len(df)} rows"

    def assert_not_empty(self, query: str, msg: str = None):
        """Assert query returns at least one row."""
        df = self.execute(query)
        assert len(df) > 0, msg or "Expected non-empty result, got 0 rows"


@pytest.fixture
def validator(db_connection):
    """
    QueryValidator instance for the practice database.

    Example:
        def test_salary_filter(validator):
            query = "SELECT * FROM employees WHERE salary > 50000"
            validator.assert_all_values_satisfy(
                query, "salary", lambda x: x > 50000
            )
    """
    return QueryValidator(db_connection)


# Utility functions for generating expected results
def generate_expected_result(conn, query: str, hints: list = None) -> dict:
    """
    Generate expected result metadata for an exercise.

    This is used when creating new exercises to generate the
    expected_results JSON files.

    Args:
        conn: Database connection
        query: The solution SQL query
        hints: Optional list of hints for the exercise

    Returns:
        Dict with hash, row_count, columns, and hints
    """
    df = conn.execute(query).fetchdf()

    if df.empty:
        content_hash = hashlib.sha256(b"empty").hexdigest()[:16]
    else:
        try:
            df_sorted = df.sort_values(by=list(df.columns)).reset_index(drop=True)
        except TypeError:
            df_sorted = df.astype(str).sort_values(by=list(df.columns)).reset_index(drop=True)
        content = df_sorted.to_csv(index=False)
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

    return {
        "hash": content_hash,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "hints": hints or []
    }
