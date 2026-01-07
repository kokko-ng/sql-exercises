"""SQL Exercise Checker.

Validates student SQL query results against expected outcomes
without revealing the actual answers. Can be run standalone without pytest.
"""

import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import ModuleType, TracebackType
from typing import Any, Optional
import duckdb
import pandas as pd

try:
    from IPython.display import display, HTML

    IN_NOTEBOOK = True
except ImportError:
    IN_NOTEBOOK = False

# Paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_EXPECTED_RESULTS_DIR = _PROJECT_ROOT / "tests" / "expected_results"
_DATABASES_DIR = _PROJECT_ROOT / "data" / "databases"
_SOLUTIONS_DIR = _PROJECT_ROOT / "solutions"


def _display_html(html: str) -> None:
    """Display HTML in notebook or print fallback."""
    if IN_NOTEBOOK:
        display(HTML(html))
    else:
        # Strip HTML tags for terminal output
        import re

        text = re.sub(r"<[^>]+>", "", html)
        text = text.replace("&nbsp;", " ").strip()
        print(text)


def _hash_dataframe(df: pd.DataFrame) -> str:
    """Create a deterministic hash of a DataFrame.

    Normalizes the data by sorting to ensure consistent hashing
    regardless of row order.
    """
    if df.empty:
        return hashlib.sha256(b"empty").hexdigest()[:16]

    # Sort by all columns for deterministic ordering
    try:
        df_sorted = df.sort_values(by=list(df.columns)).reset_index(drop=True)
    except TypeError:
        # Handle unhashable types by converting to string
        df_sorted = (
            df.astype(str).sort_values(by=list(df.columns)).reset_index(drop=True)
        )

    # Convert to CSV string and hash
    content = df_sorted.to_csv(index=False)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def _get_result_signature(
    conn: duckdb.DuckDBPyConnection, query: str
) -> dict[str, Any]:
    """Execute query and get its result signature."""
    df = conn.execute(query).fetchdf()
    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "hash": _hash_dataframe(df),
    }


def _load_solutions_module(notebook_name: str) -> Optional[ModuleType]:
    """Dynamically load the solutions module for a notebook."""
    solutions_file = _SOLUTIONS_DIR / f"{notebook_name}_solutions.py"
    if not solutions_file.exists():
        return None

    spec = importlib.util.spec_from_file_location(
        f"{notebook_name}_solutions", solutions_file
    )
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class QueryChecker:
    """Validate SQL query results against expected outcomes.

    The checker compares:
    1. Column names and order
    2. Row count
    3. Content hash (to verify exact values without revealing them)
    """

    def __init__(self, notebook_name: str):
        """Initialize checker for a specific notebook.

        Args:
            notebook_name: Name like "01_select_basics" (without extension).
        """
        self.notebook_name = notebook_name
        self.conn = self._get_connection()
        self.expected = self._load_expected_results()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get read-only connection to practice database."""
        db_path = _DATABASES_DIR / "practice.duckdb"
        if not db_path.exists():
            raise FileNotFoundError(
                "Database not found. Run 'python data/scripts/init_database.py' first."
            )
        return duckdb.connect(str(db_path), read_only=True)

    def _load_expected_results(self) -> dict[str, Any]:
        """Load expected results for this notebook."""
        results_file = _EXPECTED_RESULTS_DIR / f"{self.notebook_name}.json"
        if results_file.exists():
            with open(results_file) as f:
                result: dict[str, Any] = json.load(f)
                return result
        return {}

    def _get_result_signature(self, df: pd.DataFrame) -> dict[str, Any]:
        """Get structural signature of query result."""
        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "hash": _hash_dataframe(df),
        }

    def check(self, exercise_name: str, query: str) -> bool:
        """Check if a query result matches the expected outcome.

        Args:
            exercise_name: Exercise identifier (e.g., "ex_01", "ex_02").
            query: The SQL query string to validate.

        Returns:
            True if the query produces the expected result, False otherwise.

        Example:
            >>> checker = QueryChecker("01_select_basics")
            >>> query = "SELECT * FROM employees WHERE salary > 50000"
            >>> checker.check("ex_01", query)
        """
        # Validate input
        if not query or not query.strip():
            self._display_error(
                f"Empty query for '{exercise_name}'. "
                f"Write your SQL query and try again."
            )
            return False

        # Check if expected result exists
        expected = self.expected.get(exercise_name)
        if expected is None:
            self._display_warning(
                f"No expected result found for '{exercise_name}'. "
                f"This exercise may not be set up yet."
            )
            return False

        # Execute query
        try:
            result_df = self.conn.execute(query).fetchdf()
        except duckdb.Error as e:
            self._display_error(f"SQL Error: {str(e)}")
            return False
        except Exception as e:
            self._display_error(f"Error executing query: {str(e)}")
            return False

        # Compare signatures
        actual_sig = self._get_result_signature(result_df)

        # Check each component
        column_match: bool = actual_sig["columns"] == expected["columns"]
        row_match: bool = actual_sig["row_count"] == expected["row_count"]
        hash_match: bool = actual_sig["hash"] == expected["hash"]

        passed: bool = column_match and row_match and hash_match

        if passed:
            self._display_success(exercise_name, actual_sig["row_count"])
        else:
            self._display_failure(exercise_name, expected, actual_sig)

        return passed

    def hint(self, exercise_name: str) -> None:
        """Display a hint for an exercise without revealing the answer.

        Args:
            exercise_name: Exercise identifier (e.g., "ex_01").
        """
        expected = self.expected.get(exercise_name, {})
        hints = expected.get("hints", [])

        if hints:
            html = f"""
            <div style="padding: 12px; background-color: #cce5ff;
                        border: 1px solid #b8daff; border-radius: 4px;
                        color: #004085; margin: 8px 0;">
                <strong>HINT</strong> for {exercise_name}:
                <ul style="margin: 8px 0 0 0; padding-left: 20px;">
                    {''.join(f'<li style="margin: 4px 0;">{h}</li>' for h in hints)}
                </ul>
            </div>
            """
            _display_html(html)
        else:
            self._display_warning(f"No hints available for '{exercise_name}'")

    def _display_success(self, exercise_name: str, row_count: int) -> None:
        """Display success message."""
        msg = f"Query returned {row_count} row(s) with correct results."
        html = f"""
        <div style="padding: 12px; background-color: #d4edda;
                    border: 1px solid #c3e6cb; border-radius: 4px;
                    color: #155724; margin: 8px 0;">
            <strong>PASS</strong> {exercise_name}: {msg}
        </div>
        """
        _display_html(html)

    def _display_failure(
        self, exercise_name: str, expected: dict[str, Any], actual: dict[str, Any]
    ) -> None:
        """Display failure message with helpful hints (but not answers)."""
        hints = []

        # Check columns first
        if actual["columns"] != expected["columns"]:
            if set(actual["columns"]) == set(expected["columns"]):
                hints.append(
                    f"Columns are correct but in wrong order. "
                    f"Expected order: {expected['columns']}"
                )
            else:
                missing = set(expected["columns"]) - set(actual["columns"])
                extra = set(actual["columns"]) - set(expected["columns"])
                if missing:
                    hints.append(f"Missing columns: {list(missing)}")
                if extra:
                    hints.append(f"Extra columns not expected: {list(extra)}")
                if not missing and not extra:
                    hints.append(f"Expected columns: {expected['columns']}")

        # Check row count
        elif actual["row_count"] != expected["row_count"]:
            diff = actual["row_count"] - expected["row_count"]
            exp_rows = expected["row_count"]
            act_rows = actual["row_count"]
            if diff > 0:
                hints.append(
                    f"Too many rows. Expected {exp_rows}, got {act_rows}. "
                    f"Check your WHERE conditions."
                )
            else:
                hints.append(
                    f"Too few rows. Expected {exp_rows}, got {act_rows}. "
                    f"Your filter may be too restrictive."
                )

        # Values don't match
        elif actual["hash"] != expected["hash"]:
            hints.append(
                "Row count and columns match, but values differ. "
                "Check your calculations, joins, or sorting."
            )

        # Add exercise-specific hints if available
        exercise_hints = expected.get("hints", [])
        if exercise_hints and len(hints) < 3:
            hints.append(f"Tip: {exercise_hints[0]}")

        html = f"""
        <div style="padding: 12px; background-color: #f8d7da; border: 1px solid #f5c6cb;
                    border-radius: 4px; color: #721c24; margin: 8px 0;">
            <strong>FAIL</strong> {exercise_name}
            <ul style="margin: 8px 0 0 0; padding-left: 20px;">
                {''.join(f'<li style="margin: 4px 0;">{hint}</li>' for hint in hints)}
            </ul>
        </div>
        """
        _display_html(html)

    def _display_error(self, message: str) -> None:
        """Display error message."""
        html = f"""
        <div style="padding: 12px; background-color: #fff3cd; border: 1px solid #ffeeba;
                    border-radius: 4px; color: #856404; margin: 8px 0;">
            <strong>ERROR</strong>: {message}
        </div>
        """
        _display_html(html)

    def _display_warning(self, message: str) -> None:
        """Display warning message."""
        html = f"""
        <div style="padding: 12px; background-color: #e2e3e5; border: 1px solid #d6d8db;
                    border-radius: 4px; color: #383d41; margin: 8px 0;">
            <strong>WARNING</strong>: {message}
        </div>
        """
        _display_html(html)

    def close(self) -> None:
        """Close database connection."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def __enter__(self) -> "QueryChecker":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit context manager and close connection."""
        self.close()


# Global checker instance cache
_checkers: dict[str, QueryChecker] = {}


def check(exercise_name: str, query: str, notebook: Optional[str] = None) -> bool:
    """Check a SQL query result against expected outcome.

    This is the main function students will use in notebooks.

    Args:
        exercise_name: Exercise identifier (e.g., "ex_01").
        query: The SQL query string to validate.
        notebook: Notebook name. If None, tries to infer from environment.

    Returns:
        True if query is correct, False otherwise.

    Example:
        >>> from sql_exercises import check
        >>>
        >>> ex_01 = '''
        ... SELECT first_name, last_name, salary
        ... FROM employees
        ... WHERE salary > 50000
        ... '''
        >>> check("ex_01", ex_01, notebook="01_select_basics")
    """
    # Try to infer notebook name if not provided
    if notebook is None:
        notebook = os.environ.get("SQL_NOTEBOOK_NAME", "unknown")

    # Get or create checker for this notebook
    if notebook not in _checkers:
        _checkers[notebook] = QueryChecker(notebook)

    return _checkers[notebook].check(exercise_name, query)


def hint(exercise_name: str, notebook: Optional[str] = None) -> None:
    """Display a hint for an exercise.

    Args:
        exercise_name: Exercise identifier (e.g., "ex_01").
        notebook: Notebook name. If None, tries to infer from environment.

    Example:
        >>> from sql_exercises import hint
        >>> hint("ex_01", notebook="01_select_basics")
    """
    if notebook is None:
        notebook = os.environ.get("SQL_NOTEBOOK_NAME", "unknown")

    if notebook not in _checkers:
        _checkers[notebook] = QueryChecker(notebook)

    _checkers[notebook].hint(exercise_name)


# =============================================================================
# Inline Test Runner (no pytest required)
# =============================================================================


class TestResult:
    """Hold results from a single test case."""

    def __init__(self, name: str, passed: bool, message: str = ""):
        """Initialize test result.

        Args:
            name: Name of the test.
            passed: Whether the test passed.
            message: Optional message about the result.
        """
        self.name = name
        self.passed = passed
        self.message = message


class TestRunner:
    """Simple test runner that validates solutions against expected results.

    Usage:
        runner = TestRunner()
        runner.run_notebook_tests("01_select_basics")
        runner.print_summary()
    """

    def __init__(self) -> None:
        """Initialize the test runner with database connection."""
        self.results: list[TestResult] = []
        self.conn = self._get_connection()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get read-only connection to practice database."""
        db_path = _DATABASES_DIR / "practice.duckdb"
        if not db_path.exists():
            raise FileNotFoundError(
                f"Database not found at {db_path}. "
                f"Run 'python data/scripts/init_database.py' first."
            )
        return duckdb.connect(str(db_path), read_only=True)

    def _load_expected(self, notebook_name: str) -> dict[str, Any]:
        """Load expected results for a notebook."""
        results_file = _EXPECTED_RESULTS_DIR / f"{notebook_name}.json"
        if results_file.exists():
            with open(results_file) as f:
                result: dict[str, Any] = json.load(f)
                return result
        return {}

    def test_query(self, name: str, query: str, expected: dict[str, Any]) -> TestResult:
        """Test a single query against expected results.

        Args:
            name: Test name (e.g., "ex_01").
            query: SQL query to execute.
            expected: Expected result signature.

        Returns:
            TestResult with pass/fail status.
        """
        try:
            actual = _get_result_signature(self.conn, query)

            # Compare signatures
            column_match = actual["columns"] == expected["columns"]
            row_match = actual["row_count"] == expected["row_count"]
            hash_match = actual["hash"] == expected["hash"]

            if column_match and row_match and hash_match:
                return TestResult(name, True, f"OK ({actual['row_count']} rows)")

            # Build failure message
            issues = []
            if not column_match:
                issues.append(
                    f"columns: got {actual['columns']}, expected {expected['columns']}"
                )
            if not row_match:
                issues.append(
                    f"rows: got {actual['row_count']}, expected {expected['row_count']}"
                )
            if not hash_match and column_match and row_match:
                issues.append("values differ")

            return TestResult(name, False, "; ".join(issues))

        except duckdb.Error as e:
            return TestResult(name, False, f"SQL Error: {e}")
        except Exception as e:
            return TestResult(name, False, f"Error: {e}")

    def run_notebook_tests(self, notebook_name: str) -> list[TestResult]:
        """Run all tests for a notebook by loading solutions and expected results.

        Args:
            notebook_name: Name like "01_select_basics".

        Returns:
            List of TestResult objects.
        """
        # Load solutions module
        solutions = _load_solutions_module(notebook_name)
        if solutions is None:
            print(f"ERROR: Solutions file not found for {notebook_name}")
            return []

        # Load expected results
        expected_results = self._load_expected(notebook_name)
        if not expected_results:
            print(f"ERROR: Expected results not found for {notebook_name}")
            return []

        # Find all exercise variables (ex_01, ex_02, etc.)
        exercises = []
        for attr in dir(solutions):
            if attr.startswith("ex_"):
                query = getattr(solutions, attr)
                if isinstance(query, str):
                    exercises.append((attr, query))

        # Sort by exercise number
        exercises.sort(key=lambda x: x[0])

        print(f"\n{'='*60}")
        print(f"Testing: {notebook_name}")
        print(f"{'='*60}")

        notebook_results = []
        for ex_name, query in exercises:
            expected = expected_results.get(ex_name)
            if expected is None:
                result = TestResult(ex_name, False, "No expected result defined")
            else:
                result = self.test_query(ex_name, query, expected)

            notebook_results.append(result)
            self.results.append(result)

            # Print result
            status = "✓ PASS" if result.passed else "✗ FAIL"
            print(f"  {status}: {ex_name} - {result.message}")

        return notebook_results

    def run_all_notebooks(self) -> None:
        """Run tests for all solution files found."""
        solution_files = list(_SOLUTIONS_DIR.glob("*_solutions.py"))

        if not solution_files:
            print("No solution files found in solutions/")
            return

        for solution_file in sorted(solution_files):
            notebook_name = solution_file.stem.replace("_solutions", "")
            self.run_notebook_tests(notebook_name)

    def print_summary(self) -> None:
        """Print summary of all test results."""
        if not self.results:
            print("\nNo tests were run.")
            return

        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed

        print(f"\n{'='*60}")
        print(f"SUMMARY: {passed} passed, {failed} failed, {len(self.results)} total")
        print(f"{'='*60}")

        if failed > 0:
            print("\nFailed tests:")
            for r in self.results:
                if not r.passed:
                    print(f"  - {r.name}: {r.message}")

    def close(self) -> None:
        """Close database connection."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def __enter__(self) -> "TestRunner":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit context manager and close connection."""
        self.close()


def run_tests(notebook_name: Optional[str] = None) -> bool:
    """Run tests without pytest.

    Args:
        notebook_name: Specific notebook to test, or None for all.

    Returns:
        True if all tests passed, False otherwise.

    Example:
        >>> from sql_exercises.checker import run_tests
        >>> run_tests()  # Run all tests
        >>> run_tests('01_select_basics')  # Run specific notebook
    """
    with TestRunner() as runner:
        if notebook_name:
            runner.run_notebook_tests(notebook_name)
        else:
            runner.run_all_notebooks()

        runner.print_summary()

        return all(r.passed for r in runner.results)


# =============================================================================
# Main entry point for command-line usage
# =============================================================================

if __name__ == "__main__":
    # Parse command line arguments
    notebook = sys.argv[1] if len(sys.argv) > 1 else None

    success = run_tests(notebook)
    sys.exit(0 if success else 1)
