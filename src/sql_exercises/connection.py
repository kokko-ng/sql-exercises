"""Database Connection Utilities.

Provides functions to connect to the practice database.
"""

import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional

# Path to databases
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_DATABASES_DIR = _PROJECT_ROOT / "data" / "databases"


def get_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Get a connection to the practice database.

    Args:
        read_only: If True, open database in read-only mode (default).
                   Set to False only for data modification exercises.

    Returns:
        DuckDB connection object.

    Example:
        >>> conn = get_connection()
        >>> result = conn.execute("SELECT * FROM employees LIMIT 5").fetchdf()
    """
    db_path = _DATABASES_DIR / "practice.duckdb"

    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found at {db_path}. "
            f"Run 'python data/scripts/init_database.py' to create it."
        )

    return duckdb.connect(str(db_path), read_only=read_only)


def get_table_info(
    table_name: str, conn: Optional[duckdb.DuckDBPyConnection] = None
) -> str:
    """Get formatted information about a table's schema.

    Args:
        table_name: Name of the table to describe.
        conn: Optional existing connection. If None, creates a new one.

    Returns:
        Formatted string with table schema information.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        result = conn.execute(f"DESCRIBE {table_name}").fetchdf()
        table_str: str = result.to_string(index=False)
        return table_str
    finally:
        if close_conn:
            conn.close()


def list_tables(conn: Optional[duckdb.DuckDBPyConnection] = None) -> list[str]:
    """List all tables in the database.

    Args:
        conn: Optional existing connection. If None, creates a new one.

    Returns:
        List of table names.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        result = conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]
    finally:
        if close_conn:
            conn.close()


def preview_table(
    table_name: str, limit: int = 5, conn: Optional[duckdb.DuckDBPyConnection] = None
) -> pd.DataFrame:
    """Preview rows from a table.

    Args:
        table_name: Name of the table to preview.
        limit: Number of rows to show (default 5).
        conn: Optional existing connection.

    Returns:
        DataFrame with sample rows.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        return conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchdf()
    finally:
        if close_conn:
            conn.close()
