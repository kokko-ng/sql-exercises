"""SQL Exercises Package.

Provides utilities for SQL learning exercises including:
- check(): Validate query results without revealing answers
- get_connection(): Get database connection for exercises
"""

from .checker import check, QueryChecker
from .connection import get_connection

__all__ = ["check", "QueryChecker", "get_connection"]
__version__ = "1.0.0"
