# SQL Query Mastery

Interactive SQL learning with Jupyter notebooks and automated testing.

## Quick Start

```bash
# 1. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt
pip install -e .

# 3. Initialize the practice database
python data/scripts/init_database.py

# 4. Start Jupyter
jupyter lab notebooks/
```

## Project Structure

```
sql-exercises/
├── notebooks/           # Jupyter notebooks for learning
│   ├── 00_setup_and_introduction.ipynb
│   ├── 01_select_basics.ipynb
│   └── ...
├── data/
│   ├── databases/       # DuckDB database files
│   └── scripts/         # Database init scripts
├── src/sql_exercises/   # Helper library
│   ├── checker.py       # check() function for validation
│   └── connection.py    # Database connection utilities
├── tests/               # Pytest tests
│   ├── conftest.py
│   └── expected_results/
└── solutions/           # Solution files (git-ignored)
```

## How It Works

1. Open a notebook in the `notebooks/` folder
2. Read the problem description for each exercise
3. Write your SQL query in the provided variable
4. Run `check("ex_01", ex_01)` to validate your answer

The checker compares your query results against expected outputs without revealing the answers.

## Topics Covered

1. SELECT Basics & Filtering
2. Sorting & Limiting
3. Aggregations (GROUP BY, HAVING)
4. Joins (INNER, LEFT, RIGHT, FULL, SELF, CROSS)
5. Subqueries
6. Set Operations (UNION, INTERSECT, EXCEPT)
7. String & Date Functions
8. CASE Statements
9. Common Table Expressions (CTEs)
10. Window Functions (ROW_NUMBER, RANK, etc.)
11. Advanced Window Functions (LAG, LEAD, running totals)
12. Recursive CTEs
13. Query Optimization
14. Complex Reporting Queries

## Databases

The practice database contains three datasets:

- **employees** - HR data (departments, employees, projects, reviews)
- **ecommerce** - Transactional data (customers, orders, products, reviews)
- **analytics** - Event/session data (users, sessions, page views, conversions)

## Validating Your Work

Use the `check()` function in notebooks to validate your SQL queries:

```python
from sql_exercises import check

ex_01 = '''
SELECT * FROM employees
'''
check("ex_01", ex_01)
```

The checker will show PASS/FAIL without revealing the expected answer.

## Adding New Exercises

1. Add solution queries to `solutions/<notebook>_solutions.py`
2. Run `python data/scripts/generate_expected_results.py <notebook>`
3. Create notebook with exercises in `notebooks/`
4. Add pytest tests in `tests/test_<notebook>.py`
