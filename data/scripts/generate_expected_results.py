#!/usr/bin/env python3
"""
Generate Expected Results

Runs solution queries and generates the expected_results JSON files
that the checker uses to validate student answers.

Usage:
    python generate_expected_results.py [notebook_name]

Examples:
    python generate_expected_results.py                    # Generate all
    python generate_expected_results.py 01_select_basics   # Generate one
"""

import sys
import json
import hashlib
import importlib.util
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SOLUTIONS_DIR = PROJECT_ROOT / "solutions"
EXPECTED_DIR = PROJECT_ROOT / "tests" / "expected_results"
DB_PATH = PROJECT_ROOT / "data" / "databases" / "practice.duckdb"


def load_solutions_module(notebook_name: str):
    """Load solutions module for a notebook."""
    solution_file = SOLUTIONS_DIR / f"{notebook_name}_solutions.py"
    if not solution_file.exists():
        return None

    spec = importlib.util.spec_from_file_location("solutions", solution_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def hash_dataframe(df):
    """Create deterministic hash of DataFrame."""
    if df.empty:
        return hashlib.sha256(b"empty").hexdigest()[:16]

    try:
        df_sorted = df.sort_values(by=list(df.columns)).reset_index(drop=True)
    except TypeError:
        df_sorted = df.astype(str).sort_values(by=list(df.columns)).reset_index(drop=True)

    content = df_sorted.to_csv(index=False)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def generate_expected_for_notebook(notebook_name: str, conn):
    """Generate expected results for a single notebook."""
    print(f"\nGenerating expected results for: {notebook_name}")

    module = load_solutions_module(notebook_name)
    if module is None:
        print(f"  No solutions file found: {notebook_name}_solutions.py")
        return False

    # Get hints if available
    hints = getattr(module, 'HINTS', {})

    # Find all exercise variables (ex_01, ex_02, etc.)
    exercises = {}
    for name in dir(module):
        if name.startswith('ex_'):
            query = getattr(module, name)
            if isinstance(query, str) and query.strip():
                exercises[name] = query

    if not exercises:
        print(f"  No exercises found in solutions file")
        return False

    # Generate expected results
    expected = {}
    for ex_name, query in sorted(exercises.items()):
        try:
            df = conn.execute(query).fetchdf()
            expected[ex_name] = {
                "hash": hash_dataframe(df),
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "hints": hints.get(ex_name, [])
            }
            print(f"  {ex_name}: {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            print(f"  {ex_name}: ERROR - {e}")
            return False

    # Write to JSON file
    output_file = EXPECTED_DIR / f"{notebook_name}.json"
    EXPECTED_DIR.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(expected, f, indent=2)

    print(f"  Wrote: {output_file}")
    return True


def main():
    import duckdb

    # Check database exists
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        print("Run 'python data/scripts/init_database.py' first.")
        sys.exit(1)

    conn = duckdb.connect(str(DB_PATH), read_only=True)

    # Determine which notebooks to process
    if len(sys.argv) > 1:
        notebooks = sys.argv[1:]
    else:
        # Find all solution files
        notebooks = [
            f.stem.replace('_solutions', '')
            for f in SOLUTIONS_DIR.glob('*_solutions.py')
        ]

    if not notebooks:
        print("No solution files found.")
        sys.exit(1)

    print(f"Processing {len(notebooks)} notebook(s)...")

    success = 0
    for notebook in sorted(notebooks):
        if generate_expected_for_notebook(notebook, conn):
            success += 1

    conn.close()

    print(f"\nDone! Generated expected results for {success}/{len(notebooks)} notebooks.")


if __name__ == "__main__":
    main()
