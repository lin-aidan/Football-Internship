#!/usr/bin/env python3
"""
Convert .xlsx files under "Excel and DB Files" into separate .db files.

Behavior:
- For each Excel file found, create a SQLite database file next to the Excel file
  with the same base name but a `.db` extension (whitespace replaced with `_`).
- The first sheet is written to a table named after the file (sanitized).
- If the Excel file has additional sheets, they are written as additional tables
  named `<filebase>_<sheetname>` (sanitized).

Requires: pandas
Usage: python scripts/xlsx_to_sqlite.py
"""
import re
from pathlib import Path
import sqlite3
import sys

try:
    import pandas as pd
except Exception as e:
    print("Missing dependency: pandas is required. Install with 'pip install pandas openpyxl'")
    raise


BASE_DIR = Path("Excel and DB Files")


def sanitize(name: str) -> str:
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^0-9A-Za-z_]", "_", name)
    if not name:
        return "table"
    if name[0].isdigit():
        name = "_" + name
    return name


def convert_file(xlsx_path: Path) -> None:
    filebase = sanitize(xlsx_path.stem)
    db_path = xlsx_path.with_suffix('.db')
    sheets = pd.read_excel(xlsx_path, sheet_name=None)
    created = []
    with sqlite3.connect(db_path) as conn:
        first = True
        for sheet_name, df in sheets.items():
            if first:
                table_name = filebase
                first = False
            else:
                table_name = sanitize(f"{filebase}_{sheet_name}")
            # Ensure DataFrame columns are safe for SQL by leaving as-is;
            # pandas will handle types. Replace NaNs with NULL automatically.
            df.to_sql(table_name, conn, index=False, if_exists='replace')
            created.append(table_name)
    print(f"Converted: {xlsx_path} -> {db_path} | tables: {', '.join(created)}")


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    if not BASE_DIR.exists():
        print(f"Base directory not found: {BASE_DIR}")
        return 1
    files = list(BASE_DIR.rglob('*.xlsx'))
    if not files:
        print("No .xlsx files found under 'Excel and DB Files'.")
        return 0
    for f in files:
        try:
            convert_file(f)
        except Exception as e:
            print(f"Failed to convert {f}: {e}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
