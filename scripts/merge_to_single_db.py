#!/usr/bin/env python3
"""
Merge all .db files under "Excel and DB Files" into a single SQLite database.

Behavior:
- Scans for .db files under `Excel and DB Files` (recursively).
- Verifies there are no duplicate table names across source DBs. If duplicates
  are found, the script aborts and lists the conflicts so nothing is overwritten.
- Writes all tables into `Excel and DB Files/DB files/all_tables.db` preserving
  each table's existing name.

Requires: pandas
Usage: python3 scripts/merge_to_single_db.py
"""
from pathlib import Path
import sqlite3
import sys
import pandas as pd


BASE_DIR = Path("Excel and DB Files")
OUT_DIR = BASE_DIR / "DB files"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_DB = OUT_DIR / "all_tables.db"


def gather_tables(db_path: Path):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [r[0] for r in cur.fetchall()]


def main():
    src_dbs = sorted([p for p in BASE_DIR.rglob('*.db') if p.resolve() != OUT_DB.resolve()])
    if not src_dbs:
        print("No source .db files found to merge.")
        return 0

    table_map = {}  # table_name -> list of db paths
    for db in src_dbs:
        tables = gather_tables(db)
        for t in tables:
            table_map.setdefault(t, []).append(db)

    # detect duplicates
    duplicates = {t: dbs for t, dbs in table_map.items() if len(dbs) > 1}
    if duplicates:
        print("Duplicate table names found across DBs. Aborting to avoid overwrite.")
        for t, dbs in duplicates.items():
            print(f"Table: {t}")
            for d in dbs:
                print(f"  - {d}")
        return 2

    # merge
    if OUT_DB.exists():
        OUT_DB.unlink()

    with sqlite3.connect(OUT_DB) as out_conn:
        for db in src_dbs:
            with sqlite3.connect(db) as src_conn:
                cur = src_conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [r[0] for r in cur.fetchall()]
                for t in tables:
                    df = pd.read_sql_query(f'SELECT * FROM "{t}"', src_conn)
                    df.to_sql(t, out_conn, index=False, if_exists='fail')
                    print(f"Copied table {t} from {db} -> {OUT_DB}")

    print(f"Merge complete. Output DB: {OUT_DB}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
