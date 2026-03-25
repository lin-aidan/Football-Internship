#!/usr/bin/env python3
"""Import selected CSVs into a single SQLite database.

Creates `single_game_stats.db` in the same directory and tables named after
the CSV filenames without the .csv extension.
"""
import csv
import os
import re
import sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_FILES = [
    'passing_game_stats.csv',
    'receiving_game_stats.csv',
    'rushing_game_stats.csv',
    'full_game_stats.csv',
    'defensive_game_stats.csv'
]
DB_NAME = os.path.join(HERE, 'single_game_stats.db')


def sanitize_col(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^0-9A-Za-z_]+", "", name)
    if name == "":
        name = "col"
    if re.match(r"^[0-9]", name):
        name = "c_" + name
    return name


def import_csv_to_table(conn: sqlite3.Connection, csv_path: str):
    table = os.path.splitext(os.path.basename(csv_path))[0]
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            print(f"Skipping empty file: {csv_path}")
            return

        cols = [sanitize_col(h) for h in headers]

        col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs});'
        conn.execute(create_sql)

        placeholders = ",".join(["?" for _ in cols])
        insert_sql = f'INSERT INTO "{table}" ({",".join([f"\"{c}\"" for c in cols])}) VALUES ({placeholders})'

        batch = []
        for row in reader:
            # Pad or trim row to match header length
            if len(row) < len(cols):
                row += [None] * (len(cols) - len(row))
            elif len(row) > len(cols):
                row = row[: len(cols)]
            batch.append(row)

        if batch:
            conn.executemany(insert_sql, batch)
        conn.commit()
        print(f"Imported {len(batch)} rows into table '{table}'")


def main():
    # Remove existing DB to ensure a fresh import
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)

    conn = sqlite3.connect(DB_NAME)
    try:
        for fn in CSV_FILES:
            path = os.path.join(HERE, fn)
            if not os.path.exists(path):
                print(f"File not found, skipping: {path}")
                continue
            import_csv_to_table(conn, path)
    finally:
        conn.close()

    print(f"Created database: {DB_NAME}")


if __name__ == '__main__':
    main()
