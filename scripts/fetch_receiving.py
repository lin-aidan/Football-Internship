#!/usr/bin/env python3
"""Fetch Mercyhurst receiving stats from hurstathletics.com for 2012-2025.

Writes/appends rows to "Season Stats/Offense/Receiving/all_mercyhurst_receiving.csv".
Columns: year, #, Name, GP, NO, yds, avg, td, long, AVG/G
"""
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE_URL = "https://hurstathletics.com/sports/football/stats/{year}"
OUT_PATH = os.path.join("Season Stats", "Offense", "Receiving", "all_mercyhurst_receiving.csv")
YEARS = list(range(2012, 2026))

WANTED = ["#", "Name", "GP", "NO", "YDS", "AVG", "TD", "LONG", "AVG/G"]


def normalize_header(h):
    if not h:
        return ""
    return h.strip().upper().replace('\xa0', ' ')


def find_table(soup):
    # choose a table whose header contains the key columns like NO and YDS
    for table in soup.find_all('table'):
        headers = []
        thead = table.find('thead')
        if thead:
            for th in thead.find_all(['th', 'td']):
                headers.append(normalize_header(th.get_text()))
        else:
            # try first row
            first_row = table.find('tr')
            if first_row:
                for cell in first_row.find_all(['th', 'td']):
                    headers.append(normalize_header(cell.get_text()))

        hdrset = set([h for h in headers if h])
        if 'NO' in hdrset and ('YDS' in hdrset or 'YDS.' in hdrset):
            return table, headers
    return None, []


def parse_table(table, headers):
    rows = []
    # build index map (normalized header -> index)
    idx_map = {normalize_header(h): i for i, h in enumerate(headers)}

    def find_index(possible):
        for p in possible:
            i = idx_map.get(normalize_header(p))
            if i is not None:
                return i
        return None

    tbody = table.find('tbody') or table
    for tr in tbody.find_all('tr'):
        cols = [c.get_text().strip() for c in tr.find_all(['td', 'th'])]
        if not cols or all(not c for c in cols):
            continue

        # skip header-like rows
        if any(normalize_header(c) in ('PLAYER', 'RECEIVING') for c in cols):
            continue

        def get_by_keys(keys):
            i = find_index(keys)
            if i is None or i >= len(cols):
                return ''
            return cols[i]

        def clean_name(raw):
            if not raw:
                return ''
            parts = [p.strip() for p in raw.splitlines() if p.strip()]
            for p in parts:
                if ',' in p:
                    return p
            return parts[0] if parts else raw

        entry = {
            '#': get_by_keys(['#', 'NO', 'NUMBER']),
            'Name': clean_name(get_by_keys(['NAME', 'PLAYER'])),
            'GP': get_by_keys(['GP', 'G']),
            'NO': get_by_keys(['NO', 'REC', 'REC.']),
            'yds': get_by_keys(['YDS', 'YDS.']),
            'avg': get_by_keys(['AVG']),
            'td': get_by_keys(['TD']),
            'long': get_by_keys(['LONG', 'LG']),
            'AVG/G': get_by_keys(['AVG/G', 'AVG/G.']),
        }
        rows.append(entry)
    return rows


def fetch_year(year):
    url = BASE_URL.format(year=year)
    print(f"Fetching {url}")
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    table, headers = find_table(soup)
    if table is None:
        print(f"No receiving table found for {year}")
        return []
    rows = parse_table(table, headers)
    for row in rows:
        row['year'] = year
    return rows


def main():
    all_rows = []
    for year in YEARS:
        try:
            rows = fetch_year(year)
            print(f"Found {len(rows)} rows for {year}")
            all_rows.extend(rows)
        except Exception as e:
            print(f"Error fetching {year}: {e}")
        time.sleep(1.0)

    if not all_rows:
        print("No data collected.")
        return

    df = pd.DataFrame(all_rows)
    # reorder columns
    cols = ['year', '#', 'Name', 'GP', 'NO', 'yds', 'avg', 'td', 'long', 'AVG/G']
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    df = df[cols]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    write_header = not os.path.exists(OUT_PATH)
    df.to_csv(OUT_PATH, mode='a', index=False, header=write_header)
    print(f"Appended {len(df)} rows to {OUT_PATH}")


if __name__ == '__main__':
    main()
