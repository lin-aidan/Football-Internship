#!/usr/bin/env python3
"""Fetch Mercyhurst kickoff-return stats from hurstathletics.com for 2012-2025.

Writes to "Season Stats/Individual/Special Teams/mercyhurst_kickoff_return.csv".
Columns: Name, NO, YDS, AVG, TD, Long, Year
"""
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE_URL = "https://hurstathletics.com/sports/football/stats/{year}"
OUT_PATH = os.path.join("Season Stats", "Individual", "Special Teams", "mercyhurst_kickoff_return.csv")
YEARS = list(range(2012, 2026))


def normalize_header(h):
    if not h:
        return ""
    return h.strip().upper().replace('\xa0', ' ')


def find_table(soup):
    # prefer explicit kickoff-return caption
    for table in soup.find_all('table'):
        cap = table.caption.get_text(strip=True) if table.caption else ''
        cap_u = cap.upper() if cap else ''
        if 'KICKOFF' in cap_u and 'RETURN' in cap_u:
            headers = []
            thead = table.find('thead')
            if thead:
                for th in thead.find_all(['th', 'td']):
                    headers.append(normalize_header(th.get_text()))
            else:
                first_row = table.find('tr')
                if first_row:
                    for cell in first_row.find_all(['th', 'td']):
                        headers.append(normalize_header(cell.get_text()))
            return table, headers

    # fallback: find a table with NO, YDS, AVG, TD and avoid receiving tables
    for table in soup.find_all('table'):
        headers = []
        thead = table.find('thead')
        if thead:
            for th in thead.find_all(['th', 'td']):
                headers.append(normalize_header(th.get_text()))
        else:
            first_row = table.find('tr')
            if first_row:
                for cell in first_row.find_all(['th', 'td']):
                    headers.append(normalize_header(cell.get_text()))

        hdrset = set([h for h in headers if h])
        if any('REC' in h for h in hdrset):
            continue
        if 'NO' in hdrset and 'YDS' in hdrset and 'AVG' in hdrset and 'TD' in hdrset:
            return table, headers
    return None, []


def parse_table(table, headers):
    rows = []
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

        if any(normalize_header(c) in ('PLAYER', 'KICKOFF RETURN') for c in cols):
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
            'Name': clean_name(get_by_keys(['NAME', 'PLAYER'])),
            'NO': get_by_keys(['NO']),
            'YDS': get_by_keys(['YDS']),
            'AVG': get_by_keys(['AVG']),
            'TD': get_by_keys(['TD']),
            'Long': get_by_keys(['LONG', 'LONG.'])
        }

        name_l = entry['Name'].lower() if entry['Name'] else ''
        if any(tok in name_l for tok in ('total', 'totals', 'team', 'opponent', 'opponents', 'opp')):
            continue
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
        print(f"No kickoff-return table found for {year}")
        return []
    rows = parse_table(table, headers)
    for row in rows:
        row['Year'] = year
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
    cols = ['Name', 'NO', 'YDS', 'AVG', 'TD', 'Long', 'Year']
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    df = df[cols]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, mode='w', index=False, header=True)
    print(f"Wrote {len(df)} rows to {OUT_PATH}")


if __name__ == '__main__':
    main()
