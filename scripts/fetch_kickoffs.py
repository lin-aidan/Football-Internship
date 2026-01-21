#!/usr/bin/env python3
"""Fetch Mercyhurst kickoff stats from hurstathletics.com for 2012-2025.

Writes rows to "Season Stats/Special Teams/all_mercyhurst_kickoffs.csv".
Columns: #, Name, NO, YDS, AVG, TB, OB, Year
"""
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE_URL = "https://hurstathletics.com/sports/football/stats/{year}"
OUT_PATH = os.path.join("Season Stats", "Special Teams", "all_mercyhurst_kickoffs.csv")
YEARS = list(range(2012, 2026))


def normalize_header(h):
    if not h:
        return ""
    return h.strip().upper().replace('\xa0', ' ')


def find_table(soup):
    # Prefer explicit caption mentioning kickoff/kickoff return
    for table in soup.find_all('table'):
        cap = table.caption.get_text(strip=True) if table.caption else ''
        cap_l = cap.lower() if cap else ''
        if 'kick' in cap_l or 'kickoff' in cap_l:
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

    # fallback: choose a table whose header contains NO and YDS but exclude receiving tables
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
        # avoid selecting receiving tables (they often contain REC/RECEPTIONS)
        if any('REC' in h for h in hdrset):
            continue
        # require NO and YDS and at least one kickoff-specific column like TB or OB
        if 'NO' in hdrset and ('YDS' in hdrset or 'YDS.' in hdrset) and any(x in hdrset for x in ('TB', 'OB', 'OUT', 'OUT-OF-BOUNDS')):
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

        if any(normalize_header(c) in ('PLAYER', 'KICKOFF') for c in cols):
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
            'NO': get_by_keys(['NO']),
            'YDS': get_by_keys(['YDS']),
            'AVG': get_by_keys(['AVG']),
            'TB': get_by_keys(['TB']),
            'OB': get_by_keys(['OB', 'O.B.', 'OUT', 'OUT-OF-BOUNDS'])
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
        print(f"No kickoff table found for {year}")
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
    cols = ['#', 'Name', 'NO', 'YDS', 'AVG', 'TB', 'OB', 'Year']
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    df = df[cols]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, mode='w', index=False, header=True)
    print(f"Wrote {len(df)} rows to {OUT_PATH}")


if __name__ == '__main__':
    main()
