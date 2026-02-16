#!/usr/bin/env python3
import requests
import pandas as pd
import re
from pathlib import Path
from io import StringIO

BASE = "https://hurstathletics.com/sports/football/stats"
YEARS = list(range(2012, 2026))
OUT_PATH = Path("/workspaces/Football-Internship/Season Stats/Offense/all_mercyhurst_scoring.csv")
WANT_COLS = ["#", "Name", "TD", "FG", "SAF", "KICK", "RUSH", "RCV", "PASS", "DXP", "PTS"]


def col_label(col):
    # col may be tuple (level0, level1) or scalar
    if isinstance(col, tuple):
        # prefer level1 if present and different
        lvl0 = str(col[0]).strip()
        lvl1 = str(col[1]).strip()
        if lvl1 and lvl1 != lvl0:
            return lvl1
        return lvl0
    return str(col)


def norm(s):
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


def extract_name(cell):
    s = str(cell or "")
    # look for 'Last, First' pattern
    m = re.search(r"[A-Za-z\-\.']+,\s+[A-Za-z\-\.']+(?:\s+[A-Za-z\-\.']+)*", s)
    if m:
        return m.group(0).strip()
    # fallback: remove digits and collapse dup
    t = ''.join(ch for ch in s if not ch.isdigit()).strip()
    L = len(t)
    for n in range(3, L//2+1):
        if t[:n] == t[n:2*n]:
            return t[:n].strip()
    return t


def find_scoring_df(html):
    try:
        dfs = pd.read_html(StringIO(html), flavor='bs4')
    except Exception:
        dfs = pd.read_html(StringIO(html))

    for df in dfs:
        cols = df.columns
        labels = [col_label(c) for c in cols]
        # check for PTS header
        if any('PTS' in str(l).upper() for l in labels):
            df2 = df.copy()
            df2.columns = labels
            return df2
    return None


def fetch_year(year):
    url = f"{BASE}/{year}"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
    except Exception:
        try:
            r = requests.get(BASE, timeout=20)
            r.raise_for_status()
        except Exception:
            return []

    df = find_scoring_df(r.text)
    if df is None:
        return []

    rows = []
    for _, rrow in df.iterrows():
        cells = {col: str(rrow[col]) if col in rrow else '' for col in df.columns}
        out = {}
        # map WANT_COLS to columns by normalized substring
        labels_norm = {col: norm(col) for col in df.columns}
        for want in WANT_COLS:
            w = norm(want)
            matched = None
            for col, hn in labels_norm.items():
                if not hn:
                    continue
                if w == hn or w in hn or hn in w:
                    matched = col
                    break
            if matched:
                out[want] = cells.get(matched, '').strip()
            else:
                out[want] = ''

        # clean Name more precisely
        out['Name'] = extract_name(out.get('Name') or cells.get('Player') or '')
        out['#'] = re.search(r"\d{1,3}", str(out.get('#') or cells.get('#') or ''))
        out['#'] = out['#'].group(0) if out['#'] else ''
        out['Year'] = year
        rows.append(out)

    return rows


def main():
    all_rows = []
    for year in YEARS:
        rows = fetch_year(year)
        print(f"{year}: found {len(rows)} rows")
        all_rows.extend(rows)

    if not all_rows:
        print('No data')
        return

    df = pd.DataFrame(all_rows)
    cols = WANT_COLS + ['Year']
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    df = df[cols]
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print('Wrote', OUT_PATH)


if __name__ == '__main__':
    main()
