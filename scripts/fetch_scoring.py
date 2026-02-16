#!/usr/bin/env python3
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

BASE = "https://hurstathletics.com/sports/football/stats"
YEARS = list(range(2012, 2026))
OUT_PATH = Path("/workspaces/Football-Internship/Season Stats/Offense/all_mercyhurst_scoring.csv")

WANT_COLS = ["#", "Name", "TD", "FG", "SAF", "KICK", "RUSH", "RCV", "PASS", "DXP", "PTS"]


def find_scoring_table(soup):
    tables = soup.find_all("table")
    for table in tables:
        thead = table.find("thead")
        if not thead:
            continue
        rows = thead.find_all("tr")
        if len(rows) < 2:
            continue
        # check second header row for key columns
        second = [th.get_text(strip=True) for th in rows[1].find_all(["th", "td"])]
        if any(k in second for k in ("PTS", "TD", "KICK")):
            return table, second
    return None, None


def parse_table(table, header_row, year):
    # normalize header texts and map desired columns to indices by substring matching
    def norm(s):
        return "".join(ch for ch in (s or "") if ch.isalnum()).upper()

    header_norms = [norm(h) for h in header_row]
    idx = {}
    for want in WANT_COLS:
        w = norm(want)
        found = None
        for i, hn in enumerate(header_norms):
            if w == hn or w in hn or hn in w:
                found = i
                break
        if found is not None:
            idx[want] = found
    rows = []
    tbody = table.find("tbody")
    if not tbody:
        return rows
    for tr in tbody.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if not cells:
            continue
        row = {}
        for col in WANT_COLS:
            if col in idx and idx[col] < len(cells):
                row[col] = cells[idx[col]]
            else:
                # fallback heuristics: try common positions
                row[col] = ""
        # clean Name: remove jersey digits and collapse duplicated halves if present
        name = row.get("Name", "")
        jersey = str(row.get("#", ""))
        if jersey:
            name = name.replace(jersey, "")
        # remove stray digits
        name = ''.join(ch for ch in name if not ch.isdigit())
        # collapse duplicated halves (simple heuristic)
        def collapse_dup(s):
            s = s.strip()
            L = len(s)
            for split in range(3, L//2 + 1):
                if s[:split] == s[split:2*split]:
                    return s[split:2*split].strip()
            return s
        name = collapse_dup(name)
        row["Name"] = name
        row["Year"] = year
        rows.append(row)
    return rows


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

    # Try to parse tables with pandas to correctly handle multi-row headers
    try:
        dfs = pd.read_html(r.text)
    except Exception:
        soup = BeautifulSoup(r.text, "html5lib")
        table, header_row = find_scoring_table(soup)
        if table is None:
            return []
        return parse_table(table, header_row, year)

    for df in dfs:
        cols = df.columns
        if isinstance(cols, pd.MultiIndex):
            # take second row (level 1)
            header_row = [str(x).strip() for x in cols.get_level_values(1)]
        else:
            header_row = [str(x).strip() for x in cols]

        # check if this table looks like scoring
        if any(k in " ".join(header_row).upper() for k in ("PTS", "TD", "KICK")):
            # normalize header_row and build index mapping
            header_norms = ["".join(ch for ch in (h or "") if ch.isalnum()).upper() for h in header_row]
            idx = {}
            for want in WANT_COLS:
                w = "".join(ch for ch in want if ch.isalnum()).upper()
                for i, hn in enumerate(header_norms):
                    if w == hn or w in hn or hn in w:
                        idx[want] = i
                        break

            rows = []
            for _, rrow in df.iterrows():
                cells = [str(x).strip() for x in list(rrow)]
                row = {}
                for col in WANT_COLS:
                    if col in idx and idx[col] < len(cells):
                        row[col] = cells[idx[col]]
                    else:
                        row[col] = ""
                # heuristic: locate Name and jersey (#) if mapping failed or is messy
                # find candidate name cell (contains a comma like 'Last, First')
                if not row.get("Name") or (row.get("Name") and any(d.isdigit() for d in row.get("Name"))):
                    name_candidate = ""
                    for c in cells:
                        if "," in c and any(ch.isalpha() for ch in c):
                            name_candidate = c
                            break
                    if name_candidate:
                        row["Name"] = name_candidate

                if not row.get("#"):
                    num_candidate = ""
                    for c in cells:
                        if c.isdigit() and len(c) <= 3:
                            num_candidate = c
                            break
                    if num_candidate:
                        row["#"] = num_candidate

                # clean Name: remove jersey digits and collapse duplicated halves
                name = row.get("Name", "")
                jersey = str(row.get("#", ""))
                if jersey and jersey in name:
                    name = name.replace(jersey, "")
                name = ''.join(ch for ch in name if not ch.isdigit())
                def collapse_dup(s):
                    s = s.strip()
                    L = len(s)
                    for split in range(3, L//2 + 1):
                        if s[:split] == s[split:2*split]:
                            return s[split:2*split].strip()
                    return s
                name = collapse_dup(name)
                row["Name"] = name
                row["Year"] = year
                rows.append(row)

            return rows

    return []


def main():
    all_rows = []
    for year in YEARS:
        try:
            rows = fetch_year(year)
            print(f"{year}: found {len(rows)} rows")
            all_rows.extend(rows)
        except Exception as e:
            print(f"{year}: error {e}")
        time.sleep(1)

    if not all_rows:
        print("No data collected.")
        return

    df = pd.DataFrame(all_rows)
    # keep only wanted columns plus Year
    cols = WANT_COLS + ["Year"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(df)} rows to {OUT_PATH}")

    # Post-process written CSV to clean Name field reliably
    try:
        df2 = pd.read_csv(OUT_PATH, dtype=str).fillna("")
        def clean_name(s, jersey):
            s = str(s)
            if jersey and jersey in s:
                s = s.replace(jersey, "")
            s = ''.join(ch for ch in s if not ch.isdigit())
            s = s.strip()
            L = len(s)
            for split in range(3, L//2 + 1):
                if s[:split] == s[split:2*split]:
                    return s[split:2*split].strip()
            return s

        for i, row in df2.iterrows():
            jersey = str(row.get('#', ''))
            name = clean_name(row.get('Name', ''), jersey)
            if not name:
                # fallback: find any column value that looks like 'Last, First'
                for v in row.values:
                    v = str(v)
                    if ',' in v and any(ch.isalpha() for ch in v):
                        name = ''.join(ch for ch in v if not ch.isdigit()).strip()
                        break
            df2.at[i, 'Name'] = name

        df2.to_csv(OUT_PATH, index=False)
        print(f"Cleaned Name field and updated {OUT_PATH}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
