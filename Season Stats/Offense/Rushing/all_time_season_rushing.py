#!/usr/bin/env python3
import argparse
import sys
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import List
from pathlib import Path

URL = 'https://hurstathletics.com/sports/football/stats/2025'
BASE_URL = 'https://hurstathletics.com/sports/football/stats'


def _find_rushing_table_from_dfs(dfs):
    for df in dfs:
        cols = [str(c).strip().lower() for c in df.columns]
        joined = ' '.join(cols)
        # look for rushing-related headers and yards
        if (('rush' in joined or 'rushes' in joined or 'rsh' in joined or 'car' in joined or 'att' in joined) and
                ('yds' in joined or 'yards' in joined)):
            return df
    return None


def scrape_rushing(url=URL):
    r = requests.get(url, timeout=15)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, 'html.parser')
    # Prefer the rushing section if present
    section = soup.find(id='individual-offense-rushing')
    if section:
        tbl = section.find('table')
        if tbl:
            headers = []
            thead = tbl.find('thead')
            if thead:
                headers = [th.get_text(strip=True) for th in thead.find_all(['th', 'td'])]
                rows = tbl.find('tbody').find_all('tr') if tbl.find('tbody') else tbl.find_all('tr')
            else:
                rows = tbl.find_all('tr')
                if rows:
                    first = rows[0]
                    hdr_cells = first.find_all(['th', 'td'])
                    headers = [c.get_text(strip=True) for c in hdr_cells]
                    rows = rows[1:]

            parsed_rows = []
            for tr in rows:
                cells = [c.get_text(strip=True) for c in tr.find_all(['td', 'th'])]
                if cells:
                    parsed_rows.append(cells)

            if parsed_rows:
                df_tbl = pd.DataFrame(parsed_rows)
                if headers and len(headers) == df_tbl.shape[1]:
                    df_tbl.columns = headers
                return df_tbl

    tables = []
    for tbl in soup.find_all('table'):
        headers = []
        thead = tbl.find('thead')
        if thead:
            headers = [th.get_text(strip=True) for th in thead.find_all(['th', 'td'])]
            rows = tbl.find('tbody').find_all('tr') if tbl.find('tbody') else tbl.find_all('tr')
        else:
            rows = tbl.find_all('tr')
            if rows:
                first = rows[0]
                hdr_cells = first.find_all(['th', 'td'])
                headers = [c.get_text(strip=True) for c in hdr_cells]
                rows = rows[1:]

        parsed_rows = []
        for tr in rows:
            cells = [c.get_text(strip=True) for c in tr.find_all(['td', 'th'])]
            if cells:
                parsed_rows.append(cells)

        if parsed_rows:
            df_tbl = pd.DataFrame(parsed_rows)
            if headers and len(headers) == df_tbl.shape[1]:
                df_tbl.columns = headers
            tables.append(df_tbl)

    df = _find_rushing_table_from_dfs(tables)
    if df is not None:
        return df

    for header_tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'strong']):
        txt = header_tag.get_text(strip=True).lower()
        if 'rush' in txt or 'rushing' in txt:
            tbl = header_tag.find_next('table')
            if tbl:
                headers = []
                thead = tbl.find('thead')
                if thead:
                    headers = [th.get_text(strip=True) for th in thead.find_all(['th', 'td'])]
                    rows = tbl.find('tbody').find_all('tr') if tbl.find('tbody') else tbl.find_all('tr')
                else:
                    rows = tbl.find_all('tr')
                    if rows:
                        first = rows[0]
                        hdr_cells = first.find_all(['th', 'td'])
                        headers = [c.get_text(strip=True) for c in hdr_cells]
                        rows = rows[1:]

                parsed_rows = []
                for tr in rows:
                    cells = [c.get_text(strip=True) for c in tr.find_all(['td', 'th'])]
                    if cells:
                        parsed_rows.append(cells)

                if parsed_rows:
                    df_tbl = pd.DataFrame(parsed_rows)
                    if headers and len(headers) == df_tbl.shape[1]:
                        df_tbl.columns = headers
                    return df_tbl

    return pd.DataFrame()


def check_year(year: int, timeout: int = 10) -> bool:
    url = f"{BASE_URL}/{year}"
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
    except Exception:
        return False

    df = scrape_rushing(url)
    return not df.empty


def list_available_years(start: int = 2012, end: int = 2025) -> List[int]:
    years = []
    for y in range(start, end + 1):
        if check_year(y):
            years.append(y)
    return years


def main():
    parser = argparse.ArgumentParser(description='Scrape Mercyhurst rushing stats')
    parser.add_argument('--url', default=URL, help='Stats page URL')
    parser.add_argument('--year', type=int, help='Year to fetch (overrides --url)')
    parser.add_argument('--list-years', action='store_true', help='List available years on the site')
    parser.add_argument('--start-year', type=int, default=2012, help='Start year for listing')
    parser.add_argument('--end-year', type=int, default=2025, help='End year for listing')
    parser.add_argument('-o', '--output', default='mercyhurst_rushing_2025.csv')
    parser.add_argument('-f', '--format', choices=['csv', 'json'], default='csv')
    parser.add_argument('--append-to', help='Append results to a master CSV file (adds Year column)')
    parser.add_argument('--append-years', action='store_true', help='Find available years and append missing ones to the master CSV')
    args = parser.parse_args()

    if args.list_years:
        years = list_available_years(args.start_year, args.end_year)
        if years:
            print('\n'.join(str(y) for y in years))
            sys.exit(0)
        else:
            print('No years found in the specified range.', file=sys.stderr)
            sys.exit(1)

    if args.year:
        args.url = f"{BASE_URL}/{args.year}"

    df = scrape_rushing(args.url)
    if df.empty:
        print('No rushing table found on the page.', file=sys.stderr)
        sys.exit(2)

    df.columns = [str(c).strip() for c in df.columns]

    drop_cols = [c for c in df.columns if 'bio' in c.lower() or 'link' in c.lower()]
    if drop_cols:
        df = df.drop(columns=drop_cols, errors='ignore')

    player_col = None
    for c in df.columns:
        if 'player' in c.lower() or 'name' in c.lower():
            player_col = c
            break
    if player_col is None and len(df.columns) >= 2:
        player_col = df.columns[1]

    name_pattern = re.compile(r"([A-Za-z][A-Za-z'\-\. ]+?,\s*[A-Za-z][A-Za-z'\-\. ]+)")

    def extract_name(s):
        if not isinstance(s, str):
            return s
        matches = name_pattern.findall(s)
        if matches:
            return matches[-1].strip()
        cleaned = re.sub(r"\d+", "", s).strip()
        return cleaned

    if player_col:
        df[player_col] = df[player_col].astype(str).apply(extract_name)

    year_val = None
    if args.year:
        year_val = args.year
    else:
        m = re.search(r"/(\d{4})/?$", args.url)
        if m:
            year_val = int(m.group(1))

    if year_val is not None:
        df['Year'] = year_val

    # Normalize and select rushing columns: Name, GP, ATT, Net, avg, TD, Long, AVG/G
    def find_col(keys):
        keys = [k.lower() for k in keys]
        for c in df.columns:
            lc = str(c).lower()
            for k in keys:
                if k in lc:
                    return c
        return None

    out = pd.DataFrame()
    # detect jersey/number column
    jersey_col = find_col(['#', 'no', 'no.', 'number', 'jersey'])
    if jersey_col is not None:
        out['No'] = df[jersey_col].astype(str).str.replace('#', '').str.strip()
    else:
        out['No'] = ''

    if player_col:
        out['Name'] = df[player_col]
    else:
        out['Name'] = df.iloc[:, 0]

    # candidates for each desired stat
    candidates = {
        'GP': ['gp', 'g', 'games', 'games played'],
        'ATT': ['att', 'car', 'rushes', 'attempts', 'rsh', 'att.'],
        'Net': ['net', 'yds', 'yards', 'rush yards', 'net yds', 'rushing yards'],
        'avg': ['avg', 'avg/rush', 'yards per rush', 'yds/att', 'avg.'],
        'TD': ['td', 't', 'touchdowns'],
        'Long': ['long', 'lng', 'longest'],
        'AVG/G': ['avg/g', 'avg per game', 'yds/g', 'avg/gm', 'avg/game']
    }

    for out_col, keys in candidates.items():
        col = find_col(keys)
        if col is not None:
            out[out_col] = df[col]
        else:
            out[out_col] = ''

    # Convert numeric fields
    for ncol in ['GP', 'ATT', 'Net', 'avg', 'TD', 'Long', 'AVG/G']:
        if ncol in out.columns:
            out[ncol] = pd.to_numeric(out[ncol].astype(str).str.replace(',', '' ).replace('', pd.NA), errors='coerce')

    # Compute missing average (yards per carry) if possible
    if 'avg' in out.columns and out['avg'].isna().all() and 'Net' in out.columns and 'ATT' in out.columns:
        mask = out['ATT'] > 0
        out.loc[mask, 'avg'] = out.loc[mask, 'Net'] / out.loc[mask, 'ATT']

    # Compute AVG/G (yards per game) if missing and GP available
    if 'AVG/G' in out.columns and out['AVG/G'].isna().all():
        if 'GP' in out.columns and 'Net' in out.columns:
            mask = out['GP'] > 0
            out.loc[mask, 'AVG/G'] = out.loc[mask, 'Net'] / out.loc[mask, 'GP']

    # Round averages to one decimal where numeric
    if 'avg' in out.columns:
        out['avg'] = out['avg'].round(1)
    if 'AVG/G' in out.columns:
        out['AVG/G'] = out['AVG/G'].round(1)

    # Ensure output columns order and keep Year if present
    cols_order = ['No', 'Name', 'GP', 'ATT', 'Net', 'avg', 'TD', 'Long', 'AVG/G']
    if 'Year' in df.columns:
        cols_order.append('Year')
        out['Year'] = df['Year']
    # keep only columns that exist
    out = out[[c for c in cols_order if c in out.columns]]

    df = out

    if args.format == 'csv':
        df.to_csv(args.output, index=False)
    else:
        df.to_json(args.output, orient='records')

    print('Saved', args.output)

    if args.append_to:
        master = Path(args.append_to)
        write_header = not master.exists()

        if master.exists():
            try:
                existing_cols = list(pd.read_csv(master, nrows=0).columns)
                for c in existing_cols:
                    if c not in df.columns:
                        df[c] = ''
                new_cols = [c for c in df.columns if c not in existing_cols]
                df = df[existing_cols + new_cols]
            except Exception:
                pass

        df.to_csv(master, mode='a', index=False, header=write_header)
        print('Appended to', str(master))

    if args.append_years:
        if not args.append_to:
            print('--append-years requires --append-to to be specified', file=sys.stderr)
            sys.exit(3)

        years = list_available_years(args.start_year, args.end_year)
        if not years:
            print('No available years found to append.', file=sys.stderr)
            sys.exit(0)

        master = Path(args.append_to)
        existing_years = set()
        if master.exists():
            try:
                existing_years = set(pd.read_csv(master)['Year'].dropna().astype(int).unique())
            except Exception:
                existing_years = set()

        to_add = [y for y in years if y not in existing_years]
        if not to_add:
            print('No new years to append. Master already contains:', ','.join(map(str, sorted(existing_years))))
            sys.exit(0)

        for y in to_add:
            print('Fetching and appending year', y)
            try:
                df_y = scrape_rushing(f"{BASE_URL}/{y}")
                if df_y.empty:
                    print('No data for', y, file=sys.stderr)
                    continue

                df_y.columns = [str(c).strip() for c in df_y.columns]
                drop_cols = [c for c in df_y.columns if 'bio' in c.lower() or 'link' in c.lower()]
                if drop_cols:
                    df_y = df_y.drop(columns=drop_cols, errors='ignore')

                player_col = None
                for c in df_y.columns:
                    if 'player' in c.lower() or 'name' in c.lower():
                        player_col = c
                        break
                if player_col is None and len(df_y.columns) >= 2:
                    player_col = df_y.columns[1]

                if player_col:
                    df_y[player_col] = df_y[player_col].astype(str).apply(extract_name)

                df_y['Year'] = y

                write_header = not master.exists()
                if master.exists():
                    try:
                        existing_cols = list(pd.read_csv(master, nrows=0).columns)
                        for c in existing_cols:
                            if c not in df_y.columns:
                                df_y[c] = ''
                        new_cols = [c for c in df_y.columns if c not in existing_cols]
                        df_y = df_y[existing_cols + new_cols]
                    except Exception:
                        pass

                df_y.to_csv(master, mode='a', index=False, header=write_header)
                print('Appended year', y)
            except Exception as e:
                print('Error fetching/appending', y, str(e), file=sys.stderr)


if __name__ == '__main__':
    main()
