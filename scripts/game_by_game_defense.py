#!/usr/bin/env python3
"""Scrape Game-by-Game -> Team -> Defense tables and write game_defense.csv

Usage examples:
  python scripts/game_by_game_defense.py --year 2012
  python scripts/game_by_game_defense.py --start-year 2012 --end-year 2025
"""
import argparse
import csv
import os
import re
import sys
import time

try:
    import requests
    from bs4 import BeautifulSoup, Comment
except Exception:
    raise SystemExit('This script requires `requests` and `beautifulsoup4`. Install via `pip install requests beautifulsoup4`')


def parse_date(text, default_year=None):
    t = (text or '').strip()
    if not t:
        return None
    # try common formats
    m = re.search(r'(\d{4}-\d{2}-\d{2})', t)
    if m:
        return m.group(1)
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', t)
    if m:
        s = m.group(1)
        parts = s.split('/')
        if len(parts) == 3:
            mm, dd, yy = parts
            if len(yy) == 2:
                yy = '20' + yy
            return f"{yy.zfill(4)}-{int(mm):02d}-{int(dd):02d}"
    return None


def extract_defense_from_page(html, year=None):
    soup = BeautifulSoup(html, 'html.parser')
    rows = []

    # prefer explicit section id or aria-label
    section = soup.find(id='gbg_team_defense')
    if not section:
        section = soup.find(attrs={'aria-label': lambda v: v and 'game-by-game' in v.lower() and 'team' in v.lower() and 'defense' in v.lower()})

    tables = []
    if section:
        t = section.find('table')
        if t:
            tables = [t]
    else:
        # try locating comment marker
        for c in soup.find_all(string=lambda text: isinstance(text, Comment)):
            txt = c.strip().lower()
            if 'game-by-game' in txt and 'team' in txt and 'defense' in txt:
                tbl = c.find_next('table')
                if tbl:
                    tables.append(tbl)
        if not tables:
            tables = soup.find_all('table')

    for table in tables:
        trs = table.find_all('tr')
        # require header rows
        header_trs = [tr for tr in trs if tr.find_all('th')]
        if len(header_trs) < 2:
            continue
        tbody = table.find('tbody')
        data_trs = tbody.find_all('tr') if tbody else [tr for tr in trs if tr.find_all('td')]

        for tr in data_trs:
            cells = tr.find_all(['td', 'th'])
            if len(cells) < 3:
                continue
            date_text = cells[0].get_text(' ', strip=True)
            date_val = parse_date(date_text, default_year=year)
            if not date_val:
                continue
            opponent = cells[1].get_text(' ', strip=True)
            # collect the rest of the numeric/text columns
            nums = [c.get_text(' ', strip=True).replace(',', '') for c in cells[2:]]
            rows.append({'date': date_val, 'opponent': opponent, 'numbers': nums})

    return rows


def scrape_year(year, url_template, timeout=15):
    url = url_template.format(year=year)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return extract_defense_from_page(resp.text, year=year)


def main():
    parser = argparse.ArgumentParser(description='Extract Game-By-Game Team Defense into CSV')
    parser.add_argument('--year', type=int, help='Single season year (e.g. 2012)')
    parser.add_argument('--start-year', type=int, default=2012)
    parser.add_argument('--end-year', type=int, default=2025)
    parser.add_argument('--csv', default=None, help='Output CSV path')
    parser.add_argument('--url-template', default='https://hurstathletics.com/sports/football/stats/{year}', help='URL template with {year}')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between requests')
    args = parser.parse_args()

    if args.csv:
        out_path = args.csv if os.path.isabs(args.csv) else os.path.abspath(args.csv)
    else:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        out_dir = os.path.join(repo_root, 'Season Stats', 'Team')
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, 'game_defense.csv')

    if args.year:
        start = end = args.year
    else:
        start = args.start_year
        end = args.end_year

    header = ['date', 'opponent', 'Solo', 'aST', 'TOT', 'TFL', 'YDS', 'TOT', 'YDS', 'FF', 'FR', 'YDS', 'TOT', 'YDS', 'QBH', 'Pass Brup', 'Blkd Kick', 'SAF']

    all_rows = []
    for y in range(start, end + 1):
        try:
            print(f'Fetching {y}...')
            rows = scrape_year(y, args.url_template)
            print(f'  -> {len(rows)} rows from {y}')
            all_rows.extend(rows)
        except Exception as e:
            print(f'Failed {y}: {e}', file=sys.stderr)
        time.sleep(args.delay)

    # write CSV: date, opponent, then the requested defense fields from numbers
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        needed = len(header) - 2
        for r in all_rows:
            nums = r.get('numbers', [])
            row = [r.get('date',''), r.get('opponent','')]
            row.extend(nums[:needed] + [''] * max(0, needed - len(nums)))
            writer.writerow(row)

    print(f'Wrote {len(all_rows)} rows to {out_path}')


if __name__ == '__main__':
    main()
