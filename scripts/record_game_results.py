#!/usr/bin/env python3
"""Fetch game-by-game results from hurstathletics season stats page and
rewrite the CSV at Season Stats/Individual/game_results.csv by default.

The scraper uses heuristics to find rows containing dates and scores. It's
robust to some variation in table layout but may require tweaks for edge
cases.

Usage examples:
  python scripts/record_game_results.py --year 2025
  python scripts/record_game_results.py --year 2025 --csv "Season Stats/Individual/game_results.csv"
  python scripts/record_game_results.py --year 2025 --url "https://hurstathletics.com/sports/football/stats/2025"
"""

import argparse
import csv
import os
import re
import sys
from datetime import datetime
import time

try:
    import requests
    from bs4 import BeautifulSoup, Comment
except Exception:
    raise SystemExit('This script requires `requests` and `beautifulsoup4`. Install via `pip install requests beautifulsoup4`')


DATE_PATTERNS = [
    '%Y-%m-%d',
    '%m/%d/%Y',
    '%m/%d',
    '%b %d, %Y',
    '%B %d, %Y',
    '%b %d',
    '%B %d',
]


def parse_date(text, default_year=None):
    t = text.strip()
    if not t:
        return None
    # try to find yyyy-mm-dd or mm/dd/yyyy with regex first
    m = re.search(r'(\d{4}-\d{2}-\d{2})', t)
    if m:
        try:
            return datetime.strptime(m.group(1), '%Y-%m-%d').date().isoformat()
        except Exception:
            pass
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', t)
    if m:
        s = m.group(1)
        for fmt in ['%m/%d/%Y', '%m/%d/%y']:
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except Exception:
                continue
    # try textual formats
    for fmt in DATE_PATTERNS:
        try:
            d = datetime.strptime(t, fmt)
            if fmt in ('%m/%d', '%b %d', '%B %d') and default_year:
                d = d.replace(year=default_year)
            return d.date().isoformat()
        except Exception:
            continue
    return None


def parse_site_and_opponent(opponent_raw):
    s = opponent_raw.strip()
    # common patterns: 'vs Opponent', 'at Opponent', '@ Opponent'
    lower = s.lower()
    if lower.startswith('vs '):
        return 'H', s[3:].strip()
    if lower.startswith('at '):
        return 'A', s[3:].strip()
    if s.startswith('@'):
        return 'A', s[1:].strip()
    # if opponent contains ' vs ' or ' @ '
    if ' vs ' in lower:
        parts = s.split('vs', 1)
        return 'H', parts[1].strip()
    if ' at ' in lower or ' @ ' in lower:
        parts = re.split(r' at | @ ', s, flags=re.I)
        return 'A', parts[-1].strip()
    return '', s


def find_score_in_text(text):
    m = re.search(r'(\d{1,3})\s*-\s*(\d{1,3})', text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return ''


def find_attendance(text):
    m = re.search(r'([0-9][0-9,]+)', text.replace('\xa0', ' '))
    if m:
        return m.group(1).replace(',', '')
    return ''


def scrape_season(url, year=None):
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    rows = []

    # attempt to detect the home team name on the page to avoid mixing it
    team_name = ''
    title = soup.find('title')
    if title:
        team_name = title.get_text(strip=True).lower()
    if not team_name:
        h1 = soup.find('h1')
        if h1:
            team_name = h1.get_text(strip=True).lower()
    team_aliases = ['mercyhurst', 'mercyhurst university', 'mercyhurst lakers', 'hurstathletics']
    def is_team_name(s: str):
        if not s:
            return False
        low = s.lower()
        for a in team_aliases:
            if a in low:
                return True
        # also check page-detected title fragments
        if team_name and any(piece in low for piece in team_name.split()[:3]):
            return True
        return False

    # prefer parsing by table so we can use header columns (attendance etc.)
    for table in soup.find_all('table'):
        header_cells = []
        for tr in table.find_all('tr'):
            ths = tr.find_all('th')
            if ths and not header_cells:
                header_cells = [th.get_text(separator=' ', strip=True).lower() for th in ths]
                continue

            cells = [td.get_text(separator=' ', strip=True) for td in tr.find_all(['td', 'th'])]
            if not cells:
                continue

            # try to find a date in any of the early cells
            date = None
            for c in cells[:3]:
                date = parse_date(c, default_year=year)
                if date:
                    break
            if not date:
                date = parse_date(cells[0], default_year=year)
            if not date:
                continue

            # opponent is often in the second or third cell; avoid the page's team name
            opponent = ''
            for c in cells[1:6]:
                if not c:
                    continue
                if is_team_name(c):
                    continue
                # prefer cells with alphabetic characters that are not pure numbers
                if re.search(r'[A-Za-z]', c) and not re.match(r'^\s*\d', c):
                    opponent = c
                    break
            if not opponent and len(cells) >= 2:
                opponent = cells[1]

            # score and result heuristics from row text
            row_text = ' '.join(cells)
            score = find_score_in_text(row_text)
            result = ''
            if re.search(r'\bW\b|\bL\b|\bT\b', row_text):
                m = re.search(r'\b(W|L|T)\b', row_text)
                if m:
                    result = m.group(1)
            # duration like 2:35
            duration = ''
            m = re.search(r'\b(\d+:\d{2})\b', row_text)
            if m:
                duration = m.group(1)

            # attendance: prefer a dedicated header column if present
            attendance = ''
            if header_cells:
                for i, h in enumerate(header_cells):
                    if 'attend' in h or h.strip() in ('att', 'attendance'):
                        if i < len(cells):
                            attendance = cells[i].replace(',', '')
                        break
            # fallback: choose a numeric-looking cell from the end with >=3 digits
            if not attendance:
                for c in reversed(cells):
                    m = re.search(r'([0-9][0-9,]{2,})', c.replace('\xa0', ' '))
                    if m:
                        attendance = m.group(1).replace(',', '')
                        break

            site, opponent_clean = parse_site_and_opponent(opponent)
            # if opponent_clean is just the team name or contains it, try to pick another cell
            if is_team_name(opponent_clean) or 'mercyhurst' in opponent_clean.lower():
                # look for another candidate in nearby cells
                alt = ''
                for c in cells[1:6]:
                    if not c:
                        continue
                    if is_team_name(c):
                        continue
                    if re.search(r'[A-Za-z]', c) and not re.match(r'^\s*\d', c):
                        alt = c
                        break
                if alt:
                    site, opponent_clean = parse_site_and_opponent(alt)
            # finally, strip leftover team name fragments
            opponent_clean = re.sub(r'(?i)mercyhurst', '', opponent_clean).strip(' -:,')

            candidate = {'date': date, 'opponent': opponent_clean, 'site': site, 'result': result, 'score': score, 'duration': duration, 'attendance': attendance}
            # filter obvious non-game rows: require opponent to contain letters and
            # require either a score or a result marker
            if re.search(r'[A-Za-z]', opponent_clean) and (score or result):
                rows.append(candidate)

    # as fallback, also attempt to parse schedule lists (li)
    if not rows:
        for li in soup.find_all('li'):
            text = li.get_text(' ', strip=True)
            date = parse_date(text, default_year=year)
            if not date:
                continue
            opponent = text
            score = find_score_in_text(text)
            result = ''
            m = re.search(r'\b(W|L|T)\b', text)
            if m:
                result = m.group(1)
            duration = ''
            m = re.search(r'\b(\d+:\d{2})\b', text)
            if m:
                duration = m.group(1)
            attendance = find_attendance(text)
            site, opponent_clean = parse_site_and_opponent(opponent)
            candidate = {'date': date, 'opponent': opponent_clean, 'site': site, 'result': result, 'score': score, 'duration': duration, 'attendance': attendance}
            if re.search(r'[A-Za-z]', opponent_clean) and (score or result):
                rows.append(candidate)

    # dedupe by date+opponent
    seen = set()
    unique = []
    for r in rows:
        key = (r['date'], r['opponent'])
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)

    # sort by date
    try:
        unique.sort(key=lambda x: x['date'])
    except Exception:
        pass

    return unique


def ensure_output_path(path):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def scrape_years(start_year, end_year, url_template, delay=0.5):
    all_rows = []
    for y in range(start_year, end_year + 1):
        url = url_template.format(year=y)
        print(f'Fetching {url} ...')
        try:
            rows = scrape_season(url, year=y)
            print(f'  -> {len(rows)} rows from {y}')
            all_rows.extend(rows)
        except Exception as e:
            print(f'  Failed {y}: {e}', file=sys.stderr)
        time.sleep(delay)
    # dedupe across years
    seen = set()
    unique = []
    for r in all_rows:
        key = (r.get('date'), r.get('opponent'), r.get('score'))
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)
    try:
        unique.sort(key=lambda x: x['date'])
    except Exception:
        pass
    return unique


def main():
    parser = argparse.ArgumentParser(description='Fetch season game results and write CSV')
    parser.add_argument('--year', type=int, help='Single season year (e.g. 2025)')
    parser.add_argument('--start-year', type=int, default=2012, help='Start year (inclusive), default 2012')
    parser.add_argument('--end-year', type=int, default=2025, help='End year (inclusive), default 2025')
    parser.add_argument('--csv', default=None, help='Output CSV path (repo-relative or absolute)')
    parser.add_argument('--extract-team-stats', action='store_true', dest='extract_team_stats', help='Extract team game-by-game stats (per-year CSVs)')
    parser.add_argument('--team-offense', action='store_true', dest='team_offense', help='Extract Game-by-Game -> Team -> Offense across years into game_offense.csv')
    parser.add_argument('--url-template', default='https://hurstathletics.com/sports/football/stats/{year}', help='URL template with {year}')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between requests in seconds')
    args = parser.parse_args()

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if args.csv:
        csv_path = args.csv if os.path.isabs(args.csv) else os.path.abspath(args.csv)
    else:
        csv_path = os.path.join(repo_root, 'Season Stats', 'Individual', 'game_results.csv')

    if args.year:
        start = end = args.year
    else:
        start = args.start_year
        end = args.end_year
    # If extracting team game-by-game stats, run that mode and exit
    if getattr(args, 'extract_team_stats', False):
        for y in range(start, end + 1):
            url = args.url_template.format(year=y)
            print(f'Extracting team game stats from {url} ...')
            try:
                out = extract_team_game_stats(url, year=y)
                if out:
                    out_path = out
                    print(f'Wrote team game stats for {y} -> {out_path}')
                else:
                    print(f'No suitable team table found for {y}', file=sys.stderr)
            except Exception as e:
                print(f'Failed {y}: {e}', file=sys.stderr)
        return

    if getattr(args, 'team_offense', False):
        all_rows = []
        for y in range(start, end + 1):
            url = args.url_template.format(year=y)
            print(f'Extracting Team Offense from {url} ...')
            try:
                rows = extract_team_offense(url, year=y)
                print(f'  -> {len(rows)} rows from {y}')
                all_rows.extend(rows)
            except Exception as e:
                print(f'  Failed {y}: {e}', file=sys.stderr)
            time.sleep(args.delay)

        # write aggregated game_offense.csv
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        out_dir = os.path.join(repo_root, 'Season Stats', 'Team')
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, 'game_offense.csv')

        # header as requested
        offense_fields = ['date', 'opponent', 'att', 'yds', 'td', 'long', 'att', 'yds', 'td', 'long', 'yds', 'td', 'long', 'att', 'yds', 'td', 'long', 'att', 'yds', 'td', 'long']
        with open(out_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(offense_fields)
            for r in all_rows:
                row = [r.get('date',''), r.get('opponent','')]
                nums = r.get('numbers', [])
                # pad/truncate to fit
                needed = len(offense_fields) - 2
                row.extend(nums[:needed] + [''] * max(0, needed - len(nums)))
                writer.writerow(row)

        print(f'Wrote {len(all_rows)} rows to {out_path}')
        return

    rows = scrape_years(start, end, args.url_template, delay=args.delay)

    header = ['date', 'opponent', 'site', 'result', 'score', 'duration', 'attendance']
    ensure_output_path(csv_path)
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in rows:
            writer.writerow([r.get('date',''), r.get('opponent',''), r.get('site',''), r.get('result',''), r.get('score',''), r.get('duration',''), r.get('attendance','')])

    print(f'Wrote {len(rows)} rows to {csv_path}')


def normalize_header(h: str) -> str:
    return re.sub(r"\s+", " ", h.strip()).lower()


def extract_team_game_stats(url, year=None):
    """Find tables with two header rows, use the second header as columns,
    and write a per-year CSV containing those columns and data rows.

    Returns the output path if a file was written, otherwise None.
    """
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    results = []
    header_row = None
    for table in soup.find_all('table'):
        trs = table.find_all('tr')
        # detect consecutive header rows at the top
        header_trs = []
        for tr in trs:
            if tr.find_all('th'):
                header_trs.append(tr)
            else:
                break
        if len(header_trs) < 2:
            continue
        second_ths = header_trs[1].find_all('th')
        headers = [th.get_text(separator=' ', strip=True) for th in second_ths]
        if not headers:
            continue
        lowh = [h.lower() for h in headers]
        # require date and opponent-like headers in second header
        if not any('date' in h or re.search(r'\d{1,2}/\d{1,2}', h) for h in lowh):
            continue
        if not any('opp' in h or 'opponent' in h or 'team' in h for h in lowh):
            continue

        # collect data rows (tds)
        data_trs = [tr for tr in trs if tr.find_all('td')]
        for tr in data_trs:
            tds = [td.get_text(separator=' ', strip=True) for td in tr.find_all('td')]
            # pad if mismatch
            if len(tds) < len(headers):
                tds += [''] * (len(headers) - len(tds))
            # simple filter: require date-like and opponent-like cells
            combined = ' '.join(tds)
            date_candidate = None
            for i, h in enumerate(headers[:3]):
                dc = parse_date(tds[i] if i < len(tds) else '', default_year=year)
                if dc:
                    date_candidate = dc
                    break
            if not date_candidate:
                # try parsing anywhere
                for c in tds[:5]:
                    if parse_date(c, default_year=year):
                        date_candidate = parse_date(c, default_year=year)
                        break
            if not date_candidate:
                continue

            results.append((headers, tds))
        if results:
            header_row = headers
            break

    if not results or not header_row:
        return None

    # write per-year CSV
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    out_dir = os.path.join(repo_root, 'Season Stats', 'Team')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f'team_game_stats_{year}.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # create header: normalize second header; ensure date/opponent first
        norm = [normalize_header(h) for h in header_row]
        # try to move any date/opponent-like columns to front in the same order
        front = []
        rest = []
        for i, h in enumerate(norm):
            if 'date' in h or re.search(r'\bdate\b', h):
                front.append(header_row[i])
            elif 'opp' in h or 'opponent' in h or 'team' in h:
                front.append(header_row[i])
            else:
                rest.append(header_row[i])
        out_header = front + rest
        writer.writerow(out_header)
        for hdrs, tds in results:
            # reorder cells to match out_header
            # map original header text to index
            mapping = {hdrs[i]: (tds[i] if i < len(tds) else '') for i in range(len(hdrs))}
            row = [mapping.get(h, '') for h in out_header]
            writer.writerow(row)

    return out_path


def extract_team_offense(url, year=None):
    """Extract the Game-by-Game -> Team -> Offense table and return rows as dicts
    with keys `date`, `opponent`, and `numbers` (list of numeric columns in order).
    """
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    candidates = []
    # prefer the explicit section if present
    section = soup.find(id='gbg_team_offense')
    if not section:
        # fallback: look for aria-label containing the phrase
        section = soup.find(attrs={'aria-label': lambda v: v and 'game-by-game' in v.lower() and 'team' in v.lower() and 'offense' in v.lower()})

    tables_to_scan = []
    if section:
        tbl = section.find('table')
        if tbl:
            tables_to_scan = [tbl]
    else:
        tables_to_scan = soup.find_all('table')

    for table in tables_to_scan:
        trs = table.find_all('tr')
        # require two header rows (as described)
        header_trs = [tr for tr in trs if tr.find_all('th')]
        if len(header_trs) < 2:
            continue
        # collect data rows from tbody
        tbody = table.find('tbody')
        if not tbody:
            data_trs = [tr for tr in trs if tr.find_all('td')]
        else:
            data_trs = [tr for tr in tbody.find_all('tr') if tr.find_all(['td','th'])]

        for tr in data_trs:
            # first cell is date, second is opponent (often a th with link)
            cells = tr.find_all(['td','th'])
            if len(cells) < 3:
                continue
            date_text = cells[0].get_text(separator=' ', strip=True)
            date_val = parse_date(date_text, default_year=year)
            if not date_val:
                continue
            # opponent text may be in a th or td
            opp_cell = cells[1]
            opponent = opp_cell.get_text(separator=' ', strip=True)
            # numeric cells follow
            numeric_cells = []
            for c in cells[2:]:
                txt = c.get_text(separator=' ', strip=True).replace(',', '')
                numeric_cells.append(txt)

            candidates.append({'date': date_val, 'opponent': opponent, 'numbers': numeric_cells})

    return candidates


if __name__ == '__main__':
    main()
