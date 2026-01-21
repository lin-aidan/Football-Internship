import requests
import pandas as pd
import io
import re
from bs4 import BeautifulSoup

YEARS = list(range(2012, 2026))
BASE = 'https://hurstathletics.com/sports/football/stats/'

def norm(name):
    if pd.isna(name):
        return ''
    s = str(name)
    s = s.replace('"','')
    s = s.replace("'", '')
    s = re.sub(r"\s+", ' ', s)
    s = s.strip().lower()
    # remove digits and any characters except letters, comma, space, hyphen
    s = re.sub(r"\d+", '', s)
    s = re.sub(r"[^a-z, -]", '', s)
    return s


def swap_comma(name):
    # "Last, First" -> "First Last"
    if ',' in name:
        parts = [p.strip() for p in name.split(',', 1)]
        return (parts[1] + ' ' + parts[0]).strip()
    return name


def scrape_year(year):
    url = BASE + str(year)
    # try to use a headless browser (Playwright) to render pages that need JS
    html = None
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=15000)
            html = page.content()
            browser.close()
    except Exception:
        # Playwright not available or failed; fall back to requests
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            html = r.text
        except Exception:
            raise
    # try pandas first (may require lxml); fall back to BeautifulSoup parsing
    try:
        tables = pd.read_html(io.StringIO(html))
        for df in tables:
            cols = [c.lower() for c in df.columns.astype(str)]
            if 'name' in cols and 'att' in cols and 'td' in cols:
                mapping = {}
                for _, row in df.iterrows():
                    name = row.get('Name') if 'Name' in df.columns else row.get('name')
                    td = row.get('TD') if 'TD' in df.columns else row.get('td')
                    n = norm(str(name))
                    mapping[n] = td
                    swapped = norm(swap_comma(str(name)))
                    mapping[swapped] = td
                return mapping
    except Exception:
        pass

    # BeautifulSoup fallback: parse all <table> elements
    soup = BeautifulSoup(html, 'html.parser')
    for table in soup.find_all('table'):
        # get header names
        headers = []
        thead = table.find('thead')
        if thead:
            header_cells = thead.find_all(['th', 'td'])
            headers = [h.get_text(strip=True).lower() for h in header_cells]
        else:
            first_row = table.find('tr')
            if not first_row:
                continue
            headers = [c.get_text(strip=True).lower() for c in first_row.find_all(['th', 'td'])]
        if not headers:
            continue
        # prefer rows where player name is in a <th> and TD in a <td data-label="td">
        mapping = {}
        rows = table.find_all('tr')
        for tr in rows:
            th = tr.find('th')
            # find td with data-label 'td' and 'att' to confirm player row
            td_by_label = tr.find('td', attrs={'data-label': re.compile('^td$', re.I)})
            att_by_label = tr.find('td', attrs={'data-label': re.compile('^att$', re.I)})
            if not td_by_label:
                continue
            # require either th present (player name) or first td looks like a name
            if th:
                raw_name = th.get_text(separator=' ', strip=True)
            else:
                tds_all = tr.find_all('td')
                if not tds_all:
                    continue
                raw_name = tds_all[0].get_text(separator=' ', strip=True)
            # try to extract "Last, First" pattern from raw text
            m = re.search(r"([A-Za-z\-]+,\s*[A-Za-z\-\s]+)", raw_name)
            if m:
                name = m.group(1)
            else:
                name = raw_name

            # skip header-like rows
            name_l = name.strip().lower()
            if name_l in headers or name_l == '' or any(keyword in name_l for keyword in ('rushing', 'passing', 'scoring', 'statistic')):
                continue

            td_val = td_by_label.get_text(strip=True)
            n = norm(name)
            mapping[n] = td_val
            mapping[norm(swap_comma(name))] = td_val
        if mapping:
            return mapping
    return {}


def build_td_map():
    td_map = {}
    for y in YEARS:
        try:
            m = scrape_year(y)
            for k,v in m.items():
                td_map[(k, str(y))] = v
            print(f"Scraped {len(m)} names for {y}")
        except Exception as e:
            print(f"Failed to scrape {y}: {e}")
    return td_map


def main():
    td_map = build_td_map()
    
    # read and combine yearly local files
    out_rows = []
    for y in YEARS:
        path = f'mercyhurst_rushing_{y}.csv'
        try:
            df = pd.read_csv(path)
        except Exception:
            print(f"Missing or unreadable {path}, skipping")
            continue
        # ensure consistent column names
        df.columns = [c.strip() for c in df.columns]
        # some files use lowercase 'avg' instead of 'AVG'
        for _, row in df.iterrows():
            No = row.get('No', '')
            Name = row.get('Name', '')
            GP = row.get('GP', '')
            ATT = row.get('ATT', '')
            Net = row.get('Net', '')
            AVG = row.get('AVG', row.get('avg', ''))
            TD_old = row.get('TD', '')
            Long = row.get('Long', '')
            AVG_G = row.get('AVG/G', row.get('AVG/G', ''))
            year = row.get('Year', y)
            key = (norm(str(Name)), str(year))
            td_new = td_map.get(key)
            if td_new is None:
                # try swapped
                key2 = (norm(swap_comma(str(Name))), str(year))
                td_new = td_map.get(key2)
            if td_new is None:
                td_val = ''
            else:
                td_val = td_new
            out_rows.append({
                'No': No,
                'Name': Name,
                'GP': GP,
                'ATT': ATT,
                'Net': Net,
                'AVG': AVG,
                'TD': td_val,
                'Long': Long,
                'AVG/G': AVG_G,
                'Year': year
            })
    out_df = pd.DataFrame(out_rows)
    out_df.to_csv('all_mercyhurst_rushing.csv', index=False)
    print('Wrote all_mercyhurst_rushing.csv')

if __name__ == '__main__':
    main()
