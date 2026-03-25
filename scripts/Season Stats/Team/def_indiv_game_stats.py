import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import time

years = list(range(2012, 2026))
base_url = "https://hurstathletics.com"

# -------------------------------
# HELPERS
# -------------------------------
def clean_stat(val):
    val = val.strip()
    return "0" if val in ["-", "", None] else val

def split_stat(val):
    val = val.strip()
    if "-" in val or "/" in val:
        val = val.replace("-", "0")
        parts = val.split("/") if "/" in val else val.split("-")
        if len(parts) == 2:
            return clean_stat(parts[0]), clean_stat(parts[1])
    return clean_stat(val), "0"

def get_mu_side(soup):
    home = soup.find("th", id="home-team")
    away = soup.find("th", id="away-team")

    home_team = home.get_text(strip=True).upper() if home else ""
    away_team = away.get_text(strip=True).upper() if away else ""

    MU_KEYS = ["MU", "MER", "HURST", "MERCYHURST", "LAKERS", "MCY", "MHU", "MCU"]

    if any(k in home_team for k in MU_KEYS):
        return "home"
    elif any(k in away_team for k in MU_KEYS):
        return "away"
    else:
        return None

def get_mu_def_table(soup, url):
    section = soup.find("section", {"id": "defense-home"})
    
    if not section:
        print(f"Missing defensive section: {url}")
        return None

    table = section.find("table")

    if not table:
        print(f"Missing defensive table: {url}")
        return None

    return table

# -------------------------------
# STEP 1: GET GAME URLS
# -------------------------------
def extract_game_data(schedule_url, year):
    rows = []
    seen = set()
    
    response = requests.get(schedule_url)
    if response.status_code != 200:
        print(f"Failed to load {schedule_url}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    boxscores = soup.find_all('li', class_='sidearm-schedule-game-links-boxscore')
    
    for box in boxscores:
        anchor = box.find('a')
        if not (anchor and 'href' in anchor.attrs):
            continue
        
        link = base_url + anchor['href']
        if link in seen:
            continue
        
        seen.add(link)

        opponent_div = box.find_previous('div', class_='sidearm-schedule-game-opponent-name')
        opponent = opponent_div.get_text(strip=True) if opponent_div else "Unknown"
        
        rows.append((year, opponent, link))
    
    return rows

# -------------------------------
# STEP 2: DEFENSIVE STATS
# -------------------------------
def get_indiv_def_stats(soup, url):
    sections = soup.find_all("section")

    target_section = None

    # -------------------------------
    # FIND MERCYHURST DEFENSIVE SECTION
    # -------------------------------
    for section in sections:
        aria = section.get("aria-label", "").upper()

        if "MERCYHURST" in aria or "LAKERS" in aria or "MU" in aria or "MHU" in aria or "MCY" in aria or "MCU" in aria or "MER" in aria or "HURST" in aria:
            target_section = section
            break

    if not target_section:
        print(f"Mercyhurst defensive section not found: {url}")
        return []

    table = target_section.find("table")
    if not table:
        print(f"Missing defensive table: {url}")
        return []

    tbody = table.find("tbody")
    if not tbody:
        print(f"No tbody in defensive table: {url}")
        return []

    rows = tbody.find_all("tr")
    data = []

    # -------------------------------
    # CLEANING HELPERS
    # -------------------------------
    def clean(val):
        val = val.strip()
        return "0" if val in ["-", "", None] else val

    def split_stat(val):
        val = val.strip().replace("-", "0")
        if "/" in val:
            parts = val.split("/")
            if len(parts) == 2:
                return clean(parts[0]), clean(parts[1])
        return clean(val), "0"

    # -------------------------------
    # EXTRACT ROWS (ALL MU PLAYERS)
    # -------------------------------
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 16:
            continue

        tfl, tfl_yards = split_stat(cells[4].get_text(strip=True))
        sacks, sack_yards = split_stat(cells[6].get_text(strip=True))
        fr, fr_yards = split_stat(cells[10].get_text(strip=True))

        data.append({
            "player": clean(cells[0].get_text(strip=True)),
            "solo_tkl": clean(cells[1].get_text(strip=True)),
            "ast_tkl": clean(cells[2].get_text(strip=True)),
            "tot_tkl": clean(cells[3].get_text(strip=True)),
            "tfl": tfl,
            "tfl_yards": tfl_yards,
            "sacks": sacks,
            "sack_yards": sack_yards,
            "FF": clean(cells[8].get_text(strip=True)),
            "FR": fr,
            "FR_yards": fr_yards,
            "interceptions": clean(cells[11].get_text(strip=True)),
            "pbu": clean(cells[13].get_text(strip=True)),
            "blks": clean(cells[14].get_text(strip=True)),
            "QBH": clean(cells[15].get_text(strip=True)),
        })

    return data

# -------------------------------
# STEP 3: RUN PIPELINE
# -------------------------------
game_rows = []

for year in years:
    url = f"{base_url}/sports/football/schedule/{year}"
    game_rows.extend(extract_game_data(url, year))

def_game_rows = []

for year, opponent, link in game_rows:
    response = requests.get(link)
    if response.status_code != 200:
        print(f"Failed game page: {link}")
        continue

    soup = BeautifulSoup(response.content, "html.parser")

    def_stats = get_indiv_def_stats(soup, link)

    for stat in def_stats:
        stat["year"] = year
        stat["opponent"] = opponent
        def_game_rows.append(stat)

    time.sleep(1)

# -------------------------------
# STEP 4: SAVE CSV
# -------------------------------
df = pd.DataFrame(def_game_rows)

output_path = Path("defensive_game_stats.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(output_path, index=False)

print(f"Saved {len(df)} defensive rows to {output_path}")