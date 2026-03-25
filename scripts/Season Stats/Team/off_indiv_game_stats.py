import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import time

years = list(range(2012, 2026))
base_url = "https://hurstathletics.com"


def is_mu_caption(text):
    text = text.upper()
    team = text.split("-")[0].strip()  # isolate team code
    return team in ["MU", "MER", "HURST", "MERCYHURST"]

def get_mu_table(section):
    if not section:
        return None
    
    tables = section.find_all("table")
    
    for table in tables:
        caption = table.find("caption")
        if caption:
            if is_mu_caption(caption.get_text(strip=True)):
                return table
    
    return None


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


def get_indiv_passing_stats(soup):
    section = soup.find("section", {"id": "individual-passing"})
    table = get_mu_table(section)
    
    if not table:
        return []

    rows = table.find("tbody").find_all("tr")
    data = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) == 0:
            continue

        data.append({
            "player": cells[0].get_text(strip=True),
            "completions": cells[1].get_text(strip=True),
            "attempts": cells[2].get_text(strip=True),
            "passing_yards": cells[3].get_text(strip=True),
            "passing_tds": cells[4].get_text(strip=True),
            "interceptions": cells[5].get_text(strip=True),
            "long_pass": cells[6].get_text(strip=True),
            "sacks": cells[7].get_text(strip=True)
        })

    return data


def get_indiv_rushing_stats(soup):
    section = soup.find("section", {"id": "individual-rushing"})
    table = get_mu_table(section)
    
    if not table:
        return []

    rows = table.find("tbody").find_all("tr")
    data = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) == 0:
            continue

        data.append({
            "player": cells[0].get_text(strip=True),
            "attempts": cells[1].get_text(strip=True),
            "rush_net": cells[4].get_text(strip=True),
            "rush_tds": cells[5].get_text(strip=True),
            "long_rush": cells[6].get_text(strip=True)
        })

    return data


def get_indiv_receiving_stats(soup):
    section = soup.find("section", {"id": "individual-receiving"})
    table = get_mu_table(section)
    
    if not table:
        return []

    rows = table.find("tbody").find_all("tr")
    data = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) == 0:
            continue

        data.append({
            "player": cells[0].get_text(strip=True),
            "receptions": cells[1].get_text(strip=True),
            "receiving_yards": cells[2].get_text(strip=True),
            "receiving_tds": cells[3].get_text(strip=True),
            "long_reception": cells[4].get_text(strip=True)
        })

    return data


game_rows = []

for year in years:
    url = f"{base_url}/sports/football/schedule/{year}"
    game_rows.extend(extract_game_data(url, year))

passing_game_rows = []
rushing_game_rows = []
receiving_game_rows = []

for year, opponent, link in game_rows:
    response = requests.get(link)
    if response.status_code != 200:
        continue

    soup = BeautifulSoup(response.content, "html.parser")

    # Passing
    passing_stats = get_indiv_passing_stats(soup)
    for stat in passing_stats:
        stat["year"] = year
        stat["opponent"] = opponent
        passing_game_rows.append(stat)

    # Rushing
    rushing_stats = get_indiv_rushing_stats(soup)
    for stat in rushing_stats:
        stat["year"] = year
        stat["opponent"] = opponent
        rushing_game_rows.append(stat)

    # Receiving
    receiving_stats = get_indiv_receiving_stats(soup)
    for stat in receiving_stats:
        stat["year"] = year
        stat["opponent"] = opponent
        receiving_game_rows.append(stat)

    time.sleep(1)


pass_df = pd.DataFrame(passing_game_rows)
rush_df = pd.DataFrame(rushing_game_rows)
recv_df = pd.DataFrame(receiving_game_rows)

pass_df.to_csv("passing_game_stats.csv", index=False)
rush_df.to_csv("rushing_game_stats.csv", index=False)
recv_df.to_csv("receiving_game_stats.csv", index=False)

print(f"Saved {len(pass_df)} passing rows")
print(f"Saved {len(rush_df)} rushing rows")
print(f"Saved {len(recv_df)} receiving rows")