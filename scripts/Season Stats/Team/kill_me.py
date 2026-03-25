import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import time

years = list(range(2012, 2026))
base_url = "https://hurstathletics.com"


def is_mu(name):
    name = name.upper()
    return any(k in name for k in ["MU", "MER", "HURST", "MERCYHURST", "LAKERS", "MCY", "MHU", "MCU"])


def safe_int(x):
    try:
        return int(x)
    except:
        return 0


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


def extract_boxscore_stats(url):
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to load {url}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    header_home = soup.find("th", {"id": "home-team"})
    header_away = soup.find("th", {"id": "away-team"})

    if not header_home or not header_away:
        print(f"Missing team headers: {url}")
        return None

    home_team = header_home.get_text(strip=True)
    away_team = header_away.get_text(strip=True)

    if is_mu(home_team):
        mu_col = 2
        opp_col = 1
    elif is_mu(away_team):
        mu_col = 1
        opp_col = 2
    else:
        print(f"Mercyhurst not found in headers: {url}")
        return None


    table = soup.find("table", {"class": "sidearm-table overall-stats highlight-hover full"})
    
    if not table:
        print(f"No stats table for {url}")
        return None

    rows = table.find("tbody").find_all("tr")

    if len(rows) < 41:
        print(f"Incomplete game: {url}")
        return None


    mu_total_first_downs = rows[1].find_all("td")[mu_col].text.strip()
    opp_total_first_downs = rows[1].find_all("td")[opp_col].text.strip()

    mu_rushing_first_downs = rows[2].find_all("td")[mu_col].text.strip()
    opp_rushing_first_downs = rows[2].find_all("td")[opp_col].text.strip()

    mu_passing_first_downs = rows[3].find_all("td")[mu_col].text.strip()
    opp_passing_first_downs = rows[3].find_all("td")[opp_col].text.strip()

    mu_penalty_first_downs = rows[4].find_all("td")[mu_col].text.strip()
    opp_penalty_first_downs = rows[4].find_all("td")[opp_col].text.strip()

    mu_rushing_net = rows[6].find_all("td")[mu_col].text.strip()
    opp_rushing_net = rows[6].find_all("td")[opp_col].text.strip()

    mu_rushing_attempts = rows[7].find_all("td")[mu_col].text.strip()
    opp_rushing_attempts = rows[7].find_all("td")[opp_col].text.strip()

    mu_rushing_tds = rows[9].find_all("td")[mu_col].text.strip()
    opp_rushing_tds = rows[9].find_all("td")[opp_col].text.strip()

    mu_passing_net = rows[13].find_all("td")[mu_col].text.strip()
    opp_passing_net = rows[13].find_all("td")[opp_col].text.strip()

    mu_passing = rows[14].find_all("td")[mu_col].text.strip()
    opp_passing = rows[14].find_all("td")[opp_col].text.strip()

    mu_completions = mu_passing.split("-")[0].strip()
    mu_attempts = mu_passing.split("-")[1].strip()
    mu_passing_ints = mu_passing.split("-")[2].strip()

    opp_completions = opp_passing.split("-")[0].strip()
    opp_attempts = opp_passing.split("-")[1].strip()
    opp_passing_ints = opp_passing.split("-")[2].strip()

    mu_td_passes = rows[17].find_all("td")[mu_col].text.strip()
    opp_td_passes = rows[17].find_all("td")[opp_col].text.strip()

    mu_total_yards = rows[19].find_all("td")[mu_col].text.strip()
    opp_total_yards = rows[19].find_all("td")[opp_col].text.strip()

    mu_plays = rows[20].find_all("td")[mu_col].text.strip()
    opp_plays = rows[20].find_all("td")[opp_col].text.strip()

    mu_fumbles_stats = rows[22].find_all("td")[mu_col].text.strip()
    opp_fumbles_stats = rows[22].find_all("td")[opp_col].text.strip()

    mu_fumbles = mu_fumbles_stats.split("-")[0].strip()
    opp_fumbles = opp_fumbles_stats.split("-")[0].strip()

    mu_fumbles_lost = mu_fumbles_stats.split("-")[1].strip()
    opp_fumbles_lost = opp_fumbles_stats.split("-")[1].strip()

    mu_penalty_stats = rows[23].find_all("td")[mu_col].text.strip()
    opp_penalty_stats = rows[23].find_all("td")[opp_col].text.strip()

    mu_penalties = mu_penalty_stats.split("-")[0].strip()
    opp_penalties = opp_penalty_stats.split("-")[0].strip()

    mu_penalty_yards = mu_penalty_stats.split("-")[1].strip()
    opp_penalty_yards = opp_penalty_stats.split("-")[1].strip()

    mu_punt_stats = rows[25].find_all("td")[mu_col].text.strip()
    opp_punt_stats = rows[25].find_all("td")[opp_col].text.strip()

    mu_punts = mu_punt_stats.split("-")[0].strip()
    opp_punts = opp_punt_stats.split("-")[0].strip()

    mu_punt_yards = mu_punt_stats.split("-")[1].strip()
    opp_punt_yards = opp_punt_stats.split("-")[1].strip()

    mu_punt_avg = safe_int(mu_punt_yards) / safe_int(mu_punts) if safe_int(mu_punts) > 0 else 0
    opp_punt_avg = safe_int(opp_punt_yards) / safe_int(opp_punts) if safe_int(opp_punts) > 0 else 0

    mu_punt_return_stats = rows[36].find_all("td")[mu_col].text.strip()
    opp_punt_return_stats = rows[36].find_all("td")[opp_col].text.strip()

    mu_punt_returns = mu_punt_return_stats.split("-")[0].strip()
    opp_punt_returns = opp_punt_return_stats.split("-")[0].strip()

    mu_punt_return_yards = mu_punt_return_stats.split("-")[1].strip()
    opp_punt_return_yards = opp_punt_return_stats.split("-")[1].strip()

    mu_punt_return_tds = mu_punt_return_stats.split("-")[2].strip()
    opp_punt_return_tds = opp_punt_return_stats.split("-")[2].strip()

    mu_punt_return_avg = safe_int(mu_punt_return_yards) / safe_int(mu_punt_returns) if safe_int(mu_punt_returns) > 0 else 0
    opp_punt_return_avg = safe_int(opp_punt_return_yards) / safe_int(opp_punt_returns) if safe_int(opp_punt_returns) > 0 else 0

    mu_kickoff_return_stats = rows[38].find_all("td")[mu_col].text.strip()
    opp_kickoff_return_stats = rows[38].find_all("td")[opp_col].text.strip()

    mu_kickoff_returns = mu_kickoff_return_stats.split("-")[0].strip()
    opp_kickoff_returns = opp_kickoff_return_stats.split("-")[0].strip()

    mu_kickoff_return_yards = mu_kickoff_return_stats.split("-")[1].strip()
    opp_kickoff_return_yards = opp_kickoff_return_stats.split("-")[1].strip()

    mu_kickoff_return_tds = mu_kickoff_return_stats.split("-")[2].strip()
    opp_kickoff_return_tds = opp_kickoff_return_stats.split("-")[2].strip()

    mu_kickoff_return_avg = safe_int(mu_kickoff_return_yards) / safe_int(mu_kickoff_returns) if safe_int(mu_kickoff_returns) > 0 else 0
    opp_kickoff_return_avg = safe_int(opp_kickoff_return_yards) / safe_int(opp_kickoff_returns) if safe_int(opp_kickoff_returns) > 0 else 0

    mu_ints_stats = rows[40].find_all("td")[mu_col].text.strip()
    opp_ints_stats = rows[40].find_all("td")[opp_col].text.strip()

    mu_ints = mu_ints_stats.split("-")[0].strip()
    opp_ints = opp_ints_stats.split("-")[0].strip()

    mu_int_return_yards = mu_ints_stats.split("-")[1].strip()
    opp_int_return_yards = opp_ints_stats.split("-")[1].strip()

    mu_int_return_tds = mu_ints_stats.split("-")[2].strip()
    opp_int_return_tds = opp_ints_stats.split("-")[2].strip()

    return {
        "year": None,
        "opponent": None,
        "mu_total_first_downs": mu_total_first_downs,
        "opp_total_first_downs": opp_total_first_downs,
        "mu_rushing_first_downs": mu_rushing_first_downs,
        "opp_rushing_first_downs": opp_rushing_first_downs,
        "mu_passing_first_downs": mu_passing_first_downs,
        "opp_passing_first_downs": opp_passing_first_downs,
        "mu_penalty_first_downs": mu_penalty_first_downs,
        "opp_penalty_first_downs": opp_penalty_first_downs,
        "mu_rushing_net": mu_rushing_net,
        "opp_rushing_net": opp_rushing_net,
        "mu_rushing_attempts": mu_rushing_attempts,
        "opp_rushing_attempts": opp_rushing_attempts,
        "mu_rushing_tds": mu_rushing_tds,
        "opp_rushing_tds": opp_rushing_tds,
        "mu_passing_net": mu_passing_net,
        "opp_passing_net": opp_passing_net,
        "mu_completions": mu_completions,
        "mu_attempts": mu_attempts,
        "mu_passing_ints": mu_passing_ints,
        "opp_completions": opp_completions,
        "opp_attempts": opp_attempts,
        "opp_passing_ints": opp_passing_ints,
        "mu_td_passes": mu_td_passes,
        "opp_td_passes": opp_td_passes,
        "mu_total_yards": mu_total_yards,
        "opp_total_yards": opp_total_yards,
        "mu_plays": mu_plays,
        "opp_plays": opp_plays,
        "mu_fumbles": mu_fumbles,
        "opp_fumbles": opp_fumbles,
        "mu_fumbles_lost": mu_fumbles_lost,
        "opp_fumbles_lost": opp_fumbles_lost,
        "mu_penalties": mu_penalties,
        "opp_penalties": opp_penalties,
        "mu_penalty_yards": mu_penalty_yards,
        "opp_penalty_yards": opp_penalty_yards,
        "mu_punts": mu_punts,
        "opp_punts": opp_punts,
        "mu_punt_yards": mu_punt_yards,
        "opp_punt_yards": opp_punt_yards,
        "mu_punt_avg": mu_punt_avg,
        "opp_punt_avg": opp_punt_avg,
        "mu_punt_returns": mu_punt_returns,
        "opp_punt_returns": opp_punt_returns,
        "mu_punt_return_yards": mu_punt_return_yards,
        "opp_punt_return_yards": opp_punt_return_yards,
        "mu_punt_return_tds": mu_punt_return_tds,
        "opp_punt_return_tds": opp_punt_return_tds,
        "mu_punt_return_avg": mu_punt_return_avg,
        "opp_punt_return_avg": opp_punt_return_avg,
        "mu_kickoff_returns": mu_kickoff_returns,
        "opp_kickoff_returns": opp_kickoff_returns,
        "mu_kickoff_return_yards": mu_kickoff_return_yards,
        "opp_kickoff_return_yards": opp_kickoff_return_yards,
        "mu_kickoff_return_tds": mu_kickoff_return_tds,
        "opp_kickoff_return_tds": opp_kickoff_return_tds,
        "mu_kickoff_return_avg": mu_kickoff_return_avg,
        "opp_kickoff_return_avg": opp_kickoff_return_avg,
        "mu_ints": mu_ints,
        "opp_ints": opp_ints,
        "mu_int_return_yards": mu_int_return_yards,
        "opp_int_return_yards": opp_int_return_yards,
        "mu_int_return_tds": mu_int_return_tds,
        "opp_int_return_tds": opp_int_return_tds
    }


game_rows = []

for year in years:
    url = f"{base_url}/sports/football/schedule/{year}"
    game_rows.extend(extract_game_data(url, year))

all_games = []

for year, opponent, link in game_rows:
    stats = extract_boxscore_stats(link)
    
    if stats:
        stats["year"] = year
        stats["opponent"] = opponent
        all_games.append(stats)
    
    time.sleep(1)


df = pd.DataFrame(all_games)

output_path = Path('full_game_stats.csv')
output_path.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(output_path, index=False)

print(f"Saved {len(df)} games to {output_path}")