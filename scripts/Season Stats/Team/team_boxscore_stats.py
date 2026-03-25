import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://hurstathletics.com/sports/football/stats/2012/notre-dame-college-ohio-/boxscore/4551"

def extract_boxscore_stats(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", {"class": "sidearm-table overall-stats highlight-hover full"})
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")

    # First down statistics
    mu_total_first_downs = rows[1].find_all("td")[1].text.strip()
    opp_total_first_downs = rows[1].find_all("td")[2].text.strip()

    mu_rushing_first_downs = rows[2].find_all("td")[1].text.strip()
    opp_rushing_first_downs = rows[2].find_all("td")[2].text.strip()

    mu_passing_first_downs = rows[3].find_all("td")[1].text.strip()
    opp_passing_first_downs = rows[3].find_all("td")[2].text.strip()

    mu_penalty_first_downs = rows[4].find_all("td")[1].text.strip()
    opp_penalty_first_downs = rows[4].find_all("td")[2].text

    # Rushing Statistics
    mu_rushing_net = rows[6].find_all("td")[1].text.strip()
    opp_rushing_net = rows[6].find_all("td")[2].text.strip()

    mu_rushing_attempts = rows[7].find_all("td")[1].text.strip()
    opp_rushing_attempts = rows[7].find_all("td")[2].text.strip()

    mu_rushing_tds = rows[9].find_all("td")[1].text.strip()
    opp_rushing_tds = rows[9].find_all("td")[2].text.strip()

    # Passing Statistics
    mu_passing_net = rows[13].find_all("td")[1].text.strip()
    opp_passing_net = rows[13].find_all("td")[2].text.strip()

    mu_passing = rows[14].find_all("td")[1].text.strip()
    opp_passing = rows[14].find_all("td")[2].text.strip()

    mu_completions = mu_passing.split("-")[0].strip()
    mu_attempts = mu_passing.split("-")[1].strip()
    mu_passing_ints = mu_passing.split("-")[2].strip()

    opp_completions = opp_passing.split("-")[0].strip()
    opp_attempts = opp_passing.split("-")[1].strip()
    opp_passing_ints = opp_passing.split("-")[2].strip()

    mu_td_passes = rows[17].find_all("td")[1].text.strip()
    opp_td_passes = rows[17].find_all("td")[2].text.strip()

    # Total Offense
    mu_total_yards = rows[19].find_all("td")[1].text.strip()
    opp_total_yards = rows[19].find_all("td")[2].text.strip()

    mu_plays = rows[20].find_all("td")[1].text.strip()
    opp_plays = rows[20].find_all("td")[2].text.strip()

    mu_fumbles_stats = rows[22].find_all("td")[1].text.strip()
    opp_fumbles_stats = rows[22].find_all("td")[2].text.strip()

    mu_fumbles = mu_fumbles_stats.split("-")[0].strip()
    opp_fumbles = opp_fumbles_stats.split("-")[0].strip()

    mu_fumbles_lost = mu_fumbles_stats.split("-")[1].strip()
    opp_fumbles_lost = opp_fumbles_stats.split("-")[1].strip()

    mu_penalty_stats = rows[23].find_all("td")[1].text.strip()
    opp_penalty_stats = rows[23].find_all("td")[2].text.strip()

    mu_penalties = mu_penalty_stats.split("-")[0].strip()
    opp_penalties = opp_penalty_stats.split("-")[0].strip()

    mu_penalty_yards = mu_penalty_stats.split("-")[1].strip()
    opp_penalty_yards = opp_penalty_stats.split("-")[1].strip()

    # Punting Statistics
    mu_punt_stats = rows[25].find_all("td")[1].text.strip()
    opp_punt_stats = rows[25].find_all("td")[2].text.strip()

    mu_punts = mu_punt_stats.split("-")[0].strip()
    opp_punts = opp_punt_stats.split("-")[0].strip()

    mu_punt_yards = mu_punt_stats.split("-")[1].strip()
    opp_punt_yards = opp_punt_stats.split("-")[1].strip()

    mu_punt_avg = int(mu_punt_yards) / int(mu_punts) if int(mu_punts) > 0 else 0
    opp_punt_avg = int(opp_punt_yards) / int(opp_punts) if int(opp_punts) > 0 else 0

    # Return Statistics
    mu_punt_return_stats = rows[36].find_all("td")[1].text.strip()
    opp_punt_return_stats = rows[36].find_all("td")[2].text.strip()

    mu_punt_returns = mu_punt_return_stats.split("-")[0].strip()
    opp_punt_returns = opp_punt_return_stats.split("-")[0].strip()

    mu_punt_return_yards = mu_punt_return_stats.split("-")[1].strip()
    opp_punt_return_yards = opp_punt_return_stats.split("-")[1].strip()

    mu_punt_return_tds = mu_punt_return_stats.split("-")[2].strip()
    opp_punt_return_tds = opp_punt_return_stats.split("-")[2].strip()

    mu_punt_return_avg = int(mu_punt_return_yards) / int(mu_punt_returns) if int(mu_punt_returns) > 0 else 0
    opp_punt_return_avg = int(opp_punt_return_yards) / int(opp_punt_returns) if int(opp_punt_returns) > 0 else 0

    mu_kickoff_return_stats = rows[38].find_all("td")[1].text.strip()
    opp_kickoff_return_stats = rows[38].find_all("td")[2].text.strip()

    mu_kickoff_returns = mu_kickoff_return_stats.split("-")[0].strip()
    opp_kickoff_returns = opp_kickoff_return_stats.split("-")[0].strip()

    mu_kickoff_return_yards = mu_kickoff_return_stats.split("-")[1].strip()
    opp_kickoff_return_yards = opp_kickoff_return_stats.split("-")[1].strip()

    mu_kickoff_return_tds = mu_kickoff_return_stats.split("-")[2].strip()
    opp_kickoff_return_tds = opp_kickoff_return_stats.split("-")[2].strip()

    mu_kickoff_return_avg = int(mu_kickoff_return_yards) / int(mu_kickoff_returns) if int(mu_kickoff_returns) > 0 else 0
    opp_kickoff_return_avg = int(opp_kickoff_return_yards) / int(opp_kickoff_returns) if int(opp_kickoff_returns) > 0 else 0

    mu_ints_stats = rows[40].find_all("td")[1].text.strip()
    opp_ints_stats = rows[40].find_all("td")[2].text.strip()

    mu_ints = mu_ints_stats.split("-")[0].strip()
    opp_ints = opp_ints_stats.split("-")[0].strip()

    mu_int_return_yards = mu_ints_stats.split("-")[1].strip()
    opp_int_return_yards = opp_ints_stats.split("-")[1].strip()

    mu_int_return_tds = mu_ints_stats.split("-")[2].strip()
    opp_int_return_tds = opp_ints_stats.split("-")[2].strip()

    return {
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

stats = extract_boxscore_stats(url)
import pandas as pd
from pathlib import Path

# convert to DataFrame (1 row)
df = pd.DataFrame([stats])

# ensure folder exists
output_path = Path('game_stats.csv')
output_path.parent.mkdir(parents=True, exist_ok=True)

# save
df.to_csv(output_path, index=False)

print(f"Saved to {output_path}")