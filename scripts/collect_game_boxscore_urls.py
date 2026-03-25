import requests
from bs4 import BeautifulSoup
import csv
from pathlib import Path

years = list(range(2012, 2026))
base_url = "https://hurstathletics.com"

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


rows = []

for year in years:
    url = f"{base_url}/sports/football/schedule/{year}"
    data = extract_game_data(url, year)
    rows.extend(data)

output_path = Path('Season Stats/Team/game_boxscore_urls.csv')
output_path.parent.mkdir(parents=True, exist_ok=True)

with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['year', 'opponent', 'url'])
    
    for year, opponent, link in rows:
        writer.writerow([year, opponent, link])

print(f"Saved {len(rows)} rows to {output_path}")