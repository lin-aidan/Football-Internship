
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


def _find_passing_table_from_dfs(dfs):
	for df in dfs:
		cols = [str(c).strip().lower() for c in df.columns]
		joined = ' '.join(cols)
		if (('att' in joined or 'attempt' in joined) and
				('yds' in joined or 'yards' in joined)):
			return df
	return None


def scrape_passing(url=URL):
	r = requests.get(url, timeout=15)
	r.raise_for_status()

	# Parse tables using BeautifulSoup only (avoid XML parsers)
	soup = BeautifulSoup(r.text, 'html.parser')

	tables = []
	for tbl in soup.find_all('table'):
		# extract headers
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

	# Try to find passing table among parsed tables
	df = _find_passing_table_from_dfs(tables)
	if df is not None:
		return df

	# Try to find a section titled "Passing" and parse the next table
	for header_tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'strong']):
		txt = header_tag.get_text(strip=True).lower()
		if 'passing' in txt:
			tbl = header_tag.find_next('table')
			if tbl:
				# parse same as above
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
	"""Return True if the stats page for the year exists and contains a passing table."""
	url = f"{BASE_URL}/{year}"
	try:
		r = requests.get(url, timeout=timeout)
		r.raise_for_status()
	except Exception:
		return False

	df = scrape_passing(url)
	return not df.empty


def list_available_years(start: int = 2012, end: int = 2025) -> List[int]:
	years = []
	for y in range(start, end + 1):
		if check_year(y):
			years.append(y)
	return years


def main():
	parser = argparse.ArgumentParser(description='Scrape Mercyhurst passing stats')
	parser.add_argument('--url', default=URL, help='Stats page URL')
	parser.add_argument('--year', type=int, help='Year to fetch (overrides --url)')
	parser.add_argument('--list-years', action='store_true', help='List available years on the site')
	parser.add_argument('--start-year', type=int, default=2012, help='Start year for listing')
	parser.add_argument('--end-year', type=int, default=2025, help='End year for listing')
	parser.add_argument('-o', '--output', default='mercyhurst_passing_2025.csv')
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

	df = scrape_passing(args.url)
	if df.empty:
		print('No passing table found on the page.', file=sys.stderr)
		sys.exit(2)

	df.columns = [str(c).strip() for c in df.columns]

	# Drop bio/link columns
	drop_cols = [c for c in df.columns if 'bio' in c.lower() or 'link' in c.lower()]
	if drop_cols:
		df = df.drop(columns=drop_cols, errors='ignore')

	# Identify player/name column
	player_col = None
	for c in df.columns:
		if 'player' in c.lower() or 'name' in c.lower():
			player_col = c
			break
	if player_col is None and len(df.columns) >= 2:
		player_col = df.columns[1]

	# Clean player names like "Urena, Adam16Urena, Adam" -> "Urena, Adam"
	name_pattern = re.compile(r"([A-Za-z][A-Za-z'\-\. ]+?,\s*[A-Za-z][A-Za-z'\-\. ]+)")

	def extract_name(s):
		if not isinstance(s, str):
			return s
		matches = name_pattern.findall(s)
		if matches:
			return matches[-1].strip()
		# fallback: remove stray digits and duplicate parts
		cleaned = re.sub(r"\d+", "", s).strip()
		return cleaned

	if player_col:
		df[player_col] = df[player_col].astype(str).apply(extract_name)

	# Add Year column (try to infer from args.year or URL)
	year_val = None
	if args.year:
		year_val = args.year
	else:
		m = re.search(r"/(\d{4})/?$", args.url)
		if m:
			year_val = int(m.group(1))

	if year_val is not None:
		df['Year'] = year_val

	# Save individual file
	if args.format == 'csv':
		df.to_csv(args.output, index=False)
	else:
		df.to_json(args.output, orient='records')

	print('Saved', args.output)

	# Optionally append to master CSV
	if args.append_to:
		master = Path(args.append_to)
		write_header = not master.exists()

		if master.exists():
			# align to existing columns if present
			try:
				existing_cols = list(pd.read_csv(master, nrows=0).columns)
				# ensure all existing cols are present in df
				for c in existing_cols:
					if c not in df.columns:
						df[c] = ''
				# reindex to existing order (append any new cols at end)
				new_cols = [c for c in df.columns if c not in existing_cols]
				df = df[existing_cols + new_cols]
			except Exception:
				pass

		df.to_csv(master, mode='a', index=False, header=write_header)
		print('Appended to', str(master))

	# Append multiple years: find available years and add those not present in master
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
				df_y = scrape_passing(f"{BASE_URL}/{y}")
				if df_y.empty:
					print('No data for', y, file=sys.stderr)
					continue

				df_y.columns = [str(c).strip() for c in df_y.columns]
				drop_cols = [c for c in df_y.columns if 'bio' in c.lower() or 'link' in c.lower()]
				if drop_cols:
					df_y = df_y.drop(columns=drop_cols, errors='ignore')

				# clean player names
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
				# align to existing master columns
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

