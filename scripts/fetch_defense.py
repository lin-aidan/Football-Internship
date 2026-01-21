#!/usr/bin/env python3
"""Fetch Mercyhurst football defensive stats for 2012-2025 and append to CSV.

Usage: python scripts/fetch_defense.py
"""
import logging
import os
import re
import time
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE_URL = "https://hurstathletics.com/sports/football/stats"
OUT_CSV = Path("Season Stats/Defense/all_mercyhurst_defense.csv")
YEARS = list(range(2012, 2026))

DESIRED = ["#", "Name", "GP", "NO", "SOLO", "ASST", "TOT", "TFL-YDS", "SACKS-YDS", "INT", "BU", "QBH", "FR", "FF", "KICK", "SAF"]


def ensure_output_dir(path: Path):
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def extract_tables_with_playwright(url):
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        logging.debug("Playwright not available")
        return None

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=20000)
        # allow JS to render
        page.wait_for_timeout(800)
        # Prefer tables inside the Individual - Defense section
        sec = page.query_selector("section#individual-defense")
        if sec:
            tables = sec.query_selector_all("table")
        else:
            tables = page.query_selector_all("table")
        for t in tables:
            # try to get headers
            hdrs = []
            thead = t.query_selector("thead")
            if thead:
                ths = thead.query_selector_all("th")
                for th in ths:
                    hdrs.append(th.inner_text().strip())
            else:
                # fallback to first row
                first = t.query_selector("tr")
                if first:
                    cells = first.query_selector_all("th,td")
                    for c in cells:
                        hdrs.append(c.inner_text().strip())

            rows = []
            tbody = t.query_selector("tbody")
            if tbody:
                trs = tbody.query_selector_all("tr")
            else:
                trs = t.query_selector_all("tr")[1:]
            for tr in trs:
                cells = tr.query_selector_all("td,th")
                row = [c.inner_text().strip() for c in cells]
                if row:
                    rows.append(row)

            results.append({"headers": hdrs, "rows": rows})

        browser.close()
    return results


def find_defense_table_from_rendered(tables):
    if not tables:
        return None, None
    for t in tables:
        norm = [re.sub(r"[\s\.]+", "", (h or "").lower().replace("–", "-")) for h in t["headers"]]
        if any(x in ":" or True for x in []):
            pass
        # look for defense-specific columns
        if any(x in " ".join(norm) for x in ("solo", "asst", "tfl", "tfl-yds", "sack", "int", "bu")):
            return t["headers"], t["rows"]
    return None, None


def normalize_and_write(rows, headers, year):
    if not rows or not headers:
        return []
    # build list of dicts mapping desired columns
    header_norm = [re.sub(r"[\s\.]+", "", (h or "").upper().replace("–", "-")) for h in headers]

    def find_idx(col_name):
        target = re.sub(r"[\s\.]+", "", col_name.upper())
        for i, hn in enumerate(header_norm):
            if target == hn or target in hn or hn in target:
                return i
        # special case for '#'
        if col_name == "#":
            for i, h in enumerate(headers):
                if h.strip() in ("#", "No", "No.", "NO") or re.match(r"^#|^no\b", h, re.I):
                    return i
        # special-case Name -> look for Player header variants
        if col_name == "Name":
            for i, h in enumerate(headers):
                hn = re.sub(r"[\s\.]+", "", (h or "").lower())
                if "player" in hn or "playername" in hn or h.strip().lower() in ("player", "player name", "playername"):
                    return i
        return None

    col_indices = {col: find_idx(col) for col in DESIRED}
    out = []
    for r in rows:
        d = {}
        for col, idx in col_indices.items():
            raw = r[idx] if (idx is not None and idx < len(r)) else ""
            # clean Name field when it's present but contains duplicated number/name fragments
            if col == "Name" and raw:
                # split on newlines and choose the best candidate containing a comma (Last, First)
                parts = [p.strip() for p in re.split(r"[\n\r]+", raw) if p.strip()]
                name_candidate = ""
                for p in parts:
                    if "," in p:
                        if len(p) > len(name_candidate):
                            name_candidate = p
                if not name_candidate:
                    # fallback: remove leading numbers and stray quotes
                    name_candidate = re.sub(r"^[\d\s\"']+", "", raw).strip()
                # final cleanup: collapse internal whitespace and strip quotes
                name_candidate = name_candidate.replace('"', '').strip()
                d[col] = name_candidate
            else:
                d[col] = raw
        d["Year"] = year
        out.append(d)
    return out


def main():
    all_rows = []
    for year in YEARS:
        url = f"{BASE_URL}/{year}"
        logging.info("Rendering %s", url)
        tables = extract_tables_with_playwright(url)
        headers, rows = find_defense_table_from_rendered(tables)
        if headers is None:
            logging.warning("No defense table found for %s", url)
            time.sleep(0.3)
            continue
        parsed = normalize_and_write(rows, headers, year)
        logging.info("Found %d defensive rows for %s", len(parsed), year)
        all_rows.extend(parsed)

    if not all_rows:
        logging.info("No rows collected; nothing to append.")
        return

    cols = DESIRED + ["Year"]
    df = pd.DataFrame(all_rows)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]
    ensure_output_dir(OUT_CSV)
    # Overwrite the output CSV (do not append)
    df.to_csv(OUT_CSV, mode="w", index=False, header=True)
    logging.info("Wrote %d rows to %s", len(df), OUT_CSV)


if __name__ == "__main__":
    main()
