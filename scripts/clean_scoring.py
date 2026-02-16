#!/usr/bin/env python3
import re
import pandas as pd
from pathlib import Path

IN_PATH = Path('/workspaces/Football-Internship/Season Stats/Offense/all_mercyhurst_scoring.csv')

STAT_COLS = ["TD", "FG", "SAF", "KICK", "RUSH", "RCV", "PASS", "DXP", "PTS"]


def collapse_dup_name(s: str) -> str:
    s = (s or '').strip()
    # remove digits
    s = ''.join(ch for ch in s if not ch.isdigit())
    # collapse exact duplicated halves e.g. 'A A' or 'NameName'
    L = len(s)
    for n in range(3, L // 2 + 1):
        if s[:n] == s[n:2*n]:
            return s[:n].strip()
    return s


def extract_first_number(s: str):
    if s is None:
        return ''
    s = str(s)
    m = re.search(r"-?\d+", s)
    return m.group(0) if m else ''


def clean():
    df = pd.read_csv(IN_PATH, dtype=str).fillna("")

    for i, row in df.iterrows():
        # find best name candidate: any cell containing a comma
        name_candidate = ''
        for col in df.columns:
            val = str(row[col])
            if ',' in val and any(c.isalpha() for c in val):
                # prefer longer candidate
                if len(val) > len(name_candidate):
                    name_candidate = val

        name = collapse_dup_name(name_candidate)
        if name:
            df.at[i, 'Name'] = name

        # ensure jersey '#' is clean: first numeric in '#' or in other cells
        jersey = extract_first_number(row.get('#', ''))
        if not jersey:
            # search other columns for a small integer (<=3 digits)
            for col in df.columns:
                val = str(row[col])
                m = re.search(r"\b(\d{1,3})\b", val)
                if m:
                    jersey = m.group(1)
                    break
        df.at[i, '#'] = jersey

        # clean stat columns: keep only first integer (or blank)
        for sc in STAT_COLS:
            val = row.get(sc, "")
            num = extract_first_number(val)
            df.at[i, sc] = num

    # drop any accidental duplicate name fragments still in columns other than Name
    df.to_csv(IN_PATH, index=False)
    print(f"Cleaned CSV written to {IN_PATH}")


if __name__ == '__main__':
    clean()
