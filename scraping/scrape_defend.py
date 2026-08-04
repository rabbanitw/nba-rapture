"""Nearest-defender defended shots -> Mongo (source="nba-defend").

*** RUN THIS ON YOUR OWN MACHINE — stats.nba.com refuses datacenter IPs. ***

This is the endpoint behind the recovered RAPTOR spec's core defensive inputs
(training/raptor_methodology_fulltext.txt): defended 2-point shots as nearest
defender (missed +1.05, made -0.33 in their RAPM regression) and defended 3-point
attempts (+0.17, results deliberately ignored as noise). 538's defensive R² against
RAPM was ~0.6 with this class of data versus ~0.3 without it — it is the single
feed most likely to move our defense model, whose known ceiling is exactly the
absence of player-attributed shot defense.

Six DefenseCategory tables per cell, stored as data_type with API column names
kept verbatim -- there is no legacy schema to match, so nothing can mismap. Players
join by CLOSE_DEF_PERSON_ID against the roster files (exact ID join, no names).

Availability: 2013-14 onward, so every cell including the test seasons is covered
and the feature is fully validatable.

Run:  python scraping/scrape_defend.py                 # all cells, ~156 requests
      python scraping/scrape_defend.py --seasons 2013-14 2014-15
      python scraping/scrape_defend.py --raw-dir raw_defend   # if Atlas unreachable
"""

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import mongo_sink
import season_dates
from scrape_pbp_totals import ROSTER_DIR

SOURCE = "nba-defend"
BASE = "https://stats.nba.com/stats/leaguedashptdefend"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Connection": "keep-alive",
}

DELAY = 2.0
MAX_ATTEMPTS = 6
TIMEOUT = 90

# data_type slug -> API DefenseCategory. "2 Pointers" and "3 Pointers" are the two
# 538 actually used; the rest cost nothing and give rim/perimeter granularity.
CATEGORIES = {
    "defend-overall": "Overall",
    "defend-2pt": "2 Pointers",
    "defend-3pt": "3 Pointers",
    "defend-lt6ft": "Less Than 6Ft",
    "defend-lt10ft": "Less Than 10Ft",
    "defend-gt15ft": "Greater Than 15Ft",
}

# Identify the row; everything else is stored verbatim as a stat column.
IDENTITY = {"CLOSE_DEF_PERSON_ID", "PLAYER_ID", "PLAYER_NAME",
            "PLAYER_LAST_TEAM_ID", "PLAYER_LAST_TEAM_ABBREVIATION",
            "PLAYER_POSITION", "AGE"}


def get_json(category, season, api_type, date_from, date_to):
    params = {
        "College": "", "Conference": "", "Country": "", "DateFrom": date_from,
        "DateTo": date_to, "DefenseCategory": category, "Division": "",
        "DraftPick": "", "DraftYear": "", "GameSegment": "", "Height": "",
        "LastNGames": 0, "LeagueID": "00", "Location": "", "Month": 0,
        "OpponentTeamID": 0, "Outcome": "", "PORound": 0, "PerMode": "Totals",
        "Period": 0, "PlayerExperience": "", "PlayerID": "", "PlayerPosition": "",
        "Season": season, "SeasonSegment": "", "SeasonType": api_type, "TeamID": 0,
        "VsConference": "", "VsDivision": "", "Weight": "",
    }
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    last = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return json.loads(r.read())
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                json.JSONDecodeError, OSError) as e:
            last = f"{type(e).__name__}: {e}"
            print(f"      attempt {attempt}/{MAX_ATTEMPTS} failed ({last})")
            time.sleep(DELAY * attempt)
    raise RuntimeError(
        f"stats.nba.com unreachable after {MAX_ATTEMPTS} attempts ({last}). "
        f"If every request does this, the IP is blocked -- run from a home "
        f"connection.")


def load_roster(cell):
    path = ROSTER_DIR / (f"roster_{cell['timestamp']}_"
                         f"{cell['season_type'].replace(' ', '_')}.json")
    if not path.exists():
        raise SystemExit(f"missing {path} -- run scrape_pbp_totals.py first "
                         f"(or pull the rosters/ directory)")
    return json.loads(path.read_text())["players"]


def _mmddyyyy(iso):
    y, m, d = iso.split("-")
    return f"{m}/{d}/{y}"


def scrape_cell(coll, cell, roster, dry_run=False):
    print(f"  {cell['season']} {cell['season_type']}")
    for data_type, category in CATEGORIES.items():
        payload = get_json(category, cell["season"], cell["api_type"],
                           _mmddyyyy(cell["from"]), _mmddyyyy(cell["to"]))
        rs = payload["resultSets"][0]
        headers, rows = rs["headers"], rs["rowSet"]
        id_col = next(h for h in ("CLOSE_DEF_PERSON_ID", "PLAYER_ID")
                      if h in headers)
        pid_i = headers.index(id_col)
        name_i = headers.index("PLAYER_NAME")

        docs, unknown = [], 0
        for row in rows:
            pid = str(row[pid_i])
            api_name = row[name_i]
            player = roster.get(pid)
            if player is None:
                unknown += 1
            doc = {h: v for h, v in zip(headers, row) if h not in IDENTITY}
            doc.update({
                "PLAYER": api_name,
                "name": api_name,
                "standard_name": player["name"] if player else api_name,
                "nba_player_id": pid,
                "source": SOURCE,
                "data_type": data_type,
                "timestamp": cell["timestamp"],
                "season_type": cell["season_type"],
            })
            docs.append(doc)
        ins, mod, matched = mongo_sink.write_rows(coll, docs, SOURCE, dry_run)
        print(f"    {data_type:<16} {len(rows):>4} players "
              f"({unknown} not in roster)  "
              f"mongo: {ins} ins, {mod} upd, {matched - mod} same")
        time.sleep(DELAY)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", nargs="*", default=list(season_dates.SNAPSHOTS))
    ap.add_argument("--rs-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--raw-dir", help="write JSONL here instead of Mongo "
                                      "(load later with load_raw.py)")
    args = ap.parse_args()

    coll = (mongo_sink.RawSink(args.raw_dir) if args.raw_dir
            else None if args.dry_run else mongo_sink.check_connection())
    print(f"[nba-defend] {len(args.seasons)} season(s)")
    for cell in season_dates.cells(tuple(args.seasons)):
        if args.rs_only and cell["season_type"] != "Regular season":
            continue
        try:
            scrape_cell(coll, cell, load_roster(cell), args.dry_run)
        except SystemExit as e:
            print(f"  SKIPPED {cell['season']} {cell['season_type']}: {e}")
        except Exception as e:
            print(f"  FAILED {cell['season']} {cell['season_type']}: "
                  f"{type(e).__name__}: {e}")
    print("[nba-defend] done")


if __name__ == "__main__":
    main()
