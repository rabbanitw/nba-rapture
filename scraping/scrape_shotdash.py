"""Shot Dashboard (defender-distance splits) + time-of-possession -> Mongo
(source="nba-shotdash").

*** RUN THIS ON YOUR OWN MACHINE — stats.nba.com refuses datacenter IPs. ***

Closes the two offense-side ingredient gaps named in 538's methodology that the
matrix verifiably lacks (checked 2026-08-12):

  contested shots   538 uses "the number of contested 3-pointers the player took"
                    as a floor-spacing measure. leaguedashplayerptshot splits every
                    player's own shots by closest-defender distance: 0-2ft (very
                    tight), 2-4ft (tight), 4-6ft (open), 6+ft (wide open) -- FGA/
                    FGM/FG3A/FG3M/frequencies per bucket, verbatim columns.
  time of possess.  the box-RAPTOR spec names time of possession; we carry touch
                    counts but no TIME_OF_POSS / AVG_SEC_PER_TOUCH /
                    AVG_DRIB_PER_TOUCH. leaguedashptstats PtMeasureType=Possessions.

Five tables per cell (4 defender-distance buckets + possessions), stored with API
column names verbatim under data_type; players join by PLAYER_ID against the
roster files (exact ID join, no names). Availability: 2013-14 onward (tracking
era), so every cell including the test seasons is covered.

Run:  python scraping/scrape_shotdash.py                  # whole-season cells, ~130 requests
      python scraping/scrape_shotdash.py --snapshots      # 38 in-season cells (defend_snapshots.json)
      python scraping/scrape_shotdash.py --seasons 2013-14 2014-15
      python scraping/scrape_shotdash.py --raw-dir raw_shotdash   # if Atlas unreachable
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

SOURCE = "nba-shotdash"
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

# data_type -> (endpoint, extra params). Column names stored verbatim.
SHOT_BASE = "https://stats.nba.com/stats/leaguedashplayerptshot"
POSS_BASE = "https://stats.nba.com/stats/leaguedashptstats"
TABLES = {
    "shots-def0-2": (SHOT_BASE, {"CloseDefDistRange": "0-2 Feet - Very Tight"}),
    "shots-def2-4": (SHOT_BASE, {"CloseDefDistRange": "2-4 Feet - Tight"}),
    "shots-def4-6": (SHOT_BASE, {"CloseDefDistRange": "4-6 Feet - Open"}),
    "shots-def6plus": (SHOT_BASE, {"CloseDefDistRange": "6+ Feet - Wide Open"}),
    "possessions": (POSS_BASE, {"PtMeasureType": "Possessions"}),
}

IDENTITY = {"PLAYER_ID", "PLAYER_NAME", "PLAYER_LAST_TEAM_ID",
            "PLAYER_LAST_TEAM_ABBREVIATION", "PLAYER_POSITION", "AGE",
            "TEAM_ID", "TEAM_ABBREVIATION", "W", "L"}


def build_params(base, extra, season, api_type, date_from, date_to):
    common = {
        "College": "", "Conference": "", "Country": "", "DateFrom": date_from,
        "DateTo": date_to, "Division": "", "DraftPick": "", "DraftYear": "",
        "Height": "", "LastNGames": 0, "LeagueID": "00", "Location": "",
        "Month": 0, "OpponentTeamID": 0, "Outcome": "", "PORound": 0,
        "PerMode": "Totals", "PlayerExperience": "", "PlayerPosition": "",
        "Season": season, "SeasonSegment": "", "SeasonType": api_type,
        "TeamID": 0, "VsConference": "", "VsDivision": "", "Weight": "",
    }
    if base == SHOT_BASE:
        common.update({"CloseDefDistRange": "", "DribbleRange": "",
                       "GameSegment": "", "GeneralRange": "Overall",
                       "Period": 0, "ShotClockRange": "", "ShotDistRange": "",
                       "StarterBench": "", "TouchTimeRange": ""})
    else:
        common.update({"GameScope": "", "PlayerOrTeam": "Player",
                       "PtMeasureType": "", "StarterBench": ""})
    common.update(extra)
    return common


def get_json(base, params):
    url = f"{base}?{urllib.parse.urlencode(params)}"
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
    ts = cell["timestamp"]
    if cell.get("snapshot"):
        ts = season_dates.SNAPSHOTS[cell["season"]]
    path = ROSTER_DIR / (f"roster_{ts}_"
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
    for data_type, (base, extra) in TABLES.items():
        payload = get_json(base, build_params(
            base, extra, cell["season"], cell["api_type"],
            _mmddyyyy(cell["from"]), _mmddyyyy(cell["to"])))
        rs = payload["resultSets"][0]
        headers, rows = rs["headers"], rs["rowSet"]
        pid_i = headers.index("PLAYER_ID")
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
        print(f"    {data_type:<15} {len(rows):>4} players "
              f"({unknown} not in roster)  "
              f"mongo: {ins} ins, {mod} upd, {matched - mod} same")
        time.sleep(DELAY)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", nargs="*", default=list(season_dates.SNAPSHOTS))
    ap.add_argument("--rs-only", action="store_true")
    ap.add_argument("--snapshots", action="store_true",
                    help="scrape the in-season snapshot cells listed in "
                         "defend_snapshots.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--raw-dir", help="write JSONL here instead of Mongo "
                                      "(load later with load_raw.py)")
    args = ap.parse_args()

    coll = (mongo_sink.RawSink(args.raw_dir) if args.raw_dir
            else None if args.dry_run else mongo_sink.check_connection())
    if args.snapshots:
        cells = json.loads((Path(__file__).resolve().parent
                            / "defend_snapshots.json").read_text())
        for c in cells:
            c["snapshot"] = True
        print(f"[nba-shotdash] {len(cells)} in-season snapshot cells")
    else:
        cells = [c for c in season_dates.cells(tuple(args.seasons))]
        print(f"[nba-shotdash] {len(args.seasons)} season(s)")
    for cell in cells:
        if args.rs_only and cell["season_type"] != "Regular season":
            continue
        try:
            scrape_cell(coll, cell, load_roster(cell), args.dry_run)
        except SystemExit as e:
            print(f"  SKIPPED {cell['season']} {cell['season_type']}: {e}")
        except Exception as e:
            print(f"  FAILED {cell['season']} {cell['season_type']}: "
                  f"{type(e).__name__}: {e}")
    print("[nba-shotdash] done")


if __name__ == "__main__":
    main()
