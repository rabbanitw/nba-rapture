"""Hustle + defensive-matchup + rim-protection dashboards -> Mongo
(source="nba-hustle").

*** RUN THIS ON YOUR OWN MACHINE — stats.nba.com refuses datacenter IPs
    (verified again 2026-08-18 from the devcontainer: requests hang). ***

Closes three DEFENSE-side ingredient gaps named in 538's methodology that the
matrix verifiably lacks (see RESULTS_defense_gaps.md):

  hustle          deflections, loose balls recovered, charges drawn (the real
                  charge count -- our pbp proxy is "Charge Fouls Drawn"),
                  contested 2s/3s. leaguehustlestatsplayer, 2015-16+.
  defend dash     opponent FGA/FG% at rim and overall with the player as
                  closest defender, straight from the Defense Dashboard.
                  leaguedashptdefend, 2013-14+. Complements defend.npz (which
                  came from a different snapshot source) with a uniform
                  full-history pull.
  season matchups leagueseasonmatchups, 2017-18+: partial possessions,
                  player points, team points while GUARDING each opponent.
                  Aggregated defender-side on save (matchup poss-weighted
                  opponent scoring = the closest scrapeable stand-in for the
                  positional-opponent variables; posmatch v1/v2 built from
                  our own data were null, this is the NBA's own accounting).

Usage mirrors scrape_shotdash.py:
  python scraping/scrape_hustle.py --rs-only            # all seasons, Mongo
  python scraping/scrape_hustle.py --raw-dir raw_out    # JSONL, no Mongo
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
from scrape_shotdash import HEADERS, load_roster, _mmddyyyy

SOURCE = "nba-hustle"
DELAY = 2.0
MAX_ATTEMPTS = 6
TIMEOUT = 90
HUSTLE_BASE = "https://stats.nba.com/stats/leaguehustlestatsplayer"
DEFEND_BASE = "https://stats.nba.com/stats/leaguedashptdefend"
MATCHUP_BASE = "https://stats.nba.com/stats/leagueseasonmatchups"
FIRST_SEASON = {"hustle": "2015-16", "defend-overall": "2013-14",
                "defend-rim": "2013-14", "defend-3pt": "2013-14",
                "matchups": "2017-18"}
IDENTITY = {"PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION",
            "TEAM_NAME", "AGE", "G", "W", "L", "CLOSE_DEF_PERSON_ID",
            "PLAYER_LAST_TEAM_ID", "PLAYER_LAST_TEAM_ABBREVIATION",
            "PLAYER_POSITION", "FREQ"}


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


def common_params(season, api_type, date_from, date_to):
    return {"College": "", "Conference": "", "Country": "",
            "DateFrom": date_from, "DateTo": date_to, "Division": "",
            "DraftPick": "", "DraftYear": "", "Height": "", "LeagueID": "00",
            "Location": "", "Month": 0, "OpponentTeamID": 0, "Outcome": "",
            "PORound": 0, "PerMode": "Totals", "PlayerExperience": "",
            "PlayerPosition": "", "Season": season, "SeasonSegment": "",
            "SeasonType": api_type, "TeamID": 0, "VsConference": "",
            "VsDivision": "", "Weight": ""}


def fetch_table(kind, season, api_type, date_from, date_to):
    """-> (headers, rows) with a PLAYER_ID column, defender-side."""
    if kind == "hustle":
        p = common_params(season, api_type, date_from, date_to)
        p.update({"PaceAdjust": "N"})
        rs = get_json(HUSTLE_BASE, p)["resultSets"][0]
        return rs["headers"], rs["rowSet"]
    if kind.startswith("defend-"):
        cat = {"defend-overall": "Overall", "defend-rim": "Less Than 6Ft",
               "defend-3pt": "3 Pointers"}[kind]
        p = common_params(season, api_type, date_from, date_to)
        p.update({"DefenseCategory": cat, "GameSegment": "", "LastNGames": 0,
                  "Period": 0})
        rs = get_json(DEFEND_BASE, p)["resultSets"][0]
        heads = ["PLAYER_ID" if h == "CLOSE_DEF_PERSON_ID" else h
                 for h in rs["headers"]]
        return heads, rs["rowSet"]
    if kind == "matchups":
        p = {"LeagueID": "00", "PerMode": "Totals", "Season": season,
             "SeasonType": api_type, "DefPlayerID": "", "OffPlayerID": "",
             "DefTeamID": "", "OffTeamID": ""}
        rs = get_json(MATCHUP_BASE, p)["resultSets"][0]
        h = rs["headers"]
        di = h.index("DEF_PLAYER_ID")
        agg = {}
        for row in rs["rowSet"]:
            r = dict(zip(h, row))
            a = agg.setdefault(r["DEF_PLAYER_ID"], {
                "PARTIAL_POSS": 0.0, "PLAYER_PTS": 0.0, "MATCHUP_AST": 0.0,
                "MATCHUP_TOV": 0.0, "MATCHUP_FGM": 0.0, "MATCHUP_FGA": 0.0,
                "MATCHUP_FG3M": 0.0, "MATCHUP_FG3A": 0.0, "MATCHUP_FTA": 0.0,
                "SFL": 0.0, "MATCHUP_MIN": 0.0})
            for k in list(a):
                v = r.get(k)
                if isinstance(v, (int, float)):
                    a[k] += v
        heads = ["PLAYER_ID"] + list(next(iter(agg.values())).keys()) \
            if agg else ["PLAYER_ID"]
        rows = [[pid] + list(v.values()) for pid, v in agg.items()]
        return heads, rows
    raise ValueError(kind)


def scrape_cell(coll, cell, roster, dry_run=False):
    print(f"  {cell['season']} {cell['season_type']}")
    for kind, first in FIRST_SEASON.items():
        if cell["season"] < first:
            continue
        heads, rows = fetch_table(kind, cell["season"], cell["api_type"],
                                  _mmddyyyy(cell["from"]),
                                  _mmddyyyy(cell["to"]))
        pid_i = heads.index("PLAYER_ID")
        name_i = heads.index("PLAYER_NAME") if "PLAYER_NAME" in heads else None
        docs, unknown = [], 0
        for row in rows:
            pid = str(row[pid_i])
            player = roster.get(pid)
            if player is None:
                unknown += 1
            api_name = row[name_i] if name_i is not None else \
                (player["name"] if player else pid)
            doc = {h: v for h, v in zip(heads, row) if h not in IDENTITY}
            doc.update({"PLAYER": api_name, "name": api_name,
                        "standard_name": player["name"] if player else api_name,
                        "nba_player_id": pid, "source": SOURCE,
                        "data_type": kind, "timestamp": cell["timestamp"],
                        "season_type": cell["season_type"]})
            docs.append(doc)
        ins, mod, matched = mongo_sink.write_rows(coll, docs, SOURCE, dry_run)
        print(f"    {kind:<15} {len(rows):>5} players "
              f"({unknown} not in roster)  "
              f"mongo: {ins} ins, {mod} upd, {matched - mod} same")
        time.sleep(DELAY)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", nargs="*", default=list(season_dates.SNAPSHOTS))
    ap.add_argument("--rs-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--raw-dir", help="write JSONL here instead of Mongo")
    args = ap.parse_args()
    coll = (mongo_sink.RawSink(args.raw_dir) if args.raw_dir
            else None if args.dry_run else mongo_sink.check_connection())
    cells = [c for c in season_dates.cells(tuple(args.seasons))]
    print(f"[nba-hustle] {len(args.seasons)} season(s)")
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
    print("[nba-hustle] done")


if __name__ == "__main__":
    main()
