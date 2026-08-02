"""NBA player tracking stats -> Mongo (source="nba-tracking").

*** RUN THIS ON YOUR OWN MACHINE. ***

stats.nba.com does not answer datacenter IPs. From the dev container the TLS
handshake completes, the request goes out, and the server then says nothing at all
until the socket times out -- 50s, zero bytes, with or without browser headers.
cdn.nba.com answers 403. Selenium does not help: the browser would issue the same
XHR from the same address. Everything else in this scrape runs fine from anywhere;
this one file needs a residential connection.

Two changes from nba_tracking_scrape.py:

1. It reads the JSON API instead of driving headless Chrome against
   nba.com/stats and parsing the rendered table with str.split(). That parser
   assumed exactly 12 trailing columns, so it only ever worked for speed-distance
   -- the other 13 tracking types have 10 to 22 columns.

2. Players are joined by NBA player id, not by name. The old pipeline fuzzy-matched
   tracking names against the names 538 knew, and FuzzyDict returns the closest
   match rather than no match. In the existing collection that mis-attributed 2,572
   of 8,370 tracking documents at snapshot 20250306125347 to the wrong player --
   'Bronny James' stored as 'Bernard James', 'Bilal Coulibaly' as 'Bradley Beal',
   'Amen Thompson' as 'Jason Thompson'. Roster files written by
   scrape_pbp_totals.py carry pbpstats' EntityId, which is the NBA player id, so
   the join is exact.

Column names are translated to the labels the existing documents use, because those
were scraped from the rendered table's two-line headers and training/coverage.py
matches fields by exact name -- 'CONTESTED\\nDREB', not 'REB_CONTEST'. If the API
returns a column this file cannot place, it says so and refuses to write; send the
--report output back rather than storing a schema the rest of the pipeline can't read.

Run:  python scraping/scrape_nba_tracking.py --report      # check mapping, write nothing
      python scraping/scrape_nba_tracking.py               # scrape and upsert
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

SOURCE = "nba-tracking"
BASE = "https://stats.nba.com/stats/leaguedashptstats"

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

# stats.nba.com rate limits aggressively and answers slowly; go gently.
DELAY = 2.0
MAX_ATTEMPTS = 6
TIMEOUT = 90

# data_type stored in Mongo -> (PtMeasureType, PlayerOrTeam) for the API.
# The 14 keys are training/coverage.py's TRACK_TYPES, unchanged.
MEASURE = {
    "catch-shoot": "CatchShoot",
    "defensive-impact": "Defense",
    "defensive-rebounding": "Rebounding",
    "drives": "Drives",
    "elbow-touch": "ElbowTouch",
    "offensive-rebounding": "Rebounding",
    "paint-touch": "PaintTouch",
    "passing": "Passing",
    "pullup": "PullUpShot",
    "rebounding": "Rebounding",
    "shooting-efficiency": "Efficiency",
    "speed-distance": "SpeedDistance",
    "touches": "Possessions",
    "tracking-post-ups": "PostTouch",
}

# API column -> the label the existing documents use. Newlines are real: the old
# scraper read them out of two-line <th> elements and training/coverage.py keys
# features on the exact string.
COMMON = {"GP": "GP", "W": "W", "L": "L", "MIN": "MIN"}

FIELD_MAP = {
    # The catch-and-shoot table is the one tracking page that shows no W/L, and the
    # stored documents match. None means "the API sends it, we deliberately drop it".
    "catch-shoot": {
        "GP": "GP", "MIN": "MIN", "W": None, "L": None,
        "CATCH_SHOOT_PTS": "PTS", "CATCH_SHOOT_FGM": "FGM",
        "CATCH_SHOOT_FGA": "FGA", "CATCH_SHOOT_FG_PCT": "FG%",
        "CATCH_SHOOT_FG3M": "3PM", "CATCH_SHOOT_FG3A": "3PA",
        "CATCH_SHOOT_FG3_PCT": "3P%", "CATCH_SHOOT_EFG_PCT": "EFG%",
    },
    "defensive-impact": {
        **COMMON, "STL": "STL", "BLK": "BLK", "DREB": "DREB",
        "DEF_RIM_FGM": "DFGM", "DEF_RIM_FGA": "DFGA", "DEF_RIM_FG_PCT": "DFG%",
    },
    # DREB_UNCONTEST is one of three uncontested-rebound columns the API now returns
    # that the rendered table never showed, so no document in the collection has a
    # slot for it. Dropped rather than stored: usable_fields() only keeps a field
    # present in both the historical and modern eras, so it could never become a
    # feature, and adding it would put the new cells out of schema with the old ones.
    "defensive-rebounding": {
        **COMMON, "DREB_UNCONTEST": None,
        "DREB": "DREB", "DREB_CONTEST": "CONTESTED\nDREB",
        "DREB_CONTEST_PCT": "CONTESTED\nDREB%", "DREB_CHANCES": "DREB\nCHANCES",
        "DREB_CHANCE_PCT": "DREB\nCHANCE%",
        "DREB_CHANCE_DEFER": "DEFERRED\nDREB CHANCES",
        "DREB_CHANCE_PCT_ADJ": "ADJUSTED\nDREB CHANCE%",
        "AVG_DREB_DIST": "AVG DREB\nDISTANCE",
    },
    "drives": {
        **COMMON, "DRIVES": "DRIVES", "DRIVE_FGM": "FGM", "DRIVE_FGA": "FGA",
        "DRIVE_FG_PCT": "FG%", "DRIVE_FTM": "FTM", "DRIVE_FTA": "FTA",
        "DRIVE_FT_PCT": "FT%", "DRIVE_PTS": "PTS", "DRIVE_PTS_PCT": "PTS%",
        "DRIVE_PASSES": "PASS", "DRIVE_PASSES_PCT": "PASS%", "DRIVE_AST": "AST",
        "DRIVE_AST_PCT": "AST%", "DRIVE_TOV": "TO", "DRIVE_TOV_PCT": "TOV%",
        "DRIVE_PF": "PF", "DRIVE_PF_PCT": "PF%",
    },
    "elbow-touch": {
        **COMMON, "TOUCHES": "TOUCHES", "ELBOW_TOUCHES": "ELBOW\nTOUCHES",
        "ELBOW_TOUCH_FGM": "FGM", "ELBOW_TOUCH_FGA": "FGA",
        "ELBOW_TOUCH_FG_PCT": "FG%", "ELBOW_TOUCH_FTM": "FTM",
        "ELBOW_TOUCH_FTA": "FTA", "ELBOW_TOUCH_FT_PCT": "FT%",
        "ELBOW_TOUCH_PTS": "PTS", "ELBOW_TOUCH_PTS_PCT": "PTS%",
        "ELBOW_TOUCH_PASSES": "PASS", "ELBOW_TOUCH_PASSES_PCT": "PASS%",
        "ELBOW_TOUCH_AST": "AST", "ELBOW_TOUCH_AST_PCT": "AST%",
        "ELBOW_TOUCH_TOV": "TO", "ELBOW_TOUCH_TOV_PCT": "TOV%",
        "ELBOW_TOUCH_FOULS": "PF", "ELBOW_TOUCH_FOULS_PCT": "PF%",
    },
    "offensive-rebounding": {
        **COMMON, "OREB_UNCONTEST": None,
        "OREB": "OREB", "OREB_CONTEST": "CONTESTED\nOREB",
        "OREB_CONTEST_PCT": "CONTESTED\nOREB%", "OREB_CHANCES": "OREB\nCHANCES",
        "OREB_CHANCE_PCT": "OREB\nCHANCE%",
        "OREB_CHANCE_DEFER": "DEFERRED\nOREB CHANCES",
        "OREB_CHANCE_PCT_ADJ": "ADJUSTED\nOREB CHANCE%",
        "AVG_OREB_DIST": "AVG OREB\nDISTANCE",
    },
    "paint-touch": {
        **COMMON, "TOUCHES": "TOUCHES", "PAINT_TOUCHES": "PAINT\nTOUCHES",
        "PAINT_TOUCH_FGM": "FGM", "PAINT_TOUCH_FGA": "FGA",
        "PAINT_TOUCH_FG_PCT": "FG%", "PAINT_TOUCH_FTM": "FTM",
        "PAINT_TOUCH_FTA": "FTA", "PAINT_TOUCH_FT_PCT": "FT%",
        "PAINT_TOUCH_PTS": "PTS", "PAINT_TOUCH_PTS_PCT": "PTS%",
        "PAINT_TOUCH_PASSES": "PASS", "PAINT_TOUCH_PASSES_PCT": "PASS%",
        "PAINT_TOUCH_AST": "AST", "PAINT_TOUCH_AST_PCT": "AST%",
        "PAINT_TOUCH_TOV": "TO", "PAINT_TOUCH_TOV_PCT": "TOV%",
        "PAINT_TOUCH_FOULS": "PF", "PAINT_TOUCH_FOULS_PCT": "PF%",
    },
    # The passing table's last three legacy columns are shifted one place left
    # against their headers, because the rendered table had a header cell missing.
    # Verified arithmetically on the 2018 regular season, exactly on every player
    # checked: the blank-named column holds AST_ADJ (AST + FT_AST + SECONDARY_AST --
    # JJ Barea 434 + 18 + 53 = 505), the column headed "AST ADJ" holds
    # AST / PASSES_MADE as a percentage (434/3344 = 12.98 vs 13.0 stored), and the
    # one headed "AST TO PASS%" holds AST_ADJ / PASSES_MADE (505/3344 = 15.10 vs
    # 15.1 stored). FT_AST itself was never stored.
    "passing": {
        **COMMON, "PASSES_MADE": "PASSES\nMADE",
        "PASSES_RECEIVED": "PASSES\nRECEIVED", "AST": "AST",
        "SECONDARY_AST": "SECONDARY\nAST", "POTENTIAL_AST": "POTENTIAL\nAST",
        "AST_PTS_CREATED": "AST PTS\nCREATED",
        # Stored under its own name from now on. 538 explicitly credits free-throw
        # assists in box RAPTOR offense; v2 used to drop this because the legacy
        # schema had no slot. Legacy/v2 cells get it backfilled by
        # migrate_ft_ast.py as ('' column) - AST - SECONDARY AST, since the blank
        # column holds AST_ADJ = AST + FT_AST + SECONDARY_AST.
        "FT_AST": "FT_AST",
        "AST_ADJ": "",
        "AST_TO_PASS_PCT": "AST\nADJ",
        "AST_TO_PASS_PCT_ADJ": "AST TO\nPASS%",
    },
    "pullup": {
        **COMMON, "PULL_UP_PTS": "PTS", "PULL_UP_FGM": "FGM", "PULL_UP_FGA": "FGA",
        "PULL_UP_FG_PCT": "FG%", "PULL_UP_FG3M": "3PM", "PULL_UP_FG3A": "3PA",
        "PULL_UP_FG3_PCT": "3P%", "PULL_UP_EFG_PCT": "EFG%",
    },
    "rebounding": {
        **COMMON, "REB_UNCONTEST": None,
        "REB": "REB", "REB_CONTEST": "CONTESTED\nREB",
        "REB_CONTEST_PCT": "CONTESTED\nREB%", "REB_CHANCES": "REB\nCHANCES",
        "REB_CHANCE_PCT": "REB\nCHANCE%",
        "REB_CHANCE_DEFER": "DEFERRED\nREB CHANCES",
        "REB_CHANCE_PCT_ADJ": "ADJUSTED\nREB CHANCE%",
        "AVG_REB_DIST": "AVG REB\nDISTANCE",
    },
    # The efficiency and possessions tables spell it POINTS, not PTS, and this one
    # spells effective FG% EFF_FG_PCT rather than the EFG_PCT used elsewhere.
    "shooting-efficiency": {
        **COMMON, "POINTS": "PTS", "DRIVE_PTS": "DRIVE\nPTS",
        "DRIVE_FG_PCT": "DRIVE\nFG%", "CATCH_SHOOT_PTS": "C&S\nPTS",
        "CATCH_SHOOT_FG_PCT": "C&S\nFG%", "PULL_UP_PTS": "PULL UP\nPTS",
        "PULL_UP_FG_PCT": "PULL UP\nFG%", "PAINT_TOUCH_PTS": "PAINT\nTOUCH PTS",
        "PAINT_TOUCH_FG_PCT": "PAINT\nTOUCH FG%",
        "POST_TOUCH_PTS": "POST\nTOUCH PTS",
        "POST_TOUCH_FG_PCT": "POST\nTOUCH FG%",
        "ELBOW_TOUCH_PTS": "ELBOW\nTOUCH PTS",
        "ELBOW_TOUCH_FG_PCT": "ELBOW\nTOUCH FG%", "EFF_FG_PCT": "EFG%",
    },
    "speed-distance": {
        **COMMON, "DIST_FEET": "DIST. FEET", "DIST_MILES": "DIST. MILES",
        "DIST_MILES_OFF": "DIST. MILES OFF", "DIST_MILES_DEF": "DIST. MILES DEF",
        "AVG_SPEED": "AVG SPEED", "AVG_SPEED_OFF": "AVG SPEED OFF",
        "AVG_SPEED_DEF": "AVG SPEED DEF",
    },
    "touches": {
        **COMMON, "POINTS": "PTS", "TOUCHES": "TOUCHES",
        "FRONT_CT_TOUCHES": "FRONT CT\nTOUCHES", "TIME_OF_POSS": "TIME OF\nPOSS",
        "AVG_SEC_PER_TOUCH": "AVG SEC PER\nTOUCH",
        "AVG_DRIB_PER_TOUCH": "AVG DRIB PER\nTOUCH",
        "PTS_PER_TOUCH": "PTS PER\nTOUCH", "ELBOW_TOUCHES": "ELBOW\nTOUCHES",
        "POST_TOUCHES": "POST\nUPS", "PAINT_TOUCHES": "PAINT\nTOUCHES",
        "PTS_PER_ELBOW_TOUCH": "PTS PER\nELBOW TOUCH",
        "PTS_PER_POST_TOUCH": "PTS PER\nPOST TOUCH",
        "PTS_PER_PAINT_TOUCH": "PTS PER\nPAINT TOUCH",
    },
    "tracking-post-ups": {
        **COMMON, "TOUCHES": "TOUCHES", "POST_TOUCHES": "POST\nUPS",
        "POST_TOUCH_FGM": "FGM", "POST_TOUCH_FGA": "FGA",
        "POST_TOUCH_FG_PCT": "FG%", "POST_TOUCH_FTM": "FTM",
        "POST_TOUCH_FTA": "FTA", "POST_TOUCH_FT_PCT": "FT%",
        "POST_TOUCH_PTS": "PTS", "POST_TOUCH_PTS_PCT": "PTS%",
        "POST_TOUCH_PASSES": "PASS", "POST_TOUCH_PASSES_PCT": "PASS%",
        "POST_TOUCH_AST": "AST", "POST_TOUCH_AST_PCT": "AST%",
        "POST_TOUCH_TOV": "TO", "POST_TOUCH_TOV_PCT": "TOV%",
        "POST_TOUCH_FOULS": "PF", "POST_TOUCH_FOULS_PCT": "PF%",
    },
}

# Identify the row; never a stat column, so an unmapped one is not a problem.
IDENTITY = {"PLAYER_ID", "PLAYER_NAME", "PLAYER_LAST_TEAM_ID",
            "PLAYER_LAST_TEAM_ABBREVIATION", "TEAM_ID", "TEAM_ABBREVIATION",
            "PLAYER_POSITION", "AGE"}

# PtMeasureType=Rebounding answers one table holding REB_*, OREB_* and DREB_*, which
# the site presents as three separate pages and this collection stores as three
# data_types. So a column absent from one data_type's map is only genuinely unknown
# if no data_type sharing that measure claims it.
KNOWN_PER_MEASURE = {}
for _dt, _m in MEASURE.items():
    KNOWN_PER_MEASURE.setdefault(_m, set()).update(FIELD_MAP[_dt])


def get_json(measure_type, season, api_type, date_from, date_to):
    params = {
        "College": "", "Conference": "", "Country": "", "DateFrom": date_from,
        "DateTo": date_to, "Division": "", "DraftPick": "", "DraftYear": "",
        "GameScope": "", "Height": "", "LastNGames": 0, "LeagueID": "00",
        "Location": "", "Month": 0, "OpponentTeamID": 0, "Outcome": "", "PORound": 0,
        "PerMode": "Totals", "PlayerExperience": "", "PlayerOrTeam": "Player",
        "PlayerPosition": "", "PtMeasureType": measure_type, "Season": season,
        "SeasonSegment": "", "SeasonType": api_type, "TeamID": 0, "VsConference": "",
        "VsDivision": "", "Weight": "",
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
        f"stats.nba.com unreachable after {MAX_ATTEMPTS} attempts ({last}).\n"
        f"If this is every request rather than an occasional one, you are on an IP "
        f"the NBA blocks -- run this from a home connection.")


def load_roster(cell):
    path = ROSTER_DIR / (f"roster_{cell['timestamp']}_"
                         f"{cell['season_type'].replace(' ', '_')}.json")
    if not path.exists():
        raise SystemExit(f"missing {path} -- run scrape_pbp_totals.py first "
                         f"(or pull the rosters/ directory)")
    return json.loads(path.read_text())["players"]


def translate(headers, row, data_type):
    """-> (doc fields, API columns nothing knows about)."""
    fmap = FIELD_MAP[data_type]
    sibling = KNOWN_PER_MEASURE[MEASURE[data_type]]
    out, unmapped = {}, []
    for h, v in zip(headers, row):
        if h in IDENTITY:
            continue
        if h in fmap:
            label = fmap[h]
            if label is not None:
                out[label] = as_percent(v) if "%" in label else v
        elif h not in sibling:
            unmapped.append(h)
    return out, unmapped


def as_percent(v):
    """The API returns 0.358; the collection stores 35.8.

    Every stored percentage came off a rendered HTML table, so it is on a 0-100
    scale. Checked across all 14 tables: the two differ by exactly 100x on every
    column whose label contains '%' -- catch-shoot FG% 35.3 against 0.353,
    defensive-impact DFG% 66.5 against 0.674, and so on.
    """
    return v * 100.0 if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def scrape_cell(coll, cell, roster, report_only, problems):
    print(f"  {cell['season']} {cell['season_type']:<15}")
    for data_type, measure in MEASURE.items():
        payload = get_json(measure, cell["season"], cell["api_type"],
                           _mmddyyyy(cell["from"]), _mmddyyyy(cell["to"]))
        rs = payload["resultSets"][0]
        headers, rows = rs["headers"], rs["rowSet"]
        pid_i = headers.index("PLAYER_ID")
        name_i = headers.index("PLAYER_NAME")

        docs, unmapped_all, unknown_players = [], set(), []
        # None means "deliberately dropped", so it is not a column to expect back.
        expected = {v for v in FIELD_MAP[data_type].values() if v is not None}
        for row in rows:
            fields, unmapped = translate(headers, row, data_type)
            unmapped_all.update(unmapped)
            pid = str(row[pid_i])
            api_name = row[name_i]
            player = roster.get(pid)
            if player is None:
                unknown_players.append((pid, api_name))
                # Not in pbpstats' roster for this cell -- keep the API's own name
                # rather than guessing at a match.
                std = api_name
            else:
                std = player["name"]
            docs.append(dict(fields, **{
                "PLAYER": api_name,
                "TEAM": _team_of(headers, row),
                "name": api_name,
                "standard_name": std,
                "nba_player_id": pid,
                "source": SOURCE,
                "data_type": data_type,
                "timestamp": cell["timestamp"],
                "season_type": cell["season_type"],
                # Marks percentages as 0-100 and the passing columns as unshifted.
                # migrate_tracking_v2.py keys off its absence; a string so
                # coverage.as_float ignores it rather than making it a feature.
                "tracking_schema": "v2",
            }))

        missing = expected - set().union(*(set(d) for d in docs)) if docs else expected
        status = "ok"
        if unmapped_all or missing:
            status = "MAPPING"
            problems.append({"data_type": data_type, "season": cell["season"],
                             "season_type": cell["season_type"],
                             "api_headers": headers,
                             "unmapped_api_columns": sorted(unmapped_all),
                             "unfilled_legacy_columns": sorted(missing)})
        print(f"    {data_type:<22} {len(rows):>4} players  {status}"
              + (f"  unmapped={sorted(unmapped_all)}" if unmapped_all else "")
              + (f"  unfilled={sorted(missing)}" if missing else "")
              + (f"  not-in-roster={len(unknown_players)}" if unknown_players else ""))

        if not report_only and status == "ok":
            mongo_sink.write_rows(coll, docs, SOURCE)
        time.sleep(DELAY)


def _team_of(headers, row):
    for key in ("TEAM_ABBREVIATION", "PLAYER_LAST_TEAM_ABBREVIATION"):
        if key in headers:
            return row[headers.index(key)]
    return None


def _mmddyyyy(iso):
    y, m, d = iso.split("-")
    return f"{m}/{d}/{y}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", nargs="*", default=list(season_dates.SNAPSHOTS))
    ap.add_argument("--report", action="store_true",
                    help="fetch and check the column mapping, write nothing")
    ap.add_argument("--out", default="tracking_mapping_report.json")
    ap.add_argument("--raw-dir", help="write rows to JSONL files here instead of Mongo, for when this network cannot reach Atlas (see load_raw.py)")
    args = ap.parse_args()

    coll = (mongo_sink.RawSink(args.raw_dir) if args.raw_dir
            else None if args.report else mongo_sink.check_connection())
    problems = []
    print(f"[nba-tracking] {len(args.seasons)} season(s): {', '.join(args.seasons)}")
    for cell in season_dates.cells(tuple(args.seasons)):
        scrape_cell(coll, cell, load_roster(cell), args.report, problems)

    if problems:
        Path(args.out).write_text(json.dumps(problems, indent=2))
        print(f"\n{len(problems)} table(s) had column-mapping problems and were NOT "
              f"written.\nSend {args.out} back so FIELD_MAP can be corrected.")
    else:
        print("\n[nba-tracking] all 14 tables mapped cleanly.")


if __name__ == "__main__":
    main()
