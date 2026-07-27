"""pbpstats per-100-possession player totals -> Mongo (source="pbp").

Replaces pbp_scrape.py, which drove itself off the filenames of previously scraped
538 snapshots and wrote CSV for data_saver.py to load later.

The bug worth knowing about: a league-wide get-totals call is capped at 500 rows.
It does not say so -- it just silently returns the top 500 by minutes. For 2023-24
that is every player down to 67 minutes, dropping 72 real NBA players off the tail.
Adding &Limit=1000 changes nothing.

Filtering by TeamId is not capped, so this scrapes both and merges:

  league-wide (<=500 rows)  correctly aggregates players who were traded mid-season
                            -- OG Anunoby is one row of 50 GP, not TOR 23 + NYK 27
  per-team    (30 calls)    reaches everyone, down to 1 minute played

The league-wide row wins wherever it exists; per-team rows fill in the tail. A tail
player who was also traded can only be represented by his larger team stint, which
is recorded as pbp_row_scope="team" so those rows can be identified later.

Also writes rosters_<timestamp>_<season_type>.json: EntityId -> name and team
stints. scrape_wowy.py needs the ids to query on/off splits, and
scrape_nba_tracking.py needs them to join NBA player ids to standard_name without
fuzzy matching names.

Run:  python scraping/scrape_pbp_totals.py
      python scraping/scrape_pbp_totals.py --seasons 2025-26 --dry-run
"""

import argparse
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import mongo_sink
import nba_teams
import season_dates
from pbpstats_client import get_json, stats

SOURCE = "pbp"
ROSTER_DIR = Path(__file__).resolve().parent / "rosters"

# pbpstats caps a league-wide response here without telling you.
LEAGUE_ROW_CAP = 500

# Team calls are independent, but pbpstats sheds load hard above ~2 in flight.
TEAM_CONCURRENCY = 2


def base_params(cell):
    return {"Season": cell["season"], "SeasonType": cell["api_type"], "Type": "Player",
            "StartType": "All", "StatType": "Per100Possessions",
            "FromDate": cell["from"], "ToDate": cell["to"]}


def fetch_league(cell):
    rows = get_json("get-totals/nba", base_params(cell))["multi_row_table_data"]
    if len(rows) >= LEAGUE_ROW_CAP:
        print(f"    league-wide hit the {LEAGUE_ROW_CAP}-row cap "
              f"(min {min(r['Minutes'] for r in rows)} min) -- filling tail from teams")
    return rows


def fetch_team(cell, team_id):
    params = dict(base_params(cell), TeamId=team_id)
    return get_json("get-totals/nba", params)["multi_row_table_data"]


def fetch_all_teams(cell):
    with ThreadPoolExecutor(max_workers=TEAM_CONCURRENCY) as ex:
        per_team = list(ex.map(lambda t: fetch_team(cell, t), nba_teams.TEAM_IDS))
    out = []
    for team_id, rows in zip(nba_teams.TEAM_IDS, per_team):
        for r in rows:
            out.append((team_id, r))
    return out


def merge(league_rows, team_rows):
    """-> ({entity_id: doc-shaped row}, {entity_id: [(team_id, minutes)]})."""
    stints = defaultdict(list)
    best_team_row = {}
    for team_id, r in team_rows:
        eid = str(r["EntityId"])
        stints[eid].append((team_id, r.get("Minutes") or 0))
        prev = best_team_row.get(eid)
        if prev is None or (r.get("Minutes") or 0) > (prev[1].get("Minutes") or 0):
            best_team_row[eid] = (team_id, r)

    merged = {}
    for r in league_rows:
        eid = str(r["EntityId"])
        merged[eid] = dict(r, pbp_row_scope="league")
    for eid, (team_id, r) in best_team_row.items():
        if eid not in merged:
            merged[eid] = dict(r, pbp_row_scope="team")

    for eid, s in stints.items():
        s.sort(key=lambda x: -x[1])
    return merged, dict(stints)


def to_docs(merged, stints, cell):
    docs = []
    for eid, r in merged.items():
        name = r["Name"]
        team_id = stints.get(eid, [(None, 0)])[0][0]
        doc = dict(r)
        doc.update({
            "name": name,
            # pbpstats already spells names the way the rest of the collection does
            # ("Shai Gilgeous-Alexander", "Jaren Jackson Jr."), so this is a direct
            # copy -- no fuzzy matching, which is what corrupted the tracking data.
            "standard_name": name,
            "team": nba_teams.abbrev(team_id) if team_id else r.get("TeamAbbreviation"),
            "source": SOURCE,
            "timestamp": cell["timestamp"],
            "season_type": cell["season_type"],
        })
        docs.append(doc)
    return docs


def save_roster(merged, stints, cell):
    ROSTER_DIR.mkdir(parents=True, exist_ok=True)
    path = ROSTER_DIR / (f"roster_{cell['timestamp']}_"
                         f"{cell['season_type'].replace(' ', '_')}.json")
    roster = {
        "season": cell["season"], "api_type": cell["api_type"],
        "season_type": cell["season_type"], "timestamp": cell["timestamp"],
        "from": cell["from"], "to": cell["to"],
        "players": {eid: {"name": merged[eid]["Name"],
                          "minutes": merged[eid].get("Minutes"),
                          "stints": [{"team_id": t, "team": nba_teams.abbrev(t),
                                      "minutes": m} for t, m in stints.get(eid, [])]}
                    for eid in sorted(merged)},
    }
    path.write_text(json.dumps(roster, indent=2))
    return path


def scrape_cell(coll, cell, dry_run=False):
    print(f"  {cell['season']} {cell['season_type']:<15} {cell['from']} .. {cell['to']}")
    league_rows = fetch_league(cell)
    team_rows = fetch_all_teams(cell)
    merged, stints = merge(league_rows, team_rows)

    tail = sum(1 for r in merged.values() if r["pbp_row_scope"] == "team")
    traded_tail = sum(1 for eid, r in merged.items()
                      if r["pbp_row_scope"] == "team" and len(stints.get(eid, [])) > 1)
    docs = to_docs(merged, stints, cell)
    ins, mod, matched = mongo_sink.write_rows(coll, docs, SOURCE, dry_run)
    path = save_roster(merged, stints, cell)
    print(f"    {len(league_rows)} league + {len(team_rows)} team rows "
          f"-> {len(docs)} players ({tail} from the tail, {traded_tail} of them "
          f"team-partial)")
    print(f"    mongo: {ins} inserted, {mod} updated, {matched - mod} unchanged")
    print(f"    roster: {path.name}")
    return docs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", nargs="*", default=list(season_dates.SNAPSHOTS))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    coll = None if args.dry_run else mongo_sink.check_connection()
    print(f"[pbp] {len(args.seasons)} season(s): {', '.join(args.seasons)}")
    for cell in season_dates.cells(tuple(args.seasons)):
        scrape_cell(coll, cell, args.dry_run)
    print(f"[pbp] done. http: {stats()}")


if __name__ == "__main__":
    main()
