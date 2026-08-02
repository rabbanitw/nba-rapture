"""pbpstats on/off (with-or-without-you) splits -> Mongo (source="wowy").

Replaces wowy_scrape.py. Three things changed:

1. No hardcoded player id table. wowy_scrape.py carries an 80KB nba_player_ids dict
   and a FuzzyDict over it to turn a 538 name into a pbpstats id. That table stops
   at the players 538 knew about, so every post-2023 rookie either misses or -- worse,
   because FuzzyDict.get never returns "no match", only "closest match" -- silently
   resolves to some retired player. scrape_pbp_totals.py already wrote the exact
   EntityId for every player in the cell, so this reads it from the roster file.

2. Concurrency 2, not 50. wowy_scrape.py used asyncio.Semaphore(50); measured, that
   turns into ~90% 503s. See pbpstats_client for the numbers.

3. Resumable by inspecting Mongo, not a processed_files log. wowy_scrape.py marked a
   file done only once every player in it had succeeded, so an interrupted run redid
   the whole file.

A traded player has a separate on/off split per team -- the question "how did the
team do without him" only means something relative to one team. The collection
stores one document per (player, on|off), matching the existing wowy rows, so this
uses the player's largest stint and records team and n_stints on the document.

Run:  python scraping/scrape_wowy.py
      python scraping/scrape_wowy.py --seasons 2025-26 --limit 20
"""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import mongo_sink
import nba_teams
import season_dates
from pbpstats_client import get_json, stats
from scrape_pbp_totals import ROSTER_DIR

SOURCE = "wowy"
CONCURRENCY = 3


def load_roster(cell):
    path = ROSTER_DIR / (f"roster_{cell['timestamp']}_"
                         f"{cell['season_type'].replace(' ', '_')}.json")
    if not path.exists():
        raise SystemExit(f"missing {path} -- run scrape_pbp_totals.py first")
    return json.loads(path.read_text())


def fetch_split(cell, entity_id, team_id, on, wowy_type="Team"):
    # Type=Team -> the team's own stats with the player on/off the floor.
    # Type=Opponent -> the OPPONENT's stats over those same possessions: opponent rim
    # accuracy, opponent 3P%, full shot-location splits -- the on-court defensive
    # profile that Team-side wowy cannot see.
    params = {"Season": cell["season"], "SeasonType": cell["api_type"],
              "Type": wowy_type,
              "FromDate": cell["from"], "ToDate": cell["to"], "TeamId": team_id}
    # 0Exactly1OnFloor -> team possessions with him on; 0Exactly0OnFloor -> without.
    params["0Exactly1OnFloor" if on else "0Exactly0OnFloor"] = entity_id
    return get_json("get-wowy-stats/nba", params).get("single_row_table_data") or {}


def build_doc(row, cell, player, entity_id, team_id, n_stints, on, source=SOURCE):
    return dict(row, **{
        "name": player["name"],
        "standard_name": player["name"],
        "team": nba_teams.abbrev(team_id),
        "n_stints": n_stints,
        "source": source,
        "timestamp": cell["timestamp"],
        "season_type": cell["season_type"],
        "on_or_off": "on" if on else "off",
    })


def scrape_cell(coll, cell, limit=None, dry_run=False, force=False,
                source=SOURCE, wowy_type="Team"):
    roster = load_roster(cell)
    players = list(roster["players"].items())
    players.sort(key=lambda kv: -(kv[1].get("minutes") or 0))
    if limit:
        players = players[:limit]

    # (standard_name, on_or_off) pairs already in the collection for this cell.
    done = set()
    if coll is not None and not force:
        done = mongo_sink.existing_keys(coll, source, cell["timestamp"],
                                        cell["season_type"])

    jobs = []
    for eid, p in players:
        if not p["stints"]:
            continue
        team_id = p["stints"][0]["team_id"]
        for on in (True, False):
            if (p["name"], "on" if on else "off") in done:
                continue
            jobs.append((eid, p, team_id, on))

    skipped = len(players) * 2 - len(jobs)
    print(f"  {cell['season']} {cell['season_type']:<15} {len(players)} players, "
          f"{len(jobs)} splits to fetch ({skipped} already stored or unplayed)")
    if not jobs:
        return

    t0 = time.time()
    empty = 0
    failed = []
    docs = []

    def work(job):
        """Never raise: one unlucky split must not abandon the other thousand.

        The client already retries transient failures 40 times, so reaching here
        means the split genuinely could not be fetched. It gets reported at the end
        and picked up by the next run, since nothing was written for it.
        """
        eid, p, team_id, on = job
        try:
            return job, fetch_split(cell, eid, team_id, on, wowy_type), None
        except Exception as e:
            return job, None, f"{type(e).__name__}: {e}"

    # as_completed, not ex.map: map yields in submission order, so one split stuck in
    # a retry loop holds back every result behind it -- nothing gets written and the
    # run looks hung while the other threads are in fact working. Players are
    # submitted in descending minutes, and it is exactly those high-minute splits
    # that return half a megabyte and take upwards of 15s, so that stall is the
    # normal case rather than an unlucky one.
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = [ex.submit(work, j) for j in jobs]
        for i, fut in enumerate(as_completed(futures), 1):
            job, row, err = fut.result()
            eid, p, team_id, on = job
            if err:
                failed.append((p["name"], "on" if on else "off", err))
            elif not row:
                empty += 1
            else:
                docs.append(build_doc(row, cell, p, eid, team_id,
                                      len(p["stints"]), on, source=source))
            # Flush often. Everything already written is work a resumed run skips.
            if len(docs) >= 25:
                mongo_sink.write_rows(coll, docs, source, dry_run)
                docs = []
            if i % 50 == 0:
                rate = i / max(time.time() - t0, 1e-9)
                print(f"    {i}/{len(jobs)} splits  {rate:.2f}/s  "
                      f"eta {(len(jobs)-i)/max(rate,1e-9)/60:.0f}m  "
                      f"empty={empty} failed={len(failed)}", flush=True)

    if docs:
        mongo_sink.write_rows(coll, docs, source, dry_run)
    print(f"    done in {(time.time()-t0)/60:.1f}m, {empty} empty, {len(failed)} failed")
    for name, side, err in failed[:10]:
        print(f"      FAILED {name} {side}: {err}")
    if len(failed) > 10:
        print(f"      ... and {len(failed)-10} more; re-run to retry them")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", nargs="*", default=list(season_dates.SNAPSHOTS))
    ap.add_argument("--limit", type=int, help="only the top N players by minutes")
    ap.add_argument("--force", action="store_true", help="re-fetch rows already stored")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--raw-dir", help="write rows to JSONL files here instead of Mongo, for when this network cannot reach Atlas (see load_raw.py)")
    ap.add_argument("--opponent", action="store_true",
                    help="scrape opponent stats (Type=Opponent) -> source wowy-opp")
    ap.add_argument("--rs-only", action="store_true",
                    help="regular-season cells only")
    args = ap.parse_args()

    coll = (mongo_sink.RawSink(args.raw_dir) if args.raw_dir
            else None if args.dry_run else mongo_sink.check_connection())
    source = "wowy-opp" if args.opponent else SOURCE
    wowy_type = "Opponent" if args.opponent else "Team"
    print(f"[{source}] {len(args.seasons)} season(s): {', '.join(args.seasons)}"
          + ("  [regular season only]" if args.rs_only else ""))
    for cell in season_dates.cells(tuple(args.seasons)):
        if args.rs_only and cell["season_type"] != "Regular season":
            continue
        scrape_cell(coll, cell, args.limit, args.dry_run, args.force,
                    source=source, wowy_type=wowy_type)
    print(f"[{source}] done. http: {stats()}")


if __name__ == "__main__":
    main()
