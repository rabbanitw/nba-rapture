"""Season boundaries derived from pbpstats' schedule, not hand-maintained tables.

utils.get_season / get_date_range_extended encode start and end dates by hand, which
is how 2023-24 onward ended up missing (and how the 2024-25 finals date ended up as
an "Estimated" guess). pbpstats already knows every game's date, so ask it:

    get-games/nba?Season=2023-24&SeasonType=Regular Season  -> 1230 games with Date

min(Date) and max(Date) of that set are the real boundaries. Results are cached to
season_dates.json so a scrape run doesn't re-fetch them.

Run:  python scraping/season_dates.py            # print the table
      python scraping/season_dates.py --refresh  # ignore the cache
"""

import argparse
import json
from pathlib import Path

from pbpstats_client import get_json

CACHE = Path(__file__).resolve().parent / "season_dates.json"

# pbpstats SeasonType -> the season_type value used in Mongo documents.
SEASON_TYPES = {"Regular Season": "Regular season", "Playoffs": "Playoffs"}

# Snapshot timestamps for the seasons we are adding. The repo's convention for a
# whole-season row is a synthetic YYYY0715000000 offseason stamp (see
# training/seasons.py FULL_SEASON_SNAPSHOTS); one stamp carries both splits.
SNAPSHOTS = {
    # The historical whole-season cells, on their existing stamps. Listed here so
    # roster generation and the opponent-wowy scrape can reach them. CAUTION: a bare
    # `scrape_pbp_totals.py` run now covers these too and would upsert fresh pbp docs
    # alongside the original pipeline's stripped-name docs -- use --seasons or
    # --dry-run (rosters only) for the historical entries.
    "2013-14": "20140715000000",
    "2014-15": "20150715000000",
    "2015-16": "20160715000000",
    "2016-17": "20170715000000",
    "2017-18": "20180715000000",
    "2019-20": "20201101000000",
    # 2018-19 is the gap in the middle of the collection: no timestamp anywhere in
    # its window carries any source. Its snapshot stamp follows the YYYY0715
    # convention of the other whole-season cells (training/seasons.py).
    "2018-19": "20190715000000",
    # 2020-21 already has a whole-season cell, on a stamp inherited from the original
    # scrape. It has tracking but no wowy at all, no playoff pbp, and its regular
    # season pbp is stuck at the undocumented 500-row cap, so it is rescraped here.
    "2020-21": "20210801000000",
    # 2021-22 and 2022-23 have plenty of in-season snapshots but no whole-season cell,
    # and every test row is a whole-season cell. 2022-23 additionally contributes no
    # training rows at all right now: the snapshots that carry its features and the
    # snapshots whose 538 capture showed 2022-23 are disjoint sets, so no in-season
    # cell survives the label-season filter. A whole-season cell sidesteps that --
    # the finished-season label table exists and labels.py already knows how to find it.
    "2021-22": "20220715000000",
    "2022-23": "20230715000000",
    "2023-24": "20240715000000",
    "2024-25": "20250715000000",
    "2025-26": "20260715000000",
}

# Fallback boundaries for seasons whose schedule could not be fetched. Only used
# when the API is unreachable; a successful fetch always wins and is cached.
KNOWN_BOUNDS = {
    "2018-19": {"Regular Season": {"from": "2018-10-16", "to": "2019-04-10"},
                "Playoffs": {"from": "2019-04-13", "to": "2019-06-13"}},
}


def fetch_bounds(season, season_type):
    """-> (first_game_date, last_game_date, n_games) as YYYY-MM-DD strings."""
    data = get_json("get-games/nba", {"Season": season, "SeasonType": season_type})
    games = data.get("results") or []
    if not games:
        raise RuntimeError(f"no games returned for {season} {season_type}")
    dates = sorted(g["Date"] for g in games)
    return dates[0], dates[-1], len(games)


def load(seasons=tuple(SNAPSHOTS), refresh=False):
    """-> {season: {season_type: {"from", "to", "games"}}}, cached on disk."""
    cache = {}
    if CACHE.exists() and not refresh:
        cache = json.loads(CACHE.read_text())

    changed = False
    for season in seasons:
        for api_type in SEASON_TYPES:
            if cache.get(season, {}).get(api_type):
                continue
            try:
                start, end, n = fetch_bounds(season, api_type)
                src = f"{n} games"
            except RuntimeError:
                known = KNOWN_BOUNDS.get(season, {}).get(api_type)
                if not known:
                    raise
                start, end, n = known["from"], known["to"], None
                src = "UNVERIFIED fallback -- schedule not fetched"
            cache.setdefault(season, {})[api_type] = {"from": start, "to": end,
                                                      "games": n}
            changed = True
            print(f"  {season} {api_type:<15} {start} .. {end}  ({src})")

    if changed:
        CACHE.write_text(json.dumps(cache, indent=2, sort_keys=True))
    return cache


def cells(seasons=tuple(SNAPSHOTS), refresh=False):
    """Flatten to the units of work a scrape iterates over.

    -> [{season, api_type, season_type, timestamp, from, to}]
    """
    dates = load(seasons, refresh)
    out = []
    for season in seasons:
        for api_type, mongo_type in SEASON_TYPES.items():
            b = dates[season][api_type]
            out.append({"season": season, "api_type": api_type, "season_type": mongo_type,
                        "timestamp": SNAPSHOTS[season], "from": b["from"], "to": b["to"]})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--seasons", nargs="*", default=list(SNAPSHOTS))
    args = ap.parse_args()

    for c in cells(tuple(args.seasons), args.refresh):
        print(f"{c['season']}  {c['season_type']:<15} {c['timestamp']}  "
              f"{c['from']} .. {c['to']}")


if __name__ == "__main__":
    main()
