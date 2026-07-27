"""Check what actually landed in Mongo for the new seasons.

Answers the three questions worth asking after a scrape:

  * Is every source present for every cell, and how complete is each one?
  * Do the sources agree on who played? A player in pbp but missing from wowy or
    tracking is a row build_dataset.py would have to drop.
  * Do the new documents carry the same fields as the old ones? training/coverage.py
    matches features by exact name, so a renamed column silently becomes a column of
    NaNs rather than an error.

It also re-checks the name-attribution problem that affects the existing collection:
any document whose standard_name is a different person than its name.

Run:  python scraping/verify_scrape.py
      python scraping/verify_scrape.py --compare-to 20230306010123
"""

import argparse
import re
import unicodedata
from collections import Counter, defaultdict

import mongo_sink
import season_dates

TRACK_TYPES = ['catch-shoot', 'defensive-impact', 'defensive-rebounding', 'drives',
               'elbow-touch', 'offensive-rebounding', 'paint-touch', 'passing',
               'pullup', 'rebounding', 'shooting-efficiency', 'speed-distance',
               'touches', 'tracking-post-ups']

ID_FIELDS = {"_id", "name", "ShortName", "standard_name", "timestamp", "season_type",
             "source", "data_type", "on_or_off", "team", "TEAM", "Team", "id",
             "data_key", "row_num", "pos", "PLAYER", "nba_player_id",
             "pbp_row_scope", "n_stints"}


def name_key(s):
    """Ignore punctuation and suffixes; what's left should be the same person."""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s.lower())
    return re.sub(r"[^a-z0-9]", "", s)


def cell_report(coll, ts, season_type):
    pbp = {d["standard_name"] for d in
           coll.find({"source": "pbp", "timestamp": ts, "season_type": season_type},
                     {"standard_name": 1, "_id": 0})}
    wowy = defaultdict(set)
    for d in coll.find({"source": "wowy", "timestamp": ts, "season_type": season_type},
                       {"standard_name": 1, "on_or_off": 1, "_id": 0}):
        wowy[d["on_or_off"]].add(d["standard_name"])
    track = defaultdict(set)
    for d in coll.find({"source": "nba-tracking", "timestamp": ts,
                        "season_type": season_type},
                       {"standard_name": 1, "data_type": 1, "_id": 0}):
        track[d["data_type"]].add(d["standard_name"])

    print(f"\n  {ts}  {season_type}")
    print(f"    pbp                        {len(pbp):>4} players")
    for side in ("on", "off"):
        got = wowy[side]
        print(f"    wowy {side:<3}                   {len(got):>4} players"
              f"  ({len(pbp - got)} in pbp but not here)")
    if not track:
        print(f"    nba-tracking                  0  -- not scraped yet "
              f"(run scrape_nba_tracking.py on a residential connection)")
    else:
        missing_types = [t for t in TRACK_TYPES if t not in track]
        for t in TRACK_TYPES:
            if t in track:
                print(f"    track:{t:<22} {len(track[t]):>4} players"
                      f"  ({len(pbp - track[t])} in pbp but not here)")
        if missing_types:
            print(f"    MISSING tracking tables: {missing_types}")

    # Rows a combined model could actually build: needs pbp + both wowy sides.
    complete = pbp & wowy["on"] & wowy["off"]
    if track:
        for t in TRACK_TYPES:
            complete &= track.get(t, set())
        print(f"    -> {len(complete)} players with pbp + wowy + all 14 tracking tables")
    else:
        print(f"    -> {len(complete)} players with pbp + both wowy sides")
    return pbp


def field_report(coll, ts, season_type, compare_ts):
    """New cell's fields vs an older cell's, per source."""
    print(f"\n  fields vs {compare_ts}")
    for source, extra in (("pbp", {}), ("wowy", {"on_or_off": "on"}),
                          ("nba-tracking", {"data_type": "speed-distance"})):
        def fields(t):
            c = Counter()
            n = 0
            for d in coll.find({"source": source, "timestamp": t,
                                "season_type": season_type, **extra}):
                n += 1
                c.update(k for k in d if k not in ID_FIELDS)
            return n, c
        n_new, f_new = fields(ts)
        n_old, f_old = fields(compare_ts)
        if not n_new or not n_old:
            print(f"    {source:<14} skipped (new={n_new} docs, old={n_old} docs)")
            continue
        gone = sorted(k for k in f_old if k not in f_new)
        added = sorted(k for k in f_new if k not in f_old)
        print(f"    {source:<14} new={len(f_new)} fields / {n_new} docs, "
              f"old={len(f_old)} / {n_old} docs")
        if gone:
            print(f"      IN OLD, MISSING FROM NEW: {gone}")
        if added:
            print(f"      new only: {added}")


def misattribution_report(coll, ts):
    """Documents whose standard_name names a different player than name."""
    bad = Counter()
    examples = {}
    total = Counter()
    for d in coll.find({"timestamp": ts},
                       {"name": 1, "standard_name": 1, "source": 1, "_id": 0}):
        src = d.get("source")
        total[src] += 1
        if name_key(d.get("name")) != name_key(d.get("standard_name")):
            bad[src] += 1
            examples.setdefault(src, (d.get("name"), d.get("standard_name")))
    print(f"\n  name attribution at {ts}")
    for src in sorted(total):
        pct = 100 * bad[src] / max(total[src], 1)
        flag = "" if not bad[src] else f"   e.g. {examples[src][0]!r} -> {examples[src][1]!r}"
        print(f"    {src:<14} {bad[src]:>5}/{total[src]:<6} wrong ({pct:4.1f}%){flag}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compare-to", default="20230306010123",
                    help="an older timestamp to diff field names against")
    args = ap.parse_args()

    coll = mongo_sink.get_collection()
    cells = season_dates.cells()
    print(f"verifying {len(cells)} cells")
    for c in cells:
        cell_report(coll, c["timestamp"], c["season_type"])

    first = cells[0]
    field_report(coll, first["timestamp"], first["season_type"], args.compare_to)
    for ts in sorted({c["timestamp"] for c in cells}):
        misattribution_report(coll, ts)


if __name__ == "__main__":
    main()
