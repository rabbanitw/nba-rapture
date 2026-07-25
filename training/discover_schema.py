"""Survey the nba_rapture Mongo collection.

A standalone reconnaissance tool -- it answers what exists in the collection, not
what we train on. Season logic comes from seasons.py so there is one convention.

Run:  python training/discover_schema.py
"""

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from db import REPO_ROOT, get_collection
from seasons import FULL_SEASON_SNAPSHOTS, phase_of, season_of

TRACK_TYPES_HINT = "nba-tracking"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "schema_report.json"))
    args = ap.parse_args()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    coll = get_collection()
    report = {}

    print("=" * 72)
    print("1. INVENTORY  (grouped over the (timestamp, source) index)")
    print("=" * 72)
    per_src, ts_by_src = Counter(), defaultdict(set)
    pipe = [{"$group": {"_id": {"ts": "$timestamp", "src": "$source"},
                        "n": {"$sum": 1}}}]
    for r in coll.aggregate(pipe, hint="timestamp_1_source_1", allowDiskUse=True):
        per_src[r["_id"]["src"]] += r["n"]
        ts_by_src[r["_id"]["src"]].add(r["_id"]["ts"])
    for s, n in per_src.most_common():
        print(f"  {str(s):<15} {n:>10,} docs   {len(ts_by_src[s]):>4} timestamps")
    report["docs_per_source"] = dict(per_src)
    report["timestamps_per_source"] = {k: len(v) for k, v in ts_by_src.items()}

    print("\n" + "=" * 72)
    print("2. TIMESTAMPS BY SEASON / PHASE (source=538)")
    print("=" * 72)
    grid, synthetic = Counter(), Counter()
    for ts in sorted(ts_by_src.get("538", [])):
        key = (season_of(ts), phase_of(ts))
        grid[key] += 1
        if ts in FULL_SEASON_SNAPSHOTS:
            synthetic[key] += 1
    for key, n in sorted(grid.items(), key=lambda kv: str(kv[0])):
        tag = f"  <- {synthetic[key]} full-season snapshot(s)" if synthetic[key] else ""
        print(f"  {str(key[0]):<9} {key[1]:<10} {n:>4}{tag}")
    report["season_phase_grid"] = {f"{k[0]}|{k[1]}": v for k, v in grid.items()}

    print("\n  full-season snapshots (one row per player per whole season):")
    for ts, season in sorted(FULL_SEASON_SNAPSHOTS.items()):
        present = [s for s in ts_by_src if ts in ts_by_src[s]]
        print(f"    {ts}  {season}  sources={sorted(present)}")

    print("\n" + "=" * 72)
    print("3. SOURCE SHAPES")
    print("=" * 72)
    report["tracking_data_types"] = sorted(
        coll.distinct("data_type", {"source": TRACK_TYPES_HINT}))
    print("  nba-tracking data_type:", report["tracking_data_types"])
    print("  wowy on_or_off:", coll.distinct("on_or_off", {"source": "wowy"}))
    ex = coll.find_one({"source": "538"})
    ex.pop("_id", None)
    report["538_example"] = {k: str(v) for k, v in ex.items()}
    print("\n  example 538 label doc:")
    print("   ", json.dumps(report["538_example"], indent=4)[:900])

    json.dump(report, open(args.out, "w"), indent=2, default=str)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
