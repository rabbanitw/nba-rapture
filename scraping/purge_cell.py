"""Delete one source's documents for one timestamp, so a re-scrape starts clean.

Needed when a cell has accumulated duplicates. Upserts replace a single matching
document and keep its _id, and coverage.pick_doc resolves duplicates by max(_id), so
re-scraping over a duplicated key can leave the stale copy winning. Deleting first
removes both the duplicates and any phantom keys -- documents filed under names that
should not exist at all, which is what the old fuzzy name matcher produced.

The 2020-21 tracking cell is the case this was written for: 19,800 documents across
13,509 distinct keys, 1,199 of those keys duplicated up to 9 times, and 1.8% of the
documents attributed to the wrong player.

Deliberately narrow: one source and one timestamp per invocation, no wildcards, and
it prints what it is about to remove and requires --yes to actually do it.

Run:  python scraping/purge_cell.py --source nba-tracking --timestamp 20210801000000
      python scraping/purge_cell.py --source nba-tracking --timestamp 20210801000000 --yes
"""

import argparse
from collections import Counter

import mongo_sink


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True,
                    choices=sorted(mongo_sink.KEY_FIELDS) + ["538"])
    ap.add_argument("--timestamp", required=True)
    ap.add_argument("--yes", action="store_true",
                    help="actually delete; without it this is a dry run")
    args = ap.parse_args()

    coll = mongo_sink.check_connection()
    q = {"source": args.source, "timestamp": args.timestamp}
    n = coll.count_documents(q)
    if not n:
        print(f"nothing matches source={args.source} timestamp={args.timestamp}")
        return

    by_type = Counter()
    keys = Counter()
    for d in coll.find(q, {"season_type": 1, "standard_name": 1, "data_type": 1}):
        by_type[d.get("season_type")] += 1
        keys[(d.get("season_type"), d.get("standard_name"), d.get("data_type"))] += 1
    dupes = sum(1 for v in keys.values() if v > 1)

    print(f"source={args.source} timestamp={args.timestamp}")
    print(f"  documents:      {n}")
    print(f"  distinct keys:  {len(keys)}")
    print(f"  duplicated keys:{dupes}"
          + (f"  (up to {max(keys.values())} copies)" if keys else ""))
    for st, c in sorted(by_type.items(), key=lambda kv: str(kv[0])):
        print(f"    {st}: {c}")

    if not args.yes:
        print("\ndry run -- nothing deleted. Re-run with --yes to remove these.")
        return

    res = coll.delete_many(q)
    print(f"\ndeleted {res.deleted_count} documents")
    print(f"remaining for this source+timestamp: {coll.count_documents(q)}")


if __name__ == "__main__":
    main()
