"""Record which season each 538 document is actually about.

A 538 document only ever carried the wayback timestamp it was scraped from, and the
pipeline assumed timestamp implied season. It does not, for two reasons:

  1. The archived RAPTOR page showed the completed season's table AND the season in
     progress. Both were scraped under one timestamp. 174 snapshots hold two seasons
     at once -- 20211216233434 has 600 rows for 2020-21 and 124 for 2021-22.

  2. Some captures show only a stale season. 174 snapshots carry 2020-21 labels
     against features from 2021-22 through 2024-25.

The season is recoverable: 538's data_key ends in the season's ending year, so
"Nikola JokicNuggets2021" is 2020-21. Verified against rosters at 20201101000000 --
Kawhi on the Raptors, Davis on the Pelicans, Butler on the 76ers, no Zion or Ja --
which is 2018-19, not the 2019-20 that seasons.py assumed.

This writes that year out as label_season so nothing has to infer it again. Purely
additive: no document is moved, deleted, or re-stamped.

Run:  python scraping/migrate_label_season.py --dry-run
      python scraping/migrate_label_season.py
"""

import argparse
import re
from collections import Counter

import pymongo

import mongo_sink

YEAR_RE = re.compile(r"(\d{4})$")


def season_of_key(data_key):
    """538's data_key ends in the season's ending year: 2021 -> '2020-21'."""
    m = YEAR_RE.search(str(data_key or ""))
    if not m:
        return None
    y = int(m.group(1))
    if not 2014 <= y <= 2030:
        return None
    return f"{y - 1}-{str(y)[2:]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    coll = mongo_sink.check_connection()
    q = {"source": "538", "label_season": {"$exists": False}}
    total = coll.count_documents(q)
    print(f"{total} 538 documents without label_season")
    if not total:
        print("nothing to do")
        return

    ops, done, seen, unparsed = [], 0, Counter(), 0
    for doc in coll.find(q, {"data_key": 1}):
        s = season_of_key(doc.get("data_key"))
        if s is None:
            unparsed += 1
            continue
        seen[s] += 1
        ops.append(pymongo.UpdateOne({"_id": doc["_id"]},
                                     {"$set": {"label_season": s}}))
        if len(ops) >= 2000:
            if not args.dry_run:
                coll.bulk_write(ops, ordered=False)
            done += len(ops)
            ops = []
            if done % 50000 == 0:
                print(f"  {done}/{total}")
    if ops:
        if not args.dry_run:
            coll.bulk_write(ops, ordered=False)
        done += len(ops)

    print(f"{'would stamp' if args.dry_run else 'stamped'} {done} documents; "
          f"{unparsed} had no usable data_key")
    for s, n in sorted(seen.items()):
        print(f"  {s}: {n}")


if __name__ == "__main__":
    main()
