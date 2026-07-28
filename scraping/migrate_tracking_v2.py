"""Repair the tracking documents written before the two column bugs were found.

Both bugs are pure transformations of what is already stored, so the ~33k tracking
documents for 2023-24 onward can be corrected in place rather than re-scraped.

  1. Percentages. stats.nba.com returns 0.358; every stored percentage came off a
     rendered HTML table and is on a 0-100 scale. Verified across all 14 tables:
     exactly 100x on every column whose label contains '%'.

  2. The passing table. Its last three legacy columns sit one place left of their
     headers, because the rendered table was missing a header cell. Confirmed
     arithmetically on the 2018 regular season, exact on every player checked:

       stored ''             holds AST_ADJ        (JJ Barea 434 AST + 18 FT + 53 SEC = 505)
       stored 'AST ADJ'      holds AST / PASSES_MADE       as a percent (12.98 -> 13.0)
       stored 'AST TO PASS%' holds AST_ADJ / PASSES_MADE   as a percent (15.10 -> 15.1)

     What v1 wrote instead was FT_AST, AST_ADJ and AST_TO_PASS_PCT. AST_TO_PASS_PCT_ADJ
     was dropped by v1, but it is exactly AST_ADJ / PASSES_MADE, so nothing is lost --
     every target value is recoverable from what is on the document.

Idempotent: documents are selected by the absence of tracking_schema, and the field
is set to "v2" as part of the same update.

Run:  python scraping/migrate_tracking_v2.py --dry-run
      python scraping/migrate_tracking_v2.py
"""

import argparse

import pymongo

import mongo_sink
import season_dates

SOURCE = "nba-tracking"
TIMESTAMPS = sorted(season_dates.SNAPSHOTS.values())


def num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def fixed_fields(doc):
    """-> {field: new value} for one v1 document, or {} if nothing changes."""
    out = {}
    for k, v in doc.items():
        if isinstance(k, str) and "%" in k and num(v) is not None:
            out[k] = v * 100.0

    if doc.get("data_type") == "passing":
        ast = num(doc.get("AST"))
        passes = num(doc.get("PASSES\nMADE"))
        ast_adj = num(doc.get("AST\nADJ"))          # v1 put AST_ADJ here
        pct = num(doc.get("AST TO\nPASS%"))         # v1 put AST_TO_PASS_PCT here
        # '' held FT_AST in v1 and is overwritten; AST_TO_PASS_PCT_ADJ is recomputed.
        out[""] = ast_adj
        out["AST\nADJ"] = pct * 100.0 if pct is not None else None
        out["AST TO\nPASS%"] = (100.0 * ast_adj / passes
                                if ast_adj is not None and passes else None)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    coll = mongo_sink.check_connection()
    q = {"source": SOURCE, "timestamp": {"$in": TIMESTAMPS},
         "tracking_schema": {"$exists": False}}
    total = coll.count_documents(q)
    print(f"{total} v1 tracking documents to migrate "
          f"(timestamps {', '.join(TIMESTAMPS)})")
    if not total:
        print("nothing to do")
        return

    ops, done, shown = [], 0, 0
    for doc in coll.find(q):
        sets = fixed_fields(doc)
        sets["tracking_schema"] = "v2"
        if shown < 3 and doc.get("data_type") == "passing":
            shown += 1
            adj_k, pct_k = "AST\nADJ", "AST TO\nPASS%"
            print(f"  e.g. {doc['standard_name']} passing: "
                  f"''={doc.get('')}->{sets.get('')}, "
                  f"'AST ADJ'={doc.get(adj_k)}->{sets.get(adj_k)}, "
                  f"'AST TO PASS%'={doc.get(pct_k)}->{sets.get(pct_k):.2f}")
        # ReplaceOne, not UpdateOne: the passing table has a field literally named
        # "", and Mongo rejects an empty string as an update path ("An empty update
        # path is not valid"). Replacing the whole document sidesteps update paths.
        new_doc = {**doc, **sets}
        ops.append(pymongo.ReplaceOne({"_id": doc["_id"]}, new_doc))
        if len(ops) >= 1000:
            if not args.dry_run:
                coll.bulk_write(ops, ordered=False)
            done += len(ops)
            ops = []
            print(f"  {done}/{total}")
    if ops:
        if not args.dry_run:
            coll.bulk_write(ops, ordered=False)
        done += len(ops)

    print(f"{'would migrate' if args.dry_run else 'migrated'} {done} documents")


if __name__ == "__main__":
    main()
