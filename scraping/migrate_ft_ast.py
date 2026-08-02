"""Backfill FT_AST on every passing document that lacks it.

538's box offense explicitly credits free-throw assists ("we give partial credit
for what the NBA calls free throw assists"), and our scraper used to drop the
column because the legacy schema had no slot for it. No rescrape is needed to
recover it: in both the legacy and v2 passing schemas, the blank-named column
holds AST_ADJ, and the NBA defines AST_ADJ = AST + FT_AST + SECONDARY_AST, so

    FT_AST = ('' column) - AST - SECONDARY AST

Verified on the cells where the truth is known: JJ Barea 2017-18 505-434-53 = 18;
Nikola Jokic 2025-26 derived 86 vs the API's dropped value 87 (the one-count gap is
API-side revision noise). Values are clamped at zero and stamped ft_ast_derived=1
so exact API values from a future passing rescrape can overwrite them cleanly --
the scraper writes FT_AST directly from now on.

Run:  python scraping/migrate_ft_ast.py --dry-run
      python scraping/migrate_ft_ast.py
"""

import argparse

import pymongo

import mongo_sink


def as_num(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    coll = mongo_sink.check_connection()
    q = {"source": "nba-tracking", "data_type": "passing",
         "FT_AST": {"$exists": False}}
    total = coll.count_documents(q)
    print(f"{total} passing documents without FT_AST")

    ops, done, skipped, shown = [], 0, 0, 0
    # No projection: Mongo rejects the empty-string field path in projections, and
    # the blank-named column is the one this migration exists to read.
    for doc in coll.find(q):
        adj = as_num(doc.get(""))
        ast = as_num(doc.get("AST"))
        sec = as_num(doc.get("SECONDARY\nAST"))
        if adj is None or ast is None or sec is None:
            skipped += 1
            continue
        ft = max(adj - ast - sec, 0.0)
        if shown < 3:
            shown += 1
            print(f"  e.g. AST_ADJ={adj} AST={ast} SEC={sec} -> FT_AST={ft}")
        ops.append(pymongo.UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {"FT_AST": ft, "ft_ast_derived": 1}}))
        if len(ops) >= 2000:
            if not args.dry_run:
                coll.bulk_write(ops, ordered=False)
            done += len(ops)
            ops = []
            print(f"  {done}/{total}")
    if ops:
        if not args.dry_run:
            coll.bulk_write(ops, ordered=False)
        done += len(ops)
    print(f"{'would backfill' if args.dry_run else 'backfilled'} {done}; "
          f"skipped (missing inputs): {skipped}")


if __name__ == "__main__":
    main()
