"""Load rows a scraper wrote to disk into Mongo.

The other half of --raw-dir. Some networks reach pbpstats but not MongoDB Atlas and
vice versa -- a VPN gets past pbpstats' block, and then Atlas drops the TLS
handshake because the VPN's exit address is not on its access list. When no single
network reaches both, scrape with --raw-dir on the network that reaches the API, and
run this on the network that reaches Atlas.

Upserts on the same key fields as a direct write, so this is safe to re-run, safe to
interrupt, and safe to point at a directory that is partly loaded already.

Run:  python scraping/load_raw.py raw/ --dry-run
      python scraping/load_raw.py raw/
"""

import argparse
import json
from pathlib import Path

import mongo_sink

BATCH = 500


def read_rows(path):
    """Yield rows, skipping a torn final line from a killed scrape."""
    bad = 0
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                bad += 1
    if bad:
        print(f"    skipped {bad} unparseable line(s)")


def load_file(coll, path, source, dry_run):
    ins = mod = same = n = 0
    batch = []
    for row in read_rows(path):
        n += 1
        batch.append(row)
        if len(batch) >= BATCH:
            a, b, matched = mongo_sink.write_rows(coll, batch, source, dry_run)
            ins, mod, same = ins + a, mod + b, same + (matched - b)
            batch = []
    if batch:
        a, b, matched = mongo_sink.write_rows(coll, batch, source, dry_run)
        ins, mod, same = ins + a, mod + b, same + (matched - b)
    return n, ins, mod, same


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("raw_dir", help="the directory passed to --raw-dir")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.raw_dir)
    if not root.is_dir():
        raise SystemExit(f"{root} is not a directory")

    coll = None if args.dry_run else mongo_sink.check_connection()
    files = sorted(p for p in root.glob("*/*.jsonl"))
    if not files:
        raise SystemExit(f"no .jsonl files under {root}")
    print(f"{len(files)} file(s) under {root}")

    totals = {}
    for path in files:
        source = path.parent.name
        if source not in mongo_sink.KEY_FIELDS:
            print(f"  SKIP {path} -- '{source}' is not a known source "
                  f"({', '.join(sorted(mongo_sink.KEY_FIELDS))})")
            continue
        n, ins, mod, same = load_file(coll, path, source, args.dry_run)
        t = totals.setdefault(source, [0, 0, 0, 0])
        for i, v in enumerate((n, ins, mod, same)):
            t[i] += v
        print(f"  {path.relative_to(root)}  {n} rows -> "
              f"{ins} inserted, {mod} updated, {same} unchanged")

    print()
    for source, (n, ins, mod, same) in sorted(totals.items()):
        print(f"{source:<14} {n:>7} rows  {ins:>7} inserted  {mod:>7} updated  "
              f"{same:>7} unchanged")


if __name__ == "__main__":
    main()
