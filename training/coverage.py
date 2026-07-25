"""Measure per-field population rates so we can pick the common feature set empirically.

For every (timestamp, season_type) cell we look at the players 538 rates, pull the
matching feature docs (latest-inserted wins -- see pick_doc), and record which fields
are present and numeric.

Coverage is reported separately for the historical/test era (the YYYY0715 snapshots)
and the modern/train era, because the NBA's tracking feeds gained columns over time and
a field that only exists post-2020 is useless to us.

Run:  python training/coverage.py
"""

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from db import REPO_ROOT, get_collection

# Held-out seasons -> their single synthetic snapshot.
TEST_TIMESTAMPS = {"20140715000000": "2013-14", "20150715000000": "2014-15"}
# The other historical snapshots (also full-season, same shape as test).
HIST_TIMESTAMPS = {"20160715000000": "2015-16", "20170715000000": "2016-17",
                   "20180715000000": "2017-18"}

SEASON_TYPES = ["Regular season", "Playoffs"]

TRACK_TYPES = ['catch-shoot', 'defensive-impact', 'defensive-rebounding', 'drives',
               'elbow-touch', 'offensive-rebounding', 'paint-touch', 'passing',
               'pullup', 'rebounding', 'shooting-efficiency', 'speed-distance',
               'touches', 'tracking-post-ups']

# Identify the document; never a feature.
ID_FIELDS = {"_id", "name", "ShortName", "standard_name", "timestamp", "season_type",
             "source", "data_type", "on_or_off", "team", "TEAM", "Team", "id",
             "data_key", "row_num", "pos"}

NUM_RE = re.compile(r"^-?[\d,]*\.?\d+$")


def as_float(v):
    """Parse a scraped value to float, or None if it isn't a number."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", "").replace("%", "").lstrip("+")
        if s and NUM_RE.match(s.replace(",", "")):
            try:
                return float(s)
            except ValueError:
                return None
    return None


def pick_doc(docs):
    """Duplicate rows exist; the latest-inserted one is the correct-season row.

    Verified against 538 minutes: max(_id) -> 0.0% median error on duplicated
    players, vs 18.4% for the earliest insert.
    """
    return max(docs, key=lambda d: d["_id"])


def block_specs():
    specs = {"pbp": {"source": "pbp"},
             "wowy_on": {"source": "wowy", "on_or_off": "on"},
             "wowy_off": {"source": "wowy", "on_or_off": "off"}}
    for dt in TRACK_TYPES:
        specs[f"track:{dt}"] = {"source": "nba-tracking", "data_type": dt}
    return specs


def cell_coverage(coll, ts, season_type, specs):
    """-> (n_538_players, {block: {field: n_players_with_numeric_value}}, {block: n_players_present})"""
    players = {d["standard_name"] for d in
               coll.find({"timestamp": ts, "source": "538", "season_type": season_type},
                         {"standard_name": 1, "_id": 0})}
    if not players:
        return 0, {}, {}

    present = {}
    counts = {}
    for block, q in specs.items():
        grp = defaultdict(list)
        for d in coll.find({"timestamp": ts, "season_type": season_type, **q}):
            if d["standard_name"] in players:
                grp[d["standard_name"]].append(d)
        c = defaultdict(int)
        for name, docs in grp.items():
            doc = pick_doc(docs)
            for k, v in doc.items():
                if k in ID_FIELDS:
                    continue
                if as_float(v) is not None:
                    c[k] += 1
        counts[block] = dict(c)
        present[block] = len(grp)
    return len(players), counts, present


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-samples", type=int, default=6)
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "coverage_report.json"))
    args = ap.parse_args()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    coll = get_collection()
    specs = block_specs()

    # Sample modern timestamps spread across seasons for the train-era view.
    modern = sorted(t for t in coll.distinct("timestamp", {"source": "538"})
                    if t > "20201101000000")
    step = max(1, len(modern) // args.train_samples)
    train_ts = modern[::step][:args.train_samples]
    print(f"train-era sample timestamps: {train_ts}")

    eras = {
        "test": list(TEST_TIMESTAMPS),
        "hist": list(HIST_TIMESTAMPS),
        "train": train_ts,
    }

    report = {}
    for era, tss in eras.items():
        agg_num = defaultdict(lambda: defaultdict(int))   # block -> field -> hits
        agg_den = defaultdict(int)                         # block -> player-rows seen
        cells = []
        for ts in tss:
            for st in SEASON_TYPES:
                n538, counts, present = cell_coverage(coll, ts, st, specs)
                if not n538:
                    continue
                cells.append({"ts": ts, "season_type": st, "n_538": n538,
                              "n_block_players": present})
                for block, c in counts.items():
                    agg_den[block] += present[block]
                    for k, v in c.items():
                        agg_num[block][k] += v
                print(f"  [{era}] {ts} {st:<15} 538={n538:<4} "
                      + " ".join(f"{b.split(':')[-1][:6]}={present[b]}"
                                 for b in ("pbp", "wowy_on") if b in present))
        report[era] = {
            "cells": cells,
            "coverage": {b: {k: v / max(agg_den[b], 1) for k, v in flds.items()}
                         for b, flds in agg_num.items()},
            "denominator": dict(agg_den),
        }

    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nwrote {args.out}")

    # Summary: fields usable in BOTH eras.
    print("\n" + "=" * 78)
    print(f"{'block':<26} {'test>=95%':>10} {'train>=95%':>11} {'BOTH':>7} {'union':>7}")
    print("=" * 78)
    total_both = 0
    for block in specs:
        te = report["test"]["coverage"].get(block, {})
        tr = report["train"]["coverage"].get(block, {})
        te_ok = {k for k, v in te.items() if v >= 0.95}
        tr_ok = {k for k, v in tr.items() if v >= 0.95}
        both = te_ok & tr_ok
        total_both += len(both)
        print(f"{block:<26} {len(te_ok):>10} {len(tr_ok):>11} {len(both):>7} "
              f"{len(set(te) | set(tr)):>7}")
    print("=" * 78)
    print(f"{'TOTAL usable feature fields':<26} {total_both:>29}")


if __name__ == "__main__":
    main()
