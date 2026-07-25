"""Build the box and on/off training matrices from Mongo.

Row  = (timestamp, season_type, player). Features come from the stat sources at
that same snapshot; the label is the 538 RAPTOR total at that snapshot.

  box    features = pbp + the 14 nba-tracking blocks   -> label rap_box
  onoff  features = wowy on, wowy off, and (on - off)   -> label rap_onoff

Held out as test: seasons 2013-14 and 2014-15 (the 20140715 / 20150715 snapshots),
both the Regular season and Playoffs splits.

Run:  python training/build_dataset.py --modern-stride 6
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import numpy as np

from coverage import ID_FIELDS, TRACK_TYPES, as_float, pick_doc
from db import REPO_ROOT, get_collection
from seasons import FULL_SEASON_SNAPSHOTS, is_test, season_of, season_progress

SEASON_TYPES = ["Regular season", "Playoffs"]
POS_COLS = ["PG", "SG", "SF", "PF", "C"]

BLOCKS = {
    "box": {"pbp": {"source": "pbp"},
            **{f"track:{dt}": {"source": "nba-tracking", "data_type": dt}
               for dt in TRACK_TYPES}},
    "onoff": {"wowy_on": {"source": "wowy", "on_or_off": "on"},
              "wowy_off": {"source": "wowy", "on_or_off": "off"}},
}
# "combined" uses every source at once to predict 538's blended RAPTOR
# (raptor_offense / raptor_defense / raptor_total, "using both box and on-off
# components"). Those columns are rounded to 1 decimal in the scrape.
BLOCKS["combined"] = {**BLOCKS["box"], **BLOCKS["onoff"]}

LABELS = {"box": "rap_box", "onoff": "rap_onoff", "combined": "rap"}
# 538 splits each component into offense and defense. total == o + d up to the
# scrape's 1-decimal rounding (max observed deviation 0.101), so the parts can
# either be modelled directly or summed.
LABELS_OFF = {"box": "rap_box_o", "onoff": "rap_onoff_o", "combined": "rap_o"}
LABELS_DEF = {"box": "rap_box_d", "onoff": "rap_onoff_d", "combined": "rap_d"}

# Without these a row has no real signal; the rest may be NaN-filled. Requiring
# every block instead would have thrown away 73 of the 100 players in the 2013-14
# playoffs, where only track:defensive-impact is missing.
REQUIRED_BLOCKS = {"box": ["pbp"], "onoff": ["wowy_on", "wowy_off"],
                   "combined": ["pbp", "wowy_on", "wowy_off"]}

SOURCES_NEEDED = {"box": ["538", "pbp", "nba-tracking"],
                  "onoff": ["538", "wowy"],
                  "combined": ["538", "pbp", "nba-tracking", "wowy"]}

# A field must exist in both eras to be usable. Below this it's a schema change
# (the feed gained or lost the column), not just a player with zero events.
ERA_PRESENCE_FLOOR = 0.05


def usable_fields(report, block):
    """Fields present in both the historical/test era and the modern/train era."""
    te = report["test"]["coverage"].get(block, {})
    tr = report["train"]["coverage"].get(block, {})
    keep = [f for f in sorted(set(te) | set(tr))
            if te.get(f, 0.0) >= ERA_PRESENCE_FLOOR and tr.get(f, 0.0) >= ERA_PRESENCE_FLOOR]
    return keep


def one_hot_pos(pos):
    toks = {p.strip().upper() for p in str(pos or "").split(",") if p.strip()}
    return [1.0 if c in toks else 0.0 for c in POS_COLS]


def timestamps_with_sources(coll, cache=REPO_ROOT / "training" / "_ts_by_source.json"):
    if Path(cache).exists():
        return {k: set(v) for k, v in json.load(open(cache)).items()}
    pipe = [{"$group": {"_id": {"ts": "$timestamp", "src": "$source"}}}]
    out = defaultdict(set)
    for r in coll.aggregate(pipe, hint="timestamp_1_source_1", allowDiskUse=True):
        out[r["_id"]["src"]].add(r["_id"]["ts"])
    Path(cache).parent.mkdir(parents=True, exist_ok=True)
    json.dump({k: sorted(v) for k, v in out.items()}, open(cache, "w"))
    return dict(out)


def select_timestamps(ts_by_src, model, modern_stride):
    """Full-season snapshots always; modern snapshots subsampled to cut redundancy."""
    ok = set.intersection(*[ts_by_src[s] for s in SOURCES_NEEDED[model]])
    hist = sorted(t for t in ok if t in FULL_SEASON_SNAPSHOTS)
    modern = sorted(t for t in ok if t not in FULL_SEASON_SNAPSHOTS)
    by_season = defaultdict(list)
    for t in modern:
        by_season[season_of(t)].append(t)
    thinned = []
    for s in sorted(by_season, key=str):
        thinned.extend(by_season[s][::modern_stride])
    return hist + thinned


def fetch_cell(coll, ts, season_type, blocks, fields):
    """-> {player: {block: doc}} for the players 538 rates in this cell, plus label docs."""
    labels = {}
    for d in coll.find({"timestamp": ts, "source": "538", "season_type": season_type}):
        labels.setdefault(d["standard_name"], d)
    if not labels:
        return {}, {}

    picked = defaultdict(dict)
    for block, q in blocks.items():
        grp = defaultdict(list)
        for d in coll.find({"timestamp": ts, "season_type": season_type, **q}):
            if d["standard_name"] in labels:
                grp[d["standard_name"]].append(d)
        for name, docs in grp.items():
            picked[name][block] = pick_doc(docs)
    return labels, picked


def build(model, coll, report, timestamps, min_blocks_frac=0.8):
    blocks = BLOCKS[model]
    required = REQUIRED_BLOCKS[model]
    fields = {b: usable_fields(report, b) for b in blocks}
    n_feat = sum(len(v) for v in fields.values())
    print(f"[{model}] {len(blocks)} blocks, {n_feat} raw stat fields")

    rows, ys, ys_off, ys_def, meta = [], [], [], [], []
    skipped_no_block = 0
    for i, ts in enumerate(timestamps):
        for st in SEASON_TYPES:
            labels, picked = fetch_cell(coll, ts, st, blocks, fields)
            if not labels:
                continue
            n_before = len(rows)
            for player, lab in labels.items():
                got = picked.get(player, {})
                if (any(b not in got for b in required)
                        or len(got) < len(blocks) * min_blocks_frac):
                    skipped_no_block += 1
                    continue
                y = as_float(lab.get(LABELS[model]))
                y_off = as_float(lab.get(LABELS_OFF[model]))
                y_def = as_float(lab.get(LABELS_DEF[model]))
                # require all three so every target sees the same rows
                if y is None or y_off is None or y_def is None:
                    continue
                vec = []
                for b in blocks:
                    doc = got.get(b, {})
                    vec.extend(as_float(doc.get(f)) for f in fields[b])
                vec = [np.nan if v is None else v for v in vec]
                rows.append(vec)
                ys.append(y)
                ys_off.append(y_off)
                ys_def.append(y_def)
                meta.append({"player": player, "timestamp": ts, "season": season_of(ts),
                             "season_type": st, "mp": as_float(lab.get("mp")) or 0.0,
                             "pos": lab.get("pos", ""), "test": is_test(ts)})
            print(f"  [{i+1}/{len(timestamps)}] {ts} {st:<15} 538={len(labels):<4} "
                  f"rows+={len(rows)-n_before}")

    X = np.asarray(rows, dtype=np.float32)
    y = np.asarray(ys, dtype=np.float32)
    y_off = np.asarray(ys_off, dtype=np.float32)
    y_def = np.asarray(ys_def, dtype=np.float32)
    feat_names = [f"{b}|{f}" for b in blocks for f in fields[b]]
    print(f"[{model}] X={X.shape} y={y.shape} skipped(missing block)={skipped_no_block}")
    return X, y, y_off, y_def, feat_names, meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", choices=["box", "onoff", "combined", "both", "all"],
                    default="both")
    ap.add_argument("--modern-stride", type=int, default=6,
                    help="keep every Nth modern snapshot (they are ~daily and highly redundant)")
    ap.add_argument("--outdir", default=str(REPO_ROOT / "training" / "data"))
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    report = json.load(open(REPO_ROOT / "training" / "coverage_report.json"))
    coll = get_collection()
    ts_by_src = timestamps_with_sources(coll)

    models = {"both": ["box", "onoff"],
              "all": ["box", "onoff", "combined"]}.get(args.model, [args.model])
    for model in models:
        tss = select_timestamps(ts_by_src, model, args.modern_stride)
        print(f"\n[{model}] {len(tss)} timestamps selected")
        X, y, y_off, y_def, feat_names, meta = build(model, coll, report, tss)
        np.savez_compressed(
            outdir / f"{model}.npz", X=X, y=y, y_off=y_off, y_def=y_def,
            feat_names=np.array(feat_names, dtype=object),
            player=np.array([m["player"] for m in meta], dtype=object),
            timestamp=np.array([m["timestamp"] for m in meta], dtype=object),
            season=np.array([m["season"] for m in meta], dtype=object),
            season_type=np.array([m["season_type"] for m in meta], dtype=object),
            mp=np.array([m["mp"] for m in meta], dtype=np.float32),
            pos=np.array([m["pos"] for m in meta], dtype=object),
            test=np.array([m["test"] for m in meta], dtype=bool),
        )
        print(f"[{model}] wrote {outdir / f'{model}.npz'}")


if __name__ == "__main__":
    main()
