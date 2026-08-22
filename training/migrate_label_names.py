"""Fix the fuzzy-name label corruption in the whole-season cells.

The 538 label docs were standardized with the old fuzzy name matcher, which
mapped players missing from its roster (mostly 2021+ rookies) onto existing
names: 'Walker Kessler' -> standard_name 'Kemba Walker', 'Austin Reaves' ->
'Austin Rivers', 'Jalen Suggs' -> 'Jalen Smith', ... Because labels.py joins
by standard_name (first doc wins), those veterans' matrix rows carry the
OTHER player's rap/rap_o/rap_d/mp and component labels. Audit: 0 corrupted
rows 2013-14..2019-20, 1 in 2020-21, 25 in 2021-22, 77/361 in 2022-23.

The true identity is still on every doc ('name'/'data_key'), so this
re-joins each whole-season cell by norm_name(doc['name']) and overwrites,
for every matrix row of those cells (both season types):

  combined.npz    y, y_off, y_def, mp
  components.npz  rap_box_o, rap_box_d, rap_onoff_o, rap_onoff_d
                  (+ rap_o / rap_d if stored)

Idempotent (clean rows are no-ops). Originals backed up to *.pre_namefix
once. In-season snapshot cells share the defect but train nothing (the CV
protocol trains and tests on the 10 whole-season stamps only).

Run:  python training/migrate_label_names.py [--dry-run]
"""

import argparse
import shutil
from collections import defaultdict
from pathlib import Path

import numpy as np

from db import REPO_ROOT, get_collection
from estimated_raptor import norm_name
from coverage import as_float

TD = REPO_ROOT / "training"
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}
COMBINED_COLS = {"y": "rap", "y_off": "rap_o", "y_def": "rap_d"}
COMP_COLS = ("rap_box_o", "rap_box_d", "rap_onoff_o", "rap_onoff_d",
             "rap_o", "rap_d")


def total_minutes(table):
    t = 0.0
    for doc in table:
        try:
            t += float(str(doc.get("mp") or "").replace(",", ""))
        except ValueError:
            pass
    return t


def true_table(coll, season, season_type):
    by_ts = defaultdict(list)
    for doc in coll.find({"source": "538", "label_season": season,
                          "season_type": season_type}):
        by_ts[doc["timestamp"]].append(doc)
    if not by_ts:
        return {}
    best = max(by_ts.values(), key=lambda t: (total_minutes(t), len(t)))
    out = {}
    for doc in best:
        out.setdefault(norm_name(doc["name"]), doc)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cpath = TD / "data_fixed" / "combined.npz"
    kpath = TD / "data_fixed" / "components.npz"
    d = dict(np.load(cpath, allow_pickle=True))
    comp = dict(np.load(kpath))
    coll = get_collection()
    players = np.array([str(p) for p in d["player"]])
    tss = np.array([str(t) for t in d["timestamp"]])
    sts = np.array([str(s) for s in d["season_type"]])

    n_fix = n_lost = 0
    for season, stamp in STAMPS.items():
        for st in ("Regular season", "Playoffs"):
            tt = true_table(coll, season, st)
            if not tt:
                continue
            idx = np.where((tss == stamp) & (sts == st))[0]
            for i in idx:
                doc = tt.get(norm_name(players[i]))
                if doc is None:
                    if np.isfinite(d["y_def"][i]):
                        n_lost += 1
                        print(f"  LOST {season} {st}: {players[i]} "
                              f"(no true-name doc) -> labels NaN")
                        if not args.dry_run:
                            for k in COMBINED_COLS:
                                d[k][i] = np.nan
                            for k in COMP_COLS:
                                if k in comp:
                                    comp[k][i] = np.nan
                    continue
                newy = {k: as_float(doc.get(lab))
                        for k, lab in COMBINED_COLS.items()}
                if any(v is None for v in newy.values()):
                    continue
                changed = abs(d["y_def"][i] - newy["y_def"]) > 0.05 \
                    or abs(d["y_off"][i] - newy["y_off"]) > 0.05
                if changed:
                    n_fix += 1
                    print(f"  FIX {season} {st}: {players[i]:<24} "
                          f"y_def {d['y_def'][i]:+5.2f} -> {newy['y_def']:+5.2f} "
                          f"(was doc of '{doc.get('name')}'-free row)")
                if not args.dry_run:
                    for k, v in newy.items():
                        d[k][i] = v
                    mpv = as_float(str(doc.get("mp") or "").replace(",", ""))
                    if mpv is not None:
                        d["mp"][i] = mpv
                    for k in COMP_COLS:
                        if k in comp:
                            v = as_float(doc.get(k))
                            comp[k][i] = np.nan if v is None else v

    print(f"{n_fix} rows corrected, {n_lost} labels lost")
    if args.dry_run:
        return
    for p in (cpath, kpath):
        bak = p.with_suffix(".npz.pre_namefix")
        if not bak.exists():
            shutil.copy2(p, bak)
    np.savez_compressed(cpath, **d)
    np.savez_compressed(kpath, **comp)
    print(f"patched {cpath.name} + {kpath.name} (backups *.pre_namefix)")


if __name__ == "__main__":
    main()
