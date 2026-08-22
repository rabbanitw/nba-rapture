"""Defense outlier-robustness arms on the name-fixed data.

The label-name fix (migrate_label_names.py) removed the corrupted-label
outliers; what remains in the training labels is genuine tiny-sample 538
noise (e.g. +15.4 defense at 30 minutes) and genuine heavy tails. Arms,
each = production defense stack (cv_resid_pools verbatim) with ONE change:

  base       no change (use for seed-spread of the new baseline)
  floor250   drop training rows with mp < 250 (hat fits + final GBM);
             eval pool unchanged
  floor500   same at 500
  winsor6    clip training labels to [-6, +6] (final GBM only)
  huber      objective=huber alpha=2.0 for the final GBM

Run:  python training/raptor2/cv_def_robust.py --arm base --seedbase 10
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import REPO_ROOT
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import blend
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
HERE = TD / "raptor2"
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", choices=["base", "floor250", "floor500",
                                      "winsor6", "huber"], default="base")
    ap.add_argument("--seedbase", type=int, default=0)
    args = ap.parse_args()
    seeds = (args.seedbase, args.seedbase + 1, args.seedbase + 2)

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(HERE / "courtmate.npz")["CM"]
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OO3o = cell_relative(cm[:, [0, 2, 4]], cells, mp)
    OO3d = cell_relative(-cm[:, [1, 3, 5]], cells, mp)
    BLOCKS = {"box_o": (OB, "rap_box_o"), "onoff_o": (OO3o, "rap_onoff_o"),
              "box_d": (DB, "rap_box_d"), "onoff_d": (OO3d, "rap_onoff_d")}
    w = np.sqrt(np.maximum(mp, 1.0))
    rs = d["season_type"] == "Regular season"
    tuned = json.loads((TD / "tuned_params.json").read_text())
    y = d[TARGETS["defense"]].astype(np.float64)
    params = dict(tuned["defense"]["params"], verbose=-1)
    if args.arm == "huber":
        params.update(objective="huber", alpha=2.0)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y)
    Xf = np.hstack([X, Z, dfz["E"]])
    ref = json.loads((HERE / "RESULTS_hats3_cv.json").read_text())["defense"]
    y_tr = y.copy()
    if args.arm == "winsor6":
        y_tr = np.clip(y, -6.0, 6.0)
    floor_tr = {"floor250": 250.0, "floor500": 500.0}.get(args.arm, 0.0)

    per = {}
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp) & (mp >= floor_tr)
        elm = mp[te] >= FLOOR
        hf = {}
        for tag, (M, labname) in BLOCKS.items():
            yv = comp[labname]
            m = trn & np.isfinite(yv) & np.isfinite(M).all(axis=1)
            hf[tag] = ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1],
                                tag, quiet=True)
            hf[tag][~np.isfinite(M).all(axis=1)] = np.nan
        H = np.column_stack([hf[t] for t in
                             ("box_o", "onoff_o", "box_d", "onoff_d")])
        Xa = np.hstack([Xf, H])
        med = np.nanmedian(Xa[trn], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p = blend(Xa[trn], y_tr[trn], Xa[te], med, params, rounds, seeds)
        s = score_cells(y[te][elm], p[elm], np.full(int(elm.sum()), season))
        per[season] = round(float(s["dev@10"]), 2)
        print(f"[{args.arm}/s{args.seedbase}] {season}: "
              f"dev@10={s['dev@10']:.2f} (base {ref['per_season'][season]})",
              flush=True)

    dv = list(per.values())
    wl = [np.sign(ref["per_season"][s] - per[s]) for s in STAMPS]
    rec = [int(sum(x > 0 for x in wl)), int(sum(x == 0 for x in wl)),
           int(sum(x < 0 for x in wl))]
    print(f"[{args.arm}/s{args.seedbase}] median {np.median(dv):.2f} "
          f"mean {np.mean(dv):.2f} | base {ref['median']:.2f}/"
          f"{ref['mean']:.2f} | {rec[0]}W {rec[1]}T {rec[2]}L", flush=True)
    fp = HERE / f"RESULTS_cv_def_{args.arm}_s{args.seedbase}.json"
    fp.write_text(json.dumps({"per_season": per,
                              "median": float(np.median(dv)),
                              "mean": float(np.mean(dv)),
                              "wl_vs_base": rec}, indent=1))
    print(f"wrote {fp}", flush=True)


if __name__ == "__main__":
    main()
