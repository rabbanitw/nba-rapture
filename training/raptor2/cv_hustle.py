"""Defense CV with the scraped hustle/defend-dash/matchup features as new
GBM inputs (data_fixed/hustle.npz). Protocol is cv_resid_pools verbatim
(same folds, exact NaN-propagating struct hats, blend = 3 seeds + ridge);
the only change per arm is extra feature columns on the defense stack:

  e     + engineered rates (23 cols: hustle per36, defended pct/plusminus,
          matchup opponent scoring per 100 partial poss)
  ecr   + the same block cell-relative (minutes-weighted cell mean removed)
  er    + engineered + all 51 raw verbatim columns

Coverage is the 10 season-end stamps only (95-100% of rows there; hustle
2015-16+, matchups 2017-18+); everything else NaN -> GBM missing branch.
Hats are identical across arms, so they are built once per fold.

Run:  python training/raptor2/cv_hustle.py [--seedbase 0]
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
from experiment_oppdef import blend, engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seedbase", type=int, default=0)
    ap.add_argument("--arms", nargs="*", default=["e", "ecr", "er"])
    args = ap.parse_args()
    seeds = (args.seedbase, args.seedbase + 1, args.seedbase + 2)

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    hz = np.load(TD / "data_fixed" / "hustle.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(TD / "raptor2" / "courtmate.npz")["CM"]
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
    base = np.hstack([X, Z, dfz["E"]])

    huE = hz["E"].astype(np.float32)
    huR = hz["R"].astype(np.float32)
    huEcr = cell_relative(huE, cells, mp)
    enames = [str(x) for x in hz["enames"]]
    slim_cols = [enames.index(c) for c in
                 ("charges_per36", "deflections_per36", "mu_pts_per100",
                  "mu_efg_allowed")]
    ARMS = {"e": np.hstack([base, huE]),
            "ecr": np.hstack([base, huEcr]),
            "er": np.hstack([base, huE, huR]),
            "slim": np.hstack([base, huE[:, slim_cols]])}
    ARMS = {k: v for k, v in ARMS.items() if k in args.arms}

    def make_hats(mask_tr):
        hats = {}
        for tag, (M, labname) in BLOCKS.items():
            yv = comp[labname]
            m = mask_tr & np.isfinite(yv) & np.isfinite(M).all(axis=1)
            hats[tag] = ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1],
                                  tag, quiet=True)
            hats[tag][~np.isfinite(M).all(axis=1)] = np.nan
        return hats

    y = d[TARGETS["defense"]].astype(np.float64)
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y)
    ref = json.loads((TD / "raptor2" / "RESULTS_hats3_cv.json").read_text())
    refper = ref["defense"]["per_season"]

    per = {a: {} for a in ARMS}
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        elm = mp[te] >= FLOOR
        hf = make_hats(trn)
        H = np.column_stack([hf[t] for t in
                             ("box_o", "onoff_o", "box_d", "onoff_d")])
        for arm, Xf in ARMS.items():
            Xa = np.hstack([Xf, H])
            med = np.nanmedian(Xa[trn], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            p = blend(Xa[trn], y[trn], Xa[te], med, params, rounds, seeds)
            s = score_cells(y[te][elm], p[elm], np.full(int(elm.sum()), season))
            per[arm][season] = round(float(s["dev@10"]), 2)
            print(f"[{arm}] {season}: dev@10={s['dev@10']:.2f} "
                  f"tau@10={s['tau@10']:+.3f}  (hats3 {refper[season]})",
                  flush=True)

    out = {"seedbase": args.seedbase, "baseline": refper, "arms": {}}
    for arm in ARMS:
        dv = list(per[arm].values())
        wl = [np.sign(refper[s] - per[arm][s]) for s in STAMPS]
        rec = [int(sum(x > 0 for x in wl)), int(sum(x == 0 for x in wl)),
               int(sum(x < 0 for x in wl))]
        out["arms"][arm] = {"per_season": per[arm],
                            "median": float(np.median(dv)),
                            "mean": float(np.mean(dv)),
                            "wl_vs_hats3": rec}
        print(f"[{arm}] median {np.median(dv):.2f} mean {np.mean(dv):.2f} | "
              f"hats3 median {ref['defense']['median']:.2f} | "
              f"{rec[0]}W {rec[1]}T {rec[2]}L", flush=True)
    tag = f"_s{args.seedbase}" if args.seedbase else ""
    if set(args.arms) != {"e", "ecr", "er"}:
        tag += "_" + "-".join(sorted(args.arms))
    fp = TD / "raptor2" / f"RESULTS_cv_hustle{tag}.json"
    fp.write_text(json.dumps(out, indent=1))
    print(f"wrote {fp}", flush=True)


if __name__ == "__main__":
    main()
