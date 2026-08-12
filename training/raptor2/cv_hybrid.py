"""Season-held-out CV: does gbm+hats (structural component hats as features)
beat the plain full-matrix GBM for offense?

Hats are refit inside every fold (component ridges on fold-train rows only) --
the structural hats may not leak the held-out season's component labels.
Seed set configurable for disjoint replication: --seedbase 0 | 10 | 20.

Run:  python training/raptor2/cv_hybrid.py --seedbase 0
"""

import argparse
import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from variables2 import build_onoff2
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def blend_members(Xtr, t, Xte, med, params, rounds, seeds, ridge_w=0.25):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sdv = A.mean(0), A.std(0)
    sdv[sdv == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sdv, t).predict(
        (B - mu) / sdv)
    return np.mean([(1 - ridge_w) * lgb.train(
        dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s),
        lgb.Dataset(Xtr, t), num_boost_round=rounds).predict(Xte)
        + ridge_w * pr for s in seeds], axis=0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seedbase", type=int, default=0)
    args = ap.parse_args()
    seeds = (args.seedbase, args.seedbase + 1, args.seedbase + 2)

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    ofields = [str(f) for f in oppz["fields"]]
    Eopp, _ = engineered(oppz["on_X"], oppz["off_X"], ofields, cells)
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OFF2, _ = build_onoff2(X, feat, oppz["on_X"], oppz["off_X"], ofields,
                           cells, mp)
    OB = cell_relative(V["OB"], cells, mp)
    OOo = cell_relative(OFF2, cells, mp)

    rs = d["season_type"] == "Regular season"
    y = d[TARGETS["offense"]].astype(np.float64)
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y)
    w = np.sqrt(np.maximum(mp, 1.0))
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned["offense"]["params"], verbose=-1)
    rounds = max(tuned["offense"]["rounds"] // 3, 150)
    Xf = np.hstack([X, Z, Eopp])

    per = {}
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        el = mp[te] >= FLOOR
        hats = []
        for M, labname in ((OB, "rap_box_o"), (OOo, "rap_onoff_o")):
            yv = comp[labname]
            m = trn & np.isfinite(yv)
            hats.append(ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1],
                                  "", quiet=True))
        H = np.column_stack(hats)
        row = {}
        for arm, Xa in (("gbm", Xf), ("gbm+hats", np.hstack([Xf, H]))):
            med = np.nanmedian(Xa[trn], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            p = blend_members(Xa[trn], y[trn], Xa[te], med, params, rounds,
                              seeds)
            s = score_cells(y[te][el], p[el], np.full(int(el.sum()), season))
            row[arm] = {k: (int(v) if isinstance(v, (int, np.integer))
                            else round(float(v), 4)) for k, v in s.items()}
        per[season] = row
        print(f"{season}: gbm dev@10={row['gbm']['dev@10']:5.2f} "
              f"tau@10={row['gbm']['tau@10']:+.3f} | +hats "
              f"dev@10={row['gbm+hats']['dev@10']:5.2f} "
              f"tau@10={row['gbm+hats']['tau@10']:+.3f}", flush=True)

    print(f"\n== summary (seeds {seeds}) ==", flush=True)
    for arm in ("gbm", "gbm+hats"):
        dv = [per[s][arm]["dev@10"] for s in STAMPS]
        h = sum(per[s][arm]["hits@10"] for s in STAMPS)
        print(f"  {arm:<9} median dev@10 {np.median(dv):.2f}  mean "
              f"{np.mean(dv):.2f}  hits@10 {h}/100", flush=True)
    wins = sum(per[s]["gbm+hats"]["dev@10"] < per[s]["gbm"]["dev@10"]
               for s in STAMPS)
    ties = sum(per[s]["gbm+hats"]["dev@10"] == per[s]["gbm"]["dev@10"]
               for s in STAMPS)
    print(f"  +hats vs gbm head-to-head: {wins}W {ties}T {10-wins-ties}L",
          flush=True)
    Path(TD / "raptor2" / f"RESULTS_cv_hybrid_s{args.seedbase}.json"
         ).write_text(json.dumps(per, indent=1))
    print(f"wrote raptor2/RESULTS_cv_hybrid_s{args.seedbase}.json", flush=True)


if __name__ == "__main__":
    main()
