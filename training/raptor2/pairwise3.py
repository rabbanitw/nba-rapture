"""Pairwise at full feature width: does the classification loss beat wide
regression when both see the same features?

pairwise2 answered loss-vs-loss on the compact structural blocks (pairwise
wins accuracy on every target) but its models still lose to cv_components'
wide-feature regressions on component ordering. The confound is width. Here
the pair model gets the SAME wide matrix as the Part-A winners, in difference
form (x_a - x_b) -- diff beat concat on every pairwise2 target, and concat at
this width does not fit in 7GB.

Box components only: the on/off wide matrix (1391 cols) would need ~5GB per
fold in difference form. All ordered non-tie pairs, 10-fold season-held-out,
tournament scoring, identical metrics to pairwise2 -- numbers directly
comparable to both pairwise2 (same protocol) and RESULTS_cv_components.json
(same features, regression).

Run:  python training/raptor2/pairwise3.py [--targets box_o box_d]
"""

import argparse
import gc
import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scipy.stats import spearmanr

from db import REPO_ROOT
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS, masks_for
from experiment_components import cell_relative as cellrel_features
from experiment_topk_rank import dev_at_k, hits_at_k, tau_at_k
from predict_seasons import DROP_FEATURES
from variables import build_variables
from structural import cell_relative
from pairwise2 import (PAIR_PARAMS, PAIR_ROUNDS, STAMPS, TIE_EPS, FLOOR,
                       all_ordered_pairs, pair_accuracy)

TD = REPO_ROOT / "training"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", nargs="*", default=["box_o", "box_d"])
    ap.add_argument("--seasons", nargs="*", default=list(STAMPS))
    args = ap.parse_args()

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    box_mask, _ = masks_for(feat)
    sdR = sd["R"].astype(np.float32)
    dfzE = dfz["E"].astype(np.float32)
    MPC = (mp / 1000.0).reshape(-1, 1).astype(np.float32)
    WIDE = {"box_o": np.hstack([X[:, box_mask], sdR, Z, MPC]
                               ).astype(np.float32),
            "box_d": np.hstack([X[:, box_mask], sdR, dfzE, Z, MPC]
                               ).astype(np.float32)}
    LAB = {"box_o": "rap_box_o", "box_d": "rap_box_d"}

    y_any = d["y_off"].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y_any)

    results = {}
    for tgt in args.targets:
        F = WIDE[tgt]
        yv = comp[LAB[tgt]].astype(np.float64)
        print(f"\n=== {tgt} (wide, {F.shape[1]} cols/player, diff pairs) ===",
              flush=True)
        results[tgt] = {}
        for season in args.seasons:
            stamp = STAMPS[season]
            te = labeled & (d["timestamp"] == stamp)
            trn = labeled & (d["timestamp"] != stamp)
            ti = np.where(te)[0]
            el = mp[ti] >= FLOOR
            yte = yv[ti]
            cell_rows = [np.where(trn & (d["timestamp"] == st))[0]
                         for st in STAMPS.values() if st != stamp]
            a, b = all_ordered_pairs(cell_rows, yv)
            D = (F[a] - F[b])
            L = (yv[a] > yv[b]).astype(np.int8)
            model = lgb.train(dict(PAIR_PARAMS, seed=0, bagging_seed=0,
                                   feature_fraction_seed=0),
                              lgb.Dataset(D, L), num_boost_round=PAIR_ROUNDS)
            del D, L
            gc.collect()

            n = len(ti)
            ii, jj = np.triu_indices(n, k=1)
            pa, pb = ti[ii], ti[jj]
            p = np.zeros(len(pa))
            pr = np.zeros(len(pa))
            for lo in range(0, len(pa), 100_000):
                sl = slice(lo, min(lo + 100_000, len(pa)))
                p[sl] = model.predict((F[pa[sl]] - F[pb[sl]]))
                pr[sl] = model.predict((F[pb[sl]] - F[pa[sl]]))
            wprob = (p + (1 - pr)) / 2.0
            wins = np.zeros(n)
            np.add.at(wins, ii, wprob)
            np.add.at(wins, jj, 1 - wprob)
            sc = np.full(len(yv), np.nan)
            sc[ti] = wins / (n - 1)

            row = {"n_pairs": int(len(a)),
                   "acc": pair_accuracy(sc, yv, ti),
                   "acc@pool": pair_accuracy(sc, yv, ti, floor_mask=el),
                   "rho": float(spearmanr(yte, sc[ti]).statistic),
                   "dev@10": dev_at_k(yte[el], sc[ti][el], 10),
                   "tau@10": tau_at_k(yte[el], sc[ti][el], 10),
                   "hits@10": hits_at_k(yte[el], sc[ti][el], 10)}
            results[tgt][season] = row
            print(f"  {season} ({row['n_pairs']:,} pairs): acc {row['acc']:.3f}"
                  f" rho {row['rho']:+.3f} dev {row['dev@10']:5.2f} "
                  f"tau {row['tau@10']:+.3f} hits {row['hits@10']}/10",
                  flush=True)
            del model
            gc.collect()

        accs = [results[tgt][s]["acc"] for s in args.seasons]
        rhos = [results[tgt][s]["rho"] for s in args.seasons]
        devs = [results[tgt][s]["dev@10"] for s in args.seasons]
        hits = sum(results[tgt][s]["hits@10"] for s in args.seasons)
        print(f"-- {tgt} wide-diff summary: acc mean {np.mean(accs):.3f} | "
              f"rho med {np.median(rhos):+.3f} | dev@10 med "
              f"{np.median(devs):5.2f} mean {np.mean(devs):5.2f} | hits@10 "
              f"{hits}/{10*len(args.seasons)}", flush=True)

    out = TD / "raptor2" / "RESULTS_pairwise3.json"
    out.write_text(json.dumps(results, indent=1))
    print(f"\nwrote {out}", flush=True)


if __name__ == "__main__":
    main()
