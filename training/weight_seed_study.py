"""Is the top-end weighting doing anything, or is it seed noise?

The architecture sweep put LightGBM at 86/120 hits@30 unweighted and 89/120 with
positive-3x. Three hits out of 120 is exactly the size of difference that a single
fit cannot distinguish from run-to-run variation, and the validation seasons ranked
the two schemes equal, so there is no basis for preferring either from one run.

This refits each scheme across seeds and reports the spread. If the weighted and
unweighted distributions overlap, the honest answer is that weighting does not help,
and the way to improve the top of the leaderboard has to be something else.

Run:  python training/weight_seed_study.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np

from db import REPO_ROOT
from estimated_raptor import metrics
from experiment_arch_weight import WEIGHT_SCHEMES, cell_topk, evaluate
from experiment_combined import prepare, splits
from predict_seasons import DROP_FEATURES
from train_rapture import LGB_PARAMS, TARGETS

RS_MIN, PO_MIN = 50, 10
SCHEMES = ["none", "linear-0.5", "linear-1.0", "positive-3x"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--target", default="total")
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_weight_seeds.json"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X = X[:, keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    y = d[TARGETS[args.target]]
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])

    out = {}
    for scheme in SCHEMES:
        w = WEIGHT_SCHEMES[scheme](y[fit])
        runs = []
        for seed in range(args.seeds):
            params = dict(LGB_PARAMS, seed=seed, bagging_seed=seed,
                          feature_fraction_seed=seed)
            bst = lgb.train(params, lgb.Dataset(X[fit], y[fit], weight=w),
                            num_boost_round=4000,
                            valid_sets=[lgb.Dataset(X[val], y[val])],
                            callbacks=[lgb.early_stopping(150, verbose=False)])
            m = evaluate(y[test], bst.predict(X[test]), cells_te)
            runs.append({"seed": seed, "r2": m["r2"], "rmse": m["rmse"],
                         "hits30": m["hits@30_frac"] * 120,
                         "hits20": m["hits@20_frac"] * 80,
                         "rho30": m["rho@30"], "rmse_pos": m["rmse+"]})
            print(f"  {scheme:<12} seed {seed}  R2={m['r2']:+.3f} "
                  f"hits@30={m['hits@30']} rho@30={m['rho@30']:+.3f}", flush=True)
        out[scheme] = runs

    print(f"\n{'scheme':<13} {'R2 mean±sd':>18} {'hits@30 mean±sd':>20} "
          f"{'rho@30 mean±sd':>19}")
    for scheme, runs in out.items():
        r2 = np.array([r["r2"] for r in runs])
        h = np.array([r["hits30"] for r in runs])
        rho = np.array([r["rho30"] for r in runs])
        print(f"{scheme:<13} {r2.mean():+8.3f} ± {r2.std():.3f} "
              f"{h.mean():11.1f} ± {h.std():.1f}/120 "
              f"{rho.mean():+11.3f} ± {rho.std():.3f}")

    base = np.array([r["hits30"] for r in out["none"]])
    print()
    for scheme, runs in out.items():
        if scheme == "none":
            continue
        h = np.array([r["hits30"] for r in runs])
        diff = h.mean() - base.mean()
        pooled = np.sqrt(h.var(ddof=1) / len(h) + base.var(ddof=1) / len(base))
        print(f"{scheme:<13} hits@30 vs none: {diff:+.1f} "
              f"(standard error of the difference {pooled:.1f}) -> "
              f"{'distinguishable' if abs(diff) > 2 * pooled else 'not distinguishable'}")

    Path(args.out).write_text(json.dumps(out, indent=1))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
