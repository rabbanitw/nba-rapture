"""Optimise ordering directly, instead of trying to bribe a regression into it.

Sample weighting was the obvious way to make the model care more about the top of
the leaderboard, and it does not work -- see RESULTS_weight_seeds.json. Across 8
seeds, weighting the positive rows 3x moved hits@30 by +0.1 on a standard error of
0.9, and the strongest ramp made it worse. That is the expected outcome in
hindsight: reweighting a squared-error objective changes which rows the conditional
mean is fitted to, but it never tells the model that ordering is the thing being
judged. It just throws away effective sample size.

A ranking objective does say that. LightGBM's lambdarank works on groups -- here a
group is one (season, split) cell, exactly the unit a leaderboard is drawn over --
and optimises NDCG, which is dominated by whether the genuinely best players are
placed at the top.

The cost is that a ranker produces scores, not RAPTOR. They order players but carry
no units, so R² against RAPTOR is meaningless for them and the table below omits it.
That is fine for a leaderboard and useless for anything that wants a number, which
is why this is reported as a candidate rather than folded into the model.

Relevance grades: lambdarank wants small non-negative integers. RAPTOR is continuous,
so each cell's players are cut into 5 grades by rank -- top 10% down to bottom 40% --
which is the standard treatment and keeps the grade meaning the same across cells of
different size.

Run:  python training/experiment_ranking.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np

from db import REPO_ROOT
from experiment_arch_weight import cell_topk, evaluate
from experiment_combined import prepare, splits
from predict_seasons import DROP_FEATURES
from train_rapture import LGB_PARAMS, TARGETS

RS_MIN, PO_MIN = 50, 10
# Fractional cut points, best first. 5 grades: 4 is the top 10% of a cell.
GRADE_CUTS = [0.10, 0.25, 0.45, 0.70]


def grade(y, cells):
    """Per-cell relevance grades 0..4, highest RAPTOR gets 4."""
    g = np.zeros(len(y), dtype=int)
    for c in np.unique(cells):
        m = np.where(cells == c)[0]
        order = m[np.argsort(-y[m])]
        n = len(order)
        for i, idx in enumerate(order):
            frac = i / n
            grade_val = 4
            for cut in GRADE_CUTS:
                if frac >= cut:
                    grade_val -= 1
            g[idx] = max(grade_val, 0)
    return g


def groups(cells):
    """lambdarank wants contiguous group sizes; rows must be sorted by cell."""
    _, counts = np.unique(cells, return_counts=True)
    return counts


def sorted_by_cell(cells, *arrays):
    order = np.argsort(cells, kind="stable")
    return (cells[order],) + tuple(a[order] for a in arrays)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--target", default="total")
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_ranking.json"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X = X[:, keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    y = d[TARGETS[args.target]]

    def cellnames(mask):
        return np.array([f"{s}|{t}|{ts}" for s, t, ts in
                         zip(d["season"][mask], d["season_type"][mask],
                             d["timestamp"][mask])])

    cf, Xf, yf = sorted_by_cell(cellnames(fit), X[fit], y[fit])
    cv, Xv, yv = sorted_by_cell(cellnames(val), X[val], y[val])
    # Test cells are scored per (season, split), matching the other experiments.
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])

    gf, gv = grade(yf, cf), grade(yv, cv)
    print(f"fit groups={len(groups(cf))}  val groups={len(groups(cv))}")
    print(f"grade distribution (fit): {np.bincount(gf)}")

    params = dict(LGB_PARAMS)
    params.pop("objective")
    params.update(objective="lambdarank", metric="ndcg", ndcg_eval_at=[30],
                  lambdarank_truncation_level=60, label_gain=[0, 1, 3, 7, 15])

    runs = []
    for seed in range(args.seeds):
        p = dict(params, seed=seed, bagging_seed=seed, feature_fraction_seed=seed)
        ds = lgb.Dataset(Xf, gf, group=groups(cf))
        dv = lgb.Dataset(Xv, gv, group=groups(cv), reference=ds)
        bst = lgb.train(p, ds, num_boost_round=3000, valid_sets=[dv],
                        callbacks=[lgb.early_stopping(150, verbose=False)])
        pred = bst.predict(X[test])
        h30, n30, rho30 = cell_topk(y[test], pred, cells_te, 30)
        h20, n20, _ = cell_topk(y[test], pred, cells_te, 20)
        runs.append({"seed": seed, "rounds": bst.best_iteration,
                     "hits30": h30, "hits20": h20, "rho30": rho30})
        print(f"  lambdarank seed {seed}  {bst.best_iteration:>4} rounds  "
              f"hits@20={h20}/{n20} hits@30={h30}/{n30} rho@30={rho30:+.3f}",
              flush=True)

    h30 = np.array([r["hits30"] for r in runs], dtype=float)
    h20 = np.array([r["hits20"] for r in runs], dtype=float)
    rho = np.array([r["rho30"] for r in runs])
    print(f"\nlambdarank  hits@20 {h20.mean():.1f} ± {h20.std():.1f}/80   "
          f"hits@30 {h30.mean():.1f} ± {h30.std():.1f}/120   "
          f"rho@30 {rho.mean():+.3f} ± {rho.std():.3f}")
    print("regression baseline (8 seeds, RESULTS_weight_seeds.json): "
          "hits@30 85.4 ± 1.4/120, rho@30 +0.591 ± 0.020")

    Path(args.out).write_text(json.dumps(runs, indent=1))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
