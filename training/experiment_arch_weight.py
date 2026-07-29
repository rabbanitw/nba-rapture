"""Two questions at once: does weighting the top end help, and is LightGBM the right model?

Both are judged on the same held-out seasons (2013-14, 2014-15, regular season and
playoffs) with hyperparameters and scheme choices made on the validation seasons, so
nothing here is selected on the test rows.

Why top-end metrics and not just R². The stated use is a leaderboard: who are the
best 20-30 players. A model can win on pooled R² by being tidy in the crowded middle
of the distribution and still misorder the top, and the middle is where almost all
the rows are. So every run reports, per cell:

  hits@k    how many of the true top k appear in the predicted top k
  rho@30    Spearman among the true top 30 -- ordering where it matters
  rmse+     RMSE over rows with a positive RAPTOR, which is roughly the top third

Weighting schemes put more loss on high-RAPTOR rows. Sample weights are the honest
way to do this for a squared-error objective: a custom asymmetric loss would also
shift the conditional mean the model is estimating, which is not what is wanted --
the target is still RAPTOR, just measured more carefully at the top.

Run:  python training/experiment_arch_weight.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from scipy.stats import spearmanr
from sklearn.ensemble import (ExtraTreesRegressor, HistGradientBoostingRegressor,
                              RandomForestRegressor)
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from estimated_raptor import metrics
from experiment_combined import prepare, splits
from predict_seasons import DROP_FEATURES
from train_rapture import LGB_PARAMS, TARGETS

RS_MIN, PO_MIN = 50, 10
TOPKS = (20, 30)

# w = 1 + strength * max(0, y). Linear in the label, so a +8 player counts a few
# times a replacement-level one rather than hundreds of times; anything steeper
# collapses the effective sample size.
WEIGHT_SCHEMES = {
    "none": lambda y: np.ones_like(y),
    "linear-0.25": lambda y: 1.0 + 0.25 * np.maximum(y, 0),
    "linear-0.5": lambda y: 1.0 + 0.5 * np.maximum(y, 0),
    "linear-1.0": lambda y: 1.0 + 1.0 * np.maximum(y, 0),
    # A hard gate instead of a ramp: everything positive counts triple.
    "positive-3x": lambda y: np.where(y > 0, 3.0, 1.0),
}


def cell_topk(y, p, cells, k):
    """-> (hits, possible) summed over cells, and mean Spearman within the true top k."""
    hits = poss = 0
    rhos = []
    for c in np.unique(cells):
        m = cells == c
        yt, pt = y[m], p[m]
        if len(yt) < k:
            continue
        true_top = set(np.argsort(-yt)[:k].tolist())
        pred_top = set(np.argsort(-pt)[:k].tolist())
        hits += len(true_top & pred_top)
        poss += k
        idx = np.argsort(-yt)[:k]
        if np.std(pt[idx]) > 0:
            rhos.append(spearmanr(yt[idx], pt[idx]).statistic)
    return hits, poss, float(np.mean(rhos)) if rhos else float("nan")


def evaluate(y, p, cells):
    out = metrics(y, p)
    for k in TOPKS:
        h, n, rho = cell_topk(y, p, cells, k)
        out[f"hits@{k}"] = f"{h}/{n}"
        out[f"hits@{k}_frac"] = h / max(n, 1)
        if k == 30:
            out["rho@30"] = rho
    pos = y > 0
    out["rmse+"] = float(np.sqrt(np.mean((y[pos] - p[pos]) ** 2))) if pos.any() else float("nan")
    out["n_pos"] = int(pos.sum())
    return out


def impute(X, med):
    return np.where(np.isfinite(X), X, med)


def fit_predict(name, Xtr, ytr, w, Xva, yva, Xte, med):
    """-> (val prediction, test prediction). One place so every model sees the same rows."""
    if name == "lightgbm":
        bst = lgb.train(LGB_PARAMS, lgb.Dataset(Xtr, ytr, weight=w),
                        num_boost_round=4000,
                        valid_sets=[lgb.Dataset(Xva, yva)],
                        callbacks=[lgb.early_stopping(150, verbose=False)])
        return bst.predict(Xva), bst.predict(Xte), bst.best_iteration
    if name == "hist-gbm":
        m = HistGradientBoostingRegressor(max_iter=600, learning_rate=0.05,
                                          max_leaf_nodes=31, min_samples_leaf=40,
                                          l2_regularization=5.0, random_state=42)
        m.fit(Xtr, ytr, sample_weight=w)
        return m.predict(Xva), m.predict(Xte), m.n_iter_
    # The sklearn forests reject NaN, so they get a median-imputed copy. The median
    # comes from the training rows only.
    A, B, C = impute(Xtr, med), impute(Xva, med), impute(Xte, med)
    if name == "random-forest":
        m = RandomForestRegressor(n_estimators=300, min_samples_leaf=5,
                                  max_features=0.3, n_jobs=-1, random_state=42)
    elif name == "extra-trees":
        m = ExtraTreesRegressor(n_estimators=300, min_samples_leaf=5,
                                max_features=0.3, n_jobs=-1, random_state=42)
    elif name == "ridge":
        mu, sd = A.mean(0), A.std(0)
        sd[sd == 0] = 1.0
        m = RidgeCV(alphas=np.logspace(-2, 4, 25))
        m.fit((A - mu) / sd, ytr, sample_weight=w)
        return m.predict((B - mu) / sd), m.predict((C - mu) / sd), None
    else:
        raise ValueError(name)
    m.fit(A, ytr, sample_weight=w)
    return m.predict(B), m.predict(C), None


ARCHITECTURES = ["lightgbm", "hist-gbm", "random-forest", "extra-trees", "ridge"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_arch_weight.md"))
    ap.add_argument("--targets", nargs="*", default=["total", "offense", "defense"])
    ap.add_argument("--schemes", nargs="*", default=list(WEIGHT_SCHEMES))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    tr = fit | val
    print(f"X={X.shape}  fit={fit.sum()} val={val.sum()} test={test.sum()}")

    cells_va = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][val], d["season_type"][val])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    med = np.nanmedian(X[fit], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)

    results = []
    for target in args.targets:
        y = d[TARGETS[target]]
        for arch in ARCHITECTURES:
            for scheme in args.schemes:
                wf = WEIGHT_SCHEMES[scheme]
                w = wf(y[fit])
                pv, pt, rounds = fit_predict(arch, X[fit], y[fit], w,
                                             X[val], y[val], X[test], med)
                row = {"target": target, "arch": arch, "weights": scheme,
                       "rounds": rounds,
                       "val": evaluate(y[val], pv, cells_va),
                       "test": evaluate(y[test], pt, cells_te)}
                results.append(row)
                print(f"  {target:<8} {arch:<14} {scheme:<12} "
                      f"val R2={row['val']['r2']:+.3f} hits@30={row['val']['hits@30']:<7} "
                      f"| test R2={row['test']['r2']:+.3f} "
                      f"hits@30={row['test']['hits@30']:<7} "
                      f"rho@30={row['test']['rho@30']:+.3f} "
                      f"rmse+={row['test']['rmse+']:.3f}", flush=True)

    Path(args.out).with_suffix(".json").write_text(json.dumps(results, indent=1))
    write_report(results, args.out, X.shape, int(fit.sum()), int(val.sum()),
                 int(test.sum()))
    print(f"\nwrote {args.out}")


def write_report(results, out, shape, nfit, nval, ntest):
    L = []
    A = L.append
    A("# Architectures and top-end weighting")
    A("")
    A(f"Built on the corrected dataset including 2018-19: X={shape}, "
      f"{nfit} fit / {nval} validation / {ntest} test rows.")
    A("")
    A("Selection is on the validation seasons; the tables report the held-out")
    A("2013-14 and 2014-15 rows. `hits@k` counts how many of the true top k appear in")
    A("the predicted top k, summed over the four test cells. `rho@30` is Spearman")
    A("within the true top 30. `rmse+` is RMSE over positive-RAPTOR rows.")
    A("")
    for target in sorted({r["target"] for r in results}, key=
                         lambda t: ["total", "offense", "defense"].index(t)):
        rows = [r for r in results if r["target"] == target]
        best = max(rows, key=lambda r: r["val"]["hits@30_frac"])
        A(f"## {target}")
        A("")
        A("| architecture | weights | test R² | RMSE | rmse+ | hits@20 | hits@30 | rho@30 |")
        A("|---|---|---|---|---|---|---|---|")
        for r in sorted(rows, key=lambda r: -r["test"]["hits@30_frac"]):
            t = r["test"]
            mark = " **←val pick**" if r is best else ""
            A(f"| {r['arch']} | {r['weights']} | {t['r2']:+.3f} | {t['rmse']:.3f} | "
              f"{t['rmse+']:.3f} | {t['hits@20']} | {t['hits@30']} | "
              f"{t['rho@30']:+.3f} |{mark}")
        A("")
    Path(out).write_text("\n".join(L))


if __name__ == "__main__":
    main()
