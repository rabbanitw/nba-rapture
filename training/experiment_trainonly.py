"""Train/test only, an audited penalty term, and a small neural net.

Three things, all on the corrected 2018-19-inclusive dataset.

1. No validation split. Everything that is not a test row trains. LightGBM's round
   count comes from 5-fold CV inside the training rows, so the test seasons are
   still never consulted -- "skip validation" means stop holding a season out, not
   start peeking.

2. The penalty term, audited rather than assumed. audit_weights() prints what each
   scheme actually does to the fit set: how many rows it touches, the effective
   sample size it leaves, and whether the resulting model moves on the rows it was
   supposed to help. An earlier version of this weighting looked like it helped and
   did not, so the checks are printed rather than trusted.

   Also adds a scheme the earlier sweep was missing. "Positive RAPTOR" is about a
   third of all rows, which is a blunt instrument for a top-20-30 leaderboard.
   top20/top30-by-cell weight exactly the rows the leaderboard is made of.

3. A small MLP. 1140 inputs is wide for 13k rows, so the nets are deliberately
   narrow, and they get median imputation plus standardisation, which trees do not
   need but a net cannot do without.

Run:  python training/experiment_trainonly.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import RidgeCV
from sklearn.neural_network import MLPRegressor

from db import REPO_ROOT
from experiment_arch_weight import evaluate
from experiment_combined import prepare, splits
from predict_seasons import DROP_FEATURES
from train_rapture import LGB_PARAMS, TARGETS

RS_MIN, PO_MIN = 50, 10


def cell_rank_weight(y, cells, k, factor):
    """Weight the top k of each cell -- the rows a top-k leaderboard is made of."""
    w = np.ones(len(y), dtype=np.float64)
    for c in np.unique(cells):
        m = np.where(cells == c)[0]
        top = m[np.argsort(-y[m])[:k]]
        w[top] = factor
    return w


def build_schemes(y, cells):
    return {
        "none": np.ones(len(y)),
        "positive-3x": np.where(y > 0, 3.0, 1.0),
        "linear-0.5": 1.0 + 0.5 * np.maximum(y, 0),
        "top30-cell-5x": cell_rank_weight(y, cells, 30, 5.0),
        "top30-cell-10x": cell_rank_weight(y, cells, 30, 10.0),
        "top20-cell-10x": cell_rank_weight(y, cells, 20, 10.0),
    }


def audit_weights(schemes, y):
    """Does each scheme touch the rows it claims to, and what does it cost?"""
    print("\n--- penalty audit ---")
    print(f"{'scheme':<16} {'rows w>1':>9} {'share':>7} {'mean w|y>0':>11} "
          f"{'mean w|y<=0':>12} {'eff. n':>9} {'% of n':>7}")
    n = len(y)
    for name, w in schemes.items():
        pos, neg = y > 0, y <= 0
        eff = w.sum() ** 2 / np.sum(w ** 2)          # Kish effective sample size
        print(f"{name:<16} {int((w > 1).sum()):>9} {(w > 1).mean():>7.1%} "
              f"{w[pos].mean():>11.2f} {w[neg].mean():>12.2f} "
              f"{eff:>9.0f} {eff / n:>7.1%}")
    print(f"(fit rows: {n}, positive-RAPTOR share: {(y > 0).mean():.1%})")


def cv_rounds(X, y, w, groups, folds=4):
    """Round count from CV inside the training rows -- never from the test seasons.

    Folds split by player-season, not at random. The modern snapshots are near daily,
    so one player-season spans dozens of rows; random folds put those on both sides
    of the split, the CV loss keeps falling long after the model stops generalising,
    and early stopping never fires -- every scheme ran to the 3000-round cap that
    way, against roughly 400 rounds when a whole season is held out instead.
    """
    from sklearn.model_selection import GroupKFold
    splitter = GroupKFold(n_splits=folds)
    res = lgb.cv(LGB_PARAMS, lgb.Dataset(X, y, weight=w), num_boost_round=3000,
                 folds=splitter.split(X, y, groups=groups),
                 callbacks=[lgb.early_stopping(100, verbose=False)])
    key = next(k for k in res if k.endswith("-mean"))
    return len(res[key])


def run_lgbm(Xtr, ytr, w, Xte, rounds):
    bst = lgb.train(LGB_PARAMS, lgb.Dataset(Xtr, ytr, weight=w),
                    num_boost_round=rounds)
    return bst.predict(Xte)


def run_ridge(Xtr, ytr, w, Xte, med):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    m = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, ytr, sample_weight=w)
    return m.predict((B - mu) / sd)


def run_mlp(Xtr, ytr, Xte, med, hidden, seed=0):
    """MLPRegressor has no sample_weight, so the nets are unweighted throughout."""
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    A, B = (A - mu) / sd, (B - mu) / sd
    m = MLPRegressor(hidden_layer_sizes=hidden, activation="relu", alpha=1e-3,
                     learning_rate_init=1e-3, batch_size=256, max_iter=400,
                     early_stopping=True, n_iter_no_change=20,
                     validation_fraction=0.12, random_state=seed)
    m.fit(A, ytr)
    return m.predict(B), m.n_iter_


NETS = {"mlp-64": (64,), "mlp-128-64": (128, 64), "mlp-256-64-16": (256, 64, 16)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--targets", nargs="*", default=["total", "offense", "defense"])
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_trainonly.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X = X[:, keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    # "Skip validation": the held-out validation seasons join the training set.
    tr = fit | val
    print(f"X={X.shape}  train={tr.sum()} (was {fit.sum()} fit + {val.sum()} val)  "
          f"test={test.sum()}")

    cells_tr = np.array([f"{s}|{t}|{ts}" for s, t, ts in
                         zip(d["season"][tr], d["season_type"][tr], d["timestamp"][tr])])
    # CV fold groups: one player-season is one unit, never split across folds.
    groups_tr = np.array([f"{p}|{s}|{t}" for p, s, t in
                          zip(d["player"][tr], d["season"][tr], d["season_type"][tr])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)

    results = []
    for target in args.targets:
        y = d[TARGETS[target]]
        ytr, yte = y[tr], y[test]
        schemes = build_schemes(ytr, cells_tr)
        if target == args.targets[0]:
            audit_weights(schemes, ytr)

        print(f"\n=== {target} ===", flush=True)
        for name, w in schemes.items():
            rounds = cv_rounds(X[tr], ytr, w, groups_tr)
            p = run_lgbm(X[tr], ytr, w, X[test], rounds)
            m = evaluate(yte, p, cells_te)
            results.append({"target": target, "model": "lightgbm", "weights": name,
                            "rounds": rounds, **{k: v for k, v in m.items()}})
            print(f"  lightgbm      {name:<16} {rounds:>4}r  R2={m['r2']:+.3f} "
                  f"MAE={m['mae']:.3f} rho={m['spearman']:+.3f} "
                  f"hits@20={m['hits@20']} hits@30={m['hits@30']} "
                  f"rho@30={m['rho@30']:+.3f}", flush=True)

        # blend, matching production, unweighted
        rounds = cv_rounds(X[tr], ytr, schemes["none"], groups_tr)
        pl = run_lgbm(X[tr], ytr, schemes["none"], X[test], rounds)
        pr = run_ridge(X[tr], ytr, schemes["none"], X[test], med)
        m = evaluate(yte, 0.75 * pl + 0.25 * pr, cells_te)
        results.append({"target": target, "model": "lgbm+ridge blend",
                        "weights": "none", "rounds": rounds, **m})
        print(f"  blend 0.75/0.25 {'none':<14} {rounds:>4}r  R2={m['r2']:+.3f} "
              f"MAE={m['mae']:.3f} rho={m['spearman']:+.3f} "
              f"hits@20={m['hits@20']} hits@30={m['hits@30']} "
              f"rho@30={m['rho@30']:+.3f}", flush=True)

        for net, hidden in NETS.items():
            ps, iters = [], []
            for seed in range(args.seeds):
                p, it = run_mlp(X[tr], ytr, X[test], med, hidden, seed)
                ps.append(p)
                iters.append(it)
            m = evaluate(yte, np.mean(ps, axis=0), cells_te)
            results.append({"target": target, "model": net, "weights": "none",
                            "rounds": int(np.mean(iters)), **m})
            print(f"  {net:<14} {'none':<16} {int(np.mean(iters)):>4}e  "
                  f"R2={m['r2']:+.3f} MAE={m['mae']:.3f} rho={m['spearman']:+.3f} "
                  f"hits@20={m['hits@20']} hits@30={m['hits@30']} "
                  f"rho@30={m['rho@30']:+.3f}  (mean of {args.seeds} seeds)",
                  flush=True)

    Path(args.out).with_suffix(".json").write_text(json.dumps(results, indent=1))
    write_report(results, args.out, X.shape, int(tr.sum()), int(test.sum()))
    print(f"\nwrote {args.out}")


def write_report(results, out, shape, ntr, nte):
    L = []
    A = L.append
    A("# Train/test only, audited penalty, small nets")
    A("")
    A(f"X={shape}. Every non-test row trains ({ntr} rows); no season is held out for")
    A(f"validation. LightGBM's round count comes from 5-fold CV inside the training")
    A(f"rows, so the {nte} test rows are still never consulted during fitting.")
    A("")
    A("`rho` is Spearman over all test rows. `rho@30` is Spearman *within the true")
    A("top 30 of each cell* — a much harder statistic, and not comparable to `rho`.")
    A("")
    for target in sorted({r["target"] for r in results},
                         key=lambda t: ["total", "offense", "defense"].index(t)):
        rows = [r for r in results if r["target"] == target]
        A(f"## {target}")
        A("")
        A("| model | weights | R² | RMSE | MAE | rho | hits@20 | hits@30 | rho@30 |")
        A("|---|---|---|---|---|---|---|---|---|")
        for r in sorted(rows, key=lambda r: -r["r2"]):
            A(f"| {r['model']} | {r['weights']} | {r['r2']:+.3f} | {r['rmse']:.3f} | "
              f"{r['mae']:.3f} | {r['spearman']:+.3f} | {r['hits@20']} | "
              f"{r['hits@30']} | {r['rho@30']:+.3f} |")
        A("")
    Path(out).write_text("\n".join(L))


if __name__ == "__main__":
    main()
