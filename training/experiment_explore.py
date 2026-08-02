"""Broad exploration: feature dropout, more architectures, gap-inflating labels.

All judged on dev@10 -- mean |true rank - projected position| over the projected top
ten -- because that is the stated selection criterion, with dev@20, tau and MAE
alongside so a win at ten that costs everything else is visible.

The three lines of attack, and why each might move a metric that weighting, ranking
objectives, hyperparameter search and cascades all failed to move:

  feature dropout   1,140 features against 13,885 rows is a wide matrix. If a chunk
                    of it is noise, removing it lowers variance, and variance is the
                    binding constraint when true adjacent-rank gaps (0.04-0.12 RAPTOR)
                    sit far below model error (0.5-0.9). Tried two ways: keeping the
                    top N columns by gain, and ablating whole source blocks.

  architectures     XGBoost as a second GBM implementation, LightGBM's DART (dropout
                    between trees, a different regulariser), and linear_tree (linear
                    models in the leaves, which fits smooth relationships that
                    axis-aligned constants approximate badly).

  gap inflation     the diagnosis said the top of the board is noise-limited because
                    adjacent ranks differ by less than the model's error. A monotone
                    transform that stretches the top makes those differences larger in
                    the target, so squared error spends more of itself resolving them.
                    Unlike sample weighting this changes the geometry rather than the
                    row budget, so it is a genuinely different lever from the one that
                    already failed.

Run:  python training/experiment_explore.py --targets total defense
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import xgboost as xgb
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10


def within_cell_rank_target(y, cells, fn):
    out = np.empty(len(y), dtype=np.float64)
    for c in np.unique(cells):
        m = np.where(cells == c)[0]
        r = ranks(y[m])
        out[m] = fn(r, len(m))
    return out


def gap_labels(y, cells):
    """Monotone transforms that widen the spacing near the top of each cell."""
    return {
        "raptor": y.astype(np.float64),
        # power transforms stretch the tails in RAPTOR space
        "pow1.5": np.sign(y) * np.abs(y) ** 1.5,
        "pow2": np.sign(y) * np.abs(y) ** 2.0,
        # rank space: both give the top few ranks far more room than the middle
        "neg_log_rank": within_cell_rank_target(
            y, cells, lambda r, n: -np.log1p(r)),
        "exp_rank": within_cell_rank_target(
            y, cells, lambda r, n: np.exp(-r / 12.0)),
    }


def fit_lgbm(Xtr, ttr, Xte, params, rounds, seed=0, **over):
    p = dict(params, seed=seed, bagging_seed=seed, feature_fraction_seed=seed, **over)
    return lgb.train(p, lgb.Dataset(Xtr, ttr), num_boost_round=rounds).predict(Xte)


def fit_xgb(Xtr, ttr, Xte, rounds, seed=0):
    m = xgb.XGBRegressor(n_estimators=rounds, learning_rate=0.03, max_leaves=15,
                         grow_policy="lossguide", subsample=0.8,
                         colsample_bytree=0.5, reg_lambda=5.0, min_child_weight=40,
                         random_state=seed, n_jobs=0, tree_method="hist")
    m.fit(Xtr, ttr)
    return m.predict(Xte)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--targets", nargs="*", default=["total", "defense"])
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_explore.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    cells_tr = np.array([f"{t}|{s}" for t, s in
                         zip(d["timestamp"][tr], d["season_type"][tr])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(X[tr]), X[tr], med)
    B = np.where(np.isfinite(X[test]), X[test], med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    An, Bn = (A - mu) / sd, (B - mu) / sd
    tuned = json.loads(Path(args.tuned).read_text())
    print(f"X={X.shape} train={tr.sum()} test={test.sum()}", flush=True)

    blocks = sorted({n.split("|", 1)[0] for n in feat})
    groups = {"pbp": ["pbp"], "wowy_on": ["wowy_on"], "wowy_off": ["wowy_off"],
              "wowy_diff": ["wowy_diff"], "ctx": ["ctx"],
              "tracking": [b for b in blocks if b.startswith("track:")]}

    rows = []
    for target in args.targets:
        y = d[TARGETS[target]]
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = tuned[target]["rounds"]
        ridge = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(An, y[tr]).predict(Bn)

        def ens(Xs, ttr, Xte, use_ridge=True, **kw):
            ps = [fit_lgbm(Xs, ttr, Xte, params, rounds, s, **kw)
                  for s in range(args.seeds)]
            p = np.mean(ps, axis=0)
            return 0.75 * p + 0.25 * ridge if use_ridge else p

        def record(name, kind, p):
            s = score_cells(y[test], p, cells_te)
            rows.append({"target": target, "kind": kind, "name": name, **s})
            print(f"  {kind:<11} {name:<18} dev@10={s['dev@10']:5.2f} "
                  f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
                  f"tau@20={s['tau@20']:+.3f} MAE={s['mae']:.3f} "
                  f"hits@10={s['hits@10']}/20", flush=True)

        print(f"\n=== {target} ===", flush=True)
        base = ens(X[tr], y[tr], X[test])
        record("all 1140", "baseline", base)

        # ---- feature dropout by importance ---------------------------------
        imp = lgb.train(params, lgb.Dataset(X[tr], y[tr]),
                        num_boost_round=rounds).feature_importance("gain")
        order = np.argsort(-imp)
        for n_keep in (50, 100, 200, 400, 800):
            cols = np.sort(order[:n_keep])
            record(f"top {n_keep} by gain", "dropout",
                   ens(X[tr][:, cols], y[tr], X[test][:, cols]))

        # ---- block ablation -------------------------------------------------
        for gname, gblocks in groups.items():
            m = np.array([n.split("|", 1)[0] not in gblocks for n in feat])
            record(f"without {gname}", "ablation",
                   ens(X[tr][:, m], y[tr], X[test][:, m]))

        # ---- architectures ---------------------------------------------------
        record("xgboost", "architecture",
               0.75 * np.mean([fit_xgb(X[tr], y[tr], X[test], rounds, s)
                               for s in range(args.seeds)], axis=0) + 0.25 * ridge)
        record("lgbm dart", "architecture",
               ens(X[tr], y[tr], X[test], boosting="dart", drop_rate=0.1))
        record("lgbm linear_tree", "architecture",
               ens(A, y[tr], B, linear_tree=True))

        # ---- gap-inflating labels --------------------------------------------
        for name, ttr in gap_labels(y[tr], cells_tr).items():
            if name == "raptor":
                continue
            record(name, "gap label", ens(X[tr], ttr, X[test], use_ridge=False))

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    write_report(rows, args.out)
    print(f"\nwrote {args.out}")


def write_report(rows, out):
    L = []
    A = L.append
    A("# Exploration: feature dropout, architectures, gap-inflating labels")
    A("")
    A("Regular season only, all features unless stated, tuned params, "
      "seed-averaged. Ranked by `dev@10` — mean |true rank − projected position| "
      "over the projected top ten. Lower is better.")
    A("")
    for target in sorted({r["target"] for r in rows}):
        A(f"## {target}")
        A("")
        A("| kind | variant | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 |")
        A("|---|---|---:|---:|---:|---:|---:|---:|")
        for r in sorted([x for x in rows if x["target"] == target],
                        key=lambda r: r["dev@10"]):
            A(f"| {r['kind']} | {r['name']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
              f"{r['tau@10']:+.3f} | {r['tau@20']:+.3f} | {r['mae']:.3f} | "
              f"{r['hits@10']}/20 |")
        A("")
    Path(out).write_text("\n".join(L))


if __name__ == "__main__":
    main()
