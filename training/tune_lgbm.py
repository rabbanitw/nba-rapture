"""Random search for the lowest-MAE model, scored by grouped CV on the training rows.

Three things this does that the existing LGB_PARAMS did not:

1. Searches the objective, not just the tree shape. If the model is going to be
   selected on MAE then L1 is the objective that actually optimises it -- L2 fits the
   conditional mean and is pulled around by the tails, which for RAPTOR are large and
   one-sided. Huber is in the search as the compromise.

2. Scores on MAE regardless of objective, so every configuration is compared on the
   number that will be used to pick the winner.

3. Splits CV folds by player-season. Random folds put near-identical daily snapshots
   of the same player on both sides of the split, so CV loss keeps falling long after
   the model has stopped generalising -- with random folds every configuration ran to
   the 3000-round cap. Grouped folds land at 150-800.

The test seasons are never touched here. Everything is chosen inside the training
rows and applied once, afterwards.

Run:  python training/tune_lgbm.py --target total --trials 30
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare, splits
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
FOLDS = 3
# Grouped folds early-stop between 150 and 800 rounds, so a higher cap only burns time.
MAX_ROUNDS = 1200


def sample_params(rng):
    obj = rng.choice(["l2", "l1", "huber"])
    p = {
        "objective": obj,
        "metric": "l1",                       # always judged on MAE
        "learning_rate": float(rng.choice([0.02, 0.03, 0.05, 0.08])),
        "num_leaves": int(rng.choice([15, 31, 63, 127])),
        "min_data_in_leaf": int(rng.choice([10, 20, 40, 80])),
        "feature_fraction": float(rng.choice([0.2, 0.3, 0.5, 0.7])),
        "bagging_fraction": float(rng.choice([0.6, 0.8, 1.0])),
        "bagging_freq": 1,
        "lambda_l1": float(rng.choice([0.0, 0.5, 2.0])),
        "lambda_l2": float(rng.choice([0.0, 1.0, 5.0, 20.0])),
        "verbose": -1,
        "seed": 42,
        "num_threads": 0,
    }
    if obj == "huber":
        p["alpha"] = float(rng.choice([1.0, 2.0, 5.0]))
    return p


def cv_mae(X, y, groups, params):
    splitter = GroupKFold(n_splits=FOLDS)
    res = lgb.cv(params, lgb.Dataset(X, y), num_boost_round=MAX_ROUNDS,
                 folds=splitter.split(X, y, groups=groups),
                 callbacks=[lgb.early_stopping(100, verbose=False)])
    key = next(k for k in res if k.endswith("-mean"))
    return float(res[key][-1]), len(res[key])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--target", default="total")

    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X = X[:, keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    tr = fit | val                            # no season held out; see RESULTS_trainonly
    y = d[TARGETS[args.target]]
    groups = np.array([f"{p}|{s}|{t}" for p, s, t in
                       zip(d["player"][tr], d["season"][tr], d["season_type"][tr])])
    Xtr, ytr = X[tr], y[tr]
    print(f"[{args.target}] tuning on {Xtr.shape[0]} rows, "
          f"{len(np.unique(groups))} player-season groups")

    # Current production settings, as the thing to beat.
    from train_rapture import LGB_PARAMS
    base = dict(LGB_PARAMS, metric="l1")
    b_mae, b_rounds = cv_mae(Xtr, ytr, groups, base)
    print(f"  baseline (l2, current LGB_PARAMS): CV MAE={b_mae:.4f} "
          f"rounds={b_rounds}", flush=True)

    trials = []
    best = {"cv_mae": b_mae, "rounds": b_rounds, "params": base, "tag": "baseline-l2"}

    # Stage 1: the objective, at the current tree settings. This is the change most
    # likely to matter when the metric is MAE -- L1 optimises it directly, while L2
    # fits the conditional mean and gets pulled by RAPTOR's long positive tail.
    print("  -- stage 1: objective --", flush=True)
    for obj in ("l1", "huber"):
        p = dict(base, objective=obj)
        if obj == "huber":
            p["alpha"] = 2.0
        mae, rounds = cv_mae(Xtr, ytr, groups, p)
        trials.append({"stage": 1, "cv_mae": mae, "rounds": rounds, "params": p})
        flag = ""
        if mae < best["cv_mae"]:
            best = {"cv_mae": mae, "rounds": rounds, "params": p, "tag": f"obj-{obj}"}
            flag = "  <-- best"
        print(f"    objective={obj:<6} CV MAE={mae:.4f} r={rounds:<5}{flag}",
              flush=True)

    # Stage 2: tree shape and regularisation, around whichever objective won. One
    # factor at a time rather than random draws -- with a handful of runs affordable,
    # knowing which knob moved the number is worth more than covering the space.
    print("  -- stage 2: tree shape around the winning objective --", flush=True)
    stage1_best = dict(best["params"])
    for i, over in enumerate([
            {"num_leaves": 15}, {"num_leaves": 63}, {"num_leaves": 127},
            {"min_data_in_leaf": 20}, {"min_data_in_leaf": 80},
            {"feature_fraction": 0.3}, {"feature_fraction": 0.7},
            {"learning_rate": 0.05}, {"lambda_l2": 20.0}, {"bagging_fraction": 1.0}]):
        p = dict(stage1_best, **over)
        mae, rounds = cv_mae(Xtr, ytr, groups, p)
        trials.append({"stage": 2, "cv_mae": mae, "rounds": rounds, "params": p})
        flag = ""
        if mae < best["cv_mae"]:
            best = {"cv_mae": mae, "rounds": rounds, "params": p, "tag": f"stage2-{i}"}
            flag = "  <-- best"
        print(f"    {str(over):<32} CV MAE={mae:.4f} r={rounds:<5}{flag}", flush=True)

    out = Path(args.out)
    all_best = json.loads(out.read_text()) if out.exists() else {}
    all_best[args.target] = best
    out.write_text(json.dumps(all_best, indent=1))
    print(f"\n[{args.target}] best CV MAE={best['cv_mae']:.4f} "
          f"({best['tag']}, {best['params']['objective']}), "
          f"baseline was {b_mae:.4f} -> "
          f"{100 * (b_mae - best['cv_mae']) / b_mae:+.1f}%")
    Path(str(out).replace(".json", f"_{args.target}_trials.json")).write_text(
        json.dumps(trials, indent=1))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
