"""Sweep minimum-minutes thresholds for RAPTOR training rows.

Minutes mean different things in the two splits -- a whole playoff run is ~300
minutes, less than a fifth of a regular season -- so the thresholds are
season-type aware. A flat cutoff would delete nearly the entire playoff pool.

The threshold is chosen on validation. Test numbers for every rung are printed as
diagnostics, but only the validation-selected rung is a claim about test
performance.

Run:  python training/mp_sweep.py
"""

import argparse
import json
from pathlib import Path

import numpy as np
import lightgbm as lgb

from db import REPO_ROOT
from seasons import FULL_SEASON_SNAPSHOTS
from train_rapture import (LGB_PARAMS, ROW_FILTERS, VAL_SEASONS, add_context,
                           dedupe, load, metrics, normalize_rates)

# (regular-season minutes, playoff minutes). Playoff rungs are ~1/6 of the
# regular-season ones, matching the relative length of the two splits.
GRID = [(0, 0), (50, 10), (100, 20), (150, 25), (250, 40), (500, 80),
        (750, 120), (1000, 160), (1500, 250)]

# Non-minutes filter already selected for each model in train_rapture.
BASE_FILTER = {"box": "all", "onoff": "progress>=0.5", "combined": "all"}


def prepare(model, datadir):
    d = load(model, datadir)
    X, y, feat = d["X"], d["y"], list(d["feat_names"])
    is_test = d["test"].astype(bool)
    X = normalize_rates(X, feat, ~is_test)
    X, feat = add_context(X, feat, d, model)
    keep = dedupe(X, y, d)
    X, y = X[keep], y[keep]
    d = {k: (v[keep] if isinstance(v, np.ndarray) and v.shape[:1] == is_test.shape
             else v) for k, v in d.items()}
    return X, y, feat, d


def mp_mask(d, rs_thr, po_thr):
    rs = d["season_type"] == "Regular season"
    return np.where(rs, d["mp"] >= rs_thr, d["mp"] >= po_thr)


def run(model, datadir, outdir):
    print(f"\n{'='*96}\n{model.upper()}  (base filter: {BASE_FILTER[model]})\n{'='*96}")
    X, y, feat, d = prepare(model, datadir)
    is_test = d["test"].astype(bool)
    is_val = np.array([s in VAL_SEASONS and t in FULL_SEASON_SNAPSHOTS
                       for s, t in zip(d["season"], d["timestamp"])])
    is_fit = (~is_test) & (~is_val) & ROW_FILTERS[BASE_FILTER[model]](d)

    print(f"{'RS/PO min':<12} {'n fit':>7} {'val rmse':>9} {'val r2':>8} "
          f"| {'test rmse':>9} {'test r2':>8} {'test rho':>9} "
          f"| {'kept-test rmse':>14} {'test r2':>8} {'test rho':>9} {'n':>5}")
    print("-" * 96)

    rows, best = [], None
    for rs_thr, po_thr in GRID:
        keep = mp_mask(d, rs_thr, po_thr)
        fit = is_fit & keep
        val = is_val & keep
        if fit.sum() < 500 or val.sum() < 100:
            continue
        bst = lgb.train(LGB_PARAMS, lgb.Dataset(X[fit], y[fit]), num_boost_round=4000,
                        valid_sets=[lgb.Dataset(X[val], y[val])],
                        callbacks=[lgb.early_stopping(150, verbose=False)])
        vm = metrics(y[val], bst.predict(X[val], num_iteration=bst.best_iteration))

        # Refit on fit+val at the chosen round count, then score the test seasons.
        final = lgb.train(LGB_PARAMS, lgb.Dataset(X[fit | val], y[fit | val]),
                          num_boost_round=bst.best_iteration)
        pred_all = final.predict(X[is_test])
        tm_all = metrics(y[is_test], pred_all)                      # full test set
        kt = is_test & keep
        tm_kept = metrics(y[kt], final.predict(X[kt]))              # filtered test set

        rec = {"rs_min": rs_thr, "po_min": po_thr, "n_fit": int(fit.sum()),
               "rounds": bst.best_iteration, "val": vm,
               "test_full": tm_all, "test_filtered": tm_kept,
               "n_test_filtered": int(kt.sum())}
        rows.append(rec)
        print(f"{rs_thr:>5}/{po_thr:<6} {int(fit.sum()):>7} {vm['rmse']:>9.3f} "
              f"{vm['r2']:>+8.3f} | {tm_all['rmse']:>9.3f} {tm_all['r2']:>+8.3f} "
              f"{tm_all['spearman']:>+9.3f} | {tm_kept['rmse']:>14.3f} "
              f"{tm_kept['r2']:>+8.3f} {tm_kept['spearman']:>+9.3f} {kt.sum():>5}")
        if best is None or vm["rmse"] < best["val"]["rmse"]:
            best = rec

    print("-" * 96)
    print(f"validation picks RS>={best['rs_min']} / PO>={best['po_min']}  "
          f"-> test rmse={best['test_full']['rmse']:.3f} "
          f"r2={best['test_full']['r2']:+.3f} "
          f"spearman={best['test_full']['spearman']:+.3f} (full test set)")

    Path(outdir).mkdir(parents=True, exist_ok=True)
    json.dump({"grid": rows, "selected": best},
              open(Path(outdir) / f"{model}_mp_sweep.json", "w"), indent=2)
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", choices=["box", "onoff", "combined", "both", "all"],
                    default="both")
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data"))
    ap.add_argument("--outdir", default=str(REPO_ROOT / "training" / "models"))
    args = ap.parse_args()
    models = {"both": ["box", "onoff"],
              "all": ["box", "onoff", "combined"]}.get(args.model, [args.model])
    for m in models:
        run(m, args.datadir, args.outdir)


if __name__ == "__main__":
    main()
