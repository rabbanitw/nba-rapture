"""Ablation over --modern-stride, using one full stride-1 build.

Stride-k selection keeps every kth modern snapshot within each season, so every
stride is a subset of the stride-1 timestamp list. Building once at stride 1 and
subsetting in memory avoids about two hours of repeated Mongo pulls.

CAVEAT: this is NOT identical to rebuilding per stride. dedupe() runs over the
whole stride-1 set here, before the stride subset is taken; in the production
pipeline it runs after, over only the selected timestamps. Dedupe keeps the
latest of each byte-identical group, so pre-deduping a denser snapshot set drops
rows a direct build would have kept -- the stride-6 subset here has 10,933 fit
rows against 14,235 for a direct stride-6 build. Rankings WITHIN this sweep are
valid (one pipeline throughout); absolute numbers are not comparable to reports
built the production way.

Training rows are also filtered to MPG >= MPG_FLOOR. That is deliberately a low
floor: earlier work showed MPG >= 20 and >= 28 both cost accuracy, while removing
the token minutes filter entirely also cost accuracy. This drops genuine
garbage-time rows and nothing more.

Validation and test are the full-season snapshots, which are present at every
stride, so the eval sets are identical across the sweep.

Run:  python training/stride_ablation.py --datadir training/data_full
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from estimated_raptor import metrics
from seasons import FULL_SEASON_SNAPSHOTS, season_of
from train_rapture import (LGB_PARAMS, TARGETS, VAL_SEASONS, add_context, dedupe,
                           load, normalize_rates)

MODEL = "combined"
STRIDES = [1, 2, 3, 4, 6, 8, 12, 20]
MPG_FLOOR = 5.0
TARGETS_RUN = ["total", "offense", "defense"]

# Validation-selected in this ablation; see RESULTS_stride.md.
BEST_STRIDE = {"total": 3, "offense": 12, "defense": 3}


def stride_timestamps(all_ts, stride):
    """Every kth modern snapshot per season; full-season snapshots always kept."""
    hist = sorted(t for t in set(all_ts) if t in FULL_SEASON_SNAPSHOTS)
    modern = sorted(t for t in set(all_ts) if t not in FULL_SEASON_SNAPSHOTS)
    by_season = defaultdict(list)
    for t in modern:
        by_season[season_of(t)].append(t)
    keep = list(hist)
    for s in sorted(by_season, key=str):
        keep.extend(by_season[s][::stride])
    return set(keep)


def prepare(datadir):
    d = load(MODEL, datadir)
    X, feat = d["X"], list(d["feat_names"])
    idx = {n: i for i, n in enumerate(feat)}
    mins = X[:, idx["pbp|Minutes"]].astype(float)
    gp = X[:, idx["pbp|GamesPlayed"]].astype(float)
    mpg = np.divide(mins, gp, out=np.full_like(mins, np.nan), where=gp > 0)

    is_test = d["test"].astype(bool)
    X = normalize_rates(X, feat, ~is_test)
    X, feat = add_context(X, feat, d, MODEL)
    keep = dedupe(X, d["y"], d)
    X, mpg = X[keep], mpg[keep]
    d = {k: (v[keep] if isinstance(v, np.ndarray) and v.shape[:1] == is_test.shape
             else v) for k, v in d.items()}
    return X, feat, d, mpg


def fit_eval(X, y, fit, val, test):
    bst = lgb.train(LGB_PARAMS, lgb.Dataset(X[fit], y[fit]), num_boost_round=4000,
                    valid_sets=[lgb.Dataset(X[val], y[val])],
                    callbacks=[lgb.early_stopping(150, verbose=False)])
    rounds = bst.best_iteration
    vm = metrics(y[val], bst.predict(X[val], num_iteration=rounds))
    tr = fit | val
    final = lgb.train(LGB_PARAMS, lgb.Dataset(X[tr], y[tr]), num_boost_round=rounds)
    pred = final.predict(X[test])

    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(X[tr]), X[tr], med)
    B = np.where(np.isfinite(X[test]), X[test], med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    ridge = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, y[tr])
    rp = ridge.predict((B - mu) / sd)
    blend = 0.75 * pred + 0.25 * rp
    return vm, rounds, {"lgbm": metrics(y[test], pred),
                        "blend": metrics(y[test], blend)}, blend


def tuned_predictions(datadir=None, strides=None, mpg_floor=MPG_FLOOR):
    """Predictions for the held-out rows using each target's best stride.

    Shape matches compare_estimated_raptor.our_predictions so the leaderboard
    machinery can consume either.
    """
    datadir = datadir or (REPO_ROOT / "training" / "data_full")
    strides = strides or BEST_STRIDE
    X, feat, d, mpg = prepare(datadir)
    is_test = d["test"].astype(bool)
    is_val = np.array([s in VAL_SEASONS and t in FULL_SEASON_SNAPSHOTS
                       for s, t in zip(d["season"], d["timestamp"])])
    base_fit = ((~is_test) & (~is_val)
                & np.isfinite(mpg) & (mpg >= mpg_floor))
    all_ts = set(d["timestamp"])

    out = pd.DataFrame({"player": d["player"][is_test], "season": d["season"][is_test],
                        "split": d["season_type"][is_test], "mp": d["mp"][is_test]})
    for target, stride in strides.items():
        keep_ts = stride_timestamps(all_ts, stride)
        fit = base_fit & np.array([t in keep_ts for t in d["timestamp"]])
        _, rounds, _, blend = fit_eval(X, d[TARGETS[target]], fit, is_val, is_test)
        out[f"ours_{target}"] = blend
        out[f"truth_{target}"] = d[TARGETS[target]][is_test]
        print(f"  {target:<8} stride={stride:<3} fit={fit.sum():>6,} rounds={rounds}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_full"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_stride.md"))
    args = ap.parse_args()

    X, feat, d, mpg = prepare(args.datadir)
    is_test = d["test"].astype(bool)
    is_val = np.array([s in VAL_SEASONS and t in FULL_SEASON_SNAPSHOTS
                       for s, t in zip(d["season"], d["timestamp"])])
    enough = np.isfinite(mpg) & (mpg >= MPG_FLOOR)
    base_fit = (~is_test) & (~is_val) & enough
    print(f"rows={len(mpg):,}  fit_pool={base_fit.sum():,}  val={is_val.sum()}  "
          f"test={is_test.sum()}")
    print(f"MPG>={MPG_FLOOR} drops {int((((~is_test)&(~is_val)) & ~enough).sum()):,} "
          "training rows")

    all_ts = set(d["timestamp"])
    results = []
    for stride in STRIDES:
        keep_ts = stride_timestamps(all_ts, stride)
        in_stride = np.array([t in keep_ts for t in d["timestamp"]])
        fit = base_fit & in_stride
        n_modern_ts = len({t for t in keep_ts if t not in FULL_SEASON_SNAPSHOTS})
        rec = {"stride": stride, "modern_timestamps": n_modern_ts,
               "fit_rows": int(fit.sum())}
        for target in TARGETS_RUN:
            y = d[TARGETS[target]]
            vm, rounds, tm, _ = fit_eval(X, y, fit, is_val, is_test)
            rec[target] = {"val": vm, "rounds": rounds, **tm}
        results.append(rec)
        print(f"  stride {stride:>2}  ts={n_modern_ts:>3}  fit={fit.sum():>6,}  "
              + "  ".join(f"{t[:3]} val_rmse={rec[t]['val']['rmse']:.3f} "
                          f"test_r2={rec[t]['blend']['r2']:+.3f}"
                          for t in TARGETS_RUN))

    write_report(args.out, results, int(is_val.sum()), int(is_test.sum()),
                 int((((~is_test) & (~is_val)) & ~enough).sum()))
    json.dump(results, open(Path(args.out).with_suffix(".json"), "w"), indent=2)
    print(f"\nwrote {args.out}")


def write_report(path, results, n_val, n_test, n_dropped):
    L = []
    A = L.append
    A("# Stride ablation\n")
    A("`--modern-stride k` keeps every kth modern (2020-21 onward) snapshot within")
    A("each season. The six full-season snapshots — our two test seasons and four")
    A("validation seasons — are kept at every stride, so **validation and test are")
    A(f"identical across the sweep** ({n_val} and {n_test} rows).\n")
    A(f"Training rows are additionally filtered to **MPG ≥ {MPG_FLOOR:.0f}**, which")
    A(f"drops {n_dropped:,} garbage-time rows. That floor is deliberately low:")
    A("MPG ≥ 20 and ≥ 28 both cost accuracy in earlier runs, and so did removing the")
    A("minutes filter altogether, so only near-zero rows are excluded here.\n")
    A("Built once at stride 1 and subset in memory. **This is not identical to")
    A("rebuilding per stride**: dedupe runs over the whole stride-1 set here but")
    A("after timestamp selection in the production pipeline, so these rows are a")
    A("little sparser than a direct build at the same stride. Rankings within this")
    A("table are valid; absolute values are not comparable to other reports.\n")

    for target in TARGETS_RUN:
        A(f"## {target}\n")
        A("| stride | modern snapshots | fit rows | rounds | val RMSE | test RMSE | test R² | test ρ |")
        A("|---|---|---|---|---|---|---|---|")
        best = min(results, key=lambda r: r[target]["val"]["rmse"])
        for r in results:
            t = r[target]
            star = " ⬅" if r is best else ""
            A(f"| {r['stride']} | {r['modern_timestamps']} | {r['fit_rows']:,} | "
              f"{t['rounds']} | {t['val']['rmse']:.3f}{star} | "
              f"{t['blend']['rmse']:.3f} | {t['blend']['r2']:+.3f} | "
              f"{t['blend']['spearman']:+.3f} |")
        A("")
        A(f"Validation picks **stride {best['stride']}** "
          f"({best['fit_rows']:,} fit rows) → test R² "
          f"{best[target]['blend']['r2']:+.3f}, ρ "
          f"{best[target]['blend']['spearman']:+.3f}.\n")

    A("## Reading it\n")
    s6 = next(r for r in results if r["stride"] == 6)
    s1 = next(r for r in results if r["stride"] == 1)
    A("| target | stride 6 (previous default) | stride 1 (all snapshots) | Δ test R² |")
    A("|---|---|---|---|")
    for target in TARGETS_RUN:
        a, b = s6[target]["blend"]["r2"], s1[target]["blend"]["r2"]
        A(f"| {target} | {a:+.3f} | {b:+.3f} | {b - a:+.3f} |")
    A("")
    Path(path).write_text("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
