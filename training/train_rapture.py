"""Train and evaluate RAPTOR predictors on the built matrices.

Two independent models, matching how 538 defines the metric:
  box    stats (play-by-play + player tracking) -> rap_box
  onoff  stats (wowy on / off / on-off diff)    -> rap_onoff

RAPTOR is points above average per 100 possessions, so raw season-to-date counting
stats are normalized to per-100-possessions (or per-36-minutes for tracking) before
training. Without that, a January snapshot and an April snapshot of the same player
look like completely different inputs while carrying nearly the same label.

Run:  python training/train_rapture.py --model both
"""

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr
from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


import lightgbm as lgb

from db import REPO_ROOT
from seasons import FULL_SEASON_SNAPSHOTS, season_progress

POS_COLS = ["PG", "SG", "SF", "PF", "C"]

# The other four full-season snapshots. They have the same shape as the test
# seasons (one whole-season row per player), so they are the right place to choose
# hyperparameters and row filters -- the test seasons are never looked at.
VAL_SEASONS = ("2015-16", "2016-17", "2017-18", "2019-20")

# Minimum minutes for a *training* row, by split. A whole playoff run is ~300
# minutes so the thresholds cannot be flat. These are deliberately token values:
# mp_sweep.py shows accuracy falls monotonically above them (see training/README).
# They only drop degenerate rows -- a 20-minute season carries no signal -- and
# leave every validation and test row in place.
MIN_MP = {"Regular season": 50, "Playoffs": 10}

# 538 splits each component into offense and defense. The total is o + d up to
# the scrape's 1-decimal rounding, so "sum the two part models" is a real
# alternative to modelling the total directly -- main() reports both.
TARGETS = {"total": "y", "offense": "y_off", "defense": "y_def"}

# Candidate training-row filters. Modern snapshots are season-to-date, so an early
# January row pairs a tiny sample with a wild RAPTOR value; these ask whether
# dropping that noise helps. Chosen on validation.
ROW_FILTERS = {
    "all": lambda d: np.ones(len(d["mp"]), dtype=bool),
    "mp>=500": lambda d: d["mp"] >= 500,
    "progress>=0.5": lambda d: np.array([season_progress(t) >= 0.5
                                         for t in d["timestamp"]]),
    "mp>=500 & progress>=0.5": lambda d: (d["mp"] >= 500) & np.array(
        [season_progress(t) >= 0.5 for t in d["timestamp"]]),
}

LGB_PARAMS = dict(objective="l2", learning_rate=0.03, num_leaves=31,
                  min_data_in_leaf=40, feature_fraction=0.5, bagging_fraction=0.8,
                  bagging_freq=1, lambda_l2=5.0, verbose=-1, seed=42, num_threads=0)

# Per-block denominator used to turn counting stats into rates.
ANCHORS = {"pbp": "TotalPoss", "wowy_on": "TotalPoss", "wowy_off": "TotalPoss"}
TRACK_ANCHOR = "MIN"
ANCHOR_SCALE = {"poss": 100.0, "min": 36.0}

# Names that are already rates -- never divide these by the anchor.
RATE_RE = re.compile(
    r"(pct|%|accuracy|frequency|avg|average|perposs|per_poss|rating|pace|"
    r"ratio|share|rate|quality)", re.I)
# Context fields worth keeping raw (they describe sample size, not production).
KEEP_RAW = {"TotalPoss", "OffPoss", "DefPoss", "Minutes", "SecondsPlayed",
            "GamesPlayed", "MIN", "GP", "W", "L"}


def load(model, datadir):
    d = np.load(Path(datadir) / f"{model}.npz", allow_pickle=True)
    return {k: d[k] for k in d.files}


def block_of(name):
    return name.split("|", 1)[0]


def stat_of(name):
    return name.split("|", 1)[1]


def normalize_rates(X, feat_names, is_train, log_path=None):
    """Divide volume stats by their block's possession/minute anchor.

    A column is treated as volume if it is non-negative and correlates >= 0.75 with
    its anchor across training rows, unless its name says it is already a rate.
    """
    X = X.copy()
    idx = {n: i for i, n in enumerate(feat_names)}
    by_block = defaultdict(list)
    for n in feat_names:
        by_block[block_of(n)].append(n)

    scaled, kept = [], []
    for block, names in by_block.items():
        anchor_stat = TRACK_ANCHOR if block.startswith("track:") else ANCHORS.get(block)
        scale = ANCHOR_SCALE["min"] if block.startswith("track:") else ANCHOR_SCALE["poss"]
        akey = f"{block}|{anchor_stat}"
        if anchor_stat is None or akey not in idx:
            kept.extend(names)
            continue
        a = X[:, idx[akey]].astype(np.float64)
        valid_a = np.isfinite(a) & (a > 0)
        for n in names:
            s = stat_of(n)
            j = idx[n]
            if n == akey or s in KEEP_RAW or RATE_RE.search(s):
                kept.append(n)
                continue
            col = X[:, j].astype(np.float64)
            m = valid_a & np.isfinite(col) & is_train
            if m.sum() < 50 or np.nanmin(col[m]) < 0:
                kept.append(n)
                continue
            sd_c, sd_a = np.std(col[m]), np.std(a[m])
            corr = 0.0 if sd_c == 0 or sd_a == 0 else np.corrcoef(col[m], a[m])[0, 1]
            if corr >= 0.75:
                out = np.full(X.shape[0], np.nan)
                np.divide(col, a, out=out, where=valid_a)
                X[:, j] = (out * scale).astype(np.float32)
                scaled.append(n)
            else:
                kept.append(n)

    print(f"  rate-normalized {len(scaled)} / {len(feat_names)} columns "
          f"({len(kept)} left raw)")
    if log_path:
        json.dump({"normalized": sorted(scaled), "raw": sorted(kept)},
                  open(log_path, "w"), indent=1)
    return X


def add_context(X, feat_names, d, model):
    """Append position one-hot, minutes, season progress, and (on - off) for the on/off model."""
    extra, extra_names = [], []

    pos = np.zeros((X.shape[0], len(POS_COLS)), dtype=np.float32)
    for i, p in enumerate(d["pos"]):
        toks = {t.strip().upper() for t in str(p or "").split(",") if t.strip()}
        for j, c in enumerate(POS_COLS):
            pos[i, j] = 1.0 if c in toks else 0.0
    extra.append(pos)
    extra_names += [f"ctx|pos_{c}" for c in POS_COLS]

    mp = d["mp"].astype(np.float32).reshape(-1, 1)
    prog = np.array([season_progress(t) for t in d["timestamp"]],
                    dtype=np.float32).reshape(-1, 1)
    playoffs = (d["season_type"] == "Playoffs").astype(np.float32).reshape(-1, 1)
    extra += [mp, prog, playoffs]
    extra_names += ["ctx|mp", "ctx|season_progress", "ctx|is_playoffs"]

    # on-minus-off differentials whenever both wowy blocks are present
    if any(block_of(n) == "wowy_on" for n in feat_names):
        idx = {n: i for i, n in enumerate(feat_names)}
        on = [n for n in feat_names if block_of(n) == "wowy_on"]
        diff, diff_names = [], []
        for n in on:
            s = stat_of(n)
            off = f"wowy_off|{s}"
            if off in idx:
                diff.append(X[:, idx[n]] - X[:, idx[off]])
                diff_names.append(f"wowy_diff|{s}")
        if diff:
            extra.append(np.vstack(diff).T.astype(np.float32))
            extra_names += diff_names
            print(f"  added {len(diff_names)} on-minus-off differential features")

    return np.hstack([X] + extra).astype(np.float32), feat_names + extra_names


def dedupe(X, y, d):
    """Collapse snapshots of the same player-season whose stats are byte-identical.

    Adjacent wayback snapshots with no games played between them produce the same
    feature vector; keeping them inflates the effective sample size and lets the
    same observation land in both train and validation.
    """
    seen, keep = set(), []
    order = np.argsort(d["timestamp"])  # keep the latest identical snapshot
    for i in order[::-1]:
        key = (str(d["player"][i]), str(d["season"][i]), str(d["season_type"][i]),
               hash(np.round(np.nan_to_num(X[i], nan=-9e9), 4).tobytes()))
        if key in seen:
            continue
        seen.add(key)
        keep.append(i)
    keep = np.sort(np.array(keep))
    print(f"  dedupe: {X.shape[0]} -> {len(keep)} rows "
          f"({X.shape[0]-len(keep)} exact duplicates dropped)")
    return keep


def metrics(y, p):
    sp = float("nan") if np.std(p) == 0 else float(spearmanr(y, p).statistic)
    return {"rmse": float(np.sqrt(mean_squared_error(y, p))),
            "mae": float(mean_absolute_error(y, p)),
            "r2": float(r2_score(y, p)),
            "spearman": sp}


def run(model, datadir, outdir, target="total", seed=42):
    print(f"\n{'='*78}\n{model.upper()}  target={target}\n{'='*78}")
    d = load(model, datadir)
    X, y = d["X"], d[TARGETS[target]]
    feat_names = list(d["feat_names"])
    print(f"  loaded X={X.shape}")

    is_test = d["test"].astype(bool)
    X = normalize_rates(X, feat_names, ~is_test,
                        log_path=Path(outdir) / f"{model}_normalization.json")
    X, feat_names = add_context(X, feat_names, d, model)

    keep = dedupe(X, d["y"], d)
    X, y = X[keep], y[keep]
    d = {k: (v[keep] if isinstance(v, np.ndarray) and v.shape[:1] == is_test.shape else v)
         for k, v in d.items()}
    is_test = d["test"].astype(bool)

    # Validation = the other full-season snapshots, which match the test set's
    # shape. Everything else (modern season-to-date snapshots) is the fit pool.
    is_val = np.array([s in VAL_SEASONS and t in FULL_SEASON_SNAPSHOTS
                       for s, t in zip(d["season"], d["timestamp"])])
    min_mp = np.array([MIN_MP.get(s, 0) for s in d["season_type"]], dtype=np.float32)
    enough_mp = d["mp"] >= min_mp
    is_fit = (~is_test) & (~is_val) & enough_mp
    print(f"  min-minutes filter on training rows {MIN_MP}: dropped "
          f"{int(((~is_test) & (~is_val) & ~enough_mp).sum())} rows")
    Xva, yva = X[is_val], y[is_val]
    Xte, yte = X[is_test], y[is_test]
    print(f"  fit={int(is_fit.sum())}  val={len(yva)} {sorted(set(d['season'][is_val]))}"
          f"  test={len(yte)} {sorted(set(d['season'][is_test]))}")
    print(f"  label {model}: fit sd={y[is_fit].std():.2f}  val sd={yva.std():.2f}  "
          f"test sd={yte.std():.2f}")

    params = dict(LGB_PARAMS, seed=seed)

    # --- pick the training-row filter and round count on validation only ----
    print("  selecting training-row filter on validation:")
    chosen, best_val, n_rounds = None, None, 500
    for fname, fn in ROW_FILTERS.items():
        m = is_fit & fn(d)
        if m.sum() < 500:
            continue
        bst = lgb.train(params, lgb.Dataset(X[m], y[m]), num_boost_round=4000,
                        valid_sets=[lgb.Dataset(Xva, yva)],
                        callbacks=[lgb.early_stopping(150, verbose=False)])
        vm = metrics(yva, bst.predict(Xva, num_iteration=bst.best_iteration))
        print(f"    {fname:<26} n={int(m.sum()):<6} rounds={bst.best_iteration:<5} "
              f"val rmse={vm['rmse']:.3f}  r2={vm['r2']:+.3f}  "
              f"spearman={vm['spearman']:+.3f}")
        if best_val is None or vm["rmse"] < best_val["rmse"]:
            chosen, best_val, n_rounds = fname, vm, bst.best_iteration
    print(f"  -> chose '{chosen}' ({n_rounds} rounds), val rmse={best_val['rmse']:.3f}")

    # Refit on the chosen fit rows plus validation, then score the test seasons once.
    fit_mask = (is_fit & ROW_FILTERS[chosen](d)) | is_val
    Xtr, ytr = X[fit_mask], y[fit_mask]

    results = {"validation_lgbm": best_val}
    results["mean_baseline"] = metrics(yte, np.full_like(yte, ytr.mean()))

    med = np.nanmedian(Xtr, axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    Xtr_i = np.where(np.isfinite(Xtr), Xtr, med)
    Xte_i = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = Xtr_i.mean(0), Xtr_i.std(0)
    sd[sd == 0] = 1.0
    ridge = RidgeCV(alphas=np.logspace(-2, 4, 25))
    ridge.fit((Xtr_i - mu) / sd, ytr)
    ridge_pred = ridge.predict((Xte_i - mu) / sd)
    results["ridge"] = metrics(yte, ridge_pred)

    final = lgb.train(params, lgb.Dataset(Xtr, ytr), num_boost_round=n_rounds)
    pred = final.predict(Xte)
    results["lgbm"] = metrics(yte, pred)
    results["blend_lgbm_ridge"] = metrics(yte, 0.75 * pred + 0.25 * ridge_pred)

    # --- per-slice test breakdown -----------------------------------------
    slices = {}
    for season in sorted(set(d["season"][is_test])):
        for st in sorted(set(d["season_type"][is_test])):
            m = (d["season"][is_test] == season) & (d["season_type"][is_test] == st)
            if m.sum() >= 10:
                slices[f"{season} {st}"] = {"n": int(m.sum()),
                                            **metrics(yte[m], pred[m])}

    imp = sorted(zip(feat_names, final.feature_importance("gain")),
                 key=lambda kv: -kv[1])
    print("\n  top features by gain:")
    for n, g in imp[:15]:
        print(f"    {n:<52} {g:12.1f}")

    print("\n  TEST RESULTS (held-out 2013-14 + 2014-15)")
    for k, v in results.items():
        print(f"    {k:<20} rmse={v['rmse']:.3f}  mae={v['mae']:.3f}  "
              f"r2={v['r2']:+.3f}  spearman={v['spearman']:+.3f}")
    print("\n  by slice (LightGBM):")
    for k, v in slices.items():
        print(f"    {k:<28} n={v['n']:<4} rmse={v['rmse']:.3f}  r2={v['r2']:+.3f}  "
              f"spearman={v['spearman']:+.3f}")

    Path(outdir).mkdir(parents=True, exist_ok=True)
    tag = model if target == "total" else f"{model}_{target}"
    final.save_model(str(Path(outdir) / f"{tag}_lgbm.txt"))
    json.dump({"results": results, "slices": slices, "n_rounds": n_rounds, "row_filter": chosen,
               "n_train": int(len(ytr)), "n_test": int(len(yte)),
               "top_features": [[n, float(g)] for n, g in imp[:60]]},
              open(Path(outdir) / f"{tag}_results.json", "w"), indent=2)
    return {"results": results, "pred": pred, "blend": 0.75 * pred + 0.25 * ridge_pred,
            "yte": yte, "season": d["season"][is_test],
            "season_type": d["season_type"][is_test]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", choices=["box", "onoff", "combined", "both", "all"],
                    default="both")
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data"))
    ap.add_argument("--outdir", default=str(REPO_ROOT / "training" / "models"))
    ap.add_argument("--targets", nargs="+", default=["total", "offense", "defense"],
                    choices=["total", "offense", "defense"])
    args = ap.parse_args()
    Path(args.outdir).mkdir(parents=True, exist_ok=True)
    models = {"both": ["box", "onoff"],
              "all": ["box", "onoff", "combined"]}.get(args.model, [args.model])
    for m in models:
        out = {t: run(m, args.datadir, args.outdir, target=t) for t in args.targets}
        if {"total", "offense", "defense"} <= set(out):
            compare_sum_vs_total(m, out, args.outdir)


def compare_sum_vs_total(model, out, outdir):
    """Does predicting offense and defense separately beat predicting the total?"""
    yt = out["total"]["yte"]
    print(f"\n{'='*78}\n{model.upper()}  offense + defense  vs  direct total\n{'='*78}")
    rows = {}
    for kind in ("pred", "blend"):
        direct = metrics(yt, out["total"][kind])
        summed = metrics(yt, out["offense"][kind] + out["defense"][kind])
        rows[f"direct_total_{kind}"] = direct
        rows[f"sum_of_parts_{kind}"] = summed
        label = "LightGBM" if kind == "pred" else "blend"
        print(f"  {label}:")
        for name, v in (("direct total", direct), ("offense+defense", summed)):
            print(f"    {name:<18} rmse={v['rmse']:.3f}  mae={v['mae']:.3f}  "
                  f"r2={v['r2']:+.3f}  spearman={v['spearman']:+.3f}")
    json.dump(rows, open(Path(outdir) / f"{model}_target_comparison.json", "w"), indent=2)


if __name__ == "__main__":
    main()
