"""Leave-one-season-out CV, and a check on whether it predicts held-out performance.

Grouped CV -- folds split by player-season -- has now disagreed with the test seasons
three times: hyperparameter tuning gained 2.4% in CV and 0.0% on test, the polarity
restriction won on CV and was flat on test, and dropping the wowy levels won on CV
while costing 15% of test MAE on offence. All three failures share a shape: the folds
share seasons, so anything that exploits era-specific context looks good in CV and
does not transfer to a test set drawn from a different era.

Leave-one-season-out removes that. Each fold holds out a whole season, which is the
same generalisation the test set asks for.

Two wrinkles the fold structure forces, both visible in fold_report():

  Size. Training rows per season run from 165 (2019-20) to 8320 (2020-21) -- 64% of
  the training set is 2020-21 in-season snapshots, an artefact of which archived 538
  captures happened to be aligned. So the pooled MAE across folds is really the
  2020-21 MAE. macro is the mean of per-season MAEs, one vote per season, and is the
  number to read.

  Shape. Most training rows are in-season snapshots; every test row is a whole-season
  cell. Scoring a fold on its whole-season cell only -- "full" below -- matches the
  test set's shape as well as its era, and is the closest analogue available.

Run:  python training/season_cv.py --validate
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
from seasons import FULL_SEASON_SNAPSHOTS
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10


def fold_report(d, tr):
    seasons = sorted(set(d["season"][tr]))
    print(f"{'season':<10} {'rows':>7} {'whole-season rows':>19}")
    for s in seasons:
        m = tr & (d["season"] == s)
        full = m & np.array([t in FULL_SEASON_SNAPSHOTS for t in d["timestamp"]])
        print(f"{s:<10} {m.sum():>7} {full.sum():>19}")
    return seasons


def loso_mae(X, y, d, tr, params, rounds, full_only=False):
    """-> (macro MAE, pooled MAE, {season: MAE}). One fold per season.

    full_only scores each fold on that season's whole-season cell alone, which is the
    shape every test row has. Seasons without such a cell are skipped in that mode.
    """
    seasons = sorted(set(d["season"][tr]))
    is_full = np.array([t in FULL_SEASON_SNAPSHOTS for t in d["timestamp"]])
    per, errs_pooled = {}, []
    for s in seasons:
        held = tr & (d["season"] == s)
        eval_mask = held & is_full if full_only else held
        if eval_mask.sum() < 30:
            continue
        fit_mask = tr & (d["season"] != s)
        bst = lgb.train(params, lgb.Dataset(X[fit_mask], y[fit_mask]),
                        num_boost_round=rounds)
        e = np.abs(y[eval_mask] - bst.predict(X[eval_mask]))
        per[s] = float(e.mean())
        errs_pooled.append(e)
    macro = float(np.mean(list(per.values()))) if per else float("nan")
    pooled = float(np.concatenate(errs_pooled).mean()) if errs_pooled else float("nan")
    return macro, pooled, per


def grouped_mae(X, y, d, tr, params, folds=3):
    groups = np.array([f"{p}|{s}|{t}" for p, s, t in
                       zip(d["player"][tr], d["season"][tr], d["season_type"][tr])])
    sp = GroupKFold(n_splits=folds)
    res = lgb.cv(dict(params, metric="l1"), lgb.Dataset(X[tr], y[tr]),
                 num_boost_round=1500, folds=sp.split(X[tr], y[tr], groups=groups),
                 callbacks=[lgb.early_stopping(100, verbose=False)])
    k = next(k for k in res if k.endswith("-mean"))
    return float(res[k][-1])


# The four wowy representations, which already have measured test MAE -- the set used
# to ask which CV scheme actually predicts the test seasons.
WOWY_VARIANTS = {
    "on+off+diff": {"wowy_on", "wowy_off", "wowy_diff"},
    "diff only": {"wowy_diff"},
    "on+off": {"wowy_on", "wowy_off"},
    "diff+off": {"wowy_diff", "wowy_off"},
}
# Measured earlier in this session (test MAE on 2013-14 + 2014-15).
TEST_MAE = {
    ("total", "on+off+diff"): 1.173, ("total", "diff only"): 1.188,
    ("total", "on+off"): 1.189, ("total", "diff+off"): 1.178,
    ("offense", "on+off+diff"): 0.650, ("offense", "diff only"): 0.749,
    ("offense", "on+off"): 0.651, ("offense", "diff+off"): 0.653,
    ("defense", "on+off+diff"): 1.008, ("defense", "diff only"): 1.055,
    ("defense", "on+off"): 1.003, ("defense", "diff+off"): 1.020,
}
GROUPED_MAE = {
    ("total", "on+off+diff"): 1.7994, ("total", "diff only"): 1.7775,
    ("total", "on+off"): 1.8177, ("total", "diff+off"): 1.7861,
    ("offense", "on+off+diff"): 1.0748, ("offense", "diff only"): 1.0684,
    ("offense", "on+off"): 1.0757, ("offense", "diff+off"): 1.0726,
    ("defense", "on+off+diff"): 1.3289, ("defense", "diff only"): 1.3254,
    ("defense", "on+off"): 1.3362, ("defense", "diff+off"): 1.3290,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--targets", nargs="*", default=["total", "offense", "defense"])
    ap.add_argument("--validate", action="store_true",
                    help="score the four wowy variants and compare CV schemes")
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_season_cv.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    tr = fit | val
    print(f"X={X.shape} train={tr.sum()} test={test.sum()}\n")
    seasons = fold_report(d, tr)
    print()

    tuned = json.loads(Path(args.tuned).read_text())
    rows = []
    for target in args.targets:
        y = d[TARGETS[target]]
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = tuned[target]["rounds"]
        variants = WOWY_VARIANTS if args.validate else {"on+off+diff": None}
        for name, blocks in variants.items():
            if blocks is None:
                mask = np.ones(len(feat), bool)
            else:
                mask = np.array([(n.split("|", 1)[0] in blocks)
                                 if n.split("|", 1)[0].startswith("wowy") else True
                                 for n in feat])
            Xs = X[:, mask]
            macro, pooled, per = loso_mae(Xs, y, d, tr, params, rounds)
            fmacro, fpooled, fper = loso_mae(Xs, y, d, tr, params, rounds,
                                             full_only=True)
            rows.append({"target": target, "variant": name,
                         "loso_macro": macro, "loso_pooled": pooled,
                         "loso_full_macro": fmacro, "per_season": per,
                         "per_season_full": fper,
                         "grouped": GROUPED_MAE.get((target, name)),
                         "test": TEST_MAE.get((target, name))})
            print(f"  {target:<8} {name:<12} LOSO macro={macro:.4f} pooled={pooled:.4f}"
                  f"  full-cell macro={fmacro:.4f}"
                  f"  | grouped={GROUPED_MAE.get((target, name))}"
                  f" test={TEST_MAE.get((target, name))}", flush=True)

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    if args.validate:
        report_alignment(rows, args.out, seasons)
    print(f"\nwrote {args.out}")


def report_alignment(rows, out, seasons):
    """Which CV scheme ranks the variants the way the test set does?"""
    from scipy.stats import kendalltau, pearsonr
    L, A = [], None
    L = []
    A = L.append
    A("# Leave-one-season-out CV")
    A("")
    A("Grouped CV (folds split by player-season) disagreed with the held-out seasons")
    A("three times running. Leave-one-season-out holds out a whole season per fold,")
    A("which is the generalisation the test set actually asks for.")
    A("")
    A("## Fold sizes are wildly uneven")
    A("")
    A("Training rows per season: " + ", ".join(seasons) + ".")
    A("2020-21 alone is 8,320 of 13,063 rows — 64% — because it is the season whose")
    A("archived 538 captures happened to be aligned in bulk. So a pooled MAE across")
    A("folds is largely the 2020-21 MAE; `macro` gives each season one vote and is the")
    A("number to read.")
    A("")
    A("**2022-23 contributes no training rows at all**, despite having 40 timestamps")
    A("with all four sources and correctly aligned labels. The label-season filter")
    A("drops a cell when the snapshot's 538 capture showed a different season, and for")
    A("2022-23 the stride-6 selection landed on snapshots where that is true. Same")
    A("class of gap as 2018-19 was — worth recovering separately.")
    A("")
    A("## Does LOSO predict the test seasons better than grouped CV?")
    A("")
    A("Across the four wowy representations, per target: correlation between each CV")
    A("scheme's MAE and the measured test MAE. Higher is better; the point of a CV")
    A("scheme is to rank configurations the way the test set will.")
    A("")
    A("| target | scheme | Pearson r vs test MAE | Kendall tau |")
    A("|---|---|---:|---:|")
    for target in sorted({r["target"] for r in rows}):
        rs = [r for r in rows if r["target"] == target and r["test"] is not None]
        if len(rs) < 3:
            continue
        t = np.array([r["test"] for r in rs])
        for scheme, key in (("grouped (player-season)", "grouped"),
                            ("LOSO macro", "loso_macro"),
                            ("LOSO whole-season cells", "loso_full_macro")):
            v = np.array([r[key] for r in rs], dtype=float)
            if np.any(~np.isfinite(v)):
                continue
            pr = pearsonr(v, t).statistic
            kt = kendalltau(v, t).statistic
            A(f"| {target} | {scheme} | {pr:+.3f} | {kt:+.3f} |")
    A("")
    A("## Per-variant numbers")
    A("")
    A("| target | wowy variant | grouped CV | LOSO macro | LOSO whole-cell | test MAE |")
    A("|---|---|---:|---:|---:|---:|")
    for r in rows:
        A(f"| {r['target']} | {r['variant']} | "
          f"{r['grouped'] if r['grouped'] else float('nan'):.4f} | "
          f"{r['loso_macro']:.4f} | {r['loso_full_macro']:.4f} | "
          f"{r['test'] if r['test'] else float('nan'):.3f} |")
    A("")
    A("## Per-season MAE, current representation")
    A("")
    A("| target | " + " | ".join(seasons) + " |")
    A("|---" * (len(seasons) + 1) + "|")
    for r in rows:
        if r["variant"] != "on+off+diff":
            continue
        cells = [f"{r['per_season'].get(s, float('nan')):.3f}" for s in seasons]
        A(f"| {r['target']} | " + " | ".join(cells) + " |")
    A("")
    Path(out).write_text("\n".join(L))


if __name__ == "__main__":
    main()
