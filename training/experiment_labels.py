"""Alternative label formulations: does predicting rank beat predicting RAPTOR?

A leaderboard only needs the ordering inside a cell. RAPTOR itself is one way to
encode that ordering and not obviously the best one -- it is heavy-tailed, its scale
drifts between cells (a 250-player 538 pool and a 570-player modern pool are not the
same population), and squared error on it spends most of its budget on the crowded
middle. Any within-cell monotone transform preserves the ranking task while changing
what the loss cares about.

The targets, all computed within each (timestamp, season_type) cell:

  raptor        RAPTOR itself. The baseline.
  cell_z        (y - cell mean) / cell sd. Removes cell-level level and spread, so
                the model stops having to learn that different pools sit differently.
  cell_pct      within-cell percentile in [0,1]. Pure ordering, every cell on one
                scale, tails flattened completely.
  cell_rankit   normal quantile of the percentile (van der Waerden). Ordering again,
                but with a Gaussian shape, so squared error is well-matched and the
                extremes keep some separation instead of being squashed flat.
  signed_sqrt   sign(y)*sqrt(|y|). Global, not per-cell: compresses the tail without
                touching ordering, a middle ground between raptor and pure rank.
  winsor        y clipped to the training 1st/99th percentile. Tests whether the
                tail is simply noise the model would be better off not chasing.

Getting the comparison fair. Rank-shaped targets are not in RAPTOR units, so MAE
against RAPTOR is undefined for them. Every prediction is therefore also mapped back:
rank the predictions inside the test cell, take the percentile, and read it off the
training RAPTOR distribution's quantile function. That is applied identically to
every target including the baseline, so the MAE column compares like with like. The
ranking columns need no mapping -- they are invariant to any monotone transform.

Built on the current best setup: regular season only, all features.

Run:  python training/experiment_labels.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from scipy.stats import norm
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_arch_weight import cell_topk
from experiment_combined import prepare, splits
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
TAU_K = 30


def within_cell(y, cells, fn):
    out = np.empty_like(y, dtype=np.float64)
    for c in np.unique(cells):
        m = cells == c
        out[m] = fn(y[m])
    return out


def pct_of(v):
    """Within-cell percentile in [0,1], ties broken by order."""
    n = len(v)
    order = np.argsort(v)
    r = np.empty(n)
    r[order] = np.arange(n)
    return (r + 0.5) / n


def make_targets(y, cells, y_train_for_clip=None):
    lo, hi = np.percentile(y_train_for_clip if y_train_for_clip is not None else y,
                           [1, 99])
    return {
        "raptor": y.astype(np.float64),
        "cell_z": within_cell(y, cells, lambda v: (v - v.mean()) / (v.std() or 1.0)),
        "cell_pct": within_cell(y, cells, pct_of),
        "cell_rankit": within_cell(y, cells, lambda v: norm.ppf(pct_of(v))),
        "signed_sqrt": np.sign(y) * np.sqrt(np.abs(y)),
        "winsor": np.clip(y, lo, hi).astype(np.float64),
    }


def to_raptor_units(pred, cells, y_train):
    """Map predictions onto the training RAPTOR distribution by within-cell rank.

    Applied to every target the same way, baseline included, so the MAE column is a
    like-for-like comparison rather than a comparison of output scales.
    """
    out = np.empty_like(pred, dtype=np.float64)
    qs = np.sort(y_train.astype(np.float64))
    for c in np.unique(cells):
        m = cells == c
        p = pct_of(pred[m])
        out[m] = np.quantile(qs, np.clip(p, 0, 1))
    return out


def fit_blend(Xtr, ttr, Xte, med, params, rounds, ridge_w=0.25):
    bst = lgb.train(params, lgb.Dataset(Xtr, ttr), num_boost_round=rounds)
    pl = bst.predict(Xte)
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, ttr).predict((B - mu) / sd)
    return (1 - ridge_w) * pl + ridge_w * pr


def score(y_true, pred_raw, pred_raptor, cells):
    from scipy.stats import kendalltau, spearmanr
    h30, n30, _ = cell_topk(y_true, pred_raw, cells, 30)
    h20, n20, _ = cell_topk(y_true, pred_raw, cells, 20)
    taus = []
    for c in np.unique(cells):
        m = cells == c
        yt, pt = y_true[m], pred_raw[m]
        if len(yt) < TAU_K:
            continue
        order = np.argsort(-yt)
        tr = np.empty(len(yt), int); tr[order] = np.arange(len(yt))
        po = np.argsort(-pt)
        pr = np.empty(len(yt), int); pr[po] = np.arange(len(yt))
        sel = order[:TAU_K]
        taus.append(kendalltau(tr[sel], pr[sel]).statistic)
    return {
        "mae_mapped": float(np.mean(np.abs(y_true - pred_raptor))),
        "spearman": float(spearmanr(y_true, pred_raw).statistic),
        "hits20": f"{h20}/{n20}", "hits30": f"{h30}/{n30}",
        "hits30_frac": h30 / max(n30, 1),
        "tau30": float(np.mean(taus)) if taus else float("nan"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--targets", nargs="*", default=["total", "offense", "defense"])
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_labels.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X = X[:, keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    cells_tr = np.array([f"{t}|{s}" for t, s in
                         zip(d["timestamp"][tr], d["season_type"][tr])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    print(f"X={X.shape} train={tr.sum()} test={test.sum()} "
          f"({len(np.unique(cells_tr))} training cells)")

    tuned = json.loads(Path(args.tuned).read_text())
    rows = []
    for target in args.targets:
        y = d[TARGETS[target]]
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = tuned[target]["rounds"]
        tt = make_targets(y[tr], cells_tr, y_train_for_clip=y[tr])
        print(f"\n=== {target} ===", flush=True)
        for name, ttr in tt.items():
            pred = fit_blend(X[tr], ttr, X[test], med, params, rounds)
            mapped = to_raptor_units(pred, cells_te, y[tr])
            s = score(y[test], pred, mapped, cells_te)
            rows.append({"target": target, "label": name, **s})
            print(f"  {name:<12} MAE*={s['mae_mapped']:.3f} rho={s['spearman']:+.3f} "
                  f"hits@20={s['hits20']} hits@30={s['hits30']} "
                  f"tau30={s['tau30']:+.3f}", flush=True)

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    write_report(rows, args.out)
    print(f"\nwrote {args.out}")


def write_report(rows, out):
    L = []
    A = L.append
    A("# Alternative label formulations")
    A("")
    A("A leaderboard needs the ordering inside a cell, and RAPTOR is only one way to")
    A("encode it. Each label below is a within-cell monotone transform, so the ranking")
    A("task is unchanged and only the loss geometry moves.")
    A("")
    A("Regular season only, all features, blend of tuned LightGBM + RidgeCV.")
    A("")
    A("`MAE*` is measured after mapping every prediction back to RAPTOR units by")
    A("within-cell rank through the training distribution's quantile function — applied")
    A("identically to every label including the baseline, so the column compares like")
    A("with like rather than comparing output scales. Ranking columns need no mapping.")
    A("")
    for target in sorted({r["target"] for r in rows},
                         key=lambda t: ["total", "offense", "defense"].index(t)):
        A(f"## {target}")
        A("")
        A("| label | MAE* | rho | hits@20 | hits@30 | tau30 |")
        A("|---|---:|---:|---:|---:|---:|")
        for r in sorted([x for x in rows if x["target"] == target],
                        key=lambda r: -r["tau30"]):
            A(f"| {r['label']} | {r['mae_mapped']:.3f} | {r['spearman']:+.3f} | "
              f"{r['hits20']} | {r['hits30']} | {r['tau30']:+.3f} |")
        A("")
    Path(out).write_text("\n".join(L))


if __name__ == "__main__":
    main()
