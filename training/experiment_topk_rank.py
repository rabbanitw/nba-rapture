"""Why rho is high while top-30 ordering is poor, and what actually fixes the latter.

The diagnosis first, because it bounds what any strategy can achieve. Spearman rho is
computed over every player in a cell -- roughly 250 -- and is dominated by separating
an All-Star from a replacement-level guard, which is a large, easy signal. tau@30 and
the rank deviations are computed *inside* the true top 30, where the whole population
spans a few RAPTOR points and adjacent ranks are separated by a fraction of one. If
the model's own error is comparable to the gap between rank 5 and rank 6, the ordering
there is noise-limited no matter how good rho looks. diagnose() measures those gaps
against the model's error so the ceiling is a number rather than an intuition.

Then the strategies, all judged on mean |drank| over the top 10 -- the projected
board's first ten positions, scored against where those players truly belong:

  baseline        one fit, as shipped
  seed-ens-N      average N fits over different seeds. Pure variance reduction, and
                  when the true gaps are smaller than the noise that is exactly the
                  binding constraint
  rank-blend      average the within-cell ranks of a RAPTOR-target model and a
                  percentile-target model. They were shown to have complementary
                  strengths -- ordering vs membership -- in RESULTS_labels.md
  two-stage       a specialist refit only on rows in the true top 60 of their cell,
                  used to re-score the first model's top 60. Its loss then spends
                  everything on the range that decides the leaderboard
  top-slice       a single model trained only on top-60 rows, no cascade

Run:  python training/experiment_topk_rank.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from scipy.stats import kendalltau
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_labels import make_targets
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
KS = (10, 20, 30)


def ranks(v):
    """Descending rank, 0-based: rank 0 is the largest value."""
    order = np.argsort(-v)
    r = np.empty(len(v), int)
    r[order] = np.arange(len(v))
    return r


def dev_at_k(y, p, k):
    """Mean |true rank - projected position| over the projected top k."""
    tr, pr = ranks(y), ranks(p)
    sel = np.argsort(pr)[:k]
    return float(np.mean(np.abs(tr[sel] - pr[sel])))


def tau_at_k(y, p, k):
    tr, pr = ranks(y), ranks(p)
    sel = np.argsort(tr)[:k]
    return float(kendalltau(tr[sel], pr[sel]).statistic)


def hits_at_k(y, p, k):
    tr, pr = ranks(y), ranks(p)
    return int(len(set(np.where(tr < k)[0]) & set(np.where(pr < k)[0])))


def score_cells(y, p, cells):
    out = {}
    for k in KS:
        out[f"dev@{k}"] = float(np.mean([dev_at_k(y[cells == c], p[cells == c], k)
                                         for c in np.unique(cells)]))
        out[f"tau@{k}"] = float(np.mean([tau_at_k(y[cells == c], p[cells == c], k)
                                         for c in np.unique(cells)]))
        out[f"hits@{k}"] = int(sum(hits_at_k(y[cells == c], p[cells == c], k)
                                   for c in np.unique(cells)))
    out["mae"] = float(np.mean(np.abs(y - p)))
    return out


def diagnose(y, cells):
    """How much true RAPTOR separates adjacent ranks, by depth."""
    rows = []
    for c in np.unique(cells):
        v = np.sort(y[cells == c])[::-1]
        for lo, hi, name in ((0, 10, "top 10"), (10, 20, "11-20"),
                             (20, 30, "21-30"), (30, 100, "31-100"),
                             (100, len(v), "101+")):
            seg = v[lo:min(hi, len(v))]
            if len(seg) > 1:
                rows.append((name, float(np.mean(-np.diff(seg))),
                             float(seg[0] - seg[-1])))
    agg = {}
    for name, gap, span in rows:
        agg.setdefault(name, []).append((gap, span))
    return {k: {"mean_adjacent_gap": float(np.mean([g for g, _ in v])),
                "mean_span": float(np.mean([s for _, s in v]))}
            for k, v in agg.items()}


def fit_blend(Xtr, ttr, Xte, med, params, rounds, seed=0, ridge_w=0.25, ridge=None):
    p = dict(params, seed=seed, bagging_seed=seed, feature_fraction_seed=seed)
    bst = lgb.train(p, lgb.Dataset(Xtr, ttr), num_boost_round=rounds)
    pl = bst.predict(Xte)
    if ridge is None:
        return pl
    return (1 - ridge_w) * pl + ridge_w * ridge


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--targets", nargs="*", default=["total", "offense", "defense"])
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--slice-k", type=int, default=60)
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_topk_rank.md"))
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
    A = np.where(np.isfinite(X[tr]), X[tr], med)
    B = np.where(np.isfinite(X[test]), X[test], med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    An, Bn = (A - mu) / sd, (B - mu) / sd
    tuned = json.loads(Path(args.tuned).read_text())
    print(f"X={X.shape} train={tr.sum()} test={test.sum()}", flush=True)

    out = {"diagnosis": {}, "strategies": []}
    for target in args.targets:
        y = d[TARGETS[target]]
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = tuned[target]["rounds"]
        out["diagnosis"][target] = diagnose(y[test], cells_te)
        ridge = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(An, y[tr]).predict(Bn)

        print(f"\n=== {target} ===", flush=True)
        g = out["diagnosis"][target]
        for band in ("top 10", "11-20", "21-30", "31-100", "101+"):
            if band in g:
                print(f"    {band:<8} mean gap between adjacent ranks "
                      f"{g[band]['mean_adjacent_gap']:.3f} RAPTOR", flush=True)

        preds = {}
        # baseline + seed ensemble in one pass
        singles = [fit_blend(X[tr], y[tr], X[test], med, params, rounds, s, ridge=ridge)
                   for s in range(args.seeds)]
        preds["baseline"] = singles[0]
        preds[f"seed-ens-{args.seeds}"] = np.mean(singles, axis=0)

        # rank-blend with a percentile-target model
        tpct = make_targets(y[tr], cells_tr, y_train_for_clip=y[tr])["cell_pct"]
        ridge_p = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(An, tpct).predict(Bn)
        pct_pred = np.mean([fit_blend(X[tr], tpct, X[test], med, params, rounds, s,
                                      ridge=ridge_p) for s in range(3)], axis=0)
        rb = np.empty(len(y[test]))
        for c in np.unique(cells_te):
            m = cells_te == c
            rb[m] = -(ranks(preds[f"seed-ens-{args.seeds}"][m]) + ranks(pct_pred[m])) / 2.0
        preds["rank-blend"] = rb

        # top-slice specialist, and the two-stage cascade that uses it
        top_mask = np.zeros(int(tr.sum()), bool)
        ytr = y[tr]
        for c in np.unique(cells_tr):
            m = np.where(cells_tr == c)[0]
            top_mask[m[np.argsort(-ytr[m])[:args.slice_k]]] = True
        Xs, ys = X[tr][top_mask], ytr[top_mask]
        ridge_s = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(
            An[top_mask], ys).predict(Bn)
        slice_pred = np.mean([fit_blend(Xs, ys, X[test], med, params, rounds, s,
                                        ridge=ridge_s) for s in range(3)], axis=0)
        preds["top-slice"] = slice_pred

        base = preds[f"seed-ens-{args.seeds}"]
        two = base.copy()
        for c in np.unique(cells_te):
            m = np.where(cells_te == c)[0]
            cand = m[np.argsort(-base[m])[:args.slice_k]]
            # re-score the candidates, keeping them above everyone else
            r = slice_pred[cand]
            two[cand] = r.max() + 1.0 + (r - r.min()) / (np.ptp(r) or 1.0)
        preds["two-stage"] = two

        for name, p in preds.items():
            s = score_cells(y[test], p, cells_te)
            out["strategies"].append({"target": target, "strategy": name, **s})
            print(f"  {name:<14} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
                  f"dev@30={s['dev@30']:5.2f}  tau@10={s['tau@10']:+.3f} "
                  f"tau@20={s['tau@20']:+.3f}  hits@10={s['hits@10']}/20 "
                  f"hits@20={s['hits@20']}/40", flush=True)

    Path(args.out).with_suffix(".json").write_text(json.dumps(out, indent=1))
    write_report(out, args.out, args)
    print(f"\nwrote {args.out}")


def write_report(out, path, args):
    L = []
    A = L.append
    A("# Top-10 and top-20 rank deviation")
    A("")
    A("## Why rho is high while top-30 ordering is not")
    A("")
    A("Spearman rho runs over every player in a cell — about 250 — and is carried by")
    A("separating tiers, which is a large and easy signal. The rank deviations run")
    A("*inside* the top of the board, where the whole population is a few RAPTOR points")
    A("wide. The gap between adjacent true ranks there is what any model has to resolve:")
    A("")
    A("| band | mean gap between adjacent true ranks (RAPTOR) |")
    A("|---|---:|")
    for target in out["diagnosis"]:
        for band in ("top 10", "11-20", "21-30", "31-100", "101+"):
            g = out["diagnosis"][target].get(band)
            if g:
                A(f"| {target} {band} | {g['mean_adjacent_gap']:.3f} |")
    A("")
    A("Set those against the model's MAE. Where the adjacent gap is far below the error,")
    A("the ordering is noise-limited and no amount of model capacity recovers it.")
    A("")
    A("## Strategies, judged on dev@10")
    A("")
    A("`dev@k` is the mean |true rank - projected position| over the projected top k.")
    A("Lower is better; 0 would be a perfect board.")
    A("")
    for target in sorted({r["target"] for r in out["strategies"]},
                         key=lambda t: ["total", "offense", "defense"].index(t)):
        A(f"### {target}")
        A("")
        A("| strategy | dev@10 | dev@20 | dev@30 | tau@10 | tau@20 | hits@10 | hits@20 |")
        A("|---|---:|---:|---:|---:|---:|---:|---:|")
        for r in sorted([x for x in out["strategies"] if x["target"] == target],
                        key=lambda r: r["dev@10"]):
            A(f"| {r['strategy']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
              f"{r['dev@30']:.2f} | {r['tau@10']:+.3f} | {r['tau@20']:+.3f} | "
              f"{r['hits@10']}/20 | {r['hits@20']}/40 |")
        A("")
    Path(path).write_text("\n".join(L))


if __name__ == "__main__":
    main()
