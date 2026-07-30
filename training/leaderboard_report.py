"""Top-50 leaderboards with per-position error, for the test seasons and 2023-26.

For the two held-out seasons every column can be checked against 538's published
RAPTOR. For 2023-24 onward none of them can: 538 shut down, so those cells have
features and no labels. Their tables carry projections only, and no displacement or
Kendall tau, because there is nothing to displace against. Inventing a "true" column
there would be the single most misleading thing this report could do.

Per position i in the projected order:

  projected      the player the model puts at i
  est            the model's score for them
  true           their actual RAPTOR
  true rank      where they actually belong
  drank          true rank - i. Positive means the model placed them higher than
                 they deserved: +2 at position 3 means they truly belong 5th.
  actual at i    who really belongs at position i

Kendall tau is computed over the true top 30, comparing their true order against
their projected order -- the same population as rho@30 elsewhere, so the two are
comparable. A tau over the union of the true and projected top 30 is also reported,
since the two sets are not identical and the union version penalises a projection
for players it wrongly promoted as well as for misordering.

The model is whichever configuration tune_lgbm.py found had the lowest cross-
validated MAE, refit on every non-test row. Selection never sees the test seasons.

Run:  python training/leaderboard_report.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import kendalltau, spearmanr
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT, get_collection
from experiment_arch_weight import evaluate
from experiment_combined import prepare, splits
from labels import SEASON_WIDE
from predict_seasons import (DROP_FEATURES, build_unlabeled, carry_over_positions,
                             eligibility, impute_positions)
from seasons import UNLABELED_SNAPSHOTS
from train_rapture import LGB_PARAMS, TARGETS, add_context, normalize_rates

RS_MIN, PO_MIN = 50, 10
TOP_N = 50
TAU_K = 30


def load_tuned(path, target):
    if not Path(path).exists():
        return dict(LGB_PARAMS), None, "untuned (LGB_PARAMS)"
    all_best = json.loads(Path(path).read_text())
    if target not in all_best:
        return dict(LGB_PARAMS), None, "untuned (LGB_PARAMS)"
    b = all_best[target]
    return dict(b["params"]), b["rounds"], f"{b['tag']} / {b['params']['objective']}"


def blend(Xtr, ytr, Xte, med, params, rounds, ridge_w=0.25):
    bst = lgb.train(params, lgb.Dataset(Xtr, ytr), num_boost_round=rounds)
    pl = bst.predict(Xte)
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    r = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, ytr)
    pr = r.predict((B - mu) / sd)
    return (1 - ridge_w) * pl + ridge_w * pr, pl


def tau_stats(y, p, k=TAU_K):
    """Kendall tau over the true top k, and over the union of true/predicted top k."""
    n = len(y)
    if n < k:
        return float("nan"), float("nan")
    true_order = np.argsort(-y)
    pred_order = np.argsort(-p)
    true_rank = np.empty(n, int)
    true_rank[true_order] = np.arange(n)
    pred_rank = np.empty(n, int)
    pred_rank[pred_order] = np.arange(n)

    tset = true_order[:k]
    tau_true = kendalltau(true_rank[tset], pred_rank[tset]).statistic
    union = np.union1d(tset, pred_order[:k])
    tau_union = kendalltau(true_rank[union], pred_rank[union]).statistic
    return float(tau_true), float(tau_union)


def labeled_table(players, y, p, top_n=TOP_N):
    n = len(y)
    true_order = np.argsort(-y)
    pred_order = np.argsort(-p)
    true_rank = np.empty(n, int)
    true_rank[true_order] = np.arange(n)

    rows = []
    for i in range(min(top_n, n)):
        j = pred_order[i]
        rows.append({
            "pos": i + 1,
            "projected": players[j],
            "est": float(p[j]),
            "true": float(y[j]),
            "true_rank": int(true_rank[j]) + 1,
            "drank": int(true_rank[j]) - i,
            "actual_at_pos": players[true_order[i]],
            "actual_true": float(y[true_order[i]]),
        })
    return rows


def unlabeled_table(players, p, mp, top_n=TOP_N):
    order = np.argsort(-p)
    return [{"pos": i + 1, "projected": players[j], "est": float(p[j]),
             "mp": float(mp[j])}
            for i, j in enumerate(order[:top_n])]


def build_all(args):
    coll = get_collection()
    X, feat, d = prepare(args.datadir)
    # Two column spaces to keep straight. `feat` is post-add_context (1140 after the
    # identifier drop) and indexes X, which is what the test-season fits use. The
    # unlabeled path has to start from the raw pre-context columns instead, because
    # normalize_rates and add_context are applied to it from scratch.
    keep_cols = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X = X[:, keep_cols]

    raw_names = list(d["feat_names"])
    raw_keep = [i for i, n in enumerate(raw_names) if n not in DROP_FEATURES]
    raw_feat_kept = [raw_names[i] for i in raw_keep]
    d["X"] = d["X"][:, raw_keep]

    fit, val, test = splits(d, RS_MIN, PO_MIN)
    tr = fit | val
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    print(f"prepared: X={X.shape} (raw {d['X'].shape}) "
          f"train={tr.sum()} test={test.sum()}")

    # ---- the unlabeled 2023-26 cells, in the raw kept column order ---------
    Xn, meta = build_unlabeled(coll, raw_feat_kept, list(UNLABELED_SNAPSHOTS))
    carry_over_positions(coll, meta)
    impute_positions(d["X"], d["pos"], Xn, meta)
    floors = eligibility(coll)

    out = {"labeled": {}, "unlabeled": {}, "summary": [], "floors": floors}
    for target in args.targets:
        params, rounds, tag = load_tuned(args.tuned, target)
        params = dict(params, verbose=-1)
        if rounds is None:
            rounds = 600
        y = d[TARGETS[target]]
        print(f"\n[{target}] model: {tag}, {rounds} rounds")

        # test-season predictions
        pte, pte_lgbm = blend(X[tr], y[tr], X[test], med, params, rounds,
                              args.ridge_weight)
        cells_te = np.array([f"{s}|{t}" for s, t in
                             zip(d["season"][test], d["season_type"][test])])
        m = evaluate(y[test], pte, cells_te)
        m_lgbm = evaluate(y[test], pte_lgbm, cells_te)
        print(f"  blend  MAE={m['mae']:.3f} R2={m['r2']:+.3f} rho={m['spearman']:+.3f}")
        print(f"  lgbm   MAE={m_lgbm['mae']:.3f} R2={m_lgbm['r2']:+.3f} "
              f"rho={m_lgbm['spearman']:+.3f}")
        use_blend = m["mae"] <= m_lgbm["mae"]
        pick = pte if use_blend else pte_lgbm
        out["summary"].append({"target": target, "model": tag,
                              "chosen": "blend" if use_blend else "lightgbm",
                              "rounds": rounds,
                              "blend": m, "lightgbm": m_lgbm})

        players_te = d["player"][test]
        for cell in np.unique(cells_te):
            mask = cells_te == cell
            tt, tu = tau_stats(y[test][mask], pick[mask])
            rows = labeled_table(players_te[mask], y[test][mask], pick[mask],
                                 args.top_n)
            hits = sum(1 for r in rows[:TAU_K] if r["true_rank"] <= TAU_K)
            out["labeled"][f"{target}|{cell}"] = {
                "rows": rows, "tau_true30": tt, "tau_union30": tu,
                "pool": int(mask.sum()), "hits30": hits,
                "mean_abs_drank": float(np.mean([abs(r["drank"]) for r in rows])),
            }
            print(f"    {cell:<26} pool={mask.sum():<4} tau(true30)={tt:+.3f} "
                  f"tau(union30)={tu:+.3f} hits@30={hits}/30 "
                  f"mean|drank|={out['labeled'][f'{target}|{cell}']['mean_abs_drank']:.1f}")

        # ---- 2023-26 projections, same fitted model ------------------------
        X_all = np.vstack([d["X"], Xn])
        is_train = np.concatenate([~d["test"].astype(bool),
                                   np.zeros(Xn.shape[0], dtype=bool)])
        X_all = normalize_rates(X_all, raw_feat_kept, is_train)
        ctx = {k: np.concatenate([d[k], np.array([mm[k] for mm in meta],
                                                 dtype=d[k].dtype)])
               for k in ("pos", "mp", "timestamp", "season_type")}
        X_all, _ = add_context(X_all, raw_feat_kept, ctx, "combined")
        n_lab = d["X"].shape[0]
        Xlab, Xnew = X_all[:n_lab], X_all[n_lab:]
        med2 = np.nanmedian(Xlab[tr], axis=0)
        med2 = np.where(np.isfinite(med2), med2, 0.0)
        pn, pn_lgbm = blend(Xlab[tr], y[tr], Xnew, med2, params, rounds,
                            args.ridge_weight)
        pn = pn if use_blend else pn_lgbm

        df = pd.DataFrame(meta)
        df["est"] = pn
        for season in sorted(df.season.unique()):
            for st in ("Regular season", "Playoffs"):
                g = df[(df.season == season) & (df.season_type == st)]
                g = g[g.mp >= floors[st]]
                if g.empty:
                    continue
                out["unlabeled"][f"{target}|{season}|{st}"] = {
                    "rows": unlabeled_table(g.player.values, g.est.values,
                                            g.mp.values, args.top_n),
                    "pool": int(len(g)),
                }
    return out


def write_report(out, path, args):
    L = []
    A = L.append
    A("# Top-50 leaderboards: held-out seasons and 2023-26")
    A("")
    A("## What can and cannot be checked")
    A("")
    A("**2013-14 and 2014-15** are held out of training, and 538 published RAPTOR for")
    A("them, so every projected position can be scored against the truth.")
    A("")
    A("**2023-24, 2024-25 and 2025-26 have no ground truth.** 538 shut down; those")
    A("cells have features and no labels. Their tables below carry projections only.")
    A("There is no `true`, no `drank` and no Kendall tau for them, because there is")
    A("nothing to compare against — not because it was omitted.")
    A("")
    A("## Columns")
    A("")
    A("| column | meaning |")
    A("|---|---|")
    A("| `pos` | position in the **projected** order |")
    A("| `projected` | the player the model puts there |")
    A("| `est` | the model's score |")
    A("| `true` | that player's actual RAPTOR |")
    A("| `true rank` | where that player actually belongs |")
    A("| `Δrank` | `true rank - pos`. **Positive means placed too high**: +2 at pos 3 means they truly belong 5th. 0 is exact. |")
    A("| `actual at pos` | who really belongs at that position |")
    A("")
    A("## Model")
    A("")
    A("| target | config | chosen | rounds | test MAE | R² | rho |")
    A("|---|---|---|---|---|---|---|")
    for s in out["summary"]:
        m = s[s["chosen"]]
        A(f"| {s['target']} | {s['model']} | {s['chosen']} | {s['rounds']} | "
          f"{m['mae']:.3f} | {m['r2']:+.3f} | {m['spearman']:+.3f} |")
    A("")
    A("Selected by cross-validated MAE inside the training rows; the test seasons are")
    A("used once, for the tables below. Every non-test row trains — no season is held")
    A("out for validation (see RESULTS_trainonly.md: it costs nothing).")
    A("")
    A(f"Minutes floor for the 2023-26 boards: "
      + ", ".join(f"{k} {v:.0f}" for k, v in out["floors"].items())
      + " — the lowest 538 itself ever rated in that split.")
    A("")

    A("## Kendall tau over the top 30, held-out seasons")
    A("")
    A("`tau(true30)` compares the true order of the true top 30 against their")
    A("projected order. `tau(union30)` widens the set to the union of the true and")
    A("projected top 30, so it also penalises wrongly promoted players.")
    A("")
    A("| target | season | split | pool | tau(true30) | tau(union30) | hits@30 | mean &#124;Δrank&#124; |")
    A("|---|---|---|---|---|---|---|---|")
    for key, v in out["labeled"].items():
        target, cell = key.split("|", 1)
        season, split = cell.split("|")
        A(f"| {target} | {season} | {split} | {v['pool']} | {v['tau_true30']:+.3f} | "
          f"{v['tau_union30']:+.3f} | {v['hits30']}/30 | {v['mean_abs_drank']:.1f} |")
    A("")

    for key, v in out["labeled"].items():
        target, cell = key.split("|", 1)
        season, split = cell.split("|")
        A(f"## {season} {split} — {target}, top {len(v['rows'])}")
        A("")
        A(f"> pool {v['pool']} players &nbsp;·&nbsp; tau(true30) {v['tau_true30']:+.3f}"
          f" &nbsp;·&nbsp; hits@30 {v['hits30']}/30")
        A("")
        A("| pos | projected | est | true | true rank | Δrank | actual at pos | true |")
        A("|---:|---|---:|---:|---:|---:|---|---:|")
        for r in v["rows"]:
            A(f"| {r['pos']} | {r['projected']} | {r['est']:+.2f} | {r['true']:+.2f} | "
              f"{r['true_rank']} | {r['drank']:+d} | {r['actual_at_pos']} | "
              f"{r['actual_true']:+.2f} |")
        A("")

    for key, v in out["unlabeled"].items():
        target, season, split = key.split("|")
        A(f"## {season} {split} — {target}, top {len(v['rows'])} (projected, no truth)")
        A("")
        A(f"> pool {v['pool']} players above the minutes floor")
        A("")
        A("| pos | projected | est | mp |")
        A("|---:|---|---:|---:|")
        for r in v["rows"]:
            A(f"| {r['pos']} | {r['projected']} | {r['est']:+.2f} | {r['mp']:.0f} |")
        A("")

    Path(path).write_text("\n".join(L))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--targets", nargs="*", default=["total", "offense", "defense"])
    ap.add_argument("--top-n", type=int, default=TOP_N)
    ap.add_argument("--ridge-weight", type=float, default=0.25)
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                        / "RESULTS_leaderboards.md"))
    args = ap.parse_args()

    out = build_all(args)
    Path(args.out).with_suffix(".json").write_text(json.dumps(out, indent=1))
    write_report(out, args.out, args)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
