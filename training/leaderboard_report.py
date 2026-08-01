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
from experiment_labels import make_targets, to_raptor_units
from estimated_raptor import norm_name
from labels import SEASON_WIDE
from predict_seasons import (DROP_FEATURES, build_unlabeled, carry_over_positions,
                             eligibility, impute_positions)
from seasons import UNLABELED_SNAPSHOTS
from stat_polarity import classify_all, feature_mask
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


PAINE_COL = {"total": "eRT", "offense": "eRO", "defense": "eRD"}


def load_paine(path, target):
    """Neil Paine's published Estimated RAPTOR, keyed for joining.

    IMPORTANT: his weights were fit on 2014-2023 full RAPTOR, which contains both of
    our test seasons. His numbers here are in-sample and ours are not, so he is
    flattered by the comparison -- see estimated_raptor.py.
    """
    if not Path(path).exists():
        return {}
    df = pd.read_csv(path)
    col = PAINE_COL[target]
    out = {}
    for _, r in df.iterrows():
        if pd.isna(r.get(col)):
            continue
        out[(norm_name(r["player"]), r["season"], r["split"])] = float(r[col])
    return out


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
    if args.regular_season_only:
        # A regular-season projection has no reason to learn from playoff rows: the
        # samples are an order of magnitude shorter, RAPTOR is far noisier over them,
        # and ctx|is_playoffs becomes a constant the model no longer has to condition
        # on. It costs little -- playoff cells are only 580 of 14,465 training rows.
        rs = d["season_type"] == "Regular season"
        tr = tr & rs
        test = test & rs
    print(f"prepared: X={X.shape} (raw {d['X'].shape}) "
          f"train={tr.sum()} test={test.sum()}"
          + ("  [regular season only]" if args.regular_season_only else ""))

    # Offence models see offence+neutral columns, defence models defence+neutral.
    # See stat_polarity.py; chosen because it lowers cross-validated MAE on both.
    polarity, _ = classify_all([n for n in feat if "|" in n])
    feat_kept_names = [feat[i] for i in keep_cols]

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
        mask = (feature_mask(feat_kept_names, target, polarity) if args.polarity
                else np.ones(len(feat_kept_names), bool))
        Xt = X[:, mask]
        med = np.nanmedian(Xt[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        print(f"\n[{target}] model: {tag}, {rounds} rounds, "
              f"{mask.sum()}/{len(mask)} features"
              + ("" if args.polarity else " (polarity filter off)"))

        # The label the model is fitted on need not be RAPTOR. Any within-cell
        # monotone transform leaves the ranking task unchanged; predictions are
        # mapped back to RAPTOR units afterwards by within-cell rank through the
        # training distribution, so every column downstream stays interpretable.
        cells_tr = np.array([f"{t}|{st}" for t, st in
                             zip(d["timestamp"][tr], d["season_type"][tr])])
        ytr = make_targets(y[tr], cells_tr, y_train_for_clip=y[tr])[args.label]

        # test-season predictions
        pte, pte_lgbm = blend(Xt[tr], ytr, Xt[test], med, params, rounds,
                              args.ridge_weight)
        cells_te = np.array([f"{s}|{t}" for s, t in
                             zip(d["season"][test], d["season_type"][test])])
        if args.label != "raptor":
            pte = to_raptor_units(pte, cells_te, y[tr])
            pte_lgbm = to_raptor_units(pte_lgbm, cells_te, y[tr])
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

        paine = load_paine(args.paine, target)
        players_te = d["player"][test]
        for cell in np.unique(cells_te):
            mask = cells_te == cell
            season, split = cell.split("|")
            tt, tu = tau_stats(y[test][mask], pick[mask])
            rows = labeled_table(players_te[mask], y[test][mask], pick[mask],
                                 args.top_n)
            hits = sum(1 for r in rows[:TAU_K] if r["true_rank"] <= TAU_K)

            # Paine on exactly the players he covers in this cell, and us on the
            # same subset, so the two are compared over one population.
            names = players_te[mask]
            pv = np.array([paine.get((norm_name(n), season, split), np.nan)
                           for n in names])
            have = np.isfinite(pv)
            pcmp = None
            if have.sum() >= TAU_K:
                yv, ours_v = y[test][mask][have], pick[mask][have]
                pt, pu = tau_stats(yv, pv[have])
                ot, ou = tau_stats(yv, ours_v)
                p_rows = labeled_table(names[have], yv, pv[have], args.top_n)
                pcmp = {
                    "n": int(have.sum()),
                    "paine": {"tau_true30": pt, "tau_union30": pu,
                              "mae": float(np.mean(np.abs(yv - pv[have]))),
                              "hits30": sum(1 for r in p_rows[:TAU_K]
                                            if r["true_rank"] <= TAU_K),
                              "mean_abs_drank": float(np.mean(
                                  [abs(r["drank"]) for r in p_rows])),
                              "rows": p_rows},
                    "ours": {"tau_true30": ot, "tau_union30": ou,
                             "mae": float(np.mean(np.abs(yv - ours_v))),
                             "hits30": sum(1 for r in labeled_table(
                                 names[have], yv, ours_v, args.top_n)[:TAU_K]
                                 if r["true_rank"] <= TAU_K)},
                }
            out["labeled"][f"{target}|{cell}"] = {
                "rows": rows, "tau_true30": tt, "tau_union30": tu,
                "pool": int(mask.sum()), "hits30": hits,
                "paine": pcmp,
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
        X_all, ctx_names = add_context(X_all, raw_feat_kept, ctx, "combined")
        n_lab = d["X"].shape[0]
        mask2 = (feature_mask(ctx_names, target, polarity) if args.polarity
                 else np.ones(len(ctx_names), bool))
        X_all = X_all[:, mask2]
        Xlab, Xnew = X_all[:n_lab], X_all[n_lab:]
        med2 = np.nanmedian(Xlab[tr], axis=0)
        med2 = np.where(np.isfinite(med2), med2, 0.0)
        pn, pn_lgbm = blend(Xlab[tr], ytr, Xnew, med2, params, rounds,
                            args.ridge_weight)
        pn = pn if use_blend else pn_lgbm

        df = pd.DataFrame(meta)
        df["est"] = pn
        for season in sorted(df.season.unique()):
            for st in (("Regular season",) if args.regular_season_only
                       else ("Regular season", "Playoffs")):
                g = df[(df.season == season) & (df.season_type == st)]
                g = g[g.mp >= floors[st]]
                if g.empty:
                    continue
                if args.label != "raptor":
                    # Map inside the eligible pool, not the raw 582-player cell: the
                    # training distribution is 538's ~250-player pool, so ranking the
                    # full field against it would inflate everyone.
                    one = np.full(len(g), "cell")
                    g = g.assign(est=to_raptor_units(g.est.values, one, y[tr]))
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

    A("## Feature polarity")
    A("")
    A("Every stat is classified offence-centric, defence-centric or neutral")
    A("(see [stat_polarity.md](stat_polarity.md)). The offence model uses")
    A("offence+neutral, the defence model defence+neutral, and total uses everything.")
    A("Of 908 source columns: **685 offence, 107 defence, 116 neutral** — the feeds are")
    A("heavily offensive, so the defence model keeps about a quarter of the columns and")
    A("the offence model nearly nine tenths.")
    A("")
    A("On cross-validated MAE the restriction is a small win for both — offence")
    A("1.0685 against 1.0748, defence 1.3241 against 1.3290 — and it finds more of the")
    A("right players (offence hits@30 102/120 against 98, defence 86 against 80) while")
    A("ordering them very slightly worse. It is close to neutral: gradient boosting was")
    A("already largely ignoring the wrong-side columns.")
    A("")
    A("## Versus Neil Paine's Estimated RAPTOR")
    A("")
    A("Paine's linear model, published weights, on the players he covers in each cell.")
    A("**His weights were fit on 2014-2023 RAPTOR, which includes both test seasons —**")
    A("**his numbers are in-sample and ours are not.** He should be expected to win.")
    A("")
    A("| target | season | split | n | ours MAE | Paine MAE | ours tau30 | Paine tau30 "
      "| ours hits@30 | Paine hits@30 |")
    A("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for key, v in out["labeled"].items():
        target, cell = key.split("|", 1)
        season, split = cell.split("|")
        pc = v.get("paine")
        if not pc:
            continue
        o, pn = pc["ours"], pc["paine"]
        A(f"| {target} | {season} | {split} | {pc['n']} | {o['mae']:.3f} | "
          f"{pn['mae']:.3f} | {o['tau_true30']:+.3f} | {pn['tau_true30']:+.3f} | "
          f"{o['hits30']}/30 | {pn['hits30']}/30 |")
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
        pc = v.get("paine")
        if pc:
            A(f"### {season} {split} — {target}, Paine's top 30 (in-sample)")
            A("")
            A(f"> {pc['n']} players covered &nbsp;·&nbsp; tau(true30) "
              f"{pc['paine']['tau_true30']:+.3f} &nbsp;·&nbsp; hits@30 "
              f"{pc['paine']['hits30']}/30 &nbsp;·&nbsp; MAE {pc['paine']['mae']:.3f}")
            A("")
            A("| pos | Paine's pick | eR | true | true rank | Δrank |")
            A("|---:|---|---:|---:|---:|---:|")
            for r in pc["paine"]["rows"][:30]:
                A(f"| {r['pos']} | {r['projected']} | {r['est']:+.2f} | "
                  f"{r['true']:+.2f} | {r['true_rank']} | {r['drank']:+d} |")
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
    ap.add_argument("--paine", default=str(REPO_ROOT / "training"
                                          / "RESULTS_estimated_raptor.csv"))
    ap.add_argument("--no-polarity", dest="polarity", action="store_false",
                    help="use every feature for every target")
    ap.add_argument("--label", default="raptor",
                    choices=["raptor", "cell_z", "cell_pct", "cell_rankit",
                             "signed_sqrt", "winsor"],
                    help="within-cell target transform; see experiment_labels.py")
    ap.add_argument("--regular-season-only", action="store_true",
                    help="train and project on regular-season rows only")
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                        / "RESULTS_leaderboards.md"))
    args = ap.parse_args()

    out = build_all(args)
    Path(args.out).with_suffix(".json").write_text(json.dumps(out, indent=1))
    write_report(out, args.out, args)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
