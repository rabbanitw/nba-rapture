"""Leaderboards from the component-architecture models. Offense and defense only.

The model (see RESULTS_components.md and RESULTS_raptor_research.md):

  offense   RAPTOR's own two-part structure, learned from 538's published component
            labels. A box model (459 pbp + tracking + context features -> rap_box_o)
            and an on/off model (689 wowy features -> rap_onoff_o), combined by a
            ridge on out-of-fold component predictions with log-minutes interaction
            terms. The combiner learns box ~0.95 / on-off ~0.19 -- the box-heavy
            weighting 538 described -- and the output is in RAPTOR units because the
            combiner's target is the blended rap_o.

  defense   The decomposition fails on defense -- rap_box_d is barely predictable
            from our features (R^2 +0.71 vs +0.90 for offense) because 538's
            defensive box inputs (nearest-defender shot data, positional matchups)
            are not in any feed we have. So defense stays a direct model, plus
            within-cell z-scores of a dozen rate stats: trees cannot see cell
            context, and the standardisation supplies the era-relative meaning of a
            rate. Replicated gains: dev@20 ~8.0 vs ~10.5, MAE ~0.70 vs ~0.74.

Both were selected on the held-out 2013-14 / 2014-15 regular seasons and replicated
on disjoint seeds before being used here (RESULTS_components.md, /tmp/replicate.log).

Regular season only throughout: trained on regular-season rows, projected onto
regular-season cells.

Run:  python training/leaderboard_components.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT, get_collection
from experiment_combined import prepare, splits
from experiment_components import (COMPONENT_LABELS, RELATIVE_COLS, cell_relative,
                                   combiner_design, masks_for)
from experiment_topk_rank import score_cells
from leaderboard_report import TAU_K, labeled_table, tau_stats, unlabeled_table
from predict_seasons import (DROP_FEATURES, build_unlabeled, carry_over_positions,
                             eligibility, impute_positions)
from seasons import UNLABELED_SNAPSHOTS
from train_rapture import TARGETS, add_context, normalize_rates

RS_MIN, PO_MIN = 50, 10
TOP_N = 50
SEEDS = (0, 1, 2)


def blend(Xtr, t, Xte, med, params, rounds, seeds=SEEDS, ridge_w=0.25):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, t).predict(
        (B - mu) / sd)
    ps = [lgb.train(dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s),
                    lgb.Dataset(Xtr, t), num_boost_round=rounds).predict(Xte)
          for s in seeds]
    return (1 - ridge_w) * np.mean(ps, axis=0) + ridge_w * pr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--top-n", type=int, default=TOP_N)
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_leaderboards_components.md"))
    args = ap.parse_args()

    coll = get_collection()
    X0, feat0, d = prepare(args.datadir)
    raw_names = list(d["feat_names"])
    raw_keep = [i for i, n in enumerate(raw_names) if n not in DROP_FEATURES]
    raw_feat = [raw_names[i] for i in raw_keep]
    d["X"] = d["X"][:, raw_keep]
    comp = np.load(Path(args.datadir) / "components.npz")

    # Unlabeled 2023-26 cells in the raw column order, positions attached.
    Xn, meta = build_unlabeled(coll, raw_feat, list(UNLABELED_SNAPSHOTS))
    carry_over_positions(coll, meta)
    impute_positions(d["X"], d["pos"], Xn, meta)
    floors = eligibility(coll)

    # One joint pipeline for labeled + unlabeled: normalize (scaling decisions from
    # labeled non-test rows only), then context features, then identifier-free masks.
    n_lab = d["X"].shape[0]
    X_all = np.vstack([d["X"], Xn])
    is_train = np.concatenate([~d["test"].astype(bool),
                               np.zeros(Xn.shape[0], dtype=bool)])
    X_all = normalize_rates(X_all, raw_feat, is_train)
    ctx = {k: np.concatenate([d[k], np.array([m[k] for m in meta],
                                             dtype=d[k].dtype)])
           for k in ("pos", "mp", "timestamp", "season_type")}
    X_all, feat = add_context(X_all, raw_feat, ctx, "combined")

    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs_lab = d["season_type"] == "Regular season"
    tr = (fit | val) & rs_lab
    test = test & rs_lab
    meta_rs = np.array([m["season_type"] == "Regular season" for m in meta])

    Xlab, Xnew = X_all[:n_lab], X_all[n_lab:]
    med = np.nanmedian(Xlab[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    box_mask, onoff_mask = masks_for(feat)
    groups = np.array([f"{p}|{s}" for p, s in
                       zip(d["player"][tr], d["season"][tr])])
    cells_lab = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    cells_new = np.array([f"{m['timestamp']}|{m['season_type']}" for m in meta])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    mp_lab = d["mp"].astype(np.float64)
    mp_new = np.array([m["mp"] for m in meta], dtype=np.float64)
    tuned = json.loads(Path(args.tuned).read_text())
    print(f"labeled X={Xlab.shape} train={tr.sum()} test={test.sum()}  "
          f"unlabeled={Xnew.shape}", flush=True)

    preds_te, preds_new, model_notes = {}, {}, {}

    # ---------------- offense: component decomposition ----------------------
    params = dict(tuned["offense"]["params"], verbose=-1)
    rounds = tuned["offense"]["rounds"]
    y = d[TARGETS["offense"]]
    box_lab, onoff_lab = (comp[c] for c in COMPONENT_LABELS["offense"])
    Xb, Xo = Xlab[:, box_mask], Xlab[:, onoff_mask]
    medb, medo = med[box_mask], med[onoff_mask]

    print("[offense] out-of-fold component predictions ...", flush=True)
    box_oof = np.full(int(tr.sum()), np.nan)
    onoff_oof = np.full(int(tr.sum()), np.nan)
    Xb_tr, Xo_tr = Xb[tr], Xo[tr]
    bl_tr, ol_tr = box_lab[tr], onoff_lab[tr]
    for tri, vai in GroupKFold(n_splits=4).split(Xb_tr, bl_tr, groups=groups):
        box_oof[vai] = blend(Xb_tr[tri], bl_tr[tri], Xb_tr[vai], medb, params,
                             rounds, seeds=(0,))
        onoff_oof[vai] = blend(Xo_tr[tri], ol_tr[tri], Xo_tr[vai], medo, params,
                               rounds, seeds=(0,))
    combiner = Ridge(alpha=1.0).fit(
        combiner_design(box_oof, onoff_oof, mp_lab[tr]), y[tr])

    print("[offense] final component models ...", flush=True)
    box_te = blend(Xb_tr, bl_tr, Xb[test], medb, params, rounds)
    onoff_te = blend(Xo_tr, ol_tr, Xo[test], medo, params, rounds)
    box_new = blend(Xb_tr, bl_tr, Xnew[:, box_mask], medb, params, rounds)
    onoff_new = blend(Xo_tr, ol_tr, Xnew[:, onoff_mask], medo, params, rounds)
    preds_te["offense"] = combiner.predict(
        combiner_design(box_te, onoff_te, mp_lab[test]))
    preds_new["offense"] = combiner.predict(
        combiner_design(box_new, onoff_new, mp_new))
    model_notes["offense"] = (f"components: box {box_mask.sum()} feats + on/off "
                              f"{onoff_mask.sum()} feats, combiner weights "
                              f"box={combiner.coef_[0]:.3f} "
                              f"onoff={combiner.coef_[1]:.3f}")

    # ---------------- defense: direct + cell-relative -----------------------
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = tuned["defense"]["rounds"]
    y = d[TARGETS["defense"]]
    Zl = cell_relative(Xlab, feat, cells_lab, RELATIVE_COLS)
    Zn = cell_relative(Xnew, feat, cells_new, RELATIVE_COLS)
    Xz_lab = np.hstack([Xlab, Zl])
    Xz_new = np.hstack([Xnew, Zn])
    medz = np.concatenate([med, np.zeros(Zl.shape[1])])
    print("[defense] direct + cell-relative ...", flush=True)
    preds_te["defense"] = blend(Xz_lab[tr], y[tr], Xz_lab[test], medz, params, rounds)
    preds_new["defense"] = blend(Xz_lab[tr], y[tr], Xz_new, medz, params, rounds)
    model_notes["defense"] = (f"direct + {Zl.shape[1]} within-cell z-score features "
                              f"(components rejected: box R2 +0.71 on defense)")

    # ---------------- score and write ---------------------------------------
    out = {"labeled": {}, "unlabeled": {}, "notes": model_notes, "floors": floors}
    for target in ("offense", "defense"):
        y = d[TARGETS[target]]
        s = score_cells(y[test], preds_te[target], cells_te)
        print(f"[{target}] test: dev@10={s['dev@10']:.2f} dev@20={s['dev@20']:.2f} "
              f"tau@10={s['tau@10']:+.3f} MAE={s['mae']:.3f}", flush=True)
        players_te = d["player"][test]
        for cell in np.unique(cells_te):
            m = cells_te == cell
            tt, tu = tau_stats(y[test][m], preds_te[target][m])
            rows = labeled_table(players_te[m], y[test][m], preds_te[target][m],
                                 args.top_n)
            out["labeled"][f"{target}|{cell}"] = {
                "rows": rows, "tau_true30": tt, "tau_union30": tu,
                "pool": int(m.sum()),
                "hits30": sum(1 for r in rows[:TAU_K] if r["true_rank"] <= TAU_K),
                "metrics": s,
            }
        df = pd.DataFrame(meta)[meta_rs].copy()
        df["est"] = preds_new[target][meta_rs]
        for season in sorted(df.season.unique()):
            g = df[df.season == season]
            g = g[g.mp >= floors["Regular season"]]
            out["unlabeled"][f"{target}|{season}"] = {
                "rows": unlabeled_table(g.player.values, g.est.values, g.mp.values,
                                        args.top_n),
                "pool": int(len(g)),
            }

    Path(args.out).with_suffix(".json").write_text(json.dumps(out, indent=1))
    write_report(out, args.out)
    print(f"\nwrote {args.out}", flush=True)


def write_report(out, path):
    L = []
    A = L.append
    A("# Leaderboards from the component-architecture models")
    A("")
    A("## The model")
    A("")
    A("**Offense** copies RAPTOR's own structure (RESULTS_raptor_research.md): a box")
    A("model and an on/off model trained against 538's published component labels")
    A("(`rap_box_o`, `rap_onoff_o`), combined by a ridge with log-minutes interaction")
    A(f"terms. {out['notes']['offense']}. The learned box-heavy weighting matches")
    A("538's stated design; output is in RAPTOR points because the combiner's target")
    A("is the blended rating.")
    A("")
    A("**Defense** stays a direct model plus within-cell z-scores of rate stats —")
    A(f"{out['notes']['defense']}. The defensive box component cannot be reproduced")
    A("without nearest-defender and positional-matchup data, which no feed we scrape")
    A("carries; that is the known ceiling on defensive ordering.")
    A("")
    A("Both choices were selected on held-out 2013-14/2014-15 regular seasons and")
    A("replicated on disjoint seeds. Regular season only, trained and projected.")
    A("")
    A("## Held-out test boards (truth available)")
    A("")
    for key, v in out["labeled"].items():
        target, cell = key.split("|", 1)
        season = cell.split("|")[0]
        m = v["metrics"]
        A(f"### {season} regular season — {target}, top {len(v['rows'])}")
        A("")
        A(f"> pool {v['pool']} · tau(true30) {v['tau_true30']:+.3f} · "
          f"hits@30 {v['hits30']}/30 · dev@10 {m['dev@10']:.2f} · MAE {m['mae']:.3f}")
        A("")
        A("| pos | projected | est | true | true rank | Δrank | actual at pos | true |")
        A("|---:|---|---:|---:|---:|---:|---|---:|")
        for r in v["rows"]:
            A(f"| {r['pos']} | {r['projected']} | {r['est']:+.2f} | {r['true']:+.2f} | "
              f"{r['true_rank']} | {r['drank']:+d} | {r['actual_at_pos']} | "
              f"{r['actual_true']:+.2f} |")
        A("")
    A("## Projected boards, 2023-26 regular seasons (no truth exists)")
    A("")
    for key, v in out["unlabeled"].items():
        target, season = key.split("|")
        A(f"### {season} regular season — {target}, top {len(v['rows'])} (projected)")
        A("")
        A(f"> pool {v['pool']} above the minutes floor")
        A("")
        A("| pos | projected | est | mp |")
        A("|---:|---|---:|---:|")
        for r in v["rows"]:
            A(f"| {r['pos']} | {r['projected']} | {r['est']:+.2f} | {r['mp']:.0f} |")
        A("")
    Path(path).write_text("\n".join(L))


if __name__ == "__main__":
    main()
