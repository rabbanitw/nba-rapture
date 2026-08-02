"""RAPTOR-faithful architecture: predict the two components, then combine.

What the research established (see RESULTS_raptor_research.md): RAPTOR is not one
model. It is a box/tracking component -- a regression of box, play-by-play and
tracking stats against six-year RAPM -- and a separate on/off component built from
teammate/opponent-adjusted, luck-adjusted lineup data, combined with the box side
weighted more heavily. 538 published both components per player, our 538 documents
carry them (rap_box_o/d, rap_onoff_o/d), and every model so far has ignored them and
predicted the blended number directly from everything at once.

Direct prediction makes the model learn g(f_box(x), f_onoff(x)) end to end from a
noisy blended label. The decomposition learns each part against its own cleaner
label -- rap_box is a deterministic-ish function of stats we largely have, and
rap_onoff is a function of the wowy block -- and then learns the small combiner g,
where 538 applied its minutes-dependent weighting.

Arms, offense and defense only:

  direct          all features -> blended label. The current best, as the baseline.
  components      box-feature model -> rap_box; wowy-feature model -> rap_onoff;
                  ridge combiner on out-of-fold component predictions with
                  log-minutes interactions (the combination is minutes-dependent
                  in the real thing).
  comp+direct     average of the two above. They see the label through different
                  structures, so their errors need not be shared.
  cell-relative   direct, plus within-cell z-scores of key rate stats. Trees cannot
                  see cell context: a 57% TS season means something different in
                  2015 and 2021, and no split on a raw column can express that.
  prior-season    direct, plus the player's previous-season out-of-fold prediction.
                  538's projections lean on multi-season priors; a season of history
                  is exactly the stabiliser a noisy top-10 ordering lacks.
  catboost        a genuinely different GBM (oblivious trees, ordered boosting),
                  native NaN handling, on all features.

Run:  python training/experiment_components.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
COMPONENT_LABELS = {
    "offense": ("rap_box_o", "rap_onoff_o"),
    "defense": ("rap_box_d", "rap_onoff_d"),
}
# Within-cell z-scored copies for the cell-relative arm: rates whose meaning drifts
# with the league environment.
RELATIVE_COLS = ["pbp|TsPct", "pbp|EfgPct", "pbp|Usage", "pbp|ShotQualityAvg",
                 "pbp|OnDefRtg", "pbp|OnOffRtg", "pbp|Fg3Pct", "pbp|Points",
                 "wowy_diff|PlusMinus", "wowy_diff|Points", "wowy_off|PlusMinus",
                 "pbp|DefFGReboundPct"]


def block_of(name):
    return name.split("|", 1)[0]


def masks_for(feat):
    box = np.array([block_of(n) in ("pbp", "ctx") or block_of(n).startswith("track:")
                    for n in feat])
    onoff = np.array([block_of(n) in ("wowy_on", "wowy_off", "wowy_diff", "ctx")
                      for n in feat])
    return box, onoff


def blend_fit(X, t, Xte, med, params, rounds, seeds=(0, 1, 2), ridge_w=0.25):
    """Seed-averaged LGBM + ridge blend, returning (test pred, per-seed models)."""
    A = np.where(np.isfinite(X), X, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, t).predict(
        (B - mu) / sd)
    ps = []
    for s in seeds:
        p = dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s)
        ps.append(lgb.train(p, lgb.Dataset(X, t), num_boost_round=rounds).predict(Xte))
    return (1 - ridge_w) * np.mean(ps, axis=0) + ridge_w * pr


def oof_predict(X, t, groups, med, params, rounds, folds=4):
    """Out-of-fold predictions on the training rows, for combiner fitting."""
    out = np.full(len(t), np.nan)
    for tr_i, va_i in GroupKFold(n_splits=folds).split(X, t, groups=groups):
        out[va_i] = blend_fit(X[tr_i], t[tr_i], X[va_i], med, params, rounds,
                              seeds=(0,))
    return out


def combiner_design(box_p, onoff_p, mp):
    lm = np.log1p(mp)
    lm = (lm - lm.mean()) / (lm.std() or 1.0)
    return np.column_stack([box_p, onoff_p, lm, box_p * lm, onoff_p * lm])


def cell_relative(X, feat, cells, cols):
    """Append within-cell z-scores of the named columns."""
    idx = {n: i for i, n in enumerate(feat)}
    added = []
    for c in cols:
        if c not in idx:
            continue
        v = X[:, idx[c]].astype(np.float64)
        z = np.full(len(v), np.nan)
        for cell in np.unique(cells):
            m = cells == cell
            vv = v[m]
            mu = np.nanmean(vv)
            sd = np.nanstd(vv)
            z[m] = (vv - mu) / (sd or 1.0)
        added.append(z)
    return np.column_stack(added).astype(np.float32) if added else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--targets", nargs="*", default=["offense", "defense"])
    ap.add_argument("--arms", nargs="*",
                    default=["direct", "components", "comp+direct",
                             "cell-relative", "prior-season", "catboost"])
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_components.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(Path(args.datadir) / "components.npz")
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    tr_idx, te_idx = np.where(tr)[0], np.where(test)[0]
    cells_tr_all = np.array([f"{t}|{s}" for t, s in
                             zip(d["timestamp"], d["season_type"])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    groups = np.array([f"{p}|{s}" for p, s in
                       zip(d["player"][tr], d["season"][tr])])
    mp = d["mp"].astype(np.float64)
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    box_mask, onoff_mask = masks_for(feat)
    tuned = json.loads(Path(args.tuned).read_text())
    print(f"X={X.shape} train={tr.sum()} test={test.sum()}  "
          f"box feats={box_mask.sum()} onoff feats={onoff_mask.sum()}", flush=True)

    rows = []

    def record(target, name, p, extra=""):
        y = d[TARGETS[target]]
        s = score_cells(y[test], p, cells_te)
        rows.append({"target": target, "arm": name, **s})
        print(f"  {name:<14} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40  {extra}", flush=True)

    for target in args.targets:
        y = d[TARGETS[target]]
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = tuned[target]["rounds"]
        box_lab = comp[COMPONENT_LABELS[target][0]]
        onoff_lab = comp[COMPONENT_LABELS[target][1]]
        print(f"\n=== {target} ===", flush=True)

        direct_pred = None
        if "direct" in args.arms or "comp+direct" in args.arms:
            direct_pred = blend_fit(X[tr], y[tr], X[test], med, params, rounds)
            record(target, "direct", direct_pred)

        if "components" in args.arms or "comp+direct" in args.arms:
            Xb, Xo = X[:, box_mask], X[:, onoff_mask]
            medb, medo = med[box_mask], med[onoff_mask]
            # component fit quality is itself diagnostic
            box_te = blend_fit(Xb[tr], box_lab[tr], Xb[test], medb, params, rounds)
            onoff_te = blend_fit(Xo[tr], onoff_lab[tr], Xo[test], medo, params, rounds)
            r2b = 1 - np.nanvar(box_lab[test] - box_te) / np.nanvar(box_lab[test])
            r2o = 1 - np.nanvar(onoff_lab[test] - onoff_te) / np.nanvar(onoff_lab[test])
            box_oof = oof_predict(Xb[tr], box_lab[tr], groups, medb, params, rounds)
            onoff_oof = oof_predict(Xo[tr], onoff_lab[tr], groups, medo, params, rounds)
            comb = Ridge(alpha=1.0).fit(
                combiner_design(box_oof, onoff_oof, mp[tr]), y[tr])
            comp_pred = comb.predict(combiner_design(box_te, onoff_te, mp[test]))
            record(target, "components", comp_pred,
                   extra=f"[box R2={r2b:+.3f} onoff R2={r2o:+.3f} "
                         f"w={np.round(comb.coef_[:2], 3)}]")
            if "comp+direct" in args.arms and direct_pred is not None:
                record(target, "comp+direct", 0.5 * direct_pred + 0.5 * comp_pred)

        if "cell-relative" in args.arms:
            Z = cell_relative(X, feat, cells_tr_all, RELATIVE_COLS)
            Xz = np.hstack([X, Z])
            medz = np.concatenate([med, np.zeros(Z.shape[1])])
            record(target, "cell-relative",
                   blend_fit(Xz[tr], y[tr], Xz[test], medz, params, rounds))

        if "prior-season" in args.arms:
            # previous-season prediction per player, never using that season's labels
            seasons = np.array([str(s) for s in d["season"]])
            prior = np.full(len(y), np.nan)
            season_list = sorted(set(seasons[tr]))
            base_by_ps = {}
            for s in season_list:
                held = tr & (seasons == s)
                m = lgb.train(dict(params, seed=0),
                              lgb.Dataset(X[tr & (seasons != s)], y[tr & (seasons != s)]),
                              num_boost_round=rounds)
                p = m.predict(X[held])
                for pl, v in zip(d["player"][held], p):
                    base_by_ps.setdefault((str(pl), s), []).append(v)
            full = lgb.train(dict(params, seed=0), lgb.Dataset(X[tr], y[tr]),
                             num_boost_round=rounds)
            p14 = full.predict(X[test & (seasons == "2013-14")])
            for pl, v in zip(d["player"][test & (seasons == "2013-14")], p14):
                base_by_ps.setdefault((str(pl), "2013-14"), []).append(v)

            def prev_season(s):
                y0 = int(s[:4])
                return f"{y0 - 1}-{str(y0)[2:]}"
            for i in np.concatenate([tr_idx, te_idx]):
                k = (str(d["player"][i]), prev_season(str(seasons[i])))
                if k in base_by_ps:
                    prior[i] = float(np.mean(base_by_ps[k]))
            n_have = np.isfinite(prior[tr]).sum()
            Xp = np.hstack([X, prior.reshape(-1, 1).astype(np.float32),
                            np.isfinite(prior).reshape(-1, 1).astype(np.float32)])
            medp = np.concatenate([med, [0.0, 0.0]])
            record(target, "prior-season",
                   blend_fit(Xp[tr], y[tr], Xp[test], medp, params, rounds),
                   extra=f"[prior available for {n_have}/{tr.sum()} train rows]")

        if "catboost" in args.arms:
            from catboost import CatBoostRegressor
            ps = []
            for s in range(3):
                m = CatBoostRegressor(iterations=1500, learning_rate=0.03, depth=6,
                                      l2_leaf_reg=5.0, random_seed=s, verbose=False,
                                      allow_writing_files=False)
                m.fit(X[tr], y[tr])
                ps.append(m.predict(X[test]))
            record(target, "catboost", np.mean(ps, axis=0))

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    write_report(rows, args.out)
    print(f"\nwrote {args.out}")


def write_report(rows, out):
    L = []
    A = L.append
    A("# RAPTOR-faithful components, cell-relative features, priors")
    A("")
    A("Offense and defense only, regular season only, all features, seed-averaged.")
    A("See the module docstring and RESULTS_raptor_research.md for the rationale.")
    A("")
    for target in sorted({r["target"] for r in rows}):
        A(f"## {target}")
        A("")
        A("| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |")
        A("|---|---:|---:|---:|---:|---:|---:|---:|")
        for r in sorted([x for x in rows if x["target"] == target],
                        key=lambda r: r["dev@10"]):
            A(f"| {r['arm']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
              f"{r['tau@10']:+.3f} | {r['tau@20']:+.3f} | {r['mae']:.3f} | "
              f"{r['hits@10']}/20 | {r['hits@20']}/40 |")
        A("")
    Path(out).write_text("\n".join(L))


if __name__ == "__main__":
    main()
