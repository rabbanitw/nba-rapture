"""Leave-one-season-out over the ten labeled seasons, plus per-position confidence.

For each season with an end-of-year 538 RAPTOR table (2013-14 .. 2022-23), the final
models are trained on the other nine seasons' regular-season rows and scored on the
held-out season's whole-season cell. This is the honest generalization estimate for
the production stack -- every season gets a turn as unseen data, not just 2013-14
and 2014-15.

Models:
  offense          components+opp: box -> rap_box_o, (wowy + opponent block) ->
                   rap_onoff_o, minutes-aware ridge combiner on out-of-fold preds.
  defense members  lgbm-full  (all rows, + cell-relative + defend-eng)
                   lgbm-matched (whole-season rows only, same features)
                   catboost   (same features)
                   ens        their average. The LOSO decides between them with ten
                   seasons of evidence instead of two.

Confidence at every position, from two sources of uncertainty:
  seed spread      each model is refit across seeds; the member predictions differ.
  residual pool    LOSO residuals from the OTHER nine folds, bucketed by minutes
                   tercile -- an empirical, out-of-sample error distribution that
                   never uses the fold's own truth.

Monte Carlo: draw prediction = seed-member (uniform) + residual (bucket-matched
bootstrap), re-rank the cell, repeat N times. Per board position that yields the
player's rank distribution: a 90% CI on their true rank and P(truly top-10/top-30).

Because truth exists for every fold, the confidence machinery is CALIBRATED, not
asserted: the report states what fraction of 90% CIs contain the true rank.

Run:  python training/loso_confidence.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from catboost import CatBoostRegressor
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import (COMPONENT_LABELS, RELATIVE_COLS, cell_relative,
                                   combiner_design, masks_for)
from experiment_oppdef import engineered, per100
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from seasons import FULL_SEASON_SNAPSHOTS
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
LABELED = {"2013-14": "20140715000000", "2014-15": "20150715000000",
           "2015-16": "20160715000000", "2016-17": "20170715000000",
           "2017-18": "20180715000000", "2018-19": "20190715000000",
           "2019-20": "20201101000000", "2020-21": "20210801000000",
           "2021-22": "20220715000000", "2022-23": "20230715000000"}
SEEDS = (0, 1, 2)
CAT_SEEDS = (0, 1)
MC_DRAWS = 2000


def lgbm_ridge(Xtr, t, Xte, med, params, rounds, seed, ridge_pred, ridge_w=0.25):
    p = dict(params, seed=seed, bagging_seed=seed, feature_fraction_seed=seed)
    pl = lgb.train(p, lgb.Dataset(Xtr, t), num_boost_round=rounds).predict(Xte)
    return (1 - ridge_w) * pl + ridge_w * ridge_pred


def ridge_fit(Xtr, t, Xte, med):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    return RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, t).predict(
        (B - mu) / sd)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_loso_confidence.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(Path(args.datadir) / "components.npz")
    dfz = np.load(Path(args.datadir) / "defend.npz", allow_pickle=True)
    E = dfz["E"]
    opp = np.load(Path(args.datadir) / "wowyopp.npz", allow_pickle=True)
    on100 = per100(opp["on_X"], [str(f) for f in opp["fields"]])
    off100 = per100(opp["off_X"], [str(f) for f in opp["fields"]])
    Bopp = np.hstack([on100, off100, on100 - off100]).astype(np.float32)

    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    usable = (fit | val | test) & rs
    isfull = np.array([t in FULL_SEASON_SNAPSHOTS for t in d["timestamp"]])
    seasons = np.array([str(s) for s in d["season"]])
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    box_mask, onoff_mask = masks_for(feat)
    mp = d["mp"].astype(np.float64)
    tuned = json.loads(Path(args.tuned).read_text())
    params_o = dict(tuned["offense"]["params"], verbose=-1)
    rounds_o = tuned["offense"]["rounds"]
    params_d = dict(tuned["defense"]["params"], verbose=-1)
    rounds_d = tuned["defense"]["rounds"]
    y_o = d[TARGETS["offense"]]
    y_d = d[TARGETS["defense"]]
    box_lab, onoff_lab = (comp[c] for c in COMPONENT_LABELS["offense"])

    Xb = X[:, box_mask]
    Xo = np.hstack([X[:, onoff_mask], Bopp])
    Xd = np.hstack([X, Z, E])

    results = {}       # season -> per-model metrics + per-player detail
    for season, stamp in LABELED.items():
        tr = usable & (seasons != season)
        tr_m = tr & isfull
        ev = usable & (d["timestamp"] == stamp)
        groups = np.array([f"{p}|{s}" for p, s in
                           zip(d["player"][tr], seasons[tr])])
        print(f"\n=== fold {season}: train={tr.sum()} matched={tr_m.sum()} "
              f"eval={ev.sum()} ===", flush=True)

        # ---- offense: components+opp ------------------------------------
        comp_members = []
        oofs = {}
        for tag, Xs, lab in (("box", Xb, box_lab), ("onoff", Xo, onoff_lab)):
            ms = np.nanmedian(Xs[tr], axis=0)
            ms = np.where(np.isfinite(ms), ms, 0.0)
            oof = np.full(int(tr.sum()), np.nan)
            Xs_tr, lab_tr = Xs[tr], lab[tr]
            rp_parts = {}
            for tri, vai in GroupKFold(n_splits=3).split(Xs_tr, lab_tr,
                                                         groups=groups):
                rp = ridge_fit(Xs_tr[tri], lab_tr[tri], Xs_tr[vai], ms)
                oof[vai] = lgbm_ridge(Xs_tr[tri], lab_tr[tri], Xs_tr[vai], ms,
                                      params_o, rounds_o, 0, rp)
            oofs[tag] = oof
            rp_ev = ridge_fit(Xs_tr, lab_tr, Xs[ev], ms)
            oofs[tag + "_ev"] = [lgbm_ridge(Xs_tr, lab_tr, Xs[ev], ms, params_o,
                                            rounds_o, s, rp_ev) for s in SEEDS]
        cb = Ridge(alpha=1.0).fit(
            combiner_design(oofs["box"], oofs["onoff"], mp[tr]), y_o[tr])
        for s_i in range(len(SEEDS)):
            comp_members.append(cb.predict(combiner_design(
                oofs["box_ev"][s_i], oofs["onoff_ev"][s_i], mp[ev])))
        print(f"  offense combiner w={np.round(cb.coef_[:2], 3)}", flush=True)

        # ---- defense members --------------------------------------------
        md = np.nanmedian(Xd[tr], axis=0)
        md = np.where(np.isfinite(md), md, 0.0)
        rp_full = ridge_fit(Xd[tr], y_d[tr], Xd[ev], md)
        full_members = [lgbm_ridge(Xd[tr], y_d[tr], Xd[ev], md, params_d,
                                   rounds_d, s, rp_full) for s in SEEDS]
        md_m = np.nanmedian(Xd[tr_m], axis=0)
        md_m = np.where(np.isfinite(md_m), md_m, 0.0)
        rp_m = ridge_fit(Xd[tr_m], y_d[tr_m], Xd[ev], md_m)
        matched_members = [lgbm_ridge(Xd[tr_m], y_d[tr_m], Xd[ev], md_m, params_d,
                                      max(rounds_d // 3, 150), s, rp_m)
                           for s in SEEDS]
        cat_members = []
        for s in CAT_SEEDS:
            m = CatBoostRegressor(iterations=1500, learning_rate=0.03, depth=6,
                                  l2_leaf_reg=5.0, random_seed=s, verbose=False,
                                  allow_writing_files=False)
            m.fit(Xd[tr], y_d[tr])
            cat_members.append(m.predict(Xd[ev]))

        det = {"players": [str(p) for p in d["player"][ev]],
               "mp": mp[ev].tolist(),
               "y_off": y_o[ev].tolist(), "y_def": y_d[ev].tolist(),
               "off_members": [m.tolist() for m in comp_members],
               "def_members": {
                   "lgbm-full": [m.tolist() for m in full_members],
                   "lgbm-matched": [m.tolist() for m in matched_members],
                   "catboost": [m.tolist() for m in cat_members]}}
        results[season] = det

        cell = np.zeros(int(ev.sum()), dtype="U4")
        for name, mem in (("offense", comp_members),
                          ("def lgbm-full", full_members),
                          ("def lgbm-matched", matched_members),
                          ("def catboost", cat_members)):
            yv = y_o[ev] if name == "offense" else y_d[ev]
            s = score_cells(yv, np.mean(mem, axis=0), cell)
            print(f"  {name:<17} dev@10={s['dev@10']:5.2f} "
                  f"tau@10={s['tau@10']:+.3f} MAE={s['mae']:.3f} "
                  f"hits@10={s['hits@10']}/10 hits@30={s['hits@30']}/30",
                  flush=True)
        allm = [np.mean(m, axis=0) for m in (full_members, matched_members,
                                             cat_members)]
        s = score_cells(y_d[ev], np.mean(allm, axis=0), cell)
        print(f"  {'def ens(3)':<17} dev@10={s['dev@10']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} MAE={s['mae']:.3f} "
              f"hits@10={s['hits@10']}/10 hits@30={s['hits@30']}/30", flush=True)

    Path(args.datadir, "loso_detail.json").write_text(json.dumps(results))
    print("\nwrote loso_detail.json -- run confidence_report.py next")


if __name__ == "__main__":
    main()
