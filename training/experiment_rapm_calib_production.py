"""RAPM calibration on the production architectures (best dev@10 ever reported).

The first calibration test used the plain GBM blend (baselines 1.95/3.65).
The best-reported stacks are different animals:
  offense   components+opp: box model -> rap_box_o, on/off model (wowy + opp
            per-100 block) -> rap_onoff_o, minutes-aware ridge combiner,
            3-seed members. Reported test dev@10 1.10 (>=1065 pool).
  defense   lgbm-matched + defend: whole-season training rows only, X+Z+E,
            rounds//3, 3-seed members + ridge. Reported 3.80 (>=1065 pool).

Arms: baseline reproduction, +hat-own, +hat-both. The rapm-hats are identical
to experiment_rapm_calibration.py (aux GBM -> leak-free 2015-19 pooled RAPM,
player-grouped OOF on training rows). For offense the hats are appended to BOTH
component feature sets; for defense to the matched matrix.

Scored on the test seasons' >=1065 pools -- the exact convention behind the
1.10/3.80 numbers.

Run:  python training/experiment_rapm_calib_production.py
"""

import json

import lightgbm as lgb
import numpy as np
from scipy.stats import spearmanr
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare, splits
from experiment_components import (COMPONENT_LABELS, RELATIVE_COLS,
                                   cell_relative, combiner_design, masks_for)
from experiment_oppdef import engineered, per100
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
FLOOR = 1065
SEEDS = (0, 1, 2)
MIN_POSS = 4000
TD = REPO_ROOT / "training"


def lgbm_ridge_members(Xtr, t, Xte, med, params, rounds, seeds=SEEDS,
                       ridge_w=0.25):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, t).predict(
        (B - mu) / sd)
    return [(1 - ridge_w) * lgb.train(
        dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s),
        lgb.Dataset(Xtr, t), num_boost_round=rounds).predict(Xte) + ridge_w * pr
        for s in seeds]


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(TD / "data_fixed" / "components.npz")
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    ofields = [str(f) for f in oppz["fields"]]
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Eopp, _ = engineered(oppz["on_X"], oppz["off_X"], ofields, cells_all)
    on100, off100 = per100(oppz["on_X"], ofields), per100(oppz["off_X"], ofields)
    Bopp = np.hstack([on100, off100, on100 - off100]).astype(np.float32)
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)

    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    isfull = np.array([t in {
        "20140715000000", "20150715000000", "20160715000000", "20170715000000",
        "20180715000000", "20190715000000", "20201101000000", "20210801000000",
        "20220715000000", "20230715000000"} for t in d["timestamp"]])
    tr_m = tr & isfull
    mp = d["mp"].astype(np.float64)
    groups = np.array([f"{p}|{s}" for p, s in
                       zip(d["player"][tr], d["season"][tr])])
    cells_te = np.array([str(s) for s in d["season"][test]])
    el = mp[test] >= FLOOR
    tuned = json.loads((TD / "tuned_params.json").read_text())
    box_mask, onoff_mask = masks_for(feat)
    players = np.array([norm_name(str(p)) for p in d["player"]])

    # ---- rapm hats (identical construction to experiment_rapm_calibration) --
    z = np.load(TD / "data_fixed" / "rapm_recreated.npz", allow_pickle=True)
    ok = np.isfinite(z["orapm4"]) & (z["poss4"] >= MIN_POSS)
    tgt = {"offense": {norm_name(n): v for n, v, k in
                       zip(z["names"], z["orapm4"], ok) if k},
           "defense": {norm_name(n): v for n, v, k in
                       zip(z["names"], z["drapm4"], ok) if k}}
    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, dfz["E"]])}
    hats = {}
    for side in ("offense", "defense"):
        Xf = FEATS[side]
        y_aux = np.array([tgt[side].get(p, np.nan) for p in players])
        m_aux = tr & np.isfinite(y_aux)
        params = dict(tuned[side]["params"], verbose=-1)
        rounds = max(tuned[side]["rounds"] // 3, 150)
        hat = np.full(len(players), np.nan)
        for tri, tei in GroupKFold(n_splits=5).split(
                np.where(m_aux)[0], groups=players[m_aux]):
            rows_tr = np.where(m_aux)[0][tri]
            rows_te = np.where(m_aux)[0][tei]
            m = lgb.train(dict(params, seed=0),
                          lgb.Dataset(Xf[rows_tr], y_aux[rows_tr]),
                          num_boost_round=rounds)
            hat[rows_te] = m.predict(Xf[rows_te])
        full = lgb.train(dict(params, seed=0),
                         lgb.Dataset(Xf[m_aux], y_aux[m_aux]),
                         num_boost_round=rounds)
        hat[~m_aux] = full.predict(Xf[~m_aux])
        hats[side] = hat
        print(f"[{side}] hat ready (OOF rho "
              f"{spearmanr(hat[m_aux], y_aux[m_aux]).statistic:+.3f})",
              flush=True)
    own = {s: hats[s].reshape(-1, 1) for s in hats}
    both = np.column_stack([hats["offense"], hats["defense"]])

    out = {}

    # ---- offense: components+opp -------------------------------------------
    params_o = dict(tuned["offense"]["params"], verbose=-1)
    rounds_o = tuned["offense"]["rounds"]
    y_o = d[TARGETS["offense"]]
    for arm, extra in (("baseline", None), ("+hat-own", own["offense"]),
                       ("+hat-both", both)):
        Xb = X[:, box_mask]
        Xo = np.hstack([X[:, onoff_mask], Bopp])
        if extra is not None:
            Xb = np.hstack([Xb, extra])
            Xo = np.hstack([Xo, extra])
        oof = {}
        for tag, Xs, labname in (("box", Xb, COMPONENT_LABELS["offense"][0]),
                                 ("onoff", Xo, COMPONENT_LABELS["offense"][1])):
            labv = comp[labname]
            ms = np.nanmedian(Xs[tr], axis=0)
            ms = np.where(np.isfinite(ms), ms, 0.0)
            o = np.full(int(tr.sum()), np.nan)
            Xs_tr = Xs[tr]
            for tri, vai in GroupKFold(n_splits=3).split(Xs_tr, labv[tr],
                                                         groups=groups):
                o[vai] = lgbm_ridge_members(Xs_tr[tri], labv[tr][tri],
                                            Xs_tr[vai], ms, params_o, rounds_o,
                                            seeds=(0,))[0]
            oof[tag] = o
            oof[tag + "_te"] = lgbm_ridge_members(Xs_tr, labv[tr], Xs[test],
                                                  ms, params_o, rounds_o)
        cbn = Ridge(alpha=1.0).fit(
            combiner_design(oof["box"], oof["onoff"], mp[tr]), y_o[tr])
        members = [cbn.predict(combiner_design(oof["box_te"][i],
                                               oof["onoff_te"][i], mp[test]))
                   for i in range(len(SEEDS))]
        s = score_cells(y_o[test][el], np.mean(members, axis=0)[el],
                        cells_te[el])
        out[f"offense|{arm}"] = {k: (int(v) if isinstance(v, (int, np.integer))
                                     else round(float(v), 4))
                                 for k, v in s.items()}
        print(f"[offense] {arm:<10} dev@10={s['dev@10']:5.2f} "
              f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40  (combiner w="
              f"{np.round(cbn.coef_[:2], 3)})", flush=True)

    # ---- defense: lgbm-matched + defend ------------------------------------
    params_d = dict(tuned["defense"]["params"], verbose=-1)
    rounds_d = max(tuned["defense"]["rounds"] // 3, 150)
    y_d = d[TARGETS["defense"]]
    for arm, extra in (("baseline", None), ("+hat-own", own["defense"]),
                       ("+hat-both", both)):
        Xd = np.hstack([X, Z, dfz["E"]] + ([extra] if extra is not None else []))
        md = np.nanmedian(Xd[tr_m], axis=0)
        md = np.where(np.isfinite(md), md, 0.0)
        members = lgbm_ridge_members(Xd[tr_m], y_d[tr_m], Xd[test], md,
                                     params_d, rounds_d)
        s = score_cells(y_d[test][el], np.mean(members, axis=0)[el],
                        cells_te[el])
        out[f"defense|{arm}"] = {k: (int(v) if isinstance(v, (int, np.integer))
                                     else round(float(v), 4))
                                 for k, v in s.items()}
        print(f"[defense] {arm:<10} dev@10={s['dev@10']:5.2f} "
              f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40", flush=True)

    (TD / "RESULTS_rapm_calib_production.json").write_text(
        json.dumps(out, indent=1))
    print("\nwrote RESULTS_rapm_calib_production.json", flush=True)


if __name__ == "__main__":
    main()
