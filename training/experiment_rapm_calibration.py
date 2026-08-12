"""Calibration against long-term RAPM, per 538's construction.

538's box component IS a prediction of six-year RAPM from box/tracking/pbp
variables. This transplants that: an auxiliary GBM maps our per-season features
to the recreated pooled RAPM (data_fixed/rapm_recreated.npz), and its
prediction ("rapm-hat") becomes an input feature to the production model that
predicts the RAPTOR label.

Leakage guards:
  target   the aux target is the 4-year pool (2015-16..2018-19) -- no possession
           from the 2013-14/2014-15 test seasons touches it
  rows     aux-model hats for training rows are OUT-OF-FOLD (5-fold grouped by
           player, so a player's own target never shapes his own hat); test-row
           hats come from the aux model fit on all training rows
  identity there are no player-identity features, so the aux model can only
           express RAPM as a function of statistical profile

Arms per target (test seasons 2013-14/2014-15 RS, standard metrics):
  baseline    production features
  +hat-own    + rapm-hat for the same side (offense hat for offense model)
  +hat-both   + both hats (role information may cross sides)

Run:  python training/experiment_rapm_calibration.py
"""

import json

import lightgbm as lgb
import numpy as np
from scipy.stats import spearmanr
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend, engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
MIN_POSS = 4000
TD = REPO_ROOT / "training"


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    opp = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Eopp, _ = engineered(opp["on_X"], opp["off_X"],
                         [str(f) for f in opp["fields"]], cells_all)
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, dfz["E"]])}
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    cells_te = np.array([str(s) for s in d["season"][test]])
    tuned = json.loads((TD / "tuned_params.json").read_text())
    players = np.array([norm_name(str(p)) for p in d["player"]])

    z = np.load(TD / "data_fixed" / "rapm_recreated.npz", allow_pickle=True)
    ok = np.isfinite(z["orapm4"]) & (z["poss4"] >= MIN_POSS)
    tgt = {"offense": {norm_name(n): v for n, v, k in
                       zip(z["names"], z["orapm4"], ok) if k},
           "defense": {norm_name(n): v for n, v, k in
                       zip(z["names"], z["drapm4"], ok) if k}}
    print(f"aux targets: {int(ok.sum())} players with >= {MIN_POSS} "
          f"possessions in 2015-19 pool", flush=True)

    # ---- auxiliary rapm-hat features ---------------------------------------
    hats = {}
    for side in ("offense", "defense"):
        Xf = FEATS[side]
        y_aux = np.array([tgt[side].get(p, np.nan) for p in players])
        m_aux = tr & np.isfinite(y_aux)
        params = dict(tuned[side]["params"], verbose=-1)
        rounds = max(tuned[side]["rounds"] // 3, 150)
        hat = np.full(len(players), np.nan)
        gkf = GroupKFold(n_splits=5)
        for tr_i, te_i in gkf.split(np.where(m_aux)[0], groups=players[m_aux]):
            rows_tr = np.where(m_aux)[0][tr_i]
            rows_te = np.where(m_aux)[0][te_i]
            m = lgb.train(dict(params, seed=0),
                          lgb.Dataset(Xf[rows_tr], y_aux[rows_tr]),
                          num_boost_round=rounds)
            hat[rows_te] = m.predict(Xf[rows_te])
        full = lgb.train(dict(params, seed=0),
                         lgb.Dataset(Xf[m_aux], y_aux[m_aux]),
                         num_boost_round=rounds)
        rest = ~m_aux
        hat[rest] = full.predict(Xf[rest])
        hats[side] = hat
        oof = m_aux & np.isfinite(hat)
        print(f"[{side}] aux OOF rho vs rapm4: "
              f"{spearmanr(hat[oof], y_aux[oof]).statistic:+.3f} "
              f"({int(oof.sum())} rows); hat-vs-label rho on train: "
              f"{spearmanr(hat[tr], d[TARGETS[side]][tr]).statistic:+.3f}",
              flush=True)

    # ---- production arms ----------------------------------------------------
    out = {}
    for side in ("offense", "defense"):
        y = d[TARGETS[side]].astype(np.float64)
        params = dict(tuned[side]["params"], verbose=-1)
        rounds = max(tuned[side]["rounds"] // 3, 150)
        own = hats[side].reshape(-1, 1)
        both = np.column_stack([hats["offense"], hats["defense"]])
        print(f"\n=== {side} ===", flush=True)
        for name, blocks in (("baseline", [FEATS[side]]),
                             ("+hat-own", [FEATS[side], own]),
                             ("+hat-both", [FEATS[side], both])):
            Xf = np.hstack(blocks)
            med = np.nanmedian(Xf[tr], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            p = blend(Xf[tr], y[tr], Xf[test], med, params, rounds)
            s = score_cells(y[test], p, cells_te)
            out[f"{side}|{name}"] = {k: (int(v) if isinstance(v, (int, np.integer))
                                         else round(float(v), 4))
                                     for k, v in s.items()}
            print(f"  {name:<10} dev@10={s['dev@10']:5.2f} "
                  f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
                  f"tau@20={s['tau@20']:+.3f} MAE={s['mae']:.3f} "
                  f"hits@10={s['hits@10']}/20 hits@20={s['hits@20']}/40",
                  flush=True)
    (TD / "RESULTS_rapm_calibration.json").write_text(json.dumps(out, indent=1))
    print("\nwrote RESULTS_rapm_calibration.json", flush=True)


if __name__ == "__main__":
    main()
