"""Assemble every number the NAS write-up needs into one JSON.

Systems compared (offense and defense throughout):
  gbm-direct     LightGBM+ridge blend on EXACTLY the features the NAS nets
                 consume (X+Z+Eopp / X+Z+E) -- the apples-to-apples GBM
  gbm-pairwise   the 48k-pairs-per-cell tournament scores (user's 5090 run),
                 row-aligned in training/pairwise_gbm_<target>.npy
  nas-direct     NAS round 2 winner, direct rating regression (seed-averaged x3)
  nas-pairwise   NAS round 2 pairwise winner solo and top-3 ensemble
  paine          Neil Paine's Estimated RAPTOR, published (eRO/eRD) and our
                 recreation (my_eRO/my_eRD) -- test seasons only, and note his
                 weights were FIT on data containing our test seasons

Pools:
  metrics_full   full test cells (2013-14, 2014-15 RS) -- the convention every
                 prior experiment used, so numbers are comparable to the repo's
                 history
  metrics_paine  >=1065-minute rows matched into Paine's CSV by normalized name
                 (identical pool for every system including Paine)
  metrics_train  2021-22 RS cell, in-sample (systems saw these labels in
                 training) -- shows fit, not skill, and is labeled as such
  boards         top-15 leaderboards, >=1065 pools: truth + projected player and
                 his true rank, for test cells and the in-sample 2021-22 cell

Run after nas_direct.py and nas_pairwise2.py:
    python training/build_nas_report_data.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend, engineered
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
FLOOR = 1065
TRAIN_CELL = ("2021-22", "20220715000000")
TD = REPO_ROOT / "training"


def sc(y, p, cells):
    s = score_cells(y, p, cells)
    return {k: (int(v) if isinstance(v, (int, np.integer)) else round(float(v), 4))
            for k, v in s.items()}


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
    mp = d["mp"].astype(np.float64)
    players = np.array([str(p) for p in d["player"]])
    seasons = np.array([str(s) for s in d["season"]])
    tuned = json.loads((TD / "tuned_params.json").read_text())

    nd = np.load(TD / "data_fixed" / "nas_direct_preds.npz")
    np2 = np.load(TD / "data_fixed" / "nas_pairwise2_preds.npz")
    preds = {}
    for target in ("offense", "defense"):
        y = d[TARGETS[target]].astype(np.float64)
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = max(tuned[target]["rounds"] // 3, 150)
        Xf = FEATS[target]
        med = np.nanmedian(Xf[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        preds[target] = {
            "gbm-direct": blend(Xf[tr], y[tr], Xf, med, params, rounds),
            "gbm-pairwise": np.load(TD / f"pairwise_gbm_{target}.npy"),
            "nas-direct": nd[target],
            "nas-pairwise": np2[target],
        }
        print(f"{target}: predictions assembled", flush=True)

    paine = pd.read_csv(TD / "RESULTS_estimated_raptor.csv")
    paine = paine[paine["split"] == "Regular season"]
    paine["k"] = paine["player"].map(norm_name)
    pmap = {}
    for _, r in paine.iterrows():
        pmap[(r["k"], r["season"])] = r

    out = {"metrics_full": {}, "metrics_paine": {}, "metrics_train": {},
           "boards": {}, "pools": {}}
    for target in ("offense", "defense"):
        y = d[TARGETS[target]].astype(np.float64)
        P = preds[target]

        # --- full test cells (repo convention) ---
        cells_te = seasons[test]
        out["metrics_full"][target] = {
            n: sc(y[test], p[test], cells_te) for n, p in P.items()}

        # --- Paine-matched >=1065 pool ---
        pcol, mycol = ("eRO", "my_eRO") if target == "offense" \
            else ("eRD", "my_eRD")
        m_pool = test & (mp >= FLOOR)
        idx = np.where(m_pool)[0]
        keyed = [(norm_name(players[i]), seasons[i]) for i in idx]
        have = np.array([k in pmap and np.isfinite(pmap[k][pcol])
                         and np.isfinite(pmap[k][mycol]) for k in keyed])
        idx = idx[have]
        keyed = [k for k, h in zip(keyed, have) if h]
        cl = seasons[idx]
        out["pools"][target] = {"paine_matched": int(len(idx)),
                                "per_cell": {c: int((cl == c).sum())
                                             for c in np.unique(cl)}}
        mp_metrics = {n: sc(y[idx], p[idx], cl) for n, p in P.items()}
        mp_metrics["paine-published"] = sc(
            y[idx], np.array([pmap[k][pcol] for k in keyed]), cl)
        mp_metrics["paine-recreated"] = sc(
            y[idx], np.array([pmap[k][mycol] for k in keyed]), cl)
        out["metrics_paine"][target] = mp_metrics

        # --- in-sample training cell (>=1065 pool: sub-50-minute rows exist in
        # the matrix but were in nobody's training data, and their extreme
        # never-seen feature rows poison an unfloored in-sample ranking) ---
        m_trc = rs & (d["timestamp"] == TRAIN_CELL[1])
        m_trc_el = m_trc & (mp >= FLOOR)
        cl = seasons[m_trc_el]
        out["metrics_train"][target] = {
            n: sc(y[m_trc_el], p[m_trc_el], cl) for n, p in P.items()
            if np.isfinite(p[m_trc_el]).all()}

        # --- boards (>=1065 pools) ---
        for season, label, cellmask in (
                ("2013-14", "test", test & (seasons == "2013-14")),
                ("2014-15", "test", test & (seasons == "2014-15")),
                (TRAIN_CELL[0], "train-insample", m_trc)):
            pool = np.where(cellmask & (mp >= FLOOR))[0]
            true_rank = ranks(y[pool])
            order_true = np.argsort(-y[pool])
            board = {"true": [
                {"pos": k + 1, "player": players[pool[o]],
                 "score": round(float(y[pool[o]]), 2)}
                for k, o in enumerate(order_true[:15])]}
            systems = dict(P)
            if label == "test":
                keyed = [(norm_name(players[i]), seasons[i]) for i in pool]
                pv = np.array([float(pmap[k][pcol]) if k in pmap
                               and np.isfinite(pmap[k][pcol]) else np.nan
                               for k in keyed])
                systems["paine-published"] = None  # handled below
            for name in systems:
                v = pv if name == "paine-published" else \
                    systems[name][pool].astype(np.float64)
                vv = np.where(np.isfinite(v), v, -1e9)
                order = np.argsort(-vv)
                board[name] = [
                    {"pos": k + 1, "player": players[pool[o]],
                     "true_rank": int(true_rank[o]) + 1}
                    for k, o in enumerate(order[:15])]
            out["boards"][f"{target}|{season}|{label}"] = board

    (TD / "RESULTS_nas_report_data.json").write_text(
        json.dumps(out, indent=1))
    print("wrote RESULTS_nas_report_data.json", flush=True)


if __name__ == "__main__":
    main()
