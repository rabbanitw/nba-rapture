"""The bar test: gbm+hats vs the components+opp architecture, identical protocol.

Ten season-held-out folds, same rounds (tuned//3), same 3-seed members + ridge,
same >=1065 pools. The components arm is the production offense architecture
(box model -> rap_box_o, on/off model with opponent per-100 block ->
rap_onoff_o, minutes-aware ridge combiner). The hats arm is the rebuild's
candidate (full matrix + four structural component hats, hats refit per fold).

Run:  python training/raptor2/cv_bar.py --seedbase 0
"""

import argparse
import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import (COMPONENT_LABELS, RELATIVE_COLS,
                                   combiner_design, masks_for)
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import engineered, per100
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from variables2 import build_onoff2
from structural2 import ridge_hat
from cv_hybrid import blend_members

TD = REPO_ROOT / "training"
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seedbase", type=int, default=0)
    args = ap.parse_args()
    seeds = (args.seedbase, args.seedbase + 1, args.seedbase + 2)

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    ofields = [str(f) for f in oppz["fields"]]
    Eopp, _ = engineered(oppz["on_X"], oppz["off_X"], ofields, cells)
    on100, off100 = per100(oppz["on_X"], ofields), per100(oppz["off_X"], ofields)
    Bopp = np.hstack([on100, off100, on100 - off100]).astype(np.float32)
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OFF2, DEF2 = build_onoff2(X, feat, oppz["on_X"], oppz["off_X"], ofields,
                              cells, mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OOo = cell_relative(OFF2, cells, mp)
    OOd = cell_relative(DEF2, cells, mp)
    box_mask, onoff_mask = masks_for(feat)

    rs = d["season_type"] == "Regular season"
    y = d[TARGETS["offense"]].astype(np.float64)
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y)
    w = np.sqrt(np.maximum(mp, 1.0))
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned["offense"]["params"], verbose=-1)
    rounds = max(tuned["offense"]["rounds"] // 3, 150)
    Xf = np.hstack([X, Z, Eopp])
    Xb_all = X[:, box_mask]
    Xo_all = np.hstack([X[:, onoff_mask], Bopp])

    per = {}
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        el = mp[te] >= FLOOR
        groups = np.array([f"{p}|{s}" for p, s in
                           zip(d["player"][trn], d["season"][trn])])
        row = {}

        # ---- components+opp arm ----
        oof = {}
        for tag, Xs, labname in (("box", Xb_all, COMPONENT_LABELS["offense"][0]),
                                 ("onoff", Xo_all,
                                  COMPONENT_LABELS["offense"][1])):
            labv = comp[labname]
            ms = np.nanmedian(Xs[trn], axis=0)
            ms = np.where(np.isfinite(ms), ms, 0.0)
            o = np.full(int(trn.sum()), np.nan)
            Xs_tr = Xs[trn]
            for tri, vai in GroupKFold(n_splits=3).split(
                    Xs_tr, labv[trn], groups=groups):
                o[vai] = blend_members(Xs_tr[tri], labv[trn][tri], Xs_tr[vai],
                                       ms, params, rounds, seeds=(seeds[0],))
            oof[tag] = o
            oof[tag + "_te"] = blend_members(Xs_tr, labv[trn], Xs[te], ms,
                                             params, rounds, seeds=seeds)
        cbn = Ridge(alpha=1.0).fit(
            combiner_design(oof["box"], oof["onoff"], mp[trn]), y[trn])
        p = cbn.predict(combiner_design(oof["box_te"], oof["onoff_te"],
                                        mp[te]))
        s = score_cells(y[te][el], p[el], np.full(int(el.sum()), season))
        row["components"] = {k: (int(v) if isinstance(v, (int, np.integer))
                                 else round(float(v), 4)) for k, v in s.items()}

        # ---- gbm+hats arm ----
        hats = []
        for M, labname in ((OB, "rap_box_o"), (OOo, "rap_onoff_o"),
                           (DB, "rap_box_d"), (OOd, "rap_onoff_d")):
            yv = comp[labname]
            m = trn & np.isfinite(yv)
            hats.append(ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1],
                                  "", quiet=True))
        Xa = np.hstack([Xf, np.column_stack(hats)])
        med = np.nanmedian(Xa[trn], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p = blend_members(Xa[trn], y[trn], Xa[te], med, params, rounds, seeds)
        s = score_cells(y[te][el], p[el], np.full(int(el.sum()), season))
        row["gbm+hats"] = {k: (int(v) if isinstance(v, (int, np.integer))
                               else round(float(v), 4)) for k, v in s.items()}
        per[season] = row
        print(f"{season}: components dev@10={row['components']['dev@10']:5.2f} "
              f"| gbm+hats dev@10={row['gbm+hats']['dev@10']:5.2f}", flush=True)

    print(f"\n== summary (seeds {seeds}) ==", flush=True)
    for arm in ("components", "gbm+hats"):
        dv = [per[s][arm]["dev@10"] for s in STAMPS]
        h = sum(per[s][arm]["hits@10"] for s in STAMPS)
        print(f"  {arm:<11} median dev@10 {np.median(dv):.2f}  mean "
              f"{np.mean(dv):.2f}  hits@10 {h}/100", flush=True)
    wins = sum(per[s]["gbm+hats"]["dev@10"] < per[s]["components"]["dev@10"]
               for s in STAMPS)
    ties = sum(per[s]["gbm+hats"]["dev@10"] == per[s]["components"]["dev@10"]
               for s in STAMPS)
    print(f"  gbm+hats vs components head-to-head: {wins}W {ties}T "
          f"{10-wins-ties}L", flush=True)
    Path(TD / "raptor2" / f"RESULTS_cv_bar_s{args.seedbase}.json").write_text(
        json.dumps(per, indent=1))
    print(f"wrote raptor2/RESULTS_cv_bar_s{args.seedbase}.json", flush=True)


if __name__ == "__main__":
    main()
