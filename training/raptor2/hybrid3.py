"""Hybrids v3: component hats rebuilt on the courtmate-chain on-off.

The chain reproduction fits the published on-off components at rho ~0.89 (both
sides), replacing the rho 0.16-0.31 team-without approximations that v1/v2
hats carried. Rebuild all four hats and test:

  structural-v3     fixed 0.85/0.21 and learned blends (test seasons)
  gbm+hats3         offense and defense, full matrix + 4 v3 hats (test seasons)
  defense CV        10-fold season-held-out gbm vs gbm+hats3 (hats refit per
                    fold), seed set 0 -- the defense question is whether the
                    now-real on-off hat changes the earlier null

Run:  python training/raptor2/hybrid3.py
"""

import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scipy.stats import spearmanr
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import blend, engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import BOX_W, ONOFF_W, build_variables
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(TD / "raptor2" / "courtmate.npz")["CM"]
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    ofields = [str(f) for f in oppz["fields"]]
    Eopp, _ = engineered(oppz["on_X"], oppz["off_X"], ofields, cells)
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OO3o = cell_relative(cm[:, [0, 2, 4]], cells, mp)
    OO3d = cell_relative(-cm[:, [1, 3, 5]], cells, mp)

    fit, val, test = splits(d, 50, 10)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    w = np.sqrt(np.maximum(mp, 1.0))
    cells_te = np.array([str(s) for s in d["season"][test]])
    el = mp[test] >= FLOOR
    tuned = json.loads((TD / "tuned_params.json").read_text())
    y_all = {s: d[TARGETS[s]].astype(np.float64)
             for s in ("offense", "defense")}
    BLOCKS = {"box_o": (OB, "rap_box_o"), "onoff_o": (OO3o, "rap_onoff_o"),
              "box_d": (DB, "rap_box_d"), "onoff_d": (OO3d, "rap_onoff_d")}

    def make_hats(mask_tr):
        hats = {}
        for tag, (M, labname) in BLOCKS.items():
            yv = comp[labname]
            m = mask_tr & np.isfinite(yv) & np.isfinite(M).all(axis=1)
            hats[tag] = ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1],
                                  tag, quiet=True)
            hats[tag][~np.isfinite(M).all(axis=1)] = np.nan
        return hats

    hats = make_hats(tr)
    for tag, (M, labname) in BLOCKS.items():
        yv = comp[labname]
        m = tr & np.isfinite(yv) & np.isfinite(hats[tag])
        print(f"[{tag}] hat rho vs {labname}: "
              f"{spearmanr(hats[tag][m], yv[m]).statistic:+.3f} "
              f"({int(m.sum())} rows)", flush=True)

    out = {}
    # ---- structural v3 blends ----------------------------------------------
    for side, bx, oo in (("offense", "box_o", "onoff_o"),
                         ("defense", "box_d", "onoff_d")):
        y = y_all[side]
        m = tr & np.isfinite(hats[bx]) & np.isfinite(hats[oo]) & np.isfinite(y)
        D = np.column_stack([hats[bx][m], hats[oo][m]])
        cb = RidgeCV(alphas=np.logspace(-3, 3, 13)).fit(D, y[m],
                                                        sample_weight=w[m])
        print(f"[{side}] learned blend box={cb.coef_[0]:.3f} "
              f"onoff={cb.coef_[1]:.3f} (538: 0.85/0.21)", flush=True)
        hb = np.where(np.isfinite(hats[bx]), hats[bx], 0.0)
        ho = np.where(np.isfinite(hats[oo]), hats[oo], 0.0)
        for arm, p in (("fixed", BOX_W * hb + ONOFF_W * ho),
                       ("learned", cb.predict(np.column_stack([hb, ho])))):
            s = score_cells(y[test][el], p[test][el], cells_te[el])
            out[f"struct3|{side}|{arm}"] = round(float(s["dev@10"]), 2)
            print(f"  struct3-{arm:<8} dev@10={s['dev@10']:5.2f} "
                  f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
                  f"MAE={s['mae']:.3f}", flush=True)

    # ---- gbm+hats3 on test cells --------------------------------------------
    H = np.column_stack([hats[t] for t in
                         ("box_o", "onoff_o", "box_d", "onoff_d")])
    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, dfz["E"]])}
    for side in ("offense", "defense"):
        y = y_all[side]
        params = dict(tuned[side]["params"], verbose=-1)
        rounds = max(tuned[side]["rounds"] // 3, 150)
        for arm, Xa in (("gbm", FEATS[side]),
                        ("gbm+hats3", np.hstack([FEATS[side], H]))):
            med = np.nanmedian(Xa[tr], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            p = blend(Xa[tr], y[tr], Xa[test], med, params, rounds)
            s = score_cells(y[test][el], p[el], cells_te[el])
            out[f"test|{side}|{arm}"] = {
                k: (int(v) if isinstance(v, (int, np.integer))
                    else round(float(v), 4)) for k, v in s.items()}
            print(f"[{side}] {arm:<10} dev@10={s['dev@10']:5.2f} "
                  f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
                  f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20", flush=True)

    # ---- defense CV: gbm vs gbm+hats3 (per-fold hats) ------------------------
    print("\n== defense CV (seed set 0) ==", flush=True)
    y = y_all["defense"]
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y)
    Xf = FEATS["defense"]
    cvres = {}
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        elm = mp[te] >= FLOOR
        hf = make_hats(trn)
        Hf = np.column_stack([hf[t] for t in
                              ("box_o", "onoff_o", "box_d", "onoff_d")])
        row = {}
        for arm, Xa in (("gbm", Xf), ("gbm+hats3", np.hstack([Xf, Hf]))):
            med = np.nanmedian(Xa[trn], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            p = blend(Xa[trn], y[trn], Xa[te], med, params, rounds)
            s = score_cells(y[te][elm], p[elm], np.full(int(elm.sum()), season))
            row[arm] = round(float(s["dev@10"]), 2)
        cvres[season] = row
        print(f"  {season}: gbm {row['gbm']:5.2f} | +hats3 "
              f"{row['gbm+hats3']:5.2f}", flush=True)
    g = [cvres[s]["gbm"] for s in STAMPS]
    h = [cvres[s]["gbm+hats3"] for s in STAMPS]
    print(f"  medians: gbm {np.median(g):.2f} | +hats3 {np.median(h):.2f}  "
          f"head-to-head +hats3 {sum(b < a for a, b in zip(g, h))}W "
          f"{sum(b == a for a, b in zip(g, h))}T "
          f"{sum(b > a for a, b in zip(g, h))}L", flush=True)
    out["cv_defense"] = cvres
    Path(TD / "raptor2" / "RESULTS_hybrid3.json").write_text(
        json.dumps(out, indent=1))
    print("wrote raptor2/RESULTS_hybrid3.json", flush=True)


if __name__ == "__main__":
    main()
