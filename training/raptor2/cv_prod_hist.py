"""Production-config gate for the historical calibration hat, offense side.

Stack is cv_pool_po --side offense verbatim (hats4-both, playoff rows pooled
into hat fits + final GBM) plus ONE extra column: the pooled/calibrated
bbref-space ridge from cv_hist_pool (1978-2013 rows with 538 labels + the
fold's modern training rows with rap_box_o labels). Baseline is the stored
production CV, RESULTS_cv_pool_po_offense[_sN].json.

Run:  python training/raptor2/cv_prod_hist.py [--seedbase 0]
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS, masks_for
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import engineered, per100
from experiment_topk_rank import score_cells
from hist_experiments import load_bbref, feature_sets
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from variables2 import build_onoff2
from cv_components import ridge_pred
from structural2 import ridge_hat
from cv_hybrid import blend_members

TD = REPO_ROOT / "training"
HERE = TD / "raptor2"
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

    # ---- bbref calibration space (cv_hist_pool verbatim) ------------------
    bb = load_bbref()
    hist = pd.read_csv(HERE / "hist538" / "historical_RAPTOR_by_player.csv")
    dtr = bb.merge(hist, left_on=["pid", "season"],
                   right_on=["player_id", "season"], how="inner",
                   suffixes=("", "_538"))
    dtr = dtr[(dtr["mp"].fillna(0) >= 250)
              & dtr["season"].between(1978, 2013)].reset_index(drop=True)
    _, rawd = feature_sets(bb)
    rawd = [c for c in rawd if c in dtr.columns]

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    n = X.shape[0]

    dte = bb[bb["season"].between(2014, 2023)].reset_index(drop=True)
    names = np.array([norm_name(str(p)) for p in d["player"]])
    end_year = np.array([int(str(s)[:4]) + 1 for s in d["season"]])
    key, dupes = {}, set()
    for i, r in dte.iterrows():
        k = (norm_name(str(r["name"])), int(r["season"]))
        if k in key:
            dupes.add(k)
        key[k] = i
    row_of = np.full(n, -1)
    for i, (nm, yr) in enumerate(zip(names, end_year)):
        k = (nm, yr)
        if k in key and k not in dupes:
            row_of[i] = key[k]
    Gh = dtr[rawd].values.astype(np.float64)
    Gte = dte[rawd].values.astype(np.float64)
    Gm = np.full((n, len(rawd)), np.nan)
    Gm[row_of >= 0] = Gte[row_of[row_of >= 0]]
    cov = (np.isfinite(Gh).mean(0) * 0.5
           + np.isfinite(Gm[row_of >= 0]).mean(0) * 0.5)
    cols = cov >= 0.90
    Gh, Gm = Gh[:, cols], Gm[:, cols]
    ok_h = np.isfinite(Gh).all(1)
    ok_m = np.isfinite(Gm).all(1)
    Gh, w_h = Gh[ok_h], np.sqrt(np.maximum(dtr["mp"].values[ok_h], 1.0))
    yh = dtr["raptor_offense"].values[ok_h]

    # ---- production offense stack (cv_pool_po verbatim) -------------------
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(HERE / "courtmate.npz")["CM"]
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
    OO2o = cell_relative(OFF2, cells, mp)
    OO2d = cell_relative(DEF2, cells, mp)
    OO3o = cell_relative(cm[:, [0, 2, 4]], cells, mp)
    OO3d = cell_relative(-cm[:, [1, 3, 5]], cells, mp)
    box_mask, onoff_mask = masks_for(feat)
    sdR = sd["R"].astype(np.float32)
    dfzE = dfz["E"].astype(np.float32)
    wide_box_o = np.hstack([X[:, box_mask], sdR, Z]).astype(np.float32)
    wide_box_d = np.hstack([X[:, box_mask], sdR, dfzE, Z]).astype(np.float32)
    wide_onoff = np.hstack([X[:, onoff_mask], Bopp,
                            np.where(np.isfinite(cm), cm, np.nan),
                            OO2o, OO2d]).astype(np.float32)
    STRUCT = {"box_o": OB, "onoff_o": OO3o, "box_d": DB, "onoff_d": OO3d}
    WIDE = {"box_o": wide_box_o, "onoff_o": wide_onoff,
            "box_d": wide_box_d, "onoff_d": wide_onoff}
    LABN = {"box_o": "rap_box_o", "onoff_o": "rap_onoff_o",
            "box_d": "rap_box_d", "onoff_d": "rap_onoff_d"}

    y = d[TARGETS["offense"]].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    seasons = np.array([str(s) for s in d["season"]])
    at_stamp = np.isin(d["timestamp"], list(STAMPS.values()))
    labeled_rs = rs & at_stamp & np.isfinite(y)
    labeled_any = at_stamp & np.isfinite(y)
    w = np.sqrt(np.maximum(mp, 1.0))
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned["offense"]["params"], verbose=-1)
    rounds = max(tuned["offense"]["rounds"] // 3, 150)
    Xf = np.hstack([X, Z, Eopp])
    sb = args.seedbase
    reff = "RESULTS_cv_pool_po_offense.json" if sb == 0 \
        else f"RESULTS_cv_pool_po_offense_s{sb}.json"
    ref = json.loads((HERE / reff).read_text())["per_season"]
    boxo = comp["rap_box_o"].astype(np.float64)

    per = {}
    for season, stamp in STAMPS.items():
        te = labeled_rs & (d["timestamp"] == stamp)
        trn = labeled_any & (seasons != season)
        elm = mp[te] >= FLOOR
        hats = []
        for tag in ("box_o", "onoff_o", "box_d", "onoff_d"):
            yv = comp[LABN[tag]].astype(np.float64)
            S = STRUCT[tag]
            m = trn & np.isfinite(yv) & np.isfinite(S).all(axis=1)
            h = ridge_hat(S[m], yv[m], w[m], S, [""] * S.shape[1], tag,
                          quiet=True)
            h[~np.isfinite(S).all(axis=1)] = np.nan
            hats.append(h)
            m2 = trn & np.isfinite(yv)
            hats.append(ridge_pred(WIDE[tag][m2], yv[m2], w[m2], WIDE[tag],
                                   standardize=True))
        m = trn & ok_m & np.isfinite(boxo)
        A = np.vstack([Gh, Gm[m]])
        yc = np.concatenate([yh, boxo[m]])
        wc = np.concatenate([w_h, w[m]])
        mu, sdv = A.mean(0), A.std(0)
        sdv[sdv == 0] = 1.0
        r = RidgeCV(alphas=np.logspace(-3, 5, 33)).fit(
            (A - mu) / sdv, yc, sample_weight=wc)
        cal = np.full(n, np.nan)
        cal[ok_m] = r.predict((Gm[ok_m] - mu) / sdv)
        hats.append(cal)
        Xa = np.hstack([Xf, np.column_stack(hats)]).astype(np.float32)
        med = np.nanmedian(Xa[trn], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p = blend_members(Xa[trn], y[trn], Xa[te], med, params, rounds,
                          (sb, sb + 1, sb + 2))
        s = score_cells(y[te][elm], p[elm], np.full(int(elm.sum()), season))
        per[season] = round(float(s["dev@10"]), 2)
        print(f"[prod+cal] {season}: dev@10={s['dev@10']:.2f} "
              f"tau@10={s['tau@10']:+.3f} (prod {ref[season]:.2f})",
              flush=True)

    dv = list(per.values())
    rv = [ref[s] for s in STAMPS]
    wins = int(sum(a < b for a, b in zip(dv, rv)))
    ties = int(sum(a == b for a, b in zip(dv, rv)))
    print(f"[prod+cal] median {np.median(dv):.2f} mean {np.mean(dv):.2f} | "
          f"prod median {np.median(rv):.2f} mean {np.mean(rv):.2f} | "
          f"{wins}W {ties}T {10 - wins - ties}L", flush=True)
    tag = f"_s{sb}" if sb else ""
    fp = HERE / f"RESULTS_cv_prod_hist{tag}.json"
    fp.write_text(json.dumps(
        {"per_season": per, "median": float(np.median(dv)),
         "mean": float(np.mean(dv)),
         "vs_prod": f"{wins}W {ties}T {10 - wins - ties}L"}, indent=1))
    print(f"wrote {fp}", flush=True)


if __name__ == "__main__":
    main()
