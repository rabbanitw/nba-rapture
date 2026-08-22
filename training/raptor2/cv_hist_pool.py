"""Historical rows as TRAINING data (box calibration), not just analysis.

E4 tested a transfer hat (1978-2013 labels only) on defense: rejected.
This tests the pooled/calibrated form, per fold: ridge in the shared
basketball-reference feature space trained on

    all 1978-2013 player-seasons (538 raptor_offense/defense labels --
    box-only era + box+RAPM era; fold-independent, predate every fold)
  + the fold's modern TRAINING rows (rap_box_o / rap_box_d component
    labels from components.npz)

predicted onto every combined.npz row and appended to the hats3 stack as
one extra hat column. NaN convention per the exact-hats lesson: only
bbref columns with >=90% coverage are used, training keeps all-finite
rows, and the hat is NaN wherever a row's features are incomplete or the
name x season alignment failed.

Arms:  def-cal   defense stack + calibrated D hat
       off-cal   offense stack + calibrated O hat
       off-e4    offense stack + the E4 transfer hat (never tested on
                 offense; hist_hat.npz, fold-independent)

Run:  python training/raptor2/cv_hist_pool.py [--seedbase 0]
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
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import blend, engineered
from experiment_topk_rank import score_cells
from hist_experiments import load_bbref, feature_sets
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from structural2 import ridge_hat

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
    ap.add_argument("--arms", nargs="*",
                    default=["def-cal", "off-cal", "off-e4"])
    args = ap.parse_args()
    seeds = (args.seedbase, args.seedbase + 1, args.seedbase + 2)

    # ---- bbref feature space, hist pool + modern alignment ----------------
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
    print(f"bbref space: {int(cols.sum())}/{len(cols)} cols kept; hist pool "
          f"{int(ok_h.sum())}/{len(Gh)} rows all-finite; modern aligned "
          f"{int(ok_m.sum())}/{n}", flush=True)
    Gh, w_h = Gh[ok_h], np.sqrt(np.maximum(dtr["mp"].values[ok_h], 1.0))
    yh = {"offense": dtr["raptor_offense"].values[ok_h],
          "defense": dtr["raptor_defense"].values[ok_h]}

    def cal_hat(side, comp_lab, trn, mp):
        m = trn & ok_m & np.isfinite(comp_lab)
        A = np.vstack([Gh, Gm[m]])
        y = np.concatenate([yh[side], comp_lab[m]])
        w = np.concatenate([w_h, np.sqrt(np.maximum(mp[m], 1.0))])
        mu, sd = A.mean(0), A.std(0)
        sd[sd == 0] = 1.0
        r = RidgeCV(alphas=np.logspace(-3, 5, 33)).fit(
            (A - mu) / sd, y, sample_weight=w)
        out = np.full(n, np.nan)
        out[ok_m] = r.predict((Gm[ok_m] - mu) / sd)
        return out

    # ---- modern stack (cv_resid_pools verbatim) ---------------------------
    sd_ = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(HERE / "courtmate.npz")["CM"]
    e4 = np.load(HERE / "hist_hat.npz")["H"]
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    ofields = [str(f) for f in oppz["fields"]]
    Eopp, _ = engineered(oppz["on_X"], oppz["off_X"], ofields, cells)
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, sd_["R"], [str(x) for x in sd_["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OO3o = cell_relative(cm[:, [0, 2, 4]], cells, mp)
    OO3d = cell_relative(-cm[:, [1, 3, 5]], cells, mp)
    BLOCKS = {"box_o": (OB, "rap_box_o"), "onoff_o": (OO3o, "rap_onoff_o"),
              "box_d": (DB, "rap_box_d"), "onoff_d": (OO3d, "rap_onoff_d")}
    w = np.sqrt(np.maximum(mp, 1.0))
    rs = d["season_type"] == "Regular season"
    tuned = json.loads((TD / "tuned_params.json").read_text())
    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, dfz["E"]])}
    ref = json.loads((HERE / "RESULTS_hats3_cv.json").read_text())

    def make_hats(mask_tr):
        hats = {}
        for tag, (M, labname) in BLOCKS.items():
            yv = comp[labname]
            m = mask_tr & np.isfinite(yv) & np.isfinite(M).all(axis=1)
            hats[tag] = ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1],
                                  tag, quiet=True)
            hats[tag][~np.isfinite(M).all(axis=1)] = np.nan
        return hats

    ARMS = [(a, s, k) for a, s, k in
            [("def-cal", "defense", "cal"), ("off-cal", "offense", "cal"),
             ("off-e4", "offense", "e4")] if a in args.arms]
    out = {"seedbase": args.seedbase, "arms": {}}
    per = {a: {} for a, _, _ in ARMS}
    for season, stamp in STAMPS.items():
        te_all = rs & np.isin(d["timestamp"], list(STAMPS.values()))
        for arm, side, kind in ARMS:
            y = d[TARGETS[side]].astype(np.float64)
            labeled = te_all & np.isfinite(y)
            te = labeled & (d["timestamp"] == stamp)
            trn = labeled & (d["timestamp"] != stamp)
            elm = mp[te] >= FLOOR
            hf = make_hats(trn)
            H = np.column_stack([hf[t] for t in
                                 ("box_o", "onoff_o", "box_d", "onoff_d")])
            if kind == "cal":
                lab = comp["rap_box_o" if side == "offense"
                           else "rap_box_d"].astype(np.float64)
                extra = cal_hat(side, lab, trn, mp)
            else:
                extra = e4[:, 0]
            Xa = np.hstack([FEATS[side], H, extra[:, None]])
            med = np.nanmedian(Xa[trn], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            params = dict(tuned[side]["params"], verbose=-1)
            rounds = max(tuned[side]["rounds"] // 3, 150)
            p = blend(Xa[trn], y[trn], Xa[te], med, params, rounds, seeds)
            s = score_cells(y[te][elm], p[elm], np.full(int(elm.sum()), season))
            per[arm][season] = round(float(s["dev@10"]), 2)
            print(f"[{arm}] {season}: dev@10={s['dev@10']:.2f} "
                  f"(hats3 {ref[side]['per_season'][season]})", flush=True)

    for arm, side, _ in ARMS:
        refper = ref[side]["per_season"]
        dv = list(per[arm].values())
        wl = [np.sign(refper[s] - per[arm][s]) for s in STAMPS]
        rec = [int(sum(x > 0 for x in wl)), int(sum(x == 0 for x in wl)),
               int(sum(x < 0 for x in wl))]
        out["arms"][arm] = {"per_season": per[arm],
                            "median": float(np.median(dv)),
                            "mean": float(np.mean(dv)), "wl_vs_hats3": rec}
        print(f"[{arm}] median {np.median(dv):.2f} mean {np.mean(dv):.2f} | "
              f"hats3 {ref[side]['median']:.2f}/{ref[side]['mean']:.2f} | "
              f"{rec[0]}W {rec[1]}T {rec[2]}L", flush=True)
    tag = f"_s{args.seedbase}" if args.seedbase else ""
    fp = HERE / f"RESULTS_cv_hist_pool{tag}.json"
    fp.write_text(json.dumps(out, indent=1))
    print(f"wrote {fp}", flush=True)


if __name__ == "__main__":
    main()
