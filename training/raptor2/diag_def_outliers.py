"""Defense-projection outlier diagnostic. Production config verbatim
(gbm + struct hats3, RS-only training, seeds 0/1/2 blend); for every
eligible test row of every fold saves player, minutes, actual, estimate,
both ranks, the four hat values; and for the extreme misses, the
LightGBM feature contributions (seed-0 model, pred_contrib) grouped to
name the features that drove the bad estimate.

Outlier definition here is rank-based, because dev@10 is: a row is an
extreme miss if it is in the actual top-15 but projected outside 25, or
projected top-15 but actually outside 25 (both directions hurt top-10
ordering), or |resid| >= 2.5.

Run:  python training/raptor2/diag_def_outliers.py
"""

import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import REPO_ROOT
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import blend
from experiment_topk_rank import ranks
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
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(HERE / "courtmate.npz")["CM"]
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
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
    y = d[TARGETS["defense"]].astype(np.float64)
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y)
    Xf = np.hstack([X, Z, dfz["E"]])
    fnames = (feat + [f"Z:{c}" for c in RELATIVE_COLS]
              + [f"dfz:{str(c)}" for c in dfz["enames"]]
              + ["HAT_box_o", "HAT_onoff_o", "HAT_box_d", "HAT_onoff_d"])

    rows = []
    contribs = []
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        elm = np.where(te)[0][mp[te] >= FLOOR]
        hf = {}
        for tag, (M, labname) in BLOCKS.items():
            yv = comp[labname]
            m = trn & np.isfinite(yv) & np.isfinite(M).all(axis=1)
            hf[tag] = ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1],
                                tag, quiet=True)
            hf[tag][~np.isfinite(M).all(axis=1)] = np.nan
        H = np.column_stack([hf[t] for t in
                             ("box_o", "onoff_o", "box_d", "onoff_d")])
        Xa = np.hstack([Xf, H])
        med = np.nanmedian(Xa[trn], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p = blend(Xa[trn], y[trn], Xa[elm], med, params, rounds)
        rt, rp = ranks(y[elm]), ranks(p)
        m0 = lgb.train(dict(params, seed=0, bagging_seed=0,
                            feature_fraction_seed=0),
                       lgb.Dataset(Xa[trn], y[trn]), num_boost_round=rounds)
        C = m0.predict(Xa[elm], pred_contrib=True)
        for j, i in enumerate(elm):
            miss = (rt[j] < 15 and rp[j] >= 25) or \
                   (rp[j] < 15 and rt[j] >= 25) or abs(y[i] - p[j]) >= 2.5
            rows.append({"season": season, "player": str(d["player"][i]),
                         "mp": int(mp[i]), "actual": round(float(y[i]), 2),
                         "est": round(float(p[j]), 2),
                         "rank_true": int(rt[j]) + 1,
                         "rank_est": int(rp[j]) + 1,
                         "resid": round(float(y[i] - p[j]), 2),
                         "hats": [None if not np.isfinite(v)
                                  else round(float(v), 2) for v in H[i]],
                         "extreme": bool(miss)})
            if miss:
                order = np.argsort(-np.abs(C[j, :-1]))[:12]
                contribs.append({
                    "season": season, "player": str(d["player"][i]),
                    "resid": round(float(y[i] - p[j]), 2),
                    "top_contrib": [
                        {"feat": fnames[k],
                         "contrib": round(float(C[j, k]), 2),
                         "value": (None if not np.isfinite(Xa[i, k])
                                   else round(float(Xa[i, k]), 2))}
                        for k in order]})
        n_ex = sum(r["extreme"] for r in rows if r["season"] == season)
        print(f"{season}: {int(len(elm))} rows, {n_ex} extreme", flush=True)

    (HERE / "DIAG_def_outliers.json").write_text(
        json.dumps({"rows": rows, "contribs": contribs}, indent=1))
    print(f"wrote DIAG_def_outliers.json "
          f"({sum(r['extreme'] for r in rows)} extreme of {len(rows)})",
          flush=True)


if __name__ == "__main__":
    main()
