"""Projected leaderboards of the BEST available models vs actual, with rank
deviation -- season-held-out, so every board row is an honest out-of-sample
projection scored against the published truth.

Configs (winners of the promotion gates on this branch):
  offense  gbm + struct hats + linear-wide hats (hats4-both), optionally with
           playoff rows pooled into training (--off-pooled, if the s10/s20
           gate passed)
  defense  gbm + struct hats (hats3) -- every attempted upgrade regressed it

For each of the 10 CV seasons: the model's top-25 (>=1065-minute pool) with
estimate, actual RAPTOR, actual rank in pool, and |rank deviation|; plus the
usual dev@10 / tau@10 / hits@10 summary.

Run:  python training/raptor2/boards_best.py --side offense --off-pooled
      python training/raptor2/boards_best.py --side defense
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import REPO_ROOT
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS, masks_for
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import engineered, per100
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from variables2 import build_onoff2
from cv_components import ridge_pred
from structural2 import ridge_hat
from cv_hybrid import blend_members

TD = REPO_ROOT / "training"
FLOOR = 1065
TOPN = 25
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--side", choices=["offense", "defense"], required=True)
    ap.add_argument("--off-pooled", action="store_true",
                    help="offense: pool playoff rows into training")
    args = ap.parse_args()
    side = args.side

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

    y = d[TARGETS[side]].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    seasons_row = np.array([str(s) for s in d["season"]])
    at_stamp = np.isin(d["timestamp"], list(STAMPS.values()))
    labeled_rs = rs & at_stamp & np.isfinite(y)
    labeled_any = at_stamp & np.isfinite(y)
    w = np.sqrt(np.maximum(mp, 1.0))
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned[side]["params"], verbose=-1)
    rounds = max(tuned[side]["rounds"] // 3, 150)
    Xf = np.hstack([X, Z, Eopp]) if side == "offense" \
        else np.hstack([X, Z, dfz["E"]])
    pooled = side == "offense" and args.off_pooled

    boards = {}
    summary = {}
    for season, stamp in STAMPS.items():
        te = labeled_rs & (d["timestamp"] == stamp)
        trn = (labeled_any if pooled else labeled_rs) \
            & (seasons_row != season)
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
            if side == "offense":      # hats4-both adds the linear-wide hats
                m2 = trn & np.isfinite(yv)
                hats.append(ridge_pred(WIDE[tag][m2], yv[m2], w[m2],
                                       WIDE[tag], standardize=True))
        Xa = np.hstack([Xf, np.column_stack(hats)]).astype(np.float32)
        med = np.nanmedian(Xa[trn], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p = blend_members(Xa[trn], y[trn], Xa[te], med, params, rounds,
                          (0, 1, 2))
        s = score_cells(y[te][elm], p[elm], np.full(int(elm.sum()), season))
        summary[season] = {k: (int(v) if isinstance(v, (int, np.integer))
                               else round(float(v), 4)) for k, v in s.items()}
        yp, pp = y[te][elm], p[elm]
        namep = d["player"][te][elm]
        mpp = mp[te][elm]
        tr_rank, pr_rank = ranks(yp), ranks(pp)
        order = np.argsort(pr_rank)[:TOPN]
        boards[season] = [
            {"pos": int(pr_rank[o]) + 1, "player": str(namep[o]),
             "est": round(float(pp[o]), 2), "actual": round(float(yp[o]), 2),
             "actual_rank": int(tr_rank[o]) + 1,
             "rank_dev": int(abs(tr_rank[o] - pr_rank[o])),
             "mp": int(mpp[o])}
            for o in order]
        print(f"[{side}] {season}: dev@10={s['dev@10']:.2f} "
              f"tau@10={s['tau@10']:+.3f} hits@10={s['hits@10']}/10",
              flush=True)

    tag = f"{side}" + ("_pooled" if pooled else "")
    out = TD / "raptor2" / f"RESULTS_boards_best_{tag}.json"
    out.write_text(json.dumps({"summary": summary, "boards": boards},
                              indent=1))
    print(f"wrote {out}", flush=True)


if __name__ == "__main__":
    main()
