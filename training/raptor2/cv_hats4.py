"""Stack integration of the upgraded component hats ("hats4"): does replacing
the four structural ridge hats with the cv_components winners move the final
offense/defense CV?

Protocol is cv_resid_pools verbatim (same folds, same blend = 3 seeds at
tuned rounds//3, same >=1065 pools), so the stored hats3 numbers in
RESULTS_hats3_cv.json are the direct baseline. Two hat sets:

  hats4-linear   all four components = ridge-wide (standardized RidgeCV on the
                 wide masked matrix). In-sample on fold-train rows, exactly as
                 the production struct hats are today. Cheap.
  hats4-winner   box_o = ridge-wide; box_d / onoff_o / onoff_d = the
                 gbm-wide+hat stack, OOF on fold-train rows (player-grouped
                 3-fold) so the final GBM never sees in-sample tree fits.

Run:  python training/raptor2/cv_hats4.py --arm linear|winner
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS, masks_for
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import engineered, per100
from cv_hybrid import blend_members
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from variables2 import build_onoff2
from cv_components import gbm_pred, ridge_pred

TD = REPO_ROOT / "training"
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", choices=["linear", "winner", "both", "hats3"],
                default="linear")
    ap.add_argument("--side", choices=["both", "offense", "defense"],
                    default="both")
    ap.add_argument("--seedbase", type=int, default=0)
    args = ap.parse_args()

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
    BLOCKS = {"box_o": (wide_box_o, OB, "rap_box_o", "offense"),
              "onoff_o": (wide_onoff, OO3o, "rap_onoff_o", "offense"),
              "box_d": (wide_box_d, DB, "rap_box_d", "defense"),
              "onoff_d": (wide_onoff, OO3d, "rap_onoff_d", "defense")}

    y_any = d["y_off"].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y_any)
    w = np.sqrt(np.maximum(mp, 1.0))
    players = np.array([str(p) for p in d["player"]])
    tuned = json.loads((TD / "tuned_params.json").read_text())
    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, dfz["E"]])}
    ref = json.loads((TD / "raptor2" / "RESULTS_hats3_cv.json").read_text())

    def hat_linear(tag, trn):
        Wd, S, lab, side = BLOCKS[tag]
        yv = comp[lab].astype(np.float64)
        m = trn & np.isfinite(yv)
        return ridge_pred(Wd[m], yv[m], w[m], Wd, standardize=True)

    def hat_stack(tag, trn, te):
        """gbm-wide+hat, OOF on trn rows, full-fit prediction on te rows."""
        Wd, S, lab, side = BLOCKS[tag]
        yv = comp[lab].astype(np.float64)
        params = dict(tuned[side]["params"], verbose=-1)
        rounds = max(tuned[side]["rounds"] // 3, 150)
        tri = np.where(trn & np.isfinite(yv))[0]
        # inner OOF ridge-hat column
        rhat = np.full(len(yv), np.nan)
        for a, b in GroupKFold(n_splits=3).split(tri, groups=players[tri]):
            rhat[tri[b]] = ridge_pred(S[tri[a]], yv[tri[a]], w[tri[a]],
                                      S[tri[b]])
        rhat[te] = ridge_pred(S[tri], yv[tri], w[tri], S[te])
        SWH = np.hstack([Wd, S, rhat.reshape(-1, 1)]).astype(np.float32)
        out = np.full(len(yv), np.nan)
        for a, b in GroupKFold(n_splits=3).split(tri, groups=players[tri]):
            out[tri[b]] = gbm_pred(SWH[tri[a]], yv[tri[a]], w[tri[a]],
                                   SWH[tri[b]], params, rounds)
        out[te] = gbm_pred(SWH[tri], yv[tri], w[tri], SWH[te], params, rounds)
        return out

    seeds = (args.seedbase, args.seedbase + 1, args.seedbase + 2)
    summary = {}
    sides = ("offense", "defense") if args.side == "both" \
        else (args.side,)
    for side in sides:
        y = d[TARGETS[side]].astype(np.float64)
        params = dict(tuned[side]["params"], verbose=-1)
        rounds = max(tuned[side]["rounds"] // 3, 150)
        Xf = FEATS[side]
        per = {}
        for season, stamp in STAMPS.items():
            te = labeled & (d["timestamp"] == stamp)
            trn = labeled & (d["timestamp"] != stamp)
            elm = mp[te] >= FLOOR
            hats = []
            for tag in ("box_o", "onoff_o", "box_d", "onoff_d"):
                if args.arm == "hats3":
                    Wd, S, lab, _sd = BLOCKS[tag]
                    yv = comp[lab].astype(np.float64)
                    m = trn & np.isfinite(yv) & np.isfinite(S).all(axis=1)
                    hats.append(ridge_pred(S[m], yv[m], w[m], S))
                elif args.arm == "both":
                    # production struct ridge hat + the linear-wide hat
                    Wd, S, lab, _sd = BLOCKS[tag]
                    yv = comp[lab].astype(np.float64)
                    m = trn & np.isfinite(yv) & np.isfinite(S).all(axis=1)
                    hats.append(ridge_pred(S[m], yv[m], w[m], S))
                    hats.append(hat_linear(tag, trn))
                elif args.arm == "linear" or tag == "box_o":
                    hats.append(hat_linear(tag, trn))
                else:
                    hats.append(hat_stack(tag, trn, te))
            Xa = np.hstack([Xf, np.column_stack(hats)]).astype(np.float32)
            med = np.nanmedian(Xa[trn], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            p = blend_members(Xa[trn], y[trn], Xa[te], med, params,
                              rounds, seeds)
            s = score_cells(y[te][elm], p[elm], np.full(int(elm.sum()), season))
            per[season] = round(float(s["dev@10"]), 2)
            r3 = ref[side]["per_season"][season]
            print(f"[{side}] {season}: hats4-{args.arm} dev@10={s['dev@10']:.2f}"
                  f" tau@10={s['tau@10']:+.3f}  (hats3 {r3:.2f})", flush=True)
        dv = list(per.values())
        r3s = [ref[side]["per_season"][s] for s in STAMPS]
        wins = sum(a < b for a, b in zip(dv, r3s))
        ties = sum(a == b for a, b in zip(dv, r3s))
        summary[side] = {"per_season": per, "median": float(np.median(dv)),
                         "mean": float(np.mean(dv)),
                         "vs_hats3": f"{wins}W {ties}T {10-wins-ties}L"}
        print(f"[{side}] hats4-{args.arm} median {np.median(dv):.2f} mean "
              f"{np.mean(dv):.2f} | hats3 median {np.median(r3s):.2f} mean "
              f"{np.mean(r3s):.2f} | head-to-head {summary[side]['vs_hats3']}",
              flush=True)

    tag = f"_{args.arm}"
    if args.seedbase or args.side != "both":
        tag += f"_s{args.seedbase}_{args.side}"
    out = TD / "raptor2" / f"RESULTS_cv_hats4{tag}.json"
    out.write_text(json.dumps(summary, indent=1))
    print(f"wrote {out}", flush=True)


if __name__ == "__main__":
    main()
