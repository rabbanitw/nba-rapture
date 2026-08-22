"""Multi-season defense targets: the label-side change E3 calls for.

E3: year-adjacent stability of the tracking-era defense label is r=.631 —
~37% of its variance is single-season circumstance no feature set can
predict, which bounds dev@10 for any model scored against it. This swaps
the LABEL, not the model: the target for a player-season t becomes the
minutes-weighted mean of raptor_defense over {t-1, t, t+1}.

Leakage rule: for the fold holding out season S, every TRAINING row's
window drops S (a training target may never contain the held-out season's
label realization). The eval truth for S may use the full window — truth
is ground truth, and the model still sees only season-S features.

Adjacent labels: our fixed whole-season matrix rows (2013-14..2022-23)
keyed by (player, season), plus 2012-13 from the 538 historical archive
(hist538/, joined by norm_name). Windows truncate at the edges (2022-23
has no t+1 — 538 shut down).

Each fold trains TWO final GBMs on the production defense stack
(cv_resid_pools verbatim; component hats stay single-season):
  base   target = single-season y_def          (the production model)
  multi  target = per-fold multi-season y_def
and scores both against BOTH truths -> a 2x2 per fold:
  base|single (must reproduce RESULTS_hats3_cv), multi|single (does label
  smoothing help the existing metric?), base|multi + multi|multi (how high
  is the ceiling once the truth is stable skill, and who wins there?).

All predictions are saved for rank-outlier re-inspection.

Run:  python training/raptor2/cv_def_multi.py [--seedbase 0]
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import blend
from experiment_topk_rank import ranks, score_cells
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
SEASONS = list(STAMPS)


def adj_season(season, k):
    a = int(season[:4]) + k
    return f"{a}-{str(a + 1)[-2:]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seedbase", type=int, default=0)
    ap.add_argument("--blend", type=float, default=None,
                    help="train one model on (1-b)*single + b*multi target, "
                         "score vs single truth only")
    args = ap.parse_args()
    seeds = (args.seedbase, args.seedbase + 1, args.seedbase + 2)

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(HERE / "courtmate.npz")["CM"]
    mp = d["mp"].astype(np.float64)
    y = d[TARGETS["defense"]].astype(np.float64)
    players = np.array([str(p) for p in d["player"]])
    seasons_row = np.array([str(s) for s in d["season"]])
    rs = d["season_type"] == "Regular season"

    # ---- (norm_name, season) -> (y_def, mp) across 2012-13..2022-23 -------
    lab = {}
    at_stamp = np.isin(d["timestamp"], list(STAMPS.values()))
    for i in np.where(at_stamp & rs & np.isfinite(y))[0]:
        lab[(norm_name(players[i]), seasons_row[i])] = (y[i], max(mp[i], 1.0))
    hist = pd.read_csv(HERE / "hist538" / "historical_RAPTOR_by_player.csv")
    for r in hist[hist.season == 2013].itertuples():
        k = (norm_name(str(r.player_name)), "2012-13")
        lab.setdefault(k, (float(r.raptor_defense), max(float(r.mp), 1.0)))
    print(f"label lookup: {len(lab)} (player, season) entries", flush=True)

    def multi_target(i, exclude=None):
        nm, t = norm_name(players[i]), seasons_row[i]
        num = den = 0.0
        for k in (-1, 0, 1):
            s = adj_season(t, k)
            if s == exclude:
                continue
            v = lab.get((nm, s))
            if v is not None:
                num += v[0] * v[1]
                den += v[1]
        return num / den if den > 0 else np.nan

    # ---- production defense stack (cv_resid_pools verbatim) ---------------
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
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    labeled = rs & at_stamp & np.isfinite(y)
    Xf = np.hstack([X, Z, dfz["E"]])
    ref = json.loads((HERE / "RESULTS_hats3_cv.json").read_text())["defense"]

    per = {"base|single": {}, "multi|single": {},
           "base|multi": {}, "multi|multi": {}}
    rows_out = []
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        elm = np.where(te)[0][mp[te] >= FLOOR]
        ytr_multi = np.array([multi_target(i, exclude=season) if trn[i]
                              else np.nan for i in range(len(y))])
        ok = trn & np.isfinite(ytr_multi)
        ytruth_multi = np.array([multi_target(i) for i in elm])
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
        if args.blend is not None:
            b = args.blend
            yb = np.where(np.isfinite(ytr_multi),
                          (1 - b) * y + b * ytr_multi, y)
            p = blend(Xa[trn], yb[trn], Xa[elm], med, params, rounds, seeds)
            s = score_cells(y[elm], p, np.full(len(elm), season))
            per.setdefault("blend|single", {})[season] = \
                round(float(s["dev@10"]), 2)
            print(f"{season}: blend{b}|single {s['dev@10']:.2f} "
                  f"(stored base {ref['per_season'][season]})", flush=True)
            continue
        p_base = blend(Xa[trn], y[trn], Xa[elm], med, params, rounds, seeds)
        med2 = np.nanmedian(Xa[ok], axis=0)
        med2 = np.where(np.isfinite(med2), med2, 0.0)
        p_multi = blend(Xa[ok], ytr_multi[ok], Xa[elm], med2, params, rounds,
                        seeds)
        sv = np.full(len(elm), season)
        for tag, p, t in (("base|single", p_base, y[elm]),
                          ("multi|single", p_multi, y[elm]),
                          ("base|multi", p_base, ytruth_multi),
                          ("multi|multi", p_multi, ytruth_multi)):
            s = score_cells(t, p, sv)
            per[tag][season] = round(float(s["dev@10"]), 2)
        rt_s, rt_m = ranks(y[elm]), ranks(ytruth_multi)
        rb, rm = ranks(p_base), ranks(p_multi)
        for j, i in enumerate(elm):
            rows_out.append({
                "season": season, "player": players[i], "mp": int(mp[i]),
                "y_single": round(float(y[i]), 2),
                "y_multi": round(float(ytruth_multi[j]), 2),
                "est_base": round(float(p_base[j]), 2),
                "est_multi": round(float(p_multi[j]), 2),
                "rank_true_single": int(rt_s[j]) + 1,
                "rank_true_multi": int(rt_m[j]) + 1,
                "rank_est_base": int(rb[j]) + 1,
                "rank_est_multi": int(rm[j]) + 1})
        print(f"{season}: b|s {per['base|single'][season]:.2f} "
              f"(stored {ref['per_season'][season]}) "
              f"m|s {per['multi|single'][season]:.2f} "
              f"b|m {per['base|multi'][season]:.2f} "
              f"m|m {per['multi|multi'][season]:.2f}", flush=True)

    out = {"seedbase": args.seedbase, "blend": args.blend, "summary": {}}
    for tag, pp in per.items():
        if not pp:
            continue
        dv = list(pp.values())
        out["summary"][tag] = {"per_season": pp,
                               "median": float(np.median(dv)),
                               "mean": float(np.mean(dv))}
        print(f"{tag:<13} median {np.median(dv):.2f} mean {np.mean(dv):.2f}",
              flush=True)
    tag = f"_s{args.seedbase}" if args.seedbase else ""
    if args.blend is not None:
        tag += f"_b{args.blend}"
    fp = HERE / f"RESULTS_cv_def_multi{tag}.json"
    fp.write_text(json.dumps(out, indent=1))
    if args.seedbase == 0 and args.blend is None:
        (HERE / "DIAG_def_multi_rows.json").write_text(
            json.dumps(rows_out, indent=1))
        print("wrote DIAG_def_multi_rows.json", flush=True)
    print(f"wrote {fp}", flush=True)


if __name__ == "__main__":
    main()
