"""Positional matchups v2: defended-FGA-weighted matching, full 10-season
history.

Changes from v1 (posmatch.py):
  history   all ten labeled seasons (lineups now parsed through 2022-23)
  matching  scoring events are distributed over the five on-court defenders by
            position-overlap x the defender's defended-FGA rate (nearest-
            defender volume from the defend block), normalized on court --
            defenders who actually absorb shot defense absorb more matchup
            credit. Rebound events keep pure position-overlap weights.

Tests: production defense features +/- the two defensive matchup variables,
full training rows, test seasons; then a 10-fold season-held-out CV of the
same pair of arms.

Run:  python training/raptor2/posmatch2.py
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import blend
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

TD = REPO_ROOT / "training"
BUILD = Path("/tmp/rapm_build")
FLOOR = 1065
SEASON_OF = {2013: "2013-14", 2014: "2014-15", 2015: "2015-16",
             2016: "2016-17", 2017: "2017-18", 2018: "2018-19",
             2019: "2019-20", 2020: "2020-21", 2021: "2021-22",
             2022: "2022-23"}
STAMP_OF = {"2013-14": "20140715000000", "2014-15": "20150715000000",
            "2015-16": "20160715000000", "2016-17": "20170715000000",
            "2017-18": "20180715000000", "2018-19": "20190715000000",
            "2019-20": "20201101000000", "2020-21": "20210801000000",
            "2021-22": "20220715000000", "2022-23": "20230715000000"}
POS = ["PG", "SG", "SF", "PF", "C"]


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    cols = {n: i for i, n in enumerate(feat)}
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    enames = [str(x) for x in dfz["enames"]]
    dfga = dfz["E"][:, enames.index("dfga36")].astype(np.float64)
    mp = d["mp"].astype(np.float64)
    players = np.array([str(p) for p in d["player"]])
    season_by_row = np.array([str(s) for s in d["season"]])
    rs = d["season_type"] == "Regular season"

    names = {}
    for f in (REPO_ROOT / "scraping" / "rosters").glob(
            "roster_*_Regular_season.json"):
        for pid, rec in json.loads(f.read_text())["players"].items():
            names.setdefault(int(pid), rec["name"])

    P = np.column_stack([X[:, cols[f"ctx|pos_{p}"]] for p in POS])
    posvec, dfga_of = {}, {}
    for season, stamp in STAMP_OF.items():
        m = rs & (d["timestamp"] == stamp)
        for i in np.where(m)[0]:
            key = (season, norm_name(players[i]))
            v = np.nan_to_num(P[i].astype(np.float64))
            s = v.sum()
            posvec[key] = (v / s) if s > 0 else np.full(5, 0.2)
            if np.isfinite(dfga[i]) and dfga[i] > 0:
                dfga_of[key] = dfga[i]

    acc = {}
    for yr, season in SEASON_OF.items():
        fp = BUILD / f"attrib_poss_{yr}.csv"
        if not fp.exists():
            print(f"[{season}] missing possessions", flush=True)
            continue
        poss = pd.read_csv(fp)
        ev = pd.read_csv(BUILD / f"attrib_events_{yr}.csv")
        O = poss[[f"o{i}" for i in range(5)]].values
        D = poss[[f"d{i}" for i in range(5)]].values
        pv, dg = {}, {}
        for pid in np.unique(np.concatenate([O.ravel(), D.ravel()])):
            key = (season, norm_name(names.get(int(pid), f"id{pid}")))
            pv[pid] = posvec.get(key, np.full(5, 0.2))
            dg[pid] = dfga_of.get(key, np.nan)

        def bump(name, j, v):
            k = (season, name)
            if k not in acc:
                acc[k] = np.zeros(5)
            acc[k][j] += v

        for row_d in D.ravel():
            bump(norm_name(names.get(int(row_d), f"id{row_d}")), 3, 0.2)
        for row_o in O.ravel():
            bump(norm_name(names.get(int(row_o), f"id{row_o}")), 4, 0.2)

        for kind, j, opp_side, use_dfga in (("score", 0, "D", True),
                                            ("oreb", 1, "D", False),
                                            ("dreb", 2, "O", False)):
            g = ev[ev["kind"] == kind]
            OPP = D if opp_side == "D" else O
            for pi, pid, v in zip(g["poss_idx"].values, g["player_id"].values,
                                  g["value"].values.astype(np.float64)):
                q = pv.get(pid)
                if q is None:
                    continue
                opps = OPP[pi]
                ws = np.array([float(q @ pv[o]) for o in opps])
                if use_dfga:
                    dvals = np.array([dg.get(o, np.nan) for o in opps])
                    fin = np.isfinite(dvals)
                    dvals = np.where(fin, dvals, np.nanmean(dvals)
                                     if fin.any() else 1.0)
                    ws = ws * dvals
                s = ws.sum()
                ws = ws / s if s > 0 else np.full(5, 0.2)
                for o, wgt in zip(opps, ws):
                    bump(norm_name(names.get(int(o), f"id{o}")), j, wgt * v)
        print(f"[{season}] accumulated", flush=True)

    n = len(players)
    PM = np.full((n, 3), np.nan)
    for i in range(n):
        stamp = STAMP_OF.get(season_by_row[i])
        if stamp is None or d["timestamp"][i] != stamp or not rs[i]:
            continue
        a = acc.get((season_by_row[i], norm_name(players[i])))
        if a is None or a[3] * 5 < 300:
            continue
        dp, op = a[3] * 5, a[4] * 5
        PM[i, 0] = a[0] * 100.0 / dp
        PM[i, 1] = a[1] * 100.0 / dp
        PM[i, 2] = a[2] * 100.0 / op if op > 0 else np.nan
    print(f"posmatch2 coverage: {int(np.isfinite(PM[:, 0]).sum())} rows",
          flush=True)
    np.savez_compressed(TD / "raptor2" / "posmatch2.npz", PM=PM)

    # ---- tests --------------------------------------------------------------
    Z = cellrel_features(X, feat, np.array(
        [f"{t}|{s}" for t, s in zip(d["timestamp"], d["season_type"])]),
        RELATIVE_COLS)
    y = d[TARGETS["defense"]].astype(np.float64)
    fit, val, test = splits(d, 50, 10)
    tr = (fit | val) & rs
    test = test & rs
    el = mp[test] >= FLOOR
    cells_te = np.array([str(s) for s in d["season"][test]])
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    out = {}
    for arm, blocks in (("gbm", [X, Z, dfz["E"]]),
                        ("gbm+pm2", [X, Z, dfz["E"], PM[:, :2]])):
        Xf = np.hstack(blocks)
        med = np.nanmedian(Xf[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p = blend(Xf[tr], y[tr], Xf[test], med, params, rounds)
        s = score_cells(y[test][el], p[el], cells_te[el])
        out[f"test|{arm}"] = {k: (int(v) if isinstance(v, (int, np.integer))
                                  else round(float(v), 4))
                              for k, v in s.items()}
        print(f"  test {arm:<8} dev@10={s['dev@10']:5.2f} "
              f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20", flush=True)

    labeled = rs & np.isin(d["timestamp"], list(STAMP_OF.values())) \
        & np.isfinite(y)
    for season, stamp in STAMP_OF.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        elm = mp[te] >= FLOOR
        row = {}
        for arm, blocks in (("gbm", [X, Z, dfz["E"]]),
                            ("gbm+pm2", [X, Z, dfz["E"], PM[:, :2]])):
            Xf = np.hstack(blocks)
            med = np.nanmedian(Xf[trn], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            p = blend(Xf[trn], y[trn], Xf[te], med, params, rounds)
            s = score_cells(y[te][elm], p[elm], np.full(int(elm.sum()), season))
            row[arm] = round(float(s["dev@10"]), 2)
            out[f"cv|{season}|{arm}"] = round(float(s["dev@10"]), 2)
        print(f"  cv {season}: gbm {row['gbm']:5.2f} | +pm2 "
              f"{row['gbm+pm2']:5.2f}", flush=True)
    g = [out[f"cv|{s}|gbm"] for s in STAMP_OF]
    h = [out[f"cv|{s}|gbm+pm2"] for s in STAMP_OF]
    print(f"  CV medians: gbm {np.median(g):.2f} | +pm2 {np.median(h):.2f}  "
          f"(head-to-head +pm2 {sum(b < a for a, b in zip(g, h))}W "
          f"{sum(b == a for a, b in zip(g, h))}T)", flush=True)
    Path(TD / "raptor2" / "RESULTS_posmatch2.json").write_text(
        json.dumps(out, indent=1))
    print("wrote raptor2/posmatch2.npz + RESULTS_posmatch2.json", flush=True)


if __name__ == "__main__":
    main()
