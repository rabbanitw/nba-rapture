"""Positional-matchup variables from possession-level lineups + attribution.

538: "RAPTOR attempts to figure out which player was matched up with which
opponent ... player assignments are probabilistic." For every possession we
have the ten players on the floor and the attributed events; each scoring /
rebounding event is distributed over the five opposing players in proportion
to position-vector overlap (multi-hot positions from the matrix, normalized).

Variables per player-season (2013-14..2018-19 RS, the lineup-data window):
  posopp_pts100    points by positional matchups per 100 def poss   (defense)
  posopp_oreb100   OREBs by positional matchups per 100 def poss    (defense)
  posopp_dreb100   opponent DREBs by matchups per 100 off poss      (offense)

Then the tests, in-window (both test seasons are inside it):
  defense structural: DB + the two defensive variables (ridge, component label)
  defense GBM:        production features + the two variables
scored on the 2013-14/2014-15 test cells, >=1065 pools.

Run:  python training/raptor2/posmatch.py
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
from structural import cell_relative
from variables import build_variables
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
BUILD = Path("/tmp/rapm_build")
FLOOR = 1065
SEASON_OF = {2013: "2013-14", 2014: "2014-15", 2015: "2015-16",
             2016: "2016-17", 2017: "2017-18", 2018: "2018-19"}
STAMP_OF = {"2013-14": "20140715000000", "2014-15": "20150715000000",
            "2015-16": "20160715000000", "2016-17": "20170715000000",
            "2017-18": "20180715000000", "2018-19": "20190715000000"}
POS = ["PG", "SG", "SF", "PF", "C"]


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    cols = {n: i for i, n in enumerate(feat)}
    mp = d["mp"].astype(np.float64)
    players = np.array([str(p) for p in d["player"]])
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])

    # id -> standard name
    names = {}
    for f in (REPO_ROOT / "scraping" / "rosters").glob(
            "roster_*_Regular_season.json"):
        for pid, rec in json.loads(f.read_text())["players"].items():
            names.setdefault(int(pid), rec["name"])

    # per-season position vectors from the matrix's ctx dummies
    posvec = {}      # (season, norm_name) -> 5-vector
    P = np.column_stack([X[:, cols[f"ctx|pos_{p}"]] for p in POS])
    rs = d["season_type"] == "Regular season"
    for season, stamp in STAMP_OF.items():
        m = rs & (d["timestamp"] == stamp)
        for i in np.where(m)[0]:
            v = np.nan_to_num(P[i].astype(np.float64))
            s = v.sum()
            posvec[(season, norm_name(players[i]))] = \
                (v / s) if s > 0 else np.full(5, 0.2)

    acc = {}         # (season, name) -> [pts, oreb, dreb, defposs, offposs]
    for yr, season in SEASON_OF.items():
        poss = pd.read_csv(BUILD / f"attrib_poss_{yr}.csv")
        ev = pd.read_csv(BUILD / f"attrib_events_{yr}.csv")
        O = poss[[f"o{i}" for i in range(5)]].values
        D = poss[[f"d{i}" for i in range(5)]].values
        pv = {}
        for pid in np.unique(np.concatenate([O.ravel(), D.ravel()])):
            key = (season, norm_name(names.get(int(pid), f"id{pid}")))
            pv[pid] = posvec.get(key, np.full(5, 0.2))

        def bump(name, j, v):
            k = (season, name)
            if k not in acc:
                acc[k] = np.zeros(5)
            acc[k][j] += v

        # exposure
        for row_d in D.ravel():
            bump(norm_name(names.get(int(row_d), f"id{row_d}")), 3, 0.2)
        for row_o in O.ravel():
            bump(norm_name(names.get(int(row_o), f"id{row_o}")), 4, 0.2)
        # events: distribute over the five opponents by position overlap
        ev_by_kind = {k: g for k, g in ev.groupby("kind")}
        for kind, j, opp_side in (("score", 0, "D"), ("oreb", 1, "D"),
                                  ("dreb", 2, "O")):
            g = ev_by_kind.get(kind)
            if g is None:
                continue
            idxs = g["poss_idx"].values
            pids = g["player_id"].values
            vals = g["value"].values.astype(np.float64)
            OPP = D if opp_side == "D" else O
            for pi, pid, v in zip(idxs, pids, vals):
                q = pv.get(pid)
                if q is None:
                    continue
                opps = OPP[pi]
                ws = np.array([float(q @ pv[o]) for o in opps])
                s = ws.sum()
                ws = ws / s if s > 0 else np.full(5, 0.2)
                for o, wgt in zip(opps, ws):
                    bump(norm_name(names.get(int(o), f"id{o}")), j, wgt * v)
        print(f"[{season}] accumulated ({len(acc):,} player-season entries)",
              flush=True)

    # map to matrix rows
    n = len(players)
    PM = np.full((n, 3), np.nan)
    season_by_row = np.array([str(s) for s in d["season"]])
    for i in range(n):
        stamp = STAMP_OF.get(season_by_row[i])
        if stamp is None or d["timestamp"][i] != stamp or not rs[i]:
            continue
        k = (season_by_row[i], norm_name(players[i]))
        a = acc.get(k)
        if a is None or a[3] * 5 < 300:
            continue
        dp = a[3] * 5
        op = a[4] * 5
        PM[i, 0] = a[0] * 100.0 / dp
        PM[i, 1] = a[1] * 100.0 / dp
        PM[i, 2] = a[2] * 100.0 / op if op > 0 else np.nan
    print(f"posmatch coverage: {np.isfinite(PM[:, 0]).sum()} rows", flush=True)
    np.savez_compressed(TD / "raptor2" / "posmatch.npz", PM=PM,
                        names=np.array(["posopp_pts100", "posopp_oreb100",
                                        "posopp_dreb100"]))

    # ---- tests --------------------------------------------------------------
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    DBv = cell_relative(np.hstack([V["DB"], PM[:, :2]]), cells, mp)
    DB0 = cell_relative(V["DB"], cells, mp)
    w = np.sqrt(np.maximum(mp, 1.0))
    fit, val, test = splits(d, 50, 10)
    tr = (fit | val) & rs
    test = test & rs
    in_win = np.isin(d["timestamp"], list(STAMP_OF.values())) & rs
    tr_w = tr & in_win
    el = mp[test] >= FLOOR
    cells_te = np.array([str(s) for s in d["season"][test]])
    from scipy.stats import spearmanr
    yv = comp["rap_box_d"]
    for tag, M in (("DB", DB0), ("DB+posmatch", DBv)):
        m = tr_w & np.isfinite(yv)
        hat = ridge_hat(M[m], yv[m], w[m], M,
                        V["DB_NAMES"] + ["posopp_pts100", "posopp_oreb100"]
                        if tag != "DB" else V["DB_NAMES"], tag)
        print(f"  [{tag}] rho vs rap_box_d (in-window train): "
              f"{spearmanr(hat[m], yv[m]).statistic:+.3f}", flush=True)

    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    y = d[TARGETS["defense"]].astype(np.float64)
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    out = {}
    for arm, blocks in (("gbm-inwin", [X, Z, dfz["E"]]),
                        ("gbm-inwin+pm", [X, Z, dfz["E"], PM[:, :2]])):
        Xf = np.hstack(blocks)
        med = np.nanmedian(Xf[tr_w], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p = blend(Xf[tr_w], y[tr_w], Xf[test], med, params, rounds)
        s = score_cells(y[test][el], p[el], cells_te[el])
        out[arm] = {k: (int(v) if isinstance(v, (int, np.integer))
                        else round(float(v), 4)) for k, v in s.items()}
        print(f"  {arm:<14} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} MAE={s['mae']:.3f} "
              f"hits@10={s['hits@10']}/20", flush=True)
    Path(TD / "raptor2" / "RESULTS_posmatch.json").write_text(
        json.dumps(out, indent=1))
    print("wrote raptor2/posmatch.npz + RESULTS_posmatch.json", flush=True)


if __name__ == "__main__":
    main()
