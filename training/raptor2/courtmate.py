"""Courtmate-chain on-off ratings (538's steps 2 and 3) from possession lineups.

The document's on-off component regresses three ratings: (1) the player's own
on-court ratings, (2) his COURTMATES' ratings without him -- "weighted by the
number of possessions that the courtmate shared with the player, multiplied by
the number of possessions that the courtmate had without the player" -- and
(3) the courtmates' courtmates' ratings. Team-without-player (WOWY off) is the
construction the document explicitly calls inferior; this builds the real one.

Per season, one pass over possessions accumulates per-player and per-teammate-
pair totals (possessions and points, offense and defense). Then:

  rtg_without(t | p)  = 100*(pts_t - pts_shared(p,t)) / (poss_t - shared(p,t))
  cw(p)               = sum_t w * rtg_without(t|p) / sum_t w,
                        w = shared(p,t) * (poss_t - shared(p,t))
  cc(p)               = sum_t w * cw(t) / sum_t w      (step-3 approximation:
                        each courtmate's own step-2 rating stands in for his
                        "other courtmates without him")

Outputs raptor2/courtmate.npz: per player-season on/cw/cc offensive and
defensive ratings + possession counts, plus an on-off v3 component fit report
(ridge on [on, cw, cc] vs the published on-off component labels).

Run:  python training/raptor2/courtmate.py
"""

import json
import sys
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scipy.stats import spearmanr
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare, splits
from predict_seasons import DROP_FEATURES

TD = REPO_ROOT / "training"
BUILD = Path("/tmp/rapm_build")
SEASON_OF = {2013: "2013-14", 2014: "2014-15", 2015: "2015-16",
             2016: "2016-17", 2017: "2017-18", 2018: "2018-19",
             2019: "2019-20", 2020: "2020-21", 2021: "2021-22",
             2022: "2022-23", 2023: "2023-24", 2024: "2024-25",
             2025: "2025-26"}
STAMP_OF = {"2013-14": "20140715000000", "2014-15": "20150715000000",
            "2015-16": "20160715000000", "2016-17": "20170715000000",
            "2017-18": "20180715000000", "2018-19": "20190715000000",
            "2019-20": "20201101000000", "2020-21": "20210801000000",
            "2021-22": "20220715000000", "2022-23": "20230715000000"}
MIN_SHARED = 50
MIN_WITHOUT = 100
COLS = ["on_ortg", "on_drtg", "cw_ortg", "cw_drtg", "cc_ortg", "cc_drtg",
        "poss_on"]


def pair_key(a, b):
    lo, hi = np.minimum(a, b), np.maximum(a, b)
    return lo.astype(np.int64) * 10_000_000 + hi.astype(np.int64)


def season_chain(poss):
    """-> {player_id: [on_o, on_d, cw_o, cw_d, cc_o, cc_d, poss]}"""
    O = poss[[f"o{i}" for i in range(5)]].values
    D = poss[[f"d{i}" for i in range(5)]].values
    pts = poss["pts"].values.astype(np.float64)

    tot = {}   # side -> (poss_series, pts_series) per player
    shared = {}
    for side, M in (("o", O), ("d", D)):
        flat = M.ravel()
        rep = np.repeat(pts, 5)
        s_poss = pd.Series(1.0, index=flat).groupby(level=0).sum()
        s_pts = pd.Series(rep, index=flat).groupby(level=0).sum()
        tot[side] = (s_poss, s_pts)
        keys, vals = [], []
        for i, j in combinations(range(5), 2):
            keys.append(pair_key(M[:, i], M[:, j]))
            vals.append(pts)
        k = np.concatenate(keys)
        v = np.concatenate(vals)
        p_poss = pd.Series(1.0, index=k).groupby(level=0).sum()
        p_pts = pd.Series(v, index=k).groupby(level=0).sum()
        shared[side] = (p_poss, p_pts)

    players = sorted(set(tot["o"][0].index) | set(tot["d"][0].index))
    # teammate lists from offensive pairs (teams are identical on both sides)
    mates = {}
    for key in shared["o"][0].index:
        a, b = divmod(int(key), 10_000_000)
        mates.setdefault(a, []).append(b)
        mates.setdefault(b, []).append(a)

    def rtg_without(side, t, p):
        sp, spts = shared[side]
        tp, tpts = tot[side]
        key = pair_key(np.array([t]), np.array([p]))[0]
        sh = sp.get(key, 0.0)
        if sh < MIN_SHARED:
            return None, 0.0
        poss_wo = tp.get(t, 0.0) - sh
        if poss_wo < MIN_WITHOUT:
            return None, 0.0
        pts_wo = tpts.get(t, 0.0) - spts.get(key, 0.0)
        return 100.0 * pts_wo / poss_wo, sh * poss_wo

    cw = {}
    for p in players:
        row = {}
        for side in ("o", "d"):
            num = den = 0.0
            for t in mates.get(p, []):
                r, wgt = rtg_without(side, t, p)
                if r is not None:
                    num += wgt * r
                    den += wgt
            row[side] = (num / den) if den > 0 else np.nan
            row[side + "_den"] = den
        cw[p] = row

    out = {}
    for p in players:
        cc = {}
        for side in ("o", "d"):
            num = den = 0.0
            for t in mates.get(p, []):
                r, wgt = rtg_without(side, t, p)
                if r is not None and np.isfinite(cw[t][side]):
                    num += wgt * cw[t][side]
                    den += wgt
            cc[side] = (num / den) if den > 0 else np.nan
        po = tot["o"][0].get(p, 0.0)
        pd_ = tot["d"][0].get(p, 0.0)
        out[p] = [
            100.0 * tot["o"][1].get(p, 0.0) / po if po > 0 else np.nan,
            100.0 * tot["d"][1].get(p, 0.0) / pd_ if pd_ > 0 else np.nan,
            cw[p]["o"], cw[p]["d"], cc["o"], cc["d"], po]
    return out


def main():
    names = {}
    for f in (REPO_ROOT / "scraping" / "rosters").glob(
            "roster_*_Regular_season.json"):
        for pid, rec in json.loads(f.read_text())["players"].items():
            names.setdefault(int(pid), rec["name"])

    chain = {}
    for yr, season in SEASON_OF.items():
        fp = BUILD / f"attrib_poss_{yr}.csv"
        if not fp.exists():
            print(f"[{season}] possessions not parsed yet -- skipped",
                  flush=True)
            continue
        res = season_chain(pd.read_csv(fp))
        for pid, row in res.items():
            chain[(season, norm_name(names.get(int(pid), f"id{pid}")))] = row
        print(f"[{season}] {len(res)} players", flush=True)

    X, feat, d = prepare(str(TD / "data_fixed"))
    players = np.array([str(p) for p in d["player"]])
    season_by_row = np.array([str(s) for s in d["season"]])
    rs = d["season_type"] == "Regular season"
    n = len(players)
    CM = np.full((n, len(COLS)), np.nan)
    for i in range(n):
        stamp = STAMP_OF.get(season_by_row[i])
        if stamp is None or d["timestamp"][i] != stamp or not rs[i]:
            continue
        row = chain.get((season_by_row[i], norm_name(players[i])))
        if row is not None:
            CM[i] = row
    print(f"courtmate coverage: {int(np.isfinite(CM[:, 2]).sum())} rows",
          flush=True)
    np.savez_compressed(TD / "raptor2" / "courtmate.npz", CM=CM,
                        names=np.array(COLS))
    # raw (season|name) -> ratings map, for rows outside combined.npz (the
    # 2023-26 projection matrix)
    (TD / "raptor2" / "courtmate_chain.json").write_text(json.dumps(
        {f"{s}|{nm}": [None if not np.isfinite(v) else round(float(v), 4)
                       for v in row]
         for (s, nm), row in chain.items()}))
    print("wrote raptor2/courtmate_chain.json", flush=True)

    # ---- on-off v3 component fit report ------------------------------------
    from structural import cell_relative
    comp = np.load(TD / "data_fixed" / "components.npz")
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    fit, val, test = splits(d, 50, 10)
    tr = (fit | val) & rs
    w = np.sqrt(np.maximum(mp, 1.0))
    OO3o = cell_relative(CM[:, [0, 2, 4]], cells, mp)
    OO3d = cell_relative(-CM[:, [1, 3, 5]], cells, mp)   # negate: lower DRtg = better
    for tag, M, labname in (("onoff3_o", OO3o, "rap_onoff_o"),
                            ("onoff3_d", OO3d, "rap_onoff_d")):
        yv = comp[labname]
        m = tr & np.isfinite(yv) & np.isfinite(M).all(axis=1)
        model = RidgeCV(alphas=np.logspace(-3, 3, 25)).fit(
            M[m], yv[m], sample_weight=w[m])
        hat = model.predict(np.where(np.isfinite(M), M, 0.0))
        print(f"[{tag}] n={int(m.sum())} coefs "
              f"on={model.coef_[0]:+.3f} cw={model.coef_[1]:+.3f} "
              f"cc={model.coef_[2]:+.3f} | rho vs {labname}: "
              f"{spearmanr(hat[m], yv[m]).statistic:+.3f}", flush=True)
    print("wrote raptor2/courtmate.npz", flush=True)


if __name__ == "__main__":
    main()
