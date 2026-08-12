"""Season-grouped 10-fold CV: does the defensive rapm-hat beat the production
defense model?

On the test seasons the hat arm won 3.60 vs 3.80 -- a two-cell margin of exactly
the size that has fooled this project before. Ten-fold season-held-out CV
decides, with strict per-fold hygiene:

  RAPM pool   for a held-out season inside the 2013-19 window, the pooled RAPM
              target is REFIT from the cached possession files with that
              season's possessions excluded (fixed lambda 0.01, the value
              RidgeCV chose for both full pools). Held-out seasons outside the
              window use the full 6-year pool.
  aux model   trained per fold on that fold's training rows only,
              player-grouped 5-fold OOF hats for the training rows, full-fit
              hats for the held-out rows.
  arms        production defense (matched+defend, 3-seed members + ridge) with
              and without the hat column. Same fold seeds, same params.

Decision rule (unchanged): the hat must beat baseline on median dev@10 AND win
more seasons head-to-head.

Run:  python training/loso_rapm_hat.py
"""

import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy import sparse as sp
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

FLOOR = 1065
MIN_POSS = 4000
LAM = 0.01
SEEDS = (10, 11, 12)
TD = REPO_ROOT / "training"
BUILD = Path("/tmp/rapm_build")
WIN = {"2013-14": 2013, "2014-15": 2014, "2015-16": 2015, "2016-17": 2016,
       "2017-18": 2017, "2018-19": 2018}
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def fit_pool(poss):
    players = sorted(set(np.unique(poss[[f"o{i}" for i in range(5)]].values))
                     | set(np.unique(poss[[f"d{i}" for i in range(5)]].values)))
    pidx = {p: i for i, p in enumerate(players)}
    P, n = len(players), len(poss)
    rows = np.repeat(np.arange(n), 10)
    cols = np.empty(n * 10, dtype=np.int64)
    vals = np.empty(n * 10, dtype=np.float64)
    O = poss[[f"o{i}" for i in range(5)]].values
    D = poss[[f"d{i}" for i in range(5)]].values
    for j in range(5):
        cols[j::10] = [pidx[p] for p in O[:, j]]
        vals[j::10] = 1.0
        cols[5 + j::10] = [pidx[p] + P for p in D[:, j]]
        vals[5 + j::10] = -1.0
    X = sp.csr_matrix((vals, (rows, cols)), shape=(n, 2 * P))
    y = 100.0 * poss["pts"].values.astype(np.float64)
    model = Ridge(alpha=(LAM * n) / 2.0, fit_intercept=True).fit(X, y)
    d_coef = model.coef_[P:]
    cnt = np.bincount(cols[np.tile(np.arange(10) >= 5, n)] - P, minlength=P)
    d_coef = d_coef - np.average(d_coef, weights=np.maximum(cnt, 1))
    return {str(p): (v, c) for p, v, c in zip(players, d_coef, cnt)}


def lgbm_ridge_members(Xtr, t, Xte, med, params, rounds, seeds=SEEDS,
                       ridge_w=0.25):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, t).predict(
        (B - mu) / sd)
    return [(1 - ridge_w) * lgb.train(
        dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s),
        lgb.Dataset(Xtr, t), num_boost_round=rounds).predict(Xte) + ridge_w * pr
        for s in seeds]


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    Xd = np.hstack([X, Z, dfz["E"]])
    y = d[TARGETS["defense"]].astype(np.float64)
    mp = d["mp"].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) & np.isfinite(y)
    players = np.array([norm_name(str(p)) for p in d["player"]])
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)

    frames = {s: pd.read_csv(BUILD / f"poss_{s}.csv")
              for s in (2013, 2014, 2015, 2016, 2017, 2018)}
    # id -> standard name (as in build_rapm)
    names = {}
    for f in (REPO_ROOT / "scraping" / "rosters").glob(
            "roster_*_Regular_season.json"):
        for pid, rec in json.loads(f.read_text())["players"].items():
            names.setdefault(pid, rec["name"])

    pools = {}
    full = pd.concat(frames.values(), ignore_index=True)
    pools["full"] = fit_pool(full)
    for season, yr in WIN.items():
        sub = pd.concat([v for k, v in frames.items() if k != yr],
                        ignore_index=True)
        pools[season] = fit_pool(sub)
        print(f"pool ex-{season}: {len(pools[season])} players", flush=True)

    per_season = {}
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        el = mp[te] >= FLOOR
        pool = pools.get(season, pools["full"])
        tgt = {norm_name(names.get(pid, f"id{pid}")): v
               for pid, (v, c) in pool.items() if c >= MIN_POSS}
        y_aux = np.array([tgt.get(p, np.nan) for p in players])
        m_aux = trn & np.isfinite(y_aux)
        hat = np.full(len(players), np.nan)
        for tri, tei in GroupKFold(n_splits=5).split(
                np.where(m_aux)[0], groups=players[m_aux]):
            rows_tr = np.where(m_aux)[0][tri]
            rows_te = np.where(m_aux)[0][tei]
            m = lgb.train(dict(params, seed=10),
                          lgb.Dataset(Xd[rows_tr], y_aux[rows_tr]),
                          num_boost_round=rounds)
            hat[rows_te] = m.predict(Xd[rows_te])
        fullm = lgb.train(dict(params, seed=10),
                          lgb.Dataset(Xd[m_aux], y_aux[m_aux]),
                          num_boost_round=rounds)
        hat[~m_aux] = fullm.predict(Xd[~m_aux])

        row = {}
        for arm, Xa in (("base", Xd),
                        ("hat", np.hstack([Xd, hat.reshape(-1, 1)]))):
            med = np.nanmedian(Xa[trn], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            mem = lgbm_ridge_members(Xa[trn], y[trn], Xa[te], med, params,
                                     rounds)
            s = score_cells(y[te][el], np.mean(mem, axis=0)[el],
                            np.full(int(el.sum()), season))
            row[arm] = {k: (int(v) if isinstance(v, (int, np.integer))
                            else round(float(v), 4)) for k, v in s.items()}
        per_season[season] = row
        print(f"{season}: base dev@10={row['base']['dev@10']:.2f} "
              f"tau@30={row['base']['tau@30']:+.3f} | hat "
              f"dev@10={row['hat']['dev@10']:.2f} "
              f"tau@30={row['hat']['tau@30']:+.3f}", flush=True)

    print("\n== summary ==", flush=True)
    for arm in ("base", "hat"):
        dv = [per_season[s][arm]["dev@10"] for s in STAMPS]
        h = sum(per_season[s][arm]["hits@10"] for s in STAMPS)
        print(f"  {arm}: median dev@10 {np.median(dv):.2f}  mean "
              f"{np.mean(dv):.2f}  hits@10 {h}/100", flush=True)
    wins = sum(per_season[s]["hat"]["dev@10"] < per_season[s]["base"]["dev@10"]
               for s in STAMPS)
    ties = sum(per_season[s]["hat"]["dev@10"] == per_season[s]["base"]["dev@10"]
               for s in STAMPS)
    print(f"  hat vs base head-to-head (dev@10): {wins}W {ties}T "
          f"{10 - wins - ties}L", flush=True)
    (TD / "RESULTS_loso_rapm_hat_rep.json").write_text(
        json.dumps(per_season, indent=1))
    print("wrote RESULTS_loso_rapm_hat_rep.json", flush=True)


if __name__ == "__main__":
    main()
