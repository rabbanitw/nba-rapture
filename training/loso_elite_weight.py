"""LOSO adjudication of elite-weighted defense losses vs the uniform base.

Round 3 of the elite study found sigmoid elite-weighting a whisker ahead of the
base on the two test cells (dev@10 3.70 vs 3.80, tau@30 +0.552 vs +0.538) --
inside two-cell noise. This runs the full 10-fold leave-one-season-out, the only
selector this project trusts, over three arms:

  base          uniform weights (production defense: matched features + defend)
  sigmoid x3    w = 1 + 3 * sigmoid((y - cell 85th pct) / 0.3)
  sigmoid x8    w = 1 + 8 * sigmoid((y - cell 85th pct) / 0.3)

Scored per held-out season on the >=1065-minute pool: dev@10, dev@20, tau@30,
hits@10. Decision rule: an arm must beat base on median dev@10 AND win more
seasons head-to-head to displace it.

Run:  python training/loso_elite_weight.py
"""

import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}
SEEDS = (0, 1, 2)


def wblend(Xtr, ytr, w, Xte, med, params, rounds):
    models = [lgb.train(dict(params, seed=s, bagging_seed=s,
                             feature_fraction_seed=s),
                        lgb.Dataset(Xtr, ytr, weight=w),
                        num_boost_round=rounds) for s in SEEDS]
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    ridge = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(
        (A - mu) / sd, ytr, sample_weight=w)
    return 0.75 * np.mean([m.predict(Xte) for m in models], axis=0) \
        + 0.25 * ridge.predict((B - mu) / sd)


def main():
    X, feat, d = prepare(str(REPO_ROOT / "training" / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    dfz = np.load(REPO_ROOT / "training" / "data_fixed" / "defend.npz",
                  allow_pickle=True)
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    Xf = np.hstack([X, Z, dfz["E"]])
    y = d[TARGETS["defense"]].astype(np.float64)
    mp = d["mp"].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y)
    tuned = json.loads((REPO_ROOT / "training" / "tuned_params.json").read_text())
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)

    per_season = {}
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        tr = labeled & (d["timestamp"] != stamp)
        el = mp[te] >= FLOOR
        med = np.nanmedian(Xf[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        thr = np.zeros(int(tr.sum()))
        ytr = y[tr]
        ctr = cells_all[tr]
        mtr = mp[tr]
        for c in np.unique(ctr):
            m = ctr == c
            el_tr = m & (mtr >= FLOOR)
            thr[m] = np.quantile(ytr[el_tr] if el_tr.sum() >= 50 else ytr[m],
                                 0.85)
        arms = {"base": np.ones(len(ytr)),
                "sig3": 1 + 3.0 / (1 + np.exp(-(ytr - thr) / 0.3)),
                "sig8": 1 + 8.0 / (1 + np.exp(-(ytr - thr) / 0.3))}
        row = {}
        for name, w in arms.items():
            p = wblend(Xf[tr], ytr, w, Xf[te], med, params, rounds)
            s = score_cells(y[te][el], p[el],
                            np.full(int(el.sum()), season))
            row[name] = {k: s[k] for k in
                         ("dev@10", "dev@20", "tau@30", "hits@10", "mae")}
        per_season[season] = row
        print(f"{season}: " + "  ".join(
            f"{n} dev@10={row[n]['dev@10']:.2f} tau@30={row[n]['tau@30']:+.3f}"
            for n in arms), flush=True)

    print("\n== summary (10 folds) ==", flush=True)
    summary = {}
    for name in ("base", "sig3", "sig8"):
        dv = [per_season[s][name]["dev@10"] for s in STAMPS]
        tu = [per_season[s][name]["tau@30"] for s in STAMPS]
        h = sum(per_season[s][name]["hits@10"] for s in STAMPS)
        summary[name] = {"median_dev10": float(np.median(dv)),
                         "mean_dev10": float(np.mean(dv)),
                         "mean_tau30": float(np.mean(tu)),
                         "hits10": int(h)}
        print(f"  {name}: median dev@10 {np.median(dv):.2f}  mean "
              f"{np.mean(dv):.2f}  mean tau@30 {np.mean(tu):+.3f}  "
              f"hits@10 {h}/100", flush=True)
    for arm in ("sig3", "sig8"):
        wins = sum(per_season[s][arm]["dev@10"] < per_season[s]["base"]["dev@10"]
                   for s in STAMPS)
        ties = sum(per_season[s][arm]["dev@10"] == per_season[s]["base"]["dev@10"]
                   for s in STAMPS)
        print(f"  {arm} vs base head-to-head (dev@10): {wins}W "
              f"{ties}T {10 - wins - ties}L", flush=True)

    Path(REPO_ROOT / "training" / "RESULTS_loso_elite_weight.json").write_text(
        json.dumps({"per_season": per_season, "summary": summary}, indent=1))
    print("\nwrote RESULTS_loso_elite_weight.json", flush=True)


if __name__ == "__main__":
    main()
