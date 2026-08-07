"""Round 3: nan-safe ordering null, base-model elite-ordering diagnostic, and
elite-weighted losses.

Round 2 established: within-elite ordering signal exists (253 features beat the
per-feature permutation null, ~29 expected by chance), led by on-court team
defense (OpponentPoints on / OnDefRtg / wowy diff, |rho| ~ 0.40) and rim/defend
features -- a different set from the membership features (blocks, DFG%). Small
feature sets and a two-stage specialist both LOST to the base model, so the full
profile matters and the intervention has to happen inside the loss, not the
feature set or architecture.

Here:
  1  family-wise null done nan-safely (drop unusable columns first), closing the
     significance question properly.
  2  diagnostic: the base model's own Spearman within the TRUE top-30 of each test
     cell -- compared against the best single feature (~0.40) and the label's own
     year-over-year self-consistency within the elite (0.42), which brackets what
     ordering skill is achievable.
  3  loss intervention: sample-weighted training emphasizing the elite tail
     (continuous sigmoid weight and hard top-quantile weight), full feature set,
     scored on the test cells.

Run:  python training/study_defense_elite3.py
"""

import json
from pathlib import Path

import numpy as np
from scipy.stats import rankdata, spearmanr

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

import lightgbm as lgb
from sklearn.linear_model import RidgeCV

RS_MIN, PO_MIN = 50, 10
FLOOR = 1065
TOPK = 30
N_PERM = 400
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}
SEEDS = (0, 1, 2)


def col_ranks_clean(M):
    """Rank matrix over usable columns only. Returns (Z, usable_mask)."""
    n, m = M.shape
    usable = np.zeros(m, bool)
    R = np.zeros((n, m))
    for j in range(m):
        x = M[:, j]
        ok = np.isfinite(x)
        if ok.sum() < 15:
            continue
        r = np.full(n, np.nan)
        r[ok] = rankdata(x[ok])
        r = np.where(np.isfinite(r), r, np.nanmean(r))
        sd = r.std()
        if sd == 0:
            continue
        R[:, j] = (r - r.mean()) / sd
        usable[j] = True
    return R, usable


def weighted_blend(Xtr, ytr, w, Xte, med, params, rounds):
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
    names = feat + [f"cellrel|{i}" for i in range(Z.shape[1])] \
        + [f"defend|{str(n)}" for n in dfz["enames"]]
    y = d[TARGETS["defense"]].astype(np.float64)
    mp = d["mp"].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    cells = {s: np.where(rs & (d["timestamp"] == t) & (mp >= FLOOR))[0]
             for s, t in STAMPS.items()}
    n_feat = Xf.shape[1]
    rng = np.random.default_rng(0)

    # ---- 1. nan-safe family-wise null --------------------------------------
    obs = np.zeros((len(cells), n_feat))
    used = np.zeros((len(cells), n_feat), bool)
    null = np.zeros((N_PERM, n_feat))
    for si, (season, idx) in enumerate(cells.items()):
        top = idx[np.argsort(-y[idx])[:TOPK]]
        Mz, usable = col_ranks_clean(Xf[top].astype(np.float64))
        ry = rankdata(y[top])
        ry = (ry - ry.mean()) / ry.std()
        obs[si] = ry @ Mz / len(top)
        used[si] = usable
        for p in range(N_PERM):
            null[p] += rng.permutation(ry) @ Mz / len(top)
    n_seasons_used = used.sum(0)
    ok_feat = n_seasons_used >= 8
    obs_mean = obs.sum(0) / np.maximum(n_seasons_used, 1)
    null_mean = null / len(cells)
    fam95 = float(np.quantile(np.abs(null_mean[:, ok_feat]).max(axis=1), 0.95))
    per975 = np.quantile(np.abs(null_mean), 0.975, axis=0)
    n_fam = int((np.abs(obs_mean[ok_feat]) > fam95).sum())
    n_unc = int((np.abs(obs_mean[ok_feat]) > per975[ok_feat]).sum())
    print(f"ordering null (nan-safe, {int(ok_feat.sum())} usable features):",
          flush=True)
    print(f"  family-wise 95% threshold: {fam95:.3f}", flush=True)
    print(f"  beating family-wise: {n_fam} | beating per-feature 97.5%: "
          f"{n_unc} (chance ~{int(0.025 * ok_feat.sum())})", flush=True)
    okj = np.where(ok_feat)[0]
    top_ord = okj[np.argsort(-np.abs(obs_mean[okj]))[:10]]
    for j in top_ord:
        print(f"    {names[j]:<48} rho {obs_mean[j]:+.3f}"
              f"{'  ***' if abs(obs_mean[j]) > fam95 else ''}", flush=True)

    # ---- 2. base model's own within-elite ordering -------------------------
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    tr = (fit | val) & rs
    tr_m = tr & np.isin(d["timestamp"], list(STAMPS.values()))
    test = test & rs
    cells_te = np.array([str(s) for s in d["season"][test]])
    el_te = mp[test] >= FLOOR
    tuned = json.loads((REPO_ROOT / "training" / "tuned_params.json").read_text())
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    med = np.nanmedian(Xf[tr_m], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)

    p_base = blend(Xf[tr_m], y[tr_m], Xf[test], med, params, rounds)
    print("\nbase model rho within TRUE top-30 (test cells):", flush=True)
    for c in np.unique(cells_te):
        m = test.copy()
        sel = (cells_te == c) & el_te
        yv = y[test][sel]
        pv = p_base[sel]
        top = np.argsort(-yv)[:TOPK]
        print(f"  {c}: {spearmanr(pv[top], yv[top]).statistic:+.3f} "
              f"(best single feature ~0.40, label YoY self-consistency 0.42)",
              flush=True)

    # ---- 3. elite-weighted losses ------------------------------------------
    def report(name, p_el):
        s = score_cells(y[test][el_te], p_el, cells_te[el_te])
        print(f"  {name:<26} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@30={s['tau@30']:+.3f} hits@10={s['hits@10']}/20 "
              f"MAE={s['mae']:.3f}", flush=True)
        return s

    print("\nelite-weighted loss arms (test cells, >=1065 pool):", flush=True)
    s_base = report("base (uniform weight)", p_base[el_te])

    # per-cell elite threshold on eligible training rows; weights for all rows
    thr = np.zeros(Xf.shape[0])
    for c in np.unique(cells_all[tr_m]):
        m = tr_m & (cells_all == c)
        el = m & (mp >= FLOOR)
        if el.sum() >= 50:
            thr[m] = np.quantile(y[el], 0.85)
        else:
            thr[m] = np.quantile(y[m], 0.85)
    ytr = y[tr_m]
    results = {"base": s_base}
    for name, w in [
            ("sigmoid x3", 1 + 3.0 / (1 + np.exp(-(ytr - thr[tr_m]) / 0.3))),
            ("sigmoid x8", 1 + 8.0 / (1 + np.exp(-(ytr - thr[tr_m]) / 0.3))),
            ("hard top15% x4", 1 + 4.0 * (ytr >= thr[tr_m])),
    ]:
        p = weighted_blend(Xf[tr_m], ytr, w, Xf[test], med, params, rounds)
        results[name] = report(name, p[el_te])

    out = {"fam95": fam95, "n_familywise": n_fam, "n_uncorrected": n_unc,
           "ordering_top": [(names[j], float(obs_mean[j])) for j in top_ord],
           "arms": results}
    Path(REPO_ROOT / "training"
         / "RESULTS_defense_elite_study3.json").write_text(
        json.dumps(out, indent=1, default=str))
    print("\nwrote RESULTS_defense_elite_study3.json", flush=True)


if __name__ == "__main__":
    main()
