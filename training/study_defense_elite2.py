"""Round 2 of the elite-defender study: corrected stats + the models the findings license.

Fixes from round 1: membership AUC had inverted sign (ranks() is already
descending); the permutation null hit constant-column NaNs and averaged to NaN.
Here AUC uses tie-aware rankdata directly, and the null is vectorized (rank
matrices + matmul) with per-feature and family-wise (max-statistic) thresholds,
so "how many features carry real within-elite ordering signal" gets an honest
answer with the look-elsewhere effect priced in.

Then the interventions round 1's findings motivate:

  UNION SET     round 1's ordering-only sufficient sets failed badly (dev@10 15+)
                because ordering features alone cannot do membership. Test the
                union: top-40 membership + top-40 ordering features.

  TWO-STAGE     membership and ordering are empirically different problems
                (different features, different signal strength). Stage 1 =
                production blend picks the top-40; stage 2 = a specialist pairwise
                model trained ONLY on true top-40s of the eight training seasons
                re-orders them. Arms: specialist re-rank, and rank-average of
                specialist and stage-1 order (hedges stage-2 variance).

Run:  python training/study_defense_elite2.py
"""

import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from scipy.stats import rankdata

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
FLOOR = 1065
TOPK = 30          # elite definition for the scan
K_STAGE = 40       # candidate set size for the two-stage model
N_PERM = 300
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}
ELITE_PARAMS = dict(objective="binary", learning_rate=0.05, num_leaves=15,
                    min_data_in_leaf=30, feature_fraction=0.5,
                    bagging_fraction=0.8, bagging_freq=1, lambda_l2=5.0,
                    verbose=-1, num_threads=0)
ELITE_ROUNDS = 300
SEEDS = (0, 1, 2)


def col_ranks(M):
    """Column-wise tie-aware ranks; NaN -> column mean rank; z-scored columns."""
    n, m = M.shape
    R = np.empty((n, m))
    for j in range(m):
        x = M[:, j]
        ok = np.isfinite(x)
        r = np.full(n, np.nan)
        if ok.sum() >= 3:
            r[ok] = rankdata(x[ok])
        R[:, j] = np.where(np.isfinite(r), r, np.nanmean(r) if ok.any() else 0)
    mu, sd = R.mean(0), R.std(0)
    sd[sd == 0] = 1.0
    return (R - mu) / sd


def auc_tieaware(x, pos):
    ok = np.isfinite(x)
    x, p = x[ok], pos[ok]
    n1, n0 = int(p.sum()), int((~p).sum())
    if n1 < 5 or n0 < 5:
        return np.nan
    r = rankdata(x)
    return (r[p].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)


def main():
    X, feat, d = prepare(str(REPO_ROOT / "training" / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    dfz = np.load(REPO_ROOT / "training" / "data_fixed" / "defend.npz",
                  allow_pickle=True)
    E, enames = dfz["E"], [str(n) for n in dfz["enames"]]
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    znames = [f"cellrel|{i}" for i in range(Z.shape[1])]
    Xf = np.hstack([X, Z, E])
    names = feat + znames + [f"defend|{n}" for n in enames]
    y = d[TARGETS["defense"]].astype(np.float64)
    mp = d["mp"].astype(np.float64)
    rs = d["season_type"] == "Regular season"

    cells = {s: np.where(rs & (d["timestamp"] == t) & (mp >= FLOOR))[0]
             for s, t in STAMPS.items()}
    n_feat = Xf.shape[1]
    rng = np.random.default_rng(0)

    # ---- corrected membership AUC + vectorized ordering scan with null -----
    memb = np.full((len(cells), n_feat), np.nan)
    obs = np.zeros((len(cells), n_feat))
    null = np.zeros((N_PERM, n_feat))
    for si, (season, idx) in enumerate(cells.items()):
        yv = y[idx]
        order = np.argsort(-yv)
        top = idx[order[:TOPK]]
        pos = np.zeros(len(idx), bool)
        pos[order[:TOPK]] = True
        for j in range(n_feat):
            memb[si, j] = auc_tieaware(Xf[idx, j].astype(np.float64), pos)
        Mz = col_ranks(Xf[top].astype(np.float64))
        ry = rankdata(y[top])
        ry = (ry - ry.mean()) / ry.std()
        obs[si] = ry @ Mz / len(top)
        for p in range(N_PERM):
            rp = rng.permutation(ry)
            null[p] += rp @ Mz / len(top)
        print(f"  scanned {season}", flush=True)
    obs_mean = obs.mean(0)
    null /= len(cells)
    per_feat_975 = np.quantile(np.abs(null), 0.975, axis=0)
    fam_null_95 = np.quantile(np.abs(null).max(axis=1), 0.95)
    n_uncorr = int((np.abs(obs_mean) > per_feat_975).sum())
    n_fam = int((np.abs(obs_mean) > fam_null_95).sum())
    print(f"\nwithin-elite ordering signal (mean rho over 10 seasons):",
          flush=True)
    print(f"  per-feature null 97.5%: ~{np.median(per_feat_975):.3f} | "
          f"family-wise (max over {n_feat} feats) 95%: {fam_null_95:.3f}",
          flush=True)
    print(f"  features beating per-feature null: {n_uncorr} "
          f"(chance ~{int(0.025 * n_feat)}) | beating family-wise: {n_fam}",
          flush=True)

    memb_mean = np.nanmean(memb, axis=0)
    memb_key = np.abs(np.where(np.isfinite(memb_mean), memb_mean, 0.5) - 0.5)
    ord_key = np.abs(obs_mean)
    print("\ntop 15 MEMBERSHIP (corrected AUC, top-30 vs pool):", flush=True)
    for j in np.argsort(-memb_key)[:15]:
        print(f"  {names[j]:<48} AUC {memb_mean[j]:.3f}", flush=True)
    print("\ntop 15 ELITE-ORDERING (mean rho within top-30, vs fam null "
          f"{fam_null_95:.3f}):", flush=True)
    for j in np.argsort(-ord_key)[:15]:
        print(f"  {names[j]:<48} rho {obs_mean[j]:+.3f}", flush=True)

    # ---- model tests -------------------------------------------------------
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

    def report(name, p_el):
        s = score_cells(y[test][el_te], p_el, cells_te[el_te])
        print(f"  {name:<24} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@30={s['tau@30']:+.3f} hits@10={s['hits@10']}/20", flush=True)
        return s

    print("\nmodel tests (test cells, >=1065 pool):", flush=True)
    p_base = blend(Xf[tr_m], y[tr_m], Xf[test], med, params, rounds)[el_te]
    s_base = report("base matched+defend", p_base)

    union = np.union1d(np.argsort(-memb_key)[:40], np.argsort(-ord_key)[:40])
    Xs = Xf[:, union]
    ms = np.where(np.isfinite(np.nanmedian(Xs[tr_m], 0)),
                  np.nanmedian(Xs[tr_m], 0), 0.0)
    report(f"union-{len(union)} feats",
           blend(Xs[tr_m], y[tr_m], Xs[test], ms, params, rounds)[el_te])

    # two-stage: elite-pair specialist over true top-K_STAGE of training cells
    train_seasons = [s for s in STAMPS if s not in ("2013-14", "2014-15")]
    P_list, L_list = [], []
    for s in train_seasons:
        idx = cells[s]
        top = idx[np.argsort(-y[idx])[:K_STAGE]]
        ii, jj = np.meshgrid(np.arange(len(top)), np.arange(len(top)),
                             indexing="ij")
        m = ii.ravel() != jj.ravel()
        a, b = top[ii.ravel()[m]], top[jj.ravel()[m]]
        m = np.abs(y[a] - y[b]) >= 0.05
        a, b = a[m], b[m]
        P_list.append((Xf[a] - Xf[b]).astype(np.float32))
        L_list.append((y[a] > y[b]).astype(np.int8))
    P, L = np.vstack(P_list), np.concatenate(L_list)
    print(f"\n  elite-pair training set: {P.shape[0]:,} pairs "
          f"({len(train_seasons)} seasons x top-{K_STAGE})", flush=True)
    emodels = [lgb.train(dict(ELITE_PARAMS, seed=s, bagging_seed=s,
                              feature_fraction_seed=s),
                         lgb.Dataset(P, L), num_boost_round=ELITE_ROUNDS)
               for s in SEEDS]

    te_idx = np.where(test)[0][el_te]
    p2 = p_base.copy()
    p2_avg = p_base.copy()
    for c in np.unique(cells_te[el_te]):
        m = cells_te[el_te] == c
        sub_pos = np.where(m)[0]
        cand = sub_pos[np.argsort(-p_base[sub_pos])[:K_STAGE]]
        A = Xf[te_idx[cand]]
        ii, jj = np.triu_indices(len(cand), k=1)
        D = (A[ii] - A[jj]).astype(np.float32)
        w = np.mean([mm.predict(D) for mm in emodels], axis=0)
        wr = np.mean([mm.predict(-D) for mm in emodels], axis=0)
        wp = (w + 1 - wr) / 2
        wins = np.zeros(len(cand))
        np.add.at(wins, ii, wp)
        np.add.at(wins, jj, 1 - wp)
        wins /= len(cand) - 1
        base_hi = p_base[cand].max() + 1.0
        # specialist order replaces stage-1 order inside the candidate set
        r_spec = ranks(wins)
        p2[cand] = base_hi + (K_STAGE - r_spec)
        r_avg = ranks(-(ranks(wins) + ranks(p_base[cand])) / 2.0)
        p2_avg[cand] = base_hi + (K_STAGE - r_avg)
    report("two-stage specialist", p2)
    s_avg = report("two-stage rank-avg", p2_avg)

    out = {"n_uncorrected": n_uncorr, "n_familywise": n_fam,
           "fam_null_95": float(fam_null_95),
           "membership_top": [(names[j], float(memb_mean[j]))
                              for j in np.argsort(-memb_key)[:40]],
           "ordering_top": [(names[j], float(obs_mean[j]))
                            for j in np.argsort(-ord_key)[:40]],
           "base": s_base, "two_stage_rank_avg": s_avg}
    Path(REPO_ROOT / "training"
         / "RESULTS_defense_elite_study2.json").write_text(
        json.dumps(out, indent=1, default=str))
    print("\nwrote RESULTS_defense_elite_study2.json", flush=True)


if __name__ == "__main__":
    main()
