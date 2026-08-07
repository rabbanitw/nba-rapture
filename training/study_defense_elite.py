"""Forensic study of the true top-30 defenders: what actually separates and orders them.

Every defensive experiment so far measured aggregate fit and let the model imply
feature relevance. This asks the questions directly, per labeled season (ten
whole-season cells, >=1065 minutes):

  MEMBERSHIP    per feature: AUC separating the true top-30 from the rest of the
                pool, plus sign consistency across the ten seasons. What makes a
                player an elite defender at all, in 538's labels?

  ORDERING      per feature: Spearman with rap_d restricted to the true top-30,
                against a permutation null (labels shuffled within the elite). If
                few or no features beat the null, the within-elite ordering signal
                does not exist in this feature space -- the dev@10~4-5 floor is
                explained, and model/loss changes cannot fix it.

  LABEL         year-over-year stability of rap_d for repeat-elite players. The
                label is 538's own model output; its self-consistency bounds what
                any predictor can recover. Reported alongside full-pool stability.

  COMPOSITION   position mix of the true elite, per season -- whether "top
                defender" is structurally a rim-protector category in the labels.

Then the interventions the findings license:

  PERMUTATION   the production defense model with feature groups permuted, scored
                on ELITE ordering (tau@30 / dev@20), not global MAE.

  SUFFICIENT    models on only the top-N ordering-relevant features vs all 1,169.
                If 20 features match the full matrix, the rest is noise for this
                task and the model/loss should change accordingly.

Run:  python training/study_defense_elite.py
"""

import json
from collections import defaultdict
from pathlib import Path

import lightgbm as lgb
import numpy as np
from scipy.stats import spearmanr
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
FLOOR = 1065
TOPK = 30
N_PERM = 200
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def auc_fast(x, pos_mask):
    """Rank-based AUC of feature x separating pos_mask, NaNs dropped."""
    ok = np.isfinite(x)
    x, p = x[ok], pos_mask[ok]
    if p.sum() < 5 or (~p).sum() < 5:
        return np.nan
    r = ranks(-x)          # descending rank, 0 = largest
    n1, n0 = p.sum(), (~p).sum()
    # AUC via rank-sum on ascending ranks
    asc = len(x) - 1 - r
    return (asc[p].sum() - n1 * (n1 - 1) / 2) / (n1 * n0)


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
    znames = [f"cellrel|{c.split('|', 1)[1]}" for c in RELATIVE_COLS if c in feat]
    # align znames length with Z (cell_relative skips missing cols)
    znames = znames[:Z.shape[1]] if len(znames) >= Z.shape[1] else \
        [f"cellrel|{i}" for i in range(Z.shape[1])]
    Xf = np.hstack([X, Z, E])
    names = feat + znames + [f"defend|{n}" for n in enames]
    y = d[TARGETS["defense"]].astype(np.float64)
    mp = d["mp"].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    players = np.array([str(p) for p in d["player"]])

    cells = {}
    for season, stamp in STAMPS.items():
        m = rs & (d["timestamp"] == stamp) & (mp >= FLOOR)
        cells[season] = np.where(m)[0]
    print(f"{len(names)} features; pools: "
          f"{[len(v) for v in cells.values()]}", flush=True)

    # ================= 1. composition + label stability ======================
    idxf = {n: i for i, n in enumerate(names)}
    big_col = Xf[:, idxf["ctx|pos_C"]] + Xf[:, idxf["ctx|pos_PF"]]
    comp_rows = []
    for season, idx in cells.items():
        top = idx[np.argsort(-y[idx])[:TOPK]]
        comp_rows.append((season, float(np.mean(big_col[top] > 0)),
                          float(np.mean(big_col[idx] > 0))))
    print("\nposition mix (share bigs C/PF):", flush=True)
    for s, elite, pool in comp_rows:
        print(f"  {s}: elite {elite:.0%} vs pool {pool:.0%}", flush=True)

    seasons_sorted = sorted(STAMPS)
    yoy_elite, yoy_pool = [], []
    for a, b in zip(seasons_sorted[:-1], seasons_sorted[1:]):
        pa = {players[i]: y[i] for i in cells[a]}
        pb = {players[i]: y[i] for i in cells[b]}
        common = sorted(set(pa) & set(pb))
        if len(common) < 30:
            continue
        va = np.array([pa[p] for p in common])
        vb = np.array([pb[p] for p in common])
        yoy_pool.append(spearmanr(va, vb).statistic)
        elite_a = {players[i] for i in
                   cells[a][np.argsort(-y[cells[a]])[:TOPK]]}
        ce = [p for p in common if p in elite_a]
        if len(ce) >= 10:
            va = np.array([pa[p] for p in ce])
            vb = np.array([pb[p] for p in ce])
            yoy_elite.append(spearmanr(va, vb).statistic)
    print(f"\nlabel YoY stability (Spearman, repeat players):", flush=True)
    print(f"  full pool: mean {np.mean(yoy_pool):+.3f} "
          f"(n={len(yoy_pool)} season-pairs)", flush=True)
    print(f"  within prior-year elite: mean {np.mean(yoy_elite):+.3f} "
          f"(n={len(yoy_elite)})", flush=True)

    # ================= 2. membership + within-elite ordering =================
    n_feat = Xf.shape[1]
    memb_auc = np.full((len(cells), n_feat), np.nan)
    order_rho = np.full((len(cells), n_feat), np.nan)
    null_max = []
    rng = np.random.default_rng(0)
    for si, (season, idx) in enumerate(cells.items()):
        yv = y[idx]
        order = np.argsort(-yv)
        top = idx[order[:TOPK]]
        is_top = np.zeros(len(idx), bool)
        is_top[order[:TOPK]] = True
        for j in range(n_feat):
            memb_auc[si, j] = auc_fast(Xf[idx, j].astype(np.float64), is_top)
            xt = Xf[top, j].astype(np.float64)
            ok = np.isfinite(xt)
            if ok.sum() >= 15:
                order_rho[si, j] = spearmanr(xt[ok], y[top][ok]).statistic
        # permutation null for the ordering stat: max |rho| across features
        yt = y[top]
        for _ in range(N_PERM // len(cells) + 1):
            yp = rng.permutation(yt)
            rhos = []
            for j in rng.choice(n_feat, size=200, replace=False):
                xt = Xf[top, j].astype(np.float64)
                ok = np.isfinite(xt)
                if ok.sum() >= 15:
                    rhos.append(abs(spearmanr(xt[ok], yp[ok]).statistic))
            if rhos:
                null_max.append(np.mean(rhos))
        print(f"  scanned {season}", flush=True)

    mean_auc = np.nanmean(memb_auc, axis=0)
    sign_consist = np.nanmean(np.sign(memb_auc - 0.5), axis=0)
    mean_rho = np.nanmean(order_rho, axis=0)
    rho_consist = np.nanmean(np.sign(order_rho), axis=0)
    null_mean_abs = float(np.mean(null_max))
    print(f"\nnull |rho| within elite (permuted labels): "
          f"mean {null_mean_abs:.3f}", flush=True)

    def top_table(scores, consist, title, k=20, key=None):
        key = key if key is not None else np.abs(scores - 0.5)
        order = np.argsort(-np.where(np.isfinite(key), key, -1))[:k]
        print(f"\n{title}", flush=True)
        for j in order:
            print(f"  {names[j]:<48} {scores[j]:+.3f}  "
                  f"consistency {consist[j]:+.2f}", flush=True)
        return order

    top_table(mean_auc, sign_consist,
              "top 20 MEMBERSHIP features (AUC top-30 vs rest):")
    order_feats = top_table(mean_rho, rho_consist,
                            "top 20 ELITE-ORDERING features (rho within true "
                            "top-30):", key=np.abs(mean_rho))
    n_beat_null = int(np.sum(np.abs(mean_rho) > 2 * null_mean_abs))
    print(f"\nfeatures with mean |rho| > 2x null: {n_beat_null} / {n_feat}",
          flush=True)

    # ================= 3. permutation importance on elite ordering ===========
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    tr = (fit | val) & rs
    isfull = np.isin(d["timestamp"], list(STAMPS.values()))
    tr_m = tr & isfull
    test = test & rs
    cells_te = np.array([f"{s}" for s in d["season"][test]])
    el_te = mp[test] >= FLOOR
    tuned = json.loads((REPO_ROOT / "training" / "tuned_params.json").read_text())
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    med = np.nanmedian(Xf[tr_m], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)

    base_pred = blend(Xf[tr_m], y[tr_m], Xf[test], med, params, rounds)
    s0 = score_cells(y[test][el_te], base_pred[el_te], cells_te[el_te])
    print(f"\nbase (matched+defend): dev@10={s0['dev@10']:.2f} "
          f"tau@30={s0['tau@30']:+.3f}", flush=True)

    groups = {"defend-eng": [j for j, n in enumerate(names)
                             if n.startswith("defend|")],
              "cell-relative": [j for j, n in enumerate(names)
                                if n.startswith("cellrel|")],
              "pbp-defensive": [j for j, n in enumerate(names)
                                if n.startswith("pbp|") and any(
                                    k in n for k in ("Steals", "Blocks", "Def",
                                                     "Fouls", "Opponent"))],
              "track-defense": [j for j, n in enumerate(names)
                                if n.startswith("track:defensive")],
              "wowy-all": [j for j, n in enumerate(names)
                           if n.split("|")[0].startswith("wowy")],
              "pbp-offensive-rest": [j for j, n in enumerate(names)
                                     if n.startswith("pbp|") and not any(
                                         k in n for k in ("Steals", "Blocks",
                                                          "Def", "Fouls",
                                                          "Opponent"))]}
    # permutation on the TEST matrix only (model fixed): what does elite ordering
    # lose when a group's information is destroyed?
    bst_models = None
    rng = np.random.default_rng(1)
    print("\npermutation importance on ELITE ordering (delta vs base):",
          flush=True)
    for gname, cols in groups.items():
        deltas_tau, deltas_dev = [], []
        for rep in range(5):
            Xp = Xf[test].copy()
            perm = rng.permutation(Xp.shape[0])
            Xp[:, cols] = Xp[perm][:, cols]
            pp = blend(Xf[tr_m], y[tr_m], Xp, med, params, rounds, seeds=(0,)) \
                if rep == 0 and False else None
            # reuse a single fitted model: refit once, predict permuted copies
            if bst_models is None:
                A = np.where(np.isfinite(Xf[tr_m]), Xf[tr_m], med)
                mu, sd = A.mean(0), A.std(0)
                sd[sd == 0] = 1.0
                ridge = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(
                    (A - mu) / sd, y[tr_m])
                bst_models = ([lgb.train(dict(params, seed=s, bagging_seed=s,
                                              feature_fraction_seed=s),
                                         lgb.Dataset(Xf[tr_m], y[tr_m]),
                                         num_boost_round=rounds)
                               for s in (0, 1, 2)], ridge, mu, sd)
            models, ridge, mu, sd = bst_models
            B = np.where(np.isfinite(Xp), Xp, med)
            pp = 0.75 * np.mean([m.predict(Xp) for m in models], axis=0) \
                + 0.25 * ridge.predict((B - mu) / sd)
            sp = score_cells(y[test][el_te], pp[el_te], cells_te[el_te])
            deltas_tau.append(sp["tau@30"] - s0["tau@30"])
            deltas_dev.append(sp["dev@20"] - s0["dev@20"])
        print(f"  {gname:<20} ({len(cols):>4} cols)  "
              f"d_tau@30={np.mean(deltas_tau):+.3f}  "
              f"d_dev@20={np.mean(deltas_dev):+.2f}", flush=True)

    # ================= 4. sufficient-set models ==============================
    print("\nsufficient-set models (ordering-relevant features only):",
          flush=True)
    rank_by_rho = np.argsort(-np.abs(np.where(np.isfinite(mean_rho),
                                              mean_rho, 0)))
    for n_keep in (15, 40, 100):
        cols = np.sort(rank_by_rho[:n_keep])
        Xs = Xf[:, cols]
        ms = np.nanmedian(Xs[tr_m], axis=0)
        ms = np.where(np.isfinite(ms), ms, 0.0)
        pp = blend(Xs[tr_m], y[tr_m], Xs[test], ms, params, rounds)
        sp = score_cells(y[test][el_te], pp[el_te], cells_te[el_te])
        print(f"  top-{n_keep:<4} dev@10={sp['dev@10']:5.2f} "
              f"dev@20={sp['dev@20']:5.2f} tau@30={sp['tau@30']:+.3f} "
              f"MAE={sp['mae']:.3f} hits@10={sp['hits@10']}/20", flush=True)

    out = {"null_mean_abs_rho": null_mean_abs,
           "n_features_beat_2x_null": n_beat_null,
           "yoy_pool": float(np.mean(yoy_pool)),
           "yoy_elite": float(np.mean(yoy_elite)),
           "membership_top": [(names[j], float(mean_auc[j]),
                               float(sign_consist[j]))
                              for j in np.argsort(-np.abs(mean_auc - 0.5))[:40]],
           "ordering_top": [(names[j], float(mean_rho[j]),
                             float(rho_consist[j]))
                            for j in np.argsort(-np.abs(np.where(
                                np.isfinite(mean_rho), mean_rho, 0)))[:40]]}
    Path(REPO_ROOT / "training" / "RESULTS_defense_elite_study.json").write_text(
        json.dumps(out, indent=1))
    print("\nwrote RESULTS_defense_elite_study.json", flush=True)


if __name__ == "__main__":
    main()
