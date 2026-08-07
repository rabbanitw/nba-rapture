"""The 2023-26 projection boards on the final validated stack. The closing artifact.

Models (each validated by ten-fold LOSO, RESULTS_loso.md):
  offense   components+opp -- box model -> rap_box_o, (wowy + opponent block) ->
            rap_onoff_o, minutes-aware ridge combiner. LOSO median dev@10 1.50,
            hits@10 88/100.
  defense   lgbm-matched + defend -- whole-season training rows, cell-relative and
            nearest-defender features. LOSO median dev@10 5.00, hits@10 69/100.

Confidence per position: seed-member spread + the LOSO residual pools (all ten
folds, minutes-bucketed), 2000 Monte Carlo re-rankings -- the same machinery whose
90% intervals measured 92-94% coverage against truth. Eligibility >= 1065 minutes
throughout, matching 538's own floor.

Offense boards carry a companion win%% column from the pairwise tournament model
(LOSO-equivalent to components, structurally independent): the share of eligible
opponents the player beats head-to-head.

538 is gone; nothing here can be checked against truth for these seasons. The
verification section reports what these exact fitted models score on the held-out
2013-14/2014-15 cells, and the LOSO statistics say how that generalizes.

Run:  python training/final_boards.py
"""

import gc
import json
from collections import defaultdict
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

import experiment_pairwise as EP
from coverage import as_float
from db import REPO_ROOT, get_collection
from experiment_combined import prepare, splits
from experiment_components import (COMPONENT_LABELS, RELATIVE_COLS, cell_relative,
                                   combiner_design, masks_for)
from experiment_defend import extract as extract_defend
from experiment_oppdef import engineered, per100
from experiment_topk_rank import ranks, score_cells
from predict_seasons import (DROP_FEATURES, build_unlabeled, carry_over_positions,
                             impute_positions)
from seasons import UNLABELED_SNAPSHOTS
from train_rapture import TARGETS, add_context, normalize_rates

RS_MIN, PO_MIN = 50, 10
FLOOR = 1065
SEEDS = (0, 1, 2)
MC = 2000
CI = (5, 95)
TOP_N = 30
PROJ_SEASONS = {"20240715000000": "2023-24", "20250715000000": "2024-25",
                "20260715000000": "2025-26"}

WOPP_KEYS = {"_id", "name", "standard_name", "team", "n_stints", "source",
             "timestamp", "season_type", "on_or_off"}


def extract_wowyopp_for(coll, meta, fields):
    fi = {f: i for i, f in enumerate(fields)}
    on = np.full((len(meta), len(fields)), np.nan, dtype=np.float32)
    off = np.full((len(meta), len(fields)), np.nan, dtype=np.float32)
    cache = {}
    for i, m in enumerate(meta):
        key = (m["timestamp"], m["season_type"])
        if key not in cache:
            cell = {}
            for doc in coll.find({"source": "wowy-opp", "timestamp": key[0],
                                  "season_type": key[1]}):
                cell[(doc["standard_name"], doc["on_or_off"])] = doc
            cache[key] = cell
        for side, M in (("on", on), ("off", off)):
            doc = cache[key].get((m["player"], side))
            if doc:
                for k, v in doc.items():
                    if k in fi:
                        x = as_float(v)
                        if x is not None:
                            M[i, fi[k]] = x
    return on, off


def lgbm_ridge_members(Xtr, t, Xte, med, params, rounds, seeds=SEEDS, ridge_w=0.25):
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
    coll = get_collection()
    X0, feat0, d = prepare(str(REPO_ROOT / "training" / "data_fixed"))
    raw_names = list(d["feat_names"])
    raw_keep = [i for i, n in enumerate(raw_names) if n not in DROP_FEATURES]
    raw_feat = [raw_names[i] for i in raw_keep]
    d["X"] = d["X"][:, raw_keep]
    comp = np.load(REPO_ROOT / "training" / "data_fixed" / "components.npz")
    dfz = np.load(REPO_ROOT / "training" / "data_fixed" / "defend.npz",
                  allow_pickle=True)
    oppz = np.load(REPO_ROOT / "training" / "data_fixed" / "wowyopp.npz",
                   allow_pickle=True)
    ofields = [str(f) for f in oppz["fields"]]

    print("building unlabeled 2023-26 matrix ...", flush=True)
    Xn, meta = build_unlabeled(coll, raw_feat, list(UNLABELED_SNAPSHOTS))
    carry_over_positions(coll, meta)
    impute_positions(d["X"], d["pos"], Xn, meta)
    meta_rs = np.array([m["season_type"] == "Regular season" for m in meta])

    n_lab = d["X"].shape[0]
    X_all = np.vstack([d["X"], Xn])
    is_train = np.concatenate([~d["test"].astype(bool),
                               np.zeros(Xn.shape[0], dtype=bool)])
    X_all = normalize_rates(X_all, raw_feat, is_train)
    ctx = {k: np.concatenate([d[k], np.array([m[k] for m in meta],
                                             dtype=d[k].dtype)])
           for k in ("pos", "mp", "timestamp", "season_type")}
    X_all, feat = add_context(X_all, raw_feat, ctx, "combined")
    cells_joint = np.array([f"{t}|{s}" for t, s in
                            zip(ctx["timestamp"], ctx["season_type"])])
    print(f"joint matrix {X_all.shape}", flush=True)

    # opponent + defend features for the unlabeled rows, fresh from Mongo --------
    print("extracting opponent + defend features for 2023-26 ...", flush=True)
    on_n, off_n = extract_wowyopp_for(coll, meta, ofields)
    on_j = np.vstack([oppz["on_X"], on_n])
    off_j = np.vstack([oppz["off_X"], off_n])
    Eopp_j, _ = engineered(on_j, off_j, ofields, cells_joint)
    on100, off100 = per100(on_j, ofields), per100(off_j, ofields)
    Bopp_j = np.hstack([on100, off100, on100 - off100]).astype(np.float32)
    del on_j, off_j, on100, off100
    gc.collect()

    dmeta = {"player": np.array([m["player"] for m in meta]),
             "timestamp": np.array([m["timestamp"] for m in meta]),
             "season_type": np.array([m["season_type"] for m in meta]),
             "mp": np.array([m["mp"] for m in meta])}
    E_n, _, _, _ = extract_defend(dmeta)
    E_j = np.vstack([dfz["E"], E_n])
    Z_j = cell_relative(X_all, feat, cells_joint, RELATIVE_COLS)
    print(f"defend coverage on projections: "
          f"{np.isfinite(E_n).any(axis=1).sum()}/{len(meta)}", flush=True)

    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs_lab = d["season_type"] == "Regular season"
    tr = (fit | val) & rs_lab
    test = test & rs_lab
    isfull_lab = np.array(
        [t in {"20140715000000", "20150715000000", "20160715000000",
               "20170715000000", "20180715000000", "20190715000000",
               "20201101000000", "20210801000000", "20220715000000",
               "20230715000000"} for t in d["timestamp"]])
    tr_m = tr & isfull_lab
    groups = np.array([f"{p}|{s}" for p, s in
                       zip(d["player"][tr], d["season"][tr])])
    mp_j = ctx["mp"].astype(np.float64)
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    tuned = json.loads((REPO_ROOT / "training" / "tuned_params.json").read_text())
    box_mask, onoff_mask = masks_for(feat)

    lab = slice(0, n_lab)
    new = slice(n_lab, X_all.shape[0])
    tr_j = np.zeros(X_all.shape[0], bool)
    tr_j[:n_lab] = tr
    test_j = np.zeros(X_all.shape[0], bool)
    test_j[:n_lab] = test

    # ---------------- offense: components+opp -------------------------------
    print("\n[offense] components+opp ...", flush=True)
    params_o = dict(tuned["offense"]["params"], verbose=-1)
    rounds_o = tuned["offense"]["rounds"]
    y_o = d[TARGETS["offense"]]
    Xb = X_all[:, box_mask]
    Xo = np.hstack([X_all[:, onoff_mask], Bopp_j])
    off_members_te, off_members_new = [], []
    oof = {}
    for tag, Xs, labname in (("box", Xb, COMPONENT_LABELS["offense"][0]),
                             ("onoff", Xo, COMPONENT_LABELS["offense"][1])):
        labv = comp[labname]
        ms = np.nanmedian(Xs[tr_j], axis=0)
        ms = np.where(np.isfinite(ms), ms, 0.0)
        o = np.full(int(tr.sum()), np.nan)
        Xs_tr = Xs[tr_j]
        for tri, vai in GroupKFold(n_splits=3).split(Xs_tr, labv[tr], groups=groups):
            o[vai] = lgbm_ridge_members(Xs_tr[tri], labv[tr][tri], Xs_tr[vai], ms,
                                        params_o, rounds_o, seeds=(0,))[0]
        oof[tag] = o
        oof[tag + "_te"] = lgbm_ridge_members(Xs_tr, labv[tr], Xs[test_j], ms,
                                              params_o, rounds_o)
        oof[tag + "_new"] = lgbm_ridge_members(Xs_tr, labv[tr], Xs[new], ms,
                                               params_o, rounds_o)
        print(f"  {tag} done", flush=True)
    cbn = Ridge(alpha=1.0).fit(
        combiner_design(oof["box"], oof["onoff"], mp_j[:n_lab][tr]), y_o[tr])
    for i in range(len(SEEDS)):
        off_members_te.append(cbn.predict(combiner_design(
            oof["box_te"][i], oof["onoff_te"][i], mp_j[:n_lab][test])))
        off_members_new.append(cbn.predict(combiner_design(
            oof["box_new"][i], oof["onoff_new"][i], mp_j[new])))
    print(f"  combiner w={np.round(cbn.coef_[:2], 3)}", flush=True)

    # ---------------- defense: lgbm-matched + defend ------------------------
    print("[defense] lgbm-matched + defend ...", flush=True)
    params_d = dict(tuned["defense"]["params"], verbose=-1)
    rounds_d = max(tuned["defense"]["rounds"] // 3, 150)
    y_d = d[TARGETS["defense"]]
    Xd = np.hstack([X_all, Z_j, E_j])
    trm_j = np.zeros(X_all.shape[0], bool)
    trm_j[:n_lab] = tr_m
    md = np.nanmedian(Xd[trm_j], axis=0)
    md = np.where(np.isfinite(md), md, 0.0)
    def_members_te = lgbm_ridge_members(Xd[trm_j], y_d[tr_m], Xd[test_j], md,
                                        params_d, rounds_d)
    def_members_new = lgbm_ridge_members(Xd[trm_j], y_d[tr_m], Xd[new], md,
                                         params_d, rounds_d)
    del Xd
    gc.collect()

    # ---------------- verification on held-out truth ------------------------
    verif = {}
    for name, mem, yv in (("offense", off_members_te, y_o[test]),
                          ("defense", def_members_te, y_d[test])):
        el = mp_j[:n_lab][test] >= FLOOR
        s = score_cells(yv[el], np.mean(mem, axis=0)[el], cells_te[el])
        verif[name] = s
        print(f"  verify {name}: dev@10={s['dev@10']:.2f} tau@10={s['tau@10']:+.3f} "
              f"MAE={s['mae']:.3f} hits@20={s['hits@20']}/40", flush=True)

    # ---------------- pairwise win% companion (offense) ---------------------
    print("[pairwise] tournament win% for offense boards ...", flush=True)
    EP.PAIRS_PER_CELL = 12000
    rng = np.random.default_rng(0)
    Xp = np.hstack([X_all, Z_j, Eopp_j]).astype(np.float32)
    P, L = EP.build_pairs(Xp, np.concatenate([y_o, np.full(len(meta), np.nan)]),
                          cells_joint, np.where(tr_j)[0], rng)
    pmodels = [lgb.train(dict(EP.PAIR_PARAMS, seed=s, bagging_seed=s,
                              feature_fraction_seed=s),
                         lgb.Dataset(P, L), num_boost_round=EP.PAIR_ROUNDS)
               for s in SEEDS]
    del P, L
    gc.collect()
    winpct = np.full(len(meta), np.nan)
    for stamp in PROJ_SEASONS:
        sub_meta = np.where(
            (dmeta["timestamp"] == stamp) & meta_rs
            & (dmeta["mp"] >= FLOOR))[0]
        sub_joint = sub_meta + n_lab
        w = EP.tournament_scores(pmodels, Xp, sub_joint)
        winpct[sub_meta] = w
    del Xp, pmodels
    gc.collect()

    # ---------------- confidence: LOSO residual pools -----------------------
    detail = json.loads((REPO_ROOT / "training" / "data_fixed"
                         / "loso_detail.json").read_text())
    pools = {}
    for target, ykey, memkey in (("offense", "y_off", "off_members"),
                                 ("defense", "y_def", None)):
        res_by_bucket = defaultdict(list)
        for sdet in detail.values():
            el = np.array(sdet["mp"]) >= FLOOR
            if target == "offense":
                mems = [np.array(m) for m in sdet["off_members"]]
            else:
                mems = [np.array(m) for m in sdet["def_members"]["lgbm-matched"]]
            pred = np.mean(mems, axis=0)[el]
            mpv = np.array(sdet["mp"])[el]
            edges = np.quantile(mpv, [1 / 3, 2 / 3])
            b = np.digitize(mpv, edges)
            res = np.array(sdet[ykey])[el] - pred
            for k in (0, 1, 2):
                res_by_bucket[k].extend(res[b == k].tolist())
        pools[target] = {k: np.array(v) for k, v in res_by_bucket.items()}

    boards = {}
    rng = np.random.default_rng(11)
    for target, mem_new in (("offense", off_members_new),
                            ("defense", def_members_new)):
        for stamp, season in PROJ_SEASONS.items():
            sub = np.where((dmeta["timestamp"] == stamp) & meta_rs
                           & (dmeta["mp"] >= FLOOR))[0]
            mems = [m[sub] for m in mem_new]
            mean_pred = np.mean(mems, axis=0)
            n = len(sub)
            mpv = dmeta["mp"][sub]
            edges = np.quantile(mpv, [1 / 3, 2 / 3])
            b = np.digitize(mpv, edges)
            rank_counts = np.zeros((n, n), dtype=np.int32)
            for _ in range(MC):
                base = mems[rng.integers(len(mems))]
                noise = np.empty(n)
                for k in (0, 1, 2):
                    mk = b == k
                    noise[mk] = rng.choice(pools[target][k], size=mk.sum())
                r = ranks(base + noise)
                rank_counts[np.arange(n), r] += 1
            order = np.argsort(ranks(mean_pred))
            rows = []
            for pos, j in enumerate(order[:TOP_N], 1):
                dist = rank_counts[j] / MC
                cum = np.cumsum(dist)
                lo = int(np.searchsorted(cum, CI[0] / 100)) + 1
                hi = int(np.searchsorted(cum, CI[1] / 100)) + 1
                row = {"pos": pos, "player": str(dmeta["player"][sub[j]]),
                       "est": float(mean_pred[j]), "mp": float(mpv[j]),
                       "ci": (lo, hi), "p10": float(dist[:10].sum()),
                       "p30": float(dist[:30].sum())}
                if target == "offense":
                    row["win_pct"] = float(winpct[sub[j]])
                rows.append(row)
            boards[(target, season)] = {"rows": rows, "pool": n}
            print(f"  board {target} {season}: pool {n}", flush=True)

    # ---------------- report -------------------------------------------------
    L = []
    A = L.append
    A("# Rapture 2023-26 projections — final validated stack")
    A("")
    A("**Offense** = the components architecture (538's own two-part structure) with")
    A("the opponent on/off block. **Defense** = matched-regime LightGBM with")
    A("nearest-defender features. Both selected by ten-fold leave-one-season-out")
    A("validation ([RESULTS_loso.md](RESULTS_loso.md)): offense median dev@10 1.50")
    A("(hits@10 88/100), defense median dev@10 5.00 (hits@10 69/100).")
    A("")
    A("**No truth exists for these seasons** — 538 shut down. What can be stated:")
    A("these exact fitted models score, on the held-out 2013-14/2014-15 cells:")
    A("")
    A("| target | dev@10 | tau@10 | MAE | hits@20 |")
    A("|---|---:|---:|---:|---:|")
    for t in ("offense", "defense"):
        s = verif[t]
        A(f"| {t} | {s['dev@10']:.2f} | {s['tau@10']:+.3f} | {s['mae']:.3f} | "
          f"{s['hits@20']}/40 |")
    A("")
    A("The 90% rank intervals use the LOSO-calibrated machinery (measured coverage")
    A("92–94% against truth across ten seasons). `win%` on offense boards is the")
    A("share of eligible opponents beaten head-to-head by the independent pairwise")
    A("tournament model (LOSO-equivalent quality, no shared structure).")
    A(f"Eligibility: ≥{FLOOR} regular-season minutes, 538's own floor.")
    A("")
    for (target, season) in sorted(boards, key=lambda k: (k[1], k[0])):
        v = boards[(target, season)]
        A(f"## {season} — {target}, top {TOP_N} (pool {v['pool']})")
        A("")
        head = "| pos | player | est | mp | 90% rank CI | P(top-10) | P(top-30) |"
        if target == "offense":
            head = head[:-1] + " win% |"
        A(head)
        A("|---:|---|---:|---:|---|---:|---:|" +
          ("---:|" if target == "offense" else ""))
        for r in v["rows"]:
            line = (f"| {r['pos']} | {r['player']} | {r['est']:+.2f} | "
                    f"{r['mp']:.0f} | {r['ci'][0]}–{r['ci'][1]} | "
                    f"{r['p10']:.0%} | {r['p30']:.0%} |")
            if target == "offense":
                line += f" {r['win_pct']:.1%} |"
            A(line)
        A("")
    out = REPO_ROOT / "training" / "RESULTS_final_boards.md"
    out.write_text("\n".join(L))
    json.dump({f"{t}|{s}": v for (t, s), v in boards.items()},
              open(REPO_ROOT / "training" / "RESULTS_final_boards.json", "w"),
              indent=1)
    print(f"\nwrote {out}", flush=True)


if __name__ == "__main__":
    main()
