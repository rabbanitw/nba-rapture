"""2023-26 projection boards on the promoted gbm+hats3 stack (both sides).

Assembly mirrors final_boards.py (joint labeled+unlabeled matrix, fresh Mongo
extraction of opponent/defend features for the projection rows) and adds the
rebuild's blocks for the unlabeled rows: shot-dashboard features (Mongo),
structural variables, courtmate-chain ratings (courtmate_chain.json), and the
four component hats.

Models: full matrix + hats, LightGBM x3 seeds + ridge (tuned params,
rounds//3 -- the protocol the promotion CVs validated). Verification on the
held-out 2013-14/2014-15 truth before any board is written. Confidence: 90%
rank intervals and P(top-10/30) from 2,000 Monte-Carlo re-rankings using the
leave-fold-out residual pools (minutes terciles) of the same stack.

Run:  python training/raptor2/final_boards2.py
"""

import gc
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import REPO_ROOT, get_collection
from coverage import as_float
from estimated_raptor import norm_name
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_defend import extract as extract_defend
from experiment_oppdef import blend, engineered
from experiment_topk_rank import ranks, score_cells
from final_boards import extract_wowyopp_for
from predict_seasons import (DROP_FEATURES, build_unlabeled,
                             carry_over_positions, impute_positions)
from seasons import UNLABELED_SNAPSHOTS
from train_rapture import TARGETS, add_context, normalize_rates
from structural import cell_relative as cellrel_struct
from variables import build_variables
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
FLOOR = 1065
MC = 2000
SEASON_OF_STAMP = {"20240715000000": "2023-24", "20250715000000": "2024-25",
                   "20260715000000": "2025-26"}
SD_TABLES = ("shots-def0-2", "shots-def2-4", "shots-def4-6", "shots-def6plus",
             "possessions")
SD_META = {"_id", "PLAYER", "name", "standard_name", "nba_player_id", "source",
           "data_type", "timestamp", "season_type"}


def extract_shotdash_for(coll, meta, rnames):
    """Shot-dashboard raw block for the unlabeled rows, matching shotdash.npz
    column order (sd:<table>|<col>)."""
    slot = {nm: j for j, nm in enumerate(rnames)}
    R = np.full((len(meta), len(rnames)), np.nan)
    docs = {}
    for doc in coll.find({"source": "nba-shotdash",
                          "timestamp": {"$in": list(UNLABELED_SNAPSHOTS)}}):
        key = (str(doc["timestamp"]), str(doc["season_type"]),
               str(doc["standard_name"]))
        docs.setdefault(key, {})[doc["data_type"]] = doc
    hit = 0
    for i, m in enumerate(meta):
        entry = docs.get((str(m["timestamp"]), str(m["season_type"]),
                          str(m["player"])))
        if not entry:
            continue
        hit += 1
        for t, doc in entry.items():
            for c, v in doc.items():
                if c in SD_META:
                    continue
                j = slot.get(f"sd:{t}|{c}")
                fv = as_float(v)
                if j is not None and fv is not None:
                    R[i, j] = fv
    print(f"  shotdash on projections: {hit}/{len(meta)}", flush=True)
    return R


def main():
    coll = get_collection()
    X0, feat0, d = prepare(str(TD / "data_fixed"))
    raw_names = list(d["feat_names"])
    raw_keep = [i for i, n in enumerate(raw_names) if n not in DROP_FEATURES]
    raw_feat = [raw_names[i] for i in raw_keep]
    d["X"] = d["X"][:, raw_keep]
    comp = np.load(TD / "data_fixed" / "components.npz")
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    sdz = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    cmz = np.load(TD / "raptor2" / "courtmate.npz")
    chain = json.loads((TD / "raptor2" / "courtmate_chain.json").read_text())
    ofields = [str(f) for f in oppz["fields"]]
    rnames = [str(x) for x in sdz["rnames"]]

    print("building unlabeled matrix ...", flush=True)
    Xn, meta = build_unlabeled(coll, raw_feat, list(UNLABELED_SNAPSHOTS))
    carry_over_positions(coll, meta)
    impute_positions(d["X"], d["pos"], Xn, meta)

    n_lab = d["X"].shape[0]
    X_all = np.vstack([d["X"], Xn])
    is_train = np.concatenate([~d["test"].astype(bool),
                               np.zeros(Xn.shape[0], dtype=bool)])
    X_all = normalize_rates(X_all, raw_feat, is_train)
    ctx = {k: np.concatenate([d[k], np.array([m[k] for m in meta],
                                             dtype=d[k].dtype)])
           for k in ("pos", "mp", "timestamp", "season_type")}
    X_all, feat = add_context(X_all, raw_feat, ctx, "combined")
    cells_j = np.array([f"{t}|{s}" for t, s in
                        zip(ctx["timestamp"], ctx["season_type"])])
    mp_j = ctx["mp"].astype(np.float64)
    print(f"joint matrix {X_all.shape}", flush=True)

    print("extracting opponent/defend/shotdash for projections ...",
          flush=True)
    on_n, off_n = extract_wowyopp_for(coll, meta, ofields)
    on_j = np.vstack([oppz["on_X"], on_n])
    off_j = np.vstack([oppz["off_X"], off_n])
    Eopp_j, _ = engineered(on_j, off_j, ofields, cells_j)
    del on_j, off_j
    gc.collect()
    dmeta = {"player": np.array([m["player"] for m in meta]),
             "timestamp": np.array([m["timestamp"] for m in meta]),
             "season_type": np.array([m["season_type"] for m in meta]),
             "mp": np.array([m["mp"] for m in meta])}
    E_n, _, _, _ = extract_defend(dmeta)
    E_j = np.vstack([dfz["E"], E_n])
    R_n = extract_shotdash_for(coll, meta, rnames)
    R_j = np.vstack([sdz["R"], R_n])
    Z_j = cell_relative(X_all, feat, cells_j, RELATIVE_COLS)

    print("structural variables + courtmate chain (joint) ...", flush=True)
    V = build_variables(X_all, feat, R_j, rnames, E_j,
                        [str(x) for x in dfz["enames"]], mp_j)
    CM_j = np.full((X_all.shape[0], 7), np.nan)
    CM_j[:n_lab] = cmz["CM"]
    for k, m in enumerate(meta):
        season = SEASON_OF_STAMP.get(str(m["timestamp"]))
        if season is None:
            continue
        row = chain.get(f"{season}|{norm_name(str(m['player']))}")
        if row is not None:
            CM_j[n_lab + k] = [np.nan if v is None else v for v in row]
    print(f"  courtmate on projections: "
          f"{int(np.isfinite(CM_j[n_lab:, 2]).sum())}/{len(meta)}", flush=True)
    OB = cellrel_struct(V["OB"], cells_j, mp_j)
    DB = cellrel_struct(V["DB"], cells_j, mp_j)
    OO3o = cellrel_struct(CM_j[:, [0, 2, 4]], cells_j, mp_j)
    OO3d = cellrel_struct(-CM_j[:, [1, 3, 5]], cells_j, mp_j)

    fit, val, test = splits(d, 50, 10)
    rs_lab = d["season_type"] == "Regular season"
    tr = (fit | val) & rs_lab
    test = test & rs_lab
    tr_j = np.zeros(X_all.shape[0], bool)
    tr_j[:n_lab] = tr
    test_j = np.zeros(X_all.shape[0], bool)
    test_j[:n_lab] = test
    w = np.sqrt(np.maximum(mp_j, 1.0))
    tuned = json.loads((TD / "tuned_params.json").read_text())

    hats = {}
    for tag, (M, labname) in (("box_o", (OB, "rap_box_o")),
                              ("onoff_o", (OO3o, "rap_onoff_o")),
                              ("box_d", (DB, "rap_box_d")),
                              ("onoff_d", (OO3d, "rap_onoff_d"))):
        yv = np.concatenate([comp[labname], np.full(len(meta), np.nan)])
        m = tr_j & np.isfinite(yv) & np.isfinite(M).all(axis=1)
        hats[tag] = ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1], tag,
                              quiet=True)
        hats[tag][~np.isfinite(M).all(axis=1)] = np.nan
    H = np.column_stack([hats[t] for t in
                         ("box_o", "onoff_o", "box_d", "onoff_d")])

    FEATS = {"offense": np.hstack([X_all, Z_j, Eopp_j, H]),
             "defense": np.hstack([X_all, Z_j, E_j, H])}
    del X_all, Z_j, Eopp_j
    gc.collect()

    pools = np.load(TD / "raptor2" / "hats3_resid_pools.npz",
                    allow_pickle=True)
    rng = np.random.default_rng(0)
    cells_te = np.array([f"{s}" for s in d["season"][test]])
    el_te = mp_j[:n_lab][test] >= FLOOR
    players_j = np.concatenate([np.array([str(p) for p in d["player"]]),
                                dmeta["player"]])
    boards, verif = {}, {}
    for side in ("offense", "defense"):
        y = d[TARGETS[side]].astype(np.float64)
        params = dict(tuned[side]["params"], verbose=-1)
        rounds = max(tuned[side]["rounds"] // 3, 150)
        Xf = FEATS[side]
        med = np.nanmedian(Xf[tr_j], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p_te = blend(Xf[tr_j], y[tr], Xf[test_j], med, params, rounds)
        s = score_cells(y[test][el_te], p_te[el_te], cells_te[el_te])
        verif[side] = {k: (int(v) if isinstance(v, (int, np.integer))
                           else round(float(v), 4)) for k, v in s.items()}
        print(f"[{side}] verify: dev@10={s['dev@10']:.2f} "
              f"tau@10={s['tau@10']:+.3f} MAE={s['mae']:.3f} "
              f"hits@20={s['hits@20']}/40", flush=True)
        p_new = blend(Xf[tr_j], y[tr], Xf[n_lab:], med, params, rounds)

        P = pools[side]
        terc = np.quantile(P[:, 1], [1 / 3, 2 / 3])
        buckets = [P[P[:, 1] <= terc[0], 0],
                   P[(P[:, 1] > terc[0]) & (P[:, 1] <= terc[1]), 0],
                   P[P[:, 1] > terc[1], 0]]
        for stamp, season in SEASON_OF_STAMP.items():
            m_cell = (dmeta["timestamp"] == stamp) & \
                     (dmeta["season_type"] == "Regular season") & \
                     (dmeta["mp"].astype(float) >= FLOOR)
            idx = np.where(m_cell)[0]
            if not len(idx):
                continue
            est = p_new[idx]
            mps = dmeta["mp"][idx].astype(float)
            b_of = np.digitize(mps, terc)
            order0 = np.argsort(-est)
            # rk[j, t] = rank of player j in draw t (0-based)
            rk = np.zeros((len(idx), MC), dtype=np.int32)
            for t in range(MC):
                draw = est + np.array(
                    [rng.choice(buckets[b]) for b in b_of])
                rk[np.argsort(-draw), t] = np.arange(len(idx))
            rows = []
            for pos_i, j in enumerate(order0[:30]):
                lo, hi = np.percentile(rk[j], [5, 95]).astype(int) + 1
                rows.append({
                    "pos": pos_i + 1,
                    "player": str(dmeta["player"][idx[j]]),
                    "est": round(float(est[j]), 2),
                    "mp": int(mps[j]),
                    "ci": f"{lo}-{hi}",
                    "p_top10": round(float((rk[j] < 10).mean()), 2),
                    "p_top30": round(float((rk[j] < 30).mean()), 2)})
            boards[f"{side}|{season}"] = rows
        del Xf
        gc.collect()

    LN = ["# 2023-26 projection boards -- gbm+hats3 stack (rebuild branch)",
          "",
          "Verification on held-out 2013-14/2014-15 truth (>=1065 pools):",
          ""]
    for side, s in verif.items():
        LN.append(f"- **{side}**: dev@10 {s['dev@10']}, tau@10 {s['tau@10']}, "
                  f"MAE {s['mae']}, hits@20 {s['hits@20']}/40")
    LN.append("")
    for key, rows in boards.items():
        side, season = key.split("|")
        LN += [f"## {season} — {side} (top 30, >=1065 min)", "",
               "| # | player | est | mp | 90% rank CI | P(top-10) | P(top-30) |",
               "|---:|---|---:|---:|---|---:|---:|"]
        LN += [f"| {r['pos']} | {r['player']} | {r['est']:+.2f} | {r['mp']} | "
               f"{r['ci']} | {r['p_top10']:.2f} | {r['p_top30']:.2f} |"
               for r in rows]
        LN.append("")
    (TD / "raptor2" / "RESULTS_final_boards2.md").write_text("\n".join(LN))
    (TD / "raptor2" / "RESULTS_final_boards2.json").write_text(
        json.dumps({"verification": verif, "boards": boards}, indent=1))
    print("wrote raptor2/RESULTS_final_boards2.md/.json", flush=True)


if __name__ == "__main__":
    main()
