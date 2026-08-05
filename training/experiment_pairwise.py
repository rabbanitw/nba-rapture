"""Pairwise ranking: input = a pair of players, output = who has the higher RAPTOR.

The motivation, per the diagnosis in RESULTS_topk_rank.md: adjacent ranks in the
top 30 are separated by 0.04-0.12 RAPTOR while regression error is 0.5-0.9, so a
regression must resolve magnitudes far below its own noise floor. A pairwise binary
model never sees magnitudes -- every ordered pair contributes the same
cross-entropy whether the gap is 0.1 or 8.0 -- and pairwise accuracy IS Kendall
tau, the metric the leaderboard is judged on. It is also genuinely distinct from
the two rank-family failures: lambdarank was listwise over coarse grades, and
cell_pct was still magnitude regression on a squashed label.

Construction:
  pairs        within-cell only (same league context, same label table), training
               cells, up to PAIRS_PER_CELL random ordered pairs each; exact ties
               (|dy| < 0.05, i.e. 538's rounding) dropped; feature vector is
               x_a - x_b, label = [y_a > y_b]
  model        LightGBM binary on the differences, seed-averaged
  tournament   full round-robin per cell: every eligible pair "plays", win
               probability antisymmetrized as (p(a,b) + 1 - p(b,a)) / 2; a
               player's score is their mean win probability across the field
               (a league table, which uses every game -- single elimination
               would discard most of the information)

Scored on the full test cells for comparability with every prior number, and
additionally as a >=1000-minute tournament, per the leaderboard framing.

Arms per target: direct regression on the same features (baseline), pairwise
tournament, and their rank average.

Run:  python training/experiment_pairwise.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend, engineered, per100
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
PAIRS_PER_CELL = 6000
TIE_EPS = 0.05
MIN_CUTOFF = 1000
PAIR_PARAMS = dict(objective="binary", learning_rate=0.05, num_leaves=31,
                   min_data_in_leaf=100, feature_fraction=0.5,
                   bagging_fraction=0.8, bagging_freq=1, lambda_l2=5.0,
                   verbose=-1, num_threads=0)
PAIR_ROUNDS = 500
SEEDS = (0, 1, 2)


def build_pairs(Xf, y, cells, tr_idx, rng):
    """-> (diff matrix, labels). Ordered pairs sampled within each training cell."""
    # two passes: count, then fill a preallocated matrix -- a vstack of chunks
    # briefly holds two copies and OOM-killed the first run alongside the LOSO job
    pair_ab = []
    for c in np.unique(cells[tr_idx]):
        idx = tr_idx[cells[tr_idx] == c]
        n = len(idx)
        if n < 20:
            continue
        k = min(PAIRS_PER_CELL, n * (n - 1))
        a = rng.integers(0, n, size=k)
        b = rng.integers(0, n, size=k)
        keep = a != b
        a, b = idx[a[keep]], idx[b[keep]]
        dy = y[a] - y[b]
        keep = np.abs(dy) >= TIE_EPS
        pair_ab.append((a[keep], b[keep]))
    total = sum(len(a) for a, _ in pair_ab)
    P = np.empty((total, Xf.shape[1]), dtype=np.float32)
    L = np.empty(total, dtype=np.int8)
    pos = 0
    for a, b in pair_ab:
        P[pos:pos + len(a)] = Xf[a] - Xf[b]
        L[pos:pos + len(a)] = (y[a] - y[b] > 0)
        pos += len(a)
    return P, L


def tournament_scores(models, Xf, idx):
    """Mean antisymmetrized win probability for each player in idx (round-robin)."""
    n = len(idx)
    A = Xf[idx]
    ii, jj = np.triu_indices(n, k=1)
    D = (A[ii] - A[jj]).astype(np.float32)
    p = np.mean([m.predict(D) for m in models], axis=0)
    pr = np.mean([m.predict(-D) for m in models], axis=0)
    w = (p + (1 - pr)) / 2.0        # P(i beats j), antisymmetrized
    wins = np.zeros(n)
    np.add.at(wins, ii, w)
    np.add.at(wins, jj, 1 - w)
    return wins / (n - 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_pairwise.md"))
    ap.add_argument("--pairs-per-cell", type=int, default=PAIRS_PER_CELL,
                    help="6k fits in 7GB; 12k was measurably better; a 32-64GB "
                         "machine can try 48000+ (memory ~ pairs x 1170 x 4B x ~2)")
    ap.add_argument("--save-preds", action="store_true",
                    help="write row-aligned tournament scores for ALL regular-season "
                         "rows to data_fixed/pairwise_gbm_preds.npz for integration")
    args = ap.parse_args()
    global PAIRS_PER_CELL
    PAIRS_PER_CELL = args.pairs_per_cell
    rng = np.random.default_rng(0)

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    dfz = np.load(Path(args.datadir) / "defend.npz", allow_pickle=True)
    E = dfz["E"]
    opp = np.load(Path(args.datadir) / "wowyopp.npz", allow_pickle=True)
    Eopp, _ = engineered(opp["on_X"], opp["off_X"],
                         [str(f) for f in opp["fields"]],
                         np.array([f"{t}|{s}" for t, s in
                                   zip(d["timestamp"], d["season_type"])]))
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    mp = d["mp"].astype(np.float64)
    tuned = json.loads(Path(args.tuned).read_text()) if hasattr(args, "tuned") \
        else json.loads((REPO_ROOT / "training" / "tuned_params.json").read_text())

    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, E])}

    rows = []

    def record(target, name, p_test_full):
        y = d[TARGETS[target]]
        s = score_cells(y[test], p_test_full, cells_te)
        rows.append({"target": target, "arm": name, **s})
        print(f"  {name:<16} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40", flush=True)

    boards = {}
    for target in ("offense", "defense"):
        y = d[TARGETS[target]]
        Xf = FEATS[target]
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = tuned[target]["rounds"]
        print(f"\n=== {target} ===", flush=True)

        tr_idx = np.where(tr)[0]
        P, L = build_pairs(Xf, y, cells_all, tr_idx, rng)
        print(f"  pairs: {P.shape[0]:,} x {P.shape[1]} "
              f"(label balance {L.mean():.3f})", flush=True)
        models = [lgb.train(dict(PAIR_PARAMS, seed=s, bagging_seed=s,
                                 feature_fraction_seed=s),
                            lgb.Dataset(P, L), num_boost_round=PAIR_ROUNDS)
                  for s in SEEDS]
        acc = np.mean([(m.predict(P) > 0.5) == L for m in models])
        print(f"  train pairwise accuracy {acc:.3f}", flush=True)

        # direct regression baseline on identical features
        med = np.nanmedian(Xf[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        p_direct = blend(Xf[tr], y[tr], Xf[test], med, params, rounds)
        record(target, "direct", p_direct)

        # pairwise tournament over each full test cell
        p_pair = np.empty(int(test.sum()))
        te_idx = np.where(test)[0]
        pos_of = {j: k for k, j in enumerate(te_idx)}
        for c in np.unique(cells_te):
            sub = te_idx[cells_te == c]
            w = tournament_scores(models, Xf, sub)
            for j, wv in zip(sub, w):
                p_pair[pos_of[j]] = wv
        record(target, "pairwise", p_pair)

        del P, L
        import gc
        gc.collect()
        rb = np.empty(len(p_pair))
        for c in np.unique(cells_te):
            m = cells_te == c
            rb[m] = -(ranks(p_direct[m]) + ranks(p_pair[m])) / 2.0
        record(target, "rank-avg(d,p)", rb)

        if args.save_preds:
            # row-aligned tournament scores for every RS cell (train + test), so
            # the container-side pipeline can blend and LOSO-validate this model
            allsc = np.full(Xf.shape[0], np.nan)
            for c in np.unique(cells_all[(tr | test)]):
                sub = np.where((tr | test) & (cells_all == c))[0]
                if len(sub) >= 20:
                    allsc[sub] = tournament_scores(models, Xf, sub)
            outp = Path(args.datadir) / f"pairwise_gbm_{target}.npy"
            np.save(outp, allsc)
            print(f"  saved {outp}", flush=True)

        # tournament board with the minutes cutoff, per test season
        for c in np.unique(cells_te):
            sub = te_idx[(cells_te == c) & (mp[test] >= MIN_CUTOFF)]
            w = tournament_scores(models, Xf, sub)
            order = np.argsort(-w)
            tr_rank = ranks(y[sub])
            boards[(target, c)] = [
                {"pos": k + 1, "player": str(d["player"][sub[o]]),
                 "win_pct": float(w[o]), "true_rank_in_pool":
                     int(tr_rank[o]) + 1}
                for k, o in enumerate(order[:10])]

    Path(args.out).with_suffix(".json").write_text(json.dumps(
        {"metrics": rows,
         "boards": {f"{t}|{c}": b for (t, c), b in boards.items()}}, indent=1))
    LN = ["# Pairwise ranking (who is better?) vs regression", ""]
    for target in ("offense", "defense"):
        LN += [f"## {target}", "",
               "| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |",
               "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for r in [x for x in rows if x["target"] == target]:
            LN.append(f"| {r['arm']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
                      f"{r['tau@10']:+.3f} | {r['tau@20']:+.3f} | {r['mae']:.3f} | "
                      f"{r['hits@10']}/20 | {r['hits@20']}/40 |")
        LN.append("")
    for (t, c), b in boards.items():
        LN += [f"## {c} — {t} tournament (>= {MIN_CUTOFF} min), top 10", "",
               "| pos | player | win % | true rank in pool |", "|---:|---|---:|---:|"]
        LN += [f"| {r['pos']} | {r['player']} | {r['win_pct']:.1%} | "
               f"{r['true_rank_in_pool']} |" for r in b]
        LN.append("")
    Path(args.out).write_text("\n".join(LN))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
