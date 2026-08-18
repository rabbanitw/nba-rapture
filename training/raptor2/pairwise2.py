"""Player-v-player classification, rebuilt per spec: BOTH players' feature
vectors are submitted (concatenated, not differenced), the label is who has
the higher published value, and training uses ALL ordered pairs within each
training cell -- both orientations of every pair, so the label balance is
exactly 0.5 and no "higher-rated player on top" shortcut exists.

Eight targets, each trained as its own model on its structurally-matched
per-player block (plus a minutes column):

  off      rap_o                    OB + courtmate-chain + onoff2 (offense)
  def      rap_d                    DB + courtmate-chain + onoff2 (defense)
  box      rap_box_o + rap_box_d    OB + DB
  onoff    rap_onoff_o + rap_onoff_d  both chains + both onoff2 blocks
  box_o    rap_box_o                OB
  box_d    rap_box_d                DB
  onoff_o  rap_onoff_o              chain + onoff2 (offense)
  onoff_d  rap_onoff_d              chain + onoff2 (defense)

Protocol: 10-fold season-held-out (the branch standard). Per fold the model
sees every ordered non-tie pair (|dy| >= 0.05) from the 9 training cells.
Scoring on the held-out cell is a full round-robin: every ordered pair is
predicted, antisymmetrized (p(a,b) + 1 - p(b,a))/2, and a player's score is
the mean win probability against the field. Metrics: ordered-pair accuracy
(full cell and >=1065 pool), spearman rho of tournament score vs label, and
dev@10 / tau@10 / hits@10 over the >=1065 pool.

Arms per target:
  pair-concat   LightGBM binary on [x_a, x_b]           (the requested model)
  pair-diff     LightGBM binary on x_a - x_b            (the old agent's input)
  pair-mlp      torch MLP on [x_a, x_b]
  reg-ridge     RidgeCV regression on the same block, ranked
  reg-gbm       LightGBM regression on the same block, ranked

Run:  python training/raptor2/pairwise2.py [--targets off box_o ...]
"""

import argparse
import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scipy.stats import spearmanr

from db import REPO_ROOT
from experiment_combined import prepare
from experiment_oppdef import per100
from experiment_topk_rank import dev_at_k, hits_at_k, tau_at_k
from predict_seasons import DROP_FEATURES
from variables import build_variables
from variables2 import build_onoff2
from structural import cell_relative
from cv_components import ridge_pred, gbm_pred

TD = REPO_ROOT / "training"
FLOOR = 1065
TIE_EPS = 0.05
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}
PAIR_PARAMS = dict(objective="binary", learning_rate=0.05, num_leaves=31,
                   min_data_in_leaf=200, feature_fraction=0.7,
                   bagging_fraction=0.8, bagging_freq=1, lambda_l2=5.0,
                   verbose=-1, num_threads=0)
PAIR_ROUNDS = 400
ARMS = ["pair-concat", "pair-diff", "pair-mlp", "reg-ridge", "reg-gbm"]


def all_ordered_pairs(cell_rows, y, eps=TIE_EPS):
    """Every ordered non-tie pair within each cell -> (a_idx, b_idx)."""
    A, B = [], []
    for idx in cell_rows:
        n = len(idx)
        if n < 20:
            continue
        a = np.repeat(idx, n)
        b = np.tile(idx, n)
        keep = (a != b) & (np.abs(y[a] - y[b]) >= eps)
        A.append(a[keep])
        B.append(b[keep])
    return np.concatenate(A), np.concatenate(B)


def concat_pairs(F, a, b):
    P = np.empty((len(a), 2 * F.shape[1]), dtype=np.float32)
    P[:, :F.shape[1]] = F[a]
    P[:, F.shape[1]:] = F[b]
    return P


def train_pair_gbm(F, a, b, y, mode, seeds=(0,), rounds=PAIR_ROUNDS):
    L = (y[a] > y[b]).astype(np.int8)
    if mode == "concat":
        P = concat_pairs(F, a, b)
    else:
        P = (F[a] - F[b]).astype(np.float32)
    models = []
    for s in seeds:
        p = dict(PAIR_PARAMS, seed=s, bagging_seed=s, feature_fraction_seed=s)
        models.append(lgb.train(p, lgb.Dataset(P, L), num_boost_round=rounds))
    del P
    return models


def train_pair_mlp(F, a, b, y, med, seeds=(0,), epochs=6):
    import torch
    L = (y[a] > y[b]).astype(np.float32)
    A = np.where(np.isfinite(F), F, med)
    mu, sdv = A.mean(0), A.std(0)
    sdv[sdv == 0] = 1.0
    A = ((A - mu) / sdv).astype(np.float32)
    nets = []
    for s in seeds:
        torch.manual_seed(s)
        net = torch.nn.Sequential(
            torch.nn.Linear(2 * F.shape[1], 128), torch.nn.ReLU(),
            torch.nn.Dropout(0.2),
            torch.nn.Linear(128, 64), torch.nn.ReLU(),
            torch.nn.Linear(64, 1))
        opt = torch.optim.Adam(net.parameters(), lr=1e-3, weight_decay=1e-5)
        lossf = torch.nn.BCEWithLogitsLoss()
        n = len(a)
        rng = np.random.default_rng(s)
        for ep in range(epochs):
            perm = rng.permutation(n)
            for i in range(0, n, 8192):
                idx = perm[i:i + 8192]
                xb = torch.tensor(
                    np.hstack([A[a[idx]], A[b[idx]]]), dtype=torch.float32)
                yb = torch.tensor(L[idx])
                opt.zero_grad()
                loss = lossf(net(xb).squeeze(-1), yb)
                loss.backward()
                opt.step()
        nets.append(net)
    return nets, (A, mu, sdv)


def predict_pairs(models, F, a, b, mode, mlp_state=None, chunk=200_000):
    out = np.zeros(len(a))
    for lo in range(0, len(a), chunk):
        sl = slice(lo, min(lo + chunk, len(a)))
        if mode == "mlp":
            import torch
            A = mlp_state[0]
            xb = torch.tensor(np.hstack([A[a[sl]], A[b[sl]]]),
                              dtype=torch.float32)
            with torch.no_grad():
                ps = [torch.sigmoid(m(xb).squeeze(-1)).numpy()
                      for m in models]
        elif mode == "concat":
            P = concat_pairs(F, a[sl], b[sl])
            ps = [m.predict(P) for m in models]
        else:
            P = (F[a[sl]] - F[b[sl]]).astype(np.float32)
            ps = [m.predict(P) for m in models]
        out[sl] = np.mean(ps, axis=0)
    return out


def tournament(models, F, idx, mode, mlp_state=None):
    """Mean antisymmetrized win probability for every row in idx."""
    n = len(idx)
    ii, jj = np.triu_indices(n, k=1)
    a, b = idx[ii], idx[jj]
    p = predict_pairs(models, F, a, b, mode, mlp_state)
    pr = predict_pairs(models, F, b, a, mode, mlp_state)
    wprob = (p + (1 - pr)) / 2.0
    wins = np.zeros(n)
    np.add.at(wins, ii, wprob)
    np.add.at(wins, jj, 1 - wprob)
    return wins / (n - 1)


def pair_accuracy(scores_by_row, y, idx, eps=TIE_EPS, floor_mask=None):
    """Fraction of ordered non-tie pairs the score ranks correctly."""
    if floor_mask is not None:
        idx = idx[floor_mask]
    s, yy = scores_by_row, y
    ii, jj = np.triu_indices(len(idx), k=1)
    a, b = idx[ii], idx[jj]
    keep = np.abs(yy[a] - yy[b]) >= eps
    a, b = a[keep], b[keep]
    correct = ((s[a] > s[b]) == (yy[a] > yy[b])).sum()
    ties = (s[a] == s[b]).sum()
    return float((correct + 0.5 * ties) / max(len(a), 1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", nargs="*",
                    default=["off", "def", "box", "onoff",
                             "box_o", "box_d", "onoff_o", "onoff_d"])
    ap.add_argument("--arms", nargs="*", default=ARMS)
    ap.add_argument("--seasons", nargs="*", default=list(STAMPS))
    ap.add_argument("--seeds", type=int, default=1)
    ap.add_argument("--out-tag", default="")
    args = ap.parse_args()
    seeds = tuple(range(args.seeds))

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(TD / "raptor2" / "courtmate.npz")["CM"]
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    ofields = [str(f) for f in oppz["fields"]]
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OFF2, DEF2 = build_onoff2(X, feat, oppz["on_X"], oppz["off_X"], ofields,
                              cells, mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OO2o = cell_relative(OFF2, cells, mp)
    OO2d = cell_relative(DEF2, cells, mp)
    OO3o = cell_relative(cm[:, [0, 2, 4]], cells, mp)
    OO3d = cell_relative(-cm[:, [1, 3, 5]], cells, mp)
    MPC = (mp / 1000.0).reshape(-1, 1)

    y_any = d["y_off"].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y_any)
    w = np.sqrt(np.maximum(mp, 1.0))
    tuned = json.loads((TD / "tuned_params.json").read_text())

    LABELS = {
        "off": comp["rap_o"], "def": comp["rap_d"],
        "box": comp["rap_box_o"] + comp["rap_box_d"],
        "onoff": comp["rap_onoff_o"] + comp["rap_onoff_d"],
        "box_o": comp["rap_box_o"], "box_d": comp["rap_box_d"],
        "onoff_o": comp["rap_onoff_o"], "onoff_d": comp["rap_onoff_d"],
    }
    FEATS = {
        "off": [OB, OO3o, OO2o], "def": [DB, OO3d, OO2d],
        "box": [OB, DB], "onoff": [OO3o, OO2o, OO3d, OO2d],
        "box_o": [OB], "box_d": [DB],
        "onoff_o": [OO3o, OO2o], "onoff_d": [OO3d, OO2d],
    }
    SIDE = {"off": "offense", "def": "defense", "box": "offense",
            "onoff": "offense", "box_o": "offense", "box_d": "defense",
            "onoff_o": "offense", "onoff_d": "defense"}

    results = {}
    for tgt in args.targets:
        yv = LABELS[tgt].astype(np.float64)
        F = np.hstack(FEATS[tgt] + [MPC]).astype(np.float32)
        params = dict(tuned[SIDE[tgt]]["params"], verbose=-1)
        rounds = max(tuned[SIDE[tgt]]["rounds"] // 3, 150)
        print(f"\n=== {tgt} ({F.shape[1]} cols/player) ===", flush=True)
        results[tgt] = {}
        for season in args.seasons:
            stamp = STAMPS[season]
            te = labeled & (d["timestamp"] == stamp)
            trn = labeled & (d["timestamp"] != stamp)
            ti = np.where(te)[0]
            el = mp[ti] >= FLOOR
            yte = yv[ti]
            cell_rows = [np.where(trn & (d["timestamp"] == st))[0]
                         for st in STAMPS.values() if st != stamp]
            a, b = all_ordered_pairs(cell_rows, yv)
            med = np.nanmedian(F[trn], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            row = {}

            def rec(name, scores):
                acc = pair_accuracy(scores, yv, ti)
                accp = pair_accuracy(scores, yv, ti, floor_mask=el)
                row[name] = {
                    "acc": acc, "acc@pool": accp,
                    "rho": float(spearmanr(yte, scores[ti]).statistic),
                    "dev@10": dev_at_k(yte[el], scores[ti][el], 10),
                    "tau@10": tau_at_k(yte[el], scores[ti][el], 10),
                    "hits@10": hits_at_k(yte[el], scores[ti][el], 10)}

            full_scores = np.full(len(yv), np.nan)
            for arm in args.arms:
                sc = np.full(len(yv), np.nan)
                if arm == "pair-concat":
                    ms = train_pair_gbm(F, a, b, yv, "concat", seeds)
                    sc[ti] = tournament(ms, F, ti, "concat")
                elif arm == "pair-diff":
                    ms = train_pair_gbm(F, a, b, yv, "diff", seeds)
                    sc[ti] = tournament(ms, F, ti, "diff")
                elif arm == "pair-mlp":
                    ms, st = train_pair_mlp(F, a, b, yv, med, seeds)
                    sc[ti] = tournament(ms, F, ti, "mlp", mlp_state=st)
                elif arm == "reg-ridge":
                    sc[ti] = ridge_pred(F[trn], yv[trn], w[trn], F[te])
                elif arm == "reg-gbm":
                    sc[ti] = gbm_pred(F[trn], yv[trn], w[trn], F[te],
                                      params, rounds, seeds=seeds)
                rec(arm, sc)
            results[tgt][season] = {"n_pairs": int(len(a)), **row}
            print(f"  {season} ({len(a):,} train pairs): " + "  ".join(
                f"{n}: acc {row[n]['acc']:.3f} rho {row[n]['rho']:+.3f} "
                f"dev {row[n]['dev@10']:5.2f}" for n in row), flush=True)

        print(f"\n-- {tgt} summary --", flush=True)
        arms_here = [k for k in next(iter(results[tgt].values())) if
                     k != "n_pairs"]
        for arm in arms_here:
            accs = [results[tgt][s][arm]["acc"] for s in args.seasons]
            rhos = [results[tgt][s][arm]["rho"] for s in args.seasons]
            devs = [results[tgt][s][arm]["dev@10"] for s in args.seasons]
            hits = sum(results[tgt][s][arm]["hits@10"] for s in args.seasons)
            print(f"  {arm:<12} acc mean {np.mean(accs):.3f} | rho med "
                  f"{np.median(rhos):+.3f} | dev@10 med {np.median(devs):5.2f}"
                  f" mean {np.mean(devs):5.2f} | hits@10 "
                  f"{hits}/{10*len(args.seasons)}", flush=True)

    out = TD / "raptor2" / f"RESULTS_pairwise2{args.out_tag}.json"
    out.write_text(json.dumps(results, indent=1))
    print(f"\nwrote {out}", flush=True)


if __name__ == "__main__":
    main()
