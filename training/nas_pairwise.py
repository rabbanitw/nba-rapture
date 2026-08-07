"""Neural architecture search for the player-vs-player ranking model.

The one prior neural pairwise attempt (pairwise_gpu.py) was a single point in
architecture space -- one trunk shape, one formulation -- and it lost to the GBM
(offense dev@10 3.65 vs 1.25). This searches the space properly, and crucially it
searches across FORMULATIONS, because the failed RankNet imposes structure the
winning GBM does not have:

  family A  scorer      logit = s(a) - s(b).  Bradley-Terry: forces a total order.
  family B  diff-net    logit = f(a - b).      The GBM's formulation: can represent
                                               context-dependent comparisons.
  family C  two-tower   e = enc(x); logit = g([e(a)-e(b), e(a)*e(b)]).  Learned
                                               interaction space.

Plus depth/width/activation/norm/dropout/lr/wd/batch. Search = random sampling with
successive halving (ASHA-style): 20 configs x 6 epochs -> top 6 x +14 -> top 2 x
+20, judged on validation pair accuracy (VAL_SEASON whole cells; test cells never
touched until the single final evaluation). Pairs stream within-cell with fresh
sampling each epoch, so memory stays flat on this 7GB box.

Yardsticks the winner must beat: GBM pairwise dev@10 1.25-1.55 / tau@20 ~0.78;
the original RankNet 3.65. Offense only -- pairwise is flat on defense at every
scale tested.

Run:  python training/nas_pairwise.py
"""

import json
import time
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import engineered
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS, VAL_SEASONS

RS_MIN, PO_MIN = 50, 10
TIE_EPS = 0.05
PAIRS_PER_EPOCH = 150_000
N_CONFIGS = 20
STAGE_EPOCHS = (6, 14, 20)
KEEP = (6, 2)
DEV = "cuda" if torch.cuda.is_available() else "cpu"
rng_global = np.random.default_rng(0)


def sample_config(rng):
    width = int(rng.choice([128, 256, 512]))
    depth = int(rng.choice([1, 2, 3]))
    hidden = tuple(max(width // (2 ** i), 32) for i in range(depth)) \
        if rng.random() < 0.5 else tuple([width] * depth)
    return {
        "family": str(rng.choice(["scorer", "diffnet", "twotower"])),
        "hidden": hidden,
        "act": str(rng.choice(["relu", "gelu", "silu"])),
        "norm": str(rng.choice(["batch", "layer", "none"])),
        "dropout": float(rng.choice([0.0, 0.1, 0.2, 0.3])),
        "lr": float(10 ** rng.uniform(-3.5, -2.5)),
        "wd": float(rng.choice([0.0, 1e-5, 1e-4])),
        "batch": int(rng.choice([2048, 4096, 8192])),
        "emb": int(rng.choice([32, 64, 128])),      # two-tower embedding size
    }


ACTS = {"relu": nn.ReLU, "gelu": nn.GELU, "silu": nn.SiLU}


def mlp(n_in, hidden, act, norm, dropout, n_out=1):
    layers, prev = [], n_in
    for h in hidden:
        layers.append(nn.Linear(prev, h))
        if norm == "batch":
            layers.append(nn.BatchNorm1d(h))
        elif norm == "layer":
            layers.append(nn.LayerNorm(h))
        layers.append(ACTS[act]())
        if dropout:
            layers.append(nn.Dropout(dropout))
        prev = h
    layers.append(nn.Linear(prev, n_out))
    return nn.Sequential(*layers)


class PairModel(nn.Module):
    def __init__(self, n_in, cfg):
        super().__init__()
        self.family = cfg["family"]
        if self.family == "scorer":
            self.net = mlp(n_in, cfg["hidden"], cfg["act"], cfg["norm"],
                           cfg["dropout"])
        elif self.family == "diffnet":
            self.net = mlp(n_in, cfg["hidden"], cfg["act"], cfg["norm"],
                           cfg["dropout"])
        else:
            self.enc = mlp(n_in, cfg["hidden"], cfg["act"], cfg["norm"],
                           cfg["dropout"], n_out=cfg["emb"])
            self.head = mlp(2 * cfg["emb"], (cfg["emb"],), cfg["act"], "none",
                            0.0)

    def pair_logit(self, xa, xb):
        if self.family == "scorer":
            return self.net(xa).squeeze(-1) - self.net(xb).squeeze(-1)
        if self.family == "diffnet":
            d = xa - xb
            return (self.net(d).squeeze(-1) - self.net(-d).squeeze(-1)) / 2
        ea, eb = self.enc(xa), self.enc(xb)
        z1 = self.head(torch.cat([ea - eb, ea * eb], -1)).squeeze(-1)
        z2 = self.head(torch.cat([eb - ea, eb * ea], -1)).squeeze(-1)
        return (z1 - z2) / 2


def epoch_pairs(fit_cells, y, rng, total=PAIRS_PER_EPOCH):
    per = max(total // len(fit_cells), 200)
    a_all, b_all = [], []
    for idx in fit_cells:
        n = len(idx)
        k = min(per, n * (n - 1))
        a = rng.integers(0, n, size=k)
        b = rng.integers(0, n, size=k)
        m = a != b
        a, b = idx[a[m]], idx[b[m]]
        m = np.abs(y[a] - y[b]) >= TIE_EPS
        a_all.append(a[m])
        b_all.append(b[m])
    a = np.concatenate(a_all)
    b = np.concatenate(b_all)
    perm = rng.permutation(len(a))
    return a[perm], b[perm]


def val_pair_acc(model, At, y, val_cells):
    model.eval()
    accs = []
    with torch.no_grad():
        for idx in val_cells:
            n = len(idx)
            ii, jj = np.triu_indices(n, k=1)
            m = np.abs(y[idx[ii]] - y[idx[jj]]) >= TIE_EPS
            ii, jj = ii[m], jj[m]
            logits = []
            for k in range(0, len(ii), 8192):
                s = model.pair_logit(At[idx[ii[k:k + 8192]]],
                                     At[idx[jj[k:k + 8192]]])
                logits.append(s.cpu().numpy())
            lg = np.concatenate(logits)
            accs.append(np.mean((lg > 0) == (y[idx[ii]] > y[idx[jj]])))
    return float(np.mean(accs))


def train_stage(model, opt, At, y, fit_cells, rng, epochs, cfg):
    bce = nn.BCEWithLogitsLoss()
    for _ in range(epochs):
        model.train()
        a, b = epoch_pairs(fit_cells, y, rng)
        for k in range(0, len(a), cfg["batch"]):
            xa = At[a[k:k + cfg["batch"]]]
            xb = At[b[k:k + cfg["batch"]]]
            if len(xa) < 8:
                continue
            lab = torch.tensor((y[a[k:k + cfg["batch"]]]
                                > y[b[k:k + cfg["batch"]]]).astype(np.float32),
                               device=DEV)
            loss = bce(model.pair_logit(xa, xb), lab)
            opt.zero_grad()
            loss.backward()
            opt.step()


def main():
    X, feat, d = prepare(str(REPO_ROOT / "training" / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    oppz = np.load(REPO_ROOT / "training" / "data_fixed" / "wowyopp.npz",
                   allow_pickle=True)
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Eopp, _ = engineered(oppz["on_X"], oppz["off_X"],
                         [str(f) for f in oppz["fields"]], cells_all)
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    Xf = np.hstack([X, Z, Eopp]).astype(np.float32)
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    y = d[TARGETS["offense"]].astype(np.float64)
    seasons = np.array([str(s) for s in d["season"]])
    stamps = {"2015-16": "20160715000000", "2016-17": "20170715000000",
              "2017-18": "20180715000000", "2019-20": "20201101000000"}
    is_valcell = tr & np.isin(seasons, VAL_SEASONS) & np.isin(
        d["timestamp"], list(stamps.values()))
    tr_fit = tr & ~is_valcell
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])

    med = np.nanmedian(Xf[tr_fit], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Xf), Xf, med)
    mu, sd = A[tr_fit].mean(0), A[tr_fit].std(0)
    sd[sd == 0] = 1.0
    A = ((A - mu) / sd).astype(np.float32)
    At = torch.tensor(A, device=DEV)
    fit_cells = [np.where(tr_fit & (cells_all == c))[0]
                 for c in np.unique(cells_all[tr_fit])]
    fit_cells = [c for c in fit_cells if len(c) >= 20]
    val_cells = [np.where(is_valcell & (cells_all == c))[0]
                 for c in np.unique(cells_all[is_valcell])]
    print(f"device={DEV} train={tr_fit.sum()} valcells={len(val_cells)} "
          f"test={test.sum()} feat={A.shape[1]}", flush=True)

    # ---- stage 1: broad random sample --------------------------------------
    trials = []
    for i in range(N_CONFIGS):
        cfg = sample_config(rng_global)
        torch.manual_seed(i)
        rng = np.random.default_rng(i)
        model = PairModel(A.shape[1], cfg).to(DEV)
        opt = torch.optim.AdamW(model.parameters(), lr=cfg["lr"],
                                weight_decay=cfg["wd"])
        t0 = time.time()
        train_stage(model, opt, At, y, fit_cells, rng, STAGE_EPOCHS[0], cfg)
        acc = val_pair_acc(model, At, y, val_cells)
        trials.append({"i": i, "cfg": cfg, "acc": acc, "model": model,
                       "opt": opt, "rng": rng})
        print(f"[s1 {i:>2}] {cfg['family']:<9} h={cfg['hidden']} "
              f"{cfg['act']}/{cfg['norm']} do={cfg['dropout']} "
              f"lr={cfg['lr']:.4f} -> val acc {acc:.4f} "
              f"({time.time()-t0:.0f}s)", flush=True)

    # ---- stage 2 + 3: successive halving -----------------------------------
    for stage, (keep_n, ep) in enumerate(zip(KEEP, STAGE_EPOCHS[1:]), start=2):
        trials.sort(key=lambda t: -t["acc"])
        trials = trials[:keep_n]
        print(f"\n-- stage {stage}: top {keep_n} continue ({ep} more epochs) --",
              flush=True)
        for t in trials:
            train_stage(t["model"], t["opt"], At, y, fit_cells, t["rng"], ep,
                        t["cfg"])
            t["acc"] = val_pair_acc(t["model"], At, y, val_cells)
            print(f"[s{stage} {t['i']:>2}] {t['cfg']['family']:<9} -> "
                  f"val acc {t['acc']:.4f}", flush=True)

    # ---- final evaluation of the winner on the test cells ------------------
    trials.sort(key=lambda t: -t["acc"])
    best = trials[0]
    print(f"\nwinner: config {best['i']} {json.dumps(best['cfg'])} "
          f"val acc {best['acc']:.4f}", flush=True)
    model = best["model"]
    model.eval()
    te_idx = np.where(test)[0]
    p = np.empty(len(te_idx))
    pos_of = {j: k for k, j in enumerate(te_idx)}
    with torch.no_grad():
        for c in np.unique(cells_te):
            sub = te_idx[cells_te == c]
            n = len(sub)
            ii, jj = np.triu_indices(n, k=1)
            wins = np.zeros(n)
            for k in range(0, len(ii), 8192):
                lg = model.pair_logit(At[sub[ii[k:k + 8192]]],
                                      At[sub[jj[k:k + 8192]]]).cpu().numpy()
                w = 1 / (1 + np.exp(-lg))
                np.add.at(wins, ii[k:k + 8192], w)
                np.add.at(wins, jj[k:k + 8192], 1 - w)
            for j, wv in zip(sub, wins / (n - 1)):
                p[pos_of[j]] = wv
    s = score_cells(y[test], p, cells_te)
    print(f"\nTEST  dev@10={s['dev@10']:.2f} dev@20={s['dev@20']:.2f} "
          f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
          f"hits@20={s['hits@20']}/40", flush=True)
    print("yardsticks: gbm-pairwise 1.25-1.55 / tau@20 ~0.78; "
          "original ranknet 3.65", flush=True)
    out = {"winner_cfg": best["cfg"], "val_acc": best["acc"],
           "test": {k: v for k, v in s.items()},
           "leaderboard": [{"i": t["i"], "cfg": t["cfg"], "acc": t["acc"]}
                           for t in trials]}
    Path(REPO_ROOT / "training" / "RESULTS_nas_pairwise.json").write_text(
        json.dumps(out, indent=1, default=str))
    print("wrote RESULTS_nas_pairwise.json", flush=True)


if __name__ == "__main__":
    main()
