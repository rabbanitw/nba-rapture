"""Pairwise (player-vs-player) NAS, round 2.

Round 1 (nas_pairwise.py) landed on dev@10 3.60 -- identical to the hand-built
RankNet -- with every config in a 0.869-0.883 val-accuracy band. Two things it
never tried: residual trunks / lr schedules (capacity+optimization upgrades) and
ENSEMBLING -- the GBM yardstick is a 3-seed average, while round 1 fielded a
single net. Round 2 searches the widened space and evaluates both the solo
winner and a top-3 ensemble (mean antisymmetrized win probability across the
three best architectures found).

Same discipline as round 1: pairs are sampled within-cell (ties |dy|<0.05
dropped), selection on VAL_SEASON whole-cell pair accuracy, the 2013-14/2014-15
test cells scored exactly once at the end. Saves row-aligned tournament scores
for every whole-season RS cell (train cells in-sample, test out-of-sample) for
the write-up's leaderboards.

Run:  python training/nas_pairwise2.py
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
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS, VAL_SEASONS

RS_MIN, PO_MIN = 50, 10
TIE_EPS = 0.05
PAIRS_PER_EPOCH = 200_000
N_CONFIGS = 20
STAGE_EPOCHS = (8, 16, 24)
KEEP = (6, 3)
DEV = "cuda" if torch.cuda.is_available() else "cpu"
ACTS = {"relu": nn.ReLU, "gelu": nn.GELU, "silu": nn.SiLU}
torch.set_num_threads(6)


def sample_config(rng):
    width = int(rng.choice([128, 256, 512]))
    depth = int(rng.choice([1, 2, 3]))
    return {"family": str(rng.choice(["scorer", "diffnet", "twotower"])),
            "res": bool(rng.random() < 0.4),
            "width": width, "depth": depth,
            "act": str(rng.choice(["relu", "gelu", "silu"])),
            "norm": str(rng.choice(["batch", "layer", "none"])),
            "dropout": float(rng.choice([0.0, 0.1, 0.2, 0.3])),
            "lr": float(10 ** rng.uniform(-3.5, -2.5)),
            "wd": float(rng.choice([0.0, 1e-5, 1e-4])),
            "batch": int(rng.choice([2048, 4096, 8192])),
            "sched": str(rng.choice(["none", "cosine"])),
            "emb": int(rng.choice([32, 64, 128]))}


def norm_layer(kind, h):
    return nn.BatchNorm1d(h) if kind == "batch" else \
        nn.LayerNorm(h) if kind == "layer" else nn.Identity()


class ResBlock(nn.Module):
    def __init__(self, h, act, norm, drop):
        super().__init__()
        self.f = nn.Sequential(nn.Linear(h, h), norm_layer(norm, h),
                               ACTS[act](), nn.Dropout(drop), nn.Linear(h, h))

    def forward(self, x):
        return x + self.f(x)


def trunk(n_in, cfg, n_out=1):
    w, act, nm, dr = cfg["width"], cfg["act"], cfg["norm"], cfg["dropout"]
    if cfg["res"]:
        layers = [nn.Linear(n_in, w), norm_layer(nm, w), ACTS[act]()]
        layers += [ResBlock(w, act, nm, dr) for _ in range(cfg["depth"])]
        layers.append(nn.Linear(w, n_out))
        return nn.Sequential(*layers)
    layers, prev = [], n_in
    for _ in range(cfg["depth"]):
        layers += [nn.Linear(prev, w), norm_layer(nm, w), ACTS[act](),
                   nn.Dropout(dr)]
        prev = w
    layers.append(nn.Linear(prev, n_out))
    return nn.Sequential(*layers)


class PairModel(nn.Module):
    def __init__(self, n_in, cfg):
        super().__init__()
        self.family = cfg["family"]
        if self.family == "twotower":
            self.enc = trunk(n_in, cfg, n_out=cfg["emb"])
            h = cfg["emb"]
            self.head = nn.Sequential(nn.Linear(2 * h, h), ACTS[cfg["act"]](),
                                      nn.Linear(h, 1))
        else:
            self.net = trunk(n_in, cfg)

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
        a = rng.integers(0, n, size=min(per, n * (n - 1)))
        b = rng.integers(0, n, size=len(a))
        m = a != b
        a, b = idx[a[m]], idx[b[m]]
        m = np.abs(y[a] - y[b]) >= TIE_EPS
        a_all.append(a[m])
        b_all.append(b[m])
    a, b = np.concatenate(a_all), np.concatenate(b_all)
    perm = rng.permutation(len(a))
    return a[perm], b[perm]


def train_stage(t, At, y, fit_cells, epochs):
    bce = nn.BCEWithLogitsLoss()
    model, opt, cfg, rng = t["model"], t["opt"], t["cfg"], t["rng"]
    for _ in range(epochs):
        model.train()
        a, b = epoch_pairs(fit_cells, y, rng)
        for k in range(0, len(a), cfg["batch"]):
            sa, sb = a[k:k + cfg["batch"]], b[k:k + cfg["batch"]]
            if len(sa) < 8:
                continue
            lab = torch.tensor((y[sa] > y[sb]).astype(np.float32), device=DEV)
            loss = bce(model.pair_logit(At[sa], At[sb]), lab)
            opt.zero_grad()
            loss.backward()
            opt.step()
        if t["sched"] is not None:
            t["sched"].step()


def val_pair_acc(model, At, y, val_cells):
    model.eval()
    accs = []
    with torch.no_grad():
        for idx in val_cells:
            n = len(idx)
            ii, jj = np.triu_indices(n, k=1)
            m = np.abs(y[idx[ii]] - y[idx[jj]]) >= TIE_EPS
            ii, jj = ii[m], jj[m]
            lg = np.concatenate([
                model.pair_logit(At[idx[ii[k:k + 8192]]],
                                 At[idx[jj[k:k + 8192]]]).cpu().numpy()
                for k in range(0, len(ii), 8192)])
            accs.append(np.mean((lg > 0) == (y[idx[ii]] > y[idx[jj]])))
    return float(np.mean(accs))


def tourney(models, At, sub):
    """Mean antisymmetrized win prob per player in sub, averaged over models."""
    n = len(sub)
    ii, jj = np.triu_indices(n, k=1)
    wins = np.zeros(n)
    with torch.no_grad():
        for k in range(0, len(ii), 8192):
            lg = np.mean([m.pair_logit(At[sub[ii[k:k + 8192]]],
                                       At[sub[jj[k:k + 8192]]]).cpu().numpy()
                          for m in models], axis=0)
            w = 1 / (1 + np.exp(-lg))
            np.add.at(wins, ii[k:k + 8192], w)
            np.add.at(wins, jj[k:k + 8192], 1 - w)
    return wins / (n - 1)


def run_target(target, Xf, d, y):
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    seasons = np.array([str(s) for s in d["season"]])
    stamps = ("20160715000000", "20170715000000", "20180715000000",
              "20201101000000")
    is_valcell = tr & np.isin(seasons, VAL_SEASONS) & \
        np.isin(d["timestamp"], stamps)
    tr_fit = tr & ~is_valcell
    cells_te = np.array([str(s) for s in d["season"][test]])

    med = np.nanmedian(Xf[tr_fit], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Xf), Xf, med)
    mu, sd = A[tr_fit].mean(0), A[tr_fit].std(0)
    sd[sd == 0] = 1.0
    At = torch.tensor(((A - mu) / sd).astype(np.float32), device=DEV)
    fit_cells = [np.where(tr_fit & (cells_all == c))[0]
                 for c in np.unique(cells_all[tr_fit])]
    fit_cells = [c for c in fit_cells if len(c) >= 20]
    val_cells = [np.where(is_valcell & (cells_all == c))[0]
                 for c in np.unique(cells_all[is_valcell])]
    print(f"\n=== {target}: {len(fit_cells)} train cells, "
          f"{len(val_cells)} val cells ===", flush=True)

    rngg = np.random.default_rng(7)
    trials = []
    total_ep = sum(STAGE_EPOCHS)
    for i in range(N_CONFIGS):
        cfg = sample_config(rngg)
        torch.manual_seed(i)
        t = {"i": i, "cfg": cfg, "rng": np.random.default_rng(i),
             "model": PairModel(At.shape[1], cfg).to(DEV)}
        t["opt"] = torch.optim.AdamW(t["model"].parameters(), lr=cfg["lr"],
                                     weight_decay=cfg["wd"])
        t["sched"] = torch.optim.lr_scheduler.CosineAnnealingLR(
            t["opt"], T_max=total_ep) if cfg["sched"] == "cosine" else None
        t0 = time.time()
        train_stage(t, At, y, fit_cells, STAGE_EPOCHS[0])
        t["acc"] = val_pair_acc(t["model"], At, y, val_cells)
        trials.append(t)
        print(f"[{target} s1 {i:>2}] {cfg['family']:<9} res={cfg['res']} "
              f"w={cfg['width']} d={cfg['depth']} {cfg['act']}/{cfg['norm']} "
              f"-> val acc {t['acc']:.4f} ({time.time()-t0:.0f}s)", flush=True)

    for stage, (keep_n, ep) in enumerate(zip(KEEP, STAGE_EPOCHS[1:]), 2):
        trials.sort(key=lambda t: -t["acc"])
        trials = trials[:keep_n]
        for t in trials:
            train_stage(t, At, y, fit_cells, ep)
            t["acc"] = val_pair_acc(t["model"], At, y, val_cells)
            print(f"[{target} s{stage} {t['i']:>2}] {t['cfg']['family']:<9} "
                  f"-> val acc {t['acc']:.4f}", flush=True)

    trials.sort(key=lambda t: -t["acc"])
    top3 = trials[:3]
    for t in top3:
        t["model"].eval()
    print(f"[{target}] winner cfg {top3[0]['i']} "
          f"{json.dumps(top3[0]['cfg'])} val acc {top3[0]['acc']:.4f}",
          flush=True)

    te_idx = np.where(test)[0]
    res = {}
    scores_ens = np.full(Xf.shape[0], np.nan)
    for name, models in (("solo", [top3[0]["model"]]),
                         ("top3-ens", [t["model"] for t in top3])):
        p = np.empty(len(te_idx))
        pos_of = {j: k for k, j in enumerate(te_idx)}
        for c in np.unique(cells_te):
            sub = te_idx[cells_te == c]
            for j, wv in zip(sub, tourney(models, At, sub)):
                p[pos_of[j]] = wv
        s = score_cells(y[test], p, cells_te)
        res[name] = {k: (int(v) if isinstance(v, (int, np.integer)) else
                         float(v)) for k, v in s.items()}
        print(f"[{target}] TEST {name}: dev@10={s['dev@10']:.2f} "
              f"dev@20={s['dev@20']:.2f} tau@10={s['tau@10']:+.3f} "
              f"tau@20={s['tau@20']:+.3f} hits@20={s['hits@20']}/40",
              flush=True)
    # row-aligned ensemble tournament scores for all whole-season RS cells
    whole = np.isin(d["timestamp"],
                    ["20140715000000", "20150715000000"] + list(stamps)
                    + ["20190715000000", "20210801000000", "20220715000000",
                       "20230715000000"]) & rs
    models = [t["model"] for t in top3]
    for c in np.unique(cells_all[whole]):
        sub = np.where(whole & (cells_all == c))[0]
        if len(sub) >= 20:
            scores_ens[sub] = tourney(models, At, sub)
    return {"winner": top3[0]["cfg"], "val_acc": top3[0]["acc"],
            "top3": [{"i": t["i"], "cfg": t["cfg"], "acc": t["acc"]}
                     for t in top3],
            "test": res}, scores_ens


def main():
    X, feat, d = prepare(str(REPO_ROOT / "training" / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    dfz = np.load(REPO_ROOT / "training" / "data_fixed" / "defend.npz",
                  allow_pickle=True)
    opp = np.load(REPO_ROOT / "training" / "data_fixed" / "wowyopp.npz",
                  allow_pickle=True)
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Eopp, _ = engineered(opp["on_X"], opp["off_X"],
                         [str(f) for f in opp["fields"]], cells_all)
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    FEATS = {"offense": np.hstack([X, Z, Eopp]).astype(np.float32),
             "defense": np.hstack([X, Z, dfz["E"]]).astype(np.float32)}
    out, preds = {}, {}
    for target in ("offense", "defense"):
        out[target], preds[target] = run_target(
            target, FEATS[target], d, d[TARGETS[target]].astype(np.float64))
    np.savez_compressed(REPO_ROOT / "training" / "data_fixed"
                        / "nas_pairwise2_preds.npz", **preds)
    Path(REPO_ROOT / "training" / "RESULTS_nas_pairwise2.json").write_text(
        json.dumps(out, indent=1))
    print("\nwrote RESULTS_nas_pairwise2.json + "
          "data_fixed/nas_pairwise2_preds.npz", flush=True)


if __name__ == "__main__":
    main()
