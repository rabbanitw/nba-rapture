"""NAS round 2: direct-rating neural networks (offense and defense regression).

Round 1 searched only the pairwise scorer. This searches architectures that
predict the rating directly -- the task the production GBM blends win at -- over
a wider family space than any prior neural attempt here:

  mlp        plain trunk, depth 1-4
  resmlp     residual blocks (Linear-Norm-Act-Drop-Linear + skip), depth 2-6
  snn        self-normalizing net: SELU + LeCun-normal init + alpha-dropout
  bottleneck wide first layer -> narrow bottleneck -> head (learned compression
             of the 1.1k-column profile)

plus activation/norm/dropout/lr/weight-decay/batch/scheduler. Search = random
sample of 24 configs x short budget, successive halving 24 -> 8 -> 3, selection
on the mean validation-cell Spearman (whole-season VAL cells; test never touched
until the end). The 3 finalists are seed-averaged (x3) and scored once on the
test cells. Rows are minute-weighted in the loss (sqrt(mp), the convention the
GBM benefits from implicitly via label reliability).

Targets: offense uses X+Z+Eopp (production offense feature set), defense
X+Z+E(defend). Benchmarks: production GBM blends offense dev@10 ~1.10-1.50,
defense ~3.80 on the test cells.

Run:  python training/nas_direct.py
"""

import json
import time
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from scipy.stats import spearmanr

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS, VAL_SEASONS

RS_MIN, PO_MIN = 50, 10
N_CONFIGS = 24
STAGE_EPOCHS = (30, 60, 120)
KEEP = (8, 3)
FINAL_SEEDS = (0, 1, 2)
DEV = "cuda" if torch.cuda.is_available() else "cpu"
ACTS = {"relu": nn.ReLU, "gelu": nn.GELU, "silu": nn.SiLU}


def sample_config(rng):
    fam = str(rng.choice(["mlp", "resmlp", "snn", "bottleneck"]))
    width = int(rng.choice([128, 256, 512, 1024]))
    depth = int(rng.choice([1, 2, 3, 4] if fam != "resmlp" else [2, 3, 4, 6]))
    return {"family": fam, "width": width, "depth": depth,
            "act": str(rng.choice(["relu", "gelu", "silu"])),
            "norm": str(rng.choice(["batch", "layer", "none"])),
            "dropout": float(rng.choice([0.0, 0.1, 0.2, 0.35])),
            "lr": float(10 ** rng.uniform(-3.7, -2.3)),
            "wd": float(rng.choice([0.0, 1e-5, 1e-4, 1e-3])),
            "batch": int(rng.choice([256, 512, 1024])),
            "sched": str(rng.choice(["none", "cosine"])),
            "bneck": int(rng.choice([32, 64, 128]))}


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


def build_net(n_in, cfg):
    fam, w, dep = cfg["family"], cfg["width"], cfg["depth"]
    act, nm, dr = cfg["act"], cfg["norm"], cfg["dropout"]
    if fam == "snn":
        layers, prev = [], n_in
        for _ in range(dep):
            lin = nn.Linear(prev, w)
            nn.init.normal_(lin.weight, 0, (1 / prev) ** 0.5)
            layers += [lin, nn.SELU()]
            if dr:
                layers.append(nn.AlphaDropout(dr / 2))
            prev = w
        layers.append(nn.Linear(prev, 1))
        return nn.Sequential(*layers)
    if fam == "resmlp":
        layers = [nn.Linear(n_in, w), norm_layer(nm, w), ACTS[act]()]
        layers += [ResBlock(w, act, nm, dr) for _ in range(dep)]
        layers.append(nn.Linear(w, 1))
        return nn.Sequential(*layers)
    if fam == "bottleneck":
        b = cfg["bneck"]
        layers = [nn.Linear(n_in, w), norm_layer(nm, w), ACTS[act](),
                  nn.Dropout(dr), nn.Linear(w, b), norm_layer(nm, b),
                  ACTS[act]()]
        for _ in range(max(dep - 1, 0)):
            layers += [nn.Linear(b, b), norm_layer(nm, b), ACTS[act](),
                       nn.Dropout(dr)]
        layers.append(nn.Linear(b, 1))
        return nn.Sequential(*layers)
    layers, prev = [], n_in
    for _ in range(dep):
        layers += [nn.Linear(prev, w), norm_layer(nm, w), ACTS[act](),
                   nn.Dropout(dr)]
        prev = w
    layers.append(nn.Linear(prev, 1))
    return nn.Sequential(*layers)


def train_epochs(model, opt, sched, At, yt, wt, idx, cfg, rng, epochs):
    lossf = nn.SmoothL1Loss(reduction="none")
    for _ in range(epochs):
        model.train()
        perm = rng.permutation(len(idx))
        for k in range(0, len(perm), cfg["batch"]):
            sel = idx[perm[k:k + cfg["batch"]]]
            if len(sel) < 8:
                continue
            out = model(At[sel]).squeeze(-1)
            loss = (lossf(out, yt[sel]) * wt[sel]).mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
        if sched is not None:
            sched.step()


def val_score(model, At, y, val_cells):
    model.eval()
    with torch.no_grad():
        p = model(At).squeeze(-1).cpu().numpy()
    return float(np.mean([spearmanr(p[c], y[c]).statistic
                          for c in val_cells]))


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
    cells_te = np.array([f"{s}" for s in d["season"][test]])

    med = np.nanmedian(Xf[tr_fit], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Xf), Xf, med)
    mu, sd = A[tr_fit].mean(0), A[tr_fit].std(0)
    sd[sd == 0] = 1.0
    A = ((A - mu) / sd).astype(np.float32)
    At = torch.tensor(A, device=DEV)
    yt = torch.tensor(y.astype(np.float32), device=DEV)
    w = np.sqrt(np.maximum(d["mp"].astype(np.float64), 1.0))
    w = w / w[tr_fit].mean()
    wt = torch.tensor(w.astype(np.float32), device=DEV)
    tr_idx = np.where(tr_fit)[0]
    val_cells = [np.where(is_valcell & (cells_all == c))[0]
                 for c in np.unique(cells_all[is_valcell])]
    print(f"\n=== {target}: train {len(tr_idx)}, {len(val_cells)} val cells, "
          f"{int(test.sum())} test rows, {A.shape[1]} feats ===", flush=True)

    rngg = np.random.default_rng(42)
    trials = []
    for i in range(N_CONFIGS):
        cfg = sample_config(rngg)
        torch.manual_seed(i)
        rng = np.random.default_rng(i)
        model = build_net(A.shape[1], cfg).to(DEV)
        opt = torch.optim.AdamW(model.parameters(), lr=cfg["lr"],
                                weight_decay=cfg["wd"])
        sched = torch.optim.lr_scheduler.CosineAnnealingLR(
            opt, T_max=sum(STAGE_EPOCHS)) if cfg["sched"] == "cosine" else None
        t0 = time.time()
        train_epochs(model, opt, sched, At, yt, wt, tr_idx, cfg, rng,
                     STAGE_EPOCHS[0])
        sc = val_score(model, At, y, val_cells)
        trials.append({"i": i, "cfg": cfg, "sc": sc, "model": model,
                       "opt": opt, "sched": sched, "rng": rng})
        print(f"[{target} s1 {i:>2}] {cfg['family']:<10} w={cfg['width']} "
              f"d={cfg['depth']} {cfg['act']}/{cfg['norm']} "
              f"do={cfg['dropout']} lr={cfg['lr']:.4f} -> val rho {sc:+.4f} "
              f"({time.time()-t0:.0f}s)", flush=True)

    for stage, (keep_n, ep) in enumerate(zip(KEEP, STAGE_EPOCHS[1:]), 2):
        trials.sort(key=lambda t: -t["sc"])
        trials = trials[:keep_n]
        for t in trials:
            train_epochs(t["model"], t["opt"], t["sched"], At, yt, wt, tr_idx,
                         t["cfg"], t["rng"], ep)
            t["sc"] = val_score(t["model"], At, y, val_cells)
            print(f"[{target} s{stage} {t['i']:>2}] {t['cfg']['family']:<10} "
                  f"-> val rho {t['sc']:+.4f}", flush=True)

    trials.sort(key=lambda t: -t["sc"])
    best = trials[0]
    print(f"[{target}] winner cfg {best['i']}: {json.dumps(best['cfg'])}",
          flush=True)
    # retrain winner from scratch with 3 seeds on the full budget, seed-average
    total_ep = sum(STAGE_EPOCHS)
    preds = []
    for seed in FINAL_SEEDS:
        torch.manual_seed(100 + seed)
        rng = np.random.default_rng(100 + seed)
        model = build_net(A.shape[1], best["cfg"]).to(DEV)
        opt = torch.optim.AdamW(model.parameters(), lr=best["cfg"]["lr"],
                                weight_decay=best["cfg"]["wd"])
        sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=total_ep) \
            if best["cfg"]["sched"] == "cosine" else None
        train_epochs(model, opt, sched, At, yt, wt, tr_idx, best["cfg"], rng,
                     total_ep)
        model.eval()
        with torch.no_grad():
            preds.append(model(At).squeeze(-1).cpu().numpy())
        print(f"[{target}] final seed {seed} val rho "
              f"{val_score(model, At, y, val_cells):+.4f}", flush=True)
    p_all = np.mean(preds, axis=0)
    s = score_cells(y[test], p_all[test], cells_te)
    print(f"[{target}] TEST dev@10={s['dev@10']:.2f} dev@20={s['dev@20']:.2f} "
          f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
          f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
          f"hits@20={s['hits@20']}/40", flush=True)
    return {"winner": best["cfg"], "val_rho": best["sc"],
            "test": {k: (int(v) if isinstance(v, (int, np.integer)) else
                         float(v)) for k, v in s.items()},
            "leaderboard": [{"i": t["i"], "cfg": t["cfg"], "val": t["sc"]}
                            for t in trials]}, p_all


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
                        / "nas_direct_preds.npz", **preds)
    Path(REPO_ROOT / "training" / "RESULTS_nas_direct.json").write_text(
        json.dumps(out, indent=1))
    print("\nwrote RESULTS_nas_direct.json + data_fixed/nas_direct_preds.npz",
          flush=True)


if __name__ == "__main__":
    main()
