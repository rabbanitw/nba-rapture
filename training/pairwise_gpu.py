"""All-pairs pairwise ranker on GPU (RankNet-style). Run on the RTX 5090 box.

The CPU version (experiment_pairwise.py) materializes difference vectors and could
only afford ~6k sampled pairs per cell on a 7GB machine. On GPU the formulation
changes: train a scoring network s(x) with BCE on s(x_a) - s(x_b), streaming
minibatches of within-cell pairs. Every pair is visited across epochs -- true
all-pairs coverage (~9M ordered pairs/epoch) with nothing ever materialized --
antisymmetry is exact by construction, and inference is O(n) scores per cell
instead of an O(n^2) round-robin.

Setup (RTX 5090 needs the CUDA 12.8 wheels):

    pip install torch --index-url https://download.pytorch.org/whl/cu128
    python training/pairwise_gpu.py

Prerequisites: training/data_fixed/{combined,defend,wowyopp}.npz -- already present
if this workspace is shared with the container; otherwise build_dataset.py,
experiment_defend.py and extract_wowyopp.py produce them.

Outputs: RESULTS_pairwise_gpu.json (test metrics per epoch/seed) and
data_fixed/pairwise_gpu_preds.npz (row-aligned scores for both targets, all rows)
so the container-side pipeline can score blends against the existing models.

Validation/early stopping uses the four VAL_SEASON whole-season cells (repo
convention) -- the 2013-14/2014-15 test cells are never touched during training.
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
EPOCHS = 40
PATIENCE = 6
BATCH_PAIRS = 8192
SEEDS = (0, 1, 2)
DEV = "cuda" if torch.cuda.is_available() else "cpu"


class Scorer(nn.Module):
    def __init__(self, n_in, hidden=(512, 256), p_drop=0.15):
        super().__init__()
        layers, prev = [], n_in
        for h in hidden:
            layers += [nn.Linear(prev, h), nn.BatchNorm1d(h), nn.ReLU(),
                       nn.Dropout(p_drop)]
            prev = h
        layers.append(nn.Linear(prev, 1))
        self.net = nn.Sequential(*layers)

    def forward(self, x):
        return self.net(x).squeeze(-1)


def cell_pairs(idx_by_cell, y, rng):
    """Yield (a, b) index minibatches covering every valid ordered pair per epoch."""
    order = rng.permutation(len(idx_by_cell))
    cells = [idx_by_cell[i] for i in order]
    for idx in cells:
        n = len(idx)
        ii, jj = np.meshgrid(np.arange(n), np.arange(n), indexing="ij")
        m = ii.ravel() != jj.ravel()
        a, b = idx[ii.ravel()[m]], idx[jj.ravel()[m]]
        keep = np.abs(y[a] - y[b]) >= TIE_EPS
        a, b = a[keep], b[keep]
        perm = rng.permutation(len(a))
        a, b = a[perm], b[perm]
        for k in range(0, len(a), BATCH_PAIRS):
            yield a[k:k + BATCH_PAIRS], b[k:k + BATCH_PAIRS]


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
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    seasons = np.array([str(s) for s in d["season"]])
    is_valcell = tr & np.isin(seasons, VAL_SEASONS) & \
        np.array([t.endswith("0715000000") or t == "20201101000000"
                  for t in d["timestamp"]])
    tr_fit = tr & ~is_valcell
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    print(f"device={DEV}  train={tr_fit.sum()} val={is_valcell.sum()} "
          f"test={test.sum()}", flush=True)

    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, dfz["E"]])}
    out_preds, results = {}, {}
    for target in ("offense", "defense"):
        Xf = FEATS[target].astype(np.float32)
        y = d[TARGETS[target]].astype(np.float32)
        med = np.nanmedian(Xf[tr_fit], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        A = np.where(np.isfinite(Xf), Xf, med)
        mu = A[tr_fit].mean(0)
        sd = A[tr_fit].std(0)
        sd[sd == 0] = 1.0
        A = (A - mu) / sd
        At = torch.tensor(A, device=DEV)

        fit_cells = [np.where(tr_fit & (cells_all == c))[0]
                     for c in np.unique(cells_all[tr_fit])]
        fit_cells = [c for c in fit_cells if len(c) >= 20]
        val_cells = [np.where(is_valcell & (cells_all == c))[0]
                     for c in np.unique(cells_all[is_valcell])]
        n_pairs = sum(len(c) * (len(c) - 1) for c in fit_cells)
        print(f"[{target}] {len(fit_cells)} cells, ~{n_pairs:,} ordered "
              f"pairs/epoch", flush=True)

        seed_scores = []
        for seed in SEEDS:
            torch.manual_seed(seed)
            rng = np.random.default_rng(seed)
            model = Scorer(A.shape[1]).to(DEV)
            opt = torch.optim.AdamW(model.parameters(), lr=1e-3,
                                    weight_decay=1e-4)
            bce = nn.BCEWithLogitsLoss()
            best, best_state, bad = -np.inf, None, 0
            for epoch in range(EPOCHS):
                model.train()
                t0 = time.time()
                for a, b in cell_pairs(fit_cells, y, rng):
                    sa = model(At[a])
                    sb = model(At[b])
                    lab = torch.tensor((y[a] > y[b]).astype(np.float32),
                                       device=DEV)
                    loss = bce(sa - sb, lab)
                    opt.zero_grad()
                    loss.backward()
                    opt.step()
                model.eval()
                with torch.no_grad():
                    accs = []
                    for idx in val_cells:
                        s = model(At[idx]).cpu().numpy()
                        yy = y[idx]
                        ii, jj = np.triu_indices(len(idx), k=1)
                        m = np.abs(yy[ii] - yy[jj]) >= TIE_EPS
                        accs.append(np.mean((s[ii[m]] > s[jj[m]])
                                            == (yy[ii[m]] > yy[jj[m]])))
                    vacc = float(np.mean(accs))
                print(f"  [{target} seed {seed}] epoch {epoch} "
                      f"val pair-acc {vacc:.4f} ({time.time()-t0:.0f}s)",
                      flush=True)
                if vacc > best + 1e-4:
                    best, bad = vacc, 0
                    best_state = {k: v.clone() for k, v
                                  in model.state_dict().items()}
                else:
                    bad += 1
                    if bad >= PATIENCE:
                        break
            model.load_state_dict(best_state)
            model.eval()
            with torch.no_grad():
                seed_scores.append(model(At).cpu().numpy())
            print(f"  [{target} seed {seed}] best val pair-acc {best:.4f}",
                  flush=True)

        scores = np.mean(seed_scores, axis=0)
        out_preds[target] = scores
        s = score_cells(y[test], scores[test], cells_te)
        results[target] = s
        print(f"[{target}] TEST dev@10={s['dev@10']:.2f} dev@20={s['dev@20']:.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"hits@20={s['hits@20']}/40", flush=True)

    np.savez_compressed(REPO_ROOT / "training" / "data_fixed"
                        / "pairwise_gpu_preds.npz",
                        offense=out_preds["offense"],
                        defense=out_preds["defense"])
    Path(REPO_ROOT / "training" / "RESULTS_pairwise_gpu.json").write_text(
        json.dumps(results, indent=1))
    print("\nwrote data_fixed/pairwise_gpu_preds.npz and "
          "RESULTS_pairwise_gpu.json")


if __name__ == "__main__":
    main()
