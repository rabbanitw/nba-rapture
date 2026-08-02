"""Shared-trunk two-headed net: one representation, offense and defense heads.

The idea being tested: offense and defense have so far been entirely separate
models, but they describe one player-season, and the recovered RAPTOR spec makes
clear the two ends share structure (usage shows up in both regressions, the on/off
machinery is identical on both ends, several stats carry opposite-signed value on
each end). A shared trunk lets the representation be supervised by both labels --
and by 538's four published component labels as auxiliary heads, which is exactly
the kind of extra supervision multi-task learning wants.

Architecture:

    input (1140, median-imputed, standardized)
      -> trunk Linear-BN-ReLU-Dropout stack (512 -> 256)
      -> offense head (64 -> 1)   loss vs rap_o
      -> defense head (64 -> 1)   loss vs rap_d
      -> aux heads (64 -> 1) x4   loss vs rap_box_o, rap_onoff_o, rap_box_d,
                                  rap_onoff_d, weighted 0.3 each

Early stopping on a grouped (player-season) validation split, never on the test
seasons. Seed-averaged over 3 runs. Compared against the per-target LightGBM
production models on the same rows.

Run:  python training/siamese_model.py
"""

import argparse
import json
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from sklearn.model_selection import GroupShuffleSplit

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import COMPONENT_LABELS
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
AUX_W = 0.3
MAX_EPOCHS = 300
PATIENCE = 25


class Siamese(nn.Module):
    def __init__(self, n_in, trunk=(512, 256), head=64, p_drop=0.2):
        super().__init__()
        layers, prev = [], n_in
        for h in trunk:
            layers += [nn.Linear(prev, h), nn.BatchNorm1d(h), nn.ReLU(),
                       nn.Dropout(p_drop)]
            prev = h
        self.trunk = nn.Sequential(*layers)

        def make_head():
            return nn.Sequential(nn.Linear(prev, head), nn.ReLU(),
                                 nn.Linear(head, 1))
        self.off_head = make_head()
        self.def_head = make_head()
        self.aux_heads = nn.ModuleList([make_head() for _ in range(4)])

    def forward(self, x):
        z = self.trunk(x)
        return (self.off_head(z).squeeze(-1), self.def_head(z).squeeze(-1),
                [h(z).squeeze(-1) for h in self.aux_heads])


def train_one(Xtr, Ytr, Xva, Yva, seed, aux_w=AUX_W, lr=1e-3, batch=256):
    torch.manual_seed(seed)
    np.random.seed(seed)
    model = Siamese(Xtr.shape[1])
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    sched = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, factor=0.5, patience=8)
    mse = nn.MSELoss()
    Xtr_t = torch.tensor(Xtr, dtype=torch.float32)
    Ytr_t = torch.tensor(Ytr, dtype=torch.float32)
    Xva_t = torch.tensor(Xva, dtype=torch.float32)
    Yva_t = torch.tensor(Yva, dtype=torch.float32)
    n = len(Xtr_t)
    best, best_state, bad = np.inf, None, 0
    for epoch in range(MAX_EPOCHS):
        model.train()
        perm = torch.randperm(n)
        for i in range(0, n, batch):
            idx = perm[i:i + batch]
            if len(idx) < 8:          # BatchNorm needs a real batch
                continue
            xo, do, aux = model(Xtr_t[idx])
            yb = Ytr_t[idx]
            loss = mse(xo, yb[:, 0]) + mse(do, yb[:, 1])
            for j, a in enumerate(aux):
                loss = loss + aux_w * mse(a, yb[:, 2 + j])
            opt.zero_grad()
            loss.backward()
            opt.step()
        model.eval()
        with torch.no_grad():
            xo, do, _ = model(Xva_t)
            vloss = (mse(xo, Yva_t[:, 0]) + mse(do, Yva_t[:, 1])).item()
        sched.step(vloss)
        if vloss < best - 1e-4:
            best, bad = vloss, 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            bad += 1
            if bad >= PATIENCE:
                break
    model.load_state_dict(best_state)
    return model, epoch, best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--no-aux", action="store_true",
                    help="drop the component auxiliary heads (ablation)")
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_siamese.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X = X[:, keep]
    comp = np.load(Path(args.datadir) / "components.npz")
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    groups = np.array([f"{p}|{s}" for p, s in
                       zip(d["player"][tr], d["season"][tr])])

    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(X[tr]), X[tr], med)
    B = np.where(np.isfinite(X[test]), X[test], med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    A, B = ((A - mu) / sd).astype(np.float32), ((B - mu) / sd).astype(np.float32)

    Y = np.column_stack([
        d[TARGETS["offense"]][tr], d[TARGETS["defense"]][tr],
        comp["rap_box_o"][tr], comp["rap_onoff_o"][tr],
        comp["rap_box_d"][tr], comp["rap_onoff_d"][tr]]).astype(np.float32)
    print(f"X={A.shape} test={B.shape} targets={Y.shape} "
          f"aux={'off' if args.no_aux else 'on'}", flush=True)

    tr_i, va_i = next(GroupShuffleSplit(n_splits=1, test_size=0.12,
                                        random_state=7).split(A, Y, groups=groups))
    aux_w = 0.0 if args.no_aux else AUX_W
    preds_o, preds_d = [], []
    for seed in range(args.seeds):
        model, epochs, vloss = train_one(A[tr_i], Y[tr_i], A[va_i], Y[va_i],
                                         seed, aux_w=aux_w)
        model.eval()
        with torch.no_grad():
            po, pd_, _ = model(torch.tensor(B))
        preds_o.append(po.numpy())
        preds_d.append(pd_.numpy())
        print(f"  seed {seed}: stopped epoch {epochs}, val loss {vloss:.3f}",
              flush=True)

    rows = []
    for target, ps in (("offense", preds_o), ("defense", preds_d)):
        y = d[TARGETS[target]]
        s = score_cells(y[test], np.mean(ps, axis=0), cells_te)
        rows.append({"target": target, "model": "siamese"
                     + ("-noaux" if args.no_aux else ""), **s})
        print(f"[{target}] dev@10={s['dev@10']:.2f} dev@20={s['dev@20']:.2f} "
              f"tau@10={s['tau@10']:+.3f} MAE={s['mae']:.3f} "
              f"hits@10={s['hits@10']}/20 hits@20={s['hits@20']}/40", flush=True)

    out = Path(args.out)
    old = json.loads(out.with_suffix(".json").read_text()) \
        if out.with_suffix(".json").exists() else []
    out.with_suffix(".json").write_text(json.dumps(old + rows, indent=1))
    print("done")


if __name__ == "__main__":
    main()
