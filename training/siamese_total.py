"""Siamese network for TOTAL RAPTOR: shared trunk, offense and defense branches,
output = their sum.

The published labels satisfy rap = rap_o + rap_d up to 538's one-decimal rounding
(max observed deviation 0.101), so total prediction decomposes exactly. The network
makes that structure explicit:

    input -> shared trunk -> offense head -> o_pred
                          -> defense head -> d_pred
    total prediction = o_pred + d_pred

    loss = MSE(o_pred + d_pred, rap)                 the quantity being predicted
         + MSE(o_pred, rap_o) + MSE(d_pred, rap_d)   keeps the branches honest
         + 0.3 * aux component losses                rap_box_o/d, rap_onoff_o/d
                                                     (the ablation showed these are
                                                     worth several dev@10 points)

Compared against, on the same rows and the same total label:

    lgbm-total       one direct model on rap (the old way, retired at the user's
                     request for *separate* models -- here purely as the yardstick)
    lgbm o+d sum     sum of two independent LightGBM blends (offense, defense)
    production sum   offense components architecture + defense cell-relative,
                     summed -- what the shipped separate models imply for total

Run:  python training/siamese_total.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import torch
import torch.nn as nn
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold, GroupShuffleSplit

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import (COMPONENT_LABELS, RELATIVE_COLS, cell_relative,
                                   combiner_design, masks_for)
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from siamese_model import Siamese
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
AUX_W = 0.3
MAX_EPOCHS = 300
PATIENCE = 25


class SiameseTotal(nn.Module):
    """Trunk + offense/defense branches; total is their sum by construction."""

    def __init__(self, n_in, trunk=(512, 256), head=64, p_drop=0.2):
        super().__init__()
        base = Siamese(n_in, trunk=trunk, head=head, p_drop=p_drop)
        self.trunk = base.trunk
        self.off_head = base.off_head
        self.def_head = base.def_head
        self.aux_heads = base.aux_heads

    def forward(self, x):
        z = self.trunk(x)
        o = self.off_head(z).squeeze(-1)
        d = self.def_head(z).squeeze(-1)
        return o, d, [h(z).squeeze(-1) for h in self.aux_heads]


def train_one(Xtr, Ytr, Xva, Yva, seed, lr=1e-3, batch=256):
    """Y columns: [rap, rap_o, rap_d, box_o, onoff_o, box_d, onoff_d]."""
    torch.manual_seed(seed)
    np.random.seed(seed)
    model = SiameseTotal(Xtr.shape[1])
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    sched = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, factor=0.5, patience=8)
    mse = nn.MSELoss()
    Xtr_t = torch.tensor(Xtr)
    Ytr_t = torch.tensor(Ytr)
    Xva_t = torch.tensor(Xva)
    Yva_t = torch.tensor(Yva)
    n = len(Xtr_t)
    best, best_state, bad = np.inf, None, 0
    for epoch in range(MAX_EPOCHS):
        model.train()
        perm = torch.randperm(n)
        for i in range(0, n, batch):
            idx = perm[i:i + batch]
            if len(idx) < 8:
                continue
            o, dfn, aux = model(Xtr_t[idx])
            yb = Ytr_t[idx]
            loss = (mse(o + dfn, yb[:, 0]) + mse(o, yb[:, 1]) + mse(dfn, yb[:, 2]))
            for j, a in enumerate(aux):
                loss = loss + AUX_W * mse(a, yb[:, 3 + j])
            opt.zero_grad()
            loss.backward()
            opt.step()
        model.eval()
        with torch.no_grad():
            o, dfn, _ = model(Xva_t)
            vloss = mse(o + dfn, Yva_t[:, 0]).item()
        sched.step(vloss)
        if vloss < best - 1e-4:
            best, bad = vloss, 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            bad += 1
            if bad >= PATIENCE:
                break
    model.load_state_dict(best_state)
    return model, epoch


def blend(Xtr, t, Xte, med, params, rounds, seeds=(0, 1, 2), ridge_w=0.25):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, t).predict(
        (B - mu) / sd)
    ps = [lgb.train(dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s),
                    lgb.Dataset(Xtr, t), num_boost_round=rounds).predict(Xte)
          for s in seeds]
    return (1 - ridge_w) * np.mean(ps, axis=0) + ridge_w * pr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_siamese_total.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(Path(args.datadir) / "components.npz")
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    groups = np.array([f"{p}|{s}" for p, s in
                       zip(d["player"][tr], d["season"][tr])])
    mp = d["mp"].astype(np.float64)
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    tuned = json.loads(Path(args.tuned).read_text())
    y_tot = d[TARGETS["total"]]
    y_off = d[TARGETS["offense"]]
    y_def = d[TARGETS["defense"]]
    print(f"X={X.shape} train={tr.sum()} test={test.sum()}", flush=True)
    rows = []

    def record(name, p, extra=""):
        s = score_cells(y_tot[test], p, cells_te)
        rows.append({"model": name, **s})
        print(f"  {name:<16} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40  {extra}", flush=True)

    # ---- Siamese total ------------------------------------------------------
    A = np.where(np.isfinite(X[tr]), X[tr], med)
    B = np.where(np.isfinite(X[test]), X[test], med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    An = ((A - mu) / sd).astype(np.float32)
    Bn = ((B - mu) / sd).astype(np.float32)
    Y = np.column_stack([y_tot[tr], y_off[tr], y_def[tr],
                         comp["rap_box_o"][tr], comp["rap_onoff_o"][tr],
                         comp["rap_box_d"][tr], comp["rap_onoff_d"][tr]]
                        ).astype(np.float32)
    tr_i, va_i = next(GroupShuffleSplit(n_splits=1, test_size=0.12,
                                        random_state=7).split(An, Y, groups=groups))
    preds, opreds, dpreds = [], [], []
    for seed in range(args.seeds):
        model, epochs = train_one(An[tr_i], Y[tr_i], An[va_i], Y[va_i], seed)
        model.eval()
        with torch.no_grad():
            o, dfn, _ = model(torch.tensor(Bn))
        preds.append((o + dfn).numpy())
        opreds.append(o.numpy())
        dpreds.append(dfn.numpy())
        print(f"  [siamese seed {seed}] stopped epoch {epochs}", flush=True)
    record("siamese-total", np.mean(preds, axis=0))
    so = score_cells(y_off[test], np.mean(opreds, axis=0), cells_te)
    sd_ = score_cells(y_def[test], np.mean(dpreds, axis=0), cells_te)
    print(f"    (branches: offense dev@10={so['dev@10']:.2f} MAE={so['mae']:.3f}; "
          f"defense dev@10={sd_['dev@10']:.2f} MAE={sd_['mae']:.3f})", flush=True)

    # ---- LGBM baselines -----------------------------------------------------
    params_t = dict(tuned["total"]["params"], verbose=-1)
    rounds_t = tuned["total"]["rounds"]
    record("lgbm-total", blend(X[tr], y_tot[tr], X[test], med, params_t, rounds_t))

    params_o = dict(tuned["offense"]["params"], verbose=-1)
    rounds_o = tuned["offense"]["rounds"]
    params_d = dict(tuned["defense"]["params"], verbose=-1)
    rounds_d = tuned["defense"]["rounds"]
    po = blend(X[tr], y_off[tr], X[test], med, params_o, rounds_o)
    pd_ = blend(X[tr], y_def[tr], X[test], med, params_d, rounds_d)
    record("lgbm o+d sum", po + pd_)

    # ---- production sum: offense components + defense cell-relative ---------
    box_mask, onoff_mask = masks_for(feat)
    bl, ol = (comp[c] for c in COMPONENT_LABELS["offense"])
    Xb, Xo = X[:, box_mask], X[:, onoff_mask]
    mb, mo = med[box_mask], med[onoff_mask]
    box_oof = np.full(int(tr.sum()), np.nan)
    onoff_oof = np.full(int(tr.sum()), np.nan)
    Xb_tr, Xo_tr = Xb[tr], Xo[tr]
    for tri, vai in GroupKFold(n_splits=4).split(Xb_tr, bl[tr], groups=groups):
        box_oof[vai] = blend(Xb_tr[tri], bl[tr][tri], Xb_tr[vai], mb,
                             params_o, rounds_o, seeds=(0,))
        onoff_oof[vai] = blend(Xo_tr[tri], ol[tr][tri], Xo_tr[vai], mo,
                               params_o, rounds_o, seeds=(0,))
    combiner = Ridge(alpha=1.0).fit(
        combiner_design(box_oof, onoff_oof, mp[tr]), y_off[tr])
    p_off_prod = combiner.predict(combiner_design(
        blend(Xb_tr, bl[tr], Xb[test], mb, params_o, rounds_o),
        blend(Xo_tr, ol[tr], Xo[test], mo, params_o, rounds_o), mp[test]))
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    Xz = np.hstack([X, Z])
    mz = np.concatenate([med, np.zeros(Z.shape[1])])
    p_def_prod = blend(Xz[tr], y_def[tr], Xz[test], mz, params_d, rounds_d)
    record("production sum", p_off_prod + p_def_prod)

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    L = ["# Total RAPTOR via summed branches", "",
         "Total = offense + defense (exact in the labels up to 538's rounding), so",
         "every model here predicts the two ends and is scored on the sum.", "",
         "| model | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |",
         "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in sorted(rows, key=lambda r: r["dev@10"]):
        L.append(f"| {r['model']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
                 f"{r['tau@10']:+.3f} | {r['tau@20']:+.3f} | {r['mae']:.3f} | "
                 f"{r['hits@10']}/20 | {r['hits@20']}/40 |")
    Path(args.out).write_text("\n".join(L))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
