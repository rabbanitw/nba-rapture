"""Opponent features folded into the components architecture. Offense only.

The components model (box -> rap_box_o, on/off -> rap_onoff_o, minutes-aware ridge
combiner) is the best offense model (dev@10 1.20/1.35 across seed sets). Opponent
on/off context helped offense on a weaker base (RESULTS_oppdef.md). This combines
them the way 538's own structure says to:

  box model  + engineered opponent features (20)   538's box-offense regression
                                                   includes "opponents' defensive
                                                   rating" -- strength of the
                                                   defenses faced. That is what the
                                                   engineered set carries.
  on/off model + the full opponent block (687)     the on/off component is lineup
                                                   data, and the opponent block IS
                                                   lineup data -- its other half.

A 2x2 grid isolates each addition. Component models are shared across arms, so four
arms cost the same fits as two.

Run:  python training/experiment_comp_opp.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import COMPONENT_LABELS, combiner_design, masks_for
from experiment_oppdef import blend, engineered, per100
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_comp_opp.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(Path(args.datadir) / "components.npz")
    opp = np.load(Path(args.datadir) / "wowyopp.npz", allow_pickle=True)
    on_raw, off_raw = opp["on_X"], opp["off_X"]
    ofields = [str(f) for f in opp["fields"]]

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
    box_mask, onoff_mask = masks_for(feat)
    tuned = json.loads(Path(args.tuned).read_text())
    params = dict(tuned["offense"]["params"], verbose=-1)
    rounds = tuned["offense"]["rounds"]
    y = d[TARGETS["offense"]]
    box_lab, onoff_lab = (comp[c] for c in COMPONENT_LABELS["offense"])

    E, enames = engineered(on_raw, off_raw, ofields, cells_all)
    on100, off100 = per100(on_raw, ofields), per100(off_raw, ofields)
    B = np.hstack([on100, off100, on100 - off100]).astype(np.float32)
    print(f"X={X.shape}  E={E.shape[1]} cols  B={B.shape[1]} cols  "
          f"train={tr.sum()} test={test.sum()}", flush=True)

    def component(Xs, lab, tag):
        """OOF (seed 0) + seed-averaged final predictions for one component."""
        ms = np.nanmedian(Xs[tr], axis=0)
        ms = np.where(np.isfinite(ms), ms, 0.0)
        oof = np.full(int(tr.sum()), np.nan)
        Xs_tr, lab_tr = Xs[tr], lab[tr]
        for tri, vai in GroupKFold(n_splits=4).split(Xs_tr, lab_tr, groups=groups):
            oof[vai] = blend(Xs_tr[tri], lab_tr[tri], Xs_tr[vai], ms, params,
                             rounds, seeds=(0,))
        te = blend(Xs_tr, lab_tr, Xs[test], ms, params, rounds)
        r2 = 1 - np.nanvar(lab[test] - te) / np.nanvar(lab[test])
        print(f"  [{tag}] component ready (test R2 {r2:+.3f})", flush=True)
        return oof, te

    # the four component variants, computed once each --------------------------
    print("fitting component variants ...", flush=True)
    box_plain = component(X[:, box_mask], box_lab, "box")
    box_oppE = component(np.hstack([X[:, box_mask], E]), box_lab, "box+E")
    onoff_plain = component(X[:, onoff_mask], onoff_lab, "onoff")
    onoff_oppB = component(np.hstack([X[:, onoff_mask], B]), onoff_lab, "onoff+B")

    rows = []
    grid = {
        "components (base)": (box_plain, onoff_plain),
        "box+opp": (box_oppE, onoff_plain),
        "onoff+opp": (box_plain, onoff_oppB),
        "both+opp": (box_oppE, onoff_oppB),
    }
    for name, ((b_oof, b_te), (o_oof, o_te)) in grid.items():
        cb = Ridge(alpha=1.0).fit(combiner_design(b_oof, o_oof, mp[tr]), y[tr])
        pred = cb.predict(combiner_design(b_te, o_te, mp[test]))
        s = score_cells(y[test], pred, cells_te)
        rows.append({"arm": name, "w_box": float(cb.coef_[0]),
                     "w_onoff": float(cb.coef_[1]), **s})
        print(f"  {name:<18} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40  w=[{cb.coef_[0]:.3f} {cb.coef_[1]:.3f}]",
              flush=True)

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    L = ["# Opponent features in the components architecture (offense)", "",
         "| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 | box w | onoff w |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in sorted(rows, key=lambda r: r["dev@10"]):
        L.append(f"| {r['arm']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
                 f"{r['tau@10']:+.3f} | {r['tau@20']:+.3f} | {r['mae']:.3f} | "
                 f"{r['hits@10']}/20 | {r['hits@20']}/40 | {r['w_box']:.3f} | "
                 f"{r['w_onoff']:.3f} |")
    Path(args.out).write_text("\n".join(L))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
