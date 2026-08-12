"""Exploration round 1 on the rebuild branch: structural-prior hybrids.

The structural reproduction encodes 538's domain knowledge in ~30 variables
with published constants. Three ways to combine it with a flexible learner,
against a fresh full-matrix baseline:

  struct        the v2 learned-blend structural model alone (reference)
  gbm           full-matrix LightGBM+ridge blend (fresh fit on this branch)
  gbm-resid     structural hat + GBM trained on the RESIDUAL (y - hat):
                the learner spends capacity only where 538's model is wrong
  gbm+hats      full matrix with the four component hats appended as features

Test seasons scored once per arm; season-held-out CV for any arm that beats
the gbm reference there (cv flag).

Run:  python training/raptor2/hybrid.py
"""

import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import blend, engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from variables2 import OO2_NAMES_DEF, OO2_NAMES_OFF, build_onoff2
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
RS_MIN, PO_MIN = 50, 10
FLOOR = 1065


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    ofields = [str(f) for f in oppz["fields"]]
    Eopp, _ = engineered(oppz["on_X"], oppz["off_X"], ofields, cells)
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)

    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OFF2, DEF2 = build_onoff2(X, feat, oppz["on_X"], oppz["off_X"],
                              ofields, cells, mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OOo = cell_relative(OFF2, cells, mp)
    OOd = cell_relative(DEF2, cells, mp)

    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    w = np.sqrt(np.maximum(mp, 1.0))
    cells_te = np.array([str(s) for s in d["season"][test]])
    el = mp[test] >= FLOOR
    tuned = json.loads((TD / "tuned_params.json").read_text())

    BLOCKS = {"box_o": (OB, V["OB_NAMES"], "rap_box_o"),
              "onoff_o": (OOo, OO2_NAMES_OFF, "rap_onoff_o"),
              "box_d": (DB, V["DB_NAMES"], "rap_box_d"),
              "onoff_d": (OOd, OO2_NAMES_DEF, "rap_onoff_d")}
    hats = {}
    for tag, (M, names, labname) in BLOCKS.items():
        yv = comp[labname]
        m = tr & np.isfinite(yv)
        hats[tag] = ridge_hat(M[m], yv[m], w[m], M, names, tag, quiet=True)
    HATS = np.column_stack([hats[t] for t in
                            ("box_o", "onoff_o", "box_d", "onoff_d")])

    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, dfz["E"]])}
    out = {}
    for side, bx, oo in (("offense", "box_o", "onoff_o"),
                         ("defense", "box_d", "onoff_d")):
        y = d[TARGETS[side]].astype(np.float64)
        params = dict(tuned[side]["params"], verbose=-1)
        rounds = max(tuned[side]["rounds"] // 3, 150)
        Xf = FEATS[side]
        med = np.nanmedian(Xf[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)

        # structural learned blend
        m = tr & np.isfinite(y)
        D = np.column_stack([hats[bx][m], hats[oo][m]])
        cb = RidgeCV(alphas=np.logspace(-3, 3, 13)).fit(D, y[m],
                                                        sample_weight=w[m])
        p_struct = cb.predict(np.column_stack([hats[bx], hats[oo]]))

        arms = {}
        arms["struct"] = p_struct[test]
        arms["gbm"] = blend(Xf[tr], y[tr], Xf[test], med, params, rounds)
        resid = y - p_struct
        arms["gbm-resid"] = p_struct[test] + blend(
            Xf[tr], resid[tr], Xf[test], med, params, rounds)
        Xh = np.hstack([Xf, HATS])
        mh = np.nanmedian(Xh[tr], axis=0)
        mh = np.where(np.isfinite(mh), mh, 0.0)
        arms["gbm+hats"] = blend(Xh[tr], y[tr], Xh[test], mh, params, rounds)

        print(f"\n=== {side} (test cells, >=1065 pool) ===", flush=True)
        for name, p in arms.items():
            s = score_cells(y[test][el], p[el], cells_te[el])
            out[f"{side}|{name}"] = {k: (int(v) if isinstance(
                v, (int, np.integer)) else round(float(v), 4))
                for k, v in s.items()}
            print(f"  {name:<10} dev@10={s['dev@10']:5.2f} "
                  f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
                  f"tau@20={s['tau@20']:+.3f} MAE={s['mae']:.3f} "
                  f"hits@10={s['hits@10']}/20", flush=True)

    Path(TD / "raptor2" / "RESULTS_hybrid.json").write_text(
        json.dumps(out, indent=1))
    print("\nwrote raptor2/RESULTS_hybrid.json", flush=True)


if __name__ == "__main__":
    main()
