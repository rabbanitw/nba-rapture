"""Promotion run: gbm+hats offense at FULL tuned rounds, 3 seeds, verified on
the test seasons against the production components+opp number measured under
the same full-rounds protocol (final_boards verification convention: >=1065
pools).

Run:  python training/raptor2/promote_hats.py
"""

import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from variables2 import build_onoff2
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
FLOOR = 1065
SEEDS = (0, 1, 2)


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
    OFF2, DEF2 = build_onoff2(X, feat, oppz["on_X"], oppz["off_X"], ofields,
                              cells, mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OOo = cell_relative(OFF2, cells, mp)
    OOd = cell_relative(DEF2, cells, mp)

    fit, val, test = splits(d, 50, 10)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    w = np.sqrt(np.maximum(mp, 1.0))
    cells_te = np.array([str(s) for s in d["season"][test]])
    el = mp[test] >= FLOOR
    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = dict(tuned["offense"]["params"], verbose=-1)
    rounds = tuned["offense"]["rounds"]

    hats = []
    for M, labname in ((OB, "rap_box_o"), (OOo, "rap_onoff_o"),
                       (DB, "rap_box_d"), (OOd, "rap_onoff_d")):
        yv = comp[labname]
        m = tr & np.isfinite(yv)
        hats.append(ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1], "",
                              quiet=True))
    Xf = np.hstack([X, Z, Eopp, np.column_stack(hats)])
    y = d[TARGETS["offense"]].astype(np.float64)
    med = np.nanmedian(Xf[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Xf[tr]), Xf[tr], med)
    B = np.where(np.isfinite(Xf[test]), Xf[test], med)
    mu, sdv = A.mean(0), A.std(0)
    sdv[sdv == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(
        (A - mu) / sdv, y[tr]).predict((B - mu) / sdv)
    members = [0.75 * lgb.train(
        dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s),
        lgb.Dataset(Xf[tr], y[tr]), num_boost_round=rounds).predict(Xf[test])
        + 0.25 * pr for s in SEEDS]
    p = np.mean(members, axis=0)
    s = score_cells(y[test][el], p[el], cells_te[el])
    print(f"[gbm+hats FULL rounds={rounds}] dev@10={s['dev@10']:.2f} "
          f"dev@20={s['dev@20']:.2f} tau@10={s['tau@10']:+.3f} "
          f"tau@20={s['tau@20']:+.3f} MAE={s['mae']:.3f} "
          f"hits@10={s['hits@10']}/20 hits@20={s['hits@20']}/40", flush=True)
    print("production components+opp reference (same pools, full rounds): "
          "dev@10=1.10 tau@10=+0.800 hits@20=35/40", flush=True)
    Path(TD / "raptor2" / "RESULTS_promote_hats.json").write_text(json.dumps(
        {k: (int(v) if isinstance(v, (int, np.integer)) else round(float(v), 4))
         for k, v in s.items()}, indent=1))


if __name__ == "__main__":
    main()
