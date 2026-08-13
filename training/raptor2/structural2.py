"""Structural reproduction v2: box block from v1 + pre-adjusted on-off block.

Adds the decisive read: 10-fold season-held-out CV of the fixed-blend and
learned-blend structural models (test seasons evaluated once, like every
number on this branch).

Run:  python training/raptor2/structural2.py
"""

import json
import sys
import warnings
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scipy.stats import spearmanr
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import BOX_W, ONOFF_W, build_variables
from variables2 import OO2_NAMES_DEF, OO2_NAMES_OFF, build_onoff2

TD = REPO_ROOT / "training"
RS_MIN, PO_MIN = 50, 10
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}


def ridge_hat(Vtr, ytr, wtr, Vall, names, tag, quiet=False):
    # Some methodology variables are structurally unavailable in an early fold.
    # They are intentionally zero-imputed below; suppress NumPy's expected
    # all-NaN-column warning so canonical runs stay readable.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="All-NaN slice encountered")
        med = np.nanmedian(Vtr, axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Vtr), Vtr, med)
    B = np.where(np.isfinite(Vall), Vall, med)
    model = RidgeCV(alphas=np.logspace(-3, 3, 25)).fit(A, ytr,
                                                       sample_weight=wtr)
    if not quiet:
        order = np.argsort(-np.abs(model.coef_))
        print(f"  [{tag}] " + "  ".join(
            f"{names[j]}={model.coef_[j]:+.2f}" for j in order[:5]),
            flush=True)
    return model.predict(B)


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
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OFF2, DEF2 = build_onoff2(X, feat, oppz["on_X"], oppz["off_X"],
                              [str(f) for f in oppz["fields"]], cells, mp)
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
    y_all = {s: d[TARGETS[s]].astype(np.float64)
             for s in ("offense", "defense")}

    BLOCKS = {"box_o": (OB, V["OB_NAMES"], "rap_box_o"),
              "onoff_o": (OOo, OO2_NAMES_OFF, "rap_onoff_o"),
              "box_d": (DB, V["DB_NAMES"], "rap_box_d"),
              "onoff_d": (OOd, OO2_NAMES_DEF, "rap_onoff_d")}

    def fit_structural(mask_tr, quiet=False):
        """Fit all four component ridges on mask_tr; return row-aligned hats."""
        hats = {}
        for tag, (M, names, labname) in BLOCKS.items():
            yv = comp[labname]
            m = mask_tr & np.isfinite(yv)
            hats[tag] = ridge_hat(M[m], yv[m], w[m], M, names, tag, quiet)
            if not quiet:
                print(f"      rho vs {labname}: "
                      f"{spearmanr(hats[tag][m], yv[m]).statistic:+.3f}",
                      flush=True)
        return hats

    print("== full-train fit ==", flush=True)
    hats = fit_structural(tr)
    results = {}
    for side, bx, oo in (("offense", "box_o", "onoff_o"),
                         ("defense", "box_d", "onoff_d")):
        y = y_all[side]
        m = tr & np.isfinite(y)
        D = np.column_stack([hats[bx][m], hats[oo][m]])
        cb = RidgeCV(alphas=np.logspace(-3, 3, 13)).fit(D, y[m],
                                                        sample_weight=w[m])
        print(f"[{side}] learned blend: box={cb.coef_[0]:.3f} "
              f"onoff={cb.coef_[1]:.3f} (538: 0.85/0.21)", flush=True)
        for arm, p_all in (
                ("fixed", BOX_W * hats[bx] + ONOFF_W * hats[oo]),
                ("learned", cb.predict(np.column_stack([hats[bx], hats[oo]])))):
            s = score_cells(y[test][el], p_all[test][el], cells_te[el])
            results[f"{side}|{arm}"] = {k: (int(v) if isinstance(
                v, (int, np.integer)) else round(float(v), 4))
                for k, v in s.items()}
            print(f"  {arm:<8} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
                  f"tau@10={s['tau@10']:+.3f} MAE={s['mae']:.3f} "
                  f"hits@10={s['hits@10']}/20", flush=True)

    # ---- 10-fold season-held-out CV of the learned blend -------------------
    print("\n== season-held-out CV (learned blend) ==", flush=True)
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values()))
    cv = {s: {} for s in ("offense", "defense")}
    for season, stamp in STAMPS.items():
        te_m = labeled & (d["timestamp"] == stamp)
        tr_m = labeled & (d["timestamp"] != stamp)
        el_m = mp[te_m] >= FLOOR
        h = fit_structural(tr_m, quiet=True)
        for side, bx, oo in (("offense", "box_o", "onoff_o"),
                             ("defense", "box_d", "onoff_d")):
            y = y_all[side]
            m = tr_m & np.isfinite(y)
            D = np.column_stack([h[bx][m], h[oo][m]])
            cb = RidgeCV(alphas=np.logspace(-3, 3, 13)).fit(
                D, y[m], sample_weight=w[m])
            p = cb.predict(np.column_stack([h[bx], h[oo]]))
            s = score_cells(y[te_m][el_m], p[te_m][el_m],
                            np.full(int(el_m.sum()), season))
            cv[side][season] = round(float(s["dev@10"]), 2)
        print(f"  {season}: off {cv['offense'][season]:5.2f}  "
              f"def {cv['defense'][season]:5.2f}", flush=True)
    for side in ("offense", "defense"):
        dv = list(cv[side].values())
        print(f"[{side}] CV median dev@10 {np.median(dv):.2f}  "
              f"mean {np.mean(dv):.2f}", flush=True)
        results[f"{side}|cv"] = cv[side]

    np.savez_compressed(TD / "raptor2" / "structural2_hats.npz",
                        **{k: v for k, v in hats.items()})
    Path(TD / "raptor2" / "RESULTS_structural2.json").write_text(
        json.dumps(results, indent=1))
    print("\nwrote raptor2/structural2_hats.npz + RESULTS_structural2.json",
          flush=True)


if __name__ == "__main__":
    main()
