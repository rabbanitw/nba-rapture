"""Feature selection against a stability target, per 538's own recipe.

538 chose box-RAPTOR variables by how well they predicted LONG-TERM RAPM -- a
multi-year, denoised target -- not by fitting the rating itself. We have always
selected implicitly against single-season labels, which we've measured to be
noisy exactly where it hurts (the label's own elite ordering repeats at only
rho 0.42 year over year, defense).

The analog here: for player p in season t, the stability target is the mean of
p's OWN labels in the adjacent seasons (t-1, t+1) -- sharing no season-t noise
with the training label. Features that predict adjacent-season ratings encode
persistent skill; features that only predict season-t encode skill + noise.

Protocol (per target):
  1  Fit a selection GBM against y_stab on training rows (test-season labels are
     NEVER used as neighbors, so nothing about 2013-14/2014-15 leaks into
     selection). Rank features by gain. Control: identical fit against the
     ordinary label y_curr.
  2  For K in {150, 400, 800}: train the production blend on y_curr restricted
     to the top-K features of each ranking; baseline = all features.
  3  Score everything on the held-out test seasons (2013-14, 2014-15 RS):
     rank deviation dev@10/dev@20, Kendall tau, MAE.
Any stab-selection win on test gets adjudicated by season-grouped 10-fold CV
before being believed.

Run:  python training/experiment_stability_select.py
"""

import json
from pathlib import Path

import lightgbm as lgb
import numpy as np

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend, engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
KS = (150, 400, 800)
TEST_SEASONS = {"2013-14", "2014-15"}
TD = REPO_ROOT / "training"


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    opp = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Eopp, _ = engineered(opp["on_X"], opp["off_X"],
                         [str(f) for f in opp["fields"]], cells_all)
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    FEATS = {"offense": np.hstack([X, Z, Eopp]),
             "defense": np.hstack([X, Z, dfz["E"]])}
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    players = np.array([str(p) for p in d["player"]])
    seasons = np.array([str(s) for s in d["season"]])
    cells_te = seasons[test]
    tuned = json.loads((TD / "tuned_params.json").read_text())

    # season order for neighbor lookup
    ORDER = ["2013-14", "2014-15", "2015-16", "2016-17", "2017-18", "2018-19",
             "2019-20", "2020-21", "2021-22", "2022-23"]
    prev_next = {s: [ORDER[i - 1] if i > 0 else None,
                     ORDER[i + 1] if i < len(ORDER) - 1 else None]
                 for i, s in enumerate(ORDER)}

    results = {}
    for target in ("offense", "defense"):
        y = d[TARGETS[target]].astype(np.float64)
        Xf = FEATS[target]
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = max(tuned[target]["rounds"] // 3, 150)

        # player -> season -> label, from training-season rows only (labels of
        # the test seasons must not shape selection)
        lab = {}
        m_lab = tr & ~np.isin(seasons, list(TEST_SEASONS)) & np.isfinite(y)
        for i in np.where(m_lab)[0]:
            lab.setdefault(players[i], {})[seasons[i]] = y[i]

        y_stab = np.full(len(y), np.nan)
        for i in np.where(tr)[0]:
            neigh = [lab.get(players[i], {}).get(s)
                     for s in prev_next.get(seasons[i], [None, None]) if s]
            neigh = [v for v in neigh if v is not None]
            if neigh:
                y_stab[i] = float(np.mean(neigh))
        m_stab = tr & np.isfinite(y_stab)
        print(f"\n=== {target}: {int(m_stab.sum())}/{int(tr.sum())} training "
              f"rows have a stability target ===", flush=True)

        # selection fits
        gain = {}
        for name, mask, yy in (("stab", m_stab, y_stab), ("curr", tr, y)):
            m = lgb.train(dict(params, seed=0),
                          lgb.Dataset(Xf[mask], yy[mask]),
                          num_boost_round=rounds)
            gain[name] = np.asarray(m.feature_importance("gain"))
            print(f"  selection fit vs y_{name}: "
                  f"{int((gain[name] > 0).sum())} features used", flush=True)
        # how different are the two rankings?
        top_stab = set(np.argsort(-gain["stab"])[:400])
        top_curr = set(np.argsort(-gain["curr"])[:400])
        print(f"  top-400 overlap stab vs curr: "
              f"{len(top_stab & top_curr)}/400", flush=True)

        def report(name, p_te):
            s = score_cells(y[test], p_te, cells_te)
            print(f"  {name:<22} dev@10={s['dev@10']:5.2f} "
                  f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f} "
                  f"tau@20={s['tau@20']:+.3f} MAE={s['mae']:.3f} "
                  f"hits@10={s['hits@10']}/20", flush=True)
            return {k: (int(v) if isinstance(v, (int, np.integer))
                        else round(float(v), 4)) for k, v in s.items()}

        res = {}
        med = np.nanmedian(Xf[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        res["baseline-all"] = report(
            f"baseline ({Xf.shape[1]} feats)",
            blend(Xf[tr], y[tr], Xf[test], med, params, rounds))
        for K in KS:
            for name in ("stab", "curr"):
                cols = np.sort(np.argsort(-gain[name])[:K])
                Xs = Xf[:, cols]
                ms = np.nanmedian(Xs[tr], axis=0)
                ms = np.where(np.isfinite(ms), ms, 0.0)
                res[f"{name}-top{K}"] = report(
                    f"{name}-select top-{K}",
                    blend(Xs[tr], y[tr], Xs[test], ms, params, rounds))
        results[target] = res

    Path(TD / "RESULTS_stability_select.json").write_text(
        json.dumps(results, indent=1))
    print("\nwrote RESULTS_stability_select.json", flush=True)


if __name__ == "__main__":
    main()
