"""The structural RAPTOR reproduction: 538's variables, 538's architecture.

Two regressions per side fit on 538's OWN component labels (they fit on
long-term RAPM; we have the component outputs row-aligned, which is the
stronger calibration target for reproducing them):

  box_o     ridge( OB variables  -> rap_box_o )
  onoff_o   ridge( OO[offense]   -> rap_onoff_o )
  box_d     ridge( DB variables  -> rap_box_d )
  onoff_d   ridge( OO[defense]   -> rap_onoff_d )

then overall = 0.85 * box + 0.21 * onoff (their published blend), plus a
learned-blend arm. Variables are league-relative per cell (minute-weighted),
as the document specifies. Coefficients are printed with names so signs can be
compared against the published ones (steals +1.49, charges +2.28, d2 weights,
no blocks, contested > uncontested rebounds, iso turnovers negative...).

Evaluation: held-out test seasons 2013-14/2014-15 RS (>=1065 pool and full),
standard metrics; then 10-fold season-held-out CV in cv_structural.py.

Run:  python training/raptor2/structural.py
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
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from variables import BOX_W, ONOFF_W, build_variables

TD = REPO_ROOT / "training"
RS_MIN, PO_MIN = 50, 10
FLOOR = 1065


def cell_relative(V, cells, mp):
    """Minute-weighted league-relative standardization per cell."""
    out = np.full_like(V, np.nan, dtype=np.float64)
    for c in np.unique(cells):
        m = cells == c
        w = np.sqrt(np.maximum(mp[m], 1.0))
        for j in range(V.shape[1]):
            v = V[m, j]
            ok = np.isfinite(v)
            if ok.sum() < 20:
                continue
            mu = np.average(v[ok], weights=w[ok])
            sd = np.sqrt(np.average((v[ok] - mu) ** 2, weights=w[ok]))
            out[np.where(m)[0][ok], j] = (v[ok] - mu) / (sd if sd > 0 else 1.0)
    return out


def ridge_fit(Vtr, ytr, wtr, Vte, names, tag):
    med = np.nanmedian(Vtr, axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Vtr), Vtr, med)
    B = np.where(np.isfinite(Vte), Vte, med)
    model = RidgeCV(alphas=np.logspace(-3, 3, 25)).fit(A, ytr,
                                                       sample_weight=wtr)
    order = np.argsort(-np.abs(model.coef_))
    print(f"  [{tag}] alpha={model.alpha_:.3g}  top coefficients:", flush=True)
    for j in order[:8]:
        print(f"      {names[j]:<20} {model.coef_[j]:+.3f}", flush=True)
    return model.predict(A), model.predict(B), model


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    mp = d["mp"].astype(np.float64)
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OO = cell_relative(V["OO"], cells, mp)
    OOo, OOd = OO[:, :4], OO[:, 4:]
    for tag, M in (("OB", OB), ("DB", DB), ("OO", OO)):
        print(f"{tag}: {M.shape[1]} vars, coverage "
              f"{np.isfinite(M).mean():.2f}", flush=True)

    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    w = np.sqrt(np.maximum(mp, 1.0))
    cells_te = np.array([str(s) for s in d["season"][test]])
    el = mp[test] >= FLOOR

    hats_tr, hats_te = {}, {}
    for tag, M, labname in (("box_o", OB, "rap_box_o"),
                            ("onoff_o", OOo, "rap_onoff_o"),
                            ("box_d", DB, "rap_box_d"),
                            ("onoff_d", OOd, "rap_onoff_d")):
        yv = comp[labname]
        m = tr & np.isfinite(yv)
        names = (V["OB_NAMES"] if tag == "box_o" else
                 V["DB_NAMES"] if tag == "box_d" else
                 V["OO_NAMES"][:4] if tag == "onoff_o" else V["OO_NAMES"][4:])
        ptr, pte, _ = ridge_fit(M[m], yv[m], w[m], M[test], names, tag)
        htr = np.full(len(mp), np.nan)
        htr[m] = ptr
        hats_tr[tag], hats_te[tag] = htr, pte
        ok = np.isfinite(yv[m])
        from scipy.stats import spearmanr
        print(f"      fit rho vs {labname}: "
              f"{spearmanr(ptr, yv[m]).statistic:+.3f}", flush=True)

    results = {}
    for side, box, onoff, lab in (("offense", "box_o", "onoff_o", "offense"),
                                  ("defense", "box_d", "onoff_d", "defense")):
        y = d[TARGETS[lab]].astype(np.float64)
        p_fixed = BOX_W * hats_te[box] + ONOFF_W * hats_te[onoff]
        m = tr & np.isfinite(hats_tr[box]) & np.isfinite(hats_tr[onoff]) \
            & np.isfinite(y)
        D = np.column_stack([hats_tr[box][m], hats_tr[onoff][m]])
        cb = RidgeCV(alphas=np.logspace(-3, 3, 13)).fit(D, y[m],
                                                        sample_weight=w[m])
        p_learn = cb.predict(np.column_stack([hats_te[box], hats_te[onoff]]))
        print(f"[{side}] learned blend weights: box={cb.coef_[0]:.3f} "
              f"onoff={cb.coef_[1]:.3f} (538: 0.85/0.21)", flush=True)
        for arm, p in (("fixed-0.85/0.21", p_fixed), ("learned-blend", p_learn)):
            for pool, msk in (("el", el), ("full", np.ones(len(p), bool))):
                s = score_cells(y[test][msk], p[msk], cells_te[msk])
                results[f"{side}|{arm}|{pool}"] = {
                    k: (int(v) if isinstance(v, (int, np.integer))
                        else round(float(v), 4)) for k, v in s.items()}
                if pool == "el":
                    print(f"  {arm:<16} dev@10={s['dev@10']:5.2f} "
                          f"dev@20={s['dev@20']:5.2f} tau@10={s['tau@10']:+.3f}"
                          f" MAE={s['mae']:.3f} hits@10={s['hits@10']}/20",
                          flush=True)

    np.savez_compressed(TD / "raptor2" / "structural_vars.npz",
                        OB=OB, DB=DB, OO=OO,
                        OB_NAMES=V["OB_NAMES"], DB_NAMES=V["DB_NAMES"],
                        OO_NAMES=V["OO_NAMES"])
    Path(TD / "raptor2" / "RESULTS_structural.json").write_text(
        json.dumps(results, indent=1))
    print("\nwrote raptor2/structural_vars.npz + RESULTS_structural.json",
          flush=True)


if __name__ == "__main__":
    main()
