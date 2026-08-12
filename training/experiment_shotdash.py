"""Do the shot-dashboard features (538's spacing + time-of-possession inputs)
move the offense model?

Arms on identical splits, scored on the held-out test seasons (2013-14/2014-15
RS) with the standard metrics:
  baseline    X + Z + Eopp (production offense features, 1173 cols)
  +sd-eng     baseline + 9 engineered cols (covered3pa per 538's 100/80/57/31
              weights, tight/wide-open shares, time-of-poss per 36, touch rates)
  +sd-all     baseline + engineered + 75 raw defender-distance/possession cols

Defense gets one arm too (+sd-eng): time-of-possession and covered-3 shares are
offense-flavored, but held shots and touch tempo could carry role information.

Run:  python training/experiment_shotdash.py
"""

import json

import numpy as np

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend, engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
TD = REPO_ROOT / "training"


def main():
    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    opp = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    Eopp, _ = engineered(opp["on_X"], opp["off_X"],
                         [str(f) for f in opp["fields"]], cells_all)
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    cells_te = np.array([str(s) for s in d["season"][test]])
    tuned = json.loads((TD / "tuned_params.json").read_text())

    ARMS = {
        "offense": [("baseline", [X, Z, Eopp]),
                    ("+sd-eng", [X, Z, Eopp, sd["E"]]),
                    ("+sd-all", [X, Z, Eopp, sd["E"], sd["R"]])],
        "defense": [("baseline", [X, Z, dfz["E"]]),
                    ("+sd-eng", [X, Z, dfz["E"], sd["E"]])],
    }
    out = {}
    for target, arms in ARMS.items():
        y = d[TARGETS[target]].astype(np.float64)
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = max(tuned[target]["rounds"] // 3, 150)
        print(f"\n=== {target} ===", flush=True)
        for name, blocks in arms:
            Xf = np.hstack(blocks)
            med = np.nanmedian(Xf[tr], axis=0)
            med = np.where(np.isfinite(med), med, 0.0)
            p = blend(Xf[tr], y[tr], Xf[test], med, params, rounds)
            s = score_cells(y[test], p, cells_te)
            out[f"{target}|{name}"] = {k: (int(v) if isinstance(v, (int, np.integer))
                                           else round(float(v), 4))
                                       for k, v in s.items()}
            print(f"  {name:<10} ({Xf.shape[1]:>4} cols) "
                  f"dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
                  f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
                  f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20", flush=True)
    (TD / "RESULTS_shotdash.json").write_text(json.dumps(out, indent=1))
    print("\nwrote RESULTS_shotdash.json", flush=True)


if __name__ == "__main__":
    main()
