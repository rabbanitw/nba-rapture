"""Per-component bake-off: the four most granular published labels -- rap_box_o,
rap_box_d, rap_onoff_o, rap_onoff_d -- each get their own architecture search,
because nothing says the best model for a near-deterministic box recipe and the
best model for a noisy on/off regression are the same model.

Protocol: the branch's 10-fold season-held-out CV (train on 9 full-season
stamp cells, test on the held-out one), sqrt-minutes weights, all arms scored
against the component label itself: spearman rho over the full labeled test
cell, MAE, and dev@10/tau@10/hits@10 over the >=1065-minute pool ranked by the
true component.

Arms per component:

  ridge-struct   RidgeCV on the structural variable block (cell-relative).
                 This is the production hat (hats3): OB/DB for box,
                 courtmate-chain [on, cw, cc] for on/off. The baseline.
  ridge-wide     RidgeCV on the wide masked raw matrix, standardized.
  gbm-struct     LightGBM on the structural block alone.
  gbm-wide       LightGBM on the wide masked raw matrix (box: box-masked X +
                 shotdash + Z; onoff: onoff-masked X + opponent per-100 block +
                 courtmate + onoff2; defense box adds the defend block).
  gbm-wide+hat   gbm-wide + the structural block + an OOF ridge-hat column
                 (stacking, hat out-of-fold on train rows, refit for test).
  blend          0.5 * ridge-struct + 0.5 * gbm-wide+hat.
  mlp-struct     small torch MLP on the structural block, 2 seeds.
  gbm-aug        gbm-wide+hat trained with mid-season labeled cells of the
                 TRAIN seasons added (weight sqrt(mp)/n_cells_of_season).

Run:  python training/raptor2/cv_components.py [--comps box_o onoff_d ...]
"""

import argparse
import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scipy.stats import spearmanr
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, masks_for
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import per100
from experiment_topk_rank import dev_at_k, hits_at_k, tau_at_k
from predict_seasons import DROP_FEATURES
from variables import build_variables
from variables2 import build_onoff2
from structural import cell_relative

TD = REPO_ROOT / "training"
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}
ARMS = ["ridge-struct", "ridge-wide", "gbm-struct", "gbm-wide",
        "gbm-wide+hat", "blend", "mlp-struct", "gbm-aug"]


def ridge_pred(Xtr, ytr, wtr, Xte, standardize=False):
    med = np.nanmedian(Xtr, axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    if standardize:
        mu, sd = A.mean(0), A.std(0)
        sd[sd == 0] = 1.0
        A, B = (A - mu) / sd, (B - mu) / sd
    m = RidgeCV(alphas=np.logspace(-3, 5, 33)).fit(A, ytr, sample_weight=wtr)
    return m.predict(B)


def gbm_pred(Xtr, ytr, wtr, Xte, params, rounds, seeds=(0,)):
    ps = []
    for s in seeds:
        p = dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s)
        ps.append(lgb.train(p, lgb.Dataset(Xtr, ytr, weight=wtr),
                            num_boost_round=rounds).predict(Xte))
    return np.mean(ps, axis=0)


def mlp_pred(Xtr, ytr, wtr, Xte, seeds=(0, 1), epochs=300):
    import torch
    med = np.nanmedian(Xtr, axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    A, B = (A - mu) / sd, (B - mu) / sd
    ysd = ytr.std() or 1.0
    ymu = ytr.mean()
    w = (wtr / wtr.mean()).astype(np.float32)
    At = torch.tensor(A, dtype=torch.float32)
    Bt = torch.tensor(B, dtype=torch.float32)
    yt = torch.tensor((ytr - ymu) / ysd, dtype=torch.float32)
    wt = torch.tensor(w)
    preds = []
    for s in seeds:
        torch.manual_seed(s)
        net = torch.nn.Sequential(
            torch.nn.Linear(A.shape[1], 64), torch.nn.ReLU(),
            torch.nn.Dropout(0.15),
            torch.nn.Linear(64, 32), torch.nn.ReLU(),
            torch.nn.Linear(32, 1))
        opt = torch.optim.Adam(net.parameters(), lr=1e-3, weight_decay=1e-4)
        n = len(yt)
        for ep in range(epochs):
            perm = torch.randperm(n)
            for i in range(0, n, 256):
                idx = perm[i:i + 256]
                opt.zero_grad()
                out = net(At[idx]).squeeze(-1)
                loss = (wt[idx] * (out - yt[idx]) ** 2).mean()
                loss.backward()
                opt.step()
        with torch.no_grad():
            preds.append(net(Bt).squeeze(-1).numpy() * ysd + ymu)
    return np.mean(preds, axis=0)


def score(yte, p, mpte):
    el = mpte >= FLOOR
    return {"rho": float(spearmanr(yte, p).statistic),
            "mae": float(np.mean(np.abs(yte - p))),
            "dev@10": dev_at_k(yte[el], p[el], 10),
            "tau@10": tau_at_k(yte[el], p[el], 10),
            "hits@10": hits_at_k(yte[el], p[el], 10)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--comps", nargs="*",
                    default=["box_o", "box_d", "onoff_o", "onoff_d"])
    ap.add_argument("--arms", nargs="*", default=ARMS)
    ap.add_argument("--seasons", nargs="*", default=list(STAMPS),
                    help="subset of held-out seasons (smoke tests)")
    ap.add_argument("--out-tag", default="")
    args = ap.parse_args()
    stamps_run = {s: STAMPS[s] for s in args.seasons}

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    oppz = np.load(TD / "data_fixed" / "wowyopp.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(TD / "raptor2" / "courtmate.npz")["CM"]
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    ofields = [str(f) for f in oppz["fields"]]
    on100, off100 = per100(oppz["on_X"], ofields), per100(oppz["off_X"], ofields)
    Bopp = np.hstack([on100, off100, on100 - off100]).astype(np.float32)
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OFF2, DEF2 = build_onoff2(X, feat, oppz["on_X"], oppz["off_X"], ofields,
                              cells, mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OO2o = cell_relative(OFF2, cells, mp)
    OO2d = cell_relative(DEF2, cells, mp)
    OO3o = cell_relative(cm[:, [0, 2, 4]], cells, mp)
    OO3d = cell_relative(-cm[:, [1, 3, 5]], cells, mp)
    box_mask, onoff_mask = masks_for(feat)
    sdR = sd["R"].astype(np.float32)
    dfzE = dfz["E"].astype(np.float32)

    y_any = d["y_off"].astype(np.float64)
    rs = d["season_type"] == "Regular season"
    stamps = list(STAMPS.values())
    labeled = rs & np.isin(d["timestamp"], stamps) & np.isfinite(y_any)
    midseason = rs & ~np.isin(d["timestamp"], stamps) & np.isfinite(y_any)
    seasons = np.array([str(s) for s in d["season"]])
    # snapshots per season, for downweighting augmentation rows
    k_cells = {s: max(len(np.unique(d["timestamp"][midseason
                                                   & (seasons == s)])), 1)
               for s in np.unique(seasons[midseason])}
    w = np.sqrt(np.maximum(mp, 1.0))
    players = np.array([str(p) for p in d["player"]])
    tuned = json.loads((TD / "tuned_params.json").read_text())

    wide_box_o = np.hstack([X[:, box_mask], sdR, Z]).astype(np.float32)
    wide_box_d = np.hstack([X[:, box_mask], sdR, dfzE, Z]).astype(np.float32)
    wide_onoff = np.hstack([X[:, onoff_mask], Bopp,
                            np.where(np.isfinite(cm), cm, np.nan),
                            OO2o, OO2d]).astype(np.float32)
    COMP = {
        "box_o": dict(lab="rap_box_o", struct=OB, wide=wide_box_o,
                      side="offense"),
        "box_d": dict(lab="rap_box_d", struct=DB, wide=wide_box_d,
                      side="defense"),
        "onoff_o": dict(lab="rap_onoff_o", struct=OO3o, wide=wide_onoff,
                        side="offense"),
        "onoff_d": dict(lab="rap_onoff_d", struct=OO3d, wide=wide_onoff,
                        side="defense"),
    }

    results = {}
    preds_store = {}
    for tag in args.comps:
        cfg = COMP[tag]
        yv = comp[cfg["lab"]].astype(np.float64)
        params = dict(tuned[cfg["side"]]["params"], verbose=-1)
        rounds = max(tuned[cfg["side"]]["rounds"] // 3, 150)
        S, W = cfg["struct"], cfg["wide"]
        SW = np.hstack([W, S]).astype(np.float32)
        print(f"\n=== {tag} (label {cfg['lab']}, struct {S.shape[1]} cols, "
              f"wide {W.shape[1]} cols, rounds {rounds}) ===", flush=True)
        results[tag] = {}
        for season, stamp in stamps_run.items():
            te = labeled & (d["timestamp"] == stamp)
            trn = labeled & (d["timestamp"] != stamp) & (seasons != season)
            ti = np.where(te)[0]
            yte, mpte = yv[te], mp[te]
            row = {}
            arm_preds = {}

            def rec(name, pv):
                arm_preds[name] = pv
                row[name] = score(yte, pv, mpte)

            p_ridge = ridge_pred(S[trn], yv[trn], w[trn], S[te])
            if "ridge-struct" in args.arms:
                rec("ridge-struct", p_ridge)
            if "ridge-wide" in args.arms:
                rec("ridge-wide", ridge_pred(W[trn], yv[trn], w[trn], W[te],
                                             standardize=True))
            if "gbm-struct" in args.arms:
                rec("gbm-struct", gbm_pred(S[trn], yv[trn], w[trn], S[te],
                                           params, rounds))
            if "gbm-wide" in args.arms:
                rec("gbm-wide", gbm_pred(W[trn], yv[trn], w[trn], W[te],
                                         params, rounds))

            p_stack = None
            if {"gbm-wide+hat", "blend", "gbm-aug"} & set(args.arms):
                # OOF ridge hat on train rows (grouped by player), refit for test
                hat = np.full(len(yv), np.nan)
                tri = np.where(trn)[0]
                for a, b in GroupKFold(n_splits=3).split(
                        tri, groups=players[tri]):
                    hat[tri[b]] = ridge_pred(S[tri[a]], yv[tri[a]], w[tri[a]],
                                             S[tri[b]])
                hat[te] = p_ridge
                SWH = np.hstack([SW, hat.reshape(-1, 1)]).astype(np.float32)
                p_stack = gbm_pred(SWH[trn], yv[trn], w[trn], SWH[te],
                                   params, rounds)
                if "gbm-wide+hat" in args.arms:
                    rec("gbm-wide+hat", p_stack)
                if "blend" in args.arms:
                    rec("blend", 0.5 * p_ridge + 0.5 * p_stack)
                if "gbm-aug" in args.arms:
                    aug = midseason & (seasons != season)
                    trn2 = trn | aug
                    w2 = w.copy()
                    for s2, k in k_cells.items():
                        w2[midseason & (seasons == s2)] /= k
                    rec("gbm-aug", gbm_pred(SWH[trn2], yv[trn2], w2[trn2],
                                            SWH[te], params, rounds))
            if "mlp-struct" in args.arms:
                rec("mlp-struct", mlp_pred(S[trn], yv[trn], w[trn], S[te]))

            results[tag][season] = row
            preds_store[f"{tag}|{season}|idx"] = ti
            for aname, pv in arm_preds.items():
                preds_store[f"{tag}|{season}|{aname}"] = pv
            best = min(row, key=lambda a: row[a]["dev@10"])
            print(f"  {season}: " + "  ".join(
                f"{a}: rho {row[a]['rho']:+.3f} dev {row[a]['dev@10']:5.2f}"
                for a in row) + f"   [best dev: {best}]", flush=True)

        print(f"\n-- {tag} summary --", flush=True)
        arms_here = list(next(iter(results[tag].values())).keys())
        for a in arms_here:
            rhos = [results[tag][s][a]["rho"] for s in stamps_run]
            devs = [results[tag][s][a]["dev@10"] for s in stamps_run]
            hits = sum(results[tag][s][a]["hits@10"] for s in stamps_run)
            wins_rho = sum(results[tag][s][a]["rho"]
                           > results[tag][s]["ridge-struct"]["rho"]
                           for s in stamps_run)
            wins_dev = sum(results[tag][s][a]["dev@10"]
                           < results[tag][s]["ridge-struct"]["dev@10"]
                           for s in stamps_run)
            print(f"  {a:<13} rho med {np.median(rhos):+.3f} mean "
                  f"{np.mean(rhos):+.3f} | dev@10 med {np.median(devs):5.2f} "
                  f"mean {np.mean(devs):5.2f} | hits@10 {hits}/100 | "
                  f"vs ridge: rho {wins_rho}/{len(stamps_run)} dev {wins_dev}/{len(stamps_run)}", flush=True)

    Path(TD / "raptor2" / f"RESULTS_cv_components{args.out_tag}.json").write_text(
        json.dumps(results, indent=1))
    np.savez_compressed(TD / "raptor2" / f"cv_components_preds{args.out_tag}.npz",
                        **preds_store)
    print("\nwrote raptor2/RESULTS_cv_components.json + cv_components_preds.npz",
          flush=True)


if __name__ == "__main__":
    main()
