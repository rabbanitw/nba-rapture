"""Defense wing-vs-big bias: position-aware arms on the production stack.

RESULTS_def_rank_outliers.md: the defense projection under-ranks wings whose
538 box-D comes from ball pressure / matchup suppression and over-ranks
contest-volume bigs; the miss sits mostly in the struct box-D hat. Position
is already a GBM input (ctx|pos_* multi-hot), so these arms change the
*structure* instead. Every arm = production defense (cv_def_robust base:
gbm + exact NaN-propagating hats3, huber, blend 3 seeds + ridge) with one
change:

  base        no change (reproduces RESULTS_hats3_cv.json)
  ghat_bd     box_d hat fit separately per group (big / non-big)
  ghat        box_d AND onoff_d hats fit per group
  prelhat     box_d hat on position-relative DB (standardized within
              cell x group) + big flag, replacing the global box_d hat
  prelf       + position-relative DB and hustle columns as GBM features
  split       separate GBM+ridge blend per group (hats global)
  splitblend  0.5 * base + 0.5 * split
  prelf_sb    0.5 * prelf + 0.5 * (prelf features, per-group GBMs)
  cov_full    hustle-era test seasons (2015-16+): 0.5 * base + 0.5 * a GBM
              trained only on hustle-era rows with all 23 hustle columns
              raw + position-relative added; earlier seasons = base
  cov_slim    same, but the hustle-era GBM sees ~80 focused columns (hats,
              DB raw/cell/position-relative, hustle raw/position-relative,
              on/off-d block, positions) so the matchup columns are not
              drowned among 1,100+
  prel3       prelf with 3 groups (guard <=2, wing, big >=4)
  prelc       prelf with continuous position adjustment: per cell, each
              column minus its minutes-weighted quadratic fit on bigness,
              scaled by the cell residual sd (no hard SF/PF threshold)
  prelfx      prelf + position-relative defend-dash (8) and on/off-d (3)
  prelc_sb    0.5 * prelc + 0.5 * (prelc features, per-group GBMs)
  prelfx_sb   0.5 * prelfx + 0.5 * (prelfx features, per-group GBMs)
  psb_cov     hustle-era test seasons: 0.5 * prelf_sb + 0.5 * the cov_full
              hustle-era GBM; earlier seasons = prelf_sb

Group: bigness = mean of multi-hot position indices (PG=1 .. C=5);
big = bigness >= 4 (PF, PF/C, C).

Besides dev@10/dev@20 each arm reports the bias the arms target, on the
>=1065 pool per season: under = actual top-20 projected 20+ ranks worse,
over = projected top-20 actually 20+ ranks worse, and the big share of the
projected vs actual top-20.

Run:  python training/raptor2/cv_def_wingbig.py --seedbase 0 --arms base split
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import REPO_ROOT
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import blend
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS
from structural import cell_relative
from variables import build_variables
from structural2 import ridge_hat

TD = REPO_ROOT / "training"
HERE = TD / "raptor2"
FLOOR = 1065
STAMPS = {"2013-14": "20140715000000", "2014-15": "20150715000000",
          "2015-16": "20160715000000", "2016-17": "20170715000000",
          "2017-18": "20180715000000", "2018-19": "20190715000000",
          "2019-20": "20201101000000", "2020-21": "20210801000000",
          "2021-22": "20220715000000", "2022-23": "20230715000000"}
ALL_ARMS = ["base", "ghat_bd", "ghat", "prelhat", "prelf", "split",
            "splitblend", "prelf_sb", "cov_full", "cov_slim", "prel3",
            "prelc", "prelfx", "prelc_sb", "prelfx_sb", "psb_cov"]
HUSTLE_FROM = STAMPS["2015-16"]
HU_COLS = ("deflections_per36", "charges_per36", "contested2_per36",
           "rim_dfga_per36", "rim_plusminus", "ov_plusminus",
           "mu_pts_per100", "mu_efg_allowed")


def ranks(v):
    r = np.empty(len(v), dtype=int)
    r[np.argsort(-v)] = np.arange(1, len(v) + 1)
    return r


def bias_stats(y, p, big, k=20, gap=20):
    ra, rp = ranks(y), ranks(p)
    under = (ra <= k) & (rp - ra >= gap)
    over = (rp <= k) & (ra - rp >= gap)
    return {"under": int(under.sum()), "over": int(over.sum()),
            "under_big": int((under & big).sum()),
            "over_big": int((over & big).sum()),
            "big_top_pred": int((big & (rp <= k)).sum()),
            "big_top_act": int((big & (ra <= k)).sum())}


def bigness_relative(V, cells, mp, b):
    """Per cell: column minus its sqrt(mp)-weighted quadratic fit on
    bigness, divided by the weighted residual sd."""
    out = np.full_like(V, np.nan, dtype=np.float64)
    B = np.column_stack([np.ones_like(b), b, b * b])
    for c in np.unique(cells):
        m = np.where(cells == c)[0]
        w = np.sqrt(np.maximum(mp[m], 1.0))
        for j in range(V.shape[1]):
            v = V[m, j]
            ok = np.isfinite(v)
            if ok.sum() < 20:
                continue
            sw = np.sqrt(w[ok])[:, None]
            coef = np.linalg.lstsq(B[m][ok] * sw, v[ok] * sw[:, 0],
                                   rcond=None)[0]
            r = v[ok] - B[m][ok] @ coef
            sd = np.sqrt(np.average(r ** 2, weights=w[ok]))
            out[m[ok], j] = r / (sd if sd > 0 else 1.0)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arms", nargs="*", default=ALL_ARMS)
    ap.add_argument("--seedbase", type=int, default=0)
    args = ap.parse_args()
    seeds = (args.seedbase, args.seedbase + 1, args.seedbase + 2)

    X, feat, d = prepare(str(TD / "data_fixed"))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    sd = np.load(TD / "data_fixed" / "shotdash.npz", allow_pickle=True)
    dfz = np.load(TD / "data_fixed" / "defend.npz", allow_pickle=True)
    hz = np.load(TD / "data_fixed" / "hustle.npz", allow_pickle=True)
    comp = np.load(TD / "data_fixed" / "components.npz")
    cm = np.load(HERE / "courtmate.npz")["CM"]
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, sd["R"], [str(x) for x in sd["rnames"]],
                        dfz["E"], [str(x) for x in dfz["enames"]], mp)
    OB = cell_relative(V["OB"], cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OO3o = cell_relative(cm[:, [0, 2, 4]], cells, mp)
    OO3d = cell_relative(-cm[:, [1, 3, 5]], cells, mp)
    BLOCKS = {"box_o": (OB, "rap_box_o"), "onoff_o": (OO3o, "rap_onoff_o"),
              "box_d": (DB, "rap_box_d"), "onoff_d": (OO3d, "rap_onoff_d")}
    HT = ("box_o", "onoff_o", "box_d", "onoff_d")

    P = X[:, [feat.index(f"ctx|pos_{p}") for p in
              ("PG", "SG", "SF", "PF", "C")]]
    P = np.nan_to_num(P)
    bigness = (P @ np.arange(1, 6)) / np.maximum(P.sum(1), 1)
    big = bigness >= 4.0
    gcells = np.array([f"{c}|{int(b)}" for c, b in zip(cells, big)])
    DBg = cell_relative(V["DB"], gcells, mp)
    DBg_hat = np.column_stack([DBg, big.astype(float)])
    enames = [str(x) for x in hz["enames"]]
    HUg = cell_relative(hz["E"][:, [enames.index(c) for c in HU_COLS]]
                        .astype(np.float64), gcells, mp)
    HUE = hz["E"].astype(np.float64)
    HUEg = cell_relative(HUE, gcells, mp)
    SLIM = np.column_stack([V["DB"], DB, DBg, HUE, HUEg, OO3d, P, bigness])
    hustle_era = d["timestamp"] >= HUSTLE_FROM
    HU8 = hz["E"][:, [enames.index(c) for c in HU_COLS]].astype(np.float64)
    grp3 = np.where(bigness >= 4.0, 2, np.where(bigness <= 2.0, 0, 1))
    g3cells = np.array([f"{c}|{g}" for c, g in zip(cells, grp3)])
    PREL3 = np.column_stack([cell_relative(V["DB"], g3cells, mp),
                             cell_relative(HU8, g3cells, mp)])
    PRELC = bigness_relative(np.column_stack([V["DB"], HU8]), cells, mp,
                             bigness)
    PRELX = np.column_stack([cell_relative(dfz["E"].astype(np.float64),
                                           gcells, mp),
                             cell_relative(-cm[:, [1, 3, 5]], gcells, mp)])

    w = np.sqrt(np.maximum(mp, 1.0))
    rs = d["season_type"] == "Regular season"
    tuned = json.loads((TD / "tuned_params.json").read_text())
    y = d[TARGETS["defense"]].astype(np.float64)
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = max(tuned["defense"]["rounds"] // 3, 150)
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values())) \
        & np.isfinite(y)
    Xf = np.hstack([X, Z, dfz["E"]])
    ref = json.loads((HERE / "RESULTS_hats3_cv.json").read_text())["defense"]

    def hat(M, labname, fit_mask, apply_mask=None):
        yv = comp[labname]
        m = fit_mask & np.isfinite(yv) & np.isfinite(M).all(axis=1)
        h = ridge_hat(M[m], yv[m], w[m], M, [""] * M.shape[1], "",
                      quiet=True)
        h[~np.isfinite(M).all(axis=1)] = np.nan
        if apply_mask is not None:
            h[~apply_mask] = np.nan
        return h

    def gbm(Xa, trn, te):
        med = np.nanmedian(Xa[trn], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        return blend(Xa[trn], y[trn], Xa[te], med, params, rounds, seeds)

    def group_hat(tag, trn):
        M, labname = BLOCKS[tag]
        h = np.full(len(y), np.nan)
        for g in (big, ~big):
            hg = hat(M, labname, trn & g)
            h[g] = hg[g]
        return h

    per = {a: {} for a in args.arms}
    oof = {a: np.full(len(y), np.nan) for a in args.arms}
    for season, stamp in STAMPS.items():
        te = labeled & (d["timestamp"] == stamp)
        trn = labeled & (d["timestamp"] != stamp)
        elm = mp[te] >= FLOOR
        hf = {t: hat(*BLOCKS[t], trn) for t in HT}
        H = np.column_stack([hf[t] for t in HT])
        Xa = np.hstack([Xf, H])
        preds = {}
        need_base = {"base", "splitblend", "cov_full", "cov_slim"} \
            & set(args.arms)
        if need_base:
            preds["base"] = gbm(Xa, trn, te)
        if {"split", "splitblend"} & set(args.arms):
            ps = np.full(int(te.sum()), np.nan)
            for g in (big, ~big):
                ps[g[te]] = gbm(Xa, trn & g, te & g)
            preds["split"] = ps
        if "splitblend" in args.arms:
            preds["splitblend"] = 0.5 * preds["base"] + 0.5 * preds["split"]
        if "ghat_bd" in args.arms:
            H2 = H.copy()
            H2[:, 2] = group_hat("box_d", trn)
            preds["ghat_bd"] = gbm(np.hstack([Xf, H2]), trn, te)
        if "ghat" in args.arms:
            H2 = H.copy()
            H2[:, 2] = group_hat("box_d", trn)
            H2[:, 3] = group_hat("onoff_d", trn)
            preds["ghat"] = gbm(np.hstack([Xf, H2]), trn, te)
        if "prelhat" in args.arms:
            H2 = H.copy()
            H2[:, 2] = hat(DBg_hat, "rap_box_d", trn)
            preds["prelhat"] = gbm(np.hstack([Xf, H2]), trn, te)
        if {"prelf", "prelf_sb", "psb_cov"} & set(args.arms):
            Xp = np.hstack([Xa, DBg, HUg])
            preds["prelf"] = gbm(Xp, trn, te)
        if {"prelf_sb", "psb_cov"} & set(args.arms):
            ps = np.full(int(te.sum()), np.nan)
            for g in (big, ~big):
                ps[g[te]] = gbm(Xp, trn & g, te & g)
            preds["prelf_sb"] = 0.5 * preds["prelf"] + 0.5 * ps
        if "prel3" in args.arms:
            preds["prel3"] = gbm(np.hstack([Xa, PREL3]), trn, te)
        if {"prelfx", "prelfx_sb"} & set(args.arms):
            Xx = np.hstack([Xa, DBg, HUg, PRELX])
            preds["prelfx"] = gbm(Xx, trn, te)
        if "prelfx_sb" in args.arms:
            ps = np.full(int(te.sum()), np.nan)
            for g in (big, ~big):
                ps[g[te]] = gbm(Xx, trn & g, te & g)
            preds["prelfx_sb"] = 0.5 * preds["prelfx"] + 0.5 * ps
        if {"prelc", "prelc_sb"} & set(args.arms):
            Xq = np.hstack([Xa, PRELC])
            preds["prelc"] = gbm(Xq, trn, te)
        if "prelc_sb" in args.arms:
            ps = np.full(int(te.sum()), np.nan)
            for g in (big, ~big):
                ps[g[te]] = gbm(Xq, trn & g, te & g)
            preds["prelc_sb"] = 0.5 * preds["prelc"] + 0.5 * ps
        for arm, Xc in (("cov_full", lambda: np.hstack([Xa, HUE, HUEg])),
                        ("cov_slim", lambda: np.hstack([H, SLIM]))):
            if arm not in args.arms:
                continue
            if stamp < HUSTLE_FROM:
                preds[arm] = preds["base"]
                continue
            pc = gbm(Xc(), trn & hustle_era, te)
            preds[arm] = 0.5 * preds["base"] + 0.5 * pc
        if "psb_cov" in args.arms:
            if stamp < HUSTLE_FROM:
                preds["psb_cov"] = preds["prelf_sb"]
            else:
                pc = gbm(np.hstack([Xa, HUE, HUEg]), trn & hustle_era, te)
                preds["psb_cov"] = 0.5 * preds["prelf_sb"] + 0.5 * pc

        yt, bt = y[te][elm], big[te][elm]
        for arm in args.arms:
            p = preds[arm]
            oof[arm][np.where(te)[0]] = p
            s = score_cells(yt, p[elm], np.full(int(elm.sum()), season))
            b = bias_stats(yt, p[elm], bt)
            per[arm][season] = {"dev@10": round(s["dev@10"], 2),
                                "dev@20": round(s["dev@20"], 2),
                                "tau@10": round(s["tau@10"], 3), **b}
            print(f"[{arm}/s{args.seedbase}] {season}: dev@10="
                  f"{s['dev@10']:.2f} dev@20={s['dev@20']:.2f} "
                  f"under={b['under']} over={b['over']} bigs top20 "
                  f"{b['big_top_pred']}/{b['big_top_act']} "
                  f"(base {ref['per_season'][season]})", flush=True)

    out = {"seedbase": args.seedbase, "arms": {}}
    for arm in args.arms:
        pr = per[arm]
        dv = [pr[s]["dev@10"] for s in STAMPS]
        wl = [np.sign(ref["per_season"][s] - pr[s]["dev@10"])
              for s in STAMPS]
        rec = [int(sum(x > 0 for x in wl)), int(sum(x == 0 for x in wl)),
               int(sum(x < 0 for x in wl))]
        tot = {k: int(sum(pr[s][k] for s in STAMPS)) for k in
               ("under", "over", "under_big", "over_big", "big_top_pred",
                "big_top_act")}
        out["arms"][arm] = {"per_season": pr,
                            "median": float(np.median(dv)),
                            "mean": float(np.mean(dv)),
                            "dev20_mean": float(np.mean(
                                [pr[s]["dev@20"] for s in STAMPS])),
                            "wl_vs_base": rec, "bias_totals": tot}
        print(f"[{arm}/s{args.seedbase}] median {np.median(dv):.2f} mean "
              f"{np.mean(dv):.2f} dev@20 {out['arms'][arm]['dev20_mean']:.2f}"
              f" | {rec[0]}W {rec[1]}T {rec[2]}L | {tot}", flush=True)
    tag = f"s{args.seedbase}_" + "-".join(args.arms)
    fp = HERE / f"RESULTS_cv_wingbig_{tag}.json"
    fp.write_text(json.dumps(out, indent=1))
    np.savez_compressed(HERE / f"DIAG_wingbig_{tag}.npz", big=big,
                        bigness=bigness, y=y, labeled=labeled,
                        **{f"p_{a}": oof[a] for a in args.arms})
    print(f"wrote {fp}", flush=True)


if __name__ == "__main__":
    main()
