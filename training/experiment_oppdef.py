"""Does opponent on/off data move the defense model? The measurement the scrape was for.

Arms, on the replicated defense production model (direct + cell-relative):

  base            as shipped
  +opp-engineered ~18 curated features transcribing what the recovered spec says
                  matters: opponent rim FG% and rim attempt rate allowed (on/off/
                  diff), opponent shot quality allowed, opponent eFG/TS allowed,
                  opponent 3PA rate (attempts, not results -- 538 found 3P results
                  are noise), a luck-adjusted defensive rating with opponent 3P
                  makes replaced at the cell-average 3P%, and turnovers forced.
  +opp-block      the full opponent block: on + off + diff of every field, counting
                  stats per-100 of the opponent's possessions. ~600 columns.
  +opp-both       engineered + block.

Offense gets the mirror test with the opponent's *defensive* events (their blocks,
steals, rebounds while the player is on) as strength-of-defense-faced context --
538's "opponents' defensive rating" input.

Run:  python training/experiment_oppdef.py
"""

import argparse
import json
import re
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import RATE_RE, TARGETS

RS_MIN, PO_MIN = 50, 10
SEEDS = (0, 1, 2)


def blend(Xtr, t, Xte, med, params, rounds, seeds=SEEDS, ridge_w=0.25):
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


def per100(M, fields):
    """Counting fields per 100 opponent possessions; rates left alone."""
    fi = {f: i for i, f in enumerate(fields)}
    poss = M[:, fi["TotalPoss"]].astype(np.float64)
    out = M.astype(np.float64).copy()
    with np.errstate(invalid="ignore", divide="ignore"):
        for f, i in fi.items():
            if f in ("TotalPoss", "OffPoss", "DefPoss", "Minutes", "SecondsPlayed",
                     "GamesPlayed") or RATE_RE.search(f):
                continue
            out[:, i] = np.where(poss > 0, 100.0 * out[:, i] / poss, np.nan)
    return out.astype(np.float32)


def engineered(on, off, fields, cells):
    """~18 curated opponent features per the recovered RAPTOR spec."""
    fi = {f: i for i, f in enumerate(fields)}

    def g(M, name):
        return M[:, fi[name]].astype(np.float64) if name in fi else None

    def drtg(M):
        pts, poss = g(M, "Points"), g(M, "OffPoss")
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.where(poss > 0, 100.0 * pts / poss, np.nan)

    def luck_adj_drtg(M):
        """Opponent 3P makes replaced at the cell-average opponent 3P%."""
        pts, poss = g(M, "Points"), g(M, "OffPoss")
        m3, a3 = g(M, "FG3M"), g(M, "FG3A")
        lg = np.full(len(pts), np.nan)
        for c in np.unique(cells):
            mask = cells == c
            mm, aa = np.nansum(m3[mask]), np.nansum(a3[mask])
            lg[mask] = mm / aa if aa > 0 else np.nan
        adj_pts = pts - 3.0 * (m3 - lg * a3)
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.where(poss > 0, 100.0 * adj_pts / poss, np.nan)

    base = {
        "opp_rim_pct": g(on, "AtRimAccuracy"), "opp_rim_pct_off": g(off, "AtRimAccuracy"),
        "opp_rim_freq": g(on, "AtRimFrequency"), "opp_rim_freq_off": g(off, "AtRimFrequency"),
        "opp_sq": g(on, "ShotQualityAvg"), "opp_sq_off": g(off, "ShotQualityAvg"),
        "opp_efg": g(on, "EfgPct"), "opp_efg_off": g(off, "EfgPct"),
        "opp_3pa_rate": g(on, "FG3APct"), "opp_3pa_rate_off": g(off, "FG3APct"),
        "opp_drtg": drtg(on), "opp_drtg_off": drtg(off),
        "opp_ladrtg": luck_adj_drtg(on), "opp_ladrtg_off": luck_adj_drtg(off),
    }
    diffs = {
        "opp_rim_pct_diff": base["opp_rim_pct"] - base["opp_rim_pct_off"],
        "opp_sq_diff": base["opp_sq"] - base["opp_sq_off"],
        "opp_drtg_diff": base["opp_drtg"] - base["opp_drtg_off"],
        "opp_ladrtg_diff": base["opp_ladrtg"] - base["opp_ladrtg_off"],
    }
    # offense context: the opponent's defensive events while the player is on
    with np.errstate(invalid="ignore", divide="ignore"):
        poss_on = g(on, "TotalPoss")
        off_ctx = {
            "oppdef_blocks100": np.where(poss_on > 0, 100 * g(on, "Blocks") / poss_on, np.nan),
            "oppdef_steals100": np.where(poss_on > 0, 100 * g(on, "Steals") / poss_on, np.nan),
        }
    all_f = {**base, **diffs, **off_ctx}
    names = sorted(all_f)
    return np.column_stack([all_f[n] for n in names]).astype(np.float32), names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_oppdef.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
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
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    tuned = json.loads(Path(args.tuned).read_text())

    cov = np.isfinite(on_raw).any(axis=1)
    print(f"X={X.shape}  opp coverage: train {cov[tr].sum()}/{tr.sum()}  "
          f"test {cov[test].sum()}/{test.sum()}", flush=True)

    E, enames = engineered(on_raw, off_raw, ofields, cells_all)
    on100, off100 = per100(on_raw, ofields), per100(off_raw, ofields)
    B_full = np.hstack([on100, off100, on100 - off100])
    print(f"engineered: {E.shape[1]} cols   full block: {B_full.shape[1]} cols",
          flush=True)

    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    rows = []

    def record(target, name, p):
        y = d[TARGETS[target]]
        s = score_cells(y[test], p, cells_te)
        rows.append({"target": target, "arm": name, **s})
        print(f"  {name:<16} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40", flush=True)

    for target in ("defense", "offense"):
        y = d[TARGETS[target]]
        params = dict(tuned[target]["params"], verbose=-1)
        rounds = tuned[target]["rounds"]
        print(f"\n=== {target} ===", flush=True)
        variants = {
            "base": [Z],
            "+opp-engineered": [Z, E],
            "+opp-block": [Z, B_full],
            "+opp-both": [Z, E, B_full],
        }
        for name, parts in variants.items():
            Xtr_v = np.hstack([X[tr]] + [p[tr] for p in parts])
            Xte_v = np.hstack([X[test]] + [p[test] for p in parts])
            med_v = np.concatenate([med, np.zeros(Xtr_v.shape[1] - X.shape[1])])
            record(target, name, blend(Xtr_v, y[tr], Xte_v, med_v, params, rounds))

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    L = ["# Opponent on/off data: the A/B the scrape was for", "",
         "Regular season only; base = the replicated production model per target.", ""]
    for target in ("defense", "offense"):
        L += [f"## {target}", "",
              "| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for r in sorted([x for x in rows if x["target"] == target],
                        key=lambda r: r["dev@10"]):
            L.append(f"| {r['arm']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
                     f"{r['tau@10']:+.3f} | {r['tau@20']:+.3f} | {r['mae']:.3f} | "
                     f"{r['hits@10']}/20 | {r['hits@20']}/40 |")
        L.append("")
    Path(args.out).write_text("\n".join(L))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
