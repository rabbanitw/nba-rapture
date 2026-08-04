"""The nearest-defender A/B: does leaguedashptdefend move the defense model?

This is the measurement the whole defensive thread points at. The recovered spec
says 538's defensive edge came from player-attributed shot defense -- defended
2-point misses +1.05, makes -0.33, defended 3-point attempts +0.17 with results
ignored as noise -- and reports defensive R² of ~0.6 with this data class against
~0.3 without. Our defense model has never had any of it.

Features per player, normalized per 36 minutes where they are counts:

  d2_value      1.05*(FG2A-FG2M) - 0.33*FG2M       the 538-weighted 2-pt defense
  d2_pct_pm     defended FG2% minus shooters' normal FG2% (PLUSMINUS)
  rim_*         the same pair from the <6ft table
  d3a_rate      defended 3PA per 36 -- attempts only, per the spec
  freq          share of opponent shots defended (defensive usage, also an input
                to 538's team-effects allocation)
  dfga_rate     overall defended FGA per 36

plus an all-columns arm with every numeric field from all six category tables.

Coverage caveat, same as the opponent A/B: the scrape stored whole-season cells
only, so the features exist for ~17% of training rows but ~96% of test rows. Both
regimes are therefore run: full training set, and matched (whole-season rows only).

Run:  python training/experiment_defend.py
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT, get_collection
from coverage import as_float
from experiment_combined import prepare, splits
from experiment_components import RELATIVE_COLS, cell_relative
from experiment_oppdef import blend
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from seasons import FULL_SEASON_SNAPSHOTS
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
KEYS = {"_id", "name", "standard_name", "PLAYER", "nba_player_id", "source",
        "data_type", "timestamp", "season_type"}
CATS = ["defend-overall", "defend-2pt", "defend-3pt", "defend-lt6ft",
        "defend-lt10ft", "defend-gt15ft"]


def extract(d):
    """Row-aligned defend matrices: engineered features and the full column set."""
    coll = get_collection()
    players, tss, sts, mp = d["player"], d["timestamp"], d["season_type"], d["mp"]

    cache = {}
    all_fields = set()
    for i, (ts, st) in enumerate(zip(tss, sts)):
        key = (str(ts), str(st))
        if key in cache:
            continue
        cell = defaultdict(dict)
        for doc in coll.find({"source": "nba-defend", "timestamp": str(ts),
                              "season_type": str(st)}):
            cell[doc["standard_name"]][doc["data_type"]] = doc
            all_fields.update((doc["data_type"], k) for k in doc if k not in KEYS)
        cache[key] = cell
        if cell:
            print(f"  defend cell {key}: {len(cell)} players", flush=True)

    full_cols = sorted(all_fields)
    fci = {c: i for i, c in enumerate(full_cols)}
    F = np.full((len(players), len(full_cols)), np.nan, dtype=np.float32)
    E = np.full((len(players), 8), np.nan, dtype=np.float32)

    for i, (p, ts, st) in enumerate(zip(players, tss, sts)):
        byname = cache[(str(ts), str(st))]
        tables = byname.get(str(p))
        if not tables:
            continue
        for dt, doc in tables.items():
            for k, v in doc.items():
                if (dt, k) in fci:
                    x = as_float(v)
                    if x is not None:
                        F[i, fci[(dt, k)]] = x
        m = float(mp[i]) or np.nan
        per36 = (lambda v: 36.0 * v / m if v is not None and m and m > 0 else np.nan)
        d2 = tables.get("defend-2pt", {})
        rim = tables.get("defend-lt6ft", {})
        d3 = tables.get("defend-3pt", {})
        ov = tables.get("defend-overall", {})
        f2m, f2a = as_float(d2.get("FG2M")), as_float(d2.get("FG2A"))
        if f2m is not None and f2a is not None:
            E[i, 0] = per36(1.05 * (f2a - f2m) - 0.33 * f2m)
        E[i, 1] = as_float(d2.get("PLUSMINUS")) or np.nan
        rm, ra = as_float(rim.get("LT_06_PCT")), None
        E[i, 2] = as_float(rim.get("PLUSMINUS")) or np.nan
        fga_r = as_float(rim.get("FGA_LT_06") or rim.get("FGA"))
        E[i, 3] = per36(fga_r) if fga_r is not None else np.nan
        f3a = as_float(d3.get("FG3A"))
        E[i, 4] = per36(f3a) if f3a is not None else np.nan
        E[i, 5] = as_float(ov.get("FREQ")) or np.nan
        dfga = as_float(ov.get("D_FGA") or ov.get("FGA"))
        E[i, 6] = per36(dfga) if dfga is not None else np.nan
        E[i, 7] = as_float(ov.get("PLUSMINUS")) or np.nan
    enames = ["d2_value36", "d2_pct_pm", "rim_pct_pm", "rim_fga36",
              "d3a36", "freq", "dfga36", "ov_pct_pm"]
    return E, enames, F, full_cols


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_defend.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr_full, test = (fit | val) & rs, test & rs
    isfull = np.array([t in FULL_SEASON_SNAPSHOTS for t in d["timestamp"]])
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    tuned = json.loads(Path(args.tuned).read_text())

    print("extracting defend features ...", flush=True)
    E, enames, F, fcols = extract(d)
    cov = np.isfinite(E).any(axis=1)
    print(f"engineered {E.shape[1]} cols, full {F.shape[1]} cols; coverage "
          f"train {cov[tr_full].sum()}/{tr_full.sum()} "
          f"test {cov[test].sum()}/{test.sum()}", flush=True)
    np.savez_compressed(Path(args.datadir) / "defend.npz", E=E, F=F,
                        enames=np.array(enames, dtype=object),
                        fcols=np.array([f"{a}|{b}" for a, b in fcols], dtype=object))

    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    rows = []

    def record(regime, target, name, p):
        y = d[TARGETS[target]]
        s = score_cells(y[test], p, cells_te)
        rows.append({"regime": regime, "target": target, "arm": name, **s})
        print(f"  {name:<18} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40", flush=True)

    for regime, tr in (("full", tr_full), ("matched", tr_full & isfull)):
        med = np.nanmedian(X[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        for target in ("defense", "offense"):
            y = d[TARGETS[target]]
            params = dict(tuned[target]["params"], verbose=-1)
            rounds = tuned[target]["rounds"] if regime == "full" \
                else max(tuned[target]["rounds"] // 3, 150)
            print(f"\n=== {regime} / {target} (train={tr.sum()}) ===", flush=True)
            for name, parts in {"base": [Z], "+defend-eng": [Z, E],
                                "+defend-all": [Z, F],
                                "+defend-both": [Z, E, F]}.items():
                Xtr = np.hstack([X[tr]] + [p[tr] for p in parts])
                Xte = np.hstack([X[test]] + [p[test] for p in parts])
                mv = np.concatenate([med, np.zeros(Xtr.shape[1] - X.shape[1])])
                record(regime, target, name,
                       blend(Xtr, y[tr], Xte, mv, params, rounds))

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    L = ["# Nearest-defender data (leaguedashptdefend): the defense A/B", ""]
    for regime in ("full", "matched"):
        for target in ("defense", "offense"):
            sub = [r for r in rows if r["regime"] == regime
                   and r["target"] == target]
            if not sub:
                continue
            L += [f"## {regime} training / {target}", "",
                  "| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|"]
            for r in sorted(sub, key=lambda r: r["dev@10"]):
                L.append(f"| {r['arm']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
                         f"{r['tau@10']:+.3f} | {r['tau@20']:+.3f} | "
                         f"{r['mae']:.3f} | {r['hits@10']}/20 | {r['hits@20']}/40 |")
            L.append("")
    Path(args.out).write_text("\n".join(L))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
