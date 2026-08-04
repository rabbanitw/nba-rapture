"""Per-position confidence from the LOSO runs, with a real calibration check.

Consumes data_fixed/loso_detail.json (written by loso_confidence.py). For every
held-out season and both targets it Monte-Carlos the leaderboard:

    draw = random seed-member prediction + bootstrap residual
    residuals come from the OTHER nine folds only, bucketed by minutes tercile,
    so a fold's own truth never informs its own intervals

Re-ranking each draw yields, for every board position: the projected player's 90%
CI on true rank, P(truly top-10) and P(truly top-30). Because truth exists for all
ten folds, the report closes with coverage: the fraction of 90% intervals that
contain the true rank. If that is far from 90%, the intervals are decoration; if
close, they mean what they say.

Defense uses all eight member predictions (3 lgbm-full + 3 lgbm-matched +
2 catboost) -- the ens(3) model -- and offense its three component-model members.

Run:  python training/confidence_report.py
"""

import json
from pathlib import Path

import numpy as np

from db import REPO_ROOT
from experiment_topk_rank import ranks

MC = 2000
CI = (5, 95)
TOP_SHOW = 20
rng = np.random.default_rng(7)

detail = json.loads((REPO_ROOT / "training" / "data_fixed"
                     / "loso_detail.json").read_text())
seasons = sorted(detail)


def members_of(det, target):
    if target == "offense":
        return [np.array(m) for m in det["off_members"]]
    out = []
    for fam in det["def_members"].values():
        out += [np.array(m) for m in fam]
    return out


def buckets(mp, edges=None):
    mp = np.asarray(mp)
    if edges is None:
        edges = np.quantile(mp, [1 / 3, 2 / 3])
    return np.digitize(mp, edges), edges


# ---- residual pools: leave-fold-out, per target, per minutes bucket ----------
pools = {}
for target, ykey in (("offense", "y_off"), ("defense", "y_def")):
    per_fold = {}
    for s in seasons:
        det = detail[s]
        mean_pred = np.mean(members_of(det, target), axis=0)
        b, _ = buckets(det["mp"])
        res = np.array(det[ykey]) - mean_pred
        per_fold[s] = (b, res)
    pools[target] = per_fold


def pool_for(target, exclude, bucket):
    out = []
    for s, (b, res) in pools[target].items():
        if s == exclude:
            continue
        out.append(res[b == bucket])
    return np.concatenate(out)


boards = {}
coverage = {"offense": [0, 0], "defense": [0, 0]}
cover_top30 = {"offense": [0, 0], "defense": [0, 0]}
for target, ykey in (("offense", "y_off"), ("defense", "y_def")):
    for s in seasons:
        det = detail[s]
        mem = members_of(det, target)
        mean_pred = np.mean(mem, axis=0)
        y = np.array(det[ykey])
        n = len(y)
        b, _ = buckets(det["mp"])
        bucket_pools = {k: pool_for(target, s, k) for k in np.unique(b)}

        rank_counts = np.zeros((n, n), dtype=np.int32)
        for _ in range(MC):
            base = mem[rng.integers(len(mem))]
            noise = np.empty(n)
            for k, pl in bucket_pools.items():
                m = b == k
                noise[m] = rng.choice(pl, size=m.sum(), replace=True)
            r = ranks(base + noise)
            rank_counts[np.arange(n), r] += 1

        true_rank = ranks(y)
        order = np.argsort(ranks(mean_pred))
        rows = []
        for pos, j in enumerate(order, 1):
            dist = rank_counts[j] / MC
            cum = np.cumsum(dist)
            lo = int(np.searchsorted(cum, CI[0] / 100)) + 1
            hi = int(np.searchsorted(cum, CI[1] / 100)) + 1
            p10 = float(dist[:10].sum())
            p30 = float(dist[:30].sum())
            tr = int(true_rank[j]) + 1
            inside = lo <= tr <= hi
            coverage[target][0] += inside
            coverage[target][1] += 1
            if pos <= 30:
                cover_top30[target][0] += inside
                cover_top30[target][1] += 1
            rows.append({"pos": pos, "player": det["players"][j],
                         "est": float(mean_pred[j]), "true_rank": tr,
                         "ci": (lo, hi), "p10": p10, "p30": p30,
                         "inside": inside})
        boards[(target, s)] = rows

# ------------------------------- report ---------------------------------------
L = []
A = L.append
A("# LOSO confidence: rank intervals at every position, calibrated")
A("")
A("Uncertainty = seed-ensemble spread + leave-fold-out residual bootstrap "
  f"(minutes-bucketed), {MC} Monte Carlo re-rankings per board.")
A("")
A("## Calibration -- do the intervals mean what they say?")
A("")
A("| target | 90% CI coverage, all positions | coverage, top-30 positions |")
A("|---|---:|---:|")
for t in ("offense", "defense"):
    c, n = coverage[t]
    c30, n30 = cover_top30[t]
    A(f"| {t} | {100 * c / n:.1f}% (n={n}) | {100 * c30 / n30:.1f}% (n={n30}) |")
A("")
for (target, s) in sorted(boards, key=lambda k: (k[0], k[1])):
    rows = boards[(target, s)]
    A(f"## {s} — {target}, top {TOP_SHOW} with confidence")
    A("")
    A("| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |")
    A("|---:|---|---:|---:|---|---:|---:|")
    for r in rows[:TOP_SHOW]:
        star = "" if r["inside"] else " ⚠"
        A(f"| {r['pos']} | {r['player']} | {r['est']:+.2f} | {r['true_rank']}{star} | "
          f"{r['ci'][0]}–{r['ci'][1]} | {r['p10']:.0%} | {r['p30']:.0%} |")
    A("")
out = REPO_ROOT / "training" / "RESULTS_loso_confidence.md"
out.write_text("\n".join(L))
json.dump({f"{t}|{s}": rows for (t, s), rows in boards.items()},
          open(REPO_ROOT / "training" / "RESULTS_loso_confidence.json", "w"))
print(f"wrote {out}")
print("coverage:", {t: f"{100 * c / n:.1f}%" for t, (c, n) in coverage.items()})
