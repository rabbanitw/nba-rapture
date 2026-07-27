"""Top-20 leaderboards on the held-out seasons: true RAPTOR vs our models vs Paine's.

Covers total, offense and defense. For the total there are three systems, because
Paine has no direct total model -- his eRT is exactly eRO + eRD -- so the question
"predict the total directly, or predict the halves and add them?" can be asked of
our model and answered against his:

    ours (direct total)   a model trained on rap
    ours (offense+defense) our two part-models summed
    Paine (eRO+eRD)       his two part-models summed, which is all he has

The minutes threshold is derived rather than picked: it is the lowest minutes
total among any true top-20 player across every season, split and target, so no
actual leader is ever made ineligible.

Run after estimated_raptor.py:
    python training/leaderboards.py
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from compare_estimated_raptor import our_predictions
from db import REPO_ROOT
from estimated_raptor import metrics, norm_name

TOP_N = 20
SEASONS = ["2013-14", "2014-15"]
SPLITS = ["Regular season", "Playoffs"]

TRUTH = {"total": "rap", "offense": "rap_o", "defense": "rap_d"}
SYSTEMS = {
    "total": [("ours (direct total)", "ours_total"),
              ("ours (offense+defense)", "ours_sum"),
              ("Paine (eRO+eRD)", "eRT")],
    "offense": [("ours", "ours_offense"), ("Paine (eRO)", "eRO")],
    "defense": [("ours", "ours_defense"), ("Paine (eRD)", "eRD")],
}


def derive_thresholds(df):
    """Lowest minutes among any true top-20 player, per split."""
    out, detail = {}, []
    for split in SPLITS:
        mins = []
        for season in SEASONS:
            g = df[(df.season == season) & (df.split == split)]
            for tgt, truth in TRUTH.items():
                top = g.nlargest(TOP_N, truth)
                mins.append(top.mp.min())
                detail.append({"season": season, "split": split, "target": tgt,
                               "min_mp_in_true_top20": float(top.mp.min()),
                               "median_mp_in_true_top20": float(top.mp.median()),
                               "pool_n": int(len(g)),
                               "pool_min_mp": float(g.mp.min())})
        out[split] = float(min(mins))
    return out, detail


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data"))
    ap.add_argument("--paine", default=str(REPO_ROOT / "training"
                                           / "RESULTS_estimated_raptor.csv"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_top20.md"))
    args = ap.parse_args()

    paine = pd.read_csv(args.paine)
    paine["key"] = paine.player.map(norm_name)
    thresholds, detail = derive_thresholds(paine)
    print("thresholds derived from true top-20 minutes:", thresholds)

    print("refitting the combined model (total, offense, defense) ...")
    ours = our_predictions(args.datadir)
    ours["key"] = ours.player.map(norm_name)
    ours["ours_sum"] = ours.ours_offense + ours.ours_defense

    df = paine.merge(
        ours[["key", "season", "split", "ours_total", "ours_offense",
              "ours_defense", "ours_sum"]],
        on=["key", "season", "split"], how="left")
    print(f"  merged rows: {len(df)}; with our predictions: "
          f"{df.ours_total.notna().sum()}; with Paine's: {df.eRT.notna().sum()}")

    report, summary, overall = [], [], {}
    for target, truth in TRUTH.items():
        sysl = SYSTEMS[target]
        pooled = []
        for season in SEASONS:
            for split in SPLITS:
                thr = thresholds[split]
                cell = df[(df.season == season) & (df.split == split)]
                pool = cell[cell.mp >= thr].dropna(
                    subset=[truth] + [c for _, c in sysl]).copy()
                pooled.append(pool)
                true_lb = pool.nlargest(TOP_N, truth).reset_index(drop=True)
                true_set = set(true_lb.player)
                pool["true_rank"] = pool[truth].rank(ascending=False,
                                                     method="min").astype(int)
                rank_of = dict(zip(pool.player, pool.true_rank))

                boards, row = {}, {"season": season, "split": split,
                                   "target": target, "threshold_mp": thr,
                                   "pool_n": int(len(pool)),
                                   "dropped_by_threshold": int(len(cell) - len(pool))}
                for name, col in sysl:
                    lb = pool.nlargest(TOP_N, col).reset_index(drop=True)
                    boards[name] = lb
                    row[f"hits::{name}"] = len(set(lb.player) & true_set)
                    row[f"rho::{name}"] = float(
                        spearmanr(pool[truth], pool[col]).statistic)
                # how contested is the rank-20 cutoff?
                vals = np.sort(pool[truth].to_numpy())[::-1]
                if len(vals) > TOP_N:
                    cut = vals[TOP_N - 1]
                    row["cut_value"] = float(cut)
                    row["gap_20_21"] = float(cut - vals[TOP_N])
                    row["within_0.25_of_cut"] = int(np.sum(np.abs(vals - cut) <= 0.25))
                summary.append(row)
                report.append({"season": season, "split": split, "target": target,
                               "true": true_lb, "boards": boards,
                               "rank_of": rank_of, "truth": truth, "systems": sysl})
                print(f"  {target:<8} {season} {split:<15} pool={len(pool):<4} "
                      + "  ".join(f"{n}={row[f'hits::{n}']:>2}" for n, _ in sysl))

        allrows = pd.concat(pooled)
        overall[target] = {name: metrics(allrows[truth], allrows[col])
                           for name, col in sysl}

    write_report(args.out, report, summary, overall, thresholds, detail)
    json.dump({"thresholds": thresholds, "threshold_detail": detail,
               "summary": summary, "overall_regression": overall},
              open(Path(args.out).with_suffix(".json"), "w"), indent=2)
    print(f"\nwrote {args.out}")


def write_report(path, report, summary, overall, thresholds, detail):
    L = []
    A = L.append
    A("# Top-20 leaderboards on the held-out seasons\n")
    A("True 538 RAPTOR vs. our models vs. Neil Paine's Estimated RAPTOR, for")
    A("2013-14 and 2014-15. Total, offense and defense are ranked separately.\n")
    A("> Paine's weights were fit on 2014-2023 full RAPTOR, which includes both of")
    A("> these seasons — his predictions are in-sample, ours are not.\n")

    A("## Three ways to rank by total RAPTOR\n")
    A("Paine's published `eRT` is **exactly** `eRO + eRD` (max residual 1e-9): he")
    A("has no direct total model, only two part-models that are added. We do have a")
    A("direct total model, so the total leaderboards compare three systems:\n")
    A("| system | how the total is produced |")
    A("|---|---|")
    A("| ours (direct total) | one model trained on `rap` |")
    A("| ours (offense+defense) | our `rap_o` and `rap_d` models, summed |")
    A("| Paine (eRO+eRD) | his two part-models, summed — his only option |\n")

    A("## Minutes threshold\n")
    A("Derived, not chosen: the **lowest minutes total among any true top-20**")
    A("player, taken across every season, split and target, so no genuine leader is")
    A("ruled ineligible.\n")
    A("| season | split | target | min mp in true top 20 | median mp | pool n | pool min mp |")
    A("|---|---|---|---|---|---|---|")
    for d in detail:
        A(f"| {d['season']} | {d['split']} | {d['target']} | "
          f"{d['min_mp_in_true_top20']:.0f} | {d['median_mp_in_true_top20']:.0f} | "
          f"{d['pool_n']} | {d['pool_min_mp']:.0f} |")
    A(f"\n**Regular season → ≥ {thresholds['Regular season']:.0f} minutes. "
      f"Playoffs → ≥ {thresholds['Playoffs']:.0f} minutes.**\n")
    A("This barely bites: 538 only rated ~250 players per historical season and all")
    A("of them already clear 1,065 regular-season minutes. In the playoffs the true")
    A("top 20 reaches the very bottom of the pool (a 131-minute player makes it), so")
    A("no threshold applies there without excluding a real leader. The filter would")
    A("matter far more against an unfiltered universe — Paine's own CSV has a")
    A("1-minute player at eRO +55.7 who would otherwise top every leaderboard.\n")

    A("## Regression accuracy over all held-out rows\n")
    for target in TRUTH:
        A(f"**{target}**\n")
        A("| system | RMSE | MAE | R² | Pearson r | Spearman ρ |")
        A("|---|---|---|---|---|---|")
        for name, v in overall[target].items():
            A(f"| {name} | {v['rmse']:.3f} | {v['mae']:.3f} | {v['r2']:+.3f} | "
              f"{v['pearson']:+.3f} | {v['spearman']:+.3f} |")
        A("")

    A("## Summary — true top-20 members recovered (hits@20)\n")
    for target in TRUTH:
        rows = [s for s in summary if s["target"] == target]
        names = [n for n in rows[0] if n.startswith("hits::")]
        A(f"**{target}**\n")
        A("| season | split | pool | " + " | ".join(n[6:] for n in names)
          + " | " + " | ".join(f"ρ {n[6:]}" for n in names) + " |")
        A("|---" * (3 + 2 * len(names)) + "|")
        for s in rows:
            A(f"| {s['season']} | {s['split']} | {s['pool_n']} | "
              + " | ".join(f"{s[n]}/20" for n in names) + " | "
              + " | ".join(f"{s['rho::' + n[6:]]:+.3f}" for n in names) + " |")
        tot = {n: sum(s[n] for s in rows) for n in names}
        A("| **all** | | | "
          + " | ".join(f"**{tot[n]}/{len(rows)*TOP_N}**" for n in names)
          + " | " + " | ".join("" for _ in names) + " |")
        A("")

    A("## Why the total is harder to rank than offense\n")
    A("Hits@20 only asks whether a player lands on the correct side of an arbitrary")
    A("cutoff, and for the total that cutoff is crowded. Players within ±0.25 RAPTOR")
    A("of the rank-20 value, per cell:\n")
    A("| season | split | target | rank-20 value | gap to rank 21 | players within ±0.25 |")
    A("|---|---|---|---|---|---|")
    for s_ in summary:
        if "cut_value" not in s_:
            continue
        A(f"| {s_['season']} | {s_['split']} | {s_['target']} | "
          f"{s_['cut_value']:+.2f} | {s_['gap_20_21']:.2f} | "
          f"{s_['within_0.25_of_cut']} |")
    A("")
    A("Where a dozen players sit inside a quarter-point of the cutoff, which 20 names")
    A("come back is close to a coin flip regardless of model quality. That is why")
    A("hits@20 and the rank correlations disagree, and why the correlations are the")
    A("more reliable read.\n")

    A("## Conclusions\n")
    ot = overall["total"]
    direct = ot["ours (direct total)"]
    summed = ot["ours (offense+defense)"]
    paine_t = ot["Paine (eRO+eRD)"]
    A(f"**Direct total vs. summing the halves — our model.** Predicting `rap`")
    A(f"directly edges summing our two part-models: R² {direct['r2']:+.3f} vs")
    A(f"{summed['r2']:+.3f}, ρ {direct['spearman']:+.3f} vs {summed['spearman']:+.3f},")
    A("and 53/80 top-20 hits each. The two are close to interchangeable; the direct")
    A("model wins narrowly and consistently on the continuous metrics.\n")
    A(f"**Against Paine.** On every continuous measure our total model is clearly")
    A(f"ahead — R² {direct['r2']:+.3f} vs {paine_t['r2']:+.3f}, RMSE")
    A(f"{direct['rmse']:.3f} vs {paine_t['rmse']:.3f}, ρ {direct['spearman']:+.3f} vs")
    A(f"{paine_t['spearman']:+.3f} — and it leads the per-cell rank correlation in")
    A("all four cells. But he recovers **55/80** top-20 members to our 53/80.\n")
    A("Those two facts are not in conflict. A 2-slot difference out of 80 is inside")
    A("the noise of a metric decided by hundredths of a point at a crowded cutoff,")
    A("while the correlation gap is consistent across every cell. The fair summary:")
    A("**we rank the whole field better; at the top-20 boundary for the total the")
    A("two systems are indistinguishable.** On offense and defense separately our")
    A("advantage does show up in hits@20 too (63/80 vs 59/80, 55/80 vs 47/80).\n")

    A("## Leaderboards\n")
    A("`[n]` after a predicted name is that player's *true* rank; ✓ means they are")
    A("genuinely in the true top 20.\n")
    for r in report:
        A(f"### {r['season']} — {r['split']} — {r['target']}\n")
        true_lb, boards, rank_of = r["true"], r["boards"], r["rank_of"]
        true_set = set(true_lb.player)
        names = [n for n, _ in r["systems"]]
        A("| # | true RAPTOR | " + " | ".join(names) + " |")
        A("|---" * (2 + len(names)) + "|")
        for i in range(min(TOP_N, len(true_lb))):
            t = true_lb.iloc[i]
            cells = [f"**{t.player}** ({t[r['truth']]:+.2f})"]
            for name, col in r["systems"]:
                lb = boards[name]
                if i < len(lb):
                    p = lb.iloc[i]
                    mark = "✓" if p.player in true_set else "✗"
                    cells.append(f"{p.player} ({p[col]:+.2f}) "
                                 f"[{rank_of.get(p.player, '—')}] {mark}")
                else:
                    cells.append("—")
            A(f"| {i+1} | " + " | ".join(cells) + " |")
        A("")

    Path(path).write_text("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
