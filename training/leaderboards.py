"""Top-20 leaderboards on the held-out seasons: true RAPTOR vs our model vs Paine's.

The minutes threshold is derived from the data rather than picked: it is the
lowest minutes total among any true top-20 player, so no actual leader is ever
made ineligible. Only players at or above it are ranked.

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
from estimated_raptor import norm_name

TOP_N = 20
SEASONS = ["2013-14", "2014-15"]
SPLITS = ["Regular season", "Playoffs"]
TARGETS = {"offense": ("rap_o", "ours_offense", "eRO"),
           "defense": ("rap_d", "ours_defense", "eRD")}


def derive_thresholds(df):
    """Lowest minutes among any true top-20 player, per split.

    Taking the minimum across both seasons and both targets guarantees the
    threshold never excludes a player who actually belongs on a leaderboard.
    """
    out, detail = {}, []
    for split in SPLITS:
        mins = []
        for season in SEASONS:
            g = df[(df.season == season) & (df.split == split)]
            for tgt, (truth, _, _) in TARGETS.items():
                top = g.nlargest(TOP_N, truth)
                mins.append(top.mp.min())
                detail.append({"season": season, "split": split, "target": tgt,
                               "min_mp_in_true_top20": float(top.mp.min()),
                               "median_mp_in_true_top20": float(top.mp.median()),
                               "pool_n": int(len(g)),
                               "pool_min_mp": float(g.mp.min())})
        out[split] = float(min(mins))
    return out, detail


def leaderboard(g, col, n=TOP_N):
    return g.nlargest(n, col).reset_index(drop=True)


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

    print("refitting the combined model ...")
    ours = our_predictions(args.datadir)
    ours["key"] = ours.player.map(norm_name)

    df = paine.merge(ours[["key", "season", "split", "ours_offense", "ours_defense"]],
                     on=["key", "season", "split"], how="left")
    print(f"  merged rows: {len(df)}; with our prediction: {df.ours_offense.notna().sum()}; "
          f"with Paine's: {df.eRO.notna().sum()}")

    report, summary = [], []
    for season in SEASONS:
        for split in SPLITS:
            thr = thresholds[split]
            cell = df[(df.season == season) & (df.split == split)]
            elig = cell[cell.mp >= thr].copy()
            for tgt, (truth, ocol, pcol) in TARGETS.items():
                # every system ranks the same eligible pool
                pool = elig.dropna(subset=[truth, ocol, pcol]).copy()
                true_lb = leaderboard(pool, truth)
                true_set = set(true_lb.player)
                pool["true_rank"] = pool[truth].rank(ascending=False,
                                                     method="min").astype(int)
                rank_of = dict(zip(pool.player, pool.true_rank))

                rows, hits = [], {}
                for sys_name, col in (("ours", ocol), ("paine", pcol)):
                    lb = leaderboard(pool, col)
                    hits[sys_name] = len(set(lb.player) & true_set)
                    rows.append((sys_name, lb))
                summary.append({
                    "season": season, "split": split, "target": tgt,
                    "threshold_mp": thr, "pool_n": int(len(pool)),
                    "dropped_by_threshold": int(len(cell) - len(elig)),
                    "hits_ours": hits["ours"], "hits_paine": hits["paine"],
                    "spearman_ours": float(spearmanr(pool[truth], pool[ocol]).statistic),
                    "spearman_paine": float(spearmanr(pool[truth], pool[pcol]).statistic),
                })
                report.append({"season": season, "split": split, "target": tgt,
                               "true": true_lb, "systems": dict(rows),
                               "rank_of": rank_of, "truth_col": truth,
                               "ocol": ocol, "pcol": pcol})
                print(f"  {season} {split:<15} {tgt:<8} pool={len(pool):<4} "
                      f"hits@20 ours={hits['ours']:>2} paine={hits['paine']:>2}")

    write_report(args.out, report, summary, thresholds, detail)
    json.dump({"thresholds": thresholds, "threshold_detail": detail,
               "summary": summary}, open(Path(args.out).with_suffix(".json"), "w"),
              indent=2)
    print(f"\nwrote {args.out}")


def write_report(path, report, summary, thresholds, detail):
    L = []
    A = L.append
    A("# Top-20 leaderboards on the held-out seasons\n")
    A("True 538 RAPTOR vs. our combined model vs. Neil Paine's Estimated RAPTOR,")
    A("for 2013-14 and 2014-15. Offense and defense are ranked separately.\n")
    A("> Paine's weights were fit on 2014-2023 full RAPTOR, which includes both of")
    A("> these seasons — his predictions are in-sample, ours are not.\n")

    A("## Minutes threshold\n")
    A("The threshold is derived, not chosen: it is the **lowest minutes total among")
    A("any true top-20 player**, taken across both seasons and both targets, so no")
    A("genuine leader is ever ruled ineligible.\n")
    A("| season | split | target | min mp in true top 20 | median mp | pool n | pool min mp |")
    A("|---|---|---|---|---|---|---|")
    for d in detail:
        A(f"| {d['season']} | {d['split']} | {d['target']} | "
          f"{d['min_mp_in_true_top20']:.0f} | {d['median_mp_in_true_top20']:.0f} | "
          f"{d['pool_n']} | {d['pool_min_mp']:.0f} |")
    A(f"\n**Regular season → ≥ {thresholds['Regular season']:.0f} minutes. "
      f"Playoffs → ≥ {thresholds['Playoffs']:.0f} minutes.**\n")
    A("Note how little this bites: 538 only rated ~250 players per historical")
    A("season, and every one of them already clears 1,065 regular-season minutes.")
    A("In the playoffs the true top 20 reaches down to the very bottom of the pool")
    A("(a 131-minute player makes it), so no threshold can be applied there without")
    A("excluding a real leader. The filter matters far more when ranking an")
    A("unfiltered player universe — Paine's own CSV has a 1-minute player at")
    A("eRO +55.7 who would otherwise top every offensive leaderboard.\n")

    A("## Summary — how many of the true top 20 each system recovers\n")
    A("| season | split | target | pool | dropped by threshold | ours hits@20 | Paine hits@20 | ours ρ | Paine ρ |")
    A("|---|---|---|---|---|---|---|---|---|")
    for s in summary:
        A(f"| {s['season']} | {s['split']} | {s['target']} | {s['pool_n']} | "
          f"{s['dropped_by_threshold']} | **{s['hits_ours']}/20** | "
          f"{s['hits_paine']}/20 | {s['spearman_ours']:+.3f} | "
          f"{s['spearman_paine']:+.3f} |")
    tot_o = sum(s["hits_ours"] for s in summary)
    tot_p = sum(s["hits_paine"] for s in summary)
    A(f"\n**Overall: ours {tot_o}/{len(summary)*TOP_N}, "
      f"Paine {tot_p}/{len(summary)*TOP_N}.**\n")

    A("## Leaderboards\n")
    A("`[n]` after a predicted name is that player's *true* rank. A ✓ means the")
    A("player is genuinely in the true top 20.\n")
    for r in report:
        A(f"### {r['season']} — {r['split']} — {r['target']}\n")
        true_lb, sysd, rank_of = r["true"], r["systems"], r["rank_of"]
        true_set = set(true_lb.player)
        A("| # | true RAPTOR | ours (predicted) | Paine (predicted) |")
        A("|---|---|---|---|")
        for i in range(TOP_N):
            t = true_lb.iloc[i]
            cells = [f"**{t.player}** ({t[r['truth_col']]:+.2f})"]
            for sys_name, col in (("ours", r["ocol"]), ("paine", r["pcol"])):
                lb = sysd[sys_name]
                if i < len(lb):
                    p = lb.iloc[i]
                    mark = "✓" if p.player in true_set else "✗"
                    cells.append(f"{p.player} ({p[col]:+.2f}) [{rank_of.get(p.player,'—')}] {mark}")
                else:
                    cells.append("—")
            A(f"| {i+1} | " + " | ".join(cells) + " |")
        A("")

    Path(path).write_text("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
