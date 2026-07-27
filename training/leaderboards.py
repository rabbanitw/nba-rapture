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

TOP_N = 20          # default; main() overrides from --top-n
HITS_AT = (10, 25, 50, 100)
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
    ap.add_argument("--out", default=None)
    ap.add_argument("--top-n", type=int, default=20)
    ap.add_argument("--stride-tuned", action="store_true",
                    help="use per-target best strides from RESULTS_stride.md, "
                         "trained off the full stride-1 build")
    ap.add_argument("--stride-datadir",
                    default=str(REPO_ROOT / "training" / "data_full"))
    ap.add_argument("--no-min-mp", action="store_true",
                    help="rank every player: no eligibility threshold and no "
                         "minimum-minutes filter on training rows either")
    args = ap.parse_args()

    global TOP_N
    TOP_N = args.top_n
    suffix = ("_stride" if args.stride_tuned else "") + \
             ("_nofilter" if args.no_min_mp else "")
    out = args.out or str(REPO_ROOT / "training"
                          / f"RESULTS_top{TOP_N}{suffix}.md")

    paine = pd.read_csv(args.paine)
    paine["key"] = paine.player.map(norm_name)
    thresholds, detail = derive_thresholds(paine)
    print(f"thresholds derived from true top-{TOP_N} minutes:", thresholds)
    if args.no_min_mp:
        thresholds = {k: 0.0 for k in thresholds}
        print("  --no-min-mp: eligibility threshold and training MIN_MP both off")

    if args.stride_tuned:
        from stride_ablation import BEST_STRIDE, tuned_predictions
        print(f"refitting with per-target best strides {BEST_STRIDE} ...")
        ours = tuned_predictions(args.stride_datadir)
    else:
        print("refitting the combined model (total, offense, defense) ...")
        ours = our_predictions(args.datadir,
                               rs_min=0 if args.no_min_mp else None,
                               po_min=0 if args.no_min_mp else None)
    ours["key"] = ours.player.map(norm_name)
    ours["ours_sum"] = ours.ours_offense + ours.ours_defense

    df = paine.merge(
        ours[["key", "season", "split", "ours_total", "ours_offense",
              "ours_defense", "ours_sum"]],
        on=["key", "season", "split"], how="left")
    print(f"  merged rows: {len(df)}; with our predictions: "
          f"{df.ours_total.notna().sum()}; with Paine's: {df.eRT.notna().sum()}")

    report, summary, overall, skipped = [], [], {}, []
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
                if len(pool) < TOP_N * 1.2:
                    # a top-N over a pool barely bigger than N is trivially
                    # near-perfect for every system and says nothing
                    skipped.append({"season": season, "split": split,
                                    "target": target, "pool_n": int(len(pool))})
                    continue
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
                    for k in HITS_AT:
                        if k <= TOP_N and k <= len(pool):
                            tk = set(pool.nlargest(k, truth).player)
                            pk = set(pool.nlargest(k, col).player)
                            row[f"hits{k}::{name}"] = len(tk & pk)
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

    for sk in skipped:
        print(f"  SKIPPED {sk['target']} {sk['season']} {sk['split']}: "
              f"pool={sk['pool_n']} too small for a top-{TOP_N}")
    write_report(out, report, summary, overall, thresholds, detail, skipped,
                 no_min_mp=args.no_min_mp)
    json.dump({"top_n": TOP_N, "thresholds": thresholds,
               "threshold_detail": detail, "summary": summary,
               "skipped": skipped, "overall_regression": overall},
              open(Path(out).with_suffix(".json"), "w"), indent=2)
    print(f"\nwrote {out}")


def write_report(path, report, summary, overall, thresholds, detail, skipped=(),
                 no_min_mp=False):
    L = []
    A = L.append
    A(f"# Top-{TOP_N} leaderboards on the held-out seasons\n")
    A("True 538 RAPTOR vs. our models vs. Neil Paine's Estimated RAPTOR, for")
    A("2013-14 and 2014-15. Total, offense and defense are ranked separately.")
    A("These are the models trained on **all** data points — no starter or")
    A("near-zero filtering (see RESULTS_starters.md for why those filters lose).\n")
    if skipped:
        cells = sorted({(s_["season"], s_["split"], s_["pool_n"]) for s_ in skipped})
        A("> Skipped as degenerate: "
          + "; ".join(f"{a} {b} (pool {c})" for a, b, c in cells)
          + f". A top-{TOP_N} over a pool of that size is nearly the whole field,")
        A("> so every system scores near-perfectly and the comparison says nothing.\n")
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
    if no_min_mp:
        A("**No minutes filter is applied in this run** — every rated player is")
        A("ranked, and the training rows carry no minimum-minutes filter either.")
        A("The table below is retained for reference: it shows what a derived")
        A("threshold *would* have been.\n")
    A(f"Derived, not chosen: the **lowest minutes total among any true top-{TOP_N}**")
    A("player, taken across every season, split and target, so no genuine leader is")
    A("ruled ineligible.\n")
    A("| season | split | target | min mp in true top 20 | median mp | pool n | pool min mp |")
    A("|---|---|---|---|---|---|---|")
    for d in detail:
        A(f"| {d['season']} | {d['split']} | {d['target']} | "
          f"{d['min_mp_in_true_top20']:.0f} | {d['median_mp_in_true_top20']:.0f} | "
          f"{d['pool_n']} | {d['pool_min_mp']:.0f} |")
    if no_min_mp:
        A("\n**Applied here: no threshold (0 minutes, both splits).**\n")
    else:
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

    A(f"## Summary — true top-{TOP_N} members recovered (hits@{TOP_N})\n")
    for target in TRUTH:
        rows = [s for s in summary if s["target"] == target]
        names = [n for n in rows[0] if n.startswith("hits::")]
        A(f"**{target}**\n")
        A("| season | split | pool | " + " | ".join(n[6:] for n in names)
          + " | " + " | ".join(f"ρ {n[6:]}" for n in names) + " |")
        A("|---" * (3 + 2 * len(names)) + "|")
        for s in rows:
            A(f"| {s['season']} | {s['split']} | {s['pool_n']} | "
              + " | ".join(f"{s[n]}/{TOP_N}" for n in names) + " | "
              + " | ".join(f"{s['rho::' + n[6:]]:+.3f}" for n in names) + " |")
        tot = {n: sum(s[n] for s in rows) for n in names}
        A("| **all** | | | "
          + " | ".join(f"**{tot[n]}/{len(rows)*TOP_N}**" for n in names)
          + " | " + " | ".join("" for _ in names) + " |")
        A("")
        ks = [k for k in HITS_AT if k <= TOP_N
              and any(f"hits{k}::{n[6:]}" in rows[0] for n in names)]
        if len(ks) > 1:
            A(f"Precision@K for {target}, summed over {len(rows)} cells:\n")
            A("| K | " + " | ".join(n[6:] for n in names) + " |")
            A("|---" * (1 + len(names)) + "|")
            for k in ks:
                A(f"| {k} | " + " | ".join(
                    f"{sum(s.get(f'hits{k}::' + n[6:], 0) for s in rows)}/"
                    f"{len(rows)*k}" for n in names) + " |")
            A("")

    A("## How contested is the cutoff\n")
    A(f"Hits@{TOP_N} only asks whether a player lands on the correct side of an")
    A("arbitrary cutoff. Players within ±0.25 RAPTOR of the boundary value, per cell:\n")
    A(f"| season | split | target | rank-{TOP_N} value | gap to rank {TOP_N+1} | "
      "players within ±0.25 |")
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
    hits = {}
    for target in TRUTH:
        rows = [x for x in summary if x["target"] == target]
        if not rows:
            continue
        names = [k[6:] for k in rows[0] if k.startswith("hits::")]
        hits[target] = {n: (sum(x[f"hits::{n}"] for x in rows), len(rows) * TOP_N)
                        for n in names}

    if "total" in hits:
        ot = overall["total"]
        direct = ot["ours (direct total)"]
        summed = ot["ours (offense+defense)"]
        paine_t = ot["Paine (eRO+eRD)"]
        hd = hits["total"]["ours (direct total)"]
        hs = hits["total"]["ours (offense+defense)"]
        hp = hits["total"]["Paine (eRO+eRD)"]
        A("**Direct total vs. summing the halves.** Predicting `rap` directly and")
        A("summing our two part-models are near-interchangeable: R² "
          f"{direct['r2']:+.3f} vs {summed['r2']:+.3f}, ρ {direct['spearman']:+.3f} "
          f"vs {summed['spearman']:+.3f}, hits@{TOP_N} {hd[0]}/{hd[1]} vs "
          f"{hs[0]}/{hs[1]}.\n")
        A(f"**Against Paine on the total.** R² {direct['r2']:+.3f} vs "
          f"{paine_t['r2']:+.3f}, RMSE {direct['rmse']:.3f} vs {paine_t['rmse']:.3f}, "
          f"ρ {direct['spearman']:+.3f} vs {paine_t['spearman']:+.3f}; "
          f"hits@{TOP_N} {hd[0]}/{hd[1]} vs {hp[0]}/{hp[1]}.\n")
    for target in ("offense", "defense"):
        if target not in hits:
            continue
        o = overall[target]
        oname = [n for n in o if n.startswith("ours")][0]
        pname = [n for n in o if n.startswith("Paine")][0]
        ho, hp2 = hits[target][oname], hits[target][pname]
        A(f"**{target.capitalize()}.** ours R² {o[oname]['r2']:+.3f} / ρ "
          f"{o[oname]['spearman']:+.3f} / hits@{TOP_N} {ho[0]}/{ho[1]}; "
          f"Paine R² {o[pname]['r2']:+.3f} / ρ {o[pname]['spearman']:+.3f} / "
          f"hits@{TOP_N} {hp2[0]}/{hp2[1]}.\n")
    A("Read the precision@K tables above rather than a single cutoff: they show")
    A("where each system's advantage actually lives, and a hits count at one")
    A("arbitrary K is decided by hundredths of a point among near-tied players.\n")

    A("## Leaderboards\n")
    A("`[n]` after a predicted name is that player's *true* rank; ✓ means they are")
    A(f"genuinely in the true top {TOP_N}.\n")
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
