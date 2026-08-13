"""Select and audit the defense ensemble from confirmed LOSO predictions.

This post-processing script does not fit player models.  It combines the two
three-seed, out-of-fold defense heads produced by ``experiment_defense_deep``:

* direct: the prior production feature set;
* hats: the same learner augmented by fold-fitted defensive box/on-off hats and
  their published ``0.85*box + 0.21*onoff`` combination.

The fixed production candidate is 60% direct + 40% hats.  The script measures
weight sensitivity, leave-one-season-out weight stability, season-level paired
uncertainty, and the downstream effect on total RAPTOR.
"""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd

from benchmark_final_architecture import scalar_metrics
from db import REPO_ROOT
from experiment_topk_rank import score_cells


TD = REPO_ROOT / "training"
HAT_WEIGHT = 0.40
RANK_FIRST_HAT_WEIGHT = 0.30


def clean(values):
    return {k: (int(v) if isinstance(v, (int, np.integer)) else float(v))
            for k, v in values.items()}


def metrics(y, p, seasons):
    out = scalar_metrics(y, p)
    out.update(score_cells(np.asarray(y), np.asarray(p), np.asarray(seasons)))
    return clean(out)


def cluster_rmse_delta(df, truth, a, b, draws=10000, seed=538):
    rng = np.random.default_rng(seed)
    seasons = np.array(sorted(df.season.unique()))
    groups = {s: df[df.season == s] for s in seasons}
    values = np.empty(draws)
    for i in range(draws):
        picked = rng.choice(seasons, len(seasons), replace=True)
        g = pd.concat([groups[s] for s in picked], ignore_index=True)
        values[i] = (np.sqrt(np.mean((g[truth] - g[a]) ** 2)) -
                     np.sqrt(np.mean((g[truth] - g[b]) ** 2)))
    point = (np.sqrt(np.mean((df[truth] - df[a]) ** 2)) -
             np.sqrt(np.mean((df[truth] - df[b]) ** 2)))
    return {"delta": float(point), "lo": float(np.quantile(values, 0.025)),
            "hi": float(np.quantile(values, 0.975))}


def paired_rank_test(deltas):
    """Exact one-sided sign-flip test plus season bootstrap interval."""
    deltas = np.asarray(deltas, dtype=float)
    observed = deltas.mean()
    null = np.array([(deltas * np.asarray(signs)).mean()
                     for signs in itertools.product((-1, 1), repeat=len(deltas))])
    rng = np.random.default_rng(538)
    boot = np.mean(rng.choice(deltas, (10000, len(deltas)), replace=True), axis=1)
    return {"mean_delta": float(observed),
            "bootstrap_lo": float(np.quantile(boot, 0.025)),
            "bootstrap_hi": float(np.quantile(boot, 0.975)),
            "one_sided_signflip_p": float(np.mean(null <= observed))}


def run(confirm_csv, canonical_csv, loss_json=None):
    d = pd.read_csv(confirm_csv)
    d["selected_defense"] = ((1.0 - HAT_WEIGHT) * d["baseline"] +
                             HAT_WEIGHT * d["hats_features"])
    d["rank_first_defense"] = (
        (1.0 - RANK_FIRST_HAT_WEIGHT) * d["baseline"] +
        RANK_FIRST_HAT_WEIGHT * d["hats_features"])
    seasons = d["season"].to_numpy()
    y = d["y_defense"].to_numpy(float)

    arms = {
        "old_direct": "baseline",
        "hat_augmented": "hats_features",
        "published_component_fixed": "component_fixed",
        "selected_60_40": "selected_defense",
        "rank_first_70_30": "rank_first_defense",
    }
    defense = {name: metrics(y, d[col], seasons) for name, col in arms.items()}
    per_season = {}
    for season, g in d.groupby("season"):
        per_season[season] = {
            name: metrics(g.y_defense, g[col], g.season)
            for name, col in arms.items()
        }

    grid = []
    for alpha in np.arange(0.0, 1.001, 0.05):
        pred = (1.0 - alpha) * d.baseline + alpha * d.hats_features
        row = metrics(y, pred, seasons)
        row["mean_normalized_dev_10_20_30"] = float(np.mean(
            [row["dev@10"] / 10, row["dev@20"] / 20, row["dev@30"] / 30]))
        grid.append({"hat_weight": float(alpha), **row})

    crossfit = np.empty(len(d))
    choices = {}
    for hold in sorted(d.season.unique()):
        train = d.season != hold
        test = ~train
        candidates = []
        for row in grid:
            alpha = row["hat_weight"]
            pred = ((1.0 - alpha) * d.loc[train, "baseline"] +
                    alpha * d.loc[train, "hats_features"])
            score = metrics(d.loc[train, "y_defense"], pred,
                            d.loc[train, "season"])
            candidates.append((score["dev@10"], score["rmse"], alpha))
        alpha = min(candidates)[2]
        choices[hold] = float(alpha)
        crossfit[test] = ((1.0 - alpha) * d.loc[test, "baseline"] +
                          alpha * d.loc[test, "hats_features"])
    crossfit_metrics = metrics(y, crossfit, seasons)

    rank_deltas = [per_season[s]["selected_60_40"]["dev@10"] -
                   per_season[s]["old_direct"]["dev@10"]
                   for s in sorted(per_season)]
    rank_test = paired_rank_test(rank_deltas)
    residual_correlation = float(np.corrcoef(
        y - d.baseline.to_numpy(), y - d.hats_features.to_numpy())[0, 1])

    canonical = pd.read_csv(canonical_csv)
    merged = canonical.merge(
        d[["player", "season", "selected_defense", "rank_first_defense"]],
        on=["player", "season"], how="inner")
    merged = merged[(merged.mp >= 1065) & merged.eRT.notna()].copy()
    merged["old_total"] = merged.ours_offense + merged.direct_defense
    merged["selected_total"] = merged.ours_offense + merged.selected_defense
    merged["rank_first_total"] = merged.ours_offense + merged.rank_first_defense
    total = {
        "old_total": metrics(merged.y_total, merged.old_total, merged.season),
        "selected_total": metrics(merged.y_total, merged.selected_total,
                                  merged.season),
        "rank_first_total": metrics(merged.y_total, merged.rank_first_total,
                                    merged.season),
        "paine_total": metrics(merged.y_total, merged.eRT, merged.season),
    }
    common_defense = {
        "old_defense": metrics(merged.y_defense, merged.direct_defense,
                               merged.season),
        "selected_defense": metrics(merged.y_defense, merged.selected_defense,
                                    merged.season),
        "rank_first_defense": metrics(merged.y_defense,
                                      merged.rank_first_defense, merged.season),
        "paine_defense": metrics(merged.y_defense, merged.eRD, merged.season),
    }
    bootstrap = {
        "defense_vs_old": cluster_rmse_delta(
            merged, "y_defense", "selected_defense", "direct_defense"),
        "defense_vs_paine": cluster_rmse_delta(
            merged, "y_defense", "selected_defense", "eRD"),
        "total_vs_old": cluster_rmse_delta(
            merged, "y_total", "selected_total", "old_total"),
        "total_vs_paine": cluster_rmse_delta(
            merged, "y_total", "selected_total", "eRT"),
    }
    out = merged[["player", "season", "mp", "y_offense", "y_defense",
                  "y_total", "ours_offense", "direct_defense",
                  "selected_defense", "rank_first_defense", "ours_total",
                  "old_total", "selected_total", "rank_first_total",
                  "eRD", "eRT"]].copy()
    exact_loss = None
    if loss_json and Path(loss_json).exists():
        exact_loss = json.loads(Path(loss_json).read_text(encoding="utf-8"))["results"]
    return {
        "metadata": {
            "hat_weight": HAT_WEIGHT, "direct_weight": 1.0 - HAT_WEIGHT,
            "rank_first_hat_weight": RANK_FIRST_HAT_WEIGHT,
            "rows_defense": int(len(d)), "rows_paine_common": int(len(merged)),
            "residual_correlation_direct_vs_hats": residual_correlation,
            "selection_caveat": "Weight selected after comparison on this corpus",
        },
        "defense_all_eligible": defense,
        "defense_paine_common": common_defense,
        "total_paine_common": total,
        "per_season": per_season,
        "weight_grid": grid,
        "crossfit_weight_choices": choices,
        "crossfit_metrics": crossfit_metrics,
        "rank_deviation_test": rank_test,
        "bootstrap_rmse_delta": bootstrap,
        "exact_structure_loss": exact_loss,
        "_oof": out,
    }


def write_report(path, payload):
    m = payload["metadata"]
    defense = payload["defense_paine_common"]
    total = payload["total_paine_common"]
    lines = [
        "# Selected defense architecture", "",
        "The selected defense rating is **60% direct matched defense + 40% "
        "structural-hat-augmented defense**. Both heads are three-seed "
        "LightGBM/Ridge ensembles and every structural hat is refit inside its "
        "outer season fold. The augmented head receives predicted box defense, "
        "predicted on/off defense, and their published 0.85/0.21 combination.", "",
        "The 40% coefficient is post-selected on this research corpus. A "
        "leave-one-season-at-a-time weight check chooses 0.40 in nine seasons and "
        "0.30 in one; its cross-fitted dev@10 is "
        f"{payload['crossfit_metrics']['dev@10']:.2f}.", "",
        "A ranking-first sensitivity variant uses 30% hats. It minimizes mean "
        "normalized deviation across k=10/20/30, while the selected 40% model "
        "has the best top-10 deviation and lower RMSE.", "",
        "## Common-pool results", "",
        "| Target / model | RMSE | MAE | Spearman | dev@10 | dev@20 | tau@10 | hits@10 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label, values in (
            ("Defense — old", defense["old_defense"]),
            ("Defense — selected", defense["selected_defense"]),
            ("Defense — rank-first sensitivity", defense["rank_first_defense"]),
            ("Defense — Paine", defense["paine_defense"]),
            ("Total — old", total["old_total"]),
            ("Total — selected", total["selected_total"]),
            ("Total — rank-first sensitivity", total["rank_first_total"]),
            ("Total — Paine", total["paine_total"])):
        lines.append(f"| {label} | {values['rmse']:.3f} | {values['mae']:.3f} | "
                     f"{values['spearman']:+.3f} | {values['dev@10']:.2f} | "
                     f"{values['dev@20']:.2f} | {values['tau@10']:+.3f} | "
                     f"{values['hits@10']}/100 |")
    r = payload["rank_deviation_test"]
    lines += [
        "", "## Stability", "",
        f"Defense dev@10 changes by **{r['mean_delta']:+.2f} ranks per season** "
        f"(season bootstrap 95% interval {r['bootstrap_lo']:+.2f} to "
        f"{r['bootstrap_hi']:+.2f}; exact one-sided sign-flip p="
        f"{r['one_sided_signflip_p']:.4f}).", "",
        f"Direct and augmented residual correlation is "
        f"{m['residual_correlation_direct_vs_hats']:.3f}; their remaining error "
        "diversity is what makes the ensemble outperform either head alone.", "",
    ]
    loss = payload.get("exact_structure_loss")
    if loss:
        best = loss["structure_penalty_l2_0.05"]
        lines += [
            "An exact squared-loss penalty toward the fold-fitted published "
            "structure was tested at λ=0.05, 0.10, and 0.25. The best RMSE was "
            f"{best['rmse']:.3f} at λ=0.05, but dev@10 worsened to "
            f"{best['dev@10']:.2f}; larger penalties worsened both. The published "
            "structure works better as a separate representation and ensemble "
            "member than as a pointwise penalty.", "",
        ]
    lines += [
        "Season-cluster bootstrap RMSE differences (selected minus comparator):", "",
        "| Comparison | Difference | 95% interval |", "|---|---:|---:|",
    ]
    for name, b in payload["bootstrap_rmse_delta"].items():
        lines.append(f"| {name} | {b['delta']:+.3f} | "
                     f"[{b['lo']:+.3f}, {b['hi']:+.3f}] |")
    lines += ["", "Full weight sweep, per-season results, and uncertainty values "
              "are stored in the adjacent JSON; row-level predictions are in the "
              "adjacent CSV.", ""]
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm-csv",
                        default=str(TD / "RESULTS_defense_deep_confirm.csv"))
    parser.add_argument("--canonical-csv",
                        default=str(TD / "RESULTS_final_architecture.csv"))
    parser.add_argument("--loss-json",
                        default=str(TD / "RESULTS_defense_deep_loss.json"))
    parser.add_argument("--out-prefix",
                        default=str(TD / "RESULTS_defense_deep_final"))
    args = parser.parse_args()
    payload = run(args.confirm_csv, args.canonical_csv, args.loss_json)
    oof = payload.pop("_oof")
    prefix = Path(args.out_prefix)
    oof.to_csv(prefix.with_suffix(".csv"), index=False)
    prefix.with_suffix(".json").write_text(json.dumps(payload, indent=2),
                                            encoding="utf-8")
    write_report(prefix.with_suffix(".md"), payload)
    print(json.dumps(payload["metadata"], indent=2))


if __name__ == "__main__":
    main()
