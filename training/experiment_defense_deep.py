"""Defense-first, ranking-aware RAPTOR architecture exploration.

All candidates are evaluated with ten leave-one-season-out folds over the
complete regular-season cells used by ``benchmark_final_architecture.py``.
The experiment covers four ideas motivated by the published RAPTOR method:

* top-sensitive regression weights for projected leaderboard quality;
* LightGBM learning-to-rank objectives grouped by NBA season;
* nearest-defender context and a results-noise ablation;
* structural regularization toward 0.85*box + 0.21*on/off defense.

The default ``screen`` stage uses one fixed seed for breadth.  ``confirm`` uses
three disjoint seeds for the candidates promoted from the screen.  ``loss``
isolates the exact published-structure squared-loss penalty.

Run from the repository root::

    python training/experiment_defense_deep.py --stage screen
    python training/experiment_defense_deep.py --stage confirm
    python training/experiment_defense_deep.py --stage loss
"""

from __future__ import annotations

import argparse
import json
import time
import warnings
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import kendalltau
from sklearn.linear_model import RidgeCV

from benchmark_final_architecture import STAMPS, scalar_metrics
from db import REPO_ROOT
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS, masks_for
from experiment_components import cell_relative as cellrel_features
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from raptor2.structural import cell_relative
from raptor2.structural2 import ridge_hat
from raptor2.variables import build_variables
from raptor2.variables2 import build_onoff2
from train_rapture import TARGETS


TD = REPO_ROOT / "training"
FLOOR = 1065
BOX_WEIGHT = 0.85
ONOFF_WEIGHT = 0.21


def safe_median(X):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="All-NaN slice encountered")
        med = np.nanmedian(X, axis=0)
    return np.where(np.isfinite(med), med, 0.0)


def ridge_predict(Xtr, ytr, Xte, med, weights=None):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    model = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(
        (A - mu) / sd, ytr, sample_weight=weights)
    return model.predict((B - mu) / sd)


def regression_predict(Xtr, ytr, Xte, params, rounds, seeds,
                       weights=None, ridge_weight=0.25):
    med = safe_median(Xtr)
    ridge = ridge_predict(Xtr, ytr, Xte, med, weights)
    tree_preds = []
    for seed in seeds:
        p = dict(params, seed=seed, bagging_seed=seed,
                 feature_fraction_seed=seed)
        data = lgb.Dataset(Xtr, ytr, weight=weights)
        tree_preds.append(lgb.train(p, data, num_boost_round=rounds).predict(Xte))
    return (1.0 - ridge_weight) * np.mean(tree_preds, axis=0) + ridge_weight * ridge


def season_top_weights(y, seasons, strength, decay=20.0):
    """Smoothly spend more loss on the best defenders within each season."""
    out = np.ones(len(y), dtype=np.float64)
    for season in np.unique(seasons):
        m = seasons == season
        out[m] += strength * np.exp(-ranks(y[m]) / decay)
    return out


def relevance_labels(y, seasons, levels=10):
    """Integer within-season relevance: ``levels`` is best, zero is worst."""
    labels = np.empty(len(y), dtype=np.int32)
    for season in np.unique(seasons):
        m = seasons == season
        r = ranks(y[m]).astype(float)
        denom = max(len(r) - 1, 1)
        labels[m] = np.floor(levels * (1.0 - r / denom) + 1e-12).astype(int)
    return labels


def ranking_predict(Xtr, ytr, seasons_tr, Xte, params, rounds, seeds,
                    objective, truncation):
    """Fit a season-grouped ranker and linearly calibrate its raw score."""
    order = np.argsort(seasons_tr, kind="stable")
    seasons_ordered = seasons_tr[order]
    _, group = np.unique(seasons_ordered, return_counts=True)
    labels = relevance_labels(ytr, seasons_tr)[order]
    gain = list(range(11))
    test_scores, train_scores = [], []
    for seed in seeds:
        p = dict(params)
        p.update(objective=objective, metric="ndcg", seed=seed,
                 bagging_seed=seed, feature_fraction_seed=seed,
                 lambdarank_truncation_level=truncation,
                 label_gain=gain, verbose=-1)
        model = lgb.train(p, lgb.Dataset(Xtr[order], labels, group=group),
                          num_boost_round=rounds)
        train_scores.append(model.predict(Xtr))
        test_scores.append(model.predict(Xte))
    raw_tr = np.mean(train_scores, axis=0)
    raw_te = np.mean(test_scores, axis=0)
    calibrator = RidgeCV(alphas=np.logspace(-3, 3, 13)).fit(
        raw_tr.reshape(-1, 1), ytr)
    return calibrator.predict(raw_te.reshape(-1, 1)), raw_te


def defender_context(E, positions, cells):
    """Position-adjusted shot volume and cell-relative defender features."""
    cols = [0, 3, 4, 5, 6]
    out = np.full((len(E), len(cols) * 2), np.nan, dtype=np.float32)
    for cell in np.unique(cells):
        m = cells == cell
        for k, j in enumerate(cols):
            v = E[m, j].astype(float)
            ok = np.isfinite(v)
            if ok.sum() < 5:
                continue
            mu, sd = np.nanmean(v), np.nanstd(v)
            out[m, k] = (v - mu) / (sd or 1.0)
            pos_means = np.full(positions.shape[1], mu)
            for q in range(positions.shape[1]):
                qok = ok & (positions[m, q] > 0)
                if qok.sum() >= 3:
                    pos_means[q] = np.average(v[qok], weights=positions[m, q][qok])
            expected = positions[m] @ pos_means
            denom = positions[m].sum(axis=1)
            expected = np.divide(expected, denom, out=np.full_like(expected, mu),
                                 where=denom > 0)
            out[m, len(cols) + k] = v - expected
    return out


def remap_rank_distribution(base, rank_score, seasons, alpha):
    """Blend within-season ranks, preserving the baseline rating distribution."""
    out = np.empty(len(base), dtype=float)
    for season in np.unique(seasons):
        m = seasons == season
        combo = (1.0 - alpha) * ranks(base[m]) + alpha * ranks(rank_score[m])
        order = np.argsort(combo, kind="stable")
        values = np.sort(base[m])[::-1]
        out[np.where(m)[0][order]] = values
    return out


def clean(d):
    return {k: (int(v) if isinstance(v, (int, np.integer)) else float(v))
            for k, v in d.items()}


def evaluate(y, p, seasons):
    out = scalar_metrics(y, p)
    out.update(score_cells(y, p, seasons))
    return clean(out)


def prepare_research(data_dir):
    X, feat, d = prepare(str(data_dir))
    keep = [i for i, name in enumerate(feat) if name not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(Path(data_dir) / "components.npz")
    defend = np.load(Path(data_dir) / "defend.npz", allow_pickle=True)
    opp = np.load(Path(data_dir) / "wowyopp.npz", allow_pickle=True)
    shot = np.load(Path(data_dir) / "shotdash.npz", allow_pickle=True)
    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    seasons = d["season"].astype(str)
    labeled = ((d["season_type"] == "Regular season") &
               np.isin(d["timestamp"], list(STAMPS.values())))
    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    V = build_variables(X, feat, shot["R"], [str(x) for x in shot["rnames"]],
                        defend["E"], [str(x) for x in defend["enames"]], mp)
    ofields = [str(x) for x in opp["fields"]]
    _, DEF2 = build_onoff2(X, feat, opp["on_X"], opp["off_X"], ofields,
                           cells, mp)
    DB = cell_relative(V["DB"], cells, mp)
    OOd = cell_relative(DEF2, cells, mp)
    pos_idx = [feat.index(f"ctx|pos_{p}") for p in ("PG", "SG", "SF", "PF", "C")]
    C = defender_context(defend["E"], X[:, pos_idx], cells)
    return X, feat, d, comp, defend["E"], Z, C, DB, OOd, mp, cells, seasons, labeled


def run(data_dir, stage):
    started = time.perf_counter()
    (X, feat, d, comp, E, Z, C, DB, OOd, mp, cells, seasons,
     labeled) = prepare_research(data_dir)
    y = d[TARGETS["defense"]].astype(float)
    weight = np.sqrt(np.maximum(mp, 1.0))
    tuned = json.loads((TD / "tuned_params.json").read_text())["defense"]
    params = dict(tuned["params"], verbose=-1)
    rounds = max(int(tuned["rounds"]) // 3, 150)
    seeds = (0,) if stage == "screen" else (0, 1, 2)

    base = np.hstack([X, Z, E])
    faithful_E = E[:, [0, 3, 4, 5, 6]]
    faithful = np.hstack([X, Z, faithful_E])
    contextual = np.hstack([base, C])
    predictions = {}

    screen_arms = (
        "baseline", "faithful_no_pct", "defender_context", "hats_features",
        "structure_loss_0.10", "structure_loss_0.25",
        "top_weight_2", "top_weight_5",
        "lambdarank_10", "lambdarank_30", "rank_xendcg_30",
        "component_fixed",
    )
    confirm_arms = (
        "baseline", "hats_features", "structure_loss_0.10",
        "component_fixed", "component_fixed_l2", "component_fixed_masked",
    )
    loss_arms = (
        "baseline", "structure_penalty_l2_0.05",
        "structure_penalty_l2_0.10", "structure_penalty_l2_0.25",
    )
    arms = (screen_arms if stage == "screen" else
            confirm_arms if stage == "confirm" else loss_arms)
    for arm in arms:
        predictions[arm] = np.full(len(y), np.nan)
    for alpha in (0.25, 0.50, 0.75):
        predictions[f"rank_blend_{alpha:.2f}"] = np.full(len(y), np.nan)
    predictions["published_output_0.075"] = np.full(len(y), np.nan)
    predictions["direct_component_blend_0.25"] = np.full(len(y), np.nan)

    comp_box = comp["rap_box_d"].astype(float)
    comp_onoff = comp["rap_onoff_d"].astype(float)

    for fold, (season, stamp) in enumerate(STAMPS.items(), 1):
        tr = labeled & (d["timestamp"] != stamp)
        te = labeled & (d["timestamp"] == stamp)
        tr_seasons = seasons[tr]

        hats = []
        for M, target in ((DB, comp_box), (OOd, comp_onoff)):
            m = tr & np.isfinite(target)
            hats.append(ridge_hat(M[m], target[m], weight[m], M,
                                  [""] * M.shape[1], "", quiet=True))
        H = np.column_stack(hats)
        structural = BOX_WEIGHT * H[:, 0] + ONOFF_WEIGHT * H[:, 1]
        with_hats = np.hstack([base, H, structural[:, None]])

        fold_preds = {}
        if "baseline" in arms:
            fold_preds["baseline"] = regression_predict(
                base[tr], y[tr], base[te], params, rounds, seeds)
        if "faithful_no_pct" in arms:
            fold_preds["faithful_no_pct"] = regression_predict(
                faithful[tr], y[tr], faithful[te], params, rounds, seeds)
        if "defender_context" in arms:
            fold_preds["defender_context"] = regression_predict(
                contextual[tr], y[tr], contextual[te], params, rounds, seeds)
        if "hats_features" in arms:
            fold_preds["hats_features"] = regression_predict(
                with_hats[tr], y[tr], with_hats[te], params, rounds, seeds)
        for lam in (0.10, 0.25):
            name = f"structure_loss_{lam:.2f}"
            if name in arms:
                soft_target = (y[tr] + lam * structural[tr]) / (1.0 + lam)
                fold_preds[name] = regression_predict(
                    with_hats[tr], soft_target, with_hats[te], params, rounds,
                    seeds)
        # For squared error, fitting (y + lambda*s)/(1+lambda) is exactly
        # equivalent to minimizing MSE(f,y) + lambda*MSE(f,s).  Here s is the
        # fold-fitted published 0.85 box + 0.21 on/off structural estimate.
        for lam in (0.05, 0.10, 0.25):
            name = f"structure_penalty_l2_{lam:.2f}"
            if name in arms:
                l2params = dict(params, objective="l2")
                l2params.pop("alpha", None)
                penalized_target = ((y[tr] + lam * structural[tr]) /
                                    (1.0 + lam))
                fold_preds[name] = regression_predict(
                    with_hats[tr], penalized_target, with_hats[te], l2params,
                    rounds, seeds)
        for strength in (2, 5):
            name = f"top_weight_{strength}"
            if name in arms:
                sw = season_top_weights(y[tr], tr_seasons, strength)
                fold_preds[name] = regression_predict(
                    base[tr], y[tr], base[te], params, rounds, seeds, weights=sw)

        rank_raw = None
        for objective, trunc, name in (
                ("lambdarank", 10, "lambdarank_10"),
                ("lambdarank", 30, "lambdarank_30"),
                ("rank_xendcg", 30, "rank_xendcg_30")):
            if name in arms:
                calibrated, raw = ranking_predict(
                    base[tr], y[tr], tr_seasons, base[te], params,
                    max(rounds, 250), seeds, objective, trunc)
                fold_preds[name] = calibrated
                if name == "lambdarank_10":
                    rank_raw = raw

        if any(name in arms for name in (
                "component_fixed", "component_fixed_l2",
                "component_fixed_masked")):
            if "component_fixed" in arms:
                pb = regression_predict(base[tr], comp_box[tr], base[te], params,
                                        rounds, seeds)
                po = regression_predict(base[tr], comp_onoff[tr], base[te], params,
                                        rounds, seeds)
                fold_preds["component_fixed"] = BOX_WEIGHT * pb + ONOFF_WEIGHT * po
            if "component_fixed_l2" in arms:
                l2params = dict(params, objective="l2")
                l2params.pop("alpha", None)
                pb2 = regression_predict(base[tr], comp_box[tr], base[te],
                                         l2params, rounds, seeds)
                po2 = regression_predict(base[tr], comp_onoff[tr], base[te],
                                         l2params, rounds, seeds)
                fold_preds["component_fixed_l2"] = (BOX_WEIGHT * pb2 +
                                                      ONOFF_WEIGHT * po2)
            if "component_fixed_masked" in arms:
                box_mask, onoff_mask = masks_for(feat)
                Xbox = np.hstack([X[:, box_mask], Z, E])
                Xonoff = np.hstack([X[:, onoff_mask], Z])
                pbm = regression_predict(Xbox[tr], comp_box[tr], Xbox[te],
                                         params, rounds, seeds)
                pom = regression_predict(Xonoff[tr], comp_onoff[tr], Xonoff[te],
                                         params, rounds, seeds)
                fold_preds["component_fixed_masked"] = (BOX_WEIGHT * pbm +
                                                          ONOFF_WEIGHT * pom)

        for name, pred in fold_preds.items():
            predictions[name][te] = pred
        baseline_pred = fold_preds["baseline"]
        predictions["published_output_0.075"][te] = (
            0.925 * baseline_pred + 0.075 * structural[te])
        if "component_fixed" in fold_preds:
            predictions["direct_component_blend_0.25"][te] = (
                0.75 * baseline_pred + 0.25 * fold_preds["component_fixed"])
        if rank_raw is not None:
            for alpha in (0.25, 0.50, 0.75):
                predictions[f"rank_blend_{alpha:.2f}"][te] = remap_rank_distribution(
                    baseline_pred, rank_raw, seasons[te], alpha)
        print(f"{stage} {fold:02d}/10 {season}: train={tr.sum()} test={te.sum()}",
              flush=True)

    eligible = labeled & (mp >= FLOOR)
    results = {}
    per_season = {}
    for name, pred in predictions.items():
        ok = eligible & np.isfinite(pred)
        if not ok.any():
            continue
        results[name] = evaluate(y[ok], pred[ok], seasons[ok])
        per_season[name] = {
            season: evaluate(y[ok & (seasons == season)],
                             pred[ok & (seasons == season)],
                             seasons[ok & (seasons == season)])
            for season in STAMPS
        }
    elapsed = time.perf_counter() - started
    eligible_idx = np.where(eligible)[0]
    oof = pd.DataFrame({
        "player": d["player"][eligible_idx].astype(str),
        "season": seasons[eligible_idx], "mp": mp[eligible_idx],
        "y_defense": y[eligible_idx],
    })
    for name, pred in predictions.items():
        if np.isfinite(pred[eligible_idx]).any():
            oof[name] = pred[eligible_idx]
    return {
        "metadata": {
            "stage": stage, "protocol": "10-fold leave-one-season-out",
            "eligibility_minutes": FLOOR, "seeds": list(seeds),
            "rounds": rounds, "rows_eligible": int(eligible.sum()),
            "base_features": int(base.shape[1]),
            "selection_caveat": "Post-selection comparison on the research corpus",
            "wall_seconds": elapsed,
        },
        "results": results,
        "per_season": per_season,
        "_oof": oof,
    }


def write_report(path, payload):
    m, results = payload["metadata"], payload["results"]
    ordered = sorted(results, key=lambda x: (results[x]["dev@10"],
                                              results[x]["rmse"]))
    lines = [
        f"# Defense architecture exploration — {m['stage']}", "",
        "Ten complete regular seasons, leave-one-season-out, evaluated at the "
        f"{m['eligibility_minutes']}-minute floor. This is a post-selection research "
        "comparison, not a pristine future-season test.", "",
        "| Architecture | RMSE | MAE | Spearman | dev@10 | dev@20 | tau@10 | hits@10 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in ordered:
        v = results[name]
        lines.append(f"| {name} | {v['rmse']:.3f} | {v['mae']:.3f} | "
                     f"{v['spearman']:+.3f} | {v['dev@10']:.2f} | "
                     f"{v['dev@20']:.2f} | {v['tau@10']:+.3f} | "
                     f"{v['hits@10']}/100 |")
    lines += ["", f"Wall time: {m['wall_seconds']:.1f} seconds. Full parameters "
              "and per-season metrics are in the adjacent JSON.", ""]
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datadir", default=str(TD / "data_fixed"))
    parser.add_argument("--stage", choices=("screen", "confirm", "loss"),
                        default="screen")
    parser.add_argument("--out-prefix")
    args = parser.parse_args()
    prefix = Path(args.out_prefix) if args.out_prefix else \
        TD / f"RESULTS_defense_deep_{args.stage}"
    payload = run(args.datadir, args.stage)
    oof = payload.pop("_oof")
    oof.to_csv(prefix.with_suffix(".csv"), index=False)
    prefix.with_suffix(".json").write_text(json.dumps(payload, indent=2),
                                            encoding="utf-8")
    write_report(prefix.with_suffix(".md"), payload)
    print(json.dumps(payload["metadata"], indent=2), flush=True)


if __name__ == "__main__":
    main()
