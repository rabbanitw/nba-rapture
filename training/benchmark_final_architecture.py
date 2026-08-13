"""Canonical leakage-safe RAPTOR benchmark and Neil Paine comparison.

This is the canonical evaluation entry point for the selected pipeline and the
root-level reproduction report.  Every prediction is leave-one-season-out over
the ten finished regular seasons (2013-14 through 2022-23); the held-out season
is excluded from the component hats as well as the final learner.

Candidate promoted by the repository's prior disjoint-seed studies:

* offense: full matrix + cell-relative/opponent features + four structural
  RAPTOR component hats, 3-seed LightGBM/Ridge blend;
* defense: whole-season-matched full matrix + cell-relative and nearest-
  defender features, 3-seed LightGBM/Ridge blend;
* total: independently predicted offense + defense.

The script also scores the small structural reproduction, a direct-total
LightGBM baseline, and Paine's published Estimated RAPTOR on one common pool.
Paine's coefficients were fit on 2014-2023 RAPTOR, so his values are in-sample
for every season in this comparison while ours remain out-of-fold.

Run from the repository root::

    python training/benchmark_final_architecture.py \
        --paine-repo /path/to/Neil-Paine-1/NBA-elo
"""

from __future__ import annotations

import argparse
import difflib
import json
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare
from experiment_components import RELATIVE_COLS
from experiment_components import cell_relative as cellrel_features
from experiment_oppdef import engineered
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from raptor2.cv_hybrid import blend_members
from raptor2.structural import cell_relative
from raptor2.structural2 import ridge_hat
from raptor2.variables import build_variables
from raptor2.variables2 import build_onoff2
from train_rapture import TARGETS


TD = REPO_ROOT / "training"
FLOOR = 1065
SEEDS = (0, 1, 2)
STAMPS = {
    "2013-14": "20140715000000",
    "2014-15": "20150715000000",
    "2015-16": "20160715000000",
    "2016-17": "20170715000000",
    "2017-18": "20180715000000",
    "2018-19": "20190715000000",
    "2019-20": "20201101000000",
    "2020-21": "20210801000000",
    "2021-22": "20220715000000",
    "2022-23": "20230715000000",
}


def scalar_metrics(y, p):
    y, p = np.asarray(y, dtype=float), np.asarray(p, dtype=float)
    ok = np.isfinite(y) & np.isfinite(p)
    y, p = y[ok], p[ok]
    err = y - p
    denom = np.sum((y - y.mean()) ** 2)
    return {
        "n": int(len(y)),
        "rmse": float(np.sqrt(np.mean(err ** 2))),
        "mae": float(np.mean(np.abs(err))),
        "r2": float(1.0 - np.sum(err ** 2) / denom),
        "pearson": float(np.corrcoef(y, p)[0, 1]),
        "spearman": float(pd.Series(y).corr(pd.Series(p), method="spearman")),
    }


def all_metrics(y, p, seasons):
    out = scalar_metrics(y, p)
    out.update(score_cells(np.asarray(y), np.asarray(p), np.asarray(seasons)))
    return out


def _weighted_player_rows(g):
    w = pd.to_numeric(g["MP"], errors="coerce").fillna(0).to_numpy(float)
    if w.sum() <= 0:
        w = np.ones(len(g))
    return pd.Series({
        "paine_mp": float(pd.to_numeric(g["MP"], errors="coerce").fillna(0).sum()),
        "eRO": float(np.average(g["eRO"], weights=w)),
        "eRD": float(np.average(g["eRD"], weights=w)),
        "eRT": float(np.average(g["eRT"], weights=w)),
    })


def load_paine(path):
    """Load Paine's published values and collapse traded-player stints."""
    p = Path(path)
    if p.is_dir():
        p = p / "nba_estimated_RAPTOR.csv"
    df = pd.read_csv(p, encoding="latin-1")
    df = df[df["Year"].between(2014, 2023) & (df["Type"] == "RS")].copy()
    df["key"] = df["Player"].map(norm_name)
    df["season"] = df["Year"].map(lambda y: f"{int(y)-1}-{int(y)%100:02d}")
    return df.groupby(["key", "season"], as_index=False).apply(
        _weighted_player_rows, include_groups=False)


def join_paine(oof, paine):
    """Exact normalized-name join, then a conservative within-season fallback."""
    lookup = {(r.season, r.key): r for r in paine.itertuples(index=False)}
    keys_by_season = {
        s: sorted(g.key.unique()) for s, g in paine.groupby("season")
    }
    rows, exact, fuzzy = [], 0, 0
    for r in oof.itertuples(index=False):
        key = norm_name(r.player)
        hit = lookup.get((r.season, key))
        how = "exact"
        if hit is None:
            close = difflib.get_close_matches(
                key, keys_by_season.get(r.season, []), n=1, cutoff=0.88)
            if close:
                hit = lookup[(r.season, close[0])]
                how = "fuzzy"
        d = r._asdict()
        if hit is None:
            d.update({"eRO": np.nan, "eRD": np.nan, "eRT": np.nan,
                      "paine_mp": np.nan, "match": "none"})
        else:
            d.update({"eRO": hit.eRO, "eRD": hit.eRD, "eRT": hit.eRT,
                      "paine_mp": hit.paine_mp, "match": how})
            exact += how == "exact"
            fuzzy += how == "fuzzy"
        rows.append(d)
    return pd.DataFrame(rows), {"exact": exact, "fuzzy": fuzzy,
                                "unmatched": len(oof) - exact - fuzzy}


def cluster_bootstrap_rmse_delta(df, truth, ours, theirs, draws=10000, seed=538):
    """Season-cluster bootstrap CI for RMSE(ours) - RMSE(theirs)."""
    rng = np.random.default_rng(seed)
    seasons = np.array(sorted(df.season.unique()))
    vals = np.empty(draws)
    groups = {s: df[df.season == s] for s in seasons}
    for i in range(draws):
        picked = rng.choice(seasons, size=len(seasons), replace=True)
        a, b = [], []
        for s in picked:
            g = groups[s]
            y = g[truth].to_numpy(float)
            a.extend((y - g[ours].to_numpy(float)) ** 2)
            b.extend((y - g[theirs].to_numpy(float)) ** 2)
        vals[i] = np.sqrt(np.mean(a)) - np.sqrt(np.mean(b))
    return {"delta": float(np.sqrt(np.mean((df[truth] - df[ours]) ** 2))
                           - np.sqrt(np.mean((df[truth] - df[theirs]) ** 2))),
            "lo": float(np.quantile(vals, 0.025)),
            "hi": float(np.quantile(vals, 0.975))}


def clean_metrics(d):
    return {k: (int(v) if isinstance(v, (int, np.integer)) else float(v))
            for k, v in d.items()}


def safe_nanmedian(X):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="All-NaN slice encountered")
        med = np.nanmedian(X, axis=0)
    return np.where(np.isfinite(med), med, 0.0)


def run(data_dir, paine_path, out_prefix):
    started = time.perf_counter()
    X, feat, d = prepare(str(data_dir))
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(Path(data_dir) / "components.npz")
    defend = np.load(Path(data_dir) / "defend.npz", allow_pickle=True)
    opp = np.load(Path(data_dir) / "wowyopp.npz", allow_pickle=True)
    shot = np.load(Path(data_dir) / "shotdash.npz", allow_pickle=True)

    mp = d["mp"].astype(np.float64)
    cells = np.array([f"{t}|{s}" for t, s in
                      zip(d["timestamp"], d["season_type"])])
    seasons = np.array([str(s) for s in d["season"]])
    rs = d["season_type"] == "Regular season"
    labeled = rs & np.isin(d["timestamp"], list(STAMPS.values()))

    Z = cellrel_features(X, feat, cells, RELATIVE_COLS)
    ofields = [str(f) for f in opp["fields"]]
    Eopp, _ = engineered(opp["on_X"], opp["off_X"], ofields, cells)
    V = build_variables(X, feat, shot["R"], [str(x) for x in shot["rnames"]],
                        defend["E"], [str(x) for x in defend["enames"]], mp)
    OFF2, DEF2 = build_onoff2(X, feat, opp["on_X"], opp["off_X"], ofields,
                              cells, mp)
    blocks = {
        "box_o": (cell_relative(V["OB"], cells, mp), "rap_box_o"),
        "onoff_o": (cell_relative(OFF2, cells, mp), "rap_onoff_o"),
        "box_d": (cell_relative(V["DB"], cells, mp), "rap_box_d"),
        "onoff_d": (cell_relative(DEF2, cells, mp), "rap_onoff_d"),
    }

    tuned = json.loads((TD / "tuned_params.json").read_text())
    params = {k: dict(tuned[k]["params"], verbose=-1)
              for k in ("total", "offense", "defense")}
    rounds = {k: max(int(tuned[k]["rounds"]) // 3, 150)
              for k in ("total", "offense", "defense")}
    weight = np.sqrt(np.maximum(mp, 1.0))
    y_o = d[TARGETS["offense"]].astype(float)
    y_d = d[TARGETS["defense"]].astype(float)
    y_t = d[TARGETS["total"]].astype(float)
    rows = []

    for season, stamp in STAMPS.items():
        tr = labeled & (d["timestamp"] != stamp)
        te = labeled & (d["timestamp"] == stamp)
        hats = {}
        for tag, (M, label) in blocks.items():
            target = comp[label].astype(float)
            m = tr & np.isfinite(target)
            hats[tag] = ridge_hat(M[m], target[m], weight[m], M,
                                  [""] * M.shape[1], tag, quiet=True)
        H = np.column_stack([hats[k] for k in
                             ("box_o", "onoff_o", "box_d", "onoff_d")])

        Xo = np.hstack([X, Z, Eopp, H])
        Xd = np.hstack([X, Z, defend["E"]])
        med_o = safe_nanmedian(Xo[tr])
        med_d = safe_nanmedian(Xd[tr])
        med_t = safe_nanmedian(X[tr])

        p_o = blend_members(Xo[tr], y_o[tr], Xo[te], med_o,
                            params["offense"], rounds["offense"], SEEDS)
        p_d = blend_members(Xd[tr], y_d[tr], Xd[te], med_d,
                            params["defense"], rounds["defense"], SEEDS)
        p_direct = blend_members(X[tr], y_t[tr], X[te], med_t,
                                 params["total"], rounds["total"], SEEDS)
        p_struct_o = 0.85 * hats["box_o"][te] + 0.21 * hats["onoff_o"][te]
        p_struct_d = 0.85 * hats["box_d"][te] + 0.21 * hats["onoff_d"][te]

        for j, i in enumerate(np.where(te)[0]):
            rows.append({
                "player": str(d["player"][i]), "season": season,
                "mp": float(mp[i]), "y_offense": float(y_o[i]),
                "y_defense": float(y_d[i]), "y_total": float(y_t[i]),
                "ours_offense": float(p_o[j]), "ours_defense": float(p_d[j]),
                "ours_total": float(p_o[j] + p_d[j]),
                "direct_total": float(p_direct[j]),
                "struct_offense": float(p_struct_o[j]),
                "struct_defense": float(p_struct_d[j]),
                "struct_total": float(p_struct_o[j] + p_struct_d[j]),
            })
        print(f"{season}: train={int(tr.sum())} eval={int(te.sum())}", flush=True)

    oof = pd.DataFrame(rows)
    paine = load_paine(paine_path)
    joined, match_counts = join_paine(oof, paine)
    eligible = joined[joined.mp >= FLOOR].copy()
    common = eligible[np.isfinite(eligible.eRT)].copy()

    systems = {
        "offense": [("ours hybrid OOF", "ours_offense"),
                    ("structural fixed OOF", "struct_offense"),
                    ("Paine published", "eRO")],
        "defense": [("ours matched OOF", "ours_defense"),
                    ("structural fixed OOF", "struct_defense"),
                    ("Paine published", "eRD")],
        "total": [("ours O+D OOF", "ours_total"),
                  ("direct-total OOF", "direct_total"),
                  ("structural fixed OOF", "struct_total"),
                  ("Paine published", "eRT")],
    }
    truth = {k: f"y_{k}" for k in systems}
    results = {}
    for target, arms in systems.items():
        results[target] = {}
        for name, col in arms:
            results[target][name] = clean_metrics(all_metrics(
                common[truth[target]], common[col], common["season"]))

    per_season = {}
    for target, arms in systems.items():
        per_season[target] = {}
        for season, g in common.groupby("season"):
            per_season[target][season] = {
                name: clean_metrics(scalar_metrics(g[truth[target]], g[col]))
                for name, col in arms
            }

    bootstrap = {}
    for target, ours_col, paine_col in (
            ("offense", "ours_offense", "eRO"),
            ("defense", "ours_defense", "eRD"),
            ("total", "ours_total", "eRT")):
        bootstrap[target] = cluster_bootstrap_rmse_delta(
            common, truth[target], ours_col, paine_col)

    elapsed = time.perf_counter() - started
    eligible_match_counts = {
        "exact": int((eligible.match == "exact").sum()),
        "fuzzy": int((eligible.match == "fuzzy").sum()),
        "unmatched": int((eligible.match == "none").sum()),
    }
    metadata = {
        "protocol": "10-fold leave-one-season-out, regular season",
        "post_selection_caveat": (
            "Architecture and hyperparameters were selected in earlier experiments "
            "on these seasons; intervals are conditional on that selection."
        ),
        "seasons": list(STAMPS), "eligibility_minutes": FLOOR,
        "seeds": list(SEEDS), "ridge_weight": 0.25,
        "features": {"base": int(X.shape[1]), "cell_relative": int(Z.shape[1]),
                     "opponent_engineered": int(Eopp.shape[1]),
                     "defend_engineered": int(defend["E"].shape[1]),
                     "structural_hats": 4},
        "rounds": rounds, "params": params,
        "rows_eligible": int(len(eligible)), "rows_common": int(len(common)),
        "matches_all_rows": match_counts,
        "matches_eligible": eligible_match_counts,
        "wall_seconds": elapsed,
    }
    payload = {"metadata": metadata, "results": results,
               "per_season": per_season, "bootstrap_rmse_delta": bootstrap}

    out_prefix = Path(out_prefix)
    joined.to_csv(out_prefix.with_suffix(".csv"), index=False)
    out_prefix.with_suffix(".json").write_text(json.dumps(payload, indent=2))
    write_report(out_prefix.with_suffix(".md"), payload)
    return payload


def write_report(path, payload):
    m, results = payload["metadata"], payload["results"]
    lines = [
        "# Canonical RAPTOR architecture benchmark", "",
        "All predictions below are out-of-fold across ten complete regular seasons "
        "(2013-14 through 2022-23), with a 1,065-minute eligibility floor. Paine's "
        "published Estimated RAPTOR was fit on these seasons, so his side of the "
        "comparison is in-sample. This is a post-selection estimate: the fixed "
        "architecture was chosen in earlier experiments on this corpus, so it is "
        "not a pristine external test.", "",
        f"Common pool: **{m['rows_common']:,} player-seasons** "
        f"({m['matches_eligible']['exact']} exact-name and "
        f"{m['matches_eligible']['fuzzy']} fuzzy-name matches; "
        f"{m['matches_eligible']['unmatched']} eligible rows unmatched).", "",
    ]
    for target in ("total", "offense", "defense"):
        lines += [f"## {target.title()}", "",
                  "| system | n | RMSE | MAE | R² | Pearson | Spearman | dev@10 | "
                  "tau@10 | hits@10 |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for name, v in results[target].items():
            lines.append(
                f"| {name} | {v['n']} | {v['rmse']:.3f} | {v['mae']:.3f} | "
                f"{v['r2']:+.3f} | {v['pearson']:+.3f} | {v['spearman']:+.3f} | "
                f"{v['dev@10']:.2f} | {v['tau@10']:+.3f} | {v['hits@10']}/100 |")
        b = payload["bootstrap_rmse_delta"][target]
        lines += ["", f"Season-cluster bootstrap, RMSE(ours) − RMSE(Paine): "
                  f"**{b['delta']:+.3f}** (95% CI {b['lo']:+.3f} to {b['hi']:+.3f}).",
                  ""]

    ours_labels = {"total": "ours O+D OOF", "offense": "ours hybrid OOF",
                   "defense": "ours matched OOF"}
    wins = {
        target: sum(
            cell[ours_labels[target]]["rmse"] < cell["Paine published"]["rmse"]
            for cell in payload["per_season"][target].values()
        )
        for target in ours_labels
    }
    lines += [
        "The selected model has lower RMSE than Paine in "
        f"**{wins['total']}/10 total**, **{wins['offense']}/10 offense**, and "
        f"**{wins['defense']}/10 defense** season-level comparisons.", "",
    ]

    lines += [
        "## Reproduction details", "",
        f"- Base features: {m['features']['base']}; offense adds "
        f"{m['features']['cell_relative']} cell-relative, "
        f"{m['features']['opponent_engineered']} opponent, and four structural hats.",
        f"- Defense adds {m['features']['cell_relative']} cell-relative and "
        f"{m['features']['defend_engineered']} nearest-defender features.",
        f"- LightGBM members use seeds {m['seeds']}, a 0.75 tree / 0.25 RidgeCV "
        "blend, and the checked-in tuned parameters.",
        f"- Effective rounds (full-season regime): total {m['rounds']['total']}, "
        f"offense {m['rounds']['offense']}, defense {m['rounds']['defense']}.",
        f"- End-to-end benchmark wall time: {m['wall_seconds']:.1f} seconds on the "
        "machine that generated this report.", "",
        "Artifacts: `RESULTS_final_architecture.csv` contains every OOF prediction; "
        "the adjacent JSON contains metrics, fold results, parameters, and bootstrap "
        "intervals.", "",
    ]
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(TD / "data_fixed"))
    ap.add_argument("--paine-repo", required=True,
                    help="NBA-elo checkout or nba_estimated_RAPTOR.csv path")
    ap.add_argument("--out-prefix", default=str(TD / "RESULTS_final_architecture"))
    args = ap.parse_args()
    payload = run(args.datadir, args.paine_repo, args.out_prefix)
    print(json.dumps(payload["metadata"], indent=2), flush=True)


if __name__ == "__main__":
    main()
