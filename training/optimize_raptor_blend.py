"""Fit box/on-off RAPTOR blend weights on training rows and score test rows.

The published structural blend has no intercept::

    RAPTOR = 0.85 * box_RAPTOR + 0.21 * onoff_RAPTOR

This script uses ordinary least squares to choose the two coefficients that
minimize total-RAPTOR RMSE on every row marked as training, without consulting
the test targets.  It also reports a complete-season-only sensitivity fit
because the repository's training partition contains many repeated in-season
snapshots whereas the test partition contains complete-season rows.

Run from the repository root::

    python training/optimize_raptor_blend.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from db import REPO_ROOT
from seasons import FULL_SEASON_SNAPSHOTS


PUBLISHED_WEIGHTS = np.array([0.85, 0.21], dtype=np.float64)


def fit_weights(X: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Return the no-intercept least-squares coefficients minimizing RMSE."""
    if X.ndim != 2 or X.shape[1] != 2 or len(X) != len(y):
        raise ValueError("X must have shape (n, 2) and align with y")
    valid = np.isfinite(y) & np.isfinite(X).all(axis=1)
    if valid.sum() < 2:
        raise ValueError("at least two finite rows are required")
    return np.linalg.lstsq(X[valid], y[valid], rcond=None)[0]


def scores(y: np.ndarray, pred: np.ndarray, mask: np.ndarray) -> dict:
    valid = mask & np.isfinite(y) & np.isfinite(pred)
    err = y[valid] - pred[valid]
    return {
        "n": int(valid.sum()),
        "rmse": float(np.sqrt(np.mean(err ** 2))),
        "mae": float(np.mean(np.abs(err))),
    }


def run(datadir: str | Path) -> dict:
    datadir = Path(datadir)
    d = np.load(datadir / "combined.npz", allow_pickle=True)
    c = np.load(datadir / "components.npz")

    n = len(d["y"])
    if any(len(c[k]) != n for k in (
            "rap_box_o", "rap_box_d", "rap_onoff_o", "rap_onoff_d")):
        raise ValueError("component labels are not row-aligned with combined.npz")

    y = d["y"].astype(np.float64)
    box = (c["rap_box_o"] + c["rap_box_d"]).astype(np.float64)
    onoff = (c["rap_onoff_o"] + c["rap_onoff_d"]).astype(np.float64)
    X = np.column_stack([box, onoff])

    test = d["test"].astype(bool)
    train = ~test
    regular = d["season_type"] == "Regular season"
    playoffs = d["season_type"] == "Playoffs"
    complete = train & np.isin(d["timestamp"], list(FULL_SEASON_SNAPSHOTS))

    learned = fit_weights(X[train], y[train])
    complete_learned = fit_weights(X[complete], y[complete])
    systems = {
        "published_0.85_0.21": PUBLISHED_WEIGHTS,
        "learned_all_training": learned,
        "learned_complete_seasons_sensitivity": complete_learned,
    }
    masks = {
        "train_all": train,
        "train_complete_seasons": complete,
        "test_all": test,
        "test_regular_season": test & regular,
        "test_playoffs": test & playoffs,
    }

    results = {}
    for name, weights in systems.items():
        pred = X @ weights
        results[name] = {
            "box_weight": float(weights[0]),
            "onoff_weight": float(weights[1]),
            "scores": {label: scores(y, pred, mask)
                       for label, mask in masks.items()},
        }

    # Locate the distribution mismatch without changing the requested fit.
    by_season = {}
    for season in sorted(set(str(s) for s in d["season"][train])):
        mask = train & (d["season"].astype(str) == season)
        by_season[season] = {
            "n": int(mask.sum()),
            "published_rmse": scores(y, X @ PUBLISHED_WEIGHTS, mask)["rmse"],
            "learned_rmse": scores(y, X @ learned, mask)["rmse"],
        }

    return {
        "protocol": {
            "objective": "unweighted training RMSE",
            "model": "no-intercept linear blend of total box and on/off RAPTOR",
            "test_used_for_fit": False,
            "n_train": int(train.sum()),
            "n_test": int(test.sum()),
            "test_seasons": sorted(set(str(s) for s in d["season"][test])),
        },
        "results": results,
        "training_diagnostic_by_season": by_season,
    }


def write_report(path: str | Path, payload: dict) -> None:
    r = payload["results"]
    pub = r["published_0.85_0.21"]
    learned = r["learned_all_training"]
    complete = r["learned_complete_seasons_sensitivity"]
    p = payload["protocol"]

    lines = [
        "# Training-optimized RAPTOR component weights", "",
        f"The requested fit uses all **{p['n_train']:,} training rows** and never "
        f"uses the **{p['n_test']:,} test rows** to choose weights. Ordinary least "
        "squares minimizes unweighted total-RAPTOR RMSE with no intercept.", "",
        "## Result", "",
        "| Blend | Box weight | On/off weight | All-row train RMSE | Test RMSE | Test MAE |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for label, item in (
            ("Published", pub),
            ("Learned on all training", learned),
            ("Complete-season sensitivity", complete)):
        lines.append(
            f"| {label} | {item['box_weight']:.6f} | "
            f"{item['onoff_weight']:.6f} | "
            f"{item['scores']['train_all']['rmse']:.6f} | "
            f"{item['scores']['test_all']['rmse']:.6f} | "
            f"{item['scores']['test_all']['mae']:.6f} |"
        )

    train_gain = 1 - (learned["scores"]["train_all"]["rmse"] /
                      pub["scores"]["train_all"]["rmse"])
    test_change = (learned["scores"]["test_all"]["rmse"] /
                   pub["scores"]["test_all"]["rmse"] - 1)
    lines += [
        "",
        f"The all-training optimum lowers training RMSE by **{100*train_gain:.1f}%** "
        f"but raises untouched-test RMSE by **{100*test_change:.1f}%**. It should "
        "therefore not replace the published weights.", "",
        "## Test split", "",
        "| Blend | Regular-season n | Regular-season RMSE | Playoff n | Playoff RMSE |",
        "|---|---:|---:|---:|---:|",
    ]
    for label, item in (("Published", pub), ("Learned", learned),
                        ("Complete-season sensitivity", complete)):
        rs = item["scores"]["test_regular_season"]
        po = item["scores"]["test_playoffs"]
        lines.append(f"| {label} | {rs['n']} | {rs['rmse']:.6f} | "
                     f"{po['n']} | {po['rmse']:.6f} |")

    lines += [
        "", "## Why the training optimum fails", "",
        "The literal training set contains repeated in-season snapshots and is "
        "dominated by 2020-21 and 2021-22 rows. Several early snapshots in those "
        "seasons do not satisfy the otherwise stable published component identity. "
        "The test set consists of complete-season rows. Fitting only complete-season "
        "training rows is a diagnostic—not the requested primary fit—and returns "
        f"{complete['box_weight']:.6f}/{complete['onoff_weight']:.6f}, with test "
        f"RMSE {complete['scores']['test_all']['rmse']:.6f}. This confirms that the "
        "published 0.85/0.21 blend generalizes and that the all-row result reflects "
        "snapshot distribution mismatch.", "",
        "The adjacent JSON contains every split metric and the season-level training "
        "diagnostic.", "",
    ]
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    parser.add_argument("--out-prefix",
                        default=str(REPO_ROOT / "training" / "RESULTS_optimized_blend"))
    args = parser.parse_args()

    payload = run(args.datadir)
    prefix = Path(args.out_prefix)
    prefix.with_suffix(".json").write_text(json.dumps(payload, indent=2),
                                            encoding="utf-8")
    write_report(prefix.with_suffix(".md"), payload)
    learned = payload["results"]["learned_all_training"]
    print(json.dumps({
        "box_weight": learned["box_weight"],
        "onoff_weight": learned["onoff_weight"],
        "train": learned["scores"]["train_all"],
        "test": learned["scores"]["test_all"],
    }, indent=2))


if __name__ == "__main__":
    main()
