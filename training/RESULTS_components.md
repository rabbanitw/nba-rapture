# RAPTOR-faithful components, cell-relative features, priors

Offense and defense only, regular season only, all features, seed-averaged.
See the module docstring and RESULTS_raptor_research.md for the rationale.

## defense

| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| prior-season | 4.85 | 9.60 | +0.444 | +0.463 | 0.735 | 14/20 | 27/40 |
| cell-relative | 5.00 | 8.07 | +0.378 | +0.437 | 0.696 | 14/20 | 29/40 |
| direct | 5.10 | 11.12 | +0.400 | +0.389 | 0.735 | 13/20 | 27/40 |
| comp+direct | 5.10 | 11.07 | +0.311 | +0.400 | 0.781 | 12/20 | 27/40 |
| catboost | 5.25 | 9.22 | +0.378 | +0.389 | 0.734 | 13/20 | 29/40 |
| components | 5.80 | 13.30 | +0.222 | +0.400 | 0.879 | 13/20 | 28/40 |

## offense

| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| components | 1.20 | 3.48 | +0.778 | +0.732 | 0.607 | 16/20 | 34/40 |
| comp+direct | 1.25 | 3.60 | +0.756 | +0.726 | 0.542 | 16/20 | 34/40 |
| direct | 1.95 | 3.95 | +0.689 | +0.705 | 0.532 | 16/20 | 32/40 |
| catboost | 1.95 | 3.77 | +0.778 | +0.695 | 0.548 | 16/20 | 34/40 |
| prior-season | 2.05 | 3.92 | +0.711 | +0.695 | 0.526 | 16/20 | 32/40 |
| cell-relative | 2.15 | 3.58 | +0.667 | +0.716 | 0.489 | 16/20 | 32/40 |
