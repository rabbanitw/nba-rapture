# Alternative label formulations

A leaderboard needs the ordering inside a cell, and RAPTOR is only one way to
encode it. Each label below is a within-cell monotone transform, so the ranking
task is unchanged and only the loss geometry moves.

Regular season only, all features, blend of tuned LightGBM + RidgeCV.

`MAE*` is measured after mapping every prediction back to RAPTOR units by
within-cell rank through the training distribution's quantile function — applied
identically to every label including the baseline, so the column compares like
with like rather than comparing output scales. Ranking columns need no mapping.

## total

| label | MAE* | rho | hits@20 | hits@30 | tau30 |
|---|---:|---:|---:|---:|---:|
| cell_pct | 1.582 | +0.905 | 28/40 | 46/60 | +0.589 |
| signed_sqrt | 1.582 | +0.907 | 28/40 | 43/60 | +0.584 |
| cell_rankit | 1.586 | +0.909 | 29/40 | 44/60 | +0.577 |
| raptor | 1.562 | +0.910 | 30/40 | 46/60 | +0.575 |
| winsor | 1.569 | +0.910 | 29/40 | 47/60 | +0.566 |
| cell_z | 1.575 | +0.909 | 28/40 | 45/60 | +0.533 |

## offense

| label | MAE* | rho | hits@20 | hits@30 | tau30 |
|---|---:|---:|---:|---:|---:|
| raptor | 1.080 | +0.939 | 32/40 | 48/60 | +0.736 |
| signed_sqrt | 1.089 | +0.933 | 31/40 | 51/60 | +0.729 |
| cell_rankit | 1.075 | +0.941 | 32/40 | 50/60 | +0.717 |
| cell_pct | 1.090 | +0.935 | 31/40 | 50/60 | +0.690 |
| winsor | 1.085 | +0.941 | 31/40 | 48/60 | +0.685 |
| cell_z | 1.087 | +0.940 | 32/40 | 45/60 | +0.680 |

## defense

| label | MAE* | rho | hits@20 | hits@30 | tau30 |
|---|---:|---:|---:|---:|---:|
| raptor | 1.068 | +0.871 | 28/40 | 43/60 | +0.487 |
| cell_z | 1.055 | +0.876 | 28/40 | 46/60 | +0.469 |
| winsor | 1.068 | +0.872 | 27/40 | 43/60 | +0.469 |
| signed_sqrt | 1.078 | +0.869 | 27/40 | 41/60 | +0.453 |
| cell_rankit | 1.061 | +0.876 | 27/40 | 43/60 | +0.451 |
| cell_pct | 1.097 | +0.865 | 26/40 | 41/60 | +0.377 |
