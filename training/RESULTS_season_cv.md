# Leave-one-season-out CV

Grouped CV (folds split by player-season) disagreed with the held-out seasons
three times running. Leave-one-season-out holds out a whole season per fold,
which is the generalisation the test set actually asks for.

## Fold sizes are wildly uneven

Training rows per season: 2015-16, 2016-17, 2017-18, 2018-19, 2019-20, 2020-21, 2021-22.
2020-21 alone is 8,320 of 13,063 rows — 64% — because it is the season whose
archived 538 captures happened to be aligned in bulk. So a pooled MAE across
folds is largely the 2020-21 MAE; `macro` gives each season one vote and is the
number to read.

**2022-23 contributes no training rows at all**, despite having 40 timestamps
with all four sources and correctly aligned labels. The label-season filter
drops a cell when the snapshot's 538 capture showed a different season, and for
2022-23 the stride-6 selection landed on snapshots where that is true. Same
class of gap as 2018-19 was — worth recovering separately.

## Does LOSO predict the test seasons better than grouped CV?

Across the four wowy representations, per target: correlation between each CV
scheme's MAE and the measured test MAE. Higher is better; the point of a CV
scheme is to rank configurations the way the test set will.

| target | scheme | Pearson r vs test MAE | Kendall tau |
|---|---|---:|---:|
| defense | grouped (player-season) | -0.791 | -0.667 |
| defense | LOSO macro | -0.622 | -1.000 |
| defense | LOSO whole-season cells | -0.402 | -0.333 |
| offense | grouped (player-season) | -0.925 | -0.667 |
| offense | LOSO macro | +0.348 | +0.000 |
| offense | LOSO whole-season cells | -0.299 | -0.333 |
| total | grouped (player-season) | +0.122 | +0.000 |
| total | LOSO macro | -0.114 | +0.000 |
| total | LOSO whole-season cells | -0.070 | +0.000 |

## Per-variant numbers

| target | wowy variant | grouped CV | LOSO macro | LOSO whole-cell | test MAE |
|---|---|---:|---:|---:|---:|
| total | on+off+diff | 1.7994 | 1.4724 | 1.2506 | 1.173 |
| total | diff only | 1.7775 | 1.4408 | 1.2145 | 1.188 |
| total | on+off | 1.8177 | 1.4762 | 1.2592 | 1.189 |
| total | diff+off | 1.7861 | 1.4440 | 1.2205 | 1.178 |
| offense | on+off+diff | 1.0748 | 0.8531 | 0.6976 | 0.650 |
| offense | diff only | 1.0684 | 0.8512 | 0.6931 | 0.749 |
| offense | on+off | 1.0757 | 0.8444 | 0.6907 | 0.651 |
| offense | diff+off | 1.0726 | 0.8479 | 0.6970 | 0.653 |
| defense | on+off+diff | 1.3289 | 1.1464 | 1.0028 | 1.008 |
| defense | diff only | 1.3254 | 1.1440 | 1.0058 | 1.055 |
| defense | on+off | 1.3362 | 1.1588 | 1.0182 | 1.003 |
| defense | diff+off | 1.3290 | 1.1442 | 1.0074 | 1.020 |

## Per-season MAE, current representation

| target | 2015-16 | 2016-17 | 2017-18 | 2018-19 | 2019-20 | 2020-21 | 2021-22 |
|---|---|---|---|---|---|---|---|
| total | 1.299 | 1.315 | 1.274 | 1.235 | 1.130 | 1.856 | 2.198 |
| offense | 0.839 | 0.697 | 0.682 | 0.649 | 0.621 | 1.133 | 1.351 |
| defense | 1.137 | 1.081 | 0.979 | 0.972 | 0.845 | 1.322 | 1.689 |
