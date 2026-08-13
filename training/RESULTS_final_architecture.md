# Canonical RAPTOR architecture benchmark

All predictions below are out-of-fold across ten complete regular seasons (2013-14 through 2022-23), with a 1,065-minute eligibility floor. Paine's published Estimated RAPTOR was fit on these seasons, so his side of the comparison is in-sample. This is a post-selection estimate: the fixed architecture was chosen in earlier experiments on this corpus, so it is not a pristine external test.

Common pool: **2,238 player-seasons** (2153 exact-name and 85 fuzzy-name matches; 4 eligible rows unmatched).

## Total

| system | n | RMSE | MAE | R² | Pearson | Spearman | dev@10 | tau@10 | hits@10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours O+D OOF | 2238 | 1.051 | 0.800 | +0.854 | +0.925 | +0.913 | 3.27 | +0.453 | 81/100 |
| direct-total OOF | 2238 | 1.187 | 0.911 | +0.814 | +0.903 | +0.889 | 4.21 | +0.391 | 80/100 |
| structural fixed OOF | 2238 | 1.755 | 1.369 | +0.593 | +0.776 | +0.744 | 9.85 | +0.338 | 59/100 |
| Paine published | 2238 | 1.299 | 1.017 | +0.777 | +0.882 | +0.862 | 4.71 | +0.418 | 73/100 |

Season-cluster bootstrap, RMSE(ours) − RMSE(Paine): **-0.248** (95% CI -0.294 to -0.198).

## Offense

| system | n | RMSE | MAE | R² | Pearson | Spearman | dev@10 | tau@10 | hits@10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours hybrid OOF | 2238 | 0.645 | 0.475 | +0.912 | +0.955 | +0.940 | 1.70 | +0.578 | 91/100 |
| structural fixed OOF | 2238 | 1.329 | 1.035 | +0.625 | +0.794 | +0.735 | 10.99 | +0.431 | 63/100 |
| Paine published | 2238 | 0.916 | 0.716 | +0.822 | +0.907 | +0.875 | 4.68 | +0.511 | 71/100 |

Season-cluster bootstrap, RMSE(ours) − RMSE(Paine): **-0.271** (95% CI -0.301 to -0.244).

## Defense

| system | n | RMSE | MAE | R² | Pearson | Spearman | dev@10 | tau@10 | hits@10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours selected OOF | 2238 | 0.793 | 0.615 | +0.806 | +0.899 | +0.891 | 4.51 | +0.502 | 73/100 |
| old matched OOF | 2238 | 0.811 | 0.629 | +0.797 | +0.895 | +0.886 | 6.09 | +0.498 | 71/100 |
| structural fixed OOF | 2238 | 1.064 | 0.837 | +0.650 | +0.807 | +0.795 | 12.57 | +0.360 | 59/100 |
| Paine published | 2238 | 1.138 | 0.896 | +0.600 | +0.781 | +0.770 | 16.46 | +0.267 | 48/100 |

Season-cluster bootstrap, RMSE(ours) − RMSE(Paine): **-0.345** (95% CI -0.388 to -0.302).

The selected model has lower RMSE than Paine in **10/10 total**, **10/10 offense**, and **10/10 defense** season-level comparisons.

## Reproduction details

- Base features: 1141; offense adds 12 cell-relative, 20 opponent, and four structural hats.
- Defense adds 12 cell-relative and 8 nearest-defender features; its second head also receives two defensive component hats and their fixed 0.85/0.21 combination. Final defense is 60% direct / 40% hat-augmented.
- LightGBM members use seeds [0, 1, 2], a 0.75 tree / 0.25 RidgeCV blend, and the checked-in tuned parameters.
- Effective rounds (full-season regime): total 383, offense 394, defense 216.
- End-to-end benchmark wall time: 148.4 seconds on the machine that generated this report.

Artifacts: `RESULTS_final_architecture.csv` contains every OOF prediction; the adjacent JSON contains metrics, fold results, parameters, and bootstrap intervals.
