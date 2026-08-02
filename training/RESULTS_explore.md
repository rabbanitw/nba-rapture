# Exploration: feature dropout, architectures, gap-inflating labels

Regular season only, all features unless stated, tuned params, seed-averaged. Ranked by `dev@10` — mean |true rank − projected position| over the projected top ten. Lower is better.

## defense

| kind | variant | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 |
|---|---|---:|---:|---:|---:|---:|---:|
| ablation | without wowy_on | 4.35 | 9.20 | +0.378 | +0.379 | 0.744 | 15/20 |
| dropout | top 200 by gain | 4.45 | 7.60 | +0.400 | +0.447 | 0.745 | 14/20 |
| ablation | without ctx | 4.90 | 9.32 | +0.378 | +0.389 | 0.737 | 14/20 |
| ablation | without wowy_off | 5.00 | 9.90 | +0.311 | +0.389 | 0.743 | 14/20 |
| baseline | all 1140 | 5.10 | 11.12 | +0.400 | +0.389 | 0.735 | 13/20 |
| dropout | top 100 by gain | 5.10 | 8.60 | +0.400 | +0.416 | 0.761 | 13/20 |
| dropout | top 400 by gain | 5.10 | 10.78 | +0.333 | +0.411 | 0.737 | 13/20 |
| dropout | top 800 by gain | 5.10 | 9.50 | +0.400 | +0.411 | 0.740 | 13/20 |
| architecture | xgboost | 5.10 | 7.97 | +0.378 | +0.363 | 0.734 | 13/20 |
| ablation | without wowy_diff | 5.15 | 9.45 | +0.356 | +0.437 | 0.741 | 14/20 |
| architecture | lgbm linear_tree | 5.70 | 8.88 | +0.333 | +0.389 | 0.716 | 13/20 |
| ablation | without tracking | 5.75 | 10.97 | +0.422 | +0.432 | 0.746 | 13/20 |
| gap label | neg_log_rank | 6.05 | 10.60 | +0.378 | +0.453 | 4.456 | 14/20 |
| gap label | pow2 | 6.35 | 10.70 | +0.267 | +0.326 | 1.744 | 11/20 |
| architecture | lgbm dart | 6.40 | 9.43 | +0.333 | +0.379 | 0.835 | 12/20 |
| dropout | top 50 by gain | 6.65 | 10.20 | +0.333 | +0.416 | 0.775 | 14/20 |
| gap label | pow1.5 | 6.85 | 10.78 | +0.222 | +0.337 | 1.098 | 11/20 |
| gap label | exp_rank | 7.35 | 17.40 | +0.489 | +0.511 | 1.403 | 14/20 |
| ablation | without pbp | 12.40 | 9.85 | +0.356 | +0.332 | 0.778 | 11/20 |

## total

| kind | variant | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 |
|---|---|---:|---:|---:|---:|---:|---:|
| ablation | without wowy_diff | 4.65 | 7.82 | +0.644 | +0.621 | 0.884 | 16/20 |
| architecture | lgbm linear_tree | 4.65 | 8.10 | +0.578 | +0.605 | 0.818 | 14/20 |
| baseline | all 1140 | 4.70 | 7.45 | +0.556 | +0.616 | 0.859 | 16/20 |
| dropout | top 200 by gain | 4.70 | 6.50 | +0.556 | +0.605 | 0.863 | 16/20 |
| ablation | without wowy_on | 4.80 | 7.65 | +0.511 | +0.584 | 0.853 | 15/20 |
| architecture | xgboost | 4.80 | 6.68 | +0.533 | +0.605 | 0.866 | 15/20 |
| dropout | top 100 by gain | 4.90 | 6.80 | +0.556 | +0.574 | 0.849 | 15/20 |
| dropout | top 800 by gain | 4.90 | 7.62 | +0.556 | +0.611 | 0.866 | 16/20 |
| ablation | without ctx | 4.90 | 7.50 | +0.511 | +0.595 | 0.876 | 15/20 |
| dropout | top 400 by gain | 5.00 | 8.35 | +0.511 | +0.595 | 0.871 | 15/20 |
| ablation | without wowy_off | 5.00 | 7.67 | +0.578 | +0.579 | 0.860 | 15/20 |
| architecture | lgbm dart | 5.00 | 7.65 | +0.489 | +0.537 | 0.915 | 15/20 |
| dropout | top 50 by gain | 5.60 | 11.00 | +0.533 | +0.584 | 0.861 | 14/20 |
| ablation | without tracking | 5.65 | 7.88 | +0.600 | +0.632 | 0.873 | 15/20 |
| gap label | pow1.5 | 5.70 | 9.72 | +0.600 | +0.558 | 2.292 | 14/20 |
| gap label | pow2 | 6.75 | 10.65 | +0.622 | +0.495 | 7.820 | 14/20 |
| gap label | neg_log_rank | 8.85 | 10.12 | +0.533 | +0.574 | 4.707 | 13/20 |
| ablation | without pbp | 9.15 | 12.22 | +0.511 | +0.479 | 1.056 | 12/20 |
| gap label | exp_rank | 11.60 | 11.93 | +0.467 | +0.505 | 2.107 | 12/20 |
