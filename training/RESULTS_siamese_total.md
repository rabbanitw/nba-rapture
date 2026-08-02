# Total RAPTOR via summed branches

Total = offense + defense (exact in the labels up to 538's rounding), so
every model here predicts the two ends and is scored on the sum.

| model | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| production sum | 4.55 | 7.07 | +0.511 | +0.574 | 0.855 | 15/20 | 30/40 |
| lgbm-total | 4.70 | 7.45 | +0.556 | +0.616 | 0.859 | 16/20 | 29/40 |
| lgbm o+d sum | 4.85 | 8.60 | +0.511 | +0.532 | 0.854 | 14/20 | 28/40 |
| siamese-total | 10.10 | 10.18 | +0.489 | +0.542 | 0.934 | 13/20 | 24/40 |