# Train/test only, audited penalty, small nets

X=(15469, 1140). Every non-test row trains (13063 rows); no season is held out for
validation. LightGBM's round count comes from 5-fold CV inside the training
rows, so the 692 test rows are still never consulted during fitting.

`rho` is Spearman over all test rows. `rho@30` is Spearman *within the true
top 30 of each cell* — a much harder statistic, and not comparable to `rho`.

## offense

| model | weights | R² | RMSE | MAE | rho | hits@20 | hits@30 | rho@30 |
|---|---|---|---|---|---|---|---|---|
| lgbm+ridge blend | none | +0.870 | 0.870 | 0.658 | +0.930 | 65/80 | 99/120 | +0.803 |
| lightgbm | positive-3x | +0.846 | 0.948 | 0.723 | +0.912 | 64/80 | 98/120 | +0.789 |
| lightgbm | none | +0.846 | 0.948 | 0.729 | +0.915 | 63/80 | 99/120 | +0.775 |
| lightgbm | linear-0.5 | +0.840 | 0.966 | 0.743 | +0.906 | 63/80 | 97/120 | +0.773 |
| lightgbm | top30-cell-5x | +0.837 | 0.975 | 0.738 | +0.908 | 62/80 | 97/120 | +0.756 |
| lightgbm | top30-cell-10x | +0.834 | 0.985 | 0.742 | +0.901 | 64/80 | 97/120 | +0.770 |
| lightgbm | top20-cell-10x | +0.823 | 1.017 | 0.773 | +0.899 | 63/80 | 95/120 | +0.750 |
| mlp-256-64-16 | none | +0.819 | 1.028 | 0.757 | +0.896 | 60/80 | 99/120 | +0.663 |
| mlp-128-64 | none | +0.808 | 1.057 | 0.779 | +0.889 | 61/80 | 96/120 | +0.681 |
| mlp-64 | none | +0.792 | 1.102 | 0.822 | +0.880 | 59/80 | 98/120 | +0.658 |

## defense

| model | weights | R² | RMSE | MAE | rho | hits@20 | hits@30 | rho@30 |
|---|---|---|---|---|---|---|---|---|
| lgbm+ridge blend | none | +0.666 | 1.347 | 0.986 | +0.824 | 53/80 | 81/120 | +0.587 |
| lightgbm | none | +0.645 | 1.387 | 1.020 | +0.811 | 57/80 | 79/120 | +0.591 |
| mlp-256-64-16 | none | +0.619 | 1.437 | 1.045 | +0.787 | 51/80 | 78/120 | +0.534 |
| mlp-128-64 | none | +0.596 | 1.480 | 1.098 | +0.772 | 50/80 | 76/120 | +0.522 |
| lightgbm | positive-3x | +0.564 | 1.539 | 1.174 | +0.804 | 55/80 | 84/120 | +0.566 |
| lightgbm | linear-0.5 | +0.543 | 1.575 | 1.210 | +0.791 | 52/80 | 81/120 | +0.523 |
| lightgbm | top30-cell-5x | +0.531 | 1.595 | 1.239 | +0.805 | 53/80 | 83/120 | +0.524 |
| mlp-64 | none | +0.516 | 1.620 | 1.183 | +0.727 | 47/80 | 75/120 | +0.461 |
| lightgbm | top20-cell-10x | +0.480 | 1.679 | 1.330 | +0.793 | 52/80 | 81/120 | +0.475 |
| lightgbm | top30-cell-10x | +0.391 | 1.817 | 1.465 | +0.789 | 51/80 | 81/120 | +0.485 |
