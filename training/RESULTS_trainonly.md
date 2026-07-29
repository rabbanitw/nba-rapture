# Train/test only, audited penalty, small nets

X=(15469, 1140). Every non-test row trains (13063 rows); no season is held out for
validation. LightGBM's round count comes from 5-fold CV inside the training
rows, so the 692 test rows are still never consulted during fitting.

`rho` is Spearman over all test rows. `rho@30` is Spearman *within the true
top 30 of each cell* — a much harder statistic, and not comparable to `rho`.

## total

| model | weights | R² | RMSE | MAE | rho | hits@20 | hits@30 | rho@30 |
|---|---|---|---|---|---|---|---|---|
| lgbm+ridge blend | none | +0.784 | 1.597 | 1.170 | +0.889 | 57/80 | 94/120 | +0.666 |
| lightgbm | none | +0.757 | 1.694 | 1.242 | +0.875 | 60/80 | 91/120 | +0.645 |
| lightgbm | positive-3x | +0.745 | 1.735 | 1.306 | +0.870 | 57/80 | 91/120 | +0.593 |
| lightgbm | top30-cell-5x | +0.739 | 1.758 | 1.305 | +0.861 | 58/80 | 85/120 | +0.635 |
| lightgbm | linear-0.5 | +0.731 | 1.783 | 1.333 | +0.858 | 55/80 | 87/120 | +0.598 |
| mlp-256-64-16 | none | +0.718 | 1.825 | 1.350 | +0.852 | 51/80 | 86/120 | +0.581 |
| lightgbm | top20-cell-10x | +0.715 | 1.835 | 1.359 | +0.849 | 53/80 | 84/120 | +0.602 |
| mlp-128-64 | none | +0.712 | 1.846 | 1.362 | +0.850 | 53/80 | 85/120 | +0.571 |
| lightgbm | top30-cell-10x | +0.692 | 1.907 | 1.445 | +0.843 | 53/80 | 82/120 | +0.572 |
| mlp-64 | none | +0.667 | 1.985 | 1.497 | +0.826 | 49/80 | 81/120 | +0.556 |
