# Architectures and top-end weighting

Built on the corrected dataset including 2018-19: X=(15469, 1140), 11860 fit / 1203 validation / 692 test rows.

Selection is on the validation seasons; the tables report the held-out
2013-14 and 2014-15 rows. `hits@k` counts how many of the true top k appear in
the predicted top k, summed over the four test cells. `rho@30` is Spearman
within the true top 30. `rmse+` is RMSE over positive-RAPTOR rows.

## total

| architecture | weights | test R² | RMSE | rmse+ | hits@20 | hits@30 | rho@30 |
|---|---|---|---|---|---|---|---|
| lightgbm | positive-3x | +0.710 | 1.852 | 1.925 | 55/80 | 89/120 | +0.576 |
| lightgbm | linear-1.0 | +0.694 | 1.902 | 1.854 | 54/80 | 88/120 | +0.549 |
| hist-gbm | linear-0.5 | +0.674 | 1.963 | 2.094 | 52/80 | 87/120 | +0.529 |
| lightgbm | none | +0.704 | 1.872 | 1.891 | 55/80 | 86/120 | +0.587 | **←val pick**
| lightgbm | linear-0.5 | +0.697 | 1.894 | 1.918 | 54/80 | 85/120 | +0.582 |
| hist-gbm | linear-0.25 | +0.688 | 1.920 | 2.030 | 55/80 | 84/120 | +0.519 |
| hist-gbm | linear-1.0 | +0.663 | 1.997 | 2.095 | 53/80 | 84/120 | +0.578 |
| hist-gbm | positive-3x | +0.678 | 1.950 | 2.088 | 54/80 | 84/120 | +0.566 |
| lightgbm | linear-0.25 | +0.707 | 1.862 | 1.891 | 55/80 | 83/120 | +0.581 |
| hist-gbm | none | +0.705 | 1.866 | 1.962 | 53/80 | 82/120 | +0.562 |
| random-forest | positive-3x | +0.597 | 2.184 | 2.369 | 54/80 | 82/120 | +0.507 |
| ridge | none | +0.528 | 2.362 | 2.311 | 48/80 | 82/120 | +0.568 |
| random-forest | none | +0.552 | 2.300 | 2.674 | 50/80 | 81/120 | +0.511 |
| random-forest | linear-0.5 | +0.619 | 2.123 | 2.234 | 50/80 | 80/120 | +0.489 |
| extra-trees | none | +0.511 | 2.403 | 2.796 | 52/80 | 80/120 | +0.519 |
| extra-trees | linear-0.5 | +0.553 | 2.298 | 2.490 | 49/80 | 80/120 | +0.439 |
| ridge | linear-0.25 | +0.409 | 2.642 | 2.499 | 48/80 | 80/120 | +0.538 |
| ridge | linear-1.0 | +0.381 | 2.705 | 2.440 | 45/80 | 80/120 | +0.501 |
| random-forest | linear-1.0 | +0.611 | 2.145 | 2.145 | 50/80 | 79/120 | +0.480 |
| extra-trees | positive-3x | +0.537 | 2.340 | 2.618 | 50/80 | 79/120 | +0.453 |
| ridge | positive-3x | +0.551 | 2.304 | 2.152 | 48/80 | 79/120 | +0.520 |
| random-forest | linear-0.25 | +0.604 | 2.163 | 2.372 | 50/80 | 78/120 | +0.490 |
| ridge | linear-0.5 | +0.401 | 2.660 | 2.463 | 48/80 | 78/120 | +0.527 |
| extra-trees | linear-0.25 | +0.549 | 2.309 | 2.556 | 51/80 | 77/120 | +0.485 |
| extra-trees | linear-1.0 | +0.568 | 2.260 | 2.384 | 52/80 | 77/120 | +0.473 |
