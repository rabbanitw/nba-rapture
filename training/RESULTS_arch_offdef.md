# Architectures and top-end weighting

Built on the corrected dataset including 2018-19: X=(15469, 1140), 11860 fit / 1203 validation / 692 test rows.

Selection is on the validation seasons; the tables report the held-out
2013-14 and 2014-15 rows. `hits@k` counts how many of the true top k appear in
the predicted top k, summed over the four test cells. `rho@30` is Spearman
within the true top 30. `rmse+` is RMSE over positive-RAPTOR rows.

## offense

| architecture | weights | test R² | RMSE | rmse+ | hits@20 | hits@30 | rho@30 |
|---|---|---|---|---|---|---|---|
| lightgbm | positive-3x | +0.728 | 1.259 | 1.490 | 61/80 | 98/120 | +0.756 | **←val pick**
| hist-gbm | none | +0.722 | 1.274 | 1.521 | 60/80 | 98/120 | +0.780 |
| hist-gbm | positive-3x | +0.725 | 1.268 | 1.485 | 64/80 | 96/120 | +0.770 |
| lightgbm | none | +0.730 | 1.255 | 1.529 | 65/80 | 92/120 | +0.801 |
| random-forest | positive-3x | +0.605 | 1.518 | 1.897 | 61/80 | 92/120 | +0.715 |
| extra-trees | none | +0.546 | 1.628 | 2.069 | 62/80 | 92/120 | +0.683 |
| extra-trees | positive-3x | +0.585 | 1.555 | 1.947 | 60/80 | 92/120 | +0.683 |
| random-forest | none | +0.542 | 1.634 | 2.078 | 61/80 | 91/120 | +0.694 |
| ridge | positive-3x | +0.714 | 1.291 | 1.307 | 58/80 | 89/120 | +0.693 |
| ridge | none | +0.729 | 1.256 | 1.306 | 53/80 | 86/120 | +0.718 |

## defense

| architecture | weights | test R² | RMSE | rmse+ | hits@20 | hits@30 | rho@30 |
|---|---|---|---|---|---|---|---|
| lightgbm | none | +0.481 | 1.677 | 1.486 | 54/80 | 80/120 | +0.404 |
| extra-trees | positive-3x | +0.420 | 1.774 | 1.514 | 49/80 | 80/120 | +0.412 |
| random-forest | none | +0.419 | 1.776 | 1.552 | 50/80 | 79/120 | +0.446 |
| lightgbm | positive-3x | +0.460 | 1.712 | 1.504 | 49/80 | 78/120 | +0.413 | **←val pick**
| hist-gbm | none | +0.436 | 1.750 | 1.535 | 51/80 | 76/120 | +0.507 |
| hist-gbm | positive-3x | +0.410 | 1.789 | 1.587 | 45/80 | 76/120 | +0.387 |
| random-forest | positive-3x | +0.355 | 1.872 | 1.466 | 47/80 | 76/120 | +0.444 |
| extra-trees | none | +0.420 | 1.774 | 1.612 | 47/80 | 76/120 | +0.369 |
| ridge | none | +0.111 | 2.196 | 2.160 | 43/80 | 69/120 | +0.396 |
| ridge | positive-3x | +0.117 | 2.189 | 2.009 | 46/80 | 67/120 | +0.409 |
