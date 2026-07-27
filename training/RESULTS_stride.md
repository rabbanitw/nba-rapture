# Stride ablation

`--modern-stride k` keeps every kth modern (2020-21 onward) snapshot within
each season. The six full-season snapshots — our two test seasons and four
validation seasons — are kept at every stride, so **validation and test are
identical across the sweep** (1241 and 692 rows).

Training rows are additionally filtered to **MPG ≥ 5**, which
drops 3,654 garbage-time rows. That floor is deliberately low:
MPG ≥ 20 and ≥ 28 both cost accuracy in earlier runs, and so did removing the
minutes filter altogether, so only near-zero rows are excluded here.

Built once at stride 1 and subset in memory — stride-k is a strict subset of
stride-1, so this is equivalent to rebuilding per stride.

## total

| stride | modern snapshots | fit rows | rounds | val RMSE | test RMSE | test R² | test ρ |
|---|---|---|---|---|---|---|---|
| 1 | 245 | 63,340 | 353 | 2.454 | 1.917 | +0.689 | +0.842 |
| 2 | 123 | 31,490 | 394 | 2.430 | 1.841 | +0.713 | +0.862 |
| 3 | 82 | 20,881 | 496 | 2.411 ⬅ | 1.772 | +0.734 | +0.869 |
| 4 | 62 | 14,931 | 310 | 2.490 | 1.841 | +0.713 | +0.864 |
| 6 | 42 | 10,933 | 310 | 2.481 | 1.827 | +0.718 | +0.865 |
| 8 | 32 | 7,943 | 306 | 2.496 | 1.797 | +0.727 | +0.862 |
| 12 | 22 | 5,269 | 272 | 2.437 | 1.782 | +0.731 | +0.873 |
| 20 | 14 | 3,469 | 204 | 2.526 | 1.813 | +0.722 | +0.869 |

Validation picks **stride 3** (20,881 fit rows) → test R² +0.734, ρ +0.869.

## offense

| stride | modern snapshots | fit rows | rounds | val RMSE | test RMSE | test R² | test ρ |
|---|---|---|---|---|---|---|---|
| 1 | 245 | 63,340 | 393 | 1.493 | 1.153 | +0.772 | +0.885 |
| 2 | 123 | 31,490 | 380 | 1.474 | 1.109 | +0.789 | +0.897 |
| 3 | 82 | 20,881 | 429 | 1.472 | 1.059 | +0.808 | +0.902 |
| 4 | 62 | 14,931 | 434 | 1.515 | 1.060 | +0.807 | +0.905 |
| 6 | 42 | 10,933 | 335 | 1.485 | 1.068 | +0.805 | +0.908 |
| 8 | 32 | 7,943 | 279 | 1.497 | 1.053 | +0.810 | +0.916 |
| 12 | 22 | 5,269 | 563 | 1.468 ⬅ | 1.013 | +0.824 | +0.917 |
| 20 | 14 | 3,469 | 290 | 1.536 | 1.020 | +0.822 | +0.913 |

Validation picks **stride 12** (5,269 fit rows) → test R² +0.824, ρ +0.917.

## defense

| stride | modern snapshots | fit rows | rounds | val RMSE | test RMSE | test R² | test ρ |
|---|---|---|---|---|---|---|---|
| 1 | 245 | 63,340 | 250 | 1.762 | 1.574 | +0.543 | +0.767 |
| 2 | 123 | 31,490 | 328 | 1.770 | 1.488 | +0.592 | +0.789 |
| 3 | 82 | 20,881 | 964 | 1.746 ⬅ | 1.402 | +0.638 | +0.795 |
| 4 | 62 | 14,931 | 367 | 1.780 | 1.412 | +0.633 | +0.810 |
| 6 | 42 | 10,933 | 291 | 1.772 | 1.424 | +0.626 | +0.806 |
| 8 | 32 | 7,943 | 225 | 1.764 | 1.411 | +0.633 | +0.815 |
| 12 | 22 | 5,269 | 200 | 1.775 | 1.422 | +0.627 | +0.814 |
| 20 | 14 | 3,469 | 153 | 1.777 | 1.418 | +0.630 | +0.811 |

Validation picks **stride 3** (20,881 fit rows) → test R² +0.638, ρ +0.795.

## Reading it

| target | stride 6 (previous default) | stride 1 (all snapshots) | Δ test R² |
|---|---|---|---|
| total | +0.718 | +0.689 | -0.029 |
| offense | +0.805 | +0.772 | -0.032 |
| defense | +0.626 | +0.543 | -0.083 |

