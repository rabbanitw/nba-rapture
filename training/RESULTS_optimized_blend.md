# Training-optimized RAPTOR component weights

The requested fit uses all **16,259 training rows** and never uses the **692 test rows** to choose weights. Ordinary least squares minimizes unweighted total-RAPTOR RMSE with no intercept.

## Result

| Blend | Box weight | On/off weight | All-row train RMSE | Test RMSE | Test MAE |
|---|---:|---:|---:|---:|---:|
| Published | 0.850000 | 0.210000 | 1.102480 | 0.097052 | 0.076924 |
| Learned on all training | 0.916759 | 0.129057 | 0.719648 | 0.457489 | 0.300197 |
| Complete-season sensitivity | 0.847351 | 0.209146 | 1.093952 | 0.094191 | 0.074707 |

The all-training optimum lowers training RMSE by **34.7%** but raises untouched-test RMSE by **371.4%**. It should therefore not replace the published weights.

## Test split

| Blend | Regular-season n | Regular-season RMSE | Playoff n | Playoff RMSE |
|---|---:|---:|---:|---:|
| Published | 494 | 0.088965 | 198 | 0.114769 |
| Learned | 494 | 0.244247 | 198 | 0.763308 |
| Complete-season sensitivity | 494 | 0.086426 | 198 | 0.111225 |

## Why the training optimum fails

The literal training set contains repeated in-season snapshots and is dominated by 2020-21 and 2021-22 rows. Several early snapshots in those seasons do not satisfy the otherwise stable published component identity. The test set consists of complete-season rows. Fitting only complete-season training rows is a diagnostic—not the requested primary fit—and returns 0.847351/0.209146, with test RMSE 0.094191. This confirms that the published 0.85/0.21 blend generalizes and that the all-row result reflects snapshot distribution mismatch.

The adjacent JSON contains every split metric and the season-level training diagnostic.
