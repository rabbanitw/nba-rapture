# Defense architecture exploration — loss

Ten complete regular seasons, leave-one-season-out, evaluated at the 1065-minute floor. This is a post-selection research comparison, not a pristine future-season test.

| Architecture | RMSE | MAE | Spearman | dev@10 | dev@20 | tau@10 | hits@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| published_output_0.075 | 0.813 | 0.632 | +0.886 | 4.70 | 9.41 | +0.502 | 73/100 |
| baseline | 0.813 | 0.630 | +0.885 | 6.09 | 9.52 | +0.498 | 71/100 |
| structure_penalty_l2_0.10 | 0.816 | 0.634 | +0.886 | 6.82 | 10.55 | +0.476 | 69/100 |
| structure_penalty_l2_0.25 | 0.828 | 0.647 | +0.883 | 6.97 | 10.62 | +0.480 | 69/100 |
| structure_penalty_l2_0.05 | 0.811 | 0.631 | +0.887 | 7.10 | 10.00 | +0.498 | 68/100 |

Wall time: 116.5 seconds. Full parameters and per-season metrics are in the adjacent JSON.
