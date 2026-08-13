# Defense architecture exploration — confirm

Ten complete regular seasons, leave-one-season-out, evaluated at the 1065-minute floor. This is a post-selection research comparison, not a pristine future-season test.

| Architecture | RMSE | MAE | Spearman | dev@10 | dev@20 | tau@10 | hits@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| published_output_0.075 | 0.813 | 0.632 | +0.886 | 4.70 | 9.41 | +0.502 | 73/100 |
| component_fixed_l2 | 0.811 | 0.629 | +0.885 | 5.29 | 10.35 | +0.471 | 67/100 |
| component_fixed_masked | 0.811 | 0.629 | +0.885 | 5.63 | 10.21 | +0.493 | 68/100 |
| component_fixed | 0.805 | 0.625 | +0.887 | 5.91 | 9.29 | +0.489 | 71/100 |
| direct_component_blend_0.25 | 0.809 | 0.628 | +0.886 | 6.05 | 9.37 | +0.511 | 71/100 |
| baseline | 0.813 | 0.630 | +0.885 | 6.09 | 9.52 | +0.498 | 71/100 |
| structure_loss_0.10 | 0.804 | 0.624 | +0.890 | 7.57 | 10.19 | +0.471 | 69/100 |
| hats_features | 0.795 | 0.614 | +0.892 | 7.65 | 10.31 | +0.453 | 68/100 |

Wall time: 225.9 seconds. Full parameters and per-season metrics are in the adjacent JSON.
