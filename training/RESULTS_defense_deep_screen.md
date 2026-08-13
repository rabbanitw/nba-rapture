# Defense architecture exploration — screen

Ten complete regular seasons, leave-one-season-out, evaluated at the 1065-minute floor. This is a post-selection research comparison, not a pristine future-season test.

| Architecture | RMSE | MAE | Spearman | dev@10 | dev@20 | tau@10 | hits@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| component_fixed | 0.807 | 0.626 | +0.886 | 5.20 | 9.76 | +0.520 | 72/100 |
| published_output_0.075 | 0.817 | 0.637 | +0.884 | 5.41 | 9.56 | +0.516 | 72/100 |
| defender_context | 0.823 | 0.641 | +0.882 | 5.89 | 10.67 | +0.484 | 69/100 |
| direct_component_blend_0.25 | 0.812 | 0.632 | +0.885 | 5.93 | 9.32 | +0.493 | 70/100 |
| faithful_no_pct | 0.840 | 0.652 | +0.876 | 6.25 | 11.97 | +0.502 | 73/100 |
| top_weight_2 | 0.821 | 0.641 | +0.880 | 6.28 | 9.25 | +0.480 | 71/100 |
| top_weight_5 | 0.838 | 0.651 | +0.876 | 6.51 | 10.46 | +0.449 | 74/100 |
| baseline | 0.817 | 0.636 | +0.883 | 6.67 | 9.28 | +0.484 | 69/100 |
| structure_loss_0.25 | 0.823 | 0.640 | +0.884 | 7.55 | 10.64 | +0.453 | 70/100 |
| structure_loss_0.10 | 0.809 | 0.627 | +0.888 | 7.57 | 10.52 | +0.453 | 69/100 |
| rank_blend_0.25 | 0.877 | 0.679 | +0.870 | 7.67 | 11.44 | +0.462 | 66/100 |
| hats_features | 0.797 | 0.615 | +0.891 | 7.71 | 10.11 | +0.462 | 68/100 |
| rank_blend_0.50 | 1.022 | 0.784 | +0.832 | 9.04 | 13.36 | +0.489 | 61/100 |
| lambdarank_30 | 1.200 | 0.947 | +0.766 | 9.85 | 16.23 | +0.453 | 58/100 |
| rank_xendcg_30 | 1.088 | 0.855 | +0.816 | 11.22 | 15.88 | +0.293 | 56/100 |
| rank_blend_0.75 | 1.203 | 0.928 | +0.772 | 11.31 | 16.59 | +0.453 | 57/100 |
| lambdarank_10 | 1.326 | 1.048 | +0.704 | 15.01 | 21.25 | +0.418 | 50/100 |

Wall time: 176.2 seconds. Full parameters and per-season metrics are in the adjacent JSON.
