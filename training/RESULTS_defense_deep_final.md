# Selected defense architecture

The selected defense rating is **60% direct matched defense + 40% structural-hat-augmented defense**. Both heads are three-seed LightGBM/Ridge ensembles and every structural hat is refit inside its outer season fold. The augmented head receives predicted box defense, predicted on/off defense, and their published 0.85/0.21 combination.

The 40% coefficient is post-selected on this research corpus. A leave-one-season-at-a-time weight check chooses 0.40 in nine seasons and 0.30 in one; its cross-fitted dev@10 is 4.55.

A ranking-first sensitivity variant uses 30% hats. It minimizes mean normalized deviation across k=10/20/30, while the selected 40% model has the best top-10 deviation and lower RMSE.

## Common-pool results

| Target / model | RMSE | MAE | Spearman | dev@10 | dev@20 | tau@10 | hits@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Defense — old | 0.811 | 0.629 | +0.886 | 6.09 | 9.52 | +0.498 | 71/100 |
| Defense — selected | 0.793 | 0.615 | +0.891 | 4.51 | 9.83 | +0.502 | 73/100 |
| Defense — rank-first sensitivity | 0.796 | 0.618 | +0.890 | 4.53 | 9.59 | +0.489 | 74/100 |
| Defense — Paine | 1.138 | 0.896 | +0.770 | 16.46 | 19.61 | +0.267 | 48/100 |
| Total — old | 1.063 | 0.809 | +0.911 | 4.05 | 6.27 | +0.436 | 81/100 |
| Total — selected | 1.051 | 0.800 | +0.913 | 3.27 | 5.90 | +0.453 | 81/100 |
| Total — rank-first sensitivity | 1.053 | 0.802 | +0.913 | 3.43 | 5.91 | +0.449 | 81/100 |
| Total — Paine | 1.299 | 1.017 | +0.862 | 4.71 | 9.50 | +0.418 | 73/100 |

## Stability

Defense dev@10 changes by **-1.58 ranks per season** (season bootstrap 95% interval -2.90 to -0.46; exact one-sided sign-flip p=0.0107).

Direct and augmented residual correlation is 0.947; their remaining error diversity is what makes the ensemble outperform either head alone.

An exact squared-loss penalty toward the fold-fitted published structure was tested at λ=0.05, 0.10, and 0.25. The best RMSE was 0.811 at λ=0.05, but dev@10 worsened to 7.10; larger penalties worsened both. The published structure works better as a separate representation and ensemble member than as a pointwise penalty.

Season-cluster bootstrap RMSE differences (selected minus comparator):

| Comparison | Difference | 95% interval |
|---|---:|---:|
| defense_vs_old | -0.018 | [-0.023, -0.013] |
| defense_vs_paine | -0.345 | [-0.388, -0.302] |
| total_vs_old | -0.012 | [-0.016, -0.008] |
| total_vs_paine | -0.248 | [-0.294, -0.198] |

Full weight sweep, per-season results, and uncertainty values are stored in the adjacent JSON; row-level predictions are in the adjacent CSV.
