# Top-10 and top-20 rank deviation

## Why rho is high while top-30 ordering is not

Spearman rho runs over every player in a cell — about 250 — and is carried by
separating tiers, which is a large and easy signal. The rank deviations run
*inside* the top of the board, where the whole population is a few RAPTOR points
wide. The gap between adjacent true ranks there is what any model has to resolve:

| band | mean gap between adjacent true ranks (RAPTOR) |
|---|---:|
| total top 10 | 0.628 |
| total 11-20 | 0.122 |
| total 21-30 | 0.067 |
| total 31-100 | 0.036 |
| total 101+ | 0.058 |
| offense top 10 | 0.433 |
| offense 11-20 | 0.106 |
| offense 21-30 | 0.072 |
| offense 31-100 | 0.027 |
| offense 101+ | 0.037 |
| defense top 10 | 0.178 |
| defense 11-20 | 0.067 |
| defense 21-30 | 0.044 |
| defense 31-100 | 0.028 |
| defense 101+ | 0.040 |

Set those against the model's MAE. Where the adjacent gap is far below the error,
the ordering is noise-limited and no amount of model capacity recovers it.

## Strategies, judged on dev@10

`dev@k` is the mean |true rank - projected position| over the projected top k.
Lower is better; 0 would be a perfect board.

### total

| strategy | dev@10 | dev@20 | dev@30 | tau@10 | tau@20 | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| rank-blend | 4.15 | 7.47 | 10.42 | +0.533 | +0.632 | 16/20 | 28/40 |
| seed-ens-8 | 4.80 | 7.35 | 9.18 | +0.556 | +0.611 | 16/20 | 30/40 |
| baseline | 5.60 | 6.35 | 8.90 | +0.578 | +0.611 | 14/20 | 30/40 |
| top-slice | 8.75 | 10.03 | 15.93 | +0.422 | +0.563 | 13/20 | 27/40 |
| two-stage | 8.75 | 10.03 | 13.40 | +0.422 | +0.563 | 13/20 | 27/40 |

### offense

| strategy | dev@10 | dev@20 | dev@30 | tau@10 | tau@20 | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| rank-blend | 1.80 | 4.60 | 6.22 | +0.756 | +0.763 | 16/20 | 31/40 |
| baseline | 1.90 | 3.85 | 8.17 | +0.689 | +0.726 | 16/20 | 32/40 |
| seed-ens-8 | 1.95 | 4.00 | 6.72 | +0.711 | +0.721 | 16/20 | 32/40 |
| top-slice | 2.40 | 5.35 | 9.65 | +0.800 | +0.626 | 15/20 | 33/40 |
| two-stage | 2.40 | 5.35 | 8.42 | +0.800 | +0.626 | 15/20 | 33/40 |

### defense

| strategy | dev@10 | dev@20 | dev@30 | tau@10 | tau@20 | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| seed-ens-8 | 5.00 | 9.82 | 12.72 | +0.400 | +0.411 | 13/20 | 27/40 |
| baseline | 5.10 | 11.38 | 12.43 | +0.400 | +0.389 | 13/20 | 28/40 |
| top-slice | 5.40 | 12.93 | 17.47 | +0.244 | +0.505 | 14/20 | 27/40 |
| two-stage | 5.40 | 12.93 | 17.27 | +0.244 | +0.489 | 14/20 | 27/40 |
| rank-blend | 5.55 | 10.47 | 13.15 | +0.400 | +0.411 | 13/20 | 26/40 |
