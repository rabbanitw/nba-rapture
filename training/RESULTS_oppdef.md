# Opponent on/off data: the A/B the scrape was for

Regular season only; base = the replicated production model per target.

## defense

| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| +opp-block | 5.15 | 8.72 | +0.333 | +0.395 | 0.691 | 13/20 | 30/40 |
| base | 5.20 | 8.65 | +0.333 | +0.421 | 0.701 | 13/20 | 30/40 |
| +opp-both | 5.65 | 9.88 | +0.289 | +0.368 | 0.691 | 13/20 | 29/40 |
| +opp-engineered | 5.70 | 10.18 | +0.378 | +0.442 | 0.694 | 13/20 | 29/40 |

## offense

| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| +opp-engineered | 1.45 | 3.38 | +0.667 | +0.737 | 0.488 | 17/20 | 32/40 |
| +opp-both | 1.50 | 4.03 | +0.689 | +0.732 | 0.484 | 16/20 | 32/40 |
| base | 1.65 | 3.52 | +0.667 | +0.721 | 0.484 | 16/20 | 32/40 |
| +opp-block | 1.90 | 4.17 | +0.644 | +0.711 | 0.493 | 16/20 | 32/40 |
