# Estimated RAPTOR (Neil Paine) vs. our combined model

Recreated from <https://github.com/Neil-Paine-1/NBA-elo> and scored on our
held-out seasons. Both systems predict the same 538 RAPTOR labels on the
same **689 rows** (2013-14 and 2014-15, regular season and playoffs).

> **Paine's weights were fit on full RAPTOR from 2014-2023, which contains
> both of our test seasons.** His numbers are in-sample here; ours are
> strictly out-of-sample. The comparison flatters him, not us.

## What Estimated RAPTOR is

A linear model with 13 published coefficients per side: an intercept, MPG,
eight box-score actions per 100 possessions (PTS, TSA, AST, TOV, ORB, DRB,
STL, BLK, PF), and two plus-minus terms (on-court, on-off). Plus-minus is
weighted far more heavily on defense — 0.089 vs 0.018 on offense — because,
in Paine's words, "the boxscore is less effective at measuring defensive
performance than it is on offense." Raw ratings are then adjusted so each
position hits a leaguewide minute-weighted target, and so each team's
players sum to the team's actual rating.

## Recreating it from our data

`training/estimated_raptor.py` rebuilds the formula from our Mongo features
— per-100 box actions from `pbp`, on-court/on-off from `wowy` — applies the
published weights, then the position adjustment. Checked against his own
published columns (`per100` convention `side`, n=683):

| column | RMSE vs his published value | Pearson r |
|---|---|---|
| eRO (offense) | 0.560 | 0.9706 |
| eRD (defense) | 0.614 | 0.9088 |
| eRT (total) | 0.843 | 0.9586 |

The recreation is faithful but not exact, for two reasons worth naming:

- **No team adjustment.** His final step rescales players so 4.5× each
  team's minute-weighted average equals the team's actual offensive and
  defensive rating relative to league average. That needs team-level ratings,
  which our player-level collection does not carry, so it is omitted.
- **Approximate position shares.** He uses per-player minute shares by
  position (`PG%` … `C%`); we only have 538's `pos` string, so a player
  listed "PG, SG" is split 50/50.

Both gaps are calibration rather than ranking effects, which is why the
recreation still correlates at r≈0.96 on the total but loses ~0.10 R² against
the 538 labels. **The published columns are therefore the fair benchmark**;
the recreation is reported to show the method reproduces from our data.

## Results

### Total

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| **ours (combined, blend)** | **1.718** | 1.282 | **+0.751** | +0.883 | +0.886 |
| Paine published eRT | 1.936 | 1.379 | +0.684 | +0.841 | +0.846 |
| Paine recreated eRT | 2.229 | 1.636 | +0.581 | +0.804 | +0.809 |

### Offense

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| **ours (combined, blend)** | **1.022** | 0.780 | **+0.821** | +0.923 | +0.908 |
| Paine published eRO | 1.308 | 0.959 | +0.707 | +0.847 | +0.825 |
| Paine recreated eRO | 1.419 | 1.040 | +0.656 | +0.838 | +0.822 |

### Defense

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| **ours (combined, blend)** | **1.408** | 1.040 | **+0.635** | +0.817 | +0.818 |
| Paine published eRD | 1.641 | 1.194 | +0.504 | +0.726 | +0.728 |
| Paine recreated eRD | 1.775 | 1.298 | +0.414 | +0.683 | +0.694 |

## By slice (R²)

| slice | target | ours | Paine published |
|---|---|---|---|
| 2013-14 Playoffs | total | +0.682 | +0.507 |
| 2013-14 Regular season | total | +0.796 | +0.793 |
| 2014-15 Playoffs | total | +0.712 | +0.605 |
| 2014-15 Regular season | total | +0.808 | +0.835 |
| 2013-14 Playoffs | offense | +0.819 | +0.649 |
| 2013-14 Regular season | offense | +0.862 | +0.808 |
| 2014-15 Playoffs | offense | +0.737 | +0.519 |
| 2014-15 Regular season | offense | +0.851 | +0.817 |
| 2013-14 Playoffs | defense | +0.493 | +0.352 |
| 2013-14 Regular season | defense | +0.720 | +0.617 |
| 2014-15 Playoffs | defense | +0.591 | +0.398 |
| 2014-15 Regular season | defense | +0.734 | +0.669 |

## Sanity check against Paine's own reported numbers

He reports correlations against full RAPTOR of **0.913 offense / 0.784
defense / 0.890 total** for players with ≥1,000 minutes, 2014-2023. On our
492 ≥1,000-minute regular-season rows:

| target | his published eRO/eRD/eRT, r | he reports |
|---|---|---|
| offense | +0.902 | 0.913 |
| defense | +0.810 | 0.784 |
| total | +0.904 | 0.890 |

Close enough to confirm the join and the label mapping are sound. Our
defense figure runs above his because our test rows are 538's top-250
players, a higher-minute population than the full league he averages over.

## Conclusion

Our combined model beats Estimated RAPTOR on all three targets — total R² +0.751 vs +0.684, and by a wider margin on offense (+0.821 vs +0.707) and defense (+0.635 vs +0.504) — **while being scored
out-of-sample against an in-sample opponent.**

That said, the honest read is how *close* a 13-coefficient linear formula
gets. Estimated RAPTOR reaches ρ=0.846 on the total using twelve inputs; our
model uses 1,143 features and 15,476 training rows to reach ρ=0.886. Most of
the signal in RAPTOR is captured by per-100 box actions plus an on-off term,
and the gradient-boosted model is buying the last stretch, not the bulk.

Paine's design corroborates our own defensive finding independently: he
weights plus-minus ~5× more heavily on defense than offense (0.089 vs 0.018)
for exactly the reason our box-only defense model underperforms.
