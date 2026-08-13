# RAPTOR reproduction and model-selection report

## Executive decision

The best supported architecture in this repository is a two-head ensemble:

- **Offense:** full player matrix + cell-relative features + engineered
  opponent context + four low-dimensional structural RAPTOR component hats,
  learned by a three-seed LightGBM ensemble and blended 75/25 with RidgeCV.
- **Defense:** 60% of the whole-season matched LightGBM/RidgeCV head plus 40%
  of a second head augmented by fold-fitted defensive box/on-off hats and their
  published 0.85/0.21 combination. Both heads use three seeds.
- **Total:** independently predicted offense + defense. This beats a model
  trained directly on total RAPTOR.

On a canonical ten-season leave-one-season-out (LOSO) benchmark over 2,238
player-seasons common to Neil Paine's file, all with at least 1,065 minutes, the
stack records **RMSE 1.051, R² 0.854, and Spearman 0.913** on total RAPTOR.
Paine's published Estimated RAPTOR records 1.299, 0.777, and 0.862 on the same
rows. The season-cluster bootstrap difference is −0.248 RMSE (95% CI −0.294 to
−0.198), favoring this repository.

This is the best *predictor of the published ratings*. The most faithful small
structural reproduction is retained separately because fidelity and predictive
accuracy are different goals. The public article does not disclose enough
information to reproduce FiveThirtyEight's private coefficients, exact
positional assignment, pace regression, biographical projection curves, or
market-value curve bit-for-bit.

Sources: [FiveThirtyEight's recovered RAPTOR methodology](https://mysportsanalysis.com/blogs/sports-fivethirtyeight/how-our-raptor-metric-works),
[FiveThirtyEight's DRAYMOND defense study](https://fivethirtyeight.com/features/a-better-way-to-evaluate-nba-defense/),
[FiveThirtyEight's RAPTOR data dictionary](https://github.com/fivethirtyeight/data/tree/master/nba-raptor),
[FiveThirtyEight's later NBA forecast methodology](https://fivethirtyeight.com/methodology/how-our-nba-predictions-work/),
[Neil Paine's NBA-elo / Estimated RAPTOR repository](https://github.com/Neil-Paine-1/NBA-elo).

## What is being reproduced

RAPTOR is a descriptive plus-minus rating in points per 100 possessions above
league average. It has separate offensive and defensive ratings and two
underlying components:

\[
RAPTOR = 0.85 \times Box\ RAPTOR + 0.21 \times OnOff\ RAPTOR.
\]

The weights intentionally sum to 1.06 because box and on/off contain
non-redundant signal. Descriptive RAPTOR uses no age, height, draft, or award
priors. Those belong to PREDATOR, the projection variant.

FiveThirtyEight later reported that standard RAPTOR outperformed PREDATOR for
forecasting and switched its NBA forecast back to standard RAPTOR. That later
production decision reinforces keeping this reproduction centered on the
descriptive rating rather than adding biographical priors to improve a
historical-label fit.

The public workflow is:

1. Fit separate offensive and defensive box regressions to multi-year RAPM.
2. Fit separate on/off regressions from the player's adjusted on-court rating,
   courtmates without the player, and courtmates' other courtmates.
3. Combine box and on/off at 0.85/0.21.
4. Remove score effects and reconcile the player ratings to adjusted team
   performance, allocating more residual to high-usage players.
5. Compute Individual Pace Impact for WAR, then use replacement level −2.75.

## Methodology coverage

| Published component | Repository implementation | Fidelity |
|---|---|---|
| Six-year RAPM calibration | 1.42M-possession pooled ridge RAPM, 2013-19; validated against public RAPM | High; RAPM-hat did not improve final prediction |
| League-relative inputs | Minute-weighted, within-cell centering in `raptor2/structural.py` | High |
| Scoring / usage / heaves | Zone scoring, usage, FT trips, turnover detail, heave discount | High; dunk/layup split unavailable |
| Assisted-shot deduction / enhanced assists | Zone-EV assisted and unassisted makes, zone assists, FT assists | Partial; no distinct dunk category |
| Time of possession / net passes | NBA tracking possessions, seconds/touch, passes made−received | High |
| Enhanced rebounds | Contested/uncontested, self-OREB, putbacks | Partial; preceding-shot value and loose-ball linkage unavailable in aggregates |
| Covered 3PA / gravity | Exact 100/80/57/31 defender-distance weights | High |
| Isolation turnovers | Published 100%/75% turnover classes | High for available event types |
| Fast-break / foul constants | +0.20 steal, +0.11 recovered block, +0.16 nonshooting foul, ±0.04 penalty foul | High |
| Nearest-defender defense | +1.05 missed 2, −0.33 made 2, +0.17 attempted 3; no block input | High and empirically the most important defensive block |
| Perimeter activity | Defensive miles gated by defended-3/defended-2 ratio | Close approximation; exact gate unpublished |
| Positional opponents | Possession reparse + probabilistic position-overlap attribution, 2013-19 | Partial window; neutral in the measured ablation |
| Three-level courtmate chain | `raptor2/courtmate_chain.py` builds all three levels from possession lineups | Implemented; raw caches are not checked in or in the canonical benchmark |
| 3-point luck / opponent quality | On/off ratings pre-adjusted for 3-point variance; opponent context emitted separately | Close approximation |
| 0.85/0.21 blend | Exact in `raptor2/postprocess.py` and structural model | Exact |
| Score effects | Exact period/split coefficients and tied-game conversion | Exact formula; row-level application needs unshipped possession margins |
| Team effects | Constraint-preserving usage-weighted reconciliation | Structural match; exact usage transform and team targets unpublished/unshipped |
| Replacement / WAR | −2.75 and exact 0.0005102/0.0005262 formula | Exact when Individual Pace Impact is supplied |
| Individual Pace Impact | WAR accepts it as input | Regression coefficients were not published; cannot be independently identified |
| PREDATOR / market values | Documented only | Projection and nonlinear dollar curves were not published |

The detailed field-by-field audit is in
[`training/RESULTS_raptor_coverage.md`](training/RESULTS_raptor_coverage.md).

## Data and validation protocol

The checked-in matrix contains 16,951 player-cell rows, 909 base columns, and
ten labeled complete regular seasons from 2013-14 through 2022-23. Model
preparation adds 227 on-minus-off columns and eight context columns; three ID
columns are then removed, leaving **1,141 base model features**.

The canonical comparison in
[`training/benchmark_final_architecture.py`](training/benchmark_final_architecture.py)
uses ten LOSO folds. For each fold:

- only the other nine complete regular-season cells train the final models;
- all four structural component hats are refit without the held-out season;
- the held-out pool is never used to fit trees, ridges, structural hats, or
  imputation statistics inside that run;
- evaluation uses players with at least 1,065 minutes;
- Paine's traded-player stints are collapsed by minutes and joined by normalized
  player name and season.

The architecture and hyperparameters were selected by earlier experiments on
this same ten-season corpus. The canonical table is therefore an honest
out-of-fold estimate of the *selected pipeline*, but not a pristine external
test of the entire research process; its bootstrap intervals are conditional on
that selection. A future season with newly available RAPTOR-equivalent truth is
the right final confirmation.

This still corrects an important comparability issue in older reports. Paine states
that his regression was fit against 2014-2023 full RAPTOR, which is exactly the
ten-season evaluation window here. His results are therefore in-sample, while
every repository prediction in the canonical table is out-of-fold.

### Metrics

- **RMSE / MAE:** magnitude error in RAPTOR points per 100 possessions.
- **R²:** pooled explained variance across all common player-seasons.
- **Pearson / Spearman:** linear and rank correlation.
- **dev@k:** mean absolute true-rank error among the model's projected top k,
  averaged over season cells; lower is better.
- **tau@k:** Kendall ordering correlation inside the projected top k.
- **hits@k:** overlap between predicted and true top-k sets, summed over ten
  seasons (so hits@10 is out of 100).

## Architecture exploration

The repository evaluates substantially more than a single boosted-tree model.
The important conclusions, all taken from checked-in result artifacts, are:

| Family / intervention | Best relevant result | Decision |
|---|---:|---|
| Linear structural RAPTOR | canonical total RMSE 1.755, R² 0.593 | Keep as fidelity/audit model, not production predictor |
| Direct LightGBM + Ridge | canonical total RMSE 1.187, R² 0.814 | Strong baseline; loses to separate O+D heads |
| Box/on-off component stack | pooled three-run offense LOSO mean dev@10 1.867 | Strong and interpretable, superseded by hybrid hats |
| Full matrix + structural hats | pooled three-run offense LOSO mean dev@10 1.703; canonical 1.70 | **Selected offense** |
| Whole-season-matched GBM + defended-shot block | defense RMSE 0.811, dev@10 6.09 | Retained as direct defense head |
| Fixed component heads: 0.85 box + 0.21 on/off | defense RMSE 0.805, dev@10 5.91; RMSE wins 10/10 versus direct | Validated structural auxiliary |
| Direct + structural-hat defense ensemble | defense RMSE 0.793, dev@10 4.51 | **Selected defense** |
| Pairwise LightGBM | two-test-season offense dev@10 1.55; LOSO ≈1.60 | Excellent ranking companion, but no calibrated rating scale |
| Direct neural NAS | offense dev@10 3.65; defense 12.75 | Rejected |
| Pairwise neural NAS | offense dev@10 3.25; defense 6.00 | Rejected |
| CatBoost defense | LOSO median dev@10 6.45 | Rejected versus matched LightGBM |
| Random forest / Extra Trees | materially lower held-out R² | Rejected |
| RAPM-hat calibration | pooled 30-fold defense dev@10 exactly 5.91 vs 5.91 base | Rejected as seed noise |
| LambdaRank / XE-NDCG | defense dev@10 9.85–15.01 in the broad screen | Rejected; NDCG grades mismatch fine RAPTOR ordering |
| Top-weighted Huber loss | best defense dev@10 6.28, RMSE 0.821 | Rejected |
| Huber target shrink toward published blend | RMSE 0.804 but dev@10 7.57 | Rejected; approximate penalty helps magnitude only |
| Exact squared published-structure penalty | best RMSE 0.811 at λ=0.05, dev@10 7.10 | Rejected; all λ values worsen ranking |
| Covered-3, time-of-possession, positional-matchup additions | faithful but neutral in ablation | Retained structurally, not promoted alone |

Neural search covered 108 configurations across direct MLP, residual MLP,
self-normalizing, bottleneck, Bradley-Terry, antisymmetrized difference, and
two-tower families. The neural models fit training seasons extremely well but
generalized worse across seasons, which is consistent with a small, irregular,
heterogeneous tabular dataset where missingness itself is informative.

### Defense-first ranking and structural-loss cycle

The follow-up search in `training/experiment_defense_deep.py` screened 12 model
families over all ten outer folds, then confirmed the structural finalists with
three seeds. It was motivated by two details from the surviving FiveThirtyEight
documents: nearest-defender shot volume and two-point context carry real signal,
while opponent three-point results are largely noise; and the 0.85/0.21 blend
was selected against out-of-sample RAPM rather than in-sample rating identity.
The listwise arms use LightGBM's documented `lambdarank` and `rank_xendcg`
objectives with season query groups and integer relevance labels.

| Candidate | Defense RMSE | dev@10 | Result |
|---|---:|---:|---|
| Old matched direct head | 0.813 | 6.67 | One-seed screen baseline |
| Top-weighted Huber, best arm | 0.821 | 6.28 | Rejected |
| LambdaRank, truncation 30 | 1.200 | 9.85 | Rejected |
| XE-NDCG ranker | 1.088 | 11.22 | Rejected |
| Target penalty toward published structural estimate | 0.804 | 7.57 | Better magnitude, worse ordering |
| Exact `MSE(y)+λ·MSE(0.85 box+0.21 on/off)` penalty | 0.811 | 7.10 | λ=0.05; rejected |
| Separate component heads, fixed 0.85/0.21 output | 0.805 | 5.91 | RMSE improvement in 10/10 seasons |
| Hat-augmented head alone | 0.795 | 7.65 | Excellent magnitude, unstable elite ordering |
| **60% direct + 40% hat-augmented** | **0.793** | **4.51** | **Selected** |

The last row reduces defense dev@10 by 1.58 ranks per season versus the old head
(season bootstrap 95% interval −2.90 to −0.46; exact one-sided sign-flip
`p=0.0107`). Its RMSE improvement is −0.018 with season-cluster 95% interval
[−0.023, −0.013]. A 70/30 sensitivity blend minimizes mean normalized rank
deviation across k=10/20/30 and scores dev@10 4.53, dev@20 9.59, and RMSE 0.796;
the selected 60/40 blend has slightly better top-10 deviation, RMSE, and total
RAPTOR performance. Full screens and row-level predictions are in
[`training/RESULTS_defense_deep_final.md`](training/RESULTS_defense_deep_final.md).

## Selected architecture in detail

### Shared preprocessing

- 1,141 base features after rate normalization, context construction, on-off
  differences, and ID removal.
- Counts are normalized per 100 possessions or per 36 minutes where appropriate.
- Missing values remain NaN for LightGBM; RidgeCV receives fold-training median
  imputation and fold-training standardization.
- Training is complete-season only in the canonical model, eliminating the
  distribution mismatch and repeated-player overweighting caused by in-season
  snapshots.
- Three LightGBM seeds: **0, 1, 2**.
- Each seed member is **0.75 LightGBM + 0.25 RidgeCV**; the three members are
  averaged.
- Ridge alpha grid: **logspace(−2, 4, 25)**.

### Structural hats

Four RidgeCV models predict `rap_box_o`, `rap_onoff_o`, `rap_box_d`, and
`rap_onoff_d` from the methodology's compact structural variables. They use
sqrt(minutes) sample weights and alpha grid **logspace(−3, 3, 25)**. Hats are
refit inside every LOSO fold. They inject RAPTOR's domain structure without
forcing the final nonlinear model to inherit the structural model's bias.

### Offense head

Inputs: 1,141 base + 12 cell-relative + 20 opponent-engineered + 4 hats =
**1,177 columns**.

LightGBM:

| Hyperparameter | Value |
|---|---:|
| objective | L2 regression |
| learning rate | 0.03 |
| boosting rounds | 394 |
| num leaves | 15 |
| min data in leaf | 40 |
| feature fraction | 0.50 |
| bagging fraction | 0.80 |
| bagging frequency | 1 |
| L2 regularization | 5.0 |

### Defense head

The direct head has 1,141 base + 12 cell-relative + 8 engineered
nearest-defender inputs = **1,161 columns**. The augmented head adds predicted
defensive box and on/off hats plus their fixed `0.85*box + 0.21*onoff` value =
**1,164 columns**. Training rows are complete seasons only. Final defense is
`0.60*direct + 0.40*augmented`.

| Hyperparameter | Value |
|---|---:|
| objective | Huber |
| Huber alpha | 2.0 |
| learning rate | 0.03 |
| boosting rounds | 216 |
| num leaves | 15 |
| min data in leaf | 40 |
| feature fraction | 0.50 |
| bagging fraction | 0.80 |
| bagging frequency | 1 |
| L2 regularization | 5.0 |

### Total rating

Total RAPTOR is `predicted offense + predicted defense`. In the canonical
benchmark it beats a direct-total head: RMSE **1.051 vs 1.187**, R² **0.854 vs
0.814**, and Spearman **0.913 vs 0.889**.

## Canonical results against Paine

Common pool: 2,238 regular-season player-seasons, at least 1,065 minutes, ten
seasons. Paine match quality: 2,153 exact normalized names, 85 conservative
within-season fuzzy matches, four unmatched eligible rows. A prior bug that
mistook the first name “JR” for a suffix was fixed before the final run.

| Target / system | RMSE | MAE | R² | Pearson | Spearman | dev@10 | tau@10 | hits@10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Total — selected O+D** | **1.051** | **0.800** | **0.854** | **0.925** | **0.913** | **3.27** | +0.453 | **81/100** |
| Total — direct head | 1.187 | 0.911 | 0.814 | 0.903 | 0.889 | 4.21 | +0.391 | 80/100 |
| Total — structural fixed | 1.755 | 1.369 | 0.593 | 0.776 | 0.744 | 9.85 | +0.338 | 59/100 |
| Total — Paine eRT | 1.299 | 1.017 | 0.777 | 0.882 | 0.862 | 4.71 | +0.418 | 73/100 |
| **Offense — selected hybrid** | **0.645** | **0.475** | **0.912** | **0.955** | **0.940** | **1.70** | **+0.578** | **91/100** |
| Offense — Paine eRO | 0.916 | 0.716 | 0.822 | 0.907 | 0.875 | 4.68 | +0.511 | 71/100 |
| **Defense — selected ensemble** | **0.793** | **0.615** | **0.806** | **0.899** | **0.891** | **4.51** | **+0.502** | **73/100** |
| Defense — old matched head | 0.811 | 0.629 | 0.797 | 0.895 | 0.886 | 6.09 | +0.498 | 71/100 |
| Defense — Paine eRD | 1.138 | 0.896 | 0.600 | 0.781 | 0.770 | 16.46 | +0.267 | 48/100 |

Season-cluster bootstrap RMSE differences, ours minus Paine:

| Target | Difference | 95% interval |
|---|---:|---:|
| offense | −0.271 | [−0.301, −0.244] |
| defense | −0.345 | [−0.388, −0.302] |
| total | −0.248 | [−0.294, −0.198] |

The selected model has lower RMSE than Paine in **10/10 total**, **10/10
offense**, and **10/10 defense** season-level comparisons.

The protocol favors Paine at the row-fitting level: the selected stack is OOF,
while Paine's published values are fitted on this window. It does not capture
this repository's prior model-selection exposure to the same seasons. Paine
remains the operational
winner for simplicity—roughly a dozen inputs and published linear coefficients
versus more than a thousand inputs and a multi-model ensemble. The earlier
runtime comparison is in [`training/RESULTS_benchmark.md`](training/RESULTS_benchmark.md).

Paine's **NBA Elo** table is a game-level team win-probability archive, not a
player-rating target. It cannot be scored against player RAPTOR without adding a
team aggregation, roster projection, schedule, and game-outcome evaluation
pipeline. The apples-to-apples competitor in his repository is Estimated RAPTOR,
which is what is compared here.

## What remains irreducible

“Faithful” cannot honestly mean bit-identical from the public report. The main
remaining limitations are:

1. The original fitted box/on-off coefficients were never published.
2. Dunk versus non-dunk layup detail and play-linked rebound values are absent
   from the checked-in aggregates.
3. The three-level courtmate builder requires possession caches generated by
   `build_rapm.py`; those large raw files are not versioned.
4. Score effects require each player's on-court lead by period. The coefficients
   are exact, but those margins are not in `combined.npz`.
5. Team reconciliation's exact usage transform is unpublished, and team targets
   are not in the matrix.
6. Individual Pace Impact's switcher-regression coefficients, PREDATOR's aging
   curves, and the market-value curve are unpublished.

Accordingly, the recommended product surface should label two outputs clearly:

- **Structural RAPTOR reproduction:** transparent, uses published constants,
  closest to the article, lower predictive accuracy.
- **Rapture estimate:** the selected nonlinear O+D ensemble, highest validated
  accuracy against the historical RAPTOR labels.

## Reproduction

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# Clone Paine outside this repository, then run the canonical ten-fold benchmark.
git clone --depth 1 https://github.com/Neil-Paine-1/NBA-elo.git C:\tmp\NBA-elo
.\.venv\Scripts\python.exe training\benchmark_final_architecture.py `
  --paine-repo C:\tmp\NBA-elo

# Defense breadth screen, three-seed confirmation, exact loss, and selection.
.\.venv\Scripts\python.exe training\experiment_defense_deep.py --stage screen
.\.venv\Scripts\python.exe training\experiment_defense_deep.py --stage confirm
.\.venv\Scripts\python.exe training\experiment_defense_deep.py --stage loss
.\.venv\Scripts\python.exe training\select_defense_architecture.py

# Deterministic formula, courtmate-chain, and identity-join tests.
.\.venv\Scripts\python.exe -m unittest discover -s training -p "test_*.py" -v
```

Generated artifacts:

- `training/RESULTS_final_architecture.md` — concise canonical result table.
- `training/RESULTS_final_architecture.json` — metrics, all hyperparameters,
  per-season scores, bootstrap intervals, and runtime.
- `training/RESULTS_final_architecture.csv` — every out-of-fold prediction and
  Paine match.
- `training/RESULTS_defense_deep_{screen,confirm,loss}.*` — defense candidate
  sweeps and exact published-structure loss study.
- `training/RESULTS_defense_deep_final.*` — weight stability, uncertainty,
  rank-first sensitivity, and row-level selected predictions.
- `training/report_nas/nas_report.pdf` — detailed neural architecture search.

The canonical run completed in 148 seconds on the evaluation machine. It used
Python 3.12 with the pinned packages in `requirements.txt`.
