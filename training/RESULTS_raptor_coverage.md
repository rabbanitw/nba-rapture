# RAPTOR methodology coverage audit — every section, our status

Section-by-section accounting of 538's "How Our RAPTOR Metric Works" against
this project's pipeline. Status codes: ✅ covered (data + model), 🟡 partial
(components present, exact construction untested or unbuilt), ❌ gap (data we
don't have), ➖ not applicable to replicating descriptive ratings, 🔬 tested
and empirically rejected on our task.

Every feature claim below was verified against the live matrix (grep of
feature names, 2026-08-12), not recalled from memory.

| # | Section | Status |
|---|---|---|
| 1 | Box RAPTOR (fit vs 6-year RAPM) | 🔬 recreated + tested, rejected |
| 2 | Points | ✅ |
| 3 | Usage rate | ✅ |
| 4 | Time of possession | ✅ scraped, 🔬 no gain |
| 5 | Assisted field goals | ✅/🟡 |
| 6 | Enhanced assists | ✅/🟡 |
| 7 | Net passes | ✅ |
| 8 | Enhanced offensive rebounds | 🟡 |
| 9 | Team OREB on missed shots | 🟡 |
| 10 | Positional opponents' DREB | 🟡 possession-lineup implementation (2013-19) |
| 11 | Defended (covered) 3PA | ✅ scraped, 🔬 no gain |
| 12 | Isolation turnovers | 🟡 |
| 13 | Fast-break starts | 🟡 |
| 14 | Nonshooting defensive fouls drawn | ✅ |
| 15 | Penalty fouls drawn | 🟡 |
| 16 | Opponents' defensive rating | 🟡 (production, different form) |
| 17 | Steals | ✅ |
| 18 | Offensive fouls drawn | 🟡 (charges only) |
| 19 | Opponents' FGM/FGA (nearest defender) | ✅ — most valuable defense block |
| 20 | Enhanced defensive rebounds | 🟡 |
| 21 | Positional opponents' points | 🟡 implemented/tested, no gain |
| 22 | Positional opponents' OREB | 🟡 implemented/tested, no gain |
| 23 | Distance traveled (perimeter-gated) | 🟡 |
| 24 | Opponents' FTM on own fouls | 🟡 proxy |
| 25 | Fastbreak turnovers committed | ✅ |
| 26 | Penalty fouls committed | 🟡 |
| 27 | Opponents' offensive rating | 🔬 tested, rejected for production |
| 28 | RAPTOR On-Off | 🟡 — chain builder implemented; artifact not checked in |
| 29 | Combining Box and On-Off | ✅ replicated + validated |
| 30 | Score effects adjustment | 🟡 exact formula; possession inputs absent |
| 31 | Team effects adjustment | 🟡 constraint implemented; target/transform partial |
| 32 | Individual Pace Impact | ➖ |
| 33 | Replacement level / WAR / market values | ✅ WAR; market curve unpublished |
| 34 | PREDATOR | ➖ |

---

## Box RAPTOR (the RAPM-calibration recipe) 🔬

538 fit linear coefficients against six-year RAPM (Ryan Davis, 2013-19,
mean-reversion removed, re-zeroed per season) and screened variables by
out-of-sample RAPM prediction. We addressed this at full fidelity this week:
recreated the pooled six-year RAPM from 1.42M possessions per the Davis
tutorial (validated rho +0.825 vs the public dataset; canonical top-10),
then tested calibration as an auxiliary rapm-hat feature in both production
stacks. **Rejected**: offense 1.10→1.35-1.95; defense 3.60 on test cells but
dead even (5.91 vs 5.91 mean dev@10) over 30 CV folds across three seed sets
(RESULTS_rapm_hat_verdict.md). The OOS-screening idea was also tested as
stability-target feature selection (adjacent-season labels): rejected
(RESULTS_stability_select.json). Conclusion: a GBM fit directly on RAPTOR
labels performs this calibration implicitly; the recipe mattered for their
linear model, not for ours.

## Box RAPTOR Offense

**Points** ✅ `pbp|Points` plus full zone scoring detail (AtRim/ShortMidRange/
LongMidRange/Corner3/Arc3 FGA/FGM/accuracy), penalty and second-chance splits.

**Usage rate** ✅ `pbp|Usage` directly, plus all components (FGA, TOs, FT trips).
538's heave discount: `pbp|HeaveAttempts` and NonHeave* accuracy variants exist,
so heaves are separable exactly as they intend.

**Time of possession** ✅→🔬 Named gap closed this week via `leaguedashptstats`
Possessions (TIME_OF_POSS, AVG_SEC_PER_TOUCH, AVG_DRIB_PER_TOUCH + per-36
engineered). Tested: no test-cell gain (RESULTS_shotdash.json) — touch-count
tables already carried the signal.

**Assisted field goals** ✅/🟡 Zone-level assisted shares present
(`AtRimPctAssisted`, `Corner3PctAssisted`, `Arc3PctAssisted`,
`Assisted2sPct/3sPct`, `NonPutbacksAssisted2sPct`). 538's exact construction
(deduction proportional to a 7-category shot-EV table incl. dunks at 1.83) is
not reproducible verbatim: **no dunk split** in our sources (their table needs
dunks separated from layups). The GBM sees assisted-share × zone-value jointly.

**Enhanced assists** ✅/🟡 `PtsAssisted2s/3s`, zone assists (`AtRimAssists`,
`Corner3Assists`, `Arc3Assists`), FT assists (`track:passing|FT_AST`, backfilled
via AST_ADJ−AST−SECONDARY), and `POTENTIAL AST` (which 538 found worthless —
the GBM is free to ignore it). Exact EV-weighting again subsumed by zone detail.

**Net passes** ✅ `track:passing|PASSES MADE` and `PASSES RECEIVED` — both raw
columns; the explicit differential is a linear combination the GBM can form.

**Enhanced offensive rebounds** 🟡 Contested and deferred rebounds present
(`track:rebounding` contested/deferred chances). Missing: 538's
shot-location-conditioned rebound values and the loose-ball-foul credit fix —
both need play-level linkage our per-player tables don't carry.

**Team OREB on missed shots** 🟡 `pbp|SelfOReb`, `SelfORebPct`, `PtsPutbacks`
cover the shooter's own-rebound effect; the team-level rate on a shooter's
misses (in-bounds, not-after-block conditioning) is not constructible.

**Positional opponents' DREB** 🟡 `raptor2/parse_attrib.py` and
`raptor2/posmatch.py` now reconstruct possession lineups, attribute rebounds,
and distribute events probabilistically by position-vector overlap. The
`posopp_dreb100` feature covers 2013-14 through 2018-19. It is not available for
later seasons and has not yet earned promotion into the offense stack.

**Defended 3-point attempts (spacing)** ✅→🔬 Closed this week: shot dashboard
scraped at all 64 cells, `covered3pa` engineered with 538's exact 100/80/57/31
weights plus tight/wide-open shares. Tested: no gain (offense 1.95 baseline vs
2.15-2.45 with) — assisted-share and 3PA-rate features already span it.

**Isolation turnovers** 🟡 Full taxonomy present (`Travels`,
`LostBallTurnovers`, `StepOutOfBoundsTurnovers`, `BadPassTurnovers`,
`LiveBallTurnovers`, `DeadBallTurnovers`, charge fouls). 538's explicit
100%/75% groupings not engineered as sums; the GBM consumes the components.

**Fast-break starts** 🟡 `Steals`, `RecoveredBlocks` present; 538's fixed
credits (+0.2/+0.11) are implicit in the label fit rather than hand-applied.

**Nonshooting defensive fouls drawn** ✅ `pbp|NonShootingFoulsDrawn` verbatim.

**Penalty fouls drawn** 🟡 `NonShootingPenaltyNonTakeFoulsDrawn`,
`PenaltyTurnovers` and penalty-state shooting splits exist; the exact
0.04-per-foul bonus construction is not replicated (538 sized it as small).

**Opponents' defensive rating faced** 🟡 538 average each individual opposing
defender's rating. We have no per-defender exposure log; the production offense
model instead carries the full opponent-WOWY block (opponent shot profile
on/off court, `Eopp`/`Bopp`) — a richer but differently-shaped adjustment,
and it earns its place (part of the 1.10 stack).

## Box RAPTOR Defense

**Steals** ✅ (their +1.49). Elite study: a top membership AND ordering feature.

**Offensive fouls drawn** 🟡 `Charge Fouls Drawn` exactly; non-charge offensive
fouls drawn (illegal screens) are not separately attributed in our sources, so
their +2.28 variable is covered by its dominant component only.

**Opponents' FGM/FGA as nearest defender** ✅ The centerpiece. Scraped
(`leaguedashptdefend`, 6 categories, all 64 cells), engineered verbatim:
`d2_value36` = (1.05·missed − 0.33·made) per 36, `d2_pct_pm`, `rim_pct_pm`,
`d3a36` frequency-only (their finding that defended-3 results are noise is
encoded structurally). Permutation shows this 8-column block is the single
most load-bearing group for elite defensive ordering (Δtau@30 −0.223).

**Enhanced defensive rebounds** 🟡 Contested DREB and deferred-chance columns
present; shot-location-conditioned values not constructible (as with OREB).

**Positional opponents' points / OREB** 🟡 The same possession-lineup parser
now emits `posopp_pts100` and `posopp_oreb100` for the 2013-19 window. Appending
them to the in-window defense GBM was neutral (dev@10 5.05→5.55; MAE
0.642→0.639), so the variables remain an audited structural reproduction rather
than production inputs (`raptor2/RESULTS_posmatch.json`).

**Distance traveled, perimeter-gated** 🟡 Both halves present
(`track:speed-distance|DIST. MILES DEF`, `AVG SPEED DEF`; defended 2PA/3PA for
the gate) but the explicit perimeter-gated interaction was never built. Elite
study evidence suggests low stakes: permuting the whole track-defense block
costs ~nothing for elite ordering (+0.005 tau@30).

**Opponents' FTM on own fouls** 🟡 No player-attributed opponent FT makes;
proxied by `ShootingFouls` / `TwoPt/ThreePtShootingFoulsDrawn` committed-side
counterparts. Their coefficient is small (−0.19/FTM).

**Fastbreak turnovers committed** ✅ `LiveBallTurnovers` (they penalize
live-ball TOs −0.2) plus blocked-attempt detail.

**Penalty fouls committed** 🟡 `NonShootingPenaltyNonTakeFouls` and penalty
splits; exact −0.04 bonus-state accounting not replicated.

**Opponents' offensive rating faced** 🔬 Tested directly: the opponent-WOWY
block with engineered luck-adjusted opponent ratings (experiment_oppdef) was
trialed for the defense model and lost to matched+defend; it survives only in
the offense stack. Competition adjustment for defense is therefore implicit.

## RAPTOR On-Off 🟡 — the biggest remaining structural difference

538: (1) own on-court ratings, (2) courtmates' ratings WITHOUT the player,
weighted by shared×apart possessions, (3) courtmates' courtmates' ratings, all
competition-adjusted; they note team-without-player (our `wowy_off`) is the
inferior construction, and that this replicates RAPM out-of-sample as well as
RAPM itself. We carry: full on/off/diff WOWY (227 differential features),
opponent-WOWY for competition context, and 3-point-luck ingredients (opponent
Arc3/Corner3 accuracy and frequency, used in the luck-adjusted DRtg features).
`raptor2/courtmate_chain.py` now builds the three-level chain directly from the
possession lineups produced by `build_rapm.py`, avoiding O(rotation²) pair-WOWY
requests, and emits opponent quality separately. The raw possession caches are
not checked in, so this block has not been materialized for the canonical
ten-season benchmark. The production matrix therefore still uses the inferior
team-without-player proxy. Our components architecture nonetheless recovers
their on-off component well enough for the combiner to reproduce their weights.

## Combining Box and On-Off ✅ — replicated and independently validated

Their blend: 0.85·box + 0.21·on-off. Our components architecture learns the
combiner from data (minutes-aware ridge): fitted weights 0.948/0.185 on the
box/on-off scale — same structure, same "sums to slightly more than 1"
property, box dominant. This is the production offense model (test dev@10
1.10, LOSO median 1.50).

**Score effects** 🟡 The exact published coefficients and tied-game conversion
are implemented and unit-tested in `raptor2/postprocess.py`. Per-quarter,
per-10-point coasting adjustments
(−1.1/−1.7/−2.3/−2.9 RS; roughly half in playoffs) computed on-court per
player still need possession-level score margins, which are not retained in the
checked-in aggregate artifacts. Our labels contain the adjustment; our feature
matrix cannot apply it row-by-row — a real, quantified distortion channel for
players on extreme teams.

**Team effects** 🟡 The constraint-preserving, usage-weighted reconciliation is
implemented and unit-tested in `raptor2/postprocess.py`. Ratings are reconciled
to team totals, weighted by
offensive/defensive usage (their defensive-usage definition — induced TOs +
fouls-to-FTs + defended FGA — is buildable from our columns but the
exact nonlinear usage transform was not published). Team-season targets are not
stored in the checked-in matrix, so this remains an optional post-process.
Mitigating context from the doc:
PREDATOR omits this adjustment because it doesn't help out-of-sample — 538
themselves conclude the residual is mostly luck.

## Individual Pace Impact ➖

Feeds WAR/projections, not the descriptive per-100 ratings we predict. Our
rate normalization (306 columns per-possession) handles pace at the feature
level.

## Replacement Level, WAR and Market Values ➖

Constants documented and implemented in `raptor2/postprocess.py` (−2.75, WAR
multipliers 0.0005102/0.0005262, position table). Individual Pace Impact must be
supplied because its switcher-regression coefficients were never published.

## PREDATOR ➖

Predictive variant (out-of-sample coefficients, minutes-per-game, biographical
priors). Our task is descriptive replication, which correctly excludes age/
height/draft priors per their own statement. Their 0.98/0.95 RAPTOR-PREDATOR
correlations tell us the distinction is minor even for them.

---

## Bottom line

- **Fully covered or better**: scoring/usage detail, passing, steals/fouls
  taxonomy, the nearest-defender shot-defense block (our best defense asset),
  the box+on-off architecture and blend weights.
- **Closed this week, tested, no gain**: time of possession, covered-3PA
  spacing, RAPM calibration (all three at full methodological fidelity).
- **Partial by data granularity**: enhanced rebounding values, exact
  EV-weighted assist accounting, penalty-state bookkeeping, perimeter-gated
  distance (interaction unbuilt; evidence says low-stakes).
- **Implemented but not materialized in the canonical ten-season artifact**:
  courtmate-chain on-off, score effects, and team reconciliation. They require
  possession caches or team targets that are not checked in.
- **Finite-window reproduction, empirically neutral**: positional-matchup
  points/rebounds allowed (2013-19).
