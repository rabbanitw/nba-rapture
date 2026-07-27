# Top-100 leaderboards on the held-out seasons

True 538 RAPTOR vs. our models vs. Neil Paine's Estimated RAPTOR, for
2013-14 and 2014-15. Total, offense and defense are ranked separately.
These are the models trained on **all** data points — no starter or
near-zero filtering (see RESULTS_starters.md for why those filters lose).

> Skipped as degenerate: 2013-14 Playoffs (pool 99); 2014-15 Playoffs (pool 98). A top-100 over a pool of that size is nearly the whole field,
> so every system scores near-perfectly and the comparison says nothing.

> Paine's weights were fit on 2014-2023 full RAPTOR, which includes both of
> these seasons — his predictions are in-sample, ours are not.

## Three ways to rank by total RAPTOR

Paine's published `eRT` is **exactly** `eRO + eRD` (max residual 1e-9): he
has no direct total model, only two part-models that are added. We do have a
direct total model, so the total leaderboards compare three systems:

| system | how the total is produced |
|---|---|
| ours (direct total) | one model trained on `rap` |
| ours (offense+defense) | our `rap_o` and `rap_d` models, summed |
| Paine (eRO+eRD) | his two part-models, summed — his only option |

## Minutes threshold

Derived, not chosen: the **lowest minutes total among any true top-20**
player, taken across every season, split and target, so no genuine leader is
ruled ineligible.

| season | split | target | min mp in true top 20 | median mp | pool n | pool min mp |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | total | 1143 | 2250 | 250 | 1065 |
| 2013-14 | Regular season | offense | 1077 | 2336 | 250 | 1065 |
| 2013-14 | Regular season | defense | 1072 | 1967 | 250 | 1065 |
| 2014-15 | Regular season | total | 1173 | 2104 | 250 | 1148 |
| 2014-15 | Regular season | offense | 1173 | 2116 | 250 | 1148 |
| 2014-15 | Regular season | defense | 1148 | 2018 | 250 | 1148 |
| 2013-14 | Playoffs | total | 167 | 306 | 100 | 167 |
| 2013-14 | Playoffs | offense | 167 | 306 | 100 | 167 |
| 2013-14 | Playoffs | defense | 167 | 306 | 100 | 167 |
| 2014-15 | Playoffs | total | 131 | 278 | 100 | 131 |
| 2014-15 | Playoffs | offense | 131 | 278 | 100 | 131 |
| 2014-15 | Playoffs | defense | 131 | 278 | 100 | 131 |

**Regular season → ≥ 1072 minutes. Playoffs → ≥ 131 minutes.**

This barely bites: 538 only rated ~250 players per historical season and all
of them already clear 1,065 regular-season minutes. In the playoffs the true
top 20 reaches the very bottom of the pool (a 131-minute player makes it), so
no threshold applies there without excluding a real leader. The filter would
matter far more against an unfiltered universe — Paine's own CSV has a
1-minute player at eRO +55.7 who would otherwise top every leaderboard.

## Regression accuracy over all held-out rows

**total**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours (direct total) | 1.719 | 1.283 | +0.751 | +0.883 | +0.886 |
| ours (offense+defense) | 1.742 | 1.287 | +0.744 | +0.880 | +0.878 |
| Paine (eRO+eRD) | 1.938 | 1.380 | +0.683 | +0.841 | +0.846 |

**offense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.022 | 0.780 | +0.821 | +0.923 | +0.908 |
| Paine (eRO) | 1.309 | 0.960 | +0.707 | +0.847 | +0.825 |

**defense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.409 | 1.041 | +0.635 | +0.816 | +0.818 |
| Paine (eRD) | 1.642 | 1.196 | +0.504 | +0.726 | +0.727 |

## Summary — true top-100 members recovered (hits@100)

**total**

| season | split | pool | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) | ρ ours (direct total) | ρ ours (offense+defense) | ρ Paine (eRO+eRD) |
|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 84/100 | 85/100 | 80/100 | +0.896 | +0.897 | +0.889 |
| 2014-15 | Regular season | 246 | 84/100 | 84/100 | 85/100 | +0.902 | +0.900 | +0.901 |
| **all** | | | **168/200** | **169/200** | **165/200** |  |  |  |

Precision@K for total, summed over 2 cells:

| K | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|
| 10 | 15/20 | 15/20 | 15/20 |
| 25 | 36/50 | 36/50 | 35/50 |
| 50 | 78/100 | 79/100 | 75/100 |
| 100 | 168/200 | 169/200 | 165/200 |

**offense**

| season | split | pool | ours | Paine (eRO) | ρ ours | ρ Paine (eRO) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 83/100 | 80/100 | +0.924 | +0.866 |
| 2014-15 | Regular season | 246 | 88/100 | 80/100 | +0.925 | +0.858 |
| **all** | | | **171/200** | **160/200** |  |  |

Precision@K for offense, summed over 2 cells:

| K | ours | Paine (eRO) |
|---|---|---|
| 10 | 16/20 | 17/20 |
| 25 | 40/50 | 40/50 |
| 50 | 83/100 | 76/100 |
| 100 | 171/200 | 160/200 |

**defense**

| season | split | pool | ours | Paine (eRD) | ρ ours | ρ Paine (eRD) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 83/100 | 76/100 | +0.871 | +0.794 |
| 2014-15 | Regular season | 246 | 82/100 | 73/100 | +0.871 | +0.801 |
| **all** | | | **165/200** | **149/200** |  |  |

Precision@K for defense, summed over 2 cells:

| K | ours | Paine (eRD) |
|---|---|---|
| 10 | 12/20 | 11/20 |
| 25 | 36/50 | 31/50 |
| 50 | 75/100 | 68/100 |
| 100 | 165/200 | 149/200 |

## How contested is the cutoff

Hits@100 only asks whether a player lands on the correct side of an
arbitrary cutoff. Players within ±0.25 RAPTOR of the boundary value, per cell:

| season | split | target | rank-100 value | gap to rank 101 | players within ±0.25 |
|---|---|---|---|---|---|
| 2013-14 | Regular season | total | +0.60 | 0.00 | 19 |
| 2014-15 | Regular season | total | +0.80 | 0.00 | 17 |
| 2013-14 | Regular season | offense | +0.50 | 0.00 | 38 |
| 2014-15 | Regular season | offense | +0.30 | 0.00 | 32 |
| 2013-14 | Regular season | defense | +0.40 | 0.00 | 29 |
| 2014-15 | Regular season | defense | +0.40 | 0.00 | 39 |

Where a dozen players sit inside a quarter-point of the cutoff, which 20 names
come back is close to a coin flip regardless of model quality. That is why
hits@20 and the rank correlations disagree, and why the correlations are the
more reliable read.

## Conclusions

**Direct total vs. summing the halves.** Predicting `rap` directly and
summing our two part-models are near-interchangeable: R² +0.751 vs +0.744, ρ +0.886 vs +0.878, hits@100 168/200 vs 169/200.

**Against Paine on the total.** R² +0.751 vs +0.683, RMSE 1.719 vs 1.938, ρ +0.886 vs +0.846; hits@100 168/200 vs 165/200.

**Offense.** ours R² +0.821 / ρ +0.908 / hits@100 171/200; Paine R² +0.707 / ρ +0.825 / hits@100 160/200.

**Defense.** ours R² +0.635 / ρ +0.818 / hits@100 165/200; Paine R² +0.504 / ρ +0.727 / hits@100 149/200.

Read the precision@K tables above rather than a single cutoff: they show
where each system's advantage actually lives, and a hits count at one
arbitrary K is decided by hundredths of a point among near-tied players.

## Leaderboards

`[n]` after a predicted name is that player's *true* rank; ✓ means they are
genuinely in the true top 100.

### 2013-14 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+11.00) | LeBron James (+6.37) [15] ✓ | Chris Paul (+7.60) [1] ✓ | Chris Paul (+8.79) [1] ✓ |
| 2 | **Kevin Durant** (+7.10) | Kevin Durant (+6.29) [2] ✓ | Kevin Durant (+6.60) [2] ✓ | Kevin Durant (+7.21) [2] ✓ |
| 3 | **Kawhi Leonard** (+6.70) | Chris Paul (+6.24) [1] ✓ | LeBron James (+6.58) [15] ✓ | LeBron James (+6.77) [15] ✓ |
| 4 | **Kevin Love** (+6.60) | Paul George (+5.54) [8] ✓ | Paul George (+5.39) [8] ✓ | Kawhi Leonard (+5.95) [3] ✓ |
| 5 | **James Harden** (+6.10) | Kevin Love (+5.49) [4] ✓ | Manu Ginobili (+4.81) [9] ✓ | James Harden (+5.50) [5] ✓ |
| 6 | **Joakim Noah** (+5.90) | Kawhi Leonard (+5.02) [3] ✓ | Kyle Lowry (+4.69) [7] ✓ | Paul George (+5.08) [8] ✓ |
| 7 | **Kyle Lowry** (+5.70) | Kyle Lowry (+4.73) [7] ✓ | Kevin Love (+4.52) [4] ✓ | Manu Ginobili (+4.99) [9] ✓ |
| 8 | **Paul George** (+5.60) | James Harden (+4.70) [5] ✓ | Kawhi Leonard (+4.46) [3] ✓ | Kevin Love (+4.86) [4] ✓ |
| 9 | **Manu Ginobili** (+5.10) | Manu Ginobili (+4.52) [9] ✓ | Goran Dragic (+4.30) [10] ✓ | Goran Dragic (+4.71) [10] ✓ |
| 10 | **Goran Dragic** (+5.00) | Blake Griffin (+4.37) [34] ✓ | Andrew Bogut (+4.25) [36] ✓ | Andre Iguodala (+4.53) [21] ✓ |
| 11 | **DeMarcus Cousins** (+5.00) | Andrew Bogut (+4.34) [36] ✓ | James Harden (+4.20) [5] ✓ | Kyle Lowry (+4.27) [7] ✓ |
| 12 | **Patty Mills** (+4.80) | Andre Iguodala (+4.21) [21] ✓ | Anthony Davis (+4.20) [28] ✓ | Joakim Noah (+4.22) [6] ✓ |
| 13 | **Dirk Nowitzki** (+4.70) | Dirk Nowitzki (+4.17) [13] ✓ | Andre Iguodala (+4.12) [21] ✓ | Russell Westbrook (+4.18) [25] ✓ |
| 14 | **Danny Green** (+4.70) | Goran Dragic (+4.11) [10] ✓ | Dirk Nowitzki (+3.97) [13] ✓ | Anthony Davis (+4.18) [28] ✓ |
| 15 | **LeBron James** (+4.60) | Carmelo Anthony (+3.69) [21] ✓ | Blake Griffin (+3.94) [34] ✓ | Blake Griffin (+4.00) [34] ✓ |
| 16 | **Anderson Varejao** (+4.10) | Ricky Rubio (+3.68) [25] ✓ | Patty Mills (+3.60) [12] ✓ | Ricky Rubio (+3.91) [25] ✓ |
| 17 | **Patrick Beverley** (+4.10) | Russell Westbrook (+3.49) [25] ✓ | Jimmy Butler (+3.57) [19] ✓ | Brandan Wright (+3.89) [115] ✗ |
| 18 | **Mario Chalmers** (+4.00) | Isaiah Thomas (+3.43) [19] ✓ | Mike Conley (+3.53) [21] ✓ | Dirk Nowitzki (+3.81) [13] ✓ |
| 19 | **Jimmy Butler** (+3.90) | Anthony Davis (+3.42) [28] ✓ | Ricky Rubio (+3.46) [25] ✓ | DeMarcus Cousins (+3.60) [10] ✓ |
| 20 | **Isaiah Thomas** (+3.90) | Draymond Green (+3.41) [29] ✓ | LaMarcus Aldridge (+3.43) [29] ✓ | Patty Mills (+3.50) [12] ✓ |
| 21 | **Carmelo Anthony** (+3.80) | Chris Bosh (+3.18) [93] ✓ | Mario Chalmers (+3.37) [18] ✓ | Jimmy Butler (+3.31) [19] ✓ |
| 22 | **Kemba Walker** (+3.80) | Mike Conley (+3.08) [21] ✓ | Marcin Gortat (+3.37) [57] ✓ | Carmelo Anthony (+3.28) [21] ✓ |
| 23 | **Mike Conley** (+3.80) | Jimmy Butler (+3.08) [19] ✓ | Russell Westbrook (+3.32) [25] ✓ | DeAndre Jordan (+3.19) [62] ✓ |
| 24 | **Andre Iguodala** (+3.80) | Joakim Noah (+3.05) [6] ✓ | Danny Green (+3.17) [13] ✓ | LaMarcus Aldridge (+3.12) [29] ✓ |
| 25 | **Ricky Rubio** (+3.70) | LaMarcus Aldridge (+3.04) [29] ✓ | Damian Lillard (+3.09) [53] ✓ | Mike Conley (+3.12) [21] ✓ |
| 26 | **Eric Bledsoe** (+3.70) | Damian Lillard (+2.95) [53] ✓ | Pablo Prigioni (+3.06) [70] ✓ | Trevor Ariza (+3.03) [66] ✓ |
| 27 | **Russell Westbrook** (+3.70) | Patty Mills (+2.93) [12] ✓ | Isaiah Thomas (+2.99) [19] ✓ | Dwyane Wade (+3.01) [83] ✓ |
| 28 | **Anthony Davis** (+3.50) | Deron Williams (+2.91) [34] ✓ | Paul Millsap (+2.95) [36] ✓ | Al Jefferson (+2.99) [78] ✓ |
| 29 | **LaMarcus Aldridge** (+3.40) | Anderson Varejao (+2.88) [16] ✓ | DeMarcus Cousins (+2.94) [10] ✓ | Tony Allen (+2.97) [46] ✓ |
| 30 | **Draymond Green** (+3.40) | Paul Millsap (+2.88) [36] ✓ | Carmelo Anthony (+2.90) [21] ✓ | Paul Millsap (+2.91) [36] ✓ |
| 31 | **DeMarre Carroll** (+3.30) | Pablo Prigioni (+2.84) [70] ✓ | Joakim Noah (+2.87) [6] ✓ | Chris Bosh (+2.91) [93] ✓ |
| 32 | **Nikola Pekovic** (+3.30) | Derek Fisher (+2.72) [50] ✓ | Kemba Walker (+2.84) [21] ✓ | Deron Williams (+2.86) [34] ✓ |
| 33 | **Tiago Splitter** (+3.30) | Danny Green (+2.69) [13] ✓ | Chris Bosh (+2.84) [93] ✓ | David West (+2.83) [51] ✓ |
| 34 | **Blake Griffin** (+3.20) | Kemba Walker (+2.63) [21] ✓ | Derek Fisher (+2.80) [50] ✓ | Anderson Varejao (+2.81) [16] ✓ |
| 35 | **Deron Williams** (+3.20) | Eric Bledsoe (+2.59) [25] ✓ | Draymond Green (+2.75) [29] ✓ | Andre Drummond (+2.77) [74] ✓ |
| 36 | **Paul Millsap** (+3.10) | Paul Pierce (+2.59) [59] ✓ | Nicolas Batum (+2.74) [59] ✓ | Andrew Bogut (+2.74) [36] ✓ |
| 37 | **Andrew Bogut** (+3.10) | Nicolas Batum (+2.58) [59] ✓ | Al Jefferson (+2.69) [78] ✓ | John Wall (+2.73) [66] ✓ |
| 38 | **Kris Humphries** (+3.00) | DeMarcus Cousins (+2.56) [10] ✓ | Wesley Matthews (+2.65) [46] ✓ | Isaiah Thomas (+2.68) [19] ✓ |
| 39 | **Klay Thompson** (+2.90) | DeAndre Jordan (+2.54) [62] ✓ | George Hill (+2.58) [48] ✓ | Danny Green (+2.63) [13] ✓ |
| 40 | **Robin Lopez** (+2.90) | Mario Chalmers (+2.52) [18] ✓ | Deron Williams (+2.57) [34] ✓ | Wesley Matthews (+2.56) [46] ✓ |
| 41 | **Ty Lawson** (+2.90) | John Wall (+2.43) [66] ✓ | Anderson Varejao (+2.52) [16] ✓ | Corey Brewer (+2.50) [89] ✓ |
| 42 | **Vince Carter** (+2.90) | Trevor Ariza (+2.42) [66] ✓ | Dwyane Wade (+2.33) [83] ✓ | Nicolas Batum (+2.49) [59] ✓ |
| 43 | **Jae Crowder** (+2.90) | Robin Lopez (+2.34) [39] ✓ | Paul Pierce (+2.32) [59] ✓ | DeMarre Carroll (+2.43) [31] ✓ |
| 44 | **Darren Collison** (+2.70) | Marcin Gortat (+2.32) [57] ✓ | DeAndre Jordan (+2.25) [62] ✓ | Chandler Parsons (+2.38) [62] ✓ |
| 45 | **Shane Battier** (+2.70) | Wesley Matthews (+2.27) [46] ✓ | David West (+2.23) [51] ✓ | Klay Thompson (+2.36) [39] ✓ |
| 46 | **Wesley Matthews** (+2.60) | David West (+2.23) [51] ✓ | Nikola Pekovic (+2.15) [31] ✓ | Dwight Howard (+2.35) [57] ✓ |
| 47 | **Tony Allen** (+2.60) | Vince Carter (+2.22) [39] ✓ | Channing Frye (+2.14) [49] ✓ | Ty Lawson (+2.35) [39] ✓ |
| 48 | **George Hill** (+2.50) | Dwight Howard (+2.09) [57] ✓ | Dwight Howard (+2.12) [57] ✓ | DeMar DeRozan (+2.26) [107] ✗ |
| 49 | **Channing Frye** (+2.40) | Kyle Korver (+2.07) [78] ✓ | Tiago Splitter (+2.09) [31] ✓ | Tim Duncan (+2.20) [66] ✓ |
| 50 | **Derek Fisher** (+2.30) | Amir Johnson (+2.06) [89] ✓ | Patrick Beverley (+2.08) [16] ✓ | Eric Bledsoe (+2.17) [25] ✓ |
| 51 | **David West** (+2.20) | Jae Crowder (+1.96) [39] ✓ | Trevor Ariza (+2.08) [66] ✓ | Damian Lillard (+2.15) [53] ✓ |
| 52 | **Jrue Holiday** (+2.20) | DeMarre Carroll (+1.95) [31] ✓ | Eric Bledsoe (+2.02) [25] ✓ | David Lee (+2.09) [70] ✓ |
| 53 | **Damian Lillard** (+2.10) | George Hill (+1.91) [48] ✓ | Robin Lopez (+1.98) [39] ✓ | Nikola Pekovic (+2.08) [31] ✓ |
| 54 | **Michael KiddGilchrist** (+2.10) | Patrick Beverley (+1.91) [16] ✓ | John Wall (+1.95) [66] ✓ | Robin Lopez (+2.04) [39] ✓ |
| 55 | **Chris Andersen** (+2.10) | Dwyane Wade (+1.91) [83] ✓ | Jae Crowder (+1.93) [39] ✓ | Serge Ibaka (+1.97) [59] ✓ |
| 56 | **CJ Watson** (+2.10) | Kirk Hinrich (+1.84) [107] ✗ | Brandan Wright (+1.93) [115] ✗ | George Hill (+1.95) [48] ✓ |
| 57 | **Marcin Gortat** (+2.00) | Channing Frye (+1.80) [49] ✓ | Josh McRoberts (+1.93) [89] ✓ | Kyle Korver (+1.94) [78] ✓ |
| 58 | **Dwight Howard** (+2.00) | David Lee (+1.78) [70] ✓ | DeMarre Carroll (+1.92) [31] ✓ | Terrence Jones (+1.72) [152] ✗ |
| 59 | **Nicolas Batum** (+1.90) | Nick Collison (+1.77) [83] ✓ | Kyle Korver (+1.90) [78] ✓ | Jae Crowder (+1.68) [39] ✓ |
| 60 | **Serge Ibaka** (+1.90) | Iman Shumpert (+1.75) [97] ✓ | Nick Collison (+1.80) [83] ✓ | Lance Stephenson (+1.66) [135] ✗ |
| 61 | **Paul Pierce** (+1.90) | Jrue Holiday (+1.74) [51] ✓ | Tim Duncan (+1.78) [66] ✓ | Marc Gasol (+1.66) [62] ✓ |
| 62 | **DeAndre Jordan** (+1.80) | Al Jefferson (+1.72) [78] ✓ | Vince Carter (+1.77) [39] ✓ | Draymond Green (+1.65) [29] ✓ |
| 63 | **Chandler Parsons** (+1.80) | Tiago Splitter (+1.69) [31] ✓ | Amir Johnson (+1.75) [89] ✓ | Marcin Gortat (+1.63) [57] ✓ |
| 64 | **Roy Hibbert** (+1.80) | Nikola Pekovic (+1.65) [31] ✓ | Jrue Holiday (+1.74) [51] ✓ | Paul Pierce (+1.60) [59] ✓ |
| 65 | **Marc Gasol** (+1.80) | Tony Allen (+1.65) [46] ✓ | David Lee (+1.66) [70] ✓ | Kemba Walker (+1.58) [21] ✓ |
| 66 | **John Wall** (+1.70) | Shaun Livingston (+1.65) [87] ✓ | CJ Watson (+1.65) [53] ✓ | Marco Belinelli (+1.57) [74] ✓ |
| 67 | **Trevor Ariza** (+1.70) | Brandan Wright (+1.60) [115] ✗ | Chris Andersen (+1.65) [53] ✓ | CJ Watson (+1.54) [53] ✓ |
| 68 | **PJ Tucker** (+1.70) | Corey Brewer (+1.58) [89] ✓ | Corey Brewer (+1.58) [89] ✓ | Chris Andersen (+1.50) [53] ✓ |
| 69 | **Tim Duncan** (+1.70) | Chris Andersen (+1.56) [53] ✓ | Jeremy Lamb (+1.55) [102] ✗ | Patrick Beverley (+1.46) [16] ✓ |
| 70 | **David Lee** (+1.60) | Nene (+1.38) [70] ✓ | Anthony Tolliver (+1.49) [118] ✗ | Josh McRoberts (+1.46) [89] ✓ |
| 71 | **Courtney Lee** (+1.60) | Shane Battier (+1.35) [44] ✓ | Klay Thompson (+1.47) [39] ✓ | Kyrie Irving (+1.43) [83] ✓ |
| 72 | **Nene** (+1.60) | Andray Blatche (+1.32) [118] ✗ | Kirk Hinrich (+1.43) [107] ✗ | Tiago Splitter (+1.43) [31] ✓ |
| 73 | **Pablo Prigioni** (+1.60) | Jeremy Lamb (+1.27) [102] ✗ | Andre Drummond (+1.43) [74] ✓ | Pablo Prigioni (+1.43) [70] ✓ |
| 74 | **Andre Drummond** (+1.50) | Anthony Tolliver (+1.27) [118] ✗ | Marc Gasol (+1.39) [62] ✓ | Derek Fisher (+1.36) [50] ✓ |
| 75 | **Jared Sullinger** (+1.50) | Marc Gasol (+1.23) [62] ✓ | Nene (+1.37) [70] ✓ | Patrick Patterson (+1.33) [107] ✗ |
| 76 | **Marco Belinelli** (+1.50) | CJ Watson (+1.21) [53] ✓ | Iman Shumpert (+1.33) [97] ✓ | Mario Chalmers (+1.32) [18] ✓ |
| 77 | **Matthew Dellavedova** (+1.50) | Mike Dunleavy (+1.16) [107] ✗ | Kenneth Faried (+1.29) [170] ✗ | Jrue Holiday (+1.24) [51] ✓ |
| 78 | **Al Jefferson** (+1.30) | Josh McRoberts (+1.16) [89] ✓ | Rudy Gay (+1.27) [100] ✗ | Mason Plumlee (+1.23) [198] ✗ |
| 79 | **Kyle Korver** (+1.30) | Jeff Teague (+1.15) [122] ✗ | Shane Battier (+1.25) [44] ✓ | Courtney Lee (+1.23) [70] ✓ |
| 80 | **Reggie Jackson** (+1.30) | DJ Augustin (+1.12) [131] ✗ | DJ Augustin (+1.23) [131] ✗ | PJ Tucker (+1.08) [66] ✓ |
| 81 | **Jeremy Lin** (+1.30) | PJ Tucker (+1.09) [66] ✓ | Shaun Livingston (+1.22) [87] ✓ | Jeremy Lamb (+1.03) [102] ✗ |
| 82 | **Jeremy Evans** (+1.30) | Terrence Jones (+1.07) [152] ✗ | Tony Allen (+1.21) [46] ✓ | Amir Johnson (+1.02) [89] ✓ |
| 83 | **Kyrie Irving** (+1.20) | Patrick Patterson (+0.96) [107] ✗ | Jared Sullinger (+1.18) [74] ✓ | Jamal Crawford (+0.95) [115] ✗ |
| 84 | **Dwyane Wade** (+1.20) | Darren Collison (+0.94) [44] ✓ | Darren Collison (+1.18) [44] ✓ | Kevin Martin (+0.94) [181] ✗ |
| 85 | **Nick Collison** (+1.20) | Tim Duncan (+0.91) [66] ✓ | Terrence Jones (+1.14) [152] ✗ | Tony Parker (+0.92) [131] ✗ |
| 86 | **Nate Wolters** (+1.20) | Chandler Parsons (+0.91) [62] ✓ | Kyrie Irving (+1.13) [83] ✓ | Rudy Gay (+0.90) [100] ✗ |
| 87 | **Shaun Livingston** (+1.10) | Andre Drummond (+0.89) [74] ✓ | Mike Dunleavy (+1.10) [107] ✗ | Mike Dunleavy (+0.88) [107] ✗ |
| 88 | **Nick Calathes** (+1.10) | Reggie Jackson (+0.88) [78] ✓ | Reggie Jackson (+1.02) [78] ✓ | Nene (+0.87) [70] ✓ |
| 89 | **Corey Brewer** (+1.00) | Monta Ellis (+0.85) [118] ✗ | PJ Tucker (+0.97) [66] ✓ | Kenneth Faried (+0.85) [170] ✗ |
| 90 | **Josh McRoberts** (+1.00) | Zach Randolph (+0.84) [125] ✗ | Andray Blatche (+0.97) [118] ✗ | Andray Blatche (+0.82) [118] ✗ |
| 91 | **Amir Johnson** (+1.00) | Taj Gibson (+0.81) [149] ✗ | Patrick Patterson (+0.96) [107] ✗ | Darren Collison (+0.79) [44] ✓ |
| 92 | **Boris Diaw** (+1.00) | Kenneth Faried (+0.70) [170] ✗ | Jeremy Lin (+0.92) [78] ✓ | DJ Augustin (+0.78) [131] ✗ |
| 93 | **Chris Bosh** (+0.90) | Klay Thompson (+0.70) [39] ✓ | Jeff Teague (+0.90) [122] ✗ | Monta Ellis (+0.73) [118] ✗ |
| 94 | **Luol Deng** (+0.90) | Jared Sullinger (+0.68) [74] ✓ | Victor Oladipo (+0.81) [152] ✗ | Markieff Morris (+0.73) [152] ✗ |
| 95 | **Nick Young** (+0.90) | Nick Calathes (+0.63) [87] ✓ | Chandler Parsons (+0.80) [62] ✓ | Nikola Vucevic (+0.73) [122] ✗ |
| 96 | **Omri Casspi** (+0.80) | Markieff Morris (+0.56) [152] ✗ | Josh Smith (+0.76) [169] ✗ | Luol Deng (+0.70) [93] ✓ |
| 97 | **Bradley Beal** (+0.70) | Tyson Chandler (+0.50) [140] ✗ | Omri Casspi (+0.67) [96] ✓ | Thabo Sefolosha (+0.69) [102] ✗ |
| 98 | **Randy Foye** (+0.70) | Kyrie Irving (+0.50) [83] ✓ | Taj Gibson (+0.64) [149] ✗ | Tyson Chandler (+0.66) [140] ✗ |
| 99 | **Iman Shumpert** (+0.70) | Boris Diaw (+0.48) [89] ✓ | Luol Deng (+0.60) [93] ✓ | Boris Diaw (+0.65) [89] ✓ |
| 100 | **Gordon Hayward** (+0.60) | Omri Casspi (+0.45) [96] ✓ | Roy Hibbert (+0.57) [62] ✓ | Nick Calathes (+0.65) [87] ✓ |

### 2014-15 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | LeBron James (+6.65) [11] ✓ | Chris Paul (+7.02) [1] ✓ | Chris Paul (+8.27) [1] ✓ |
| 2 | **Kawhi Leonard** (+8.90) | Chris Paul (+6.58) [1] ✓ | LeBron James (+6.97) [11] ✓ | Anthony Davis (+7.97) [3] ✓ |
| 3 | **Anthony Davis** (+8.80) | Draymond Green (+6.43) [5] ✓ | Draymond Green (+6.08) [5] ✓ | Kawhi Leonard (+7.91) [2] ✓ |
| 4 | **James Harden** (+7.70) | James Harden (+6.32) [4] ✓ | Kawhi Leonard (+5.65) [2] ✓ | LeBron James (+6.66) [11] ✓ |
| 5 | **Draymond Green** (+6.50) | Anthony Davis (+6.17) [3] ✓ | Anthony Davis (+5.42) [3] ✓ | James Harden (+6.55) [4] ✓ |
| 6 | **Danny Green** (+6.10) | Kawhi Leonard (+5.52) [2] ✓ | James Harden (+5.27) [4] ✓ | Russell Westbrook (+5.71) [7] ✓ |
| 7 | **Russell Westbrook** (+5.60) | Jimmy Butler (+4.95) [34] ✓ | Russell Westbrook (+4.96) [7] ✓ | Jimmy Butler (+5.58) [34] ✓ |
| 8 | **George Hill** (+5.60) | Russell Westbrook (+4.65) [7] ✓ | Klay Thompson (+4.87) [10] ✓ | George Hill (+5.08) [7] ✓ |
| 9 | **DeMarcus Cousins** (+5.40) | Lou Williams (+4.63) [34] ✓ | Kyrie Irving (+4.83) [13] ✓ | Klay Thompson (+4.74) [10] ✓ |
| 10 | **Klay Thompson** (+5.30) | Klay Thompson (+4.40) [10] ✓ | Andrew Bogut (+4.43) [23] ✓ | Tony Allen (+4.57) [16] ✓ |
| 11 | **LeBron James** (+5.10) | DeMarcus Cousins (+4.26) [9] ✓ | Khris Middleton (+4.21) [12] ✓ | Draymond Green (+4.55) [5] ✓ |
| 12 | **Khris Middleton** (+4.80) | Kyrie Irving (+4.16) [13] ✓ | Jimmy Butler (+4.20) [34] ✓ | DeAndre Jordan (+4.31) [13] ✓ |
| 13 | **Kyrie Irving** (+4.60) | George Hill (+3.81) [7] ✓ | Danny Green (+4.09) [6] ✓ | Danny Green (+4.23) [6] ✓ |
| 14 | **DeAndre Jordan** (+4.60) | Khris Middleton (+3.70) [12] ✓ | George Hill (+3.69) [7] ✓ | Blake Griffin (+4.02) [60] ✓ |
| 15 | **Kyle Korver** (+4.60) | Danny Green (+3.57) [6] ✓ | Blake Griffin (+3.66) [60] ✓ | Paul Millsap (+4.01) [27] ✓ |
| 16 | **LaMarcus Aldridge** (+4.30) | Andrew Bogut (+3.55) [23] ✓ | Damian Lillard (+3.55) [42] ✓ | Kyrie Irving (+4.00) [13] ✓ |
| 17 | **Tony Allen** (+4.30) | Damian Lillard (+3.54) [42] ✓ | Jeff Teague (+3.54) [42] ✓ | Khris Middleton (+3.91) [12] ✓ |
| 18 | **Nikola Mirotic** (+4.20) | LaMarcus Aldridge (+3.45) [16] ✓ | Kyle Lowry (+3.51) [22] ✓ | John Wall (+3.82) [60] ✓ |
| 19 | **Rudy Gobert** (+4.10) | Tony Allen (+3.30) [16] ✓ | Lou Williams (+3.51) [34] ✓ | Jeff Teague (+3.76) [42] ✓ |
| 20 | **Marc Gasol** (+4.00) | Kyle Lowry (+3.28) [22] ✓ | Rudy Gobert (+3.46) [19] ✓ | Tim Duncan (+3.72) [27] ✓ |
| 21 | **Darren Collison** (+4.00) | Wesley Matthews (+3.25) [24] ✓ | Tony Allen (+3.43) [16] ✓ | Wesley Matthews (+3.64) [24] ✓ |
| 22 | **Kyle Lowry** (+3.90) | Isaiah Thomas (+3.21) [71] ✓ | Wesley Matthews (+3.39) [24] ✓ | Lou Williams (+3.53) [34] ✓ |
| 23 | **Andrew Bogut** (+3.70) | Blake Griffin (+3.20) [60] ✓ | LaMarcus Aldridge (+3.32) [16] ✓ | Gordon Hayward (+3.29) [26] ✓ |
| 24 | **Wesley Matthews** (+3.60) | Gordon Hayward (+3.17) [26] ✓ | John Wall (+3.21) [60] ✓ | Al Horford (+3.23) [60] ✓ |
| 25 | **Jonas Jerebko** (+3.60) | Manu Ginobili (+3.07) [29] ✓ | Mike Conley (+3.20) [37] ✓ | DeMarcus Cousins (+3.15) [9] ✓ |
| 26 | **Gordon Hayward** (+3.40) | Rudy Gobert (+3.05) [19] ✓ | Paul Millsap (+3.18) [27] ✓ | Rudy Gobert (+3.08) [19] ✓ |
| 27 | **Paul Millsap** (+3.30) | Paul Millsap (+3.03) [27] ✓ | Manu Ginobili (+3.10) [29] ✓ | Damian Lillard (+3.06) [42] ✓ |
| 28 | **Tim Duncan** (+3.30) | Jeff Teague (+3.00) [42] ✓ | Nikola Mirotic (+3.02) [18] ✓ | LaMarcus Aldridge (+2.97) [16] ✓ |
| 29 | **Kevin Love** (+3.20) | Nikola Mirotic (+2.94) [18] ✓ | DeMarcus Cousins (+2.95) [9] ✓ | Brandan Wright (+2.85) [69] ✓ |
| 30 | **Marcin Gortat** (+3.20) | DeAndre Jordan (+2.86) [13] ✓ | Isaiah Thomas (+2.88) [71] ✓ | Kyle Korver (+2.83) [13] ✓ |
| 31 | **JJ Redick** (+3.20) | John Wall (+2.79) [60] ✓ | Kyle Korver (+2.80) [13] ✓ | Marc Gasol (+2.80) [20] ✓ |
| 32 | **Manu Ginobili** (+3.20) | Marcus Smart (+2.69) [56] ✓ | Gordon Hayward (+2.69) [26] ✓ | Jrue Holiday (+2.73) [42] ✓ |
| 33 | **Brandon Jennings** (+3.20) | Kyle Korver (+2.67) [13] ✓ | Pau Gasol (+2.66) [100] ✓ | Darren Collison (+2.52) [20] ✓ |
| 34 | **Jimmy Butler** (+3.00) | Mike Conley (+2.65) [37] ✓ | Jrue Holiday (+2.61) [42] ✓ | DeMarre Carroll (+2.51) [37] ✓ |
| 35 | **Lou Williams** (+3.00) | Pau Gasol (+2.64) [100] ✓ | Jonas Jerebko (+2.60) [24] ✓ | Derrick Favors (+2.50) [53] ✓ |
| 36 | **Danilo Gallinari** (+3.00) | Marcin Gortat (+2.48) [29] ✓ | Zach Randolph (+2.48) [37] ✓ | James Johnson (+2.50) [60] ✓ |
| 37 | **Eric Bledsoe** (+2.90) | Darren Collison (+2.39) [20] ✓ | DeAndre Jordan (+2.43) [13] ✓ | Kyle Lowry (+2.47) [22] ✓ |
| 38 | **Zach Randolph** (+2.90) | Jrue Holiday (+2.30) [42] ✓ | Andre Iguodala (+2.40) [81] ✓ | Marcin Gortat (+2.44) [29] ✓ |
| 39 | **Mike Conley** (+2.90) | Jared Dudley (+2.29) [66] ✓ | Tim Duncan (+2.35) [27] ✓ | Anthony Morrow (+2.43) [42] ✓ |
| 40 | **DeMarre Carroll** (+2.90) | Marc Gasol (+2.28) [20] ✓ | Nene (+2.32) [109] ✗ | Ty Lawson (+2.40) [56] ✓ |
| 41 | **Kelly Olynyk** (+2.80) | Ersan Ilyasova (+2.27) [49] ✓ | Marc Gasol (+2.23) [20] ✓ | Kevin Love (+2.34) [29] ✓ |
| 42 | **Damian Lillard** (+2.70) | Eric Bledsoe (+2.24) [37] ✓ | Darren Collison (+2.22) [20] ✓ | Tyson Chandler (+2.28) [47] ✓ |
| 43 | **Jeff Teague** (+2.70) | Patrick Patterson (+2.17) [71] ✓ | Greg Monroe (+2.21) [124] ✗ | Mike Conley (+2.25) [37] ✓ |
| 44 | **Anthony Morrow** (+2.70) | Jonas Jerebko (+2.12) [24] ✓ | Patrick Patterson (+2.17) [71] ✓ | Pau Gasol (+2.23) [100] ✓ |
| 45 | **Zaza Pachulia** (+2.70) | Zach Randolph (+2.11) [37] ✓ | Danilo Gallinari (+2.15) [34] ✓ | Manu Ginobili (+2.08) [29] ✓ |
| 46 | **Jrue Holiday** (+2.70) | Monta Ellis (+2.10) [78] ✓ | Monta Ellis (+2.08) [78] ✓ | Carmelo Anthony (+2.06) [81] ✓ |
| 47 | **Tyson Chandler** (+2.60) | Danilo Gallinari (+2.08) [34] ✓ | Jared Dudley (+2.07) [66] ✓ | Trevor Ariza (+2.00) [116] ✗ |
| 48 | **Serge Ibaka** (+2.60) | CJ Miles (+2.06) [71] ✓ | Kevin Love (+2.06) [29] ✓ | JJ Redick (+1.99) [29] ✓ |
| 49 | **Devin Harris** (+2.50) | Tyson Chandler (+2.04) [47] ✓ | Kelly Olynyk (+2.04) [41] ✓ | Zach Randolph (+1.99) [37] ✓ |
| 50 | **Ersan Ilyasova** (+2.50) | Tim Duncan (+2.04) [27] ✓ | Tyson Chandler (+2.01) [47] ✓ | Nikola Mirotic (+1.98) [18] ✓ |
| 51 | **Rudy Gay** (+2.40) | Jae Crowder (+2.03) [56] ✓ | Ersan Ilyasova (+2.00) [49] ✓ | Nicolas Batum (+1.97) [116] ✗ |
| 52 | **Kemba Walker** (+2.40) | Andre Iguodala (+2.01) [81] ✓ | Eric Bledsoe (+1.92) [37] ✓ | Andrew Bogut (+1.96) [23] ✓ |
| 53 | **Derrick Favors** (+2.30) | AlFarouq Aminu (+1.99) [56] ✓ | Marcin Gortat (+1.86) [29] ✓ | Chandler Parsons (+1.93) [68] ✓ |
| 54 | **Joakim Noah** (+2.20) | Aaron Brooks (+1.98) [162] ✗ | Derrick Favors (+1.82) [53] ✓ | Rudy Gay (+1.90) [51] ✓ |
| 55 | **Andre Roberson** (+2.20) | James Johnson (+1.97) [60] ✓ | Anthony Morrow (+1.81) [42] ✓ | Greg Monroe (+1.83) [124] ✗ |
| 56 | **Ty Lawson** (+2.10) | Kevin Love (+1.97) [29] ✓ | Marcus Smart (+1.80) [56] ✓ | Danilo Gallinari (+1.82) [34] ✓ |
| 57 | **Marcus Smart** (+2.10) | Devin Harris (+1.86) [49] ✓ | James Johnson (+1.78) [60] ✓ | Monta Ellis (+1.82) [78] ✓ |
| 58 | **Jae Crowder** (+2.10) | Iman Shumpert (+1.83) [66] ✓ | Cory Joseph (+1.78) [87] ✓ | Brandon Jennings (+1.81) [29] ✓ |
| 59 | **AlFarouq Aminu** (+2.10) | Tyreke Evans (+1.79) [87] ✓ | Tyreke Evans (+1.73) [87] ✓ | CJ Miles (+1.79) [71] ✓ |
| 60 | **John Wall** (+2.00) | Al Horford (+1.78) [60] ✓ | Timofey Mozgov (+1.68) [78] ✓ | Andre Iguodala (+1.78) [81] ✓ |
| 61 | **Blake Griffin** (+2.00) | Nene (+1.73) [109] ✗ | Jae Crowder (+1.57) [56] ✓ | Isaiah Thomas (+1.77) [71] ✓ |
| 62 | **Al Horford** (+2.00) | Reggie Jackson (+1.73) [81] ✓ | Aaron Brooks (+1.55) [162] ✗ | Patrick Patterson (+1.75) [71] ✓ |
| 63 | **Michael KiddGilchrist** (+2.00) | Greg Monroe (+1.68) [124] ✗ | Iman Shumpert (+1.54) [66] ✓ | Kelly Olynyk (+1.73) [41] ✓ |
| 64 | **Cody Zeller** (+2.00) | JJ Redick (+1.62) [29] ✓ | Al Horford (+1.54) [60] ✓ | AlFarouq Aminu (+1.73) [56] ✓ |
| 65 | **James Johnson** (+2.00) | Ed Davis (+1.60) [71] ✓ | Bradley Beal (+1.51) [81] ✓ | Jonas Jerebko (+1.63) [24] ✓ |
| 66 | **Jared Dudley** (+1.90) | Kelly Olynyk (+1.58) [41] ✓ | CJ Miles (+1.51) [71] ✓ | Jae Crowder (+1.62) [56] ✓ |
| 67 | **Iman Shumpert** (+1.90) | DeMarre Carroll (+1.54) [37] ✓ | Dwight Howard (+1.51) [96] ✓ | Tyreke Evans (+1.54) [87] ✓ |
| 68 | **Chandler Parsons** (+1.80) | Derrick Favors (+1.53) [53] ✓ | Rudy Gay (+1.48) [51] ✓ | Bradley Beal (+1.44) [81] ✓ |
| 69 | **Luol Deng** (+1.70) | Zaza Pachulia (+1.52) [42] ✓ | Amir Johnson (+1.46) [100] ✗ | Eric Bledsoe (+1.42) [37] ✓ |
| 70 | **Brandan Wright** (+1.70) | Luis Scola (+1.49) [105] ✗ | Reggie Jackson (+1.45) [81] ✓ | Paul Pierce (+1.41) [87] ✓ |
| 71 | **Matt Barnes** (+1.60) | Amir Johnson (+1.41) [100] ✗ | Matt Barnes (+1.45) [71] ✓ | Zaza Pachulia (+1.39) [42] ✓ |
| 72 | **Patrick Patterson** (+1.60) | Timofey Mozgov (+1.39) [78] ✓ | Paul Pierce (+1.35) [87] ✓ | Michael KiddGilchrist (+1.34) [60] ✓ |
| 73 | **CJ Miles** (+1.60) | Harrison Barnes (+1.32) [142] ✗ | JJ Redick (+1.35) [29] ✓ | Jared Dudley (+1.30) [66] ✓ |
| 74 | **Ed Davis** (+1.60) | Matt Barnes (+1.30) [71] ✓ | Devin Harris (+1.34) [49] ✓ | Brook Lopez (+1.29) [96] ✓ |
| 75 | **Isaiah Thomas** (+1.60) | Andre Roberson (+1.29) [54] ✓ | AlFarouq Aminu (+1.32) [56] ✓ | Goran Dragic (+1.25) [116] ✗ |
| 76 | **JJ Barea** (+1.60) | Brandon Jennings (+1.23) [29] ✓ | Brandan Wright (+1.28) [69] ✓ | Dirk Nowitzki (+1.24) [96] ✓ |
| 77 | **Pablo Prigioni** (+1.60) | Mike Dunleavy (+1.23) [116] ✗ | Josh Smith (+1.28) [123] ✗ | CJ Watson (+1.24) [81] ✓ |
| 78 | **Monta Ellis** (+1.40) | Bradley Beal (+1.22) [81] ✓ | Donatas Motiejunas (+1.27) [124] ✗ | Robert Covington (+1.24) [94] ✓ |
| 79 | **Timofey Mozgov** (+1.40) | Chandler Parsons (+1.17) [68] ✓ | DeMarre Carroll (+1.26) [37] ✓ | Andre Drummond (+1.22) [139] ✗ |
| 80 | **Jeremy Lin** (+1.40) | Nicolas Batum (+1.16) [116] ✗ | Ed Davis (+1.26) [71] ✓ | Serge Ibaka (+1.21) [47] ✓ |
| 81 | **Reggie Jackson** (+1.30) | Anthony Morrow (+1.13) [42] ✓ | Harrison Barnes (+1.23) [142] ✗ | Ed Davis (+1.18) [71] ✓ |
| 82 | **Bradley Beal** (+1.30) | Goran Dragic (+1.11) [116] ✗ | Brandon Jennings (+1.21) [29] ✓ | Dwight Howard (+1.17) [96] ✓ |
| 83 | **Andre Iguodala** (+1.30) | Derrick Rose (+1.08) [94] ✓ | Anthony Tolliver (+1.20) [154] ✗ | Al Jefferson (+1.14) [189] ✗ |
| 84 | **Carmelo Anthony** (+1.30) | Josh Smith (+1.03) [123] ✗ | Andre Drummond (+1.16) [139] ✗ | Harrison Barnes (+1.13) [142] ✗ |
| 85 | **CJ Watson** (+1.30) | Trevor Booker (+1.00) [105] ✗ | Chandler Parsons (+1.12) [68] ✓ | Kemba Walker (+1.10) [51] ✓ |
| 86 | **Amare Stoudemire** (+1.30) | Dwight Howard (+0.99) [96] ✓ | Andre Roberson (+1.06) [54] ✓ | Matt Barnes (+1.09) [71] ✓ |
| 87 | **Tyreke Evans** (+1.20) | Thaddeus Young (+0.99) [96] ✓ | Dirk Nowitzki (+1.04) [96] ✓ | Thaddeus Young (+0.99) [96] ✓ |
| 88 | **Deron Williams** (+1.20) | Michael KiddGilchrist (+0.99) [60] ✓ | Kenneth Faried (+1.02) [111] ✗ | Mike Dunleavy (+0.95) [116] ✗ |
| 89 | **Paul Pierce** (+1.20) | PJ Tucker (+0.95) [100] ✗ | Carmelo Anthony (+0.99) [81] ✓ | Dwyane Wade (+0.94) [144] ✗ |
| 90 | **Patrick Beverley** (+1.20) | Trevor Ariza (+0.92) [116] ✗ | Goran Dragic (+0.99) [116] ✗ | Luol Deng (+0.94) [69] ✓ |
| 91 | **Cory Joseph** (+1.20) | Paul Pierce (+0.89) [87] ✓ | CJ Watson (+0.98) [81] ✓ | Pablo Prigioni (+0.87) [71] ✓ |
| 92 | **Jonas Valanciunas** (+1.10) | Joakim Noah (+0.87) [54] ✓ | Jared Sullinger (+0.98) [92] ✓ | Devin Harris (+0.82) [49] ✓ |
| 93 | **Jared Sullinger** (+1.10) | Anthony Tolliver (+0.82) [154] ✗ | Robert Covington (+0.97) [94] ✓ | Kenneth Faried (+0.80) [111] ✗ |
| 94 | **Robert Covington** (+1.00) | Victor Oladipo (+0.79) [147] ✗ | Thaddeus Young (+0.96) [96] ✓ | Joakim Noah (+0.79) [54] ✓ |
| 95 | **Derrick Rose** (+1.00) | Jared Sullinger (+0.75) [92] ✓ | Brandon Knight (+0.95) [116] ✗ | Markieff Morris (+0.78) [135] ✗ |
| 96 | **Thaddeus Young** (+0.90) | Cory Joseph (+0.75) [87] ✓ | Zaza Pachulia (+0.92) [42] ✓ | David West (+0.77) [159] ✗ |
| 97 | **Dirk Nowitzki** (+0.90) | Dirk Nowitzki (+0.75) [96] ✓ | PJ Tucker (+0.90) [100] ✗ | DeMar DeRozan (+0.77) [167] ✗ |
| 98 | **Brook Lopez** (+0.90) | Markieff Morris (+0.74) [135] ✗ | Omer Asik (+0.87) [113] ✗ | PJ Tucker (+0.71) [100] ✗ |
| 99 | **Dwight Howard** (+0.90) | CJ Watson (+0.73) [81] ✓ | Mike Dunleavy (+0.87) [116] ✗ | Iman Shumpert (+0.69) [66] ✓ |
| 100 | **Pau Gasol** (+0.80) | JJ Barea (+0.73) [71] ✓ | Luis Scola (+0.86) [105] ✗ | Luis Scola (+0.68) [105] ✗ |

### 2013-14 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Kevin Durant** (+7.60) | Kevin Durant (+6.35) [1] ✓ | Kevin Durant (+7.41) [1] ✓ |
| 2 | **Chris Paul** (+7.10) | LeBron James (+6.18) [4] ✓ | LeBron James (+6.80) [4] ✓ |
| 3 | **James Harden** (+6.30) | Chris Paul (+5.94) [2] ✓ | Chris Paul (+6.79) [2] ✓ |
| 4 | **LeBron James** (+5.80) | James Harden (+5.31) [3] ✓ | James Harden (+5.29) [3] ✓ |
| 5 | **Kevin Love** (+5.70) | Damian Lillard (+4.21) [12] ✓ | Goran Dragic (+4.87) [6] ✓ |
| 6 | **Goran Dragic** (+4.80) | Goran Dragic (+4.21) [6] ✓ | Kevin Love (+4.76) [5] ✓ |
| 7 | **Kyle Lowry** (+4.40) | Kevin Love (+4.04) [5] ✓ | Dirk Nowitzki (+4.33) [7] ✓ |
| 8 | **Dirk Nowitzki** (+4.40) | Kyle Lowry (+3.70) [7] ✓ | Brandan Wright (+4.19) [40] ✓ |
| 9 | **Carmelo Anthony** (+4.20) | Russell Westbrook (+3.62) [15] ✓ | Carmelo Anthony (+3.80) [9] ✓ |
| 10 | **Manu Ginobili** (+4.00) | Carmelo Anthony (+3.26) [9] ✓ | Kyle Lowry (+3.71) [7] ✓ |
| 11 | **Patty Mills** (+3.90) | Manu Ginobili (+3.18) [10] ✓ | Blake Griffin (+3.57) [17] ✓ |
| 12 | **Damian Lillard** (+3.60) | Isaiah Thomas (+3.02) [13] ✓ | Isaiah Thomas (+3.48) [13] ✓ |
| 13 | **Isaiah Thomas** (+3.50) | Mike Conley (+2.90) [13] ✓ | Manu Ginobili (+3.25) [10] ✓ |
| 14 | **Mike Conley** (+3.50) | Dirk Nowitzki (+2.86) [7] ✓ | Russell Westbrook (+3.24) [15] ✓ |
| 15 | **Russell Westbrook** (+3.30) | Blake Griffin (+2.53) [17] ✓ | Nikola Pekovic (+3.10) [64] ✓ |
| 16 | **Ty Lawson** (+3.20) | Kyrie Irving (+2.52) [27] ✓ | Damian Lillard (+3.03) [12] ✓ |
| 17 | **Blake Griffin** (+2.90) | DJ Augustin (+2.44) [31] ✓ | Dwyane Wade (+2.82) [47] ✓ |
| 18 | **Wesley Matthews** (+2.80) | Patty Mills (+2.43) [11] ✓ | Ty Lawson (+2.80) [16] ✓ |
| 19 | **Marco Belinelli** (+2.80) | Paul George (+2.30) [22] ✓ | Mike Conley (+2.80) [13] ✓ |
| 20 | **Jamal Crawford** (+2.80) | Jamal Crawford (+2.28) [18] ✓ | Wesley Matthews (+2.68) [18] ✓ |
| 21 | **Joe Johnson** (+2.70) | John Wall (+1.82) [36] ✓ | Patty Mills (+2.59) [11] ✓ |
| 22 | **Paul George** (+2.60) | Ty Lawson (+1.79) [16] ✓ | Anthony Davis (+2.52) [57] ✓ |
| 23 | **Chandler Parsons** (+2.60) | Jrue Holiday (+1.76) [28] ✓ | Kawhi Leonard (+2.51) [40] ✓ |
| 24 | **Deron Williams** (+2.60) | Deron Williams (+1.66) [22] ✓ | Chandler Parsons (+2.47) [22] ✓ |
| 25 | **Vince Carter** (+2.40) | Dwyane Wade (+1.63) [47] ✓ | Paul George (+2.46) [22] ✓ |
| 26 | **Nick Young** (+2.40) | Ricky Rubio (+1.57) [36] ✓ | Deron Williams (+2.34) [22] ✓ |
| 27 | **Kyrie Irving** (+2.30) | Wesley Matthews (+1.54) [18] ✓ | DeMar DeRozan (+2.30) [40] ✓ |
| 28 | **Patrick Beverley** (+2.20) | Darren Collison (+1.51) [64] ✓ | Jamal Crawford (+2.28) [18] ✓ |
| 29 | **Jrue Holiday** (+2.20) | Klay Thompson (+1.49) [31] ✓ | Tony Parker (+2.17) [71] ✓ |
| 30 | **Brandon Jennings** (+2.20) | Vince Carter (+1.44) [25] ✓ | DeMarcus Cousins (+2.15) [40] ✓ |
| 31 | **Klay Thompson** (+2.10) | DeMar DeRozan (+1.40) [40] ✓ | Chris Bosh (+1.99) [136] ✗ |
| 32 | **Randy Foye** (+2.10) | Andre Iguodala (+1.39) [57] ✓ | Marco Belinelli (+1.94) [18] ✓ |
| 33 | **DJ Augustin** (+2.10) | Joe Johnson (+1.39) [21] ✓ | Kyrie Irving (+1.93) [27] ✓ |
| 34 | **Channing Frye** (+2.00) | Nikola Pekovic (+1.33) [64] ✓ | Andre Drummond (+1.91) [53] ✓ |
| 35 | **Josh McRoberts** (+2.00) | Jeff Teague (+1.33) [90] ✓ | Robin Lopez (+1.86) [68] ✓ |
| 36 | **Ricky Rubio** (+1.90) | Nick Young (+1.33) [25] ✓ | LaMarcus Aldridge (+1.86) [64] ✓ |
| 37 | **Nicolas Batum** (+1.90) | Rudy Gay (+1.32) [64] ✓ | Andre Iguodala (+1.84) [57] ✓ |
| 38 | **John Wall** (+1.90) | Eric Bledsoe (+1.31) [47] ✓ | Nicolas Batum (+1.84) [36] ✓ |
| 39 | **Kyle Korver** (+1.90) | Kevin Martin (+1.29) [83] ✓ | Eric Bledsoe (+1.67) [47] ✓ |
| 40 | **Kawhi Leonard** (+1.70) | Kyle Korver (+1.27) [36] ✓ | Kevin Martin (+1.64) [83] ✓ |
| 41 | **DeMarcus Cousins** (+1.70) | Kawhi Leonard (+1.25) [40] ✓ | Joe Johnson (+1.61) [21] ✓ |
| 42 | **DeMar DeRozan** (+1.70) | Kemba Walker (+1.24) [51] ✓ | Trevor Ariza (+1.61) [68] ✓ |
| 43 | **Pablo Prigioni** (+1.70) | Mario Chalmers (+1.21) [47] ✓ | Jose Calderon (+1.61) [45] ✓ |
| 44 | **Brandan Wright** (+1.70) | Marco Belinelli (+1.21) [18] ✓ | Al Jefferson (+1.59) [149] ✗ |
| 45 | **Jose Calderon** (+1.60) | Nicolas Batum (+1.21) [36] ✓ | John Wall (+1.57) [36] ✓ |
| 46 | **Mirza Teletovic** (+1.60) | Brandon Knight (+1.19) [71] ✓ | Nick Young (+1.55) [25] ✓ |
| 47 | **Joakim Noah** (+1.50) | Brandon Jennings (+1.19) [28] ✓ | Klay Thompson (+1.45) [31] ✓ |
| 48 | **Mario Chalmers** (+1.50) | Jose Calderon (+1.18) [45] ✓ | Tyreke Evans (+1.40) [57] ✓ |
| 49 | **Eric Bledsoe** (+1.50) | Pablo Prigioni (+1.16) [40] ✓ | David Lee (+1.33) [112] ✗ |
| 50 | **Dwyane Wade** (+1.50) | Brandan Wright (+1.13) [40] ✓ | DJ Augustin (+1.28) [31] ✓ |
| 51 | **Kemba Walker** (+1.40) | LaMarcus Aldridge (+1.13) [64] ✓ | Arron Afflalo (+1.21) [57] ✓ |
| 52 | **Ray Allen** (+1.40) | Tony Parker (+1.06) [71] ✓ | Terrence Jones (+1.19) [71] ✓ |
| 53 | **Andre Drummond** (+1.30) | Randy Foye (+1.03) [31] ✓ | Joakim Noah (+1.17) [47] ✓ |
| 54 | **Zach Randolph** (+1.30) | George Hill (+1.02) [112] ✗ | Kyle Korver (+1.16) [36] ✓ |
| 55 | **Gerald Green** (+1.30) | Anthony Morrow (+1.00) [53] ✓ | Jodie Meeks (+1.14) [83] ✓ |
| 56 | **Anthony Morrow** (+1.30) | Kenneth Faried (+1.00) [57] ✓ | Gerald Green (+1.13) [53] ✓ |
| 57 | **Anthony Davis** (+1.20) | DeMarcus Cousins (+0.94) [40] ✓ | Ricky Rubio (+1.12) [36] ✓ |
| 58 | **Andre Iguodala** (+1.20) | Bradley Beal (+0.94) [112] ✗ | Jrue Holiday (+1.12) [28] ✓ |
| 59 | **Tyreke Evans** (+1.20) | Jimmy Butler (+0.92) [97] ✓ | Corey Brewer (+1.12) [112] ✗ |
| 60 | **Kenneth Faried** (+1.20) | Chandler Parsons (+0.90) [22] ✓ | Monta Ellis (+1.12) [68] ✓ |
| 61 | **Arron Afflalo** (+1.20) | Alec Burks (+0.84) [71] ✓ | Paul Pierce (+1.10) [126] ✗ |
| 62 | **Jameer Nelson** (+1.20) | Reggie Jackson (+0.84) [71] ✓ | Jeff Teague (+1.08) [90] ✓ |
| 63 | **Lou Williams** (+1.20) | Monta Ellis (+0.83) [68] ✓ | Darren Collison (+1.08) [64] ✓ |
| 64 | **LaMarcus Aldridge** (+1.10) | Jameer Nelson (+0.82) [57] ✓ | DeMarre Carroll (+1.07) [83] ✓ |
| 65 | **Darren Collison** (+1.10) | Anthony Davis (+0.79) [57] ✓ | Pablo Prigioni (+1.05) [40] ✓ |
| 66 | **Nikola Pekovic** (+1.10) | Lance Stephenson (+0.77) [90] ✓ | Rudy Gay (+1.04) [64] ✓ |
| 67 | **Rudy Gay** (+1.10) | Josh McRoberts (+0.75) [34] ✓ | Vince Carter (+1.01) [25] ✓ |
| 68 | **Robin Lopez** (+1.00) | Greivis Vasquez (+0.71) [112] ✗ | Dwight Howard (+1.00) [144] ✗ |
| 69 | **Trevor Ariza** (+1.00) | Trevor Ariza (+0.70) [68] ✓ | Mason Plumlee (+0.99) [176] ✗ |
| 70 | **Monta Ellis** (+1.00) | Gordon Hayward (+0.69) [83] ✓ | Gordon Hayward (+0.93) [83] ✓ |
| 71 | **Reggie Jackson** (+0.90) | David West (+0.67) [90] ✓ | Markieff Morris (+0.89) [129] ✗ |
| 72 | **Alec Burks** (+0.90) | Gerald Green (+0.64) [53] ✓ | Luol Deng (+0.88) [112] ✗ |
| 73 | **Matthew Dellavedova** (+0.90) | Channing Frye (+0.63) [34] ✓ | Anthony Morrow (+0.87) [53] ✓ |
| 74 | **Tony Parker** (+0.90) | Trey Burke (+0.59) [109] ✗ | Amare Stoudemire (+0.86) [166] ✗ |
| 75 | **Shelvin Mack** (+0.90) | David Lee (+0.59) [112] ✗ | Kenneth Faried (+0.85) [57] ✓ |
| 76 | **Terrence Jones** (+0.90) | Patrick Beverley (+0.58) [28] ✓ | Brandon Jennings (+0.84) [28] ✓ |
| 77 | **Brandon Knight** (+0.90) | Arron Afflalo (+0.58) [57] ✓ | Randy Foye (+0.78) [31] ✓ |
| 78 | **Martell Webster** (+0.90) | Anthony Tolliver (+0.54) [97] ✗ | Anderson Varejao (+0.74) [97] ✓ |
| 79 | **PJ Tucker** (+0.80) | Mirza Teletovic (+0.54) [45] ✓ | Eric Gordon (+0.72) [90] ✓ |
| 80 | **Boris Diaw** (+0.80) | Mike Dunleavy (+0.50) [112] ✗ | Paul Millsap (+0.71) [109] ✗ |
| 81 | **Matt Barnes** (+0.80) | Austin Rivers (+0.49) [139] ✗ | Alec Burks (+0.68) [71] ✓ |
| 82 | **Marvin Williams** (+0.80) | Al Jefferson (+0.48) [149] ✗ | Greivis Vasquez (+0.63) [112] ✗ |
| 83 | **DeMarre Carroll** (+0.70) | Jeremy Lamb (+0.47) [97] ✗ | Marc Gasol (+0.61) [176] ✗ |
| 84 | **DeAndre Jordan** (+0.70) | Terrence Ross (+0.46) [97] ✓ | Tiago Splitter (+0.58) [166] ✗ |
| 85 | **Danny Green** (+0.70) | Chris Bosh (+0.46) [136] ✗ | Courtney Lee (+0.54) [83] ✓ |
| 86 | **Gordon Hayward** (+0.70) | Corey Brewer (+0.45) [112] ✗ | Josh McRoberts (+0.51) [34] ✓ |
| 87 | **Courtney Lee** (+0.70) | Andre Drummond (+0.45) [53] ✓ | David West (+0.50) [90] ✓ |
| 88 | **Jodie Meeks** (+0.70) | Joakim Noah (+0.43) [47] ✓ | Kemba Walker (+0.50) [51] ✓ |
| 89 | **Kevin Martin** (+0.70) | Zach Randolph (+0.41) [53] ✓ | Patrick Beverley (+0.46) [28] ✓ |
| 90 | **David West** (+0.60) | Ramon Sessions (+0.41) [90] ✓ | PJ Tucker (+0.45) [79] ✓ |
| 91 | **Jeff Teague** (+0.60) | Danny Green (+0.40) [83] ✓ | Reggie Jackson (+0.37) [71] ✓ |
| 92 | **Lance Stephenson** (+0.60) | Luol Deng (+0.38) [112] ✗ | Greg Monroe (+0.36) [112] ✗ |
| 93 | **Marcus Thornton** (+0.60) | Terrence Jones (+0.38) [71] ✓ | Ramon Sessions (+0.35) [90] ✓ |
| 94 | **Ramon Sessions** (+0.60) | Eric Gordon (+0.35) [90] ✓ | Matthew Dellavedova (+0.33) [71] ✓ |
| 95 | **Mike Miller** (+0.60) | Paul Millsap (+0.33) [109] ✗ | Jeremy Lamb (+0.33) [97] ✗ |
| 96 | **Eric Gordon** (+0.60) | Lou Williams (+0.29) [57] ✓ | Tim Hardaway Jr. (+0.33) [129] ✗ |
| 97 | **Jimmy Butler** (+0.50) | Dwight Howard (+0.29) [144] ✗ | Chris Andersen (+0.31) [97] ✗ |
| 98 | **Anderson Varejao** (+0.50) | Mike Miller (+0.24) [90] ✓ | Brandon Knight (+0.30) [71] ✓ |
| 99 | **Jared Sullinger** (+0.50) | Dion Waiters (+0.23) [149] ✗ | Bradley Beal (+0.29) [112] ✗ |
| 100 | **Terrence Ross** (+0.50) | Derek Fisher (+0.22) [112] ✗ | Marcin Gortat (+0.28) [156] ✗ |

### 2014-15 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+8.50) | Chris Paul (+6.61) [1] ✓ | Chris Paul (+6.99) [1] ✓ |
| 2 | **James Harden** (+7.70) | James Harden (+6.12) [2] ✓ | James Harden (+5.71) [2] ✓ |
| 3 | **Russell Westbrook** (+6.10) | LeBron James (+5.95) [5] ✓ | LeBron James (+5.62) [5] ✓ |
| 4 | **Kyrie Irving** (+5.50) | Kyrie Irving (+5.20) [4] ✓ | Anthony Davis (+5.11) [9] ✓ |
| 5 | **LeBron James** (+5.30) | Russell Westbrook (+5.17) [3] ✓ | Russell Westbrook (+5.02) [3] ✓ |
| 6 | **Lou Williams** (+5.20) | Damian Lillard (+4.40) [11] ✓ | Jimmy Butler (+4.38) [20] ✓ |
| 7 | **Kyle Korver** (+4.60) | Isaiah Thomas (+3.94) [8] ✓ | Blake Griffin (+4.19) [20] ✓ |
| 8 | **Isaiah Thomas** (+4.50) | Lou Williams (+3.93) [6] ✓ | Kyrie Irving (+4.13) [4] ✓ |
| 9 | **Anthony Davis** (+4.30) | Klay Thompson (+3.82) [9] ✓ | Lou Williams (+4.08) [6] ✓ |
| 10 | **Klay Thompson** (+4.30) | Blake Griffin (+3.54) [20] ✓ | Klay Thompson (+4.00) [9] ✓ |
| 11 | **Damian Lillard** (+4.00) | Mike Conley (+2.71) [30] ✓ | George Hill (+3.81) [12] ✓ |
| 12 | **George Hill** (+3.90) | George Hill (+2.66) [12] ✓ | Kawhi Leonard (+3.44) [15] ✓ |
| 13 | **Ty Lawson** (+3.80) | Aaron Brooks (+2.54) [55] ✓ | JJ Redick (+3.36) [29] ✓ |
| 14 | **Carmelo Anthony** (+3.80) | Kyle Lowry (+2.52) [18] ✓ | Ty Lawson (+3.18) [13] ✓ |
| 15 | **Kawhi Leonard** (+3.70) | Jeff Teague (+2.46) [34] ✓ | Gordon Hayward (+3.01) [20] ✓ |
| 16 | **Rudy Gay** (+3.50) | Gordon Hayward (+2.46) [20] ✓ | Isaiah Thomas (+2.98) [8] ✓ |
| 17 | **DeAndre Jordan** (+3.40) | Jimmy Butler (+2.43) [20] ✓ | Carmelo Anthony (+2.96) [13] ✓ |
| 18 | **Kyle Lowry** (+3.30) | Carmelo Anthony (+2.43) [13] ✓ | Damian Lillard (+2.92) [11] ✓ |
| 19 | **Jrue Holiday** (+3.30) | Jrue Holiday (+2.40) [18] ✓ | Wesley Matthews (+2.78) [33] ✓ |
| 20 | **Gordon Hayward** (+3.20) | Brandon Jennings (+2.34) [23] ✓ | Brandon Jennings (+2.74) [23] ✓ |
| 21 | **Jimmy Butler** (+3.20) | Dwyane Wade (+2.30) [40] ✓ | Anthony Morrow (+2.73) [26] ✓ |
| 22 | **Blake Griffin** (+3.20) | Ty Lawson (+2.30) [13] ✓ | Jeff Teague (+2.72) [34] ✓ |
| 23 | **Danny Green** (+3.10) | Kyle Korver (+2.29) [7] ✓ | Rudy Gay (+2.65) [16] ✓ |
| 24 | **Brandon Jennings** (+3.10) | JJ Redick (+2.21) [29] ✓ | Kyle Lowry (+2.48) [18] ✓ |
| 25 | **Danilo Gallinari** (+2.80) | Anthony Davis (+2.11) [9] ✓ | Al Horford (+2.33) [96] ✓ |
| 26 | **Anthony Morrow** (+2.70) | Kawhi Leonard (+2.06) [15] ✓ | LaMarcus Aldridge (+2.33) [30] ✓ |
| 27 | **Tyreke Evans** (+2.60) | John Wall (+1.99) [37] ✓ | Goran Dragic (+2.33) [42] ✓ |
| 28 | **Chandler Parsons** (+2.60) | Rudy Gay (+1.90) [16] ✓ | Darren Collison (+2.31) [49] ✓ |
| 29 | **JJ Redick** (+2.50) | Reggie Jackson (+1.89) [40] ✓ | Dirk Nowitzki (+2.24) [34] ✓ |
| 30 | **LaMarcus Aldridge** (+2.40) | Tyreke Evans (+1.89) [27] ✓ | Dwyane Wade (+2.22) [40] ✓ |
| 31 | **Mike Conley** (+2.40) | LaMarcus Aldridge (+1.87) [30] ✓ | Jrue Holiday (+2.21) [18] ✓ |
| 32 | **Patrick Patterson** (+2.40) | Eric Gordon (+1.80) [87] ✓ | Chandler Parsons (+2.18) [27] ✓ |
| 33 | **Wesley Matthews** (+2.30) | Darren Collison (+1.75) [49] ✓ | Kyle Korver (+2.00) [7] ✓ |
| 34 | **Jeff Teague** (+2.20) | Jamal Crawford (+1.67) [67] ✓ | Danilo Gallinari (+1.99) [25] ✓ |
| 35 | **Dirk Nowitzki** (+2.20) | Khris Middleton (+1.64) [49] ✓ | Kevin Love (+1.92) [49] ✓ |
| 36 | **Gerald Green** (+2.20) | Draymond Green (+1.46) [57] ✓ | Mike Conley (+1.91) [30] ✓ |
| 37 | **John Wall** (+2.10) | Manu Ginobili (+1.45) [49] ✓ | Brandan Wright (+1.89) [98] ✓ |
| 38 | **Devin Harris** (+2.10) | Danilo Gallinari (+1.43) [25] ✓ | John Wall (+1.85) [37] ✓ |
| 39 | **Ersan Ilyasova** (+2.10) | Danny Green (+1.43) [23] ✓ | Khris Middleton (+1.84) [49] ✓ |
| 40 | **Reggie Jackson** (+2.00) | Gerald Green (+1.43) [34] ✓ | DeMarre Carroll (+1.75) [42] ✓ |
| 41 | **Dwyane Wade** (+2.00) | Dirk Nowitzki (+1.42) [34] ✓ | Paul Millsap (+1.66) [69] ✓ |
| 42 | **DeMarre Carroll** (+1.90) | Anthony Morrow (+1.41) [26] ✓ | Tyson Chandler (+1.60) [119] ✗ |
| 43 | **Nikola Mirotic** (+1.90) | Bradley Beal (+1.36) [74] ✓ | Patrick Patterson (+1.59) [30] ✓ |
| 44 | **Goran Dragic** (+1.90) | Patrick Patterson (+1.33) [30] ✓ | Brook Lopez (+1.59) [104] ✗ |
| 45 | **JJ Barea** (+1.90) | Derrick Rose (+1.26) [110] ✗ | Chris Bosh (+1.55) [150] ✗ |
| 46 | **Luol Deng** (+1.80) | Goran Dragic (+1.24) [42] ✓ | Tyreke Evans (+1.54) [27] ✓ |
| 47 | **Jae Crowder** (+1.80) | JJ Barea (+1.20) [42] ✓ | Derrick Favors (+1.52) [110] ✗ |
| 48 | **Joe Johnson** (+1.80) | Joe Johnson (+1.18) [46] ✓ | Monta Ellis (+1.52) [79] ✓ |
| 49 | **Khris Middleton** (+1.70) | Ryan Anderson (+1.15) [57] ✓ | Danny Green (+1.52) [23] ✓ |
| 50 | **Eric Bledsoe** (+1.70) | Wesley Matthews (+1.15) [33] ✓ | Marc Gasol (+1.49) [59] ✓ |
| 51 | **Kevin Love** (+1.70) | Marc Gasol (+1.13) [59] ✓ | DeMar DeRozan (+1.48) [130] ✗ |
| 52 | **Darren Collison** (+1.70) | Zach Randolph (+1.11) [55] ✓ | Jamal Crawford (+1.45) [67] ✓ |
| 53 | **Manu Ginobili** (+1.70) | Eric Bledsoe (+1.10) [49] ✓ | Greg Monroe (+1.42) [123] ✗ |
| 54 | **Kevin Martin** (+1.70) | Kevin Love (+1.08) [49] ✓ | Luol Deng (+1.37) [46] ✓ |
| 55 | **Zach Randolph** (+1.60) | Chandler Parsons (+1.04) [27] ✓ | Kevin Martin (+1.36) [49] ✓ |
| 56 | **Aaron Brooks** (+1.60) | Monta Ellis (+1.03) [79] ✓ | Pau Gasol (+1.23) [98] ✓ |
| 57 | **Draymond Green** (+1.50) | Victor Oladipo (+0.92) [87] ✓ | Tim Duncan (+1.23) [130] ✗ |
| 58 | **Ryan Anderson** (+1.50) | Paul Millsap (+0.92) [69] ✓ | Amare Stoudemire (+1.22) [67] ✓ |
| 59 | **Marc Gasol** (+1.40) | Mo Williams (+0.73) [61] ✓ | DeMarcus Cousins (+1.20) [72] ✓ |
| 60 | **CJ Miles** (+1.40) | Kobe Bryant (+0.73) [61] ✓ | Nikola Vucevic (+1.18) [159] ✗ |
| 61 | **Kemba Walker** (+1.20) | Pau Gasol (+0.72) [98] ✓ | DeAndre Jordan (+1.15) [17] ✓ |
| 62 | **Deron Williams** (+1.20) | Nikola Mirotic (+0.69) [42] ✓ | James Johnson (+1.15) [79] ✓ |
| 63 | **Ed Davis** (+1.20) | Devin Harris (+0.67) [37] ✓ | Reggie Jackson (+1.11) [40] ✓ |
| 64 | **Robert Covington** (+1.20) | Kevin Martin (+0.64) [49] ✓ | Tyler Zeller (+1.07) [141] ✗ |
| 65 | **Mo Williams** (+1.20) | DeAndre Jordan (+0.63) [17] ✓ | Ed Davis (+1.05) [61] ✓ |
| 66 | **Kobe Bryant** (+1.20) | Paul Pierce (+0.63) [69] ✓ | Paul Pierce (+1.05) [69] ✓ |
| 67 | **Amare Stoudemire** (+1.10) | CJ Miles (+0.59) [59] ✓ | Ersan Ilyasova (+1.05) [37] ✓ |
| 68 | **Jamal Crawford** (+1.10) | Mike Dunleavy (+0.59) [79] ✓ | Manu Ginobili (+1.02) [49] ✓ |
| 69 | **Paul Millsap** (+1.00) | Thaddeus Young (+0.56) [87] ✓ | Zach Randolph (+1.01) [55] ✓ |
| 70 | **Matt Barnes** (+1.00) | Ersan Ilyasova (+0.56) [37] ✓ | JJ Barea (+0.94) [42] ✓ |
| 71 | **Paul Pierce** (+1.00) | Greivis Vasquez (+0.55) [141] ✗ | Jonas Valanciunas (+0.93) [123] ✗ |
| 72 | **DeMarcus Cousins** (+0.90) | Brandon Knight (+0.55) [87] ✓ | Tony Parker (+0.90) [87] ✓ |
| 73 | **Jeremy Lin** (+0.90) | Deron Williams (+0.49) [61] ✓ | Devin Harris (+0.87) [37] ✓ |
| 74 | **Bradley Beal** (+0.80) | Kemba Walker (+0.49) [61] ✓ | Jodie Meeks (+0.86) [123] ✗ |
| 75 | **Kentavious CaldwellPope** (+0.80) | Trey Burke (+0.47) [119] ✗ | Thaddeus Young (+0.86) [87] ✓ |
| 76 | **Kelly Olynyk** (+0.80) | Matt Barnes (+0.47) [69] ✓ | Eric Gordon (+0.86) [87] ✓ |
| 77 | **Jonas Jerebko** (+0.80) | Luol Deng (+0.47) [46] ✓ | Eric Bledsoe (+0.83) [49] ✓ |
| 78 | **Wilson Chandler** (+0.80) | DeMar DeRozan (+0.43) [130] ✗ | Ryan Anderson (+0.82) [57] ✓ |
| 79 | **Monta Ellis** (+0.70) | Kenneth Faried (+0.41) [84] ✓ | Draymond Green (+0.80) [57] ✓ |
| 80 | **James Johnson** (+0.70) | Ed Davis (+0.40) [61] ✓ | Deron Williams (+0.77) [61] ✓ |
| 81 | **Cory Joseph** (+0.70) | DeMarcus Cousins (+0.37) [72] ✓ | Joe Johnson (+0.72) [46] ✓ |
| 82 | **Mike Dunleavy** (+0.70) | Tony Parker (+0.36) [87] ✓ | Jonas Jerebko (+0.71) [74] ✓ |
| 83 | **Anthony Tolliver** (+0.70) | DeMarre Carroll (+0.36) [42] ✓ | Harrison Barnes (+0.69) [137] ✗ |
| 84 | **Nicolas Batum** (+0.60) | Nikola Vucevic (+0.33) [159] ✗ | Kenneth Faried (+0.66) [84] ✓ |
| 85 | **Kenneth Faried** (+0.60) | Anthony Tolliver (+0.31) [79] ✓ | Nikola Mirotic (+0.63) [42] ✓ |
| 86 | **Omri Casspi** (+0.60) | Evan Fournier (+0.31) [104] ✗ | CJ Miles (+0.63) [59] ✓ |
| 87 | **Thaddeus Young** (+0.50) | Harrison Barnes (+0.29) [137] ✗ | Mike Dunleavy (+0.60) [79] ✓ |
| 88 | **Elfrid Payton** (+0.50) | James Johnson (+0.23) [79] ✓ | Marreese Speights (+0.59) [194] ✗ |
| 89 | **Brandon Knight** (+0.50) | Andre Miller (+0.22) [87] ✓ | Andre Iguodala (+0.56) [137] ✗ |
| 90 | **Jared Sullinger** (+0.50) | Andre Iguodala (+0.18) [137] ✗ | CJ Watson (+0.53) [110] ✗ |
| 91 | **Rodney Stuckey** (+0.50) | Jeremy Lin (+0.18) [72] ✓ | Marcin Gortat (+0.51) [141] ✗ |
| 92 | **Victor Oladipo** (+0.50) | Wilson Chandler (+0.17) [74] ✓ | Tobias Harris (+0.50) [119] ✗ |
| 93 | **Eric Gordon** (+0.50) | Greg Monroe (+0.16) [123] ✗ | Bradley Beal (+0.50) [74] ✓ |
| 94 | **Tony Parker** (+0.50) | Jae Crowder (+0.16) [46] ✓ | Trevor Ariza (+0.49) [123] ✗ |
| 95 | **Andre Miller** (+0.50) | Robert Covington (+0.15) [61] ✓ | Kemba Walker (+0.48) [61] ✓ |
| 96 | **Al Horford** (+0.40) | CJ Watson (+0.14) [110] ✗ | Brandon Bass (+0.47) [202] ✗ |
| 97 | **Tim Hardaway Jr.** (+0.40) | DJ Augustin (+0.13) [130] ✗ | Amir Johnson (+0.43) [123] ✗ |
| 98 | **Pau Gasol** (+0.30) | Kentavious CaldwellPope (+0.10) [74] ✓ | Nicolas Batum (+0.43) [84] ✓ |
| 99 | **Marcus Smart** (+0.30) | Jordan Clarkson (+0.08) [104] ✗ | Cory Joseph (+0.40) [79] ✓ |
| 100 | **Brandan Wright** (+0.30) | Cory Joseph (+0.06) [79] ✓ | Brandon Knight (+0.38) [87] ✓ |

### 2013-14 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.00) | Andrew Bogut (+4.53) [4] ✓ | Andrew Bogut (+3.58) [4] ✓ |
| 2 | **Draymond Green** (+4.60) | Draymond Green (+3.78) [2] ✓ | Kawhi Leonard (+3.44) [1] ✓ |
| 3 | **Joakim Noah** (+4.50) | Anthony Davis (+3.41) [33] ✓ | Draymond Green (+3.32) [2] ✓ |
| 4 | **Andrew Bogut** (+4.40) | Tiago Splitter (+3.37) [6] ✓ | DeAndre Jordan (+3.19) [64] ✓ |
| 5 | **Michael KiddGilchrist** (+4.40) | Marcin Gortat (+3.37) [27] ✓ | Jimmy Butler (+3.17) [14] ✓ |
| 6 | **Tiago Splitter** (+4.20) | Kawhi Leonard (+3.20) [1] ✓ | Tony Allen (+3.07) [30] ✓ |
| 7 | **Danny Green** (+4.00) | Kevin Garnett (+3.19) [11] ✓ | Joakim Noah (+3.05) [3] ✓ |
| 8 | **Chris Paul** (+3.90) | Jae Crowder (+3.15) [18] ✓ | Danny Green (+2.82) [7] ✓ |
| 9 | **Nene** (+3.80) | Paul George (+3.09) [21] ✓ | Ricky Rubio (+2.79) [47] ✓ |
| 10 | **Anderson Varejao** (+3.60) | Danny Green (+2.76) [7] ✓ | Andre Iguodala (+2.69) [25] ✓ |
| 11 | **Nick Calathes** (+3.50) | Anderson Varejao (+2.75) [10] ✓ | Paul George (+2.62) [21] ✓ |
| 12 | **Ian Mahinmi** (+3.50) | Andre Iguodala (+2.73) [25] ✓ | Kyle OQuinn (+2.43) [52] ✓ |
| 13 | **Kevin Garnett** (+3.50) | Nene (+2.68) [9] ✓ | Roy Hibbert (+2.43) [14] ✓ |
| 14 | **Jimmy Butler** (+3.40) | Jimmy Butler (+2.65) [14] ✓ | David West (+2.32) [57] ✓ |
| 15 | **Roy Hibbert** (+3.40) | Paul Millsap (+2.61) [23] ✓ | Paul Millsap (+2.20) [23] ✓ |
| 16 | **DeMarcus Cousins** (+3.30) | CJ Watson (+2.60) [17] ✓ | Ian Mahinmi (+2.14) [11] ✓ |
| 17 | **CJ Watson** (+3.20) | Derek Fisher (+2.58) [43] ✓ | Anderson Varejao (+2.08) [10] ✓ |
| 18 | **Tim Duncan** (+3.00) | Michael KiddGilchrist (+2.46) [4] ✓ | Tim Duncan (+2.05) [18] ✓ |
| 19 | **Kris Humphries** (+3.00) | Joakim Noah (+2.44) [3] ✓ | CJ Watson (+2.03) [17] ✓ |
| 20 | **Jae Crowder** (+3.00) | Tony Allen (+2.39) [30] ✓ | Chris Paul (+2.00) [8] ✓ |
| 21 | **Paul George** (+2.90) | Chris Bosh (+2.38) [72] ✓ | Bismack Biyombo (+1.84) [64] ✓ |
| 22 | **Marc Gasol** (+2.80) | Tim Duncan (+2.33) [18] ✓ | Kevin Garnett (+1.76) [11] ✓ |
| 23 | **Paul Millsap** (+2.70) | Iman Shumpert (+2.33) [72] ✓ | Manu Ginobili (+1.74) [64] ✓ |
| 24 | **Shane Battier** (+2.70) | LaMarcus Aldridge (+2.30) [37] ✓ | George Hill (+1.72) [40] ✓ |
| 25 | **DeMarre Carroll** (+2.60) | Gerald Wallace (+2.23) [52] ✓ | Iman Shumpert (+1.70) [72] ✓ |
| 26 | **Andre Iguodala** (+2.60) | Al Jefferson (+2.21) [47] ✓ | Serge Ibaka (+1.70) [33] ✓ |
| 27 | **Mario Chalmers** (+2.50) | Paul Pierce (+2.20) [47] ✓ | Nick Calathes (+1.65) [11] ✓ |
| 28 | **Marcin Gortat** (+2.50) | Mario Chalmers (+2.15) [27] ✓ | Anthony Davis (+1.65) [33] ✓ |
| 29 | **Samuel Dalembert** (+2.50) | Ian Mahinmi (+2.15) [11] ✓ | Kirk Hinrich (+1.56) [40] ✓ |
| 30 | **Dwight Howard** (+2.40) | Kyle OQuinn (+2.14) [52] ✓ | Phil Pressey (+1.55) [76] ✓ |
| 31 | **Tony Allen** (+2.40) | DeAndre Jordan (+2.14) [64] ✓ | Thabo Sefolosha (+1.51) [33] ✓ |
| 32 | **Victor Oladipo** (+2.40) | Nick Calathes (+2.12) [11] ✓ | Taj Gibson (+1.46) [83] ✓ |
| 33 | **Kemba Walker** (+2.30) | Ersan Ilyasova (+2.06) [88] ✓ | DeMarcus Cousins (+1.45) [16] ✓ |
| 34 | **Anthony Davis** (+2.30) | Kirk Hinrich (+2.03) [40] ✓ | Lance Stephenson (+1.45) [164] ✗ |
| 35 | **Serge Ibaka** (+2.30) | DeMarcus Cousins (+2.00) [16] ✓ | Nene (+1.44) [9] ✓ |
| 36 | **Thabo Sefolosha** (+2.30) | Chris Andersen (+2.00) [52] ✓ | Trevor Ariza (+1.42) [88] ✓ |
| 37 | **LaMarcus Aldridge** (+2.20) | Roy Hibbert (+1.97) [14] ✓ | Jae Crowder (+1.41) [18] ✓ |
| 38 | **Nikola Pekovic** (+2.20) | Darrell Arthur (+1.97) [45] ✓ | Al Jefferson (+1.40) [47] ✓ |
| 39 | **Eric Bledsoe** (+2.20) | Amir Johnson (+1.93) [62] ✓ | Corey Brewer (+1.38) [88] ✓ |
| 40 | **George Hill** (+2.10) | Pablo Prigioni (+1.90) [124] ✗ | DeMarre Carroll (+1.35) [25] ✓ |
| 41 | **Kirk Hinrich** (+2.10) | Ricky Rubio (+1.89) [47] ✓ | Dwight Howard (+1.35) [30] ✓ |
| 42 | **Kosta Koufos** (+2.10) | Robin Lopez (+1.89) [43] ✓ | Marcin Gortat (+1.34) [27] ✓ |
| 43 | **Robin Lopez** (+2.00) | Shane Battier (+1.87) [23] ✓ | Elton Brand (+1.32) [83] ✓ |
| 44 | **Derek Fisher** (+2.00) | Miles Plumlee (+1.84) [52] ✓ | LaMarcus Aldridge (+1.26) [37] ✓ |
| 45 | **Patrick Beverley** (+1.90) | Dwight Howard (+1.83) [30] ✓ | Patrick Patterson (+1.23) [83] ✓ |
| 46 | **Darrell Arthur** (+1.90) | Marc Gasol (+1.80) [22] ✓ | Michael KiddGilchrist (+1.22) [4] ✓ |
| 47 | **Ricky Rubio** (+1.80) | Nick Collison (+1.80) [88] ✓ | Chris Andersen (+1.19) [52] ✓ |
| 48 | **Al Jefferson** (+1.80) | DeMarre Carroll (+1.79) [25] ✓ | Derek Fisher (+1.17) [43] ✓ |
| 49 | **Paul Pierce** (+1.80) | Patrick Patterson (+1.71) [83] ✓ | Mario Chalmers (+1.17) [27] ✓ |
| 50 | **Jeremy Lin** (+1.80) | Kosta Koufos (+1.68) [40] ✓ | John Wall (+1.16) [136] ✗ |
| 51 | **Kendrick Perkins** (+1.70) | Victor Oladipo (+1.67) [30] ✓ | Gerald Wallace (+1.14) [52] ✓ |
| 52 | **Darren Collison** (+1.60) | DeJuan Blair (+1.66) [129] ✗ | Amir Johnson (+1.12) [62] ✓ |
| 53 | **Chris Andersen** (+1.60) | Chris Paul (+1.65) [8] ✓ | Kemba Walker (+1.08) [33] ✓ |
| 54 | **Kyle OQuinn** (+1.60) | Manu Ginobili (+1.63) [64] ✓ | Marc Gasol (+1.05) [22] ✓ |
| 55 | **Gerald Wallace** (+1.60) | Samuel Dalembert (+1.61) [27] ✓ | Mike Dunleavy (+1.05) [121] ✗ |
| 56 | **Miles Plumlee** (+1.60) | Kemba Walker (+1.61) [33] ✓ | Patrick Beverley (+1.00) [45] ✓ |
| 57 | **David West** (+1.50) | Elton Brand (+1.60) [83] ✓ | Josh McRoberts (+0.95) [180] ✗ |
| 58 | **Kyle Lowry** (+1.30) | PJ Tucker (+1.60) [76] ✓ | Kosta Koufos (+0.95) [40] ✓ |
| 59 | **Nate Wolters** (+1.30) | Josh Smith (+1.58) [83] ✓ | Russell Westbrook (+0.94) [99] ✗ |
| 60 | **Omri Casspi** (+1.30) | George Hill (+1.57) [40] ✓ | Patty Mills (+0.92) [76] ✓ |
| 61 | **Tayshaun Prince** (+1.30) | Taj Gibson (+1.56) [83] ✓ | Chris Bosh (+0.91) [72] ✓ |
| 62 | **David Lee** (+1.20) | Thabo Sefolosha (+1.56) [33] ✓ | Klay Thompson (+0.91) [83] ✓ |
| 63 | **Amir Johnson** (+1.20) | David West (+1.56) [57] ✓ | Darrell Arthur (+0.88) [45] ✓ |
| 64 | **DeAndre Jordan** (+1.10) | Nicolas Batum (+1.53) [124] ✗ | Andre Drummond (+0.86) [114] ✗ |
| 65 | **Manu Ginobili** (+1.10) | Channing Frye (+1.51) [99] ✗ | Tiago Splitter (+0.86) [6] ✓ |
| 66 | **Jared Sullinger** (+1.10) | Patrick Beverley (+1.50) [45] ✓ | Victor Oladipo (+0.85) [30] ✓ |
| 67 | **Andray Blatche** (+1.10) | Timofey Mozgov (+1.48) [64] ✓ | Shaun Livingston (+0.85) [76] ✓ |
| 68 | **Jeremy Evans** (+1.10) | Tyson Chandler (+1.47) [64] ✓ | Kyle Korver (+0.77) [158] ✗ |
| 69 | **Timofey Mozgov** (+1.10) | Blake Griffin (+1.41) [110] ✗ | David Lee (+0.76) [62] ✓ |
| 70 | **Tyson Chandler** (+1.10) | Jason Thompson (+1.40) [110] ✗ | Derrick Favors (+0.75) [99] ✗ |
| 71 | **Bismack Biyombo** (+1.10) | Trevor Ariza (+1.38) [88] ✓ | Carlos Boozer (+0.71) [188] ✗ |
| 72 | **Chris Bosh** (+1.00) | Shaun Livingston (+1.36) [76] ✓ | Jeremy Lamb (+0.70) [124] ✗ |
| 73 | **Courtney Lee** (+1.00) | Bismack Biyombo (+1.31) [64] ✓ | Nikola Vucevic (+0.70) [76] ✓ |
| 74 | **Iman Shumpert** (+1.00) | Omri Casspi (+1.30) [58] ✓ | Courtney Lee (+0.69) [72] ✓ |
| 75 | **ETwaun Moore** (+1.00) | Phil Pressey (+1.29) [76] ✓ | Andray Blatche (+0.69) [64] ✓ |
| 76 | **Kevin Love** (+0.90) | Andray Blatche (+1.22) [64] ✓ | Maurice Harkless (+0.68) [99] ✗ |
| 77 | **Patty Mills** (+0.90) | Josh McRoberts (+1.18) [180] ✗ | Nicolas Batum (+0.65) [124] ✗ |
| 78 | **PJ Tucker** (+0.90) | Patty Mills (+1.17) [76] ✓ | Steven Adams (+0.65) [121] ✗ |
| 79 | **Shaun Livingston** (+0.90) | ETwaun Moore (+1.16) [72] ✓ | Dante Cunningham (+0.64) [114] ✗ |
| 80 | **Nikola Vucevic** (+0.90) | Jared Sullinger (+1.13) [64] ✓ | Shane Battier (+0.64) [23] ✓ |
| 81 | **Avery Bradley** (+0.90) | Corey Brewer (+1.13) [88] ✓ | PJ Tucker (+0.63) [76] ✓ |
| 82 | **Phil Pressey** (+0.90) | Wesley Matthews (+1.11) [129] ✗ | Channing Frye (+0.62) [99] ✗ |
| 83 | **Klay Thompson** (+0.80) | Dirk Nowitzki (+1.11) [99] ✓ | Miles Plumlee (+0.59) [52] ✓ |
| 84 | **Taj Gibson** (+0.80) | Steven Adams (+1.11) [121] ✗ | Tyson Chandler (+0.57) [64] ✓ |
| 85 | **Patrick Patterson** (+0.80) | Andrew Nicholson (+1.10) [164] ✗ | Kyle Lowry (+0.56) [58] ✓ |
| 86 | **Josh Smith** (+0.80) | David Lee (+1.08) [62] ✓ | Matt Barnes (+0.55) [143] ✗ |
| 87 | **Elton Brand** (+0.80) | Jeremy Lamb (+1.08) [124] ✗ | Terrence Jones (+0.53) [202] ✗ |
| 88 | **Trevor Ariza** (+0.70) | Kendrick Perkins (+1.06) [51] ✓ | Josh Smith (+0.52) [83] ✓ |
| 89 | **Corey Brewer** (+0.70) | Jeremy Evans (+1.05) [64] ✓ | Deron Williams (+0.52) [92] ✓ |
| 90 | **Nick Collison** (+0.70) | Kyle Lowry (+0.99) [58] ✓ | Eric Bledsoe (+0.50) [37] ✓ |
| 91 | **Ersan Ilyasova** (+0.70) | Andre Drummond (+0.98) [114] ✗ | Michael CarterWilliams (+0.50) [92] ✓ |
| 92 | **Deron Williams** (+0.60) | Giannis Antetokounmpo (+0.96) [114] ✗ | Paul Pierce (+0.49) [47] ✓ |
| 93 | **Luol Deng** (+0.60) | Anthony Tolliver (+0.95) [143] ✗ | Boris Diaw (+0.47) [110] ✗ |
| 94 | **Michael CarterWilliams** (+0.60) | Deron Williams (+0.91) [92] ✓ | Blake Griffin (+0.43) [110] ✗ |
| 95 | **Matthew Dellavedova** (+0.60) | Jeremy Lin (+0.84) [47] ✓ | Terrence Ross (+0.41) [129] ✗ |
| 96 | **Andrea Bargnani** (+0.60) | Nikola Pekovic (+0.83) [37] ✓ | Kris Humphries (+0.39) [18] ✓ |
| 97 | **Spencer Hawes** (+0.50) | Robert Sacre (+0.80) [114] ✗ | Cody Zeller (+0.38) [136] ✗ |
| 98 | **AlFarouq Aminu** (+0.50) | Brandan Wright (+0.80) [195] ✗ | Pablo Prigioni (+0.38) [124] ✗ |
| 99 | **Dirk Nowitzki** (+0.40) | Kentavious CaldwellPope (+0.80) [114] ✗ | Giannis Antetokounmpo (+0.36) [114] ✗ |
| 100 | **Isaiah Thomas** (+0.40) | Serge Ibaka (+0.79) [33] ✓ | Gerald Henderson (+0.34) [136] ✗ |

### 2014-15 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.20) | Andrew Bogut (+4.82) [5] ✓ | Kawhi Leonard (+4.47) [1] ✓ |
| 2 | **Draymond Green** (+5.10) | Draymond Green (+4.63) [2] ✓ | Tony Allen (+4.32) [3] ✓ |
| 3 | **Rudy Gobert** (+4.80) | Rudy Gobert (+4.35) [3] ✓ | Draymond Green (+3.75) [2] ✓ |
| 4 | **Tony Allen** (+4.80) | Tony Allen (+3.99) [3] ✓ | Andrew Bogut (+3.21) [5] ✓ |
| 5 | **Andrew Bogut** (+4.70) | Kawhi Leonard (+3.59) [1] ✓ | DeAndre Jordan (+3.15) [60] ✓ |
| 6 | **Anthony Davis** (+4.50) | Nerlens Noel (+3.53) [19] ✓ | Nerlens Noel (+2.89) [19] ✓ |
| 7 | **DeMarcus Cousins** (+4.40) | Anthony Davis (+3.31) [6] ✓ | Anthony Davis (+2.86) [6] ✓ |
| 8 | **Marcin Gortat** (+3.60) | Nene (+3.08) [17] ✓ | Rudy Gobert (+2.78) [3] ✓ |
| 9 | **Tim Duncan** (+3.50) | Andre Roberson (+2.94) [10] ✓ | Danny Green (+2.72) [14] ✓ |
| 10 | **Andre Roberson** (+3.40) | AlFarouq Aminu (+2.93) [20] ✓ | AlFarouq Aminu (+2.59) [20] ✓ |
| 11 | **Kosta Koufos** (+3.30) | Danny Green (+2.66) [14] ✓ | Tim Duncan (+2.49) [9] ✓ |
| 12 | **Zaza Pachulia** (+3.20) | Jonas Jerebko (+2.65) [17] ✓ | Paul Millsap (+2.35) [26] ✓ |
| 13 | **Khris Middleton** (+3.10) | Tim Duncan (+2.60) [9] ✓ | Khris Middleton (+2.07) [13] ✓ |
| 14 | **Danny Green** (+3.00) | Tyson Chandler (+2.60) [20] ✓ | Andre Roberson (+2.05) [10] ✓ |
| 15 | **Serge Ibaka** (+3.00) | Marcin Gortat (+2.59) [8] ✓ | Bismack Biyombo (+2.05) [102] ✗ |
| 16 | **Michael KiddGilchrist** (+3.00) | DeMarcus Cousins (+2.59) [7] ✓ | Marcus Smart (+1.98) [42] ✓ |
| 17 | **Jonas Jerebko** (+2.80) | Khris Middleton (+2.58) [13] ✓ | John Wall (+1.97) [127] ✗ |
| 18 | **Nene** (+2.80) | Kosta Koufos (+2.50) [11] ✓ | DeMarcus Cousins (+1.95) [7] ✓ |
| 19 | **Nerlens Noel** (+2.70) | Marcus Smart (+2.44) [42] ✓ | Marcin Gortat (+1.93) [8] ✓ |
| 20 | **Marc Gasol** (+2.60) | Dwight Howard (+2.40) [40] ✓ | Michael KiddGilchrist (+1.74) [14] ✓ |
| 21 | **Tyson Chandler** (+2.60) | Michael KiddGilchrist (+2.34) [14] ✓ | John Henson (+1.64) [85] ✓ |
| 22 | **Joakim Noah** (+2.60) | Josh Smith (+2.33) [20] ✓ | Nicolas Batum (+1.55) [136] ✗ |
| 23 | **Josh Smith** (+2.60) | Nikola Mirotic (+2.32) [33] ✓ | Zaza Pachulia (+1.53) [12] ✓ |
| 24 | **AlFarouq Aminu** (+2.60) | Iman Shumpert (+2.31) [29] ✓ | Trevor Ariza (+1.51) [102] ✗ |
| 25 | **Alex Len** (+2.50) | Derrick Favors (+2.29) [34] ✓ | Andre Drummond (+1.50) [93] ✓ |
| 26 | **Paul Millsap** (+2.40) | Paul Millsap (+2.26) [26] ✓ | Kosta Koufos (+1.48) [11] ✓ |
| 27 | **Timofey Mozgov** (+2.40) | Zaza Pachulia (+2.26) [12] ✓ | Iman Shumpert (+1.40) [29] ✓ |
| 28 | **Omer Asik** (+2.40) | Jared Dudley (+2.26) [42] ✓ | Cody Zeller (+1.40) [34] ✓ |
| 29 | **Darren Collison** (+2.30) | Wesley Matthews (+2.25) [54] ✓ | Kelly Olynyk (+1.40) [34] ✓ |
| 30 | **Iman Shumpert** (+2.30) | Andre Iguodala (+2.22) [46] ✓ | Jae Crowder (+1.36) [102] ✗ |
| 31 | **Michael CarterWilliams** (+2.30) | Kelly Olynyk (+2.20) [34] ✓ | James Johnson (+1.35) [54] ✓ |
| 32 | **Luc Mbah a Moute** (+2.30) | Omer Asik (+2.19) [26] ✓ | Nikola Mirotic (+1.34) [33] ✓ |
| 33 | **Nikola Mirotic** (+2.20) | Timofey Mozgov (+2.13) [26] ✓ | Serge Ibaka (+1.33) [14] ✓ |
| 34 | **Chris Paul** (+2.10) | John Henson (+2.09) [85] ✓ | Marc Gasol (+1.31) [20] ✓ |
| 35 | **Derrick Favors** (+2.10) | Greg Monroe (+2.05) [111] ✗ | Dwight Howard (+1.31) [40] ✓ |
| 36 | **Kelly Olynyk** (+2.10) | Pau Gasol (+1.94) [85] ✓ | Nene (+1.28) [17] ✓ |
| 37 | **Cody Zeller** (+2.10) | Michael CarterWilliams (+1.92) [29] ✓ | Chris Paul (+1.28) [34] ✓ |
| 38 | **Steven Adams** (+2.00) | Joakim Noah (+1.85) [20] ✓ | George Hill (+1.27) [45] ✓ |
| 39 | **Roy Hibbert** (+2.00) | DeAndre Jordan (+1.80) [60] ✓ | Giannis Antetokounmpo (+1.26) [146] ✗ |
| 40 | **LaMarcus Aldridge** (+1.90) | Andre Drummond (+1.80) [93] ✓ | Marvin Williams (+1.25) [69] ✓ |
| 41 | **Dwight Howard** (+1.90) | Jimmy Butler (+1.77) [136] ✗ | Miles Plumlee (+1.25) [85] ✓ |
| 42 | **Marcus Smart** (+1.80) | Brandan Wright (+1.77) [54] ✓ | Alex Len (+1.22) [25] ✓ |
| 43 | **Jared Dudley** (+1.80) | Cory Joseph (+1.73) [85] ✓ | Al Jefferson (+1.22) [102] ✗ |
| 44 | **Pablo Prigioni** (+1.80) | Amir Johnson (+1.69) [73] ✓ | Andre Iguodala (+1.22) [46] ✓ |
| 45 | **George Hill** (+1.70) | Manu Ginobili (+1.65) [51] ✓ | Jimmy Butler (+1.20) [136] ✗ |
| 46 | **Kevin Love** (+1.60) | PJ Tucker (+1.62) [54] ✓ | CJ Miles (+1.16) [111] ✗ |
| 47 | **Al Horford** (+1.60) | Pablo Prigioni (+1.59) [42] ✓ | Elfrid Payton (+1.16) [111] ✗ |
| 48 | **Andre Iguodala** (+1.60) | Al Horford (+1.57) [46] ✓ | Robert Covington (+1.12) [136] ✗ |
| 49 | **Mario Chalmers** (+1.50) | James Johnson (+1.55) [54] ✓ | Josh Smith (+1.12) [20] ✓ |
| 50 | **Kris Humphries** (+1.50) | Alex Len (+1.48) [25] ✓ | Michael CarterWilliams (+1.12) [29] ✓ |
| 51 | **Manu Ginobili** (+1.40) | LaMarcus Aldridge (+1.46) [40] ✓ | Pablo Prigioni (+1.11) [42] ✓ |
| 52 | **Alan Anderson** (+1.40) | Ersan Ilyasova (+1.44) [93] ✓ | Jared Dudley (+1.07) [42] ✓ |
| 53 | **Robin Lopez** (+1.40) | Donatas Motiejunas (+1.43) [73] ✓ | Manu Ginobili (+1.06) [51] ✓ |
| 54 | **Wesley Matthews** (+1.30) | Mario Chalmers (+1.42) [49] ✓ | LeBron James (+1.04) [132] ✗ |
| 55 | **Zach Randolph** (+1.30) | Jae Crowder (+1.42) [102] ✗ | Jeff Teague (+1.04) [85] ✓ |
| 56 | **PJ Tucker** (+1.30) | Luis Scola (+1.39) [82] ✓ | PJ Tucker (+1.03) [54] ✓ |
| 57 | **James Johnson** (+1.30) | Zach Randolph (+1.38) [54] ✓ | Joakim Noah (+1.01) [20] ✓ |
| 58 | **Brandan Wright** (+1.30) | Luc Mbah a Moute (+1.36) [29] ✓ | Pau Gasol (+1.00) [85] ✓ |
| 59 | **Langston Galloway** (+1.30) | Corey Brewer (+1.30) [69] ✓ | Roy Hibbert (+1.00) [38] ✓ |
| 60 | **DeAndre Jordan** (+1.20) | Jared Sullinger (+1.29) [85] ✓ | Derrick Favors (+0.98) [34] ✓ |
| 61 | **Eric Bledsoe** (+1.20) | Bismack Biyombo (+1.28) [102] ✗ | Zach Randolph (+0.98) [54] ✓ |
| 62 | **Kemba Walker** (+1.20) | Cody Zeller (+1.27) [34] ✓ | Brandan Wright (+0.96) [54] ✓ |
| 63 | **Jonas Valanciunas** (+1.20) | Trevor Ariza (+1.25) [102] ✗ | Bradley Beal (+0.94) [85] ✓ |
| 64 | **Gerald Henderson** (+1.10) | John Wall (+1.22) [127] ✗ | Luis Scola (+0.94) [82] ✓ |
| 65 | **CJ Watson** (+1.10) | Jonas Valanciunas (+1.22) [60] ✓ | Matt Barnes (+0.93) [79] ✓ |
| 66 | **Klay Thompson** (+1.00) | Roy Hibbert (+1.21) [38] ✓ | Jonas Jerebko (+0.92) [17] ✓ |
| 67 | **DeMarre Carroll** (+1.00) | Markieff Morris (+1.19) [69] ✓ | Al Horford (+0.90) [46] ✓ |
| 68 | **Patrick Beverley** (+1.00) | Taj Gibson (+1.16) [73] ✓ | David West (+0.89) [102] ✗ |
| 69 | **Corey Brewer** (+0.90) | Steven Adams (+1.14) [38] ✓ | Wesley Matthews (+0.86) [54] ✓ |
| 70 | **Markieff Morris** (+0.90) | Chris Kaman (+1.13) [111] ✗ | Jerami Grant (+0.85) [102] ✗ |
| 71 | **Marvin Williams** (+0.90) | Tyler Zeller (+1.12) [102] ✗ | James Harden (+0.84) [121] ✗ |
| 72 | **Trevor Booker** (+0.90) | Al Jefferson (+1.12) [102] ✗ | Kyle Korver (+0.83) [121] ✗ |
| 73 | **Monta Ellis** (+0.80) | Marc Gasol (+1.10) [20] ✓ | DeMarre Carroll (+0.77) [66] ✓ |
| 74 | **Amir Johnson** (+0.80) | Brook Lopez (+1.10) [82] ✓ | Klay Thompson (+0.74) [66] ✓ |
| 75 | **Donatas Motiejunas** (+0.80) | Serge Ibaka (+1.09) [14] ✓ | CJ Watson (+0.71) [64] ✓ |
| 76 | **Derrick Rose** (+0.80) | Jeff Teague (+1.07) [85] ✓ | Russell Westbrook (+0.69) [151] ✗ |
| 77 | **Taj Gibson** (+0.80) | Trevor Booker (+1.06) [69] ✓ | Tyson Chandler (+0.68) [20] ✓ |
| 78 | **Kendrick Perkins** (+0.80) | Monta Ellis (+1.05) [73] ✓ | Kent Bazemore (+0.66) [127] ✗ |
| 79 | **JJ Redick** (+0.70) | Klay Thompson (+1.04) [66] ✓ | LaMarcus Aldridge (+0.64) [40] ✓ |
| 80 | **Matt Barnes** (+0.70) | George Hill (+1.03) [45] ✓ | Kemba Walker (+0.62) [60] ✓ |
| 81 | **KJ McDaniels** (+0.70) | Kent Bazemore (+1.02) [127] ✗ | Eric Bledsoe (+0.59) [60] ✓ |
| 82 | **Kyle Lowry** (+0.60) | Robin Lopez (+1.02) [51] ✓ | Evan Turner (+0.58) [111] ✗ |
| 83 | **Brook Lopez** (+0.60) | LeBron James (+1.02) [132] ✗ | KJ McDaniels (+0.52) [79] ✓ |
| 84 | **Luis Scola** (+0.60) | Marvin Williams (+1.00) [69] ✓ | Jrue Holiday (+0.51) [155] ✗ |
| 85 | **Mike Conley** (+0.50) | Kyle Lowry (+0.99) [82] ✓ | Otto Porter Jr. (+0.45) [191] ✗ |
| 86 | **Jeff Teague** (+0.50) | Matt Barnes (+0.99) [79] ✓ | Kris Humphries (+0.45) [49] ✓ |
| 87 | **Pau Gasol** (+0.50) | Kevin Love (+0.98) [46] ✓ | Harrison Barnes (+0.44) [132] ✗ |
| 88 | **Bradley Beal** (+0.50) | Kris Humphries (+0.95) [49] ✓ | Markieff Morris (+0.43) [69] ✓ |
| 89 | **Jared Sullinger** (+0.50) | Harrison Barnes (+0.95) [132] ✗ | Kevin Love (+0.42) [46] ✓ |
| 90 | **Cory Joseph** (+0.50) | David West (+0.95) [102] ✗ | Greg Monroe (+0.41) [111] ✗ |
| 91 | **Miles Plumlee** (+0.50) | Dante Exum (+0.93) [162] ✗ | Corey Brewer (+0.37) [69] ✓ |
| 92 | **John Henson** (+0.50) | CJ Miles (+0.92) [111] ✗ | Paul Pierce (+0.37) [111] ✗ |
| 93 | **Devin Harris** (+0.40) | Miles Plumlee (+0.92) [85] ✓ | Timofey Mozgov (+0.37) [26] ✓ |
| 94 | **Thaddeus Young** (+0.40) | Kendrick Perkins (+0.91) [73] ✓ | Steven Adams (+0.36) [38] ✓ |
| 95 | **Ed Davis** (+0.40) | DeMarre Carroll (+0.90) [66] ✓ | Mario Chalmers (+0.36) [49] ✓ |
| 96 | **Jeremy Lin** (+0.40) | Anthony Tolliver (+0.89) [199] ✗ | Mike Dunleavy (+0.35) [151] ✗ |
| 97 | **Ersan Ilyasova** (+0.40) | Nicolas Batum (+0.89) [136] ✗ | Mike Conley (+0.34) [85] ✓ |
| 98 | **Andre Drummond** (+0.40) | Ed Davis (+0.86) [93] ✓ | Rajon Rondo (+0.34) [93] ✗ |
| 99 | **Tony Snell** (+0.40) | Patrick Patterson (+0.84) [162] ✗ | Monta Ellis (+0.30) [73] ✓ |
| 100 | **Chris Bosh** (+0.40) | CJ Watson (+0.84) [64] ✓ | Mason Plumlee (+0.28) [214] ✗ |

