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

**No minutes filter is applied in this run** — every rated player is
ranked, and the training rows carry no minimum-minutes filter either.
The table below is retained for reference: it shows what a derived
threshold *would* have been.

Derived, not chosen: the **lowest minutes total among any true top-100**
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

**Applied here: no threshold (0 minutes, both splits).**

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
| ours (direct total) | 1.775 | 1.325 | +0.734 | +0.868 | +0.868 |
| ours (offense+defense) | 1.831 | 1.349 | +0.717 | +0.861 | +0.861 |
| Paine (eRO+eRD) | 1.936 | 1.379 | +0.684 | +0.841 | +0.846 |

**offense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.073 | 0.811 | +0.803 | +0.915 | +0.904 |
| Paine (eRO) | 1.308 | 0.959 | +0.707 | +0.847 | +0.825 |

**defense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.482 | 1.103 | +0.596 | +0.795 | +0.797 |
| Paine (eRD) | 1.641 | 1.194 | +0.504 | +0.726 | +0.728 |

## Summary — true top-100 members recovered (hits@100)

**total**

| season | split | pool | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) | ρ ours (direct total) | ρ ours (offense+defense) | ρ Paine (eRO+eRD) |
|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 246 | 80/100 | 81/100 | 80/100 | +0.882 | +0.890 | +0.889 |
| 2014-15 | Regular season | 246 | 82/100 | 83/100 | 85/100 | +0.882 | +0.883 | +0.901 |
| **all** | | | **162/200** | **164/200** | **165/200** |  |  |  |

Precision@K for total, summed over 2 cells:

| K | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|
| 10 | 13/20 | 15/20 | 15/20 |
| 25 | 38/50 | 38/50 | 35/50 |
| 50 | 77/100 | 75/100 | 75/100 |
| 100 | 162/200 | 164/200 | 165/200 |

**offense**

| season | split | pool | ours | Paine (eRO) | ρ ours | ρ Paine (eRO) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 246 | 84/100 | 80/100 | +0.923 | +0.867 |
| 2014-15 | Regular season | 246 | 84/100 | 80/100 | +0.916 | +0.858 |
| **all** | | | **168/200** | **160/200** |  |  |

Precision@K for offense, summed over 2 cells:

| K | ours | Paine (eRO) |
|---|---|---|
| 10 | 16/20 | 17/20 |
| 25 | 40/50 | 40/50 |
| 50 | 77/100 | 76/100 |
| 100 | 168/200 | 160/200 |

**defense**

| season | split | pool | ours | Paine (eRD) | ρ ours | ρ Paine (eRD) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 246 | 81/100 | 76/100 | +0.862 | +0.795 |
| 2014-15 | Regular season | 246 | 77/100 | 73/100 | +0.833 | +0.801 |
| **all** | | | **158/200** | **149/200** |  |  |

Precision@K for defense, summed over 2 cells:

| K | ours | Paine (eRD) |
|---|---|---|
| 10 | 9/20 | 11/20 |
| 25 | 35/50 | 31/50 |
| 50 | 71/100 | 68/100 |
| 100 | 158/200 | 149/200 |

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
summing our two part-models are near-interchangeable: R² +0.734 vs +0.717, ρ +0.868 vs +0.861, hits@100 162/200 vs 164/200.

**Against Paine on the total.** R² +0.734 vs +0.684, RMSE 1.775 vs 1.936, ρ +0.868 vs +0.846; hits@100 162/200 vs 165/200.

**Offense.** ours R² +0.803 / ρ +0.904 / hits@100 168/200; Paine R² +0.707 / ρ +0.825 / hits@100 160/200.

**Defense.** ours R² +0.596 / ρ +0.797 / hits@100 158/200; Paine R² +0.504 / ρ +0.728 / hits@100 149/200.

Read the precision@K tables above rather than a single cutoff: they show
where each system's advantage actually lives, and a hits count at one
arbitrary K is decided by hundredths of a point among near-tied players.

## Leaderboards

`[n]` after a predicted name is that player's *true* rank; ✓ means they are
genuinely in the true top 100.

### 2013-14 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+11.00) | Chris Paul (+7.07) [1] ✓ | Chris Paul (+8.52) [1] ✓ | Chris Paul (+8.79) [1] ✓ |
| 2 | **Kevin Durant** (+7.10) | Kevin Durant (+6.49) [2] ✓ | Kevin Durant (+6.83) [2] ✓ | Kevin Durant (+7.21) [2] ✓ |
| 3 | **Kawhi Leonard** (+6.70) | LeBron James (+5.96) [15] ✓ | LeBron James (+5.89) [15] ✓ | LeBron James (+6.77) [15] ✓ |
| 4 | **Kevin Love** (+6.60) | Paul George (+5.46) [8] ✓ | Paul George (+5.40) [8] ✓ | Kawhi Leonard (+5.95) [3] ✓ |
| 5 | **James Harden** (+6.10) | Kevin Love (+5.35) [4] ✓ | Manu Ginobili (+4.90) [9] ✓ | James Harden (+5.50) [5] ✓ |
| 6 | **Joakim Noah** (+5.90) | Blake Griffin (+5.29) [34] ✓ | Blake Griffin (+4.68) [34] ✓ | Paul George (+5.08) [8] ✓ |
| 7 | **Kyle Lowry** (+5.70) | Kawhi Leonard (+4.97) [3] ✓ | Kawhi Leonard (+4.47) [3] ✓ | Manu Ginobili (+4.99) [9] ✓ |
| 8 | **Paul George** (+5.60) | James Harden (+4.90) [5] ✓ | Kyle Lowry (+4.40) [7] ✓ | Kevin Love (+4.86) [4] ✓ |
| 9 | **Manu Ginobili** (+5.10) | Manu Ginobili (+4.37) [9] ✓ | Kevin Love (+4.37) [4] ✓ | Goran Dragic (+4.71) [10] ✓ |
| 10 | **Goran Dragic** (+5.00) | Andrew Bogut (+4.31) [36] ✓ | James Harden (+4.36) [5] ✓ | Andre Iguodala (+4.53) [21] ✓ |
| 11 | **DeMarcus Cousins** (+5.00) | Dirk Nowitzki (+4.19) [13] ✓ | Dirk Nowitzki (+4.31) [13] ✓ | Kyle Lowry (+4.27) [7] ✓ |
| 12 | **Patty Mills** (+4.80) | Kyle Lowry (+4.12) [7] ✓ | Andrew Bogut (+4.25) [36] ✓ | Joakim Noah (+4.22) [6] ✓ |
| 13 | **Dirk Nowitzki** (+4.70) | Andre Iguodala (+3.93) [21] ✓ | Andre Iguodala (+4.09) [21] ✓ | Russell Westbrook (+4.18) [25] ✓ |
| 14 | **Danny Green** (+4.70) | Carmelo Anthony (+3.82) [21] ✓ | Goran Dragic (+4.03) [10] ✓ | Anthony Davis (+4.18) [28] ✓ |
| 15 | **LeBron James** (+4.60) | Ricky Rubio (+3.69) [25] ✓ | Patty Mills (+3.95) [12] ✓ | Blake Griffin (+4.00) [34] ✓ |
| 16 | **Anderson Varejao** (+4.10) | Patty Mills (+3.64) [12] ✓ | Jimmy Butler (+3.83) [19] ✓ | Ricky Rubio (+3.91) [25] ✓ |
| 17 | **Patrick Beverley** (+4.10) | Russell Westbrook (+3.58) [25] ✓ | Anthony Davis (+3.81) [28] ✓ | Brandan Wright (+3.89) [115] ✗ |
| 18 | **Mario Chalmers** (+4.00) | Jimmy Butler (+3.55) [19] ✓ | Mike Conley (+3.70) [21] ✓ | Dirk Nowitzki (+3.81) [13] ✓ |
| 19 | **Jimmy Butler** (+3.90) | Draymond Green (+3.50) [29] ✓ | Russell Westbrook (+3.54) [25] ✓ | DeMarcus Cousins (+3.60) [10] ✓ |
| 20 | **Isaiah Thomas** (+3.90) | Goran Dragic (+3.44) [10] ✓ | Damian Lillard (+3.50) [53] ✓ | Patty Mills (+3.50) [12] ✓ |
| 21 | **Carmelo Anthony** (+3.80) | Paul Pierce (+3.36) [59] ✓ | Carmelo Anthony (+3.49) [21] ✓ | Jimmy Butler (+3.31) [19] ✓ |
| 22 | **Kemba Walker** (+3.80) | Anthony Davis (+3.34) [28] ✓ | Pablo Prigioni (+3.42) [70] ✓ | Carmelo Anthony (+3.28) [21] ✓ |
| 23 | **Mike Conley** (+3.80) | Mike Conley (+3.34) [21] ✓ | LaMarcus Aldridge (+3.28) [29] ✓ | DeAndre Jordan (+3.19) [62] ✓ |
| 24 | **Andre Iguodala** (+3.80) | Damian Lillard (+3.32) [53] ✓ | Joakim Noah (+3.23) [6] ✓ | LaMarcus Aldridge (+3.12) [29] ✓ |
| 25 | **Ricky Rubio** (+3.70) | Anderson Varejao (+3.27) [16] ✓ | DeMarcus Cousins (+3.18) [10] ✓ | Mike Conley (+3.12) [21] ✓ |
| 26 | **Eric Bledsoe** (+3.70) | Derek Fisher (+3.21) [50] ✓ | Draymond Green (+3.14) [29] ✓ | Trevor Ariza (+3.03) [66] ✓ |
| 27 | **Russell Westbrook** (+3.70) | Paul Millsap (+3.14) [36] ✓ | George Hill (+3.10) [48] ✓ | Dwyane Wade (+3.01) [83] ✓ |
| 28 | **Anthony Davis** (+3.50) | Joakim Noah (+3.09) [6] ✓ | Derek Fisher (+3.07) [50] ✓ | Al Jefferson (+2.99) [78] ✓ |
| 29 | **LaMarcus Aldridge** (+3.40) | Isaiah Thomas (+3.07) [19] ✓ | Danny Green (+2.95) [13] ✓ | Tony Allen (+2.97) [46] ✓ |
| 30 | **Draymond Green** (+3.40) | Chris Bosh (+3.06) [93] ✓ | Paul Millsap (+2.95) [36] ✓ | Paul Millsap (+2.91) [36] ✓ |
| 31 | **DeMarre Carroll** (+3.30) | Pablo Prigioni (+3.02) [70] ✓ | Ricky Rubio (+2.87) [25] ✓ | Chris Bosh (+2.91) [93] ✓ |
| 32 | **Nikola Pekovic** (+3.30) | Trevor Ariza (+2.90) [66] ✓ | Deron Williams (+2.85) [34] ✓ | Deron Williams (+2.86) [34] ✓ |
| 33 | **Tiago Splitter** (+3.30) | Nicolas Batum (+2.89) [59] ✓ | Chris Bosh (+2.82) [93] ✓ | David West (+2.83) [51] ✓ |
| 34 | **Blake Griffin** (+3.20) | LaMarcus Aldridge (+2.83) [29] ✓ | Kemba Walker (+2.82) [21] ✓ | Anderson Varejao (+2.81) [16] ✓ |
| 35 | **Deron Williams** (+3.20) | Kemba Walker (+2.82) [21] ✓ | Isaiah Thomas (+2.77) [19] ✓ | Andre Drummond (+2.77) [74] ✓ |
| 36 | **Paul Millsap** (+3.10) | DeMarcus Cousins (+2.73) [10] ✓ | Mario Chalmers (+2.74) [18] ✓ | Andrew Bogut (+2.74) [36] ✓ |
| 37 | **Andrew Bogut** (+3.10) | Deron Williams (+2.66) [34] ✓ | David West (+2.73) [51] ✓ | John Wall (+2.73) [66] ✓ |
| 38 | **Kris Humphries** (+3.00) | George Hill (+2.51) [48] ✓ | Nicolas Batum (+2.54) [59] ✓ | Isaiah Thomas (+2.68) [19] ✓ |
| 39 | **Klay Thompson** (+2.90) | Wesley Matthews (+2.50) [46] ✓ | Channing Frye (+2.53) [49] ✓ | Danny Green (+2.63) [13] ✓ |
| 40 | **Robin Lopez** (+2.90) | Mario Chalmers (+2.45) [18] ✓ | Paul Pierce (+2.48) [59] ✓ | Wesley Matthews (+2.56) [46] ✓ |
| 41 | **Ty Lawson** (+2.90) | Marcin Gortat (+2.41) [57] ✓ | Trevor Ariza (+2.44) [66] ✓ | Corey Brewer (+2.50) [89] ✓ |
| 42 | **Vince Carter** (+2.90) | Eric Bledsoe (+2.41) [25] ✓ | Marcin Gortat (+2.43) [57] ✓ | Nicolas Batum (+2.49) [59] ✓ |
| 43 | **Jae Crowder** (+2.90) | Danny Green (+2.34) [13] ✓ | Patrick Beverley (+2.37) [16] ✓ | DeMarre Carroll (+2.43) [31] ✓ |
| 44 | **Darren Collison** (+2.70) | David West (+2.30) [51] ✓ | David Lee (+2.37) [70] ✓ | Chandler Parsons (+2.38) [62] ✓ |
| 45 | **Shane Battier** (+2.70) | Patrick Beverley (+2.30) [16] ✓ | Greg Monroe (+2.34) [125] ✗ | Klay Thompson (+2.36) [39] ✓ |
| 46 | **Wesley Matthews** (+2.60) | DeMarre Carroll (+2.24) [31] ✓ | Dwight Howard (+2.31) [57] ✓ | Dwight Howard (+2.35) [57] ✓ |
| 47 | **Tony Allen** (+2.60) | Channing Frye (+2.24) [49] ✓ | Anderson Varejao (+2.31) [16] ✓ | Ty Lawson (+2.35) [39] ✓ |
| 48 | **George Hill** (+2.50) | David Lee (+2.23) [70] ✓ | Wesley Matthews (+2.31) [46] ✓ | DeMar DeRozan (+2.26) [107] ✗ |
| 49 | **Channing Frye** (+2.40) | Robin Lopez (+2.22) [39] ✓ | CJ Watson (+2.26) [53] ✓ | Tim Duncan (+2.20) [66] ✓ |
| 50 | **Derek Fisher** (+2.30) | Al Jefferson (+2.21) [78] ✓ | Al Jefferson (+2.24) [78] ✓ | Eric Bledsoe (+2.17) [25] ✓ |
| 51 | **David West** (+2.20) | John Wall (+2.18) [66] ✓ | Nick Collison (+2.11) [83] ✓ | Damian Lillard (+2.15) [53] ✓ |
| 52 | **Jrue Holiday** (+2.20) | Kirk Hinrich (+2.14) [107] ✗ | Tiago Splitter (+2.06) [31] ✓ | David Lee (+2.09) [70] ✓ |
| 53 | **Damian Lillard** (+2.10) | Nick Collison (+2.03) [83] ✓ | Tony Allen (+2.04) [46] ✓ | Nikola Pekovic (+2.08) [31] ✓ |
| 54 | **Michael KiddGilchrist** (+2.10) | Nikola Pekovic (+1.97) [31] ✓ | Robin Lopez (+2.02) [39] ✓ | Robin Lopez (+2.04) [39] ✓ |
| 55 | **Chris Andersen** (+2.10) | Amir Johnson (+1.94) [89] ✓ | Jae Crowder (+1.99) [39] ✓ | Serge Ibaka (+1.97) [59] ✓ |
| 56 | **CJ Watson** (+2.10) | Shaun Livingston (+1.93) [87] ✓ | DeMarre Carroll (+1.94) [31] ✓ | George Hill (+1.95) [48] ✓ |
| 57 | **Marcin Gortat** (+2.00) | Dwight Howard (+1.93) [57] ✓ | John Wall (+1.88) [66] ✓ | Kyle Korver (+1.94) [78] ✓ |
| 58 | **Dwight Howard** (+2.00) | Kyle Korver (+1.89) [78] ✓ | DeAndre Jordan (+1.86) [62] ✓ | Terrence Jones (+1.72) [152] ✗ |
| 59 | **Nicolas Batum** (+1.90) | DeAndre Jordan (+1.77) [62] ✓ | Kirk Hinrich (+1.85) [107] ✗ | Jae Crowder (+1.68) [39] ✓ |
| 60 | **Serge Ibaka** (+1.90) | Tony Allen (+1.72) [46] ✓ | Kyle Korver (+1.77) [78] ✓ | Lance Stephenson (+1.66) [135] ✗ |
| 61 | **Paul Pierce** (+1.90) | Jae Crowder (+1.71) [39] ✓ | Amir Johnson (+1.77) [89] ✓ | Marc Gasol (+1.66) [62] ✓ |
| 62 | **DeAndre Jordan** (+1.80) | Iman Shumpert (+1.70) [97] ✓ | Eric Bledsoe (+1.77) [25] ✓ | Draymond Green (+1.65) [29] ✓ |
| 63 | **Chandler Parsons** (+1.80) | Tiago Splitter (+1.67) [31] ✓ | Dwyane Wade (+1.74) [83] ✓ | Marcin Gortat (+1.63) [57] ✓ |
| 64 | **Roy Hibbert** (+1.80) | Andray Blatche (+1.65) [118] ✗ | Rudy Gay (+1.72) [100] ✗ | Paul Pierce (+1.60) [59] ✓ |
| 65 | **Marc Gasol** (+1.80) | Dwyane Wade (+1.65) [83] ✓ | Chris Andersen (+1.64) [53] ✓ | Kemba Walker (+1.58) [21] ✓ |
| 66 | **John Wall** (+1.70) | CJ Watson (+1.64) [53] ✓ | Kyrie Irving (+1.59) [83] ✓ | Marco Belinelli (+1.57) [74] ✓ |
| 67 | **Trevor Ariza** (+1.70) | Greg Monroe (+1.63) [125] ✗ | Jeremy Lamb (+1.56) [102] ✗ | CJ Watson (+1.54) [53] ✓ |
| 68 | **PJ Tucker** (+1.70) | Shane Battier (+1.57) [44] ✓ | Nikola Pekovic (+1.55) [31] ✓ | Chris Andersen (+1.50) [53] ✓ |
| 69 | **Tim Duncan** (+1.70) | Chris Andersen (+1.45) [53] ✓ | Vince Carter (+1.52) [39] ✓ | Patrick Beverley (+1.46) [16] ✓ |
| 70 | **David Lee** (+1.60) | Nene (+1.41) [70] ✓ | Reggie Jackson (+1.51) [78] ✓ | Josh McRoberts (+1.46) [89] ✓ |
| 71 | **Courtney Lee** (+1.60) | Terrence Jones (+1.35) [152] ✗ | Brandan Wright (+1.46) [115] ✗ | Kyrie Irving (+1.43) [83] ✓ |
| 72 | **Nene** (+1.60) | Vince Carter (+1.32) [39] ✓ | Corey Brewer (+1.45) [89] ✓ | Tiago Splitter (+1.43) [31] ✓ |
| 73 | **Pablo Prigioni** (+1.60) | Jeremy Lamb (+1.25) [102] ✗ | DJ Augustin (+1.45) [131] ✗ | Pablo Prigioni (+1.43) [70] ✓ |
| 74 | **Andre Drummond** (+1.50) | Josh McRoberts (+1.23) [89] ✓ | Jrue Holiday (+1.44) [51] ✓ | Derek Fisher (+1.36) [50] ✓ |
| 75 | **Jared Sullinger** (+1.50) | Kenneth Faried (+1.19) [170] ✗ | Shaun Livingston (+1.44) [87] ✓ | Patrick Patterson (+1.33) [107] ✗ |
| 76 | **Marco Belinelli** (+1.50) | Corey Brewer (+1.16) [89] ✓ | Andray Blatche (+1.41) [118] ✗ | Mario Chalmers (+1.32) [18] ✓ |
| 77 | **Matthew Dellavedova** (+1.50) | Jrue Holiday (+1.11) [51] ✓ | Tim Duncan (+1.41) [66] ✓ | Jrue Holiday (+1.24) [51] ✓ |
| 78 | **Al Jefferson** (+1.30) | Jeff Teague (+1.10) [122] ✗ | Klay Thompson (+1.39) [39] ✓ | Mason Plumlee (+1.23) [198] ✗ |
| 79 | **Kyle Korver** (+1.30) | Jared Sullinger (+1.10) [74] ✓ | Nene (+1.39) [70] ✓ | Courtney Lee (+1.23) [70] ✓ |
| 80 | **Reggie Jackson** (+1.30) | Brandan Wright (+1.10) [115] ✗ | Andre Drummond (+1.39) [74] ✓ | PJ Tucker (+1.08) [66] ✓ |
| 81 | **Jeremy Lin** (+1.30) | Tim Duncan (+1.09) [66] ✓ | Anthony Tolliver (+1.24) [118] ✗ | Jeremy Lamb (+1.03) [102] ✗ |
| 82 | **Jeremy Evans** (+1.30) | Anthony Tolliver (+1.06) [118] ✗ | PJ Tucker (+1.23) [66] ✓ | Amir Johnson (+1.02) [89] ✓ |
| 83 | **Kyrie Irving** (+1.20) | Andre Drummond (+1.00) [74] ✓ | Patrick Patterson (+1.17) [107] ✗ | Jamal Crawford (+0.95) [115] ✗ |
| 84 | **Dwyane Wade** (+1.20) | Mike Dunleavy (+0.99) [107] ✗ | Iman Shumpert (+1.15) [97] ✓ | Kevin Martin (+0.94) [181] ✗ |
| 85 | **Nick Collison** (+1.20) | Patrick Patterson (+0.99) [107] ✗ | Terrence Jones (+1.12) [152] ✗ | Tony Parker (+0.92) [131] ✗ |
| 86 | **Nate Wolters** (+1.20) | Klay Thompson (+0.97) [39] ✓ | Josh McRoberts (+1.12) [89] ✓ | Rudy Gay (+0.90) [100] ✗ |
| 87 | **Shaun Livingston** (+1.10) | DJ Augustin (+0.94) [131] ✗ | Shane Battier (+1.10) [44] ✓ | Mike Dunleavy (+0.88) [107] ✗ |
| 88 | **Nick Calathes** (+1.10) | Rudy Gay (+0.92) [100] ✗ | Jeff Teague (+1.06) [122] ✗ | Nene (+0.87) [70] ✓ |
| 89 | **Corey Brewer** (+1.00) | Jonas Valanciunas (+0.90) [140] ✗ | Kenneth Faried (+1.06) [170] ✗ | Kenneth Faried (+0.85) [170] ✗ |
| 90 | **Josh McRoberts** (+1.00) | Taj Gibson (+0.81) [149] ✗ | Mike Dunleavy (+0.97) [107] ✗ | Andray Blatche (+0.82) [118] ✗ |
| 91 | **Amir Johnson** (+1.00) | Reggie Jackson (+0.77) [78] ✓ | Jared Sullinger (+0.97) [74] ✓ | Darren Collison (+0.79) [44] ✓ |
| 92 | **Boris Diaw** (+1.00) | Giannis Antetokounmpo (+0.66) [161] ✗ | Jonas Valanciunas (+0.93) [140] ✗ | DJ Augustin (+0.78) [131] ✗ |
| 93 | **Chris Bosh** (+0.90) | Markieff Morris (+0.63) [152] ✗ | Taj Gibson (+0.92) [149] ✗ | Monta Ellis (+0.73) [118] ✗ |
| 94 | **Luol Deng** (+0.90) | PJ Tucker (+0.63) [66] ✓ | Joe Johnson (+0.92) [125] ✗ | Markieff Morris (+0.73) [152] ✗ |
| 95 | **Nick Young** (+0.90) | Zach Randolph (+0.62) [125] ✗ | Victor Oladipo (+0.88) [152] ✗ | Nikola Vucevic (+0.73) [122] ✗ |
| 96 | **Omri Casspi** (+0.80) | Boris Diaw (+0.58) [89] ✓ | Giannis Antetokounmpo (+0.87) [161] ✗ | Luol Deng (+0.70) [93] ✓ |
| 97 | **Bradley Beal** (+0.70) | Tyson Chandler (+0.55) [140] ✗ | Zach Randolph (+0.83) [125] ✗ | Thabo Sefolosha (+0.69) [102] ✗ |
| 98 | **Randy Foye** (+0.70) | Omri Casspi (+0.55) [96] ✓ | Jeremy Evans (+0.74) [78] ✓ | Tyson Chandler (+0.66) [140] ✗ |
| 99 | **Iman Shumpert** (+0.70) | Chandler Parsons (+0.54) [62] ✓ | Boris Diaw (+0.74) [89] ✓ | Boris Diaw (+0.65) [89] ✓ |
| 100 | **Gordon Hayward** (+0.60) | Monta Ellis (+0.54) [118] ✗ | Marc Gasol (+0.73) [62] ✓ | Nick Calathes (+0.65) [87] ✓ |

### 2014-15 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | Chris Paul (+7.06) [1] ✓ | Chris Paul (+7.88) [1] ✓ | Chris Paul (+8.27) [1] ✓ |
| 2 | **Kawhi Leonard** (+8.90) | Draymond Green (+6.27) [5] ✓ | LeBron James (+6.54) [11] ✓ | Anthony Davis (+7.97) [3] ✓ |
| 3 | **Anthony Davis** (+8.80) | Kawhi Leonard (+6.26) [2] ✓ | Draymond Green (+5.93) [5] ✓ | Kawhi Leonard (+7.91) [2] ✓ |
| 4 | **James Harden** (+7.70) | LeBron James (+6.23) [11] ✓ | Kawhi Leonard (+5.92) [2] ✓ | LeBron James (+6.66) [11] ✓ |
| 5 | **Draymond Green** (+6.50) | Anthony Davis (+6.11) [3] ✓ | James Harden (+5.30) [4] ✓ | James Harden (+6.55) [4] ✓ |
| 6 | **Danny Green** (+6.10) | James Harden (+5.79) [4] ✓ | Anthony Davis (+5.05) [3] ✓ | Russell Westbrook (+5.71) [7] ✓ |
| 7 | **Russell Westbrook** (+5.60) | Jimmy Butler (+5.40) [34] ✓ | Kyrie Irving (+4.78) [13] ✓ | Jimmy Butler (+5.58) [34] ✓ |
| 8 | **George Hill** (+5.60) | Russell Westbrook (+4.71) [7] ✓ | Jimmy Butler (+4.67) [34] ✓ | George Hill (+5.08) [7] ✓ |
| 9 | **DeMarcus Cousins** (+5.40) | Damian Lillard (+4.37) [42] ✓ | Russell Westbrook (+4.64) [7] ✓ | Klay Thompson (+4.74) [10] ✓ |
| 10 | **Klay Thompson** (+5.30) | Lou Williams (+4.15) [34] ✓ | Klay Thompson (+4.56) [10] ✓ | Tony Allen (+4.57) [16] ✓ |
| 11 | **LeBron James** (+5.10) | DeMarcus Cousins (+4.14) [9] ✓ | Rudy Gobert (+4.49) [19] ✓ | Draymond Green (+4.55) [5] ✓ |
| 12 | **Khris Middleton** (+4.80) | Kyrie Irving (+4.10) [13] ✓ | George Hill (+4.41) [7] ✓ | DeAndre Jordan (+4.31) [13] ✓ |
| 13 | **Kyrie Irving** (+4.60) | Klay Thompson (+4.07) [10] ✓ | Damian Lillard (+4.25) [42] ✓ | Danny Green (+4.23) [6] ✓ |
| 14 | **DeAndre Jordan** (+4.60) | Rudy Gobert (+3.89) [19] ✓ | Andrew Bogut (+4.15) [23] ✓ | Blake Griffin (+4.02) [60] ✓ |
| 15 | **Kyle Korver** (+4.60) | George Hill (+3.75) [7] ✓ | Greg Monroe (+4.06) [124] ✗ | Paul Millsap (+4.01) [27] ✓ |
| 16 | **LaMarcus Aldridge** (+4.30) | Andrew Bogut (+3.70) [23] ✓ | Khris Middleton (+3.98) [12] ✓ | Kyrie Irving (+4.00) [13] ✓ |
| 17 | **Tony Allen** (+4.30) | Gordon Hayward (+3.66) [26] ✓ | Lou Williams (+3.88) [34] ✓ | Khris Middleton (+3.91) [12] ✓ |
| 18 | **Nikola Mirotic** (+4.20) | Khris Middleton (+3.55) [12] ✓ | Tony Allen (+3.76) [16] ✓ | John Wall (+3.82) [60] ✓ |
| 19 | **Rudy Gobert** (+4.10) | Tony Allen (+3.49) [16] ✓ | Danny Green (+3.46) [6] ✓ | Jeff Teague (+3.76) [42] ✓ |
| 20 | **Marc Gasol** (+4.00) | Wesley Matthews (+3.41) [24] ✓ | Kyle Lowry (+3.45) [22] ✓ | Tim Duncan (+3.72) [27] ✓ |
| 21 | **Darren Collison** (+4.00) | Jeff Teague (+3.38) [42] ✓ | DeMarcus Cousins (+3.37) [9] ✓ | Wesley Matthews (+3.64) [24] ✓ |
| 22 | **Kyle Lowry** (+3.90) | Danny Green (+3.29) [6] ✓ | LaMarcus Aldridge (+3.31) [16] ✓ | Lou Williams (+3.53) [34] ✓ |
| 23 | **Andrew Bogut** (+3.70) | LaMarcus Aldridge (+3.28) [16] ✓ | Nikola Mirotic (+3.26) [18] ✓ | Gordon Hayward (+3.29) [26] ✓ |
| 24 | **Wesley Matthews** (+3.60) | Kyle Lowry (+3.25) [22] ✓ | Wesley Matthews (+3.25) [24] ✓ | Al Horford (+3.23) [60] ✓ |
| 25 | **Jonas Jerebko** (+3.60) | Nikola Mirotic (+3.00) [18] ✓ | Isaiah Thomas (+3.21) [71] ✓ | DeMarcus Cousins (+3.15) [9] ✓ |
| 26 | **Gordon Hayward** (+3.40) | Paul Millsap (+3.00) [27] ✓ | Blake Griffin (+3.20) [60] ✓ | Rudy Gobert (+3.08) [19] ✓ |
| 27 | **Paul Millsap** (+3.30) | Greg Monroe (+2.95) [124] ✗ | Pau Gasol (+3.13) [100] ✓ | Damian Lillard (+3.06) [42] ✓ |
| 28 | **Tim Duncan** (+3.30) | Isaiah Thomas (+2.82) [71] ✓ | Manu Ginobili (+3.09) [29] ✓ | LaMarcus Aldridge (+2.97) [16] ✓ |
| 29 | **Kevin Love** (+3.20) | Mike Conley (+2.79) [37] ✓ | Mike Conley (+3.07) [37] ✓ | Brandan Wright (+2.85) [69] ✓ |
| 30 | **Marcin Gortat** (+3.20) | Kevin Love (+2.73) [29] ✓ | Gordon Hayward (+3.07) [26] ✓ | Kyle Korver (+2.83) [13] ✓ |
| 31 | **JJ Redick** (+3.20) | Blake Griffin (+2.71) [60] ✓ | Jeff Teague (+2.94) [42] ✓ | Marc Gasol (+2.80) [20] ✓ |
| 32 | **Manu Ginobili** (+3.20) | Jrue Holiday (+2.65) [42] ✓ | Zach Randolph (+2.85) [37] ✓ | Jrue Holiday (+2.73) [42] ✓ |
| 33 | **Brandon Jennings** (+3.20) | Danilo Gallinari (+2.62) [34] ✓ | John Wall (+2.84) [60] ✓ | Darren Collison (+2.52) [20] ✓ |
| 34 | **Jimmy Butler** (+3.00) | Zach Randolph (+2.49) [37] ✓ | Paul Millsap (+2.80) [27] ✓ | DeMarre Carroll (+2.51) [37] ✓ |
| 35 | **Lou Williams** (+3.00) | Tyson Chandler (+2.38) [47] ✓ | Patrick Patterson (+2.78) [71] ✓ | Derrick Favors (+2.50) [53] ✓ |
| 36 | **Danilo Gallinari** (+3.00) | Patrick Patterson (+2.31) [71] ✓ | Nene (+2.67) [109] ✗ | James Johnson (+2.50) [60] ✓ |
| 37 | **Eric Bledsoe** (+2.90) | Kyle Korver (+2.31) [13] ✓ | Jonas Jerebko (+2.56) [24] ✓ | Kyle Lowry (+2.47) [22] ✓ |
| 38 | **Zach Randolph** (+2.90) | Manu Ginobili (+2.28) [29] ✓ | Kyle Korver (+2.56) [13] ✓ | Marcin Gortat (+2.44) [29] ✓ |
| 39 | **Mike Conley** (+2.90) | Jae Crowder (+2.22) [56] ✓ | Tyson Chandler (+2.47) [47] ✓ | Anthony Morrow (+2.43) [42] ✓ |
| 40 | **DeMarre Carroll** (+2.90) | Jared Dudley (+2.21) [66] ✓ | Jrue Holiday (+2.44) [42] ✓ | Ty Lawson (+2.40) [56] ✓ |
| 41 | **Kelly Olynyk** (+2.80) | Pau Gasol (+2.14) [100] ✓ | Jared Dudley (+2.41) [66] ✓ | Kevin Love (+2.34) [29] ✓ |
| 42 | **Damian Lillard** (+2.70) | James Johnson (+2.10) [60] ✓ | Andre Iguodala (+2.23) [81] ✓ | Tyson Chandler (+2.28) [47] ✓ |
| 43 | **Jeff Teague** (+2.70) | DeAndre Jordan (+2.04) [13] ✓ | Kelly Olynyk (+2.23) [41] ✓ | Mike Conley (+2.25) [37] ✓ |
| 44 | **Anthony Morrow** (+2.70) | Marcus Smart (+2.03) [56] ✓ | Derrick Favors (+2.20) [53] ✓ | Pau Gasol (+2.23) [100] ✓ |
| 45 | **Zaza Pachulia** (+2.70) | Eric Bledsoe (+1.97) [37] ✓ | Ersan Ilyasova (+2.18) [49] ✓ | Manu Ginobili (+2.08) [29] ✓ |
| 46 | **Jrue Holiday** (+2.70) | John Wall (+1.97) [60] ✓ | Danilo Gallinari (+2.15) [34] ✓ | Carmelo Anthony (+2.06) [81] ✓ |
| 47 | **Tyson Chandler** (+2.60) | Nene (+1.94) [109] ✗ | Monta Ellis (+2.11) [78] ✓ | Trevor Ariza (+2.00) [116] ✗ |
| 48 | **Serge Ibaka** (+2.60) | Tyreke Evans (+1.94) [87] ✓ | Zaza Pachulia (+2.06) [42] ✓ | JJ Redick (+1.99) [29] ✓ |
| 49 | **Devin Harris** (+2.50) | Ersan Ilyasova (+1.92) [49] ✓ | Marc Gasol (+2.00) [20] ✓ | Zach Randolph (+1.99) [37] ✓ |
| 50 | **Ersan Ilyasova** (+2.50) | Iman Shumpert (+1.86) [66] ✓ | Jae Crowder (+2.00) [56] ✓ | Nikola Mirotic (+1.98) [18] ✓ |
| 51 | **Rudy Gay** (+2.40) | Andre Iguodala (+1.86) [81] ✓ | Harrison Barnes (+1.94) [142] ✗ | Nicolas Batum (+1.97) [116] ✗ |
| 52 | **Kemba Walker** (+2.40) | Aaron Brooks (+1.86) [162] ✗ | Marcin Gortat (+1.92) [29] ✓ | Andrew Bogut (+1.96) [23] ✓ |
| 53 | **Derrick Favors** (+2.30) | Devin Harris (+1.83) [49] ✓ | DeAndre Jordan (+1.89) [13] ✓ | Chandler Parsons (+1.93) [68] ✓ |
| 54 | **Joakim Noah** (+2.20) | Jonas Jerebko (+1.82) [24] ✓ | Tim Duncan (+1.87) [27] ✓ | Rudy Gay (+1.90) [51] ✓ |
| 55 | **Andre Roberson** (+2.20) | Marc Gasol (+1.82) [20] ✓ | Reggie Jackson (+1.83) [81] ✓ | Greg Monroe (+1.83) [124] ✗ |
| 56 | **Ty Lawson** (+2.10) | Marcin Gortat (+1.81) [29] ✓ | Andre Drummond (+1.77) [139] ✗ | Danilo Gallinari (+1.82) [34] ✓ |
| 57 | **Marcus Smart** (+2.10) | Monta Ellis (+1.80) [78] ✓ | Devin Harris (+1.75) [49] ✓ | Monta Ellis (+1.82) [78] ✓ |
| 58 | **Jae Crowder** (+2.10) | AlFarouq Aminu (+1.79) [56] ✓ | James Johnson (+1.74) [60] ✓ | Brandon Jennings (+1.81) [29] ✓ |
| 59 | **AlFarouq Aminu** (+2.10) | Tim Duncan (+1.79) [27] ✓ | Rudy Gay (+1.73) [51] ✓ | CJ Miles (+1.79) [71] ✓ |
| 60 | **John Wall** (+2.00) | Kelly Olynyk (+1.74) [41] ✓ | Timofey Mozgov (+1.72) [78] ✓ | Andre Iguodala (+1.78) [81] ✓ |
| 61 | **Blake Griffin** (+2.00) | Reggie Jackson (+1.69) [81] ✓ | Kevin Love (+1.72) [29] ✓ | Isaiah Thomas (+1.77) [71] ✓ |
| 62 | **Al Horford** (+2.00) | Amir Johnson (+1.66) [100] ✗ | Anthony Morrow (+1.71) [42] ✓ | Patrick Patterson (+1.75) [71] ✓ |
| 63 | **Michael KiddGilchrist** (+2.00) | Harrison Barnes (+1.57) [142] ✗ | CJ Watson (+1.67) [81] ✓ | Kelly Olynyk (+1.73) [41] ✓ |
| 64 | **Cody Zeller** (+2.00) | Darren Collison (+1.57) [20] ✓ | Cory Joseph (+1.66) [87] ✓ | AlFarouq Aminu (+1.73) [56] ✓ |
| 65 | **James Johnson** (+2.00) | Zaza Pachulia (+1.55) [42] ✓ | Tyreke Evans (+1.62) [87] ✓ | Jonas Jerebko (+1.63) [24] ✓ |
| 66 | **Jared Dudley** (+1.90) | JJ Redick (+1.54) [29] ✓ | Iman Shumpert (+1.61) [66] ✓ | Jae Crowder (+1.62) [56] ✓ |
| 67 | **Iman Shumpert** (+1.90) | Al Horford (+1.46) [60] ✓ | Marcus Smart (+1.61) [56] ✓ | Tyreke Evans (+1.54) [87] ✓ |
| 68 | **Chandler Parsons** (+1.80) | Nicolas Batum (+1.42) [116] ✗ | Bradley Beal (+1.59) [81] ✓ | Bradley Beal (+1.44) [81] ✓ |
| 69 | **Luol Deng** (+1.70) | Derrick Favors (+1.42) [53] ✓ | Aaron Brooks (+1.59) [162] ✗ | Eric Bledsoe (+1.42) [37] ✓ |
| 70 | **Brandan Wright** (+1.70) | Andre Roberson (+1.36) [54] ✓ | Dwight Howard (+1.59) [96] ✓ | Paul Pierce (+1.41) [87] ✓ |
| 71 | **Matt Barnes** (+1.60) | Matt Barnes (+1.31) [71] ✓ | Matt Barnes (+1.56) [71] ✓ | Zaza Pachulia (+1.39) [42] ✓ |
| 72 | **Patrick Patterson** (+1.60) | Trevor Ariza (+1.24) [116] ✗ | JJ Redick (+1.49) [29] ✓ | Michael KiddGilchrist (+1.34) [60] ✓ |
| 73 | **CJ Miles** (+1.60) | Bradley Beal (+1.24) [81] ✓ | Amir Johnson (+1.45) [100] ✗ | Jared Dudley (+1.30) [66] ✓ |
| 74 | **Ed Davis** (+1.60) | Andre Drummond (+1.23) [139] ✗ | Nicolas Batum (+1.38) [116] ✗ | Brook Lopez (+1.29) [96] ✓ |
| 75 | **Isaiah Thomas** (+1.60) | Donatas Motiejunas (+1.21) [124] ✗ | Victor Oladipo (+1.38) [147] ✗ | Goran Dragic (+1.25) [116] ✗ |
| 76 | **JJ Barea** (+1.60) | DeMarre Carroll (+1.20) [37] ✓ | Andre Roberson (+1.35) [54] ✓ | Dirk Nowitzki (+1.24) [96] ✓ |
| 77 | **Pablo Prigioni** (+1.60) | Victor Oladipo (+1.14) [147] ✗ | Ed Davis (+1.34) [71] ✓ | CJ Watson (+1.24) [81] ✓ |
| 78 | **Monta Ellis** (+1.40) | Mike Dunleavy (+1.14) [116] ✗ | Michael KiddGilchrist (+1.31) [60] ✓ | Robert Covington (+1.24) [94] ✓ |
| 79 | **Timofey Mozgov** (+1.40) | Trevor Booker (+1.12) [105] ✗ | AlFarouq Aminu (+1.30) [56] ✓ | Andre Drummond (+1.22) [139] ✗ |
| 80 | **Jeremy Lin** (+1.40) | Ed Davis (+1.09) [71] ✓ | Donatas Motiejunas (+1.27) [124] ✗ | Serge Ibaka (+1.21) [47] ✓ |
| 81 | **Reggie Jackson** (+1.30) | Cory Joseph (+1.04) [87] ✓ | Eric Bledsoe (+1.26) [37] ✓ | Ed Davis (+1.18) [71] ✓ |
| 82 | **Bradley Beal** (+1.30) | Anthony Morrow (+1.04) [42] ✓ | Jared Sullinger (+1.23) [92] ✓ | Dwight Howard (+1.17) [96] ✓ |
| 83 | **Andre Iguodala** (+1.30) | Joakim Noah (+1.04) [54] ✓ | Josh Smith (+1.23) [123] ✗ | Al Jefferson (+1.14) [189] ✗ |
| 84 | **Carmelo Anthony** (+1.30) | PJ Tucker (+1.03) [100] ✗ | Darren Collison (+1.22) [20] ✓ | Harrison Barnes (+1.13) [142] ✗ |
| 85 | **CJ Watson** (+1.30) | Derrick Rose (+1.03) [94] ✓ | Brandan Wright (+1.22) [69] ✓ | Kemba Walker (+1.10) [51] ✓ |
| 86 | **Amare Stoudemire** (+1.30) | Rudy Gay (+1.00) [51] ✓ | Dirk Nowitzki (+1.18) [96] ✓ | Matt Barnes (+1.09) [71] ✓ |
| 87 | **Tyreke Evans** (+1.20) | Jared Sullinger (+0.95) [92] ✓ | Luis Scola (+1.18) [105] ✗ | Thaddeus Young (+0.99) [96] ✓ |
| 88 | **Deron Williams** (+1.20) | Brandan Wright (+0.92) [69] ✓ | Pablo Prigioni (+1.17) [71] ✓ | Mike Dunleavy (+0.95) [116] ✗ |
| 89 | **Paul Pierce** (+1.20) | Giannis Antetokounmpo (+0.91) [159] ✗ | Al Horford (+1.14) [60] ✓ | Dwyane Wade (+0.94) [144] ✗ |
| 90 | **Patrick Beverley** (+1.20) | Kenneth Faried (+0.91) [111] ✗ | Carmelo Anthony (+1.13) [81] ✓ | Luol Deng (+0.94) [69] ✓ |
| 91 | **Cory Joseph** (+1.20) | Dirk Nowitzki (+0.87) [96] ✓ | Thaddeus Young (+1.12) [96] ✓ | Pablo Prigioni (+0.87) [71] ✓ |
| 92 | **Jonas Valanciunas** (+1.10) | Timofey Mozgov (+0.87) [78] ✓ | Jeremy Lin (+1.09) [78] ✓ | Devin Harris (+0.82) [49] ✓ |
| 93 | **Jared Sullinger** (+1.10) | CJ Watson (+0.82) [81] ✓ | Brandon Jennings (+1.07) [29] ✓ | Kenneth Faried (+0.80) [111] ✗ |
| 94 | **Robert Covington** (+1.00) | JJ Barea (+0.78) [71] ✓ | Jonas Valanciunas (+1.05) [92] ✓ | Joakim Noah (+0.79) [54] ✓ |
| 95 | **Derrick Rose** (+1.00) | Goran Dragic (+0.77) [116] ✗ | PJ Tucker (+1.03) [100] ✗ | Markieff Morris (+0.78) [135] ✗ |
| 96 | **Thaddeus Young** (+0.90) | Luis Scola (+0.76) [105] ✗ | Nikola Vucevic (+1.03) [175] ✗ | David West (+0.77) [159] ✗ |
| 97 | **Dirk Nowitzki** (+0.90) | Chandler Parsons (+0.76) [68] ✓ | Anthony Tolliver (+1.00) [154] ✗ | DeMar DeRozan (+0.77) [167] ✗ |
| 98 | **Brook Lopez** (+0.90) | Brandon Jennings (+0.76) [29] ✓ | Trevor Booker (+0.95) [105] ✗ | PJ Tucker (+0.71) [100] ✗ |
| 99 | **Dwight Howard** (+0.90) | Paul Pierce (+0.73) [87] ✓ | Trevor Ariza (+0.94) [116] ✗ | Iman Shumpert (+0.69) [66] ✓ |
| 100 | **Pau Gasol** (+0.80) | Josh Smith (+0.72) [123] ✗ | Giannis Antetokounmpo (+0.94) [159] ✗ | Luis Scola (+0.68) [105] ✗ |

### 2013-14 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Kevin Durant** (+7.60) | Chris Paul (+6.65) [2] ✓ | Kevin Durant (+7.41) [1] ✓ |
| 2 | **Chris Paul** (+7.10) | Kevin Durant (+6.48) [1] ✓ | LeBron James (+6.80) [4] ✓ |
| 3 | **James Harden** (+6.30) | LeBron James (+5.67) [4] ✓ | Chris Paul (+6.79) [2] ✓ |
| 4 | **LeBron James** (+5.80) | James Harden (+5.17) [3] ✓ | James Harden (+5.29) [3] ✓ |
| 5 | **Kevin Love** (+5.70) | Damian Lillard (+4.54) [12] ✓ | Goran Dragic (+4.87) [6] ✓ |
| 6 | **Goran Dragic** (+4.80) | Goran Dragic (+4.00) [6] ✓ | Kevin Love (+4.76) [5] ✓ |
| 7 | **Kyle Lowry** (+4.40) | Kevin Love (+3.99) [5] ✓ | Dirk Nowitzki (+4.33) [7] ✓ |
| 8 | **Dirk Nowitzki** (+4.40) | Carmelo Anthony (+3.49) [9] ✓ | Brandan Wright (+4.19) [40] ✓ |
| 9 | **Carmelo Anthony** (+4.20) | Manu Ginobili (+3.43) [10] ✓ | Carmelo Anthony (+3.80) [9] ✓ |
| 10 | **Manu Ginobili** (+4.00) | Russell Westbrook (+3.32) [15] ✓ | Kyle Lowry (+3.71) [7] ✓ |
| 11 | **Patty Mills** (+3.90) | Kyle Lowry (+3.09) [7] ✓ | Blake Griffin (+3.57) [17] ✓ |
| 12 | **Damian Lillard** (+3.60) | Dirk Nowitzki (+2.95) [7] ✓ | Isaiah Thomas (+3.48) [13] ✓ |
| 13 | **Isaiah Thomas** (+3.50) | Mike Conley (+2.88) [13] ✓ | Manu Ginobili (+3.25) [10] ✓ |
| 14 | **Mike Conley** (+3.50) | Isaiah Thomas (+2.82) [13] ✓ | Russell Westbrook (+3.24) [15] ✓ |
| 15 | **Russell Westbrook** (+3.30) | Joe Johnson (+2.65) [21] ✓ | Nikola Pekovic (+3.10) [64] ✓ |
| 16 | **Ty Lawson** (+3.20) | Kyrie Irving (+2.64) [27] ✓ | Damian Lillard (+3.03) [12] ✓ |
| 17 | **Blake Griffin** (+2.90) | Blake Griffin (+2.50) [17] ✓ | Dwyane Wade (+2.82) [47] ✓ |
| 18 | **Wesley Matthews** (+2.80) | Patty Mills (+2.37) [11] ✓ | Ty Lawson (+2.80) [16] ✓ |
| 19 | **Marco Belinelli** (+2.80) | Paul George (+2.34) [22] ✓ | Mike Conley (+2.80) [13] ✓ |
| 20 | **Jamal Crawford** (+2.80) | John Wall (+2.33) [36] ✓ | Wesley Matthews (+2.68) [18] ✓ |
| 21 | **Joe Johnson** (+2.70) | DJ Augustin (+2.28) [31] ✓ | Patty Mills (+2.59) [11] ✓ |
| 22 | **Paul George** (+2.60) | Ty Lawson (+2.06) [16] ✓ | Anthony Davis (+2.52) [57] ✓ |
| 23 | **Chandler Parsons** (+2.60) | Jamal Crawford (+2.05) [18] ✓ | Kawhi Leonard (+2.51) [40] ✓ |
| 24 | **Deron Williams** (+2.60) | Deron Williams (+1.82) [22] ✓ | Chandler Parsons (+2.47) [22] ✓ |
| 25 | **Vince Carter** (+2.40) | Pablo Prigioni (+1.53) [40] ✓ | Paul George (+2.46) [22] ✓ |
| 26 | **Nick Young** (+2.40) | Ricky Rubio (+1.50) [36] ✓ | Deron Williams (+2.34) [22] ✓ |
| 27 | **Kyrie Irving** (+2.30) | Jrue Holiday (+1.49) [28] ✓ | DeMar DeRozan (+2.30) [40] ✓ |
| 28 | **Patrick Beverley** (+2.20) | Wesley Matthews (+1.49) [18] ✓ | Jamal Crawford (+2.28) [18] ✓ |
| 29 | **Jrue Holiday** (+2.20) | Kevin Martin (+1.37) [83] ✓ | Tony Parker (+2.17) [71] ✓ |
| 30 | **Brandon Jennings** (+2.20) | Kawhi Leonard (+1.36) [40] ✓ | DeMarcus Cousins (+2.15) [40] ✓ |
| 31 | **Klay Thompson** (+2.10) | Kemba Walker (+1.26) [51] ✓ | Chris Bosh (+1.99) [136] ✗ |
| 32 | **Randy Foye** (+2.10) | DeMar DeRozan (+1.25) [40] ✓ | Marco Belinelli (+1.94) [18] ✓ |
| 33 | **DJ Augustin** (+2.10) | Bradley Beal (+1.25) [112] ✗ | Kyrie Irving (+1.93) [27] ✓ |
| 34 | **Channing Frye** (+2.00) | George Hill (+1.24) [112] ✗ | Andre Drummond (+1.91) [53] ✓ |
| 35 | **Josh McRoberts** (+2.00) | Rudy Gay (+1.22) [64] ✓ | Robin Lopez (+1.86) [68] ✓ |
| 36 | **Ricky Rubio** (+1.90) | Andre Iguodala (+1.21) [57] ✓ | LaMarcus Aldridge (+1.86) [64] ✓ |
| 37 | **Nicolas Batum** (+1.90) | Jose Calderon (+1.16) [45] ✓ | Andre Iguodala (+1.84) [57] ✓ |
| 38 | **John Wall** (+1.90) | Klay Thompson (+1.15) [31] ✓ | Nicolas Batum (+1.84) [36] ✓ |
| 39 | **Kyle Korver** (+1.90) | Eric Bledsoe (+1.13) [47] ✓ | Eric Bledsoe (+1.67) [47] ✓ |
| 40 | **Kawhi Leonard** (+1.70) | Jimmy Butler (+1.12) [97] ✓ | Kevin Martin (+1.64) [83] ✓ |
| 41 | **DeMarcus Cousins** (+1.70) | Jeff Teague (+1.12) [90] ✓ | Joe Johnson (+1.61) [21] ✓ |
| 42 | **DeMar DeRozan** (+1.70) | LaMarcus Aldridge (+1.10) [64] ✓ | Trevor Ariza (+1.61) [68] ✓ |
| 43 | **Pablo Prigioni** (+1.70) | Reggie Jackson (+1.10) [71] ✓ | Jose Calderon (+1.61) [45] ✓ |
| 44 | **Brandan Wright** (+1.70) | Nikola Pekovic (+1.09) [64] ✓ | Al Jefferson (+1.59) [149] ✗ |
| 45 | **Jose Calderon** (+1.60) | Brandan Wright (+1.09) [40] ✓ | John Wall (+1.57) [36] ✓ |
| 46 | **Mirza Teletovic** (+1.60) | David Lee (+1.08) [112] ✗ | Nick Young (+1.55) [25] ✓ |
| 47 | **Joakim Noah** (+1.50) | Nick Young (+1.07) [25] ✓ | Klay Thompson (+1.45) [31] ✓ |
| 48 | **Mario Chalmers** (+1.50) | Nicolas Batum (+1.06) [36] ✓ | Tyreke Evans (+1.40) [57] ✓ |
| 49 | **Eric Bledsoe** (+1.50) | Dwyane Wade (+1.02) [47] ✓ | David Lee (+1.33) [112] ✗ |
| 50 | **Dwyane Wade** (+1.50) | Trevor Ariza (+1.02) [68] ✓ | DJ Augustin (+1.28) [31] ✓ |
| 51 | **Kemba Walker** (+1.40) | Channing Frye (+0.99) [34] ✓ | Arron Afflalo (+1.21) [57] ✓ |
| 52 | **Ray Allen** (+1.40) | Randy Foye (+0.99) [31] ✓ | Terrence Jones (+1.19) [71] ✓ |
| 53 | **Andre Drummond** (+1.30) | Vince Carter (+0.99) [25] ✓ | Joakim Noah (+1.17) [47] ✓ |
| 54 | **Zach Randolph** (+1.30) | Marco Belinelli (+0.97) [18] ✓ | Kyle Korver (+1.16) [36] ✓ |
| 55 | **Gerald Green** (+1.30) | Kenneth Faried (+0.94) [57] ✓ | Jodie Meeks (+1.14) [83] ✓ |
| 56 | **Anthony Morrow** (+1.30) | Mario Chalmers (+0.92) [47] ✓ | Gerald Green (+1.13) [53] ✓ |
| 57 | **Anthony Davis** (+1.20) | DeMarcus Cousins (+0.90) [40] ✓ | Ricky Rubio (+1.12) [36] ✓ |
| 58 | **Andre Iguodala** (+1.20) | Darren Collison (+0.87) [64] ✓ | Jrue Holiday (+1.12) [28] ✓ |
| 59 | **Tyreke Evans** (+1.20) | Zach Randolph (+0.85) [53] ✓ | Corey Brewer (+1.12) [112] ✗ |
| 60 | **Kenneth Faried** (+1.20) | Tony Parker (+0.85) [71] ✓ | Monta Ellis (+1.12) [68] ✓ |
| 61 | **Arron Afflalo** (+1.20) | Brandon Knight (+0.82) [71] ✓ | Paul Pierce (+1.10) [126] ✗ |
| 62 | **Jameer Nelson** (+1.20) | Brandon Jennings (+0.79) [28] ✓ | Jeff Teague (+1.08) [90] ✓ |
| 63 | **Lou Williams** (+1.20) | Patrick Beverley (+0.75) [28] ✓ | Darren Collison (+1.08) [64] ✓ |
| 64 | **LaMarcus Aldridge** (+1.10) | Greivis Vasquez (+0.72) [112] ✗ | DeMarre Carroll (+1.07) [83] ✓ |
| 65 | **Darren Collison** (+1.10) | Kyle Korver (+0.72) [36] ✓ | Pablo Prigioni (+1.05) [40] ✓ |
| 66 | **Nikola Pekovic** (+1.10) | Jameer Nelson (+0.71) [57] ✓ | Rudy Gay (+1.04) [64] ✓ |
| 67 | **Rudy Gay** (+1.10) | Chandler Parsons (+0.69) [22] ✓ | Vince Carter (+1.01) [25] ✓ |
| 68 | **Robin Lopez** (+1.00) | Anthony Morrow (+0.69) [53] ✓ | Dwight Howard (+1.00) [144] ✗ |
| 69 | **Trevor Ariza** (+1.00) | Gordon Hayward (+0.66) [83] ✓ | Mason Plumlee (+0.99) [176] ✗ |
| 70 | **Monta Ellis** (+1.00) | Joakim Noah (+0.65) [47] ✓ | Gordon Hayward (+0.93) [83] ✓ |
| 71 | **Reggie Jackson** (+0.90) | Gerald Green (+0.65) [53] ✓ | Markieff Morris (+0.89) [129] ✗ |
| 72 | **Alec Burks** (+0.90) | Monta Ellis (+0.64) [68] ✓ | Luol Deng (+0.88) [112] ✗ |
| 73 | **Matthew Dellavedova** (+0.90) | Lance Stephenson (+0.63) [90] ✓ | Anthony Morrow (+0.87) [53] ✓ |
| 74 | **Tony Parker** (+0.90) | Corey Brewer (+0.62) [112] ✗ | Amare Stoudemire (+0.86) [166] ✗ |
| 75 | **Shelvin Mack** (+0.90) | Ramon Sessions (+0.55) [90] ✓ | Kenneth Faried (+0.85) [57] ✓ |
| 76 | **Terrence Jones** (+0.90) | David West (+0.55) [90] ✓ | Brandon Jennings (+0.84) [28] ✓ |
| 77 | **Brandon Knight** (+0.90) | Mirza Teletovic (+0.50) [45] ✓ | Randy Foye (+0.78) [31] ✓ |
| 78 | **Martell Webster** (+0.90) | Trey Burke (+0.50) [109] ✗ | Anderson Varejao (+0.74) [97] ✓ |
| 79 | **PJ Tucker** (+0.80) | Lou Williams (+0.44) [57] ✓ | Eric Gordon (+0.72) [90] ✓ |
| 80 | **Boris Diaw** (+0.80) | Terrence Ross (+0.36) [97] ✓ | Paul Millsap (+0.71) [109] ✗ |
| 81 | **Matt Barnes** (+0.80) | Paul Pierce (+0.35) [126] ✗ | Alec Burks (+0.68) [71] ✓ |
| 82 | **Marvin Williams** (+0.80) | Eric Gordon (+0.33) [90] ✓ | Greivis Vasquez (+0.63) [112] ✗ |
| 83 | **DeMarre Carroll** (+0.70) | Boris Diaw (+0.32) [79] ✓ | Marc Gasol (+0.61) [176] ✗ |
| 84 | **DeAndre Jordan** (+0.70) | Greg Monroe (+0.32) [112] ✗ | Tiago Splitter (+0.58) [166] ✗ |
| 85 | **Danny Green** (+0.70) | Jeremy Lamb (+0.31) [97] ✗ | Courtney Lee (+0.54) [83] ✓ |
| 86 | **Gordon Hayward** (+0.70) | Terrence Jones (+0.26) [71] ✓ | Josh McRoberts (+0.51) [34] ✓ |
| 87 | **Courtney Lee** (+0.70) | Luol Deng (+0.25) [112] ✗ | David West (+0.50) [90] ✓ |
| 88 | **Jodie Meeks** (+0.70) | Mike Dunleavy (+0.23) [112] ✗ | Kemba Walker (+0.50) [51] ✓ |
| 89 | **Kevin Martin** (+0.70) | Andre Drummond (+0.22) [53] ✓ | Patrick Beverley (+0.46) [28] ✓ |
| 90 | **David West** (+0.60) | Robin Lopez (+0.21) [68] ✓ | PJ Tucker (+0.45) [79] ✓ |
| 91 | **Jeff Teague** (+0.60) | Danny Green (+0.16) [83] ✓ | Reggie Jackson (+0.37) [71] ✓ |
| 92 | **Lance Stephenson** (+0.60) | Josh McRoberts (+0.15) [34] ✓ | Greg Monroe (+0.36) [112] ✗ |
| 93 | **Marcus Thornton** (+0.60) | Chris Bosh (+0.15) [136] ✗ | Ramon Sessions (+0.35) [90] ✓ |
| 94 | **Ramon Sessions** (+0.60) | Paul Millsap (+0.15) [109] ✗ | Matthew Dellavedova (+0.33) [71] ✓ |
| 95 | **Mike Miller** (+0.60) | Dwight Howard (+0.15) [144] ✗ | Jeremy Lamb (+0.33) [97] ✗ |
| 96 | **Eric Gordon** (+0.60) | Ray Allen (+0.13) [51] ✓ | Tim Hardaway Jr. (+0.33) [129] ✗ |
| 97 | **Jimmy Butler** (+0.50) | Matt Barnes (+0.13) [79] ✓ | Chris Andersen (+0.31) [97] ✗ |
| 98 | **Anderson Varejao** (+0.50) | Mike Miller (+0.11) [90] ✓ | Brandon Knight (+0.30) [71] ✓ |
| 99 | **Jared Sullinger** (+0.50) | Khris Middleton (+0.10) [121] ✗ | Bradley Beal (+0.29) [112] ✗ |
| 100 | **Terrence Ross** (+0.50) | Derek Fisher (+0.08) [112] ✗ | Marcin Gortat (+0.28) [156] ✗ |

### 2014-15 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+8.50) | Chris Paul (+7.71) [1] ✓ | Chris Paul (+6.99) [1] ✓ |
| 2 | **James Harden** (+7.70) | James Harden (+5.85) [2] ✓ | James Harden (+5.71) [2] ✓ |
| 3 | **Russell Westbrook** (+6.10) | LeBron James (+5.54) [5] ✓ | LeBron James (+5.62) [5] ✓ |
| 4 | **Kyrie Irving** (+5.50) | Kyrie Irving (+5.15) [4] ✓ | Anthony Davis (+5.11) [9] ✓ |
| 5 | **LeBron James** (+5.30) | Damian Lillard (+4.73) [11] ✓ | Russell Westbrook (+5.02) [3] ✓ |
| 6 | **Lou Williams** (+5.20) | Russell Westbrook (+4.70) [3] ✓ | Jimmy Butler (+4.38) [20] ✓ |
| 7 | **Kyle Korver** (+4.60) | Lou Williams (+3.89) [6] ✓ | Blake Griffin (+4.19) [20] ✓ |
| 8 | **Isaiah Thomas** (+4.50) | Isaiah Thomas (+3.83) [8] ✓ | Kyrie Irving (+4.13) [4] ✓ |
| 9 | **Anthony Davis** (+4.30) | Klay Thompson (+3.33) [9] ✓ | Lou Williams (+4.08) [6] ✓ |
| 10 | **Klay Thompson** (+4.30) | Blake Griffin (+3.14) [20] ✓ | Klay Thompson (+4.00) [9] ✓ |
| 11 | **Damian Lillard** (+4.00) | George Hill (+2.76) [12] ✓ | George Hill (+3.81) [12] ✓ |
| 12 | **George Hill** (+3.90) | Gordon Hayward (+2.59) [20] ✓ | Kawhi Leonard (+3.44) [15] ✓ |
| 13 | **Ty Lawson** (+3.80) | Kyle Lowry (+2.56) [18] ✓ | JJ Redick (+3.36) [29] ✓ |
| 14 | **Carmelo Anthony** (+3.80) | Jimmy Butler (+2.53) [20] ✓ | Ty Lawson (+3.18) [13] ✓ |
| 15 | **Kawhi Leonard** (+3.70) | JJ Redick (+2.49) [29] ✓ | Gordon Hayward (+3.01) [20] ✓ |
| 16 | **Rudy Gay** (+3.50) | Mike Conley (+2.48) [30] ✓ | Isaiah Thomas (+2.98) [8] ✓ |
| 17 | **DeAndre Jordan** (+3.40) | Dwyane Wade (+2.41) [40] ✓ | Carmelo Anthony (+2.96) [13] ✓ |
| 18 | **Kyle Lowry** (+3.30) | Jrue Holiday (+2.39) [18] ✓ | Damian Lillard (+2.92) [11] ✓ |
| 19 | **Jrue Holiday** (+3.30) | Carmelo Anthony (+2.36) [13] ✓ | Wesley Matthews (+2.78) [33] ✓ |
| 20 | **Gordon Hayward** (+3.20) | Ty Lawson (+2.28) [13] ✓ | Brandon Jennings (+2.74) [23] ✓ |
| 21 | **Jimmy Butler** (+3.20) | John Wall (+2.24) [37] ✓ | Anthony Morrow (+2.73) [26] ✓ |
| 22 | **Blake Griffin** (+3.20) | Kawhi Leonard (+2.22) [15] ✓ | Jeff Teague (+2.72) [34] ✓ |
| 23 | **Danny Green** (+3.10) | Jeff Teague (+2.16) [34] ✓ | Rudy Gay (+2.65) [16] ✓ |
| 24 | **Brandon Jennings** (+3.10) | Joe Johnson (+2.08) [46] ✓ | Kyle Lowry (+2.48) [18] ✓ |
| 25 | **Danilo Gallinari** (+2.80) | Anthony Davis (+2.06) [9] ✓ | Al Horford (+2.33) [96] ✓ |
| 26 | **Anthony Morrow** (+2.70) | LaMarcus Aldridge (+2.04) [30] ✓ | LaMarcus Aldridge (+2.33) [30] ✓ |
| 27 | **Tyreke Evans** (+2.60) | Reggie Jackson (+2.04) [40] ✓ | Goran Dragic (+2.33) [42] ✓ |
| 28 | **Chandler Parsons** (+2.60) | Kyle Korver (+2.01) [7] ✓ | Darren Collison (+2.31) [49] ✓ |
| 29 | **JJ Redick** (+2.50) | Aaron Brooks (+2.00) [55] ✓ | Dirk Nowitzki (+2.24) [34] ✓ |
| 30 | **LaMarcus Aldridge** (+2.40) | Brandon Jennings (+1.94) [23] ✓ | Dwyane Wade (+2.22) [40] ✓ |
| 31 | **Mike Conley** (+2.40) | Tyreke Evans (+1.86) [27] ✓ | Jrue Holiday (+2.21) [18] ✓ |
| 32 | **Patrick Patterson** (+2.40) | Rudy Gay (+1.73) [16] ✓ | Chandler Parsons (+2.18) [27] ✓ |
| 33 | **Wesley Matthews** (+2.30) | Jamal Crawford (+1.66) [67] ✓ | Kyle Korver (+2.00) [7] ✓ |
| 34 | **Jeff Teague** (+2.20) | Bradley Beal (+1.58) [74] ✓ | Danilo Gallinari (+1.99) [25] ✓ |
| 35 | **Dirk Nowitzki** (+2.20) | Eric Gordon (+1.57) [87] ✓ | Kevin Love (+1.92) [49] ✓ |
| 36 | **Gerald Green** (+2.20) | Khris Middleton (+1.45) [49] ✓ | Mike Conley (+1.91) [30] ✓ |
| 37 | **John Wall** (+2.10) | Gerald Green (+1.36) [34] ✓ | Brandan Wright (+1.89) [98] ✓ |
| 38 | **Devin Harris** (+2.10) | Patrick Patterson (+1.34) [30] ✓ | John Wall (+1.85) [37] ✓ |
| 39 | **Ersan Ilyasova** (+2.10) | Danilo Gallinari (+1.25) [25] ✓ | Khris Middleton (+1.84) [49] ✓ |
| 40 | **Reggie Jackson** (+2.00) | Marc Gasol (+1.22) [59] ✓ | DeMarre Carroll (+1.75) [42] ✓ |
| 41 | **Dwyane Wade** (+2.00) | Draymond Green (+1.21) [57] ✓ | Paul Millsap (+1.66) [69] ✓ |
| 42 | **DeMarre Carroll** (+1.90) | Danny Green (+1.21) [23] ✓ | Tyson Chandler (+1.60) [119] ✗ |
| 43 | **Nikola Mirotic** (+1.90) | Manu Ginobili (+1.17) [49] ✓ | Patrick Patterson (+1.59) [30] ✓ |
| 44 | **Goran Dragic** (+1.90) | Victor Oladipo (+1.16) [87] ✓ | Brook Lopez (+1.59) [104] ✗ |
| 45 | **JJ Barea** (+1.90) | Anthony Morrow (+1.09) [26] ✓ | Chris Bosh (+1.55) [150] ✗ |
| 46 | **Luol Deng** (+1.80) | Monta Ellis (+1.08) [79] ✓ | Tyreke Evans (+1.54) [27] ✓ |
| 47 | **Jae Crowder** (+1.80) | Dirk Nowitzki (+1.08) [34] ✓ | Derrick Favors (+1.52) [110] ✗ |
| 48 | **Joe Johnson** (+1.80) | Goran Dragic (+0.97) [42] ✓ | Monta Ellis (+1.52) [79] ✓ |
| 49 | **Khris Middleton** (+1.70) | Derrick Rose (+0.96) [110] ✗ | Danny Green (+1.52) [23] ✓ |
| 50 | **Eric Bledsoe** (+1.70) | Wesley Matthews (+0.94) [33] ✓ | Marc Gasol (+1.49) [59] ✓ |
| 51 | **Kevin Love** (+1.70) | Darren Collison (+0.93) [49] ✓ | DeMar DeRozan (+1.48) [130] ✗ |
| 52 | **Darren Collison** (+1.70) | Kevin Love (+0.92) [49] ✓ | Jamal Crawford (+1.45) [67] ✓ |
| 53 | **Manu Ginobili** (+1.70) | Greivis Vasquez (+0.91) [141] ✗ | Greg Monroe (+1.42) [123] ✗ |
| 54 | **Kevin Martin** (+1.70) | Devin Harris (+0.90) [37] ✓ | Luol Deng (+1.37) [46] ✓ |
| 55 | **Zach Randolph** (+1.60) | Mo Williams (+0.88) [61] ✓ | Kevin Martin (+1.36) [49] ✓ |
| 56 | **Aaron Brooks** (+1.60) | Eric Bledsoe (+0.86) [49] ✓ | Pau Gasol (+1.23) [98] ✓ |
| 57 | **Draymond Green** (+1.50) | Zach Randolph (+0.86) [55] ✓ | Tim Duncan (+1.23) [130] ✗ |
| 58 | **Ryan Anderson** (+1.50) | JJ Barea (+0.84) [42] ✓ | Amare Stoudemire (+1.22) [67] ✓ |
| 59 | **Marc Gasol** (+1.40) | DeMarcus Cousins (+0.77) [72] ✓ | DeMarcus Cousins (+1.20) [72] ✓ |
| 60 | **CJ Miles** (+1.40) | Paul Millsap (+0.70) [69] ✓ | Nikola Vucevic (+1.18) [159] ✗ |
| 61 | **Kemba Walker** (+1.20) | Chandler Parsons (+0.68) [27] ✓ | DeAndre Jordan (+1.15) [17] ✓ |
| 62 | **Deron Williams** (+1.20) | Nikola Mirotic (+0.67) [42] ✓ | James Johnson (+1.15) [79] ✓ |
| 63 | **Ed Davis** (+1.20) | Ersan Ilyasova (+0.63) [37] ✓ | Reggie Jackson (+1.11) [40] ✓ |
| 64 | **Robert Covington** (+1.20) | Ryan Anderson (+0.57) [57] ✓ | Tyler Zeller (+1.07) [141] ✗ |
| 65 | **Mo Williams** (+1.20) | Thaddeus Young (+0.51) [87] ✓ | Ed Davis (+1.05) [61] ✓ |
| 66 | **Kobe Bryant** (+1.20) | Ed Davis (+0.50) [61] ✓ | Paul Pierce (+1.05) [69] ✓ |
| 67 | **Amare Stoudemire** (+1.10) | DeAndre Jordan (+0.49) [17] ✓ | Ersan Ilyasova (+1.05) [37] ✓ |
| 68 | **Jamal Crawford** (+1.10) | Paul Pierce (+0.47) [69] ✓ | Manu Ginobili (+1.02) [49] ✓ |
| 69 | **Paul Millsap** (+1.00) | Matt Barnes (+0.45) [69] ✓ | Zach Randolph (+1.01) [55] ✓ |
| 70 | **Matt Barnes** (+1.00) | Kemba Walker (+0.42) [61] ✓ | JJ Barea (+0.94) [42] ✓ |
| 71 | **Paul Pierce** (+1.00) | CJ Miles (+0.41) [59] ✓ | Jonas Valanciunas (+0.93) [123] ✗ |
| 72 | **DeMarcus Cousins** (+0.90) | Pau Gasol (+0.40) [98] ✓ | Tony Parker (+0.90) [87] ✓ |
| 73 | **Jeremy Lin** (+0.90) | DJ Augustin (+0.40) [130] ✗ | Devin Harris (+0.87) [37] ✓ |
| 74 | **Bradley Beal** (+0.80) | Trey Burke (+0.38) [119] ✗ | Jodie Meeks (+0.86) [123] ✗ |
| 75 | **Kentavious CaldwellPope** (+0.80) | Greg Monroe (+0.38) [123] ✗ | Thaddeus Young (+0.86) [87] ✓ |
| 76 | **Kelly Olynyk** (+0.80) | Nikola Vucevic (+0.34) [159] ✗ | Eric Gordon (+0.86) [87] ✓ |
| 77 | **Jonas Jerebko** (+0.80) | CJ Watson (+0.32) [110] ✗ | Eric Bledsoe (+0.83) [49] ✓ |
| 78 | **Wilson Chandler** (+0.80) | Mike Dunleavy (+0.32) [79] ✓ | Ryan Anderson (+0.82) [57] ✓ |
| 79 | **Monta Ellis** (+0.70) | Kenneth Faried (+0.29) [84] ✓ | Draymond Green (+0.80) [57] ✓ |
| 80 | **James Johnson** (+0.70) | Tony Parker (+0.27) [87] ✓ | Deron Williams (+0.77) [61] ✓ |
| 81 | **Cory Joseph** (+0.70) | Nicolas Batum (+0.25) [84] ✓ | Joe Johnson (+0.72) [46] ✓ |
| 82 | **Mike Dunleavy** (+0.70) | Jordan Clarkson (+0.18) [104] ✗ | Jonas Jerebko (+0.71) [74] ✓ |
| 83 | **Anthony Tolliver** (+0.70) | Terrence Ross (+0.18) [98] ✗ | Harrison Barnes (+0.69) [137] ✗ |
| 84 | **Nicolas Batum** (+0.60) | Jeremy Lin (+0.17) [72] ✓ | Kenneth Faried (+0.66) [84] ✓ |
| 85 | **Kenneth Faried** (+0.60) | Evan Fournier (+0.16) [104] ✗ | Nikola Mirotic (+0.63) [42] ✓ |
| 86 | **Omri Casspi** (+0.60) | Dennis Schroder (+0.14) [159] ✗ | CJ Miles (+0.63) [59] ✓ |
| 87 | **Thaddeus Young** (+0.50) | DeMarre Carroll (+0.13) [42] ✓ | Mike Dunleavy (+0.60) [79] ✓ |
| 88 | **Elfrid Payton** (+0.50) | Jae Crowder (+0.13) [46] ✓ | Marreese Speights (+0.59) [194] ✗ |
| 89 | **Brandon Knight** (+0.50) | James Johnson (+0.11) [79] ✓ | Andre Iguodala (+0.56) [137] ✗ |
| 90 | **Jared Sullinger** (+0.50) | Harrison Barnes (+0.09) [137] ✗ | CJ Watson (+0.53) [110] ✗ |
| 91 | **Rodney Stuckey** (+0.50) | Luol Deng (+0.06) [46] ✓ | Marcin Gortat (+0.51) [141] ✗ |
| 92 | **Victor Oladipo** (+0.50) | Brandon Knight (+0.03) [87] ✓ | Tobias Harris (+0.50) [119] ✗ |
| 93 | **Eric Gordon** (+0.50) | Kentavious CaldwellPope (+0.03) [74] ✓ | Bradley Beal (+0.50) [74] ✓ |
| 94 | **Tony Parker** (+0.50) | Kevin Martin (+0.02) [49] ✓ | Trevor Ariza (+0.49) [123] ✗ |
| 95 | **Andre Miller** (+0.50) | DeMar DeRozan (+0.01) [130] ✗ | Kemba Walker (+0.48) [61] ✓ |
| 96 | **Al Horford** (+0.40) | Andre Iguodala (-0.00) [137] ✗ | Brandon Bass (+0.47) [202] ✗ |
| 97 | **Tim Hardaway Jr.** (+0.40) | Wayne Ellington (-0.01) [98] ✗ | Amir Johnson (+0.43) [123] ✗ |
| 98 | **Pau Gasol** (+0.30) | Jonas Jerebko (-0.05) [74] ✓ | Nicolas Batum (+0.43) [84] ✓ |
| 99 | **Marcus Smart** (+0.30) | Tristan Thompson (-0.07) [137] ✗ | Cory Joseph (+0.40) [79] ✓ |
| 100 | **Brandan Wright** (+0.30) | Cory Joseph (-0.08) [79] ✓ | Brandon Knight (+0.38) [87] ✓ |

### 2013-14 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.00) | Andrew Bogut (+4.44) [4] ✓ | Andrew Bogut (+3.58) [4] ✓ |
| 2 | **Draymond Green** (+4.60) | Draymond Green (+4.12) [2] ✓ | Kawhi Leonard (+3.44) [1] ✓ |
| 3 | **Joakim Noah** (+4.50) | Anthony Davis (+3.83) [33] ✓ | Draymond Green (+3.32) [2] ✓ |
| 4 | **Andrew Bogut** (+4.40) | Kevin Garnett (+3.63) [11] ✓ | DeAndre Jordan (+3.19) [64] ✓ |
| 5 | **Michael KiddGilchrist** (+4.40) | Tiago Splitter (+3.40) [6] ✓ | Jimmy Butler (+3.17) [14] ✓ |
| 6 | **Tiago Splitter** (+4.20) | CJ Watson (+3.22) [17] ✓ | Tony Allen (+3.07) [30] ✓ |
| 7 | **Danny Green** (+4.00) | Jae Crowder (+3.18) [18] ✓ | Joakim Noah (+3.05) [3] ✓ |
| 8 | **Chris Paul** (+3.90) | Kawhi Leonard (+3.11) [1] ✓ | Danny Green (+2.82) [7] ✓ |
| 9 | **Nene** (+3.80) | Paul George (+3.06) [21] ✓ | Ricky Rubio (+2.79) [47] ✓ |
| 10 | **Anderson Varejao** (+3.60) | Derek Fisher (+2.99) [43] ✓ | Andre Iguodala (+2.69) [25] ✓ |
| 11 | **Nick Calathes** (+3.50) | Andre Iguodala (+2.87) [25] ✓ | Paul George (+2.62) [21] ✓ |
| 12 | **Ian Mahinmi** (+3.50) | Ian Mahinmi (+2.86) [11] ✓ | Kyle OQuinn (+2.43) [52] ✓ |
| 13 | **Kevin Garnett** (+3.50) | Paul Millsap (+2.80) [23] ✓ | Roy Hibbert (+2.43) [14] ✓ |
| 14 | **Jimmy Butler** (+3.40) | Tony Allen (+2.80) [30] ✓ | David West (+2.32) [57] ✓ |
| 15 | **Roy Hibbert** (+3.40) | Danny Green (+2.79) [7] ✓ | Paul Millsap (+2.20) [23] ✓ |
| 16 | **DeMarcus Cousins** (+3.30) | Jimmy Butler (+2.71) [14] ✓ | Ian Mahinmi (+2.14) [11] ✓ |
| 17 | **CJ Watson** (+3.20) | Nene (+2.70) [9] ✓ | Anderson Varejao (+2.08) [10] ✓ |
| 18 | **Tim Duncan** (+3.00) | Chris Bosh (+2.67) [72] ✓ | Tim Duncan (+2.05) [18] ✓ |
| 19 | **Kris Humphries** (+3.00) | Joakim Noah (+2.57) [3] ✓ | CJ Watson (+2.03) [17] ✓ |
| 20 | **Jae Crowder** (+3.00) | Anderson Varejao (+2.57) [10] ✓ | Chris Paul (+2.00) [8] ✓ |
| 21 | **Paul George** (+2.90) | Kirk Hinrich (+2.52) [40] ✓ | Bismack Biyombo (+1.84) [64] ✓ |
| 22 | **Marc Gasol** (+2.80) | Gerald Wallace (+2.51) [52] ✓ | Kevin Garnett (+1.76) [11] ✓ |
| 23 | **Paul Millsap** (+2.70) | Marcin Gortat (+2.45) [27] ✓ | Manu Ginobili (+1.74) [64] ✓ |
| 24 | **Shane Battier** (+2.70) | Tim Duncan (+2.32) [18] ✓ | George Hill (+1.72) [40] ✓ |
| 25 | **DeMarre Carroll** (+2.60) | Kyle OQuinn (+2.31) [52] ✓ | Iman Shumpert (+1.70) [72] ✓ |
| 26 | **Andre Iguodala** (+2.60) | DeMarcus Cousins (+2.28) [16] ✓ | Serge Ibaka (+1.70) [33] ✓ |
| 27 | **Mario Chalmers** (+2.50) | Al Jefferson (+2.27) [47] ✓ | Nick Calathes (+1.65) [11] ✓ |
| 28 | **Marcin Gortat** (+2.50) | Ersan Ilyasova (+2.27) [88] ✓ | Anthony Davis (+1.65) [33] ✓ |
| 29 | **Samuel Dalembert** (+2.50) | Chris Andersen (+2.25) [52] ✓ | Kirk Hinrich (+1.56) [40] ✓ |
| 30 | **Dwight Howard** (+2.40) | David West (+2.18) [57] ✓ | Phil Pressey (+1.55) [76] ✓ |
| 31 | **Tony Allen** (+2.40) | Blake Griffin (+2.17) [110] ✗ | Thabo Sefolosha (+1.51) [33] ✓ |
| 32 | **Victor Oladipo** (+2.40) | LaMarcus Aldridge (+2.17) [37] ✓ | Taj Gibson (+1.46) [83] ✓ |
| 33 | **Kemba Walker** (+2.30) | Dwight Howard (+2.16) [30] ✓ | DeMarcus Cousins (+1.45) [16] ✓ |
| 34 | **Anthony Davis** (+2.30) | Nick Collison (+2.15) [88] ✓ | Lance Stephenson (+1.45) [164] ✗ |
| 35 | **Serge Ibaka** (+2.30) | Paul Pierce (+2.13) [47] ✓ | Nene (+1.44) [9] ✓ |
| 36 | **Thabo Sefolosha** (+2.30) | Darrell Arthur (+2.13) [45] ✓ | Trevor Ariza (+1.42) [88] ✓ |
| 37 | **LaMarcus Aldridge** (+2.20) | Nick Calathes (+2.09) [11] ✓ | Jae Crowder (+1.41) [18] ✓ |
| 38 | **Nikola Pekovic** (+2.20) | Thabo Sefolosha (+2.09) [33] ✓ | Al Jefferson (+1.40) [47] ✓ |
| 39 | **Eric Bledsoe** (+2.20) | Greg Monroe (+2.03) [143] ✗ | Corey Brewer (+1.38) [88] ✓ |
| 40 | **George Hill** (+2.10) | Michael KiddGilchrist (+2.02) [4] ✓ | DeMarre Carroll (+1.35) [25] ✓ |
| 41 | **Kirk Hinrich** (+2.10) | Miles Plumlee (+2.01) [52] ✓ | Dwight Howard (+1.35) [30] ✓ |
| 42 | **Kosta Koufos** (+2.10) | Amir Johnson (+1.98) [62] ✓ | Marcin Gortat (+1.34) [27] ✓ |
| 43 | **Robin Lopez** (+2.00) | Patrick Patterson (+1.94) [83] ✓ | Elton Brand (+1.32) [83] ✓ |
| 44 | **Derek Fisher** (+2.00) | Iman Shumpert (+1.93) [72] ✓ | LaMarcus Aldridge (+1.26) [37] ✓ |
| 45 | **Patrick Beverley** (+1.90) | Pablo Prigioni (+1.90) [124] ✗ | Patrick Patterson (+1.23) [83] ✓ |
| 46 | **Darrell Arthur** (+1.90) | DeMarre Carroll (+1.87) [25] ✓ | Michael KiddGilchrist (+1.22) [4] ✓ |
| 47 | **Ricky Rubio** (+1.80) | Chris Paul (+1.87) [8] ✓ | Chris Andersen (+1.19) [52] ✓ |
| 48 | **Al Jefferson** (+1.80) | Shane Battier (+1.87) [23] ✓ | Derek Fisher (+1.17) [43] ✓ |
| 49 | **Paul Pierce** (+1.80) | George Hill (+1.86) [40] ✓ | Mario Chalmers (+1.17) [27] ✓ |
| 50 | **Jeremy Lin** (+1.80) | DeAndre Jordan (+1.85) [64] ✓ | John Wall (+1.16) [136] ✗ |
| 51 | **Kendrick Perkins** (+1.70) | Mario Chalmers (+1.82) [27] ✓ | Gerald Wallace (+1.14) [52] ✓ |
| 52 | **Darren Collison** (+1.60) | Robin Lopez (+1.80) [43] ✓ | Amir Johnson (+1.12) [62] ✓ |
| 53 | **Chris Andersen** (+1.60) | Josh Smith (+1.75) [83] ✓ | Kemba Walker (+1.08) [33] ✓ |
| 54 | **Kyle OQuinn** (+1.60) | Roy Hibbert (+1.74) [14] ✓ | Marc Gasol (+1.05) [22] ✓ |
| 55 | **Gerald Wallace** (+1.60) | Victor Oladipo (+1.72) [30] ✓ | Mike Dunleavy (+1.05) [121] ✗ |
| 56 | **Miles Plumlee** (+1.60) | Shaun Livingston (+1.67) [76] ✓ | Patrick Beverley (+1.00) [45] ✓ |
| 57 | **David West** (+1.50) | DeJuan Blair (+1.66) [129] ✗ | Josh McRoberts (+0.95) [180] ✗ |
| 58 | **Kyle Lowry** (+1.30) | Elton Brand (+1.66) [83] ✓ | Kosta Koufos (+0.95) [40] ✓ |
| 59 | **Nate Wolters** (+1.30) | ETwaun Moore (+1.65) [72] ✓ | Russell Westbrook (+0.94) [99] ✗ |
| 60 | **Omri Casspi** (+1.30) | Patrick Beverley (+1.62) [45] ✓ | Patty Mills (+0.92) [76] ✓ |
| 61 | **Tayshaun Prince** (+1.30) | Samuel Dalembert (+1.60) [27] ✓ | Chris Bosh (+0.91) [72] ✓ |
| 62 | **David Lee** (+1.20) | Taj Gibson (+1.60) [83] ✓ | Klay Thompson (+0.91) [83] ✓ |
| 63 | **Amir Johnson** (+1.20) | Timofey Mozgov (+1.58) [64] ✓ | Darrell Arthur (+0.88) [45] ✓ |
| 64 | **DeAndre Jordan** (+1.10) | Patty Mills (+1.58) [76] ✓ | Andre Drummond (+0.86) [114] ✗ |
| 65 | **Manu Ginobili** (+1.10) | Kemba Walker (+1.56) [33] ✓ | Tiago Splitter (+0.86) [6] ✓ |
| 66 | **Jared Sullinger** (+1.10) | Andray Blatche (+1.56) [64] ✓ | Victor Oladipo (+0.85) [30] ✓ |
| 67 | **Andray Blatche** (+1.10) | PJ Tucker (+1.55) [76] ✓ | Shaun Livingston (+0.85) [76] ✓ |
| 68 | **Jeremy Evans** (+1.10) | Channing Frye (+1.54) [99] ✗ | Kyle Korver (+0.77) [158] ✗ |
| 69 | **Timofey Mozgov** (+1.10) | Jason Thompson (+1.52) [110] ✗ | David Lee (+0.76) [62] ✓ |
| 70 | **Tyson Chandler** (+1.10) | Steven Adams (+1.51) [121] ✗ | Derrick Favors (+0.75) [99] ✗ |
| 71 | **Bismack Biyombo** (+1.10) | Kosta Koufos (+1.50) [40] ✓ | Carlos Boozer (+0.71) [188] ✗ |
| 72 | **Chris Bosh** (+1.00) | Omri Casspi (+1.48) [58] ✓ | Jeremy Lamb (+0.70) [124] ✗ |
| 73 | **Courtney Lee** (+1.00) | Nicolas Batum (+1.48) [124] ✗ | Nikola Vucevic (+0.70) [76] ✓ |
| 74 | **Iman Shumpert** (+1.00) | Manu Ginobili (+1.46) [64] ✓ | Courtney Lee (+0.69) [72] ✓ |
| 75 | **ETwaun Moore** (+1.00) | Tyson Chandler (+1.42) [64] ✓ | Andray Blatche (+0.69) [64] ✓ |
| 76 | **Kevin Love** (+0.90) | Trevor Ariza (+1.42) [88] ✓ | Maurice Harkless (+0.68) [99] ✗ |
| 77 | **Patty Mills** (+0.90) | Ricky Rubio (+1.38) [47] ✓ | Nicolas Batum (+0.65) [124] ✗ |
| 78 | **PJ Tucker** (+0.90) | Andrew Nicholson (+1.36) [164] ✗ | Steven Adams (+0.65) [121] ✗ |
| 79 | **Shaun Livingston** (+0.90) | Dirk Nowitzki (+1.36) [99] ✓ | Dante Cunningham (+0.64) [114] ✗ |
| 80 | **Nikola Vucevic** (+0.90) | Kyle Lowry (+1.30) [58] ✓ | Shane Battier (+0.64) [23] ✓ |
| 81 | **Avery Bradley** (+0.90) | Luis Scola (+1.30) [124] ✗ | PJ Tucker (+0.63) [76] ✓ |
| 82 | **Phil Pressey** (+0.90) | David Lee (+1.29) [62] ✓ | Channing Frye (+0.62) [99] ✗ |
| 83 | **Klay Thompson** (+0.80) | Jeremy Lamb (+1.25) [124] ✗ | Miles Plumlee (+0.59) [52] ✓ |
| 84 | **Taj Gibson** (+0.80) | Phil Pressey (+1.25) [76] ✓ | Tyson Chandler (+0.57) [64] ✓ |
| 85 | **Patrick Patterson** (+0.80) | Marc Gasol (+1.23) [22] ✓ | Kyle Lowry (+0.56) [58] ✓ |
| 86 | **Josh Smith** (+0.80) | Jared Sullinger (+1.23) [64] ✓ | Matt Barnes (+0.55) [143] ✗ |
| 87 | **Elton Brand** (+0.80) | Jeremy Evans (+1.22) [64] ✓ | Terrence Jones (+0.53) [203] ✗ |
| 88 | **Trevor Ariza** (+0.70) | Andre Drummond (+1.17) [114] ✗ | Josh Smith (+0.52) [83] ✓ |
| 89 | **Corey Brewer** (+0.70) | Anthony Tolliver (+1.16) [143] ✗ | Deron Williams (+0.52) [92] ✓ |
| 90 | **Nick Collison** (+0.70) | Robert Sacre (+1.15) [114] ✗ | Eric Bledsoe (+0.50) [37] ✓ |
| 91 | **Ersan Ilyasova** (+0.70) | Kendrick Perkins (+1.12) [51] ✓ | Michael CarterWilliams (+0.50) [92] ✓ |
| 92 | **Deron Williams** (+0.60) | Dante Cunningham (+1.11) [114] ✗ | Paul Pierce (+0.49) [47] ✓ |
| 93 | **Luol Deng** (+0.60) | Kyle Korver (+1.05) [158] ✗ | Boris Diaw (+0.47) [110] ✗ |
| 94 | **Michael CarterWilliams** (+0.60) | Deron Williams (+1.04) [92] ✓ | Blake Griffin (+0.43) [110] ✗ |
| 95 | **Matthew Dellavedova** (+0.60) | Kentavious CaldwellPope (+1.02) [114] ✗ | Terrence Ross (+0.41) [129] ✗ |
| 96 | **Andrea Bargnani** (+0.60) | Nikola Vucevic (+0.99) [76] ✓ | Kris Humphries (+0.39) [18] ✓ |
| 97 | **Spencer Hawes** (+0.50) | Josh McRoberts (+0.97) [180] ✗ | Cody Zeller (+0.38) [136] ✗ |
| 98 | **AlFarouq Aminu** (+0.50) | Jonas Valanciunas (+0.96) [121] ✗ | Pablo Prigioni (+0.38) [124] ✗ |
| 99 | **Dirk Nowitzki** (+0.40) | Bismack Biyombo (+0.94) [64] ✓ | Giannis Antetokounmpo (+0.36) [114] ✗ |
| 100 | **Isaiah Thomas** (+0.40) | Kris Humphries (+0.93) [18] ✓ | Gerald Henderson (+0.34) [136] ✗ |

### 2014-15 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.20) | Andrew Bogut (+4.93) [5] ✓ | Kawhi Leonard (+4.47) [1] ✓ |
| 2 | **Draymond Green** (+5.10) | Rudy Gobert (+4.81) [3] ✓ | Tony Allen (+4.32) [3] ✓ |
| 3 | **Rudy Gobert** (+4.80) | Draymond Green (+4.72) [2] ✓ | Draymond Green (+3.75) [2] ✓ |
| 4 | **Tony Allen** (+4.80) | Tony Allen (+4.13) [3] ✓ | Andrew Bogut (+3.21) [5] ✓ |
| 5 | **Andrew Bogut** (+4.70) | Kawhi Leonard (+3.70) [1] ✓ | DeAndre Jordan (+3.15) [60] ✓ |
| 6 | **Anthony Davis** (+4.50) | Nerlens Noel (+3.69) [19] ✓ | Nerlens Noel (+2.89) [19] ✓ |
| 7 | **DeMarcus Cousins** (+4.40) | Greg Monroe (+3.68) [111] ✗ | Anthony Davis (+2.86) [6] ✓ |
| 8 | **Marcin Gortat** (+3.60) | Nene (+3.54) [17] ✓ | Rudy Gobert (+2.78) [3] ✓ |
| 9 | **Tim Duncan** (+3.50) | Tyson Chandler (+3.18) [20] ✓ | Danny Green (+2.72) [14] ✓ |
| 10 | **Andre Roberson** (+3.40) | Zaza Pachulia (+3.16) [12] ✓ | AlFarouq Aminu (+2.59) [20] ✓ |
| 11 | **Kosta Koufos** (+3.30) | Anthony Davis (+2.99) [6] ✓ | Tim Duncan (+2.49) [9] ✓ |
| 12 | **Zaza Pachulia** (+3.20) | Dwight Howard (+2.92) [40] ✓ | Paul Millsap (+2.35) [26] ✓ |
| 13 | **Khris Middleton** (+3.10) | Jared Dudley (+2.91) [42] ✓ | Khris Middleton (+2.07) [13] ✓ |
| 14 | **Danny Green** (+3.00) | Andre Roberson (+2.88) [10] ✓ | Andre Roberson (+2.05) [10] ✓ |
| 15 | **Serge Ibaka** (+3.00) | Tim Duncan (+2.76) [9] ✓ | Bismack Biyombo (+2.05) [102] ✗ |
| 16 | **Michael KiddGilchrist** (+3.00) | Pau Gasol (+2.74) [85] ✓ | Marcus Smart (+1.98) [42] ✓ |
| 17 | **Jonas Jerebko** (+2.80) | AlFarouq Aminu (+2.69) [20] ✓ | John Wall (+1.97) [127] ✗ |
| 18 | **Nene** (+2.80) | Jonas Jerebko (+2.61) [17] ✓ | DeMarcus Cousins (+1.95) [7] ✓ |
| 19 | **Nerlens Noel** (+2.70) | DeMarcus Cousins (+2.60) [7] ✓ | Marcin Gortat (+1.93) [8] ✓ |
| 20 | **Marc Gasol** (+2.60) | Nikola Mirotic (+2.59) [33] ✓ | Michael KiddGilchrist (+1.74) [14] ✓ |
| 21 | **Tyson Chandler** (+2.60) | Michael KiddGilchrist (+2.57) [14] ✓ | John Henson (+1.64) [85] ✓ |
| 22 | **Joakim Noah** (+2.60) | Andre Drummond (+2.53) [93] ✓ | Nicolas Batum (+1.55) [136] ✗ |
| 23 | **Josh Smith** (+2.60) | Khris Middleton (+2.52) [13] ✓ | Zaza Pachulia (+1.53) [12] ✓ |
| 24 | **AlFarouq Aminu** (+2.60) | Kosta Koufos (+2.49) [11] ✓ | Trevor Ariza (+1.51) [102] ✗ |
| 25 | **Alex Len** (+2.50) | Marcin Gortat (+2.44) [8] ✓ | Andre Drummond (+1.50) [93] ✓ |
| 26 | **Paul Millsap** (+2.40) | Derrick Favors (+2.41) [34] ✓ | Kosta Koufos (+1.48) [11] ✓ |
| 27 | **Timofey Mozgov** (+2.40) | Josh Smith (+2.39) [20] ✓ | Iman Shumpert (+1.40) [29] ✓ |
| 28 | **Omer Asik** (+2.40) | Kelly Olynyk (+2.35) [34] ✓ | Cody Zeller (+1.40) [34] ✓ |
| 29 | **Darren Collison** (+2.30) | Wesley Matthews (+2.30) [54] ✓ | Kelly Olynyk (+1.40) [34] ✓ |
| 30 | **Iman Shumpert** (+2.30) | Michael CarterWilliams (+2.28) [29] ✓ | Jae Crowder (+1.36) [102] ✗ |
| 31 | **Michael CarterWilliams** (+2.30) | Danny Green (+2.25) [14] ✓ | James Johnson (+1.35) [54] ✓ |
| 32 | **Luc Mbah a Moute** (+2.30) | Timofey Mozgov (+2.24) [26] ✓ | Nikola Mirotic (+1.34) [33] ✓ |
| 33 | **Nikola Mirotic** (+2.20) | Andre Iguodala (+2.24) [46] ✓ | Serge Ibaka (+1.33) [14] ✓ |
| 34 | **Chris Paul** (+2.10) | Omer Asik (+2.21) [26] ✓ | Marc Gasol (+1.31) [20] ✓ |
| 35 | **Derrick Favors** (+2.10) | Marcus Smart (+2.21) [42] ✓ | Dwight Howard (+1.31) [40] ✓ |
| 36 | **Kelly Olynyk** (+2.10) | Iman Shumpert (+2.19) [29] ✓ | Nene (+1.28) [17] ✓ |
| 37 | **Cody Zeller** (+2.10) | Jimmy Butler (+2.14) [136] ✗ | Chris Paul (+1.28) [34] ✓ |
| 38 | **Steven Adams** (+2.00) | Paul Millsap (+2.10) [26] ✓ | George Hill (+1.27) [45] ✓ |
| 39 | **Roy Hibbert** (+2.00) | Pablo Prigioni (+2.04) [42] ✓ | Giannis Antetokounmpo (+1.26) [146] ✗ |
| 40 | **LaMarcus Aldridge** (+1.90) | Luc Mbah a Moute (+2.02) [29] ✓ | Marvin Williams (+1.25) [69] ✓ |
| 41 | **Dwight Howard** (+1.90) | Joakim Noah (+2.01) [20] ✓ | Miles Plumlee (+1.25) [85] ✓ |
| 42 | **Marcus Smart** (+1.80) | Zach Randolph (+1.99) [54] ✓ | Alex Len (+1.22) [25] ✓ |
| 43 | **Jared Dudley** (+1.80) | Manu Ginobili (+1.92) [51] ✓ | Al Jefferson (+1.22) [102] ✗ |
| 44 | **Pablo Prigioni** (+1.80) | Jae Crowder (+1.87) [102] ✗ | Andre Iguodala (+1.22) [46] ✓ |
| 45 | **George Hill** (+1.70) | Brandan Wright (+1.87) [54] ✓ | Jimmy Butler (+1.20) [136] ✗ |
| 46 | **Kevin Love** (+1.60) | Harrison Barnes (+1.85) [132] ✗ | CJ Miles (+1.16) [111] ✗ |
| 47 | **Al Horford** (+1.60) | Amir Johnson (+1.80) [73] ✓ | Elfrid Payton (+1.16) [111] ✗ |
| 48 | **Andre Iguodala** (+1.60) | Al Jefferson (+1.79) [102] ✗ | Robert Covington (+1.12) [136] ✗ |
| 49 | **Mario Chalmers** (+1.50) | Donatas Motiejunas (+1.78) [73] ✓ | Josh Smith (+1.12) [20] ✓ |
| 50 | **Kris Humphries** (+1.50) | John Henson (+1.77) [85] ✓ | Michael CarterWilliams (+1.12) [29] ✓ |
| 51 | **Manu Ginobili** (+1.40) | Mario Chalmers (+1.77) [49] ✓ | Pablo Prigioni (+1.11) [42] ✓ |
| 52 | **Alan Anderson** (+1.40) | Cory Joseph (+1.74) [85] ✓ | Jared Dudley (+1.07) [42] ✓ |
| 53 | **Robin Lopez** (+1.40) | PJ Tucker (+1.72) [54] ✓ | Manu Ginobili (+1.06) [51] ✓ |
| 54 | **Wesley Matthews** (+1.30) | Luis Scola (+1.70) [82] ✓ | LeBron James (+1.04) [132] ✗ |
| 55 | **Zach Randolph** (+1.30) | George Hill (+1.66) [45] ✓ | Jeff Teague (+1.04) [85] ✓ |
| 56 | **PJ Tucker** (+1.30) | James Johnson (+1.63) [54] ✓ | PJ Tucker (+1.03) [54] ✓ |
| 57 | **James Johnson** (+1.30) | Marvin Williams (+1.61) [69] ✓ | Joakim Noah (+1.01) [20] ✓ |
| 58 | **Brandan Wright** (+1.30) | Steven Adams (+1.59) [38] ✓ | Pau Gasol (+1.00) [85] ✓ |
| 59 | **Langston Galloway** (+1.30) | Jonas Valanciunas (+1.58) [60] ✓ | Roy Hibbert (+1.00) [38] ✓ |
| 60 | **DeAndre Jordan** (+1.20) | Ersan Ilyasova (+1.55) [93] ✓ | Derrick Favors (+0.98) [34] ✓ |
| 61 | **Eric Bledsoe** (+1.20) | Corey Brewer (+1.53) [69] ✓ | Zach Randolph (+0.98) [54] ✓ |
| 62 | **Kemba Walker** (+1.20) | Chris Kaman (+1.50) [111] ✗ | Brandan Wright (+0.96) [54] ✓ |
| 63 | **Jonas Valanciunas** (+1.20) | Alex Len (+1.49) [25] ✓ | Bradley Beal (+0.94) [85] ✓ |
| 64 | **Gerald Henderson** (+1.10) | Kent Bazemore (+1.47) [127] ✗ | Luis Scola (+0.94) [82] ✓ |
| 65 | **CJ Watson** (+1.10) | Roy Hibbert (+1.46) [38] ✓ | Matt Barnes (+0.93) [79] ✓ |
| 66 | **Klay Thompson** (+1.00) | Patrick Patterson (+1.45) [162] ✗ | Jonas Jerebko (+0.92) [17] ✓ |
| 67 | **DeMarre Carroll** (+1.00) | Jared Sullinger (+1.44) [85] ✓ | Al Horford (+0.90) [46] ✓ |
| 68 | **Patrick Beverley** (+1.00) | Al Horford (+1.43) [46] ✓ | David West (+0.89) [102] ✗ |
| 69 | **Corey Brewer** (+0.90) | Trevor Booker (+1.43) [69] ✓ | Wesley Matthews (+0.86) [54] ✓ |
| 70 | **Markieff Morris** (+0.90) | DeAndre Jordan (+1.40) [60] ✓ | Jerami Grant (+0.85) [102] ✗ |
| 71 | **Marvin Williams** (+0.90) | Bismack Biyombo (+1.35) [102] ✗ | James Harden (+0.84) [121] ✗ |
| 72 | **Trevor Booker** (+0.90) | CJ Watson (+1.35) [64] ✓ | Kyle Korver (+0.83) [121] ✗ |
| 73 | **Monta Ellis** (+0.80) | Kendrick Perkins (+1.32) [73] ✓ | DeMarre Carroll (+0.77) [66] ✓ |
| 74 | **Amir Johnson** (+0.80) | Anthony Tolliver (+1.30) [199] ✗ | Klay Thompson (+0.74) [66] ✓ |
| 75 | **Donatas Motiejunas** (+0.80) | Taj Gibson (+1.29) [73] ✓ | CJ Watson (+0.71) [64] ✓ |
| 76 | **Derrick Rose** (+0.80) | LaMarcus Aldridge (+1.27) [40] ✓ | Russell Westbrook (+0.69) [151] ✗ |
| 77 | **Taj Gibson** (+0.80) | Robert Covington (+1.26) [136] ✗ | Tyson Chandler (+0.68) [20] ✓ |
| 78 | **Kendrick Perkins** (+0.80) | Giannis Antetokounmpo (+1.26) [146] ✗ | Kent Bazemore (+0.66) [127] ✗ |
| 79 | **JJ Redick** (+0.70) | Klay Thompson (+1.23) [66] ✓ | LaMarcus Aldridge (+0.64) [40] ✓ |
| 80 | **Matt Barnes** (+0.70) | Trevor Ariza (+1.18) [102] ✗ | Kemba Walker (+0.62) [60] ✓ |
| 81 | **KJ McDaniels** (+0.70) | Nicolas Batum (+1.13) [136] ✗ | Eric Bledsoe (+0.59) [60] ✓ |
| 82 | **Kyle Lowry** (+0.60) | Matt Barnes (+1.11) [79] ✓ | Evan Turner (+0.58) [111] ✗ |
| 83 | **Brook Lopez** (+0.60) | David West (+1.07) [102] ✗ | KJ McDaniels (+0.52) [79] ✓ |
| 84 | **Luis Scola** (+0.60) | Monta Ellis (+1.03) [73] ✓ | Jrue Holiday (+0.51) [155] ✗ |
| 85 | **Mike Conley** (+0.50) | Steve Blake (+1.01) [172] ✗ | Otto Porter Jr. (+0.45) [191] ✗ |
| 86 | **Jeff Teague** (+0.50) | LeBron James (+1.00) [132] ✗ | Kris Humphries (+0.45) [49] ✓ |
| 87 | **Pau Gasol** (+0.50) | Patrick Beverley (+0.98) [66] ✓ | Harrison Barnes (+0.44) [132] ✗ |
| 88 | **Bradley Beal** (+0.50) | Alan Anderson (+0.95) [51] ✓ | Markieff Morris (+0.43) [69] ✓ |
| 89 | **Jared Sullinger** (+0.50) | Brook Lopez (+0.94) [82] ✓ | Kevin Love (+0.42) [46] ✓ |
| 90 | **Cory Joseph** (+0.50) | Quincy Acy (+0.94) [146] ✗ | Greg Monroe (+0.41) [111] ✗ |
| 91 | **Miles Plumlee** (+0.50) | Miles Plumlee (+0.94) [85] ✓ | Corey Brewer (+0.37) [69] ✓ |
| 92 | **John Henson** (+0.50) | Dante Exum (+0.92) [162] ✗ | Paul Pierce (+0.37) [111] ✗ |
| 93 | **Devin Harris** (+0.40) | Jeremy Lin (+0.91) [93] ✓ | Timofey Mozgov (+0.37) [26] ✓ |
| 94 | **Thaddeus Young** (+0.40) | Boris Diaw (+0.91) [155] ✗ | Steven Adams (+0.36) [38] ✓ |
| 95 | **Ed Davis** (+0.40) | Danilo Gallinari (+0.90) [111] ✗ | Mario Chalmers (+0.36) [49] ✓ |
| 96 | **Jeremy Lin** (+0.40) | Markieff Morris (+0.90) [69] ✓ | Mike Dunleavy (+0.35) [151] ✗ |
| 97 | **Ersan Ilyasova** (+0.40) | Tyler Zeller (+0.90) [102] ✗ | Mike Conley (+0.34) [85] ✓ |
| 98 | **Andre Drummond** (+0.40) | Kyle Lowry (+0.89) [82] ✓ | Rajon Rondo (+0.34) [93] ✗ |
| 99 | **Tony Snell** (+0.40) | Henry Sims (+0.88) [214] ✗ | Monta Ellis (+0.30) [73] ✓ |
| 100 | **Chris Bosh** (+0.40) | Serge Ibaka (+0.87) [14] ✓ | Mason Plumlee (+0.28) [214] ✗ |

