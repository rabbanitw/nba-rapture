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
| ours (direct total) | 1.776 | 1.337 | +0.734 | +0.873 | +0.869 |
| ours (offense+defense) | 1.745 | 1.305 | +0.743 | +0.871 | +0.865 |
| Paine (eRO+eRD) | 1.938 | 1.380 | +0.683 | +0.841 | +0.846 |

**offense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.012 | 0.752 | +0.825 | +0.924 | +0.917 |
| Paine (eRO) | 1.309 | 0.960 | +0.707 | +0.847 | +0.825 |

**defense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.403 | 1.047 | +0.638 | +0.806 | +0.795 |
| Paine (eRD) | 1.642 | 1.196 | +0.504 | +0.726 | +0.727 |

## Summary — true top-100 members recovered (hits@100)

**total**

| season | split | pool | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) | ρ ours (direct total) | ρ ours (offense+defense) | ρ Paine (eRO+eRD) |
|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 82/100 | 81/100 | 80/100 | +0.880 | +0.885 | +0.889 |
| 2014-15 | Regular season | 246 | 81/100 | 86/100 | 85/100 | +0.879 | +0.887 | +0.901 |
| **all** | | | **163/200** | **167/200** | **165/200** |  |  |  |

Precision@K for total, summed over 2 cells:

| K | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|
| 10 | 15/20 | 15/20 | 15/20 |
| 25 | 34/50 | 36/50 | 35/50 |
| 50 | 79/100 | 76/100 | 75/100 |
| 100 | 163/200 | 167/200 | 165/200 |

**offense**

| season | split | pool | ours | Paine (eRO) | ρ ours | ρ Paine (eRO) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 84/100 | 80/100 | +0.935 | +0.866 |
| 2014-15 | Regular season | 246 | 86/100 | 80/100 | +0.938 | +0.858 |
| **all** | | | **170/200** | **160/200** |  |  |

Precision@K for offense, summed over 2 cells:

| K | ours | Paine (eRO) |
|---|---|---|
| 10 | 16/20 | 17/20 |
| 25 | 40/50 | 40/50 |
| 50 | 81/100 | 76/100 |
| 100 | 170/200 | 160/200 |

**defense**

| season | split | pool | ours | Paine (eRD) | ρ ours | ρ Paine (eRD) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 79/100 | 76/100 | +0.852 | +0.794 |
| 2014-15 | Regular season | 246 | 79/100 | 73/100 | +0.838 | +0.801 |
| **all** | | | **158/200** | **149/200** |  |  |

Precision@K for defense, summed over 2 cells:

| K | ours | Paine (eRD) |
|---|---|---|
| 10 | 9/20 | 11/20 |
| 25 | 32/50 | 31/50 |
| 50 | 70/100 | 68/100 |
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
summing our two part-models are near-interchangeable: R² +0.734 vs +0.743, ρ +0.869 vs +0.865, hits@100 163/200 vs 167/200.

**Against Paine on the total.** R² +0.734 vs +0.683, RMSE 1.776 vs 1.938, ρ +0.869 vs +0.846; hits@100 163/200 vs 165/200.

**Offense.** ours R² +0.825 / ρ +0.917 / hits@100 170/200; Paine R² +0.707 / ρ +0.825 / hits@100 160/200.

**Defense.** ours R² +0.638 / ρ +0.795 / hits@100 158/200; Paine R² +0.504 / ρ +0.727 / hits@100 149/200.

Read the precision@K tables above rather than a single cutoff: they show
where each system's advantage actually lives, and a hits count at one
arbitrary K is decided by hundredths of a point among near-tied players.

## Leaderboards

`[n]` after a predicted name is that player's *true* rank; ✓ means they are
genuinely in the true top 100.

### 2013-14 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+11.00) | Kevin Durant (+6.55) [2] ✓ | Chris Paul (+7.03) [1] ✓ | Chris Paul (+8.79) [1] ✓ |
| 2 | **Kevin Durant** (+7.10) | LeBron James (+5.96) [15] ✓ | Kevin Durant (+6.92) [2] ✓ | Kevin Durant (+7.21) [2] ✓ |
| 3 | **Kawhi Leonard** (+6.70) | Blake Griffin (+5.58) [34] ✓ | LeBron James (+6.43) [15] ✓ | LeBron James (+6.77) [15] ✓ |
| 4 | **Kevin Love** (+6.60) | Chris Paul (+5.39) [1] ✓ | Kevin Love (+5.59) [4] ✓ | Kawhi Leonard (+5.95) [3] ✓ |
| 5 | **James Harden** (+6.10) | Kevin Love (+4.97) [4] ✓ | Paul George (+5.45) [8] ✓ | James Harden (+5.50) [5] ✓ |
| 6 | **Joakim Noah** (+5.90) | Paul George (+4.79) [8] ✓ | Blake Griffin (+5.28) [34] ✓ | Paul George (+5.08) [8] ✓ |
| 7 | **Kyle Lowry** (+5.70) | Kawhi Leonard (+4.58) [3] ✓ | Kyle Lowry (+5.12) [7] ✓ | Manu Ginobili (+4.99) [9] ✓ |
| 8 | **Paul George** (+5.60) | James Harden (+4.44) [5] ✓ | Manu Ginobili (+4.78) [9] ✓ | Kevin Love (+4.86) [4] ✓ |
| 9 | **Manu Ginobili** (+5.10) | Kyle Lowry (+4.33) [7] ✓ | Goran Dragic (+4.60) [10] ✓ | Goran Dragic (+4.71) [10] ✓ |
| 10 | **Goran Dragic** (+5.00) | Manu Ginobili (+4.22) [9] ✓ | James Harden (+4.46) [5] ✓ | Andre Iguodala (+4.53) [21] ✓ |
| 11 | **DeMarcus Cousins** (+5.00) | Dirk Nowitzki (+4.04) [13] ✓ | Pablo Prigioni (+4.22) [70] ✓ | Kyle Lowry (+4.27) [7] ✓ |
| 12 | **Patty Mills** (+4.80) | Anderson Varejao (+3.97) [16] ✓ | Andre Iguodala (+4.05) [21] ✓ | Joakim Noah (+4.22) [6] ✓ |
| 13 | **Dirk Nowitzki** (+4.70) | Andrew Bogut (+3.83) [36] ✓ | Andrew Bogut (+4.03) [36] ✓ | Russell Westbrook (+4.18) [25] ✓ |
| 14 | **Danny Green** (+4.70) | Carmelo Anthony (+3.62) [21] ✓ | Damian Lillard (+3.99) [53] ✓ | Anthony Davis (+4.18) [28] ✓ |
| 15 | **LeBron James** (+4.60) | Draymond Green (+3.45) [29] ✓ | Kawhi Leonard (+3.96) [3] ✓ | Blake Griffin (+4.00) [34] ✓ |
| 16 | **Anderson Varejao** (+4.10) | Damian Lillard (+3.44) [53] ✓ | Patty Mills (+3.81) [12] ✓ | Ricky Rubio (+3.91) [25] ✓ |
| 17 | **Patrick Beverley** (+4.10) | Andre Iguodala (+3.41) [21] ✓ | Dirk Nowitzki (+3.72) [13] ✓ | Brandan Wright (+3.89) [115] ✗ |
| 18 | **Mario Chalmers** (+4.00) | DeMarcus Cousins (+3.40) [10] ✓ | Paul Millsap (+3.63) [36] ✓ | Dirk Nowitzki (+3.81) [13] ✓ |
| 19 | **Jimmy Butler** (+3.90) | LaMarcus Aldridge (+3.31) [29] ✓ | Derek Fisher (+3.56) [50] ✓ | DeMarcus Cousins (+3.60) [10] ✓ |
| 20 | **Isaiah Thomas** (+3.90) | Paul Millsap (+3.31) [36] ✓ | Jimmy Butler (+3.36) [19] ✓ | Patty Mills (+3.50) [12] ✓ |
| 21 | **Carmelo Anthony** (+3.80) | Russell Westbrook (+3.28) [25] ✓ | Anthony Davis (+3.30) [28] ✓ | Jimmy Butler (+3.31) [19] ✓ |
| 22 | **Kemba Walker** (+3.80) | Mike Conley (+3.27) [21] ✓ | Mike Conley (+3.29) [21] ✓ | Carmelo Anthony (+3.28) [21] ✓ |
| 23 | **Mike Conley** (+3.80) | Jimmy Butler (+3.22) [19] ✓ | Anderson Varejao (+3.27) [16] ✓ | DeAndre Jordan (+3.19) [62] ✓ |
| 24 | **Andre Iguodala** (+3.80) | Goran Dragic (+3.20) [10] ✓ | Carmelo Anthony (+3.26) [21] ✓ | LaMarcus Aldridge (+3.12) [29] ✓ |
| 25 | **Ricky Rubio** (+3.70) | Anthony Davis (+3.09) [28] ✓ | Ricky Rubio (+3.25) [25] ✓ | Mike Conley (+3.12) [21] ✓ |
| 26 | **Eric Bledsoe** (+3.70) | Derek Fisher (+3.01) [50] ✓ | LaMarcus Aldridge (+3.13) [29] ✓ | Trevor Ariza (+3.03) [66] ✓ |
| 27 | **Russell Westbrook** (+3.70) | Pablo Prigioni (+2.97) [70] ✓ | Wesley Matthews (+3.01) [46] ✓ | Dwyane Wade (+3.01) [83] ✓ |
| 28 | **Anthony Davis** (+3.50) | Iman Shumpert (+2.93) [97] ✓ | Draymond Green (+3.00) [29] ✓ | Al Jefferson (+2.99) [78] ✓ |
| 29 | **LaMarcus Aldridge** (+3.40) | Trevor Ariza (+2.79) [66] ✓ | Russell Westbrook (+2.93) [25] ✓ | Tony Allen (+2.97) [46] ✓ |
| 30 | **Draymond Green** (+3.40) | Kemba Walker (+2.79) [21] ✓ | DeMarcus Cousins (+2.90) [10] ✓ | Paul Millsap (+2.91) [36] ✓ |
| 31 | **DeMarre Carroll** (+3.30) | Nicolas Batum (+2.77) [59] ✓ | George Hill (+2.81) [48] ✓ | Chris Bosh (+2.91) [93] ✓ |
| 32 | **Nikola Pekovic** (+3.30) | Ricky Rubio (+2.76) [25] ✓ | Deron Williams (+2.79) [34] ✓ | Deron Williams (+2.86) [34] ✓ |
| 33 | **Tiago Splitter** (+3.30) | Patty Mills (+2.64) [12] ✓ | Mario Chalmers (+2.79) [18] ✓ | David West (+2.83) [51] ✓ |
| 34 | **Blake Griffin** (+3.20) | Deron Williams (+2.64) [34] ✓ | Kemba Walker (+2.62) [21] ✓ | Anderson Varejao (+2.81) [16] ✓ |
| 35 | **Deron Williams** (+3.20) | DeAndre Jordan (+2.54) [62] ✓ | David Lee (+2.59) [70] ✓ | Andre Drummond (+2.77) [74] ✓ |
| 36 | **Paul Millsap** (+3.10) | Paul Pierce (+2.53) [59] ✓ | Marcin Gortat (+2.57) [57] ✓ | Andrew Bogut (+2.74) [36] ✓ |
| 37 | **Andrew Bogut** (+3.10) | Wesley Matthews (+2.45) [46] ✓ | Joakim Noah (+2.52) [6] ✓ | John Wall (+2.73) [66] ✓ |
| 38 | **Kris Humphries** (+3.00) | Chris Bosh (+2.40) [93] ✓ | Dwight Howard (+2.49) [57] ✓ | Isaiah Thomas (+2.68) [19] ✓ |
| 39 | **Klay Thompson** (+2.90) | Mario Chalmers (+2.39) [18] ✓ | Nicolas Batum (+2.48) [59] ✓ | Danny Green (+2.63) [13] ✓ |
| 40 | **Robin Lopez** (+2.90) | Eric Bledsoe (+2.36) [25] ✓ | Danny Green (+2.47) [13] ✓ | Wesley Matthews (+2.56) [46] ✓ |
| 41 | **Ty Lawson** (+2.90) | Isaiah Thomas (+2.29) [19] ✓ | Chris Bosh (+2.47) [93] ✓ | Corey Brewer (+2.50) [89] ✓ |
| 42 | **Vince Carter** (+2.90) | Robin Lopez (+2.29) [39] ✓ | Jae Crowder (+2.40) [39] ✓ | Nicolas Batum (+2.49) [59] ✓ |
| 43 | **Jae Crowder** (+2.90) | Amir Johnson (+2.28) [89] ✓ | Paul Pierce (+2.38) [59] ✓ | DeMarre Carroll (+2.43) [31] ✓ |
| 44 | **Darren Collison** (+2.70) | DeMarre Carroll (+2.24) [31] ✓ | Trevor Ariza (+2.33) [66] ✓ | Chandler Parsons (+2.38) [62] ✓ |
| 45 | **Shane Battier** (+2.70) | Dwight Howard (+2.19) [57] ✓ | Patrick Beverley (+2.28) [16] ✓ | Klay Thompson (+2.36) [39] ✓ |
| 46 | **Wesley Matthews** (+2.60) | Tony Allen (+2.19) [46] ✓ | Isaiah Thomas (+2.25) [19] ✓ | Dwight Howard (+2.35) [57] ✓ |
| 47 | **Tony Allen** (+2.60) | Danny Green (+2.16) [13] ✓ | Channing Frye (+2.25) [49] ✓ | Ty Lawson (+2.35) [39] ✓ |
| 48 | **George Hill** (+2.50) | Marcin Gortat (+2.12) [57] ✓ | DeAndre Jordan (+2.23) [62] ✓ | DeMar DeRozan (+2.26) [107] ✗ |
| 49 | **Channing Frye** (+2.40) | Channing Frye (+2.05) [49] ✓ | Al Jefferson (+2.14) [78] ✓ | Tim Duncan (+2.20) [66] ✓ |
| 50 | **Derek Fisher** (+2.30) | Jae Crowder (+2.03) [39] ✓ | Iman Shumpert (+2.14) [97] ✓ | Eric Bledsoe (+2.17) [25] ✓ |
| 51 | **David West** (+2.20) | Nick Collison (+2.01) [83] ✓ | Kyle Korver (+2.06) [78] ✓ | Damian Lillard (+2.15) [53] ✓ |
| 52 | **Jrue Holiday** (+2.20) | Joakim Noah (+2.00) [6] ✓ | David West (+2.00) [51] ✓ | David Lee (+2.09) [70] ✓ |
| 53 | **Damian Lillard** (+2.10) | Kyle Korver (+1.99) [78] ✓ | Eric Bledsoe (+2.00) [25] ✓ | Nikola Pekovic (+2.08) [31] ✓ |
| 54 | **Michael KiddGilchrist** (+2.10) | Vince Carter (+1.98) [39] ✓ | Dwyane Wade (+1.95) [83] ✓ | Robin Lopez (+2.04) [39] ✓ |
| 55 | **Chris Andersen** (+2.10) | Patrick Beverley (+1.96) [16] ✓ | Chris Andersen (+1.94) [53] ✓ | Serge Ibaka (+1.97) [59] ✓ |
| 56 | **CJ Watson** (+2.10) | George Hill (+1.96) [48] ✓ | Nick Collison (+1.91) [83] ✓ | George Hill (+1.95) [48] ✓ |
| 57 | **Marcin Gortat** (+2.00) | John Wall (+1.94) [66] ✓ | Anthony Tolliver (+1.86) [118] ✗ | Kyle Korver (+1.94) [78] ✓ |
| 58 | **Dwight Howard** (+2.00) | Dwyane Wade (+1.89) [83] ✓ | Robin Lopez (+1.85) [39] ✓ | Terrence Jones (+1.72) [152] ✗ |
| 59 | **Nicolas Batum** (+1.90) | Nene (+1.81) [70] ✓ | John Wall (+1.76) [66] ✓ | Jae Crowder (+1.68) [39] ✓ |
| 60 | **Serge Ibaka** (+1.90) | Al Jefferson (+1.80) [78] ✓ | Tiago Splitter (+1.73) [31] ✓ | Lance Stephenson (+1.66) [135] ✗ |
| 61 | **Paul Pierce** (+1.90) | Andray Blatche (+1.77) [118] ✗ | Klay Thompson (+1.73) [39] ✓ | Marc Gasol (+1.66) [62] ✓ |
| 62 | **DeAndre Jordan** (+1.80) | Shaun Livingston (+1.71) [87] ✓ | DeMarre Carroll (+1.72) [31] ✓ | Draymond Green (+1.65) [29] ✓ |
| 63 | **Chandler Parsons** (+1.80) | Chris Andersen (+1.70) [53] ✓ | Vince Carter (+1.70) [39] ✓ | Marcin Gortat (+1.63) [57] ✓ |
| 64 | **Roy Hibbert** (+1.80) | Kirk Hinrich (+1.70) [107] ✗ | Tim Duncan (+1.68) [66] ✓ | Paul Pierce (+1.60) [59] ✓ |
| 65 | **Marc Gasol** (+1.80) | David West (+1.58) [51] ✓ | Andray Blatche (+1.61) [118] ✗ | Kemba Walker (+1.58) [21] ✓ |
| 66 | **John Wall** (+1.70) | Tim Duncan (+1.48) [66] ✓ | Amir Johnson (+1.54) [89] ✓ | Marco Belinelli (+1.57) [74] ✓ |
| 67 | **Trevor Ariza** (+1.70) | Greg Monroe (+1.47) [125] ✗ | Nene (+1.48) [70] ✓ | CJ Watson (+1.54) [53] ✓ |
| 68 | **PJ Tucker** (+1.70) | Anthony Tolliver (+1.40) [118] ✗ | Shaun Livingston (+1.44) [87] ✓ | Chris Andersen (+1.50) [53] ✓ |
| 69 | **Tim Duncan** (+1.70) | Tiago Splitter (+1.39) [31] ✓ | Rudy Gay (+1.42) [100] ✗ | Patrick Beverley (+1.46) [16] ✓ |
| 70 | **David Lee** (+1.60) | CJ Watson (+1.37) [53] ✓ | Brandan Wright (+1.37) [115] ✗ | Josh McRoberts (+1.46) [89] ✓ |
| 71 | **Courtney Lee** (+1.60) | Terrence Jones (+1.32) [152] ✗ | CJ Watson (+1.33) [53] ✓ | Kyrie Irving (+1.43) [83] ✓ |
| 72 | **Nene** (+1.60) | Nikola Pekovic (+1.25) [31] ✓ | Greg Monroe (+1.30) [125] ✗ | Tiago Splitter (+1.43) [31] ✓ |
| 73 | **Pablo Prigioni** (+1.60) | Brandan Wright (+1.25) [115] ✗ | Patrick Patterson (+1.24) [107] ✗ | Pablo Prigioni (+1.43) [70] ✓ |
| 74 | **Andre Drummond** (+1.50) | DJ Augustin (+1.21) [131] ✗ | Tony Allen (+1.24) [46] ✓ | Derek Fisher (+1.36) [50] ✓ |
| 75 | **Jared Sullinger** (+1.50) | Andre Drummond (+1.18) [74] ✓ | Nikola Pekovic (+1.22) [31] ✓ | Patrick Patterson (+1.33) [107] ✗ |
| 76 | **Marco Belinelli** (+1.50) | Jeremy Lamb (+1.13) [102] ✗ | Josh McRoberts (+1.21) [89] ✓ | Mario Chalmers (+1.32) [18] ✓ |
| 77 | **Matthew Dellavedova** (+1.50) | PJ Tucker (+1.09) [66] ✓ | Kirk Hinrich (+1.20) [107] ✗ | Jrue Holiday (+1.24) [51] ✓ |
| 78 | **Al Jefferson** (+1.30) | Shane Battier (+1.07) [44] ✓ | Kyrie Irving (+1.17) [83] ✓ | Mason Plumlee (+1.23) [198] ✗ |
| 79 | **Kyle Korver** (+1.30) | Josh McRoberts (+1.06) [89] ✓ | Shane Battier (+1.13) [44] ✓ | Courtney Lee (+1.23) [70] ✓ |
| 80 | **Reggie Jackson** (+1.30) | DeMar DeRozan (+1.01) [107] ✗ | Chandler Parsons (+1.10) [62] ✓ | PJ Tucker (+1.08) [66] ✓ |
| 81 | **Jeremy Lin** (+1.30) | Taj Gibson (+0.99) [149] ✗ | Jrue Holiday (+1.09) [51] ✓ | Jeremy Lamb (+1.03) [102] ✗ |
| 82 | **Jeremy Evans** (+1.30) | Jrue Holiday (+0.98) [51] ✓ | Terrence Jones (+1.06) [152] ✗ | Amir Johnson (+1.02) [89] ✓ |
| 83 | **Kyrie Irving** (+1.20) | Patrick Patterson (+0.97) [107] ✗ | Josh Smith (+1.04) [169] ✗ | Jamal Crawford (+0.95) [115] ✗ |
| 84 | **Dwyane Wade** (+1.20) | Corey Brewer (+0.97) [89] ✓ | Matthew Dellavedova (+1.01) [74] ✓ | Kevin Martin (+0.94) [181] ✗ |
| 85 | **Nick Collison** (+1.20) | David Lee (+0.95) [70] ✓ | Jeremy Lamb (+1.01) [102] ✗ | Tony Parker (+0.92) [131] ✗ |
| 86 | **Nate Wolters** (+1.20) | Reggie Jackson (+0.93) [78] ✓ | DJ Augustin (+0.95) [131] ✗ | Rudy Gay (+0.90) [100] ✗ |
| 87 | **Shaun Livingston** (+1.10) | Monta Ellis (+0.89) [118] ✗ | Boris Diaw (+0.93) [89] ✓ | Mike Dunleavy (+0.88) [107] ✗ |
| 88 | **Nick Calathes** (+1.10) | Mike Dunleavy (+0.83) [107] ✗ | Mike Dunleavy (+0.92) [107] ✗ | Nene (+0.87) [70] ✓ |
| 89 | **Corey Brewer** (+1.00) | Kenneth Faried (+0.81) [170] ✗ | Corey Brewer (+0.85) [89] ✓ | Kenneth Faried (+0.85) [170] ✗ |
| 90 | **Josh McRoberts** (+1.00) | Boris Diaw (+0.70) [89] ✓ | Reggie Jackson (+0.80) [78] ✓ | Andray Blatche (+0.82) [118] ✗ |
| 91 | **Amir Johnson** (+1.00) | Chandler Parsons (+0.67) [62] ✓ | PJ Tucker (+0.79) [66] ✓ | Darren Collison (+0.79) [44] ✓ |
| 92 | **Boris Diaw** (+1.00) | Klay Thompson (+0.65) [39] ✓ | Terrence Ross (+0.70) [107] ✗ | DJ Augustin (+0.78) [131] ✗ |
| 93 | **Chris Bosh** (+0.90) | Thabo Sefolosha (+0.60) [102] ✗ | Andre Drummond (+0.66) [74] ✓ | Monta Ellis (+0.73) [118] ✗ |
| 94 | **Luol Deng** (+0.90) | Giannis Antetokounmpo (+0.59) [161] ✗ | Giannis Antetokounmpo (+0.61) [161] ✗ | Markieff Morris (+0.73) [152] ✗ |
| 95 | **Nick Young** (+0.90) | Matthew Dellavedova (+0.54) [74] ✓ | Jared Sullinger (+0.59) [74] ✓ | Nikola Vucevic (+0.73) [122] ✗ |
| 96 | **Omri Casspi** (+0.80) | Jeff Teague (+0.54) [122] ✗ | Taj Gibson (+0.58) [149] ✗ | Luol Deng (+0.70) [93] ✓ |
| 97 | **Bradley Beal** (+0.70) | Marc Gasol (+0.53) [62] ✓ | DeMar DeRozan (+0.51) [107] ✗ | Thabo Sefolosha (+0.69) [102] ✗ |
| 98 | **Randy Foye** (+0.70) | Luol Deng (+0.51) [93] ✓ | Kenneth Faried (+0.51) [170] ✗ | Tyson Chandler (+0.66) [140] ✗ |
| 99 | **Iman Shumpert** (+0.70) | Victor Oladipo (+0.51) [152] ✗ | Victor Oladipo (+0.48) [152] ✗ | Boris Diaw (+0.65) [89] ✓ |
| 100 | **Gordon Hayward** (+0.60) | Jared Sullinger (+0.50) [74] ✓ | Jonas Valanciunas (+0.47) [140] ✗ | Nick Calathes (+0.65) [87] ✓ |

### 2014-15 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | LeBron James (+6.15) [11] ✓ | Chris Paul (+6.93) [1] ✓ | Chris Paul (+8.27) [1] ✓ |
| 2 | **Kawhi Leonard** (+8.90) | James Harden (+6.09) [4] ✓ | Draymond Green (+6.17) [5] ✓ | Anthony Davis (+7.97) [3] ✓ |
| 3 | **Anthony Davis** (+8.80) | Chris Paul (+5.90) [1] ✓ | LeBron James (+5.96) [11] ✓ | Kawhi Leonard (+7.91) [2] ✓ |
| 4 | **James Harden** (+7.70) | Russell Westbrook (+5.86) [7] ✓ | James Harden (+5.77) [4] ✓ | LeBron James (+6.66) [11] ✓ |
| 5 | **Draymond Green** (+6.50) | Anthony Davis (+5.58) [3] ✓ | Anthony Davis (+5.64) [3] ✓ | James Harden (+6.55) [4] ✓ |
| 6 | **Danny Green** (+6.10) | Draymond Green (+5.41) [5] ✓ | Kawhi Leonard (+5.34) [2] ✓ | Russell Westbrook (+5.71) [7] ✓ |
| 7 | **Russell Westbrook** (+5.60) | Kawhi Leonard (+5.11) [2] ✓ | Damian Lillard (+4.95) [42] ✓ | Jimmy Butler (+5.58) [34] ✓ |
| 8 | **George Hill** (+5.60) | Damian Lillard (+5.00) [42] ✓ | Klay Thompson (+4.70) [10] ✓ | George Hill (+5.08) [7] ✓ |
| 9 | **DeMarcus Cousins** (+5.40) | DeMarcus Cousins (+4.89) [9] ✓ | Russell Westbrook (+4.69) [7] ✓ | Klay Thompson (+4.74) [10] ✓ |
| 10 | **Klay Thompson** (+5.30) | Jimmy Butler (+4.74) [34] ✓ | Jimmy Butler (+4.63) [34] ✓ | Tony Allen (+4.57) [16] ✓ |
| 11 | **LeBron James** (+5.10) | Klay Thompson (+3.95) [10] ✓ | George Hill (+4.39) [7] ✓ | Draymond Green (+4.55) [5] ✓ |
| 12 | **Khris Middleton** (+4.80) | Rudy Gobert (+3.95) [19] ✓ | Rudy Gobert (+4.17) [19] ✓ | DeAndre Jordan (+4.31) [13] ✓ |
| 13 | **Kyrie Irving** (+4.60) | Lou Williams (+3.90) [34] ✓ | Andrew Bogut (+4.02) [23] ✓ | Danny Green (+4.23) [6] ✓ |
| 14 | **DeAndre Jordan** (+4.60) | LaMarcus Aldridge (+3.86) [16] ✓ | Lou Williams (+4.00) [34] ✓ | Blake Griffin (+4.02) [60] ✓ |
| 15 | **Kyle Korver** (+4.60) | Iman Shumpert (+3.77) [66] ✓ | Kyrie Irving (+3.79) [13] ✓ | Paul Millsap (+4.01) [27] ✓ |
| 16 | **LaMarcus Aldridge** (+4.30) | Gordon Hayward (+3.71) [26] ✓ | LaMarcus Aldridge (+3.64) [16] ✓ | Kyrie Irving (+4.00) [13] ✓ |
| 17 | **Tony Allen** (+4.30) | George Hill (+3.57) [7] ✓ | Khris Middleton (+3.63) [12] ✓ | Khris Middleton (+3.91) [12] ✓ |
| 18 | **Nikola Mirotic** (+4.20) | Kyrie Irving (+3.46) [13] ✓ | Danny Green (+3.57) [6] ✓ | John Wall (+3.82) [60] ✓ |
| 19 | **Rudy Gobert** (+4.10) | Kyle Lowry (+3.25) [22] ✓ | Pau Gasol (+3.37) [100] ✓ | Jeff Teague (+3.76) [42] ✓ |
| 20 | **Marc Gasol** (+4.00) | Blake Griffin (+3.23) [60] ✓ | Wesley Matthews (+3.27) [24] ✓ | Tim Duncan (+3.72) [27] ✓ |
| 21 | **Darren Collison** (+4.00) | Danny Green (+3.22) [6] ✓ | Blake Griffin (+3.20) [60] ✓ | Wesley Matthews (+3.64) [24] ✓ |
| 22 | **Kyle Lowry** (+3.90) | Paul Millsap (+3.20) [27] ✓ | Manu Ginobili (+3.16) [29] ✓ | Lou Williams (+3.53) [34] ✓ |
| 23 | **Andrew Bogut** (+3.70) | Khris Middleton (+3.19) [12] ✓ | Kyle Lowry (+3.09) [22] ✓ | Gordon Hayward (+3.29) [26] ✓ |
| 24 | **Wesley Matthews** (+3.60) | Greg Monroe (+3.14) [124] ✗ | Tony Allen (+3.09) [16] ✓ | Al Horford (+3.23) [60] ✓ |
| 25 | **Jonas Jerebko** (+3.60) | Andrew Bogut (+3.10) [23] ✓ | Zach Randolph (+3.03) [37] ✓ | DeMarcus Cousins (+3.15) [9] ✓ |
| 26 | **Gordon Hayward** (+3.40) | Tony Allen (+3.01) [16] ✓ | Isaiah Thomas (+3.02) [71] ✓ | Rudy Gobert (+3.08) [19] ✓ |
| 27 | **Paul Millsap** (+3.30) | Wesley Matthews (+2.92) [24] ✓ | Danilo Gallinari (+2.97) [34] ✓ | Damian Lillard (+3.06) [42] ✓ |
| 28 | **Tim Duncan** (+3.30) | Manu Ginobili (+2.81) [29] ✓ | DeMarcus Cousins (+2.94) [9] ✓ | LaMarcus Aldridge (+2.97) [16] ✓ |
| 29 | **Kevin Love** (+3.20) | Nikola Mirotic (+2.81) [18] ✓ | Greg Monroe (+2.92) [124] ✗ | Brandan Wright (+2.85) [69] ✓ |
| 30 | **Marcin Gortat** (+3.20) | Pau Gasol (+2.75) [100] ✓ | Mike Conley (+2.91) [37] ✓ | Kyle Korver (+2.83) [13] ✓ |
| 31 | **JJ Redick** (+3.20) | Kyle Korver (+2.72) [13] ✓ | Kyle Korver (+2.79) [13] ✓ | Marc Gasol (+2.80) [20] ✓ |
| 32 | **Manu Ginobili** (+3.20) | Jeff Teague (+2.61) [42] ✓ | Iman Shumpert (+2.73) [66] ✓ | Jrue Holiday (+2.73) [42] ✓ |
| 33 | **Brandon Jennings** (+3.20) | Isaiah Thomas (+2.53) [71] ✓ | Paul Millsap (+2.70) [27] ✓ | Darren Collison (+2.52) [20] ✓ |
| 34 | **Jimmy Butler** (+3.00) | DeAndre Jordan (+2.51) [13] ✓ | Gordon Hayward (+2.67) [26] ✓ | DeMarre Carroll (+2.51) [37] ✓ |
| 35 | **Lou Williams** (+3.00) | Danilo Gallinari (+2.44) [34] ✓ | Nikola Mirotic (+2.66) [18] ✓ | Derrick Favors (+2.50) [53] ✓ |
| 36 | **Danilo Gallinari** (+3.00) | John Wall (+2.38) [60] ✓ | Patrick Patterson (+2.57) [71] ✓ | James Johnson (+2.50) [60] ✓ |
| 37 | **Eric Bledsoe** (+2.90) | Jrue Holiday (+2.35) [42] ✓ | Jeff Teague (+2.35) [42] ✓ | Kyle Lowry (+2.47) [22] ✓ |
| 38 | **Zach Randolph** (+2.90) | Kevin Love (+2.25) [29] ✓ | Jared Dudley (+2.26) [66] ✓ | Marcin Gortat (+2.44) [29] ✓ |
| 39 | **Mike Conley** (+2.90) | Mike Conley (+2.22) [37] ✓ | Jrue Holiday (+2.24) [42] ✓ | Anthony Morrow (+2.43) [42] ✓ |
| 40 | **DeMarre Carroll** (+2.90) | Jared Dudley (+2.21) [66] ✓ | Jonas Jerebko (+2.22) [24] ✓ | Ty Lawson (+2.40) [56] ✓ |
| 41 | **Kelly Olynyk** (+2.80) | Eric Bledsoe (+2.17) [37] ✓ | John Wall (+2.18) [60] ✓ | Kevin Love (+2.34) [29] ✓ |
| 42 | **Damian Lillard** (+2.70) | Tyson Chandler (+2.16) [47] ✓ | Jae Crowder (+2.15) [56] ✓ | Tyson Chandler (+2.28) [47] ✓ |
| 43 | **Jeff Teague** (+2.70) | Jae Crowder (+2.11) [56] ✓ | Kevin Love (+2.12) [29] ✓ | Mike Conley (+2.25) [37] ✓ |
| 44 | **Anthony Morrow** (+2.70) | Marc Gasol (+2.11) [20] ✓ | Marc Gasol (+2.10) [20] ✓ | Pau Gasol (+2.23) [100] ✓ |
| 45 | **Zaza Pachulia** (+2.70) | Patrick Patterson (+2.11) [71] ✓ | Ersan Ilyasova (+2.04) [49] ✓ | Manu Ginobili (+2.08) [29] ✓ |
| 46 | **Jrue Holiday** (+2.70) | Zach Randolph (+2.07) [37] ✓ | Kelly Olynyk (+2.02) [41] ✓ | Carmelo Anthony (+2.06) [81] ✓ |
| 47 | **Tyson Chandler** (+2.60) | Devin Harris (+2.06) [49] ✓ | Derrick Favors (+1.99) [53] ✓ | Trevor Ariza (+2.00) [116] ✗ |
| 48 | **Serge Ibaka** (+2.60) | Jonas Jerebko (+2.05) [24] ✓ | Marcus Smart (+1.97) [56] ✓ | JJ Redick (+1.99) [29] ✓ |
| 49 | **Devin Harris** (+2.50) | Marcus Smart (+1.97) [56] ✓ | Dirk Nowitzki (+1.94) [96] ✓ | Zach Randolph (+1.99) [37] ✓ |
| 50 | **Ersan Ilyasova** (+2.50) | Marcin Gortat (+1.94) [29] ✓ | DeAndre Jordan (+1.92) [13] ✓ | Nikola Mirotic (+1.98) [18] ✓ |
| 51 | **Rudy Gay** (+2.40) | Nene (+1.90) [109] ✗ | Tyreke Evans (+1.90) [87] ✓ | Nicolas Batum (+1.97) [116] ✗ |
| 52 | **Kemba Walker** (+2.40) | AlFarouq Aminu (+1.85) [56] ✓ | Nene (+1.89) [109] ✗ | Andrew Bogut (+1.96) [23] ✓ |
| 53 | **Derrick Favors** (+2.30) | James Johnson (+1.81) [60] ✓ | Eric Bledsoe (+1.86) [37] ✓ | Chandler Parsons (+1.93) [68] ✓ |
| 54 | **Joakim Noah** (+2.20) | Aaron Brooks (+1.77) [162] ✗ | Tyson Chandler (+1.83) [47] ✓ | Rudy Gay (+1.90) [51] ✓ |
| 55 | **Andre Roberson** (+2.20) | Tim Duncan (+1.76) [27] ✓ | Andre Iguodala (+1.80) [81] ✓ | Greg Monroe (+1.83) [124] ✗ |
| 56 | **Ty Lawson** (+2.10) | Zaza Pachulia (+1.73) [42] ✓ | Reggie Jackson (+1.76) [81] ✓ | Danilo Gallinari (+1.82) [34] ✓ |
| 57 | **Marcus Smart** (+2.10) | Ersan Ilyasova (+1.73) [49] ✓ | Tim Duncan (+1.74) [27] ✓ | Monta Ellis (+1.82) [78] ✓ |
| 58 | **Jae Crowder** (+2.10) | Andre Iguodala (+1.68) [81] ✓ | James Johnson (+1.68) [60] ✓ | Brandon Jennings (+1.81) [29] ✓ |
| 59 | **AlFarouq Aminu** (+2.10) | Kelly Olynyk (+1.59) [41] ✓ | Monta Ellis (+1.66) [78] ✓ | CJ Miles (+1.79) [71] ✓ |
| 60 | **John Wall** (+2.00) | Tyreke Evans (+1.54) [87] ✓ | Aaron Brooks (+1.55) [162] ✗ | Andre Iguodala (+1.78) [81] ✓ |
| 61 | **Blake Griffin** (+2.00) | Derrick Favors (+1.54) [53] ✓ | Matt Barnes (+1.54) [71] ✓ | Isaiah Thomas (+1.77) [71] ✓ |
| 62 | **Al Horford** (+2.00) | Amir Johnson (+1.51) [100] ✗ | Anthony Morrow (+1.54) [42] ✓ | Patrick Patterson (+1.75) [71] ✓ |
| 63 | **Michael KiddGilchrist** (+2.00) | Monta Ellis (+1.46) [78] ✓ | Timofey Mozgov (+1.35) [78] ✓ | Kelly Olynyk (+1.73) [41] ✓ |
| 64 | **Cody Zeller** (+2.00) | Andre Roberson (+1.45) [54] ✓ | Amir Johnson (+1.32) [100] ✗ | AlFarouq Aminu (+1.73) [56] ✓ |
| 65 | **James Johnson** (+2.00) | Trevor Ariza (+1.38) [116] ✗ | Harrison Barnes (+1.31) [142] ✗ | Jonas Jerebko (+1.63) [24] ✓ |
| 66 | **Jared Dudley** (+1.90) | Ed Davis (+1.35) [71] ✓ | Dwight Howard (+1.30) [96] ✓ | Jae Crowder (+1.62) [56] ✓ |
| 67 | **Iman Shumpert** (+1.90) | Matt Barnes (+1.34) [71] ✓ | Zaza Pachulia (+1.29) [42] ✓ | Tyreke Evans (+1.54) [87] ✓ |
| 68 | **Chandler Parsons** (+1.80) | Nicolas Batum (+1.31) [116] ✗ | Marcin Gortat (+1.26) [29] ✓ | Bradley Beal (+1.44) [81] ✓ |
| 69 | **Luol Deng** (+1.70) | Reggie Jackson (+1.31) [81] ✓ | Rudy Gay (+1.26) [51] ✓ | Eric Bledsoe (+1.42) [37] ✓ |
| 70 | **Brandan Wright** (+1.70) | Bradley Beal (+1.30) [81] ✓ | AlFarouq Aminu (+1.22) [56] ✓ | Paul Pierce (+1.41) [87] ✓ |
| 71 | **Matt Barnes** (+1.60) | Harrison Barnes (+1.29) [142] ✗ | Devin Harris (+1.16) [49] ✓ | Zaza Pachulia (+1.39) [42] ✓ |
| 72 | **Patrick Patterson** (+1.60) | Derrick Rose (+1.25) [94] ✓ | Cory Joseph (+1.15) [87] ✓ | Michael KiddGilchrist (+1.34) [60] ✓ |
| 73 | **CJ Miles** (+1.60) | Darren Collison (+1.21) [20] ✓ | Donatas Motiejunas (+1.14) [124] ✗ | Jared Dudley (+1.30) [66] ✓ |
| 74 | **Ed Davis** (+1.60) | Timofey Mozgov (+1.14) [78] ✓ | Jared Sullinger (+1.13) [92] ✓ | Brook Lopez (+1.29) [96] ✓ |
| 75 | **Isaiah Thomas** (+1.60) | JJ Redick (+1.13) [29] ✓ | Brandon Jennings (+1.07) [29] ✓ | Goran Dragic (+1.25) [116] ✗ |
| 76 | **JJ Barea** (+1.60) | DeMarre Carroll (+1.12) [37] ✓ | Carmelo Anthony (+1.05) [81] ✓ | Dirk Nowitzki (+1.24) [96] ✓ |
| 77 | **Pablo Prigioni** (+1.60) | Cory Joseph (+1.07) [87] ✓ | Darren Collison (+1.05) [20] ✓ | CJ Watson (+1.24) [81] ✓ |
| 78 | **Monta Ellis** (+1.40) | Goran Dragic (+1.07) [116] ✗ | Goran Dragic (+1.02) [116] ✗ | Robert Covington (+1.24) [94] ✓ |
| 79 | **Timofey Mozgov** (+1.40) | Al Horford (+1.04) [60] ✓ | DeMarre Carroll (+1.01) [37] ✓ | Andre Drummond (+1.22) [139] ✗ |
| 80 | **Jeremy Lin** (+1.40) | Donatas Motiejunas (+1.02) [124] ✗ | Al Horford (+0.98) [60] ✓ | Serge Ibaka (+1.21) [47] ✓ |
| 81 | **Reggie Jackson** (+1.30) | Joakim Noah (+0.97) [54] ✓ | Josh Smith (+0.98) [123] ✗ | Ed Davis (+1.18) [71] ✓ |
| 82 | **Bradley Beal** (+1.30) | Chandler Parsons (+0.96) [68] ✓ | Chandler Parsons (+0.92) [68] ✓ | Dwight Howard (+1.17) [96] ✓ |
| 83 | **Andre Iguodala** (+1.30) | Brandon Jennings (+0.95) [29] ✓ | Trevor Booker (+0.92) [105] ✗ | Al Jefferson (+1.14) [189] ✗ |
| 84 | **Carmelo Anthony** (+1.30) | Victor Oladipo (+0.94) [147] ✗ | JJ Redick (+0.91) [29] ✓ | Harrison Barnes (+1.13) [142] ✗ |
| 85 | **CJ Watson** (+1.30) | Luis Scola (+0.92) [105] ✗ | Victor Oladipo (+0.89) [147] ✗ | Kemba Walker (+1.10) [51] ✓ |
| 86 | **Amare Stoudemire** (+1.30) | Dwight Howard (+0.91) [96] ✓ | Bradley Beal (+0.87) [81] ✓ | Matt Barnes (+1.09) [71] ✓ |
| 87 | **Tyreke Evans** (+1.20) | JJ Barea (+0.90) [71] ✓ | Brandan Wright (+0.85) [69] ✓ | Thaddeus Young (+0.99) [96] ✓ |
| 88 | **Deron Williams** (+1.20) | Josh Smith (+0.88) [123] ✗ | Thaddeus Young (+0.84) [96] ✓ | Mike Dunleavy (+0.95) [116] ✗ |
| 89 | **Paul Pierce** (+1.20) | Anthony Tolliver (+0.88) [154] ✗ | Kemba Walker (+0.83) [51] ✓ | Dwyane Wade (+0.94) [144] ✗ |
| 90 | **Patrick Beverley** (+1.20) | Dirk Nowitzki (+0.88) [96] ✓ | Kenneth Faried (+0.80) [111] ✗ | Luol Deng (+0.94) [69] ✓ |
| 91 | **Cory Joseph** (+1.20) | Jared Sullinger (+0.87) [92] ✓ | Deron Williams (+0.76) [87] ✓ | Pablo Prigioni (+0.87) [71] ✓ |
| 92 | **Jonas Valanciunas** (+1.10) | Anthony Morrow (+0.86) [42] ✓ | Mike Dunleavy (+0.76) [116] ✗ | Devin Harris (+0.82) [49] ✓ |
| 93 | **Jared Sullinger** (+1.10) | Mike Dunleavy (+0.85) [116] ✗ | CJ Watson (+0.75) [81] ✓ | Kenneth Faried (+0.80) [111] ✗ |
| 94 | **Robert Covington** (+1.00) | Giannis Antetokounmpo (+0.82) [159] ✗ | Paul Pierce (+0.75) [87] ✓ | Joakim Noah (+0.79) [54] ✓ |
| 95 | **Derrick Rose** (+1.00) | Thaddeus Young (+0.80) [96] ✓ | Andre Drummond (+0.72) [139] ✗ | Markieff Morris (+0.78) [135] ✗ |
| 96 | **Thaddeus Young** (+0.90) | Andre Drummond (+0.76) [139] ✗ | Andre Roberson (+0.72) [54] ✓ | David West (+0.77) [159] ✗ |
| 97 | **Dirk Nowitzki** (+0.90) | Paul Pierce (+0.71) [87] ✓ | Ed Davis (+0.66) [71] ✓ | DeMar DeRozan (+0.77) [167] ✗ |
| 98 | **Brook Lopez** (+0.90) | Gerald Green (+0.66) [124] ✗ | Robert Covington (+0.66) [94] ✓ | PJ Tucker (+0.71) [100] ✗ |
| 99 | **Dwight Howard** (+0.90) | PJ Tucker (+0.58) [100] ✗ | Anthony Tolliver (+0.60) [154] ✗ | Iman Shumpert (+0.69) [66] ✓ |
| 100 | **Pau Gasol** (+0.80) | Trevor Booker (+0.55) [105] ✗ | Pablo Prigioni (+0.57) [71] ✓ | Luis Scola (+0.68) [105] ✗ |

### 2013-14 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Kevin Durant** (+7.60) | Kevin Durant (+6.73) [1] ✓ | Kevin Durant (+7.41) [1] ✓ |
| 2 | **Chris Paul** (+7.10) | LeBron James (+6.35) [4] ✓ | LeBron James (+6.80) [4] ✓ |
| 3 | **James Harden** (+6.30) | Chris Paul (+5.76) [2] ✓ | Chris Paul (+6.79) [2] ✓ |
| 4 | **LeBron James** (+5.80) | James Harden (+5.45) [3] ✓ | James Harden (+5.29) [3] ✓ |
| 5 | **Kevin Love** (+5.70) | Kevin Love (+4.97) [5] ✓ | Goran Dragic (+4.87) [6] ✓ |
| 6 | **Goran Dragic** (+4.80) | Goran Dragic (+4.44) [6] ✓ | Kevin Love (+4.76) [5] ✓ |
| 7 | **Kyle Lowry** (+4.40) | Damian Lillard (+4.31) [12] ✓ | Dirk Nowitzki (+4.33) [7] ✓ |
| 8 | **Dirk Nowitzki** (+4.40) | Kyle Lowry (+3.76) [7] ✓ | Brandan Wright (+4.19) [40] ✓ |
| 9 | **Carmelo Anthony** (+4.20) | Carmelo Anthony (+3.69) [9] ✓ | Carmelo Anthony (+3.80) [9] ✓ |
| 10 | **Manu Ginobili** (+4.00) | Russell Westbrook (+3.61) [15] ✓ | Kyle Lowry (+3.71) [7] ✓ |
| 11 | **Patty Mills** (+3.90) | Manu Ginobili (+3.35) [10] ✓ | Blake Griffin (+3.57) [17] ✓ |
| 12 | **Damian Lillard** (+3.60) | Isaiah Thomas (+3.00) [13] ✓ | Isaiah Thomas (+3.48) [13] ✓ |
| 13 | **Isaiah Thomas** (+3.50) | Dirk Nowitzki (+2.97) [7] ✓ | Manu Ginobili (+3.25) [10] ✓ |
| 14 | **Mike Conley** (+3.50) | Blake Griffin (+2.91) [17] ✓ | Russell Westbrook (+3.24) [15] ✓ |
| 15 | **Russell Westbrook** (+3.30) | Mike Conley (+2.91) [13] ✓ | Nikola Pekovic (+3.10) [64] ✓ |
| 16 | **Ty Lawson** (+3.20) | Paul George (+2.41) [22] ✓ | Damian Lillard (+3.03) [12] ✓ |
| 17 | **Blake Griffin** (+2.90) | Patty Mills (+2.40) [11] ✓ | Dwyane Wade (+2.82) [47] ✓ |
| 18 | **Wesley Matthews** (+2.80) | John Wall (+2.31) [36] ✓ | Ty Lawson (+2.80) [16] ✓ |
| 19 | **Marco Belinelli** (+2.80) | Ty Lawson (+2.25) [16] ✓ | Mike Conley (+2.80) [13] ✓ |
| 20 | **Jamal Crawford** (+2.80) | Kyrie Irving (+2.23) [27] ✓ | Wesley Matthews (+2.68) [18] ✓ |
| 21 | **Joe Johnson** (+2.70) | Pablo Prigioni (+2.18) [40] ✓ | Patty Mills (+2.59) [11] ✓ |
| 22 | **Paul George** (+2.60) | Deron Williams (+1.97) [22] ✓ | Anthony Davis (+2.52) [57] ✓ |
| 23 | **Chandler Parsons** (+2.60) | Ricky Rubio (+1.84) [36] ✓ | Kawhi Leonard (+2.51) [40] ✓ |
| 24 | **Deron Williams** (+2.60) | Jamal Crawford (+1.83) [18] ✓ | Chandler Parsons (+2.47) [22] ✓ |
| 25 | **Vince Carter** (+2.40) | Dwyane Wade (+1.72) [47] ✓ | Paul George (+2.46) [22] ✓ |
| 26 | **Nick Young** (+2.40) | DJ Augustin (+1.71) [31] ✓ | Deron Williams (+2.34) [22] ✓ |
| 27 | **Kyrie Irving** (+2.30) | Wesley Matthews (+1.63) [18] ✓ | DeMar DeRozan (+2.30) [40] ✓ |
| 28 | **Patrick Beverley** (+2.20) | Kevin Martin (+1.56) [83] ✓ | Jamal Crawford (+2.28) [18] ✓ |
| 29 | **Jrue Holiday** (+2.20) | DeMar DeRozan (+1.54) [40] ✓ | Tony Parker (+2.17) [71] ✓ |
| 30 | **Brandon Jennings** (+2.20) | Andre Iguodala (+1.54) [57] ✓ | DeMarcus Cousins (+2.15) [40] ✓ |
| 31 | **Klay Thompson** (+2.10) | Klay Thompson (+1.52) [31] ✓ | Chris Bosh (+1.99) [136] ✗ |
| 32 | **Randy Foye** (+2.10) | Jrue Holiday (+1.52) [28] ✓ | Marco Belinelli (+1.94) [18] ✓ |
| 33 | **DJ Augustin** (+2.10) | LaMarcus Aldridge (+1.50) [64] ✓ | Kyrie Irving (+1.93) [27] ✓ |
| 34 | **Channing Frye** (+2.00) | David Lee (+1.48) [112] ✗ | Andre Drummond (+1.91) [53] ✓ |
| 35 | **Josh McRoberts** (+2.00) | Jose Calderon (+1.44) [45] ✓ | Robin Lopez (+1.86) [68] ✓ |
| 36 | **Ricky Rubio** (+1.90) | Eric Bledsoe (+1.43) [47] ✓ | LaMarcus Aldridge (+1.86) [64] ✓ |
| 37 | **Nicolas Batum** (+1.90) | Marco Belinelli (+1.35) [18] ✓ | Andre Iguodala (+1.84) [57] ✓ |
| 38 | **John Wall** (+1.90) | Nikola Pekovic (+1.35) [64] ✓ | Nicolas Batum (+1.84) [36] ✓ |
| 39 | **Kyle Korver** (+1.90) | Rudy Gay (+1.31) [64] ✓ | Eric Bledsoe (+1.67) [47] ✓ |
| 40 | **Kawhi Leonard** (+1.70) | Randy Foye (+1.30) [31] ✓ | Kevin Martin (+1.64) [83] ✓ |
| 41 | **DeMarcus Cousins** (+1.70) | Brandan Wright (+1.26) [40] ✓ | Joe Johnson (+1.61) [21] ✓ |
| 42 | **DeMar DeRozan** (+1.70) | Vince Carter (+1.23) [25] ✓ | Trevor Ariza (+1.61) [68] ✓ |
| 43 | **Pablo Prigioni** (+1.70) | Mario Chalmers (+1.21) [47] ✓ | Jose Calderon (+1.61) [45] ✓ |
| 44 | **Brandan Wright** (+1.70) | Kyle Korver (+1.20) [36] ✓ | Al Jefferson (+1.59) [149] ✗ |
| 45 | **Jose Calderon** (+1.60) | Joe Johnson (+1.19) [21] ✓ | John Wall (+1.57) [36] ✓ |
| 46 | **Mirza Teletovic** (+1.60) | Brandon Jennings (+1.18) [28] ✓ | Nick Young (+1.55) [25] ✓ |
| 47 | **Joakim Noah** (+1.50) | Trevor Ariza (+1.17) [68] ✓ | Klay Thompson (+1.45) [31] ✓ |
| 48 | **Mario Chalmers** (+1.50) | Jeff Teague (+1.15) [90] ✓ | Tyreke Evans (+1.40) [57] ✓ |
| 49 | **Eric Bledsoe** (+1.50) | Kawhi Leonard (+1.13) [40] ✓ | David Lee (+1.33) [112] ✗ |
| 50 | **Dwyane Wade** (+1.50) | George Hill (+1.10) [112] ✗ | DJ Augustin (+1.28) [31] ✓ |
| 51 | **Kemba Walker** (+1.40) | Nick Young (+1.07) [25] ✓ | Arron Afflalo (+1.21) [57] ✓ |
| 52 | **Ray Allen** (+1.40) | Channing Frye (+1.06) [34] ✓ | Terrence Jones (+1.19) [71] ✓ |
| 53 | **Andre Drummond** (+1.30) | Anthony Morrow (+0.99) [53] ✓ | Joakim Noah (+1.17) [47] ✓ |
| 54 | **Zach Randolph** (+1.30) | Chandler Parsons (+0.99) [22] ✓ | Kyle Korver (+1.16) [36] ✓ |
| 55 | **Gerald Green** (+1.30) | DeMarcus Cousins (+0.97) [40] ✓ | Jodie Meeks (+1.14) [83] ✓ |
| 56 | **Anthony Morrow** (+1.30) | Jimmy Butler (+0.95) [97] ✓ | Gerald Green (+1.13) [53] ✓ |
| 57 | **Anthony Davis** (+1.20) | Patrick Beverley (+0.94) [28] ✓ | Ricky Rubio (+1.12) [36] ✓ |
| 58 | **Andre Iguodala** (+1.20) | Kemba Walker (+0.94) [51] ✓ | Jrue Holiday (+1.12) [28] ✓ |
| 59 | **Tyreke Evans** (+1.20) | Jameer Nelson (+0.85) [57] ✓ | Corey Brewer (+1.12) [112] ✗ |
| 60 | **Kenneth Faried** (+1.20) | Kenneth Faried (+0.85) [57] ✓ | Monta Ellis (+1.12) [68] ✓ |
| 61 | **Arron Afflalo** (+1.20) | Greivis Vasquez (+0.84) [112] ✗ | Paul Pierce (+1.10) [126] ✗ |
| 62 | **Jameer Nelson** (+1.20) | Nicolas Batum (+0.79) [36] ✓ | Jeff Teague (+1.08) [90] ✓ |
| 63 | **Lou Williams** (+1.20) | Joakim Noah (+0.77) [47] ✓ | Darren Collison (+1.08) [64] ✓ |
| 64 | **LaMarcus Aldridge** (+1.10) | Reggie Jackson (+0.76) [71] ✓ | DeMarre Carroll (+1.07) [83] ✓ |
| 65 | **Darren Collison** (+1.10) | Andre Drummond (+0.76) [53] ✓ | Pablo Prigioni (+1.05) [40] ✓ |
| 66 | **Nikola Pekovic** (+1.10) | Monta Ellis (+0.73) [68] ✓ | Rudy Gay (+1.04) [64] ✓ |
| 67 | **Rudy Gay** (+1.10) | Gerald Green (+0.71) [53] ✓ | Vince Carter (+1.01) [25] ✓ |
| 68 | **Robin Lopez** (+1.00) | Mirza Teletovic (+0.70) [45] ✓ | Dwight Howard (+1.00) [144] ✗ |
| 69 | **Trevor Ariza** (+1.00) | Chris Bosh (+0.61) [136] ✗ | Mason Plumlee (+0.99) [176] ✗ |
| 70 | **Monta Ellis** (+1.00) | Corey Brewer (+0.58) [112] ✗ | Gordon Hayward (+0.93) [83] ✓ |
| 71 | **Reggie Jackson** (+0.90) | Mike Miller (+0.57) [90] ✓ | Markieff Morris (+0.89) [129] ✗ |
| 72 | **Alec Burks** (+0.90) | Arron Afflalo (+0.57) [57] ✓ | Luol Deng (+0.88) [112] ✗ |
| 73 | **Matthew Dellavedova** (+0.90) | Josh McRoberts (+0.56) [34] ✓ | Anthony Morrow (+0.87) [53] ✓ |
| 74 | **Tony Parker** (+0.90) | Trey Burke (+0.55) [109] ✗ | Amare Stoudemire (+0.86) [166] ✗ |
| 75 | **Shelvin Mack** (+0.90) | Bradley Beal (+0.54) [112] ✗ | Kenneth Faried (+0.85) [57] ✓ |
| 76 | **Terrence Jones** (+0.90) | Tony Parker (+0.53) [71] ✓ | Brandon Jennings (+0.84) [28] ✓ |
| 77 | **Brandon Knight** (+0.90) | Gordon Hayward (+0.51) [83] ✓ | Randy Foye (+0.78) [31] ✓ |
| 78 | **Martell Webster** (+0.90) | Matthew Dellavedova (+0.51) [71] ✓ | Anderson Varejao (+0.74) [97] ✓ |
| 79 | **PJ Tucker** (+0.80) | Dwight Howard (+0.51) [144] ✗ | Eric Gordon (+0.72) [90] ✓ |
| 80 | **Boris Diaw** (+0.80) | Brandon Knight (+0.51) [71] ✓ | Paul Millsap (+0.71) [109] ✗ |
| 81 | **Matt Barnes** (+0.80) | David West (+0.49) [90] ✓ | Alec Burks (+0.68) [71] ✓ |
| 82 | **Marvin Williams** (+0.80) | Terrence Ross (+0.44) [97] ✓ | Greivis Vasquez (+0.63) [112] ✗ |
| 83 | **DeMarre Carroll** (+0.70) | Anthony Tolliver (+0.44) [97] ✗ | Marc Gasol (+0.61) [176] ✗ |
| 84 | **DeAndre Jordan** (+0.70) | Derek Fisher (+0.42) [112] ✗ | Tiago Splitter (+0.58) [166] ✗ |
| 85 | **Danny Green** (+0.70) | Ramon Sessions (+0.40) [90] ✓ | Courtney Lee (+0.54) [83] ✓ |
| 86 | **Gordon Hayward** (+0.70) | Darren Collison (+0.38) [64] ✓ | Josh McRoberts (+0.51) [34] ✓ |
| 87 | **Courtney Lee** (+0.70) | Al Jefferson (+0.38) [149] ✗ | David West (+0.50) [90] ✓ |
| 88 | **Jodie Meeks** (+0.70) | Paul Millsap (+0.37) [109] ✗ | Kemba Walker (+0.50) [51] ✓ |
| 89 | **Kevin Martin** (+0.70) | Lance Stephenson (+0.36) [90] ✓ | Patrick Beverley (+0.46) [28] ✓ |
| 90 | **David West** (+0.60) | Greg Monroe (+0.34) [112] ✗ | PJ Tucker (+0.45) [79] ✓ |
| 91 | **Jeff Teague** (+0.60) | Andray Blatche (+0.31) [166] ✗ | Reggie Jackson (+0.37) [71] ✓ |
| 92 | **Lance Stephenson** (+0.60) | Jeremy Lamb (+0.27) [97] ✗ | Greg Monroe (+0.36) [112] ✗ |
| 93 | **Marcus Thornton** (+0.60) | Zach Randolph (+0.27) [53] ✓ | Ramon Sessions (+0.35) [90] ✓ |
| 94 | **Ramon Sessions** (+0.60) | Ray Allen (+0.27) [51] ✓ | Matthew Dellavedova (+0.33) [71] ✓ |
| 95 | **Mike Miller** (+0.60) | Boris Diaw (+0.26) [79] ✓ | Jeremy Lamb (+0.33) [97] ✗ |
| 96 | **Eric Gordon** (+0.60) | Lou Williams (+0.26) [57] ✓ | Tim Hardaway Jr. (+0.33) [129] ✗ |
| 97 | **Jimmy Butler** (+0.50) | Matt Barnes (+0.23) [79] ✓ | Chris Andersen (+0.31) [97] ✗ |
| 98 | **Anderson Varejao** (+0.50) | Anthony Davis (+0.21) [57] ✓ | Brandon Knight (+0.30) [71] ✓ |
| 99 | **Jared Sullinger** (+0.50) | Marvin Williams (+0.20) [79] ✓ | Bradley Beal (+0.29) [112] ✗ |
| 100 | **Terrence Ross** (+0.50) | Paul Pierce (+0.15) [126] ✗ | Marcin Gortat (+0.28) [156] ✗ |

### 2014-15 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+8.50) | Chris Paul (+6.83) [1] ✓ | Chris Paul (+6.99) [1] ✓ |
| 2 | **James Harden** (+7.70) | James Harden (+6.72) [2] ✓ | James Harden (+5.71) [2] ✓ |
| 3 | **Russell Westbrook** (+6.10) | LeBron James (+5.24) [5] ✓ | LeBron James (+5.62) [5] ✓ |
| 4 | **Kyrie Irving** (+5.50) | Damian Lillard (+4.88) [11] ✓ | Anthony Davis (+5.11) [9] ✓ |
| 5 | **LeBron James** (+5.30) | Kyrie Irving (+4.86) [4] ✓ | Russell Westbrook (+5.02) [3] ✓ |
| 6 | **Lou Williams** (+5.20) | Russell Westbrook (+4.85) [3] ✓ | Jimmy Butler (+4.38) [20] ✓ |
| 7 | **Kyle Korver** (+4.60) | Isaiah Thomas (+4.53) [8] ✓ | Blake Griffin (+4.19) [20] ✓ |
| 8 | **Isaiah Thomas** (+4.50) | Lou Williams (+4.47) [6] ✓ | Kyrie Irving (+4.13) [4] ✓ |
| 9 | **Anthony Davis** (+4.30) | Klay Thompson (+3.64) [9] ✓ | Lou Williams (+4.08) [6] ✓ |
| 10 | **Klay Thompson** (+4.30) | Blake Griffin (+3.29) [20] ✓ | Klay Thompson (+4.00) [9] ✓ |
| 11 | **Damian Lillard** (+4.00) | Anthony Davis (+2.96) [9] ✓ | George Hill (+3.81) [12] ✓ |
| 12 | **George Hill** (+3.90) | Jimmy Butler (+2.73) [20] ✓ | Kawhi Leonard (+3.44) [15] ✓ |
| 13 | **Ty Lawson** (+3.80) | George Hill (+2.72) [12] ✓ | JJ Redick (+3.36) [29] ✓ |
| 14 | **Carmelo Anthony** (+3.80) | Kyle Lowry (+2.72) [18] ✓ | Ty Lawson (+3.18) [13] ✓ |
| 15 | **Kawhi Leonard** (+3.70) | Carmelo Anthony (+2.64) [13] ✓ | Gordon Hayward (+3.01) [20] ✓ |
| 16 | **Rudy Gay** (+3.50) | LaMarcus Aldridge (+2.54) [30] ✓ | Isaiah Thomas (+2.98) [8] ✓ |
| 17 | **DeAndre Jordan** (+3.40) | Ty Lawson (+2.54) [13] ✓ | Carmelo Anthony (+2.96) [13] ✓ |
| 18 | **Kyle Lowry** (+3.30) | Mike Conley (+2.47) [30] ✓ | Damian Lillard (+2.92) [11] ✓ |
| 19 | **Jrue Holiday** (+3.30) | Brandon Jennings (+2.39) [23] ✓ | Wesley Matthews (+2.78) [33] ✓ |
| 20 | **Gordon Hayward** (+3.20) | Tyreke Evans (+2.38) [27] ✓ | Brandon Jennings (+2.74) [23] ✓ |
| 21 | **Jimmy Butler** (+3.20) | Gordon Hayward (+2.37) [20] ✓ | Anthony Morrow (+2.73) [26] ✓ |
| 22 | **Blake Griffin** (+3.20) | JJ Redick (+2.36) [29] ✓ | Jeff Teague (+2.72) [34] ✓ |
| 23 | **Danny Green** (+3.10) | John Wall (+2.26) [37] ✓ | Rudy Gay (+2.65) [16] ✓ |
| 24 | **Brandon Jennings** (+3.10) | Kyle Korver (+2.19) [7] ✓ | Kyle Lowry (+2.48) [18] ✓ |
| 25 | **Danilo Gallinari** (+2.80) | Jrue Holiday (+2.18) [18] ✓ | Al Horford (+2.33) [96] ✓ |
| 26 | **Anthony Morrow** (+2.70) | Jeff Teague (+2.16) [34] ✓ | LaMarcus Aldridge (+2.33) [30] ✓ |
| 27 | **Tyreke Evans** (+2.60) | Reggie Jackson (+2.16) [40] ✓ | Goran Dragic (+2.33) [42] ✓ |
| 28 | **Chandler Parsons** (+2.60) | Aaron Brooks (+2.14) [55] ✓ | Darren Collison (+2.31) [49] ✓ |
| 29 | **JJ Redick** (+2.50) | Kawhi Leonard (+2.06) [15] ✓ | Dirk Nowitzki (+2.24) [34] ✓ |
| 30 | **LaMarcus Aldridge** (+2.40) | Dwyane Wade (+2.01) [40] ✓ | Dwyane Wade (+2.22) [40] ✓ |
| 31 | **Mike Conley** (+2.40) | Dirk Nowitzki (+1.92) [34] ✓ | Jrue Holiday (+2.21) [18] ✓ |
| 32 | **Patrick Patterson** (+2.40) | Jamal Crawford (+1.82) [67] ✓ | Chandler Parsons (+2.18) [27] ✓ |
| 33 | **Wesley Matthews** (+2.30) | Rudy Gay (+1.82) [16] ✓ | Kyle Korver (+2.00) [7] ✓ |
| 34 | **Jeff Teague** (+2.20) | Danilo Gallinari (+1.73) [25] ✓ | Danilo Gallinari (+1.99) [25] ✓ |
| 35 | **Dirk Nowitzki** (+2.20) | Anthony Morrow (+1.65) [26] ✓ | Kevin Love (+1.92) [49] ✓ |
| 36 | **Gerald Green** (+2.20) | Khris Middleton (+1.60) [49] ✓ | Mike Conley (+1.91) [30] ✓ |
| 37 | **John Wall** (+2.10) | Marc Gasol (+1.53) [59] ✓ | Brandan Wright (+1.89) [98] ✓ |
| 38 | **Devin Harris** (+2.10) | Patrick Patterson (+1.48) [30] ✓ | John Wall (+1.85) [37] ✓ |
| 39 | **Ersan Ilyasova** (+2.10) | Manu Ginobili (+1.37) [49] ✓ | Khris Middleton (+1.84) [49] ✓ |
| 40 | **Reggie Jackson** (+2.00) | Zach Randolph (+1.35) [55] ✓ | DeMarre Carroll (+1.75) [42] ✓ |
| 41 | **Dwyane Wade** (+2.00) | Gerald Green (+1.34) [34] ✓ | Paul Millsap (+1.66) [69] ✓ |
| 42 | **DeMarre Carroll** (+1.90) | Danny Green (+1.32) [23] ✓ | Tyson Chandler (+1.60) [119] ✗ |
| 43 | **Nikola Mirotic** (+1.90) | Eric Gordon (+1.29) [87] ✓ | Patrick Patterson (+1.59) [30] ✓ |
| 44 | **Goran Dragic** (+1.90) | Draymond Green (+1.29) [57] ✓ | Brook Lopez (+1.59) [104] ✗ |
| 45 | **JJ Barea** (+1.90) | Kevin Love (+1.28) [49] ✓ | Chris Bosh (+1.55) [150] ✗ |
| 46 | **Luol Deng** (+1.80) | Wesley Matthews (+1.23) [33] ✓ | Tyreke Evans (+1.54) [27] ✓ |
| 47 | **Jae Crowder** (+1.80) | Eric Bledsoe (+1.21) [49] ✓ | Derrick Favors (+1.52) [110] ✗ |
| 48 | **Joe Johnson** (+1.80) | Monta Ellis (+1.19) [79] ✓ | Monta Ellis (+1.52) [79] ✓ |
| 49 | **Khris Middleton** (+1.70) | Derrick Rose (+1.15) [110] ✗ | Danny Green (+1.52) [23] ✓ |
| 50 | **Eric Bledsoe** (+1.70) | JJ Barea (+1.13) [42] ✓ | Marc Gasol (+1.49) [59] ✓ |
| 51 | **Kevin Love** (+1.70) | Mo Williams (+0.99) [61] ✓ | DeMar DeRozan (+1.48) [130] ✗ |
| 52 | **Darren Collison** (+1.70) | Darren Collison (+0.97) [49] ✓ | Jamal Crawford (+1.45) [67] ✓ |
| 53 | **Manu Ginobili** (+1.70) | Bradley Beal (+0.96) [74] ✓ | Greg Monroe (+1.42) [123] ✗ |
| 54 | **Kevin Martin** (+1.70) | Goran Dragic (+0.96) [42] ✓ | Luol Deng (+1.37) [46] ✓ |
| 55 | **Zach Randolph** (+1.60) | Chandler Parsons (+0.93) [27] ✓ | Kevin Martin (+1.36) [49] ✓ |
| 56 | **Aaron Brooks** (+1.60) | Ryan Anderson (+0.87) [57] ✓ | Pau Gasol (+1.23) [98] ✓ |
| 57 | **Draymond Green** (+1.50) | Nikola Mirotic (+0.84) [42] ✓ | Tim Duncan (+1.23) [130] ✗ |
| 58 | **Ryan Anderson** (+1.50) | Kenneth Faried (+0.83) [84] ✓ | Amare Stoudemire (+1.22) [67] ✓ |
| 59 | **Marc Gasol** (+1.40) | Pau Gasol (+0.80) [98] ✓ | DeMarcus Cousins (+1.20) [72] ✓ |
| 60 | **CJ Miles** (+1.40) | Ed Davis (+0.73) [61] ✓ | Nikola Vucevic (+1.18) [159] ✗ |
| 61 | **Kemba Walker** (+1.20) | Deron Williams (+0.66) [61] ✓ | DeAndre Jordan (+1.15) [17] ✓ |
| 62 | **Deron Williams** (+1.20) | Luol Deng (+0.60) [46] ✓ | James Johnson (+1.15) [79] ✓ |
| 63 | **Ed Davis** (+1.20) | Paul Millsap (+0.60) [69] ✓ | Reggie Jackson (+1.11) [40] ✓ |
| 64 | **Robert Covington** (+1.20) | Ersan Ilyasova (+0.59) [37] ✓ | Tyler Zeller (+1.07) [141] ✗ |
| 65 | **Mo Williams** (+1.20) | Paul Pierce (+0.56) [69] ✓ | Ed Davis (+1.05) [61] ✓ |
| 66 | **Kobe Bryant** (+1.20) | Thaddeus Young (+0.55) [87] ✓ | Paul Pierce (+1.05) [69] ✓ |
| 67 | **Amare Stoudemire** (+1.10) | Kevin Martin (+0.53) [49] ✓ | Ersan Ilyasova (+1.05) [37] ✓ |
| 68 | **Jamal Crawford** (+1.10) | Joe Johnson (+0.52) [46] ✓ | Manu Ginobili (+1.02) [49] ✓ |
| 69 | **Paul Millsap** (+1.00) | Victor Oladipo (+0.51) [87] ✓ | Zach Randolph (+1.01) [55] ✓ |
| 70 | **Matt Barnes** (+1.00) | Mike Dunleavy (+0.49) [79] ✓ | JJ Barea (+0.94) [42] ✓ |
| 71 | **Paul Pierce** (+1.00) | Matt Barnes (+0.49) [69] ✓ | Jonas Valanciunas (+0.93) [123] ✗ |
| 72 | **DeMarcus Cousins** (+0.90) | Kentavious CaldwellPope (+0.43) [74] ✓ | Tony Parker (+0.90) [87] ✓ |
| 73 | **Jeremy Lin** (+0.90) | Wilson Chandler (+0.43) [74] ✓ | Devin Harris (+0.87) [37] ✓ |
| 74 | **Bradley Beal** (+0.80) | DeMar DeRozan (+0.42) [130] ✗ | Jodie Meeks (+0.86) [123] ✗ |
| 75 | **Kentavious CaldwellPope** (+0.80) | Andre Miller (+0.42) [87] ✓ | Thaddeus Young (+0.86) [87] ✓ |
| 76 | **Kelly Olynyk** (+0.80) | DeMarcus Cousins (+0.41) [72] ✓ | Eric Gordon (+0.86) [87] ✓ |
| 77 | **Jonas Jerebko** (+0.80) | Jae Crowder (+0.40) [46] ✓ | Eric Bledsoe (+0.83) [49] ✓ |
| 78 | **Wilson Chandler** (+0.80) | Kemba Walker (+0.39) [61] ✓ | Ryan Anderson (+0.82) [57] ✓ |
| 79 | **Monta Ellis** (+0.70) | Kobe Bryant (+0.39) [61] ✓ | Draymond Green (+0.80) [57] ✓ |
| 80 | **James Johnson** (+0.70) | DeMarre Carroll (+0.38) [42] ✓ | Deron Williams (+0.77) [61] ✓ |
| 81 | **Cory Joseph** (+0.70) | Greivis Vasquez (+0.38) [141] ✗ | Joe Johnson (+0.72) [46] ✓ |
| 82 | **Mike Dunleavy** (+0.70) | Devin Harris (+0.35) [37] ✓ | Jonas Jerebko (+0.71) [74] ✓ |
| 83 | **Anthony Tolliver** (+0.70) | DeAndre Jordan (+0.35) [17] ✓ | Harrison Barnes (+0.69) [137] ✗ |
| 84 | **Nicolas Batum** (+0.60) | DJ Augustin (+0.29) [130] ✗ | Kenneth Faried (+0.66) [84] ✓ |
| 85 | **Kenneth Faried** (+0.60) | James Johnson (+0.24) [79] ✓ | Nikola Mirotic (+0.63) [42] ✓ |
| 86 | **Omri Casspi** (+0.60) | Trey Burke (+0.23) [119] ✗ | CJ Miles (+0.63) [59] ✓ |
| 87 | **Thaddeus Young** (+0.50) | Evan Fournier (+0.22) [104] ✗ | Mike Dunleavy (+0.60) [79] ✓ |
| 88 | **Elfrid Payton** (+0.50) | Greg Monroe (+0.19) [123] ✗ | Marreese Speights (+0.59) [194] ✗ |
| 89 | **Brandon Knight** (+0.50) | Courtney Lee (+0.19) [119] ✗ | Andre Iguodala (+0.56) [137] ✗ |
| 90 | **Jared Sullinger** (+0.50) | Dennis Schroder (+0.11) [159] ✗ | CJ Watson (+0.53) [110] ✗ |
| 91 | **Rodney Stuckey** (+0.50) | Jonas Jerebko (+0.05) [74] ✓ | Marcin Gortat (+0.51) [141] ✗ |
| 92 | **Victor Oladipo** (+0.50) | Harrison Barnes (+0.04) [137] ✗ | Tobias Harris (+0.50) [119] ✗ |
| 93 | **Eric Gordon** (+0.50) | CJ Watson (+0.01) [110] ✗ | Bradley Beal (+0.50) [74] ✓ |
| 94 | **Tony Parker** (+0.50) | CJ Miles (-0.01) [59] ✓ | Trevor Ariza (+0.49) [123] ✗ |
| 95 | **Andre Miller** (+0.50) | Andre Iguodala (-0.01) [137] ✗ | Kemba Walker (+0.48) [61] ✓ |
| 96 | **Al Horford** (+0.40) | Jameer Nelson (-0.05) [110] ✗ | Brandon Bass (+0.47) [202] ✗ |
| 97 | **Tim Hardaway Jr.** (+0.40) | Rodney Stuckey (-0.06) [87] ✓ | Amir Johnson (+0.43) [123] ✗ |
| 98 | **Pau Gasol** (+0.30) | Tony Parker (-0.08) [87] ✓ | Nicolas Batum (+0.43) [84] ✓ |
| 99 | **Marcus Smart** (+0.30) | Terrence Ross (-0.12) [98] ✗ | Cory Joseph (+0.40) [79] ✓ |
| 100 | **Brandan Wright** (+0.30) | Kelly Olynyk (-0.17) [74] ✓ | Brandon Knight (+0.38) [87] ✓ |

### 2013-14 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.00) | Andrew Bogut (+4.48) [4] ✓ | Andrew Bogut (+3.58) [4] ✓ |
| 2 | **Draymond Green** (+4.60) | Draymond Green (+4.38) [2] ✓ | Kawhi Leonard (+3.44) [1] ✓ |
| 3 | **Joakim Noah** (+4.50) | Anderson Varejao (+3.44) [10] ✓ | Draymond Green (+3.32) [2] ✓ |
| 4 | **Andrew Bogut** (+4.40) | Kevin Garnett (+3.28) [11] ✓ | DeAndre Jordan (+3.19) [64] ✓ |
| 5 | **Michael KiddGilchrist** (+4.40) | Paul Millsap (+3.25) [23] ✓ | Jimmy Butler (+3.17) [14] ✓ |
| 6 | **Tiago Splitter** (+4.20) | Derek Fisher (+3.14) [43] ✓ | Tony Allen (+3.07) [30] ✓ |
| 7 | **Danny Green** (+4.00) | Anthony Davis (+3.09) [33] ✓ | Joakim Noah (+3.05) [3] ✓ |
| 8 | **Chris Paul** (+3.90) | Jae Crowder (+3.07) [18] ✓ | Danny Green (+2.82) [7] ✓ |
| 9 | **Nene** (+3.80) | Paul George (+3.05) [21] ✓ | Ricky Rubio (+2.79) [47] ✓ |
| 10 | **Anderson Varejao** (+3.60) | Tiago Splitter (+3.05) [6] ✓ | Andre Iguodala (+2.69) [25] ✓ |
| 11 | **Nick Calathes** (+3.50) | Iman Shumpert (+3.02) [72] ✓ | Paul George (+2.62) [21] ✓ |
| 12 | **Ian Mahinmi** (+3.50) | Marcin Gortat (+2.97) [27] ✓ | Kyle OQuinn (+2.43) [52] ✓ |
| 13 | **Kevin Garnett** (+3.50) | Nene (+2.91) [9] ✓ | Roy Hibbert (+2.43) [14] ✓ |
| 14 | **Jimmy Butler** (+3.40) | Kawhi Leonard (+2.83) [1] ✓ | David West (+2.32) [57] ✓ |
| 15 | **Roy Hibbert** (+3.40) | Danny Green (+2.58) [7] ✓ | Paul Millsap (+2.20) [23] ✓ |
| 16 | **DeMarcus Cousins** (+3.30) | Andre Iguodala (+2.52) [25] ✓ | Ian Mahinmi (+2.14) [11] ✓ |
| 17 | **CJ Watson** (+3.20) | Jimmy Butler (+2.42) [14] ✓ | Anderson Varejao (+2.08) [10] ✓ |
| 18 | **Tim Duncan** (+3.00) | Chris Andersen (+2.40) [52] ✓ | Tim Duncan (+2.05) [18] ✓ |
| 19 | **Kris Humphries** (+3.00) | Blake Griffin (+2.37) [110] ✗ | CJ Watson (+2.03) [17] ✓ |
| 20 | **Jae Crowder** (+3.00) | CJ Watson (+2.35) [17] ✓ | Chris Paul (+2.00) [8] ✓ |
| 21 | **Paul George** (+2.90) | Tim Duncan (+2.26) [18] ✓ | Bismack Biyombo (+1.84) [64] ✓ |
| 22 | **Marc Gasol** (+2.80) | Paul Pierce (+2.23) [47] ✓ | Kevin Garnett (+1.76) [11] ✓ |
| 23 | **Paul Millsap** (+2.70) | DeAndre Jordan (+2.14) [64] ✓ | Manu Ginobili (+1.74) [64] ✓ |
| 24 | **Shane Battier** (+2.70) | Josh Smith (+2.12) [83] ✓ | George Hill (+1.72) [40] ✓ |
| 25 | **DeMarre Carroll** (+2.60) | Michael KiddGilchrist (+2.10) [4] ✓ | Iman Shumpert (+1.70) [72] ✓ |
| 26 | **Andre Iguodala** (+2.60) | Tony Allen (+2.08) [30] ✓ | Serge Ibaka (+1.70) [33] ✓ |
| 27 | **Mario Chalmers** (+2.50) | Gerald Wallace (+2.08) [52] ✓ | Nick Calathes (+1.65) [11] ✓ |
| 28 | **Marcin Gortat** (+2.50) | Pablo Prigioni (+2.04) [124] ✗ | Anthony Davis (+1.65) [33] ✓ |
| 29 | **Samuel Dalembert** (+2.50) | Nick Collison (+2.01) [88] ✓ | Kirk Hinrich (+1.56) [40] ✓ |
| 30 | **Dwight Howard** (+2.40) | Dwight Howard (+1.98) [30] ✓ | Phil Pressey (+1.55) [76] ✓ |
| 31 | **Tony Allen** (+2.40) | Robin Lopez (+1.94) [43] ✓ | Thabo Sefolosha (+1.51) [33] ✓ |
| 32 | **Victor Oladipo** (+2.40) | DeMarcus Cousins (+1.93) [16] ✓ | Taj Gibson (+1.46) [83] ✓ |
| 33 | **Kemba Walker** (+2.30) | Amir Johnson (+1.87) [62] ✓ | DeMarcus Cousins (+1.45) [16] ✓ |
| 34 | **Anthony Davis** (+2.30) | Kirk Hinrich (+1.87) [40] ✓ | Lance Stephenson (+1.45) [164] ✗ |
| 35 | **Serge Ibaka** (+2.30) | Patrick Patterson (+1.86) [83] ✓ | Nene (+1.44) [9] ✓ |
| 36 | **Thabo Sefolosha** (+2.30) | Chris Bosh (+1.86) [72] ✓ | Trevor Ariza (+1.42) [88] ✓ |
| 37 | **LaMarcus Aldridge** (+2.20) | Kyle OQuinn (+1.84) [52] ✓ | Jae Crowder (+1.41) [18] ✓ |
| 38 | **Nikola Pekovic** (+2.20) | Shane Battier (+1.83) [23] ✓ | Al Jefferson (+1.40) [47] ✓ |
| 39 | **Eric Bledsoe** (+2.20) | Al Jefferson (+1.77) [47] ✓ | Corey Brewer (+1.38) [88] ✓ |
| 40 | **George Hill** (+2.10) | Taj Gibson (+1.76) [83] ✓ | DeMarre Carroll (+1.35) [25] ✓ |
| 41 | **Kirk Hinrich** (+2.10) | Joakim Noah (+1.75) [3] ✓ | Dwight Howard (+1.35) [30] ✓ |
| 42 | **Kosta Koufos** (+2.10) | Ian Mahinmi (+1.73) [11] ✓ | Marcin Gortat (+1.34) [27] ✓ |
| 43 | **Robin Lopez** (+2.00) | Nick Calathes (+1.72) [11] ✓ | Elton Brand (+1.32) [83] ✓ |
| 44 | **Derek Fisher** (+2.00) | George Hill (+1.71) [40] ✓ | LaMarcus Aldridge (+1.26) [37] ✓ |
| 45 | **Patrick Beverley** (+1.90) | DeMarre Carroll (+1.70) [25] ✓ | Patrick Patterson (+1.23) [83] ✓ |
| 46 | **Darrell Arthur** (+1.90) | Nicolas Batum (+1.69) [124] ✗ | Michael KiddGilchrist (+1.22) [4] ✓ |
| 47 | **Ricky Rubio** (+1.80) | Kemba Walker (+1.68) [33] ✓ | Chris Andersen (+1.19) [52] ✓ |
| 48 | **Al Jefferson** (+1.80) | Roy Hibbert (+1.67) [14] ✓ | Derek Fisher (+1.17) [43] ✓ |
| 49 | **Paul Pierce** (+1.80) | Shaun Livingston (+1.65) [76] ✓ | Mario Chalmers (+1.17) [27] ✓ |
| 50 | **Jeremy Lin** (+1.80) | Thabo Sefolosha (+1.63) [33] ✓ | John Wall (+1.16) [136] ✗ |
| 51 | **Kendrick Perkins** (+1.70) | LaMarcus Aldridge (+1.63) [37] ✓ | Gerald Wallace (+1.14) [52] ✓ |
| 52 | **Darren Collison** (+1.60) | Ersan Ilyasova (+1.61) [88] ✓ | Amir Johnson (+1.12) [62] ✓ |
| 53 | **Chris Andersen** (+1.60) | Victor Oladipo (+1.60) [30] ✓ | Kemba Walker (+1.08) [33] ✓ |
| 54 | **Kyle OQuinn** (+1.60) | Mario Chalmers (+1.58) [27] ✓ | Marc Gasol (+1.05) [22] ✓ |
| 55 | **Gerald Wallace** (+1.60) | Samuel Dalembert (+1.55) [27] ✓ | Mike Dunleavy (+1.05) [121] ✗ |
| 56 | **Miles Plumlee** (+1.60) | David West (+1.52) [57] ✓ | Patrick Beverley (+1.00) [45] ✓ |
| 57 | **David West** (+1.50) | Elton Brand (+1.45) [83] ✓ | Josh McRoberts (+0.95) [180] ✗ |
| 58 | **Kyle Lowry** (+1.30) | Manu Ginobili (+1.42) [64] ✓ | Kosta Koufos (+0.95) [40] ✓ |
| 59 | **Nate Wolters** (+1.30) | Anthony Tolliver (+1.42) [143] ✗ | Russell Westbrook (+0.94) [99] ✗ |
| 60 | **Omri Casspi** (+1.30) | Patty Mills (+1.41) [76] ✓ | Patty Mills (+0.92) [76] ✓ |
| 61 | **Tayshaun Prince** (+1.30) | Ricky Rubio (+1.41) [47] ✓ | Chris Bosh (+0.91) [72] ✓ |
| 62 | **David Lee** (+1.20) | Darrell Arthur (+1.39) [45] ✓ | Klay Thompson (+0.91) [83] ✓ |
| 63 | **Amir Johnson** (+1.20) | Wesley Matthews (+1.38) [129] ✗ | Darrell Arthur (+0.88) [45] ✓ |
| 64 | **DeAndre Jordan** (+1.10) | Kyle Lowry (+1.36) [58] ✓ | Andre Drummond (+0.86) [114] ✗ |
| 65 | **Manu Ginobili** (+1.10) | PJ Tucker (+1.36) [76] ✓ | Tiago Splitter (+0.86) [6] ✓ |
| 66 | **Jared Sullinger** (+1.10) | Patrick Beverley (+1.34) [45] ✓ | Victor Oladipo (+0.85) [30] ✓ |
| 67 | **Andray Blatche** (+1.10) | Andray Blatche (+1.30) [64] ✓ | Shaun Livingston (+0.85) [76] ✓ |
| 68 | **Jeremy Evans** (+1.10) | Chris Paul (+1.27) [8] ✓ | Kyle Korver (+0.77) [158] ✗ |
| 69 | **Timofey Mozgov** (+1.10) | Marc Gasol (+1.25) [22] ✓ | David Lee (+0.76) [62] ✓ |
| 70 | **Tyson Chandler** (+1.10) | Tyson Chandler (+1.22) [64] ✓ | Derrick Favors (+0.75) [99] ✗ |
| 71 | **Bismack Biyombo** (+1.10) | DeJuan Blair (+1.20) [129] ✗ | Carlos Boozer (+0.71) [188] ✗ |
| 72 | **Chris Bosh** (+1.00) | Miles Plumlee (+1.20) [52] ✓ | Jeremy Lamb (+0.70) [124] ✗ |
| 73 | **Courtney Lee** (+1.00) | Channing Frye (+1.19) [99] ✗ | Nikola Vucevic (+0.70) [76] ✓ |
| 74 | **Iman Shumpert** (+1.00) | Trevor Ariza (+1.16) [88] ✓ | Courtney Lee (+0.69) [72] ✓ |
| 75 | **ETwaun Moore** (+1.00) | Omri Casspi (+1.11) [58] ✓ | Andray Blatche (+0.69) [64] ✓ |
| 76 | **Kevin Love** (+0.90) | David Lee (+1.11) [62] ✓ | Maurice Harkless (+0.68) [99] ✗ |
| 77 | **Patty Mills** (+0.90) | ETwaun Moore (+1.06) [72] ✓ | Nicolas Batum (+0.65) [124] ✗ |
| 78 | **PJ Tucker** (+0.90) | Steven Adams (+0.99) [121] ✗ | Steven Adams (+0.65) [121] ✗ |
| 79 | **Shaun Livingston** (+0.90) | Jason Thompson (+0.98) [110] ✗ | Dante Cunningham (+0.64) [114] ✗ |
| 80 | **Nikola Vucevic** (+0.90) | Giannis Antetokounmpo (+0.97) [114] ✗ | Shane Battier (+0.64) [23] ✓ |
| 81 | **Avery Bradley** (+0.90) | Greg Monroe (+0.97) [143] ✗ | PJ Tucker (+0.63) [76] ✓ |
| 82 | **Phil Pressey** (+0.90) | Timofey Mozgov (+0.96) [64] ✓ | Channing Frye (+0.62) [99] ✗ |
| 83 | **Klay Thompson** (+0.80) | Jeremy Evans (+0.95) [64] ✓ | Miles Plumlee (+0.59) [52] ✓ |
| 84 | **Taj Gibson** (+0.80) | Terrence Jones (+0.93) [202] ✗ | Tyson Chandler (+0.57) [64] ✓ |
| 85 | **Patrick Patterson** (+0.80) | Kendrick Perkins (+0.89) [51] ✓ | Kyle Lowry (+0.56) [58] ✓ |
| 86 | **Josh Smith** (+0.80) | Kyle Korver (+0.85) [158] ✗ | Matt Barnes (+0.55) [143] ✗ |
| 87 | **Elton Brand** (+0.80) | Deron Williams (+0.83) [92] ✓ | Terrence Jones (+0.53) [202] ✗ |
| 88 | **Trevor Ariza** (+0.70) | Mike Dunleavy (+0.82) [121] ✗ | Josh Smith (+0.52) [83] ✓ |
| 89 | **Corey Brewer** (+0.70) | Kosta Koufos (+0.81) [40] ✓ | Deron Williams (+0.52) [92] ✓ |
| 90 | **Nick Collison** (+0.70) | Jared Dudley (+0.74) [114] ✗ | Eric Bledsoe (+0.50) [37] ✓ |
| 91 | **Ersan Ilyasova** (+0.70) | Dirk Nowitzki (+0.74) [99] ✓ | Michael CarterWilliams (+0.50) [92] ✓ |
| 92 | **Deron Williams** (+0.60) | Andrew Nicholson (+0.73) [164] ✗ | Paul Pierce (+0.49) [47] ✓ |
| 93 | **Luol Deng** (+0.60) | Jeremy Lamb (+0.73) [124] ✗ | Boris Diaw (+0.47) [110] ✗ |
| 94 | **Michael CarterWilliams** (+0.60) | Phil Pressey (+0.72) [76] ✓ | Blake Griffin (+0.43) [110] ✗ |
| 95 | **Matthew Dellavedova** (+0.60) | Bismack Biyombo (+0.72) [64] ✓ | Terrence Ross (+0.41) [129] ✗ |
| 96 | **Andrea Bargnani** (+0.60) | Jared Sullinger (+0.69) [64] ✓ | Kris Humphries (+0.39) [18] ✓ |
| 97 | **Spencer Hawes** (+0.50) | Harrison Barnes (+0.67) [158] ✗ | Cody Zeller (+0.38) [136] ✗ |
| 98 | **AlFarouq Aminu** (+0.50) | Boris Diaw (+0.67) [110] ✗ | Pablo Prigioni (+0.38) [124] ✗ |
| 99 | **Dirk Nowitzki** (+0.40) | Josh McRoberts (+0.65) [180] ✗ | Giannis Antetokounmpo (+0.36) [114] ✗ |
| 100 | **Isaiah Thomas** (+0.40) | Kentavious CaldwellPope (+0.64) [114] ✗ | Gerald Henderson (+0.34) [136] ✗ |

### 2014-15 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.20) | Draymond Green (+4.89) [2] ✓ | Kawhi Leonard (+4.47) [1] ✓ |
| 2 | **Draymond Green** (+5.10) | Rudy Gobert (+4.65) [3] ✓ | Tony Allen (+4.32) [3] ✓ |
| 3 | **Rudy Gobert** (+4.80) | Andrew Bogut (+4.50) [5] ✓ | Draymond Green (+3.75) [2] ✓ |
| 4 | **Tony Allen** (+4.80) | Tony Allen (+3.61) [3] ✓ | Andrew Bogut (+3.21) [5] ✓ |
| 5 | **Andrew Bogut** (+4.70) | Iman Shumpert (+3.45) [29] ✓ | DeAndre Jordan (+3.15) [60] ✓ |
| 6 | **Anthony Davis** (+4.50) | Nerlens Noel (+3.38) [19] ✓ | Nerlens Noel (+2.89) [19] ✓ |
| 7 | **DeMarcus Cousins** (+4.40) | Kawhi Leonard (+3.28) [1] ✓ | Anthony Davis (+2.86) [6] ✓ |
| 8 | **Marcin Gortat** (+3.60) | Nene (+3.27) [17] ✓ | Rudy Gobert (+2.78) [3] ✓ |
| 9 | **Tim Duncan** (+3.50) | AlFarouq Aminu (+2.86) [20] ✓ | Danny Green (+2.72) [14] ✓ |
| 10 | **Andre Roberson** (+3.40) | Greg Monroe (+2.73) [111] ✗ | AlFarouq Aminu (+2.59) [20] ✓ |
| 11 | **Kosta Koufos** (+3.30) | Anthony Davis (+2.68) [6] ✓ | Tim Duncan (+2.49) [9] ✓ |
| 12 | **Zaza Pachulia** (+3.20) | Andre Roberson (+2.68) [10] ✓ | Paul Millsap (+2.35) [26] ✓ |
| 13 | **Khris Middleton** (+3.10) | Zaza Pachulia (+2.67) [12] ✓ | Khris Middleton (+2.07) [13] ✓ |
| 14 | **Danny Green** (+3.00) | Jared Dudley (+2.63) [42] ✓ | Andre Roberson (+2.05) [10] ✓ |
| 15 | **Serge Ibaka** (+3.00) | Pau Gasol (+2.57) [85] ✓ | Bismack Biyombo (+2.05) [102] ✗ |
| 16 | **Michael KiddGilchrist** (+3.00) | DeMarcus Cousins (+2.53) [7] ✓ | Marcus Smart (+1.98) [42] ✓ |
| 17 | **Jonas Jerebko** (+2.80) | Marcus Smart (+2.51) [42] ✓ | John Wall (+1.97) [127] ✗ |
| 18 | **Nene** (+2.80) | Derrick Favors (+2.49) [34] ✓ | DeMarcus Cousins (+1.95) [7] ✓ |
| 19 | **Nerlens Noel** (+2.70) | Tyson Chandler (+2.47) [20] ✓ | Marcin Gortat (+1.93) [8] ✓ |
| 20 | **Marc Gasol** (+2.60) | Kosta Koufos (+2.25) [11] ✓ | Michael KiddGilchrist (+1.74) [14] ✓ |
| 21 | **Tyson Chandler** (+2.60) | Danny Green (+2.25) [14] ✓ | John Henson (+1.64) [85] ✓ |
| 22 | **Joakim Noah** (+2.60) | Tim Duncan (+2.23) [9] ✓ | Nicolas Batum (+1.55) [136] ✗ |
| 23 | **Josh Smith** (+2.60) | Kelly Olynyk (+2.19) [34] ✓ | Zaza Pachulia (+1.53) [12] ✓ |
| 24 | **AlFarouq Aminu** (+2.60) | Dwight Howard (+2.18) [40] ✓ | Trevor Ariza (+1.51) [102] ✗ |
| 25 | **Alex Len** (+2.50) | Jonas Jerebko (+2.17) [17] ✓ | Andre Drummond (+1.50) [93] ✓ |
| 26 | **Paul Millsap** (+2.40) | Paul Millsap (+2.11) [26] ✓ | Kosta Koufos (+1.48) [11] ✓ |
| 27 | **Timofey Mozgov** (+2.40) | Josh Smith (+2.10) [20] ✓ | Iman Shumpert (+1.40) [29] ✓ |
| 28 | **Omer Asik** (+2.40) | Michael KiddGilchrist (+2.07) [14] ✓ | Cody Zeller (+1.40) [34] ✓ |
| 29 | **Darren Collison** (+2.30) | Timofey Mozgov (+2.06) [26] ✓ | Kelly Olynyk (+1.40) [34] ✓ |
| 30 | **Iman Shumpert** (+2.30) | Wesley Matthews (+2.03) [54] ✓ | Jae Crowder (+1.36) [102] ✗ |
| 31 | **Michael CarterWilliams** (+2.30) | Khris Middleton (+2.03) [13] ✓ | James Johnson (+1.35) [54] ✓ |
| 32 | **Luc Mbah a Moute** (+2.30) | Marcin Gortat (+2.01) [8] ✓ | Nikola Mirotic (+1.34) [33] ✓ |
| 33 | **Nikola Mirotic** (+2.20) | Donatas Motiejunas (+1.95) [73] ✓ | Serge Ibaka (+1.33) [14] ✓ |
| 34 | **Chris Paul** (+2.10) | Jimmy Butler (+1.90) [136] ✗ | Marc Gasol (+1.31) [20] ✓ |
| 35 | **Derrick Favors** (+2.10) | Mario Chalmers (+1.90) [49] ✓ | Dwight Howard (+1.31) [40] ✓ |
| 36 | **Kelly Olynyk** (+2.10) | Nikola Mirotic (+1.81) [33] ✓ | Nene (+1.28) [17] ✓ |
| 37 | **Cody Zeller** (+2.10) | Andre Iguodala (+1.81) [46] ✓ | Chris Paul (+1.28) [34] ✓ |
| 38 | **Steven Adams** (+2.00) | Manu Ginobili (+1.79) [51] ✓ | George Hill (+1.27) [45] ✓ |
| 39 | **Roy Hibbert** (+2.00) | Jae Crowder (+1.75) [102] ✗ | Giannis Antetokounmpo (+1.26) [146] ✗ |
| 40 | **LaMarcus Aldridge** (+1.90) | Amir Johnson (+1.75) [73] ✓ | Marvin Williams (+1.25) [69] ✓ |
| 41 | **Dwight Howard** (+1.90) | Kendrick Perkins (+1.71) [73] ✓ | Miles Plumlee (+1.25) [85] ✓ |
| 42 | **Marcus Smart** (+1.80) | Andre Drummond (+1.70) [93] ✓ | Alex Len (+1.22) [25] ✓ |
| 43 | **Jared Dudley** (+1.80) | Michael CarterWilliams (+1.69) [29] ✓ | Al Jefferson (+1.22) [102] ✗ |
| 44 | **Pablo Prigioni** (+1.80) | Zach Randolph (+1.68) [54] ✓ | Andre Iguodala (+1.22) [46] ✓ |
| 45 | **George Hill** (+1.70) | George Hill (+1.67) [45] ✓ | Jimmy Butler (+1.20) [136] ✗ |
| 46 | **Kevin Love** (+1.60) | Brandan Wright (+1.63) [54] ✓ | CJ Miles (+1.16) [111] ✗ |
| 47 | **Al Horford** (+1.60) | John Henson (+1.58) [85] ✓ | Elfrid Payton (+1.16) [111] ✗ |
| 48 | **Andre Iguodala** (+1.60) | DeAndre Jordan (+1.57) [60] ✓ | Robert Covington (+1.12) [136] ✗ |
| 49 | **Mario Chalmers** (+1.50) | Joakim Noah (+1.47) [20] ✓ | Josh Smith (+1.12) [20] ✓ |
| 50 | **Kris Humphries** (+1.50) | Ersan Ilyasova (+1.45) [93] ✓ | Michael CarterWilliams (+1.12) [29] ✓ |
| 51 | **Manu Ginobili** (+1.40) | Jared Sullinger (+1.44) [85] ✓ | Pablo Prigioni (+1.11) [42] ✓ |
| 52 | **Alan Anderson** (+1.40) | James Johnson (+1.44) [54] ✓ | Jared Dudley (+1.07) [42] ✓ |
| 53 | **Robin Lopez** (+1.40) | Cory Joseph (+1.44) [85] ✓ | Manu Ginobili (+1.06) [51] ✓ |
| 54 | **Wesley Matthews** (+1.30) | Alex Len (+1.42) [25] ✓ | LeBron James (+1.04) [132] ✗ |
| 55 | **Zach Randolph** (+1.30) | PJ Tucker (+1.39) [54] ✓ | Jeff Teague (+1.04) [85] ✓ |
| 56 | **PJ Tucker** (+1.30) | Miles Plumlee (+1.34) [85] ✓ | PJ Tucker (+1.03) [54] ✓ |
| 57 | **James Johnson** (+1.30) | Harrison Barnes (+1.27) [132] ✗ | Joakim Noah (+1.01) [20] ✓ |
| 58 | **Brandan Wright** (+1.30) | Danilo Gallinari (+1.24) [111] ✗ | Pau Gasol (+1.00) [85] ✓ |
| 59 | **Langston Galloway** (+1.30) | Pablo Prigioni (+1.23) [42] ✓ | Roy Hibbert (+1.00) [38] ✓ |
| 60 | **DeAndre Jordan** (+1.20) | Chris Kaman (+1.22) [111] ✗ | Derrick Favors (+0.98) [34] ✓ |
| 61 | **Eric Bledsoe** (+1.20) | Luc Mbah a Moute (+1.22) [29] ✓ | Zach Randolph (+0.98) [54] ✓ |
| 62 | **Kemba Walker** (+1.20) | Al Jefferson (+1.22) [102] ✗ | Brandan Wright (+0.96) [54] ✓ |
| 63 | **Jonas Valanciunas** (+1.20) | Steven Adams (+1.20) [38] ✓ | Bradley Beal (+0.94) [85] ✓ |
| 64 | **Gerald Henderson** (+1.10) | Al Horford (+1.17) [46] ✓ | Luis Scola (+0.94) [82] ✓ |
| 65 | **CJ Watson** (+1.10) | Taj Gibson (+1.14) [73] ✓ | Matt Barnes (+0.93) [79] ✓ |
| 66 | **Klay Thompson** (+1.00) | Trevor Booker (+1.10) [69] ✓ | Jonas Jerebko (+0.92) [17] ✓ |
| 67 | **DeMarre Carroll** (+1.00) | Omer Asik (+1.09) [26] ✓ | Al Horford (+0.90) [46] ✓ |
| 68 | **Patrick Beverley** (+1.00) | LaMarcus Aldridge (+1.09) [40] ✓ | David West (+0.89) [102] ✗ |
| 69 | **Corey Brewer** (+0.90) | Trevor Ariza (+1.09) [102] ✗ | Wesley Matthews (+0.86) [54] ✓ |
| 70 | **Markieff Morris** (+0.90) | Patrick Patterson (+1.08) [162] ✗ | Jerami Grant (+0.85) [102] ✗ |
| 71 | **Marvin Williams** (+0.90) | Giannis Antetokounmpo (+1.07) [146] ✗ | James Harden (+0.84) [121] ✗ |
| 72 | **Trevor Booker** (+0.90) | Klay Thompson (+1.06) [66] ✓ | Kyle Korver (+0.83) [121] ✗ |
| 73 | **Monta Ellis** (+0.80) | Jonas Valanciunas (+1.06) [60] ✓ | DeMarre Carroll (+0.77) [66] ✓ |
| 74 | **Amir Johnson** (+0.80) | Matt Barnes (+1.06) [79] ✓ | Klay Thompson (+0.74) [66] ✓ |
| 75 | **Donatas Motiejunas** (+0.80) | Kent Bazemore (+1.05) [127] ✗ | CJ Watson (+0.71) [64] ✓ |
| 76 | **Derrick Rose** (+0.80) | Marvin Williams (+1.04) [69] ✓ | Russell Westbrook (+0.69) [151] ✗ |
| 77 | **Taj Gibson** (+0.80) | Dante Exum (+1.02) [162] ✗ | Tyson Chandler (+0.68) [20] ✓ |
| 78 | **Kendrick Perkins** (+0.80) | Markieff Morris (+0.97) [69] ✓ | Kent Bazemore (+0.66) [127] ✗ |
| 79 | **JJ Redick** (+0.70) | Corey Brewer (+0.93) [69] ✓ | LaMarcus Aldridge (+0.64) [40] ✓ |
| 80 | **Matt Barnes** (+0.70) | Tyler Zeller (+0.91) [102] ✗ | Kemba Walker (+0.62) [60] ✓ |
| 81 | **KJ McDaniels** (+0.70) | Roy Hibbert (+0.90) [38] ✓ | Eric Bledsoe (+0.59) [60] ✓ |
| 82 | **Kyle Lowry** (+0.60) | Bismack Biyombo (+0.89) [102] ✗ | Evan Turner (+0.58) [111] ✗ |
| 83 | **Brook Lopez** (+0.60) | Kris Humphries (+0.86) [49] ✓ | KJ McDaniels (+0.52) [79] ✓ |
| 84 | **Luis Scola** (+0.60) | Luis Scola (+0.84) [82] ✓ | Jrue Holiday (+0.51) [155] ✗ |
| 85 | **Mike Conley** (+0.50) | Nicolas Batum (+0.84) [136] ✗ | Otto Porter Jr. (+0.45) [191] ✗ |
| 86 | **Jeff Teague** (+0.50) | Robert Covington (+0.84) [136] ✗ | Kris Humphries (+0.45) [49] ✓ |
| 87 | **Pau Gasol** (+0.50) | Kevin Love (+0.84) [46] ✓ | Harrison Barnes (+0.44) [132] ✗ |
| 88 | **Bradley Beal** (+0.50) | Anthony Tolliver (+0.82) [199] ✗ | Markieff Morris (+0.43) [69] ✓ |
| 89 | **Jared Sullinger** (+0.50) | Quincy Acy (+0.81) [146] ✗ | Kevin Love (+0.42) [46] ✓ |
| 90 | **Cory Joseph** (+0.50) | Devin Harris (+0.80) [93] ✓ | Greg Monroe (+0.41) [111] ✗ |
| 91 | **Miles Plumlee** (+0.50) | KJ McDaniels (+0.75) [79] ✓ | Corey Brewer (+0.37) [69] ✓ |
| 92 | **John Henson** (+0.50) | David West (+0.74) [102] ✗ | Paul Pierce (+0.37) [111] ✗ |
| 93 | **Devin Harris** (+0.40) | Langston Galloway (+0.74) [54] ✓ | Timofey Mozgov (+0.37) [26] ✓ |
| 94 | **Thaddeus Young** (+0.40) | CJ Watson (+0.74) [64] ✓ | Steven Adams (+0.36) [38] ✓ |
| 95 | **Ed Davis** (+0.40) | Patrick Beverley (+0.73) [66] ✓ | Mario Chalmers (+0.36) [49] ✓ |
| 96 | **Jeremy Lin** (+0.40) | LeBron James (+0.72) [132] ✗ | Mike Dunleavy (+0.35) [151] ✗ |
| 97 | **Ersan Ilyasova** (+0.40) | Kirk Hinrich (+0.69) [155] ✗ | Mike Conley (+0.34) [85] ✓ |
| 98 | **Andre Drummond** (+0.40) | Cody Zeller (+0.66) [34] ✓ | Rajon Rondo (+0.34) [93] ✗ |
| 99 | **Tony Snell** (+0.40) | Eric Bledsoe (+0.65) [60] ✓ | Monta Ellis (+0.30) [73] ✓ |
| 100 | **Chris Bosh** (+0.40) | Serge Ibaka (+0.64) [14] ✓ | Mason Plumlee (+0.28) [214] ✗ |

