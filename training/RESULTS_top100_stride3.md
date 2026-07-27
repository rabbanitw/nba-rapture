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
| ours (direct total) | 1.746 | 1.314 | +0.743 | +0.880 | +0.881 |
| ours (offense+defense) | 1.830 | 1.371 | +0.717 | +0.865 | +0.860 |
| Paine (eRO+eRD) | 1.938 | 1.380 | +0.683 | +0.841 | +0.846 |

**offense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.031 | 0.789 | +0.818 | +0.920 | +0.904 |
| Paine (eRO) | 1.309 | 0.960 | +0.707 | +0.847 | +0.825 |

**defense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.506 | 1.138 | +0.583 | +0.789 | +0.793 |
| Paine (eRD) | 1.642 | 1.196 | +0.504 | +0.726 | +0.727 |

## Summary — true top-100 members recovered (hits@100)

**total**

| season | split | pool | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) | ρ ours (direct total) | ρ ours (offense+defense) | ρ Paine (eRO+eRD) |
|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 82/100 | 82/100 | 80/100 | +0.885 | +0.891 | +0.889 |
| 2014-15 | Regular season | 246 | 82/100 | 81/100 | 85/100 | +0.896 | +0.853 | +0.901 |
| **all** | | | **164/200** | **163/200** | **165/200** |  |  |  |

Precision@K for total, summed over 2 cells:

| K | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|
| 10 | 15/20 | 14/20 | 15/20 |
| 25 | 36/50 | 34/50 | 35/50 |
| 50 | 76/100 | 78/100 | 75/100 |
| 100 | 164/200 | 163/200 | 165/200 |

**offense**

| season | split | pool | ours | Paine (eRO) | ρ ours | ρ Paine (eRO) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 84/100 | 80/100 | +0.913 | +0.866 |
| 2014-15 | Regular season | 246 | 87/100 | 80/100 | +0.918 | +0.858 |
| **all** | | | **171/200** | **160/200** |  |  |

Precision@K for offense, summed over 2 cells:

| K | ours | Paine (eRO) |
|---|---|---|
| 10 | 16/20 | 17/20 |
| 25 | 38/50 | 40/50 |
| 50 | 79/100 | 76/100 |
| 100 | 171/200 | 160/200 |

**defense**

| season | split | pool | ours | Paine (eRD) | ρ ours | ρ Paine (eRD) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 245 | 81/100 | 76/100 | +0.869 | +0.794 |
| 2014-15 | Regular season | 246 | 77/100 | 73/100 | +0.814 | +0.801 |
| **all** | | | **158/200** | **149/200** |  |  |

Precision@K for defense, summed over 2 cells:

| K | ours | Paine (eRD) |
|---|---|---|
| 10 | 11/20 | 11/20 |
| 25 | 34/50 | 31/50 |
| 50 | 69/100 | 68/100 |
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
summing our two part-models are near-interchangeable: R² +0.743 vs +0.717, ρ +0.881 vs +0.860, hits@100 164/200 vs 163/200.

**Against Paine on the total.** R² +0.743 vs +0.683, RMSE 1.746 vs 1.938, ρ +0.881 vs +0.846; hits@100 164/200 vs 165/200.

**Offense.** ours R² +0.818 / ρ +0.904 / hits@100 171/200; Paine R² +0.707 / ρ +0.825 / hits@100 160/200.

**Defense.** ours R² +0.583 / ρ +0.793 / hits@100 158/200; Paine R² +0.504 / ρ +0.727 / hits@100 149/200.

Read the precision@K tables above rather than a single cutoff: they show
where each system's advantage actually lives, and a hits count at one
arbitrary K is decided by hundredths of a point among near-tied players.

## Leaderboards

`[n]` after a predicted name is that player's *true* rank; ✓ means they are
genuinely in the true top 100.

### 2013-14 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+11.00) | LeBron James (+6.60) [15] ✓ | LeBron James (+7.14) [15] ✓ | Chris Paul (+8.79) [1] ✓ |
| 2 | **Kevin Durant** (+7.10) | Kevin Durant (+6.40) [2] ✓ | Chris Paul (+6.96) [1] ✓ | Kevin Durant (+7.21) [2] ✓ |
| 3 | **Kawhi Leonard** (+6.70) | Chris Paul (+6.04) [1] ✓ | Kevin Durant (+6.59) [2] ✓ | LeBron James (+6.77) [15] ✓ |
| 4 | **Kevin Love** (+6.60) | Kevin Love (+5.14) [4] ✓ | Paul George (+5.62) [8] ✓ | Kawhi Leonard (+5.95) [3] ✓ |
| 5 | **James Harden** (+6.10) | Kawhi Leonard (+5.08) [3] ✓ | Kyle Lowry (+4.85) [7] ✓ | James Harden (+5.50) [5] ✓ |
| 6 | **Joakim Noah** (+5.90) | Paul George (+4.97) [8] ✓ | Kevin Love (+4.83) [4] ✓ | Paul George (+5.08) [8] ✓ |
| 7 | **Kyle Lowry** (+5.70) | Blake Griffin (+4.65) [34] ✓ | Manu Ginobili (+4.77) [9] ✓ | Manu Ginobili (+4.99) [9] ✓ |
| 8 | **Paul George** (+5.60) | Manu Ginobili (+4.64) [9] ✓ | Dirk Nowitzki (+4.35) [13] ✓ | Kevin Love (+4.86) [4] ✓ |
| 9 | **Manu Ginobili** (+5.10) | Andrew Bogut (+4.45) [36] ✓ | Goran Dragic (+4.29) [10] ✓ | Goran Dragic (+4.71) [10] ✓ |
| 10 | **Goran Dragic** (+5.00) | Kyle Lowry (+4.44) [7] ✓ | Anthony Davis (+4.23) [28] ✓ | Andre Iguodala (+4.53) [21] ✓ |
| 11 | **DeMarcus Cousins** (+5.00) | James Harden (+4.30) [5] ✓ | Andrew Bogut (+4.22) [36] ✓ | Kyle Lowry (+4.27) [7] ✓ |
| 12 | **Patty Mills** (+4.80) | Andre Iguodala (+4.21) [21] ✓ | Andre Iguodala (+4.20) [21] ✓ | Joakim Noah (+4.22) [6] ✓ |
| 13 | **Dirk Nowitzki** (+4.70) | Dirk Nowitzki (+4.16) [13] ✓ | James Harden (+4.15) [5] ✓ | Russell Westbrook (+4.18) [25] ✓ |
| 14 | **Danny Green** (+4.70) | Goran Dragic (+3.86) [10] ✓ | Mike Conley (+3.96) [21] ✓ | Anthony Davis (+4.18) [28] ✓ |
| 15 | **LeBron James** (+4.60) | Carmelo Anthony (+3.82) [21] ✓ | Blake Griffin (+3.94) [34] ✓ | Blake Griffin (+4.00) [34] ✓ |
| 16 | **Anderson Varejao** (+4.10) | Anthony Davis (+3.68) [28] ✓ | Kawhi Leonard (+3.91) [3] ✓ | Ricky Rubio (+3.91) [25] ✓ |
| 17 | **Patrick Beverley** (+4.10) | Russell Westbrook (+3.50) [25] ✓ | LaMarcus Aldridge (+3.90) [29] ✓ | Brandan Wright (+3.89) [115] ✗ |
| 18 | **Mario Chalmers** (+4.00) | Patty Mills (+3.41) [12] ✓ | Paul Millsap (+3.55) [36] ✓ | Dirk Nowitzki (+3.81) [13] ✓ |
| 19 | **Jimmy Butler** (+3.90) | Isaiah Thomas (+3.24) [19] ✓ | Russell Westbrook (+3.49) [25] ✓ | DeMarcus Cousins (+3.60) [10] ✓ |
| 20 | **Isaiah Thomas** (+3.90) | Mike Conley (+3.18) [21] ✓ | Patty Mills (+3.43) [12] ✓ | Patty Mills (+3.50) [12] ✓ |
| 21 | **Carmelo Anthony** (+3.80) | Derek Fisher (+3.15) [50] ✓ | Joakim Noah (+3.35) [6] ✓ | Jimmy Butler (+3.31) [19] ✓ |
| 22 | **Kemba Walker** (+3.80) | Ricky Rubio (+3.11) [25] ✓ | Isaiah Thomas (+3.29) [19] ✓ | Carmelo Anthony (+3.28) [21] ✓ |
| 23 | **Mike Conley** (+3.80) | Joakim Noah (+3.10) [6] ✓ | Jimmy Butler (+3.29) [19] ✓ | DeAndre Jordan (+3.19) [62] ✓ |
| 24 | **Andre Iguodala** (+3.80) | Chris Bosh (+3.09) [93] ✓ | Carmelo Anthony (+3.28) [21] ✓ | LaMarcus Aldridge (+3.12) [29] ✓ |
| 25 | **Ricky Rubio** (+3.70) | LaMarcus Aldridge (+3.07) [29] ✓ | Nicolas Batum (+3.27) [59] ✓ | Mike Conley (+3.12) [21] ✓ |
| 26 | **Eric Bledsoe** (+3.70) | Draymond Green (+2.98) [29] ✓ | Ricky Rubio (+3.21) [25] ✓ | Trevor Ariza (+3.03) [66] ✓ |
| 27 | **Russell Westbrook** (+3.70) | Paul Millsap (+2.96) [36] ✓ | Al Jefferson (+3.13) [78] ✓ | Dwyane Wade (+3.01) [83] ✓ |
| 28 | **Anthony Davis** (+3.50) | Jimmy Butler (+2.92) [19] ✓ | Nikola Pekovic (+3.00) [31] ✓ | Al Jefferson (+2.99) [78] ✓ |
| 29 | **LaMarcus Aldridge** (+3.40) | Pablo Prigioni (+2.85) [70] ✓ | Deron Williams (+2.96) [34] ✓ | Tony Allen (+2.97) [46] ✓ |
| 30 | **Draymond Green** (+3.40) | Paul Pierce (+2.84) [59] ✓ | Damian Lillard (+2.91) [53] ✓ | Paul Millsap (+2.91) [36] ✓ |
| 31 | **DeMarre Carroll** (+3.30) | Dwight Howard (+2.82) [57] ✓ | DeMarcus Cousins (+2.85) [10] ✓ | Chris Bosh (+2.91) [93] ✓ |
| 32 | **Nikola Pekovic** (+3.30) | DeAndre Jordan (+2.79) [62] ✓ | Danny Green (+2.75) [13] ✓ | Deron Williams (+2.86) [34] ✓ |
| 33 | **Tiago Splitter** (+3.30) | Deron Williams (+2.79) [34] ✓ | Derek Fisher (+2.75) [50] ✓ | David West (+2.83) [51] ✓ |
| 34 | **Blake Griffin** (+3.20) | Anderson Varejao (+2.78) [16] ✓ | George Hill (+2.74) [48] ✓ | Anderson Varejao (+2.81) [16] ✓ |
| 35 | **Deron Williams** (+3.20) | Al Jefferson (+2.78) [78] ✓ | Anderson Varejao (+2.71) [16] ✓ | Andre Drummond (+2.77) [74] ✓ |
| 36 | **Paul Millsap** (+3.10) | Eric Bledsoe (+2.73) [25] ✓ | Chris Bosh (+2.71) [93] ✓ | Andrew Bogut (+2.74) [36] ✓ |
| 37 | **Andrew Bogut** (+3.10) | Kemba Walker (+2.72) [21] ✓ | Kemba Walker (+2.66) [21] ✓ | John Wall (+2.73) [66] ✓ |
| 38 | **Kris Humphries** (+3.00) | DeMarcus Cousins (+2.69) [10] ✓ | Draymond Green (+2.65) [29] ✓ | Isaiah Thomas (+2.68) [19] ✓ |
| 39 | **Klay Thompson** (+2.90) | Nicolas Batum (+2.68) [59] ✓ | Mario Chalmers (+2.57) [18] ✓ | Danny Green (+2.63) [13] ✓ |
| 40 | **Robin Lopez** (+2.90) | Damian Lillard (+2.63) [53] ✓ | Pablo Prigioni (+2.56) [70] ✓ | Wesley Matthews (+2.56) [46] ✓ |
| 41 | **Ty Lawson** (+2.90) | David West (+2.55) [51] ✓ | Wesley Matthews (+2.54) [46] ✓ | Corey Brewer (+2.50) [89] ✓ |
| 42 | **Vince Carter** (+2.90) | Mario Chalmers (+2.52) [18] ✓ | Marcin Gortat (+2.52) [57] ✓ | Nicolas Batum (+2.49) [59] ✓ |
| 43 | **Jae Crowder** (+2.90) | Trevor Ariza (+2.39) [66] ✓ | David West (+2.42) [51] ✓ | DeMarre Carroll (+2.43) [31] ✓ |
| 44 | **Darren Collison** (+2.70) | Kyle Korver (+2.36) [78] ✓ | Paul Pierce (+2.39) [59] ✓ | Chandler Parsons (+2.38) [62] ✓ |
| 45 | **Shane Battier** (+2.70) | Robin Lopez (+2.35) [39] ✓ | Patrick Beverley (+2.33) [16] ✓ | Klay Thompson (+2.36) [39] ✓ |
| 46 | **Wesley Matthews** (+2.60) | Amir Johnson (+2.33) [89] ✓ | Dwyane Wade (+2.29) [83] ✓ | Dwight Howard (+2.35) [57] ✓ |
| 47 | **Tony Allen** (+2.60) | George Hill (+2.27) [48] ✓ | Dwight Howard (+2.23) [57] ✓ | Ty Lawson (+2.35) [39] ✓ |
| 48 | **George Hill** (+2.50) | Danny Green (+2.26) [13] ✓ | Tiago Splitter (+2.14) [31] ✓ | DeMar DeRozan (+2.26) [107] ✗ |
| 49 | **Channing Frye** (+2.40) | Dwyane Wade (+2.23) [83] ✓ | Brandan Wright (+2.09) [115] ✗ | Tim Duncan (+2.20) [66] ✓ |
| 50 | **Derek Fisher** (+2.30) | Nikola Pekovic (+2.19) [31] ✓ | Channing Frye (+2.08) [49] ✓ | Eric Bledsoe (+2.17) [25] ✓ |
| 51 | **David West** (+2.20) | John Wall (+2.13) [66] ✓ | Tim Duncan (+2.08) [66] ✓ | Damian Lillard (+2.15) [53] ✓ |
| 52 | **Jrue Holiday** (+2.20) | Wesley Matthews (+2.12) [46] ✓ | David Lee (+2.06) [70] ✓ | David Lee (+2.09) [70] ✓ |
| 53 | **Damian Lillard** (+2.10) | Marcin Gortat (+2.10) [57] ✓ | Trevor Ariza (+2.06) [66] ✓ | Nikola Pekovic (+2.08) [31] ✓ |
| 54 | **Michael KiddGilchrist** (+2.10) | Kirk Hinrich (+2.05) [107] ✗ | Robin Lopez (+2.04) [39] ✓ | Robin Lopez (+2.04) [39] ✓ |
| 55 | **Chris Andersen** (+2.10) | David Lee (+2.03) [70] ✓ | Rudy Gay (+2.04) [100] ✗ | Serge Ibaka (+1.97) [59] ✓ |
| 56 | **CJ Watson** (+2.10) | Nene (+1.98) [70] ✓ | DeAndre Jordan (+1.96) [62] ✓ | George Hill (+1.95) [48] ✓ |
| 57 | **Marcin Gortat** (+2.00) | Patrick Beverley (+1.94) [16] ✓ | CJ Watson (+1.88) [53] ✓ | Kyle Korver (+1.94) [78] ✓ |
| 58 | **Dwight Howard** (+2.00) | Chris Andersen (+1.90) [53] ✓ | Nick Collison (+1.87) [83] ✓ | Terrence Jones (+1.72) [152] ✗ |
| 59 | **Nicolas Batum** (+1.90) | DeMarre Carroll (+1.87) [31] ✓ | Eric Bledsoe (+1.86) [25] ✓ | Jae Crowder (+1.68) [39] ✓ |
| 60 | **Serge Ibaka** (+1.90) | Jrue Holiday (+1.77) [51] ✓ | Andre Drummond (+1.84) [74] ✓ | Lance Stephenson (+1.66) [135] ✗ |
| 61 | **Paul Pierce** (+1.90) | Shaun Livingston (+1.76) [87] ✓ | DeMarre Carroll (+1.83) [31] ✓ | Marc Gasol (+1.66) [62] ✓ |
| 62 | **DeAndre Jordan** (+1.80) | Tony Allen (+1.69) [46] ✓ | Amir Johnson (+1.82) [89] ✓ | Draymond Green (+1.65) [29] ✓ |
| 63 | **Chandler Parsons** (+1.80) | Vince Carter (+1.68) [39] ✓ | Jrue Holiday (+1.78) [51] ✓ | Marcin Gortat (+1.63) [57] ✓ |
| 64 | **Roy Hibbert** (+1.80) | Iman Shumpert (+1.67) [97] ✓ | Darren Collison (+1.76) [44] ✓ | Paul Pierce (+1.60) [59] ✓ |
| 65 | **Marc Gasol** (+1.80) | Andray Blatche (+1.53) [118] ✗ | Jeremy Lamb (+1.72) [102] ✗ | Kemba Walker (+1.58) [21] ✓ |
| 66 | **John Wall** (+1.70) | Brandan Wright (+1.53) [115] ✗ | Nene (+1.69) [70] ✓ | Marco Belinelli (+1.57) [74] ✓ |
| 67 | **Trevor Ariza** (+1.70) | Jae Crowder (+1.51) [39] ✓ | John Wall (+1.68) [66] ✓ | CJ Watson (+1.54) [53] ✓ |
| 68 | **PJ Tucker** (+1.70) | Nick Collison (+1.51) [83] ✓ | Jae Crowder (+1.68) [39] ✓ | Chris Andersen (+1.50) [53] ✓ |
| 69 | **Tim Duncan** (+1.70) | CJ Watson (+1.47) [53] ✓ | Chris Andersen (+1.66) [53] ✓ | Patrick Beverley (+1.46) [16] ✓ |
| 70 | **David Lee** (+1.60) | Mike Dunleavy (+1.46) [107] ✗ | Vince Carter (+1.63) [39] ✓ | Josh McRoberts (+1.46) [89] ✓ |
| 71 | **Courtney Lee** (+1.60) | Shane Battier (+1.44) [44] ✓ | Josh McRoberts (+1.62) [89] ✓ | Kyrie Irving (+1.43) [83] ✓ |
| 72 | **Nene** (+1.60) | Patrick Patterson (+1.40) [107] ✗ | Shaun Livingston (+1.53) [87] ✓ | Tiago Splitter (+1.43) [31] ✓ |
| 73 | **Pablo Prigioni** (+1.60) | Anthony Tolliver (+1.37) [118] ✗ | Tony Allen (+1.50) [46] ✓ | Pablo Prigioni (+1.43) [70] ✓ |
| 74 | **Andre Drummond** (+1.50) | Jeremy Lamb (+1.35) [102] ✗ | Klay Thompson (+1.45) [39] ✓ | Derek Fisher (+1.36) [50] ✓ |
| 75 | **Jared Sullinger** (+1.50) | Corey Brewer (+1.35) [89] ✓ | Jared Sullinger (+1.45) [74] ✓ | Patrick Patterson (+1.33) [107] ✗ |
| 76 | **Marco Belinelli** (+1.50) | Tim Duncan (+1.34) [66] ✓ | Kyrie Irving (+1.44) [83] ✓ | Mario Chalmers (+1.32) [18] ✓ |
| 77 | **Matthew Dellavedova** (+1.50) | Zach Randolph (+1.33) [125] ✗ | DJ Augustin (+1.44) [131] ✗ | Jrue Holiday (+1.24) [51] ✓ |
| 78 | **Al Jefferson** (+1.30) | Tiago Splitter (+1.33) [31] ✓ | Victor Oladipo (+1.43) [152] ✗ | Mason Plumlee (+1.23) [198] ✗ |
| 79 | **Kyle Korver** (+1.30) | Channing Frye (+1.33) [49] ✓ | Anthony Tolliver (+1.43) [118] ✗ | Courtney Lee (+1.23) [70] ✓ |
| 80 | **Reggie Jackson** (+1.30) | Darren Collison (+1.32) [44] ✓ | Marc Gasol (+1.43) [62] ✓ | PJ Tucker (+1.08) [66] ✓ |
| 81 | **Jeremy Lin** (+1.30) | DJ Augustin (+1.31) [131] ✗ | Corey Brewer (+1.43) [89] ✓ | Jeremy Lamb (+1.03) [102] ✗ |
| 82 | **Jeremy Evans** (+1.30) | Josh McRoberts (+1.28) [89] ✓ | Chandler Parsons (+1.42) [62] ✓ | Amir Johnson (+1.02) [89] ✓ |
| 83 | **Kyrie Irving** (+1.20) | Marc Gasol (+1.15) [62] ✓ | Kenneth Faried (+1.39) [170] ✗ | Jamal Crawford (+0.95) [115] ✗ |
| 84 | **Dwyane Wade** (+1.20) | Terrence Jones (+1.13) [152] ✗ | Kyle Korver (+1.39) [78] ✓ | Kevin Martin (+0.94) [181] ✗ |
| 85 | **Nick Collison** (+1.20) | Roy Hibbert (+1.12) [62] ✓ | Zach Randolph (+1.34) [125] ✗ | Tony Parker (+0.92) [131] ✗ |
| 86 | **Nate Wolters** (+1.20) | Jeff Teague (+1.06) [122] ✗ | Nikola Vucevic (+1.34) [122] ✗ | Rudy Gay (+0.90) [100] ✗ |
| 87 | **Shaun Livingston** (+1.10) | Kenneth Faried (+0.97) [170] ✗ | Kirk Hinrich (+1.32) [107] ✗ | Mike Dunleavy (+0.88) [107] ✗ |
| 88 | **Nick Calathes** (+1.10) | Reggie Jackson (+0.95) [78] ✓ | Taj Gibson (+1.28) [149] ✗ | Nene (+0.87) [70] ✓ |
| 89 | **Corey Brewer** (+1.00) | Taj Gibson (+0.94) [149] ✗ | Reggie Jackson (+1.25) [78] ✓ | Kenneth Faried (+0.85) [170] ✗ |
| 90 | **Josh McRoberts** (+1.00) | Nick Calathes (+0.90) [87] ✓ | Patrick Patterson (+1.23) [107] ✗ | Andray Blatche (+0.82) [118] ✗ |
| 91 | **Amir Johnson** (+1.00) | PJ Tucker (+0.90) [66] ✓ | Andray Blatche (+1.19) [118] ✗ | Darren Collison (+0.79) [44] ✓ |
| 92 | **Boris Diaw** (+1.00) | Andre Drummond (+0.86) [74] ✓ | Terrence Jones (+1.14) [152] ✗ | DJ Augustin (+0.78) [131] ✗ |
| 93 | **Chris Bosh** (+0.90) | Jared Sullinger (+0.85) [74] ✓ | Josh Smith (+1.12) [169] ✗ | Monta Ellis (+0.73) [118] ✗ |
| 94 | **Luol Deng** (+0.90) | Klay Thompson (+0.83) [39] ✓ | Shane Battier (+1.09) [44] ✓ | Markieff Morris (+0.73) [152] ✗ |
| 95 | **Nick Young** (+0.90) | Chandler Parsons (+0.81) [62] ✓ | Greg Monroe (+1.03) [125] ✗ | Nikola Vucevic (+0.73) [122] ✗ |
| 96 | **Omri Casspi** (+0.80) | Tyson Chandler (+0.74) [140] ✗ | Jeff Teague (+1.01) [122] ✗ | Luol Deng (+0.70) [93] ✓ |
| 97 | **Bradley Beal** (+0.70) | Monta Ellis (+0.73) [118] ✗ | Mike Dunleavy (+0.98) [107] ✗ | Thabo Sefolosha (+0.69) [102] ✗ |
| 98 | **Randy Foye** (+0.70) | DeMar DeRozan (+0.69) [107] ✗ | Jeremy Lin (+0.97) [78] ✓ | Tyson Chandler (+0.66) [140] ✗ |
| 99 | **Iman Shumpert** (+0.70) | Markieff Morris (+0.66) [152] ✗ | PJ Tucker (+0.88) [66] ✓ | Boris Diaw (+0.65) [89] ✓ |
| 100 | **Gordon Hayward** (+0.60) | Rudy Gay (+0.65) [100] ✗ | Boris Diaw (+0.87) [89] ✓ | Nick Calathes (+0.65) [87] ✓ |

### 2014-15 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | LeBron James (+6.70) [11] ✓ | LeBron James (+7.00) [11] ✓ | Chris Paul (+8.27) [1] ✓ |
| 2 | **Kawhi Leonard** (+8.90) | Chris Paul (+6.16) [1] ✓ | Chris Paul (+6.72) [1] ✓ | Anthony Davis (+7.97) [3] ✓ |
| 3 | **Anthony Davis** (+8.80) | Anthony Davis (+6.06) [3] ✓ | Draymond Green (+6.10) [5] ✓ | Kawhi Leonard (+7.91) [2] ✓ |
| 4 | **James Harden** (+7.70) | Draymond Green (+5.71) [5] ✓ | Kawhi Leonard (+5.67) [2] ✓ | LeBron James (+6.66) [11] ✓ |
| 5 | **Draymond Green** (+6.50) | Kawhi Leonard (+5.64) [2] ✓ | Anthony Davis (+5.58) [3] ✓ | James Harden (+6.55) [4] ✓ |
| 6 | **Danny Green** (+6.10) | James Harden (+5.59) [4] ✓ | James Harden (+5.31) [4] ✓ | Russell Westbrook (+5.71) [7] ✓ |
| 7 | **Russell Westbrook** (+5.60) | Klay Thompson (+4.63) [10] ✓ | Andrew Bogut (+5.25) [23] ✓ | Jimmy Butler (+5.58) [34] ✓ |
| 8 | **George Hill** (+5.60) | Jimmy Butler (+4.53) [34] ✓ | Russell Westbrook (+5.14) [7] ✓ | George Hill (+5.08) [7] ✓ |
| 9 | **DeMarcus Cousins** (+5.40) | Russell Westbrook (+4.33) [7] ✓ | Jimmy Butler (+4.96) [34] ✓ | Klay Thompson (+4.74) [10] ✓ |
| 10 | **Klay Thompson** (+5.30) | DeMarcus Cousins (+4.29) [9] ✓ | Klay Thompson (+4.82) [10] ✓ | Tony Allen (+4.57) [16] ✓ |
| 11 | **LeBron James** (+5.10) | George Hill (+4.19) [7] ✓ | Zach Randolph (+4.68) [37] ✓ | Draymond Green (+4.55) [5] ✓ |
| 12 | **Khris Middleton** (+4.80) | Damian Lillard (+3.92) [42] ✓ | Kyrie Irving (+4.57) [13] ✓ | DeAndre Jordan (+4.31) [13] ✓ |
| 13 | **Kyrie Irving** (+4.60) | Lou Williams (+3.81) [34] ✓ | Pau Gasol (+4.56) [100] ✓ | Danny Green (+4.23) [6] ✓ |
| 14 | **DeAndre Jordan** (+4.60) | Khris Middleton (+3.65) [12] ✓ | George Hill (+4.11) [7] ✓ | Blake Griffin (+4.02) [60] ✓ |
| 15 | **Kyle Korver** (+4.60) | Kyrie Irving (+3.59) [13] ✓ | Rudy Gobert (+4.04) [19] ✓ | Paul Millsap (+4.01) [27] ✓ |
| 16 | **LaMarcus Aldridge** (+4.30) | Danny Green (+3.45) [6] ✓ | LaMarcus Aldridge (+4.00) [16] ✓ | Kyrie Irving (+4.00) [13] ✓ |
| 17 | **Tony Allen** (+4.30) | Gordon Hayward (+3.39) [26] ✓ | Danny Green (+3.80) [6] ✓ | Khris Middleton (+3.91) [12] ✓ |
| 18 | **Nikola Mirotic** (+4.20) | Isaiah Thomas (+3.38) [71] ✓ | Lou Williams (+3.75) [34] ✓ | John Wall (+3.82) [60] ✓ |
| 19 | **Rudy Gobert** (+4.10) | Andrew Bogut (+3.31) [23] ✓ | Isaiah Thomas (+3.70) [71] ✓ | Jeff Teague (+3.76) [42] ✓ |
| 20 | **Marc Gasol** (+4.00) | Wesley Matthews (+3.30) [24] ✓ | Damian Lillard (+3.68) [42] ✓ | Tim Duncan (+3.72) [27] ✓ |
| 21 | **Darren Collison** (+4.00) | Jeff Teague (+3.28) [42] ✓ | Blake Griffin (+3.64) [60] ✓ | Wesley Matthews (+3.64) [24] ✓ |
| 22 | **Kyle Lowry** (+3.90) | Kyle Lowry (+3.20) [22] ✓ | Manu Ginobili (+3.64) [29] ✓ | Lou Williams (+3.53) [34] ✓ |
| 23 | **Andrew Bogut** (+3.70) | Blake Griffin (+3.18) [60] ✓ | Khris Middleton (+3.60) [12] ✓ | Gordon Hayward (+3.29) [26] ✓ |
| 24 | **Wesley Matthews** (+3.60) | Rudy Gobert (+3.16) [19] ✓ | Jeff Teague (+3.59) [42] ✓ | Al Horford (+3.23) [60] ✓ |
| 25 | **Jonas Jerebko** (+3.60) | LaMarcus Aldridge (+2.97) [16] ✓ | Tony Allen (+3.59) [16] ✓ | DeMarcus Cousins (+3.15) [9] ✓ |
| 26 | **Gordon Hayward** (+3.40) | Manu Ginobili (+2.95) [29] ✓ | Nikola Mirotic (+3.53) [18] ✓ | Rudy Gobert (+3.08) [19] ✓ |
| 27 | **Paul Millsap** (+3.30) | Darren Collison (+2.87) [20] ✓ | Wesley Matthews (+3.50) [24] ✓ | Damian Lillard (+3.06) [42] ✓ |
| 28 | **Tim Duncan** (+3.30) | Marc Gasol (+2.80) [20] ✓ | Nene (+3.47) [109] ✗ | LaMarcus Aldridge (+2.97) [16] ✓ |
| 29 | **Kevin Love** (+3.20) | Nikola Mirotic (+2.80) [18] ✓ | Paul Millsap (+3.38) [27] ✓ | Brandan Wright (+2.85) [69] ✓ |
| 30 | **Marcin Gortat** (+3.20) | Paul Millsap (+2.80) [27] ✓ | DeMarcus Cousins (+3.36) [9] ✓ | Kyle Korver (+2.83) [13] ✓ |
| 31 | **JJ Redick** (+3.20) | Tony Allen (+2.76) [16] ✓ | Kyle Lowry (+3.28) [22] ✓ | Marc Gasol (+2.80) [20] ✓ |
| 32 | **Manu Ginobili** (+3.20) | DeAndre Jordan (+2.70) [13] ✓ | Gordon Hayward (+3.21) [26] ✓ | Jrue Holiday (+2.73) [42] ✓ |
| 33 | **Brandon Jennings** (+3.20) | Pau Gasol (+2.70) [100] ✓ | Tim Duncan (+3.06) [27] ✓ | Darren Collison (+2.52) [20] ✓ |
| 34 | **Jimmy Butler** (+3.00) | John Wall (+2.66) [60] ✓ | Mike Conley (+2.94) [37] ✓ | DeMarre Carroll (+2.51) [37] ✓ |
| 35 | **Lou Williams** (+3.00) | Tyson Chandler (+2.60) [47] ✓ | John Wall (+2.92) [60] ✓ | Derrick Favors (+2.50) [53] ✓ |
| 36 | **Danilo Gallinari** (+3.00) | Mike Conley (+2.59) [37] ✓ | Greg Monroe (+2.90) [124] ✗ | James Johnson (+2.50) [60] ✓ |
| 37 | **Eric Bledsoe** (+2.90) | Kyle Korver (+2.55) [13] ✓ | Danilo Gallinari (+2.85) [34] ✓ | Kyle Lowry (+2.47) [22] ✓ |
| 38 | **Zach Randolph** (+2.90) | Marcus Smart (+2.54) [56] ✓ | Darren Collison (+2.83) [20] ✓ | Marcin Gortat (+2.44) [29] ✓ |
| 39 | **Mike Conley** (+2.90) | Zach Randolph (+2.47) [37] ✓ | Jrue Holiday (+2.77) [42] ✓ | Anthony Morrow (+2.43) [42] ✓ |
| 40 | **DeMarre Carroll** (+2.90) | Jared Dudley (+2.40) [66] ✓ | Marc Gasol (+2.69) [20] ✓ | Ty Lawson (+2.40) [56] ✓ |
| 41 | **Kelly Olynyk** (+2.80) | Andre Iguodala (+2.39) [81] ✓ | Kyle Korver (+2.68) [13] ✓ | Kevin Love (+2.34) [29] ✓ |
| 42 | **Damian Lillard** (+2.70) | Danilo Gallinari (+2.30) [34] ✓ | Patrick Patterson (+2.58) [71] ✓ | Tyson Chandler (+2.28) [47] ✓ |
| 43 | **Jeff Teague** (+2.70) | Marcin Gortat (+2.29) [29] ✓ | Marcin Gortat (+2.42) [29] ✓ | Mike Conley (+2.25) [37] ✓ |
| 44 | **Anthony Morrow** (+2.70) | Kevin Love (+2.26) [29] ✓ | Kelly Olynyk (+2.40) [41] ✓ | Pau Gasol (+2.23) [100] ✓ |
| 45 | **Zaza Pachulia** (+2.70) | Jrue Holiday (+2.21) [42] ✓ | Al Jefferson (+2.35) [189] ✗ | Manu Ginobili (+2.08) [29] ✓ |
| 46 | **Jrue Holiday** (+2.70) | Iman Shumpert (+2.20) [66] ✓ | Jared Dudley (+2.32) [66] ✓ | Carmelo Anthony (+2.06) [81] ✓ |
| 47 | **Tyson Chandler** (+2.60) | Patrick Patterson (+2.18) [71] ✓ | Dirk Nowitzki (+2.26) [96] ✓ | Trevor Ariza (+2.00) [116] ✗ |
| 48 | **Serge Ibaka** (+2.60) | Monta Ellis (+2.11) [78] ✓ | Andre Iguodala (+2.26) [81] ✓ | JJ Redick (+1.99) [29] ✓ |
| 49 | **Devin Harris** (+2.50) | Nene (+2.08) [109] ✗ | Ersan Ilyasova (+2.24) [49] ✓ | Zach Randolph (+1.99) [37] ✓ |
| 50 | **Ersan Ilyasova** (+2.50) | Ersan Ilyasova (+2.07) [49] ✓ | Tyson Chandler (+2.15) [47] ✓ | Nikola Mirotic (+1.98) [18] ✓ |
| 51 | **Rudy Gay** (+2.40) | Amir Johnson (+2.05) [100] ✗ | Tyreke Evans (+2.15) [87] ✓ | Nicolas Batum (+1.97) [116] ✗ |
| 52 | **Kemba Walker** (+2.40) | CJ Miles (+2.04) [71] ✓ | Josh Smith (+2.14) [123] ✗ | Andrew Bogut (+1.96) [23] ✓ |
| 53 | **Derrick Favors** (+2.30) | Tim Duncan (+2.01) [27] ✓ | James Johnson (+2.14) [60] ✓ | Chandler Parsons (+1.93) [68] ✓ |
| 54 | **Joakim Noah** (+2.20) | Zaza Pachulia (+1.95) [42] ✓ | Jonas Jerebko (+2.10) [24] ✓ | Rudy Gay (+1.90) [51] ✓ |
| 55 | **Andre Roberson** (+2.20) | Eric Bledsoe (+1.92) [37] ✓ | Monta Ellis (+2.10) [78] ✓ | Greg Monroe (+1.83) [124] ✗ |
| 56 | **Ty Lawson** (+2.10) | Greg Monroe (+1.89) [124] ✗ | Zaza Pachulia (+2.06) [42] ✓ | Danilo Gallinari (+1.82) [34] ✓ |
| 57 | **Marcus Smart** (+2.10) | James Johnson (+1.88) [60] ✓ | Derrick Favors (+2.05) [53] ✓ | Monta Ellis (+1.82) [78] ✓ |
| 58 | **Jae Crowder** (+2.10) | Timofey Mozgov (+1.83) [78] ✓ | Iman Shumpert (+1.99) [66] ✓ | Brandon Jennings (+1.81) [29] ✓ |
| 59 | **AlFarouq Aminu** (+2.10) | Devin Harris (+1.79) [49] ✓ | Eric Bledsoe (+1.95) [37] ✓ | CJ Miles (+1.79) [71] ✓ |
| 60 | **John Wall** (+2.00) | Jae Crowder (+1.75) [56] ✓ | Amir Johnson (+1.90) [100] ✗ | Andre Iguodala (+1.78) [81] ✓ |
| 61 | **Blake Griffin** (+2.00) | Jonas Jerebko (+1.75) [24] ✓ | Cory Joseph (+1.88) [87] ✓ | Isaiah Thomas (+1.77) [71] ✓ |
| 62 | **Al Horford** (+2.00) | Luis Scola (+1.74) [105] ✗ | Jae Crowder (+1.87) [56] ✓ | Patrick Patterson (+1.75) [71] ✓ |
| 63 | **Michael KiddGilchrist** (+2.00) | Derrick Favors (+1.65) [53] ✓ | Luis Scola (+1.87) [105] ✗ | Kelly Olynyk (+1.73) [41] ✓ |
| 64 | **Cody Zeller** (+2.00) | Dwight Howard (+1.63) [96] ✓ | Jared Sullinger (+1.86) [92] ✓ | AlFarouq Aminu (+1.73) [56] ✓ |
| 65 | **James Johnson** (+2.00) | Ed Davis (+1.60) [71] ✓ | Kevin Love (+1.86) [29] ✓ | Jonas Jerebko (+1.63) [24] ✓ |
| 66 | **Jared Dudley** (+1.90) | Mike Dunleavy (+1.58) [116] ✗ | CJ Miles (+1.76) [71] ✓ | Jae Crowder (+1.62) [56] ✓ |
| 67 | **Iman Shumpert** (+1.90) | Tyreke Evans (+1.58) [87] ✓ | Ed Davis (+1.76) [71] ✓ | Tyreke Evans (+1.54) [87] ✓ |
| 68 | **Chandler Parsons** (+1.80) | JJ Redick (+1.57) [29] ✓ | Marcus Smart (+1.74) [56] ✓ | Bradley Beal (+1.44) [81] ✓ |
| 69 | **Luol Deng** (+1.70) | Aaron Brooks (+1.50) [162] ✗ | Dwight Howard (+1.74) [96] ✓ | Eric Bledsoe (+1.42) [37] ✓ |
| 70 | **Brandan Wright** (+1.70) | Derrick Rose (+1.45) [94] ✓ | Rudy Gay (+1.72) [51] ✓ | Paul Pierce (+1.41) [87] ✓ |
| 71 | **Matt Barnes** (+1.60) | Josh Smith (+1.44) [123] ✗ | Bradley Beal (+1.67) [81] ✓ | Zaza Pachulia (+1.39) [42] ✓ |
| 72 | **Patrick Patterson** (+1.60) | Al Horford (+1.42) [60] ✓ | Timofey Mozgov (+1.61) [78] ✓ | Michael KiddGilchrist (+1.34) [60] ✓ |
| 73 | **CJ Miles** (+1.60) | Joakim Noah (+1.41) [54] ✓ | Donatas Motiejunas (+1.58) [124] ✗ | Jared Dudley (+1.30) [66] ✓ |
| 74 | **Ed Davis** (+1.60) | Nicolas Batum (+1.41) [116] ✗ | Aaron Brooks (+1.58) [162] ✗ | Brook Lopez (+1.29) [96] ✓ |
| 75 | **Isaiah Thomas** (+1.60) | DeMarre Carroll (+1.34) [37] ✓ | Andre Roberson (+1.56) [54] ✓ | Goran Dragic (+1.25) [116] ✗ |
| 76 | **JJ Barea** (+1.60) | Kelly Olynyk (+1.33) [41] ✓ | DeAndre Jordan (+1.53) [13] ✓ | Dirk Nowitzki (+1.24) [96] ✓ |
| 77 | **Pablo Prigioni** (+1.60) | Cory Joseph (+1.31) [87] ✓ | Anthony Morrow (+1.44) [42] ✓ | CJ Watson (+1.24) [81] ✓ |
| 78 | **Monta Ellis** (+1.40) | Harrison Barnes (+1.30) [142] ✗ | Reggie Jackson (+1.44) [81] ✓ | Robert Covington (+1.24) [94] ✓ |
| 79 | **Timofey Mozgov** (+1.40) | Anthony Morrow (+1.29) [42] ✓ | Brandan Wright (+1.42) [69] ✓ | Andre Drummond (+1.22) [139] ✗ |
| 80 | **Jeremy Lin** (+1.40) | Brandon Jennings (+1.28) [29] ✓ | Brandon Knight (+1.32) [116] ✗ | Serge Ibaka (+1.21) [47] ✓ |
| 81 | **Reggie Jackson** (+1.30) | Andre Roberson (+1.23) [54] ✓ | Nicolas Batum (+1.29) [116] ✗ | Ed Davis (+1.18) [71] ✓ |
| 82 | **Bradley Beal** (+1.30) | Anthony Tolliver (+1.23) [154] ✗ | Devin Harris (+1.29) [49] ✓ | Dwight Howard (+1.17) [96] ✓ |
| 83 | **Andre Iguodala** (+1.30) | Chandler Parsons (+1.20) [68] ✓ | Andre Drummond (+1.27) [139] ✗ | Al Jefferson (+1.14) [189] ✗ |
| 84 | **Carmelo Anthony** (+1.30) | Matt Barnes (+1.19) [71] ✓ | Anthony Tolliver (+1.26) [154] ✗ | Harrison Barnes (+1.13) [142] ✗ |
| 85 | **CJ Watson** (+1.30) | Paul Pierce (+1.17) [87] ✓ | JJ Redick (+1.23) [29] ✓ | Kemba Walker (+1.10) [51] ✓ |
| 86 | **Amare Stoudemire** (+1.30) | CJ Watson (+1.16) [81] ✓ | Chandler Parsons (+1.18) [68] ✓ | Matt Barnes (+1.09) [71] ✓ |
| 87 | **Tyreke Evans** (+1.20) | Tristan Thompson (+1.14) [139] ✗ | DeMarre Carroll (+1.18) [37] ✓ | Thaddeus Young (+0.99) [96] ✓ |
| 88 | **Deron Williams** (+1.20) | Bradley Beal (+1.14) [81] ✓ | Carmelo Anthony (+1.18) [81] ✓ | Mike Dunleavy (+0.95) [116] ✗ |
| 89 | **Paul Pierce** (+1.20) | AlFarouq Aminu (+1.12) [56] ✓ | Goran Dragic (+1.15) [116] ✗ | Dwyane Wade (+0.94) [144] ✗ |
| 90 | **Patrick Beverley** (+1.20) | Reggie Jackson (+1.11) [81] ✓ | Robert Covington (+1.14) [94] ✓ | Luol Deng (+0.94) [69] ✓ |
| 91 | **Cory Joseph** (+1.20) | Michael KiddGilchrist (+1.11) [60] ✓ | Victor Oladipo (+1.11) [147] ✗ | Pablo Prigioni (+0.87) [71] ✓ |
| 92 | **Jonas Valanciunas** (+1.10) | Goran Dragic (+1.11) [116] ✗ | Harrison Barnes (+1.11) [142] ✗ | Devin Harris (+0.82) [49] ✓ |
| 93 | **Jared Sullinger** (+1.10) | Donatas Motiejunas (+0.99) [124] ✗ | Dwyane Wade (+1.07) [144] ✗ | Kenneth Faried (+0.80) [111] ✗ |
| 94 | **Robert Covington** (+1.00) | Omer Asik (+0.97) [113] ✗ | Paul Pierce (+1.07) [87] ✓ | Joakim Noah (+0.79) [54] ✓ |
| 95 | **Derrick Rose** (+1.00) | Trevor Ariza (+0.94) [116] ✗ | Al Horford (+1.06) [60] ✓ | Markieff Morris (+0.78) [135] ✗ |
| 96 | **Thaddeus Young** (+0.90) | Jared Sullinger (+0.93) [92] ✓ | Thaddeus Young (+1.06) [96] ✓ | David West (+0.77) [159] ✗ |
| 97 | **Dirk Nowitzki** (+0.90) | Taj Gibson (+0.88) [144] ✗ | Gerald Green (+1.06) [124] ✗ | DeMar DeRozan (+0.77) [167] ✗ |
| 98 | **Brook Lopez** (+0.90) | Kenneth Faried (+0.73) [111] ✗ | Joakim Noah (+1.04) [54] ✓ | PJ Tucker (+0.71) [100] ✗ |
| 99 | **Dwight Howard** (+0.90) | Andre Drummond (+0.72) [139] ✗ | Giannis Antetokounmpo (+0.92) [159] ✗ | Iman Shumpert (+0.69) [66] ✓ |
| 100 | **Pau Gasol** (+0.80) | Jonas Valanciunas (+0.72) [92] ✓ | Nikola Vucevic (+0.92) [175] ✗ | Luis Scola (+0.68) [105] ✗ |

### 2013-14 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Kevin Durant** (+7.60) | LeBron James (+6.71) [4] ✓ | Kevin Durant (+7.41) [1] ✓ |
| 2 | **Chris Paul** (+7.10) | Kevin Durant (+6.29) [1] ✓ | LeBron James (+6.80) [4] ✓ |
| 3 | **James Harden** (+6.30) | Chris Paul (+5.49) [2] ✓ | Chris Paul (+6.79) [2] ✓ |
| 4 | **LeBron James** (+5.80) | James Harden (+5.08) [3] ✓ | James Harden (+5.29) [3] ✓ |
| 5 | **Kevin Love** (+5.70) | Kevin Love (+4.29) [5] ✓ | Goran Dragic (+4.87) [6] ✓ |
| 6 | **Goran Dragic** (+4.80) | Damian Lillard (+3.99) [12] ✓ | Kevin Love (+4.76) [5] ✓ |
| 7 | **Kyle Lowry** (+4.40) | Kyle Lowry (+3.91) [7] ✓ | Dirk Nowitzki (+4.33) [7] ✓ |
| 8 | **Dirk Nowitzki** (+4.40) | Goran Dragic (+3.81) [6] ✓ | Brandan Wright (+4.19) [40] ✓ |
| 9 | **Carmelo Anthony** (+4.20) | Russell Westbrook (+3.62) [15] ✓ | Carmelo Anthony (+3.80) [9] ✓ |
| 10 | **Manu Ginobili** (+4.00) | Carmelo Anthony (+3.44) [9] ✓ | Kyle Lowry (+3.71) [7] ✓ |
| 11 | **Patty Mills** (+3.90) | Manu Ginobili (+3.28) [10] ✓ | Blake Griffin (+3.57) [17] ✓ |
| 12 | **Damian Lillard** (+3.60) | Isaiah Thomas (+3.19) [13] ✓ | Isaiah Thomas (+3.48) [13] ✓ |
| 13 | **Isaiah Thomas** (+3.50) | Mike Conley (+2.98) [13] ✓ | Manu Ginobili (+3.25) [10] ✓ |
| 14 | **Mike Conley** (+3.50) | Dirk Nowitzki (+2.73) [7] ✓ | Russell Westbrook (+3.24) [15] ✓ |
| 15 | **Russell Westbrook** (+3.30) | Blake Griffin (+2.66) [17] ✓ | Nikola Pekovic (+3.10) [64] ✓ |
| 16 | **Ty Lawson** (+3.20) | Paul George (+2.64) [22] ✓ | Damian Lillard (+3.03) [12] ✓ |
| 17 | **Blake Griffin** (+2.90) | Jamal Crawford (+2.46) [18] ✓ | Dwyane Wade (+2.82) [47] ✓ |
| 18 | **Wesley Matthews** (+2.80) | Patty Mills (+2.25) [11] ✓ | Ty Lawson (+2.80) [16] ✓ |
| 19 | **Marco Belinelli** (+2.80) | Kyrie Irving (+2.23) [27] ✓ | Mike Conley (+2.80) [13] ✓ |
| 20 | **Jamal Crawford** (+2.80) | Deron Williams (+2.05) [22] ✓ | Wesley Matthews (+2.68) [18] ✓ |
| 21 | **Joe Johnson** (+2.70) | DJ Augustin (+1.99) [31] ✓ | Patty Mills (+2.59) [11] ✓ |
| 22 | **Paul George** (+2.60) | John Wall (+1.91) [36] ✓ | Anthony Davis (+2.52) [57] ✓ |
| 23 | **Chandler Parsons** (+2.60) | Dwyane Wade (+1.90) [47] ✓ | Kawhi Leonard (+2.51) [40] ✓ |
| 24 | **Deron Williams** (+2.60) | Ricky Rubio (+1.72) [36] ✓ | Chandler Parsons (+2.47) [22] ✓ |
| 25 | **Vince Carter** (+2.40) | Ty Lawson (+1.67) [16] ✓ | Paul George (+2.46) [22] ✓ |
| 26 | **Nick Young** (+2.40) | Joe Johnson (+1.62) [21] ✓ | Deron Williams (+2.34) [22] ✓ |
| 27 | **Kyrie Irving** (+2.30) | Darren Collison (+1.62) [64] ✓ | DeMar DeRozan (+2.30) [40] ✓ |
| 28 | **Patrick Beverley** (+2.20) | Jeff Teague (+1.61) [90] ✓ | Jamal Crawford (+2.28) [18] ✓ |
| 29 | **Jrue Holiday** (+2.20) | Jrue Holiday (+1.60) [28] ✓ | Tony Parker (+2.17) [71] ✓ |
| 30 | **Brandon Jennings** (+2.20) | LaMarcus Aldridge (+1.58) [64] ✓ | DeMarcus Cousins (+2.15) [40] ✓ |
| 31 | **Klay Thompson** (+2.10) | Wesley Matthews (+1.55) [18] ✓ | Chris Bosh (+1.99) [136] ✗ |
| 32 | **Randy Foye** (+2.10) | DeMar DeRozan (+1.53) [40] ✓ | Marco Belinelli (+1.94) [18] ✓ |
| 33 | **DJ Augustin** (+2.10) | Brandon Knight (+1.41) [71] ✓ | Kyrie Irving (+1.93) [27] ✓ |
| 34 | **Channing Frye** (+2.00) | Andre Iguodala (+1.39) [57] ✓ | Andre Drummond (+1.91) [53] ✓ |
| 35 | **Josh McRoberts** (+2.00) | Kemba Walker (+1.35) [51] ✓ | Robin Lopez (+1.86) [68] ✓ |
| 36 | **Ricky Rubio** (+1.90) | Kevin Martin (+1.34) [83] ✓ | LaMarcus Aldridge (+1.86) [64] ✓ |
| 37 | **Nicolas Batum** (+1.90) | Marco Belinelli (+1.32) [18] ✓ | Andre Iguodala (+1.84) [57] ✓ |
| 38 | **John Wall** (+1.90) | George Hill (+1.32) [112] ✗ | Nicolas Batum (+1.84) [36] ✓ |
| 39 | **Kyle Korver** (+1.90) | Nikola Pekovic (+1.30) [64] ✓ | Eric Bledsoe (+1.67) [47] ✓ |
| 40 | **Kawhi Leonard** (+1.70) | Nicolas Batum (+1.30) [36] ✓ | Kevin Martin (+1.64) [83] ✓ |
| 41 | **DeMarcus Cousins** (+1.70) | Klay Thompson (+1.29) [31] ✓ | Joe Johnson (+1.61) [21] ✓ |
| 42 | **DeMar DeRozan** (+1.70) | Tony Parker (+1.27) [71] ✓ | Trevor Ariza (+1.61) [68] ✓ |
| 43 | **Pablo Prigioni** (+1.70) | Rudy Gay (+1.25) [64] ✓ | Jose Calderon (+1.61) [45] ✓ |
| 44 | **Brandan Wright** (+1.70) | Chandler Parsons (+1.25) [22] ✓ | Al Jefferson (+1.59) [149] ✗ |
| 45 | **Jose Calderon** (+1.60) | Vince Carter (+1.22) [25] ✓ | John Wall (+1.57) [36] ✓ |
| 46 | **Mirza Teletovic** (+1.60) | Mario Chalmers (+1.21) [47] ✓ | Nick Young (+1.55) [25] ✓ |
| 47 | **Joakim Noah** (+1.50) | Brandan Wright (+1.20) [40] ✓ | Klay Thompson (+1.45) [31] ✓ |
| 48 | **Mario Chalmers** (+1.50) | Jose Calderon (+1.17) [45] ✓ | Tyreke Evans (+1.40) [57] ✓ |
| 49 | **Eric Bledsoe** (+1.50) | Nick Young (+1.16) [25] ✓ | David Lee (+1.33) [112] ✗ |
| 50 | **Dwyane Wade** (+1.50) | Randy Foye (+1.10) [31] ✓ | DJ Augustin (+1.28) [31] ✓ |
| 51 | **Kemba Walker** (+1.40) | Bradley Beal (+1.07) [112] ✗ | Arron Afflalo (+1.21) [57] ✓ |
| 52 | **Ray Allen** (+1.40) | Joakim Noah (+1.06) [47] ✓ | Terrence Jones (+1.19) [71] ✓ |
| 53 | **Andre Drummond** (+1.30) | Kenneth Faried (+1.05) [57] ✓ | Joakim Noah (+1.17) [47] ✓ |
| 54 | **Zach Randolph** (+1.30) | Eric Bledsoe (+1.04) [47] ✓ | Kyle Korver (+1.16) [36] ✓ |
| 55 | **Gerald Green** (+1.30) | Kawhi Leonard (+0.99) [40] ✓ | Jodie Meeks (+1.14) [83] ✓ |
| 56 | **Anthony Morrow** (+1.30) | Kyle Korver (+0.96) [36] ✓ | Gerald Green (+1.13) [53] ✓ |
| 57 | **Anthony Davis** (+1.20) | Pablo Prigioni (+0.96) [40] ✓ | Ricky Rubio (+1.12) [36] ✓ |
| 58 | **Andre Iguodala** (+1.20) | Arron Afflalo (+0.95) [57] ✓ | Jrue Holiday (+1.12) [28] ✓ |
| 59 | **Tyreke Evans** (+1.20) | Lance Stephenson (+0.92) [90] ✓ | Corey Brewer (+1.12) [112] ✗ |
| 60 | **Kenneth Faried** (+1.20) | David West (+0.91) [90] ✓ | Monta Ellis (+1.12) [68] ✓ |
| 61 | **Arron Afflalo** (+1.20) | Reggie Jackson (+0.89) [71] ✓ | Paul Pierce (+1.10) [126] ✗ |
| 62 | **Jameer Nelson** (+1.20) | Jameer Nelson (+0.85) [57] ✓ | Jeff Teague (+1.08) [90] ✓ |
| 63 | **Lou Williams** (+1.20) | Brandon Jennings (+0.84) [28] ✓ | Darren Collison (+1.08) [64] ✓ |
| 64 | **LaMarcus Aldridge** (+1.10) | Jimmy Butler (+0.81) [97] ✓ | DeMarre Carroll (+1.07) [83] ✓ |
| 65 | **Darren Collison** (+1.10) | Greivis Vasquez (+0.81) [112] ✗ | Pablo Prigioni (+1.05) [40] ✓ |
| 66 | **Nikola Pekovic** (+1.10) | Gordon Hayward (+0.75) [83] ✓ | Rudy Gay (+1.04) [64] ✓ |
| 67 | **Rudy Gay** (+1.10) | Mike Dunleavy (+0.74) [112] ✗ | Vince Carter (+1.01) [25] ✓ |
| 68 | **Robin Lopez** (+1.00) | Anthony Morrow (+0.72) [53] ✓ | Dwight Howard (+1.00) [144] ✗ |
| 69 | **Trevor Ariza** (+1.00) | Monta Ellis (+0.72) [68] ✓ | Mason Plumlee (+0.99) [176] ✗ |
| 70 | **Monta Ellis** (+1.00) | Patrick Beverley (+0.69) [28] ✓ | Gordon Hayward (+0.93) [83] ✓ |
| 71 | **Reggie Jackson** (+0.90) | Josh McRoberts (+0.68) [34] ✓ | Markieff Morris (+0.89) [129] ✗ |
| 72 | **Alec Burks** (+0.90) | David Lee (+0.67) [112] ✗ | Luol Deng (+0.88) [112] ✗ |
| 73 | **Matthew Dellavedova** (+0.90) | Anthony Davis (+0.66) [57] ✓ | Anthony Morrow (+0.87) [53] ✓ |
| 74 | **Tony Parker** (+0.90) | Trevor Ariza (+0.66) [68] ✓ | Amare Stoudemire (+0.86) [166] ✗ |
| 75 | **Shelvin Mack** (+0.90) | Channing Frye (+0.65) [34] ✓ | Kenneth Faried (+0.85) [57] ✓ |
| 76 | **Terrence Jones** (+0.90) | Jeremy Lamb (+0.63) [97] ✗ | Brandon Jennings (+0.84) [28] ✓ |
| 77 | **Brandon Knight** (+0.90) | Gerald Green (+0.63) [53] ✓ | Randy Foye (+0.78) [31] ✓ |
| 78 | **Martell Webster** (+0.90) | Trey Burke (+0.59) [109] ✗ | Anderson Varejao (+0.74) [97] ✓ |
| 79 | **PJ Tucker** (+0.80) | Paul Millsap (+0.57) [109] ✗ | Eric Gordon (+0.72) [90] ✓ |
| 80 | **Boris Diaw** (+0.80) | Zach Randolph (+0.56) [53] ✓ | Paul Millsap (+0.71) [109] ✗ |
| 81 | **Matt Barnes** (+0.80) | DeMarcus Cousins (+0.56) [40] ✓ | Alec Burks (+0.68) [71] ✓ |
| 82 | **Marvin Williams** (+0.80) | Andre Drummond (+0.55) [53] ✓ | Greivis Vasquez (+0.63) [112] ✗ |
| 83 | **DeMarre Carroll** (+0.70) | Ray Allen (+0.52) [51] ✓ | Marc Gasol (+0.61) [176] ✗ |
| 84 | **DeAndre Jordan** (+0.70) | Al Jefferson (+0.50) [149] ✗ | Tiago Splitter (+0.58) [166] ✗ |
| 85 | **Danny Green** (+0.70) | Mirza Teletovic (+0.49) [45] ✓ | Courtney Lee (+0.54) [83] ✓ |
| 86 | **Gordon Hayward** (+0.70) | Alec Burks (+0.48) [71] ✓ | Josh McRoberts (+0.51) [34] ✓ |
| 87 | **Courtney Lee** (+0.70) | Chris Bosh (+0.47) [136] ✗ | David West (+0.50) [90] ✓ |
| 88 | **Jodie Meeks** (+0.70) | Boris Diaw (+0.39) [79] ✓ | Kemba Walker (+0.50) [51] ✓ |
| 89 | **Kevin Martin** (+0.70) | Anthony Tolliver (+0.37) [97] ✗ | Patrick Beverley (+0.46) [28] ✓ |
| 90 | **David West** (+0.60) | Eric Gordon (+0.34) [90] ✓ | PJ Tucker (+0.45) [79] ✓ |
| 91 | **Jeff Teague** (+0.60) | Dion Waiters (+0.33) [149] ✗ | Reggie Jackson (+0.37) [71] ✓ |
| 92 | **Lance Stephenson** (+0.60) | Corey Brewer (+0.31) [112] ✗ | Greg Monroe (+0.36) [112] ✗ |
| 93 | **Marcus Thornton** (+0.60) | Derek Fisher (+0.30) [112] ✗ | Ramon Sessions (+0.35) [90] ✓ |
| 94 | **Ramon Sessions** (+0.60) | Lou Williams (+0.28) [57] ✓ | Matthew Dellavedova (+0.33) [71] ✓ |
| 95 | **Mike Miller** (+0.60) | Mike Miller (+0.22) [90] ✓ | Jeremy Lamb (+0.33) [97] ✗ |
| 96 | **Eric Gordon** (+0.60) | Terrence Ross (+0.22) [97] ✓ | Tim Hardaway Jr. (+0.33) [129] ✗ |
| 97 | **Jimmy Butler** (+0.50) | Jerryd Bayless (+0.17) [126] ✗ | Chris Andersen (+0.31) [97] ✗ |
| 98 | **Anderson Varejao** (+0.50) | Ramon Sessions (+0.17) [90] ✓ | Brandon Knight (+0.30) [71] ✓ |
| 99 | **Jared Sullinger** (+0.50) | Tyreke Evans (+0.17) [57] ✓ | Bradley Beal (+0.29) [112] ✗ |
| 100 | **Terrence Ross** (+0.50) | Jeremy Lin (+0.17) [149] ✗ | Marcin Gortat (+0.28) [156] ✗ |

### 2014-15 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+8.50) | Chris Paul (+6.49) [1] ✓ | Chris Paul (+6.99) [1] ✓ |
| 2 | **James Harden** (+7.70) | James Harden (+6.02) [2] ✓ | James Harden (+5.71) [2] ✓ |
| 3 | **Russell Westbrook** (+6.10) | LeBron James (+5.77) [5] ✓ | LeBron James (+5.62) [5] ✓ |
| 4 | **Kyrie Irving** (+5.50) | Kyrie Irving (+5.00) [4] ✓ | Anthony Davis (+5.11) [9] ✓ |
| 5 | **LeBron James** (+5.30) | Russell Westbrook (+4.95) [3] ✓ | Russell Westbrook (+5.02) [3] ✓ |
| 6 | **Lou Williams** (+5.20) | Isaiah Thomas (+4.51) [8] ✓ | Jimmy Butler (+4.38) [20] ✓ |
| 7 | **Kyle Korver** (+4.60) | Damian Lillard (+4.22) [11] ✓ | Blake Griffin (+4.19) [20] ✓ |
| 8 | **Isaiah Thomas** (+4.50) | Klay Thompson (+3.88) [9] ✓ | Kyrie Irving (+4.13) [4] ✓ |
| 9 | **Anthony Davis** (+4.30) | Lou Williams (+3.73) [6] ✓ | Lou Williams (+4.08) [6] ✓ |
| 10 | **Klay Thompson** (+4.30) | Blake Griffin (+3.33) [20] ✓ | Klay Thompson (+4.00) [9] ✓ |
| 11 | **Damian Lillard** (+4.00) | Gordon Hayward (+2.90) [20] ✓ | George Hill (+3.81) [12] ✓ |
| 12 | **George Hill** (+3.90) | George Hill (+2.81) [12] ✓ | Kawhi Leonard (+3.44) [15] ✓ |
| 13 | **Ty Lawson** (+3.80) | Jimmy Butler (+2.78) [20] ✓ | JJ Redick (+3.36) [29] ✓ |
| 14 | **Carmelo Anthony** (+3.80) | Kyle Lowry (+2.63) [18] ✓ | Ty Lawson (+3.18) [13] ✓ |
| 15 | **Kawhi Leonard** (+3.70) | Jeff Teague (+2.59) [34] ✓ | Gordon Hayward (+3.01) [20] ✓ |
| 16 | **Rudy Gay** (+3.50) | Anthony Davis (+2.53) [9] ✓ | Isaiah Thomas (+2.98) [8] ✓ |
| 17 | **DeAndre Jordan** (+3.40) | Dwyane Wade (+2.39) [40] ✓ | Carmelo Anthony (+2.96) [13] ✓ |
| 18 | **Kyle Lowry** (+3.30) | Jrue Holiday (+2.36) [18] ✓ | Damian Lillard (+2.92) [11] ✓ |
| 19 | **Jrue Holiday** (+3.30) | John Wall (+2.35) [37] ✓ | Wesley Matthews (+2.78) [33] ✓ |
| 20 | **Gordon Hayward** (+3.20) | JJ Redick (+2.31) [29] ✓ | Brandon Jennings (+2.74) [23] ✓ |
| 21 | **Jimmy Butler** (+3.20) | Mike Conley (+2.30) [30] ✓ | Anthony Morrow (+2.73) [26] ✓ |
| 22 | **Blake Griffin** (+3.20) | Aaron Brooks (+2.26) [55] ✓ | Jeff Teague (+2.72) [34] ✓ |
| 23 | **Danny Green** (+3.10) | Tyreke Evans (+2.23) [27] ✓ | Rudy Gay (+2.65) [16] ✓ |
| 24 | **Brandon Jennings** (+3.10) | Carmelo Anthony (+2.16) [13] ✓ | Kyle Lowry (+2.48) [18] ✓ |
| 25 | **Danilo Gallinari** (+2.80) | Kyle Korver (+2.16) [7] ✓ | Al Horford (+2.33) [96] ✓ |
| 26 | **Anthony Morrow** (+2.70) | Kawhi Leonard (+2.10) [15] ✓ | LaMarcus Aldridge (+2.33) [30] ✓ |
| 27 | **Tyreke Evans** (+2.60) | LaMarcus Aldridge (+2.09) [30] ✓ | Goran Dragic (+2.33) [42] ✓ |
| 28 | **Chandler Parsons** (+2.60) | Brandon Jennings (+1.94) [23] ✓ | Darren Collison (+2.31) [49] ✓ |
| 29 | **JJ Redick** (+2.50) | Ty Lawson (+1.91) [13] ✓ | Dirk Nowitzki (+2.24) [34] ✓ |
| 30 | **LaMarcus Aldridge** (+2.40) | Reggie Jackson (+1.83) [40] ✓ | Dwyane Wade (+2.22) [40] ✓ |
| 31 | **Mike Conley** (+2.40) | Rudy Gay (+1.81) [16] ✓ | Jrue Holiday (+2.21) [18] ✓ |
| 32 | **Patrick Patterson** (+2.40) | Eric Gordon (+1.81) [87] ✓ | Chandler Parsons (+2.18) [27] ✓ |
| 33 | **Wesley Matthews** (+2.30) | Gerald Green (+1.69) [34] ✓ | Kyle Korver (+2.00) [7] ✓ |
| 34 | **Jeff Teague** (+2.20) | Darren Collison (+1.67) [49] ✓ | Danilo Gallinari (+1.99) [25] ✓ |
| 35 | **Dirk Nowitzki** (+2.20) | Danilo Gallinari (+1.66) [25] ✓ | Kevin Love (+1.92) [49] ✓ |
| 36 | **Gerald Green** (+2.20) | Khris Middleton (+1.64) [49] ✓ | Mike Conley (+1.91) [30] ✓ |
| 37 | **John Wall** (+2.10) | Manu Ginobili (+1.63) [49] ✓ | Brandan Wright (+1.89) [98] ✓ |
| 38 | **Devin Harris** (+2.10) | Jamal Crawford (+1.55) [67] ✓ | John Wall (+1.85) [37] ✓ |
| 39 | **Ersan Ilyasova** (+2.10) | Dirk Nowitzki (+1.53) [34] ✓ | Khris Middleton (+1.84) [49] ✓ |
| 40 | **Reggie Jackson** (+2.00) | Draymond Green (+1.37) [57] ✓ | DeMarre Carroll (+1.75) [42] ✓ |
| 41 | **Dwyane Wade** (+2.00) | Patrick Patterson (+1.36) [30] ✓ | Paul Millsap (+1.66) [69] ✓ |
| 42 | **DeMarre Carroll** (+1.90) | Goran Dragic (+1.35) [42] ✓ | Tyson Chandler (+1.60) [119] ✗ |
| 43 | **Nikola Mirotic** (+1.90) | Marc Gasol (+1.30) [59] ✓ | Patrick Patterson (+1.59) [30] ✓ |
| 44 | **Goran Dragic** (+1.90) | Bradley Beal (+1.27) [74] ✓ | Brook Lopez (+1.59) [104] ✗ |
| 45 | **JJ Barea** (+1.90) | Danny Green (+1.25) [23] ✓ | Chris Bosh (+1.55) [150] ✗ |
| 46 | **Luol Deng** (+1.80) | Joe Johnson (+1.25) [46] ✓ | Tyreke Evans (+1.54) [27] ✓ |
| 47 | **Jae Crowder** (+1.80) | Zach Randolph (+1.21) [55] ✓ | Derrick Favors (+1.52) [110] ✗ |
| 48 | **Joe Johnson** (+1.80) | Wesley Matthews (+1.20) [33] ✓ | Monta Ellis (+1.52) [79] ✓ |
| 49 | **Khris Middleton** (+1.70) | Derrick Rose (+1.18) [110] ✗ | Danny Green (+1.52) [23] ✓ |
| 50 | **Eric Bledsoe** (+1.70) | Anthony Morrow (+1.15) [26] ✓ | Marc Gasol (+1.49) [59] ✓ |
| 51 | **Kevin Love** (+1.70) | Paul Millsap (+1.13) [69] ✓ | DeMar DeRozan (+1.48) [130] ✗ |
| 52 | **Darren Collison** (+1.70) | Kobe Bryant (+1.09) [61] ✓ | Jamal Crawford (+1.45) [67] ✓ |
| 53 | **Manu Ginobili** (+1.70) | JJ Barea (+1.09) [42] ✓ | Greg Monroe (+1.42) [123] ✗ |
| 54 | **Kevin Martin** (+1.70) | Eric Bledsoe (+1.02) [49] ✓ | Luol Deng (+1.37) [46] ✓ |
| 55 | **Zach Randolph** (+1.60) | Chandler Parsons (+0.96) [27] ✓ | Kevin Martin (+1.36) [49] ✓ |
| 56 | **Aaron Brooks** (+1.60) | Brandon Knight (+0.93) [87] ✓ | Pau Gasol (+1.23) [98] ✓ |
| 57 | **Draymond Green** (+1.50) | Monta Ellis (+0.93) [79] ✓ | Tim Duncan (+1.23) [130] ✗ |
| 58 | **Ryan Anderson** (+1.50) | Mo Williams (+0.93) [61] ✓ | Amare Stoudemire (+1.22) [67] ✓ |
| 59 | **Marc Gasol** (+1.40) | Victor Oladipo (+0.92) [87] ✓ | DeMarcus Cousins (+1.20) [72] ✓ |
| 60 | **CJ Miles** (+1.40) | Pau Gasol (+0.87) [98] ✓ | Nikola Vucevic (+1.18) [159] ✗ |
| 61 | **Kemba Walker** (+1.20) | Kevin Love (+0.85) [49] ✓ | DeAndre Jordan (+1.15) [17] ✓ |
| 62 | **Deron Williams** (+1.20) | Ryan Anderson (+0.82) [57] ✓ | James Johnson (+1.15) [79] ✓ |
| 63 | **Ed Davis** (+1.20) | CJ Miles (+0.77) [59] ✓ | Reggie Jackson (+1.11) [40] ✓ |
| 64 | **Robert Covington** (+1.20) | DeMar DeRozan (+0.74) [130] ✗ | Tyler Zeller (+1.07) [141] ✗ |
| 65 | **Mo Williams** (+1.20) | Greivis Vasquez (+0.73) [141] ✗ | Ed Davis (+1.05) [61] ✓ |
| 66 | **Kobe Bryant** (+1.20) | Kevin Martin (+0.67) [49] ✓ | Paul Pierce (+1.05) [69] ✓ |
| 67 | **Amare Stoudemire** (+1.10) | DeMarcus Cousins (+0.66) [72] ✓ | Ersan Ilyasova (+1.05) [37] ✓ |
| 68 | **Jamal Crawford** (+1.10) | Trey Burke (+0.64) [119] ✗ | Manu Ginobili (+1.02) [49] ✓ |
| 69 | **Paul Millsap** (+1.00) | Ersan Ilyasova (+0.57) [37] ✓ | Zach Randolph (+1.01) [55] ✓ |
| 70 | **Matt Barnes** (+1.00) | Ed Davis (+0.55) [61] ✓ | JJ Barea (+0.94) [42] ✓ |
| 71 | **Paul Pierce** (+1.00) | Thaddeus Young (+0.53) [87] ✓ | Jonas Valanciunas (+0.93) [123] ✗ |
| 72 | **DeMarcus Cousins** (+0.90) | Nikola Mirotic (+0.49) [42] ✓ | Tony Parker (+0.90) [87] ✓ |
| 73 | **Jeremy Lin** (+0.90) | Devin Harris (+0.49) [37] ✓ | Devin Harris (+0.87) [37] ✓ |
| 74 | **Bradley Beal** (+0.80) | Paul Pierce (+0.48) [69] ✓ | Jodie Meeks (+0.86) [123] ✗ |
| 75 | **Kentavious CaldwellPope** (+0.80) | Evan Fournier (+0.42) [104] ✗ | Thaddeus Young (+0.86) [87] ✓ |
| 76 | **Kelly Olynyk** (+0.80) | Luol Deng (+0.42) [46] ✓ | Eric Gordon (+0.86) [87] ✓ |
| 77 | **Jonas Jerebko** (+0.80) | Mike Dunleavy (+0.40) [79] ✓ | Eric Bledsoe (+0.83) [49] ✓ |
| 78 | **Wilson Chandler** (+0.80) | Andre Miller (+0.40) [87] ✓ | Ryan Anderson (+0.82) [57] ✓ |
| 79 | **Monta Ellis** (+0.70) | Deron Williams (+0.38) [61] ✓ | Draymond Green (+0.80) [57] ✓ |
| 80 | **James Johnson** (+0.70) | Matt Barnes (+0.36) [69] ✓ | Deron Williams (+0.77) [61] ✓ |
| 81 | **Cory Joseph** (+0.70) | Tony Parker (+0.34) [87] ✓ | Joe Johnson (+0.72) [46] ✓ |
| 82 | **Mike Dunleavy** (+0.70) | Kemba Walker (+0.32) [61] ✓ | Jonas Jerebko (+0.71) [74] ✓ |
| 83 | **Anthony Tolliver** (+0.70) | Nikola Vucevic (+0.32) [159] ✗ | Harrison Barnes (+0.69) [137] ✗ |
| 84 | **Nicolas Batum** (+0.60) | James Johnson (+0.30) [79] ✓ | Kenneth Faried (+0.66) [84] ✓ |
| 85 | **Kenneth Faried** (+0.60) | Cory Joseph (+0.26) [79] ✓ | Nikola Mirotic (+0.63) [42] ✓ |
| 86 | **Omri Casspi** (+0.60) | DeMarre Carroll (+0.21) [42] ✓ | CJ Miles (+0.63) [59] ✓ |
| 87 | **Thaddeus Young** (+0.50) | Greg Monroe (+0.14) [123] ✗ | Mike Dunleavy (+0.60) [79] ✓ |
| 88 | **Elfrid Payton** (+0.50) | DeAndre Jordan (+0.13) [17] ✓ | Marreese Speights (+0.59) [194] ✗ |
| 89 | **Brandon Knight** (+0.50) | Wilson Chandler (+0.13) [74] ✓ | Andre Iguodala (+0.56) [137] ✗ |
| 90 | **Jared Sullinger** (+0.50) | Terrence Ross (+0.12) [98] ✗ | CJ Watson (+0.53) [110] ✗ |
| 91 | **Rodney Stuckey** (+0.50) | Dennis Schroder (+0.12) [159] ✗ | Marcin Gortat (+0.51) [141] ✗ |
| 92 | **Victor Oladipo** (+0.50) | Kentavious CaldwellPope (+0.12) [74] ✓ | Tobias Harris (+0.50) [119] ✗ |
| 93 | **Eric Gordon** (+0.50) | Wayne Ellington (+0.11) [98] ✗ | Bradley Beal (+0.50) [74] ✓ |
| 94 | **Tony Parker** (+0.50) | Nicolas Batum (+0.10) [84] ✓ | Trevor Ariza (+0.49) [123] ✗ |
| 95 | **Andre Miller** (+0.50) | Harrison Barnes (+0.08) [137] ✗ | Kemba Walker (+0.48) [61] ✓ |
| 96 | **Al Horford** (+0.40) | Kenneth Faried (+0.07) [84] ✓ | Brandon Bass (+0.47) [202] ✗ |
| 97 | **Tim Hardaway Jr.** (+0.40) | Jordan Clarkson (+0.04) [104] ✗ | Amir Johnson (+0.43) [123] ✗ |
| 98 | **Pau Gasol** (+0.30) | Anthony Tolliver (+0.04) [79] ✓ | Nicolas Batum (+0.43) [84] ✓ |
| 99 | **Marcus Smart** (+0.30) | Jae Crowder (+0.02) [46] ✓ | Cory Joseph (+0.40) [79] ✓ |
| 100 | **Brandan Wright** (+0.30) | Andre Iguodala (+0.02) [137] ✗ | Brandon Knight (+0.38) [87] ✓ |

### 2013-14 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.00) | Andrew Bogut (+4.61) [4] ✓ | Andrew Bogut (+3.58) [4] ✓ |
| 2 | **Draymond Green** (+4.60) | Draymond Green (+3.84) [2] ✓ | Kawhi Leonard (+3.44) [1] ✓ |
| 3 | **Joakim Noah** (+4.50) | Anthony Davis (+3.56) [33] ✓ | Draymond Green (+3.32) [2] ✓ |
| 4 | **Andrew Bogut** (+4.40) | Kevin Garnett (+3.47) [11] ✓ | DeAndre Jordan (+3.19) [64] ✓ |
| 5 | **Michael KiddGilchrist** (+4.40) | Tiago Splitter (+3.34) [6] ✓ | Jimmy Butler (+3.17) [14] ✓ |
| 6 | **Tiago Splitter** (+4.20) | Nene (+3.07) [9] ✓ | Tony Allen (+3.07) [30] ✓ |
| 7 | **Danny Green** (+4.00) | Paul Millsap (+2.98) [23] ✓ | Joakim Noah (+3.05) [3] ✓ |
| 8 | **Chris Paul** (+3.90) | Paul George (+2.98) [21] ✓ | Danny Green (+2.82) [7] ✓ |
| 9 | **Nene** (+3.80) | Jae Crowder (+2.93) [18] ✓ | Ricky Rubio (+2.79) [47] ✓ |
| 10 | **Anderson Varejao** (+3.60) | Kawhi Leonard (+2.92) [1] ✓ | Andre Iguodala (+2.69) [25] ✓ |
| 11 | **Nick Calathes** (+3.50) | Anderson Varejao (+2.91) [10] ✓ | Paul George (+2.62) [21] ✓ |
| 12 | **Ian Mahinmi** (+3.50) | Andre Iguodala (+2.81) [25] ✓ | Kyle OQuinn (+2.43) [52] ✓ |
| 13 | **Kevin Garnett** (+3.50) | CJ Watson (+2.75) [17] ✓ | Roy Hibbert (+2.43) [14] ✓ |
| 14 | **Jimmy Butler** (+3.40) | Marcin Gortat (+2.72) [27] ✓ | David West (+2.32) [57] ✓ |
| 15 | **Roy Hibbert** (+3.40) | Tim Duncan (+2.71) [18] ✓ | Paul Millsap (+2.20) [23] ✓ |
| 16 | **DeMarcus Cousins** (+3.30) | Al Jefferson (+2.63) [47] ✓ | Ian Mahinmi (+2.14) [11] ✓ |
| 17 | **CJ Watson** (+3.20) | Danny Green (+2.59) [7] ✓ | Anderson Varejao (+2.08) [10] ✓ |
| 18 | **Tim Duncan** (+3.00) | Tony Allen (+2.56) [30] ✓ | Tim Duncan (+2.05) [18] ✓ |
| 19 | **Kris Humphries** (+3.00) | Gerald Wallace (+2.50) [52] ✓ | CJ Watson (+2.03) [17] ✓ |
| 20 | **Jae Crowder** (+3.00) | Jimmy Butler (+2.47) [14] ✓ | Chris Paul (+2.00) [8] ✓ |
| 21 | **Paul George** (+2.90) | Derek Fisher (+2.45) [43] ✓ | Bismack Biyombo (+1.84) [64] ✓ |
| 22 | **Marc Gasol** (+2.80) | Michael KiddGilchrist (+2.41) [4] ✓ | Kevin Garnett (+1.76) [11] ✓ |
| 23 | **Paul Millsap** (+2.70) | Ian Mahinmi (+2.40) [11] ✓ | Manu Ginobili (+1.74) [64] ✓ |
| 24 | **Shane Battier** (+2.70) | Nick Calathes (+2.40) [11] ✓ | George Hill (+1.72) [40] ✓ |
| 25 | **DeMarre Carroll** (+2.60) | Darrell Arthur (+2.35) [45] ✓ | Iman Shumpert (+1.70) [72] ✓ |
| 26 | **Andre Iguodala** (+2.60) | LaMarcus Aldridge (+2.32) [37] ✓ | Serge Ibaka (+1.70) [33] ✓ |
| 27 | **Mario Chalmers** (+2.50) | DeMarcus Cousins (+2.29) [16] ✓ | Nick Calathes (+1.65) [11] ✓ |
| 28 | **Marcin Gortat** (+2.50) | Joakim Noah (+2.29) [3] ✓ | Anthony Davis (+1.65) [33] ✓ |
| 29 | **Samuel Dalembert** (+2.50) | Ersan Ilyasova (+2.26) [88] ✓ | Kirk Hinrich (+1.56) [40] ✓ |
| 30 | **Dwight Howard** (+2.40) | Paul Pierce (+2.26) [47] ✓ | Phil Pressey (+1.55) [76] ✓ |
| 31 | **Tony Allen** (+2.40) | Chris Bosh (+2.24) [72] ✓ | Thabo Sefolosha (+1.51) [33] ✓ |
| 32 | **Victor Oladipo** (+2.40) | Thabo Sefolosha (+2.21) [33] ✓ | Taj Gibson (+1.46) [83] ✓ |
| 33 | **Kemba Walker** (+2.30) | Patrick Patterson (+2.21) [83] ✓ | DeMarcus Cousins (+1.45) [16] ✓ |
| 34 | **Anthony Davis** (+2.30) | Kyle OQuinn (+2.20) [52] ✓ | Lance Stephenson (+1.45) [164] ✗ |
| 35 | **Serge Ibaka** (+2.30) | Kosta Koufos (+2.20) [40] ✓ | Nene (+1.44) [9] ✓ |
| 36 | **Thabo Sefolosha** (+2.30) | Dwight Howard (+2.15) [30] ✓ | Trevor Ariza (+1.42) [88] ✓ |
| 37 | **LaMarcus Aldridge** (+2.20) | Taj Gibson (+2.14) [83] ✓ | Jae Crowder (+1.41) [18] ✓ |
| 38 | **Nikola Pekovic** (+2.20) | Chris Andersen (+2.13) [52] ✓ | Al Jefferson (+1.40) [47] ✓ |
| 39 | **Eric Bledsoe** (+2.20) | DeMarre Carroll (+2.04) [25] ✓ | Corey Brewer (+1.38) [88] ✓ |
| 40 | **George Hill** (+2.10) | Victor Oladipo (+1.99) [30] ✓ | DeMarre Carroll (+1.35) [25] ✓ |
| 41 | **Kirk Hinrich** (+2.10) | DeAndre Jordan (+1.98) [64] ✓ | Dwight Howard (+1.35) [30] ✓ |
| 42 | **Kosta Koufos** (+2.10) | Marc Gasol (+1.97) [22] ✓ | Marcin Gortat (+1.34) [27] ✓ |
| 43 | **Robin Lopez** (+2.00) | Nicolas Batum (+1.97) [124] ✗ | Elton Brand (+1.32) [83] ✓ |
| 44 | **Derek Fisher** (+2.00) | Robin Lopez (+1.96) [43] ✓ | LaMarcus Aldridge (+1.26) [37] ✓ |
| 45 | **Patrick Beverley** (+1.90) | Nick Collison (+1.96) [88] ✓ | Patrick Patterson (+1.23) [83] ✓ |
| 46 | **Darrell Arthur** (+1.90) | Amir Johnson (+1.95) [62] ✓ | Michael KiddGilchrist (+1.22) [4] ✓ |
| 47 | **Ricky Rubio** (+1.80) | Timofey Mozgov (+1.90) [64] ✓ | Chris Andersen (+1.19) [52] ✓ |
| 48 | **Al Jefferson** (+1.80) | Miles Plumlee (+1.88) [52] ✓ | Derek Fisher (+1.17) [43] ✓ |
| 49 | **Paul Pierce** (+1.80) | Andrew Nicholson (+1.88) [164] ✗ | Mario Chalmers (+1.17) [27] ✓ |
| 50 | **Jeremy Lin** (+1.80) | Iman Shumpert (+1.85) [72] ✓ | John Wall (+1.16) [136] ✗ |
| 51 | **Kendrick Perkins** (+1.70) | Shane Battier (+1.84) [23] ✓ | Gerald Wallace (+1.14) [52] ✓ |
| 52 | **Darren Collison** (+1.60) | DeJuan Blair (+1.83) [129] ✗ | Amir Johnson (+1.12) [62] ✓ |
| 53 | **Chris Andersen** (+1.60) | Elton Brand (+1.75) [83] ✓ | Kemba Walker (+1.08) [33] ✓ |
| 54 | **Kyle OQuinn** (+1.60) | Josh Smith (+1.70) [83] ✓ | Marc Gasol (+1.05) [22] ✓ |
| 55 | **Gerald Wallace** (+1.60) | Shaun Livingston (+1.70) [76] ✓ | Mike Dunleavy (+1.05) [121] ✗ |
| 56 | **Miles Plumlee** (+1.60) | Nikola Pekovic (+1.70) [37] ✓ | Patrick Beverley (+1.00) [45] ✓ |
| 57 | **David West** (+1.50) | PJ Tucker (+1.67) [76] ✓ | Josh McRoberts (+0.95) [180] ✗ |
| 58 | **Kyle Lowry** (+1.30) | Andray Blatche (+1.65) [64] ✓ | Kosta Koufos (+0.95) [40] ✓ |
| 59 | **Nate Wolters** (+1.30) | Patrick Beverley (+1.64) [45] ✓ | Russell Westbrook (+0.94) [99] ✗ |
| 60 | **Omri Casspi** (+1.30) | Dirk Nowitzki (+1.62) [99] ✓ | Patty Mills (+0.92) [76] ✓ |
| 61 | **Tayshaun Prince** (+1.30) | Samuel Dalembert (+1.61) [27] ✓ | Chris Bosh (+0.91) [72] ✓ |
| 62 | **David Lee** (+1.20) | Pablo Prigioni (+1.61) [124] ✗ | Klay Thompson (+0.91) [83] ✓ |
| 63 | **Amir Johnson** (+1.20) | Roy Hibbert (+1.58) [14] ✓ | Darrell Arthur (+0.88) [45] ✓ |
| 64 | **DeAndre Jordan** (+1.10) | David West (+1.51) [57] ✓ | Andre Drummond (+0.86) [114] ✗ |
| 65 | **Manu Ginobili** (+1.10) | Ricky Rubio (+1.50) [47] ✓ | Tiago Splitter (+0.86) [6] ✓ |
| 66 | **Jared Sullinger** (+1.10) | Manu Ginobili (+1.49) [64] ✓ | Victor Oladipo (+0.85) [30] ✓ |
| 67 | **Andray Blatche** (+1.10) | Chris Paul (+1.48) [8] ✓ | Shaun Livingston (+0.85) [76] ✓ |
| 68 | **Jeremy Evans** (+1.10) | Kirk Hinrich (+1.46) [40] ✓ | Kyle Korver (+0.77) [158] ✗ |
| 69 | **Timofey Mozgov** (+1.10) | Tyson Chandler (+1.43) [64] ✓ | David Lee (+0.76) [62] ✓ |
| 70 | **Tyson Chandler** (+1.10) | Channing Frye (+1.43) [99] ✗ | Derrick Favors (+0.75) [99] ✗ |
| 71 | **Bismack Biyombo** (+1.10) | George Hill (+1.42) [40] ✓ | Carlos Boozer (+0.71) [188] ✗ |
| 72 | **Chris Bosh** (+1.00) | ETwaun Moore (+1.42) [72] ✓ | Jeremy Lamb (+0.70) [124] ✗ |
| 73 | **Courtney Lee** (+1.00) | Steven Adams (+1.41) [121] ✗ | Nikola Vucevic (+0.70) [76] ✓ |
| 74 | **Iman Shumpert** (+1.00) | Trevor Ariza (+1.40) [88] ✓ | Courtney Lee (+0.69) [72] ✓ |
| 75 | **ETwaun Moore** (+1.00) | Omri Casspi (+1.39) [58] ✓ | Andray Blatche (+0.69) [64] ✓ |
| 76 | **Kevin Love** (+0.90) | Phil Pressey (+1.39) [76] ✓ | Maurice Harkless (+0.68) [99] ✗ |
| 77 | **Patty Mills** (+0.90) | David Lee (+1.39) [62] ✓ | Nicolas Batum (+0.65) [124] ✗ |
| 78 | **PJ Tucker** (+0.90) | Mario Chalmers (+1.35) [27] ✓ | Steven Adams (+0.65) [121] ✗ |
| 79 | **Shaun Livingston** (+0.90) | Jason Thompson (+1.35) [110] ✗ | Dante Cunningham (+0.64) [114] ✗ |
| 80 | **Nikola Vucevic** (+0.90) | Bismack Biyombo (+1.34) [64] ✓ | Shane Battier (+0.64) [23] ✓ |
| 81 | **Avery Bradley** (+0.90) | Kemba Walker (+1.30) [33] ✓ | PJ Tucker (+0.63) [76] ✓ |
| 82 | **Phil Pressey** (+0.90) | Jared Sullinger (+1.30) [64] ✓ | Channing Frye (+0.62) [99] ✗ |
| 83 | **Klay Thompson** (+0.80) | Andre Drummond (+1.28) [114] ✗ | Miles Plumlee (+0.59) [52] ✓ |
| 84 | **Taj Gibson** (+0.80) | Blake Griffin (+1.28) [110] ✗ | Tyson Chandler (+0.57) [64] ✓ |
| 85 | **Patrick Patterson** (+0.80) | Nikola Vucevic (+1.27) [76] ✓ | Kyle Lowry (+0.56) [58] ✓ |
| 86 | **Josh Smith** (+0.80) | Jeremy Evans (+1.23) [64] ✓ | Matt Barnes (+0.55) [143] ✗ |
| 87 | **Elton Brand** (+0.80) | Patty Mills (+1.18) [76] ✓ | Terrence Jones (+0.53) [202] ✗ |
| 88 | **Trevor Ariza** (+0.70) | Greg Monroe (+1.14) [143] ✗ | Josh Smith (+0.52) [83] ✓ |
| 89 | **Corey Brewer** (+0.70) | Robert Sacre (+1.14) [114] ✗ | Deron Williams (+0.52) [92] ✓ |
| 90 | **Nick Collison** (+0.70) | Kendrick Perkins (+1.13) [51] ✓ | Eric Bledsoe (+0.50) [37] ✓ |
| 91 | **Ersan Ilyasova** (+0.70) | Corey Brewer (+1.11) [88] ✓ | Michael CarterWilliams (+0.50) [92] ✓ |
| 92 | **Deron Williams** (+0.60) | Kris Humphries (+1.11) [18] ✓ | Paul Pierce (+0.49) [47] ✓ |
| 93 | **Luol Deng** (+0.60) | Jeremy Lamb (+1.09) [124] ✗ | Boris Diaw (+0.47) [110] ✗ |
| 94 | **Michael CarterWilliams** (+0.60) | Anthony Tolliver (+1.06) [143] ✗ | Blake Griffin (+0.43) [110] ✗ |
| 95 | **Matthew Dellavedova** (+0.60) | Terrence Jones (+1.05) [202] ✗ | Terrence Ross (+0.41) [129] ✗ |
| 96 | **Andrea Bargnani** (+0.60) | Kentavious CaldwellPope (+1.04) [114] ✗ | Kris Humphries (+0.39) [18] ✓ |
| 97 | **Spencer Hawes** (+0.50) | Wesley Matthews (+0.99) [129] ✗ | Cody Zeller (+0.38) [136] ✗ |
| 98 | **AlFarouq Aminu** (+0.50) | Maurice Harkless (+0.98) [99] ✗ | Pablo Prigioni (+0.38) [124] ✗ |
| 99 | **Dirk Nowitzki** (+0.40) | Glen Davis (+0.98) [99] ✗ | Giannis Antetokounmpo (+0.36) [114] ✗ |
| 100 | **Isaiah Thomas** (+0.40) | Mike Conley (+0.98) [99] ✗ | Gerald Henderson (+0.34) [136] ✗ |

### 2014-15 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.20) | Andrew Bogut (+5.28) [5] ✓ | Kawhi Leonard (+4.47) [1] ✓ |
| 2 | **Draymond Green** (+5.10) | Rudy Gobert (+4.73) [3] ✓ | Tony Allen (+4.32) [3] ✓ |
| 3 | **Rudy Gobert** (+4.80) | Draymond Green (+4.73) [2] ✓ | Draymond Green (+3.75) [2] ✓ |
| 4 | **Tony Allen** (+4.80) | Nene (+4.29) [17] ✓ | Andrew Bogut (+3.21) [5] ✓ |
| 5 | **Andrew Bogut** (+4.70) | Tony Allen (+4.15) [3] ✓ | DeAndre Jordan (+3.15) [60] ✓ |
| 6 | **Anthony Davis** (+4.50) | Nerlens Noel (+3.92) [19] ✓ | Nerlens Noel (+2.89) [19] ✓ |
| 7 | **DeMarcus Cousins** (+4.40) | Pau Gasol (+3.69) [85] ✓ | Anthony Davis (+2.86) [6] ✓ |
| 8 | **Marcin Gortat** (+3.60) | Kawhi Leonard (+3.57) [1] ✓ | Rudy Gobert (+2.78) [3] ✓ |
| 9 | **Tim Duncan** (+3.50) | Andre Roberson (+3.54) [10] ✓ | Danny Green (+2.72) [14] ✓ |
| 10 | **Andre Roberson** (+3.40) | Zach Randolph (+3.48) [54] ✓ | AlFarouq Aminu (+2.59) [20] ✓ |
| 11 | **Kosta Koufos** (+3.30) | Tim Duncan (+3.41) [9] ✓ | Tim Duncan (+2.49) [9] ✓ |
| 12 | **Zaza Pachulia** (+3.20) | Al Jefferson (+3.23) [102] ✗ | Paul Millsap (+2.35) [26] ✓ |
| 13 | **Khris Middleton** (+3.10) | Kosta Koufos (+3.17) [11] ✓ | Khris Middleton (+2.07) [13] ✓ |
| 14 | **Danny Green** (+3.00) | Anthony Davis (+3.05) [6] ✓ | Andre Roberson (+2.05) [10] ✓ |
| 15 | **Serge Ibaka** (+3.00) | Nikola Mirotic (+3.04) [33] ✓ | Bismack Biyombo (+2.05) [102] ✗ |
| 16 | **Michael KiddGilchrist** (+3.00) | Zaza Pachulia (+2.99) [12] ✓ | Marcus Smart (+1.98) [42] ✓ |
| 17 | **Jonas Jerebko** (+2.80) | Tyson Chandler (+2.92) [20] ✓ | John Wall (+1.97) [127] ✗ |
| 18 | **Nene** (+2.80) | Josh Smith (+2.83) [20] ✓ | DeMarcus Cousins (+1.95) [7] ✓ |
| 19 | **Nerlens Noel** (+2.70) | Marcin Gortat (+2.78) [8] ✓ | Marcin Gortat (+1.93) [8] ✓ |
| 20 | **Marc Gasol** (+2.60) | Greg Monroe (+2.77) [111] ✗ | Michael KiddGilchrist (+1.74) [14] ✓ |
| 21 | **Tyson Chandler** (+2.60) | Iman Shumpert (+2.71) [29] ✓ | John Henson (+1.64) [85] ✓ |
| 22 | **Joakim Noah** (+2.60) | DeMarcus Cousins (+2.70) [7] ✓ | Nicolas Batum (+1.55) [136] ✗ |
| 23 | **Josh Smith** (+2.60) | Dwight Howard (+2.69) [40] ✓ | Zaza Pachulia (+1.53) [12] ✓ |
| 24 | **AlFarouq Aminu** (+2.60) | Kelly Olynyk (+2.63) [34] ✓ | Trevor Ariza (+1.51) [102] ✗ |
| 25 | **Alex Len** (+2.50) | Danny Green (+2.55) [14] ✓ | Andre Drummond (+1.50) [93] ✓ |
| 26 | **Paul Millsap** (+2.40) | Jared Dudley (+2.54) [42] ✓ | Kosta Koufos (+1.48) [11] ✓ |
| 27 | **Timofey Mozgov** (+2.40) | John Henson (+2.52) [85] ✓ | Iman Shumpert (+1.40) [29] ✓ |
| 28 | **Omer Asik** (+2.40) | Derrick Favors (+2.50) [34] ✓ | Cody Zeller (+1.40) [34] ✓ |
| 29 | **Darren Collison** (+2.30) | Marcus Smart (+2.36) [42] ✓ | Kelly Olynyk (+1.40) [34] ✓ |
| 30 | **Iman Shumpert** (+2.30) | Jonas Jerebko (+2.35) [17] ✓ | Jae Crowder (+1.36) [102] ✗ |
| 31 | **Michael CarterWilliams** (+2.30) | Wesley Matthews (+2.30) [54] ✓ | James Johnson (+1.35) [54] ✓ |
| 32 | **Luc Mbah a Moute** (+2.30) | Timofey Mozgov (+2.30) [26] ✓ | Nikola Mirotic (+1.34) [33] ✓ |
| 33 | **Nikola Mirotic** (+2.20) | Paul Millsap (+2.26) [26] ✓ | Serge Ibaka (+1.33) [14] ✓ |
| 34 | **Chris Paul** (+2.10) | Luis Scola (+2.25) [82] ✓ | Marc Gasol (+1.31) [20] ✓ |
| 35 | **Derrick Favors** (+2.10) | Andre Iguodala (+2.24) [46] ✓ | Dwight Howard (+1.31) [40] ✓ |
| 36 | **Kelly Olynyk** (+2.10) | Andre Drummond (+2.18) [93] ✓ | Nene (+1.28) [17] ✓ |
| 37 | **Cody Zeller** (+2.10) | Jimmy Butler (+2.17) [136] ✗ | Chris Paul (+1.28) [34] ✓ |
| 38 | **Steven Adams** (+2.00) | Roy Hibbert (+2.14) [38] ✓ | George Hill (+1.27) [45] ✓ |
| 39 | **Roy Hibbert** (+2.00) | Amir Johnson (+2.14) [73] ✓ | Giannis Antetokounmpo (+1.26) [146] ✗ |
| 40 | **LaMarcus Aldridge** (+1.90) | Omer Asik (+2.11) [26] ✓ | Marvin Williams (+1.25) [69] ✓ |
| 41 | **Dwight Howard** (+1.90) | Donatas Motiejunas (+2.10) [73] ✓ | Miles Plumlee (+1.25) [85] ✓ |
| 42 | **Marcus Smart** (+1.80) | Jared Sullinger (+2.08) [85] ✓ | Alex Len (+1.22) [25] ✓ |
| 43 | **Jared Dudley** (+1.80) | Manu Ginobili (+2.01) [51] ✓ | Al Jefferson (+1.22) [102] ✗ |
| 44 | **Pablo Prigioni** (+1.80) | Joakim Noah (+2.00) [20] ✓ | Andre Iguodala (+1.22) [46] ✓ |
| 45 | **George Hill** (+1.70) | Khris Middleton (+1.95) [13] ✓ | Jimmy Butler (+1.20) [136] ✗ |
| 46 | **Kevin Love** (+1.60) | LaMarcus Aldridge (+1.91) [40] ✓ | CJ Miles (+1.16) [111] ✗ |
| 47 | **Al Horford** (+1.60) | Jae Crowder (+1.85) [102] ✗ | Elfrid Payton (+1.16) [111] ✗ |
| 48 | **Andre Iguodala** (+1.60) | James Johnson (+1.83) [54] ✓ | Robert Covington (+1.12) [136] ✗ |
| 49 | **Mario Chalmers** (+1.50) | Brandan Wright (+1.82) [54] ✓ | Josh Smith (+1.12) [20] ✓ |
| 50 | **Kris Humphries** (+1.50) | Pablo Prigioni (+1.80) [42] ✓ | Michael CarterWilliams (+1.12) [29] ✓ |
| 51 | **Manu Ginobili** (+1.40) | Luc Mbah a Moute (+1.79) [29] ✓ | Pablo Prigioni (+1.11) [42] ✓ |
| 52 | **Alan Anderson** (+1.40) | AlFarouq Aminu (+1.74) [20] ✓ | Jared Dudley (+1.07) [42] ✓ |
| 53 | **Robin Lopez** (+1.40) | Bismack Biyombo (+1.74) [102] ✗ | Manu Ginobili (+1.06) [51] ✓ |
| 54 | **Wesley Matthews** (+1.30) | Michael KiddGilchrist (+1.69) [14] ✓ | LeBron James (+1.04) [132] ✗ |
| 55 | **Zach Randolph** (+1.30) | Ersan Ilyasova (+1.67) [93] ✓ | Jeff Teague (+1.04) [85] ✓ |
| 56 | **PJ Tucker** (+1.30) | Cory Joseph (+1.62) [85] ✓ | PJ Tucker (+1.03) [54] ✓ |
| 57 | **James Johnson** (+1.30) | Chris Kaman (+1.62) [111] ✗ | Joakim Noah (+1.01) [20] ✓ |
| 58 | **Brandan Wright** (+1.30) | Marvin Williams (+1.56) [69] ✓ | Pau Gasol (+1.00) [85] ✓ |
| 59 | **Langston Galloway** (+1.30) | Taj Gibson (+1.55) [73] ✓ | Roy Hibbert (+1.00) [38] ✓ |
| 60 | **DeAndre Jordan** (+1.20) | Alex Len (+1.54) [25] ✓ | Derrick Favors (+0.98) [34] ✓ |
| 61 | **Eric Bledsoe** (+1.20) | Kendrick Perkins (+1.52) [73] ✓ | Zach Randolph (+0.98) [54] ✓ |
| 62 | **Kemba Walker** (+1.20) | David West (+1.44) [102] ✗ | Brandan Wright (+0.96) [54] ✓ |
| 63 | **Jonas Valanciunas** (+1.20) | Michael CarterWilliams (+1.43) [29] ✓ | Bradley Beal (+0.94) [85] ✓ |
| 64 | **Gerald Henderson** (+1.10) | Corey Brewer (+1.43) [69] ✓ | Luis Scola (+0.94) [82] ✓ |
| 65 | **CJ Watson** (+1.10) | PJ Tucker (+1.41) [54] ✓ | Matt Barnes (+0.93) [79] ✓ |
| 66 | **Klay Thompson** (+1.00) | DeAndre Jordan (+1.40) [60] ✓ | Jonas Jerebko (+0.92) [17] ✓ |
| 67 | **DeMarre Carroll** (+1.00) | Marc Gasol (+1.39) [20] ✓ | Al Horford (+0.90) [46] ✓ |
| 68 | **Patrick Beverley** (+1.00) | Robert Covington (+1.38) [136] ✗ | David West (+0.89) [102] ✗ |
| 69 | **Corey Brewer** (+0.90) | Brook Lopez (+1.37) [82] ✓ | Wesley Matthews (+0.86) [54] ✓ |
| 70 | **Markieff Morris** (+0.90) | Mario Chalmers (+1.37) [49] ✓ | Jerami Grant (+0.85) [102] ✗ |
| 71 | **Marvin Williams** (+0.90) | Trevor Booker (+1.34) [69] ✓ | James Harden (+0.84) [121] ✗ |
| 72 | **Trevor Booker** (+0.90) | Steven Adams (+1.34) [38] ✓ | Kyle Korver (+0.83) [121] ✗ |
| 73 | **Monta Ellis** (+0.80) | Miles Plumlee (+1.30) [85] ✓ | DeMarre Carroll (+0.77) [66] ✓ |
| 74 | **Amir Johnson** (+0.80) | George Hill (+1.30) [45] ✓ | Klay Thompson (+0.74) [66] ✓ |
| 75 | **Donatas Motiejunas** (+0.80) | Jonas Valanciunas (+1.30) [60] ✓ | CJ Watson (+0.71) [64] ✓ |
| 76 | **Derrick Rose** (+0.80) | Henry Sims (+1.25) [214] ✗ | Russell Westbrook (+0.69) [151] ✗ |
| 77 | **Taj Gibson** (+0.80) | Al Horford (+1.25) [46] ✓ | Tyson Chandler (+0.68) [20] ✓ |
| 78 | **Kendrick Perkins** (+0.80) | LeBron James (+1.22) [132] ✗ | Kent Bazemore (+0.66) [127] ✗ |
| 79 | **JJ Redick** (+0.70) | Patrick Patterson (+1.22) [162] ✗ | LaMarcus Aldridge (+0.64) [40] ✓ |
| 80 | **Matt Barnes** (+0.70) | Anthony Tolliver (+1.22) [199] ✗ | Kemba Walker (+0.62) [60] ✓ |
| 81 | **KJ McDaniels** (+0.70) | Tyler Zeller (+1.21) [102] ✗ | Eric Bledsoe (+0.59) [60] ✓ |
| 82 | **Kyle Lowry** (+0.60) | Ed Davis (+1.20) [93] ✓ | Evan Turner (+0.58) [111] ✗ |
| 83 | **Brook Lopez** (+0.60) | Nicolas Batum (+1.19) [136] ✗ | KJ McDaniels (+0.52) [79] ✓ |
| 84 | **Luis Scola** (+0.60) | Danilo Gallinari (+1.19) [111] ✗ | Jrue Holiday (+0.51) [155] ✗ |
| 85 | **Mike Conley** (+0.50) | Monta Ellis (+1.17) [73] ✓ | Otto Porter Jr. (+0.45) [191] ✗ |
| 86 | **Jeff Teague** (+0.50) | Darren Collison (+1.16) [29] ✓ | Kris Humphries (+0.45) [49] ✓ |
| 87 | **Pau Gasol** (+0.50) | Trevor Ariza (+1.14) [102] ✗ | Harrison Barnes (+0.44) [132] ✗ |
| 88 | **Bradley Beal** (+0.50) | Kent Bazemore (+1.08) [127] ✗ | Markieff Morris (+0.43) [69] ✓ |
| 89 | **Jared Sullinger** (+0.50) | Harrison Barnes (+1.03) [132] ✗ | Kevin Love (+0.42) [46] ✓ |
| 90 | **Cory Joseph** (+0.50) | Quincy Acy (+1.01) [146] ✗ | Greg Monroe (+0.41) [111] ✗ |
| 91 | **Miles Plumlee** (+0.50) | Kevin Love (+1.01) [46] ✓ | Corey Brewer (+0.37) [69] ✓ |
| 92 | **John Henson** (+0.50) | Jeff Teague (+1.01) [85] ✓ | Paul Pierce (+0.37) [111] ✗ |
| 93 | **Devin Harris** (+0.40) | CJ Miles (+0.99) [111] ✗ | Timofey Mozgov (+0.37) [26] ✓ |
| 94 | **Thaddeus Young** (+0.40) | Boris Diaw (+0.97) [155] ✗ | Steven Adams (+0.36) [38] ✓ |
| 95 | **Ed Davis** (+0.40) | Kris Humphries (+0.97) [49] ✓ | Mario Chalmers (+0.36) [49] ✓ |
| 96 | **Jeremy Lin** (+0.40) | DeMarre Carroll (+0.97) [66] ✓ | Mike Dunleavy (+0.35) [151] ✗ |
| 97 | **Ersan Ilyasova** (+0.40) | Marreese Speights (+0.94) [199] ✗ | Mike Conley (+0.34) [85] ✓ |
| 98 | **Andre Drummond** (+0.40) | Klay Thompson (+0.94) [66] ✓ | Rajon Rondo (+0.34) [93] ✗ |
| 99 | **Tony Snell** (+0.40) | Eric Bledsoe (+0.93) [60] ✓ | Monta Ellis (+0.30) [73] ✓ |
| 100 | **Chris Bosh** (+0.40) | Giannis Antetokounmpo (+0.92) [146] ✗ | Mason Plumlee (+0.28) [214] ✗ |

