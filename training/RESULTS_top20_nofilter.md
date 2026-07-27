# Top-20 leaderboards on the held-out seasons

True 538 RAPTOR vs. our models vs. Neil Paine's Estimated RAPTOR, for
2013-14 and 2014-15. Total, offense and defense are ranked separately.
These are the models trained on **all** data points — no starter or
near-zero filtering (see RESULTS_starters.md for why those filters lose).

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

Derived, not chosen: the **lowest minutes total among any true top-20**
player, taken across every season, split and target, so no genuine leader is
ruled ineligible.

| season | split | target | min mp in true top 20 | median mp | pool n | pool min mp |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | total | 1527 | 2610 | 250 | 1065 |
| 2013-14 | Regular season | offense | 1412 | 2722 | 250 | 1065 |
| 2013-14 | Regular season | defense | 1109 | 1710 | 250 | 1065 |
| 2014-15 | Regular season | total | 1267 | 2436 | 250 | 1148 |
| 2014-15 | Regular season | offense | 1267 | 2436 | 250 | 1148 |
| 2014-15 | Regular season | defense | 1230 | 2074 | 250 | 1148 |
| 2013-14 | Playoffs | total | 172 | 434 | 100 | 167 |
| 2013-14 | Playoffs | offense | 176 | 454 | 100 | 167 |
| 2013-14 | Playoffs | defense | 170 | 283 | 100 | 167 |
| 2014-15 | Playoffs | total | 131 | 430 | 100 | 131 |
| 2014-15 | Playoffs | offense | 131 | 224 | 100 | 131 |
| 2014-15 | Playoffs | defense | 150 | 412 | 100 | 131 |

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

## Summary — true top-20 members recovered (hits@20)

**total**

| season | split | pool | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) | ρ ours (direct total) | ρ ours (offense+defense) | ρ Paine (eRO+eRD) |
|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 246 | 13/20 | 13/20 | 14/20 | +0.882 | +0.890 | +0.889 |
| 2013-14 | Playoffs | 99 | 15/20 | 15/20 | 14/20 | +0.839 | +0.825 | +0.794 |
| 2014-15 | Regular season | 246 | 14/20 | 14/20 | 14/20 | +0.882 | +0.883 | +0.901 |
| 2014-15 | Playoffs | 98 | 12/20 | 11/20 | 12/20 | +0.867 | +0.833 | +0.863 |
| **all** | | | **54/80** | **53/80** | **54/80** |  |  |  |

**offense**

| season | split | pool | ours | Paine (eRO) | ρ ours | ρ Paine (eRO) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 246 | 16/20 | 17/20 | +0.923 | +0.867 |
| 2013-14 | Playoffs | 99 | 16/20 | 13/20 | +0.889 | +0.838 |
| 2014-15 | Regular season | 246 | 15/20 | 15/20 | +0.916 | +0.858 |
| 2014-15 | Playoffs | 98 | 15/20 | 13/20 | +0.896 | +0.787 |
| **all** | | | **62/80** | **58/80** |  |  |

**defense**

| season | split | pool | ours | Paine (eRD) | ρ ours | ρ Paine (eRD) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 246 | 13/20 | 12/20 | +0.862 | +0.795 |
| 2013-14 | Playoffs | 99 | 13/20 | 11/20 | +0.690 | +0.667 |
| 2014-15 | Regular season | 246 | 13/20 | 14/20 | +0.833 | +0.801 |
| 2014-15 | Playoffs | 98 | 12/20 | 10/20 | +0.745 | +0.633 |
| **all** | | | **51/80** | **47/80** |  |  |

## How contested is the cutoff

Hits@20 only asks whether a player lands on the correct side of an
arbitrary cutoff. Players within ±0.25 RAPTOR of the boundary value, per cell:

| season | split | target | rank-20 value | gap to rank 21 | players within ±0.25 |
|---|---|---|---|---|---|
| 2013-14 | Regular season | total | +3.90 | 0.10 | 12 |
| 2013-14 | Playoffs | total | +5.60 | 0.10 | 6 |
| 2014-15 | Regular season | total | +4.00 | 0.00 | 5 |
| 2014-15 | Playoffs | total | +5.30 | 0.30 | 2 |
| 2013-14 | Regular season | offense | +2.80 | 0.10 | 8 |
| 2013-14 | Playoffs | offense | +3.10 | 0.00 | 10 |
| 2014-15 | Regular season | offense | +3.20 | 0.00 | 8 |
| 2014-15 | Playoffs | offense | +3.50 | 0.20 | 5 |
| 2013-14 | Regular season | defense | +3.00 | 0.10 | 6 |
| 2013-14 | Playoffs | defense | +3.50 | 0.10 | 4 |
| 2014-15 | Regular season | defense | +2.60 | 0.00 | 12 |
| 2014-15 | Playoffs | defense | +2.80 | 0.00 | 6 |

Where a dozen players sit inside a quarter-point of the cutoff, which 20 names
come back is close to a coin flip regardless of model quality. That is why
hits@20 and the rank correlations disagree, and why the correlations are the
more reliable read.

## Conclusions

**Direct total vs. summing the halves.** Predicting `rap` directly and
summing our two part-models are near-interchangeable: R² +0.734 vs +0.717, ρ +0.868 vs +0.861, hits@20 54/80 vs 53/80.

**Against Paine on the total.** R² +0.734 vs +0.684, RMSE 1.775 vs 1.936, ρ +0.868 vs +0.846; hits@20 54/80 vs 54/80.

**Offense.** ours R² +0.803 / ρ +0.904 / hits@20 62/80; Paine R² +0.707 / ρ +0.825 / hits@20 58/80.

**Defense.** ours R² +0.596 / ρ +0.797 / hits@20 51/80; Paine R² +0.504 / ρ +0.728 / hits@20 47/80.

Read the precision@K tables above rather than a single cutoff: they show
where each system's advantage actually lives, and a hits count at one
arbitrary K is decided by hundredths of a point among near-tied players.

## Leaderboards

`[n]` after a predicted name is that player's *true* rank; ✓ means they are
genuinely in the true top 20.

### 2013-14 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+11.00) | Chris Paul (+7.07) [1] ✓ | Chris Paul (+8.52) [1] ✓ | Chris Paul (+8.79) [1] ✓ |
| 2 | **Kevin Durant** (+7.10) | Kevin Durant (+6.49) [2] ✓ | Kevin Durant (+6.83) [2] ✓ | Kevin Durant (+7.21) [2] ✓ |
| 3 | **Kawhi Leonard** (+6.70) | LeBron James (+5.96) [15] ✓ | LeBron James (+5.89) [15] ✓ | LeBron James (+6.77) [15] ✓ |
| 4 | **Kevin Love** (+6.60) | Paul George (+5.46) [8] ✓ | Paul George (+5.40) [8] ✓ | Kawhi Leonard (+5.95) [3] ✓ |
| 5 | **James Harden** (+6.10) | Kevin Love (+5.35) [4] ✓ | Manu Ginobili (+4.90) [9] ✓ | James Harden (+5.50) [5] ✓ |
| 6 | **Joakim Noah** (+5.90) | Blake Griffin (+5.29) [34] ✗ | Blake Griffin (+4.68) [34] ✗ | Paul George (+5.08) [8] ✓ |
| 7 | **Kyle Lowry** (+5.70) | Kawhi Leonard (+4.97) [3] ✓ | Kawhi Leonard (+4.47) [3] ✓ | Manu Ginobili (+4.99) [9] ✓ |
| 8 | **Paul George** (+5.60) | James Harden (+4.90) [5] ✓ | Kyle Lowry (+4.40) [7] ✓ | Kevin Love (+4.86) [4] ✓ |
| 9 | **Manu Ginobili** (+5.10) | Manu Ginobili (+4.37) [9] ✓ | Kevin Love (+4.37) [4] ✓ | Goran Dragic (+4.71) [10] ✓ |
| 10 | **Goran Dragic** (+5.00) | Andrew Bogut (+4.31) [36] ✗ | James Harden (+4.36) [5] ✓ | Andre Iguodala (+4.53) [21] ✗ |
| 11 | **DeMarcus Cousins** (+5.00) | Dirk Nowitzki (+4.19) [13] ✓ | Dirk Nowitzki (+4.31) [13] ✓ | Kyle Lowry (+4.27) [7] ✓ |
| 12 | **Patty Mills** (+4.80) | Kyle Lowry (+4.12) [7] ✓ | Andrew Bogut (+4.25) [36] ✗ | Joakim Noah (+4.22) [6] ✓ |
| 13 | **Dirk Nowitzki** (+4.70) | Andre Iguodala (+3.93) [21] ✗ | Andre Iguodala (+4.09) [21] ✗ | Russell Westbrook (+4.18) [25] ✗ |
| 14 | **Danny Green** (+4.70) | Carmelo Anthony (+3.82) [21] ✗ | Goran Dragic (+4.03) [10] ✓ | Anthony Davis (+4.18) [28] ✗ |
| 15 | **LeBron James** (+4.60) | Ricky Rubio (+3.69) [25] ✗ | Patty Mills (+3.95) [12] ✓ | Blake Griffin (+4.00) [34] ✗ |
| 16 | **Anderson Varejao** (+4.10) | Patty Mills (+3.64) [12] ✓ | Jimmy Butler (+3.83) [19] ✓ | Ricky Rubio (+3.91) [25] ✗ |
| 17 | **Patrick Beverley** (+4.10) | Russell Westbrook (+3.58) [25] ✗ | Anthony Davis (+3.81) [28] ✗ | Brandan Wright (+3.89) [115] ✗ |
| 18 | **Mario Chalmers** (+4.00) | Jimmy Butler (+3.55) [19] ✓ | Mike Conley (+3.70) [21] ✗ | Dirk Nowitzki (+3.81) [13] ✓ |
| 19 | **Jimmy Butler** (+3.90) | Draymond Green (+3.50) [29] ✗ | Russell Westbrook (+3.54) [25] ✗ | DeMarcus Cousins (+3.60) [10] ✓ |
| 20 | **Isaiah Thomas** (+3.90) | Goran Dragic (+3.44) [10] ✓ | Damian Lillard (+3.50) [53] ✗ | Patty Mills (+3.50) [12] ✓ |

### 2013-14 — Playoffs — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+13.20) | LeBron James (+7.91) [8] ✓ | Chris Paul (+7.82) [1] ✓ | Chris Paul (+8.08) [1] ✓ |
| 2 | **Draymond Green** (+10.80) | Chris Paul (+7.48) [1] ✓ | LeBron James (+7.23) [8] ✓ | LeBron James (+7.21) [8] ✓ |
| 3 | **Stephen Curry** (+10.40) | Russell Westbrook (+6.65) [14] ✓ | Russell Westbrook (+6.92) [14] ✓ | Manu Ginobili (+5.71) [7] ✓ |
| 4 | **James Harden** (+10.00) | Stephen Curry (+6.43) [3] ✓ | Stephen Curry (+6.50) [3] ✓ | Kawhi Leonard (+5.01) [16] ✓ |
| 5 | **Paul Millsap** (+8.40) | Manu Ginobili (+6.20) [7] ✓ | Kevin Durant (+6.28) [20] ✓ | Russell Westbrook (+4.98) [14] ✓ |
| 6 | **Vince Carter** (+8.10) | Patty Mills (+6.17) [10] ✓ | Manu Ginobili (+6.11) [7] ✓ | Danny Green (+4.41) [14] ✓ |
| 7 | **Manu Ginobili** (+7.90) | Kevin Durant (+6.13) [20] ✓ | Kyle Lowry (+5.98) [30] ✗ | Tiago Splitter (+4.22) [21] ✗ |
| 8 | **LeBron James** (+7.70) | Damian Lillard (+5.86) [11] ✓ | Danny Green (+5.48) [14] ✓ | Stephen Curry (+4.14) [3] ✓ |
| 9 | **Greivis Vasquez** (+7.30) | Kawhi Leonard (+5.74) [16] ✓ | Serge Ibaka (+5.23) [24] ✗ | Trevor Ariza (+3.95) [21] ✗ |
| 10 | **Patty Mills** (+7.20) | Serge Ibaka (+5.21) [24] ✗ | Patty Mills (+5.02) [10] ✓ | Patrick Patterson (+3.62) [45] ✗ |
| 11 | **Damian Lillard** (+6.90) | Patrick Patterson (+4.80) [45] ✗ | Damian Lillard (+4.97) [11] ✓ | Bradley Beal (+3.57) [19] ✓ |
| 12 | **Andray Blatche** (+6.90) | Draymond Green (+4.75) [2] ✓ | Kawhi Leonard (+4.97) [16] ✓ | Draymond Green (+3.38) [2] ✓ |
| 13 | **Deron Williams** (+6.60) | Danny Green (+4.75) [14] ✓ | Greivis Vasquez (+4.68) [9] ✓ | Greivis Vasquez (+3.33) [9] ✓ |
| 14 | **Russell Westbrook** (+6.40) | Kyle Lowry (+4.73) [30] ✗ | James Harden (+4.29) [4] ✓ | Tim Duncan (+3.25) [32] ✗ |
| 15 | **Danny Green** (+6.40) | Greivis Vasquez (+4.56) [9] ✓ | LaMarcus Aldridge (+4.19) [17] ✓ | Dwight Howard (+3.17) [43] ✗ |
| 16 | **Kawhi Leonard** (+6.20) | Vince Carter (+4.17) [6] ✓ | Patrick Patterson (+4.17) [45] ✗ | Patty Mills (+2.91) [10] ✓ |
| 17 | **LaMarcus Aldridge** (+6.00) | Deron Williams (+3.96) [13] ✓ | Vince Carter (+3.97) [6] ✓ | James Harden (+2.91) [4] ✓ |
| 18 | **Chris Andersen** (+6.00) | James Harden (+3.95) [4] ✓ | Kyle Korver (+3.86) [41] ✗ | Damian Lillard (+2.74) [11] ✓ |
| 19 | **Bradley Beal** (+5.70) | Joe Johnson (+3.94) [25] ✗ | David West (+3.72) [37] ✗ | Joe Johnson (+2.73) [25] ✗ |
| 20 | **Kevin Durant** (+5.60) | Tiago Splitter (+3.92) [21] ✗ | Deron Williams (+3.67) [13] ✓ | Kevin Durant (+2.72) [20] ✓ |

### 2014-15 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | Chris Paul (+7.06) [1] ✓ | Chris Paul (+7.88) [1] ✓ | Chris Paul (+8.27) [1] ✓ |
| 2 | **Kawhi Leonard** (+8.90) | Draymond Green (+6.27) [5] ✓ | LeBron James (+6.54) [11] ✓ | Anthony Davis (+7.97) [3] ✓ |
| 3 | **Anthony Davis** (+8.80) | Kawhi Leonard (+6.26) [2] ✓ | Draymond Green (+5.93) [5] ✓ | Kawhi Leonard (+7.91) [2] ✓ |
| 4 | **James Harden** (+7.70) | LeBron James (+6.23) [11] ✓ | Kawhi Leonard (+5.92) [2] ✓ | LeBron James (+6.66) [11] ✓ |
| 5 | **Draymond Green** (+6.50) | Anthony Davis (+6.11) [3] ✓ | James Harden (+5.30) [4] ✓ | James Harden (+6.55) [4] ✓ |
| 6 | **Danny Green** (+6.10) | James Harden (+5.79) [4] ✓ | Anthony Davis (+5.05) [3] ✓ | Russell Westbrook (+5.71) [7] ✓ |
| 7 | **Russell Westbrook** (+5.60) | Jimmy Butler (+5.40) [34] ✗ | Kyrie Irving (+4.78) [13] ✓ | Jimmy Butler (+5.58) [34] ✗ |
| 8 | **George Hill** (+5.60) | Russell Westbrook (+4.71) [7] ✓ | Jimmy Butler (+4.67) [34] ✗ | George Hill (+5.08) [7] ✓ |
| 9 | **DeMarcus Cousins** (+5.40) | Damian Lillard (+4.37) [42] ✗ | Russell Westbrook (+4.64) [7] ✓ | Klay Thompson (+4.74) [10] ✓ |
| 10 | **Klay Thompson** (+5.30) | Lou Williams (+4.15) [34] ✗ | Klay Thompson (+4.56) [10] ✓ | Tony Allen (+4.57) [16] ✓ |
| 11 | **LeBron James** (+5.10) | DeMarcus Cousins (+4.14) [9] ✓ | Rudy Gobert (+4.49) [19] ✓ | Draymond Green (+4.55) [5] ✓ |
| 12 | **Khris Middleton** (+4.80) | Kyrie Irving (+4.10) [13] ✓ | George Hill (+4.41) [7] ✓ | DeAndre Jordan (+4.31) [13] ✓ |
| 13 | **Kyrie Irving** (+4.60) | Klay Thompson (+4.07) [10] ✓ | Damian Lillard (+4.25) [42] ✗ | Danny Green (+4.23) [6] ✓ |
| 14 | **DeAndre Jordan** (+4.60) | Rudy Gobert (+3.89) [19] ✓ | Andrew Bogut (+4.15) [23] ✗ | Blake Griffin (+4.02) [60] ✗ |
| 15 | **Kyle Korver** (+4.60) | George Hill (+3.75) [7] ✓ | Greg Monroe (+4.06) [124] ✗ | Paul Millsap (+4.01) [27] ✗ |
| 16 | **LaMarcus Aldridge** (+4.30) | Andrew Bogut (+3.70) [23] ✗ | Khris Middleton (+3.98) [12] ✓ | Kyrie Irving (+4.00) [13] ✓ |
| 17 | **Tony Allen** (+4.30) | Gordon Hayward (+3.66) [26] ✗ | Lou Williams (+3.88) [34] ✗ | Khris Middleton (+3.91) [12] ✓ |
| 18 | **Nikola Mirotic** (+4.20) | Khris Middleton (+3.55) [12] ✓ | Tony Allen (+3.76) [16] ✓ | John Wall (+3.82) [60] ✗ |
| 19 | **Rudy Gobert** (+4.10) | Tony Allen (+3.49) [16] ✓ | Danny Green (+3.46) [6] ✓ | Jeff Teague (+3.76) [42] ✗ |
| 20 | **Marc Gasol** (+4.00) | Wesley Matthews (+3.41) [24] ✗ | Kyle Lowry (+3.45) [22] ✗ | Tim Duncan (+3.72) [27] ✗ |

### 2014-15 — Playoffs — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Jarrett Jack** (+11.30) | Jimmy Butler (+6.61) [4] ✓ | Stephen Curry (+7.52) [7] ✓ | AlFarouq Aminu (+6.55) [3] ✓ |
| 2 | **Chris Paul** (+11.10) | LeBron James (+6.60) [25] ✗ | Jarrett Jack (+7.26) [1] ✓ | Jimmy Butler (+6.25) [4] ✓ |
| 3 | **AlFarouq Aminu** (+11.00) | Jarrett Jack (+6.58) [1] ✓ | Alan Anderson (+6.51) [5] ✓ | Chris Paul (+6.06) [2] ✓ |
| 4 | **Jimmy Butler** (+9.00) | Stephen Curry (+6.57) [7] ✓ | Jimmy Butler (+6.20) [4] ✓ | Stephen Curry (+5.81) [7] ✓ |
| 5 | **Alan Anderson** (+8.90) | Alan Anderson (+6.17) [5] ✓ | AlFarouq Aminu (+6.13) [3] ✓ | Tim Duncan (+5.69) [8] ✓ |
| 6 | **Otto Porter Jr.** (+8.80) | Chris Paul (+6.10) [2] ✓ | Tim Duncan (+5.90) [8] ✓ | Jarrett Jack (+5.03) [1] ✓ |
| 7 | **Stephen Curry** (+8.70) | AlFarouq Aminu (+5.41) [3] ✓ | James Harden (+5.36) [13] ✓ | CJ McCollum (+4.60) [18] ✓ |
| 8 | **Tim Duncan** (+7.90) | Mike Dunleavy (+5.12) [19] ✓ | Chris Paul (+5.34) [2] ✓ | Kyrie Irving (+4.22) [51] ✗ |
| 9 | **Danny Green** (+7.80) | Tim Duncan (+5.00) [8] ✓ | Mike Dunleavy (+5.28) [19] ✓ | Tony Allen (+4.08) [21] ✗ |
| 10 | **Trevor Ariza** (+7.30) | Otto Porter Jr. (+4.94) [6] ✓ | Otto Porter Jr. (+4.41) [6] ✓ | Anthony Davis (+4.06) [27] ✗ |
| 11 | **Blake Griffin** (+7.20) | James Harden (+4.81) [13] ✓ | Kyle Korver (+4.29) [40] ✗ | Blake Griffin (+3.94) [11] ✓ |
| 12 | **JJ Barea** (+6.50) | DeAndre Jordan (+4.59) [33] ✗ | LeBron James (+4.17) [25] ✗ | James Harden (+3.78) [13] ✓ |
| 13 | **James Harden** (+6.40) | Kyle Korver (+4.30) [40] ✗ | Mike Conley (+4.05) [21] ✗ | LeBron James (+3.62) [25] ✗ |
| 14 | **Manu Ginobili** (+6.30) | Monta Ellis (+4.03) [35] ✗ | DeAndre Jordan (+4.04) [33] ✗ | Alan Anderson (+3.60) [5] ✓ |
| 15 | **Marc Gasol** (+6.10) | Marcin Gortat (+3.83) [45] ✗ | Brook Lopez (+3.89) [27] ✗ | Otto Porter Jr. (+3.38) [6] ✓ |
| 16 | **Derrick Rose** (+5.80) | Dwight Howard (+3.64) [17] ✓ | Blake Griffin (+3.46) [11] ✓ | Pau Gasol (+3.27) [34] ✗ |
| 17 | **Dwight Howard** (+5.70) | Blake Griffin (+3.60) [11] ✓ | Kyrie Irving (+3.43) [51] ✗ | John Wall (+3.26) [40] ✗ |
| 18 | **CJ McCollum** (+5.60) | Kyrie Irving (+3.51) [51] ✗ | Marcin Gortat (+3.36) [45] ✗ | Mike Dunleavy (+2.98) [19] ✓ |
| 19 | **Timofey Mozgov** (+5.30) | Brook Lopez (+3.49) [27] ✗ | Monta Ellis (+3.33) [35] ✗ | Monta Ellis (+2.80) [35] ✗ |
| 20 | **Mike Dunleavy** (+5.30) | Paul Millsap (+3.41) [21] ✗ | Bradley Beal (+3.08) [29] ✗ | DeAndre Jordan (+2.77) [33] ✗ |

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
| 8 | **Dirk Nowitzki** (+4.40) | Carmelo Anthony (+3.49) [9] ✓ | Brandan Wright (+4.19) [40] ✗ |
| 9 | **Carmelo Anthony** (+4.20) | Manu Ginobili (+3.43) [10] ✓ | Carmelo Anthony (+3.80) [9] ✓ |
| 10 | **Manu Ginobili** (+4.00) | Russell Westbrook (+3.32) [15] ✓ | Kyle Lowry (+3.71) [7] ✓ |
| 11 | **Patty Mills** (+3.90) | Kyle Lowry (+3.09) [7] ✓ | Blake Griffin (+3.57) [17] ✓ |
| 12 | **Damian Lillard** (+3.60) | Dirk Nowitzki (+2.95) [7] ✓ | Isaiah Thomas (+3.48) [13] ✓ |
| 13 | **Isaiah Thomas** (+3.50) | Mike Conley (+2.88) [13] ✓ | Manu Ginobili (+3.25) [10] ✓ |
| 14 | **Mike Conley** (+3.50) | Isaiah Thomas (+2.82) [13] ✓ | Russell Westbrook (+3.24) [15] ✓ |
| 15 | **Russell Westbrook** (+3.30) | Joe Johnson (+2.65) [21] ✗ | Nikola Pekovic (+3.10) [64] ✗ |
| 16 | **Ty Lawson** (+3.20) | Kyrie Irving (+2.64) [27] ✗ | Damian Lillard (+3.03) [12] ✓ |
| 17 | **Blake Griffin** (+2.90) | Blake Griffin (+2.50) [17] ✓ | Dwyane Wade (+2.82) [47] ✗ |
| 18 | **Wesley Matthews** (+2.80) | Patty Mills (+2.37) [11] ✓ | Ty Lawson (+2.80) [16] ✓ |
| 19 | **Marco Belinelli** (+2.80) | Paul George (+2.34) [22] ✗ | Mike Conley (+2.80) [13] ✓ |
| 20 | **Jamal Crawford** (+2.80) | John Wall (+2.33) [36] ✗ | Wesley Matthews (+2.68) [18] ✓ |

### 2013-14 — Playoffs — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | Russell Westbrook (+7.11) [6] ✓ | LeBron James (+6.69) [5] ✓ |
| 2 | **Stephen Curry** (+9.20) | Stephen Curry (+6.39) [2] ✓ | Chris Paul (+6.31) [1] ✓ |
| 3 | **Damian Lillard** (+8.00) | Damian Lillard (+6.29) [3] ✓ | Stephen Curry (+4.52) [2] ✓ |
| 4 | **James Harden** (+8.00) | Chris Paul (+6.15) [1] ✓ | Joe Johnson (+4.46) [9] ✓ |
| 5 | **LeBron James** (+6.90) | Kevin Durant (+5.37) [7] ✓ | Russell Westbrook (+4.05) [6] ✓ |
| 6 | **Russell Westbrook** (+6.20) | LeBron James (+5.36) [5] ✓ | Damian Lillard (+3.92) [3] ✓ |
| 7 | **Kevin Durant** (+5.10) | James Harden (+4.54) [3] ✓ | Vince Carter (+3.45) [19] ✗ |
| 8 | **Jose Calderon** (+5.10) | Kyle Lowry (+4.52) [11] ✓ | Dwight Howard (+3.31) [34] ✗ |
| 9 | **Joe Johnson** (+4.90) | Manu Ginobili (+3.59) [10] ✓ | James Harden (+3.31) [3] ✓ |
| 10 | **Manu Ginobili** (+4.40) | LaMarcus Aldridge (+3.52) [19] ✓ | Manu Ginobili (+3.24) [10] ✓ |
| 11 | **Kyle Lowry** (+3.80) | Jose Calderon (+3.22) [7] ✓ | Tiago Splitter (+2.79) [50] ✗ |
| 12 | **Blake Griffin** (+3.70) | DeMar DeRozan (+3.19) [14] ✓ | Kevin Durant (+2.72) [7] ✓ |
| 13 | **Patty Mills** (+3.50) | Joe Johnson (+3.06) [9] ✓ | Tim Duncan (+2.54) [32] ✗ |
| 14 | **DeMar DeRozan** (+3.40) | Mirza Teletovic (+2.79) [17] ✓ | Patrick Patterson (+2.45) [29] ✗ |
| 15 | **JJ Redick** (+3.30) | Patty Mills (+2.77) [13] ✓ | Kyle Lowry (+2.44) [11] ✓ |
| 16 | **Devin Harris** (+3.30) | Jamal Crawford (+2.67) [23] ✗ | Blake Griffin (+2.27) [12] ✓ |
| 17 | **Ray Allen** (+3.20) | Devin Harris (+2.67) [15] ✓ | JJ Redick (+2.21) [15] ✓ |
| 18 | **Mirza Teletovic** (+3.20) | Danny Green (+2.39) [23] ✗ | Chris Bosh (+2.19) [41] ✗ |
| 19 | **LaMarcus Aldridge** (+3.10) | Kawhi Leonard (+2.38) [32] ✗ | Greivis Vasquez (+2.18) [38] ✗ |
| 20 | **Trevor Ariza** (+3.10) | Bradley Beal (+2.36) [22] ✗ | DeMar DeRozan (+2.04) [14] ✓ |

### 2014-15 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+8.50) | Chris Paul (+7.71) [1] ✓ | Chris Paul (+6.99) [1] ✓ |
| 2 | **James Harden** (+7.70) | James Harden (+5.85) [2] ✓ | James Harden (+5.71) [2] ✓ |
| 3 | **Russell Westbrook** (+6.10) | LeBron James (+5.54) [5] ✓ | LeBron James (+5.62) [5] ✓ |
| 4 | **Kyrie Irving** (+5.50) | Kyrie Irving (+5.15) [4] ✓ | Anthony Davis (+5.11) [9] ✓ |
| 5 | **LeBron James** (+5.30) | Damian Lillard (+4.73) [11] ✓ | Russell Westbrook (+5.02) [3] ✓ |
| 6 | **Lou Williams** (+5.20) | Russell Westbrook (+4.70) [3] ✓ | Jimmy Butler (+4.38) [20] ✗ |
| 7 | **Kyle Korver** (+4.60) | Lou Williams (+3.89) [6] ✓ | Blake Griffin (+4.19) [20] ✗ |
| 8 | **Isaiah Thomas** (+4.50) | Isaiah Thomas (+3.83) [8] ✓ | Kyrie Irving (+4.13) [4] ✓ |
| 9 | **Anthony Davis** (+4.30) | Klay Thompson (+3.33) [9] ✓ | Lou Williams (+4.08) [6] ✓ |
| 10 | **Klay Thompson** (+4.30) | Blake Griffin (+3.14) [20] ✗ | Klay Thompson (+4.00) [9] ✓ |
| 11 | **Damian Lillard** (+4.00) | George Hill (+2.76) [12] ✓ | George Hill (+3.81) [12] ✓ |
| 12 | **George Hill** (+3.90) | Gordon Hayward (+2.59) [20] ✓ | Kawhi Leonard (+3.44) [15] ✓ |
| 13 | **Ty Lawson** (+3.80) | Kyle Lowry (+2.56) [18] ✓ | JJ Redick (+3.36) [29] ✗ |
| 14 | **Carmelo Anthony** (+3.80) | Jimmy Butler (+2.53) [20] ✗ | Ty Lawson (+3.18) [13] ✓ |
| 15 | **Kawhi Leonard** (+3.70) | JJ Redick (+2.49) [29] ✗ | Gordon Hayward (+3.01) [20] ✓ |
| 16 | **Rudy Gay** (+3.50) | Mike Conley (+2.48) [30] ✗ | Isaiah Thomas (+2.98) [8] ✓ |
| 17 | **DeAndre Jordan** (+3.40) | Dwyane Wade (+2.41) [40] ✗ | Carmelo Anthony (+2.96) [13] ✓ |
| 18 | **Kyle Lowry** (+3.30) | Jrue Holiday (+2.39) [18] ✓ | Damian Lillard (+2.92) [11] ✓ |
| 19 | **Jrue Holiday** (+3.30) | Carmelo Anthony (+2.36) [13] ✓ | Wesley Matthews (+2.78) [33] ✗ |
| 20 | **Gordon Hayward** (+3.20) | Ty Lawson (+2.28) [13] ✓ | Brandon Jennings (+2.74) [23] ✗ |

### 2014-15 — Playoffs — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+8.70) | James Harden (+5.64) [2] ✓ | Chris Paul (+6.15) [1] ✓ |
| 2 | **James Harden** (+8.00) | Stephen Curry (+5.63) [6] ✓ | James Harden (+4.99) [2] ✓ |
| 3 | **CJ McCollum** (+7.90) | Chris Paul (+4.84) [1] ✓ | Stephen Curry (+4.89) [6] ✓ |
| 4 | **Monta Ellis** (+6.20) | CJ McCollum (+4.36) [3] ✓ | Anthony Davis (+4.28) [86] ✗ |
| 5 | **Alan Anderson** (+6.10) | Monta Ellis (+4.10) [4] ✓ | Tim Duncan (+4.15) [9] ✓ |
| 6 | **Stephen Curry** (+5.70) | Jarrett Jack (+3.68) [16] ✓ | Jimmy Butler (+3.97) [7] ✓ |
| 7 | **Jimmy Butler** (+5.30) | Kyrie Irving (+3.59) [15] ✓ | Blake Griffin (+3.83) [20] ✓ |
| 8 | **AlFarouq Aminu** (+5.30) | Jimmy Butler (+3.54) [7] ✓ | Monta Ellis (+3.68) [4] ✓ |
| 9 | **Tim Duncan** (+5.20) | Alan Anderson (+3.53) [5] ✓ | AlFarouq Aminu (+3.58) [7] ✓ |
| 10 | **Vince Carter** (+5.20) | Tim Duncan (+3.45) [9] ✓ | Kyrie Irving (+3.26) [15] ✓ |
| 11 | **Mike Dunleavy** (+4.70) | DeMar DeRozan (+3.03) [12] ✓ | CJ McCollum (+3.18) [3] ✓ |
| 12 | **DeMar DeRozan** (+4.60) | John Wall (+3.03) [42] ✗ | Alan Anderson (+3.09) [5] ✓ |
| 13 | **Eric Gordon** (+4.50) | Damian Lillard (+2.79) [57] ✗ | LeBron James (+2.41) [17] ✓ |
| 14 | **JJ Barea** (+4.40) | Mike Dunleavy (+2.69) [11] ✓ | Courtney Lee (+2.32) [33] ✗ |
| 15 | **Kyrie Irving** (+4.10) | AlFarouq Aminu (+2.63) [7] ✓ | Mike Dunleavy (+2.00) [11] ✓ |
| 16 | **Jarrett Jack** (+3.80) | Manu Ginobili (+2.44) [17] ✓ | Mike Conley (+1.97) [21] ✗ |
| 17 | **LeBron James** (+3.60) | LeBron James (+2.08) [17] ✓ | DeMarre Carroll (+1.88) [23] ✗ |
| 18 | **Manu Ginobili** (+3.60) | Bradley Beal (+2.06) [27] ✗ | Bradley Beal (+1.82) [27] ✗ |
| 19 | **Paul Pierce** (+3.60) | Derrick Rose (+2.03) [26] ✗ | Andre Iguodala (+1.74) [38] ✗ |
| 20 | **Blake Griffin** (+3.50) | Dirk Nowitzki (+1.90) [42] ✗ | Boris Diaw (+1.66) [37] ✗ |

### 2013-14 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.00) | Andrew Bogut (+4.44) [4] ✓ | Andrew Bogut (+3.58) [4] ✓ |
| 2 | **Draymond Green** (+4.60) | Draymond Green (+4.12) [2] ✓ | Kawhi Leonard (+3.44) [1] ✓ |
| 3 | **Joakim Noah** (+4.50) | Anthony Davis (+3.83) [33] ✗ | Draymond Green (+3.32) [2] ✓ |
| 4 | **Andrew Bogut** (+4.40) | Kevin Garnett (+3.63) [11] ✓ | DeAndre Jordan (+3.19) [64] ✗ |
| 5 | **Michael KiddGilchrist** (+4.40) | Tiago Splitter (+3.40) [6] ✓ | Jimmy Butler (+3.17) [14] ✓ |
| 6 | **Tiago Splitter** (+4.20) | CJ Watson (+3.22) [17] ✓ | Tony Allen (+3.07) [30] ✗ |
| 7 | **Danny Green** (+4.00) | Jae Crowder (+3.18) [18] ✓ | Joakim Noah (+3.05) [3] ✓ |
| 8 | **Chris Paul** (+3.90) | Kawhi Leonard (+3.11) [1] ✓ | Danny Green (+2.82) [7] ✓ |
| 9 | **Nene** (+3.80) | Paul George (+3.06) [21] ✗ | Ricky Rubio (+2.79) [47] ✗ |
| 10 | **Anderson Varejao** (+3.60) | Derek Fisher (+2.99) [43] ✗ | Andre Iguodala (+2.69) [25] ✗ |
| 11 | **Nick Calathes** (+3.50) | Andre Iguodala (+2.87) [25] ✗ | Paul George (+2.62) [21] ✗ |
| 12 | **Ian Mahinmi** (+3.50) | Ian Mahinmi (+2.86) [11] ✓ | Kyle OQuinn (+2.43) [52] ✗ |
| 13 | **Kevin Garnett** (+3.50) | Paul Millsap (+2.80) [23] ✗ | Roy Hibbert (+2.43) [14] ✓ |
| 14 | **Jimmy Butler** (+3.40) | Tony Allen (+2.80) [30] ✗ | David West (+2.32) [57] ✗ |
| 15 | **Roy Hibbert** (+3.40) | Danny Green (+2.79) [7] ✓ | Paul Millsap (+2.20) [23] ✗ |
| 16 | **DeMarcus Cousins** (+3.30) | Jimmy Butler (+2.71) [14] ✓ | Ian Mahinmi (+2.14) [11] ✓ |
| 17 | **CJ Watson** (+3.20) | Nene (+2.70) [9] ✓ | Anderson Varejao (+2.08) [10] ✓ |
| 18 | **Tim Duncan** (+3.00) | Chris Bosh (+2.67) [72] ✗ | Tim Duncan (+2.05) [18] ✓ |
| 19 | **Kris Humphries** (+3.00) | Joakim Noah (+2.57) [3] ✓ | CJ Watson (+2.03) [17] ✓ |
| 20 | **Jae Crowder** (+3.00) | Anderson Varejao (+2.57) [10] ✓ | Chris Paul (+2.00) [8] ✓ |

### 2013-14 — Playoffs — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Draymond Green** (+8.00) | Joakim Noah (+4.97) [8] ✓ | Danny Green (+3.39) [19] ✓ |
| 2 | **Paul Millsap** (+7.60) | Marcin Gortat (+3.31) [19] ✓ | Pero Antic (+3.12) [3] ✓ |
| 3 | **Pero Antic** (+6.50) | Chris Andersen (+3.24) [7] ✓ | Kawhi Leonard (+3.08) [12] ✓ |
| 4 | **Andray Blatche** (+6.10) | Pero Antic (+3.10) [3] ✓ | Paul Millsap (+2.79) [2] ✓ |
| 5 | **Nick Collison** (+6.10) | Danny Green (+3.10) [19] ✓ | Draymond Green (+2.77) [1] ✓ |
| 6 | **Greivis Vasquez** (+6.00) | Kevin Garnett (+3.06) [49] ✗ | Trevor Ariza (+2.74) [26] ✗ |
| 7 | **Chris Andersen** (+5.40) | Serge Ibaka (+2.96) [13] ✓ | Manu Ginobili (+2.46) [21] ✗ |
| 8 | **Joakim Noah** (+5.30) | Tiago Splitter (+2.91) [9] ✓ | John Wall (+2.42) [41] ✗ |
| 9 | **Tiago Splitter** (+5.00) | Andray Blatche (+2.83) [4] ✓ | Serge Ibaka (+2.33) [13] ✓ |
| 10 | **Vince Carter** (+5.00) | Greivis Vasquez (+2.79) [6] ✓ | Joakim Noah (+2.13) [8] ✓ |
| 11 | **Rashard Lewis** (+4.90) | Paul Pierce (+2.77) [34] ✗ | Bradley Beal (+2.06) [24] ✗ |
| 12 | **Kawhi Leonard** (+4.40) | Kyle Korver (+2.65) [62] ✗ | Marcin Gortat (+2.04) [19] ✓ |
| 13 | **Serge Ibaka** (+4.20) | Trevor Ariza (+2.61) [26] ✗ | DeAndre Jordan (+1.81) [40] ✗ |
| 14 | **Ian Mahinmi** (+4.20) | Kawhi Leonard (+2.59) [12] ✓ | Chris Andersen (+1.81) [7] ✓ |
| 15 | **Zach Randolph** (+4.20) | Manu Ginobili (+2.52) [21] ✗ | Chris Paul (+1.76) [24] ✗ |
| 16 | **Marc Gasol** (+4.10) | John Wall (+2.51) [41] ✗ | Patty Mills (+1.56) [18] ✓ |
| 17 | **Deron Williams** (+3.80) | Draymond Green (+2.43) [1] ✓ | David West (+1.50) [39] ✗ |
| 18 | **Patty Mills** (+3.70) | Patty Mills (+2.25) [18] ✓ | Tony Allen (+1.46) [30] ✗ |
| 19 | **Danny Green** (+3.50) | Patrick Patterson (+2.17) [64] ✗ | Tiago Splitter (+1.43) [9] ✓ |
| 20 | **Marcin Gortat** (+3.50) | Paul Millsap (+2.16) [2] ✓ | Kevin Garnett (+1.25) [49] ✗ |

### 2014-15 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.20) | Andrew Bogut (+4.93) [5] ✓ | Kawhi Leonard (+4.47) [1] ✓ |
| 2 | **Draymond Green** (+5.10) | Rudy Gobert (+4.81) [3] ✓ | Tony Allen (+4.32) [3] ✓ |
| 3 | **Rudy Gobert** (+4.80) | Draymond Green (+4.72) [2] ✓ | Draymond Green (+3.75) [2] ✓ |
| 4 | **Tony Allen** (+4.80) | Tony Allen (+4.13) [3] ✓ | Andrew Bogut (+3.21) [5] ✓ |
| 5 | **Andrew Bogut** (+4.70) | Kawhi Leonard (+3.70) [1] ✓ | DeAndre Jordan (+3.15) [60] ✗ |
| 6 | **Anthony Davis** (+4.50) | Nerlens Noel (+3.69) [19] ✓ | Nerlens Noel (+2.89) [19] ✓ |
| 7 | **DeMarcus Cousins** (+4.40) | Greg Monroe (+3.68) [111] ✗ | Anthony Davis (+2.86) [6] ✓ |
| 8 | **Marcin Gortat** (+3.60) | Nene (+3.54) [17] ✓ | Rudy Gobert (+2.78) [3] ✓ |
| 9 | **Tim Duncan** (+3.50) | Tyson Chandler (+3.18) [20] ✗ | Danny Green (+2.72) [14] ✓ |
| 10 | **Andre Roberson** (+3.40) | Zaza Pachulia (+3.16) [12] ✓ | AlFarouq Aminu (+2.59) [20] ✗ |
| 11 | **Kosta Koufos** (+3.30) | Anthony Davis (+2.99) [6] ✓ | Tim Duncan (+2.49) [9] ✓ |
| 12 | **Zaza Pachulia** (+3.20) | Dwight Howard (+2.92) [40] ✗ | Paul Millsap (+2.35) [26] ✗ |
| 13 | **Khris Middleton** (+3.10) | Jared Dudley (+2.91) [42] ✗ | Khris Middleton (+2.07) [13] ✓ |
| 14 | **Danny Green** (+3.00) | Andre Roberson (+2.88) [10] ✓ | Andre Roberson (+2.05) [10] ✓ |
| 15 | **Serge Ibaka** (+3.00) | Tim Duncan (+2.76) [9] ✓ | Bismack Biyombo (+2.05) [102] ✗ |
| 16 | **Michael KiddGilchrist** (+3.00) | Pau Gasol (+2.74) [85] ✗ | Marcus Smart (+1.98) [42] ✗ |
| 17 | **Jonas Jerebko** (+2.80) | AlFarouq Aminu (+2.69) [20] ✗ | John Wall (+1.97) [127] ✗ |
| 18 | **Nene** (+2.80) | Jonas Jerebko (+2.61) [17] ✓ | DeMarcus Cousins (+1.95) [7] ✓ |
| 19 | **Nerlens Noel** (+2.70) | DeMarcus Cousins (+2.60) [7] ✓ | Marcin Gortat (+1.93) [8] ✓ |
| 20 | **Marc Gasol** (+2.60) | Nikola Mirotic (+2.59) [33] ✗ | Michael KiddGilchrist (+1.74) [14] ✓ |

### 2014-15 — Playoffs — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Jarrett Jack** (+7.50) | DeAndre Jordan (+4.48) [29] ✗ | Tony Allen (+5.01) [10] ✓ |
| 2 | **Anthony Davis** (+7.20) | Tony Allen (+4.04) [10] ✓ | Jarrett Jack (+3.62) [1] ✓ |
| 3 | **Timofey Mozgov** (+6.90) | Dwight Howard (+3.58) [7] ✓ | Pau Gasol (+3.02) [15] ✓ |
| 4 | **Otto Porter Jr.** (+6.30) | Jarrett Jack (+3.57) [1] ✓ | AlFarouq Aminu (+2.97) [6] ✓ |
| 5 | **Trevor Ariza** (+6.10) | Pau Gasol (+3.55) [15] ✓ | Jimmy Butler (+2.29) [13] ✓ |
| 6 | **AlFarouq Aminu** (+5.80) | Otto Porter Jr. (+3.51) [4] ✓ | Andrew Bogut (+2.28) [35] ✗ |
| 7 | **Dwight Howard** (+5.70) | AlFarouq Aminu (+3.50) [6] ✓ | Otto Porter Jr. (+2.24) [4] ✓ |
| 8 | **Danny Green** (+5.50) | Al Horford (+3.46) [12] ✓ | Paul Millsap (+2.24) [29] ✗ |
| 9 | **Marc Gasol** (+5.30) | Timofey Mozgov (+3.41) [3] ✓ | John Wall (+2.07) [44] ✗ |
| 10 | **Tony Allen** (+5.00) | Nene (+3.21) [11] ✓ | Dwight Howard (+2.05) [7] ✓ |
| 11 | **Nene** (+4.70) | Joakim Noah (+3.11) [29] ✗ | Kyle Korver (+2.03) [41] ✗ |
| 12 | **Al Horford** (+4.40) | Kyle Korver (+3.10) [41] ✗ | DeAndre Jordan (+1.99) [29] ✗ |
| 13 | **Jimmy Butler** (+3.70) | Andrew Bogut (+2.98) [35] ✗ | Drew Gooden (+1.97) [69] ✗ |
| 14 | **Blake Griffin** (+3.70) | Alan Anderson (+2.98) [20] ✓ | Iman Shumpert (+1.72) [54] ✗ |
| 15 | **Pau Gasol** (+3.50) | Harrison Barnes (+2.90) [43] ✗ | Danny Green (+1.71) [8] ✓ |
| 16 | **Ramon Sessions** (+3.50) | Mike Conley (+2.71) [38] ✗ | Matt Barnes (+1.68) [17] ✓ |
| 17 | **Matt Barnes** (+3.40) | Jimmy Butler (+2.66) [13] ✓ | Joakim Noah (+1.65) [29] ✗ |
| 18 | **Stephen Curry** (+3.00) | Marc Gasol (+2.62) [9] ✓ | Tim Duncan (+1.54) [22] ✗ |
| 19 | **Derrick Rose** (+3.00) | Mike Dunleavy (+2.59) [49] ✗ | Nikola Mirotic (+1.51) [77] ✗ |
| 20 | **Alan Anderson** (+2.80) | Tim Duncan (+2.44) [22] ✗ | Derrick Rose (+1.42) [18] ✓ |

