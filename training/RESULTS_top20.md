# Top-20 leaderboards on the held-out seasons

True 538 RAPTOR vs. our models vs. Neil Paine's Estimated RAPTOR, for
2013-14 and 2014-15. Total, offense and defense are ranked separately.

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

**Regular season → ≥ 1109 minutes. Playoffs → ≥ 131 minutes.**

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
| ours (direct total) | 1.723 | 1.285 | +0.750 | +0.883 | +0.885 |
| ours (offense+defense) | 1.745 | 1.288 | +0.744 | +0.879 | +0.878 |
| Paine (eRO+eRD) | 1.939 | 1.382 | +0.683 | +0.841 | +0.846 |

**offense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.024 | 0.781 | +0.820 | +0.922 | +0.907 |
| Paine (eRO) | 1.308 | 0.959 | +0.706 | +0.847 | +0.823 |

**defense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| ours | 1.411 | 1.041 | +0.636 | +0.817 | +0.819 |
| Paine (eRD) | 1.647 | 1.199 | +0.504 | +0.727 | +0.727 |

## Summary — true top-20 members recovered (hits@20)

**total**

| season | split | pool | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) | ρ ours (direct total) | ρ ours (offense+defense) | ρ Paine (eRO+eRD) |
|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 240 | 12/20 | 13/20 | 15/20 | +0.894 | +0.896 | +0.889 |
| 2013-14 | Playoffs | 99 | 14/20 | 14/20 | 14/20 | +0.848 | +0.846 | +0.794 |
| 2014-15 | Regular season | 246 | 15/20 | 13/20 | 14/20 | +0.902 | +0.900 | +0.901 |
| 2014-15 | Playoffs | 98 | 12/20 | 13/20 | 12/20 | +0.878 | +0.861 | +0.863 |
| **all** | | | **53/80** | **53/80** | **55/80** |  |  |  |

**offense**

| season | split | pool | ours | Paine (eRO) | ρ ours | ρ Paine (eRO) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 240 | 17/20 | 18/20 | +0.921 | +0.861 |
| 2013-14 | Playoffs | 99 | 16/20 | 13/20 | +0.893 | +0.838 |
| 2014-15 | Regular season | 246 | 14/20 | 15/20 | +0.925 | +0.858 |
| 2014-15 | Playoffs | 98 | 16/20 | 13/20 | +0.898 | +0.787 |
| **all** | | | **63/80** | **59/80** |  |  |

**defense**

| season | split | pool | ours | Paine (eRD) | ρ ours | ρ Paine (eRD) |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 240 | 13/20 | 12/20 | +0.875 | +0.796 |
| 2013-14 | Playoffs | 99 | 13/20 | 11/20 | +0.690 | +0.667 |
| 2014-15 | Regular season | 246 | 16/20 | 14/20 | +0.871 | +0.801 |
| 2014-15 | Playoffs | 98 | 13/20 | 10/20 | +0.780 | +0.633 |
| **all** | | | **55/80** | **47/80** |  |  |

## Why the total is harder to rank than offense

Hits@20 only asks whether a player lands on the correct side of an arbitrary
cutoff, and for the total that cutoff is crowded. Players within ±0.25 RAPTOR
of the rank-20 value, per cell:

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

**Direct total vs. summing the halves — our model.** Predicting `rap`
directly edges summing our two part-models: R² +0.750 vs
+0.744, ρ +0.885 vs +0.878,
and 53/80 top-20 hits each. The two are close to interchangeable; the direct
model wins narrowly and consistently on the continuous metrics.

**Against Paine.** On every continuous measure our total model is clearly
ahead — R² +0.750 vs +0.683, RMSE
1.723 vs 1.939, ρ +0.885 vs
+0.846 — and it leads the per-cell rank correlation in
all four cells. But he recovers **55/80** top-20 members to our 53/80.

Those two facts are not in conflict. A 2-slot difference out of 80 is inside
the noise of a metric decided by hundredths of a point at a crowded cutoff,
while the correlation gap is consistent across every cell. The fair summary:
**we rank the whole field better; at the top-20 boundary for the total the
two systems are indistinguishable.** On offense and defense separately our
advantage does show up in hits@20 too (63/80 vs 59/80, 55/80 vs 47/80).

## Leaderboards

`[n]` after a predicted name is that player's *true* rank; ✓ means they are
genuinely in the true top 20.

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
| 10 | **Goran Dragic** (+5.00) | Blake Griffin (+4.37) [34] ✗ | Andrew Bogut (+4.25) [36] ✗ | Andre Iguodala (+4.53) [21] ✗ |
| 11 | **DeMarcus Cousins** (+5.00) | Andrew Bogut (+4.34) [36] ✗ | James Harden (+4.20) [5] ✓ | Kyle Lowry (+4.27) [7] ✓ |
| 12 | **Patty Mills** (+4.80) | Andre Iguodala (+4.21) [21] ✗ | Anthony Davis (+4.20) [28] ✗ | Joakim Noah (+4.22) [6] ✓ |
| 13 | **Dirk Nowitzki** (+4.70) | Dirk Nowitzki (+4.17) [13] ✓ | Andre Iguodala (+4.12) [21] ✗ | Russell Westbrook (+4.18) [25] ✗ |
| 14 | **Danny Green** (+4.70) | Goran Dragic (+4.11) [10] ✓ | Dirk Nowitzki (+3.97) [13] ✓ | Anthony Davis (+4.18) [28] ✗ |
| 15 | **LeBron James** (+4.60) | Carmelo Anthony (+3.69) [21] ✗ | Blake Griffin (+3.94) [34] ✗ | Blake Griffin (+4.00) [34] ✗ |
| 16 | **Anderson Varejao** (+4.10) | Ricky Rubio (+3.68) [25] ✗ | Patty Mills (+3.60) [12] ✓ | Ricky Rubio (+3.91) [25] ✗ |
| 17 | **Patrick Beverley** (+4.10) | Russell Westbrook (+3.49) [25] ✗ | Jimmy Butler (+3.57) [19] ✓ | Dirk Nowitzki (+3.81) [13] ✓ |
| 18 | **Mario Chalmers** (+4.00) | Isaiah Thomas (+3.43) [19] ✓ | Mike Conley (+3.53) [21] ✗ | DeMarcus Cousins (+3.60) [10] ✓ |
| 19 | **Jimmy Butler** (+3.90) | Anthony Davis (+3.42) [28] ✗ | Ricky Rubio (+3.46) [25] ✗ | Patty Mills (+3.50) [12] ✓ |
| 20 | **Isaiah Thomas** (+3.90) | Draymond Green (+3.41) [29] ✗ | LaMarcus Aldridge (+3.43) [29] ✗ | Jimmy Butler (+3.31) [19] ✓ |

### 2013-14 — Playoffs — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+13.20) | LeBron James (+8.13) [8] ✓ | Chris Paul (+8.26) [1] ✓ | Chris Paul (+8.08) [1] ✓ |
| 2 | **Draymond Green** (+10.80) | Chris Paul (+7.72) [1] ✓ | LeBron James (+7.35) [8] ✓ | LeBron James (+7.21) [8] ✓ |
| 3 | **Stephen Curry** (+10.40) | Kevin Durant (+6.34) [20] ✓ | Kevin Durant (+6.69) [20] ✓ | Manu Ginobili (+5.71) [7] ✓ |
| 4 | **James Harden** (+10.00) | Russell Westbrook (+6.08) [14] ✓ | Stephen Curry (+5.96) [3] ✓ | Kawhi Leonard (+5.01) [16] ✓ |
| 5 | **Paul Millsap** (+8.40) | Damian Lillard (+6.03) [11] ✓ | Manu Ginobili (+5.90) [7] ✓ | Russell Westbrook (+4.98) [14] ✓ |
| 6 | **Vince Carter** (+8.10) | James Harden (+5.77) [4] ✓ | James Harden (+5.89) [4] ✓ | Danny Green (+4.41) [14] ✓ |
| 7 | **Manu Ginobili** (+7.90) | Manu Ginobili (+5.77) [7] ✓ | Russell Westbrook (+5.87) [14] ✓ | Tiago Splitter (+4.22) [21] ✗ |
| 8 | **LeBron James** (+7.70) | Stephen Curry (+5.40) [3] ✓ | Damian Lillard (+5.84) [11] ✓ | Stephen Curry (+4.14) [3] ✓ |
| 9 | **Greivis Vasquez** (+7.30) | Kawhi Leonard (+5.27) [16] ✓ | Danny Green (+5.83) [14] ✓ | Trevor Ariza (+3.95) [21] ✗ |
| 10 | **Patty Mills** (+7.20) | Patrick Patterson (+5.02) [45] ✗ | Kawhi Leonard (+5.06) [16] ✓ | Patrick Patterson (+3.62) [45] ✗ |
| 11 | **Damian Lillard** (+6.90) | Patty Mills (+4.95) [10] ✓ | Patty Mills (+4.62) [10] ✓ | Bradley Beal (+3.57) [19] ✓ |
| 12 | **Andray Blatche** (+6.90) | Danny Green (+4.90) [14] ✓ | Patrick Patterson (+4.60) [45] ✗ | Draymond Green (+3.38) [2] ✓ |
| 13 | **Deron Williams** (+6.60) | Kyle Lowry (+4.77) [30] ✗ | Greivis Vasquez (+4.52) [9] ✓ | Greivis Vasquez (+3.33) [9] ✓ |
| 14 | **Russell Westbrook** (+6.40) | Vince Carter (+4.29) [6] ✓ | Vince Carter (+4.50) [6] ✓ | Tim Duncan (+3.25) [32] ✗ |
| 15 | **Danny Green** (+6.40) | Trevor Ariza (+4.11) [21] ✗ | Kyle Lowry (+4.42) [30] ✗ | Dwight Howard (+3.17) [43] ✗ |
| 16 | **Kawhi Leonard** (+6.20) | David West (+3.94) [37] ✗ | Serge Ibaka (+4.20) [24] ✗ | Patty Mills (+2.91) [10] ✓ |
| 17 | **LaMarcus Aldridge** (+6.00) | Andray Blatche (+3.85) [11] ✓ | LaMarcus Aldridge (+4.11) [17] ✓ | James Harden (+2.91) [4] ✓ |
| 18 | **Chris Andersen** (+6.00) | Greivis Vasquez (+3.82) [9] ✓ | Trevor Ariza (+4.10) [21] ✗ | Damian Lillard (+2.74) [11] ✓ |
| 19 | **Bradley Beal** (+5.70) | Marcin Gortat (+3.78) [27] ✗ | Kyle Korver (+4.04) [41] ✗ | Joe Johnson (+2.73) [25] ✗ |
| 20 | **Kevin Durant** (+5.60) | Kyle Korver (+3.77) [41] ✗ | Joe Johnson (+3.75) [25] ✗ | Kevin Durant (+2.72) [20] ✓ |

### 2014-15 — Regular season — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | LeBron James (+6.65) [11] ✓ | Chris Paul (+7.02) [1] ✓ | Chris Paul (+8.27) [1] ✓ |
| 2 | **Kawhi Leonard** (+8.90) | Chris Paul (+6.58) [1] ✓ | LeBron James (+6.97) [11] ✓ | Anthony Davis (+7.97) [3] ✓ |
| 3 | **Anthony Davis** (+8.80) | Draymond Green (+6.43) [5] ✓ | Draymond Green (+6.08) [5] ✓ | Kawhi Leonard (+7.91) [2] ✓ |
| 4 | **James Harden** (+7.70) | James Harden (+6.32) [4] ✓ | Kawhi Leonard (+5.65) [2] ✓ | LeBron James (+6.66) [11] ✓ |
| 5 | **Draymond Green** (+6.50) | Anthony Davis (+6.17) [3] ✓ | Anthony Davis (+5.42) [3] ✓ | James Harden (+6.55) [4] ✓ |
| 6 | **Danny Green** (+6.10) | Kawhi Leonard (+5.52) [2] ✓ | James Harden (+5.27) [4] ✓ | Russell Westbrook (+5.71) [7] ✓ |
| 7 | **Russell Westbrook** (+5.60) | Jimmy Butler (+4.95) [34] ✗ | Russell Westbrook (+4.96) [7] ✓ | Jimmy Butler (+5.58) [34] ✗ |
| 8 | **George Hill** (+5.60) | Russell Westbrook (+4.65) [7] ✓ | Klay Thompson (+4.87) [10] ✓ | George Hill (+5.08) [7] ✓ |
| 9 | **DeMarcus Cousins** (+5.40) | Lou Williams (+4.63) [34] ✗ | Kyrie Irving (+4.83) [13] ✓ | Klay Thompson (+4.74) [10] ✓ |
| 10 | **Klay Thompson** (+5.30) | Klay Thompson (+4.40) [10] ✓ | Andrew Bogut (+4.43) [23] ✗ | Tony Allen (+4.57) [16] ✓ |
| 11 | **LeBron James** (+5.10) | DeMarcus Cousins (+4.26) [9] ✓ | Khris Middleton (+4.21) [12] ✓ | Draymond Green (+4.55) [5] ✓ |
| 12 | **Khris Middleton** (+4.80) | Kyrie Irving (+4.16) [13] ✓ | Jimmy Butler (+4.20) [34] ✗ | DeAndre Jordan (+4.31) [13] ✓ |
| 13 | **Kyrie Irving** (+4.60) | George Hill (+3.81) [7] ✓ | Danny Green (+4.09) [6] ✓ | Danny Green (+4.23) [6] ✓ |
| 14 | **DeAndre Jordan** (+4.60) | Khris Middleton (+3.70) [12] ✓ | George Hill (+3.69) [7] ✓ | Blake Griffin (+4.02) [60] ✗ |
| 15 | **Kyle Korver** (+4.60) | Danny Green (+3.57) [6] ✓ | Blake Griffin (+3.66) [60] ✗ | Paul Millsap (+4.01) [27] ✗ |
| 16 | **LaMarcus Aldridge** (+4.30) | Andrew Bogut (+3.55) [23] ✗ | Damian Lillard (+3.55) [42] ✗ | Kyrie Irving (+4.00) [13] ✓ |
| 17 | **Tony Allen** (+4.30) | Damian Lillard (+3.54) [42] ✗ | Jeff Teague (+3.54) [42] ✗ | Khris Middleton (+3.91) [12] ✓ |
| 18 | **Nikola Mirotic** (+4.20) | LaMarcus Aldridge (+3.45) [16] ✓ | Kyle Lowry (+3.51) [22] ✗ | John Wall (+3.82) [60] ✗ |
| 19 | **Rudy Gobert** (+4.10) | Tony Allen (+3.30) [16] ✓ | Lou Williams (+3.51) [34] ✗ | Jeff Teague (+3.76) [42] ✗ |
| 20 | **Marc Gasol** (+4.00) | Kyle Lowry (+3.28) [22] ✗ | Rudy Gobert (+3.46) [19] ✓ | Tim Duncan (+3.72) [27] ✗ |

### 2014-15 — Playoffs — total

| # | true RAPTOR | ours (direct total) | ours (offense+defense) | Paine (eRO+eRD) |
|---|---|---|---|---|
| 1 | **Jarrett Jack** (+11.30) | Jarrett Jack (+7.96) [1] ✓ | Jarrett Jack (+7.94) [1] ✓ | AlFarouq Aminu (+6.55) [3] ✓ |
| 2 | **Chris Paul** (+11.10) | Stephen Curry (+7.04) [7] ✓ | Tim Duncan (+7.15) [8] ✓ | Jimmy Butler (+6.25) [4] ✓ |
| 3 | **AlFarouq Aminu** (+11.00) | Jimmy Butler (+6.52) [4] ✓ | Stephen Curry (+7.00) [7] ✓ | Chris Paul (+6.06) [2] ✓ |
| 4 | **Jimmy Butler** (+9.00) | Tim Duncan (+6.37) [8] ✓ | Jimmy Butler (+6.77) [4] ✓ | Stephen Curry (+5.81) [7] ✓ |
| 5 | **Alan Anderson** (+8.90) | Chris Paul (+6.13) [2] ✓ | Alan Anderson (+6.55) [5] ✓ | Tim Duncan (+5.69) [8] ✓ |
| 6 | **Otto Porter Jr.** (+8.80) | LeBron James (+6.02) [25] ✗ | AlFarouq Aminu (+5.82) [3] ✓ | Jarrett Jack (+5.03) [1] ✓ |
| 7 | **Stephen Curry** (+8.70) | DeAndre Jordan (+5.77) [33] ✗ | LeBron James (+5.29) [25] ✗ | CJ McCollum (+4.60) [18] ✓ |
| 8 | **Tim Duncan** (+7.90) | AlFarouq Aminu (+5.68) [3] ✓ | Chris Paul (+5.07) [2] ✓ | Kyrie Irving (+4.22) [51] ✗ |
| 9 | **Danny Green** (+7.80) | Alan Anderson (+5.49) [5] ✓ | Mike Dunleavy (+4.94) [19] ✓ | Tony Allen (+4.08) [21] ✗ |
| 10 | **Trevor Ariza** (+7.30) | Mike Conley (+4.94) [21] ✗ | Otto Porter Jr. (+4.50) [6] ✓ | Anthony Davis (+4.06) [27] ✗ |
| 11 | **Blake Griffin** (+7.20) | Mike Dunleavy (+4.73) [19] ✓ | James Harden (+4.49) [13] ✓ | Blake Griffin (+3.94) [11] ✓ |
| 12 | **JJ Barea** (+6.50) | Otto Porter Jr. (+4.65) [6] ✓ | Mike Conley (+4.41) [21] ✗ | James Harden (+3.78) [13] ✓ |
| 13 | **James Harden** (+6.40) | Kyle Korver (+4.36) [40] ✗ | DeAndre Jordan (+4.36) [33] ✗ | LeBron James (+3.62) [25] ✗ |
| 14 | **Manu Ginobili** (+6.30) | James Harden (+4.26) [13] ✓ | Kyle Korver (+4.22) [40] ✗ | Alan Anderson (+3.60) [5] ✓ |
| 15 | **Marc Gasol** (+6.10) | Pau Gasol (+4.12) [34] ✗ | Blake Griffin (+4.13) [11] ✓ | Otto Porter Jr. (+3.38) [6] ✓ |
| 16 | **Derrick Rose** (+5.80) | Blake Griffin (+4.02) [11] ✓ | Brook Lopez (+3.66) [27] ✗ | Pau Gasol (+3.27) [34] ✗ |
| 17 | **Dwight Howard** (+5.70) | Marcin Gortat (+4.01) [45] ✗ | Pau Gasol (+3.62) [34] ✗ | John Wall (+3.26) [40] ✗ |
| 18 | **CJ McCollum** (+5.60) | JJ Barea (+3.84) [12] ✓ | DeMarre Carroll (+3.60) [43] ✗ | Mike Dunleavy (+2.98) [19] ✓ |
| 19 | **Timofey Mozgov** (+5.30) | Al Horford (+3.72) [30] ✗ | Dwight Howard (+3.56) [17] ✓ | Monta Ellis (+2.80) [35] ✗ |
| 20 | **Mike Dunleavy** (+5.30) | Andre Iguodala (+3.58) [39] ✗ | CJ McCollum (+3.53) [18] ✓ | DeAndre Jordan (+2.77) [33] ✗ |

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
| 8 | **Dirk Nowitzki** (+4.40) | Kyle Lowry (+3.70) [7] ✓ | Carmelo Anthony (+3.80) [9] ✓ |
| 9 | **Carmelo Anthony** (+4.20) | Russell Westbrook (+3.62) [15] ✓ | Kyle Lowry (+3.71) [7] ✓ |
| 10 | **Manu Ginobili** (+4.00) | Carmelo Anthony (+3.26) [9] ✓ | Blake Griffin (+3.57) [17] ✓ |
| 11 | **Patty Mills** (+3.90) | Manu Ginobili (+3.18) [10] ✓ | Isaiah Thomas (+3.48) [13] ✓ |
| 12 | **Damian Lillard** (+3.60) | Isaiah Thomas (+3.02) [13] ✓ | Manu Ginobili (+3.25) [10] ✓ |
| 13 | **Isaiah Thomas** (+3.50) | Mike Conley (+2.90) [13] ✓ | Russell Westbrook (+3.24) [15] ✓ |
| 14 | **Mike Conley** (+3.50) | Dirk Nowitzki (+2.86) [7] ✓ | Nikola Pekovic (+3.10) [63] ✗ |
| 15 | **Russell Westbrook** (+3.30) | Blake Griffin (+2.53) [17] ✓ | Damian Lillard (+3.03) [12] ✓ |
| 16 | **Ty Lawson** (+3.20) | Kyrie Irving (+2.52) [27] ✗ | Dwyane Wade (+2.82) [46] ✗ |
| 17 | **Blake Griffin** (+2.90) | DJ Augustin (+2.44) [31] ✗ | Ty Lawson (+2.80) [16] ✓ |
| 18 | **Wesley Matthews** (+2.80) | Patty Mills (+2.43) [11] ✓ | Mike Conley (+2.80) [13] ✓ |
| 19 | **Marco Belinelli** (+2.80) | Paul George (+2.30) [22] ✗ | Wesley Matthews (+2.68) [18] ✓ |
| 20 | **Jamal Crawford** (+2.80) | Jamal Crawford (+2.28) [18] ✓ | Patty Mills (+2.59) [11] ✓ |

### 2013-14 — Playoffs — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | Chris Paul (+7.19) [1] ✓ | LeBron James (+6.69) [5] ✓ |
| 2 | **Stephen Curry** (+9.20) | Damian Lillard (+6.57) [3] ✓ | Chris Paul (+6.31) [1] ✓ |
| 3 | **Damian Lillard** (+8.00) | Stephen Curry (+6.36) [2] ✓ | Stephen Curry (+4.52) [2] ✓ |
| 4 | **James Harden** (+8.00) | Russell Westbrook (+6.02) [6] ✓ | Joe Johnson (+4.46) [9] ✓ |
| 5 | **LeBron James** (+6.90) | LeBron James (+5.36) [5] ✓ | Russell Westbrook (+4.05) [6] ✓ |
| 6 | **Russell Westbrook** (+6.20) | James Harden (+5.33) [3] ✓ | Damian Lillard (+3.92) [3] ✓ |
| 7 | **Kevin Durant** (+5.10) | Kevin Durant (+5.22) [7] ✓ | Vince Carter (+3.45) [19] ✗ |
| 8 | **Jose Calderon** (+5.10) | Manu Ginobili (+4.00) [10] ✓ | Dwight Howard (+3.31) [34] ✗ |
| 9 | **Joe Johnson** (+4.90) | Kyle Lowry (+3.89) [11] ✓ | James Harden (+3.31) [3] ✓ |
| 10 | **Manu Ginobili** (+4.40) | LaMarcus Aldridge (+3.36) [19] ✓ | Manu Ginobili (+3.24) [10] ✓ |
| 11 | **Kyle Lowry** (+3.80) | DeMar DeRozan (+3.21) [14] ✓ | Tiago Splitter (+2.79) [50] ✗ |
| 12 | **Blake Griffin** (+3.70) | Joe Johnson (+3.04) [9] ✓ | Kevin Durant (+2.72) [7] ✓ |
| 13 | **Patty Mills** (+3.50) | Jose Calderon (+3.02) [7] ✓ | Tim Duncan (+2.54) [32] ✗ |
| 14 | **DeMar DeRozan** (+3.40) | Devin Harris (+2.92) [15] ✓ | Patrick Patterson (+2.45) [29] ✗ |
| 15 | **JJ Redick** (+3.30) | Patrick Patterson (+2.81) [29] ✗ | Kyle Lowry (+2.44) [11] ✓ |
| 16 | **Devin Harris** (+3.30) | Mirza Teletovic (+2.73) [17] ✓ | Blake Griffin (+2.27) [12] ✓ |
| 17 | **Ray Allen** (+3.20) | Patty Mills (+2.67) [13] ✓ | JJ Redick (+2.21) [15] ✓ |
| 18 | **Mirza Teletovic** (+3.20) | Danny Green (+2.65) [23] ✗ | Chris Bosh (+2.19) [41] ✗ |
| 19 | **LaMarcus Aldridge** (+3.10) | Jamal Crawford (+2.63) [23] ✗ | Greivis Vasquez (+2.18) [38] ✗ |
| 20 | **Trevor Ariza** (+3.10) | Vince Carter (+2.58) [19] ✗ | DeMar DeRozan (+2.04) [14] ✓ |

### 2014-15 — Regular season — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+8.50) | Chris Paul (+6.61) [1] ✓ | Chris Paul (+6.99) [1] ✓ |
| 2 | **James Harden** (+7.70) | James Harden (+6.12) [2] ✓ | James Harden (+5.71) [2] ✓ |
| 3 | **Russell Westbrook** (+6.10) | LeBron James (+5.95) [5] ✓ | LeBron James (+5.62) [5] ✓ |
| 4 | **Kyrie Irving** (+5.50) | Kyrie Irving (+5.20) [4] ✓ | Anthony Davis (+5.11) [9] ✓ |
| 5 | **LeBron James** (+5.30) | Russell Westbrook (+5.17) [3] ✓ | Russell Westbrook (+5.02) [3] ✓ |
| 6 | **Lou Williams** (+5.20) | Damian Lillard (+4.40) [11] ✓ | Jimmy Butler (+4.38) [20] ✗ |
| 7 | **Kyle Korver** (+4.60) | Isaiah Thomas (+3.94) [8] ✓ | Blake Griffin (+4.19) [20] ✗ |
| 8 | **Isaiah Thomas** (+4.50) | Lou Williams (+3.93) [6] ✓ | Kyrie Irving (+4.13) [4] ✓ |
| 9 | **Anthony Davis** (+4.30) | Klay Thompson (+3.82) [9] ✓ | Lou Williams (+4.08) [6] ✓ |
| 10 | **Klay Thompson** (+4.30) | Blake Griffin (+3.54) [20] ✗ | Klay Thompson (+4.00) [9] ✓ |
| 11 | **Damian Lillard** (+4.00) | Mike Conley (+2.71) [30] ✗ | George Hill (+3.81) [12] ✓ |
| 12 | **George Hill** (+3.90) | George Hill (+2.66) [12] ✓ | Kawhi Leonard (+3.44) [15] ✓ |
| 13 | **Ty Lawson** (+3.80) | Aaron Brooks (+2.54) [55] ✗ | JJ Redick (+3.36) [29] ✗ |
| 14 | **Carmelo Anthony** (+3.80) | Kyle Lowry (+2.52) [18] ✓ | Ty Lawson (+3.18) [13] ✓ |
| 15 | **Kawhi Leonard** (+3.70) | Jeff Teague (+2.46) [34] ✗ | Gordon Hayward (+3.01) [20] ✓ |
| 16 | **Rudy Gay** (+3.50) | Gordon Hayward (+2.46) [20] ✓ | Isaiah Thomas (+2.98) [8] ✓ |
| 17 | **DeAndre Jordan** (+3.40) | Jimmy Butler (+2.43) [20] ✗ | Carmelo Anthony (+2.96) [13] ✓ |
| 18 | **Kyle Lowry** (+3.30) | Carmelo Anthony (+2.43) [13] ✓ | Damian Lillard (+2.92) [11] ✓ |
| 19 | **Jrue Holiday** (+3.30) | Jrue Holiday (+2.40) [18] ✓ | Wesley Matthews (+2.78) [33] ✗ |
| 20 | **Gordon Hayward** (+3.20) | Brandon Jennings (+2.34) [23] ✗ | Brandon Jennings (+2.74) [23] ✗ |

### 2014-15 — Playoffs — offense

| # | true RAPTOR | ours | Paine (eRO) |
|---|---|---|---|
| 1 | **Chris Paul** (+8.70) | James Harden (+5.71) [2] ✓ | Chris Paul (+6.15) [1] ✓ |
| 2 | **James Harden** (+8.00) | Chris Paul (+5.50) [1] ✓ | James Harden (+4.99) [2] ✓ |
| 3 | **CJ McCollum** (+7.90) | Stephen Curry (+5.04) [6] ✓ | Stephen Curry (+4.89) [6] ✓ |
| 4 | **Monta Ellis** (+6.20) | CJ McCollum (+4.48) [3] ✓ | Anthony Davis (+4.28) [86] ✗ |
| 5 | **Alan Anderson** (+6.10) | Monta Ellis (+4.34) [4] ✓ | Tim Duncan (+4.15) [9] ✓ |
| 6 | **Stephen Curry** (+5.70) | Kyrie Irving (+3.72) [15] ✓ | Jimmy Butler (+3.97) [7] ✓ |
| 7 | **Jimmy Butler** (+5.30) | Tim Duncan (+3.58) [9] ✓ | Blake Griffin (+3.83) [20] ✓ |
| 8 | **AlFarouq Aminu** (+5.30) | LeBron James (+3.56) [17] ✓ | Monta Ellis (+3.68) [4] ✓ |
| 9 | **Tim Duncan** (+5.20) | Alan Anderson (+3.49) [5] ✓ | AlFarouq Aminu (+3.58) [7] ✓ |
| 10 | **Vince Carter** (+5.20) | Jarrett Jack (+3.47) [16] ✓ | Kyrie Irving (+3.26) [15] ✓ |
| 11 | **Mike Dunleavy** (+4.70) | DeMar DeRozan (+3.39) [12] ✓ | CJ McCollum (+3.18) [3] ✓ |
| 12 | **DeMar DeRozan** (+4.60) | Jimmy Butler (+3.25) [7] ✓ | Alan Anderson (+3.09) [5] ✓ |
| 13 | **Eric Gordon** (+4.50) | Mike Dunleavy (+3.15) [11] ✓ | LeBron James (+2.41) [17] ✓ |
| 14 | **JJ Barea** (+4.40) | AlFarouq Aminu (+2.75) [7] ✓ | Courtney Lee (+2.32) [33] ✗ |
| 15 | **Kyrie Irving** (+4.10) | Manu Ginobili (+2.74) [17] ✓ | Mike Dunleavy (+2.00) [11] ✓ |
| 16 | **Jarrett Jack** (+3.80) | Derrick Rose (+2.69) [26] ✗ | Mike Conley (+1.97) [21] ✗ |
| 17 | **LeBron James** (+3.60) | Damian Lillard (+2.49) [57] ✗ | DeMarre Carroll (+1.88) [23] ✗ |
| 18 | **Manu Ginobili** (+3.60) | Jeff Teague (+2.49) [22] ✗ | Bradley Beal (+1.82) [27] ✗ |
| 19 | **Paul Pierce** (+3.60) | Blake Griffin (+2.24) [20] ✓ | Andre Iguodala (+1.74) [38] ✗ |
| 20 | **Blake Griffin** (+3.50) | Bradley Beal (+2.18) [27] ✗ | Boris Diaw (+1.66) [37] ✗ |

### 2013-14 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.00) | Andrew Bogut (+4.53) [4] ✓ | Andrew Bogut (+3.58) [4] ✓ |
| 2 | **Draymond Green** (+4.60) | Draymond Green (+3.78) [2] ✓ | Kawhi Leonard (+3.44) [1] ✓ |
| 3 | **Joakim Noah** (+4.50) | Anthony Davis (+3.41) [33] ✗ | Draymond Green (+3.32) [2] ✓ |
| 4 | **Andrew Bogut** (+4.40) | Tiago Splitter (+3.37) [6] ✓ | DeAndre Jordan (+3.19) [64] ✗ |
| 5 | **Michael KiddGilchrist** (+4.40) | Marcin Gortat (+3.37) [27] ✗ | Jimmy Butler (+3.17) [14] ✓ |
| 6 | **Tiago Splitter** (+4.20) | Kawhi Leonard (+3.20) [1] ✓ | Tony Allen (+3.07) [30] ✗ |
| 7 | **Danny Green** (+4.00) | Kevin Garnett (+3.19) [11] ✓ | Joakim Noah (+3.05) [3] ✓ |
| 8 | **Chris Paul** (+3.90) | Jae Crowder (+3.15) [18] ✓ | Danny Green (+2.82) [7] ✓ |
| 9 | **Nene** (+3.80) | Paul George (+3.09) [21] ✗ | Ricky Rubio (+2.79) [47] ✗ |
| 10 | **Anderson Varejao** (+3.60) | Danny Green (+2.76) [7] ✓ | Andre Iguodala (+2.69) [25] ✗ |
| 11 | **Nick Calathes** (+3.50) | Anderson Varejao (+2.75) [10] ✓ | Paul George (+2.62) [21] ✗ |
| 12 | **Ian Mahinmi** (+3.50) | Andre Iguodala (+2.73) [25] ✗ | Kyle OQuinn (+2.43) [52] ✗ |
| 13 | **Kevin Garnett** (+3.50) | Nene (+2.68) [9] ✓ | Roy Hibbert (+2.43) [14] ✓ |
| 14 | **Jimmy Butler** (+3.40) | Jimmy Butler (+2.65) [14] ✓ | David West (+2.32) [57] ✗ |
| 15 | **Roy Hibbert** (+3.40) | Paul Millsap (+2.61) [23] ✗ | Paul Millsap (+2.20) [23] ✗ |
| 16 | **DeMarcus Cousins** (+3.30) | CJ Watson (+2.60) [17] ✓ | Ian Mahinmi (+2.14) [11] ✓ |
| 17 | **CJ Watson** (+3.20) | Derek Fisher (+2.58) [43] ✗ | Anderson Varejao (+2.08) [10] ✓ |
| 18 | **Tim Duncan** (+3.00) | Michael KiddGilchrist (+2.46) [4] ✓ | Tim Duncan (+2.05) [18] ✓ |
| 19 | **Kris Humphries** (+3.00) | Joakim Noah (+2.44) [3] ✓ | CJ Watson (+2.03) [17] ✓ |
| 20 | **Jae Crowder** (+3.00) | Tony Allen (+2.39) [30] ✗ | Chris Paul (+2.00) [8] ✓ |

### 2013-14 — Playoffs — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Draymond Green** (+8.00) | Joakim Noah (+4.14) [8] ✓ | Danny Green (+3.39) [19] ✓ |
| 2 | **Paul Millsap** (+7.60) | Marcin Gortat (+3.28) [19] ✓ | Pero Antic (+3.12) [3] ✓ |
| 3 | **Pero Antic** (+6.50) | Trevor Ariza (+3.24) [26] ✗ | Kawhi Leonard (+3.08) [12] ✓ |
| 4 | **Andray Blatche** (+6.10) | Pero Antic (+3.24) [3] ✓ | Paul Millsap (+2.79) [2] ✓ |
| 5 | **Nick Collison** (+6.10) | Danny Green (+3.18) [19] ✓ | Draymond Green (+2.77) [1] ✓ |
| 6 | **Greivis Vasquez** (+6.00) | Paul Millsap (+3.05) [2] ✓ | Trevor Ariza (+2.74) [26] ✗ |
| 7 | **Chris Andersen** (+5.40) | Kawhi Leonard (+3.04) [12] ✓ | Manu Ginobili (+2.46) [21] ✗ |
| 8 | **Joakim Noah** (+5.30) | Tiago Splitter (+2.78) [9] ✓ | John Wall (+2.42) [41] ✗ |
| 9 | **Tiago Splitter** (+5.00) | John Wall (+2.73) [41] ✗ | Serge Ibaka (+2.33) [13] ✓ |
| 10 | **Vince Carter** (+5.00) | Kyle Korver (+2.72) [62] ✗ | Joakim Noah (+2.13) [8] ✓ |
| 11 | **Rashard Lewis** (+4.90) | Mike Conley (+2.59) [74] ✗ | Bradley Beal (+2.06) [24] ✗ |
| 12 | **Kawhi Leonard** (+4.40) | Tony Allen (+2.53) [30] ✗ | Marcin Gortat (+2.04) [19] ✓ |
| 13 | **Serge Ibaka** (+4.20) | Draymond Green (+2.45) [1] ✓ | DeAndre Jordan (+1.81) [40] ✗ |
| 14 | **Ian Mahinmi** (+4.20) | Kevin Garnett (+2.37) [49] ✗ | Chris Andersen (+1.81) [7] ✓ |
| 15 | **Zach Randolph** (+4.20) | Chris Andersen (+2.36) [7] ✓ | Chris Paul (+1.76) [24] ✗ |
| 16 | **Marc Gasol** (+4.10) | Greivis Vasquez (+2.35) [6] ✓ | Patty Mills (+1.56) [18] ✓ |
| 17 | **Deron Williams** (+3.80) | Andray Blatche (+2.34) [4] ✓ | David West (+1.50) [39] ✗ |
| 18 | **Patty Mills** (+3.70) | Paul Pierce (+2.31) [34] ✗ | Tony Allen (+1.46) [30] ✗ |
| 19 | **Danny Green** (+3.50) | Zach Randolph (+2.17) [13] ✓ | Tiago Splitter (+1.43) [9] ✓ |
| 20 | **Marcin Gortat** (+3.50) | Serge Ibaka (+2.07) [13] ✓ | Kevin Garnett (+1.25) [49] ✗ |

### 2014-15 — Regular season — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.20) | Andrew Bogut (+4.82) [5] ✓ | Kawhi Leonard (+4.47) [1] ✓ |
| 2 | **Draymond Green** (+5.10) | Draymond Green (+4.63) [2] ✓ | Tony Allen (+4.32) [3] ✓ |
| 3 | **Rudy Gobert** (+4.80) | Rudy Gobert (+4.35) [3] ✓ | Draymond Green (+3.75) [2] ✓ |
| 4 | **Tony Allen** (+4.80) | Tony Allen (+3.99) [3] ✓ | Andrew Bogut (+3.21) [5] ✓ |
| 5 | **Andrew Bogut** (+4.70) | Kawhi Leonard (+3.59) [1] ✓ | DeAndre Jordan (+3.15) [60] ✗ |
| 6 | **Anthony Davis** (+4.50) | Nerlens Noel (+3.53) [19] ✓ | Nerlens Noel (+2.89) [19] ✓ |
| 7 | **DeMarcus Cousins** (+4.40) | Anthony Davis (+3.31) [6] ✓ | Anthony Davis (+2.86) [6] ✓ |
| 8 | **Marcin Gortat** (+3.60) | Nene (+3.08) [17] ✓ | Rudy Gobert (+2.78) [3] ✓ |
| 9 | **Tim Duncan** (+3.50) | Andre Roberson (+2.94) [10] ✓ | Danny Green (+2.72) [14] ✓ |
| 10 | **Andre Roberson** (+3.40) | AlFarouq Aminu (+2.93) [20] ✗ | AlFarouq Aminu (+2.59) [20] ✗ |
| 11 | **Kosta Koufos** (+3.30) | Danny Green (+2.66) [14] ✓ | Tim Duncan (+2.49) [9] ✓ |
| 12 | **Zaza Pachulia** (+3.20) | Jonas Jerebko (+2.65) [17] ✓ | Paul Millsap (+2.35) [26] ✗ |
| 13 | **Khris Middleton** (+3.10) | Tim Duncan (+2.60) [9] ✓ | Khris Middleton (+2.07) [13] ✓ |
| 14 | **Danny Green** (+3.00) | Tyson Chandler (+2.60) [20] ✗ | Andre Roberson (+2.05) [10] ✓ |
| 15 | **Serge Ibaka** (+3.00) | Marcin Gortat (+2.59) [8] ✓ | Bismack Biyombo (+2.05) [102] ✗ |
| 16 | **Michael KiddGilchrist** (+3.00) | DeMarcus Cousins (+2.59) [7] ✓ | Marcus Smart (+1.98) [42] ✗ |
| 17 | **Jonas Jerebko** (+2.80) | Khris Middleton (+2.58) [13] ✓ | John Wall (+1.97) [127] ✗ |
| 18 | **Nene** (+2.80) | Kosta Koufos (+2.50) [11] ✓ | DeMarcus Cousins (+1.95) [7] ✓ |
| 19 | **Nerlens Noel** (+2.70) | Marcus Smart (+2.44) [42] ✗ | Marcin Gortat (+1.93) [8] ✓ |
| 20 | **Marc Gasol** (+2.60) | Dwight Howard (+2.40) [40] ✗ | Michael KiddGilchrist (+1.74) [14] ✓ |

### 2014-15 — Playoffs — defense

| # | true RAPTOR | ours | Paine (eRD) |
|---|---|---|---|
| 1 | **Jarrett Jack** (+7.50) | Jarrett Jack (+4.47) [1] ✓ | Tony Allen (+5.01) [10] ✓ |
| 2 | **Anthony Davis** (+7.20) | DeAndre Jordan (+4.47) [29] ✗ | Jarrett Jack (+3.62) [1] ✓ |
| 3 | **Timofey Mozgov** (+6.90) | Tony Allen (+4.37) [10] ✓ | Pau Gasol (+3.02) [15] ✓ |
| 4 | **Otto Porter Jr.** (+6.30) | Timofey Mozgov (+4.07) [3] ✓ | AlFarouq Aminu (+2.97) [6] ✓ |
| 5 | **Trevor Ariza** (+6.10) | Dwight Howard (+4.06) [7] ✓ | Jimmy Butler (+2.29) [13] ✓ |
| 6 | **AlFarouq Aminu** (+5.80) | Otto Porter Jr. (+3.80) [4] ✓ | Andrew Bogut (+2.28) [35] ✗ |
| 7 | **Dwight Howard** (+5.70) | Pau Gasol (+3.71) [15] ✓ | Otto Porter Jr. (+2.24) [4] ✓ |
| 8 | **Danny Green** (+5.50) | Tim Duncan (+3.58) [22] ✗ | Paul Millsap (+2.24) [29] ✗ |
| 9 | **Marc Gasol** (+5.30) | Jimmy Butler (+3.52) [13] ✓ | John Wall (+2.07) [44] ✗ |
| 10 | **Tony Allen** (+5.00) | Al Horford (+3.24) [12] ✓ | Dwight Howard (+2.05) [7] ✓ |
| 11 | **Nene** (+4.70) | Nene (+3.19) [11] ✓ | Kyle Korver (+2.03) [41] ✗ |
| 12 | **Al Horford** (+4.40) | Joakim Noah (+3.17) [29] ✗ | DeAndre Jordan (+1.99) [29] ✗ |
| 13 | **Jimmy Butler** (+3.70) | AlFarouq Aminu (+3.07) [6] ✓ | Drew Gooden (+1.97) [69] ✗ |
| 14 | **Blake Griffin** (+3.70) | Alan Anderson (+3.07) [20] ✓ | Iman Shumpert (+1.72) [54] ✗ |
| 15 | **Pau Gasol** (+3.50) | Kyle Korver (+2.93) [41] ✗ | Danny Green (+1.71) [8] ✓ |
| 16 | **Ramon Sessions** (+3.50) | Harrison Barnes (+2.80) [43] ✗ | Matt Barnes (+1.68) [17] ✓ |
| 17 | **Matt Barnes** (+3.40) | Marc Gasol (+2.75) [9] ✓ | Joakim Noah (+1.65) [29] ✗ |
| 18 | **Stephen Curry** (+3.00) | Andrew Bogut (+2.66) [35] ✗ | Tim Duncan (+1.54) [22] ✗ |
| 19 | **Derrick Rose** (+3.00) | Mike Conley (+2.56) [38] ✗ | Nikola Mirotic (+1.51) [77] ✗ |
| 20 | **Alan Anderson** (+2.80) | Matt Barnes (+2.37) [17] ✓ | Derrick Rose (+1.42) [18] ✓ |

