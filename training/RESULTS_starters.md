# Starter-only offense and defense models

Offense (`rap_o`) and defense (`rap_d`) models trained on the combined
feature set, restricted to rotation minutes (**MPG ≥ 20**) and,
in the later variants, to labels outside the near-average band
(**|RAPTOR| > 0.5**). A stricter **MPG ≥ 28** tier is
included to show the trend. Filters apply to fit and validation rows only.

> **The evaluation pool is not filtered, on purpose.** Many genuine top-20
> players are not starters — in 2013-14 twelve of the true top-20 defenders
> play under 28 MPG (eight under 20), and the lowest is 16.1. Filtering the
> pool would
> delete the players a leaderboard exists to find, so the models are trained
> on starters and asked to rank everyone. The mismatch is the point of the
> experiment.

## Training rows after filtering

| target | variant | fit rows | val rows | boosting rounds |
|---|---|---|---|---|
| offense | baseline (all rows) | 14,235 | 1,241 | 559 |
| offense | MPG>=20 | 8,629 | 1,032 | 421 |
| offense | MPG>=20 + drop ~0 | 6,841 | 836 | 262 |
| offense | MPG>=28 + drop ~0 | 3,792 | 505 | 252 |
| defense | baseline (all rows) | 14,235 | 1,241 | 282 |
| defense | MPG>=20 | 8,629 | 1,032 | 287 |
| defense | MPG>=20 + drop ~0 | 6,848 | 816 | 201 |
| defense | MPG>=28 + drop ~0 | 3,787 | 477 | 363 |

## Accuracy over all held-out rows

**offense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| baseline (all rows) | 1.022 | 0.780 | +0.821 | +0.923 | +0.908 |
| MPG>=20 | 1.039 | 0.783 | +0.815 | +0.919 | +0.907 |
| MPG>=20 + drop ~0 | 1.082 | 0.820 | +0.800 | +0.910 | +0.897 |
| MPG>=28 + drop ~0 | 1.169 | 0.878 | +0.766 | +0.886 | +0.868 |
| Paine | 1.308 | 0.959 | +0.707 | +0.847 | +0.825 |

**defense**

| system | RMSE | MAE | R² | Pearson r | Spearman ρ |
|---|---|---|---|---|---|
| baseline (all rows) | 1.408 | 1.040 | +0.635 | +0.817 | +0.818 |
| MPG>=20 | 1.429 | 1.047 | +0.624 | +0.808 | +0.805 |
| MPG>=20 + drop ~0 | 1.445 | 1.080 | +0.616 | +0.800 | +0.800 |
| MPG>=28 + drop ~0 | 1.561 | 1.164 | +0.551 | +0.768 | +0.773 |
| Paine | 1.641 | 1.194 | +0.504 | +0.726 | +0.728 |

## Top-20 recovery (hits@20)

**offense**

| season | split | pool | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine | ρ baseline (all rows) | ρ MPG>=20 | ρ MPG>=20 + drop ~0 | ρ MPG>=28 + drop ~0 | ρ Paine |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 246 | 17/20 | 18/20 | 17/20 | 17/20 | 17/20 | +0.923 | +0.925 | +0.917 | +0.897 | +0.867 |
| 2013-14 | Playoffs | 99 | 16/20 | 17/20 | 16/20 | 16/20 | 13/20 | +0.893 | +0.883 | +0.871 | +0.844 | +0.838 |
| 2014-15 | Regular season | 246 | 14/20 | 15/20 | 15/20 | 13/20 | 15/20 | +0.925 | +0.921 | +0.910 | +0.882 | +0.858 |
| 2014-15 | Playoffs | 98 | 16/20 | 15/20 | 15/20 | 14/20 | 13/20 | +0.898 | +0.903 | +0.894 | +0.856 | +0.787 |
| **all** | | | **63/80** | **65/80** | **63/80** | **60/80** | **58/80** |  |  |  |  |  |

**defense**

| season | split | pool | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine | ρ baseline (all rows) | ρ MPG>=20 | ρ MPG>=20 + drop ~0 | ρ MPG>=28 + drop ~0 | ρ Paine |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | 246 | 13/20 | 12/20 | 12/20 | 9/20 | 12/20 | +0.871 | +0.862 | +0.852 | +0.829 | +0.795 |
| 2013-14 | Playoffs | 99 | 13/20 | 13/20 | 11/20 | 13/20 | 11/20 | +0.690 | +0.688 | +0.704 | +0.677 | +0.667 |
| 2014-15 | Regular season | 246 | 16/20 | 14/20 | 13/20 | 11/20 | 14/20 | +0.871 | +0.856 | +0.846 | +0.814 | +0.801 |
| 2014-15 | Playoffs | 98 | 13/20 | 12/20 | 12/20 | 13/20 | 10/20 | +0.780 | +0.755 | +0.761 | +0.750 | +0.633 |
| **all** | | | **55/80** | **51/80** | **48/80** | **46/80** | **47/80** |  |  |  |  |  |

## Conclusions

**Offense.** Every filter costs regression accuracy, monotonically: R² +0.821 on all rows → +0.815 → +0.800 → +0.766. Top-20 recovery goes 63/80 → 65/80 → 63/80 → 60/80.

**Defense.** Every filter costs regression accuracy, monotonically: R² +0.635 on all rows → +0.624 → +0.616 → +0.551. Top-20 recovery goes 55/80 → 51/80 → 48/80 → 46/80.

The pattern matches every other filtering experiment in this repo: the
models want more data, not cleaner data. Two specific reasons here.

First, **the evaluation pool is not the training pool**. Roughly half the
held-out field plays under 20 MPG, and a large share of the true top-20
defenders are among them, so a starters-only model is extrapolating on
exactly the rows where leaderboard mistakes are made.

Second, **dropping the near-average band removes the densest part of the
label distribution** — 13-20% of rows sit inside ±0.5 — and with it the
model's calibration through the middle. Ranking a top 20 still depends on
placing the players just below the cutoff correctly, so a model that has
never seen an average player is worse at deciding who is merely good.

The one exception is offense at MPG ≥ 20 with no label filter: 65/80 vs
63/80. That is a 2-slot move at a cutoff where several players sit within
hundredths of each other, and the rank correlations are flat to slightly
down, so it is noise rather than a real gain. Adding the near-zero filter
on top takes it back to 63/80 while costing 0.021 R².

All variants still beat Paine's Estimated RAPTOR on R² and ρ; only the
harshest (MPG ≥ 28 + drop ~0) falls behind him on defensive hits@20 (46/80 vs 47/80).

## Leaderboards

`[n]` is the player's true rank; ✓ means they are genuinely top 20.

### 2013-14 — Regular season — offense

| # | true RAPTOR | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine |
|---|---|---|---|---|---|---|
| 1 | **Kevin Durant** (+7.60) | Kevin Durant (+6.35) [1] ✓ | LeBron James (+6.36) [4] ✓ | LeBron James (+6.20) [4] ✓ | LeBron James (+6.46) [4] ✓ | Kevin Durant (+7.41) [1] ✓ |
| 2 | **Chris Paul** (+7.10) | LeBron James (+6.18) [4] ✓ | Kevin Durant (+6.30) [1] ✓ | Kevin Durant (+6.03) [1] ✓ | Kevin Durant (+6.16) [1] ✓ | LeBron James (+6.80) [4] ✓ |
| 3 | **James Harden** (+6.30) | Chris Paul (+5.94) [2] ✓ | Chris Paul (+5.91) [2] ✓ | Chris Paul (+5.69) [2] ✓ | Chris Paul (+5.65) [2] ✓ | Chris Paul (+6.79) [2] ✓ |
| 4 | **LeBron James** (+5.80) | James Harden (+5.31) [3] ✓ | James Harden (+4.99) [3] ✓ | James Harden (+5.12) [3] ✓ | James Harden (+5.18) [3] ✓ | James Harden (+5.29) [3] ✓ |
| 5 | **Kevin Love** (+5.70) | Damian Lillard (+4.21) [12] ✓ | Goran Dragic (+4.20) [6] ✓ | Goran Dragic (+4.19) [6] ✓ | Kevin Love (+4.62) [5] ✓ | Goran Dragic (+4.87) [6] ✓ |
| 6 | **Goran Dragic** (+4.80) | Goran Dragic (+4.21) [6] ✓ | Damian Lillard (+4.08) [12] ✓ | Kevin Love (+4.09) [5] ✓ | Goran Dragic (+4.14) [6] ✓ | Kevin Love (+4.76) [5] ✓ |
| 7 | **Kyle Lowry** (+4.40) | Kevin Love (+4.04) [5] ✓ | Kevin Love (+3.96) [5] ✓ | Russell Westbrook (+3.90) [15] ✓ | Damian Lillard (+4.12) [12] ✓ | Dirk Nowitzki (+4.33) [7] ✓ |
| 8 | **Dirk Nowitzki** (+4.40) | Kyle Lowry (+3.70) [7] ✓ | Kyle Lowry (+3.85) [7] ✓ | Damian Lillard (+3.74) [12] ✓ | Russell Westbrook (+4.11) [15] ✓ | Brandan Wright (+4.19) [40] ✗ |
| 9 | **Carmelo Anthony** (+4.20) | Russell Westbrook (+3.62) [15] ✓ | Carmelo Anthony (+3.54) [9] ✓ | Carmelo Anthony (+3.69) [9] ✓ | Kyle Lowry (+3.69) [7] ✓ | Carmelo Anthony (+3.80) [9] ✓ |
| 10 | **Manu Ginobili** (+4.00) | Carmelo Anthony (+3.26) [9] ✓ | Russell Westbrook (+3.30) [15] ✓ | Kyle Lowry (+3.64) [7] ✓ | Carmelo Anthony (+3.49) [9] ✓ | Kyle Lowry (+3.71) [7] ✓ |
| 11 | **Patty Mills** (+3.90) | Manu Ginobili (+3.18) [10] ✓ | Manu Ginobili (+3.27) [10] ✓ | Manu Ginobili (+3.37) [10] ✓ | Mike Conley (+3.27) [13] ✓ | Blake Griffin (+3.57) [17] ✓ |
| 12 | **Damian Lillard** (+3.60) | Isaiah Thomas (+3.02) [13] ✓ | Mike Conley (+3.10) [13] ✓ | Isaiah Thomas (+3.13) [13] ✓ | Jamal Crawford (+3.25) [18] ✓ | Isaiah Thomas (+3.48) [13] ✓ |
| 13 | **Isaiah Thomas** (+3.50) | Mike Conley (+2.90) [13] ✓ | Dirk Nowitzki (+2.92) [7] ✓ | Blake Griffin (+3.00) [17] ✓ | Manu Ginobili (+3.23) [10] ✓ | Manu Ginobili (+3.25) [10] ✓ |
| 14 | **Mike Conley** (+3.50) | Dirk Nowitzki (+2.86) [7] ✓ | Isaiah Thomas (+2.84) [13] ✓ | Dirk Nowitzki (+2.98) [7] ✓ | Isaiah Thomas (+3.08) [13] ✓ | Russell Westbrook (+3.24) [15] ✓ |
| 15 | **Russell Westbrook** (+3.30) | Blake Griffin (+2.53) [17] ✓ | Blake Griffin (+2.82) [17] ✓ | Mike Conley (+2.94) [13] ✓ | Kyrie Irving (+3.00) [27] ✗ | Nikola Pekovic (+3.10) [64] ✗ |
| 16 | **Ty Lawson** (+3.20) | Kyrie Irving (+2.52) [27] ✗ | Kyrie Irving (+2.72) [27] ✗ | Jamal Crawford (+2.62) [18] ✓ | Blake Griffin (+2.95) [17] ✓ | Damian Lillard (+3.03) [12] ✓ |
| 17 | **Blake Griffin** (+2.90) | DJ Augustin (+2.44) [31] ✗ | Paul George (+2.67) [22] ✗ | Patty Mills (+2.56) [11] ✓ | Paul George (+2.77) [22] ✗ | Dwyane Wade (+2.82) [47] ✗ |
| 18 | **Wesley Matthews** (+2.80) | Patty Mills (+2.43) [11] ✓ | Jamal Crawford (+2.34) [18] ✓ | Kyrie Irving (+2.52) [27] ✗ | Dirk Nowitzki (+2.70) [7] ✓ | Ty Lawson (+2.80) [16] ✓ |
| 19 | **Marco Belinelli** (+2.80) | Paul George (+2.30) [22] ✗ | Patty Mills (+2.29) [11] ✓ | Paul George (+2.43) [22] ✗ | Ty Lawson (+2.57) [16] ✓ | Mike Conley (+2.80) [13] ✓ |
| 20 | **Jamal Crawford** (+2.80) | Jamal Crawford (+2.28) [18] ✓ | Ty Lawson (+2.13) [16] ✓ | John Wall (+2.28) [36] ✗ | DJ Augustin (+2.39) [31] ✗ | Wesley Matthews (+2.68) [18] ✓ |

### 2013-14 — Playoffs — offense

| # | true RAPTOR | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine |
|---|---|---|---|---|---|---|
| 1 | **Chris Paul** (+10.60) | Chris Paul (+7.19) [1] ✓ | Chris Paul (+7.43) [1] ✓ | Damian Lillard (+6.99) [3] ✓ | Chris Paul (+6.75) [1] ✓ | LeBron James (+6.69) [5] ✓ |
| 2 | **Stephen Curry** (+9.20) | Damian Lillard (+6.57) [3] ✓ | Damian Lillard (+6.77) [3] ✓ | Chris Paul (+6.51) [1] ✓ | Damian Lillard (+6.60) [3] ✓ | Chris Paul (+6.31) [1] ✓ |
| 3 | **Damian Lillard** (+8.00) | Stephen Curry (+6.36) [2] ✓ | Stephen Curry (+6.49) [2] ✓ | Russell Westbrook (+6.19) [6] ✓ | Russell Westbrook (+6.46) [6] ✓ | Stephen Curry (+4.52) [2] ✓ |
| 4 | **James Harden** (+8.00) | Russell Westbrook (+6.02) [6] ✓ | Russell Westbrook (+6.06) [6] ✓ | Stephen Curry (+5.99) [2] ✓ | Stephen Curry (+5.89) [2] ✓ | Joe Johnson (+4.46) [9] ✓ |
| 5 | **LeBron James** (+6.90) | LeBron James (+5.36) [5] ✓ | Kevin Durant (+5.57) [7] ✓ | LeBron James (+5.58) [5] ✓ | Kevin Durant (+5.54) [7] ✓ | Russell Westbrook (+4.05) [6] ✓ |
| 6 | **Russell Westbrook** (+6.20) | James Harden (+5.33) [3] ✓ | LeBron James (+5.33) [5] ✓ | Kevin Durant (+5.51) [7] ✓ | LeBron James (+5.40) [5] ✓ | Damian Lillard (+3.92) [3] ✓ |
| 7 | **Kevin Durant** (+5.10) | Kevin Durant (+5.22) [7] ✓ | James Harden (+4.93) [3] ✓ | James Harden (+5.39) [3] ✓ | James Harden (+5.18) [3] ✓ | Vince Carter (+3.45) [19] ✗ |
| 8 | **Jose Calderon** (+5.10) | Manu Ginobili (+4.00) [10] ✓ | Manu Ginobili (+4.24) [10] ✓ | Manu Ginobili (+4.23) [10] ✓ | Manu Ginobili (+4.22) [10] ✓ | Dwight Howard (+3.31) [34] ✗ |
| 9 | **Joe Johnson** (+4.90) | Kyle Lowry (+3.89) [11] ✓ | DeMar DeRozan (+3.60) [14] ✓ | DeMar DeRozan (+3.75) [14] ✓ | Kyle Lowry (+3.61) [11] ✓ | James Harden (+3.31) [3] ✓ |
| 10 | **Manu Ginobili** (+4.40) | LaMarcus Aldridge (+3.36) [19] ✓ | Kyle Lowry (+3.55) [11] ✓ | Kyle Lowry (+3.66) [11] ✓ | DeMar DeRozan (+3.61) [14] ✓ | Manu Ginobili (+3.24) [10] ✓ |
| 11 | **Kyle Lowry** (+3.80) | DeMar DeRozan (+3.21) [14] ✓ | LaMarcus Aldridge (+3.31) [19] ✓ | Jamal Crawford (+3.15) [23] ✗ | Patrick Patterson (+3.39) [29] ✗ | Tiago Splitter (+2.79) [50] ✗ |
| 12 | **Blake Griffin** (+3.70) | Joe Johnson (+3.04) [9] ✓ | Joe Johnson (+2.88) [9] ✓ | LaMarcus Aldridge (+3.01) [19] ✓ | LaMarcus Aldridge (+3.34) [19] ✓ | Kevin Durant (+2.72) [7] ✓ |
| 13 | **Patty Mills** (+3.50) | Jose Calderon (+3.02) [7] ✓ | Devin Harris (+2.79) [15] ✓ | Joe Johnson (+2.74) [9] ✓ | Joe Johnson (+3.24) [9] ✓ | Tim Duncan (+2.54) [32] ✗ |
| 14 | **DeMar DeRozan** (+3.40) | Devin Harris (+2.92) [15] ✓ | Jose Calderon (+2.74) [7] ✓ | Devin Harris (+2.69) [15] ✓ | Devin Harris (+3.16) [15] ✓ | Patrick Patterson (+2.45) [29] ✗ |
| 15 | **JJ Redick** (+3.30) | Patrick Patterson (+2.81) [29] ✗ | Patrick Patterson (+2.73) [29] ✗ | Jose Calderon (+2.69) [7] ✓ | Patty Mills (+3.14) [13] ✓ | Kyle Lowry (+2.44) [11] ✓ |
| 16 | **Devin Harris** (+3.30) | Mirza Teletovic (+2.73) [17] ✓ | Patty Mills (+2.58) [13] ✓ | Patty Mills (+2.66) [13] ✓ | Jamal Crawford (+2.96) [23] ✗ | Blake Griffin (+2.27) [12] ✓ |
| 17 | **Ray Allen** (+3.20) | Patty Mills (+2.67) [13] ✓ | Mirza Teletovic (+2.54) [17] ✓ | Bradley Beal (+2.47) [22] ✗ | JJ Redick (+2.77) [15] ✓ | JJ Redick (+2.21) [15] ✓ |
| 18 | **Mirza Teletovic** (+3.20) | Danny Green (+2.65) [23] ✗ | Jamal Crawford (+2.52) [23] ✗ | JJ Redick (+2.44) [15] ✓ | Bradley Beal (+2.64) [22] ✗ | Chris Bosh (+2.19) [41] ✗ |
| 19 | **LaMarcus Aldridge** (+3.10) | Jamal Crawford (+2.63) [23] ✗ | Blake Griffin (+2.36) [12] ✓ | Patrick Patterson (+2.44) [29] ✗ | Blake Griffin (+2.51) [12] ✓ | Greivis Vasquez (+2.18) [38] ✗ |
| 20 | **Trevor Ariza** (+3.10) | Vince Carter (+2.58) [19] ✗ | Deron Williams (+2.31) [25] ✗ | Vince Carter (+2.31) [19] ✗ | Deron Williams (+2.35) [25] ✗ | DeMar DeRozan (+2.04) [14] ✓ |

### 2014-15 — Regular season — offense

| # | true RAPTOR | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine |
|---|---|---|---|---|---|---|
| 1 | **Chris Paul** (+8.50) | Chris Paul (+6.61) [1] ✓ | Chris Paul (+7.03) [1] ✓ | Chris Paul (+6.75) [1] ✓ | Chris Paul (+6.76) [1] ✓ | Chris Paul (+6.99) [1] ✓ |
| 2 | **James Harden** (+7.70) | James Harden (+6.12) [2] ✓ | James Harden (+6.24) [2] ✓ | James Harden (+6.39) [2] ✓ | James Harden (+6.24) [2] ✓ | James Harden (+5.71) [2] ✓ |
| 3 | **Russell Westbrook** (+6.10) | LeBron James (+5.95) [5] ✓ | LeBron James (+5.98) [5] ✓ | LeBron James (+6.01) [5] ✓ | LeBron James (+6.04) [5] ✓ | LeBron James (+5.62) [5] ✓ |
| 4 | **Kyrie Irving** (+5.50) | Kyrie Irving (+5.20) [4] ✓ | Russell Westbrook (+5.07) [3] ✓ | Russell Westbrook (+5.39) [3] ✓ | Russell Westbrook (+5.61) [3] ✓ | Anthony Davis (+5.11) [9] ✓ |
| 5 | **LeBron James** (+5.30) | Russell Westbrook (+5.17) [3] ✓ | Kyrie Irving (+4.95) [4] ✓ | Kyrie Irving (+5.08) [4] ✓ | Kyrie Irving (+5.30) [4] ✓ | Russell Westbrook (+5.02) [3] ✓ |
| 6 | **Lou Williams** (+5.20) | Damian Lillard (+4.40) [11] ✓ | Damian Lillard (+4.53) [11] ✓ | Damian Lillard (+4.43) [11] ✓ | Damian Lillard (+5.04) [11] ✓ | Jimmy Butler (+4.38) [20] ✗ |
| 7 | **Kyle Korver** (+4.60) | Isaiah Thomas (+3.94) [8] ✓ | Lou Williams (+4.11) [6] ✓ | Lou Williams (+4.34) [6] ✓ | Blake Griffin (+4.48) [20] ✗ | Blake Griffin (+4.19) [20] ✗ |
| 8 | **Isaiah Thomas** (+4.50) | Lou Williams (+3.93) [6] ✓ | Isaiah Thomas (+3.92) [8] ✓ | Blake Griffin (+4.01) [20] ✗ | Isaiah Thomas (+4.36) [8] ✓ | Kyrie Irving (+4.13) [4] ✓ |
| 9 | **Anthony Davis** (+4.30) | Klay Thompson (+3.82) [9] ✓ | Klay Thompson (+3.66) [9] ✓ | Isaiah Thomas (+3.97) [8] ✓ | Lou Williams (+4.16) [6] ✓ | Lou Williams (+4.08) [6] ✓ |
| 10 | **Klay Thompson** (+4.30) | Blake Griffin (+3.54) [20] ✗ | Blake Griffin (+3.60) [20] ✗ | Klay Thompson (+3.67) [9] ✓ | Klay Thompson (+3.85) [9] ✓ | Klay Thompson (+4.00) [9] ✓ |
| 11 | **Damian Lillard** (+4.00) | Mike Conley (+2.71) [30] ✗ | George Hill (+3.36) [12] ✓ | Carmelo Anthony (+3.43) [13] ✓ | Carmelo Anthony (+3.37) [13] ✓ | George Hill (+3.81) [12] ✓ |
| 12 | **George Hill** (+3.90) | George Hill (+2.66) [12] ✓ | Carmelo Anthony (+3.35) [13] ✓ | Gordon Hayward (+3.31) [20] ✓ | JJ Redick (+3.36) [29] ✗ | Kawhi Leonard (+3.44) [15] ✓ |
| 13 | **Ty Lawson** (+3.80) | Aaron Brooks (+2.54) [55] ✗ | Gordon Hayward (+2.95) [20] ✓ | George Hill (+3.15) [12] ✓ | George Hill (+3.24) [12] ✓ | JJ Redick (+3.36) [29] ✗ |
| 14 | **Carmelo Anthony** (+3.80) | Kyle Lowry (+2.52) [18] ✓ | Dwyane Wade (+2.84) [40] ✗ | JJ Redick (+3.11) [29] ✗ | Dwyane Wade (+2.96) [40] ✗ | Ty Lawson (+3.18) [13] ✓ |
| 15 | **Kawhi Leonard** (+3.70) | Jeff Teague (+2.46) [34] ✗ | Kyle Korver (+2.66) [7] ✓ | Dwyane Wade (+3.01) [40] ✗ | Brandon Jennings (+2.84) [23] ✗ | Gordon Hayward (+3.01) [20] ✓ |
| 16 | **Rudy Gay** (+3.50) | Gordon Hayward (+2.46) [20] ✓ | Kyle Lowry (+2.65) [18] ✓ | Kyle Korver (+2.97) [7] ✓ | Kobe Bryant (+2.79) [61] ✗ | Isaiah Thomas (+2.98) [8] ✓ |
| 17 | **DeAndre Jordan** (+3.40) | Jimmy Butler (+2.43) [20] ✗ | JJ Redick (+2.62) [29] ✗ | Ty Lawson (+2.54) [13] ✓ | Mike Conley (+2.77) [30] ✗ | Carmelo Anthony (+2.96) [13] ✓ |
| 18 | **Kyle Lowry** (+3.30) | Carmelo Anthony (+2.43) [13] ✓ | Jimmy Butler (+2.61) [20] ✗ | Mike Conley (+2.52) [30] ✗ | Ty Lawson (+2.70) [13] ✓ | Damian Lillard (+2.92) [11] ✓ |
| 19 | **Jrue Holiday** (+3.30) | Jrue Holiday (+2.40) [18] ✓ | Ty Lawson (+2.57) [13] ✓ | Jeff Teague (+2.49) [34] ✗ | Kyle Lowry (+2.58) [18] ✓ | Wesley Matthews (+2.78) [33] ✗ |
| 20 | **Gordon Hayward** (+3.20) | Brandon Jennings (+2.34) [23] ✗ | Mike Conley (+2.56) [30] ✗ | Kyle Lowry (+2.46) [18] ✓ | Jeff Teague (+2.49) [34] ✗ | Brandon Jennings (+2.74) [23] ✗ |

### 2014-15 — Playoffs — offense

| # | true RAPTOR | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine |
|---|---|---|---|---|---|---|
| 1 | **Chris Paul** (+8.70) | James Harden (+5.71) [2] ✓ | James Harden (+5.91) [2] ✓ | James Harden (+5.67) [2] ✓ | James Harden (+6.35) [2] ✓ | Chris Paul (+6.15) [1] ✓ |
| 2 | **James Harden** (+8.00) | Chris Paul (+5.50) [1] ✓ | Stephen Curry (+5.74) [6] ✓ | Stephen Curry (+5.37) [6] ✓ | Chris Paul (+5.65) [1] ✓ | James Harden (+4.99) [2] ✓ |
| 3 | **CJ McCollum** (+7.90) | Stephen Curry (+5.04) [6] ✓ | Chris Paul (+5.41) [1] ✓ | Chris Paul (+5.28) [1] ✓ | Stephen Curry (+5.53) [6] ✓ | Stephen Curry (+4.89) [6] ✓ |
| 4 | **Monta Ellis** (+6.20) | CJ McCollum (+4.48) [3] ✓ | CJ McCollum (+4.77) [3] ✓ | CJ McCollum (+4.95) [3] ✓ | CJ McCollum (+4.52) [3] ✓ | Anthony Davis (+4.28) [86] ✗ |
| 5 | **Alan Anderson** (+6.10) | Monta Ellis (+4.34) [4] ✓ | Jarrett Jack (+4.54) [16] ✓ | LeBron James (+4.35) [17] ✓ | Jarrett Jack (+4.38) [16] ✓ | Tim Duncan (+4.15) [9] ✓ |
| 6 | **Stephen Curry** (+5.70) | Kyrie Irving (+3.72) [15] ✓ | LeBron James (+4.05) [17] ✓ | Jarrett Jack (+4.31) [16] ✓ | Monta Ellis (+4.19) [4] ✓ | Jimmy Butler (+3.97) [7] ✓ |
| 7 | **Jimmy Butler** (+5.30) | Tim Duncan (+3.58) [9] ✓ | Kyrie Irving (+3.67) [15] ✓ | Monta Ellis (+3.57) [4] ✓ | LeBron James (+3.70) [17] ✓ | Blake Griffin (+3.83) [20] ✓ |
| 8 | **AlFarouq Aminu** (+5.30) | LeBron James (+3.56) [17] ✓ | Jimmy Butler (+3.64) [7] ✓ | Kyrie Irving (+3.53) [15] ✓ | Jimmy Butler (+3.50) [7] ✓ | Monta Ellis (+3.68) [4] ✓ |
| 9 | **Tim Duncan** (+5.20) | Alan Anderson (+3.49) [5] ✓ | Monta Ellis (+3.59) [4] ✓ | Jimmy Butler (+3.44) [7] ✓ | Alan Anderson (+3.14) [5] ✓ | AlFarouq Aminu (+3.58) [7] ✓ |
| 10 | **Vince Carter** (+5.20) | Jarrett Jack (+3.47) [16] ✓ | Derrick Rose (+3.29) [26] ✗ | Mike Dunleavy (+3.25) [11] ✓ | Kyrie Irving (+3.04) [15] ✓ | Kyrie Irving (+3.26) [15] ✓ |
| 11 | **Mike Dunleavy** (+4.70) | DeMar DeRozan (+3.39) [12] ✓ | Alan Anderson (+2.98) [5] ✓ | Derrick Rose (+3.16) [26] ✗ | Mike Dunleavy (+2.84) [11] ✓ | CJ McCollum (+3.18) [3] ✓ |
| 12 | **DeMar DeRozan** (+4.60) | Jimmy Butler (+3.25) [7] ✓ | Mike Dunleavy (+2.96) [11] ✓ | Alan Anderson (+3.04) [5] ✓ | Derrick Rose (+2.78) [26] ✗ | Alan Anderson (+3.09) [5] ✓ |
| 13 | **Eric Gordon** (+4.50) | Mike Dunleavy (+3.15) [11] ✓ | Damian Lillard (+2.77) [57] ✗ | Damian Lillard (+2.79) [57] ✗ | Damian Lillard (+2.30) [57] ✗ | LeBron James (+2.41) [17] ✓ |
| 14 | **JJ Barea** (+4.40) | AlFarouq Aminu (+2.75) [7] ✓ | AlFarouq Aminu (+2.67) [7] ✓ | AlFarouq Aminu (+2.54) [7] ✓ | Blake Griffin (+2.27) [20] ✓ | Courtney Lee (+2.32) [33] ✗ |
| 15 | **Kyrie Irving** (+4.10) | Manu Ginobili (+2.74) [17] ✓ | Tim Duncan (+2.65) [9] ✓ | Jeff Teague (+2.51) [22] ✗ | Jeff Teague (+2.24) [22] ✗ | Mike Dunleavy (+2.00) [11] ✓ |
| 16 | **Jarrett Jack** (+3.80) | Derrick Rose (+2.69) [26] ✗ | Jeff Teague (+2.27) [22] ✗ | DeMar DeRozan (+2.37) [12] ✓ | AlFarouq Aminu (+2.20) [7] ✓ | Mike Conley (+1.97) [21] ✗ |
| 17 | **LeBron James** (+3.60) | Damian Lillard (+2.49) [57] ✗ | Blake Griffin (+2.12) [20] ✓ | Tim Duncan (+2.17) [9] ✓ | Mike Conley (+2.20) [21] ✗ | DeMarre Carroll (+1.88) [23] ✗ |
| 18 | **Manu Ginobili** (+3.60) | Jeff Teague (+2.49) [22] ✗ | Bradley Beal (+2.11) [27] ✗ | Blake Griffin (+2.09) [20] ✓ | DeMarre Carroll (+2.00) [23] ✗ | Bradley Beal (+1.82) [27] ✗ |
| 19 | **Paul Pierce** (+3.60) | Blake Griffin (+2.24) [20] ✓ | Paul Pierce (+2.09) [17] ✓ | Mike Conley (+1.99) [21] ✗ | DeMar DeRozan (+1.96) [12] ✓ | Andre Iguodala (+1.74) [38] ✗ |
| 20 | **Blake Griffin** (+3.50) | Bradley Beal (+2.18) [27] ✗ | DeMarre Carroll (+1.99) [23] ✗ | Bradley Beal (+1.97) [27] ✗ | John Wall (+1.81) [42] ✗ | Boris Diaw (+1.66) [37] ✗ |

### 2013-14 — Regular season — defense

| # | true RAPTOR | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine |
|---|---|---|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.00) | Andrew Bogut (+4.53) [4] ✓ | Andrew Bogut (+4.25) [4] ✓ | Andrew Bogut (+4.21) [4] ✓ | Andrew Bogut (+4.63) [4] ✓ | Andrew Bogut (+3.58) [4] ✓ |
| 2 | **Draymond Green** (+4.60) | Draymond Green (+3.78) [2] ✓ | Draymond Green (+4.13) [2] ✓ | Draymond Green (+4.20) [2] ✓ | Anthony Davis (+4.09) [33] ✗ | Kawhi Leonard (+3.44) [1] ✓ |
| 3 | **Joakim Noah** (+4.50) | Anthony Davis (+3.41) [33] ✗ | Kevin Garnett (+3.64) [11] ✓ | Jae Crowder (+3.61) [18] ✓ | Draymond Green (+4.02) [2] ✓ | Draymond Green (+3.32) [2] ✓ |
| 4 | **Andrew Bogut** (+4.40) | Tiago Splitter (+3.37) [6] ✓ | Kawhi Leonard (+3.48) [1] ✓ | Kevin Garnett (+3.37) [11] ✓ | Kevin Garnett (+3.88) [11] ✓ | DeAndre Jordan (+3.19) [64] ✗ |
| 5 | **Michael KiddGilchrist** (+4.40) | Marcin Gortat (+3.37) [27] ✗ | Jae Crowder (+3.19) [18] ✓ | Anthony Davis (+3.35) [33] ✗ | Anderson Varejao (+3.86) [10] ✓ | Jimmy Butler (+3.17) [14] ✓ |
| 6 | **Tiago Splitter** (+4.20) | Kawhi Leonard (+3.20) [1] ✓ | Tiago Splitter (+2.99) [6] ✓ | Anderson Varejao (+3.34) [10] ✓ | Jae Crowder (+3.61) [18] ✓ | Tony Allen (+3.07) [30] ✗ |
| 7 | **Danny Green** (+4.00) | Kevin Garnett (+3.19) [11] ✓ | Nene (+2.90) [9] ✓ | Iman Shumpert (+3.20) [72] ✗ | Nene (+3.38) [9] ✓ | Joakim Noah (+3.05) [3] ✓ |
| 8 | **Chris Paul** (+3.90) | Jae Crowder (+3.15) [18] ✓ | Anthony Davis (+2.83) [33] ✗ | Kawhi Leonard (+3.20) [1] ✓ | Kyle OQuinn (+3.29) [52] ✗ | Danny Green (+2.82) [7] ✓ |
| 9 | **Nene** (+3.80) | Paul George (+3.09) [21] ✗ | Anderson Varejao (+2.77) [10] ✓ | Marcin Gortat (+3.19) [27] ✗ | Marcin Gortat (+3.25) [27] ✗ | Ricky Rubio (+2.79) [47] ✗ |
| 10 | **Anderson Varejao** (+3.60) | Danny Green (+2.76) [7] ✓ | Paul George (+2.75) [21] ✗ | Paul George (+3.12) [21] ✗ | Darrell Arthur (+3.20) [45] ✗ | Andre Iguodala (+2.69) [25] ✗ |
| 11 | **Nick Calathes** (+3.50) | Anderson Varejao (+2.75) [10] ✓ | Paul Millsap (+2.67) [23] ✗ | Tiago Splitter (+3.06) [6] ✓ | Chris Andersen (+3.16) [52] ✗ | Paul George (+2.62) [21] ✗ |
| 12 | **Ian Mahinmi** (+3.50) | Andre Iguodala (+2.73) [25] ✗ | Danny Green (+2.63) [7] ✓ | Chris Andersen (+2.84) [52] ✗ | Timofey Mozgov (+3.04) [64] ✗ | Kyle OQuinn (+2.43) [52] ✗ |
| 13 | **Kevin Garnett** (+3.50) | Nene (+2.68) [9] ✓ | Gerald Wallace (+2.59) [52] ✗ | Paul Millsap (+2.79) [23] ✗ | Kawhi Leonard (+2.98) [1] ✓ | Roy Hibbert (+2.43) [14] ✓ |
| 14 | **Jimmy Butler** (+3.40) | Jimmy Butler (+2.65) [14] ✓ | DeMarcus Cousins (+2.49) [16] ✓ | Danny Green (+2.76) [7] ✓ | Iman Shumpert (+2.96) [72] ✗ | David West (+2.32) [57] ✗ |
| 15 | **Roy Hibbert** (+3.40) | Paul Millsap (+2.61) [23] ✗ | Jimmy Butler (+2.45) [14] ✓ | Nene (+2.72) [9] ✓ | Paul George (+2.89) [21] ✗ | Paul Millsap (+2.20) [23] ✗ |
| 16 | **DeMarcus Cousins** (+3.30) | CJ Watson (+2.60) [17] ✓ | Marcin Gortat (+2.42) [27] ✗ | Ian Mahinmi (+2.69) [11] ✓ | Michael KiddGilchrist (+2.87) [4] ✓ | Ian Mahinmi (+2.14) [11] ✓ |
| 17 | **CJ Watson** (+3.20) | Derek Fisher (+2.58) [43] ✗ | Iman Shumpert (+2.41) [72] ✗ | Andre Iguodala (+2.67) [25] ✗ | Patrick Patterson (+2.77) [83] ✗ | Anderson Varejao (+2.08) [10] ✓ |
| 18 | **Tim Duncan** (+3.00) | Michael KiddGilchrist (+2.46) [4] ✓ | LaMarcus Aldridge (+2.39) [37] ✗ | CJ Watson (+2.67) [17] ✓ | Tiago Splitter (+2.75) [6] ✓ | Tim Duncan (+2.05) [18] ✓ |
| 19 | **Kris Humphries** (+3.00) | Joakim Noah (+2.44) [3] ✓ | Chris Andersen (+2.37) [52] ✗ | Jimmy Butler (+2.65) [14] ✓ | Andre Iguodala (+2.75) [25] ✗ | CJ Watson (+2.03) [17] ✓ |
| 20 | **Jae Crowder** (+3.00) | Tony Allen (+2.39) [30] ✗ | CJ Watson (+2.33) [17] ✓ | Paul Pierce (+2.61) [47] ✗ | Paul Pierce (+2.69) [47] ✗ | Chris Paul (+2.00) [8] ✓ |

### 2013-14 — Playoffs — defense

| # | true RAPTOR | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine |
|---|---|---|---|---|---|---|
| 1 | **Draymond Green** (+8.00) | Joakim Noah (+4.14) [8] ✓ | Joakim Noah (+4.06) [8] ✓ | Joakim Noah (+4.07) [8] ✓ | Joakim Noah (+5.26) [8] ✓ | Danny Green (+3.39) [19] ✓ |
| 2 | **Paul Millsap** (+7.60) | Marcin Gortat (+3.28) [19] ✓ | Danny Green (+3.67) [19] ✓ | Pero Antic (+3.47) [3] ✓ | Andray Blatche (+3.69) [4] ✓ | Pero Antic (+3.12) [3] ✓ |
| 3 | **Pero Antic** (+6.50) | Trevor Ariza (+3.24) [26] ✗ | Trevor Ariza (+3.41) [26] ✗ | Danny Green (+3.46) [19] ✓ | Kawhi Leonard (+3.67) [12] ✓ | Kawhi Leonard (+3.08) [12] ✓ |
| 4 | **Andray Blatche** (+6.10) | Pero Antic (+3.24) [3] ✓ | Pero Antic (+3.39) [3] ✓ | Trevor Ariza (+3.33) [26] ✗ | Trevor Ariza (+3.59) [26] ✗ | Paul Millsap (+2.79) [2] ✓ |
| 5 | **Nick Collison** (+6.10) | Danny Green (+3.18) [19] ✓ | Kawhi Leonard (+3.15) [12] ✓ | Kawhi Leonard (+3.32) [12] ✓ | Kevin Garnett (+3.55) [49] ✗ | Draymond Green (+2.77) [1] ✓ |
| 6 | **Greivis Vasquez** (+6.00) | Paul Millsap (+3.05) [2] ✓ | Chris Andersen (+3.04) [7] ✓ | Andray Blatche (+3.29) [4] ✓ | Chris Andersen (+3.42) [7] ✓ | Trevor Ariza (+2.74) [26] ✗ |
| 7 | **Chris Andersen** (+5.40) | Kawhi Leonard (+3.04) [12] ✓ | Kevin Garnett (+2.94) [49] ✗ | Chris Andersen (+3.24) [7] ✓ | Marcin Gortat (+3.34) [19] ✓ | Manu Ginobili (+2.46) [21] ✗ |
| 8 | **Joakim Noah** (+5.30) | Tiago Splitter (+2.78) [9] ✓ | Andray Blatche (+2.92) [4] ✓ | Serge Ibaka (+2.99) [13] ✓ | Draymond Green (+3.18) [1] ✓ | John Wall (+2.42) [41] ✗ |
| 9 | **Tiago Splitter** (+5.00) | John Wall (+2.73) [41] ✗ | Paul Millsap (+2.78) [2] ✓ | Draymond Green (+2.81) [1] ✓ | Paul Millsap (+3.18) [2] ✓ | Serge Ibaka (+2.33) [13] ✓ |
| 10 | **Vince Carter** (+5.00) | Kyle Korver (+2.72) [62] ✗ | Tiago Splitter (+2.67) [9] ✓ | Paul Millsap (+2.80) [2] ✓ | Pero Antic (+3.15) [3] ✓ | Joakim Noah (+2.13) [8] ✓ |
| 11 | **Rashard Lewis** (+4.90) | Mike Conley (+2.59) [74] ✗ | Draymond Green (+2.66) [1] ✓ | Tiago Splitter (+2.71) [9] ✓ | Tiago Splitter (+3.09) [9] ✓ | Bradley Beal (+2.06) [24] ✗ |
| 12 | **Kawhi Leonard** (+4.40) | Tony Allen (+2.53) [30] ✗ | Mike Conley (+2.61) [74] ✗ | Marcin Gortat (+2.63) [19] ✓ | Serge Ibaka (+3.07) [13] ✓ | Marcin Gortat (+2.04) [19] ✓ |
| 13 | **Serge Ibaka** (+4.20) | Draymond Green (+2.45) [1] ✓ | Greivis Vasquez (+2.60) [6] ✓ | Kevin Garnett (+2.62) [49] ✗ | Steven Adams (+2.98) [80] ✗ | DeAndre Jordan (+1.81) [40] ✗ |
| 14 | **Ian Mahinmi** (+4.20) | Kevin Garnett (+2.37) [49] ✗ | Patrick Patterson (+2.50) [64] ✗ | Mike Conley (+2.56) [74] ✗ | Patrick Patterson (+2.84) [64] ✗ | Chris Andersen (+1.81) [7] ✓ |
| 15 | **Zach Randolph** (+4.20) | Chris Andersen (+2.36) [7] ✓ | Marcin Gortat (+2.45) [19] ✓ | George Hill (+2.54) [23] ✗ | Danny Green (+2.39) [19] ✓ | Chris Paul (+1.76) [24] ✗ |
| 16 | **Marc Gasol** (+4.10) | Greivis Vasquez (+2.35) [6] ✓ | Serge Ibaka (+2.33) [13] ✓ | Manu Ginobili (+2.54) [21] ✗ | Manu Ginobili (+2.31) [21] ✗ | Patty Mills (+1.56) [18] ✓ |
| 17 | **Deron Williams** (+3.80) | Andray Blatche (+2.34) [4] ✓ | Kyle Korver (+2.30) [62] ✗ | Patrick Patterson (+2.33) [64] ✗ | Kendrick Perkins (+2.21) [30] ✗ | David West (+1.50) [39] ✗ |
| 18 | **Patty Mills** (+3.70) | Paul Pierce (+2.31) [34] ✗ | Paul Pierce (+2.24) [34] ✗ | Kyle Korver (+2.08) [62] ✗ | Greivis Vasquez (+2.15) [6] ✓ | Tony Allen (+1.46) [30] ✗ |
| 19 | **Danny Green** (+3.50) | Zach Randolph (+2.17) [13] ✓ | Tony Allen (+2.22) [30] ✗ | LeBron James (+2.06) [41] ✗ | Zach Randolph (+2.05) [13] ✓ | Tiago Splitter (+1.43) [9] ✓ |
| 20 | **Marcin Gortat** (+3.50) | Serge Ibaka (+2.07) [13] ✓ | Zach Randolph (+2.18) [13] ✓ | Alan Anderson (+1.95) [46] ✗ | LeBron James (+2.04) [41] ✗ | Kevin Garnett (+1.25) [49] ✗ |

### 2014-15 — Regular season — defense

| # | true RAPTOR | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine |
|---|---|---|---|---|---|---|
| 1 | **Kawhi Leonard** (+5.20) | Andrew Bogut (+4.82) [5] ✓ | Rudy Gobert (+4.77) [3] ✓ | Draymond Green (+4.80) [2] ✓ | Andrew Bogut (+4.82) [5] ✓ | Kawhi Leonard (+4.47) [1] ✓ |
| 2 | **Draymond Green** (+5.10) | Draymond Green (+4.63) [2] ✓ | Draymond Green (+4.58) [2] ✓ | Rudy Gobert (+4.75) [3] ✓ | Draymond Green (+4.76) [2] ✓ | Tony Allen (+4.32) [3] ✓ |
| 3 | **Rudy Gobert** (+4.80) | Rudy Gobert (+4.35) [3] ✓ | Andrew Bogut (+4.55) [5] ✓ | Andrew Bogut (+4.33) [5] ✓ | Rudy Gobert (+4.02) [3] ✓ | Draymond Green (+3.75) [2] ✓ |
| 4 | **Tony Allen** (+4.80) | Tony Allen (+3.99) [3] ✓ | Kawhi Leonard (+3.92) [1] ✓ | Anthony Davis (+3.59) [6] ✓ | Andre Roberson (+3.76) [10] ✓ | Andrew Bogut (+3.21) [5] ✓ |
| 5 | **Andrew Bogut** (+4.70) | Kawhi Leonard (+3.59) [1] ✓ | Anthony Davis (+3.31) [6] ✓ | Kawhi Leonard (+3.58) [1] ✓ | Nerlens Noel (+3.75) [19] ✓ | DeAndre Jordan (+3.15) [60] ✗ |
| 6 | **Anthony Davis** (+4.50) | Nerlens Noel (+3.53) [19] ✓ | Andre Roberson (+3.30) [10] ✓ | Tony Allen (+3.45) [3] ✓ | Anthony Davis (+3.61) [6] ✓ | Nerlens Noel (+2.89) [19] ✓ |
| 7 | **DeMarcus Cousins** (+4.40) | Anthony Davis (+3.31) [6] ✓ | AlFarouq Aminu (+3.25) [20] ✗ | Nerlens Noel (+3.36) [19] ✓ | AlFarouq Aminu (+3.59) [20] ✗ | Anthony Davis (+2.86) [6] ✓ |
| 8 | **Marcin Gortat** (+3.60) | Nene (+3.08) [17] ✓ | Nene (+3.23) [17] ✓ | AlFarouq Aminu (+3.19) [20] ✗ | Jonas Jerebko (+3.44) [17] ✓ | Rudy Gobert (+2.78) [3] ✓ |
| 9 | **Tim Duncan** (+3.50) | Andre Roberson (+2.94) [10] ✓ | Nerlens Noel (+3.21) [19] ✓ | Nene (+3.14) [17] ✓ | Kawhi Leonard (+3.43) [1] ✓ | Danny Green (+2.72) [14] ✓ |
| 10 | **Andre Roberson** (+3.40) | AlFarouq Aminu (+2.93) [20] ✗ | Tony Allen (+3.20) [3] ✓ | Andre Roberson (+3.13) [10] ✓ | Greg Monroe (+3.22) [111] ✗ | AlFarouq Aminu (+2.59) [20] ✗ |
| 11 | **Kosta Koufos** (+3.30) | Danny Green (+2.66) [14] ✓ | DeMarcus Cousins (+2.97) [7] ✓ | Tyson Chandler (+3.08) [20] ✗ | Tony Allen (+3.17) [3] ✓ | Tim Duncan (+2.49) [9] ✓ |
| 12 | **Zaza Pachulia** (+3.20) | Jonas Jerebko (+2.65) [17] ✓ | Tyson Chandler (+2.89) [20] ✗ | Iman Shumpert (+3.01) [29] ✗ | Iman Shumpert (+3.15) [29] ✗ | Paul Millsap (+2.35) [26] ✗ |
| 13 | **Khris Middleton** (+3.10) | Tim Duncan (+2.60) [9] ✓ | Danny Green (+2.77) [14] ✓ | Greg Monroe (+2.84) [111] ✗ | Bismack Biyombo (+3.14) [102] ✗ | Khris Middleton (+2.07) [13] ✓ |
| 14 | **Danny Green** (+3.00) | Tyson Chandler (+2.60) [20] ✗ | Iman Shumpert (+2.72) [29] ✗ | Zaza Pachulia (+2.73) [12] ✓ | Kelly Olynyk (+3.08) [34] ✗ | Andre Roberson (+2.05) [10] ✓ |
| 15 | **Serge Ibaka** (+3.00) | Marcin Gortat (+2.59) [8] ✓ | Jonas Jerebko (+2.71) [17] ✓ | DeMarcus Cousins (+2.73) [7] ✓ | Tyson Chandler (+3.04) [20] ✗ | Bismack Biyombo (+2.05) [102] ✗ |
| 16 | **Michael KiddGilchrist** (+3.00) | DeMarcus Cousins (+2.59) [7] ✓ | Tim Duncan (+2.43) [9] ✓ | Omer Asik (+2.68) [26] ✗ | Dwight Howard (+2.92) [40] ✗ | Marcus Smart (+1.98) [42] ✗ |
| 17 | **Jonas Jerebko** (+2.80) | Khris Middleton (+2.58) [13] ✓ | John Henson (+2.41) [85] ✗ | Jonas Jerebko (+2.61) [17] ✓ | Michael KiddGilchrist (+2.86) [14] ✓ | John Wall (+1.97) [127] ✗ |
| 18 | **Nene** (+2.80) | Kosta Koufos (+2.50) [11] ✓ | Zaza Pachulia (+2.35) [12] ✓ | Danny Green (+2.58) [14] ✓ | DeMarcus Cousins (+2.85) [7] ✓ | DeMarcus Cousins (+1.95) [7] ✓ |
| 19 | **Nerlens Noel** (+2.70) | Marcus Smart (+2.44) [42] ✗ | Marcus Smart (+2.30) [42] ✗ | Bismack Biyombo (+2.49) [102] ✗ | Brandan Wright (+2.78) [54] ✗ | Marcin Gortat (+1.93) [8] ✓ |
| 20 | **Marc Gasol** (+2.60) | Dwight Howard (+2.40) [40] ✗ | Derrick Favors (+2.28) [34] ✗ | Dwight Howard (+2.47) [40] ✗ | Marcus Smart (+2.75) [42] ✗ | Michael KiddGilchrist (+1.74) [14] ✓ |

### 2014-15 — Playoffs — defense

| # | true RAPTOR | baseline (all rows) | MPG>=20 | MPG>=20 + drop ~0 | MPG>=28 + drop ~0 | Paine |
|---|---|---|---|---|---|---|
| 1 | **Jarrett Jack** (+7.50) | Jarrett Jack (+4.47) [1] ✓ | DeAndre Jordan (+4.94) [29] ✗ | Tony Allen (+5.10) [10] ✓ | DeAndre Jordan (+4.88) [29] ✗ | Tony Allen (+5.01) [10] ✓ |
| 2 | **Anthony Davis** (+7.20) | DeAndre Jordan (+4.47) [29] ✗ | Tony Allen (+4.69) [10] ✓ | DeAndre Jordan (+4.82) [29] ✗ | Dwight Howard (+4.70) [7] ✓ | Jarrett Jack (+3.62) [1] ✓ |
| 3 | **Timofey Mozgov** (+6.90) | Tony Allen (+4.37) [10] ✓ | Dwight Howard (+4.28) [7] ✓ | John Henson (+4.25) [27] ✗ | AlFarouq Aminu (+4.65) [6] ✓ | Pau Gasol (+3.02) [15] ✓ |
| 4 | **Otto Porter Jr.** (+6.30) | Timofey Mozgov (+4.07) [3] ✓ | AlFarouq Aminu (+4.00) [6] ✓ | Dwight Howard (+4.19) [7] ✓ | John Henson (+4.38) [27] ✗ | AlFarouq Aminu (+2.97) [6] ✓ |
| 5 | **Trevor Ariza** (+6.10) | Dwight Howard (+4.06) [7] ✓ | Jarrett Jack (+3.95) [1] ✓ | Nene (+4.10) [11] ✓ | Tony Allen (+4.35) [10] ✓ | Jimmy Butler (+2.29) [13] ✓ |
| 6 | **AlFarouq Aminu** (+5.80) | Otto Porter Jr. (+3.80) [4] ✓ | John Henson (+3.61) [27] ✗ | AlFarouq Aminu (+4.04) [6] ✓ | Nene (+3.95) [11] ✓ | Andrew Bogut (+2.28) [35] ✗ |
| 7 | **Dwight Howard** (+5.70) | Pau Gasol (+3.71) [15] ✓ | Pau Gasol (+3.58) [15] ✓ | Pau Gasol (+3.94) [15] ✓ | Jarrett Jack (+3.78) [1] ✓ | Otto Porter Jr. (+2.24) [4] ✓ |
| 8 | **Danny Green** (+5.50) | Tim Duncan (+3.58) [22] ✗ | Nene (+3.57) [11] ✓ | Jarrett Jack (+3.76) [1] ✓ | Timofey Mozgov (+3.59) [3] ✓ | Paul Millsap (+2.24) [29] ✗ |
| 9 | **Marc Gasol** (+5.30) | Jimmy Butler (+3.52) [13] ✓ | Otto Porter Jr. (+3.54) [4] ✓ | Jimmy Butler (+3.63) [13] ✓ | Pau Gasol (+3.50) [15] ✓ | John Wall (+2.07) [44] ✗ |
| 10 | **Tony Allen** (+5.00) | Al Horford (+3.24) [12] ✓ | Jimmy Butler (+3.47) [13] ✓ | Al Horford (+3.62) [12] ✓ | Tim Duncan (+3.35) [22] ✗ | Dwight Howard (+2.05) [7] ✓ |
| 11 | **Nene** (+4.70) | Nene (+3.19) [11] ✓ | Al Horford (+3.45) [12] ✓ | Otto Porter Jr. (+3.60) [4] ✓ | Jimmy Butler (+3.32) [13] ✓ | Kyle Korver (+2.03) [41] ✗ |
| 12 | **Al Horford** (+4.40) | Joakim Noah (+3.17) [29] ✗ | Timofey Mozgov (+3.26) [3] ✓ | Tim Duncan (+3.56) [22] ✗ | Paul Millsap (+2.99) [29] ✗ | DeAndre Jordan (+1.99) [29] ✗ |
| 13 | **Jimmy Butler** (+3.70) | AlFarouq Aminu (+3.07) [6] ✓ | Tim Duncan (+3.22) [22] ✗ | Timofey Mozgov (+3.39) [3] ✓ | Otto Porter Jr. (+2.98) [4] ✓ | Drew Gooden (+1.97) [69] ✗ |
| 14 | **Blake Griffin** (+3.70) | Alan Anderson (+3.07) [20] ✓ | Danny Green (+2.97) [8] ✓ | Joakim Noah (+3.24) [29] ✗ | Al Horford (+2.94) [12] ✓ | Iman Shumpert (+1.72) [54] ✗ |
| 15 | **Pau Gasol** (+3.50) | Kyle Korver (+2.93) [41] ✗ | Mike Conley (+2.94) [38] ✗ | Danny Green (+3.22) [8] ✓ | Alan Anderson (+2.94) [20] ✓ | Danny Green (+1.71) [8] ✓ |
| 16 | **Ramon Sessions** (+3.50) | Harrison Barnes (+2.80) [43] ✗ | Harrison Barnes (+2.87) [43] ✗ | Matt Barnes (+3.16) [17] ✓ | Matt Barnes (+2.93) [17] ✓ | Matt Barnes (+1.68) [17] ✓ |
| 17 | **Matt Barnes** (+3.40) | Marc Gasol (+2.75) [9] ✓ | Joakim Noah (+2.79) [29] ✗ | Harrison Barnes (+3.15) [43] ✗ | Danny Green (+2.92) [8] ✓ | Joakim Noah (+1.65) [29] ✗ |
| 18 | **Stephen Curry** (+3.00) | Andrew Bogut (+2.66) [35] ✗ | Matt Barnes (+2.63) [17] ✓ | Andrew Bogut (+2.87) [35] ✗ | Joakim Noah (+2.89) [29] ✗ | Tim Duncan (+1.54) [22] ✗ |
| 19 | **Derrick Rose** (+3.00) | Mike Conley (+2.56) [38] ✗ | Kyle Korver (+2.58) [41] ✗ | Kyle Korver (+2.81) [41] ✗ | Andrew Bogut (+2.89) [35] ✗ | Nikola Mirotic (+1.51) [77] ✗ |
| 20 | **Alan Anderson** (+2.80) | Matt Barnes (+2.37) [17] ✓ | Brook Lopez (+2.55) [34] ✗ | Brook Lopez (+2.78) [34] ✗ | Harrison Barnes (+2.67) [43] ✗ | Derrick Rose (+1.42) [18] ✓ |

