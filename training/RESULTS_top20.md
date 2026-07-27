# Top-20 leaderboards on the held-out seasons

True 538 RAPTOR vs. our combined model vs. Neil Paine's Estimated RAPTOR,
for 2013-14 and 2014-15. Offense and defense are ranked separately.

> Paine's weights were fit on 2014-2023 full RAPTOR, which includes both of
> these seasons — his predictions are in-sample, ours are not.

## Minutes threshold

The threshold is derived, not chosen: it is the **lowest minutes total among
any true top-20 player**, taken across both seasons and both targets, so no
genuine leader is ever ruled ineligible.

| season | split | target | min mp in true top 20 | median mp | pool n | pool min mp |
|---|---|---|---|---|---|---|
| 2013-14 | Regular season | offense | 1412 | 2722 | 250 | 1065 |
| 2013-14 | Regular season | defense | 1109 | 1710 | 250 | 1065 |
| 2014-15 | Regular season | offense | 1267 | 2436 | 250 | 1148 |
| 2014-15 | Regular season | defense | 1230 | 2074 | 250 | 1148 |
| 2013-14 | Playoffs | offense | 176 | 454 | 100 | 167 |
| 2013-14 | Playoffs | defense | 170 | 283 | 100 | 167 |
| 2014-15 | Playoffs | offense | 131 | 224 | 100 | 131 |
| 2014-15 | Playoffs | defense | 150 | 412 | 100 | 131 |

**Regular season → ≥ 1109 minutes. Playoffs → ≥ 131 minutes.**

Note how little this bites: 538 only rated ~250 players per historical
season, and every one of them already clears 1,065 regular-season minutes.
In the playoffs the true top 20 reaches down to the very bottom of the pool
(a 131-minute player makes it), so no threshold can be applied there without
excluding a real leader. The filter matters far more when ranking an
unfiltered player universe — Paine's own CSV has a 1-minute player at
eRO +55.7 who would otherwise top every offensive leaderboard.

## Summary — how many of the true top 20 each system recovers

| season | split | target | pool | dropped by threshold | ours hits@20 | Paine hits@20 | ours ρ | Paine ρ |
|---|---|---|---|---|---|---|---|---|
| 2013-14 | Regular season | offense | 240 | 6 | **17/20** | 18/20 | +0.921 | +0.861 |
| 2013-14 | Regular season | defense | 240 | 6 | **13/20** | 12/20 | +0.875 | +0.796 |
| 2013-14 | Playoffs | offense | 99 | 0 | **16/20** | 13/20 | +0.893 | +0.838 |
| 2013-14 | Playoffs | defense | 99 | 0 | **13/20** | 11/20 | +0.690 | +0.667 |
| 2014-15 | Regular season | offense | 246 | 0 | **14/20** | 15/20 | +0.925 | +0.858 |
| 2014-15 | Regular season | defense | 246 | 0 | **16/20** | 14/20 | +0.871 | +0.801 |
| 2014-15 | Playoffs | offense | 98 | 0 | **16/20** | 13/20 | +0.898 | +0.787 |
| 2014-15 | Playoffs | defense | 98 | 0 | **13/20** | 10/20 | +0.780 | +0.633 |

**Overall: ours 118/160, Paine 106/160.**

## Leaderboards

`[n]` after a predicted name is that player's *true* rank. A ✓ means the
player is genuinely in the true top 20.

### 2013-14 — Regular season — offense

| # | true RAPTOR | ours (predicted) | Paine (predicted) |
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

### 2013-14 — Regular season — defense

| # | true RAPTOR | ours (predicted) | Paine (predicted) |
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

### 2013-14 — Playoffs — offense

| # | true RAPTOR | ours (predicted) | Paine (predicted) |
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

### 2013-14 — Playoffs — defense

| # | true RAPTOR | ours (predicted) | Paine (predicted) |
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

### 2014-15 — Regular season — offense

| # | true RAPTOR | ours (predicted) | Paine (predicted) |
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

### 2014-15 — Regular season — defense

| # | true RAPTOR | ours (predicted) | Paine (predicted) |
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

### 2014-15 — Playoffs — offense

| # | true RAPTOR | ours (predicted) | Paine (predicted) |
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

### 2014-15 — Playoffs — defense

| # | true RAPTOR | ours (predicted) | Paine (predicted) |
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

