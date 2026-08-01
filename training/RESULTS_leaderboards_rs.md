# Top-50 leaderboards: held-out seasons and 2023-26

## What can and cannot be checked

**2013-14 and 2014-15** are held out of training, and 538 published RAPTOR for
them, so every projected position can be scored against the truth.

**2023-24, 2024-25 and 2025-26 have no ground truth.** 538 shut down; those
cells have features and no labels. Their tables below carry projections only.
There is no `true`, no `drank` and no Kendall tau for them, because there is
nothing to compare against — not because it was omitted.

## Columns

| column | meaning |
|---|---|
| `pos` | position in the **projected** order |
| `projected` | the player the model puts there |
| `est` | the model's score |
| `true` | that player's actual RAPTOR |
| `true rank` | where that player actually belongs |
| `Δrank` | `true rank - pos`. **Positive means placed too high**: +2 at pos 3 means they truly belong 5th. 0 is exact. |
| `actual at pos` | who really belongs at that position |

## Model

| target | config | chosen | rounds | test MAE | R² | rho |
|---|---|---|---|---|---|---|
| total | stage2-0 / l2 | blend | 1149 | 0.874 | +0.834 | +0.910 |
| offense | stage2-0 / l2 | blend | 1183 | 0.541 | +0.894 | +0.939 |
| defense | stage2-0 / huber | blend | 649 | 0.749 | +0.746 | +0.871 |

Selected by cross-validated MAE inside the training rows; the test seasons are
used once, for the tables below. Every non-test row trains — no season is held
out for validation (see RESULTS_trainonly.md: it costs nothing).

Minutes floor for the 2023-26 boards: Regular season 1065, Playoffs 131 — the lowest 538 itself ever rated in that split.

## Feature polarity

Every stat is classified offence-centric, defence-centric or neutral
(see [stat_polarity.md](stat_polarity.md)). The offence model uses
offence+neutral, the defence model defence+neutral, and total uses everything.
Of 908 source columns: **685 offence, 107 defence, 116 neutral** — the feeds are
heavily offensive, so the defence model keeps about a quarter of the columns and
the offence model nearly nine tenths.

On cross-validated MAE the restriction is a small win for both — offence
1.0685 against 1.0748, defence 1.3241 against 1.3290 — and it finds more of the
right players (offence hits@30 102/120 against 98, defence 86 against 80) while
ordering them very slightly worse. It is close to neutral: gradient boosting was
already largely ignoring the wrong-side columns.

## Versus Neil Paine's Estimated RAPTOR

Paine's linear model, published weights, on the players he covers in each cell.
**His weights were fit on 2014-2023 RAPTOR, which includes both test seasons —**
**his numbers are in-sample and ours are not.** He should be expected to win.

| target | season | split | n | ours MAE | Paine MAE | ours tau30 | Paine tau30 | ours hits@30 | Paine hits@30 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| total | 2013-14 | Regular season | 246 | 0.879 | 0.945 | +0.490 | +0.480 | 24/30 | 22/30 |
| total | 2014-15 | Regular season | 246 | 0.870 | 0.914 | +0.664 | +0.595 | 23/30 | 22/30 |
| offense | 2013-14 | Regular season | 246 | 0.537 | 0.713 | +0.793 | +0.811 | 24/30 | 22/30 |
| offense | 2014-15 | Regular season | 246 | 0.545 | 0.722 | +0.660 | +0.490 | 25/30 | 23/30 |
| defense | 2013-14 | Regular season | 246 | 0.745 | 0.925 | +0.347 | +0.352 | 21/30 | 17/30 |
| defense | 2014-15 | Regular season | 246 | 0.744 | 0.846 | +0.536 | +0.582 | 23/30 | 19/30 |

## Kendall tau over the top 30, held-out seasons

`tau(true30)` compares the true order of the true top 30 against their
projected order. `tau(union30)` widens the set to the union of the true and
projected top 30, so it also penalises wrongly promoted players.

| target | season | split | pool | tau(true30) | tau(union30) | hits@30 | mean &#124;Δrank&#124; |
|---|---|---|---|---|---|---|---|
| total | 2013-14 | Regular season | 247 | +0.471 | +0.387 | 24/30 | 13.9 |
| total | 2014-15 | Regular season | 247 | +0.678 | +0.428 | 22/30 | 12.9 |
| offense | 2013-14 | Regular season | 247 | +0.811 | +0.688 | 23/30 | 11.5 |
| offense | 2014-15 | Regular season | 247 | +0.660 | +0.560 | 25/30 | 9.3 |
| defense | 2013-14 | Regular season | 247 | +0.375 | +0.209 | 21/30 | 17.5 |
| defense | 2014-15 | Regular season | 247 | +0.600 | +0.437 | 22/30 | 18.6 |

## 2013-14 Regular season — total, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.471 &nbsp;·&nbsp; hits@30 24/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.17 | +11.00 | 1 | +0 | Chris Paul | +11.00 |
| 2 | Kevin Durant | +7.04 | +7.10 | 2 | +0 | Kevin Durant | +7.10 |
| 3 | Paul George | +6.74 | +5.60 | 8 | +5 | Kawhi Leonard | +6.70 |
| 4 | LeBron James | +6.68 | +4.60 | 15 | +11 | Kevin Love | +6.60 |
| 5 | Kawhi Leonard | +5.50 | +6.70 | 3 | -2 | James Harden | +6.10 |
| 6 | Manu Ginobili | +5.45 | +5.10 | 9 | +3 | Joakim Noah | +5.90 |
| 7 | Andre Iguodala | +5.27 | +3.80 | 23 | +16 | Kyle Lowry | +5.70 |
| 8 | James Harden | +5.14 | +6.10 | 5 | -3 | Paul George | +5.60 |
| 9 | Kevin Love | +5.03 | +6.60 | 4 | -5 | Manu Ginobili | +5.10 |
| 10 | Blake Griffin | +4.75 | +3.20 | 34 | +24 | DeMarcus Cousins | +5.00 |
| 11 | Dirk Nowitzki | +4.58 | +4.70 | 13 | +2 | Goran Dragic | +5.00 |
| 12 | Goran Dragic | +4.56 | +5.00 | 11 | -1 | Patty Mills | +4.80 |
| 13 | Kyle Lowry | +4.52 | +5.70 | 7 | -6 | Dirk Nowitzki | +4.70 |
| 14 | Andrew Bogut | +4.50 | +3.10 | 37 | +23 | Danny Green | +4.70 |
| 15 | Jimmy Butler | +4.03 | +3.90 | 20 | +5 | LeBron James | +4.60 |
| 16 | Isaiah Thomas | +3.77 | +3.90 | 19 | +3 | Anderson Varejao | +4.10 |
| 17 | Joakim Noah | +3.72 | +5.90 | 6 | -11 | Patrick Beverley | +4.10 |
| 18 | Patty Mills | +3.68 | +4.80 | 12 | -6 | Mario Chalmers | +4.00 |
| 19 | Ricky Rubio | +3.62 | +3.70 | 26 | +7 | Isaiah Thomas | +3.90 |
| 20 | Anderson Varejao | +3.60 | +4.10 | 16 | -4 | Jimmy Butler | +3.90 |
| 21 | Carmelo Anthony | +3.59 | +3.80 | 24 | +3 | Mike Conley | +3.80 |
| 22 | Anthony Davis | +3.51 | +3.50 | 28 | +6 | Kemba Walker | +3.80 |
| 23 | Paul Millsap | +3.36 | +3.10 | 36 | +13 | Andre Iguodala | +3.80 |
| 24 | Draymond Green | +3.34 | +3.40 | 30 | +6 | Carmelo Anthony | +3.80 |
| 25 | Nikola Pekovic | +3.30 | +3.30 | 31 | +6 | Russell Westbrook | +3.70 |
| 26 | Mike Conley | +3.30 | +3.80 | 21 | -5 | Ricky Rubio | +3.70 |
| 27 | Nicolas Batum | +3.14 | +1.90 | 59 | +32 | Eric Bledsoe | +3.70 |
| 28 | Chris Bosh | +3.12 | +0.90 | 94 | +66 | Anthony Davis | +3.50 |
| 29 | LaMarcus Aldridge | +3.08 | +3.40 | 29 | +0 | LaMarcus Aldridge | +3.40 |
| 30 | Danny Green | +3.07 | +4.70 | 14 | -16 | Draymond Green | +3.40 |
| 31 | Kemba Walker | +3.05 | +3.80 | 22 | -9 | Nikola Pekovic | +3.30 |
| 32 | Paul Pierce | +3.04 | +1.90 | 60 | +28 | DeMarre Carroll | +3.30 |
| 33 | Damian Lillard | +3.03 | +2.10 | 56 | +23 | Tiago Splitter | +3.30 |
| 34 | David West | +2.98 | +2.20 | 51 | +17 | Blake Griffin | +3.20 |
| 35 | Derek Fisher | +2.97 | +2.30 | 50 | +15 | Deron Williams | +3.20 |
| 36 | Patrick Beverley | +2.88 | +4.10 | 17 | -19 | Paul Millsap | +3.10 |
| 37 | Pablo Prigioni | +2.84 | +1.60 | 73 | +36 | Andrew Bogut | +3.10 |
| 38 | George Hill | +2.73 | +2.50 | 48 | +10 | Kris Humphries | +3.00 |
| 39 | Tiago Splitter | +2.73 | +3.30 | 33 | -6 | Klay Thompson | +2.90 |
| 40 | Russell Westbrook | +2.70 | +3.70 | 25 | -15 | Ty Lawson | +2.90 |
| 41 | Deron Williams | +2.67 | +3.20 | 35 | -6 | Jae Crowder | +2.90 |
| 42 | Trevor Ariza | +2.62 | +1.70 | 67 | +25 | Robin Lopez | +2.90 |
| 43 | DeAndre Jordan | +2.56 | +1.80 | 64 | +21 | Vince Carter | +2.90 |
| 44 | Al Jefferson | +2.55 | +1.30 | 78 | +34 | Darren Collison | +2.70 |
| 45 | Mario Chalmers | +2.47 | +4.00 | 18 | -27 | Shane Battier | +2.70 |
| 46 | Kyle Korver | +2.41 | +1.30 | 80 | +34 | Wesley Matthews | +2.60 |
| 47 | DeMarre Carroll | +2.41 | +3.30 | 32 | -15 | Tony Allen | +2.60 |
| 48 | John Wall | +2.36 | +1.70 | 69 | +21 | George Hill | +2.50 |
| 49 | DeMarcus Cousins | +2.27 | +5.00 | 10 | -39 | Channing Frye | +2.40 |
| 50 | Wesley Matthews | +2.26 | +2.60 | 46 | -4 | Derek Fisher | +2.30 |

### 2013-14 Regular season — total, Paine's top 30 (in-sample)

> 246 players covered &nbsp;·&nbsp; tau(true30) +0.480 &nbsp;·&nbsp; hits@30 22/30 &nbsp;·&nbsp; MAE 0.945

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +8.79 | +11.00 | 1 | +0 |
| 2 | Kevin Durant | +7.21 | +7.10 | 2 | +0 |
| 3 | LeBron James | +6.77 | +4.60 | 15 | +12 |
| 4 | Kawhi Leonard | +5.95 | +6.70 | 3 | -1 |
| 5 | James Harden | +5.50 | +6.10 | 5 | +0 |
| 6 | Paul George | +5.08 | +5.60 | 8 | +2 |
| 7 | Manu Ginobili | +4.99 | +5.10 | 9 | +2 |
| 8 | Kevin Love | +4.86 | +6.60 | 4 | -4 |
| 9 | Goran Dragic | +4.71 | +5.00 | 10 | +1 |
| 10 | Andre Iguodala | +4.53 | +3.80 | 23 | +13 |
| 11 | Kyle Lowry | +4.27 | +5.70 | 7 | -4 |
| 12 | Joakim Noah | +4.22 | +5.90 | 6 | -6 |
| 13 | Russell Westbrook | +4.18 | +3.70 | 26 | +13 |
| 14 | Anthony Davis | +4.18 | +3.50 | 28 | +14 |
| 15 | Blake Griffin | +4.00 | +3.20 | 35 | +20 |
| 16 | Ricky Rubio | +3.91 | +3.70 | 25 | +9 |
| 17 | Brandan Wright | +3.89 | +0.30 | 115 | +98 |
| 18 | Dirk Nowitzki | +3.81 | +4.70 | 13 | -5 |
| 19 | DeMarcus Cousins | +3.60 | +5.00 | 11 | -8 |
| 20 | Patty Mills | +3.50 | +4.80 | 12 | -8 |
| 21 | Jimmy Butler | +3.31 | +3.90 | 20 | -1 |
| 22 | Carmelo Anthony | +3.28 | +3.80 | 21 | -1 |
| 23 | DeAndre Jordan | +3.19 | +1.80 | 65 | +42 |
| 24 | LaMarcus Aldridge | +3.12 | +3.40 | 30 | +6 |
| 25 | Mike Conley | +3.12 | +3.80 | 24 | -1 |
| 26 | Trevor Ariza | +3.03 | +1.70 | 67 | +41 |
| 27 | Dwyane Wade | +3.01 | +1.20 | 86 | +59 |
| 28 | Al Jefferson | +2.99 | +1.30 | 81 | +53 |
| 29 | Tony Allen | +2.97 | +2.60 | 47 | +18 |
| 30 | Paul Millsap | +2.91 | +3.10 | 36 | +6 |

## 2014-15 Regular season — total, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.678 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.52 | +10.60 | 1 | +0 | Chris Paul | +10.60 |
| 2 | LeBron James | +7.17 | +5.10 | 11 | +9 | Kawhi Leonard | +8.90 |
| 3 | Kawhi Leonard | +6.75 | +8.90 | 2 | -1 | Anthony Davis | +8.80 |
| 4 | Anthony Davis | +6.73 | +8.80 | 3 | -1 | James Harden | +7.70 |
| 5 | Draymond Green | +6.61 | +6.50 | 5 | +0 | Draymond Green | +6.50 |
| 6 | James Harden | +6.42 | +7.70 | 4 | -2 | Danny Green | +6.10 |
| 7 | Jimmy Butler | +5.61 | +3.00 | 36 | +29 | George Hill | +5.60 |
| 8 | Lou Williams | +4.72 | +3.00 | 35 | +27 | Russell Westbrook | +5.60 |
| 9 | Klay Thompson | +4.72 | +5.30 | 10 | +1 | DeMarcus Cousins | +5.40 |
| 10 | Russell Westbrook | +4.66 | +5.60 | 8 | -2 | Klay Thompson | +5.30 |
| 11 | George Hill | +4.51 | +5.60 | 7 | -4 | LeBron James | +5.10 |
| 12 | Khris Middleton | +4.28 | +4.80 | 12 | +0 | Khris Middleton | +4.80 |
| 13 | Kyrie Irving | +4.27 | +4.60 | 13 | +0 | Kyrie Irving | +4.60 |
| 14 | Danny Green | +4.26 | +6.10 | 6 | -8 | Kyle Korver | +4.60 |
| 15 | Damian Lillard | +4.23 | +2.70 | 42 | +27 | DeAndre Jordan | +4.60 |
| 16 | Andrew Bogut | +4.02 | +3.70 | 23 | +7 | LaMarcus Aldridge | +4.30 |
| 17 | Tony Allen | +3.91 | +4.30 | 17 | +0 | Tony Allen | +4.30 |
| 18 | DeMarcus Cousins | +3.76 | +5.40 | 9 | -9 | Nikola Mirotic | +4.20 |
| 19 | Wesley Matthews | +3.74 | +3.60 | 24 | +5 | Rudy Gobert | +4.10 |
| 20 | Blake Griffin | +3.63 | +2.00 | 63 | +43 | Marc Gasol | +4.00 |
| 21 | Rudy Gobert | +3.53 | +4.10 | 19 | -2 | Darren Collison | +4.00 |
| 22 | Gordon Hayward | +3.49 | +3.40 | 26 | +4 | Kyle Lowry | +3.90 |
| 23 | Kyle Korver | +3.43 | +4.60 | 14 | -9 | Andrew Bogut | +3.70 |
| 24 | John Wall | +3.43 | +2.00 | 64 | +40 | Wesley Matthews | +3.60 |
| 25 | Jared Dudley | +3.38 | +1.90 | 67 | +42 | Jonas Jerebko | +3.60 |
| 26 | DeAndre Jordan | +3.26 | +4.60 | 15 | -11 | Gordon Hayward | +3.40 |
| 27 | LaMarcus Aldridge | +3.25 | +4.30 | 16 | -11 | Tim Duncan | +3.30 |
| 28 | Manu Ginobili | +3.18 | +3.20 | 33 | +5 | Paul Millsap | +3.30 |
| 29 | Mike Conley | +3.18 | +2.90 | 39 | +10 | Marcin Gortat | +3.20 |
| 30 | Kyle Lowry | +3.09 | +3.90 | 22 | -8 | Kevin Love | +3.20 |
| 31 | Paul Millsap | +3.04 | +3.30 | 28 | -3 | JJ Redick | +3.20 |
| 32 | Danilo Gallinari | +3.03 | +3.00 | 34 | +2 | Brandon Jennings | +3.20 |
| 33 | Darren Collison | +3.03 | +4.00 | 21 | -12 | Manu Ginobili | +3.20 |
| 34 | Andre Iguodala | +2.97 | +1.30 | 82 | +48 | Danilo Gallinari | +3.00 |
| 35 | Tyson Chandler | +2.89 | +2.60 | 48 | +13 | Lou Williams | +3.00 |
| 36 | Nikola Mirotic | +2.87 | +4.20 | 18 | -18 | Jimmy Butler | +3.00 |
| 37 | Jeff Teague | +2.80 | +2.70 | 46 | +9 | DeMarre Carroll | +2.90 |
| 38 | Isaiah Thomas | +2.79 | +1.60 | 72 | +34 | Eric Bledsoe | +2.90 |
| 39 | Marcus Smart | +2.79 | +2.10 | 59 | +20 | Mike Conley | +2.90 |
| 40 | JJ Redick | +2.57 | +3.20 | 31 | -9 | Zach Randolph | +2.90 |
| 41 | CJ Miles | +2.56 | +1.60 | 75 | +34 | Kelly Olynyk | +2.80 |
| 42 | Tim Duncan | +2.51 | +3.30 | 27 | -15 | Damian Lillard | +2.70 |
| 43 | Ersan Ilyasova | +2.40 | +2.50 | 49 | +6 | Jrue Holiday | +2.70 |
| 44 | Marc Gasol | +2.37 | +4.00 | 20 | -24 | Zaza Pachulia | +2.70 |
| 45 | Iman Shumpert | +2.36 | +1.90 | 66 | +21 | Anthony Morrow | +2.70 |
| 46 | Jae Crowder | +2.34 | +2.10 | 58 | +12 | Jeff Teague | +2.70 |
| 47 | Kevin Love | +2.28 | +3.20 | 30 | -17 | Serge Ibaka | +2.60 |
| 48 | Zach Randolph | +2.27 | +2.90 | 40 | -8 | Tyson Chandler | +2.60 |
| 49 | Devin Harris | +2.23 | +2.50 | 50 | +1 | Ersan Ilyasova | +2.50 |
| 50 | Patrick Patterson | +2.22 | +1.60 | 74 | +24 | Devin Harris | +2.50 |

### 2014-15 Regular season — total, Paine's top 30 (in-sample)

> 246 players covered &nbsp;·&nbsp; tau(true30) +0.595 &nbsp;·&nbsp; hits@30 22/30 &nbsp;·&nbsp; MAE 0.914

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +8.27 | +10.60 | 1 | +0 |
| 2 | Anthony Davis | +7.97 | +8.80 | 3 | +1 |
| 3 | Kawhi Leonard | +7.91 | +8.90 | 2 | -1 |
| 4 | LeBron James | +6.66 | +5.10 | 11 | +7 |
| 5 | James Harden | +6.55 | +7.70 | 4 | -1 |
| 6 | Russell Westbrook | +5.71 | +5.60 | 7 | +1 |
| 7 | Jimmy Butler | +5.58 | +3.00 | 36 | +29 |
| 8 | George Hill | +5.08 | +5.60 | 8 | +0 |
| 9 | Klay Thompson | +4.74 | +5.30 | 10 | +1 |
| 10 | Tony Allen | +4.57 | +4.30 | 16 | +6 |
| 11 | Draymond Green | +4.55 | +6.50 | 5 | -6 |
| 12 | DeAndre Jordan | +4.31 | +4.60 | 14 | +2 |
| 13 | Danny Green | +4.23 | +6.10 | 6 | -7 |
| 14 | Blake Griffin | +4.02 | +2.00 | 61 | +47 |
| 15 | Paul Millsap | +4.01 | +3.30 | 28 | +13 |
| 16 | Kyrie Irving | +4.00 | +4.60 | 13 | -3 |
| 17 | Khris Middleton | +3.91 | +4.80 | 12 | -5 |
| 18 | John Wall | +3.82 | +2.00 | 60 | +42 |
| 19 | Jeff Teague | +3.76 | +2.70 | 44 | +25 |
| 20 | Tim Duncan | +3.72 | +3.30 | 27 | +7 |
| 21 | Wesley Matthews | +3.64 | +3.60 | 25 | +4 |
| 22 | Lou Williams | +3.53 | +3.00 | 35 | +13 |
| 23 | Gordon Hayward | +3.29 | +3.40 | 26 | +3 |
| 24 | Al Horford | +3.23 | +2.00 | 63 | +39 |
| 25 | DeMarcus Cousins | +3.15 | +5.40 | 9 | -16 |
| 26 | Rudy Gobert | +3.08 | +4.10 | 19 | -7 |
| 27 | Damian Lillard | +3.06 | +2.70 | 45 | +18 |
| 28 | LaMarcus Aldridge | +2.97 | +4.30 | 17 | -11 |
| 29 | Brandan Wright | +2.85 | +1.70 | 70 | +41 |
| 30 | Kyle Korver | +2.83 | +4.60 | 15 | -15 |

## 2013-14 Regular season — offense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.811 &nbsp;·&nbsp; hits@30 23/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Kevin Durant | +6.76 | +7.60 | 1 | +0 | Kevin Durant | +7.60 |
| 2 | LeBron James | +6.61 | +5.80 | 4 | +2 | Chris Paul | +7.10 |
| 3 | Chris Paul | +6.07 | +7.10 | 2 | -1 | James Harden | +6.30 |
| 4 | James Harden | +5.76 | +6.30 | 3 | -1 | LeBron James | +5.80 |
| 5 | Kevin Love | +4.77 | +5.70 | 5 | +0 | Kevin Love | +5.70 |
| 6 | Goran Dragic | +4.55 | +4.80 | 6 | +0 | Goran Dragic | +4.80 |
| 7 | Damian Lillard | +4.21 | +3.60 | 12 | +5 | Kyle Lowry | +4.40 |
| 8 | Kyle Lowry | +4.06 | +4.40 | 7 | -1 | Dirk Nowitzki | +4.40 |
| 9 | Manu Ginobili | +4.02 | +4.00 | 10 | +1 | Carmelo Anthony | +4.20 |
| 10 | Russell Westbrook | +3.74 | +3.30 | 15 | +5 | Manu Ginobili | +4.00 |
| 11 | Carmelo Anthony | +3.50 | +4.20 | 9 | -2 | Patty Mills | +3.90 |
| 12 | Isaiah Thomas | +3.39 | +3.50 | 14 | +2 | Damian Lillard | +3.60 |
| 13 | Dirk Nowitzki | +3.17 | +4.40 | 8 | -5 | Mike Conley | +3.50 |
| 14 | Patty Mills | +2.94 | +3.90 | 11 | -3 | Isaiah Thomas | +3.50 |
| 15 | Mike Conley | +2.89 | +3.50 | 13 | -2 | Russell Westbrook | +3.30 |
| 16 | Blake Griffin | +2.84 | +2.90 | 17 | +1 | Ty Lawson | +3.20 |
| 17 | Paul George | +2.61 | +2.60 | 22 | +5 | Blake Griffin | +2.90 |
| 18 | DJ Augustin | +2.44 | +2.10 | 32 | +14 | Marco Belinelli | +2.80 |
| 19 | Ty Lawson | +2.33 | +3.20 | 16 | -3 | Jamal Crawford | +2.80 |
| 20 | Kyrie Irving | +2.30 | +2.30 | 27 | +7 | Wesley Matthews | +2.80 |
| 21 | Jamal Crawford | +2.21 | +2.80 | 19 | -2 | Joe Johnson | +2.70 |
| 22 | Deron Williams | +2.11 | +2.60 | 23 | +1 | Paul George | +2.60 |
| 23 | Wesley Matthews | +2.02 | +2.80 | 20 | -3 | Deron Williams | +2.60 |
| 24 | John Wall | +1.98 | +1.90 | 37 | +13 | Chandler Parsons | +2.60 |
| 25 | Brandan Wright | +1.95 | +1.70 | 41 | +16 | Nick Young | +2.40 |
| 26 | Joe Johnson | +1.90 | +2.70 | 21 | -5 | Vince Carter | +2.40 |
| 27 | Nikola Pekovic | +1.86 | +1.10 | 68 | +41 | Kyrie Irving | +2.30 |
| 28 | Pablo Prigioni | +1.81 | +1.70 | 43 | +15 | Jrue Holiday | +2.20 |
| 29 | Andre Iguodala | +1.73 | +1.20 | 62 | +33 | Patrick Beverley | +2.20 |
| 30 | JR Smith | +1.66 | +1.80 | 40 | +10 | Brandon Jennings | +2.20 |
| 31 | Chandler Parsons | +1.64 | +2.60 | 24 | -7 | Randy Foye | +2.10 |
| 32 | Marco Belinelli | +1.62 | +2.80 | 18 | -14 | DJ Augustin | +2.10 |
| 33 | Ricky Rubio | +1.61 | +1.90 | 39 | +6 | Klay Thompson | +2.10 |
| 34 | Rudy Gay | +1.59 | +1.10 | 66 | +32 | Josh McRoberts | +2.00 |
| 35 | Kevin Martin | +1.59 | +0.70 | 87 | +52 | Channing Frye | +2.00 |
| 36 | Kawhi Leonard | +1.57 | +1.70 | 44 | +8 | Kyle Korver | +1.90 |
| 37 | Nick Young | +1.51 | +2.40 | 25 | -12 | John Wall | +1.90 |
| 38 | Jrue Holiday | +1.46 | +2.20 | 28 | -10 | Nicolas Batum | +1.90 |
| 39 | Patrick Beverley | +1.46 | +2.20 | 29 | -10 | Ricky Rubio | +1.90 |
| 40 | Darren Collison | +1.38 | +1.10 | 65 | +25 | JR Smith | +1.80 |
| 41 | Randy Foye | +1.37 | +2.10 | 31 | -10 | Brandan Wright | +1.70 |
| 42 | Josh McRoberts | +1.32 | +2.00 | 34 | -8 | DeMar DeRozan | +1.70 |
| 43 | Kyle Korver | +1.30 | +1.90 | 36 | -7 | Pablo Prigioni | +1.70 |
| 44 | Klay Thompson | +1.26 | +2.10 | 33 | -11 | Kawhi Leonard | +1.70 |
| 45 | Brandon Knight | +1.25 | +0.90 | 78 | +33 | DeMarcus Cousins | +1.70 |
| 46 | Trevor Ariza | +1.24 | +1.00 | 69 | +23 | Mirza Teletovic | +1.60 |
| 47 | Vince Carter | +1.23 | +2.40 | 26 | -21 | Jose Calderon | +1.60 |
| 48 | Mario Chalmers | +1.23 | +1.50 | 50 | +2 | Eric Bledsoe | +1.50 |
| 49 | George Hill | +1.21 | +0.30 | 120 | +71 | Dwyane Wade | +1.50 |
| 50 | Nicolas Batum | +1.20 | +1.90 | 38 | -12 | Mario Chalmers | +1.50 |

### 2013-14 Regular season — offense, Paine's top 30 (in-sample)

> 246 players covered &nbsp;·&nbsp; tau(true30) +0.811 &nbsp;·&nbsp; hits@30 22/30 &nbsp;·&nbsp; MAE 0.713

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Kevin Durant | +7.41 | +7.60 | 1 | +0 |
| 2 | LeBron James | +6.80 | +5.80 | 4 | +2 |
| 3 | Chris Paul | +6.79 | +7.10 | 2 | -1 |
| 4 | James Harden | +5.29 | +6.30 | 3 | -1 |
| 5 | Goran Dragic | +4.87 | +4.80 | 6 | +1 |
| 6 | Kevin Love | +4.76 | +5.70 | 5 | -1 |
| 7 | Dirk Nowitzki | +4.33 | +4.40 | 7 | +0 |
| 8 | Brandan Wright | +4.19 | +1.70 | 42 | +34 |
| 9 | Carmelo Anthony | +3.80 | +4.20 | 9 | +0 |
| 10 | Kyle Lowry | +3.71 | +4.40 | 8 | -2 |
| 11 | Blake Griffin | +3.57 | +2.90 | 17 | +6 |
| 12 | Isaiah Thomas | +3.48 | +3.50 | 13 | +1 |
| 13 | Manu Ginobili | +3.25 | +4.00 | 10 | -3 |
| 14 | Russell Westbrook | +3.24 | +3.30 | 15 | +1 |
| 15 | Nikola Pekovic | +3.10 | +1.10 | 64 | +49 |
| 16 | Damian Lillard | +3.03 | +3.60 | 12 | -4 |
| 17 | Dwyane Wade | +2.82 | +1.50 | 50 | +33 |
| 18 | Ty Lawson | +2.80 | +3.20 | 16 | -2 |
| 19 | Mike Conley | +2.80 | +3.50 | 14 | -5 |
| 20 | Wesley Matthews | +2.68 | +2.80 | 19 | -1 |
| 21 | Patty Mills | +2.59 | +3.90 | 11 | -10 |
| 22 | Anthony Davis | +2.52 | +1.20 | 61 | +39 |
| 23 | Kawhi Leonard | +2.51 | +1.70 | 44 | +21 |
| 24 | Chandler Parsons | +2.47 | +2.60 | 23 | -1 |
| 25 | Paul George | +2.46 | +2.60 | 22 | -3 |
| 26 | Deron Williams | +2.34 | +2.60 | 24 | -2 |
| 27 | DeMar DeRozan | +2.30 | +1.70 | 41 | +14 |
| 28 | Jamal Crawford | +2.28 | +2.80 | 20 | -8 |
| 29 | Tony Parker | +2.17 | +0.90 | 76 | +47 |
| 30 | DeMarcus Cousins | +2.15 | +1.70 | 43 | +13 |

## 2014-15 Regular season — offense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.660 &nbsp;·&nbsp; hits@30 25/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.14 | +8.50 | 1 | +0 | Chris Paul | +8.50 |
| 2 | James Harden | +6.89 | +7.70 | 2 | +0 | James Harden | +7.70 |
| 3 | Russell Westbrook | +6.19 | +6.10 | 3 | +0 | Russell Westbrook | +6.10 |
| 4 | LeBron James | +5.53 | +5.30 | 5 | +1 | Kyrie Irving | +5.50 |
| 5 | Kyrie Irving | +5.18 | +5.50 | 4 | -1 | LeBron James | +5.30 |
| 6 | Isaiah Thomas | +4.78 | +4.50 | 8 | +2 | Lou Williams | +5.20 |
| 7 | Lou Williams | +4.35 | +5.20 | 6 | -1 | Kyle Korver | +4.60 |
| 8 | Damian Lillard | +4.21 | +4.00 | 11 | +3 | Isaiah Thomas | +4.50 |
| 9 | Klay Thompson | +3.86 | +4.30 | 10 | +1 | Anthony Davis | +4.30 |
| 10 | Blake Griffin | +3.50 | +3.20 | 22 | +12 | Klay Thompson | +4.30 |
| 11 | Anthony Davis | +3.27 | +4.30 | 9 | -2 | Damian Lillard | +4.00 |
| 12 | George Hill | +3.24 | +3.90 | 12 | +0 | George Hill | +3.90 |
| 13 | Jimmy Butler | +3.22 | +3.20 | 20 | +7 | Carmelo Anthony | +3.80 |
| 14 | Carmelo Anthony | +2.92 | +3.80 | 13 | -1 | Ty Lawson | +3.80 |
| 15 | Gordon Hayward | +2.83 | +3.20 | 21 | +6 | Kawhi Leonard | +3.70 |
| 16 | Mike Conley | +2.78 | +2.40 | 32 | +16 | Rudy Gay | +3.50 |
| 17 | JJ Redick | +2.72 | +2.50 | 29 | +12 | DeAndre Jordan | +3.40 |
| 18 | Jeff Teague | +2.69 | +2.20 | 35 | +17 | Kyle Lowry | +3.30 |
| 19 | Kyle Lowry | +2.61 | +3.30 | 18 | -1 | Jrue Holiday | +3.30 |
| 20 | Kyle Korver | +2.50 | +4.60 | 7 | -13 | Jimmy Butler | +3.20 |
| 21 | Kawhi Leonard | +2.50 | +3.70 | 15 | -6 | Gordon Hayward | +3.20 |
| 22 | Ty Lawson | +2.49 | +3.80 | 14 | -8 | Blake Griffin | +3.20 |
| 23 | Aaron Brooks | +2.45 | +1.60 | 56 | +33 | Brandon Jennings | +3.10 |
| 24 | Brandon Jennings | +2.40 | +3.10 | 23 | -1 | Danny Green | +3.10 |
| 25 | Rudy Gay | +2.38 | +3.50 | 16 | -9 | Danilo Gallinari | +2.80 |
| 26 | Jrue Holiday | +2.34 | +3.30 | 19 | -7 | Anthony Morrow | +2.70 |
| 27 | Dwyane Wade | +2.26 | +2.00 | 42 | +15 | Chandler Parsons | +2.60 |
| 28 | Dirk Nowitzki | +2.07 | +2.20 | 34 | +6 | Tyreke Evans | +2.60 |
| 29 | LaMarcus Aldridge | +2.07 | +2.40 | 30 | +1 | JJ Redick | +2.50 |
| 30 | Danilo Gallinari | +2.01 | +2.80 | 25 | -5 | LaMarcus Aldridge | +2.40 |
| 31 | Gerald Green | +1.99 | +2.20 | 36 | +5 | Patrick Patterson | +2.40 |
| 32 | John Wall | +1.86 | +2.10 | 39 | +7 | Mike Conley | +2.40 |
| 33 | Danny Green | +1.84 | +3.10 | 24 | -9 | Wesley Matthews | +2.30 |
| 34 | Reggie Jackson | +1.75 | +2.00 | 41 | +7 | Dirk Nowitzki | +2.20 |
| 35 | Darren Collison | +1.73 | +1.70 | 55 | +20 | Jeff Teague | +2.20 |
| 36 | Patrick Patterson | +1.70 | +2.40 | 31 | -5 | Gerald Green | +2.20 |
| 37 | Anthony Morrow | +1.70 | +2.70 | 26 | -11 | Devin Harris | +2.10 |
| 38 | Tyreke Evans | +1.69 | +2.60 | 28 | -10 | JR Smith | +2.10 |
| 39 | Khris Middleton | +1.67 | +1.70 | 51 | +12 | John Wall | +2.10 |
| 40 | Chandler Parsons | +1.55 | +2.60 | 27 | -13 | Ersan Ilyasova | +2.10 |
| 41 | Wesley Matthews | +1.52 | +2.30 | 33 | -8 | Reggie Jackson | +2.00 |
| 42 | Jamal Crawford | +1.41 | +1.10 | 69 | +27 | Dwyane Wade | +2.00 |
| 43 | Draymond Green | +1.39 | +1.50 | 58 | +15 | DeMarre Carroll | +1.90 |
| 44 | Marc Gasol | +1.38 | +1.40 | 60 | +16 | Nikola Mirotic | +1.90 |
| 45 | Manu Ginobili | +1.38 | +1.70 | 54 | +9 | Goran Dragic | +1.90 |
| 46 | Eric Gordon | +1.26 | +0.50 | 89 | +43 | JJ Barea | +1.90 |
| 47 | JJ Barea | +1.24 | +1.90 | 46 | -1 | Joe Johnson | +1.80 |
| 48 | Devin Harris | +1.23 | +2.10 | 37 | -11 | Luol Deng | +1.80 |
| 49 | Bradley Beal | +1.22 | +0.80 | 78 | +29 | Jae Crowder | +1.80 |
| 50 | Paul Millsap | +1.20 | +1.00 | 70 | +20 | Eric Bledsoe | +1.70 |

### 2014-15 Regular season — offense, Paine's top 30 (in-sample)

> 246 players covered &nbsp;·&nbsp; tau(true30) +0.490 &nbsp;·&nbsp; hits@30 23/30 &nbsp;·&nbsp; MAE 0.722

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +6.99 | +8.50 | 1 | +0 |
| 2 | James Harden | +5.71 | +7.70 | 2 | +0 |
| 3 | LeBron James | +5.62 | +5.30 | 5 | +2 |
| 4 | Anthony Davis | +5.11 | +4.30 | 9 | +5 |
| 5 | Russell Westbrook | +5.02 | +6.10 | 3 | -2 |
| 6 | Jimmy Butler | +4.38 | +3.20 | 20 | +14 |
| 7 | Blake Griffin | +4.19 | +3.20 | 22 | +15 |
| 8 | Kyrie Irving | +4.13 | +5.50 | 4 | -4 |
| 9 | Lou Williams | +4.08 | +5.20 | 6 | -3 |
| 10 | Klay Thompson | +4.00 | +4.30 | 10 | +0 |
| 11 | George Hill | +3.81 | +3.90 | 12 | +1 |
| 12 | Kawhi Leonard | +3.44 | +3.70 | 15 | +3 |
| 13 | JJ Redick | +3.36 | +2.50 | 29 | +16 |
| 14 | Ty Lawson | +3.18 | +3.80 | 14 | +0 |
| 15 | Gordon Hayward | +3.01 | +3.20 | 21 | +6 |
| 16 | Isaiah Thomas | +2.98 | +4.50 | 8 | -8 |
| 17 | Carmelo Anthony | +2.96 | +3.80 | 13 | -4 |
| 18 | Damian Lillard | +2.92 | +4.00 | 11 | -7 |
| 19 | Wesley Matthews | +2.78 | +2.30 | 33 | +14 |
| 20 | Brandon Jennings | +2.74 | +3.10 | 23 | +3 |
| 21 | Anthony Morrow | +2.73 | +2.70 | 26 | +5 |
| 22 | Jeff Teague | +2.72 | +2.20 | 35 | +13 |
| 23 | Rudy Gay | +2.65 | +3.50 | 16 | -7 |
| 24 | Kyle Lowry | +2.48 | +3.30 | 18 | -6 |
| 25 | Al Horford | +2.33 | +0.40 | 96 | +71 |
| 26 | LaMarcus Aldridge | +2.33 | +2.40 | 30 | +4 |
| 27 | Goran Dragic | +2.33 | +1.90 | 44 | +17 |
| 28 | Darren Collison | +2.31 | +1.70 | 54 | +26 |
| 29 | Dirk Nowitzki | +2.24 | +2.20 | 34 | +5 |
| 30 | Dwyane Wade | +2.22 | +2.00 | 41 | +11 |

## 2013-14 Regular season — defense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.375 &nbsp;·&nbsp; hits@30 21/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Andrew Bogut | +4.48 | +4.40 | 4 | +3 | Kawhi Leonard | +5.00 |
| 2 | Draymond Green | +4.14 | +4.60 | 2 | +0 | Draymond Green | +4.60 |
| 3 | Tiago Splitter | +3.88 | +4.20 | 6 | +3 | Joakim Noah | +4.50 |
| 4 | Paul George | +3.64 | +2.90 | 21 | +17 | Andrew Bogut | +4.40 |
| 5 | Kevin Garnett | +3.63 | +3.50 | 11 | +6 | Michael KiddGilchrist | +4.40 |
| 6 | Andre Iguodala | +3.63 | +2.60 | 25 | +19 | Tiago Splitter | +4.20 |
| 7 | Anderson Varejao | +3.33 | +3.60 | 10 | +3 | Danny Green | +4.00 |
| 8 | Kawhi Leonard | +3.29 | +5.00 | 1 | -7 | Chris Paul | +3.90 |
| 9 | Nene | +3.24 | +3.80 | 9 | +0 | Nene | +3.80 |
| 10 | Paul Pierce | +3.08 | +1.80 | 50 | +40 | Anderson Varejao | +3.60 |
| 11 | Danny Green | +3.06 | +4.00 | 7 | -4 | Kevin Garnett | +3.50 |
| 12 | Iman Shumpert | +3.02 | +1.00 | 73 | +61 | Nick Calathes | +3.50 |
| 13 | CJ Watson | +2.98 | +3.20 | 17 | +4 | Ian Mahinmi | +3.50 |
| 14 | Joakim Noah | +2.94 | +4.50 | 3 | -11 | Jimmy Butler | +3.40 |
| 15 | Jae Crowder | +2.92 | +3.00 | 19 | +4 | Roy Hibbert | +3.40 |
| 16 | Tony Allen | +2.88 | +2.40 | 31 | +15 | DeMarcus Cousins | +3.30 |
| 17 | Anthony Davis | +2.87 | +2.30 | 34 | +17 | CJ Watson | +3.20 |
| 18 | Paul Millsap | +2.85 | +2.70 | 23 | +5 | Tim Duncan | +3.00 |
| 19 | Derek Fisher | +2.74 | +2.00 | 43 | +24 | Jae Crowder | +3.00 |
| 20 | Ian Mahinmi | +2.62 | +3.50 | 13 | -7 | Kris Humphries | +3.00 |
| 21 | LaMarcus Aldridge | +2.60 | +2.20 | 37 | +16 | Paul George | +2.90 |
| 22 | Marcin Gortat | +2.59 | +2.50 | 29 | +7 | Marc Gasol | +2.80 |
| 23 | Gerald Wallace | +2.58 | +1.60 | 52 | +29 | Paul Millsap | +2.70 |
| 24 | Chris Bosh | +2.54 | +1.00 | 75 | +51 | Shane Battier | +2.70 |
| 25 | Jimmy Butler | +2.54 | +3.40 | 14 | -11 | Andre Iguodala | +2.60 |
| 26 | Roy Hibbert | +2.50 | +3.40 | 15 | -11 | DeMarre Carroll | +2.60 |
| 27 | DeMarre Carroll | +2.49 | +2.60 | 26 | -1 | Mario Chalmers | +2.50 |
| 28 | Tim Duncan | +2.44 | +3.00 | 18 | -10 | Samuel Dalembert | +2.50 |
| 29 | DeMarcus Cousins | +2.42 | +3.30 | 16 | -13 | Marcin Gortat | +2.50 |
| 30 | Al Jefferson | +2.35 | +1.80 | 47 | +17 | Victor Oladipo | +2.40 |
| 31 | Chris Paul | +2.26 | +3.90 | 8 | -23 | Tony Allen | +2.40 |
| 32 | Kirk Hinrich | +2.25 | +2.10 | 42 | +10 | Dwight Howard | +2.40 |
| 33 | David West | +2.24 | +1.50 | 57 | +24 | Serge Ibaka | +2.30 |
| 34 | Michael KiddGilchrist | +2.23 | +4.40 | 5 | -29 | Anthony Davis | +2.30 |
| 35 | Ersan Ilyasova | +2.18 | +0.70 | 90 | +55 | Kemba Walker | +2.30 |
| 36 | Thabo Sefolosha | +2.11 | +2.30 | 36 | +0 | Thabo Sefolosha | +2.30 |
| 37 | Darrell Arthur | +2.10 | +1.90 | 45 | +8 | LaMarcus Aldridge | +2.20 |
| 38 | Shaun Livingston | +2.07 | +0.90 | 77 | +39 | Nikola Pekovic | +2.20 |
| 39 | Kyle OQuinn | +2.04 | +1.60 | 53 | +14 | Eric Bledsoe | +2.20 |
| 40 | Nicolas Batum | +2.03 | +0.00 | 128 | +88 | George Hill | +2.10 |
| 41 | PJ Tucker | +2.03 | +0.90 | 82 | +41 | Kosta Koufos | +2.10 |
| 42 | Chris Andersen | +1.96 | +1.60 | 55 | +13 | Kirk Hinrich | +2.10 |
| 43 | Shane Battier | +1.95 | +2.70 | 24 | -19 | Derek Fisher | +2.00 |
| 44 | Robin Lopez | +1.90 | +2.00 | 44 | +0 | Robin Lopez | +2.00 |
| 45 | DeAndre Jordan | +1.89 | +1.10 | 65 | +20 | Darrell Arthur | +1.90 |
| 46 | Marc Gasol | +1.86 | +2.80 | 22 | -24 | Patrick Beverley | +1.90 |
| 47 | Amir Johnson | +1.86 | +1.20 | 63 | +16 | Al Jefferson | +1.80 |
| 48 | Manu Ginobili | +1.85 | +1.10 | 64 | +16 | Jeremy Lin | +1.80 |
| 49 | Kemba Walker | +1.82 | +2.30 | 35 | -14 | Ricky Rubio | +1.80 |
| 50 | Miles Plumlee | +1.81 | +1.60 | 56 | +6 | Paul Pierce | +1.80 |

### 2013-14 Regular season — defense, Paine's top 30 (in-sample)

> 246 players covered &nbsp;·&nbsp; tau(true30) +0.352 &nbsp;·&nbsp; hits@30 17/30 &nbsp;·&nbsp; MAE 0.925

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Andrew Bogut | +3.58 | +4.40 | 5 | +4 |
| 2 | Kawhi Leonard | +3.44 | +5.00 | 1 | -1 |
| 3 | Draymond Green | +3.32 | +4.60 | 2 | -1 |
| 4 | DeAndre Jordan | +3.19 | +1.10 | 70 | +66 |
| 5 | Jimmy Butler | +3.17 | +3.40 | 15 | +10 |
| 6 | Tony Allen | +3.07 | +2.40 | 31 | +25 |
| 7 | Joakim Noah | +3.05 | +4.50 | 3 | -4 |
| 8 | Danny Green | +2.82 | +4.00 | 7 | -1 |
| 9 | Ricky Rubio | +2.79 | +1.80 | 49 | +40 |
| 10 | Andre Iguodala | +2.69 | +2.60 | 26 | +16 |
| 11 | Paul George | +2.62 | +2.90 | 21 | +10 |
| 12 | Kyle OQuinn | +2.43 | +1.60 | 53 | +41 |
| 13 | Roy Hibbert | +2.43 | +3.40 | 14 | +1 |
| 14 | David West | +2.32 | +1.50 | 57 | +43 |
| 15 | Paul Millsap | +2.20 | +2.70 | 23 | +8 |
| 16 | Ian Mahinmi | +2.14 | +3.50 | 11 | -5 |
| 17 | Anderson Varejao | +2.08 | +3.60 | 10 | -7 |
| 18 | Tim Duncan | +2.05 | +3.00 | 19 | +1 |
| 19 | CJ Watson | +2.03 | +3.20 | 17 | -2 |
| 20 | Chris Paul | +2.00 | +3.90 | 8 | -12 |
| 21 | Bismack Biyombo | +1.84 | +1.10 | 66 | +45 |
| 22 | Kevin Garnett | +1.76 | +3.50 | 12 | -10 |
| 23 | Manu Ginobili | +1.74 | +1.10 | 69 | +46 |
| 24 | George Hill | +1.72 | +2.10 | 40 | +16 |
| 25 | Iman Shumpert | +1.70 | +1.00 | 74 | +49 |
| 26 | Serge Ibaka | +1.70 | +2.30 | 34 | +8 |
| 27 | Nick Calathes | +1.65 | +3.50 | 13 | -14 |
| 28 | Anthony Davis | +1.65 | +2.30 | 35 | +7 |
| 29 | Kirk Hinrich | +1.56 | +2.10 | 41 | +12 |
| 30 | Phil Pressey | +1.55 | +0.90 | 80 | +50 |

## 2014-15 Regular season — defense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.600 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Draymond Green | +5.05 | +5.10 | 2 | +1 | Kawhi Leonard | +5.20 |
| 2 | Rudy Gobert | +4.39 | +4.80 | 3 | +1 | Draymond Green | +5.10 |
| 3 | Tony Allen | +4.38 | +4.80 | 4 | +1 | Rudy Gobert | +4.80 |
| 4 | Andrew Bogut | +4.29 | +4.70 | 5 | +1 | Tony Allen | +4.80 |
| 5 | Kawhi Leonard | +3.85 | +5.20 | 1 | -4 | Andrew Bogut | +4.70 |
| 6 | Anthony Davis | +3.46 | +4.50 | 6 | +0 | Anthony Davis | +4.50 |
| 7 | Nerlens Noel | +3.38 | +2.70 | 19 | +12 | DeMarcus Cousins | +4.40 |
| 8 | Andre Roberson | +3.27 | +3.40 | 10 | +2 | Marcin Gortat | +3.60 |
| 9 | Nene | +3.13 | +2.80 | 18 | +9 | Tim Duncan | +3.50 |
| 10 | DeMarcus Cousins | +3.04 | +4.40 | 7 | -3 | Andre Roberson | +3.40 |
| 11 | Zaza Pachulia | +3.04 | +3.20 | 12 | +1 | Kosta Koufos | +3.30 |
| 12 | Michael KiddGilchrist | +3.03 | +3.00 | 14 | +2 | Zaza Pachulia | +3.20 |
| 13 | Iman Shumpert | +2.99 | +2.30 | 32 | +19 | Khris Middleton | +3.10 |
| 14 | AlFarouq Aminu | +2.80 | +2.60 | 24 | +10 | Michael KiddGilchrist | +3.00 |
| 15 | Khris Middleton | +2.75 | +3.10 | 13 | -2 | Serge Ibaka | +3.00 |
| 16 | Marcus Smart | +2.74 | +1.80 | 43 | +27 | Danny Green | +3.00 |
| 17 | Tim Duncan | +2.68 | +3.50 | 9 | -8 | Jonas Jerebko | +2.80 |
| 18 | Jared Dudley | +2.65 | +1.80 | 44 | +26 | Nene | +2.80 |
| 19 | Danny Green | +2.58 | +3.00 | 16 | -3 | Nerlens Noel | +2.70 |
| 20 | Kosta Koufos | +2.56 | +3.30 | 11 | -9 | Tyson Chandler | +2.60 |
| 21 | Josh Smith | +2.50 | +2.60 | 23 | +2 | Marc Gasol | +2.60 |
| 22 | Dwight Howard | +2.50 | +1.90 | 41 | +19 | Joakim Noah | +2.60 |
| 23 | Tyson Chandler | +2.46 | +2.60 | 20 | -3 | Josh Smith | +2.60 |
| 24 | Andre Iguodala | +2.43 | +1.60 | 47 | +23 | AlFarouq Aminu | +2.60 |
| 25 | Marcin Gortat | +2.35 | +3.60 | 8 | -17 | Alex Len | +2.50 |
| 26 | Greg Monroe | +2.35 | +0.20 | 111 | +85 | Paul Millsap | +2.40 |
| 27 | Jonas Jerebko | +2.34 | +2.80 | 17 | -10 | Omer Asik | +2.40 |
| 28 | Timofey Mozgov | +2.33 | +2.40 | 28 | +0 | Timofey Mozgov | +2.40 |
| 29 | Michael CarterWilliams | +2.29 | +2.30 | 31 | +2 | Darren Collison | +2.30 |
| 30 | Derrick Favors | +2.28 | +2.10 | 34 | +4 | Luc Mbah a Moute | +2.30 |
| 31 | Wesley Matthews | +2.15 | +1.30 | 56 | +25 | Michael CarterWilliams | +2.30 |
| 32 | Kelly Olynyk | +2.13 | +2.10 | 36 | +4 | Iman Shumpert | +2.30 |
| 33 | Nikola Mirotic | +2.12 | +2.20 | 33 | +0 | Nikola Mirotic | +2.20 |
| 34 | Manu Ginobili | +2.06 | +1.40 | 52 | +18 | Derrick Favors | +2.10 |
| 35 | Ersan Ilyasova | +2.04 | +0.40 | 100 | +65 | Chris Paul | +2.10 |
| 36 | Jimmy Butler | +2.00 | -0.20 | 140 | +104 | Kelly Olynyk | +2.10 |
| 37 | Paul Millsap | +1.99 | +2.40 | 26 | -11 | Cody Zeller | +2.10 |
| 38 | Pau Gasol | +1.94 | +0.50 | 90 | +52 | Roy Hibbert | +2.00 |
| 39 | Corey Brewer | +1.92 | +0.90 | 72 | +33 | Steven Adams | +2.00 |
| 40 | PJ Tucker | +1.91 | +1.30 | 54 | +14 | LaMarcus Aldridge | +1.90 |
| 41 | Mario Chalmers | +1.85 | +1.50 | 49 | +8 | Dwight Howard | +1.90 |
| 42 | Omer Asik | +1.81 | +2.40 | 27 | -15 | Pablo Prigioni | +1.80 |
| 43 | Brandan Wright | +1.78 | +1.30 | 58 | +15 | Marcus Smart | +1.80 |
| 44 | Zach Randolph | +1.78 | +1.30 | 59 | +15 | Jared Dudley | +1.80 |
| 45 | Joakim Noah | +1.77 | +2.60 | 22 | -23 | George Hill | +1.70 |
| 46 | Cory Joseph | +1.74 | +0.50 | 85 | +39 | Al Horford | +1.60 |
| 47 | John Henson | +1.73 | +0.50 | 88 | +41 | Andre Iguodala | +1.60 |
| 48 | Donatas Motiejunas | +1.70 | +0.80 | 78 | +30 | Kevin Love | +1.60 |
| 49 | Monta Ellis | +1.69 | +0.80 | 77 | +28 | Mario Chalmers | +1.50 |
| 50 | LeBron James | +1.67 | -0.10 | 134 | +84 | Kris Humphries | +1.50 |

### 2014-15 Regular season — defense, Paine's top 30 (in-sample)

> 246 players covered &nbsp;·&nbsp; tau(true30) +0.582 &nbsp;·&nbsp; hits@30 19/30 &nbsp;·&nbsp; MAE 0.846

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Kawhi Leonard | +4.47 | +5.20 | 1 | +0 |
| 2 | Tony Allen | +4.32 | +4.80 | 4 | +2 |
| 3 | Draymond Green | +3.75 | +5.10 | 2 | -1 |
| 4 | Andrew Bogut | +3.21 | +4.70 | 5 | +1 |
| 5 | DeAndre Jordan | +3.15 | +1.20 | 62 | +57 |
| 6 | Nerlens Noel | +2.89 | +2.70 | 19 | +13 |
| 7 | Anthony Davis | +2.86 | +4.50 | 6 | -1 |
| 8 | Rudy Gobert | +2.78 | +4.80 | 3 | -5 |
| 9 | Danny Green | +2.72 | +3.00 | 16 | +7 |
| 10 | AlFarouq Aminu | +2.59 | +2.60 | 22 | +12 |
| 11 | Tim Duncan | +2.49 | +3.50 | 9 | -2 |
| 12 | Paul Millsap | +2.35 | +2.40 | 27 | +15 |
| 13 | Khris Middleton | +2.07 | +3.10 | 13 | +0 |
| 14 | Andre Roberson | +2.05 | +3.40 | 10 | -4 |
| 15 | Bismack Biyombo | +2.05 | +0.30 | 105 | +90 |
| 16 | Marcus Smart | +1.98 | +1.80 | 42 | +26 |
| 17 | John Wall | +1.97 | +0.00 | 128 | +111 |
| 18 | DeMarcus Cousins | +1.95 | +4.40 | 7 | -11 |
| 19 | Marcin Gortat | +1.93 | +3.60 | 8 | -11 |
| 20 | Michael KiddGilchrist | +1.74 | +3.00 | 14 | -6 |
| 21 | John Henson | +1.64 | +0.50 | 87 | +66 |
| 22 | Nicolas Batum | +1.55 | -0.20 | 138 | +116 |
| 23 | Zaza Pachulia | +1.53 | +3.20 | 12 | -11 |
| 24 | Trevor Ariza | +1.51 | +0.30 | 108 | +84 |
| 25 | Andre Drummond | +1.50 | +0.40 | 100 | +75 |
| 26 | Kosta Koufos | +1.48 | +3.30 | 11 | -15 |
| 27 | Iman Shumpert | +1.40 | +2.30 | 30 | +3 |
| 28 | Cody Zeller | +1.40 | +2.10 | 35 | +7 |
| 29 | Kelly Olynyk | +1.40 | +2.10 | 34 | +5 |
| 30 | Jae Crowder | +1.36 | +0.30 | 110 | +80 |

## 2023-24 Regular season — total, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +10.89 | 2737 |
| 2 | Luka Doncic | +8.16 | 2624 |
| 3 | Shai Gilgeous-Alexander | +8.00 | 2553 |
| 4 | Joel Embiid | +7.81 | 1309 |
| 5 | Kawhi Leonard | +6.86 | 2330 |
| 6 | Paul George | +6.81 | 2502 |
| 7 | Donovan Mitchell | +6.09 | 1943 |
| 8 | Jalen Brunson | +6.04 | 2726 |
| 9 | Tyrese Haliburton | +5.90 | 2224 |
| 10 | Anthony Davis | +5.85 | 2700 |
| 11 | Giannis Antetokounmpo | +5.68 | 2567 |
| 12 | Kyrie Irving | +5.35 | 2030 |
| 13 | Jayson Tatum | +5.35 | 2645 |
| 14 | De'Aaron Fox | +4.89 | 2659 |
| 15 | Damian Lillard | +4.77 | 2579 |
| 16 | LeBron James | +4.71 | 2504 |
| 17 | Stephen Curry | +4.64 | 2421 |
| 18 | Kristaps Porzingis | +4.43 | 1690 |
| 19 | Jamal Murray | +4.29 | 1861 |
| 20 | Jimmy Butler | +4.13 | 2042 |
| 21 | Fred VanVleet | +4.11 | 2684 |
| 22 | Isaiah Hartenstein | +4.03 | 1896 |
| 23 | Derrick White | +3.96 | 2381 |
| 24 | Jusuf Nurkic | +3.90 | 2078 |
| 25 | James Harden | +3.89 | 2470 |
| 26 | Lauri Markkanen | +3.80 | 1820 |
| 27 | Alex Caruso | +3.79 | 2040 |
| 28 | Rudy Gobert | +3.78 | 2593 |
| 29 | Kevin Durant | +3.77 | 2791 |
| 30 | Devin Booker | +3.76 | 2447 |
| 31 | Alperen Sengun | +3.67 | 2046 |
| 32 | Chet Holmgren | +3.39 | 2413 |
| 33 | Tyrese Maxey | +3.24 | 2626 |
| 34 | T.J. McConnell | +3.21 | 1291 |
| 35 | Mike Conley | +3.16 | 2193 |
| 36 | Bogdan Bogdanovic | +3.11 | 2401 |
| 37 | Anthony Edwards | +3.07 | 2770 |
| 38 | Trey Murphy III | +2.99 | 1690 |
| 39 | Victor Wembanyama | +2.98 | 2106 |
| 40 | Jarrett Allen | +2.87 | 2442 |
| 41 | Donte DiVincenzo | +2.75 | 2360 |
| 42 | Draymond Green | +2.60 | 1490 |
| 43 | Isaiah Joe | +2.58 | 1445 |
| 44 | Karl-Anthony Towns | +2.55 | 2026 |
| 45 | Andre Drummond | +2.49 | 1351 |
| 46 | Sam Hauser | +2.49 | 1741 |
| 47 | Moses Moody | +2.44 | 1156 |
| 48 | Jalen Williams | +2.43 | 2223 |
| 49 | Franz Wagner | +2.42 | 2337 |
| 50 | Brandin Podziemski | +2.40 | 1968 |

## 2024-25 Regular season — total, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +10.30 | 2571 |
| 2 | Shai Gilgeous-Alexander | +8.42 | 2598 |
| 3 | Luka Dončić | +7.57 | 1769 |
| 4 | Giannis Antetokounmpo | +5.98 | 2289 |
| 5 | Ivica Zubac | +5.57 | 2624 |
| 6 | Jayson Tatum | +5.50 | 2624 |
| 7 | Tyrese Haliburton | +5.45 | 2451 |
| 8 | Stephen Curry | +5.45 | 2252 |
| 9 | Donovan Mitchell | +5.42 | 2232 |
| 10 | Victor Wembanyama | +5.18 | 1527 |
| 11 | Luke Kornet | +5.16 | 1361 |
| 12 | Darius Garland | +5.15 | 2301 |
| 13 | James Harden | +4.92 | 2789 |
| 14 | Alperen Sengun | +4.75 | 2394 |
| 15 | Jimmy Butler | +4.74 | 1746 |
| 16 | Ty Jerome | +4.65 | 1393 |
| 17 | Rudy Gobert | +4.47 | 2388 |
| 18 | Derrick White | +4.32 | 2574 |
| 19 | Kawhi Leonard | +4.20 | 1180 |
| 20 | Karl-Anthony Towns | +3.83 | 2517 |
| 21 | Jarrett Allen | +3.81 | 2296 |
| 22 | Franz Wagner | +3.79 | 2023 |
| 23 | Daniel Gafford | +3.77 | 1226 |
| 24 | Tyler Herro | +3.75 | 2725 |
| 25 | Kyrie Irving | +3.74 | 1804 |
| 26 | Austin Reaves | +3.70 | 2550 |
| 27 | Kristaps Porziņģis | +3.53 | 1210 |
| 28 | Norman Powell | +3.51 | 1958 |
| 29 | Evan Mobley | +3.50 | 2167 |
| 30 | Luguentz Dort | +3.42 | 2073 |
| 31 | Payton Pritchard | +3.40 | 2271 |
| 32 | Anthony Edwards | +3.39 | 2871 |
| 33 | Jaren Jackson Jr. | +3.38 | 2207 |
| 34 | Jamal Murray | +3.32 | 2418 |
| 35 | Brandin Podziemski | +3.28 | 1716 |
| 36 | Isaiah Joe | +3.26 | 1604 |
| 37 | Anthony Davis | +3.20 | 1706 |
| 38 | Jalen Brunson | +3.12 | 2301 |
| 39 | Jalen Williams | +3.12 | 2237 |
| 40 | Damian Lillard | +2.97 | 2093 |
| 41 | Keon Ellis | +2.85 | 1948 |
| 42 | Domantas Sabonis | +2.81 | 2429 |
| 43 | Amen Thompson | +2.79 | 2225 |
| 44 | Pascal Siakam | +2.72 | 2548 |
| 45 | Cason Wallace | +2.64 | 1876 |
| 46 | Ausar Thompson | +2.62 | 1328 |
| 47 | Isaiah Hartenstein | +2.60 | 1590 |
| 48 | Jaylen Brown | +2.57 | 2158 |
| 49 | Donte DiVincenzo | +2.52 | 1606 |
| 50 | Ja Morant | +2.48 | 1519 |

## 2025-26 Regular season — total, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.86 | 2265 |
| 2 | Victor Wembanyama | +8.88 | 1866 |
| 3 | Shai Gilgeous-Alexander | +7.97 | 2259 |
| 4 | Kawhi Leonard | +7.75 | 2085 |
| 5 | Luka Dončić | +7.50 | 2289 |
| 6 | Donovan Mitchell | +5.79 | 2342 |
| 7 | LaMelo Ball | +5.78 | 2017 |
| 8 | Jimmy Butler III | +5.71 | 1182 |
| 9 | Chet Holmgren | +5.64 | 1997 |
| 10 | Jamal Murray | +5.53 | 2652 |
| 11 | Cade Cunningham | +5.36 | 2172 |
| 12 | Derrick White | +5.21 | 2625 |
| 13 | Stephen Curry | +5.01 | 1329 |
| 14 | Collin Gillespie | +4.81 | 2282 |
| 15 | Tyrese Maxey | +4.73 | 2661 |
| 16 | Jalen Duren | +4.72 | 1976 |
| 17 | Brandon Miller | +4.57 | 1968 |
| 18 | Neemias Queta | +4.32 | 1926 |
| 19 | Paul George | +4.31 | 1135 |
| 20 | Ajay Mitchell | +4.28 | 1473 |
| 21 | Austin Reaves | +4.10 | 1762 |
| 22 | Jalen Brunson | +4.08 | 2590 |
| 23 | Isaiah Joe | +3.81 | 1507 |
| 24 | Joel Embiid | +3.80 | 1201 |
| 25 | Dyson Daniels | +3.78 | 2520 |
| 26 | James Harden | +3.67 | 2438 |
| 27 | Isaiah Hartenstein | +3.63 | 1137 |
| 28 | Jrue Holiday | +3.60 | 1560 |
| 29 | Anthony Edwards | +3.59 | 2137 |
| 30 | Kevin Durant | +3.51 | 2840 |
| 31 | Scottie Barnes | +3.50 | 2681 |
| 32 | Nickeil Alexander-Walker | +3.47 | 2603 |
| 33 | Jalen Suggs | +3.36 | 1574 |
| 34 | Karl-Anthony Towns | +3.28 | 2322 |
| 35 | Deni Avdija | +3.26 | 2199 |
| 36 | Reed Sheppard | +3.18 | 2147 |
| 37 | Rudy Gobert | +3.15 | 2380 |
| 38 | Cason Wallace | +3.13 | 2046 |
| 39 | Donte DiVincenzo | +3.13 | 2494 |
| 40 | Devin Booker | +3.06 | 2146 |
| 41 | Jordan Goodwin | +3.05 | 1572 |
| 42 | Jarrett Allen | +3.00 | 1519 |
| 43 | Bam Adebayo | +2.98 | 2365 |
| 44 | Donovan Clingan | +2.96 | 2094 |
| 45 | Moussa Diabaté | +2.96 | 1899 |
| 46 | Toumani Camara | +2.86 | 2731 |
| 47 | Jaylen Brown | +2.86 | 2443 |
| 48 | Ausar Thompson | +2.84 | 1896 |
| 49 | Kon Knueppel | +2.72 | 2551 |
| 50 | De'Aaron Fox | +2.71 | 2231 |

## 2023-24 Regular season — offense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +8.26 | 2737 |
| 2 | Luka Doncic | +8.02 | 2624 |
| 3 | Shai Gilgeous-Alexander | +6.79 | 2553 |
| 4 | Jalen Brunson | +6.69 | 2726 |
| 5 | Tyrese Haliburton | +6.27 | 2224 |
| 6 | Stephen Curry | +5.84 | 2421 |
| 7 | Donovan Mitchell | +5.82 | 1943 |
| 8 | LeBron James | +5.71 | 2504 |
| 9 | Devin Booker | +5.29 | 2447 |
| 10 | Trae Young | +5.14 | 1942 |
| 11 | Giannis Antetokounmpo | +5.10 | 2567 |
| 12 | Damian Lillard | +5.04 | 2579 |
| 13 | Jamal Murray | +4.81 | 1861 |
| 14 | James Harden | +4.80 | 2470 |
| 15 | Jayson Tatum | +4.78 | 2645 |
| 16 | Kyrie Irving | +4.70 | 2030 |
| 17 | Joel Embiid | +4.51 | 1309 |
| 18 | Tyrese Maxey | +4.39 | 2626 |
| 19 | De'Aaron Fox | +4.16 | 2659 |
| 20 | Anthony Edwards | +4.16 | 2770 |
| 21 | Kawhi Leonard | +3.79 | 2330 |
| 22 | Paul George | +3.71 | 2502 |
| 23 | Lauri Markkanen | +3.64 | 1820 |
| 24 | Collin Sexton | +3.47 | 2075 |
| 25 | Fred VanVleet | +3.47 | 2684 |
| 26 | Kevin Durant | +3.45 | 2791 |
| 27 | Jimmy Butler | +3.40 | 2042 |
| 28 | T.J. McConnell | +3.23 | 1291 |
| 29 | Desmond Bane | +3.22 | 1443 |
| 30 | DeMar DeRozan | +3.19 | 2989 |
| 31 | CJ McCollum | +3.13 | 2159 |
| 32 | Payton Pritchard | +3.09 | 1826 |
| 33 | D'Angelo Russell | +3.03 | 2484 |
| 34 | Malcolm Brogdon | +2.92 | 1121 |
| 35 | Donte DiVincenzo | +2.91 | 2360 |
| 36 | Dejounte Murray | +2.75 | 2783 |
| 37 | Julius Randle | +2.67 | 1630 |
| 38 | Anthony Davis | +2.65 | 2700 |
| 39 | Mike Conley | +2.58 | 2193 |
| 40 | Anfernee Simons | +2.50 | 1582 |
| 41 | Malik Monk | +2.46 | 1872 |
| 42 | Zion Williamson | +2.40 | 2207 |
| 43 | Derrick White | +2.39 | 2381 |
| 44 | Pascal Siakam | +2.36 | 2658 |
| 45 | Immanuel Quickley | +2.25 | 1985 |
| 46 | Terry Rozier | +2.21 | 2040 |
| 47 | Jalen Williams | +2.20 | 2223 |
| 48 | Bogdan Bogdanovic | +2.18 | 2401 |
| 49 | Brandon Ingram | +2.11 | 2103 |
| 50 | Khris Middleton | +2.11 | 1487 |

## 2024-25 Regular season — offense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.03 | 2571 |
| 2 | Shai Gilgeous-Alexander | +7.58 | 2598 |
| 3 | Stephen Curry | +6.71 | 2252 |
| 4 | Luka Dončić | +6.18 | 1769 |
| 5 | LaMelo Ball | +6.05 | 1505 |
| 6 | Jalen Brunson | +5.56 | 2301 |
| 7 | Tyrese Haliburton | +5.53 | 2451 |
| 8 | Giannis Antetokounmpo | +5.39 | 2289 |
| 9 | Jayson Tatum | +5.33 | 2624 |
| 10 | Donovan Mitchell | +5.27 | 2232 |
| 11 | Damian Lillard | +5.17 | 2093 |
| 12 | James Harden | +5.12 | 2789 |
| 13 | Trae Young | +4.83 | 2739 |
| 14 | Darius Garland | +4.81 | 2301 |
| 15 | Ty Jerome | +4.75 | 1393 |
| 16 | Cade Cunningham | +4.51 | 2452 |
| 17 | Anthony Edwards | +4.20 | 2871 |
| 18 | Tyler Herro | +4.06 | 2725 |
| 19 | Jamal Murray | +3.92 | 2418 |
| 20 | Austin Reaves | +3.51 | 2550 |
| 21 | Ja Morant | +3.48 | 1519 |
| 22 | Jimmy Butler | +3.37 | 1746 |
| 23 | Payton Pritchard | +3.32 | 2271 |
| 24 | Tyrese Maxey | +3.31 | 1960 |
| 25 | Kyrie Irving | +3.17 | 1804 |
| 26 | Devin Booker | +3.13 | 2795 |
| 27 | Karl-Anthony Towns | +3.08 | 2517 |
| 28 | LeBron James | +2.90 | 2444 |
| 29 | Franz Wagner | +2.80 | 2023 |
| 30 | Kevin Durant | +2.67 | 2265 |
| 31 | Norman Powell | +2.63 | 1958 |
| 32 | Christian Braun | +2.61 | 2675 |
| 33 | Cameron Johnson | +2.39 | 1800 |
| 34 | Isaiah Joe | +2.31 | 1604 |
| 35 | Domantas Sabonis | +2.27 | 2429 |
| 36 | Derrick White | +2.26 | 2574 |
| 37 | Kawhi Leonard | +2.21 | 1180 |
| 38 | Paolo Banchero | +2.19 | 1582 |
| 39 | Jaylen Brown | +2.17 | 2158 |
| 40 | Aaron Gordon | +2.16 | 1447 |
| 41 | Jalen Green | +2.11 | 2697 |
| 42 | DeMar DeRozan | +2.11 | 2768 |
| 43 | Collin Sexton | +2.10 | 1758 |
| 44 | Desmond Bane | +2.05 | 2205 |
| 45 | Malik Beasley | +1.98 | 2283 |
| 46 | CJ McCollum | +1.95 | 1832 |
| 47 | Deni Avdija | +1.88 | 2161 |
| 48 | Zach LaVine | +1.87 | 2603 |
| 49 | Cameron Payne | +1.82 | 1090 |
| 50 | Daniel Gafford | +1.79 | 1226 |

## 2025-26 Regular season — offense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +8.53 | 2265 |
| 2 | Shai Gilgeous-Alexander | +7.56 | 2259 |
| 3 | Luka Dončić | +7.33 | 2289 |
| 4 | Donovan Mitchell | +6.45 | 2342 |
| 5 | Kawhi Leonard | +6.18 | 2085 |
| 6 | Jamal Murray | +5.85 | 2652 |
| 7 | James Harden | +5.80 | 2438 |
| 8 | LaMelo Ball | +5.66 | 2017 |
| 9 | Jalen Brunson | +5.63 | 2590 |
| 10 | Stephen Curry | +5.61 | 1329 |
| 11 | Cade Cunningham | +5.06 | 2172 |
| 12 | Tyrese Maxey | +5.03 | 2661 |
| 13 | Jimmy Butler III | +4.47 | 1182 |
| 14 | Anthony Edwards | +4.37 | 2137 |
| 15 | Devin Booker | +4.30 | 2146 |
| 16 | Deni Avdija | +4.15 | 2199 |
| 17 | Austin Reaves | +3.62 | 1762 |
| 18 | Kevin Durant | +3.57 | 2840 |
| 19 | Victor Wembanyama | +3.47 | 1866 |
| 20 | Coby White | +3.40 | 1250 |
| 21 | Joel Embiid | +3.38 | 1201 |
| 22 | Payton Pritchard | +3.35 | 2556 |
| 23 | Jaylen Brown | +3.34 | 2443 |
| 24 | Michael Porter Jr. | +3.33 | 1689 |
| 25 | Jrue Holiday | +3.26 | 1560 |
| 26 | Jalen Duren | +3.19 | 1976 |
| 27 | Cam Spencer | +2.98 | 1714 |
| 28 | Keyonte George | +2.98 | 1786 |
| 29 | Kon Knueppel | +2.96 | 2551 |
| 30 | Duncan Robinson | +2.94 | 2113 |
| 31 | Collin Gillespie | +2.88 | 2282 |
| 32 | Darius Garland | +2.86 | 1344 |
| 33 | Lauri Markkanen | +2.80 | 1443 |
| 34 | De'Aaron Fox | +2.76 | 2231 |
| 35 | Julius Randle | +2.74 | 2610 |
| 36 | Alperen Sengun | +2.68 | 2398 |
| 37 | Brandon Miller | +2.50 | 1968 |
| 38 | Ryan Rollins | +2.48 | 2375 |
| 39 | Trey Murphy III | +2.47 | 2341 |
| 40 | CJ McCollum | +2.46 | 2263 |
| 41 | Isaiah Joe | +2.46 | 1507 |
| 42 | Nickeil Alexander-Walker | +2.42 | 2603 |
| 43 | Anfernee Simons | +2.40 | 1372 |
| 44 | Reed Sheppard | +2.39 | 2147 |
| 45 | Grayson Allen | +2.32 | 1467 |
| 46 | Immanuel Quickley | +2.31 | 2231 |
| 47 | Sam Merrill | +2.30 | 1377 |
| 48 | Bones Hyland | +2.29 | 1177 |
| 49 | Miles McBride | +2.27 | 1080 |
| 50 | Luka Garza | +2.23 | 1118 |

## 2023-24 Regular season — defense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +3.93 | 2593 |
| 2 | Isaiah Hartenstein | +3.22 | 1896 |
| 3 | Dean Wade | +3.21 | 1108 |
| 4 | Alex Caruso | +3.13 | 2040 |
| 5 | Kristaps Porzingis | +3.07 | 1690 |
| 6 | Joel Embiid | +3.00 | 1309 |
| 7 | Jusuf Nurkic | +2.84 | 2078 |
| 8 | Victor Wembanyama | +2.75 | 2106 |
| 9 | Nikola Jokic | +2.52 | 2737 |
| 10 | Chet Holmgren | +2.45 | 2413 |
| 11 | Andre Drummond | +2.41 | 1351 |
| 12 | Brook Lopez | +2.39 | 2411 |
| 13 | Matisse Thybulle | +2.39 | 1487 |
| 14 | Nic Claxton | +2.38 | 2116 |
| 15 | Larry Nance Jr. | +2.37 | 1216 |
| 16 | Evan Mobley | +2.35 | 1532 |
| 17 | Draymond Green | +2.34 | 1490 |
| 18 | Anthony Davis | +2.26 | 2700 |
| 19 | Amen Thompson | +2.14 | 1388 |
| 20 | Paul Reed | +2.10 | 1590 |
| 21 | Toumani Camara | +2.04 | 1739 |
| 22 | Kawhi Leonard | +2.03 | 2330 |
| 23 | Paul George | +2.01 | 2502 |
| 24 | Derrick Jones Jr. | +2.00 | 1783 |
| 25 | Derrick White | +1.99 | 2381 |
| 26 | Ausar Thompson | +1.99 | 1583 |
| 27 | Ivica Zubac | +1.95 | 1795 |
| 28 | Clint Capela | +1.95 | 1883 |
| 29 | Walker Kessler | +1.93 | 1493 |
| 30 | OG Anunoby | +1.89 | 1702 |
| 31 | Franz Wagner | +1.86 | 2337 |
| 32 | Wendell Carter Jr. | +1.82 | 1406 |
| 33 | Isaiah Joe | +1.82 | 1445 |
| 34 | Vince Williams Jr. | +1.80 | 1436 |
| 35 | Nickeil Alexander-Walker | +1.80 | 1921 |
| 36 | Jarrett Allen | +1.79 | 2442 |
| 37 | Aaron Nesmith | +1.78 | 1995 |
| 38 | Myles Turner | +1.76 | 2077 |
| 39 | Dyson Daniels | +1.69 | 1358 |
| 40 | Daniel Gafford | +1.65 | 1814 |
| 41 | Jakob Poeltl | +1.64 | 1319 |
| 42 | Luguentz Dort | +1.58 | 2246 |
| 43 | Bam Adebayo | +1.56 | 2416 |
| 44 | Jalen Suggs | +1.53 | 2025 |
| 45 | Herbert Jones | +1.51 | 2321 |
| 46 | Naz Reid | +1.50 | 1964 |
| 47 | Shai Gilgeous-Alexander | +1.38 | 2553 |
| 48 | Sam Hauser | +1.36 | 1741 |
| 49 | Kyle Anderson | +1.34 | 1782 |
| 50 | Trey Murphy III | +1.32 | 1690 |

## 2024-25 Regular season — defense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.21 | 2388 |
| 2 | Victor Wembanyama | +3.55 | 1527 |
| 3 | Luke Kornet | +3.36 | 1361 |
| 4 | Toumani Camara | +3.17 | 2548 |
| 5 | Alperen Sengun | +3.16 | 2394 |
| 6 | Ivica Zubac | +3.15 | 2624 |
| 7 | Ausar Thompson | +2.89 | 1328 |
| 8 | Isaiah Hartenstein | +2.82 | 1590 |
| 9 | Jarrett Allen | +2.77 | 2296 |
| 10 | Kris Dunn | +2.75 | 1783 |
| 11 | Luguentz Dort | +2.66 | 2073 |
| 12 | Jaxson Hayes | +2.63 | 1093 |
| 13 | Donovan Clingan | +2.63 | 1324 |
| 14 | Kristaps Porziņģis | +2.61 | 1210 |
| 15 | Evan Mobley | +2.54 | 2167 |
| 16 | Dyson Daniels | +2.54 | 2571 |
| 17 | Draymond Green | +2.38 | 1983 |
| 18 | Nicolas Batum | +2.28 | 1367 |
| 19 | Brandon Clarke | +2.27 | 1207 |
| 20 | Brandin Podziemski | +2.25 | 1716 |
| 21 | Isaiah Stewart | +2.23 | 1434 |
| 22 | Kevon Looney | +2.12 | 1142 |
| 23 | Daniel Gafford | +2.11 | 1226 |
| 24 | Amen Thompson | +2.06 | 2225 |
| 25 | Jaden McDaniels | +1.98 | 2614 |
| 26 | Jonathan Isaac | +1.94 | 1090 |
| 27 | Kentavious Caldwell-Pope | +1.93 | 2279 |
| 28 | Walker Kessler | +1.83 | 1740 |
| 29 | Jaren Jackson Jr. | +1.78 | 2207 |
| 30 | Sam Merrill | +1.76 | 1401 |
| 31 | Cody Martin | +1.72 | 1173 |
| 32 | Keon Ellis | +1.71 | 1948 |
| 33 | Anthony Davis | +1.71 | 1706 |
| 34 | Myles Turner | +1.65 | 2174 |
| 35 | Cason Wallace | +1.64 | 1876 |
| 36 | Wendell Carter Jr. | +1.61 | 1758 |
| 37 | Mike Conley | +1.59 | 1756 |
| 38 | Dean Wade | +1.59 | 1252 |
| 39 | Donte DiVincenzo | +1.57 | 1606 |
| 40 | Scotty Pippen Jr. | +1.57 | 1683 |
| 41 | Goga Bitadze | +1.56 | 1430 |
| 42 | Shai Gilgeous-Alexander | +1.55 | 2598 |
| 43 | Derrick White | +1.50 | 2574 |
| 44 | Kenrich Williams | +1.50 | 1132 |
| 45 | Mason Plumlee | +1.47 | 1300 |
| 46 | P.J. Washington | +1.44 | 1835 |
| 47 | Jalen Johnson | +1.40 | 1284 |
| 48 | Jimmy Butler | +1.38 | 1746 |
| 49 | Paul George | +1.36 | 1334 |
| 50 | Tari Eason | +1.33 | 1420 |

## 2025-26 Regular season — defense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +5.54 | 1866 |
| 2 | Chet Holmgren | +4.06 | 1997 |
| 3 | Neemias Queta | +3.82 | 1926 |
| 4 | Isaiah Hartenstein | +3.54 | 1137 |
| 5 | Ausar Thompson | +3.36 | 1896 |
| 6 | Rudy Gobert | +3.31 | 2380 |
| 7 | Hugo González | +3.26 | 1084 |
| 8 | Derrick White | +3.17 | 2625 |
| 9 | Cason Wallace | +2.96 | 2046 |
| 10 | Ronald Holland II | +2.92 | 1550 |
| 11 | Javonte Green | +2.68 | 1446 |
| 12 | Ajay Mitchell | +2.67 | 1473 |
| 13 | Dyson Daniels | +2.62 | 2520 |
| 14 | Marcus Smart | +2.60 | 1769 |
| 15 | Toumani Camara | +2.44 | 2731 |
| 16 | Baylor Scheierman | +2.40 | 1429 |
| 17 | Ryan Kalkbrenner | +2.34 | 1479 |
| 18 | Dru Smith | +2.26 | 1141 |
| 19 | John Konchar | +2.22 | 1115 |
| 20 | Luke Kornet | +2.21 | 1430 |
| 21 | Jamal Shead | +2.21 | 1852 |
| 22 | Jarrett Allen | +2.20 | 1519 |
| 23 | Jordan Goodwin | +2.17 | 1572 |
| 24 | Sidy Cissoko | +2.16 | 1435 |
| 25 | Jaylin Williams | +2.01 | 1277 |
| 26 | Jalen Suggs | +2.01 | 1574 |
| 27 | Paul George | +2.00 | 1135 |
| 28 | Keon Ellis | +1.97 | 1479 |
| 29 | Ryan Dunn | +1.97 | 1355 |
| 30 | Sion James | +1.94 | 1843 |
| 31 | Evan Mobley | +1.94 | 2074 |
| 32 | Jusuf Nurkić | +1.92 | 1083 |
| 33 | Donte DiVincenzo | +1.90 | 2494 |
| 34 | Wendell Carter Jr. | +1.89 | 2288 |
| 35 | Collin Murray-Boyles | +1.86 | 1246 |
| 36 | Mitchell Robinson | +1.85 | 1175 |
| 37 | Josh Okogie | +1.78 | 1354 |
| 38 | OG Anunoby | +1.77 | 2224 |
| 39 | Scottie Barnes | +1.68 | 2681 |
| 40 | Bam Adebayo | +1.64 | 2365 |
| 41 | Collin Gillespie | +1.60 | 2282 |
| 42 | Kris Murray | +1.58 | 1333 |
| 43 | Naz Reid | +1.56 | 2007 |
| 44 | Luguentz Dort | +1.54 | 1849 |
| 45 | Jalen Duren | +1.52 | 1976 |
| 46 | Oso Ighodaro | +1.52 | 1808 |
| 47 | Mouhamed Gueye | +1.47 | 1179 |
| 48 | Landry Shamet | +1.45 | 1171 |
| 49 | Donovan Clingan | +1.41 | 2094 |
| 50 | Brook Lopez | +1.32 | 1635 |
