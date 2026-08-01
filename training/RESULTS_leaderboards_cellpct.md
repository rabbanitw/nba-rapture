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
| total | stage2-0 / l2 | blend | 1149 | 1.582 | +0.327 | +0.906 |
| offense | stage2-0 / l2 | blend | 1183 | 1.090 | +0.490 | +0.934 |
| defense | stage2-0 / huber | blend | 649 | 1.097 | +0.394 | +0.864 |

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
| total | 2013-14 | Regular season | 246 | 1.622 | 0.945 | +0.476 | +0.480 | 23/30 | 22/30 |
| total | 2014-15 | Regular season | 246 | 1.545 | 0.914 | +0.669 | +0.595 | 23/30 | 22/30 |
| offense | 2013-14 | Regular season | 246 | 1.099 | 0.713 | +0.779 | +0.811 | 25/30 | 22/30 |
| offense | 2014-15 | Regular season | 246 | 1.086 | 0.722 | +0.582 | +0.490 | 25/30 | 23/30 |
| defense | 2013-14 | Regular season | 246 | 1.078 | 0.925 | +0.255 | +0.352 | 19/30 | 17/30 |
| defense | 2014-15 | Regular season | 246 | 1.119 | 0.846 | +0.370 | +0.582 | 23/30 | 19/30 |

## Kendall tau over the top 30, held-out seasons

`tau(true30)` compares the true order of the true top 30 against their
projected order. `tau(union30)` widens the set to the union of the true and
projected top 30, so it also penalises wrongly promoted players.

| target | season | split | pool | tau(true30) | tau(union30) | hits@30 | mean &#124;Δrank&#124; |
|---|---|---|---|---|---|---|---|
| total | 2013-14 | Regular season | 247 | +0.490 | +0.363 | 23/30 | 14.1 |
| total | 2014-15 | Regular season | 247 | +0.678 | +0.432 | 23/30 | 13.9 |
| offense | 2013-14 | Regular season | 247 | +0.789 | +0.687 | 25/30 | 11.4 |
| offense | 2014-15 | Regular season | 247 | +0.586 | +0.486 | 25/30 | 9.6 |
| defense | 2013-14 | Regular season | 247 | +0.315 | +0.166 | 19/30 | 21.7 |
| defense | 2014-15 | Regular season | 247 | +0.444 | +0.388 | 22/30 | 17.6 |

## 2013-14 Regular season — total, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.490 &nbsp;·&nbsp; hits@30 23/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Paul George | +12.60 | +5.60 | 8 | +7 | Chris Paul | +11.00 |
| 2 | Chris Paul | +10.20 | +11.00 | 1 | -1 | Kevin Durant | +7.10 |
| 3 | Kevin Durant | +9.10 | +7.10 | 2 | -1 | Kawhi Leonard | +6.70 |
| 4 | Kevin Love | +8.40 | +6.60 | 4 | +0 | Kevin Love | +6.60 |
| 5 | Manu Ginobili | +7.90 | +5.10 | 9 | +4 | James Harden | +6.10 |
| 6 | Kyle Lowry | +7.30 | +5.70 | 7 | +1 | Joakim Noah | +5.90 |
| 7 | LeBron James | +7.00 | +4.60 | 15 | +8 | Kyle Lowry | +5.70 |
| 8 | James Harden | +6.90 | +6.10 | 5 | -3 | Paul George | +5.60 |
| 9 | Kawhi Leonard | +6.70 | +6.70 | 3 | -6 | Manu Ginobili | +5.10 |
| 10 | Andrew Bogut | +6.50 | +3.10 | 37 | +27 | DeMarcus Cousins | +5.00 |
| 11 | Dirk Nowitzki | +6.20 | +4.70 | 13 | +2 | Goran Dragic | +5.00 |
| 12 | Goran Dragic | +6.00 | +5.00 | 11 | -1 | Patty Mills | +4.80 |
| 13 | Mike Conley | +5.80 | +3.80 | 21 | +8 | Dirk Nowitzki | +4.70 |
| 14 | Andre Iguodala | +5.50 | +3.80 | 23 | +9 | Danny Green | +4.70 |
| 15 | Blake Griffin | +5.40 | +3.20 | 34 | +19 | LeBron James | +4.60 |
| 16 | Joakim Noah | +5.30 | +5.90 | 6 | -10 | Anderson Varejao | +4.10 |
| 17 | Anderson Varejao | +5.00 | +4.10 | 16 | -1 | Patrick Beverley | +4.10 |
| 18 | Ricky Rubio | +4.90 | +3.70 | 26 | +8 | Mario Chalmers | +4.00 |
| 19 | LaMarcus Aldridge | +4.71 | +3.40 | 29 | +10 | Isaiah Thomas | +3.90 |
| 20 | Damian Lillard | +4.60 | +2.10 | 56 | +36 | Jimmy Butler | +3.90 |
| 21 | Jimmy Butler | +4.50 | +3.90 | 20 | -1 | Mike Conley | +3.80 |
| 22 | Paul Millsap | +4.40 | +3.10 | 36 | +14 | Kemba Walker | +3.80 |
| 23 | Chris Bosh | +4.30 | +0.90 | 94 | +71 | Andre Iguodala | +3.80 |
| 24 | Nicolas Batum | +4.20 | +1.90 | 59 | +35 | Carmelo Anthony | +3.80 |
| 25 | Patrick Beverley | +4.10 | +4.10 | 17 | -8 | Russell Westbrook | +3.70 |
| 26 | Anthony Davis | +4.00 | +3.50 | 28 | +2 | Ricky Rubio | +3.70 |
| 27 | Kemba Walker | +3.90 | +3.80 | 22 | -5 | Eric Bledsoe | +3.70 |
| 28 | Isaiah Thomas | +3.90 | +3.90 | 19 | -9 | Anthony Davis | +3.50 |
| 29 | Mario Chalmers | +3.80 | +4.00 | 18 | -11 | LaMarcus Aldridge | +3.40 |
| 30 | George Hill | +3.70 | +2.50 | 48 | +18 | Draymond Green | +3.40 |
| 31 | Patty Mills | +3.60 | +4.80 | 12 | -19 | Nikola Pekovic | +3.30 |
| 32 | Draymond Green | +3.50 | +3.40 | 30 | -2 | DeMarre Carroll | +3.30 |
| 33 | Danny Green | +3.40 | +4.70 | 14 | -19 | Tiago Splitter | +3.30 |
| 34 | DeMarcus Cousins | +3.30 | +5.00 | 10 | -24 | Blake Griffin | +3.20 |
| 35 | DeAndre Jordan | +3.20 | +1.80 | 64 | +29 | Deron Williams | +3.20 |
| 36 | Paul Pierce | +3.20 | +1.90 | 60 | +24 | Paul Millsap | +3.10 |
| 37 | Amir Johnson | +3.10 | +1.00 | 89 | +52 | Andrew Bogut | +3.10 |
| 38 | Deron Williams | +3.10 | +3.20 | 35 | -3 | Kris Humphries | +3.00 |
| 39 | Carmelo Anthony | +3.10 | +3.80 | 24 | -15 | Klay Thompson | +2.90 |
| 40 | Pablo Prigioni | +3.00 | +1.60 | 73 | +33 | Ty Lawson | +2.90 |
| 41 | Derek Fisher | +2.90 | +2.30 | 50 | +9 | Jae Crowder | +2.90 |
| 42 | John Wall | +2.90 | +1.70 | 69 | +27 | Robin Lopez | +2.90 |
| 43 | Trevor Ariza | +2.80 | +1.70 | 67 | +24 | Vince Carter | +2.90 |
| 44 | David West | +2.78 | +2.20 | 51 | +7 | Darren Collison | +2.70 |
| 45 | Wesley Matthews | +2.70 | +2.60 | 46 | +1 | Shane Battier | +2.70 |
| 46 | Russell Westbrook | +2.60 | +3.70 | 25 | -21 | Wesley Matthews | +2.60 |
| 47 | PJ Tucker | +2.60 | +1.70 | 68 | +21 | Tony Allen | +2.60 |
| 48 | Robin Lopez | +2.60 | +2.90 | 42 | -6 | George Hill | +2.50 |
| 49 | Tiago Splitter | +2.50 | +3.30 | 33 | -16 | Channing Frye | +2.40 |
| 50 | DeMarre Carroll | +2.50 | +3.30 | 32 | -18 | Derek Fisher | +2.30 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.678 &nbsp;·&nbsp; hits@30 23/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Anthony Davis | +12.60 | +8.80 | 3 | +2 | Chris Paul | +10.60 |
| 2 | Chris Paul | +10.20 | +10.60 | 1 | -1 | Kawhi Leonard | +8.90 |
| 3 | James Harden | +9.10 | +7.70 | 4 | +1 | Anthony Davis | +8.80 |
| 4 | Draymond Green | +8.40 | +6.50 | 5 | +1 | James Harden | +7.70 |
| 5 | LeBron James | +7.90 | +5.10 | 11 | +6 | Draymond Green | +6.50 |
| 6 | Kawhi Leonard | +7.30 | +8.90 | 2 | -4 | Danny Green | +6.10 |
| 7 | Jimmy Butler | +7.00 | +3.00 | 36 | +29 | George Hill | +5.60 |
| 8 | Danny Green | +6.90 | +6.10 | 6 | -2 | Russell Westbrook | +5.60 |
| 9 | Damian Lillard | +6.70 | +2.70 | 42 | +33 | DeMarcus Cousins | +5.40 |
| 10 | Kyrie Irving | +6.50 | +4.60 | 13 | +3 | Klay Thompson | +5.30 |
| 11 | Klay Thompson | +6.20 | +5.30 | 10 | -1 | LeBron James | +5.10 |
| 12 | Russell Westbrook | +6.00 | +5.60 | 8 | -4 | Khris Middleton | +4.80 |
| 13 | Lou Williams | +5.80 | +3.00 | 35 | +22 | Kyrie Irving | +4.60 |
| 14 | DeMarcus Cousins | +5.50 | +5.40 | 9 | -5 | Kyle Korver | +4.60 |
| 15 | Kyle Lowry | +5.40 | +3.90 | 22 | +7 | DeAndre Jordan | +4.60 |
| 16 | Khris Middleton | +5.30 | +4.80 | 12 | -4 | LaMarcus Aldridge | +4.30 |
| 17 | Kyle Korver | +5.00 | +4.60 | 14 | -3 | Tony Allen | +4.30 |
| 18 | George Hill | +4.90 | +5.60 | 7 | -11 | Nikola Mirotic | +4.20 |
| 19 | Marcus Smart | +4.71 | +2.10 | 59 | +40 | Rudy Gobert | +4.10 |
| 20 | LaMarcus Aldridge | +4.60 | +4.30 | 16 | -4 | Marc Gasol | +4.00 |
| 21 | Mike Conley | +4.50 | +2.90 | 39 | +18 | Darren Collison | +4.00 |
| 22 | Andrew Bogut | +4.40 | +3.70 | 23 | +1 | Kyle Lowry | +3.90 |
| 23 | Tony Allen | +4.30 | +4.30 | 17 | -6 | Andrew Bogut | +3.70 |
| 24 | DeAndre Jordan | +4.20 | +4.60 | 15 | -9 | Wesley Matthews | +3.60 |
| 25 | Wesley Matthews | +4.10 | +3.60 | 24 | -1 | Jonas Jerebko | +3.60 |
| 26 | Paul Millsap | +4.00 | +3.30 | 28 | +2 | Gordon Hayward | +3.40 |
| 27 | Tyson Chandler | +3.90 | +2.60 | 48 | +21 | Tim Duncan | +3.30 |
| 28 | Blake Griffin | +3.90 | +2.00 | 63 | +35 | Paul Millsap | +3.30 |
| 29 | Marc Gasol | +3.80 | +4.00 | 20 | -9 | Marcin Gortat | +3.20 |
| 30 | Gordon Hayward | +3.70 | +3.40 | 26 | -4 | Kevin Love | +3.20 |
| 31 | Manu Ginobili | +3.60 | +3.20 | 33 | +2 | JJ Redick | +3.20 |
| 32 | Isaiah Thomas | +3.50 | +1.60 | 72 | +40 | Brandon Jennings | +3.20 |
| 33 | Jared Dudley | +3.40 | +1.90 | 67 | +34 | Manu Ginobili | +3.20 |
| 34 | Ersan Ilyasova | +3.30 | +2.50 | 49 | +15 | Danilo Gallinari | +3.00 |
| 35 | John Wall | +3.20 | +2.00 | 64 | +29 | Lou Williams | +3.00 |
| 36 | Tim Duncan | +3.20 | +3.30 | 27 | -9 | Jimmy Butler | +3.00 |
| 37 | JJ Redick | +3.10 | +3.20 | 31 | -6 | DeMarre Carroll | +2.90 |
| 38 | Kevin Love | +3.10 | +3.20 | 30 | -8 | Eric Bledsoe | +2.90 |
| 39 | Darren Collison | +3.10 | +4.00 | 21 | -18 | Mike Conley | +2.90 |
| 40 | Rudy Gobert | +3.00 | +4.10 | 19 | -21 | Zach Randolph | +2.90 |
| 41 | Nikola Mirotic | +2.90 | +4.20 | 18 | -23 | Kelly Olynyk | +2.80 |
| 42 | Jeff Teague | +2.90 | +2.70 | 46 | +4 | Damian Lillard | +2.70 |
| 43 | Danilo Gallinari | +2.80 | +3.00 | 34 | -9 | Jrue Holiday | +2.70 |
| 44 | Matt Barnes | +2.78 | +1.60 | 71 | +27 | Zaza Pachulia | +2.70 |
| 45 | Patrick Patterson | +2.70 | +1.60 | 74 | +29 | Anthony Morrow | +2.70 |
| 46 | Pau Gasol | +2.60 | +0.80 | 102 | +56 | Jeff Teague | +2.70 |
| 47 | Andre Iguodala | +2.60 | +1.30 | 82 | +35 | Serge Ibaka | +2.60 |
| 48 | Zaza Pachulia | +2.60 | +2.70 | 44 | -4 | Tyson Chandler | +2.60 |
| 49 | Al Horford | +2.50 | +2.00 | 65 | +16 | Ersan Ilyasova | +2.50 |
| 50 | Marcin Gortat | +2.50 | +3.20 | 29 | -21 | Devin Harris | +2.50 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.789 &nbsp;·&nbsp; hits@30 25/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | LeBron James | +9.10 | +5.80 | 4 | +3 | Kevin Durant | +7.60 |
| 2 | Chris Paul | +7.87 | +7.10 | 2 | +0 | Chris Paul | +7.10 |
| 3 | Kevin Durant | +7.00 | +7.60 | 1 | -2 | James Harden | +6.30 |
| 4 | Kevin Love | +6.50 | +5.70 | 5 | +1 | LeBron James | +5.80 |
| 5 | James Harden | +6.30 | +6.30 | 3 | -2 | Kevin Love | +5.70 |
| 6 | Goran Dragic | +5.90 | +4.80 | 6 | +0 | Goran Dragic | +4.80 |
| 7 | Manu Ginobili | +5.60 | +4.00 | 10 | +3 | Kyle Lowry | +4.40 |
| 8 | Kyle Lowry | +5.30 | +4.40 | 7 | -1 | Dirk Nowitzki | +4.40 |
| 9 | Damian Lillard | +5.10 | +3.60 | 12 | +3 | Carmelo Anthony | +4.20 |
| 10 | Mike Conley | +4.80 | +3.50 | 13 | +3 | Manu Ginobili | +4.00 |
| 11 | Isaiah Thomas | +4.70 | +3.50 | 14 | +3 | Patty Mills | +3.90 |
| 12 | Dirk Nowitzki | +4.50 | +4.40 | 8 | -4 | Damian Lillard | +3.60 |
| 13 | Paul George | +4.30 | +2.60 | 22 | +9 | Mike Conley | +3.50 |
| 14 | Russell Westbrook | +4.12 | +3.30 | 15 | +1 | Isaiah Thomas | +3.50 |
| 15 | Carmelo Anthony | +4.00 | +4.20 | 9 | -6 | Russell Westbrook | +3.30 |
| 16 | Patty Mills | +4.00 | +3.90 | 11 | -5 | Ty Lawson | +3.20 |
| 17 | Blake Griffin | +3.80 | +2.90 | 17 | +0 | Blake Griffin | +2.90 |
| 18 | John Wall | +3.70 | +1.90 | 37 | +19 | Marco Belinelli | +2.80 |
| 19 | DJ Augustin | +3.60 | +2.10 | 32 | +13 | Jamal Crawford | +2.80 |
| 20 | Jamal Crawford | +3.50 | +2.80 | 19 | -1 | Wesley Matthews | +2.80 |
| 21 | Kyrie Irving | +3.40 | +2.30 | 27 | +6 | Joe Johnson | +2.70 |
| 22 | Wesley Matthews | +3.30 | +2.80 | 20 | -2 | Paul George | +2.60 |
| 23 | Ty Lawson | +3.20 | +3.20 | 16 | -7 | Deron Williams | +2.60 |
| 24 | Ricky Rubio | +3.10 | +1.90 | 39 | +15 | Chandler Parsons | +2.60 |
| 25 | Marco Belinelli | +3.00 | +2.80 | 18 | -7 | Nick Young | +2.40 |
| 26 | Chandler Parsons | +2.90 | +2.60 | 24 | -2 | Vince Carter | +2.40 |
| 27 | Deron Williams | +2.90 | +2.60 | 23 | -4 | Kyrie Irving | +2.30 |
| 28 | Nikola Pekovic | +2.80 | +1.10 | 68 | +40 | Jrue Holiday | +2.20 |
| 29 | Joe Johnson | +2.70 | +2.70 | 21 | -8 | Patrick Beverley | +2.20 |
| 30 | George Hill | +2.70 | +0.30 | 120 | +90 | Brandon Jennings | +2.20 |
| 31 | Mario Chalmers | +2.60 | +1.50 | 50 | +19 | Randy Foye | +2.10 |
| 32 | Brandan Wright | +2.50 | +1.70 | 41 | +9 | DJ Augustin | +2.10 |
| 33 | Randy Foye | +2.50 | +2.10 | 31 | -2 | Klay Thompson | +2.10 |
| 34 | Vince Carter | +2.40 | +2.40 | 26 | -8 | Josh McRoberts | +2.00 |
| 35 | Rudy Gay | +2.30 | +1.10 | 66 | +31 | Channing Frye | +2.00 |
| 36 | Klay Thompson | +2.30 | +2.10 | 33 | -3 | Kyle Korver | +1.90 |
| 37 | Nicolas Batum | +2.20 | +1.90 | 38 | +1 | John Wall | +1.90 |
| 38 | JR Smith | +2.20 | +1.80 | 40 | +2 | Nicolas Batum | +1.90 |
| 39 | DeMar DeRozan | +2.10 | +1.70 | 42 | +3 | Ricky Rubio | +1.90 |
| 40 | Jose Calderon | +2.00 | +1.60 | 47 | +7 | JR Smith | +1.80 |
| 41 | Kemba Walker | +1.90 | +1.40 | 52 | +11 | Brandan Wright | +1.70 |
| 42 | Trevor Ariza | +1.90 | +1.00 | 69 | +27 | DeMar DeRozan | +1.70 |
| 43 | Kevin Martin | +1.80 | +0.70 | 87 | +44 | Pablo Prigioni | +1.70 |
| 44 | Kyle Korver | +1.80 | +1.90 | 36 | -8 | Kawhi Leonard | +1.70 |
| 45 | Andre Iguodala | +1.80 | +1.20 | 62 | +17 | DeMarcus Cousins | +1.70 |
| 46 | Nick Young | +1.80 | +2.40 | 25 | -21 | Mirza Teletovic | +1.60 |
| 47 | Patrick Beverley | +1.70 | +2.20 | 29 | -18 | Jose Calderon | +1.60 |
| 48 | Jrue Holiday | +1.70 | +2.20 | 28 | -20 | Eric Bledsoe | +1.50 |
| 49 | Jeff Teague | +1.60 | +0.60 | 92 | +43 | Dwyane Wade | +1.50 |
| 50 | Darren Collison | +1.60 | +1.10 | 65 | +15 | Mario Chalmers | +1.50 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.586 &nbsp;·&nbsp; hits@30 25/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +9.10 | +8.50 | 1 | +0 | Chris Paul | +8.50 |
| 2 | James Harden | +7.87 | +7.70 | 2 | +0 | James Harden | +7.70 |
| 3 | Kyrie Irving | +7.00 | +5.50 | 4 | +1 | Russell Westbrook | +6.10 |
| 4 | Russell Westbrook | +6.50 | +6.10 | 3 | -1 | Kyrie Irving | +5.50 |
| 5 | Lou Williams | +6.30 | +5.20 | 6 | +1 | LeBron James | +5.30 |
| 6 | LeBron James | +5.90 | +5.30 | 5 | -1 | Lou Williams | +5.20 |
| 7 | Damian Lillard | +5.60 | +4.00 | 11 | +4 | Kyle Korver | +4.60 |
| 8 | Isaiah Thomas | +5.30 | +4.50 | 8 | +0 | Isaiah Thomas | +4.50 |
| 9 | Klay Thompson | +5.10 | +4.30 | 10 | +1 | Anthony Davis | +4.30 |
| 10 | JJ Redick | +4.80 | +2.50 | 29 | +19 | Klay Thompson | +4.30 |
| 11 | Mike Conley | +4.70 | +2.40 | 32 | +21 | Damian Lillard | +4.00 |
| 12 | Anthony Davis | +4.50 | +4.30 | 9 | -3 | George Hill | +3.90 |
| 13 | Gordon Hayward | +4.30 | +3.20 | 21 | +8 | Carmelo Anthony | +3.80 |
| 14 | Blake Griffin | +4.12 | +3.20 | 22 | +8 | Ty Lawson | +3.80 |
| 15 | George Hill | +4.00 | +3.90 | 12 | -3 | Kawhi Leonard | +3.70 |
| 16 | Jimmy Butler | +4.00 | +3.20 | 20 | +4 | Rudy Gay | +3.50 |
| 17 | Kawhi Leonard | +3.80 | +3.70 | 15 | -2 | DeAndre Jordan | +3.40 |
| 18 | Jeff Teague | +3.70 | +2.20 | 35 | +17 | Kyle Lowry | +3.30 |
| 19 | Rudy Gay | +3.60 | +3.50 | 16 | -3 | Jrue Holiday | +3.30 |
| 20 | Khris Middleton | +3.50 | +1.70 | 51 | +31 | Jimmy Butler | +3.20 |
| 21 | Kyle Korver | +3.40 | +4.60 | 7 | -14 | Gordon Hayward | +3.20 |
| 22 | Anthony Morrow | +3.30 | +2.70 | 26 | +4 | Blake Griffin | +3.20 |
| 23 | Ty Lawson | +3.20 | +3.80 | 14 | -9 | Brandon Jennings | +3.10 |
| 24 | Brandon Jennings | +3.10 | +3.10 | 23 | -1 | Danny Green | +3.10 |
| 25 | Danny Green | +3.00 | +3.10 | 24 | -1 | Danilo Gallinari | +2.80 |
| 26 | Carmelo Anthony | +2.90 | +3.80 | 13 | -13 | Anthony Morrow | +2.70 |
| 27 | Kyle Lowry | +2.90 | +3.30 | 18 | -9 | Chandler Parsons | +2.60 |
| 28 | Jrue Holiday | +2.80 | +3.30 | 19 | -9 | Tyreke Evans | +2.60 |
| 29 | Dirk Nowitzki | +2.70 | +2.20 | 34 | +5 | JJ Redick | +2.50 |
| 30 | Patrick Patterson | +2.70 | +2.40 | 31 | +1 | LaMarcus Aldridge | +2.40 |
| 31 | Danilo Gallinari | +2.60 | +2.80 | 25 | -6 | Patrick Patterson | +2.40 |
| 32 | Aaron Brooks | +2.50 | +1.60 | 56 | +24 | Mike Conley | +2.40 |
| 33 | LaMarcus Aldridge | +2.50 | +2.40 | 30 | -3 | Wesley Matthews | +2.30 |
| 34 | John Wall | +2.40 | +2.10 | 39 | +5 | Dirk Nowitzki | +2.20 |
| 35 | Dwyane Wade | +2.30 | +2.00 | 42 | +7 | Jeff Teague | +2.20 |
| 36 | Eric Gordon | +2.30 | +0.50 | 89 | +53 | Gerald Green | +2.20 |
| 37 | Reggie Jackson | +2.20 | +2.00 | 41 | +4 | Devin Harris | +2.10 |
| 38 | Tyreke Evans | +2.20 | +2.60 | 28 | -10 | JR Smith | +2.10 |
| 39 | Chandler Parsons | +2.10 | +2.60 | 27 | -12 | John Wall | +2.10 |
| 40 | Kevin Love | +2.00 | +1.70 | 52 | +12 | Ersan Ilyasova | +2.10 |
| 41 | Gerald Green | +1.90 | +2.20 | 36 | -5 | Reggie Jackson | +2.00 |
| 42 | Darren Collison | +1.90 | +1.70 | 55 | +13 | Dwyane Wade | +2.00 |
| 43 | Manu Ginobili | +1.80 | +1.70 | 54 | +11 | DeMarre Carroll | +1.90 |
| 44 | Bradley Beal | +1.80 | +0.80 | 78 | +34 | Nikola Mirotic | +1.90 |
| 45 | Wesley Matthews | +1.80 | +2.30 | 33 | -12 | Goran Dragic | +1.90 |
| 46 | DeAndre Jordan | +1.80 | +3.40 | 17 | -29 | JJ Barea | +1.90 |
| 47 | Marc Gasol | +1.70 | +1.40 | 60 | +13 | Joe Johnson | +1.80 |
| 48 | Goran Dragic | +1.70 | +1.90 | 45 | -3 | Luol Deng | +1.80 |
| 49 | Draymond Green | +1.60 | +1.50 | 58 | +9 | Jae Crowder | +1.80 |
| 50 | Jamal Crawford | +1.60 | +1.10 | 69 | +19 | Eric Bledsoe | +1.70 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.315 &nbsp;·&nbsp; hits@30 19/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Andrew Bogut | +9.40 | +4.40 | 4 | +3 | Kawhi Leonard | +5.00 |
| 2 | Draymond Green | +7.80 | +4.60 | 2 | +0 | Draymond Green | +4.60 |
| 3 | Nene | +7.20 | +3.80 | 9 | +6 | Joakim Noah | +4.50 |
| 4 | Kevin Garnett | +6.80 | +3.50 | 11 | +7 | Andrew Bogut | +4.40 |
| 5 | DeMarcus Cousins | +6.30 | +3.30 | 16 | +11 | Michael KiddGilchrist | +4.40 |
| 6 | Kawhi Leonard | +5.90 | +5.00 | 1 | -5 | Tiago Splitter | +4.20 |
| 7 | Paul Millsap | +5.60 | +2.70 | 23 | +16 | Danny Green | +4.00 |
| 8 | Iman Shumpert | +5.30 | +1.00 | 73 | +65 | Chris Paul | +3.90 |
| 9 | Paul George | +5.10 | +2.90 | 21 | +12 | Nene | +3.80 |
| 10 | Andre Iguodala | +4.80 | +2.60 | 25 | +15 | Anderson Varejao | +3.60 |
| 11 | Joakim Noah | +4.70 | +4.50 | 3 | -8 | Kevin Garnett | +3.50 |
| 12 | Anderson Varejao | +4.60 | +3.60 | 10 | -2 | Nick Calathes | +3.50 |
| 13 | Jae Crowder | +4.40 | +3.00 | 19 | +6 | Ian Mahinmi | +3.50 |
| 14 | Paul Pierce | +4.30 | +1.80 | 50 | +36 | Jimmy Butler | +3.40 |
| 15 | LaMarcus Aldridge | +4.20 | +2.20 | 37 | +22 | Roy Hibbert | +3.40 |
| 16 | Danny Green | +4.00 | +4.00 | 7 | -9 | DeMarcus Cousins | +3.30 |
| 17 | Tiago Splitter | +3.90 | +4.20 | 6 | -11 | CJ Watson | +3.20 |
| 18 | DeAndre Jordan | +3.80 | +1.10 | 65 | +47 | Tim Duncan | +3.00 |
| 19 | Chris Bosh | +3.70 | +1.00 | 75 | +56 | Jae Crowder | +3.00 |
| 20 | Tim Duncan | +3.60 | +3.00 | 18 | -2 | Kris Humphries | +3.00 |
| 21 | Michael KiddGilchrist | +3.50 | +4.40 | 5 | -16 | Paul George | +2.90 |
| 22 | Marcin Gortat | +3.40 | +2.50 | 29 | +7 | Marc Gasol | +2.80 |
| 23 | Al Jefferson | +3.40 | +1.80 | 47 | +24 | Paul Millsap | +2.70 |
| 24 | Anthony Davis | +3.30 | +2.30 | 34 | +10 | Shane Battier | +2.70 |
| 25 | DeMarre Carroll | +3.20 | +2.60 | 26 | +1 | Andre Iguodala | +2.60 |
| 26 | Chris Andersen | +3.10 | +1.60 | 55 | +29 | DeMarre Carroll | +2.60 |
| 27 | Derek Fisher | +3.10 | +2.00 | 43 | +16 | Mario Chalmers | +2.50 |
| 28 | Miles Plumlee | +3.00 | +1.60 | 56 | +28 | Samuel Dalembert | +2.50 |
| 29 | Shane Battier | +2.90 | +2.70 | 24 | -5 | Marcin Gortat | +2.50 |
| 30 | David West | +2.90 | +1.50 | 57 | +27 | Victor Oladipo | +2.40 |
| 31 | Gerald Wallace | +2.80 | +1.60 | 52 | +21 | Tony Allen | +2.40 |
| 32 | Blake Griffin | +2.80 | +0.30 | 113 | +81 | Dwight Howard | +2.40 |
| 33 | Amir Johnson | +2.70 | +1.20 | 63 | +30 | Serge Ibaka | +2.30 |
| 34 | Ersan Ilyasova | +2.60 | +0.70 | 90 | +56 | Anthony Davis | +2.30 |
| 35 | Shaun Livingston | +2.60 | +0.90 | 77 | +42 | Kemba Walker | +2.30 |
| 36 | Jimmy Butler | +2.60 | +3.40 | 14 | -22 | Thabo Sefolosha | +2.30 |
| 37 | Roy Hibbert | +2.50 | +3.40 | 15 | -22 | LaMarcus Aldridge | +2.20 |
| 38 | PJ Tucker | +2.50 | +0.90 | 82 | +44 | Nikola Pekovic | +2.20 |
| 39 | Kyle OQuinn | +2.50 | +1.60 | 53 | +14 | Eric Bledsoe | +2.20 |
| 40 | Dwight Howard | +2.40 | +2.40 | 32 | -8 | George Hill | +2.10 |
| 41 | CJ Watson | +2.40 | +3.20 | 17 | -24 | Kosta Koufos | +2.10 |
| 42 | Thabo Sefolosha | +2.40 | +2.30 | 36 | -6 | Kirk Hinrich | +2.10 |
| 43 | Marc Gasol | +2.30 | +2.80 | 22 | -21 | Derek Fisher | +2.00 |
| 44 | Patrick Patterson | +2.30 | +0.80 | 83 | +39 | Robin Lopez | +2.00 |
| 45 | Ian Mahinmi | +2.20 | +3.50 | 13 | -32 | Darrell Arthur | +1.90 |
| 46 | Nicolas Batum | +2.20 | +0.00 | 128 | +82 | Patrick Beverley | +1.90 |
| 47 | Kemba Walker | +2.10 | +2.30 | 35 | -12 | Al Jefferson | +1.80 |
| 48 | Patrick Beverley | +2.10 | +1.90 | 46 | -2 | Jeremy Lin | +1.80 |
| 49 | Tony Allen | +2.00 | +2.40 | 31 | -18 | Ricky Rubio | +1.80 |
| 50 | Robin Lopez | +2.00 | +2.00 | 44 | -6 | Paul Pierce | +1.80 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.444 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Draymond Green | +9.40 | +5.10 | 2 | +1 | Kawhi Leonard | +5.20 |
| 2 | Andrew Bogut | +7.80 | +4.70 | 5 | +3 | Draymond Green | +5.10 |
| 3 | DeMarcus Cousins | +7.20 | +4.40 | 7 | +4 | Rudy Gobert | +4.80 |
| 4 | Anthony Davis | +6.80 | +4.50 | 6 | +2 | Tony Allen | +4.80 |
| 5 | Kawhi Leonard | +6.30 | +5.20 | 1 | -4 | Andrew Bogut | +4.70 |
| 6 | AlFarouq Aminu | +5.90 | +2.60 | 24 | +18 | Anthony Davis | +4.50 |
| 7 | Nene | +5.60 | +2.80 | 18 | +11 | DeMarcus Cousins | +4.40 |
| 8 | Tony Allen | +5.30 | +4.80 | 4 | -4 | Marcin Gortat | +3.60 |
| 9 | Rudy Gobert | +5.10 | +4.80 | 3 | -6 | Tim Duncan | +3.50 |
| 10 | Tyson Chandler | +4.80 | +2.60 | 20 | +10 | Andre Roberson | +3.40 |
| 11 | Tim Duncan | +4.70 | +3.50 | 9 | -2 | Kosta Koufos | +3.30 |
| 12 | Nerlens Noel | +4.60 | +2.70 | 19 | +7 | Zaza Pachulia | +3.20 |
| 13 | Iman Shumpert | +4.40 | +2.30 | 32 | +19 | Khris Middleton | +3.10 |
| 14 | Danny Green | +4.30 | +3.00 | 16 | +2 | Michael KiddGilchrist | +3.00 |
| 15 | Josh Smith | +4.20 | +2.60 | 23 | +8 | Serge Ibaka | +3.00 |
| 16 | Timofey Mozgov | +4.00 | +2.40 | 28 | +12 | Danny Green | +3.00 |
| 17 | Zaza Pachulia | +3.90 | +3.20 | 12 | -5 | Jonas Jerebko | +2.80 |
| 18 | Marcus Smart | +3.80 | +1.80 | 43 | +25 | Nene | +2.80 |
| 19 | Andre Roberson | +3.70 | +3.40 | 10 | -9 | Nerlens Noel | +2.70 |
| 20 | Paul Millsap | +3.60 | +2.40 | 26 | +6 | Tyson Chandler | +2.60 |
| 21 | Michael KiddGilchrist | +3.50 | +3.00 | 14 | -7 | Marc Gasol | +2.60 |
| 22 | Derrick Favors | +3.40 | +2.10 | 34 | +12 | Joakim Noah | +2.60 |
| 23 | Marcin Gortat | +3.40 | +3.60 | 8 | -15 | Josh Smith | +2.60 |
| 24 | Joakim Noah | +3.30 | +2.60 | 22 | -2 | AlFarouq Aminu | +2.60 |
| 25 | Andre Iguodala | +3.20 | +1.60 | 47 | +22 | Alex Len | +2.50 |
| 26 | Pau Gasol | +3.10 | +0.50 | 90 | +64 | Paul Millsap | +2.40 |
| 27 | DeAndre Jordan | +3.10 | +1.20 | 63 | +36 | Omer Asik | +2.40 |
| 28 | James Johnson | +3.00 | +1.30 | 55 | +27 | Timofey Mozgov | +2.40 |
| 29 | Wesley Matthews | +2.90 | +1.30 | 56 | +27 | Darren Collison | +2.30 |
| 30 | Kosta Koufos | +2.90 | +3.30 | 11 | -19 | Luc Mbah a Moute | +2.30 |
| 31 | Nikola Mirotic | +2.80 | +2.20 | 33 | +2 | Michael CarterWilliams | +2.30 |
| 32 | Al Horford | +2.80 | +1.60 | 46 | +14 | Iman Shumpert | +2.30 |
| 33 | Donatas Motiejunas | +2.70 | +0.80 | 78 | +45 | Nikola Mirotic | +2.20 |
| 34 | Jonas Jerebko | +2.60 | +2.80 | 17 | -17 | Derrick Favors | +2.10 |
| 35 | Omer Asik | +2.60 | +2.40 | 27 | -8 | Chris Paul | +2.10 |
| 36 | Jimmy Butler | +2.60 | -0.20 | 140 | +104 | Kelly Olynyk | +2.10 |
| 37 | Jared Dudley | +2.50 | +1.80 | 44 | +7 | Cody Zeller | +2.10 |
| 38 | LaMarcus Aldridge | +2.50 | +1.90 | 40 | +2 | Roy Hibbert | +2.00 |
| 39 | Andre Drummond | +2.50 | +0.40 | 98 | +59 | Steven Adams | +2.00 |
| 40 | PJ Tucker | +2.40 | +1.30 | 54 | +14 | LaMarcus Aldridge | +1.90 |
| 41 | Mario Chalmers | +2.40 | +1.50 | 49 | +8 | Dwight Howard | +1.90 |
| 42 | Brandan Wright | +2.40 | +1.30 | 58 | +16 | Pablo Prigioni | +1.80 |
| 43 | Zach Randolph | +2.30 | +1.30 | 59 | +16 | Marcus Smart | +1.80 |
| 44 | Dwight Howard | +2.30 | +1.90 | 41 | -3 | Jared Dudley | +1.80 |
| 45 | Kelly Olynyk | +2.20 | +2.10 | 36 | -9 | George Hill | +1.70 |
| 46 | Ersan Ilyasova | +2.20 | +0.40 | 100 | +54 | Al Horford | +1.60 |
| 47 | Amir Johnson | +2.10 | +0.80 | 73 | +26 | Andre Iguodala | +1.60 |
| 48 | Khris Middleton | +2.10 | +3.10 | 13 | -35 | Kevin Love | +1.60 |
| 49 | Kyle Lowry | +2.00 | +0.60 | 83 | +34 | Mario Chalmers | +1.50 |
| 50 | Michael CarterWilliams | +2.00 | +2.30 | 31 | -19 | Kris Humphries | +1.50 |

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
| 1 | Shai Gilgeous-Alexander | +12.60 | 2553 |
| 2 | Nikola Jokic | +10.20 | 2737 |
| 3 | Luka Doncic | +9.10 | 2624 |
| 4 | Jalen Brunson | +8.40 | 2726 |
| 5 | Donovan Mitchell | +7.90 | 1943 |
| 6 | Kawhi Leonard | +7.31 | 2330 |
| 7 | Joel Embiid | +7.01 | 1309 |
| 8 | Paul George | +6.90 | 2502 |
| 9 | Tyrese Haliburton | +6.70 | 2224 |
| 10 | De'Aaron Fox | +6.50 | 2659 |
| 11 | Anthony Davis | +6.20 | 2700 |
| 12 | Jayson Tatum | +6.00 | 2645 |
| 13 | Kyrie Irving | +5.80 | 2030 |
| 14 | Derrick White | +5.60 | 2381 |
| 15 | Isaiah Hartenstein | +5.40 | 1896 |
| 16 | Fred VanVleet | +5.30 | 2684 |
| 17 | Jamal Murray | +5.10 | 1861 |
| 18 | Giannis Antetokounmpo | +4.90 | 2567 |
| 19 | Damian Lillard | +4.80 | 2579 |
| 20 | Kevin Durant | +4.60 | 2791 |
| 21 | Alex Caruso | +4.50 | 2040 |
| 22 | Jimmy Butler | +4.40 | 2042 |
| 23 | Devin Booker | +4.30 | 2447 |
| 24 | Stephen Curry | +4.20 | 2421 |
| 25 | Chet Holmgren | +4.10 | 2413 |
| 26 | Trey Murphy III | +4.00 | 1690 |
| 27 | James Harden | +4.00 | 2470 |
| 28 | Rudy Gobert | +3.90 | 2593 |
| 29 | Tyrese Maxey | +3.80 | 2626 |
| 30 | Jusuf Nurkic | +3.80 | 2078 |
| 31 | LeBron James | +3.60 | 2504 |
| 32 | Draymond Green | +3.50 | 1490 |
| 33 | Jalen Williams | +3.40 | 2223 |
| 34 | Kristaps Porzingis | +3.40 | 1690 |
| 35 | T.J. McConnell | +3.26 | 1291 |
| 36 | Franz Wagner | +3.20 | 2337 |
| 37 | Anthony Edwards | +3.10 | 2770 |
| 38 | Jarrett Allen | +3.10 | 2442 |
| 39 | Victor Wembanyama | +3.10 | 2106 |
| 40 | Isaiah Joe | +3.00 | 1445 |
| 41 | Mike Conley | +2.90 | 2193 |
| 42 | Lauri Markkanen | +2.90 | 1820 |
| 43 | Luguentz Dort | +2.80 | 2246 |
| 44 | Brandin Podziemski | +2.80 | 1968 |
| 45 | Dean Wade | +2.70 | 1108 |
| 46 | CJ McCollum | +2.70 | 2159 |
| 47 | Scottie Barnes | +2.60 | 2094 |
| 48 | Sam Hauser | +2.60 | 1741 |
| 49 | Alperen Sengun | +2.50 | 2046 |
| 50 | Andre Drummond | +2.50 | 1351 |

## 2024-25 Regular season — total, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Shai Gilgeous-Alexander | +12.60 | 2598 |
| 2 | Nikola Jokić | +10.39 | 2571 |
| 3 | Luka Dončić | +9.20 | 1769 |
| 4 | Victor Wembanyama | +8.40 | 1527 |
| 5 | Donovan Mitchell | +7.90 | 2232 |
| 6 | James Harden | +7.40 | 2789 |
| 7 | Jayson Tatum | +7.20 | 2624 |
| 8 | Tyrese Haliburton | +7.00 | 2451 |
| 9 | Giannis Antetokounmpo | +6.80 | 2289 |
| 10 | Stephen Curry | +6.50 | 2252 |
| 11 | Darius Garland | +6.40 | 2301 |
| 12 | Derrick White | +6.10 | 2574 |
| 13 | Luke Kornet | +5.90 | 1361 |
| 14 | Rudy Gobert | +5.60 | 2388 |
| 15 | Jimmy Butler | +5.50 | 1746 |
| 16 | Ivica Zubac | +5.40 | 2624 |
| 17 | Luguentz Dort | +5.20 | 2073 |
| 18 | Alperen Sengun | +5.00 | 2394 |
| 19 | Evan Mobley | +4.80 | 2167 |
| 20 | Anthony Edwards | +4.70 | 2871 |
| 21 | Franz Wagner | +4.60 | 2023 |
| 22 | Kyrie Irving | +4.50 | 1804 |
| 23 | Jarrett Allen | +4.35 | 2296 |
| 24 | Amen Thompson | +4.30 | 2225 |
| 25 | Ty Jerome | +4.20 | 1393 |
| 26 | Austin Reaves | +4.10 | 2550 |
| 27 | Payton Pritchard | +4.00 | 2271 |
| 28 | Daniel Gafford | +4.00 | 1226 |
| 29 | Brandin Podziemski | +3.90 | 1716 |
| 30 | Isaiah Joe | +3.80 | 1604 |
| 31 | Tyler Herro | +3.80 | 2725 |
| 32 | Anthony Davis | +3.60 | 1706 |
| 33 | Mike Conley | +3.50 | 1756 |
| 34 | Jaren Jackson Jr. | +3.42 | 2207 |
| 35 | Jalen Brunson | +3.40 | 2301 |
| 36 | Jamal Murray | +3.30 | 2418 |
| 37 | Naz Reid | +3.20 | 2200 |
| 38 | Karl-Anthony Towns | +3.20 | 2517 |
| 39 | Jalen Williams | +3.10 | 2237 |
| 40 | Toumani Camara | +3.10 | 2548 |
| 41 | Draymond Green | +3.00 | 1983 |
| 42 | Kawhi Leonard | +3.00 | 1180 |
| 43 | Norman Powell | +2.90 | 1958 |
| 44 | Keon Ellis | +2.90 | 1948 |
| 45 | Pascal Siakam | +2.80 | 2548 |
| 46 | Cason Wallace | +2.70 | 1876 |
| 47 | Ausar Thompson | +2.70 | 1328 |
| 48 | Domantas Sabonis | +2.60 | 2429 |
| 49 | Tari Eason | +2.60 | 1420 |
| 50 | Cade Cunningham | +2.60 | 2452 |

## 2025-26 Regular season — total, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +12.60 | 1866 |
| 2 | Nikola Jokić | +10.40 | 2265 |
| 3 | Shai Gilgeous-Alexander | +9.20 | 2259 |
| 4 | Kawhi Leonard | +8.50 | 2085 |
| 5 | Derrick White | +8.00 | 2625 |
| 6 | Luka Dončić | +7.60 | 2289 |
| 7 | Chet Holmgren | +7.20 | 1997 |
| 8 | Donovan Mitchell | +7.00 | 2342 |
| 9 | Jamal Murray | +6.90 | 2652 |
| 10 | Collin Gillespie | +6.70 | 2282 |
| 11 | Neemias Queta | +6.50 | 1926 |
| 12 | LaMelo Ball | +6.20 | 2017 |
| 13 | Cade Cunningham | +6.00 | 2172 |
| 14 | Brandon Miller | +5.80 | 1968 |
| 15 | Tyrese Maxey | +5.60 | 2661 |
| 16 | Austin Reaves | +5.50 | 1762 |
| 17 | Paul George | +5.40 | 1135 |
| 18 | Jalen Duren | +5.20 | 1976 |
| 19 | Dyson Daniels | +5.00 | 2520 |
| 20 | Jimmy Butler III | +4.80 | 1182 |
| 21 | Ajay Mitchell | +4.70 | 1473 |
| 22 | Moussa Diabaté | +4.60 | 1899 |
| 23 | Scottie Barnes | +4.50 | 2681 |
| 24 | Mitchell Robinson | +4.40 | 1175 |
| 25 | Isaiah Joe | +4.30 | 1507 |
| 26 | Jalen Brunson | +4.20 | 2590 |
| 27 | Nickeil Alexander-Walker | +4.10 | 2603 |
| 28 | Reed Sheppard | +4.00 | 2147 |
| 29 | Karl-Anthony Towns | +4.00 | 2322 |
| 30 | Rudy Gobert | +3.90 | 2380 |
| 31 | Donte DiVincenzo | +3.90 | 2494 |
| 32 | Stephen Curry | +3.80 | 1329 |
| 33 | Bam Adebayo | +3.70 | 2365 |
| 34 | Evan Mobley | +3.60 | 2074 |
| 35 | Toumani Camara | +3.50 | 2731 |
| 36 | Jrue Holiday | +3.40 | 1560 |
| 37 | Devin Booker | +3.30 | 2146 |
| 38 | OG Anunoby | +3.20 | 2224 |
| 39 | James Harden | +3.20 | 2438 |
| 40 | Donovan Clingan | +3.20 | 2094 |
| 41 | Amen Thompson | +3.10 | 2953 |
| 42 | Jaylen Brown | +3.10 | 2443 |
| 43 | Ausar Thompson | +3.00 | 1896 |
| 44 | Jalen Suggs | +3.00 | 1574 |
| 45 | De'Aaron Fox | +2.90 | 2231 |
| 46 | Jordan Goodwin | +2.90 | 1572 |
| 47 | Kevin Durant | +2.80 | 2840 |
| 48 | Cason Wallace | +2.70 | 2046 |
| 49 | Anthony Edwards | +2.70 | 2137 |
| 50 | Isaiah Hartenstein | +2.60 | 1137 |

## 2023-24 Regular season — offense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +9.10 | 2737 |
| 2 | Shai Gilgeous-Alexander | +7.90 | 2553 |
| 3 | Jalen Brunson | +7.00 | 2726 |
| 4 | Tyrese Haliburton | +6.50 | 2224 |
| 5 | Luka Doncic | +6.30 | 2624 |
| 6 | Donovan Mitchell | +5.90 | 1943 |
| 7 | Devin Booker | +5.60 | 2447 |
| 8 | Kyrie Irving | +5.30 | 2030 |
| 9 | LeBron James | +5.10 | 2504 |
| 10 | Jamal Murray | +4.80 | 1861 |
| 11 | Tyrese Maxey | +4.70 | 2626 |
| 12 | Damian Lillard | +4.50 | 2579 |
| 13 | Kawhi Leonard | +4.30 | 2330 |
| 14 | Stephen Curry | +4.20 | 2421 |
| 15 | Jayson Tatum | +4.00 | 2645 |
| 16 | Giannis Antetokounmpo | +4.00 | 2567 |
| 17 | De'Aaron Fox | +3.80 | 2659 |
| 18 | Jimmy Butler | +3.70 | 2042 |
| 19 | James Harden | +3.60 | 2470 |
| 20 | Paul George | +3.60 | 2502 |
| 21 | DeMar DeRozan | +3.40 | 2989 |
| 22 | Joel Embiid | +3.30 | 1309 |
| 23 | Lauri Markkanen | +3.20 | 1820 |
| 24 | Trae Young | +3.10 | 1942 |
| 25 | Kevin Durant | +3.00 | 2791 |
| 26 | CJ McCollum | +2.90 | 2159 |
| 27 | Anthony Edwards | +2.90 | 2770 |
| 28 | Pascal Siakam | +2.80 | 2658 |
| 29 | Malik Monk | +2.70 | 1872 |
| 30 | Fred VanVleet | +2.70 | 2684 |
| 31 | Collin Sexton | +2.60 | 2075 |
| 32 | Dejounte Murray | +2.50 | 2783 |
| 33 | Scottie Barnes | +2.50 | 2094 |
| 34 | Donte DiVincenzo | +2.40 | 2360 |
| 35 | Anthony Davis | +2.30 | 2700 |
| 36 | Payton Pritchard | +2.30 | 1826 |
| 37 | Malcolm Brogdon | +2.20 | 1121 |
| 38 | D'Angelo Russell | +2.20 | 2484 |
| 39 | Derrick White | +2.10 | 2381 |
| 40 | Julius Randle | +2.00 | 1630 |
| 41 | Bogdan Bogdanovic | +1.90 | 2401 |
| 42 | Terry Rozier | +1.90 | 2040 |
| 43 | Desmond Bane | +1.90 | 1443 |
| 44 | Jalen Williams | +1.80 | 2223 |
| 45 | Brandon Ingram | +1.80 | 2103 |
| 46 | T.J. McConnell | +1.80 | 1291 |
| 47 | Immanuel Quickley | +1.70 | 1985 |
| 48 | Mike Conley | +1.70 | 2193 |
| 49 | Khris Middleton | +1.60 | 1487 |
| 50 | Grayson Allen | +1.60 | 2513 |

## 2024-25 Regular season — offense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.10 | 2571 |
| 2 | Shai Gilgeous-Alexander | +8.00 | 2598 |
| 3 | Stephen Curry | +7.00 | 2252 |
| 4 | Tyrese Haliburton | +6.59 | 2451 |
| 5 | Tyler Herro | +6.30 | 2725 |
| 6 | Ty Jerome | +6.00 | 1393 |
| 7 | Donovan Mitchell | +5.70 | 2232 |
| 8 | Luka Dončić | +5.40 | 1769 |
| 9 | Jayson Tatum | +5.20 | 2624 |
| 10 | Darius Garland | +4.90 | 2301 |
| 11 | LaMelo Ball | +4.70 | 1505 |
| 12 | Jalen Brunson | +4.60 | 2301 |
| 13 | Payton Pritchard | +4.40 | 2271 |
| 14 | Damian Lillard | +4.27 | 2093 |
| 15 | James Harden | +4.10 | 2789 |
| 16 | Jamal Murray | +4.00 | 2418 |
| 17 | Austin Reaves | +3.90 | 2550 |
| 18 | Giannis Antetokounmpo | +3.80 | 2289 |
| 19 | Trae Young | +3.70 | 2739 |
| 20 | Cade Cunningham | +3.60 | 2452 |
| 21 | Kyrie Irving | +3.50 | 1804 |
| 22 | Jimmy Butler | +3.40 | 1746 |
| 23 | DeMar DeRozan | +3.30 | 2768 |
| 24 | Karl-Anthony Towns | +3.20 | 2517 |
| 25 | Kevin Durant | +3.10 | 2265 |
| 26 | Devin Booker | +3.00 | 2795 |
| 27 | Isaiah Joe | +2.90 | 1604 |
| 28 | Anthony Edwards | +2.90 | 2871 |
| 29 | Tyrese Maxey | +2.80 | 1960 |
| 30 | Derrick White | +2.70 | 2574 |
| 31 | Franz Wagner | +2.70 | 2023 |
| 32 | Norman Powell | +2.60 | 1958 |
| 33 | Malik Beasley | +2.50 | 2283 |
| 34 | Christian Braun | +2.50 | 2675 |
| 35 | Jaylen Brown | +2.40 | 2158 |
| 36 | Deni Avdija | +2.30 | 2161 |
| 37 | Domantas Sabonis | +2.30 | 2429 |
| 38 | Michael Porter Jr. | +2.20 | 2593 |
| 39 | Cameron Johnson | +2.20 | 1800 |
| 40 | Ja Morant | +2.10 | 1519 |
| 41 | CJ McCollum | +2.00 | 1832 |
| 42 | Luke Kornet | +2.00 | 1361 |
| 43 | LeBron James | +1.90 | 2444 |
| 44 | Malik Monk | +1.90 | 2054 |
| 45 | Sam Hauser | +1.80 | 1541 |
| 46 | Desmond Bane | +1.80 | 2205 |
| 47 | Harrison Barnes | +1.80 | 2228 |
| 48 | Pascal Siakam | +1.70 | 2548 |
| 49 | Aaron Gordon | +1.70 | 1447 |
| 50 | Collin Sexton | +1.70 | 1758 |

## 2025-26 Regular season — offense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.10 | 2265 |
| 2 | Shai Gilgeous-Alexander | +8.00 | 2259 |
| 3 | Donovan Mitchell | +7.10 | 2342 |
| 4 | Jalen Brunson | +6.60 | 2590 |
| 5 | LaMelo Ball | +6.40 | 2017 |
| 6 | Kawhi Leonard | +6.00 | 2085 |
| 7 | Cade Cunningham | +5.70 | 2172 |
| 8 | Jamal Murray | +5.50 | 2652 |
| 9 | Jimmy Butler III | +5.30 | 1182 |
| 10 | James Harden | +5.00 | 2438 |
| 11 | Luka Dončić | +4.80 | 2289 |
| 12 | Jalen Duren | +4.70 | 1976 |
| 13 | Deni Avdija | +4.50 | 2199 |
| 14 | Tyrese Maxey | +4.30 | 2661 |
| 15 | Payton Pritchard | +4.20 | 2556 |
| 16 | Stephen Curry | +4.10 | 1329 |
| 17 | Kevin Durant | +4.00 | 2840 |
| 18 | De'Aaron Fox | +3.90 | 2231 |
| 19 | Jrue Holiday | +3.80 | 1560 |
| 20 | Grayson Allen | +3.70 | 1467 |
| 21 | Devin Booker | +3.60 | 2146 |
| 22 | Kon Knueppel | +3.50 | 2551 |
| 23 | Duncan Robinson | +3.40 | 2113 |
| 24 | Austin Reaves | +3.30 | 1762 |
| 25 | Collin Gillespie | +3.20 | 2282 |
| 26 | Nickeil Alexander-Walker | +3.10 | 2603 |
| 27 | Victor Wembanyama | +3.00 | 1866 |
| 28 | Brandon Miller | +3.00 | 1968 |
| 29 | Anthony Edwards | +2.90 | 2137 |
| 30 | Reed Sheppard | +2.80 | 2147 |
| 31 | Julius Randle | +2.80 | 2610 |
| 32 | Michael Porter Jr. | +2.70 | 1689 |
| 33 | CJ McCollum | +2.60 | 2263 |
| 34 | Desmond Bane | +2.50 | 2756 |
| 35 | Coby White | +2.50 | 1250 |
| 36 | Alperen Sengun | +2.50 | 2398 |
| 37 | Immanuel Quickley | +2.40 | 2231 |
| 38 | Cam Spencer | +2.30 | 1714 |
| 39 | Trey Murphy III | +2.30 | 2341 |
| 40 | Isaiah Joe | +2.20 | 1507 |
| 41 | Lauri Markkanen | +2.20 | 1443 |
| 42 | Anfernee Simons | +2.10 | 1372 |
| 43 | Luka Garza | +2.00 | 1118 |
| 44 | Jaylen Brown | +2.00 | 2443 |
| 45 | Sam Merrill | +1.90 | 1377 |
| 46 | Paul George | +1.90 | 1135 |
| 47 | Keyonte George | +1.80 | 1786 |
| 48 | Joel Embiid | +1.80 | 1201 |
| 49 | Jalen Johnson | +1.80 | 2532 |
| 50 | Karl-Anthony Towns | +1.80 | 2322 |

## 2023-24 Regular season — defense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +9.40 | 2593 |
| 2 | Isaiah Hartenstein | +7.80 | 1896 |
| 3 | Alex Caruso | +7.20 | 2040 |
| 4 | Dean Wade | +6.80 | 1108 |
| 5 | Draymond Green | +6.30 | 1490 |
| 6 | Jusuf Nurkic | +5.90 | 2078 |
| 7 | Ausar Thompson | +5.60 | 1583 |
| 8 | Aaron Nesmith | +5.30 | 1995 |
| 9 | Kristaps Porzingis | +5.10 | 1690 |
| 10 | Joel Embiid | +4.80 | 1309 |
| 11 | Amen Thompson | +4.70 | 1388 |
| 12 | Nikola Jokic | +4.60 | 2737 |
| 13 | Larry Nance Jr. | +4.40 | 1216 |
| 14 | Toumani Camara | +4.30 | 1739 |
| 15 | Derrick Jones Jr. | +4.20 | 1783 |
| 16 | Derrick White | +4.00 | 2381 |
| 17 | Anthony Davis | +3.90 | 2700 |
| 18 | Luguentz Dort | +3.80 | 2246 |
| 19 | Paul Reed | +3.70 | 1590 |
| 20 | Ivica Zubac | +3.60 | 1795 |
| 21 | Chet Holmgren | +3.50 | 2413 |
| 22 | Evan Mobley | +3.40 | 1532 |
| 23 | Victor Wembanyama | +3.40 | 2106 |
| 24 | Brook Lopez | +3.30 | 2411 |
| 25 | Jarrett Allen | +3.20 | 2442 |
| 26 | Nickeil Alexander-Walker | +3.10 | 1921 |
| 27 | Clint Capela | +3.10 | 1883 |
| 28 | Bam Adebayo | +3.00 | 2416 |
| 29 | Paul George | +2.90 | 2502 |
| 30 | Wendell Carter Jr. | +2.90 | 1406 |
| 31 | Franz Wagner | +2.80 | 2337 |
| 32 | Daniel Gafford | +2.80 | 1814 |
| 33 | Andre Drummond | +2.70 | 1351 |
| 34 | Walker Kessler | +2.60 | 1493 |
| 35 | Kyle Anderson | +2.60 | 1782 |
| 36 | Kawhi Leonard | +2.60 | 2330 |
| 37 | Vince Williams Jr. | +2.50 | 1436 |
| 38 | Matisse Thybulle | +2.50 | 1487 |
| 39 | Herbert Jones | +2.50 | 2321 |
| 40 | Isaiah Joe | +2.40 | 1445 |
| 41 | Myles Turner | +2.40 | 2077 |
| 42 | Naz Reid | +2.40 | 1964 |
| 43 | Dyson Daniels | +2.30 | 1358 |
| 44 | Nic Claxton | +2.30 | 2116 |
| 45 | Jakob Poeltl | +2.27 | 1319 |
| 46 | OG Anunoby | +2.20 | 1702 |
| 47 | Shai Gilgeous-Alexander | +2.10 | 2553 |
| 48 | Brandin Podziemski | +2.10 | 1968 |
| 49 | Al Horford | +2.00 | 1740 |
| 50 | Jalen Suggs | +2.00 | 2025 |

## 2024-25 Regular season — defense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +9.50 | 2388 |
| 2 | Toumani Camara | +7.80 | 2548 |
| 3 | Alperen Sengun | +7.20 | 2394 |
| 4 | Luguentz Dort | +6.80 | 2073 |
| 5 | Ivica Zubac | +6.40 | 2624 |
| 6 | Luke Kornet | +6.00 | 1361 |
| 7 | Evan Mobley | +5.70 | 2167 |
| 8 | Victor Wembanyama | +5.30 | 1527 |
| 9 | Ausar Thompson | +5.20 | 1328 |
| 10 | Amen Thompson | +5.00 | 2225 |
| 11 | Kris Dunn | +4.70 | 1783 |
| 12 | Donovan Clingan | +4.60 | 1324 |
| 13 | Jaxson Hayes | +4.50 | 1093 |
| 14 | Draymond Green | +4.40 | 1983 |
| 15 | Brandin Podziemski | +4.30 | 1716 |
| 16 | Isaiah Hartenstein | +4.10 | 1590 |
| 17 | Brandon Clarke | +4.00 | 1207 |
| 18 | Nicolas Batum | +3.80 | 1367 |
| 19 | Kristaps Porziņģis | +3.70 | 1210 |
| 20 | Jaden McDaniels | +3.70 | 2614 |
| 21 | Anthony Davis | +3.60 | 1706 |
| 22 | Jarrett Allen | +3.50 | 2296 |
| 23 | Walker Kessler | +3.40 | 1740 |
| 24 | Kevon Looney | +3.30 | 1142 |
| 25 | Isaiah Stewart | +3.30 | 1434 |
| 26 | Keon Ellis | +3.20 | 1948 |
| 27 | Cody Martin | +3.10 | 1173 |
| 28 | Zach Edey | +3.10 | 1416 |
| 29 | Jaren Jackson Jr. | +3.00 | 2207 |
| 30 | Jonathan Isaac | +2.90 | 1090 |
| 31 | Scotty Pippen Jr. | +2.90 | 1683 |
| 32 | Cason Wallace | +2.80 | 1876 |
| 33 | Shai Gilgeous-Alexander | +2.80 | 2598 |
| 34 | Kentavious Caldwell-Pope | +2.70 | 2279 |
| 35 | Dyson Daniels | +2.70 | 2571 |
| 36 | P.J. Washington | +2.60 | 1835 |
| 37 | Goga Bitadze | +2.60 | 1430 |
| 38 | Jrue Holiday | +2.50 | 1896 |
| 39 | Donte DiVincenzo | +2.50 | 1606 |
| 40 | Haywood Highsmith | +2.50 | 1818 |
| 41 | Sam Merrill | +2.40 | 1401 |
| 42 | Myles Turner | +2.40 | 2174 |
| 43 | Mike Conley | +2.40 | 1756 |
| 44 | Kenrich Williams | +2.40 | 1132 |
| 45 | Derrick White | +2.30 | 2574 |
| 46 | Jalen Johnson | +2.30 | 1284 |
| 47 | Daniel Gafford | +2.20 | 1226 |
| 48 | Dean Wade | +2.20 | 1252 |
| 49 | Wendell Carter Jr. | +2.10 | 1758 |
| 50 | Jabari Smith Jr. | +2.10 | 1716 |

## 2025-26 Regular season — defense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +9.50 | 1866 |
| 2 | Chet Holmgren | +7.80 | 1997 |
| 3 | Neemias Queta | +7.20 | 1926 |
| 4 | Ausar Thompson | +6.90 | 1896 |
| 5 | Derrick White | +6.60 | 2625 |
| 6 | Ronald Holland II | +6.20 | 1550 |
| 7 | Isaiah Hartenstein | +5.70 | 1137 |
| 8 | Marcus Smart | +5.50 | 1769 |
| 9 | Hugo González | +5.20 | 1084 |
| 10 | Dyson Daniels | +5.10 | 2520 |
| 11 | Rudy Gobert | +4.80 | 2380 |
| 12 | Cason Wallace | +4.70 | 2046 |
| 13 | Ajay Mitchell | +4.60 | 1473 |
| 14 | Mitchell Robinson | +4.40 | 1175 |
| 15 | Baylor Scheierman | +4.30 | 1429 |
| 16 | Javonte Green | +4.20 | 1446 |
| 17 | Jordan Goodwin | +4.10 | 1572 |
| 18 | Toumani Camara | +4.00 | 2731 |
| 19 | Sidy Cissoko | +3.80 | 1435 |
| 20 | OG Anunoby | +3.70 | 2224 |
| 21 | Paul George | +3.60 | 1135 |
| 22 | Scottie Barnes | +3.60 | 2681 |
| 23 | Jamal Shead | +3.50 | 1852 |
| 24 | Jalen Suggs | +3.40 | 1574 |
| 25 | Keon Ellis | +3.40 | 1479 |
| 26 | Jarrett Allen | +3.30 | 1519 |
| 27 | Jaylin Williams | +3.20 | 1277 |
| 28 | Naz Reid | +3.20 | 2007 |
| 29 | Luke Kornet | +3.10 | 1430 |
| 30 | Evan Mobley | +3.00 | 2074 |
| 31 | Sion James | +3.00 | 1843 |
| 32 | Wendell Carter Jr. | +2.90 | 2288 |
| 33 | Josh Okogie | +2.86 | 1354 |
| 34 | Donte DiVincenzo | +2.80 | 2494 |
| 35 | Bam Adebayo | +2.80 | 2365 |
| 36 | Ryan Dunn | +2.70 | 1355 |
| 37 | Ryan Kalkbrenner | +2.60 | 1479 |
| 38 | Collin Gillespie | +2.60 | 2282 |
| 39 | Collin Murray-Boyles | +2.60 | 1246 |
| 40 | Dru Smith | +2.50 | 1141 |
| 41 | Luguentz Dort | +2.50 | 1849 |
| 42 | Jusuf Nurkić | +2.50 | 1083 |
| 43 | Jalen Duren | +2.40 | 1976 |
| 44 | Derrick Jones Jr. | +2.40 | 1350 |
| 45 | Amen Thompson | +2.40 | 2953 |
| 46 | John Konchar | +2.40 | 1115 |
| 47 | Landry Shamet | +2.30 | 1171 |
| 48 | Brandon Miller | +2.30 | 1968 |
| 49 | Donovan Clingan | +2.20 | 2094 |
| 50 | Kris Murray | +2.20 | 1333 |
