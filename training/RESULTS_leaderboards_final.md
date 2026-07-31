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
| total | stage2-0 / l2 | blend | 1149 | 1.166 | +0.791 | +0.887 |
| offense | stage2-0 / l2 | blend | 1183 | 0.659 | +0.873 | +0.930 |
| defense | stage2-0 / huber | blend | 649 | 1.006 | +0.652 | +0.824 |

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
| total | 2013-14 | Playoffs | 99 | 1.916 | 2.538 | +0.416 | +0.347 | 24/30 | 23/30 |
| total | 2013-14 | Regular season | 246 | 0.878 | 0.945 | +0.476 | +0.480 | 24/30 | 22/30 |
| total | 2014-15 | Playoffs | 98 | 1.898 | 2.462 | +0.407 | +0.338 | 21/30 | 21/30 |
| total | 2014-15 | Regular season | 246 | 0.861 | 0.914 | +0.655 | +0.595 | 23/30 | 22/30 |
| offense | 2013-14 | Playoffs | 99 | 0.905 | 1.437 | +0.660 | +0.628 | 26/30 | 22/30 |
| offense | 2013-14 | Regular season | 246 | 0.515 | 0.713 | +0.720 | +0.811 | 24/30 | 22/30 |
| offense | 2014-15 | Playoffs | 98 | 1.003 | 1.686 | +0.618 | +0.444 | 26/30 | 20/30 |
| offense | 2014-15 | Regular season | 246 | 0.560 | 0.722 | +0.618 | +0.490 | 25/30 | 23/30 |
| defense | 2013-14 | Playoffs | 99 | 1.612 | 1.840 | +0.228 | +0.094 | 22/30 | 20/30 |
| defense | 2013-14 | Regular season | 246 | 0.726 | 0.925 | +0.453 | +0.352 | 21/30 | 17/30 |
| defense | 2014-15 | Playoffs | 98 | 1.691 | 2.090 | +0.453 | +0.310 | 20/30 | 15/30 |
| defense | 2014-15 | Regular season | 246 | 0.764 | 0.846 | +0.591 | +0.582 | 23/30 | 19/30 |

## Kendall tau over the top 30, held-out seasons

`tau(true30)` compares the true order of the true top 30 against their
projected order. `tau(union30)` widens the set to the union of the true and
projected top 30, so it also penalises wrongly promoted players.

| target | season | split | pool | tau(true30) | tau(union30) | hits@30 | mean &#124;Δrank&#124; |
|---|---|---|---|---|---|---|---|
| total | 2013-14 | Playoffs | 99 | +0.416 | +0.330 | 24/30 | 11.5 |
| total | 2013-14 | Regular season | 247 | +0.457 | +0.390 | 24/30 | 13.1 |
| total | 2014-15 | Playoffs | 99 | +0.407 | +0.287 | 21/30 | 11.6 |
| total | 2014-15 | Regular season | 247 | +0.651 | +0.442 | 22/30 | 12.8 |
| offense | 2013-14 | Playoffs | 99 | +0.660 | +0.647 | 26/30 | 7.7 |
| offense | 2013-14 | Regular season | 247 | +0.738 | +0.632 | 24/30 | 12.4 |
| offense | 2014-15 | Playoffs | 99 | +0.614 | +0.594 | 26/30 | 7.0 |
| offense | 2014-15 | Regular season | 247 | +0.618 | +0.543 | 25/30 | 9.5 |
| defense | 2013-14 | Playoffs | 99 | +0.228 | +0.164 | 22/30 | 17.3 |
| defense | 2013-14 | Regular season | 247 | +0.471 | +0.374 | 21/30 | 14.8 |
| defense | 2014-15 | Playoffs | 99 | +0.389 | +0.244 | 20/30 | 14.4 |
| defense | 2014-15 | Regular season | 247 | +0.628 | +0.468 | 22/30 | 21.4 |

## 2013-14 Playoffs — total, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.416 &nbsp;·&nbsp; hits@30 24/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +9.14 | +13.20 | 1 | +0 | Chris Paul | +13.20 |
| 2 | LeBron James | +8.49 | +7.70 | 8 | +6 | Draymond Green | +10.80 |
| 3 | Stephen Curry | +7.61 | +10.40 | 3 | +0 | Stephen Curry | +10.40 |
| 4 | Russell Westbrook | +7.12 | +6.40 | 14 | +10 | James Harden | +10.00 |
| 5 | James Harden | +7.00 | +10.00 | 4 | -1 | Paul Millsap | +8.40 |
| 6 | Damian Lillard | +7.00 | +6.90 | 12 | +6 | Vince Carter | +8.10 |
| 7 | Manu Ginobili | +6.98 | +7.90 | 7 | +0 | Manu Ginobili | +7.90 |
| 8 | Patrick Patterson | +6.80 | +1.40 | 45 | +37 | LeBron James | +7.70 |
| 9 | Kevin Durant | +6.65 | +5.60 | 20 | +11 | Greivis Vasquez | +7.30 |
| 10 | Vince Carter | +6.17 | +8.10 | 6 | -4 | Patty Mills | +7.20 |
| 11 | Kawhi Leonard | +6.09 | +6.20 | 16 | +5 | Andray Blatche | +6.90 |
| 12 | Patty Mills | +6.06 | +7.20 | 10 | -2 | Damian Lillard | +6.90 |
| 13 | Kyle Lowry | +5.86 | +4.30 | 31 | +18 | Deron Williams | +6.60 |
| 14 | Greivis Vasquez | +5.70 | +7.30 | 9 | -5 | Russell Westbrook | +6.40 |
| 15 | Danny Green | +5.56 | +6.40 | 15 | +0 | Danny Green | +6.40 |
| 16 | Joe Johnson | +5.04 | +5.30 | 25 | +9 | Kawhi Leonard | +6.20 |
| 17 | Marcin Gortat | +4.73 | +4.80 | 27 | +10 | LaMarcus Aldridge | +6.00 |
| 18 | Deron Williams | +4.71 | +6.60 | 13 | -5 | Chris Andersen | +6.00 |
| 19 | Draymond Green | +4.67 | +10.80 | 2 | -17 | Bradley Beal | +5.70 |
| 20 | Tony Allen | +4.49 | +4.30 | 30 | +10 | Kevin Durant | +5.60 |
| 21 | Kyle Korver | +4.41 | +2.30 | 41 | +20 | Rashard Lewis | +5.50 |
| 22 | Trevor Ariza | +4.37 | +5.50 | 23 | +1 | Tiago Splitter | +5.50 |
| 23 | David West | +4.22 | +3.10 | 37 | +14 | Trevor Ariza | +5.50 |
| 24 | Serge Ibaka | +4.20 | +5.40 | 24 | +0 | Serge Ibaka | +5.40 |
| 25 | Andray Blatche | +4.15 | +6.90 | 11 | -14 | Joe Johnson | +5.30 |
| 26 | LaMarcus Aldridge | +4.03 | +6.00 | 17 | -9 | JJ Redick | +5.00 |
| 27 | Tiago Splitter | +3.99 | +5.50 | 22 | -5 | Marcin Gortat | +4.80 |
| 28 | John Wall | +3.66 | +0.60 | 49 | +21 | Pero Antic | +4.60 |
| 29 | Blake Griffin | +3.30 | +4.50 | 29 | +0 | Blake Griffin | +4.50 |
| 30 | George Hill | +3.20 | +2.20 | 42 | +12 | Tony Allen | +4.30 |
| 31 | Paul Millsap | +3.11 | +8.40 | 5 | -26 | Kyle Lowry | +4.30 |
| 32 | Kevin Garnett | +3.04 | -0.70 | 60 | +28 | Tim Duncan | +3.80 |
| 33 | Bradley Beal | +3.00 | +5.70 | 19 | -14 | Nicolas Batum | +3.60 |
| 34 | David Lee | +2.91 | -3.50 | 89 | +55 | Devin Harris | +3.50 |
| 35 | JJ Redick | +2.81 | +5.00 | 26 | -9 | Mirza Teletovic | +3.50 |
| 36 | Pero Antic | +2.81 | +4.60 | 28 | -8 | Marc Gasol | +3.40 |
| 37 | Tim Duncan | +2.75 | +3.80 | 32 | -5 | David West | +3.10 |
| 38 | Chris Andersen | +2.71 | +6.00 | 18 | -20 | Boris Diaw | +2.80 |
| 39 | Mike Conley | +2.68 | -0.70 | 61 | +22 | Joakim Noah | +2.50 |
| 40 | Paul Pierce | +2.56 | +1.20 | 47 | +7 | Nick Collison | +2.40 |
| 41 | Lance Stephenson | +2.55 | -1.30 | 70 | +29 | Kyle Korver | +2.30 |
| 42 | Nicolas Batum | +2.48 | +3.60 | 33 | -9 | George Hill | +2.20 |
| 43 | Ray Allen | +2.45 | +1.90 | 44 | +1 | Dwight Howard | +2.00 |
| 44 | Joakim Noah | +2.44 | +2.50 | 39 | -5 | Ray Allen | +1.90 |
| 45 | Dwight Howard | +2.40 | +2.00 | 43 | -2 | Patrick Patterson | +1.40 |
| 46 | Boris Diaw | +2.07 | +2.80 | 38 | -8 | Chris Bosh | +1.30 |
| 47 | Devin Harris | +1.88 | +3.50 | 34 | -13 | Paul Pierce | +1.20 |
| 48 | Jeremy Lin | +1.77 | -1.00 | 66 | +18 | DeAndre Jordan | +0.80 |
| 49 | Rashard Lewis | +1.76 | +5.50 | 21 | -28 | John Wall | +0.60 |
| 50 | Mirza Teletovic | +1.66 | +3.50 | 35 | -15 | Chandler Parsons | +0.60 |

### 2013-14 Playoffs — total, Paine's top 30 (in-sample)

> 99 players covered &nbsp;·&nbsp; tau(true30) +0.347 &nbsp;·&nbsp; hits@30 23/30 &nbsp;·&nbsp; MAE 2.538

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +8.08 | +13.20 | 1 | +0 |
| 2 | LeBron James | +7.21 | +7.70 | 8 | +6 |
| 3 | Manu Ginobili | +5.71 | +7.90 | 7 | +4 |
| 4 | Kawhi Leonard | +5.01 | +6.20 | 16 | +12 |
| 5 | Russell Westbrook | +4.98 | +6.40 | 14 | +9 |
| 6 | Danny Green | +4.41 | +6.40 | 15 | +9 |
| 7 | Tiago Splitter | +4.22 | +5.50 | 22 | +15 |
| 8 | Stephen Curry | +4.14 | +10.40 | 3 | -5 |
| 9 | Trevor Ariza | +3.95 | +5.50 | 23 | +14 |
| 10 | Patrick Patterson | +3.62 | +1.40 | 45 | +35 |
| 11 | Bradley Beal | +3.57 | +5.70 | 19 | +8 |
| 12 | Draymond Green | +3.38 | +10.80 | 2 | -10 |
| 13 | Greivis Vasquez | +3.33 | +7.30 | 9 | -4 |
| 14 | Tim Duncan | +3.25 | +3.80 | 32 | +18 |
| 15 | Dwight Howard | +3.17 | +2.00 | 43 | +28 |
| 16 | Patty Mills | +2.91 | +7.20 | 10 | -6 |
| 17 | James Harden | +2.91 | +10.00 | 4 | -13 |
| 18 | Damian Lillard | +2.74 | +6.90 | 12 | -6 |
| 19 | Joe Johnson | +2.73 | +5.30 | 25 | +6 |
| 20 | Kevin Durant | +2.72 | +5.60 | 20 | +0 |
| 21 | Vince Carter | +2.62 | +8.10 | 6 | -15 |
| 22 | Serge Ibaka | +2.22 | +5.40 | 24 | +2 |
| 23 | Kevin Garnett | +1.90 | -0.70 | 60 | +37 |
| 24 | Mike Conley | +1.75 | -0.70 | 61 | +37 |
| 25 | Marcin Gortat | +1.74 | +4.80 | 27 | +2 |
| 26 | Chris Andersen | +1.57 | +6.00 | 18 | -8 |
| 27 | Kyle Lowry | +1.53 | +4.30 | 31 | +4 |
| 28 | Paul Millsap | +1.51 | +8.40 | 5 | -23 |
| 29 | Blake Griffin | +1.40 | +4.50 | 29 | +0 |
| 30 | Chris Bosh | +1.35 | +1.30 | 46 | +16 |

## 2013-14 Regular season — total, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.457 &nbsp;·&nbsp; hits@30 24/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.38 | +11.00 | 1 | +0 | Chris Paul | +11.00 |
| 2 | Kevin Durant | +6.58 | +7.10 | 2 | +0 | Kevin Durant | +7.10 |
| 3 | LeBron James | +6.57 | +4.60 | 15 | +12 | Kawhi Leonard | +6.70 |
| 4 | Paul George | +6.10 | +5.60 | 8 | +4 | Kevin Love | +6.60 |
| 5 | Kawhi Leonard | +5.46 | +6.70 | 3 | -2 | James Harden | +6.10 |
| 6 | Kevin Love | +5.42 | +6.60 | 4 | -2 | Joakim Noah | +5.90 |
| 7 | Manu Ginobili | +5.27 | +5.10 | 9 | +2 | Kyle Lowry | +5.70 |
| 8 | Andre Iguodala | +5.17 | +3.80 | 23 | +15 | Paul George | +5.60 |
| 9 | James Harden | +4.97 | +6.10 | 5 | -4 | Manu Ginobili | +5.10 |
| 10 | Dirk Nowitzki | +4.80 | +4.70 | 13 | +3 | DeMarcus Cousins | +5.00 |
| 11 | Blake Griffin | +4.75 | +3.20 | 34 | +23 | Goran Dragic | +5.00 |
| 12 | Goran Dragic | +4.61 | +5.00 | 11 | -1 | Patty Mills | +4.80 |
| 13 | Andrew Bogut | +4.43 | +3.10 | 37 | +24 | Dirk Nowitzki | +4.70 |
| 14 | Kyle Lowry | +4.26 | +5.70 | 7 | -7 | Danny Green | +4.70 |
| 15 | Carmelo Anthony | +4.19 | +3.80 | 24 | +9 | LeBron James | +4.60 |
| 16 | Isaiah Thomas | +4.08 | +3.90 | 19 | +3 | Anderson Varejao | +4.10 |
| 17 | Jimmy Butler | +4.02 | +3.90 | 20 | +3 | Patrick Beverley | +4.10 |
| 18 | Patty Mills | +3.85 | +4.80 | 12 | -6 | Mario Chalmers | +4.00 |
| 19 | Joakim Noah | +3.77 | +5.90 | 6 | -13 | Isaiah Thomas | +3.90 |
| 20 | Draymond Green | +3.65 | +3.40 | 30 | +10 | Jimmy Butler | +3.90 |
| 21 | Anderson Varejao | +3.62 | +4.10 | 16 | -5 | Mike Conley | +3.80 |
| 22 | LaMarcus Aldridge | +3.54 | +3.40 | 29 | +7 | Kemba Walker | +3.80 |
| 23 | Anthony Davis | +3.46 | +3.50 | 28 | +5 | Andre Iguodala | +3.80 |
| 24 | Chris Bosh | +3.40 | +0.90 | 94 | +70 | Carmelo Anthony | +3.80 |
| 25 | Ricky Rubio | +3.34 | +3.70 | 26 | +1 | Russell Westbrook | +3.70 |
| 26 | DeMarcus Cousins | +3.31 | +5.00 | 10 | -16 | Ricky Rubio | +3.70 |
| 27 | Nikola Pekovic | +3.29 | +3.30 | 31 | +4 | Eric Bledsoe | +3.70 |
| 28 | Paul Millsap | +3.24 | +3.10 | 36 | +8 | Anthony Davis | +3.50 |
| 29 | Mike Conley | +3.24 | +3.80 | 21 | -8 | LaMarcus Aldridge | +3.40 |
| 30 | DeAndre Jordan | +3.22 | +1.80 | 64 | +34 | Draymond Green | +3.40 |
| 31 | Danny Green | +3.17 | +4.70 | 14 | -17 | Nikola Pekovic | +3.30 |
| 32 | Kemba Walker | +3.14 | +3.80 | 22 | -10 | DeMarre Carroll | +3.30 |
| 33 | Russell Westbrook | +3.12 | +3.70 | 25 | -8 | Tiago Splitter | +3.30 |
| 34 | David West | +3.09 | +2.20 | 51 | +17 | Blake Griffin | +3.20 |
| 35 | Nicolas Batum | +2.99 | +1.90 | 59 | +24 | Deron Williams | +3.20 |
| 36 | Deron Williams | +2.99 | +3.20 | 35 | -1 | Paul Millsap | +3.10 |
| 37 | Trevor Ariza | +2.94 | +1.70 | 67 | +30 | Andrew Bogut | +3.10 |
| 38 | Damian Lillard | +2.94 | +2.10 | 56 | +18 | Kris Humphries | +3.00 |
| 39 | Patrick Beverley | +2.93 | +4.10 | 17 | -22 | Klay Thompson | +2.90 |
| 40 | Paul Pierce | +2.87 | +1.90 | 60 | +20 | Ty Lawson | +2.90 |
| 41 | Pablo Prigioni | +2.84 | +1.60 | 73 | +32 | Jae Crowder | +2.90 |
| 42 | Derek Fisher | +2.77 | +2.30 | 50 | +8 | Robin Lopez | +2.90 |
| 43 | George Hill | +2.75 | +2.50 | 48 | +5 | Vince Carter | +2.90 |
| 44 | Wesley Matthews | +2.73 | +2.60 | 46 | +2 | Darren Collison | +2.70 |
| 45 | Marcin Gortat | +2.63 | +2.00 | 58 | +13 | Shane Battier | +2.70 |
| 46 | Nick Collison | +2.62 | +1.20 | 86 | +40 | Wesley Matthews | +2.60 |
| 47 | Kyle Korver | +2.52 | +1.30 | 80 | +33 | Tony Allen | +2.60 |
| 48 | Mario Chalmers | +2.52 | +4.00 | 18 | -30 | George Hill | +2.50 |
| 49 | Robin Lopez | +2.50 | +2.90 | 42 | -7 | Channing Frye | +2.40 |
| 50 | Tiago Splitter | +2.48 | +3.30 | 33 | -17 | Derek Fisher | +2.30 |

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

## 2014-15 Playoffs — total, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.407 &nbsp;·&nbsp; hits@30 21/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Jarrett Jack | +9.28 | +11.30 | 1 | +0 | Jarrett Jack | +11.30 |
| 2 | Jimmy Butler | +8.40 | +9.00 | 4 | +2 | Chris Paul | +11.10 |
| 3 | Tim Duncan | +8.38 | +7.90 | 8 | +5 | AlFarouq Aminu | +11.00 |
| 4 | AlFarouq Aminu | +7.32 | +11.00 | 3 | -1 | Jimmy Butler | +9.00 |
| 5 | Stephen Curry | +7.26 | +8.70 | 7 | +2 | Alan Anderson | +8.90 |
| 6 | Chris Paul | +7.20 | +11.10 | 2 | -4 | Otto Porter Jr. | +8.80 |
| 7 | Alan Anderson | +6.98 | +8.90 | 5 | -2 | Stephen Curry | +8.70 |
| 8 | Mike Dunleavy | +6.54 | +5.30 | 19 | +11 | Tim Duncan | +7.90 |
| 9 | DeAndre Jordan | +6.26 | +3.60 | 33 | +24 | Danny Green | +7.80 |
| 10 | LeBron James | +6.25 | +4.70 | 25 | +15 | Trevor Ariza | +7.30 |
| 11 | Otto Porter Jr. | +5.79 | +8.80 | 6 | -5 | Blake Griffin | +7.20 |
| 12 | JJ Barea | +5.65 | +6.50 | 12 | +0 | JJ Barea | +6.50 |
| 13 | Blake Griffin | +5.09 | +7.20 | 11 | -2 | James Harden | +6.40 |
| 14 | Kyle Korver | +5.05 | +2.30 | 42 | +28 | Manu Ginobili | +6.30 |
| 15 | James Harden | +5.01 | +6.40 | 13 | -2 | Marc Gasol | +6.10 |
| 16 | Brook Lopez | +4.90 | +4.40 | 28 | +12 | Derrick Rose | +5.80 |
| 17 | Pau Gasol | +4.84 | +3.40 | 34 | +17 | Dwight Howard | +5.70 |
| 18 | Monta Ellis | +4.73 | +3.30 | 35 | +17 | CJ McCollum | +5.60 |
| 19 | Paul Millsap | +4.35 | +5.00 | 22 | +3 | Mike Dunleavy | +5.30 |
| 20 | CJ McCollum | +4.11 | +5.60 | 18 | -2 | Timofey Mozgov | +5.30 |
| 21 | Dwight Howard | +4.03 | +5.70 | 17 | -4 | Matt Barnes | +5.00 |
| 22 | Marcin Gortat | +3.98 | +1.70 | 46 | +24 | Paul Millsap | +5.00 |
| 23 | Mike Conley | +3.96 | +5.00 | 24 | +1 | Tony Allen | +5.00 |
| 24 | Al Horford | +3.78 | +3.90 | 30 | +6 | Mike Conley | +5.00 |
| 25 | Kyrie Irving | +3.68 | +1.00 | 52 | +27 | LeBron James | +4.70 |
| 26 | Jeff Teague | +3.65 | +2.90 | 36 | +10 | Tristan Thompson | +4.60 |
| 27 | Derrick Rose | +3.48 | +5.80 | 16 | -11 | Anthony Davis | +4.40 |
| 28 | Andre Iguodala | +3.31 | +2.60 | 39 | +11 | Brook Lopez | +4.40 |
| 29 | Dirk Nowitzki | +3.18 | -0.30 | 63 | +34 | Bradley Beal | +4.20 |
| 30 | Danny Green | +3.12 | +7.80 | 9 | -21 | Al Horford | +3.90 |
| 31 | DeMarre Carroll | +2.89 | +2.10 | 44 | +13 | Ramon Sessions | +3.80 |
| 32 | Bradley Beal | +2.71 | +4.20 | 29 | -3 | Vince Carter | +3.80 |
| 33 | Timofey Mozgov | +2.61 | +5.30 | 20 | -13 | DeAndre Jordan | +3.60 |
| 34 | Josh Smith | +2.32 | +2.30 | 43 | +9 | Pau Gasol | +3.40 |
| 35 | Anthony Davis | +2.28 | +4.40 | 27 | -8 | Monta Ellis | +3.30 |
| 36 | Matt Barnes | +2.26 | +5.00 | 21 | -15 | Jeff Teague | +2.90 |
| 37 | John Wall | +2.19 | +2.30 | 41 | +4 | Iman Shumpert | +2.80 |
| 38 | Tristan Thompson | +2.11 | +4.60 | 26 | -12 | Kawhi Leonard | +2.80 |
| 39 | Manu Ginobili | +2.11 | +6.30 | 14 | -25 | Andre Iguodala | +2.60 |
| 40 | Drew Gooden | +2.09 | -0.80 | 69 | +29 | JR Smith | +2.60 |
| 41 | Joakim Noah | +2.00 | +0.90 | 54 | +13 | John Wall | +2.30 |
| 42 | Vince Carter | +1.80 | +3.80 | 32 | -10 | Kyle Korver | +2.30 |
| 43 | JR Smith | +1.78 | +2.60 | 40 | -3 | Josh Smith | +2.30 |
| 44 | Tony Allen | +1.77 | +5.00 | 23 | -21 | DeMarre Carroll | +2.10 |
| 45 | Ramon Sessions | +1.71 | +3.80 | 31 | -14 | Klay Thompson | +1.80 |
| 46 | OJ Mayo | +1.70 | +1.60 | 48 | +2 | Marcin Gortat | +1.70 |
| 47 | Iman Shumpert | +1.68 | +2.80 | 37 | -10 | Courtney Lee | +1.60 |
| 48 | Trevor Ariza | +1.68 | +7.30 | 10 | -38 | OJ Mayo | +1.60 |
| 49 | JJ Redick | +1.49 | -0.70 | 65 | +16 | Avery Bradley | +1.40 |
| 50 | Boris Diaw | +1.44 | -1.20 | 71 | +21 | Nicolas Batum | +1.30 |

### 2014-15 Playoffs — total, Paine's top 30 (in-sample)

> 98 players covered &nbsp;·&nbsp; tau(true30) +0.338 &nbsp;·&nbsp; hits@30 21/30 &nbsp;·&nbsp; MAE 2.462

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | AlFarouq Aminu | +6.55 | +11.00 | 3 | +2 |
| 2 | Jimmy Butler | +6.25 | +9.00 | 4 | +2 |
| 3 | Chris Paul | +6.06 | +11.10 | 2 | -1 |
| 4 | Stephen Curry | +5.81 | +8.70 | 7 | +3 |
| 5 | Tim Duncan | +5.69 | +7.90 | 8 | +3 |
| 6 | Jarrett Jack | +5.03 | +11.30 | 1 | -5 |
| 7 | CJ McCollum | +4.60 | +5.60 | 18 | +11 |
| 8 | Kyrie Irving | +4.22 | +1.00 | 51 | +43 |
| 9 | Tony Allen | +4.08 | +5.00 | 22 | +13 |
| 10 | Anthony Davis | +4.06 | +4.40 | 27 | +17 |
| 11 | Blake Griffin | +3.94 | +7.20 | 11 | +0 |
| 12 | James Harden | +3.78 | +6.40 | 13 | +1 |
| 13 | LeBron James | +3.62 | +4.70 | 25 | +12 |
| 14 | Alan Anderson | +3.60 | +8.90 | 5 | -9 |
| 15 | Otto Porter Jr. | +3.38 | +8.80 | 6 | -9 |
| 16 | Pau Gasol | +3.27 | +3.40 | 34 | +18 |
| 17 | John Wall | +3.26 | +2.30 | 40 | +23 |
| 18 | Mike Dunleavy | +2.98 | +5.30 | 19 | +1 |
| 19 | Monta Ellis | +2.80 | +3.30 | 35 | +16 |
| 20 | DeAndre Jordan | +2.77 | +3.60 | 33 | +13 |
| 21 | Paul Millsap | +2.63 | +5.00 | 23 | +2 |
| 22 | Andre Iguodala | +2.53 | +2.60 | 39 | +17 |
| 23 | Bradley Beal | +2.37 | +4.20 | 29 | +6 |
| 24 | DeMarre Carroll | +2.07 | +2.10 | 43 | +19 |
| 25 | Derrick Rose | +2.03 | +5.80 | 16 | -9 |
| 26 | Mike Conley | +1.98 | +5.00 | 21 | -5 |
| 27 | Marcin Gortat | +1.88 | +1.70 | 45 | +18 |
| 28 | Dwight Howard | +1.88 | +5.70 | 17 | -11 |
| 29 | Al Horford | +1.85 | +3.90 | 30 | +1 |
| 30 | Jeff Teague | +1.69 | +2.90 | 36 | +6 |

## 2014-15 Regular season — total, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.651 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.52 | +10.60 | 1 | +0 | Chris Paul | +10.60 |
| 2 | LeBron James | +7.28 | +5.10 | 11 | +9 | Kawhi Leonard | +8.90 |
| 3 | Anthony Davis | +7.03 | +8.80 | 3 | +0 | Anthony Davis | +8.80 |
| 4 | Kawhi Leonard | +6.95 | +8.90 | 2 | -2 | James Harden | +7.70 |
| 5 | Draymond Green | +6.82 | +6.50 | 5 | +0 | Draymond Green | +6.50 |
| 6 | James Harden | +6.41 | +7.70 | 4 | -2 | Danny Green | +6.10 |
| 7 | Jimmy Butler | +5.73 | +3.00 | 36 | +29 | George Hill | +5.60 |
| 8 | Russell Westbrook | +5.25 | +5.60 | 8 | +0 | Russell Westbrook | +5.60 |
| 9 | Klay Thompson | +4.82 | +5.30 | 10 | +1 | DeMarcus Cousins | +5.40 |
| 10 | DeMarcus Cousins | +4.67 | +5.40 | 9 | -1 | Klay Thompson | +5.30 |
| 11 | Danny Green | +4.61 | +6.10 | 6 | -5 | LeBron James | +5.10 |
| 12 | Lou Williams | +4.52 | +3.00 | 35 | +23 | Khris Middleton | +4.80 |
| 13 | George Hill | +4.39 | +5.60 | 7 | -6 | Kyrie Irving | +4.60 |
| 14 | Andrew Bogut | +4.14 | +3.70 | 23 | +9 | Kyle Korver | +4.60 |
| 15 | Kyrie Irving | +4.11 | +4.60 | 13 | -2 | DeAndre Jordan | +4.60 |
| 16 | Khris Middleton | +4.07 | +4.80 | 12 | -4 | LaMarcus Aldridge | +4.30 |
| 17 | Tony Allen | +4.06 | +4.30 | 17 | +0 | Tony Allen | +4.30 |
| 18 | Wesley Matthews | +3.95 | +3.60 | 24 | +6 | Nikola Mirotic | +4.20 |
| 19 | Damian Lillard | +3.65 | +2.70 | 42 | +23 | Rudy Gobert | +4.10 |
| 20 | Blake Griffin | +3.57 | +2.00 | 63 | +43 | Marc Gasol | +4.00 |
| 21 | Gordon Hayward | +3.56 | +3.40 | 26 | +5 | Darren Collison | +4.00 |
| 22 | Nikola Mirotic | +3.54 | +4.20 | 18 | -4 | Kyle Lowry | +3.90 |
| 23 | Rudy Gobert | +3.53 | +4.10 | 19 | -4 | Andrew Bogut | +3.70 |
| 24 | Manu Ginobili | +3.47 | +3.20 | 33 | +9 | Wesley Matthews | +3.60 |
| 25 | DeAndre Jordan | +3.42 | +4.60 | 15 | -10 | Jonas Jerebko | +3.60 |
| 26 | Isaiah Thomas | +3.40 | +1.60 | 72 | +46 | Gordon Hayward | +3.40 |
| 27 | John Wall | +3.36 | +2.00 | 64 | +37 | Tim Duncan | +3.30 |
| 28 | Kyle Lowry | +3.33 | +3.90 | 22 | -6 | Paul Millsap | +3.30 |
| 29 | Kyle Korver | +3.27 | +4.60 | 14 | -15 | Marcin Gortat | +3.20 |
| 30 | Tyson Chandler | +3.27 | +2.60 | 48 | +18 | Kevin Love | +3.20 |
| 31 | LaMarcus Aldridge | +3.12 | +4.30 | 16 | -15 | JJ Redick | +3.20 |
| 32 | Marcus Smart | +3.11 | +2.10 | 59 | +27 | Brandon Jennings | +3.20 |
| 33 | Jared Dudley | +3.04 | +1.90 | 67 | +34 | Manu Ginobili | +3.20 |
| 34 | Andre Iguodala | +3.02 | +1.30 | 82 | +48 | Danilo Gallinari | +3.00 |
| 35 | Danilo Gallinari | +2.97 | +3.00 | 34 | -1 | Lou Williams | +3.00 |
| 36 | Paul Millsap | +2.95 | +3.30 | 28 | -8 | Jimmy Butler | +3.00 |
| 37 | Marcin Gortat | +2.87 | +3.20 | 29 | -8 | DeMarre Carroll | +2.90 |
| 38 | Darren Collison | +2.85 | +4.00 | 21 | -17 | Eric Bledsoe | +2.90 |
| 39 | CJ Miles | +2.63 | +1.60 | 75 | +36 | Mike Conley | +2.90 |
| 40 | Mike Conley | +2.60 | +2.90 | 39 | -1 | Zach Randolph | +2.90 |
| 41 | Michael KiddGilchrist | +2.55 | +2.00 | 61 | +20 | Kelly Olynyk | +2.80 |
| 42 | Jeff Teague | +2.48 | +2.70 | 46 | +4 | Damian Lillard | +2.70 |
| 43 | Jae Crowder | +2.48 | +2.10 | 58 | +15 | Jrue Holiday | +2.70 |
| 44 | Kevin Love | +2.38 | +3.20 | 30 | -14 | Zaza Pachulia | +2.70 |
| 45 | Monta Ellis | +2.35 | +1.40 | 78 | +33 | Anthony Morrow | +2.70 |
| 46 | Ersan Ilyasova | +2.28 | +2.50 | 49 | +3 | Jeff Teague | +2.70 |
| 47 | Zaza Pachulia | +2.28 | +2.70 | 44 | -3 | Serge Ibaka | +2.60 |
| 48 | Eric Bledsoe | +2.25 | +2.90 | 38 | -10 | Tyson Chandler | +2.60 |
| 49 | Jrue Holiday | +2.25 | +2.70 | 43 | -6 | Ersan Ilyasova | +2.50 |
| 50 | JJ Redick | +2.24 | +3.20 | 31 | -19 | Devin Harris | +2.50 |

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

## 2013-14 Playoffs — offense, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.660 &nbsp;·&nbsp; hits@30 26/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +8.70 | +10.60 | 1 | +0 | Chris Paul | +10.60 |
| 2 | Damian Lillard | +8.24 | +8.00 | 3 | +1 | Stephen Curry | +9.20 |
| 3 | Stephen Curry | +6.92 | +9.20 | 2 | -1 | Damian Lillard | +8.00 |
| 4 | Russell Westbrook | +6.83 | +6.20 | 6 | +2 | James Harden | +8.00 |
| 5 | LeBron James | +6.25 | +6.90 | 5 | +0 | LeBron James | +6.90 |
| 6 | James Harden | +5.80 | +8.00 | 4 | -2 | Russell Westbrook | +6.20 |
| 7 | Kevin Durant | +5.67 | +5.10 | 7 | +0 | Kevin Durant | +5.10 |
| 8 | Manu Ginobili | +4.48 | +4.40 | 10 | +2 | Jose Calderon | +5.10 |
| 9 | LaMarcus Aldridge | +4.40 | +3.10 | 19 | +10 | Joe Johnson | +4.90 |
| 10 | Kyle Lowry | +4.05 | +3.80 | 11 | +1 | Manu Ginobili | +4.40 |
| 11 | Joe Johnson | +3.91 | +4.90 | 9 | -2 | Kyle Lowry | +3.80 |
| 12 | Patty Mills | +3.66 | +3.50 | 13 | +1 | Blake Griffin | +3.70 |
| 13 | Jose Calderon | +3.15 | +5.10 | 8 | -5 | Patty Mills | +3.50 |
| 14 | Jamal Crawford | +3.07 | +2.90 | 24 | +10 | DeMar DeRozan | +3.40 |
| 15 | Devin Harris | +2.91 | +3.30 | 15 | +0 | Devin Harris | +3.30 |
| 16 | DeMar DeRozan | +2.90 | +3.40 | 14 | -2 | JJ Redick | +3.30 |
| 17 | Patrick Patterson | +2.90 | +2.20 | 29 | +12 | Mirza Teletovic | +3.20 |
| 18 | Deron Williams | +2.85 | +2.80 | 26 | +8 | Ray Allen | +3.20 |
| 19 | Mirza Teletovic | +2.79 | +3.20 | 17 | -2 | LaMarcus Aldridge | +3.10 |
| 20 | Blake Griffin | +2.62 | +3.70 | 12 | -8 | Vince Carter | +3.10 |
| 21 | Bradley Beal | +2.54 | +3.00 | 22 | +1 | Trevor Ariza | +3.10 |
| 22 | Danny Green | +2.40 | +2.90 | 23 | +1 | Bradley Beal | +3.00 |
| 23 | JJ Redick | +2.36 | +3.30 | 16 | -7 | Danny Green | +2.90 |
| 24 | Kawhi Leonard | +2.33 | +1.80 | 32 | +8 | Jamal Crawford | +2.90 |
| 25 | Draymond Green | +2.33 | +2.80 | 25 | +0 | Draymond Green | +2.80 |
| 26 | Vince Carter | +2.23 | +3.10 | 20 | -6 | Deron Williams | +2.80 |
| 27 | Greivis Vasquez | +2.05 | +1.30 | 39 | +12 | Kyle Korver | +2.60 |
| 28 | Nicolas Batum | +2.01 | +1.50 | 35 | +7 | Tony Allen | +2.50 |
| 29 | David West | +1.88 | +2.10 | 30 | +1 | Patrick Patterson | +2.20 |
| 30 | Boris Diaw | +1.87 | +2.00 | 31 | +1 | David West | +2.10 |
| 31 | Trevor Ariza | +1.83 | +3.10 | 21 | -10 | Boris Diaw | +2.00 |
| 32 | Serge Ibaka | +1.81 | +1.20 | 40 | +8 | Kawhi Leonard | +1.80 |
| 33 | Ray Allen | +1.74 | +3.20 | 18 | -15 | Tim Duncan | +1.80 |
| 34 | Kyle Korver | +1.36 | +2.60 | 27 | -7 | Dwight Howard | +1.60 |
| 35 | George Hill | +1.22 | -0.70 | 65 | +30 | Nicolas Batum | +1.50 |
| 36 | Tim Duncan | +1.13 | +1.80 | 33 | -3 | Shane Battier | +1.50 |
| 37 | John Wall | +0.99 | -0.10 | 55 | +18 | Chandler Parsons | +1.40 |
| 38 | Mike Conley | +0.96 | +0.60 | 46 | +8 | Marcin Gortat | +1.30 |
| 39 | Tiago Splitter | +0.92 | +0.50 | 50 | +11 | Greivis Vasquez | +1.30 |
| 40 | Lance Stephenson | +0.89 | -0.60 | 64 | +24 | Serge Ibaka | +1.20 |
| 41 | Tony Parker | +0.83 | +0.60 | 49 | +8 | Chris Bosh | +1.10 |
| 42 | Andre Iguodala | +0.75 | -1.40 | 74 | +32 | Mario Chalmers | +1.10 |
| 43 | Shane Battier | +0.70 | +1.50 | 36 | -7 | Courtney Lee | +0.90 |
| 44 | Tony Allen | +0.69 | +2.50 | 28 | -16 | Andray Blatche | +0.80 |
| 45 | Jeremy Lin | +0.69 | +0.00 | 54 | +9 | Paul Millsap | +0.80 |
| 46 | Dwight Howard | +0.59 | +1.60 | 34 | -12 | Mike Conley | +0.60 |
| 47 | Marcin Gortat | +0.57 | +1.30 | 38 | -9 | Rashard Lewis | +0.60 |
| 48 | Reggie Jackson | +0.56 | -0.60 | 63 | +15 | Chris Andersen | +0.60 |
| 49 | Andray Blatche | +0.56 | +0.80 | 44 | -5 | Tony Parker | +0.60 |
| 50 | David Lee | +0.29 | -1.50 | 75 | +25 | Tiago Splitter | +0.50 |

### 2013-14 Playoffs — offense, Paine's top 30 (in-sample)

> 99 players covered &nbsp;·&nbsp; tau(true30) +0.628 &nbsp;·&nbsp; hits@30 22/30 &nbsp;·&nbsp; MAE 1.437

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | LeBron James | +6.69 | +6.90 | 5 | +4 |
| 2 | Chris Paul | +6.31 | +10.60 | 1 | -1 |
| 3 | Stephen Curry | +4.52 | +9.20 | 2 | -1 |
| 4 | Joe Johnson | +4.46 | +4.90 | 9 | +5 |
| 5 | Russell Westbrook | +4.05 | +6.20 | 6 | +1 |
| 6 | Damian Lillard | +3.92 | +8.00 | 3 | -3 |
| 7 | Vince Carter | +3.45 | +3.10 | 20 | +13 |
| 8 | Dwight Howard | +3.31 | +1.60 | 34 | +26 |
| 9 | James Harden | +3.31 | +8.00 | 4 | -5 |
| 10 | Manu Ginobili | +3.24 | +4.40 | 10 | +0 |
| 11 | Tiago Splitter | +2.79 | +0.50 | 50 | +39 |
| 12 | Kevin Durant | +2.72 | +5.10 | 7 | -5 |
| 13 | Tim Duncan | +2.54 | +1.80 | 33 | +20 |
| 14 | Patrick Patterson | +2.45 | +2.20 | 29 | +15 |
| 15 | Kyle Lowry | +2.44 | +3.80 | 11 | -4 |
| 16 | Blake Griffin | +2.27 | +3.70 | 12 | -4 |
| 17 | JJ Redick | +2.21 | +3.30 | 16 | -1 |
| 18 | Chris Bosh | +2.19 | +1.10 | 41 | +23 |
| 19 | Greivis Vasquez | +2.18 | +1.30 | 39 | +20 |
| 20 | DeMar DeRozan | +2.04 | +3.40 | 14 | -6 |
| 21 | Kawhi Leonard | +1.93 | +1.80 | 32 | +11 |
| 22 | Jamal Crawford | +1.91 | +2.90 | 24 | +2 |
| 23 | Chandler Parsons | +1.65 | +1.40 | 37 | +14 |
| 24 | Jose Calderon | +1.64 | +5.10 | 8 | -16 |
| 25 | Bradley Beal | +1.51 | +3.00 | 22 | -3 |
| 26 | Patty Mills | +1.35 | +3.50 | 13 | -13 |
| 27 | Mirza Teletovic | +1.35 | +3.20 | 17 | -10 |
| 28 | Trevor Ariza | +1.21 | +3.10 | 21 | -7 |
| 29 | Dwyane Wade | +1.20 | +0.00 | 53 | +24 |
| 30 | Deron Williams | +1.19 | +2.80 | 26 | -4 |

## 2013-14 Regular season — offense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.738 &nbsp;·&nbsp; hits@30 24/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Kevin Durant | +6.89 | +7.60 | 1 | +0 | Kevin Durant | +7.60 |
| 2 | LeBron James | +6.52 | +5.80 | 4 | +2 | Chris Paul | +7.10 |
| 3 | Chris Paul | +6.30 | +7.10 | 2 | -1 | James Harden | +6.30 |
| 4 | James Harden | +6.04 | +6.30 | 3 | -1 | LeBron James | +5.80 |
| 5 | Kevin Love | +4.97 | +5.70 | 5 | +0 | Kevin Love | +5.70 |
| 6 | Goran Dragic | +4.62 | +4.80 | 6 | +0 | Goran Dragic | +4.80 |
| 7 | Kyle Lowry | +4.40 | +4.40 | 7 | +0 | Kyle Lowry | +4.40 |
| 8 | Damian Lillard | +4.24 | +3.60 | 12 | +4 | Dirk Nowitzki | +4.40 |
| 9 | Russell Westbrook | +3.99 | +3.30 | 15 | +6 | Carmelo Anthony | +4.20 |
| 10 | Manu Ginobili | +3.89 | +4.00 | 10 | +0 | Manu Ginobili | +4.00 |
| 11 | Isaiah Thomas | +3.79 | +3.50 | 14 | +3 | Patty Mills | +3.90 |
| 12 | Carmelo Anthony | +3.65 | +4.20 | 9 | -3 | Damian Lillard | +3.60 |
| 13 | Mike Conley | +3.31 | +3.50 | 13 | +0 | Mike Conley | +3.50 |
| 14 | Dirk Nowitzki | +3.29 | +4.40 | 8 | -6 | Isaiah Thomas | +3.50 |
| 15 | Blake Griffin | +2.90 | +2.90 | 17 | +2 | Russell Westbrook | +3.30 |
| 16 | Paul George | +2.73 | +2.60 | 22 | +6 | Ty Lawson | +3.20 |
| 17 | Patty Mills | +2.69 | +3.90 | 11 | -6 | Blake Griffin | +2.90 |
| 18 | DJ Augustin | +2.63 | +2.10 | 32 | +14 | Marco Belinelli | +2.80 |
| 19 | Deron Williams | +2.33 | +2.60 | 23 | +4 | Jamal Crawford | +2.80 |
| 20 | Kyrie Irving | +2.28 | +2.30 | 27 | +7 | Wesley Matthews | +2.80 |
| 21 | John Wall | +2.19 | +1.90 | 37 | +16 | Joe Johnson | +2.70 |
| 22 | Ty Lawson | +2.08 | +3.20 | 16 | -6 | Paul George | +2.60 |
| 23 | Jamal Crawford | +2.07 | +2.80 | 19 | -4 | Deron Williams | +2.60 |
| 24 | Wesley Matthews | +2.02 | +2.80 | 20 | -4 | Chandler Parsons | +2.60 |
| 25 | Nikola Pekovic | +2.01 | +1.10 | 68 | +43 | Nick Young | +2.40 |
| 26 | Ricky Rubio | +2.00 | +1.90 | 39 | +13 | Vince Carter | +2.40 |
| 27 | Joe Johnson | +1.94 | +2.70 | 21 | -6 | Kyrie Irving | +2.30 |
| 28 | Klay Thompson | +1.88 | +2.10 | 33 | +5 | Jrue Holiday | +2.20 |
| 29 | Brandan Wright | +1.76 | +1.70 | 41 | +12 | Patrick Beverley | +2.20 |
| 30 | Jrue Holiday | +1.73 | +2.20 | 28 | -2 | Brandon Jennings | +2.20 |
| 31 | Darren Collison | +1.64 | +1.10 | 65 | +34 | Randy Foye | +2.10 |
| 32 | Rudy Gay | +1.59 | +1.10 | 66 | +34 | DJ Augustin | +2.10 |
| 33 | DeMar DeRozan | +1.57 | +1.70 | 42 | +9 | Klay Thompson | +2.10 |
| 34 | Pablo Prigioni | +1.56 | +1.70 | 43 | +9 | Josh McRoberts | +2.00 |
| 35 | Kemba Walker | +1.51 | +1.40 | 52 | +17 | Channing Frye | +2.00 |
| 36 | Patrick Beverley | +1.49 | +2.20 | 29 | -7 | Kyle Korver | +1.90 |
| 37 | Nick Young | +1.47 | +2.40 | 25 | -12 | John Wall | +1.90 |
| 38 | Mario Chalmers | +1.46 | +1.50 | 50 | +12 | Nicolas Batum | +1.90 |
| 39 | Vince Carter | +1.43 | +2.40 | 26 | -13 | Ricky Rubio | +1.90 |
| 40 | Kevin Martin | +1.40 | +0.70 | 87 | +47 | JR Smith | +1.80 |
| 41 | George Hill | +1.38 | +0.30 | 120 | +79 | Brandan Wright | +1.70 |
| 42 | Marco Belinelli | +1.38 | +2.80 | 18 | -24 | DeMar DeRozan | +1.70 |
| 43 | Andre Iguodala | +1.36 | +1.20 | 62 | +19 | Pablo Prigioni | +1.70 |
| 44 | Chandler Parsons | +1.36 | +2.60 | 24 | -20 | Kawhi Leonard | +1.70 |
| 45 | Randy Foye | +1.35 | +2.10 | 31 | -14 | DeMarcus Cousins | +1.70 |
| 46 | Kawhi Leonard | +1.33 | +1.70 | 44 | -2 | Mirza Teletovic | +1.60 |
| 47 | Joakim Noah | +1.28 | +1.50 | 51 | +4 | Jose Calderon | +1.60 |
| 48 | Josh McRoberts | +1.28 | +2.00 | 34 | -14 | Eric Bledsoe | +1.50 |
| 49 | JR Smith | +1.24 | +1.80 | 40 | -9 | Dwyane Wade | +1.50 |
| 50 | Greivis Vasquez | +1.21 | +0.30 | 113 | +63 | Mario Chalmers | +1.50 |

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

## 2014-15 Playoffs — offense, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.614 &nbsp;·&nbsp; hits@30 26/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +6.72 | +8.70 | 1 | +0 | Chris Paul | +8.70 |
| 2 | James Harden | +6.40 | +8.00 | 2 | +0 | James Harden | +8.00 |
| 3 | CJ McCollum | +5.24 | +7.90 | 3 | +0 | CJ McCollum | +7.90 |
| 4 | Monta Ellis | +5.22 | +6.20 | 4 | +0 | Monta Ellis | +6.20 |
| 5 | Jimmy Butler | +4.89 | +5.30 | 8 | +3 | Alan Anderson | +6.10 |
| 6 | Tim Duncan | +4.35 | +5.20 | 9 | +3 | Stephen Curry | +5.70 |
| 7 | Kyrie Irving | +4.33 | +4.10 | 15 | +8 | AlFarouq Aminu | +5.30 |
| 8 | Stephen Curry | +4.23 | +5.70 | 6 | -2 | Jimmy Butler | +5.30 |
| 9 | Jarrett Jack | +4.01 | +3.80 | 16 | +7 | Tim Duncan | +5.20 |
| 10 | AlFarouq Aminu | +3.99 | +5.30 | 7 | -3 | Vince Carter | +5.20 |
| 11 | LeBron James | +3.87 | +3.60 | 19 | +8 | Mike Dunleavy | +4.70 |
| 12 | Mike Dunleavy | +3.55 | +4.70 | 11 | -1 | DeMar DeRozan | +4.60 |
| 13 | DeMar DeRozan | +3.41 | +4.60 | 12 | -1 | Eric Gordon | +4.50 |
| 14 | Alan Anderson | +3.30 | +6.10 | 5 | -9 | JJ Barea | +4.40 |
| 15 | Paul Pierce | +3.02 | +3.60 | 17 | +2 | Kyrie Irving | +4.10 |
| 16 | Derrick Rose | +3.01 | +2.80 | 27 | +11 | Jarrett Jack | +3.80 |
| 17 | Manu Ginobili | +2.66 | +3.60 | 18 | +1 | Paul Pierce | +3.60 |
| 18 | Jeff Teague | +2.50 | +3.10 | 22 | +4 | Manu Ginobili | +3.60 |
| 19 | Blake Griffin | +2.49 | +3.50 | 20 | +1 | LeBron James | +3.60 |
| 20 | Paul Millsap | +2.37 | +2.90 | 25 | +5 | Blake Griffin | +3.50 |
| 21 | Bradley Beal | +2.23 | +2.60 | 28 | +7 | Mike Conley | +3.30 |
| 22 | JJ Barea | +2.20 | +4.40 | 14 | -8 | Jeff Teague | +3.10 |
| 23 | Dirk Nowitzki | +2.12 | +1.40 | 43 | +20 | JR Smith | +3.00 |
| 24 | Damian Lillard | +2.11 | +0.30 | 58 | +34 | DeMarre Carroll | +2.90 |
| 25 | Mike Conley | +2.08 | +3.30 | 21 | -4 | Paul Millsap | +2.90 |
| 26 | John Wall | +1.85 | +1.40 | 44 | +18 | Klay Thompson | +2.90 |
| 27 | Vince Carter | +1.83 | +5.20 | 10 | -17 | Derrick Rose | +2.80 |
| 28 | DeMarre Carroll | +1.76 | +2.90 | 24 | -4 | Bradley Beal | +2.60 |
| 29 | Kyle Korver | +1.75 | +1.10 | 49 | +20 | Josh Smith | +2.60 |
| 30 | Josh Smith | +1.72 | +2.60 | 29 | -1 | Iman Shumpert | +2.60 |
| 31 | Marcin Gortat | +1.56 | +1.40 | 45 | +14 | Otto Porter Jr. | +2.50 |
| 32 | JR Smith | +1.48 | +3.00 | 23 | -9 | OJ Mayo | +2.40 |
| 33 | Boris Diaw | +1.42 | +2.00 | 38 | +5 | Brook Lopez | +2.40 |
| 34 | Otto Porter Jr. | +1.36 | +2.50 | 31 | -3 | Courtney Lee | +2.30 |
| 35 | Brook Lopez | +1.34 | +2.40 | 33 | -2 | Danny Green | +2.30 |
| 36 | Andre Iguodala | +1.29 | +1.90 | 39 | +3 | Jason Terry | +2.10 |
| 37 | Jamal Crawford | +1.26 | +1.60 | 40 | +3 | Tristan Thompson | +2.10 |
| 38 | Trevor Ariza | +1.18 | +1.30 | 48 | +10 | Boris Diaw | +2.00 |
| 39 | Courtney Lee | +1.04 | +2.30 | 34 | -5 | Andre Iguodala | +1.90 |
| 40 | Leandro Barbosa | +1.00 | +0.90 | 50 | +10 | Jamal Crawford | +1.60 |
| 41 | Jason Terry | +0.79 | +2.10 | 36 | -5 | DeAndre Jordan | +1.50 |
| 42 | DeAndre Jordan | +0.77 | +1.50 | 41 | -1 | Matt Barnes | +1.50 |
| 43 | Klay Thompson | +0.63 | +2.90 | 26 | -17 | Dirk Nowitzki | +1.40 |
| 44 | Kawhi Leonard | +0.57 | +1.40 | 46 | +2 | John Wall | +1.40 |
| 45 | Drew Gooden | +0.52 | +0.50 | 52 | +7 | Marcin Gortat | +1.40 |
| 46 | Shaun Livingston | +0.34 | +0.20 | 60 | +14 | Kawhi Leonard | +1.40 |
| 47 | Tristan Thompson | +0.13 | +2.10 | 37 | -10 | Beno Udrih | +1.30 |
| 48 | JJ Redick | +0.13 | -0.20 | 64 | +16 | Trevor Ariza | +1.30 |
| 49 | Beno Udrih | +0.05 | +1.30 | 47 | -2 | Kyle Korver | +1.10 |
| 50 | Dwight Howard | +0.03 | +0.00 | 62 | +12 | Leandro Barbosa | +0.90 |

### 2014-15 Playoffs — offense, Paine's top 30 (in-sample)

> 98 players covered &nbsp;·&nbsp; tau(true30) +0.444 &nbsp;·&nbsp; hits@30 20/30 &nbsp;·&nbsp; MAE 1.686

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +6.15 | +8.70 | 1 | +0 |
| 2 | James Harden | +4.99 | +8.00 | 2 | +0 |
| 3 | Stephen Curry | +4.89 | +5.70 | 6 | +3 |
| 4 | Anthony Davis | +4.28 | -2.90 | 86 | +82 |
| 5 | Tim Duncan | +4.15 | +5.20 | 9 | +4 |
| 6 | Jimmy Butler | +3.97 | +5.30 | 8 | +2 |
| 7 | Blake Griffin | +3.83 | +3.50 | 20 | +13 |
| 8 | Monta Ellis | +3.68 | +6.20 | 4 | -4 |
| 9 | AlFarouq Aminu | +3.58 | +5.30 | 7 | -2 |
| 10 | Kyrie Irving | +3.26 | +4.10 | 15 | +5 |
| 11 | CJ McCollum | +3.18 | +7.90 | 3 | -8 |
| 12 | Alan Anderson | +3.09 | +6.10 | 5 | -7 |
| 13 | LeBron James | +2.41 | +3.60 | 18 | +5 |
| 14 | Courtney Lee | +2.32 | +2.30 | 33 | +19 |
| 15 | Mike Dunleavy | +2.00 | +4.70 | 11 | -4 |
| 16 | Mike Conley | +1.97 | +3.30 | 21 | +5 |
| 17 | DeMarre Carroll | +1.88 | +2.90 | 25 | +8 |
| 18 | Bradley Beal | +1.82 | +2.60 | 27 | +9 |
| 19 | Andre Iguodala | +1.74 | +1.90 | 38 | +19 |
| 20 | Boris Diaw | +1.66 | +2.00 | 37 | +17 |
| 21 | Brook Lopez | +1.58 | +2.40 | 32 | +11 |
| 22 | Marcin Gortat | +1.57 | +1.40 | 44 | +22 |
| 23 | Jarrett Jack | +1.41 | +3.80 | 16 | -7 |
| 24 | Paul Pierce | +1.31 | +3.60 | 19 | -5 |
| 25 | John Wall | +1.19 | +1.40 | 43 | +18 |
| 26 | JJ Barea | +1.15 | +4.40 | 14 | -12 |
| 27 | Otto Porter Jr. | +1.14 | +2.50 | 30 | +3 |
| 28 | Al Horford | +1.13 | -0.50 | 64 | +36 |
| 29 | Kawhi Leonard | +1.07 | +1.40 | 45 | +16 |
| 30 | Dirk Nowitzki | +1.06 | +1.40 | 42 | +12 |

## 2014-15 Regular season — offense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.618 &nbsp;·&nbsp; hits@30 25/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.46 | +8.50 | 1 | +0 | Chris Paul | +8.50 |
| 2 | James Harden | +6.95 | +7.70 | 2 | +0 | James Harden | +7.70 |
| 3 | Russell Westbrook | +6.33 | +6.10 | 3 | +0 | Russell Westbrook | +6.10 |
| 4 | LeBron James | +5.95 | +5.30 | 5 | +1 | Kyrie Irving | +5.50 |
| 5 | Kyrie Irving | +4.99 | +5.50 | 4 | -1 | LeBron James | +5.30 |
| 6 | Lou Williams | +4.82 | +5.20 | 6 | +0 | Lou Williams | +5.20 |
| 7 | Isaiah Thomas | +4.57 | +4.50 | 8 | +1 | Kyle Korver | +4.60 |
| 8 | Damian Lillard | +4.36 | +4.00 | 11 | +3 | Isaiah Thomas | +4.50 |
| 9 | Klay Thompson | +3.87 | +4.30 | 10 | +1 | Anthony Davis | +4.30 |
| 10 | Blake Griffin | +3.64 | +3.20 | 22 | +12 | Klay Thompson | +4.30 |
| 11 | George Hill | +3.44 | +3.90 | 12 | +1 | Damian Lillard | +4.00 |
| 12 | Mike Conley | +3.31 | +2.40 | 32 | +20 | George Hill | +3.90 |
| 13 | Jimmy Butler | +3.16 | +3.20 | 20 | +7 | Carmelo Anthony | +3.80 |
| 14 | Anthony Davis | +3.10 | +4.30 | 9 | -5 | Ty Lawson | +3.80 |
| 15 | Gordon Hayward | +3.08 | +3.20 | 21 | +6 | Kawhi Leonard | +3.70 |
| 16 | Carmelo Anthony | +3.05 | +3.80 | 13 | -3 | Rudy Gay | +3.50 |
| 17 | JJ Redick | +2.81 | +2.50 | 29 | +12 | DeAndre Jordan | +3.40 |
| 18 | Jrue Holiday | +2.67 | +3.30 | 19 | +1 | Kyle Lowry | +3.30 |
| 19 | Kyle Lowry | +2.65 | +3.30 | 18 | -1 | Jrue Holiday | +3.30 |
| 20 | Ty Lawson | +2.64 | +3.80 | 14 | -6 | Jimmy Butler | +3.20 |
| 21 | Kyle Korver | +2.64 | +4.60 | 7 | -14 | Gordon Hayward | +3.20 |
| 22 | Kawhi Leonard | +2.40 | +3.70 | 15 | -7 | Blake Griffin | +3.20 |
| 23 | Jeff Teague | +2.35 | +2.20 | 35 | +12 | Brandon Jennings | +3.10 |
| 24 | Dwyane Wade | +2.32 | +2.00 | 42 | +18 | Danny Green | +3.10 |
| 25 | Brandon Jennings | +2.29 | +3.10 | 23 | -2 | Danilo Gallinari | +2.80 |
| 26 | Khris Middleton | +2.29 | +1.70 | 51 | +25 | Anthony Morrow | +2.70 |
| 27 | Aaron Brooks | +2.25 | +1.60 | 56 | +29 | Chandler Parsons | +2.60 |
| 28 | Rudy Gay | +2.20 | +3.50 | 16 | -12 | Tyreke Evans | +2.60 |
| 29 | Tyreke Evans | +2.17 | +2.60 | 28 | -1 | JJ Redick | +2.50 |
| 30 | Danilo Gallinari | +2.04 | +2.80 | 25 | -5 | LaMarcus Aldridge | +2.40 |
| 31 | Dirk Nowitzki | +2.01 | +2.20 | 34 | +3 | Patrick Patterson | +2.40 |
| 32 | LaMarcus Aldridge | +2.00 | +2.40 | 30 | -2 | Mike Conley | +2.40 |
| 33 | John Wall | +1.99 | +2.10 | 39 | +6 | Wesley Matthews | +2.30 |
| 34 | Anthony Morrow | +1.94 | +2.70 | 26 | -8 | Dirk Nowitzki | +2.20 |
| 35 | Patrick Patterson | +1.84 | +2.40 | 31 | -4 | Jeff Teague | +2.20 |
| 36 | Gerald Green | +1.82 | +2.20 | 36 | +0 | Gerald Green | +2.20 |
| 37 | Reggie Jackson | +1.80 | +2.00 | 41 | +4 | Devin Harris | +2.10 |
| 38 | Eric Gordon | +1.74 | +0.50 | 89 | +51 | JR Smith | +2.10 |
| 39 | Darren Collison | +1.70 | +1.70 | 55 | +16 | John Wall | +2.10 |
| 40 | Chandler Parsons | +1.59 | +2.60 | 27 | -13 | Ersan Ilyasova | +2.10 |
| 41 | Danny Green | +1.58 | +3.10 | 24 | -17 | Reggie Jackson | +2.00 |
| 42 | Wesley Matthews | +1.44 | +2.30 | 33 | -9 | Dwyane Wade | +2.00 |
| 43 | Mo Williams | +1.32 | +1.20 | 65 | +22 | DeMarre Carroll | +1.90 |
| 44 | Jamal Crawford | +1.22 | +1.10 | 69 | +25 | Nikola Mirotic | +1.90 |
| 45 | Ed Davis | +1.20 | +1.20 | 64 | +19 | Goran Dragic | +1.90 |
| 46 | JJ Barea | +1.15 | +1.90 | 46 | +0 | JJ Barea | +1.90 |
| 47 | Marc Gasol | +1.15 | +1.40 | 60 | +13 | Joe Johnson | +1.80 |
| 48 | DeAndre Jordan | +1.15 | +3.40 | 17 | -31 | Luol Deng | +1.80 |
| 49 | Manu Ginobili | +1.15 | +1.70 | 54 | +5 | Jae Crowder | +1.80 |
| 50 | Paul Millsap | +1.13 | +1.00 | 70 | +20 | Eric Bledsoe | +1.70 |

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

## 2013-14 Playoffs — defense, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.228 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Danny Green | +4.66 | +3.50 | 19 | +18 | Draymond Green | +8.00 |
| 2 | Trevor Ariza | +4.39 | +2.40 | 26 | +24 | Paul Millsap | +7.60 |
| 3 | Greivis Vasquez | +4.15 | +6.00 | 6 | +3 | Pero Antic | +6.50 |
| 4 | Kawhi Leonard | +4.01 | +4.40 | 12 | +8 | Nick Collison | +6.10 |
| 5 | Marcin Gortat | +3.42 | +3.50 | 20 | +15 | Andray Blatche | +6.10 |
| 6 | Manu Ginobili | +3.34 | +3.40 | 21 | +15 | Greivis Vasquez | +6.00 |
| 7 | Pero Antic | +3.30 | +6.50 | 3 | -4 | Chris Andersen | +5.40 |
| 8 | Andray Blatche | +3.21 | +6.10 | 5 | -3 | Joakim Noah | +5.30 |
| 9 | John Wall | +3.17 | +0.80 | 41 | +32 | Vince Carter | +5.00 |
| 10 | Paul Millsap | +3.16 | +7.60 | 2 | -8 | Tiago Splitter | +5.00 |
| 11 | Serge Ibaka | +3.04 | +4.20 | 13 | +2 | Rashard Lewis | +4.90 |
| 12 | George Hill | +3.02 | +2.80 | 23 | +11 | Kawhi Leonard | +4.40 |
| 13 | Joakim Noah | +3.01 | +5.30 | 8 | -5 | Serge Ibaka | +4.20 |
| 14 | Chris Paul | +2.93 | +2.70 | 24 | +10 | Ian Mahinmi | +4.20 |
| 15 | Draymond Green | +2.84 | +8.00 | 1 | -14 | Zach Randolph | +4.20 |
| 16 | David West | +2.63 | +1.00 | 39 | +23 | Marc Gasol | +4.10 |
| 17 | Kyle Korver | +2.62 | -0.40 | 62 | +45 | Deron Williams | +3.80 |
| 18 | Tiago Splitter | +2.58 | +5.00 | 10 | -8 | Patty Mills | +3.70 |
| 19 | LeBron James | +2.48 | +0.80 | 42 | +23 | Danny Green | +3.50 |
| 20 | Rashard Lewis | +2.36 | +4.90 | 11 | -9 | Marcin Gortat | +3.50 |
| 21 | Vince Carter | +2.35 | +5.00 | 9 | -12 | Manu Ginobili | +3.40 |
| 22 | Kevin Garnett | +2.28 | +0.40 | 51 | +29 | LaMarcus Aldridge | +2.90 |
| 23 | Patty Mills | +2.24 | +3.70 | 18 | -5 | George Hill | +2.80 |
| 24 | Paul Pierce | +2.13 | +1.50 | 34 | +10 | Chris Paul | +2.70 |
| 25 | Nick Collison | +2.13 | +6.10 | 4 | -21 | Bradley Beal | +2.70 |
| 26 | Kevin Durant | +2.10 | +0.50 | 48 | +22 | Trevor Ariza | +2.40 |
| 27 | David Lee | +2.02 | -2.00 | 87 | +60 | Nicolas Batum | +2.20 |
| 28 | Deron Williams | +1.98 | +3.80 | 17 | -11 | James Harden | +1.90 |
| 29 | Chris Andersen | +1.96 | +5.40 | 7 | -22 | Tim Duncan | +1.90 |
| 30 | Tim Duncan | +1.89 | +1.90 | 29 | -1 | Nene | +1.80 |
| 31 | Zach Randolph | +1.89 | +4.20 | 15 | -16 | Tony Allen | +1.80 |
| 32 | Bradley Beal | +1.88 | +2.70 | 25 | -7 | Kendrick Perkins | +1.80 |
| 33 | Patrick Patterson | +1.81 | -0.80 | 64 | +31 | JJ Redick | +1.70 |
| 34 | Dwyane Wade | +1.77 | -1.80 | 81 | +47 | Paul Pierce | +1.50 |
| 35 | Alan Anderson | +1.74 | +0.50 | 47 | +12 | Kirk Hinrich | +1.40 |
| 36 | Nene | +1.62 | +1.80 | 30 | -6 | Roy Hibbert | +1.30 |
| 37 | Lance Stephenson | +1.59 | -0.80 | 66 | +29 | Thabo Sefolosha | +1.30 |
| 38 | Tony Allen | +1.42 | +1.80 | 31 | -7 | Stephen Curry | +1.20 |
| 39 | Kendrick Perkins | +1.26 | +1.80 | 32 | -7 | David West | +1.00 |
| 40 | Jeremy Lin | +1.23 | -1.00 | 72 | +32 | DeAndre Jordan | +0.90 |
| 41 | Joe Johnson | +1.14 | +0.40 | 50 | +9 | John Wall | +0.80 |
| 42 | JJ Redick | +1.05 | +1.70 | 33 | -9 | LeBron James | +0.80 |
| 43 | Kyle Lowry | +1.04 | +0.50 | 46 | +3 | Boris Diaw | +0.80 |
| 44 | Matt Barnes | +1.03 | -2.20 | 88 | +44 | Blake Griffin | +0.80 |
| 45 | Mike Conley | +0.99 | -1.30 | 75 | +30 | Reggie Jackson | +0.70 |
| 46 | Marc Gasol | +0.93 | +4.10 | 16 | -30 | Kyle Lowry | +0.50 |
| 47 | Derek Fisher | +0.91 | -1.30 | 74 | +27 | Alan Anderson | +0.50 |
| 48 | Stephen Curry | +0.89 | +1.20 | 38 | -10 | Kevin Durant | +0.50 |
| 49 | LaMarcus Aldridge | +0.79 | +2.90 | 22 | -27 | Dwight Howard | +0.40 |
| 50 | DeAndre Jordan | +0.77 | +0.90 | 40 | -10 | Joe Johnson | +0.40 |

### 2013-14 Playoffs — defense, Paine's top 30 (in-sample)

> 99 players covered &nbsp;·&nbsp; tau(true30) +0.094 &nbsp;·&nbsp; hits@30 20/30 &nbsp;·&nbsp; MAE 1.840

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Danny Green | +3.39 | +3.50 | 19 | +18 |
| 2 | Pero Antic | +3.12 | +6.50 | 3 | +1 |
| 3 | Kawhi Leonard | +3.08 | +4.40 | 12 | +9 |
| 4 | Paul Millsap | +2.79 | +7.60 | 2 | -2 |
| 5 | Draymond Green | +2.77 | +8.00 | 1 | -4 |
| 6 | Trevor Ariza | +2.74 | +2.40 | 26 | +20 |
| 7 | Manu Ginobili | +2.46 | +3.40 | 21 | +14 |
| 8 | John Wall | +2.42 | +0.80 | 41 | +33 |
| 9 | Serge Ibaka | +2.33 | +4.20 | 13 | +4 |
| 10 | Joakim Noah | +2.13 | +5.30 | 8 | -2 |
| 11 | Bradley Beal | +2.06 | +2.70 | 25 | +14 |
| 12 | Marcin Gortat | +2.04 | +3.50 | 20 | +8 |
| 13 | DeAndre Jordan | +1.81 | +0.90 | 40 | +27 |
| 14 | Chris Andersen | +1.81 | +5.40 | 7 | -7 |
| 15 | Chris Paul | +1.76 | +2.70 | 24 | +9 |
| 16 | Patty Mills | +1.56 | +3.70 | 18 | +2 |
| 17 | David West | +1.50 | +1.00 | 39 | +22 |
| 18 | Tony Allen | +1.46 | +1.80 | 31 | +13 |
| 19 | Tiago Splitter | +1.43 | +5.00 | 10 | -9 |
| 20 | Kevin Garnett | +1.25 | +0.40 | 51 | +31 |
| 21 | Kyle Korver | +1.25 | -0.40 | 62 | +41 |
| 22 | Patrick Patterson | +1.17 | -0.80 | 64 | +42 |
| 23 | George Hill | +1.16 | +2.80 | 23 | +0 |
| 24 | Greivis Vasquez | +1.15 | +6.00 | 6 | -18 |
| 25 | Nene | +1.14 | +1.80 | 30 | +5 |
| 26 | Mike Conley | +0.97 | -1.30 | 75 | +49 |
| 27 | Marc Gasol | +0.97 | +4.10 | 16 | -11 |
| 28 | Russell Westbrook | +0.93 | +0.20 | 54 | +26 |
| 29 | Lance Stephenson | +0.86 | -0.80 | 66 | +37 |
| 30 | Ian Mahinmi | +0.78 | +4.20 | 14 | -16 |

## 2013-14 Regular season — defense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.471 &nbsp;·&nbsp; hits@30 21/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Andrew Bogut | +4.54 | +4.40 | 4 | +3 | Kawhi Leonard | +5.00 |
| 2 | Tiago Splitter | +4.13 | +4.20 | 6 | +4 | Draymond Green | +4.60 |
| 3 | Draymond Green | +3.88 | +4.60 | 2 | -1 | Joakim Noah | +4.50 |
| 4 | Marcin Gortat | +3.72 | +2.50 | 29 | +25 | Andrew Bogut | +4.40 |
| 5 | Danny Green | +3.67 | +4.00 | 7 | +2 | Michael KiddGilchrist | +4.40 |
| 6 | Anderson Varejao | +3.55 | +3.60 | 10 | +4 | Tiago Splitter | +4.20 |
| 7 | Joakim Noah | +3.53 | +4.50 | 3 | -4 | Danny Green | +4.00 |
| 8 | Kevin Garnett | +3.52 | +3.50 | 11 | +3 | Chris Paul | +3.90 |
| 9 | Paul George | +3.51 | +2.90 | 21 | +12 | Nene | +3.80 |
| 10 | Kawhi Leonard | +3.48 | +5.00 | 1 | -9 | Anderson Varejao | +3.60 |
| 11 | CJ Watson | +3.39 | +3.20 | 17 | +6 | Kevin Garnett | +3.50 |
| 12 | Nene | +3.37 | +3.80 | 9 | -3 | Nick Calathes | +3.50 |
| 13 | Tony Allen | +3.34 | +2.40 | 31 | +18 | Ian Mahinmi | +3.50 |
| 14 | Tim Duncan | +3.33 | +3.00 | 18 | +4 | Jimmy Butler | +3.40 |
| 15 | Roy Hibbert | +3.18 | +3.40 | 15 | +0 | Roy Hibbert | +3.40 |
| 16 | Jae Crowder | +3.03 | +3.00 | 19 | +3 | DeMarcus Cousins | +3.30 |
| 17 | DeMarcus Cousins | +3.02 | +3.30 | 16 | -1 | CJ Watson | +3.20 |
| 18 | Michael KiddGilchrist | +2.97 | +4.40 | 5 | -13 | Tim Duncan | +3.00 |
| 19 | Chris Bosh | +2.89 | +1.00 | 75 | +56 | Jae Crowder | +3.00 |
| 20 | LaMarcus Aldridge | +2.88 | +2.20 | 37 | +17 | Kris Humphries | +3.00 |
| 21 | Ian Mahinmi | +2.87 | +3.50 | 13 | -8 | Paul George | +2.90 |
| 22 | Iman Shumpert | +2.86 | +1.00 | 73 | +51 | Marc Gasol | +2.80 |
| 23 | Paul Pierce | +2.84 | +1.80 | 50 | +27 | Paul Millsap | +2.70 |
| 24 | Andre Iguodala | +2.75 | +2.60 | 25 | +1 | Shane Battier | +2.70 |
| 25 | Jimmy Butler | +2.75 | +3.40 | 14 | -11 | Andre Iguodala | +2.60 |
| 26 | Paul Millsap | +2.71 | +2.70 | 23 | -3 | DeMarre Carroll | +2.60 |
| 27 | Manu Ginobili | +2.67 | +1.10 | 64 | +37 | Mario Chalmers | +2.50 |
| 28 | Kirk Hinrich | +2.62 | +2.10 | 42 | +14 | Samuel Dalembert | +2.50 |
| 29 | David West | +2.59 | +1.50 | 57 | +28 | Marcin Gortat | +2.50 |
| 30 | Ersan Ilyasova | +2.53 | +0.70 | 90 | +60 | Victor Oladipo | +2.40 |
| 31 | Al Jefferson | +2.46 | +1.80 | 47 | +16 | Tony Allen | +2.40 |
| 32 | Derek Fisher | +2.39 | +2.00 | 43 | +11 | Dwight Howard | +2.40 |
| 33 | Ricky Rubio | +2.35 | +1.80 | 49 | +16 | Serge Ibaka | +2.30 |
| 34 | Robin Lopez | +2.35 | +2.00 | 44 | +10 | Anthony Davis | +2.30 |
| 35 | Kemba Walker | +2.31 | +2.30 | 35 | +0 | Kemba Walker | +2.30 |
| 36 | Nick Calathes | +2.28 | +3.50 | 12 | -24 | Thabo Sefolosha | +2.30 |
| 37 | Kosta Koufos | +2.28 | +2.10 | 41 | +4 | LaMarcus Aldridge | +2.20 |
| 38 | Marc Gasol | +2.28 | +2.80 | 22 | -16 | Nikola Pekovic | +2.20 |
| 39 | Chris Paul | +2.25 | +3.90 | 8 | -31 | Eric Bledsoe | +2.20 |
| 40 | Darrell Arthur | +2.24 | +1.90 | 45 | +5 | George Hill | +2.10 |
| 41 | Shane Battier | +2.18 | +2.70 | 24 | -17 | Kosta Koufos | +2.10 |
| 42 | Timofey Mozgov | +2.06 | +1.10 | 68 | +26 | Kirk Hinrich | +2.10 |
| 43 | Gerald Wallace | +2.05 | +1.60 | 52 | +9 | Derek Fisher | +2.00 |
| 44 | DeMarre Carroll | +2.03 | +2.60 | 26 | -18 | Robin Lopez | +2.00 |
| 45 | George Hill | +1.99 | +2.10 | 40 | -5 | Darrell Arthur | +1.90 |
| 46 | Josh Smith | +1.98 | +0.80 | 84 | +38 | Patrick Beverley | +1.90 |
| 47 | Miles Plumlee | +1.96 | +1.60 | 56 | +9 | Al Jefferson | +1.80 |
| 48 | Nick Collison | +1.91 | +0.70 | 88 | +40 | Jeremy Lin | +1.80 |
| 49 | Chris Andersen | +1.91 | +1.60 | 55 | +6 | Ricky Rubio | +1.80 |
| 50 | Amir Johnson | +1.90 | +1.20 | 63 | +13 | Paul Pierce | +1.80 |

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

## 2014-15 Playoffs — defense, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.389 &nbsp;·&nbsp; hits@30 20/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Tony Allen | +5.08 | +5.00 | 10 | +9 | Jarrett Jack | +7.50 |
| 2 | Jarrett Jack | +5.07 | +7.50 | 1 | -1 | Anthony Davis | +7.20 |
| 3 | Al Horford | +4.80 | +4.40 | 12 | +9 | Timofey Mozgov | +6.90 |
| 4 | Pau Gasol | +4.73 | +3.50 | 16 | +12 | Otto Porter Jr. | +6.30 |
| 5 | DeAndre Jordan | +4.68 | +2.10 | 29 | +24 | Trevor Ariza | +6.10 |
| 6 | Timofey Mozgov | +4.64 | +6.90 | 3 | -3 | AlFarouq Aminu | +5.80 |
| 7 | AlFarouq Aminu | +4.43 | +5.80 | 6 | -1 | Dwight Howard | +5.70 |
| 8 | Anthony Davis | +4.38 | +7.20 | 2 | -6 | Danny Green | +5.50 |
| 9 | Dwight Howard | +4.13 | +5.70 | 7 | -2 | Marc Gasol | +5.30 |
| 10 | Tim Duncan | +4.06 | +2.70 | 23 | +13 | Tony Allen | +5.00 |
| 11 | Kyle Korver | +3.94 | +1.10 | 42 | +31 | Nene | +4.70 |
| 12 | Otto Porter Jr. | +3.90 | +6.30 | 4 | -8 | Al Horford | +4.40 |
| 13 | Matt Barnes | +3.59 | +3.40 | 17 | +4 | Jimmy Butler | +3.70 |
| 14 | Nene | +3.55 | +4.70 | 11 | -3 | Blake Griffin | +3.70 |
| 15 | Danny Green | +3.40 | +5.50 | 8 | -7 | Ramon Sessions | +3.50 |
| 16 | Alan Anderson | +3.29 | +2.80 | 20 | +4 | Pau Gasol | +3.50 |
| 17 | Andrew Bogut | +3.29 | +1.90 | 35 | +18 | Matt Barnes | +3.40 |
| 18 | Jimmy Butler | +3.26 | +3.70 | 13 | -5 | Stephen Curry | +3.00 |
| 19 | Drew Gooden | +2.77 | -1.30 | 72 | +53 | Derrick Rose | +3.00 |
| 20 | Harrison Barnes | +2.76 | +1.00 | 43 | +23 | Alan Anderson | +2.80 |
| 21 | Blake Griffin | +2.69 | +3.70 | 14 | -7 | Avery Bradley | +2.80 |
| 22 | JJ Barea | +2.68 | +2.10 | 31 | +9 | Manu Ginobili | +2.70 |
| 23 | Stephen Curry | +2.66 | +3.00 | 18 | -5 | Tim Duncan | +2.70 |
| 24 | OJ Mayo | +2.58 | -0.90 | 67 | +43 | Matthew Dellavedova | +2.50 |
| 25 | Mike Dunleavy | +2.48 | +0.60 | 49 | +24 | Tristan Thompson | +2.50 |
| 26 | John Henson | +2.44 | +2.30 | 28 | +2 | Chris Paul | +2.50 |
| 27 | LeBron James | +2.39 | +1.10 | 41 | +14 | Thaddeus Young | +2.30 |
| 28 | Brook Lopez | +2.28 | +2.00 | 34 | +6 | John Henson | +2.30 |
| 29 | Joakim Noah | +2.25 | +2.10 | 33 | +4 | DeAndre Jordan | +2.10 |
| 30 | Thaddeus Young | +2.18 | +2.30 | 27 | -3 | Festus Ezeli | +2.10 |
| 31 | Mike Conley | +2.15 | +1.70 | 39 | +8 | JJ Barea | +2.10 |
| 32 | Iman Shumpert | +1.86 | +0.20 | 55 | +23 | Paul Millsap | +2.10 |
| 33 | Trevor Ariza | +1.75 | +6.10 | 5 | -28 | Joakim Noah | +2.10 |
| 34 | Marc Gasol | +1.75 | +5.30 | 9 | -25 | Brook Lopez | +2.00 |
| 35 | DeMarre Carroll | +1.72 | -0.80 | 66 | +31 | Andrew Bogut | +1.90 |
| 36 | Kent Bazemore | +1.71 | +1.80 | 37 | +1 | Pero Antic | +1.90 |
| 37 | Festus Ezeli | +1.65 | +2.10 | 30 | -7 | Kent Bazemore | +1.80 |
| 38 | Ramon Sessions | +1.61 | +3.50 | 15 | -23 | Bradley Beal | +1.70 |
| 39 | Marcin Gortat | +1.61 | +0.30 | 53 | +14 | Mike Conley | +1.70 |
| 40 | Kyle Lowry | +1.52 | +0.50 | 52 | +12 | Kawhi Leonard | +1.40 |
| 41 | Giannis Antetokounmpo | +1.45 | +0.60 | 50 | +9 | LeBron James | +1.10 |
| 42 | JR Smith | +1.43 | -0.40 | 61 | +19 | Kyle Korver | +1.10 |
| 43 | Josh Smith | +1.39 | -0.30 | 60 | +17 | Harrison Barnes | +1.00 |
| 44 | Derrick Rose | +1.37 | +3.00 | 19 | -25 | Shaun Livingston | +0.90 |
| 45 | Pero Antic | +1.35 | +1.90 | 36 | -9 | John Wall | +0.90 |
| 46 | Paul Millsap | +1.26 | +2.10 | 32 | -14 | Nicolas Batum | +0.80 |
| 47 | Matthew Dellavedova | +1.22 | +2.50 | 24 | -23 | Bojan Bogdanovic | +0.70 |
| 48 | Chris Paul | +1.01 | +2.50 | 26 | -22 | Andre Iguodala | +0.70 |
| 49 | Pablo Prigioni | +0.97 | -1.60 | 75 | +26 | Mike Dunleavy | +0.60 |
| 50 | Tony Snell | +0.95 | -2.80 | 83 | +33 | Giannis Antetokounmpo | +0.60 |

### 2014-15 Playoffs — defense, Paine's top 30 (in-sample)

> 98 players covered &nbsp;·&nbsp; tau(true30) +0.310 &nbsp;·&nbsp; hits@30 15/30 &nbsp;·&nbsp; MAE 2.090

| pos | Paine's pick | eR | true | true rank | Δrank |
|---:|---|---:|---:|---:|---:|
| 1 | Tony Allen | +5.01 | +5.00 | 10 | +9 |
| 2 | Jarrett Jack | +3.62 | +7.50 | 1 | -1 |
| 3 | Pau Gasol | +3.02 | +3.50 | 15 | +12 |
| 4 | AlFarouq Aminu | +2.97 | +5.80 | 6 | +2 |
| 5 | Jimmy Butler | +2.29 | +3.70 | 13 | +8 |
| 6 | Andrew Bogut | +2.28 | +1.90 | 35 | +29 |
| 7 | Otto Porter Jr. | +2.24 | +6.30 | 4 | -3 |
| 8 | Paul Millsap | +2.24 | +2.10 | 29 | +21 |
| 9 | John Wall | +2.07 | +0.90 | 44 | +35 |
| 10 | Dwight Howard | +2.05 | +5.70 | 7 | -3 |
| 11 | Kyle Korver | +2.03 | +1.10 | 42 | +31 |
| 12 | DeAndre Jordan | +1.99 | +2.10 | 32 | +20 |
| 13 | Drew Gooden | +1.97 | -1.30 | 69 | +56 |
| 14 | Iman Shumpert | +1.72 | +0.20 | 54 | +40 |
| 15 | Danny Green | +1.71 | +5.50 | 8 | -7 |
| 16 | Matt Barnes | +1.68 | +3.40 | 17 | +1 |
| 17 | Joakim Noah | +1.65 | +2.10 | 33 | +16 |
| 18 | Tim Duncan | +1.54 | +2.70 | 23 | +5 |
| 19 | Nikola Mirotic | +1.51 | -2.00 | 77 | +58 |
| 20 | Derrick Rose | +1.42 | +3.00 | 19 | -1 |
| 21 | CJ McCollum | +1.41 | -2.40 | 80 | +59 |
| 22 | LeBron James | +1.21 | +1.10 | 41 | +19 |
| 23 | OJ Mayo | +1.11 | -0.90 | 66 | +43 |
| 24 | Mike Dunleavy | +0.99 | +0.60 | 50 | +26 |
| 25 | Kyrie Irving | +0.96 | -3.10 | 84 | +59 |
| 26 | Stephen Curry | +0.92 | +3.00 | 18 | -8 |
| 27 | Timofey Mozgov | +0.89 | +6.90 | 3 | -24 |
| 28 | Jeff Teague | +0.86 | -0.20 | 59 | +31 |
| 29 | Andre Iguodala | +0.79 | +0.70 | 48 | +19 |
| 30 | Al Horford | +0.71 | +4.40 | 12 | -18 |

## 2014-15 Regular season — defense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.628 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Tony Allen | +5.30 | +4.80 | 4 | +3 | Kawhi Leonard | +5.20 |
| 2 | Andrew Bogut | +5.06 | +4.70 | 5 | +3 | Draymond Green | +5.10 |
| 3 | Draymond Green | +4.79 | +5.10 | 2 | -1 | Rudy Gobert | +4.80 |
| 4 | Rudy Gobert | +4.77 | +4.80 | 3 | -1 | Tony Allen | +4.80 |
| 5 | Kawhi Leonard | +4.55 | +5.20 | 1 | -4 | Andrew Bogut | +4.70 |
| 6 | DeMarcus Cousins | +3.97 | +4.40 | 7 | +1 | Anthony Davis | +4.50 |
| 7 | Tim Duncan | +3.62 | +3.50 | 9 | +2 | DeMarcus Cousins | +4.40 |
| 8 | Nerlens Noel | +3.50 | +2.70 | 19 | +11 | Marcin Gortat | +3.60 |
| 9 | Kosta Koufos | +3.42 | +3.30 | 11 | +2 | Tim Duncan | +3.50 |
| 10 | Zaza Pachulia | +3.39 | +3.20 | 12 | +2 | Andre Roberson | +3.40 |
| 11 | Nene | +3.29 | +2.80 | 18 | +7 | Kosta Koufos | +3.30 |
| 12 | Danny Green | +3.23 | +3.00 | 16 | +4 | Zaza Pachulia | +3.20 |
| 13 | Marcin Gortat | +3.17 | +3.60 | 8 | -5 | Khris Middleton | +3.10 |
| 14 | Andre Roberson | +3.07 | +3.40 | 10 | -4 | Michael KiddGilchrist | +3.00 |
| 15 | Anthony Davis | +3.02 | +4.50 | 6 | -9 | Serge Ibaka | +3.00 |
| 16 | Michael KiddGilchrist | +2.82 | +3.00 | 14 | -2 | Danny Green | +3.00 |
| 17 | AlFarouq Aminu | +2.81 | +2.60 | 24 | +7 | Jonas Jerebko | +2.80 |
| 18 | Jonas Jerebko | +2.78 | +2.80 | 17 | -1 | Nene | +2.80 |
| 19 | Greg Monroe | +2.77 | +0.20 | 111 | +92 | Nerlens Noel | +2.70 |
| 20 | Iman Shumpert | +2.69 | +2.30 | 32 | +12 | Tyson Chandler | +2.60 |
| 21 | Jared Dudley | +2.69 | +1.80 | 44 | +23 | Marc Gasol | +2.60 |
| 22 | Marcus Smart | +2.67 | +1.80 | 43 | +21 | Joakim Noah | +2.60 |
| 23 | Josh Smith | +2.64 | +2.60 | 23 | +0 | Josh Smith | +2.60 |
| 24 | Michael CarterWilliams | +2.63 | +2.30 | 31 | +7 | AlFarouq Aminu | +2.60 |
| 25 | Pau Gasol | +2.56 | +0.50 | 90 | +65 | Alex Len | +2.50 |
| 26 | Nikola Mirotic | +2.54 | +2.20 | 33 | +7 | Paul Millsap | +2.40 |
| 27 | Khris Middleton | +2.52 | +3.10 | 13 | -14 | Omer Asik | +2.40 |
| 28 | Manu Ginobili | +2.51 | +1.40 | 52 | +24 | Timofey Mozgov | +2.40 |
| 29 | Timofey Mozgov | +2.47 | +2.40 | 28 | -1 | Darren Collison | +2.30 |
| 30 | Paul Millsap | +2.41 | +2.40 | 26 | -4 | Luc Mbah a Moute | +2.30 |
| 31 | Derrick Favors | +2.41 | +2.10 | 34 | +3 | Michael CarterWilliams | +2.30 |
| 32 | Zach Randolph | +2.38 | +1.30 | 59 | +27 | Iman Shumpert | +2.30 |
| 33 | Wesley Matthews | +2.37 | +1.30 | 56 | +23 | Nikola Mirotic | +2.20 |
| 34 | Tyson Chandler | +2.36 | +2.60 | 20 | -14 | Derrick Favors | +2.10 |
| 35 | Ersan Ilyasova | +2.34 | +0.40 | 100 | +65 | Chris Paul | +2.10 |
| 36 | John Wall | +2.32 | +0.00 | 128 | +92 | Kelly Olynyk | +2.10 |
| 37 | Dwight Howard | +2.14 | +1.90 | 41 | +4 | Cody Zeller | +2.10 |
| 38 | Luis Scola | +2.09 | +0.60 | 84 | +46 | Roy Hibbert | +2.00 |
| 39 | Omer Asik | +2.08 | +2.40 | 27 | -12 | Steven Adams | +2.00 |
| 40 | Roy Hibbert | +2.07 | +2.00 | 38 | -2 | LaMarcus Aldridge | +1.90 |
| 41 | Andre Iguodala | +2.06 | +1.60 | 47 | +6 | Dwight Howard | +1.90 |
| 42 | KJ McDaniels | +2.04 | +0.70 | 79 | +37 | Pablo Prigioni | +1.80 |
| 43 | Jimmy Butler | +1.99 | -0.20 | 140 | +97 | Marcus Smart | +1.80 |
| 44 | Kelly Olynyk | +1.95 | +2.10 | 36 | -8 | Jared Dudley | +1.80 |
| 45 | Serge Ibaka | +1.94 | +3.00 | 15 | -30 | George Hill | +1.70 |
| 46 | LeBron James | +1.91 | -0.10 | 134 | +88 | Al Horford | +1.60 |
| 47 | Rajon Rondo | +1.91 | +0.40 | 93 | +46 | Andre Iguodala | +1.60 |
| 48 | Monta Ellis | +1.89 | +0.80 | 77 | +29 | Kevin Love | +1.60 |
| 49 | Cory Joseph | +1.88 | +0.50 | 85 | +36 | Mario Chalmers | +1.50 |
| 50 | CJ Miles | +1.83 | +0.20 | 117 | +67 | Kris Humphries | +1.50 |

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
| 1 | Nikola Jokic | +10.80 | 2737 |
| 2 | Shai Gilgeous-Alexander | +8.04 | 2553 |
| 3 | Luka Doncic | +7.78 | 2624 |
| 4 | Joel Embiid | +7.71 | 1309 |
| 5 | Paul George | +7.07 | 2502 |
| 6 | Kawhi Leonard | +7.01 | 2330 |
| 7 | Donovan Mitchell | +6.52 | 1943 |
| 8 | Jalen Brunson | +5.98 | 2726 |
| 9 | Tyrese Haliburton | +5.61 | 2224 |
| 10 | De'Aaron Fox | +5.54 | 2659 |
| 11 | Giannis Antetokounmpo | +5.33 | 2567 |
| 12 | Kyrie Irving | +5.25 | 2030 |
| 13 | Anthony Davis | +5.12 | 2700 |
| 14 | Jayson Tatum | +4.85 | 2645 |
| 15 | Isaiah Hartenstein | +4.60 | 1896 |
| 16 | Damian Lillard | +4.58 | 2579 |
| 17 | LeBron James | +4.55 | 2504 |
| 18 | Jamal Murray | +4.45 | 1861 |
| 19 | Kristaps Porzingis | +4.38 | 1690 |
| 20 | Stephen Curry | +4.32 | 2421 |
| 21 | Fred VanVleet | +4.20 | 2684 |
| 22 | Lauri Markkanen | +3.99 | 1820 |
| 23 | Jimmy Butler | +3.92 | 2042 |
| 24 | Derrick White | +3.91 | 2381 |
| 25 | Alex Caruso | +3.80 | 2040 |
| 26 | Devin Booker | +3.72 | 2447 |
| 27 | Jusuf Nurkic | +3.70 | 2078 |
| 28 | Alperen Sengun | +3.68 | 2046 |
| 29 | James Harden | +3.65 | 2470 |
| 30 | Kevin Durant | +3.54 | 2791 |
| 31 | Rudy Gobert | +3.42 | 2593 |
| 32 | Victor Wembanyama | +3.34 | 2106 |
| 33 | Tyrese Maxey | +3.18 | 2626 |
| 34 | Trey Murphy III | +3.17 | 1690 |
| 35 | T.J. McConnell | +3.16 | 1291 |
| 36 | Donte DiVincenzo | +3.13 | 2360 |
| 37 | Chet Holmgren | +3.08 | 2413 |
| 38 | Bogdan Bogdanovic | +3.04 | 2401 |
| 39 | Anthony Edwards | +2.90 | 2770 |
| 40 | Jarrett Allen | +2.87 | 2442 |
| 41 | Mike Conley | +2.84 | 2193 |
| 42 | Jalen Williams | +2.74 | 2223 |
| 43 | Isaiah Joe | +2.72 | 1445 |
| 44 | Karl-Anthony Towns | +2.68 | 2026 |
| 45 | Draymond Green | +2.58 | 1490 |
| 46 | Dean Wade | +2.54 | 1108 |
| 47 | Aaron Nesmith | +2.41 | 1995 |
| 48 | Brandin Podziemski | +2.37 | 1968 |
| 49 | Franz Wagner | +2.33 | 2337 |
| 50 | Paul Reed | +2.33 | 1590 |

## 2023-24 Playoffs — total, top 50 (projected, no truth)

> pool 103 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Joel Embiid | +10.90 | 248 |
| 2 | Nikola Jokic | +8.53 | 482 |
| 3 | Luka Doncic | +8.45 | 900 |
| 4 | Tyrese Haliburton | +6.95 | 522 |
| 5 | Donovan Mitchell | +6.60 | 382 |
| 6 | Kristaps Porzingis | +6.11 | 165 |
| 7 | Anthony Edwards | +5.76 | 649 |
| 8 | Shai Gilgeous-Alexander | +5.72 | 399 |
| 9 | LeBron James | +5.45 | 204 |
| 10 | Jalen Brunson | +5.24 | 518 |
| 11 | Jayson Tatum | +5.11 | 768 |
| 12 | Jalen Williams | +4.80 | 377 |
| 13 | Chet Holmgren | +4.68 | 345 |
| 14 | Dereck Lively II | +4.55 | 462 |
| 15 | Mike Conley | +4.47 | 474 |
| 16 | Ivica Zubac | +4.44 | 192 |
| 17 | Kyrie Irving | +4.35 | 879 |
| 18 | Khris Middleton | +4.31 | 230 |
| 19 | Rudy Gobert | +4.29 | 512 |
| 20 | Al Horford | +4.04 | 575 |
| 21 | Paolo Banchero | +3.99 | 262 |
| 22 | Pascal Siakam | +3.93 | 603 |
| 23 | Derrick White | +3.79 | 676 |
| 24 | Austin Reaves | +3.64 | 174 |
| 25 | Sam Hauser | +3.58 | 283 |
| 26 | Paul George | +3.48 | 222 |
| 27 | Jaylen Brown | +3.40 | 707 |
| 28 | Franz Wagner | +3.38 | 259 |
| 29 | T.J. McConnell | +3.24 | 348 |
| 30 | Bobby Portis | +3.19 | 187 |
| 31 | Aaron Wiggins | +3.16 | 157 |
| 32 | Myles Turner | +3.16 | 550 |
| 33 | Devin Booker | +3.05 | 166 |
| 34 | Jrue Holiday | +3.04 | 720 |
| 35 | Jalen Suggs | +3.00 | 232 |
| 36 | Kelly Oubre Jr. | +2.83 | 224 |
| 37 | Jaden McDaniels | +2.63 | 537 |
| 38 | Luguentz Dort | +2.63 | 350 |
| 39 | Justin Holiday | +2.56 | 150 |
| 40 | Anthony Davis | +2.48 | 208 |
| 41 | Cason Wallace | +2.45 | 198 |
| 42 | Andrew Nembhard | +2.05 | 554 |
| 43 | Wendell Carter Jr. | +2.04 | 185 |
| 44 | James Harden | +2.02 | 242 |
| 45 | Kyle Lowry | +1.93 | 175 |
| 46 | Kentavious Caldwell-Pope | +1.51 | 420 |
| 47 | Isaiah Joe | +1.49 | 173 |
| 48 | Karl-Anthony Towns | +1.45 | 522 |
| 49 | OG Anunoby | +1.37 | 324 |
| 50 | Josh Green | +1.24 | 399 |

## 2024-25 Regular season — total, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +10.29 | 2571 |
| 2 | Shai Gilgeous-Alexander | +8.33 | 2598 |
| 3 | Luka Dončić | +7.87 | 1769 |
| 4 | Giannis Antetokounmpo | +5.77 | 2289 |
| 5 | Stephen Curry | +5.46 | 2252 |
| 6 | Victor Wembanyama | +5.13 | 1527 |
| 7 | Tyrese Haliburton | +5.06 | 2451 |
| 8 | Ivica Zubac | +5.03 | 2624 |
| 9 | James Harden | +5.02 | 2789 |
| 10 | Donovan Mitchell | +5.01 | 2232 |
| 11 | Jayson Tatum | +4.80 | 2624 |
| 12 | Luke Kornet | +4.79 | 1361 |
| 13 | Alperen Sengun | +4.70 | 2394 |
| 14 | Jimmy Butler | +4.64 | 1746 |
| 15 | Darius Garland | +4.56 | 2301 |
| 16 | Kawhi Leonard | +4.12 | 1180 |
| 17 | Ty Jerome | +4.10 | 1393 |
| 18 | Jarrett Allen | +4.08 | 2296 |
| 19 | Rudy Gobert | +3.94 | 2388 |
| 20 | Karl-Anthony Towns | +3.94 | 2517 |
| 21 | Franz Wagner | +3.78 | 2023 |
| 22 | Derrick White | +3.75 | 2574 |
| 23 | Daniel Gafford | +3.75 | 1226 |
| 24 | Luguentz Dort | +3.75 | 2073 |
| 25 | Brandin Podziemski | +3.46 | 1716 |
| 26 | Tyler Herro | +3.38 | 2725 |
| 27 | Kyrie Irving | +3.36 | 1804 |
| 28 | Austin Reaves | +3.33 | 2550 |
| 29 | Kristaps Porziņģis | +3.32 | 1210 |
| 30 | Jamal Murray | +3.31 | 2418 |
| 31 | Jaren Jackson Jr. | +3.26 | 2207 |
| 32 | Norman Powell | +3.25 | 1958 |
| 33 | Evan Mobley | +3.24 | 2167 |
| 34 | Isaiah Joe | +3.21 | 1604 |
| 35 | Payton Pritchard | +3.17 | 2271 |
| 36 | Anthony Edwards | +3.09 | 2871 |
| 37 | Anthony Davis | +2.94 | 1706 |
| 38 | Keon Ellis | +2.90 | 1948 |
| 39 | Damian Lillard | +2.88 | 2093 |
| 40 | Jalen Brunson | +2.81 | 2301 |
| 41 | Kris Dunn | +2.63 | 1783 |
| 42 | Amen Thompson | +2.61 | 2225 |
| 43 | Domantas Sabonis | +2.57 | 2429 |
| 44 | Cade Cunningham | +2.51 | 2452 |
| 45 | Ausar Thompson | +2.50 | 1328 |
| 46 | Donte DiVincenzo | +2.44 | 1606 |
| 47 | Cason Wallace | +2.44 | 1876 |
| 48 | Mike Conley | +2.43 | 1756 |
| 49 | Tari Eason | +2.43 | 1420 |
| 50 | Jalen Williams | +2.41 | 2237 |

## 2024-25 Playoffs — total, top 50 (projected, no truth)

> pool 109 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +8.96 | 563 |
| 2 | Jayson Tatum | +8.09 | 322 |
| 3 | Alperen Sengun | +7.05 | 256 |
| 4 | Stephen Curry | +6.65 | 281 |
| 5 | Steven Adams | +6.40 | 155 |
| 6 | Jarrett Allen | +6.09 | 261 |
| 7 | Tyrese Haliburton | +6.01 | 772 |
| 8 | Shai Gilgeous-Alexander | +5.93 | 851 |
| 9 | Donovan Mitchell | +5.79 | 288 |
| 10 | Gary Trent Jr. | +5.74 | 171 |
| 11 | Jaden McDaniels | +5.53 | 497 |
| 12 | Alex Caruso | +5.50 | 562 |
| 13 | Luka Dončić | +5.49 | 208 |
| 14 | Cade Cunningham | +5.46 | 248 |
| 15 | Pascal Siakam | +5.27 | 771 |
| 16 | Fred VanVleet | +5.12 | 280 |
| 17 | Anthony Edwards | +4.87 | 585 |
| 18 | LeBron James | +4.87 | 204 |
| 19 | Aaron Nesmith | +4.74 | 650 |
| 20 | Rudy Gobert | +4.58 | 411 |
| 21 | Jamal Murray | +4.54 | 578 |
| 22 | Giannis Antetokounmpo | +4.48 | 188 |
| 23 | Luke Kornet | +4.45 | 180 |
| 24 | Max Strus | +4.45 | 253 |
| 25 | Isaiah Joe | +4.08 | 211 |
| 26 | Ausar Thompson | +3.66 | 135 |
| 27 | Amen Thompson | +3.45 | 231 |
| 28 | Andrew Nembhard | +3.39 | 769 |
| 29 | AJ Green | +3.35 | 135 |
| 30 | Julius Randle | +3.31 | 533 |
| 31 | Chet Holmgren | +3.25 | 686 |
| 32 | Aaron Gordon | +3.24 | 522 |
| 33 | Jalen Brunson | +3.14 | 680 |
| 34 | Cason Wallace | +3.12 | 516 |
| 35 | Derrick White | +3.05 | 415 |
| 36 | Buddy Hield | +3.01 | 327 |
| 37 | Evan Mobley | +2.97 | 257 |
| 38 | Mitchell Robinson | +2.90 | 370 |
| 39 | Isaiah Hartenstein | +2.74 | 516 |
| 40 | Karl-Anthony Towns | +2.70 | 639 |
| 41 | Payton Pritchard | +2.59 | 302 |
| 42 | Paolo Banchero | +2.39 | 197 |
| 43 | Dennis Schröder | +2.37 | 164 |
| 44 | Jaylen Brown | +2.32 | 402 |
| 45 | Mike Conley | +2.31 | 356 |
| 46 | Ty Jerome | +2.25 | 191 |
| 47 | Ivica Zubac | +2.16 | 256 |
| 48 | Jalen Williams | +2.03 | 796 |
| 49 | Brandin Podziemski | +2.01 | 385 |
| 50 | Kenrich Williams | +1.99 | 137 |

## 2025-26 Regular season — total, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +10.18 | 2265 |
| 2 | Victor Wembanyama | +8.99 | 1866 |
| 3 | Kawhi Leonard | +7.96 | 2085 |
| 4 | Shai Gilgeous-Alexander | +7.90 | 2259 |
| 5 | Luka Dončić | +7.58 | 2289 |
| 6 | Jimmy Butler III | +6.34 | 1182 |
| 7 | Donovan Mitchell | +5.91 | 2342 |
| 8 | Chet Holmgren | +5.88 | 1997 |
| 9 | LaMelo Ball | +5.67 | 2017 |
| 10 | Jamal Murray | +5.39 | 2652 |
| 11 | Derrick White | +5.03 | 2625 |
| 12 | Stephen Curry | +4.93 | 1329 |
| 13 | Cade Cunningham | +4.91 | 2172 |
| 14 | Tyrese Maxey | +4.74 | 2661 |
| 15 | Jalen Duren | +4.68 | 1976 |
| 16 | Neemias Queta | +4.58 | 1926 |
| 17 | Collin Gillespie | +4.35 | 2282 |
| 18 | Jalen Brunson | +4.33 | 2590 |
| 19 | Brandon Miller | +4.15 | 1968 |
| 20 | Paul George | +4.13 | 1135 |
| 21 | Ajay Mitchell | +4.07 | 1473 |
| 22 | Joel Embiid | +3.90 | 1201 |
| 23 | Isaiah Joe | +3.80 | 1507 |
| 24 | Austin Reaves | +3.75 | 1762 |
| 25 | Dyson Daniels | +3.66 | 2520 |
| 26 | Nickeil Alexander-Walker | +3.55 | 2603 |
| 27 | Kevin Durant | +3.54 | 2840 |
| 28 | Isaiah Hartenstein | +3.48 | 1137 |
| 29 | Karl-Anthony Towns | +3.47 | 2322 |
| 30 | Jrue Holiday | +3.41 | 1560 |
| 31 | Donte DiVincenzo | +3.39 | 2494 |
| 32 | Jordan Goodwin | +3.30 | 1572 |
| 33 | Mitchell Robinson | +3.19 | 1175 |
| 34 | Cason Wallace | +3.18 | 2046 |
| 35 | Scottie Barnes | +3.16 | 2681 |
| 36 | Jarrett Allen | +3.16 | 1519 |
| 37 | Rudy Gobert | +3.16 | 2380 |
| 38 | James Harden | +3.11 | 2438 |
| 39 | Devin Booker | +3.10 | 2146 |
| 40 | Toumani Camara | +3.10 | 2731 |
| 41 | Moussa Diabaté | +3.05 | 1899 |
| 42 | Jalen Suggs | +3.03 | 1574 |
| 43 | Reed Sheppard | +3.01 | 2147 |
| 44 | Jaylen Brown | +2.98 | 2443 |
| 45 | Bam Adebayo | +2.93 | 2365 |
| 46 | Anthony Edwards | +2.91 | 2137 |
| 47 | Deni Avdija | +2.88 | 2199 |
| 48 | Donovan Clingan | +2.85 | 2094 |
| 49 | Alperen Sengun | +2.81 | 2398 |
| 50 | OG Anunoby | +2.76 | 2224 |

## 2025-26 Playoffs — total, top 50 (projected, no truth)

> pool 112 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jayson Tatum | +9.66 | 218 |
| 2 | Victor Wembanyama | +8.28 | 750 |
| 3 | Ajay Mitchell | +7.91 | 317 |
| 4 | Karl-Anthony Towns | +7.55 | 578 |
| 5 | Alex Caruso | +7.29 | 353 |
| 6 | Amen Thompson | +6.69 | 264 |
| 7 | Tari Eason | +6.07 | 195 |
| 8 | Jose Alvarado | +5.66 | 170 |
| 9 | Chet Holmgren | +5.43 | 459 |
| 10 | OG Anunoby | +5.24 | 586 |
| 11 | James Harden | +5.22 | 672 |
| 12 | Nikola Jokić | +4.73 | 237 |
| 13 | Alperen Sengun | +4.65 | 232 |
| 14 | Cason Wallace | +4.63 | 374 |
| 15 | Jalen Brunson | +4.59 | 702 |
| 16 | Jarrett Allen | +4.56 | 529 |
| 17 | Dylan Harper | +4.05 | 615 |
| 18 | Mikal Bridges | +4.03 | 608 |
| 19 | Dillon Brooks | +3.69 | 149 |
| 20 | RJ Barrett | +3.51 | 271 |
| 21 | Dean Wade | +3.46 | 407 |
| 22 | Julian Champagnie | +3.41 | 705 |
| 23 | Devin Vassell | +3.34 | 801 |
| 24 | Scottie Barnes | +3.26 | 273 |
| 25 | Cade Cunningham | +3.12 | 572 |
| 26 | Paolo Banchero | +3.10 | 273 |
| 27 | Isaiah Hartenstein | +2.99 | 350 |
| 28 | Payton Pritchard | +2.93 | 231 |
| 29 | Neemias Queta | +2.93 | 152 |
| 30 | Collin Murray-Boyles | +2.91 | 191 |
| 31 | Ausar Thompson | +2.84 | 427 |
| 32 | Mike Conley | +2.70 | 168 |
| 33 | Mitchell Robinson | +2.65 | 251 |
| 34 | Josh Hart | +2.63 | 614 |
| 35 | Shai Gilgeous-Alexander | +2.56 | 544 |
| 36 | Duncan Robinson | +2.47 | 383 |
| 37 | Jaylin Williams | +2.47 | 240 |
| 38 | Marcus Smart | +2.36 | 345 |
| 39 | Paul George | +2.27 | 394 |
| 40 | De'Aaron Fox | +2.20 | 704 |
| 41 | VJ Edgecombe | +2.16 | 407 |
| 42 | Tyrese Maxey | +2.04 | 437 |
| 43 | Jabari Smith Jr. | +1.96 | 252 |
| 44 | Cameron Johnson | +1.93 | 186 |
| 45 | Jamal Shead | +1.88 | 224 |
| 46 | Jakob Poeltl | +1.87 | 134 |
| 47 | Scoot Henderson | +1.82 | 145 |
| 48 | Sam Merrill | +1.75 | 338 |
| 49 | Desmond Bane | +1.64 | 253 |
| 50 | Tim Hardaway Jr. | +1.45 | 140 |

## 2023-24 Regular season — offense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +8.16 | 2737 |
| 2 | Luka Doncic | +8.06 | 2624 |
| 3 | Jalen Brunson | +6.99 | 2726 |
| 4 | Shai Gilgeous-Alexander | +6.85 | 2553 |
| 5 | Tyrese Haliburton | +5.96 | 2224 |
| 6 | Donovan Mitchell | +5.71 | 1943 |
| 7 | Devin Booker | +5.33 | 2447 |
| 8 | Stephen Curry | +5.32 | 2421 |
| 9 | Giannis Antetokounmpo | +5.32 | 2567 |
| 10 | LeBron James | +5.23 | 2504 |
| 11 | Damian Lillard | +5.22 | 2579 |
| 12 | Trae Young | +4.94 | 1942 |
| 13 | Kyrie Irving | +4.87 | 2030 |
| 14 | Jamal Murray | +4.82 | 1861 |
| 15 | James Harden | +4.63 | 2470 |
| 16 | Tyrese Maxey | +4.43 | 2626 |
| 17 | Joel Embiid | +4.35 | 1309 |
| 18 | Jayson Tatum | +4.32 | 2645 |
| 19 | De'Aaron Fox | +4.23 | 2659 |
| 20 | Anthony Edwards | +4.08 | 2770 |
| 21 | Kawhi Leonard | +3.98 | 2330 |
| 22 | Paul George | +3.87 | 2502 |
| 23 | Collin Sexton | +3.64 | 2075 |
| 24 | Lauri Markkanen | +3.62 | 1820 |
| 25 | Fred VanVleet | +3.42 | 2684 |
| 26 | Jimmy Butler | +3.31 | 2042 |
| 27 | Desmond Bane | +3.29 | 1443 |
| 28 | T.J. McConnell | +3.27 | 1291 |
| 29 | DeMar DeRozan | +3.16 | 2989 |
| 30 | Kevin Durant | +3.13 | 2791 |
| 31 | CJ McCollum | +2.97 | 2159 |
| 32 | Payton Pritchard | +2.88 | 1826 |
| 33 | D'Angelo Russell | +2.87 | 2484 |
| 34 | Anfernee Simons | +2.81 | 1582 |
| 35 | Dejounte Murray | +2.72 | 2783 |
| 36 | Anthony Davis | +2.67 | 2700 |
| 37 | Malcolm Brogdon | +2.66 | 1121 |
| 38 | Mike Conley | +2.59 | 2193 |
| 39 | Donte DiVincenzo | +2.55 | 2360 |
| 40 | Julius Randle | +2.55 | 1630 |
| 41 | Pascal Siakam | +2.44 | 2658 |
| 42 | Immanuel Quickley | +2.11 | 1985 |
| 43 | Jalen Williams | +2.10 | 2223 |
| 44 | Khris Middleton | +2.09 | 1487 |
| 45 | Zion Williamson | +2.07 | 2207 |
| 46 | Brandon Ingram | +2.04 | 2103 |
| 47 | Malik Monk | +2.04 | 1872 |
| 48 | Terry Rozier | +2.04 | 2040 |
| 49 | Michael Porter Jr. | +2.01 | 2565 |
| 50 | Derrick White | +2.01 | 2381 |

## 2023-24 Playoffs — offense, top 50 (projected, no truth)

> pool 103 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +8.44 | 482 |
| 2 | Joel Embiid | +6.24 | 248 |
| 3 | Jalen Brunson | +6.11 | 518 |
| 4 | Luka Doncic | +5.96 | 900 |
| 5 | Tyrese Haliburton | +5.68 | 522 |
| 6 | Devin Booker | +5.50 | 166 |
| 7 | Kyrie Irving | +5.41 | 879 |
| 8 | Damian Lillard | +5.37 | 156 |
| 9 | Khris Middleton | +5.26 | 230 |
| 10 | Anthony Edwards | +5.03 | 649 |
| 11 | Shai Gilgeous-Alexander | +4.97 | 399 |
| 12 | Austin Reaves | +4.91 | 174 |
| 13 | Jayson Tatum | +4.72 | 768 |
| 14 | Donovan Mitchell | +4.59 | 382 |
| 15 | Tyrese Maxey | +4.57 | 267 |
| 16 | Anthony Davis | +4.23 | 208 |
| 17 | LeBron James | +4.19 | 204 |
| 18 | Kevin Durant | +4.18 | 168 |
| 19 | James Harden | +3.85 | 242 |
| 20 | Pascal Siakam | +3.77 | 603 |
| 21 | Andrew Nembhard | +3.50 | 554 |
| 22 | Paul George | +2.81 | 222 |
| 23 | Donte DiVincenzo | +2.77 | 466 |
| 24 | T.J. McConnell | +2.72 | 348 |
| 25 | Mike Conley | +2.59 | 474 |
| 26 | Jalen Williams | +2.57 | 377 |
| 27 | Derrick White | +2.47 | 676 |
| 28 | Myles Turner | +2.45 | 550 |
| 29 | Jaden McDaniels | +2.36 | 537 |
| 30 | Kyle Lowry | +2.27 | 175 |
| 31 | Bobby Portis | +1.89 | 187 |
| 32 | Patrick Beverley | +1.84 | 210 |
| 33 | Al Horford | +1.74 | 575 |
| 34 | Sam Hauser | +1.70 | 283 |
| 35 | Jaylen Brown | +1.66 | 707 |
| 36 | Jrue Holiday | +1.63 | 720 |
| 37 | Chet Holmgren | +1.59 | 345 |
| 38 | Rudy Gobert | +1.54 | 512 |
| 39 | Bam Adebayo | +1.53 | 192 |
| 40 | Kelly Oubre Jr. | +1.51 | 224 |
| 41 | Aaron Gordon | +1.48 | 445 |
| 42 | Michael Porter Jr. | +1.45 | 443 |
| 43 | Kentavious Caldwell-Pope | +1.36 | 420 |
| 44 | Jamal Murray | +1.27 | 462 |
| 45 | Kristaps Porzingis | +1.22 | 165 |
| 46 | Dereck Lively II | +1.15 | 462 |
| 47 | Franz Wagner | +1.11 | 259 |
| 48 | Bradley Beal | +0.99 | 154 |
| 49 | Karl-Anthony Towns | +0.98 | 522 |
| 50 | Josh Green | +0.82 | 399 |

## 2024-25 Regular season — offense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.32 | 2571 |
| 2 | Shai Gilgeous-Alexander | +7.99 | 2598 |
| 3 | Stephen Curry | +6.90 | 2252 |
| 4 | Luka Dončić | +6.08 | 1769 |
| 5 | Jalen Brunson | +5.53 | 2301 |
| 6 | LaMelo Ball | +5.48 | 1505 |
| 7 | Tyrese Haliburton | +5.35 | 2451 |
| 8 | Giannis Antetokounmpo | +5.32 | 2289 |
| 9 | Jayson Tatum | +5.03 | 2624 |
| 10 | James Harden | +4.98 | 2789 |
| 11 | Darius Garland | +4.89 | 2301 |
| 12 | Damian Lillard | +4.81 | 2093 |
| 13 | Donovan Mitchell | +4.77 | 2232 |
| 14 | Ty Jerome | +4.74 | 1393 |
| 15 | Trae Young | +4.72 | 2739 |
| 16 | Cade Cunningham | +4.38 | 2452 |
| 17 | Anthony Edwards | +4.22 | 2871 |
| 18 | Tyler Herro | +4.14 | 2725 |
| 19 | Jamal Murray | +4.07 | 2418 |
| 20 | Kyrie Irving | +3.66 | 1804 |
| 21 | Austin Reaves | +3.49 | 2550 |
| 22 | Tyrese Maxey | +3.37 | 1960 |
| 23 | Payton Pritchard | +3.28 | 2271 |
| 24 | Devin Booker | +3.25 | 2795 |
| 25 | Ja Morant | +3.19 | 1519 |
| 26 | Franz Wagner | +2.89 | 2023 |
| 27 | Karl-Anthony Towns | +2.79 | 2517 |
| 28 | Norman Powell | +2.74 | 1958 |
| 29 | LeBron James | +2.72 | 2444 |
| 30 | Kevin Durant | +2.68 | 2265 |
| 31 | Jimmy Butler | +2.66 | 1746 |
| 32 | Christian Braun | +2.63 | 2675 |
| 33 | Jaylen Brown | +2.39 | 2158 |
| 34 | Isaiah Joe | +2.39 | 1604 |
| 35 | Paolo Banchero | +2.32 | 1582 |
| 36 | Cameron Johnson | +2.25 | 1800 |
| 37 | Deni Avdija | +2.24 | 2161 |
| 38 | DeMar DeRozan | +2.23 | 2768 |
| 39 | Domantas Sabonis | +2.14 | 2429 |
| 40 | Derrick White | +2.08 | 2574 |
| 41 | CJ McCollum | +2.07 | 1832 |
| 42 | Zach LaVine | +2.07 | 2603 |
| 43 | Desmond Bane | +2.05 | 2205 |
| 44 | Aaron Gordon | +2.02 | 1447 |
| 45 | Kawhi Leonard | +2.01 | 1180 |
| 46 | Chris Paul | +1.98 | 2292 |
| 47 | Collin Sexton | +1.96 | 1758 |
| 48 | Jalen Green | +1.92 | 2697 |
| 49 | Malik Beasley | +1.91 | 2283 |
| 50 | Anfernee Simons | +1.89 | 2292 |

## 2024-25 Playoffs — offense, top 50 (projected, no truth)

> pool 109 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jalen Brunson | +7.93 | 680 |
| 2 | Luka Dončić | +7.46 | 208 |
| 3 | Giannis Antetokounmpo | +7.35 | 188 |
| 4 | Fred VanVleet | +6.65 | 280 |
| 5 | Nikola Jokić | +6.51 | 563 |
| 6 | Shai Gilgeous-Alexander | +5.96 | 851 |
| 7 | Donovan Mitchell | +5.83 | 288 |
| 8 | Stephen Curry | +5.78 | 281 |
| 9 | Tyrese Haliburton | +5.46 | 772 |
| 10 | LeBron James | +4.78 | 204 |
| 11 | Davion Mitchell | +4.63 | 142 |
| 12 | Jayson Tatum | +4.58 | 322 |
| 13 | Payton Pritchard | +4.44 | 302 |
| 14 | Gary Trent Jr. | +4.12 | 171 |
| 15 | Anthony Edwards | +4.11 | 585 |
| 16 | Jamal Murray | +3.92 | 578 |
| 17 | Max Strus | +3.89 | 253 |
| 18 | Kawhi Leonard | +3.85 | 265 |
| 19 | AJ Green | +3.74 | 135 |
| 20 | Isaiah Joe | +3.63 | 211 |
| 21 | Dennis Schröder | +3.52 | 164 |
| 22 | Paolo Banchero | +3.37 | 197 |
| 23 | Julius Randle | +3.29 | 533 |
| 24 | Jalen Williams | +3.21 | 796 |
| 25 | Aaron Gordon | +3.05 | 522 |
| 26 | Derrick White | +2.98 | 415 |
| 27 | Ty Jerome | +2.86 | 191 |
| 28 | Alperen Sengun | +2.80 | 256 |
| 29 | Aaron Nesmith | +2.70 | 650 |
| 30 | Pascal Siakam | +2.65 | 771 |
| 31 | Jimmy Butler III | +2.55 | 397 |
| 32 | James Harden | +2.41 | 276 |
| 33 | Amen Thompson | +2.38 | 231 |
| 34 | Jaden McDaniels | +2.23 | 497 |
| 35 | Buddy Hield | +2.20 | 327 |
| 36 | Jarrett Allen | +2.15 | 261 |
| 37 | Cade Cunningham | +2.13 | 248 |
| 38 | Sam Merrill | +2.02 | 159 |
| 39 | Franz Wagner | +2.01 | 195 |
| 40 | Steven Adams | +1.94 | 155 |
| 41 | Dillon Brooks | +1.93 | 206 |
| 42 | Ivica Zubac | +1.83 | 256 |
| 43 | Darius Garland | +1.83 | 148 |
| 44 | Andrew Nembhard | +1.80 | 769 |
| 45 | Luke Kornet | +1.77 | 180 |
| 46 | Bam Adebayo | +1.71 | 153 |
| 47 | Evan Mobley | +1.57 | 257 |
| 48 | Alex Caruso | +1.50 | 562 |
| 49 | Jalen Green | +1.36 | 219 |
| 50 | Jalen Duren | +1.34 | 203 |

## 2025-26 Regular season — offense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +8.74 | 2265 |
| 2 | Shai Gilgeous-Alexander | +7.96 | 2259 |
| 3 | Luka Dončić | +7.42 | 2289 |
| 4 | Donovan Mitchell | +6.39 | 2342 |
| 5 | Kawhi Leonard | +6.31 | 2085 |
| 6 | Jamal Murray | +6.12 | 2652 |
| 7 | James Harden | +5.75 | 2438 |
| 8 | LaMelo Ball | +5.63 | 2017 |
| 9 | Cade Cunningham | +5.37 | 2172 |
| 10 | Stephen Curry | +5.27 | 1329 |
| 11 | Jalen Brunson | +5.25 | 2590 |
| 12 | Tyrese Maxey | +5.13 | 2661 |
| 13 | Jimmy Butler III | +4.77 | 1182 |
| 14 | Anthony Edwards | +4.73 | 2137 |
| 15 | Devin Booker | +4.72 | 2146 |
| 16 | Deni Avdija | +4.51 | 2199 |
| 17 | Payton Pritchard | +3.65 | 2556 |
| 18 | Michael Porter Jr. | +3.64 | 1689 |
| 19 | Victor Wembanyama | +3.64 | 1866 |
| 20 | Jalen Duren | +3.61 | 1976 |
| 21 | Kevin Durant | +3.54 | 2840 |
| 22 | Austin Reaves | +3.51 | 1762 |
| 23 | Jrue Holiday | +3.46 | 1560 |
| 24 | Coby White | +3.39 | 1250 |
| 25 | Jaylen Brown | +3.30 | 2443 |
| 26 | Duncan Robinson | +3.28 | 2113 |
| 27 | Joel Embiid | +3.27 | 1201 |
| 28 | Keyonte George | +3.26 | 1786 |
| 29 | Lauri Markkanen | +2.95 | 1443 |
| 30 | De'Aaron Fox | +2.88 | 2231 |
| 31 | Collin Gillespie | +2.84 | 2282 |
| 32 | Kon Knueppel | +2.80 | 2551 |
| 33 | Cam Spencer | +2.70 | 1714 |
| 34 | Anfernee Simons | +2.59 | 1372 |
| 35 | Alperen Sengun | +2.56 | 2398 |
| 36 | Julius Randle | +2.53 | 2610 |
| 37 | Darius Garland | +2.46 | 1344 |
| 38 | Grayson Allen | +2.44 | 1467 |
| 39 | Reed Sheppard | +2.44 | 2147 |
| 40 | CJ McCollum | +2.43 | 2263 |
| 41 | Ryan Rollins | +2.42 | 2375 |
| 42 | Luka Garza | +2.40 | 1118 |
| 43 | Brandon Miller | +2.33 | 1968 |
| 44 | Isaiah Joe | +2.31 | 1507 |
| 45 | Nickeil Alexander-Walker | +2.27 | 2603 |
| 46 | Bones Hyland | +2.24 | 1177 |
| 47 | Trey Murphy III | +2.22 | 2341 |
| 48 | Immanuel Quickley | +2.12 | 2231 |
| 49 | Miles McBride | +2.10 | 1080 |
| 50 | Sam Merrill | +2.05 | 1377 |

## 2025-26 Playoffs — offense, top 50 (projected, no truth)

> pool 112 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jayson Tatum | +7.69 | 218 |
| 2 | Jalen Brunson | +6.29 | 702 |
| 3 | Dillon Brooks | +6.24 | 149 |
| 4 | Ajay Mitchell | +4.79 | 317 |
| 5 | Tyrese Maxey | +4.66 | 437 |
| 6 | RJ Barrett | +4.64 | 271 |
| 7 | Payton Pritchard | +4.61 | 231 |
| 8 | Karl-Anthony Towns | +4.54 | 578 |
| 9 | Scottie Barnes | +3.80 | 273 |
| 10 | Jalen Green | +3.72 | 151 |
| 11 | Cade Cunningham | +3.67 | 572 |
| 12 | Alex Caruso | +3.59 | 353 |
| 13 | Nikola Jokić | +3.55 | 237 |
| 14 | James Harden | +3.54 | 672 |
| 15 | Shai Gilgeous-Alexander | +3.46 | 544 |
| 16 | Paolo Banchero | +3.12 | 273 |
| 17 | Joel Embiid | +2.99 | 233 |
| 18 | Mike Conley | +2.95 | 168 |
| 19 | Victor Wembanyama | +2.95 | 750 |
| 20 | Duncan Robinson | +2.90 | 383 |
| 21 | Naz Reid | +2.87 | 323 |
| 22 | Sam Merrill | +2.85 | 338 |
| 23 | Paul George | +2.76 | 394 |
| 24 | Donovan Mitchell | +2.67 | 652 |
| 25 | OG Anunoby | +2.64 | 586 |
| 26 | Chet Holmgren | +2.61 | 459 |
| 27 | Devin Booker | +2.44 | 153 |
| 28 | Tim Hardaway Jr. | +2.39 | 140 |
| 29 | Desmond Bane | +2.28 | 253 |
| 30 | Jarrett Allen | +2.28 | 529 |
| 31 | Tari Eason | +2.22 | 195 |
| 32 | Amen Thompson | +2.19 | 264 |
| 33 | Jrue Holiday | +2.14 | 192 |
| 34 | Ayo Dosunmu | +2.11 | 292 |
| 35 | Austin Reaves | +2.03 | 221 |
| 36 | Dylan Harper | +2.03 | 615 |
| 37 | Julian Champagnie | +1.89 | 705 |
| 38 | Wendell Carter Jr. | +1.82 | 237 |
| 39 | Dean Wade | +1.82 | 407 |
| 40 | Cameron Johnson | +1.73 | 186 |
| 41 | Onyeka Okongwu | +1.71 | 199 |
| 42 | Isaiah Joe | +1.66 | 143 |
| 43 | Jose Alvarado | +1.55 | 170 |
| 44 | Andre Drummond | +1.42 | 142 |
| 45 | Tobias Harris | +1.41 | 485 |
| 46 | Jalen Johnson | +1.39 | 214 |
| 47 | Mikal Bridges | +1.37 | 608 |
| 48 | Collin Murray-Boyles | +1.35 | 191 |
| 49 | Caris LeVert | +1.35 | 216 |
| 50 | Miles McBride | +1.31 | 334 |

## 2023-24 Regular season — defense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +3.87 | 2106 |
| 2 | Rudy Gobert | +3.47 | 2593 |
| 3 | Joel Embiid | +3.44 | 1309 |
| 4 | Dean Wade | +3.27 | 1108 |
| 5 | Isaiah Hartenstein | +3.26 | 1896 |
| 6 | Alex Caruso | +3.17 | 2040 |
| 7 | Brook Lopez | +3.09 | 2411 |
| 8 | Jusuf Nurkic | +3.06 | 2078 |
| 9 | Nikola Jokic | +3.06 | 2737 |
| 10 | Kristaps Porzingis | +2.81 | 1690 |
| 11 | Chet Holmgren | +2.77 | 2413 |
| 12 | Draymond Green | +2.55 | 1490 |
| 13 | Anthony Davis | +2.50 | 2700 |
| 14 | Ausar Thompson | +2.41 | 1583 |
| 15 | Toumani Camara | +2.40 | 1739 |
| 16 | Andre Drummond | +2.21 | 1351 |
| 17 | Larry Nance Jr. | +2.20 | 1216 |
| 18 | Ivica Zubac | +2.18 | 1795 |
| 19 | Paul George | +2.14 | 2502 |
| 20 | Derrick White | +2.06 | 2381 |
| 21 | OG Anunoby | +2.06 | 1702 |
| 22 | Evan Mobley | +2.04 | 1532 |
| 23 | Aaron Nesmith | +2.03 | 1995 |
| 24 | Amen Thompson | +2.02 | 1388 |
| 25 | Clint Capela | +1.98 | 1883 |
| 26 | Walker Kessler | +1.97 | 1493 |
| 27 | Franz Wagner | +1.96 | 2337 |
| 28 | Jarrett Allen | +1.93 | 2442 |
| 29 | Matisse Thybulle | +1.92 | 1487 |
| 30 | Naz Reid | +1.91 | 1964 |
| 31 | Kawhi Leonard | +1.89 | 2330 |
| 32 | Myles Turner | +1.88 | 2077 |
| 33 | Bam Adebayo | +1.82 | 2416 |
| 34 | Jalen Suggs | +1.82 | 2025 |
| 35 | Paul Reed | +1.75 | 1590 |
| 36 | Jakob Poeltl | +1.69 | 1319 |
| 37 | Wendell Carter Jr. | +1.69 | 1406 |
| 38 | Nickeil Alexander-Walker | +1.65 | 1921 |
| 39 | Herbert Jones | +1.60 | 2321 |
| 40 | Moses Moody | +1.59 | 1156 |
| 41 | Isaiah Joe | +1.53 | 1445 |
| 42 | Dyson Daniels | +1.50 | 1358 |
| 43 | Kyle Anderson | +1.49 | 1782 |
| 44 | Derrick Jones Jr. | +1.48 | 1783 |
| 45 | Vince Williams Jr. | +1.47 | 1436 |
| 46 | Luguentz Dort | +1.43 | 2246 |
| 47 | Shai Gilgeous-Alexander | +1.38 | 2553 |
| 48 | Trayce Jackson-Davis | +1.35 | 1130 |
| 49 | Alperen Sengun | +1.33 | 2046 |
| 50 | Naji Marshall | +1.33 | 1257 |

## 2023-24 Playoffs — defense, top 50 (projected, no truth)

> pool 103 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Justin Holiday | +4.79 | 150 |
| 2 | Kristaps Porzingis | +4.58 | 165 |
| 3 | Dereck Lively II | +3.73 | 462 |
| 4 | Joel Embiid | +3.67 | 248 |
| 5 | Chet Holmgren | +3.64 | 345 |
| 6 | Paolo Banchero | +3.61 | 262 |
| 7 | Evan Mobley | +3.50 | 422 |
| 8 | Aaron Wiggins | +2.85 | 157 |
| 9 | Al Horford | +2.52 | 575 |
| 10 | Brandon Ingram | +2.50 | 145 |
| 11 | Rudy Gobert | +2.24 | 512 |
| 12 | Luka Doncic | +2.22 | 900 |
| 13 | Cason Wallace | +2.13 | 198 |
| 14 | Isaac Okoro | +2.08 | 263 |
| 15 | Wendell Carter Jr. | +2.04 | 185 |
| 16 | Josh Giddey | +2.00 | 181 |
| 17 | Jonathan Isaac | +1.98 | 147 |
| 18 | Jalen Suggs | +1.93 | 232 |
| 19 | Franz Wagner | +1.80 | 259 |
| 20 | Ivica Zubac | +1.67 | 192 |
| 21 | Jalen Williams | +1.66 | 377 |
| 22 | Josh Green | +1.48 | 399 |
| 23 | Kelly Oubre Jr. | +1.46 | 224 |
| 24 | Sam Hauser | +1.45 | 283 |
| 25 | Christian Braun | +1.28 | 204 |
| 26 | Derrick White | +1.22 | 676 |
| 27 | LeBron James | +1.11 | 204 |
| 28 | Jrue Holiday | +1.11 | 720 |
| 29 | Donovan Mitchell | +1.09 | 382 |
| 30 | Gary Harris | +0.97 | 159 |
| 31 | Anthony Edwards | +0.96 | 649 |
| 32 | Shai Gilgeous-Alexander | +0.96 | 399 |
| 33 | Luguentz Dort | +0.95 | 350 |
| 34 | Bobby Portis | +0.87 | 187 |
| 35 | D'Angelo Russell | +0.79 | 185 |
| 36 | Daniel Gafford | +0.66 | 445 |
| 37 | Nikola Jokic | +0.53 | 482 |
| 38 | Mike Conley | +0.52 | 474 |
| 39 | T.J. McConnell | +0.52 | 348 |
| 40 | Devin Booker | +0.46 | 166 |
| 41 | Aaron Nesmith | +0.42 | 559 |
| 42 | Paul George | +0.41 | 222 |
| 43 | OG Anunoby | +0.40 | 324 |
| 44 | Myles Turner | +0.37 | 550 |
| 45 | Jayson Tatum | +0.36 | 768 |
| 46 | Tyrese Haliburton | +0.29 | 522 |
| 47 | Jaden McDaniels | +0.25 | 537 |
| 48 | Maxi Kleber | +0.16 | 219 |
| 49 | Isaiah Joe | +0.14 | 173 |
| 50 | Derrick Jones Jr. | +0.13 | 647 |

## 2024-25 Regular season — defense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +4.31 | 1527 |
| 2 | Toumani Camara | +3.73 | 2548 |
| 3 | Rudy Gobert | +3.65 | 2388 |
| 4 | Luke Kornet | +3.56 | 1361 |
| 5 | Ausar Thompson | +3.56 | 1328 |
| 6 | Alperen Sengun | +3.42 | 2394 |
| 7 | Kris Dunn | +3.42 | 1783 |
| 8 | Ivica Zubac | +2.86 | 2624 |
| 9 | Luguentz Dort | +2.60 | 2073 |
| 10 | Dyson Daniels | +2.57 | 2571 |
| 11 | Evan Mobley | +2.55 | 2167 |
| 12 | Brandin Podziemski | +2.54 | 1716 |
| 13 | Jarrett Allen | +2.50 | 2296 |
| 14 | Jaxson Hayes | +2.48 | 1093 |
| 15 | Nicolas Batum | +2.47 | 1367 |
| 16 | Amen Thompson | +2.44 | 2225 |
| 17 | Isaiah Hartenstein | +2.30 | 1590 |
| 18 | Draymond Green | +2.29 | 1983 |
| 19 | Jaden McDaniels | +2.29 | 2614 |
| 20 | Jaren Jackson Jr. | +2.28 | 2207 |
| 21 | Scotty Pippen Jr. | +2.26 | 1683 |
| 22 | Anthony Davis | +2.25 | 1706 |
| 23 | Isaiah Stewart | +2.16 | 1434 |
| 24 | Donovan Clingan | +2.14 | 1324 |
| 25 | Myles Turner | +2.11 | 2174 |
| 26 | Kristaps Porziņģis | +2.11 | 1210 |
| 27 | Paul George | +2.09 | 1334 |
| 28 | Kevon Looney | +2.06 | 1142 |
| 29 | Jonathan Isaac | +2.06 | 1090 |
| 30 | Jalen Williams | +1.98 | 2237 |
| 31 | Jabari Smith Jr. | +1.82 | 1716 |
| 32 | Jakob Poeltl | +1.79 | 1686 |
| 33 | Franz Wagner | +1.77 | 2023 |
| 34 | Shai Gilgeous-Alexander | +1.76 | 2598 |
| 35 | Daniel Gafford | +1.75 | 1226 |
| 36 | Keon Ellis | +1.73 | 1948 |
| 37 | Brandon Clarke | +1.72 | 1207 |
| 38 | Kevin Porter Jr. | +1.71 | 1482 |
| 39 | Sam Merrill | +1.70 | 1401 |
| 40 | Wendell Carter Jr. | +1.68 | 1758 |
| 41 | Luka Dončić | +1.62 | 1769 |
| 42 | John Collins | +1.57 | 1220 |
| 43 | Tari Eason | +1.57 | 1420 |
| 44 | Donte DiVincenzo | +1.53 | 1606 |
| 45 | Kentavious Caldwell-Pope | +1.49 | 2279 |
| 46 | Goga Bitadze | +1.47 | 1430 |
| 47 | Dorian Finney-Smith | +1.46 | 1821 |
| 48 | Cody Martin | +1.46 | 1173 |
| 49 | Scottie Barnes | +1.45 | 2134 |
| 50 | Derrick White | +1.42 | 2574 |

## 2024-25 Playoffs — defense, top 50 (projected, no truth)

> pool 109 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.77 | 411 |
| 2 | Nikola Jokić | +4.69 | 563 |
| 3 | Alperen Sengun | +4.51 | 256 |
| 4 | Kenrich Williams | +4.30 | 137 |
| 5 | Jayson Tatum | +4.11 | 322 |
| 6 | Draymond Green | +3.93 | 389 |
| 7 | Alex Caruso | +3.91 | 562 |
| 8 | Chet Holmgren | +3.72 | 686 |
| 9 | Ausar Thompson | +3.60 | 135 |
| 10 | Gary Trent Jr. | +3.48 | 171 |
| 11 | Steven Adams | +3.43 | 155 |
| 12 | Jaden McDaniels | +3.39 | 497 |
| 13 | Pascal Siakam | +3.31 | 771 |
| 14 | Aaron Nesmith | +3.24 | 650 |
| 15 | Cason Wallace | +3.04 | 516 |
| 16 | Cade Cunningham | +2.98 | 248 |
| 17 | Jaylin Williams | +2.85 | 141 |
| 18 | Luguentz Dort | +2.28 | 666 |
| 19 | Mitchell Robinson | +2.21 | 370 |
| 20 | Luke Kornet | +2.08 | 180 |
| 21 | Jarrett Allen | +1.99 | 261 |
| 22 | Aaron Wiggins | +1.97 | 303 |
| 23 | Bobby Portis | +1.94 | 158 |
| 24 | OG Anunoby | +1.87 | 705 |
| 25 | Brandin Podziemski | +1.69 | 385 |
| 26 | Mike Conley | +1.68 | 356 |
| 27 | Jabari Smith Jr. | +1.67 | 143 |
| 28 | Christian Braun | +1.66 | 544 |
| 29 | Tari Eason | +1.66 | 132 |
| 30 | Shai Gilgeous-Alexander | +1.58 | 851 |
| 31 | Isaiah Hartenstein | +1.37 | 516 |
| 32 | Franz Wagner | +1.32 | 195 |
| 33 | Jaylen Brown | +1.25 | 402 |
| 34 | Kentavious Caldwell-Pope | +1.16 | 163 |
| 35 | Jamal Murray | +1.13 | 578 |
| 36 | Andrew Nembhard | +1.04 | 769 |
| 37 | Karl-Anthony Towns | +0.79 | 639 |
| 38 | Anthony Edwards | +0.76 | 585 |
| 39 | Buddy Hield | +0.74 | 327 |
| 40 | Kevin Porter Jr. | +0.69 | 151 |
| 41 | Peyton Watson | +0.67 | 199 |
| 42 | Quinten Post | +0.51 | 146 |
| 43 | Tyrese Haliburton | +0.50 | 772 |
| 44 | Amen Thompson | +0.50 | 231 |
| 45 | Dorian Finney-Smith | +0.45 | 170 |
| 46 | Josh Hart | +0.43 | 642 |
| 47 | Jalen Green | +0.26 | 219 |
| 48 | T.J. McConnell | +0.25 | 402 |
| 49 | Julius Randle | +0.19 | 533 |
| 50 | Kristaps Porziņģis | +0.18 | 231 |

## 2025-26 Regular season — defense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +5.67 | 1866 |
| 2 | Chet Holmgren | +4.30 | 1997 |
| 3 | Neemias Queta | +3.74 | 1926 |
| 4 | Isaiah Hartenstein | +3.64 | 1137 |
| 5 | Ausar Thompson | +3.64 | 1896 |
| 6 | Ronald Holland II | +3.46 | 1550 |
| 7 | Derrick White | +3.44 | 2625 |
| 8 | Cason Wallace | +3.40 | 2046 |
| 9 | Rudy Gobert | +3.20 | 2380 |
| 10 | Ajay Mitchell | +2.91 | 1473 |
| 11 | Javonte Green | +2.86 | 1446 |
| 12 | Hugo González | +2.86 | 1084 |
| 13 | Toumani Camara | +2.75 | 2731 |
| 14 | Baylor Scheierman | +2.62 | 1429 |
| 15 | Marcus Smart | +2.59 | 1769 |
| 16 | Jalen Suggs | +2.57 | 1574 |
| 17 | Jordan Goodwin | +2.50 | 1572 |
| 18 | Dyson Daniels | +2.50 | 2520 |
| 19 | Keon Ellis | +2.46 | 1479 |
| 20 | Jamal Shead | +2.37 | 1852 |
| 21 | Dru Smith | +2.22 | 1141 |
| 22 | Donte DiVincenzo | +2.22 | 2494 |
| 23 | Paul George | +2.20 | 1135 |
| 24 | Jaylin Williams | +2.13 | 1277 |
| 25 | Jusuf Nurkić | +2.10 | 1083 |
| 26 | Mitchell Robinson | +2.06 | 1175 |
| 27 | Josh Okogie | +2.01 | 1354 |
| 28 | Sidy Cissoko | +1.99 | 1435 |
| 29 | Scottie Barnes | +1.96 | 2681 |
| 30 | Ryan Kalkbrenner | +1.96 | 1479 |
| 31 | Sion James | +1.96 | 1843 |
| 32 | Luke Kornet | +1.90 | 1430 |
| 33 | Naz Reid | +1.89 | 2007 |
| 34 | John Konchar | +1.88 | 1115 |
| 35 | Ryan Dunn | +1.88 | 1355 |
| 36 | Jarrett Allen | +1.88 | 1519 |
| 37 | OG Anunoby | +1.86 | 2224 |
| 38 | Evan Mobley | +1.82 | 2074 |
| 39 | Collin Gillespie | +1.81 | 2282 |
| 40 | Oso Ighodaro | +1.76 | 1808 |
| 41 | Jaren Jackson Jr. | +1.74 | 1455 |
| 42 | Nickeil Alexander-Walker | +1.66 | 2603 |
| 43 | Landry Shamet | +1.66 | 1171 |
| 44 | Nikola Jokić | +1.62 | 2265 |
| 45 | Jalen Smith | +1.62 | 1095 |
| 46 | Brook Lopez | +1.61 | 1635 |
| 47 | Bam Adebayo | +1.59 | 2365 |
| 48 | Stephon Castle | +1.59 | 2038 |
| 49 | Collin Murray-Boyles | +1.53 | 1246 |
| 50 | Luguentz Dort | +1.51 | 1849 |

## 2025-26 Playoffs — defense, top 50 (projected, no truth)

> pool 112 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jose Alvarado | +5.36 | 170 |
| 2 | Victor Wembanyama | +5.11 | 750 |
| 3 | Karl-Anthony Towns | +4.69 | 578 |
| 4 | Cason Wallace | +4.37 | 374 |
| 5 | Alex Caruso | +4.23 | 353 |
| 6 | Tari Eason | +4.08 | 195 |
| 7 | Ajay Mitchell | +3.99 | 317 |
| 8 | Neemias Queta | +3.89 | 152 |
| 9 | Ausar Thompson | +3.86 | 427 |
| 10 | Josh Hart | +3.54 | 614 |
| 11 | Amen Thompson | +3.54 | 264 |
| 12 | Javonte Green | +3.34 | 132 |
| 13 | Alperen Sengun | +3.26 | 232 |
| 14 | Rudy Gobert | +2.52 | 372 |
| 15 | Jaylin Williams | +2.48 | 240 |
| 16 | OG Anunoby | +2.48 | 586 |
| 17 | Toumani Camara | +2.33 | 165 |
| 18 | Marcus Smart | +2.29 | 345 |
| 19 | VJ Edgecombe | +2.24 | 407 |
| 20 | Dylan Harper | +2.05 | 615 |
| 21 | Jakob Poeltl | +2.03 | 134 |
| 22 | Jarrett Allen | +1.90 | 529 |
| 23 | Isaiah Stewart | +1.83 | 165 |
| 24 | Devin Vassell | +1.80 | 801 |
| 25 | Mikal Bridges | +1.76 | 608 |
| 26 | Dean Wade | +1.67 | 407 |
| 27 | Julian Champagnie | +1.63 | 705 |
| 28 | Nikola Jokić | +1.58 | 237 |
| 29 | Jaden McDaniels | +1.56 | 406 |
| 30 | De'Aaron Fox | +1.52 | 704 |
| 31 | Luke Kornet | +1.44 | 296 |
| 32 | Anthony Black | +1.43 | 196 |
| 33 | Jaylen Brown | +1.24 | 249 |
| 34 | Isaiah Hartenstein | +1.09 | 350 |
| 35 | Christian Braun | +1.06 | 187 |
| 36 | Dyson Daniels | +1.05 | 166 |
| 37 | Reed Sheppard | +1.01 | 192 |
| 38 | Jabari Smith Jr. | +1.01 | 252 |
| 39 | Chet Holmgren | +0.99 | 459 |
| 40 | Ja'Kobe Walter | +0.97 | 224 |
| 41 | Mike Conley | +0.95 | 168 |
| 42 | Mitchell Robinson | +0.91 | 251 |
| 43 | Collin Murray-Boyles | +0.88 | 191 |
| 44 | Paolo Banchero | +0.85 | 273 |
| 45 | Jaxson Hayes | +0.71 | 163 |
| 46 | James Harden | +0.62 | 672 |
| 47 | Evan Mobley | +0.53 | 640 |
| 48 | Isaiah Joe | +0.52 | 143 |
| 49 | Paul George | +0.49 | 394 |
| 50 | Jamal Shead | +0.42 | 224 |
