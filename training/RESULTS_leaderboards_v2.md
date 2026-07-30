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
| total | stage2-0 / l2 | blend | 1149 | 1.173 | +0.789 | +0.890 |
| offense | stage2-0 / l2 | blend | 1183 | 0.666 | +0.867 | +0.928 |
| defense | stage2-0 / huber | blend | 649 | 1.006 | +0.655 | +0.827 |

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
| total | 2013-14 | Playoffs | 99 | 1.919 | 2.538 | +0.398 | +0.347 | 25/30 | 23/30 |
| total | 2013-14 | Regular season | 246 | 0.879 | 0.945 | +0.384 | +0.480 | 22/30 | 22/30 |
| total | 2014-15 | Playoffs | 98 | 1.883 | 2.462 | +0.421 | +0.338 | 21/30 | 21/30 |
| total | 2014-15 | Regular season | 246 | 0.885 | 0.914 | +0.628 | +0.595 | 23/30 | 22/30 |
| offense | 2013-14 | Playoffs | 99 | 0.939 | 1.437 | +0.637 | +0.628 | 27/30 | 22/30 |
| offense | 2013-14 | Regular season | 246 | 0.527 | 0.713 | +0.738 | +0.811 | 24/30 | 22/30 |
| offense | 2014-15 | Playoffs | 98 | 1.028 | 1.686 | +0.591 | +0.444 | 26/30 | 20/30 |
| offense | 2014-15 | Regular season | 246 | 0.544 | 0.722 | +0.651 | +0.490 | 25/30 | 23/30 |
| defense | 2013-14 | Playoffs | 99 | 1.634 | 1.840 | +0.182 | +0.094 | 22/30 | 20/30 |
| defense | 2013-14 | Regular season | 246 | 0.733 | 0.925 | +0.384 | +0.352 | 21/30 | 17/30 |
| defense | 2014-15 | Playoffs | 98 | 1.679 | 2.090 | +0.416 | +0.310 | 21/30 | 15/30 |
| defense | 2014-15 | Regular season | 246 | 0.754 | 0.846 | +0.609 | +0.582 | 23/30 | 19/30 |

## Kendall tau over the top 30, held-out seasons

`tau(true30)` compares the true order of the true top 30 against their
projected order. `tau(union30)` widens the set to the union of the true and
projected top 30, so it also penalises wrongly promoted players.

| target | season | split | pool | tau(true30) | tau(union30) | hits@30 | mean &#124;Δrank&#124; |
|---|---|---|---|---|---|---|---|
| total | 2013-14 | Playoffs | 99 | +0.398 | +0.274 | 25/30 | 12.0 |
| total | 2013-14 | Regular season | 247 | +0.366 | +0.334 | 22/30 | 14.0 |
| total | 2014-15 | Playoffs | 99 | +0.421 | +0.290 | 21/30 | 11.9 |
| total | 2014-15 | Regular season | 247 | +0.623 | +0.388 | 22/30 | 13.9 |
| offense | 2013-14 | Playoffs | 99 | +0.637 | +0.655 | 27/30 | 8.2 |
| offense | 2013-14 | Regular season | 247 | +0.756 | +0.613 | 24/30 | 10.9 |
| offense | 2014-15 | Playoffs | 99 | +0.586 | +0.583 | 26/30 | 7.9 |
| offense | 2014-15 | Regular season | 247 | +0.651 | +0.556 | 25/30 | 9.3 |
| defense | 2013-14 | Playoffs | 99 | +0.182 | +0.124 | 22/30 | 17.3 |
| defense | 2013-14 | Regular season | 247 | +0.411 | +0.336 | 21/30 | 16.5 |
| defense | 2014-15 | Playoffs | 99 | +0.361 | +0.274 | 21/30 | 14.2 |
| defense | 2014-15 | Regular season | 247 | +0.660 | +0.468 | 22/30 | 20.6 |

## 2013-14 Playoffs — total, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.398 &nbsp;·&nbsp; hits@30 25/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +8.94 | +13.20 | 1 | +0 | Chris Paul | +13.20 |
| 2 | LeBron James | +8.41 | +7.70 | 8 | +6 | Draymond Green | +10.80 |
| 3 | Manu Ginobili | +6.83 | +7.90 | 7 | +4 | Stephen Curry | +10.40 |
| 4 | Stephen Curry | +6.71 | +10.40 | 3 | -1 | James Harden | +10.00 |
| 5 | Kevin Durant | +6.64 | +5.60 | 20 | +15 | Paul Millsap | +8.40 |
| 6 | Damian Lillard | +6.53 | +6.90 | 12 | +6 | Vince Carter | +8.10 |
| 7 | Patrick Patterson | +6.28 | +1.40 | 45 | +38 | Manu Ginobili | +7.90 |
| 8 | Russell Westbrook | +6.24 | +6.40 | 14 | +6 | LeBron James | +7.70 |
| 9 | Danny Green | +6.13 | +6.40 | 15 | +6 | Greivis Vasquez | +7.30 |
| 10 | James Harden | +6.02 | +10.00 | 4 | -6 | Patty Mills | +7.20 |
| 11 | Vince Carter | +6.01 | +8.10 | 6 | -5 | Andray Blatche | +6.90 |
| 12 | Kawhi Leonard | +5.99 | +6.20 | 16 | +4 | Damian Lillard | +6.90 |
| 13 | Kyle Lowry | +5.99 | +4.30 | 31 | +18 | Deron Williams | +6.60 |
| 14 | Patty Mills | +5.56 | +7.20 | 10 | -4 | Russell Westbrook | +6.40 |
| 15 | Greivis Vasquez | +5.43 | +7.30 | 9 | -6 | Danny Green | +6.40 |
| 16 | Kyle Korver | +4.63 | +2.30 | 41 | +25 | Kawhi Leonard | +6.20 |
| 17 | Trevor Ariza | +4.63 | +5.50 | 23 | +6 | LaMarcus Aldridge | +6.00 |
| 18 | Joe Johnson | +4.53 | +5.30 | 25 | +7 | Chris Andersen | +6.00 |
| 19 | Draymond Green | +4.49 | +10.80 | 2 | -17 | Bradley Beal | +5.70 |
| 20 | Serge Ibaka | +4.36 | +5.40 | 24 | +4 | Kevin Durant | +5.60 |
| 21 | Deron Williams | +4.31 | +6.60 | 13 | -8 | Rashard Lewis | +5.50 |
| 22 | Marcin Gortat | +4.03 | +4.80 | 27 | +5 | Tiago Splitter | +5.50 |
| 23 | Tony Allen | +4.00 | +4.30 | 30 | +7 | Trevor Ariza | +5.50 |
| 24 | John Wall | +3.84 | +0.60 | 49 | +25 | Serge Ibaka | +5.40 |
| 25 | David West | +3.79 | +3.10 | 37 | +12 | Joe Johnson | +5.30 |
| 26 | LaMarcus Aldridge | +3.77 | +6.00 | 17 | -9 | JJ Redick | +5.00 |
| 27 | Blake Griffin | +3.75 | +4.50 | 29 | +2 | Marcin Gortat | +4.80 |
| 28 | Andray Blatche | +3.64 | +6.90 | 11 | -17 | Pero Antic | +4.60 |
| 29 | Tiago Splitter | +3.48 | +5.50 | 22 | -7 | Blake Griffin | +4.50 |
| 30 | Bradley Beal | +3.07 | +5.70 | 19 | -11 | Tony Allen | +4.30 |
| 31 | Paul Millsap | +3.05 | +8.40 | 5 | -26 | Kyle Lowry | +4.30 |
| 32 | George Hill | +3.01 | +2.20 | 42 | +10 | Tim Duncan | +3.80 |
| 33 | Chris Andersen | +2.84 | +6.00 | 18 | -15 | Nicolas Batum | +3.60 |
| 34 | Ray Allen | +2.80 | +1.90 | 44 | +10 | Devin Harris | +3.50 |
| 35 | Nicolas Batum | +2.79 | +3.60 | 33 | -2 | Mirza Teletovic | +3.50 |
| 36 | Kevin Garnett | +2.74 | -0.70 | 60 | +24 | Marc Gasol | +3.40 |
| 37 | Mike Conley | +2.55 | -0.70 | 61 | +24 | David West | +3.10 |
| 38 | Devin Harris | +2.50 | +3.50 | 34 | -4 | Boris Diaw | +2.80 |
| 39 | Tim Duncan | +2.46 | +3.80 | 32 | -7 | Joakim Noah | +2.50 |
| 40 | David Lee | +2.34 | -3.50 | 89 | +49 | Nick Collison | +2.40 |
| 41 | Paul Pierce | +2.33 | +1.20 | 47 | +6 | Kyle Korver | +2.30 |
| 42 | Dwight Howard | +2.26 | +2.00 | 43 | +1 | George Hill | +2.20 |
| 43 | Joakim Noah | +2.19 | +2.50 | 39 | -4 | Dwight Howard | +2.00 |
| 44 | Pero Antic | +2.16 | +4.60 | 28 | -16 | Ray Allen | +1.90 |
| 45 | Mirza Teletovic | +2.14 | +3.50 | 35 | -10 | Patrick Patterson | +1.40 |
| 46 | Lance Stephenson | +2.08 | -1.30 | 70 | +24 | Chris Bosh | +1.30 |
| 47 | JJ Redick | +1.85 | +5.00 | 26 | -21 | Paul Pierce | +1.20 |
| 48 | Jeremy Lin | +1.85 | -1.00 | 66 | +18 | DeAndre Jordan | +0.80 |
| 49 | Rashard Lewis | +1.83 | +5.50 | 21 | -28 | John Wall | +0.60 |
| 50 | Boris Diaw | +1.82 | +2.80 | 38 | -12 | Chandler Parsons | +0.60 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.366 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.59 | +11.00 | 1 | +0 | Chris Paul | +11.00 |
| 2 | Kevin Durant | +7.04 | +7.10 | 2 | +0 | Kevin Durant | +7.10 |
| 3 | LeBron James | +6.72 | +4.60 | 15 | +12 | Kawhi Leonard | +6.70 |
| 4 | Paul George | +6.07 | +5.60 | 8 | +4 | Kevin Love | +6.60 |
| 5 | Kevin Love | +5.88 | +6.60 | 4 | -1 | James Harden | +6.10 |
| 6 | Kawhi Leonard | +5.55 | +6.70 | 3 | -3 | Joakim Noah | +5.90 |
| 7 | Andre Iguodala | +5.37 | +3.80 | 23 | +16 | Kyle Lowry | +5.70 |
| 8 | Manu Ginobili | +5.36 | +5.10 | 9 | +1 | Paul George | +5.60 |
| 9 | James Harden | +5.17 | +6.10 | 5 | -4 | Manu Ginobili | +5.10 |
| 10 | Blake Griffin | +5.09 | +3.20 | 34 | +24 | DeMarcus Cousins | +5.00 |
| 11 | Dirk Nowitzki | +4.88 | +4.70 | 13 | +2 | Goran Dragic | +5.00 |
| 12 | Goran Dragic | +4.88 | +5.00 | 11 | -1 | Patty Mills | +4.80 |
| 13 | Andrew Bogut | +4.71 | +3.10 | 37 | +24 | Dirk Nowitzki | +4.70 |
| 14 | Carmelo Anthony | +4.62 | +3.80 | 24 | +10 | Danny Green | +4.70 |
| 15 | Kyle Lowry | +4.42 | +5.70 | 7 | -8 | LeBron James | +4.60 |
| 16 | Jimmy Butler | +4.14 | +3.90 | 20 | +4 | Anderson Varejao | +4.10 |
| 17 | Isaiah Thomas | +3.75 | +3.90 | 19 | +2 | Patrick Beverley | +4.10 |
| 18 | Draymond Green | +3.62 | +3.40 | 30 | +12 | Mario Chalmers | +4.00 |
| 19 | Anthony Davis | +3.58 | +3.50 | 28 | +9 | Isaiah Thomas | +3.90 |
| 20 | Ricky Rubio | +3.57 | +3.70 | 26 | +6 | Jimmy Butler | +3.90 |
| 21 | Joakim Noah | +3.56 | +5.90 | 6 | -15 | Mike Conley | +3.80 |
| 22 | LaMarcus Aldridge | +3.55 | +3.40 | 29 | +7 | Kemba Walker | +3.80 |
| 23 | Patty Mills | +3.52 | +4.80 | 12 | -11 | Andre Iguodala | +3.80 |
| 24 | Anderson Varejao | +3.49 | +4.10 | 16 | -8 | Carmelo Anthony | +3.80 |
| 25 | Nicolas Batum | +3.35 | +1.90 | 59 | +34 | Russell Westbrook | +3.70 |
| 26 | Paul Millsap | +3.30 | +3.10 | 36 | +10 | Ricky Rubio | +3.70 |
| 27 | Deron Williams | +3.27 | +3.20 | 35 | +8 | Eric Bledsoe | +3.70 |
| 28 | Chris Bosh | +3.27 | +0.90 | 94 | +66 | Anthony Davis | +3.50 |
| 29 | David West | +3.26 | +2.20 | 51 | +22 | LaMarcus Aldridge | +3.40 |
| 30 | DeAndre Jordan | +3.21 | +1.80 | 64 | +34 | Draymond Green | +3.40 |
| 31 | Damian Lillard | +3.15 | +2.10 | 56 | +25 | Nikola Pekovic | +3.30 |
| 32 | Russell Westbrook | +3.12 | +3.70 | 25 | -7 | DeMarre Carroll | +3.30 |
| 33 | Nikola Pekovic | +3.11 | +3.30 | 31 | -2 | Tiago Splitter | +3.30 |
| 34 | Mike Conley | +3.10 | +3.80 | 21 | -13 | Blake Griffin | +3.20 |
| 35 | Danny Green | +3.03 | +4.70 | 14 | -21 | Deron Williams | +3.20 |
| 36 | Kemba Walker | +2.99 | +3.80 | 22 | -14 | Paul Millsap | +3.10 |
| 37 | Pablo Prigioni | +2.98 | +1.60 | 73 | +36 | Andrew Bogut | +3.10 |
| 38 | Patrick Beverley | +2.93 | +4.10 | 17 | -21 | Kris Humphries | +3.00 |
| 39 | Mario Chalmers | +2.86 | +4.00 | 18 | -21 | Klay Thompson | +2.90 |
| 40 | Wesley Matthews | +2.84 | +2.60 | 46 | +6 | Ty Lawson | +2.90 |
| 41 | Trevor Ariza | +2.81 | +1.70 | 67 | +26 | Jae Crowder | +2.90 |
| 42 | Paul Pierce | +2.80 | +1.90 | 60 | +18 | Robin Lopez | +2.90 |
| 43 | Nick Collison | +2.78 | +1.20 | 86 | +43 | Vince Carter | +2.90 |
| 44 | Derek Fisher | +2.77 | +2.30 | 50 | +6 | Darren Collison | +2.70 |
| 45 | Marcin Gortat | +2.77 | +2.00 | 58 | +13 | Shane Battier | +2.70 |
| 46 | George Hill | +2.72 | +2.50 | 48 | +2 | Wesley Matthews | +2.60 |
| 47 | DeMarre Carroll | +2.69 | +3.30 | 32 | -15 | Tony Allen | +2.60 |
| 48 | DeMarcus Cousins | +2.68 | +5.00 | 10 | -38 | George Hill | +2.50 |
| 49 | Channing Frye | +2.60 | +2.40 | 49 | +0 | Channing Frye | +2.40 |
| 50 | Tiago Splitter | +2.49 | +3.30 | 33 | -17 | Derek Fisher | +2.30 |

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

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.421 &nbsp;·&nbsp; hits@30 21/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Jarrett Jack | +9.68 | +11.30 | 1 | +0 | Jarrett Jack | +11.30 |
| 2 | Tim Duncan | +8.04 | +7.90 | 8 | +6 | Chris Paul | +11.10 |
| 3 | Jimmy Butler | +7.92 | +9.00 | 4 | +1 | AlFarouq Aminu | +11.00 |
| 4 | Stephen Curry | +7.34 | +8.70 | 7 | +3 | Jimmy Butler | +9.00 |
| 5 | AlFarouq Aminu | +7.27 | +11.00 | 3 | -2 | Alan Anderson | +8.90 |
| 6 | Alan Anderson | +6.92 | +8.90 | 5 | -1 | Otto Porter Jr. | +8.80 |
| 7 | Chris Paul | +6.78 | +11.10 | 2 | -5 | Stephen Curry | +8.70 |
| 8 | DeAndre Jordan | +6.05 | +3.60 | 33 | +25 | Tim Duncan | +7.90 |
| 9 | LeBron James | +5.97 | +4.70 | 25 | +16 | Danny Green | +7.80 |
| 10 | Otto Porter Jr. | +5.65 | +8.80 | 6 | -4 | Trevor Ariza | +7.30 |
| 11 | James Harden | +5.53 | +6.40 | 13 | +2 | Blake Griffin | +7.20 |
| 12 | Mike Dunleavy | +5.40 | +5.30 | 19 | +7 | JJ Barea | +6.50 |
| 13 | Blake Griffin | +5.34 | +7.20 | 11 | -2 | James Harden | +6.40 |
| 14 | Monta Ellis | +5.04 | +3.30 | 35 | +21 | Manu Ginobili | +6.30 |
| 15 | Kyle Korver | +4.79 | +2.30 | 42 | +27 | Marc Gasol | +6.10 |
| 16 | JJ Barea | +4.73 | +6.50 | 12 | -4 | Derrick Rose | +5.80 |
| 17 | CJ McCollum | +4.48 | +5.60 | 18 | +1 | Dwight Howard | +5.70 |
| 18 | Brook Lopez | +4.47 | +4.40 | 28 | +10 | CJ McCollum | +5.60 |
| 19 | Marcin Gortat | +4.23 | +1.70 | 46 | +27 | Mike Dunleavy | +5.30 |
| 20 | Dwight Howard | +4.21 | +5.70 | 17 | -3 | Timofey Mozgov | +5.30 |
| 21 | Pau Gasol | +4.17 | +3.40 | 34 | +13 | Matt Barnes | +5.00 |
| 22 | Paul Millsap | +4.16 | +5.00 | 22 | +0 | Paul Millsap | +5.00 |
| 23 | Mike Conley | +4.02 | +5.00 | 24 | +1 | Tony Allen | +5.00 |
| 24 | Derrick Rose | +3.59 | +5.80 | 16 | -8 | Mike Conley | +5.00 |
| 25 | Al Horford | +3.30 | +3.90 | 30 | +5 | LeBron James | +4.70 |
| 26 | Kyrie Irving | +3.14 | +1.00 | 52 | +26 | Tristan Thompson | +4.60 |
| 27 | Andre Iguodala | +3.12 | +2.60 | 39 | +12 | Anthony Davis | +4.40 |
| 28 | Dirk Nowitzki | +3.06 | -0.30 | 63 | +35 | Brook Lopez | +4.40 |
| 29 | DeMarre Carroll | +3.06 | +2.10 | 44 | +15 | Bradley Beal | +4.20 |
| 30 | Danny Green | +3.05 | +7.80 | 9 | -21 | Al Horford | +3.90 |
| 31 | Jeff Teague | +2.96 | +2.90 | 36 | +5 | Ramon Sessions | +3.80 |
| 32 | Drew Gooden | +2.68 | -0.80 | 69 | +37 | Vince Carter | +3.80 |
| 33 | Manu Ginobili | +2.64 | +6.30 | 14 | -19 | DeAndre Jordan | +3.60 |
| 34 | Anthony Davis | +2.63 | +4.40 | 27 | -7 | Pau Gasol | +3.40 |
| 35 | Bradley Beal | +2.54 | +4.20 | 29 | -6 | Monta Ellis | +3.30 |
| 36 | Tristan Thompson | +2.43 | +4.60 | 26 | -10 | Jeff Teague | +2.90 |
| 37 | Matt Barnes | +2.42 | +5.00 | 21 | -16 | Iman Shumpert | +2.80 |
| 38 | Timofey Mozgov | +2.28 | +5.30 | 20 | -18 | Kawhi Leonard | +2.80 |
| 39 | Josh Smith | +2.19 | +2.30 | 43 | +4 | Andre Iguodala | +2.60 |
| 40 | Tony Allen | +2.13 | +5.00 | 23 | -17 | JR Smith | +2.60 |
| 41 | Trevor Ariza | +2.07 | +7.30 | 10 | -31 | John Wall | +2.30 |
| 42 | JR Smith | +2.06 | +2.60 | 40 | -2 | Kyle Korver | +2.30 |
| 43 | Vince Carter | +2.04 | +3.80 | 32 | -11 | Josh Smith | +2.30 |
| 44 | Joakim Noah | +1.83 | +0.90 | 54 | +10 | DeMarre Carroll | +2.10 |
| 45 | DeMar DeRozan | +1.66 | -1.50 | 73 | +28 | Klay Thompson | +1.80 |
| 46 | Iman Shumpert | +1.52 | +2.80 | 37 | -9 | Marcin Gortat | +1.70 |
| 47 | Harrison Barnes | +1.52 | -0.70 | 67 | +20 | Courtney Lee | +1.60 |
| 48 | Boris Diaw | +1.50 | -1.20 | 71 | +23 | OJ Mayo | +1.60 |
| 49 | Ramon Sessions | +1.49 | +3.80 | 31 | -18 | Avery Bradley | +1.40 |
| 50 | Courtney Lee | +1.47 | +1.60 | 47 | -3 | Nicolas Batum | +1.30 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.623 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | LeBron James | +7.59 | +5.10 | 11 | +10 | Chris Paul | +10.60 |
| 2 | Chris Paul | +7.37 | +10.60 | 1 | -1 | Kawhi Leonard | +8.90 |
| 3 | Anthony Davis | +6.92 | +8.80 | 3 | +0 | Anthony Davis | +8.80 |
| 4 | Draymond Green | +6.92 | +6.50 | 5 | +1 | James Harden | +7.70 |
| 5 | James Harden | +6.82 | +7.70 | 4 | -1 | Draymond Green | +6.50 |
| 6 | Kawhi Leonard | +6.64 | +8.90 | 2 | -4 | Danny Green | +6.10 |
| 7 | Jimmy Butler | +6.00 | +3.00 | 36 | +29 | George Hill | +5.60 |
| 8 | Klay Thompson | +5.17 | +5.30 | 10 | +2 | Russell Westbrook | +5.60 |
| 9 | Russell Westbrook | +4.94 | +5.60 | 8 | -1 | DeMarcus Cousins | +5.40 |
| 10 | Lou Williams | +4.90 | +3.00 | 35 | +25 | Klay Thompson | +5.30 |
| 11 | George Hill | +4.45 | +5.60 | 7 | -4 | LeBron James | +5.10 |
| 12 | Kyrie Irving | +4.40 | +4.60 | 13 | +1 | Khris Middleton | +4.80 |
| 13 | Danny Green | +4.34 | +6.10 | 6 | -7 | Kyrie Irving | +4.60 |
| 14 | Andrew Bogut | +4.31 | +3.70 | 23 | +9 | Kyle Korver | +4.60 |
| 15 | Khris Middleton | +4.22 | +4.80 | 12 | -3 | DeAndre Jordan | +4.60 |
| 16 | Wesley Matthews | +4.12 | +3.60 | 24 | +8 | LaMarcus Aldridge | +4.30 |
| 17 | Damian Lillard | +4.09 | +2.70 | 42 | +25 | Tony Allen | +4.30 |
| 18 | Tony Allen | +3.97 | +4.30 | 17 | -1 | Nikola Mirotic | +4.20 |
| 19 | DeMarcus Cousins | +3.92 | +5.40 | 9 | -10 | Rudy Gobert | +4.10 |
| 20 | Blake Griffin | +3.85 | +2.00 | 63 | +43 | Marc Gasol | +4.00 |
| 21 | Gordon Hayward | +3.79 | +3.40 | 26 | +5 | Darren Collison | +4.00 |
| 22 | LaMarcus Aldridge | +3.76 | +4.30 | 16 | -6 | Kyle Lowry | +3.90 |
| 23 | Nikola Mirotic | +3.40 | +4.20 | 18 | -5 | Andrew Bogut | +3.70 |
| 24 | Isaiah Thomas | +3.30 | +1.60 | 72 | +48 | Wesley Matthews | +3.60 |
| 25 | Tyson Chandler | +3.24 | +2.60 | 48 | +23 | Jonas Jerebko | +3.60 |
| 26 | Manu Ginobili | +3.17 | +3.20 | 33 | +7 | Gordon Hayward | +3.40 |
| 27 | Mike Conley | +3.16 | +2.90 | 39 | +12 | Tim Duncan | +3.30 |
| 28 | Rudy Gobert | +3.16 | +4.10 | 19 | -9 | Paul Millsap | +3.30 |
| 29 | DeAndre Jordan | +3.11 | +4.60 | 15 | -14 | Marcin Gortat | +3.20 |
| 30 | Kyle Korver | +3.03 | +4.60 | 14 | -16 | Kevin Love | +3.20 |
| 31 | John Wall | +2.98 | +2.00 | 64 | +33 | JJ Redick | +3.20 |
| 32 | Paul Millsap | +2.98 | +3.30 | 28 | -4 | Brandon Jennings | +3.20 |
| 33 | Danilo Gallinari | +2.95 | +3.00 | 34 | +1 | Manu Ginobili | +3.20 |
| 34 | Andre Iguodala | +2.95 | +1.30 | 82 | +48 | Danilo Gallinari | +3.00 |
| 35 | Kyle Lowry | +2.92 | +3.90 | 22 | -13 | Lou Williams | +3.00 |
| 36 | Jeff Teague | +2.89 | +2.70 | 46 | +10 | Jimmy Butler | +3.00 |
| 37 | Darren Collison | +2.88 | +4.00 | 21 | -16 | DeMarre Carroll | +2.90 |
| 38 | Marcus Smart | +2.79 | +2.10 | 59 | +21 | Eric Bledsoe | +2.90 |
| 39 | Jared Dudley | +2.77 | +1.90 | 67 | +28 | Mike Conley | +2.90 |
| 40 | CJ Miles | +2.55 | +1.60 | 75 | +35 | Zach Randolph | +2.90 |
| 41 | Michael KiddGilchrist | +2.54 | +2.00 | 61 | +20 | Kelly Olynyk | +2.80 |
| 42 | Ersan Ilyasova | +2.47 | +2.50 | 49 | +7 | Damian Lillard | +2.70 |
| 43 | Kevin Love | +2.46 | +3.20 | 30 | -13 | Jrue Holiday | +2.70 |
| 44 | Jae Crowder | +2.45 | +2.10 | 58 | +14 | Zaza Pachulia | +2.70 |
| 45 | Marcin Gortat | +2.39 | +3.20 | 29 | -16 | Anthony Morrow | +2.70 |
| 46 | Marc Gasol | +2.36 | +4.00 | 20 | -26 | Jeff Teague | +2.70 |
| 47 | Jrue Holiday | +2.33 | +2.70 | 43 | -4 | Serge Ibaka | +2.60 |
| 48 | Patrick Patterson | +2.28 | +1.60 | 74 | +26 | Tyson Chandler | +2.60 |
| 49 | Eric Bledsoe | +2.25 | +2.90 | 38 | -11 | Ersan Ilyasova | +2.50 |
| 50 | JJ Redick | +2.22 | +3.20 | 31 | -19 | Devin Harris | +2.50 |

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

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.637 &nbsp;·&nbsp; hits@30 27/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +8.04 | +10.60 | 1 | +0 | Chris Paul | +10.60 |
| 2 | Damian Lillard | +7.84 | +8.00 | 3 | +1 | Stephen Curry | +9.20 |
| 3 | Russell Westbrook | +6.73 | +6.20 | 6 | +3 | Damian Lillard | +8.00 |
| 4 | Stephen Curry | +6.42 | +9.20 | 2 | -2 | James Harden | +8.00 |
| 5 | LeBron James | +5.97 | +6.90 | 5 | +0 | LeBron James | +6.90 |
| 6 | James Harden | +5.84 | +8.00 | 4 | -2 | Russell Westbrook | +6.20 |
| 7 | Kevin Durant | +5.37 | +5.10 | 7 | +0 | Kevin Durant | +5.10 |
| 8 | Manu Ginobili | +4.88 | +4.40 | 10 | +2 | Jose Calderon | +5.10 |
| 9 | Kyle Lowry | +4.29 | +3.80 | 11 | +2 | Joe Johnson | +4.90 |
| 10 | LaMarcus Aldridge | +4.25 | +3.10 | 19 | +9 | Manu Ginobili | +4.40 |
| 11 | DeMar DeRozan | +3.66 | +3.40 | 14 | +3 | Kyle Lowry | +3.80 |
| 12 | Joe Johnson | +3.49 | +4.90 | 9 | -3 | Blake Griffin | +3.70 |
| 13 | Patrick Patterson | +3.38 | +2.20 | 29 | +16 | Patty Mills | +3.50 |
| 14 | Patty Mills | +3.29 | +3.50 | 13 | -1 | DeMar DeRozan | +3.40 |
| 15 | Devin Harris | +3.13 | +3.30 | 15 | +0 | Devin Harris | +3.30 |
| 16 | Mirza Teletovic | +3.03 | +3.20 | 17 | +1 | JJ Redick | +3.30 |
| 17 | Jose Calderon | +2.70 | +5.10 | 8 | -9 | Mirza Teletovic | +3.20 |
| 18 | Deron Williams | +2.67 | +2.80 | 26 | +8 | Ray Allen | +3.20 |
| 19 | Blake Griffin | +2.55 | +3.70 | 12 | -7 | LaMarcus Aldridge | +3.10 |
| 20 | Bradley Beal | +2.53 | +3.00 | 22 | +2 | Vince Carter | +3.10 |
| 21 | Draymond Green | +2.52 | +2.80 | 25 | +4 | Trevor Ariza | +3.10 |
| 22 | Jamal Crawford | +2.47 | +2.90 | 24 | +2 | Bradley Beal | +3.00 |
| 23 | Danny Green | +2.41 | +2.90 | 23 | +0 | Danny Green | +2.90 |
| 24 | JJ Redick | +2.23 | +3.30 | 16 | -8 | Jamal Crawford | +2.90 |
| 25 | Vince Carter | +2.18 | +3.10 | 20 | -5 | Draymond Green | +2.80 |
| 26 | Kawhi Leonard | +2.15 | +1.80 | 32 | +6 | Deron Williams | +2.80 |
| 27 | Trevor Ariza | +2.02 | +3.10 | 21 | -6 | Kyle Korver | +2.60 |
| 28 | David West | +1.86 | +2.10 | 30 | +2 | Tony Allen | +2.50 |
| 29 | Serge Ibaka | +1.81 | +1.20 | 40 | +11 | Patrick Patterson | +2.20 |
| 30 | Nicolas Batum | +1.81 | +1.50 | 35 | +5 | David West | +2.10 |
| 31 | Greivis Vasquez | +1.81 | +1.30 | 39 | +8 | Boris Diaw | +2.00 |
| 32 | Ray Allen | +1.58 | +3.20 | 18 | -14 | Kawhi Leonard | +1.80 |
| 33 | Boris Diaw | +1.58 | +2.00 | 31 | -2 | Tim Duncan | +1.80 |
| 34 | Andre Iguodala | +1.48 | -1.40 | 74 | +40 | Dwight Howard | +1.60 |
| 35 | George Hill | +1.24 | -0.70 | 65 | +30 | Nicolas Batum | +1.50 |
| 36 | Kyle Korver | +1.07 | +2.60 | 27 | -9 | Shane Battier | +1.50 |
| 37 | Lance Stephenson | +1.03 | -0.60 | 64 | +27 | Chandler Parsons | +1.40 |
| 38 | Jeremy Lin | +1.02 | +0.00 | 54 | +16 | Marcin Gortat | +1.30 |
| 39 | Tony Allen | +1.02 | +2.50 | 28 | -11 | Greivis Vasquez | +1.30 |
| 40 | John Wall | +0.94 | -0.10 | 55 | +15 | Serge Ibaka | +1.20 |
| 41 | Tim Duncan | +0.92 | +1.80 | 33 | -8 | Chris Bosh | +1.10 |
| 42 | Dwight Howard | +0.89 | +1.60 | 34 | -8 | Mario Chalmers | +1.10 |
| 43 | Tony Parker | +0.80 | +0.60 | 49 | +6 | Courtney Lee | +0.90 |
| 44 | Mike Conley | +0.67 | +0.60 | 46 | +2 | Andray Blatche | +0.80 |
| 45 | Shane Battier | +0.57 | +1.50 | 36 | -9 | Paul Millsap | +0.80 |
| 46 | Mike Miller | +0.49 | -0.60 | 62 | +16 | Mike Conley | +0.60 |
| 47 | Tiago Splitter | +0.47 | +0.50 | 50 | +3 | Rashard Lewis | +0.60 |
| 48 | Amir Johnson | +0.43 | -1.70 | 78 | +30 | Chris Andersen | +0.60 |
| 49 | Chandler Parsons | +0.38 | +1.40 | 37 | -12 | Tony Parker | +0.60 |
| 50 | David Lee | +0.33 | -1.50 | 75 | +25 | Tiago Splitter | +0.50 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.756 &nbsp;·&nbsp; hits@30 24/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Kevin Durant | +7.04 | +7.60 | 1 | +0 | Kevin Durant | +7.60 |
| 2 | LeBron James | +6.51 | +5.80 | 4 | +2 | Chris Paul | +7.10 |
| 3 | James Harden | +6.27 | +6.30 | 3 | +0 | James Harden | +6.30 |
| 4 | Chris Paul | +5.76 | +7.10 | 2 | -2 | LeBron James | +5.80 |
| 5 | Kevin Love | +5.06 | +5.70 | 5 | +0 | Kevin Love | +5.70 |
| 6 | Goran Dragic | +4.64 | +4.80 | 6 | +0 | Goran Dragic | +4.80 |
| 7 | Damian Lillard | +4.30 | +3.60 | 12 | +5 | Kyle Lowry | +4.40 |
| 8 | Russell Westbrook | +4.29 | +3.30 | 15 | +7 | Dirk Nowitzki | +4.40 |
| 9 | Kyle Lowry | +4.08 | +4.40 | 7 | -2 | Carmelo Anthony | +4.20 |
| 10 | Manu Ginobili | +3.85 | +4.00 | 10 | +0 | Manu Ginobili | +4.00 |
| 11 | Isaiah Thomas | +3.65 | +3.50 | 14 | +3 | Patty Mills | +3.90 |
| 12 | Carmelo Anthony | +3.37 | +4.20 | 9 | -3 | Damian Lillard | +3.60 |
| 13 | Mike Conley | +3.24 | +3.50 | 13 | +0 | Mike Conley | +3.50 |
| 14 | Dirk Nowitzki | +3.23 | +4.40 | 8 | -6 | Isaiah Thomas | +3.50 |
| 15 | Blake Griffin | +2.95 | +2.90 | 17 | +2 | Russell Westbrook | +3.30 |
| 16 | Patty Mills | +2.66 | +3.90 | 11 | -5 | Ty Lawson | +3.20 |
| 17 | Paul George | +2.36 | +2.60 | 22 | +5 | Blake Griffin | +2.90 |
| 18 | DJ Augustin | +2.32 | +2.10 | 32 | +14 | Marco Belinelli | +2.80 |
| 19 | John Wall | +2.25 | +1.90 | 37 | +18 | Jamal Crawford | +2.80 |
| 20 | Deron Williams | +2.20 | +2.60 | 23 | +3 | Wesley Matthews | +2.80 |
| 21 | Kyrie Irving | +2.12 | +2.30 | 27 | +6 | Joe Johnson | +2.70 |
| 22 | Ty Lawson | +2.11 | +3.20 | 16 | -6 | Paul George | +2.60 |
| 23 | Nikola Pekovic | +2.01 | +1.10 | 68 | +45 | Deron Williams | +2.60 |
| 24 | Brandan Wright | +2.01 | +1.70 | 41 | +17 | Chandler Parsons | +2.60 |
| 25 | Jamal Crawford | +1.99 | +2.80 | 19 | -6 | Nick Young | +2.40 |
| 26 | Wesley Matthews | +1.88 | +2.80 | 20 | -6 | Vince Carter | +2.40 |
| 27 | Ricky Rubio | +1.88 | +1.90 | 39 | +12 | Kyrie Irving | +2.30 |
| 28 | Klay Thompson | +1.82 | +2.10 | 33 | +5 | Jrue Holiday | +2.20 |
| 29 | Joe Johnson | +1.81 | +2.70 | 21 | -8 | Patrick Beverley | +2.20 |
| 30 | Chandler Parsons | +1.71 | +2.60 | 24 | -6 | Brandon Jennings | +2.20 |
| 31 | Jrue Holiday | +1.68 | +2.20 | 28 | -3 | Randy Foye | +2.10 |
| 32 | Darren Collison | +1.58 | +1.10 | 65 | +33 | DJ Augustin | +2.10 |
| 33 | Andre Iguodala | +1.56 | +1.20 | 62 | +29 | Klay Thompson | +2.10 |
| 34 | Nick Young | +1.54 | +2.40 | 25 | -9 | Josh McRoberts | +2.00 |
| 35 | Kemba Walker | +1.52 | +1.40 | 52 | +17 | Channing Frye | +2.00 |
| 36 | Rudy Gay | +1.50 | +1.10 | 66 | +30 | Kyle Korver | +1.90 |
| 37 | Vince Carter | +1.43 | +2.40 | 26 | -11 | John Wall | +1.90 |
| 38 | Mario Chalmers | +1.41 | +1.50 | 50 | +12 | Nicolas Batum | +1.90 |
| 39 | Joakim Noah | +1.41 | +1.50 | 51 | +12 | Ricky Rubio | +1.90 |
| 40 | Kawhi Leonard | +1.40 | +1.70 | 44 | +4 | JR Smith | +1.80 |
| 41 | Marco Belinelli | +1.40 | +2.80 | 18 | -23 | Brandan Wright | +1.70 |
| 42 | LaMarcus Aldridge | +1.40 | +1.10 | 67 | +25 | DeMar DeRozan | +1.70 |
| 43 | Patrick Beverley | +1.35 | +2.20 | 29 | -14 | Pablo Prigioni | +1.70 |
| 44 | Josh McRoberts | +1.34 | +2.00 | 34 | -10 | Kawhi Leonard | +1.70 |
| 45 | Pablo Prigioni | +1.33 | +1.70 | 43 | -2 | DeMarcus Cousins | +1.70 |
| 46 | DeMar DeRozan | +1.33 | +1.70 | 42 | -4 | Mirza Teletovic | +1.60 |
| 47 | Randy Foye | +1.32 | +2.10 | 31 | -16 | Jose Calderon | +1.60 |
| 48 | George Hill | +1.22 | +0.30 | 120 | +72 | Eric Bledsoe | +1.50 |
| 49 | Kyle Korver | +1.21 | +1.90 | 36 | -13 | Dwyane Wade | +1.50 |
| 50 | JR Smith | +1.21 | +1.80 | 40 | -10 | Mario Chalmers | +1.50 |

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

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.586 &nbsp;·&nbsp; hits@30 26/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | James Harden | +6.83 | +8.00 | 2 | +1 | Chris Paul | +8.70 |
| 2 | Chris Paul | +6.61 | +8.70 | 1 | -1 | James Harden | +8.00 |
| 3 | Monta Ellis | +5.03 | +6.20 | 4 | +1 | CJ McCollum | +7.90 |
| 4 | CJ McCollum | +4.88 | +7.90 | 3 | -1 | Monta Ellis | +6.20 |
| 5 | Jimmy Butler | +4.77 | +5.30 | 8 | +3 | Alan Anderson | +6.10 |
| 6 | Kyrie Irving | +4.40 | +4.10 | 15 | +9 | Stephen Curry | +5.70 |
| 7 | Stephen Curry | +4.26 | +5.70 | 6 | -1 | AlFarouq Aminu | +5.30 |
| 8 | Tim Duncan | +4.25 | +5.20 | 9 | +1 | Jimmy Butler | +5.30 |
| 9 | AlFarouq Aminu | +3.97 | +5.30 | 7 | -2 | Tim Duncan | +5.20 |
| 10 | Jarrett Jack | +3.90 | +3.80 | 16 | +6 | Vince Carter | +5.20 |
| 11 | Mike Dunleavy | +3.57 | +4.70 | 11 | +0 | Mike Dunleavy | +4.70 |
| 12 | Alan Anderson | +3.53 | +6.10 | 5 | -7 | DeMar DeRozan | +4.60 |
| 13 | LeBron James | +3.52 | +3.60 | 19 | +6 | Eric Gordon | +4.50 |
| 14 | Derrick Rose | +3.46 | +2.80 | 27 | +13 | JJ Barea | +4.40 |
| 15 | DeMar DeRozan | +3.42 | +4.60 | 12 | -3 | Kyrie Irving | +4.10 |
| 16 | Jeff Teague | +2.84 | +3.10 | 22 | +6 | Jarrett Jack | +3.80 |
| 17 | Paul Pierce | +2.84 | +3.60 | 17 | +0 | Paul Pierce | +3.60 |
| 18 | Manu Ginobili | +2.67 | +3.60 | 18 | +0 | Manu Ginobili | +3.60 |
| 19 | Bradley Beal | +2.52 | +2.60 | 28 | +9 | LeBron James | +3.60 |
| 20 | Blake Griffin | +2.35 | +3.50 | 20 | +0 | Blake Griffin | +3.50 |
| 21 | Mike Conley | +2.25 | +3.30 | 21 | +0 | Mike Conley | +3.30 |
| 22 | Paul Millsap | +2.23 | +2.90 | 25 | +3 | Jeff Teague | +3.10 |
| 23 | Dirk Nowitzki | +2.20 | +1.40 | 43 | +20 | JR Smith | +3.00 |
| 24 | John Wall | +2.01 | +1.40 | 44 | +20 | DeMarre Carroll | +2.90 |
| 25 | JJ Barea | +1.95 | +4.40 | 14 | -11 | Paul Millsap | +2.90 |
| 26 | Josh Smith | +1.81 | +2.60 | 29 | +3 | Klay Thompson | +2.90 |
| 27 | Damian Lillard | +1.78 | +0.30 | 58 | +31 | Derrick Rose | +2.80 |
| 28 | Vince Carter | +1.76 | +5.20 | 10 | -18 | Bradley Beal | +2.60 |
| 29 | DeMarre Carroll | +1.72 | +2.90 | 24 | -5 | Josh Smith | +2.60 |
| 30 | Kyle Korver | +1.63 | +1.10 | 49 | +19 | Iman Shumpert | +2.60 |
| 31 | Marcin Gortat | +1.50 | +1.40 | 45 | +14 | Otto Porter Jr. | +2.50 |
| 32 | Boris Diaw | +1.49 | +2.00 | 38 | +6 | OJ Mayo | +2.40 |
| 33 | Jamal Crawford | +1.45 | +1.60 | 40 | +7 | Brook Lopez | +2.40 |
| 34 | Brook Lopez | +1.22 | +2.40 | 33 | -1 | Courtney Lee | +2.30 |
| 35 | Kawhi Leonard | +1.14 | +1.40 | 46 | +11 | Danny Green | +2.30 |
| 36 | Andre Iguodala | +1.13 | +1.90 | 39 | +3 | Jason Terry | +2.10 |
| 37 | Otto Porter Jr. | +1.13 | +2.50 | 31 | -6 | Tristan Thompson | +2.10 |
| 38 | Trevor Ariza | +1.11 | +1.30 | 48 | +10 | Boris Diaw | +2.00 |
| 39 | JR Smith | +1.06 | +3.00 | 23 | -16 | Andre Iguodala | +1.90 |
| 40 | Jason Terry | +0.91 | +2.10 | 36 | -4 | Jamal Crawford | +1.60 |
| 41 | Klay Thompson | +0.86 | +2.90 | 26 | -15 | DeAndre Jordan | +1.50 |
| 42 | Drew Gooden | +0.82 | +0.50 | 52 | +10 | Matt Barnes | +1.50 |
| 43 | Leandro Barbosa | +0.80 | +0.90 | 50 | +7 | Dirk Nowitzki | +1.40 |
| 44 | Courtney Lee | +0.78 | +2.30 | 34 | -10 | John Wall | +1.40 |
| 45 | DeAndre Jordan | +0.50 | +1.50 | 41 | -4 | Marcin Gortat | +1.40 |
| 46 | Eric Gordon | +0.36 | +4.50 | 13 | -33 | Kawhi Leonard | +1.40 |
| 47 | Beno Udrih | +0.18 | +1.30 | 47 | +0 | Beno Udrih | +1.30 |
| 48 | JJ Redick | +0.11 | -0.20 | 64 | +16 | Trevor Ariza | +1.30 |
| 49 | Shaun Livingston | +0.08 | +0.20 | 60 | +11 | Kyle Korver | +1.10 |
| 50 | Pau Gasol | -0.00 | -0.10 | 63 | +13 | Leandro Barbosa | +0.90 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.651 &nbsp;·&nbsp; hits@30 25/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.47 | +8.50 | 1 | +0 | Chris Paul | +8.50 |
| 2 | James Harden | +7.17 | +7.70 | 2 | +0 | James Harden | +7.70 |
| 3 | Russell Westbrook | +6.56 | +6.10 | 3 | +0 | Russell Westbrook | +6.10 |
| 4 | LeBron James | +5.90 | +5.30 | 5 | +1 | Kyrie Irving | +5.50 |
| 5 | Kyrie Irving | +5.18 | +5.50 | 4 | -1 | LeBron James | +5.30 |
| 6 | Lou Williams | +4.78 | +5.20 | 6 | +0 | Lou Williams | +5.20 |
| 7 | Isaiah Thomas | +4.76 | +4.50 | 8 | +1 | Kyle Korver | +4.60 |
| 8 | Damian Lillard | +4.21 | +4.00 | 11 | +3 | Isaiah Thomas | +4.50 |
| 9 | Blake Griffin | +3.82 | +3.20 | 22 | +13 | Anthony Davis | +4.30 |
| 10 | Klay Thompson | +3.68 | +4.30 | 10 | +0 | Klay Thompson | +4.30 |
| 11 | George Hill | +3.29 | +3.90 | 12 | +1 | Damian Lillard | +4.00 |
| 12 | Jimmy Butler | +3.28 | +3.20 | 20 | +8 | George Hill | +3.90 |
| 13 | Carmelo Anthony | +3.07 | +3.80 | 13 | +0 | Carmelo Anthony | +3.80 |
| 14 | Mike Conley | +2.89 | +2.40 | 32 | +18 | Ty Lawson | +3.80 |
| 15 | Kyle Korver | +2.89 | +4.60 | 7 | -8 | Kawhi Leonard | +3.70 |
| 16 | JJ Redick | +2.86 | +2.50 | 29 | +13 | Rudy Gay | +3.50 |
| 17 | Anthony Davis | +2.75 | +4.30 | 9 | -8 | DeAndre Jordan | +3.40 |
| 18 | Kyle Lowry | +2.64 | +3.30 | 18 | +0 | Kyle Lowry | +3.30 |
| 19 | Ty Lawson | +2.62 | +3.80 | 14 | -5 | Jrue Holiday | +3.30 |
| 20 | Gordon Hayward | +2.59 | +3.20 | 21 | +1 | Jimmy Butler | +3.20 |
| 21 | Jrue Holiday | +2.49 | +3.30 | 19 | -2 | Gordon Hayward | +3.20 |
| 22 | Kawhi Leonard | +2.44 | +3.70 | 15 | -7 | Blake Griffin | +3.20 |
| 23 | Aaron Brooks | +2.39 | +1.60 | 56 | +33 | Brandon Jennings | +3.10 |
| 24 | Jeff Teague | +2.31 | +2.20 | 35 | +11 | Danny Green | +3.10 |
| 25 | Dwyane Wade | +2.23 | +2.00 | 42 | +17 | Danilo Gallinari | +2.80 |
| 26 | Brandon Jennings | +2.23 | +3.10 | 23 | -3 | Anthony Morrow | +2.70 |
| 27 | John Wall | +2.17 | +2.10 | 39 | +12 | Chandler Parsons | +2.60 |
| 28 | Tyreke Evans | +2.15 | +2.60 | 28 | +0 | Tyreke Evans | +2.60 |
| 29 | Rudy Gay | +2.09 | +3.50 | 16 | -13 | JJ Redick | +2.50 |
| 30 | Danilo Gallinari | +2.03 | +2.80 | 25 | -5 | LaMarcus Aldridge | +2.40 |
| 31 | Khris Middleton | +1.99 | +1.70 | 51 | +20 | Patrick Patterson | +2.40 |
| 32 | LaMarcus Aldridge | +1.99 | +2.40 | 30 | -2 | Mike Conley | +2.40 |
| 33 | Dirk Nowitzki | +1.93 | +2.20 | 34 | +1 | Wesley Matthews | +2.30 |
| 34 | Reggie Jackson | +1.93 | +2.00 | 41 | +7 | Dirk Nowitzki | +2.20 |
| 35 | Patrick Patterson | +1.75 | +2.40 | 31 | -4 | Jeff Teague | +2.20 |
| 36 | Anthony Morrow | +1.72 | +2.70 | 26 | -10 | Gerald Green | +2.20 |
| 37 | Darren Collison | +1.70 | +1.70 | 55 | +18 | Devin Harris | +2.10 |
| 38 | Danny Green | +1.61 | +3.10 | 24 | -14 | JR Smith | +2.10 |
| 39 | Eric Gordon | +1.59 | +0.50 | 89 | +50 | John Wall | +2.10 |
| 40 | Gerald Green | +1.56 | +2.20 | 36 | -4 | Ersan Ilyasova | +2.10 |
| 41 | Wesley Matthews | +1.52 | +2.30 | 33 | -8 | Reggie Jackson | +2.00 |
| 42 | Chandler Parsons | +1.46 | +2.60 | 27 | -15 | Dwyane Wade | +2.00 |
| 43 | Bradley Beal | +1.21 | +0.80 | 78 | +35 | DeMarre Carroll | +1.90 |
| 44 | Kevin Love | +1.19 | +1.70 | 52 | +8 | Nikola Mirotic | +1.90 |
| 45 | Draymond Green | +1.18 | +1.50 | 58 | +13 | Goran Dragic | +1.90 |
| 46 | Paul Millsap | +1.17 | +1.00 | 70 | +24 | JJ Barea | +1.90 |
| 47 | Marc Gasol | +1.13 | +1.40 | 60 | +13 | Joe Johnson | +1.80 |
| 48 | Jamal Crawford | +1.12 | +1.10 | 69 | +21 | Luol Deng | +1.80 |
| 49 | JR Smith | +1.11 | +2.10 | 38 | -11 | Jae Crowder | +1.80 |
| 50 | Manu Ginobili | +1.10 | +1.70 | 54 | +4 | Eric Bledsoe | +1.70 |

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

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.182 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Trevor Ariza | +4.50 | +2.40 | 26 | +25 | Draymond Green | +8.00 |
| 2 | Danny Green | +4.49 | +3.50 | 19 | +17 | Paul Millsap | +7.60 |
| 3 | Greivis Vasquez | +3.89 | +6.00 | 6 | +3 | Pero Antic | +6.50 |
| 4 | Kawhi Leonard | +3.82 | +4.40 | 12 | +8 | Nick Collison | +6.10 |
| 5 | Marcin Gortat | +3.59 | +3.50 | 20 | +15 | Andray Blatche | +6.10 |
| 6 | Pero Antic | +3.31 | +6.50 | 3 | -3 | Greivis Vasquez | +6.00 |
| 7 | John Wall | +3.28 | +0.80 | 41 | +34 | Chris Andersen | +5.40 |
| 8 | Manu Ginobili | +3.26 | +3.40 | 21 | +13 | Joakim Noah | +5.30 |
| 9 | George Hill | +3.19 | +2.80 | 23 | +14 | Vince Carter | +5.00 |
| 10 | Paul Millsap | +3.10 | +7.60 | 2 | -8 | Tiago Splitter | +5.00 |
| 11 | Joakim Noah | +3.02 | +5.30 | 8 | -3 | Rashard Lewis | +4.90 |
| 12 | Serge Ibaka | +2.96 | +4.20 | 13 | +1 | Kawhi Leonard | +4.40 |
| 13 | David West | +2.84 | +1.00 | 39 | +26 | Serge Ibaka | +4.20 |
| 14 | Andray Blatche | +2.81 | +6.10 | 5 | -9 | Ian Mahinmi | +4.20 |
| 15 | Tiago Splitter | +2.77 | +5.00 | 10 | -5 | Zach Randolph | +4.20 |
| 16 | Draymond Green | +2.75 | +8.00 | 1 | -15 | Marc Gasol | +4.10 |
| 17 | Chris Paul | +2.70 | +2.70 | 24 | +7 | Deron Williams | +3.80 |
| 18 | Kyle Korver | +2.58 | -0.40 | 62 | +44 | Patty Mills | +3.70 |
| 19 | LeBron James | +2.46 | +0.80 | 42 | +23 | Danny Green | +3.50 |
| 20 | Kevin Garnett | +2.35 | +0.40 | 51 | +31 | Marcin Gortat | +3.50 |
| 21 | Patty Mills | +2.31 | +3.70 | 18 | -3 | Manu Ginobili | +3.40 |
| 22 | Paul Pierce | +2.09 | +1.50 | 34 | +12 | LaMarcus Aldridge | +2.90 |
| 23 | Zach Randolph | +2.06 | +4.20 | 15 | -8 | George Hill | +2.80 |
| 24 | Deron Williams | +2.05 | +3.80 | 17 | -7 | Chris Paul | +2.70 |
| 25 | Nick Collison | +2.02 | +6.10 | 4 | -21 | Bradley Beal | +2.70 |
| 26 | Rashard Lewis | +2.00 | +4.90 | 11 | -15 | Trevor Ariza | +2.40 |
| 27 | Vince Carter | +1.90 | +5.00 | 9 | -18 | Nicolas Batum | +2.20 |
| 28 | Kevin Durant | +1.86 | +0.50 | 48 | +20 | James Harden | +1.90 |
| 29 | Patrick Patterson | +1.82 | -0.80 | 64 | +35 | Tim Duncan | +1.90 |
| 30 | Chris Andersen | +1.75 | +5.40 | 7 | -23 | Nene | +1.80 |
| 31 | Nene | +1.70 | +1.80 | 30 | -1 | Tony Allen | +1.80 |
| 32 | Tim Duncan | +1.67 | +1.90 | 29 | -3 | Kendrick Perkins | +1.80 |
| 33 | David Lee | +1.60 | -2.00 | 87 | +54 | JJ Redick | +1.70 |
| 34 | Lance Stephenson | +1.54 | -0.80 | 66 | +32 | Paul Pierce | +1.50 |
| 35 | Dwyane Wade | +1.43 | -1.80 | 81 | +46 | Kirk Hinrich | +1.40 |
| 36 | JJ Redick | +1.37 | +1.70 | 33 | -3 | Roy Hibbert | +1.30 |
| 37 | Alan Anderson | +1.36 | +0.50 | 47 | +10 | Thabo Sefolosha | +1.30 |
| 38 | Bradley Beal | +1.34 | +2.70 | 25 | -13 | Stephen Curry | +1.20 |
| 39 | Joe Johnson | +1.27 | +0.40 | 50 | +11 | David West | +1.00 |
| 40 | Tony Allen | +1.19 | +1.80 | 31 | -9 | DeAndre Jordan | +0.90 |
| 41 | Kendrick Perkins | +1.10 | +1.80 | 32 | -9 | John Wall | +0.80 |
| 42 | Jeremy Lin | +1.10 | -1.00 | 72 | +30 | LeBron James | +0.80 |
| 43 | Kyle Lowry | +1.03 | +0.50 | 46 | +3 | Boris Diaw | +0.80 |
| 44 | Mike Conley | +0.96 | -1.30 | 75 | +31 | Blake Griffin | +0.80 |
| 45 | Derek Fisher | +0.90 | -1.30 | 74 | +29 | Reggie Jackson | +0.70 |
| 46 | LaMarcus Aldridge | +0.87 | +2.90 | 22 | -24 | Kyle Lowry | +0.50 |
| 47 | Matt Barnes | +0.86 | -2.20 | 88 | +41 | Alan Anderson | +0.50 |
| 48 | Dwight Howard | +0.84 | +0.40 | 49 | +1 | Kevin Durant | +0.50 |
| 49 | DeMarre Carroll | +0.68 | -2.50 | 89 | +40 | Dwight Howard | +0.40 |
| 50 | DeAndre Jordan | +0.67 | +0.90 | 40 | -10 | Joe Johnson | +0.40 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.411 &nbsp;·&nbsp; hits@30 21/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Andrew Bogut | +4.65 | +4.40 | 4 | +3 | Kawhi Leonard | +5.00 |
| 2 | Tiago Splitter | +4.33 | +4.20 | 6 | +4 | Draymond Green | +4.60 |
| 3 | Marcin Gortat | +3.97 | +2.50 | 29 | +26 | Joakim Noah | +4.50 |
| 4 | Anderson Varejao | +3.82 | +3.60 | 10 | +6 | Andrew Bogut | +4.40 |
| 5 | Draymond Green | +3.79 | +4.60 | 2 | -3 | Michael KiddGilchrist | +4.40 |
| 6 | Paul George | +3.77 | +2.90 | 21 | +15 | Tiago Splitter | +4.20 |
| 7 | Danny Green | +3.58 | +4.00 | 7 | +0 | Danny Green | +4.00 |
| 8 | Joakim Noah | +3.51 | +4.50 | 3 | -5 | Chris Paul | +3.90 |
| 9 | Tim Duncan | +3.50 | +3.00 | 18 | +9 | Nene | +3.80 |
| 10 | Kevin Garnett | +3.47 | +3.50 | 11 | +1 | Anderson Varejao | +3.60 |
| 11 | Nene | +3.45 | +3.80 | 9 | -2 | Kevin Garnett | +3.50 |
| 12 | Kawhi Leonard | +3.41 | +5.00 | 1 | -11 | Nick Calathes | +3.50 |
| 13 | CJ Watson | +3.37 | +3.20 | 17 | +4 | Ian Mahinmi | +3.50 |
| 14 | Tony Allen | +3.15 | +2.40 | 31 | +17 | Jimmy Butler | +3.40 |
| 15 | Jae Crowder | +3.10 | +3.00 | 19 | +4 | Roy Hibbert | +3.40 |
| 16 | Jimmy Butler | +2.86 | +3.40 | 14 | -2 | DeMarcus Cousins | +3.30 |
| 17 | Roy Hibbert | +2.82 | +3.40 | 15 | -2 | CJ Watson | +3.20 |
| 18 | Chris Bosh | +2.82 | +1.00 | 75 | +57 | Tim Duncan | +3.00 |
| 19 | DeMarcus Cousins | +2.80 | +3.30 | 16 | -3 | Jae Crowder | +3.00 |
| 20 | Ian Mahinmi | +2.79 | +3.50 | 13 | -7 | Kris Humphries | +3.00 |
| 21 | Michael KiddGilchrist | +2.79 | +4.40 | 5 | -16 | Paul George | +2.90 |
| 22 | Iman Shumpert | +2.74 | +1.00 | 73 | +51 | Marc Gasol | +2.80 |
| 23 | Paul Millsap | +2.69 | +2.70 | 23 | +0 | Paul Millsap | +2.70 |
| 24 | Andre Iguodala | +2.68 | +2.60 | 25 | +1 | Shane Battier | +2.70 |
| 25 | Paul Pierce | +2.67 | +1.80 | 50 | +25 | Andre Iguodala | +2.60 |
| 26 | LaMarcus Aldridge | +2.60 | +2.20 | 37 | +11 | DeMarre Carroll | +2.60 |
| 27 | Manu Ginobili | +2.56 | +1.10 | 64 | +37 | Mario Chalmers | +2.50 |
| 28 | Kirk Hinrich | +2.54 | +2.10 | 42 | +14 | Samuel Dalembert | +2.50 |
| 29 | David West | +2.52 | +1.50 | 57 | +28 | Marcin Gortat | +2.50 |
| 30 | Kemba Walker | +2.42 | +2.30 | 35 | +5 | Victor Oladipo | +2.40 |
| 31 | Marc Gasol | +2.39 | +2.80 | 22 | -9 | Tony Allen | +2.40 |
| 32 | Derek Fisher | +2.37 | +2.00 | 43 | +11 | Dwight Howard | +2.40 |
| 33 | Ersan Ilyasova | +2.32 | +0.70 | 90 | +57 | Serge Ibaka | +2.30 |
| 34 | Ricky Rubio | +2.28 | +1.80 | 49 | +15 | Anthony Davis | +2.30 |
| 35 | Al Jefferson | +2.25 | +1.80 | 47 | +12 | Kemba Walker | +2.30 |
| 36 | Darrell Arthur | +2.22 | +1.90 | 45 | +9 | Thabo Sefolosha | +2.30 |
| 37 | Nick Calathes | +2.22 | +3.50 | 12 | -25 | LaMarcus Aldridge | +2.20 |
| 38 | Josh Smith | +2.20 | +0.80 | 84 | +46 | Nikola Pekovic | +2.20 |
| 39 | Kosta Koufos | +2.18 | +2.10 | 41 | +2 | Eric Bledsoe | +2.20 |
| 40 | Robin Lopez | +2.13 | +2.00 | 44 | +4 | George Hill | +2.10 |
| 41 | Timofey Mozgov | +2.12 | +1.10 | 68 | +27 | Kosta Koufos | +2.10 |
| 42 | Gerald Wallace | +2.04 | +1.60 | 52 | +10 | Kirk Hinrich | +2.10 |
| 43 | David Lee | +2.03 | +1.20 | 62 | +19 | Derek Fisher | +2.00 |
| 44 | Chris Paul | +2.03 | +3.90 | 8 | -36 | Robin Lopez | +2.00 |
| 45 | DeMarre Carroll | +2.00 | +2.60 | 26 | -19 | Darrell Arthur | +1.90 |
| 46 | Chris Andersen | +1.98 | +1.60 | 55 | +9 | Patrick Beverley | +1.90 |
| 47 | Shane Battier | +1.93 | +2.70 | 24 | -23 | Al Jefferson | +1.80 |
| 48 | Nicolas Batum | +1.89 | +0.00 | 128 | +80 | Jeremy Lin | +1.80 |
| 49 | Miles Plumlee | +1.86 | +1.60 | 56 | +7 | Ricky Rubio | +1.80 |
| 50 | Taj Gibson | +1.83 | +0.80 | 86 | +36 | Paul Pierce | +1.80 |

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

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.361 &nbsp;·&nbsp; hits@30 21/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Pau Gasol | +5.08 | +3.50 | 16 | +15 | Jarrett Jack | +7.50 |
| 2 | Tony Allen | +4.93 | +5.00 | 10 | +8 | Anthony Davis | +7.20 |
| 3 | Jarrett Jack | +4.88 | +7.50 | 1 | -2 | Timofey Mozgov | +6.90 |
| 4 | DeAndre Jordan | +4.59 | +2.10 | 29 | +25 | Otto Porter Jr. | +6.30 |
| 5 | Al Horford | +4.53 | +4.40 | 12 | +7 | Trevor Ariza | +6.10 |
| 6 | AlFarouq Aminu | +4.15 | +5.80 | 6 | +0 | AlFarouq Aminu | +5.80 |
| 7 | Tim Duncan | +4.07 | +2.70 | 23 | +16 | Dwight Howard | +5.70 |
| 8 | Kyle Korver | +4.01 | +1.10 | 42 | +34 | Danny Green | +5.50 |
| 9 | Timofey Mozgov | +4.00 | +6.90 | 3 | -6 | Marc Gasol | +5.30 |
| 10 | Otto Porter Jr. | +3.95 | +6.30 | 4 | -6 | Tony Allen | +5.00 |
| 11 | Dwight Howard | +3.83 | +5.70 | 7 | -4 | Nene | +4.70 |
| 12 | Nene | +3.81 | +4.70 | 11 | -1 | Al Horford | +4.40 |
| 13 | Anthony Davis | +3.69 | +7.20 | 2 | -11 | Jimmy Butler | +3.70 |
| 14 | Matt Barnes | +3.42 | +3.40 | 17 | +3 | Blake Griffin | +3.70 |
| 15 | Danny Green | +3.30 | +5.50 | 8 | -7 | Ramon Sessions | +3.50 |
| 16 | Andrew Bogut | +3.29 | +1.90 | 35 | +19 | Pau Gasol | +3.50 |
| 17 | Jimmy Butler | +3.22 | +3.70 | 13 | -4 | Matt Barnes | +3.40 |
| 18 | Blake Griffin | +2.77 | +3.70 | 14 | -4 | Stephen Curry | +3.00 |
| 19 | Harrison Barnes | +2.72 | +1.00 | 43 | +24 | Derrick Rose | +3.00 |
| 20 | Alan Anderson | +2.72 | +2.80 | 20 | +0 | Alan Anderson | +2.80 |
| 21 | Brook Lopez | +2.65 | +2.00 | 34 | +13 | Avery Bradley | +2.80 |
| 22 | Stephen Curry | +2.54 | +3.00 | 18 | -4 | Manu Ginobili | +2.70 |
| 23 | JJ Barea | +2.53 | +2.10 | 31 | +8 | Tim Duncan | +2.70 |
| 24 | John Henson | +2.37 | +2.30 | 28 | +4 | Matthew Dellavedova | +2.50 |
| 25 | Mike Dunleavy | +2.36 | +0.60 | 49 | +24 | Tristan Thompson | +2.50 |
| 26 | Marc Gasol | +2.09 | +5.30 | 9 | -17 | Chris Paul | +2.50 |
| 27 | Mike Conley | +2.06 | +1.70 | 39 | +12 | Thaddeus Young | +2.30 |
| 28 | OJ Mayo | +1.93 | -0.90 | 67 | +39 | John Henson | +2.30 |
| 29 | Drew Gooden | +1.91 | -1.30 | 72 | +43 | DeAndre Jordan | +2.10 |
| 30 | Thaddeus Young | +1.88 | +2.30 | 27 | -3 | Festus Ezeli | +2.10 |
| 31 | Iman Shumpert | +1.85 | +0.20 | 55 | +24 | JJ Barea | +2.10 |
| 32 | Joakim Noah | +1.82 | +2.10 | 33 | +1 | Paul Millsap | +2.10 |
| 33 | LeBron James | +1.81 | +1.10 | 41 | +8 | Joakim Noah | +2.10 |
| 34 | Marcin Gortat | +1.72 | +0.30 | 53 | +19 | Brook Lopez | +2.00 |
| 35 | Ramon Sessions | +1.72 | +3.50 | 15 | -20 | Andrew Bogut | +1.90 |
| 36 | Giannis Antetokounmpo | +1.70 | +0.60 | 50 | +14 | Pero Antic | +1.90 |
| 37 | Trevor Ariza | +1.58 | +6.10 | 5 | -32 | Kent Bazemore | +1.80 |
| 38 | DeMarre Carroll | +1.57 | -0.80 | 66 | +28 | Bradley Beal | +1.70 |
| 39 | Paul Millsap | +1.48 | +2.10 | 32 | -7 | Mike Conley | +1.70 |
| 40 | Kent Bazemore | +1.45 | +1.80 | 37 | -3 | Kawhi Leonard | +1.40 |
| 41 | Josh Smith | +1.44 | -0.30 | 60 | +19 | LeBron James | +1.10 |
| 42 | Pero Antic | +1.34 | +1.90 | 36 | -6 | Kyle Korver | +1.10 |
| 43 | Matthew Dellavedova | +1.31 | +2.50 | 24 | -19 | Harrison Barnes | +1.00 |
| 44 | Derrick Rose | +1.27 | +3.00 | 19 | -25 | Shaun Livingston | +0.90 |
| 45 | Festus Ezeli | +1.20 | +2.10 | 30 | -15 | John Wall | +0.90 |
| 46 | JR Smith | +1.15 | -0.40 | 61 | +15 | Nicolas Batum | +0.80 |
| 47 | Kyle Lowry | +1.04 | +0.50 | 52 | +5 | Bojan Bogdanovic | +0.70 |
| 48 | Tony Snell | +1.04 | -2.80 | 83 | +35 | Andre Iguodala | +0.70 |
| 49 | Pablo Prigioni | +0.90 | -1.60 | 75 | +26 | Mike Dunleavy | +0.60 |
| 50 | Chris Paul | +0.86 | +2.50 | 26 | -24 | Giannis Antetokounmpo | +0.60 |

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

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.660 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Tony Allen | +5.48 | +4.80 | 4 | +3 | Kawhi Leonard | +5.20 |
| 2 | Andrew Bogut | +5.02 | +4.70 | 5 | +3 | Draymond Green | +5.10 |
| 3 | Rudy Gobert | +4.67 | +4.80 | 3 | +0 | Rudy Gobert | +4.80 |
| 4 | Draymond Green | +4.58 | +5.10 | 2 | -2 | Tony Allen | +4.80 |
| 5 | Kawhi Leonard | +4.41 | +5.20 | 1 | -4 | Andrew Bogut | +4.70 |
| 6 | Tim Duncan | +3.65 | +3.50 | 9 | +3 | Anthony Davis | +4.50 |
| 7 | DeMarcus Cousins | +3.64 | +4.40 | 7 | +0 | DeMarcus Cousins | +4.40 |
| 8 | Nerlens Noel | +3.51 | +2.70 | 19 | +11 | Marcin Gortat | +3.60 |
| 9 | Kosta Koufos | +3.31 | +3.30 | 11 | +2 | Tim Duncan | +3.50 |
| 10 | Anthony Davis | +3.25 | +4.50 | 6 | -4 | Andre Roberson | +3.40 |
| 11 | Marcin Gortat | +3.24 | +3.60 | 8 | -3 | Kosta Koufos | +3.30 |
| 12 | Nene | +3.17 | +2.80 | 18 | +6 | Zaza Pachulia | +3.20 |
| 13 | Zaza Pachulia | +3.15 | +3.20 | 12 | -1 | Khris Middleton | +3.10 |
| 14 | Danny Green | +3.06 | +3.00 | 16 | +2 | Michael KiddGilchrist | +3.00 |
| 15 | Andre Roberson | +2.94 | +3.40 | 10 | -5 | Serge Ibaka | +3.00 |
| 16 | Michael KiddGilchrist | +2.89 | +3.00 | 14 | -2 | Danny Green | +3.00 |
| 17 | AlFarouq Aminu | +2.83 | +2.60 | 24 | +7 | Jonas Jerebko | +2.80 |
| 18 | Greg Monroe | +2.81 | +0.20 | 111 | +93 | Nene | +2.80 |
| 19 | Manu Ginobili | +2.71 | +1.40 | 52 | +33 | Nerlens Noel | +2.70 |
| 20 | Marcus Smart | +2.67 | +1.80 | 43 | +23 | Tyson Chandler | +2.60 |
| 21 | Iman Shumpert | +2.60 | +2.30 | 32 | +11 | Marc Gasol | +2.60 |
| 22 | Josh Smith | +2.60 | +2.60 | 23 | +1 | Joakim Noah | +2.60 |
| 23 | Michael CarterWilliams | +2.58 | +2.30 | 31 | +8 | Josh Smith | +2.60 |
| 24 | Jonas Jerebko | +2.57 | +2.80 | 17 | -7 | AlFarouq Aminu | +2.60 |
| 25 | Jared Dudley | +2.57 | +1.80 | 44 | +19 | Alex Len | +2.50 |
| 26 | Khris Middleton | +2.56 | +3.10 | 13 | -13 | Paul Millsap | +2.40 |
| 27 | Timofey Mozgov | +2.53 | +2.40 | 28 | +1 | Omer Asik | +2.40 |
| 28 | Derrick Favors | +2.51 | +2.10 | 34 | +6 | Timofey Mozgov | +2.40 |
| 29 | Tyson Chandler | +2.50 | +2.60 | 20 | -9 | Darren Collison | +2.30 |
| 30 | Nikola Mirotic | +2.48 | +2.20 | 33 | +3 | Luc Mbah a Moute | +2.30 |
| 31 | Pau Gasol | +2.45 | +0.50 | 90 | +59 | Michael CarterWilliams | +2.30 |
| 32 | Wesley Matthews | +2.39 | +1.30 | 56 | +24 | Iman Shumpert | +2.30 |
| 33 | Zach Randolph | +2.38 | +1.30 | 59 | +26 | Nikola Mirotic | +2.20 |
| 34 | John Wall | +2.27 | +0.00 | 128 | +94 | Derrick Favors | +2.10 |
| 35 | Paul Millsap | +2.26 | +2.40 | 26 | -9 | Chris Paul | +2.10 |
| 36 | Roy Hibbert | +2.21 | +2.00 | 38 | +2 | Kelly Olynyk | +2.10 |
| 37 | Dwight Howard | +2.21 | +1.90 | 41 | +4 | Cody Zeller | +2.10 |
| 38 | Ersan Ilyasova | +2.10 | +0.40 | 100 | +62 | Roy Hibbert | +2.00 |
| 39 | Luis Scola | +2.10 | +0.60 | 84 | +45 | Steven Adams | +2.00 |
| 40 | Kelly Olynyk | +2.10 | +2.10 | 36 | -4 | LaMarcus Aldridge | +1.90 |
| 41 | Serge Ibaka | +2.09 | +3.00 | 15 | -26 | Dwight Howard | +1.90 |
| 42 | Omer Asik | +2.08 | +2.40 | 27 | -15 | Pablo Prigioni | +1.80 |
| 43 | Cory Joseph | +2.02 | +0.50 | 85 | +42 | Marcus Smart | +1.80 |
| 44 | LeBron James | +2.02 | -0.10 | 134 | +90 | Jared Dudley | +1.80 |
| 45 | Andre Iguodala | +1.94 | +1.60 | 47 | +2 | George Hill | +1.70 |
| 46 | Rajon Rondo | +1.94 | +0.40 | 93 | +47 | Al Horford | +1.60 |
| 47 | CJ Miles | +1.92 | +0.20 | 117 | +70 | Andre Iguodala | +1.60 |
| 48 | Jimmy Butler | +1.88 | -0.20 | 140 | +92 | Kevin Love | +1.60 |
| 49 | Monta Ellis | +1.84 | +0.80 | 77 | +28 | Mario Chalmers | +1.50 |
| 50 | PJ Tucker | +1.82 | +1.30 | 54 | +4 | Kris Humphries | +1.50 |

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
| 1 | Nikola Jokic | +9.66 | 2737 |
| 2 | Shai Gilgeous-Alexander | +8.30 | 2553 |
| 3 | Luka Doncic | +8.10 | 2624 |
| 4 | Joel Embiid | +7.47 | 1309 |
| 5 | Paul George | +7.13 | 2502 |
| 6 | Kawhi Leonard | +6.43 | 2330 |
| 7 | Donovan Mitchell | +6.30 | 1943 |
| 8 | Kyrie Irving | +5.50 | 2030 |
| 9 | Giannis Antetokounmpo | +5.50 | 2567 |
| 10 | Jalen Brunson | +5.48 | 2726 |
| 11 | De'Aaron Fox | +5.47 | 2659 |
| 12 | Tyrese Haliburton | +5.27 | 2224 |
| 13 | Isaiah Hartenstein | +4.80 | 1896 |
| 14 | Anthony Davis | +4.73 | 2700 |
| 15 | Jayson Tatum | +4.70 | 2645 |
| 16 | Stephen Curry | +4.65 | 2421 |
| 17 | LeBron James | +4.47 | 2504 |
| 18 | Kristaps Porzingis | +4.43 | 1690 |
| 19 | Jamal Murray | +4.41 | 1861 |
| 20 | Damian Lillard | +4.29 | 2579 |
| 21 | Lauri Markkanen | +4.12 | 1820 |
| 22 | Alex Caruso | +4.07 | 2040 |
| 23 | Derrick White | +4.03 | 2381 |
| 24 | Jimmy Butler | +4.00 | 2042 |
| 25 | Jusuf Nurkic | +3.96 | 2078 |
| 26 | Kevin Durant | +3.86 | 2791 |
| 27 | James Harden | +3.82 | 2470 |
| 28 | Fred VanVleet | +3.72 | 2684 |
| 29 | Alperen Sengun | +3.66 | 2046 |
| 30 | Rudy Gobert | +3.40 | 2593 |
| 31 | Devin Booker | +3.35 | 2447 |
| 32 | Chet Holmgren | +3.29 | 2413 |
| 33 | Victor Wembanyama | +3.21 | 2106 |
| 34 | Trey Murphy III | +3.17 | 1690 |
| 35 | Anthony Edwards | +3.08 | 2770 |
| 36 | Karl-Anthony Towns | +3.06 | 2026 |
| 37 | Tyrese Maxey | +3.04 | 2626 |
| 38 | Bogdan Bogdanovic | +3.02 | 2401 |
| 39 | Donte DiVincenzo | +3.02 | 2360 |
| 40 | Jarrett Allen | +2.95 | 2442 |
| 41 | T.J. McConnell | +2.88 | 1291 |
| 42 | Mike Conley | +2.78 | 2193 |
| 43 | Isaiah Joe | +2.71 | 1445 |
| 44 | Andre Drummond | +2.57 | 1351 |
| 45 | Jalen Williams | +2.54 | 2223 |
| 46 | Sam Hauser | +2.51 | 1741 |
| 47 | Draymond Green | +2.50 | 1490 |
| 48 | Brandin Podziemski | +2.41 | 1968 |
| 49 | Franz Wagner | +2.41 | 2337 |
| 50 | Dean Wade | +2.39 | 1108 |

## 2023-24 Playoffs — total, top 50 (projected, no truth)

> pool 103 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Joel Embiid | +10.23 | 248 |
| 2 | Luka Doncic | +8.38 | 900 |
| 3 | Nikola Jokic | +7.57 | 482 |
| 4 | Tyrese Haliburton | +6.22 | 522 |
| 5 | Anthony Edwards | +6.15 | 649 |
| 6 | Shai Gilgeous-Alexander | +5.56 | 399 |
| 7 | Donovan Mitchell | +5.51 | 382 |
| 8 | Jayson Tatum | +5.18 | 768 |
| 9 | Jalen Williams | +5.12 | 377 |
| 10 | Chet Holmgren | +4.95 | 345 |
| 11 | LeBron James | +4.85 | 204 |
| 12 | Kristaps Porzingis | +4.57 | 165 |
| 13 | Dereck Lively II | +4.55 | 462 |
| 14 | Jalen Brunson | +4.54 | 518 |
| 15 | Derrick White | +4.52 | 676 |
| 16 | Rudy Gobert | +4.38 | 512 |
| 17 | Jaylen Brown | +4.29 | 707 |
| 18 | Ivica Zubac | +4.24 | 192 |
| 19 | Paolo Banchero | +4.19 | 262 |
| 20 | Austin Reaves | +4.11 | 174 |
| 21 | Mike Conley | +4.07 | 474 |
| 22 | Kyrie Irving | +3.68 | 879 |
| 23 | Khris Middleton | +3.67 | 230 |
| 24 | Al Horford | +3.63 | 575 |
| 25 | Pascal Siakam | +3.51 | 603 |
| 26 | Sam Hauser | +3.41 | 283 |
| 27 | Jrue Holiday | +3.26 | 720 |
| 28 | Aaron Wiggins | +3.19 | 157 |
| 29 | Paul George | +3.14 | 222 |
| 30 | Devin Booker | +3.06 | 166 |
| 31 | Justin Holiday | +3.01 | 150 |
| 32 | Franz Wagner | +2.82 | 259 |
| 33 | Myles Turner | +2.79 | 550 |
| 34 | Kelly Oubre Jr. | +2.66 | 224 |
| 35 | Luguentz Dort | +2.54 | 350 |
| 36 | Jalen Suggs | +2.47 | 232 |
| 37 | T.J. McConnell | +2.40 | 348 |
| 38 | Bobby Portis | +2.33 | 187 |
| 39 | Kyle Lowry | +2.26 | 175 |
| 40 | Cason Wallace | +1.98 | 198 |
| 41 | Jaden McDaniels | +1.96 | 537 |
| 42 | Wendell Carter Jr. | +1.77 | 185 |
| 43 | Anthony Davis | +1.71 | 208 |
| 44 | Andrew Nembhard | +1.45 | 554 |
| 45 | Damian Lillard | +1.43 | 156 |
| 46 | Kentavious Caldwell-Pope | +1.39 | 420 |
| 47 | Jonathan Isaac | +1.29 | 147 |
| 48 | Karl-Anthony Towns | +1.23 | 522 |
| 49 | OG Anunoby | +1.12 | 324 |
| 50 | Josh Green | +0.99 | 399 |

## 2024-25 Regular season — total, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +8.87 | 2571 |
| 2 | Shai Gilgeous-Alexander | +8.54 | 2598 |
| 3 | Luka Dončić | +8.17 | 1769 |
| 4 | Giannis Antetokounmpo | +5.79 | 2289 |
| 5 | Stephen Curry | +5.67 | 2252 |
| 6 | Donovan Mitchell | +5.57 | 2232 |
| 7 | Ivica Zubac | +5.14 | 2624 |
| 8 | Victor Wembanyama | +5.13 | 1527 |
| 9 | Jayson Tatum | +5.11 | 2624 |
| 10 | James Harden | +5.01 | 2789 |
| 11 | Tyrese Haliburton | +4.76 | 2451 |
| 12 | Luke Kornet | +4.73 | 1361 |
| 13 | Darius Garland | +4.71 | 2301 |
| 14 | Derrick White | +4.50 | 2574 |
| 15 | Rudy Gobert | +4.37 | 2388 |
| 16 | Jimmy Butler | +4.34 | 1746 |
| 17 | Alperen Sengun | +4.33 | 2394 |
| 18 | Ty Jerome | +4.05 | 1393 |
| 19 | Karl-Anthony Towns | +4.04 | 2517 |
| 20 | Franz Wagner | +3.89 | 2023 |
| 21 | Kawhi Leonard | +3.84 | 1180 |
| 22 | Tyler Herro | +3.77 | 2725 |
| 23 | Jarrett Allen | +3.73 | 2296 |
| 24 | Kyrie Irving | +3.54 | 1804 |
| 25 | Norman Powell | +3.52 | 1958 |
| 26 | Anthony Edwards | +3.52 | 2871 |
| 27 | Evan Mobley | +3.45 | 2167 |
| 28 | Austin Reaves | +3.34 | 2550 |
| 29 | Daniel Gafford | +3.30 | 1226 |
| 30 | Luguentz Dort | +3.29 | 2073 |
| 31 | Payton Pritchard | +3.25 | 2271 |
| 32 | Jaren Jackson Jr. | +3.19 | 2207 |
| 33 | Jamal Murray | +3.16 | 2418 |
| 34 | Brandin Podziemski | +3.07 | 1716 |
| 35 | Kristaps Porziņģis | +3.06 | 1210 |
| 36 | Damian Lillard | +2.91 | 2093 |
| 37 | Isaiah Joe | +2.88 | 1604 |
| 38 | Anthony Davis | +2.84 | 1706 |
| 39 | Pascal Siakam | +2.83 | 2548 |
| 40 | Draymond Green | +2.80 | 1983 |
| 41 | Keon Ellis | +2.79 | 1948 |
| 42 | Deni Avdija | +2.63 | 2161 |
| 43 | Ja Morant | +2.58 | 1519 |
| 44 | Donte DiVincenzo | +2.57 | 1606 |
| 45 | Mike Conley | +2.56 | 1756 |
| 46 | Domantas Sabonis | +2.51 | 2429 |
| 47 | Tari Eason | +2.47 | 1420 |
| 48 | Ausar Thompson | +2.46 | 1328 |
| 49 | Amen Thompson | +2.44 | 2225 |
| 50 | Jalen Williams | +2.39 | 2237 |

## 2024-25 Playoffs — total, top 50 (projected, no truth)

> pool 109 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +8.40 | 563 |
| 2 | Jayson Tatum | +7.87 | 322 |
| 3 | Donovan Mitchell | +6.56 | 288 |
| 4 | Stephen Curry | +6.44 | 281 |
| 5 | Alperen Sengun | +6.41 | 256 |
| 6 | Jarrett Allen | +6.14 | 261 |
| 7 | Shai Gilgeous-Alexander | +5.94 | 851 |
| 8 | Anthony Edwards | +5.83 | 585 |
| 9 | Cade Cunningham | +5.77 | 248 |
| 10 | Steven Adams | +5.69 | 155 |
| 11 | Tyrese Haliburton | +5.43 | 772 |
| 12 | Gary Trent Jr. | +5.42 | 171 |
| 13 | Jaden McDaniels | +5.34 | 497 |
| 14 | Alex Caruso | +5.26 | 562 |
| 15 | LeBron James | +4.94 | 204 |
| 16 | Luka Dončić | +4.86 | 208 |
| 17 | Pascal Siakam | +4.81 | 771 |
| 18 | Ausar Thompson | +4.59 | 135 |
| 19 | Giannis Antetokounmpo | +4.45 | 188 |
| 20 | Luke Kornet | +4.44 | 180 |
| 21 | Fred VanVleet | +4.42 | 280 |
| 22 | Rudy Gobert | +4.38 | 411 |
| 23 | Aaron Nesmith | +4.28 | 650 |
| 24 | Jamal Murray | +4.28 | 578 |
| 25 | Max Strus | +4.21 | 253 |
| 26 | Amen Thompson | +3.76 | 231 |
| 27 | Chet Holmgren | +3.58 | 686 |
| 28 | Isaiah Joe | +3.54 | 211 |
| 29 | Aaron Gordon | +3.49 | 522 |
| 30 | Derrick White | +3.43 | 415 |
| 31 | Jalen Brunson | +3.36 | 680 |
| 32 | Ty Jerome | +3.14 | 191 |
| 33 | Payton Pritchard | +3.13 | 302 |
| 34 | Julius Randle | +3.12 | 533 |
| 35 | Dennis Schröder | +2.99 | 164 |
| 36 | Andrew Nembhard | +2.93 | 769 |
| 37 | Mitchell Robinson | +2.88 | 370 |
| 38 | Brandin Podziemski | +2.88 | 385 |
| 39 | Buddy Hield | +2.78 | 327 |
| 40 | Isaiah Hartenstein | +2.68 | 516 |
| 41 | AJ Green | +2.65 | 135 |
| 42 | Karl-Anthony Towns | +2.63 | 639 |
| 43 | Jaylen Brown | +2.61 | 402 |
| 44 | Evan Mobley | +2.45 | 257 |
| 45 | Cason Wallace | +2.37 | 516 |
| 46 | Kenrich Williams | +2.32 | 137 |
| 47 | Nicolas Batum | +2.11 | 172 |
| 48 | Jalen Williams | +2.09 | 796 |
| 49 | Jimmy Butler III | +1.97 | 397 |
| 50 | Kawhi Leonard | +1.96 | 265 |

## 2025-26 Regular season — total, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.36 | 2265 |
| 2 | Kawhi Leonard | +8.44 | 2085 |
| 3 | Victor Wembanyama | +8.04 | 1866 |
| 4 | Luka Dončić | +7.93 | 2289 |
| 5 | Shai Gilgeous-Alexander | +7.73 | 2259 |
| 6 | Jimmy Butler III | +6.35 | 1182 |
| 7 | Donovan Mitchell | +6.22 | 2342 |
| 8 | Chet Holmgren | +5.58 | 1997 |
| 9 | LaMelo Ball | +5.42 | 2017 |
| 10 | Cade Cunningham | +5.33 | 2172 |
| 11 | Jamal Murray | +5.12 | 2652 |
| 12 | Derrick White | +5.07 | 2625 |
| 13 | Stephen Curry | +4.99 | 1329 |
| 14 | Brandon Miller | +4.50 | 1968 |
| 15 | Tyrese Maxey | +4.38 | 2661 |
| 16 | Collin Gillespie | +4.37 | 2282 |
| 17 | Jalen Duren | +4.30 | 1976 |
| 18 | Paul George | +4.30 | 1135 |
| 19 | Neemias Queta | +4.14 | 1926 |
| 20 | Jalen Brunson | +4.09 | 2590 |
| 21 | Ajay Mitchell | +4.01 | 1473 |
| 22 | Joel Embiid | +4.00 | 1201 |
| 23 | Karl-Anthony Towns | +3.82 | 2322 |
| 24 | Austin Reaves | +3.81 | 1762 |
| 25 | Nickeil Alexander-Walker | +3.76 | 2603 |
| 26 | Kevin Durant | +3.65 | 2840 |
| 27 | Dyson Daniels | +3.64 | 2520 |
| 28 | Jalen Suggs | +3.63 | 1574 |
| 29 | Isaiah Joe | +3.63 | 1507 |
| 30 | Anthony Edwards | +3.45 | 2137 |
| 31 | James Harden | +3.43 | 2438 |
| 32 | Donte DiVincenzo | +3.41 | 2494 |
| 33 | Deni Avdija | +3.32 | 2199 |
| 34 | Scottie Barnes | +3.30 | 2681 |
| 35 | Jrue Holiday | +3.28 | 1560 |
| 36 | Jaylen Brown | +3.27 | 2443 |
| 37 | Isaiah Hartenstein | +3.27 | 1137 |
| 38 | Bam Adebayo | +3.21 | 2365 |
| 39 | Devin Booker | +3.21 | 2146 |
| 40 | Jarrett Allen | +3.08 | 1519 |
| 41 | Toumani Camara | +3.05 | 2731 |
| 42 | Cason Wallace | +2.99 | 2046 |
| 43 | Reed Sheppard | +2.98 | 2147 |
| 44 | Ausar Thompson | +2.95 | 1896 |
| 45 | Rudy Gobert | +2.90 | 2380 |
| 46 | Moussa Diabaté | +2.86 | 1899 |
| 47 | Jordan Goodwin | +2.82 | 1572 |
| 48 | Mitchell Robinson | +2.81 | 1175 |
| 49 | Javonte Green | +2.77 | 1446 |
| 50 | OG Anunoby | +2.70 | 2224 |

## 2025-26 Playoffs — total, top 50 (projected, no truth)

> pool 112 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jayson Tatum | +9.46 | 218 |
| 2 | Karl-Anthony Towns | +7.87 | 578 |
| 3 | Alex Caruso | +7.79 | 353 |
| 4 | Victor Wembanyama | +7.77 | 750 |
| 5 | Ajay Mitchell | +7.71 | 317 |
| 6 | Amen Thompson | +6.46 | 264 |
| 7 | Tari Eason | +5.87 | 195 |
| 8 | James Harden | +5.57 | 672 |
| 9 | Chet Holmgren | +5.50 | 459 |
| 10 | Jose Alvarado | +5.48 | 170 |
| 11 | Jarrett Allen | +4.87 | 529 |
| 12 | OG Anunoby | +4.86 | 586 |
| 13 | Alperen Sengun | +4.71 | 232 |
| 14 | Jalen Brunson | +4.68 | 702 |
| 15 | Cason Wallace | +4.46 | 374 |
| 16 | Dylan Harper | +4.08 | 615 |
| 17 | Julian Champagnie | +3.81 | 705 |
| 18 | RJ Barrett | +3.59 | 271 |
| 19 | Nikola Jokić | +3.46 | 237 |
| 20 | Paolo Banchero | +3.38 | 273 |
| 21 | Neemias Queta | +3.23 | 152 |
| 22 | Mikal Bridges | +3.18 | 608 |
| 23 | Dean Wade | +3.08 | 407 |
| 24 | Devin Vassell | +3.03 | 801 |
| 25 | Cade Cunningham | +2.99 | 572 |
| 26 | Shai Gilgeous-Alexander | +2.97 | 544 |
| 27 | Dillon Brooks | +2.91 | 149 |
| 28 | Scottie Barnes | +2.87 | 273 |
| 29 | Payton Pritchard | +2.86 | 231 |
| 30 | Mike Conley | +2.62 | 168 |
| 31 | Josh Hart | +2.61 | 614 |
| 32 | Ausar Thompson | +2.45 | 427 |
| 33 | Paul George | +2.44 | 394 |
| 34 | Isaiah Hartenstein | +2.40 | 350 |
| 35 | Duncan Robinson | +2.37 | 383 |
| 36 | De'Aaron Fox | +2.37 | 704 |
| 37 | Collin Murray-Boyles | +2.33 | 191 |
| 38 | Jaylin Williams | +2.22 | 240 |
| 39 | Cameron Johnson | +2.07 | 186 |
| 40 | Mitchell Robinson | +1.98 | 251 |
| 41 | Jabari Smith Jr. | +1.89 | 252 |
| 42 | VJ Edgecombe | +1.88 | 407 |
| 43 | Marcus Smart | +1.88 | 345 |
| 44 | Jamal Shead | +1.81 | 224 |
| 45 | Sam Merrill | +1.70 | 338 |
| 46 | Tim Hardaway Jr. | +1.61 | 140 |
| 47 | Tyrese Maxey | +1.39 | 437 |
| 48 | Donovan Mitchell | +1.25 | 652 |
| 49 | Naz Reid | +1.23 | 323 |
| 50 | Scoot Henderson | +1.23 | 145 |

## 2023-24 Regular season — offense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Luka Doncic | +8.09 | 2624 |
| 2 | Nikola Jokic | +7.84 | 2737 |
| 3 | Jalen Brunson | +7.14 | 2726 |
| 4 | Shai Gilgeous-Alexander | +6.88 | 2553 |
| 5 | Tyrese Haliburton | +5.84 | 2224 |
| 6 | Donovan Mitchell | +5.78 | 1943 |
| 7 | Devin Booker | +5.40 | 2447 |
| 8 | Stephen Curry | +5.39 | 2421 |
| 9 | Damian Lillard | +5.13 | 2579 |
| 10 | Trae Young | +5.13 | 1942 |
| 11 | Kyrie Irving | +5.13 | 2030 |
| 12 | LeBron James | +5.12 | 2504 |
| 13 | Giannis Antetokounmpo | +5.11 | 2567 |
| 14 | Jamal Murray | +4.90 | 1861 |
| 15 | De'Aaron Fox | +4.63 | 2659 |
| 16 | James Harden | +4.59 | 2470 |
| 17 | Tyrese Maxey | +4.59 | 2626 |
| 18 | Anthony Edwards | +4.49 | 2770 |
| 19 | Joel Embiid | +4.45 | 1309 |
| 20 | Jayson Tatum | +4.36 | 2645 |
| 21 | Kawhi Leonard | +4.02 | 2330 |
| 22 | Paul George | +3.98 | 2502 |
| 23 | Collin Sexton | +3.82 | 2075 |
| 24 | Jimmy Butler | +3.71 | 2042 |
| 25 | Kevin Durant | +3.54 | 2791 |
| 26 | Fred VanVleet | +3.48 | 2684 |
| 27 | Lauri Markkanen | +3.43 | 1820 |
| 28 | Desmond Bane | +3.40 | 1443 |
| 29 | DeMar DeRozan | +3.31 | 2989 |
| 30 | T.J. McConnell | +3.25 | 1291 |
| 31 | CJ McCollum | +3.13 | 2159 |
| 32 | Payton Pritchard | +3.13 | 1826 |
| 33 | D'Angelo Russell | +3.11 | 2484 |
| 34 | Anfernee Simons | +3.04 | 1582 |
| 35 | Dejounte Murray | +2.97 | 2783 |
| 36 | Donte DiVincenzo | +2.93 | 2360 |
| 37 | Julius Randle | +2.82 | 1630 |
| 38 | Mike Conley | +2.77 | 2193 |
| 39 | Malcolm Brogdon | +2.66 | 1121 |
| 40 | Malik Monk | +2.55 | 1872 |
| 41 | Anthony Davis | +2.52 | 2700 |
| 42 | Khris Middleton | +2.45 | 1487 |
| 43 | Pascal Siakam | +2.44 | 2658 |
| 44 | Zion Williamson | +2.39 | 2207 |
| 45 | Immanuel Quickley | +2.33 | 1985 |
| 46 | Brandon Ingram | +2.29 | 2103 |
| 47 | Bogdan Bogdanovic | +2.23 | 2401 |
| 48 | Derrick White | +2.23 | 2381 |
| 49 | Sam Merrill | +2.21 | 1069 |
| 50 | Terry Rozier | +2.14 | 2040 |

## 2023-24 Playoffs — offense, top 50 (projected, no truth)

> pool 103 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +8.30 | 482 |
| 2 | Joel Embiid | +6.62 | 248 |
| 3 | Jalen Brunson | +6.24 | 518 |
| 4 | Luka Doncic | +5.86 | 900 |
| 5 | Devin Booker | +5.85 | 166 |
| 6 | Tyrese Haliburton | +5.40 | 522 |
| 7 | Kyrie Irving | +5.36 | 879 |
| 8 | Damian Lillard | +5.24 | 156 |
| 9 | Anthony Edwards | +5.23 | 649 |
| 10 | Austin Reaves | +5.00 | 174 |
| 11 | Donovan Mitchell | +4.99 | 382 |
| 12 | Shai Gilgeous-Alexander | +4.97 | 399 |
| 13 | Khris Middleton | +4.93 | 230 |
| 14 | Tyrese Maxey | +4.86 | 267 |
| 15 | Jayson Tatum | +4.86 | 768 |
| 16 | Kevin Durant | +4.74 | 168 |
| 17 | Anthony Davis | +4.62 | 208 |
| 18 | LeBron James | +4.45 | 204 |
| 19 | James Harden | +3.77 | 242 |
| 20 | Andrew Nembhard | +3.67 | 554 |
| 21 | Pascal Siakam | +3.50 | 603 |
| 22 | Derrick White | +3.28 | 676 |
| 23 | Jalen Williams | +2.80 | 377 |
| 24 | Paul George | +2.79 | 222 |
| 25 | Mike Conley | +2.74 | 474 |
| 26 | T.J. McConnell | +2.65 | 348 |
| 27 | Donte DiVincenzo | +2.63 | 466 |
| 28 | Jaden McDaniels | +2.47 | 537 |
| 29 | Myles Turner | +2.46 | 550 |
| 30 | Jaylen Brown | +2.41 | 707 |
| 31 | Kyle Lowry | +2.17 | 175 |
| 32 | Rudy Gobert | +2.00 | 512 |
| 33 | Jrue Holiday | +1.91 | 720 |
| 34 | Sam Hauser | +1.80 | 283 |
| 35 | Michael Porter Jr. | +1.76 | 443 |
| 36 | Al Horford | +1.55 | 575 |
| 37 | Aaron Gordon | +1.53 | 445 |
| 38 | Karl-Anthony Towns | +1.49 | 522 |
| 39 | Kristaps Porzingis | +1.49 | 165 |
| 40 | Kentavious Caldwell-Pope | +1.39 | 420 |
| 41 | Kelly Oubre Jr. | +1.39 | 224 |
| 42 | Bam Adebayo | +1.27 | 192 |
| 43 | Bobby Portis | +1.26 | 187 |
| 44 | Patrick Beverley | +1.19 | 210 |
| 45 | Dereck Lively II | +1.17 | 462 |
| 46 | Chet Holmgren | +1.13 | 345 |
| 47 | Jamal Murray | +1.04 | 462 |
| 48 | Josh Hart | +1.02 | 548 |
| 49 | Josh Green | +1.00 | 399 |
| 50 | Ivica Zubac | +0.96 | 192 |

## 2024-25 Regular season — offense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +8.80 | 2571 |
| 2 | Shai Gilgeous-Alexander | +7.98 | 2598 |
| 3 | Stephen Curry | +6.99 | 2252 |
| 4 | Luka Dončić | +6.39 | 1769 |
| 5 | LaMelo Ball | +5.90 | 1505 |
| 6 | Jalen Brunson | +5.75 | 2301 |
| 7 | Giannis Antetokounmpo | +5.38 | 2289 |
| 8 | Jayson Tatum | +5.21 | 2624 |
| 9 | Donovan Mitchell | +5.12 | 2232 |
| 10 | Damian Lillard | +5.11 | 2093 |
| 11 | James Harden | +5.08 | 2789 |
| 12 | Ty Jerome | +5.01 | 1393 |
| 13 | Tyrese Haliburton | +4.93 | 2451 |
| 14 | Darius Garland | +4.84 | 2301 |
| 15 | Trae Young | +4.82 | 2739 |
| 16 | Anthony Edwards | +4.43 | 2871 |
| 17 | Tyler Herro | +4.38 | 2725 |
| 18 | Cade Cunningham | +4.36 | 2452 |
| 19 | Jamal Murray | +4.13 | 2418 |
| 20 | Austin Reaves | +3.79 | 2550 |
| 21 | Kyrie Irving | +3.72 | 1804 |
| 22 | Tyrese Maxey | +3.63 | 1960 |
| 23 | Payton Pritchard | +3.61 | 2271 |
| 24 | Ja Morant | +3.56 | 1519 |
| 25 | Devin Booker | +3.53 | 2795 |
| 26 | Jimmy Butler | +3.25 | 1746 |
| 27 | Karl-Anthony Towns | +3.19 | 2517 |
| 28 | Franz Wagner | +3.12 | 2023 |
| 29 | Kevin Durant | +3.06 | 2265 |
| 30 | Christian Braun | +2.96 | 2675 |
| 31 | Norman Powell | +2.88 | 1958 |
| 32 | LeBron James | +2.61 | 2444 |
| 33 | Isaiah Joe | +2.61 | 1604 |
| 34 | Cameron Johnson | +2.46 | 1800 |
| 35 | DeMar DeRozan | +2.44 | 2768 |
| 36 | Collin Sexton | +2.44 | 1758 |
| 37 | Derrick White | +2.43 | 2574 |
| 38 | Paolo Banchero | +2.36 | 1582 |
| 39 | Jaylen Brown | +2.34 | 2158 |
| 40 | Kawhi Leonard | +2.33 | 1180 |
| 41 | Desmond Bane | +2.28 | 2205 |
| 42 | Aaron Gordon | +2.21 | 1447 |
| 43 | Domantas Sabonis | +2.17 | 2429 |
| 44 | Michael Porter Jr. | +2.10 | 2593 |
| 45 | Deni Avdija | +2.05 | 2161 |
| 46 | Zach LaVine | +1.97 | 2603 |
| 47 | CJ McCollum | +1.95 | 1832 |
| 48 | Jalen Green | +1.94 | 2697 |
| 49 | Sam Hauser | +1.91 | 1541 |
| 50 | Jordan Poole | +1.90 | 2001 |

## 2024-25 Playoffs — offense, top 50 (projected, no truth)

> pool 109 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jalen Brunson | +8.23 | 680 |
| 2 | Luka Dončić | +7.39 | 208 |
| 3 | Giannis Antetokounmpo | +7.07 | 188 |
| 4 | Fred VanVleet | +6.40 | 280 |
| 5 | Nikola Jokić | +6.34 | 563 |
| 6 | Shai Gilgeous-Alexander | +6.02 | 851 |
| 7 | Donovan Mitchell | +5.81 | 288 |
| 8 | Tyrese Haliburton | +5.58 | 772 |
| 9 | LeBron James | +5.46 | 204 |
| 10 | Stephen Curry | +5.37 | 281 |
| 11 | Jayson Tatum | +5.23 | 322 |
| 12 | Payton Pritchard | +4.43 | 302 |
| 13 | Gary Trent Jr. | +4.27 | 171 |
| 14 | Davion Mitchell | +4.24 | 142 |
| 15 | Anthony Edwards | +4.19 | 585 |
| 16 | Max Strus | +4.13 | 253 |
| 17 | Kawhi Leonard | +3.97 | 265 |
| 18 | Isaiah Joe | +3.76 | 211 |
| 19 | Jamal Murray | +3.69 | 578 |
| 20 | Paolo Banchero | +3.62 | 197 |
| 21 | Julius Randle | +3.59 | 533 |
| 22 | Dennis Schröder | +3.47 | 164 |
| 23 | AJ Green | +3.44 | 135 |
| 24 | Ty Jerome | +3.37 | 191 |
| 25 | Derrick White | +3.15 | 415 |
| 26 | Aaron Gordon | +3.10 | 522 |
| 27 | Jalen Williams | +3.06 | 796 |
| 28 | Alperen Sengun | +2.93 | 256 |
| 29 | James Harden | +2.91 | 276 |
| 30 | Amen Thompson | +2.76 | 231 |
| 31 | Pascal Siakam | +2.75 | 771 |
| 32 | Jimmy Butler III | +2.70 | 397 |
| 33 | Aaron Nesmith | +2.58 | 650 |
| 34 | Darius Garland | +2.50 | 148 |
| 35 | Cade Cunningham | +2.29 | 248 |
| 36 | Dillon Brooks | +2.26 | 206 |
| 37 | Franz Wagner | +2.21 | 195 |
| 38 | Buddy Hield | +2.17 | 327 |
| 39 | Jarrett Allen | +2.09 | 261 |
| 40 | Evan Mobley | +2.06 | 257 |
| 41 | Jaden McDaniels | +2.06 | 497 |
| 42 | Bam Adebayo | +2.04 | 153 |
| 43 | Andrew Nembhard | +1.78 | 769 |
| 44 | Sam Merrill | +1.72 | 159 |
| 45 | Jalen Duren | +1.70 | 203 |
| 46 | Nicolas Batum | +1.59 | 172 |
| 47 | De'Andre Hunter | +1.57 | 185 |
| 48 | Luke Kornet | +1.49 | 180 |
| 49 | Alex Caruso | +1.46 | 562 |
| 50 | Steven Adams | +1.36 | 155 |

## 2025-26 Regular season — offense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +8.48 | 2265 |
| 2 | Shai Gilgeous-Alexander | +7.79 | 2259 |
| 3 | Luka Dončić | +7.54 | 2289 |
| 4 | Donovan Mitchell | +6.38 | 2342 |
| 5 | Kawhi Leonard | +6.11 | 2085 |
| 6 | Jamal Murray | +6.11 | 2652 |
| 7 | James Harden | +5.92 | 2438 |
| 8 | LaMelo Ball | +5.79 | 2017 |
| 9 | Jalen Brunson | +5.78 | 2590 |
| 10 | Cade Cunningham | +5.25 | 2172 |
| 11 | Stephen Curry | +5.24 | 1329 |
| 12 | Tyrese Maxey | +5.01 | 2661 |
| 13 | Jimmy Butler III | +4.68 | 1182 |
| 14 | Devin Booker | +4.63 | 2146 |
| 15 | Deni Avdija | +4.50 | 2199 |
| 16 | Anthony Edwards | +4.45 | 2137 |
| 17 | Kevin Durant | +4.01 | 2840 |
| 18 | Payton Pritchard | +3.94 | 2556 |
| 19 | Victor Wembanyama | +3.72 | 1866 |
| 20 | Coby White | +3.72 | 1250 |
| 21 | Jalen Duren | +3.70 | 1976 |
| 22 | Austin Reaves | +3.68 | 1762 |
| 23 | Michael Porter Jr. | +3.64 | 1689 |
| 24 | Jaylen Brown | +3.64 | 2443 |
| 25 | Jrue Holiday | +3.63 | 1560 |
| 26 | Joel Embiid | +3.44 | 1201 |
| 27 | Keyonte George | +3.30 | 1786 |
| 28 | Lauri Markkanen | +3.24 | 1443 |
| 29 | Duncan Robinson | +3.13 | 2113 |
| 30 | De'Aaron Fox | +3.00 | 2231 |
| 31 | Collin Gillespie | +2.97 | 2282 |
| 32 | Cam Spencer | +2.93 | 1714 |
| 33 | Kon Knueppel | +2.83 | 2551 |
| 34 | Julius Randle | +2.77 | 2610 |
| 35 | Darius Garland | +2.76 | 1344 |
| 36 | Brandon Miller | +2.75 | 1968 |
| 37 | Alperen Sengun | +2.74 | 2398 |
| 38 | Anfernee Simons | +2.72 | 1372 |
| 39 | CJ McCollum | +2.58 | 2263 |
| 40 | Luka Garza | +2.58 | 1118 |
| 41 | Reed Sheppard | +2.56 | 2147 |
| 42 | Grayson Allen | +2.53 | 1467 |
| 43 | Nickeil Alexander-Walker | +2.52 | 2603 |
| 44 | LeBron James | +2.52 | 1989 |
| 45 | Isaiah Joe | +2.50 | 1507 |
| 46 | Bones Hyland | +2.38 | 1177 |
| 47 | Miles McBride | +2.37 | 1080 |
| 48 | Ryan Rollins | +2.35 | 2375 |
| 49 | Sam Merrill | +2.30 | 1377 |
| 50 | Immanuel Quickley | +2.29 | 2231 |

## 2025-26 Playoffs — offense, top 50 (projected, no truth)

> pool 112 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jayson Tatum | +7.68 | 218 |
| 2 | Jalen Brunson | +6.33 | 702 |
| 3 | Dillon Brooks | +5.96 | 149 |
| 4 | Tyrese Maxey | +4.84 | 437 |
| 5 | Ajay Mitchell | +4.68 | 317 |
| 6 | Payton Pritchard | +4.66 | 231 |
| 7 | Karl-Anthony Towns | +4.46 | 578 |
| 8 | RJ Barrett | +4.35 | 271 |
| 9 | Cade Cunningham | +4.15 | 572 |
| 10 | Scottie Barnes | +4.06 | 273 |
| 11 | Shai Gilgeous-Alexander | +3.98 | 544 |
| 12 | James Harden | +3.86 | 672 |
| 13 | Nikola Jokić | +3.41 | 237 |
| 14 | Paolo Banchero | +3.40 | 273 |
| 15 | Mike Conley | +3.39 | 168 |
| 16 | Jalen Green | +3.38 | 151 |
| 17 | Alex Caruso | +3.31 | 353 |
| 18 | Joel Embiid | +3.20 | 233 |
| 19 | Naz Reid | +3.19 | 323 |
| 20 | Devin Booker | +3.19 | 153 |
| 21 | Victor Wembanyama | +3.14 | 750 |
| 22 | Duncan Robinson | +3.01 | 383 |
| 23 | Donovan Mitchell | +2.96 | 652 |
| 24 | Sam Merrill | +2.93 | 338 |
| 25 | OG Anunoby | +2.86 | 586 |
| 26 | Paul George | +2.59 | 394 |
| 27 | Amen Thompson | +2.56 | 264 |
| 28 | Chet Holmgren | +2.44 | 459 |
| 29 | Tim Hardaway Jr. | +2.43 | 140 |
| 30 | Jrue Holiday | +2.30 | 192 |
| 31 | Tari Eason | +2.18 | 195 |
| 32 | Jarrett Allen | +2.14 | 529 |
| 33 | Julian Champagnie | +2.01 | 705 |
| 34 | Desmond Bane | +1.99 | 253 |
| 35 | Ayo Dosunmu | +1.97 | 292 |
| 36 | Austin Reaves | +1.93 | 221 |
| 37 | Onyeka Okongwu | +1.92 | 199 |
| 38 | Jalen Johnson | +1.83 | 214 |
| 39 | Dylan Harper | +1.77 | 615 |
| 40 | Wendell Carter Jr. | +1.70 | 237 |
| 41 | Cameron Johnson | +1.70 | 186 |
| 42 | Jose Alvarado | +1.68 | 170 |
| 43 | Jared McCain | +1.54 | 258 |
| 44 | Dean Wade | +1.49 | 407 |
| 45 | Mikal Bridges | +1.40 | 608 |
| 46 | Luke Kennard | +1.40 | 326 |
| 47 | Isaiah Joe | +1.37 | 143 |
| 48 | Stephon Castle | +1.26 | 760 |
| 49 | Tobias Harris | +1.24 | 485 |
| 50 | Isaiah Hartenstein | +1.24 | 350 |

## 2023-24 Regular season — defense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +3.80 | 2106 |
| 2 | Rudy Gobert | +3.44 | 2593 |
| 3 | Isaiah Hartenstein | +3.26 | 1896 |
| 4 | Dean Wade | +3.15 | 1108 |
| 5 | Joel Embiid | +3.03 | 1309 |
| 6 | Jusuf Nurkic | +3.01 | 2078 |
| 7 | Alex Caruso | +2.78 | 2040 |
| 8 | Brook Lopez | +2.77 | 2411 |
| 9 | Kristaps Porzingis | +2.76 | 1690 |
| 10 | Chet Holmgren | +2.66 | 2413 |
| 11 | Nikola Jokic | +2.55 | 2737 |
| 12 | Draymond Green | +2.38 | 1490 |
| 13 | Ausar Thompson | +2.30 | 1583 |
| 14 | Toumani Camara | +2.26 | 1739 |
| 15 | Larry Nance Jr. | +2.25 | 1216 |
| 16 | Andre Drummond | +2.14 | 1351 |
| 17 | Paul George | +2.01 | 2502 |
| 18 | Anthony Davis | +1.99 | 2700 |
| 19 | Derrick White | +1.99 | 2381 |
| 20 | OG Anunoby | +1.96 | 1702 |
| 21 | Ivica Zubac | +1.94 | 1795 |
| 22 | Amen Thompson | +1.92 | 1388 |
| 23 | Jarrett Allen | +1.85 | 2442 |
| 24 | Kawhi Leonard | +1.83 | 2330 |
| 25 | Franz Wagner | +1.81 | 2337 |
| 26 | Aaron Nesmith | +1.80 | 1995 |
| 27 | Naz Reid | +1.76 | 1964 |
| 28 | Myles Turner | +1.74 | 2077 |
| 29 | Evan Mobley | +1.70 | 1532 |
| 30 | Jalen Suggs | +1.69 | 2025 |
| 31 | Walker Kessler | +1.64 | 1493 |
| 32 | Matisse Thybulle | +1.64 | 1487 |
| 33 | Dyson Daniels | +1.58 | 1358 |
| 34 | Paul Reed | +1.57 | 1590 |
| 35 | Nickeil Alexander-Walker | +1.54 | 1921 |
| 36 | Herbert Jones | +1.53 | 2321 |
| 37 | Wendell Carter Jr. | +1.43 | 1406 |
| 38 | Bam Adebayo | +1.41 | 2416 |
| 39 | Clint Capela | +1.39 | 1883 |
| 40 | Derrick Jones Jr. | +1.38 | 1783 |
| 41 | Kyle Anderson | +1.37 | 1782 |
| 42 | Isaiah Joe | +1.34 | 1445 |
| 43 | Jakob Poeltl | +1.32 | 1319 |
| 44 | Alperen Sengun | +1.31 | 2046 |
| 45 | Shai Gilgeous-Alexander | +1.30 | 2553 |
| 46 | Moses Moody | +1.30 | 1156 |
| 47 | Vince Williams Jr. | +1.22 | 1436 |
| 48 | Daniel Gafford | +1.09 | 1814 |
| 49 | Naji Marshall | +1.08 | 1257 |
| 50 | Haywood Highsmith | +0.98 | 1366 |

## 2023-24 Playoffs — defense, top 50 (projected, no truth)

> pool 103 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Justin Holiday | +4.84 | 150 |
| 2 | Kristaps Porzingis | +4.27 | 165 |
| 3 | Joel Embiid | +4.16 | 248 |
| 4 | Paolo Banchero | +3.65 | 262 |
| 5 | Chet Holmgren | +3.50 | 345 |
| 6 | Dereck Lively II | +3.40 | 462 |
| 7 | Evan Mobley | +3.17 | 422 |
| 8 | Al Horford | +2.55 | 575 |
| 9 | Aaron Wiggins | +2.54 | 157 |
| 10 | Brandon Ingram | +2.29 | 145 |
| 11 | Wendell Carter Jr. | +2.21 | 185 |
| 12 | Rudy Gobert | +2.07 | 512 |
| 13 | Luka Doncic | +1.99 | 900 |
| 14 | Jalen Suggs | +1.86 | 232 |
| 15 | Josh Giddey | +1.72 | 181 |
| 16 | Franz Wagner | +1.69 | 259 |
| 17 | Jonathan Isaac | +1.57 | 147 |
| 18 | Jalen Williams | +1.47 | 377 |
| 19 | Cason Wallace | +1.46 | 198 |
| 20 | Isaac Okoro | +1.46 | 263 |
| 21 | Josh Green | +1.43 | 399 |
| 22 | Ivica Zubac | +1.39 | 192 |
| 23 | Kelly Oubre Jr. | +1.23 | 224 |
| 24 | Luguentz Dort | +1.07 | 350 |
| 25 | Sam Hauser | +1.04 | 283 |
| 26 | Christian Braun | +1.04 | 204 |
| 27 | Derrick White | +1.01 | 676 |
| 28 | Gary Harris | +0.97 | 159 |
| 29 | Jrue Holiday | +0.87 | 720 |
| 30 | Shai Gilgeous-Alexander | +0.84 | 399 |
| 31 | LeBron James | +0.78 | 204 |
| 32 | Anthony Edwards | +0.70 | 649 |
| 33 | Donovan Mitchell | +0.59 | 382 |
| 34 | Myles Turner | +0.57 | 550 |
| 35 | Mike Conley | +0.48 | 474 |
| 36 | Bobby Portis | +0.47 | 187 |
| 37 | Daniel Gafford | +0.46 | 445 |
| 38 | Nikola Jokic | +0.45 | 482 |
| 39 | OG Anunoby | +0.44 | 324 |
| 40 | T.J. McConnell | +0.34 | 348 |
| 41 | Jayson Tatum | +0.33 | 768 |
| 42 | Maxi Kleber | +0.20 | 219 |
| 43 | D'Angelo Russell | +0.17 | 185 |
| 44 | Paul George | +0.12 | 222 |
| 45 | Aaron Nesmith | +0.10 | 559 |
| 46 | Jaden McDaniels | +0.10 | 537 |
| 47 | Isaiah Joe | -0.05 | 173 |
| 48 | Derrick Jones Jr. | -0.08 | 647 |
| 49 | Devin Booker | -0.12 | 166 |
| 50 | Tyrese Haliburton | -0.14 | 522 |

## 2024-25 Regular season — defense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +4.20 | 1527 |
| 2 | Rudy Gobert | +3.52 | 2388 |
| 3 | Toumani Camara | +3.44 | 2548 |
| 4 | Alperen Sengun | +3.33 | 2394 |
| 5 | Luke Kornet | +3.31 | 1361 |
| 6 | Ausar Thompson | +2.91 | 1328 |
| 7 | Kris Dunn | +2.67 | 1783 |
| 8 | Ivica Zubac | +2.64 | 2624 |
| 9 | Dyson Daniels | +2.61 | 2571 |
| 10 | Brandin Podziemski | +2.45 | 1716 |
| 11 | Luguentz Dort | +2.34 | 2073 |
| 12 | Amen Thompson | +2.29 | 2225 |
| 13 | Jarrett Allen | +2.27 | 2296 |
| 14 | Jaxson Hayes | +2.26 | 1093 |
| 15 | Draymond Green | +2.25 | 1983 |
| 16 | Nicolas Batum | +2.22 | 1367 |
| 17 | Evan Mobley | +2.19 | 2167 |
| 18 | Jaren Jackson Jr. | +2.17 | 2207 |
| 19 | Jaden McDaniels | +2.12 | 2614 |
| 20 | Myles Turner | +2.01 | 2174 |
| 21 | Kristaps Porziņģis | +1.99 | 1210 |
| 22 | Scotty Pippen Jr. | +1.96 | 1683 |
| 23 | Isaiah Hartenstein | +1.96 | 1590 |
| 24 | Jonathan Isaac | +1.89 | 1090 |
| 25 | Donovan Clingan | +1.88 | 1324 |
| 26 | Kevon Looney | +1.87 | 1142 |
| 27 | Anthony Davis | +1.86 | 1706 |
| 28 | Franz Wagner | +1.81 | 2023 |
| 29 | Paul George | +1.79 | 1334 |
| 30 | Isaiah Stewart | +1.76 | 1434 |
| 31 | Jakob Poeltl | +1.67 | 1686 |
| 32 | Jalen Williams | +1.66 | 2237 |
| 33 | Keon Ellis | +1.64 | 1948 |
| 34 | Daniel Gafford | +1.57 | 1226 |
| 35 | Brandon Clarke | +1.56 | 1207 |
| 36 | Tari Eason | +1.51 | 1420 |
| 37 | Scottie Barnes | +1.51 | 2134 |
| 38 | Luka Dončić | +1.46 | 1769 |
| 39 | Shai Gilgeous-Alexander | +1.45 | 2598 |
| 40 | Kevin Porter Jr. | +1.43 | 1482 |
| 41 | Cody Martin | +1.43 | 1173 |
| 42 | Sam Merrill | +1.41 | 1401 |
| 43 | Wendell Carter Jr. | +1.37 | 1758 |
| 44 | Donte DiVincenzo | +1.36 | 1606 |
| 45 | Jabari Smith Jr. | +1.34 | 1716 |
| 46 | Derrick White | +1.33 | 2574 |
| 47 | Kentavious Caldwell-Pope | +1.31 | 2279 |
| 48 | OG Anunoby | +1.30 | 2706 |
| 49 | Davion Mitchell | +1.19 | 2027 |
| 50 | Goga Bitadze | +1.19 | 1430 |

## 2024-25 Playoffs — defense, top 50 (projected, no truth)

> pool 109 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.68 | 411 |
| 2 | Nikola Jokić | +4.36 | 563 |
| 3 | Draymond Green | +4.05 | 389 |
| 4 | Alex Caruso | +4.01 | 562 |
| 5 | Alperen Sengun | +3.97 | 256 |
| 6 | Jayson Tatum | +3.76 | 322 |
| 7 | Kenrich Williams | +3.69 | 137 |
| 8 | Chet Holmgren | +3.45 | 686 |
| 9 | Jaden McDaniels | +3.34 | 497 |
| 10 | Steven Adams | +3.27 | 155 |
| 11 | Ausar Thompson | +3.09 | 135 |
| 12 | Cason Wallace | +2.90 | 516 |
| 13 | Aaron Nesmith | +2.90 | 650 |
| 14 | Jaylin Williams | +2.87 | 141 |
| 15 | Pascal Siakam | +2.80 | 771 |
| 16 | Cade Cunningham | +2.79 | 248 |
| 17 | Gary Trent Jr. | +2.69 | 171 |
| 18 | Luke Kornet | +2.38 | 180 |
| 19 | Luguentz Dort | +2.32 | 666 |
| 20 | Mitchell Robinson | +2.08 | 370 |
| 21 | Jarrett Allen | +1.85 | 261 |
| 22 | OG Anunoby | +1.82 | 705 |
| 23 | Brandin Podziemski | +1.63 | 385 |
| 24 | Bobby Portis | +1.59 | 158 |
| 25 | Aaron Wiggins | +1.57 | 303 |
| 26 | Jabari Smith Jr. | +1.50 | 143 |
| 27 | Mike Conley | +1.36 | 356 |
| 28 | Jaylen Brown | +1.35 | 402 |
| 29 | Tari Eason | +1.25 | 132 |
| 30 | Shai Gilgeous-Alexander | +1.13 | 851 |
| 31 | Christian Braun | +1.03 | 544 |
| 32 | Isaiah Hartenstein | +1.02 | 516 |
| 33 | Andrew Nembhard | +0.93 | 769 |
| 34 | Jamal Murray | +0.80 | 578 |
| 35 | Quinten Post | +0.71 | 146 |
| 36 | Buddy Hield | +0.70 | 327 |
| 37 | Karl-Anthony Towns | +0.68 | 639 |
| 38 | Franz Wagner | +0.62 | 195 |
| 39 | Kentavious Caldwell-Pope | +0.48 | 163 |
| 40 | Peyton Watson | +0.46 | 199 |
| 41 | Tyrese Haliburton | +0.43 | 772 |
| 42 | Anthony Edwards | +0.39 | 585 |
| 43 | Josh Hart | +0.33 | 642 |
| 44 | Amen Thompson | +0.32 | 231 |
| 45 | Kevin Porter Jr. | +0.22 | 151 |
| 46 | T.J. McConnell | +0.06 | 402 |
| 47 | Kristaps Porziņģis | +0.00 | 231 |
| 48 | Paolo Banchero | -0.02 | 197 |
| 49 | Jalen Green | -0.06 | 219 |
| 50 | LeBron James | -0.06 | 204 |

## 2025-26 Regular season — defense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +5.56 | 1866 |
| 2 | Chet Holmgren | +4.04 | 1997 |
| 3 | Neemias Queta | +3.81 | 1926 |
| 4 | Ausar Thompson | +3.61 | 1896 |
| 5 | Isaiah Hartenstein | +3.34 | 1137 |
| 6 | Cason Wallace | +3.33 | 2046 |
| 7 | Derrick White | +3.33 | 2625 |
| 8 | Ronald Holland II | +3.19 | 1550 |
| 9 | Rudy Gobert | +2.95 | 2380 |
| 10 | Hugo González | +2.76 | 1084 |
| 11 | Javonte Green | +2.63 | 1446 |
| 12 | Ajay Mitchell | +2.51 | 1473 |
| 13 | Dyson Daniels | +2.47 | 2520 |
| 14 | Toumani Camara | +2.42 | 2731 |
| 15 | Marcus Smart | +2.33 | 1769 |
| 16 | Jalen Suggs | +2.30 | 1574 |
| 17 | Baylor Scheierman | +2.25 | 1429 |
| 18 | Jordan Goodwin | +2.16 | 1572 |
| 19 | Donte DiVincenzo | +2.15 | 2494 |
| 20 | Jamal Shead | +2.12 | 1852 |
| 21 | Dru Smith | +2.01 | 1141 |
| 22 | Keon Ellis | +1.96 | 1479 |
| 23 | Josh Okogie | +1.96 | 1354 |
| 24 | Paul George | +1.90 | 1135 |
| 25 | Ryan Kalkbrenner | +1.87 | 1479 |
| 26 | OG Anunoby | +1.84 | 2224 |
| 27 | Jaylin Williams | +1.81 | 1277 |
| 28 | Ryan Dunn | +1.80 | 1355 |
| 29 | Luke Kornet | +1.77 | 1430 |
| 30 | Scottie Barnes | +1.76 | 2681 |
| 31 | Mitchell Robinson | +1.75 | 1175 |
| 32 | Collin Gillespie | +1.73 | 2282 |
| 33 | Sidy Cissoko | +1.72 | 1435 |
| 34 | Sion James | +1.64 | 1843 |
| 35 | Jarrett Allen | +1.63 | 1519 |
| 36 | Oso Ighodaro | +1.61 | 1808 |
| 37 | Naz Reid | +1.61 | 2007 |
| 38 | Nikola Jokić | +1.58 | 2265 |
| 39 | Evan Mobley | +1.55 | 2074 |
| 40 | Jalen Smith | +1.47 | 1095 |
| 41 | Nickeil Alexander-Walker | +1.46 | 2603 |
| 42 | Collin Murray-Boyles | +1.44 | 1246 |
| 43 | Brandon Miller | +1.40 | 1968 |
| 44 | Brook Lopez | +1.38 | 1635 |
| 45 | John Konchar | +1.37 | 1115 |
| 46 | Jusuf Nurkić | +1.37 | 1083 |
| 47 | Landry Shamet | +1.34 | 1171 |
| 48 | Stephon Castle | +1.29 | 2038 |
| 49 | Jalen Duren | +1.26 | 1976 |
| 50 | Jaren Jackson Jr. | +1.25 | 1455 |

## 2025-26 Playoffs — defense, top 50 (projected, no truth)

> pool 112 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jose Alvarado | +5.29 | 170 |
| 2 | Victor Wembanyama | +5.04 | 750 |
| 3 | Karl-Anthony Towns | +4.28 | 578 |
| 4 | Cason Wallace | +4.25 | 374 |
| 5 | Ajay Mitchell | +3.98 | 317 |
| 6 | Alex Caruso | +3.98 | 353 |
| 7 | Tari Eason | +3.91 | 195 |
| 8 | Amen Thompson | +3.74 | 264 |
| 9 | Ausar Thompson | +3.46 | 427 |
| 10 | Josh Hart | +3.38 | 614 |
| 11 | Neemias Queta | +3.37 | 152 |
| 12 | Javonte Green | +3.24 | 132 |
| 13 | Alperen Sengun | +3.12 | 232 |
| 14 | Jaylin Williams | +2.72 | 240 |
| 15 | OG Anunoby | +2.50 | 586 |
| 16 | Marcus Smart | +2.39 | 345 |
| 17 | Toumani Camara | +2.13 | 165 |
| 18 | Jakob Poeltl | +2.06 | 134 |
| 19 | Rudy Gobert | +2.04 | 372 |
| 20 | Dylan Harper | +2.02 | 615 |
| 21 | VJ Edgecombe | +1.81 | 407 |
| 22 | Mikal Bridges | +1.69 | 608 |
| 23 | Devin Vassell | +1.61 | 801 |
| 24 | Isaiah Stewart | +1.57 | 165 |
| 25 | Dean Wade | +1.49 | 407 |
| 26 | Jarrett Allen | +1.43 | 529 |
| 27 | Nikola Jokić | +1.42 | 237 |
| 28 | De'Aaron Fox | +1.38 | 704 |
| 29 | Anthony Black | +1.38 | 196 |
| 30 | Luke Kornet | +1.37 | 296 |
| 31 | Jabari Smith Jr. | +1.29 | 252 |
| 32 | Jaden McDaniels | +1.19 | 406 |
| 33 | Julian Champagnie | +1.12 | 705 |
| 34 | Jaylen Brown | +1.11 | 249 |
| 35 | Mike Conley | +1.02 | 168 |
| 36 | Dyson Daniels | +0.91 | 166 |
| 37 | Collin Murray-Boyles | +0.90 | 191 |
| 38 | Christian Braun | +0.87 | 187 |
| 39 | Mitchell Robinson | +0.83 | 251 |
| 40 | Jaxson Hayes | +0.77 | 163 |
| 41 | Ja'Kobe Walter | +0.76 | 224 |
| 42 | Paolo Banchero | +0.72 | 273 |
| 43 | Chet Holmgren | +0.71 | 459 |
| 44 | Isaiah Hartenstein | +0.60 | 350 |
| 45 | James Harden | +0.56 | 672 |
| 46 | Scottie Barnes | +0.45 | 273 |
| 47 | Jamal Shead | +0.43 | 224 |
| 48 | Reed Sheppard | +0.38 | 192 |
| 49 | Evan Mobley | +0.23 | 640 |
| 50 | Paul George | +0.22 | 394 |
