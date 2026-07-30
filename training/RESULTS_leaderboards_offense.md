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
| offense | stage2-0 / l2 | blend | 1183 | 0.652 | +0.874 | +0.930 |

Selected by cross-validated MAE inside the training rows; the test seasons are
used once, for the tables below. Every non-test row trains — no season is held
out for validation (see RESULTS_trainonly.md: it costs nothing).

Minutes floor for the 2023-26 boards: Regular season 1065, Playoffs 131 — the lowest 538 itself ever rated in that split.

## Kendall tau over the top 30, held-out seasons

`tau(true30)` compares the true order of the true top 30 against their
projected order. `tau(union30)` widens the set to the union of the true and
projected top 30, so it also penalises wrongly promoted players.

| target | season | split | pool | tau(true30) | tau(union30) | hits@30 | mean &#124;Δrank&#124; |
|---|---|---|---|---|---|---|---|
| offense | 2013-14 | Playoffs | 99 | +0.646 | +0.640 | 26/30 | 7.5 |
| offense | 2013-14 | Regular season | 247 | +0.761 | +0.660 | 24/30 | 11.1 |
| offense | 2014-15 | Playoffs | 99 | +0.632 | +0.550 | 25/30 | 8.0 |
| offense | 2014-15 | Regular season | 247 | +0.646 | +0.559 | 23/30 | 9.9 |

## 2013-14 Playoffs — offense, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.646 &nbsp;·&nbsp; hits@30 26/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +8.05 | +10.60 | 1 | +0 | Chris Paul | +10.60 |
| 2 | Damian Lillard | +7.15 | +8.00 | 3 | +1 | Stephen Curry | +9.20 |
| 3 | Stephen Curry | +6.69 | +9.20 | 2 | -1 | Damian Lillard | +8.00 |
| 4 | Russell Westbrook | +6.65 | +6.20 | 6 | +2 | James Harden | +8.00 |
| 5 | LeBron James | +6.22 | +6.90 | 5 | +0 | LeBron James | +6.90 |
| 6 | James Harden | +6.07 | +8.00 | 4 | -2 | Russell Westbrook | +6.20 |
| 7 | Kevin Durant | +5.73 | +5.10 | 7 | +0 | Kevin Durant | +5.10 |
| 8 | Manu Ginobili | +5.05 | +4.40 | 10 | +2 | Jose Calderon | +5.10 |
| 9 | LaMarcus Aldridge | +4.27 | +3.10 | 19 | +10 | Joe Johnson | +4.90 |
| 10 | Kyle Lowry | +4.09 | +3.80 | 11 | +1 | Manu Ginobili | +4.40 |
| 11 | Joe Johnson | +3.71 | +4.90 | 9 | -2 | Kyle Lowry | +3.80 |
| 12 | Patrick Patterson | +3.57 | +2.20 | 29 | +17 | Blake Griffin | +3.70 |
| 13 | DeMar DeRozan | +3.25 | +3.40 | 14 | +1 | Patty Mills | +3.50 |
| 14 | Devin Harris | +3.18 | +3.30 | 15 | +1 | DeMar DeRozan | +3.40 |
| 15 | Jose Calderon | +3.11 | +5.10 | 8 | -7 | Devin Harris | +3.30 |
| 16 | Patty Mills | +3.08 | +3.50 | 13 | -3 | JJ Redick | +3.30 |
| 17 | Blake Griffin | +2.83 | +3.70 | 12 | -5 | Mirza Teletovic | +3.20 |
| 18 | Jamal Crawford | +2.66 | +2.90 | 24 | +6 | Ray Allen | +3.20 |
| 19 | Vince Carter | +2.54 | +3.10 | 20 | +1 | LaMarcus Aldridge | +3.10 |
| 20 | Danny Green | +2.45 | +2.90 | 23 | +3 | Vince Carter | +3.10 |
| 21 | Draymond Green | +2.43 | +2.80 | 25 | +4 | Trevor Ariza | +3.10 |
| 22 | Mirza Teletovic | +2.43 | +3.20 | 17 | -5 | Bradley Beal | +3.00 |
| 23 | Deron Williams | +2.41 | +2.80 | 26 | +3 | Danny Green | +2.90 |
| 24 | Bradley Beal | +2.23 | +3.00 | 22 | -2 | Jamal Crawford | +2.90 |
| 25 | Kawhi Leonard | +2.21 | +1.80 | 32 | +7 | Draymond Green | +2.80 |
| 26 | Nicolas Batum | +2.18 | +1.50 | 35 | +9 | Deron Williams | +2.80 |
| 27 | Trevor Ariza | +1.93 | +3.10 | 21 | -6 | Kyle Korver | +2.60 |
| 28 | Greivis Vasquez | +1.89 | +1.30 | 39 | +11 | Tony Allen | +2.50 |
| 29 | JJ Redick | +1.87 | +3.30 | 16 | -13 | Patrick Patterson | +2.20 |
| 30 | Boris Diaw | +1.72 | +2.00 | 31 | +1 | David West | +2.10 |
| 31 | Serge Ibaka | +1.70 | +1.20 | 40 | +9 | Boris Diaw | +2.00 |
| 32 | David West | +1.70 | +2.10 | 30 | -2 | Kawhi Leonard | +1.80 |
| 33 | Kyle Korver | +1.63 | +2.60 | 27 | -6 | Tim Duncan | +1.80 |
| 34 | Ray Allen | +1.58 | +3.20 | 18 | -16 | Dwight Howard | +1.60 |
| 35 | Andre Iguodala | +1.43 | -1.40 | 74 | +39 | Nicolas Batum | +1.50 |
| 36 | George Hill | +1.28 | -0.70 | 65 | +29 | Shane Battier | +1.50 |
| 37 | Tim Duncan | +1.15 | +1.80 | 33 | -4 | Chandler Parsons | +1.40 |
| 38 | Tony Parker | +1.14 | +0.60 | 49 | +11 | Marcin Gortat | +1.30 |
| 39 | Dwight Howard | +1.05 | +1.60 | 34 | -5 | Greivis Vasquez | +1.30 |
| 40 | Tony Allen | +1.00 | +2.50 | 28 | -12 | Serge Ibaka | +1.20 |
| 41 | Jeremy Lin | +0.85 | +0.00 | 54 | +13 | Chris Bosh | +1.10 |
| 42 | Lance Stephenson | +0.78 | -0.60 | 64 | +22 | Mario Chalmers | +1.10 |
| 43 | Tiago Splitter | +0.75 | +0.50 | 50 | +7 | Courtney Lee | +0.90 |
| 44 | Chandler Parsons | +0.71 | +1.40 | 37 | -7 | Andray Blatche | +0.80 |
| 45 | David Lee | +0.66 | -1.50 | 75 | +30 | Paul Millsap | +0.80 |
| 46 | Mike Conley | +0.65 | +0.60 | 46 | +0 | Mike Conley | +0.60 |
| 47 | Shane Battier | +0.62 | +1.50 | 36 | -11 | Rashard Lewis | +0.60 |
| 48 | Reggie Jackson | +0.57 | -0.60 | 63 | +15 | Chris Andersen | +0.60 |
| 49 | John Wall | +0.56 | -0.10 | 55 | +6 | Tony Parker | +0.60 |
| 50 | Andray Blatche | +0.52 | +0.80 | 44 | -6 | Tiago Splitter | +0.50 |

## 2013-14 Regular season — offense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.761 &nbsp;·&nbsp; hits@30 24/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Kevin Durant | +7.04 | +7.60 | 1 | +0 | Kevin Durant | +7.60 |
| 2 | LeBron James | +6.74 | +5.80 | 4 | +2 | Chris Paul | +7.10 |
| 3 | James Harden | +6.19 | +6.30 | 3 | +0 | James Harden | +6.30 |
| 4 | Chris Paul | +6.06 | +7.10 | 2 | -2 | LeBron James | +5.80 |
| 5 | Kevin Love | +5.04 | +5.70 | 5 | +0 | Kevin Love | +5.70 |
| 6 | Goran Dragic | +4.84 | +4.80 | 6 | +0 | Goran Dragic | +4.80 |
| 7 | Russell Westbrook | +4.23 | +3.30 | 15 | +8 | Kyle Lowry | +4.40 |
| 8 | Damian Lillard | +4.04 | +3.60 | 12 | +4 | Dirk Nowitzki | +4.40 |
| 9 | Kyle Lowry | +4.04 | +4.40 | 7 | -2 | Carmelo Anthony | +4.20 |
| 10 | Manu Ginobili | +3.72 | +4.00 | 10 | +0 | Manu Ginobili | +4.00 |
| 11 | Isaiah Thomas | +3.67 | +3.50 | 14 | +3 | Patty Mills | +3.90 |
| 12 | Carmelo Anthony | +3.28 | +4.20 | 9 | -3 | Damian Lillard | +3.60 |
| 13 | Dirk Nowitzki | +3.12 | +4.40 | 8 | -5 | Mike Conley | +3.50 |
| 14 | Mike Conley | +3.05 | +3.50 | 13 | -1 | Isaiah Thomas | +3.50 |
| 15 | Blake Griffin | +2.92 | +2.90 | 17 | +2 | Russell Westbrook | +3.30 |
| 16 | Patty Mills | +2.78 | +3.90 | 11 | -5 | Ty Lawson | +3.20 |
| 17 | Paul George | +2.72 | +2.60 | 22 | +5 | Blake Griffin | +2.90 |
| 18 | Deron Williams | +2.39 | +2.60 | 23 | +5 | Marco Belinelli | +2.80 |
| 19 | DJ Augustin | +2.36 | +2.10 | 32 | +13 | Jamal Crawford | +2.80 |
| 20 | Jamal Crawford | +2.15 | +2.80 | 19 | -1 | Wesley Matthews | +2.80 |
| 21 | Ty Lawson | +2.08 | +3.20 | 16 | -5 | Joe Johnson | +2.70 |
| 22 | Ricky Rubio | +2.02 | +1.90 | 39 | +17 | Paul George | +2.60 |
| 23 | Joe Johnson | +2.01 | +2.70 | 21 | -2 | Deron Williams | +2.60 |
| 24 | Klay Thompson | +2.01 | +2.10 | 33 | +9 | Chandler Parsons | +2.60 |
| 25 | Kyrie Irving | +1.98 | +2.30 | 27 | +2 | Nick Young | +2.40 |
| 26 | Brandan Wright | +1.96 | +1.70 | 41 | +15 | Vince Carter | +2.40 |
| 27 | Wesley Matthews | +1.89 | +2.80 | 20 | -7 | Kyrie Irving | +2.30 |
| 28 | Chandler Parsons | +1.88 | +2.60 | 24 | -4 | Jrue Holiday | +2.20 |
| 29 | Andre Iguodala | +1.87 | +1.20 | 62 | +33 | Patrick Beverley | +2.20 |
| 30 | John Wall | +1.86 | +1.90 | 37 | +7 | Brandon Jennings | +2.20 |
| 31 | Nikola Pekovic | +1.85 | +1.10 | 68 | +37 | Randy Foye | +2.10 |
| 32 | Jrue Holiday | +1.74 | +2.20 | 28 | -4 | DJ Augustin | +2.10 |
| 33 | Marco Belinelli | +1.62 | +2.80 | 18 | -15 | Klay Thompson | +2.10 |
| 34 | Rudy Gay | +1.62 | +1.10 | 66 | +32 | Josh McRoberts | +2.00 |
| 35 | Mario Chalmers | +1.60 | +1.50 | 50 | +15 | Channing Frye | +2.00 |
| 36 | Patrick Beverley | +1.56 | +2.20 | 29 | -7 | Kyle Korver | +1.90 |
| 37 | Pablo Prigioni | +1.54 | +1.70 | 43 | +6 | John Wall | +1.90 |
| 38 | LaMarcus Aldridge | +1.53 | +1.10 | 67 | +29 | Nicolas Batum | +1.90 |
| 39 | DeMar DeRozan | +1.45 | +1.70 | 42 | +3 | Ricky Rubio | +1.90 |
| 40 | Darren Collison | +1.43 | +1.10 | 65 | +25 | JR Smith | +1.80 |
| 41 | Kevin Martin | +1.42 | +0.70 | 87 | +46 | Brandan Wright | +1.70 |
| 42 | Nicolas Batum | +1.41 | +1.90 | 38 | -4 | DeMar DeRozan | +1.70 |
| 43 | Nick Young | +1.41 | +2.40 | 25 | -18 | Pablo Prigioni | +1.70 |
| 44 | George Hill | +1.41 | +0.30 | 120 | +76 | Kawhi Leonard | +1.70 |
| 45 | JR Smith | +1.39 | +1.80 | 40 | -5 | DeMarcus Cousins | +1.70 |
| 46 | Josh McRoberts | +1.35 | +2.00 | 34 | -12 | Mirza Teletovic | +1.60 |
| 47 | Kemba Walker | +1.35 | +1.40 | 52 | +5 | Jose Calderon | +1.60 |
| 48 | Vince Carter | +1.29 | +2.40 | 26 | -22 | Eric Bledsoe | +1.50 |
| 49 | Randy Foye | +1.27 | +2.10 | 31 | -18 | Dwyane Wade | +1.50 |
| 50 | Kyle Korver | +1.26 | +1.90 | 36 | -14 | Mario Chalmers | +1.50 |

## 2014-15 Playoffs — offense, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.632 &nbsp;·&nbsp; hits@30 25/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | James Harden | +6.68 | +8.00 | 2 | +1 | Chris Paul | +8.70 |
| 2 | Chris Paul | +6.57 | +8.70 | 1 | -1 | James Harden | +8.00 |
| 3 | Monta Ellis | +5.52 | +6.20 | 4 | +1 | CJ McCollum | +7.90 |
| 4 | CJ McCollum | +5.23 | +7.90 | 3 | -1 | Monta Ellis | +6.20 |
| 5 | Jimmy Butler | +4.86 | +5.30 | 8 | +3 | Alan Anderson | +6.10 |
| 6 | Kyrie Irving | +4.36 | +4.10 | 15 | +9 | Stephen Curry | +5.70 |
| 7 | Stephen Curry | +4.34 | +5.70 | 6 | -1 | AlFarouq Aminu | +5.30 |
| 8 | Tim Duncan | +4.23 | +5.20 | 9 | +1 | Jimmy Butler | +5.30 |
| 9 | Mike Dunleavy | +3.92 | +4.70 | 11 | +2 | Tim Duncan | +5.20 |
| 10 | Alan Anderson | +3.81 | +6.10 | 5 | -5 | Vince Carter | +5.20 |
| 11 | DeMar DeRozan | +3.74 | +4.60 | 12 | +1 | Mike Dunleavy | +4.70 |
| 12 | AlFarouq Aminu | +3.73 | +5.30 | 7 | -5 | DeMar DeRozan | +4.60 |
| 13 | LeBron James | +3.48 | +3.60 | 19 | +6 | Eric Gordon | +4.50 |
| 14 | Derrick Rose | +3.42 | +2.80 | 27 | +13 | JJ Barea | +4.40 |
| 15 | Jarrett Jack | +3.38 | +3.80 | 16 | +1 | Kyrie Irving | +4.10 |
| 16 | Manu Ginobili | +3.01 | +3.60 | 18 | +2 | Jarrett Jack | +3.80 |
| 17 | Jeff Teague | +2.92 | +3.10 | 22 | +5 | Paul Pierce | +3.60 |
| 18 | Paul Pierce | +2.83 | +3.60 | 17 | -1 | Manu Ginobili | +3.60 |
| 19 | Blake Griffin | +2.49 | +3.50 | 20 | +1 | LeBron James | +3.60 |
| 20 | Dirk Nowitzki | +2.49 | +1.40 | 43 | +23 | Blake Griffin | +3.50 |
| 21 | Damian Lillard | +2.29 | +0.30 | 58 | +37 | Mike Conley | +3.30 |
| 22 | JJ Barea | +2.28 | +4.40 | 14 | -8 | Jeff Teague | +3.10 |
| 23 | Kyle Korver | +2.07 | +1.10 | 49 | +26 | JR Smith | +3.00 |
| 24 | DeMarre Carroll | +2.01 | +2.90 | 24 | +0 | DeMarre Carroll | +2.90 |
| 25 | John Wall | +2.00 | +1.40 | 44 | +19 | Paul Millsap | +2.90 |
| 26 | Mike Conley | +1.97 | +3.30 | 21 | -5 | Klay Thompson | +2.90 |
| 27 | Paul Millsap | +1.95 | +2.90 | 25 | -2 | Derrick Rose | +2.80 |
| 28 | Vince Carter | +1.91 | +5.20 | 10 | -18 | Bradley Beal | +2.60 |
| 29 | Boris Diaw | +1.87 | +2.00 | 38 | +9 | Josh Smith | +2.60 |
| 30 | Bradley Beal | +1.84 | +2.60 | 28 | -2 | Iman Shumpert | +2.60 |
| 31 | Marcin Gortat | +1.41 | +1.40 | 45 | +14 | Otto Porter Jr. | +2.50 |
| 32 | Josh Smith | +1.28 | +2.60 | 29 | -3 | OJ Mayo | +2.40 |
| 33 | Brook Lopez | +1.25 | +2.40 | 33 | +0 | Brook Lopez | +2.40 |
| 34 | Jamal Crawford | +1.18 | +1.60 | 40 | +6 | Courtney Lee | +2.30 |
| 35 | Kawhi Leonard | +1.12 | +1.40 | 46 | +11 | Danny Green | +2.30 |
| 36 | Andre Iguodala | +1.10 | +1.90 | 39 | +3 | Jason Terry | +2.10 |
| 37 | Otto Porter Jr. | +1.04 | +2.50 | 31 | -6 | Tristan Thompson | +2.10 |
| 38 | Klay Thompson | +1.02 | +2.90 | 26 | -12 | Boris Diaw | +2.00 |
| 39 | JR Smith | +0.98 | +3.00 | 23 | -16 | Andre Iguodala | +1.90 |
| 40 | Courtney Lee | +0.94 | +2.30 | 34 | -6 | Jamal Crawford | +1.60 |
| 41 | Trevor Ariza | +0.84 | +1.30 | 48 | +7 | DeAndre Jordan | +1.50 |
| 42 | Leandro Barbosa | +0.82 | +0.90 | 50 | +8 | Matt Barnes | +1.50 |
| 43 | Jason Terry | +0.82 | +2.10 | 36 | -7 | Dirk Nowitzki | +1.40 |
| 44 | Drew Gooden | +0.78 | +0.50 | 52 | +8 | John Wall | +1.40 |
| 45 | DeAndre Jordan | +0.44 | +1.50 | 41 | -4 | Marcin Gortat | +1.40 |
| 46 | Shaun Livingston | +0.21 | +0.20 | 60 | +14 | Kawhi Leonard | +1.40 |
| 47 | Beno Udrih | +0.20 | +1.30 | 47 | +0 | Beno Udrih | +1.30 |
| 48 | Pau Gasol | +0.15 | -0.10 | 63 | +15 | Trevor Ariza | +1.30 |
| 49 | Eric Gordon | +0.05 | +4.50 | 13 | -36 | Kyle Korver | +1.10 |
| 50 | Danny Green | +0.03 | +2.30 | 35 | -15 | Leandro Barbosa | +0.90 |

## 2014-15 Regular season — offense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.646 &nbsp;·&nbsp; hits@30 23/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.47 | +8.50 | 1 | +0 | Chris Paul | +8.50 |
| 2 | James Harden | +7.21 | +7.70 | 2 | +0 | James Harden | +7.70 |
| 3 | Russell Westbrook | +6.57 | +6.10 | 3 | +0 | Russell Westbrook | +6.10 |
| 4 | LeBron James | +5.93 | +5.30 | 5 | +1 | Kyrie Irving | +5.50 |
| 5 | Kyrie Irving | +5.36 | +5.50 | 4 | -1 | LeBron James | +5.30 |
| 6 | Lou Williams | +4.76 | +5.20 | 6 | +0 | Lou Williams | +5.20 |
| 7 | Isaiah Thomas | +4.67 | +4.50 | 8 | +1 | Kyle Korver | +4.60 |
| 8 | Damian Lillard | +4.24 | +4.00 | 11 | +3 | Isaiah Thomas | +4.50 |
| 9 | Klay Thompson | +3.99 | +4.30 | 10 | +1 | Anthony Davis | +4.30 |
| 10 | Blake Griffin | +3.94 | +3.20 | 22 | +12 | Klay Thompson | +4.30 |
| 11 | George Hill | +3.85 | +3.90 | 12 | +1 | Damian Lillard | +4.00 |
| 12 | Carmelo Anthony | +3.22 | +3.80 | 13 | +1 | George Hill | +3.90 |
| 13 | Jimmy Butler | +3.17 | +3.20 | 20 | +7 | Carmelo Anthony | +3.80 |
| 14 | Mike Conley | +3.02 | +2.40 | 32 | +18 | Ty Lawson | +3.80 |
| 15 | Kyle Korver | +2.93 | +4.60 | 7 | -8 | Kawhi Leonard | +3.70 |
| 16 | Anthony Davis | +2.87 | +4.30 | 9 | -7 | Rudy Gay | +3.50 |
| 17 | JJ Redick | +2.78 | +2.50 | 29 | +12 | DeAndre Jordan | +3.40 |
| 18 | Gordon Hayward | +2.76 | +3.20 | 21 | +3 | Kyle Lowry | +3.30 |
| 19 | Ty Lawson | +2.60 | +3.80 | 14 | -5 | Jrue Holiday | +3.30 |
| 20 | Brandon Jennings | +2.58 | +3.10 | 23 | +3 | Jimmy Butler | +3.20 |
| 21 | Jeff Teague | +2.51 | +2.20 | 35 | +14 | Gordon Hayward | +3.20 |
| 22 | Jrue Holiday | +2.48 | +3.30 | 19 | -3 | Blake Griffin | +3.20 |
| 23 | Kawhi Leonard | +2.40 | +3.70 | 15 | -8 | Brandon Jennings | +3.10 |
| 24 | Rudy Gay | +2.29 | +3.50 | 16 | -8 | Danny Green | +3.10 |
| 25 | John Wall | +2.28 | +2.10 | 39 | +14 | Danilo Gallinari | +2.80 |
| 26 | Dwyane Wade | +2.26 | +2.00 | 42 | +16 | Anthony Morrow | +2.70 |
| 27 | Aaron Brooks | +2.23 | +1.60 | 56 | +29 | Chandler Parsons | +2.60 |
| 28 | Reggie Jackson | +2.23 | +2.00 | 41 | +13 | Tyreke Evans | +2.60 |
| 29 | LaMarcus Aldridge | +2.09 | +2.40 | 30 | +1 | JJ Redick | +2.50 |
| 30 | Khris Middleton | +2.07 | +1.70 | 51 | +21 | LaMarcus Aldridge | +2.40 |
| 31 | Kyle Lowry | +2.05 | +3.30 | 18 | -13 | Patrick Patterson | +2.40 |
| 32 | Danilo Gallinari | +2.02 | +2.80 | 25 | -7 | Mike Conley | +2.40 |
| 33 | Dirk Nowitzki | +2.02 | +2.20 | 34 | +1 | Wesley Matthews | +2.30 |
| 34 | Tyreke Evans | +1.95 | +2.60 | 28 | -6 | Dirk Nowitzki | +2.20 |
| 35 | Darren Collison | +1.89 | +1.70 | 55 | +20 | Jeff Teague | +2.20 |
| 36 | Anthony Morrow | +1.88 | +2.70 | 26 | -10 | Gerald Green | +2.20 |
| 37 | Gerald Green | +1.84 | +2.20 | 36 | -1 | Devin Harris | +2.10 |
| 38 | Danny Green | +1.78 | +3.10 | 24 | -14 | JR Smith | +2.10 |
| 39 | Patrick Patterson | +1.78 | +2.40 | 31 | -8 | John Wall | +2.10 |
| 40 | Chandler Parsons | +1.58 | +2.60 | 27 | -13 | Ersan Ilyasova | +2.10 |
| 41 | Eric Gordon | +1.50 | +0.50 | 89 | +48 | Reggie Jackson | +2.00 |
| 42 | Wesley Matthews | +1.43 | +2.30 | 33 | -9 | Dwyane Wade | +2.00 |
| 43 | DeAndre Jordan | +1.40 | +3.40 | 17 | -26 | DeMarre Carroll | +1.90 |
| 44 | Jamal Crawford | +1.38 | +1.10 | 69 | +25 | Nikola Mirotic | +1.90 |
| 45 | Manu Ginobili | +1.34 | +1.70 | 54 | +9 | Goran Dragic | +1.90 |
| 46 | Draymond Green | +1.26 | +1.50 | 58 | +12 | JJ Barea | +1.90 |
| 47 | Kevin Love | +1.24 | +1.70 | 52 | +5 | Joe Johnson | +1.80 |
| 48 | Ed Davis | +1.23 | +1.20 | 64 | +16 | Luol Deng | +1.80 |
| 49 | Bradley Beal | +1.16 | +0.80 | 78 | +29 | Jae Crowder | +1.80 |
| 50 | Marc Gasol | +1.15 | +1.40 | 60 | +10 | Eric Bledsoe | +1.70 |

## 2023-24 Regular season — offense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +7.88 | 2737 |
| 2 | Luka Doncic | +7.78 | 2624 |
| 3 | Jalen Brunson | +7.04 | 2726 |
| 4 | Shai Gilgeous-Alexander | +6.75 | 2553 |
| 5 | Tyrese Haliburton | +6.01 | 2224 |
| 6 | Stephen Curry | +5.89 | 2421 |
| 7 | Donovan Mitchell | +5.70 | 1943 |
| 8 | Damian Lillard | +5.27 | 2579 |
| 9 | Devin Booker | +5.20 | 2447 |
| 10 | Giannis Antetokounmpo | +5.19 | 2567 |
| 11 | LeBron James | +5.17 | 2504 |
| 12 | Trae Young | +5.09 | 1942 |
| 13 | Kyrie Irving | +5.02 | 2030 |
| 14 | Jamal Murray | +4.79 | 1861 |
| 15 | Jayson Tatum | +4.67 | 2645 |
| 16 | James Harden | +4.61 | 2470 |
| 17 | Joel Embiid | +4.55 | 1309 |
| 18 | Anthony Edwards | +4.54 | 2770 |
| 19 | De'Aaron Fox | +4.53 | 2659 |
| 20 | Tyrese Maxey | +4.36 | 2626 |
| 21 | Paul George | +4.01 | 2502 |
| 22 | Kawhi Leonard | +3.99 | 2330 |
| 23 | Collin Sexton | +3.81 | 2075 |
| 24 | Desmond Bane | +3.67 | 1443 |
| 25 | Lauri Markkanen | +3.56 | 1820 |
| 26 | Kevin Durant | +3.52 | 2791 |
| 27 | Fred VanVleet | +3.49 | 2684 |
| 28 | Jimmy Butler | +3.47 | 2042 |
| 29 | T.J. McConnell | +3.36 | 1291 |
| 30 | DeMar DeRozan | +3.28 | 2989 |
| 31 | CJ McCollum | +3.18 | 2159 |
| 32 | Payton Pritchard | +3.04 | 1826 |
| 33 | Julius Randle | +2.96 | 1630 |
| 34 | D'Angelo Russell | +2.88 | 2484 |
| 35 | Donte DiVincenzo | +2.87 | 2360 |
| 36 | Anfernee Simons | +2.84 | 1582 |
| 37 | Dejounte Murray | +2.78 | 2783 |
| 38 | Malcolm Brogdon | +2.71 | 1121 |
| 39 | Pascal Siakam | +2.58 | 2658 |
| 40 | Mike Conley | +2.57 | 2193 |
| 41 | Zion Williamson | +2.51 | 2207 |
| 42 | Jalen Green | +2.48 | 2602 |
| 43 | Malik Monk | +2.46 | 1872 |
| 44 | Terry Rozier | +2.38 | 2040 |
| 45 | Khris Middleton | +2.34 | 1487 |
| 46 | Michael Porter Jr. | +2.32 | 2565 |
| 47 | Immanuel Quickley | +2.30 | 1985 |
| 48 | Jalen Williams | +2.26 | 2223 |
| 49 | Anthony Davis | +2.23 | 2700 |
| 50 | Brandon Ingram | +2.21 | 2103 |

## 2023-24 Playoffs — offense, top 50 (projected, no truth)

> pool 103 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +7.76 | 482 |
| 2 | Joel Embiid | +6.77 | 248 |
| 3 | Jalen Brunson | +6.08 | 518 |
| 4 | Luka Doncic | +5.93 | 900 |
| 5 | Devin Booker | +5.75 | 166 |
| 6 | Kyrie Irving | +5.53 | 879 |
| 7 | Tyrese Haliburton | +5.53 | 522 |
| 8 | Kevin Durant | +5.24 | 168 |
| 9 | Damian Lillard | +5.18 | 156 |
| 10 | Anthony Edwards | +5.07 | 649 |
| 11 | Austin Reaves | +5.05 | 174 |
| 12 | Shai Gilgeous-Alexander | +5.01 | 399 |
| 13 | Khris Middleton | +5.01 | 230 |
| 14 | LeBron James | +4.94 | 204 |
| 15 | Tyrese Maxey | +4.89 | 267 |
| 16 | Jayson Tatum | +4.69 | 768 |
| 17 | Donovan Mitchell | +4.67 | 382 |
| 18 | Anthony Davis | +4.01 | 208 |
| 19 | Andrew Nembhard | +3.60 | 554 |
| 20 | Pascal Siakam | +3.45 | 603 |
| 21 | James Harden | +3.26 | 242 |
| 22 | Derrick White | +3.09 | 676 |
| 23 | Paul George | +2.99 | 222 |
| 24 | Jalen Williams | +2.79 | 377 |
| 25 | T.J. McConnell | +2.78 | 348 |
| 26 | Mike Conley | +2.73 | 474 |
| 27 | Myles Turner | +2.68 | 550 |
| 28 | Donte DiVincenzo | +2.67 | 466 |
| 29 | Kyle Lowry | +2.43 | 175 |
| 30 | Jaden McDaniels | +2.13 | 537 |
| 31 | Jaylen Brown | +2.05 | 707 |
| 32 | Michael Porter Jr. | +1.89 | 443 |
| 33 | Sam Hauser | +1.73 | 283 |
| 34 | Aaron Gordon | +1.64 | 445 |
| 35 | Jrue Holiday | +1.61 | 720 |
| 36 | Rudy Gobert | +1.57 | 512 |
| 37 | Al Horford | +1.56 | 575 |
| 38 | Patrick Beverley | +1.55 | 210 |
| 39 | Kelly Oubre Jr. | +1.49 | 224 |
| 40 | Kristaps Porzingis | +1.37 | 165 |
| 41 | Chet Holmgren | +1.32 | 345 |
| 42 | Kentavious Caldwell-Pope | +1.16 | 420 |
| 43 | Karl-Anthony Towns | +1.12 | 522 |
| 44 | Dereck Lively II | +1.06 | 462 |
| 45 | Bobby Portis | +1.01 | 187 |
| 46 | Jamal Murray | +0.96 | 462 |
| 47 | Ivica Zubac | +0.90 | 192 |
| 48 | Brook Lopez | +0.89 | 200 |
| 49 | Bam Adebayo | +0.86 | 192 |
| 50 | Paolo Banchero | +0.77 | 262 |

## 2024-25 Regular season — offense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.04 | 2571 |
| 2 | Shai Gilgeous-Alexander | +7.94 | 2598 |
| 3 | Stephen Curry | +6.89 | 2252 |
| 4 | Luka Dončić | +6.24 | 1769 |
| 5 | Jalen Brunson | +5.74 | 2301 |
| 6 | LaMelo Ball | +5.43 | 1505 |
| 7 | Giannis Antetokounmpo | +5.42 | 2289 |
| 8 | Jayson Tatum | +5.37 | 2624 |
| 9 | James Harden | +5.29 | 2789 |
| 10 | Damian Lillard | +5.22 | 2093 |
| 11 | Donovan Mitchell | +5.05 | 2232 |
| 12 | Tyrese Haliburton | +4.97 | 2451 |
| 13 | Darius Garland | +4.90 | 2301 |
| 14 | Ty Jerome | +4.90 | 1393 |
| 15 | Trae Young | +4.87 | 2739 |
| 16 | Tyler Herro | +4.51 | 2725 |
| 17 | Cade Cunningham | +4.34 | 2452 |
| 18 | Anthony Edwards | +4.23 | 2871 |
| 19 | Jamal Murray | +4.11 | 2418 |
| 20 | Austin Reaves | +3.84 | 2550 |
| 21 | Ja Morant | +3.63 | 1519 |
| 22 | Kyrie Irving | +3.60 | 1804 |
| 23 | Payton Pritchard | +3.52 | 2271 |
| 24 | Tyrese Maxey | +3.46 | 1960 |
| 25 | Devin Booker | +3.35 | 2795 |
| 26 | Jimmy Butler | +3.31 | 1746 |
| 27 | Karl-Anthony Towns | +3.21 | 2517 |
| 28 | Christian Braun | +3.02 | 2675 |
| 29 | Franz Wagner | +3.01 | 2023 |
| 30 | Kevin Durant | +2.95 | 2265 |
| 31 | Norman Powell | +2.78 | 1958 |
| 32 | LeBron James | +2.72 | 2444 |
| 33 | Paolo Banchero | +2.57 | 1582 |
| 34 | Cameron Johnson | +2.53 | 1800 |
| 35 | Zach LaVine | +2.51 | 2603 |
| 36 | Isaiah Joe | +2.49 | 1604 |
| 37 | Collin Sexton | +2.42 | 1758 |
| 38 | DeMar DeRozan | +2.42 | 2768 |
| 39 | Jalen Green | +2.42 | 2697 |
| 40 | Jaylen Brown | +2.41 | 2158 |
| 41 | Kawhi Leonard | +2.38 | 1180 |
| 42 | Domantas Sabonis | +2.30 | 2429 |
| 43 | Derrick White | +2.26 | 2574 |
| 44 | Deni Avdija | +2.26 | 2161 |
| 45 | Desmond Bane | +2.23 | 2205 |
| 46 | Coby White | +2.10 | 2450 |
| 47 | CJ McCollum | +2.09 | 1832 |
| 48 | Aaron Gordon | +2.00 | 1447 |
| 49 | Aaron Nesmith | +1.95 | 1123 |
| 50 | Malik Beasley | +1.95 | 2283 |

## 2024-25 Playoffs — offense, top 50 (projected, no truth)

> pool 109 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jalen Brunson | +8.23 | 680 |
| 2 | Luka Dončić | +7.09 | 208 |
| 3 | Giannis Antetokounmpo | +7.02 | 188 |
| 4 | Donovan Mitchell | +6.35 | 288 |
| 5 | Nikola Jokić | +6.34 | 563 |
| 6 | Fred VanVleet | +6.11 | 280 |
| 7 | Shai Gilgeous-Alexander | +6.05 | 851 |
| 8 | Stephen Curry | +5.92 | 281 |
| 9 | Jayson Tatum | +5.40 | 322 |
| 10 | Tyrese Haliburton | +5.38 | 772 |
| 11 | LeBron James | +5.36 | 204 |
| 12 | Kawhi Leonard | +4.52 | 265 |
| 13 | Payton Pritchard | +4.47 | 302 |
| 14 | Davion Mitchell | +4.32 | 142 |
| 15 | Max Strus | +4.28 | 253 |
| 16 | Gary Trent Jr. | +4.27 | 171 |
| 17 | Anthony Edwards | +3.91 | 585 |
| 18 | Dennis Schröder | +3.81 | 164 |
| 19 | Isaiah Joe | +3.76 | 211 |
| 20 | Jamal Murray | +3.66 | 578 |
| 21 | Julius Randle | +3.52 | 533 |
| 22 | Aaron Gordon | +3.46 | 522 |
| 23 | AJ Green | +3.43 | 135 |
| 24 | Derrick White | +3.34 | 415 |
| 25 | Jalen Williams | +3.04 | 796 |
| 26 | Jimmy Butler III | +2.93 | 397 |
| 27 | Ty Jerome | +2.92 | 191 |
| 28 | Paolo Banchero | +2.72 | 197 |
| 29 | Aaron Nesmith | +2.71 | 650 |
| 30 | Amen Thompson | +2.66 | 231 |
| 31 | James Harden | +2.66 | 276 |
| 32 | Alperen Sengun | +2.62 | 256 |
| 33 | Darius Garland | +2.59 | 148 |
| 34 | Pascal Siakam | +2.49 | 771 |
| 35 | Cade Cunningham | +2.31 | 248 |
| 36 | Evan Mobley | +2.17 | 257 |
| 37 | Jarrett Allen | +2.10 | 261 |
| 38 | Sam Merrill | +2.08 | 159 |
| 39 | Dillon Brooks | +2.08 | 206 |
| 40 | Jaden McDaniels | +2.07 | 497 |
| 41 | Buddy Hield | +1.99 | 327 |
| 42 | Andrew Nembhard | +1.98 | 769 |
| 43 | Steven Adams | +1.81 | 155 |
| 44 | Ivica Zubac | +1.77 | 256 |
| 45 | Alex Caruso | +1.68 | 562 |
| 46 | Luke Kornet | +1.66 | 180 |
| 47 | Franz Wagner | +1.59 | 195 |
| 48 | Bam Adebayo | +1.30 | 153 |
| 49 | De'Andre Hunter | +1.19 | 185 |
| 50 | Jalen Duren | +1.15 | 203 |

## 2025-26 Regular season — offense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +8.50 | 2265 |
| 2 | Shai Gilgeous-Alexander | +7.87 | 2259 |
| 3 | Luka Dončić | +7.17 | 2289 |
| 4 | Donovan Mitchell | +6.46 | 2342 |
| 5 | Kawhi Leonard | +6.33 | 2085 |
| 6 | James Harden | +6.22 | 2438 |
| 7 | Jamal Murray | +6.08 | 2652 |
| 8 | Jalen Brunson | +5.83 | 2590 |
| 9 | Stephen Curry | +5.71 | 1329 |
| 10 | LaMelo Ball | +5.60 | 2017 |
| 11 | Cade Cunningham | +5.21 | 2172 |
| 12 | Tyrese Maxey | +5.12 | 2661 |
| 13 | Jimmy Butler III | +4.81 | 1182 |
| 14 | Devin Booker | +4.75 | 2146 |
| 15 | Deni Avdija | +4.61 | 2199 |
| 16 | Anthony Edwards | +4.51 | 2137 |
| 17 | Kevin Durant | +4.22 | 2840 |
| 18 | Payton Pritchard | +3.97 | 2556 |
| 19 | Coby White | +3.91 | 1250 |
| 20 | Austin Reaves | +3.84 | 1762 |
| 21 | Michael Porter Jr. | +3.79 | 1689 |
| 22 | Jaylen Brown | +3.77 | 2443 |
| 23 | Jrue Holiday | +3.73 | 1560 |
| 24 | Victor Wembanyama | +3.64 | 1866 |
| 25 | Joel Embiid | +3.45 | 1201 |
| 26 | Jalen Duren | +3.41 | 1976 |
| 27 | Keyonte George | +3.38 | 1786 |
| 28 | Duncan Robinson | +3.00 | 2113 |
| 29 | De'Aaron Fox | +2.98 | 2231 |
| 30 | Lauri Markkanen | +2.92 | 1443 |
| 31 | Kon Knueppel | +2.89 | 2551 |
| 32 | Darius Garland | +2.88 | 1344 |
| 33 | Cam Spencer | +2.77 | 1714 |
| 34 | Collin Gillespie | +2.76 | 2282 |
| 35 | Anfernee Simons | +2.75 | 1372 |
| 36 | Brandon Miller | +2.73 | 1968 |
| 37 | Alperen Sengun | +2.71 | 2398 |
| 38 | Grayson Allen | +2.57 | 1467 |
| 39 | Nickeil Alexander-Walker | +2.56 | 2603 |
| 40 | Julius Randle | +2.55 | 2610 |
| 41 | Ryan Rollins | +2.55 | 2375 |
| 42 | Isaiah Joe | +2.52 | 1507 |
| 43 | Reed Sheppard | +2.46 | 2147 |
| 44 | Luka Garza | +2.45 | 1118 |
| 45 | CJ McCollum | +2.37 | 2263 |
| 46 | Sam Merrill | +2.36 | 1377 |
| 47 | Trey Murphy III | +2.32 | 2341 |
| 48 | Bones Hyland | +2.27 | 1177 |
| 49 | Jalen Johnson | +2.27 | 2532 |
| 50 | Pascal Siakam | +2.22 | 2057 |

## 2025-26 Playoffs — offense, top 50 (projected, no truth)

> pool 112 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Jayson Tatum | +7.83 | 218 |
| 2 | Jalen Brunson | +6.10 | 702 |
| 3 | Dillon Brooks | +5.79 | 149 |
| 4 | Tyrese Maxey | +4.83 | 437 |
| 5 | Ajay Mitchell | +4.70 | 317 |
| 6 | Payton Pritchard | +4.69 | 231 |
| 7 | RJ Barrett | +4.48 | 271 |
| 8 | Karl-Anthony Towns | +4.43 | 578 |
| 9 | Cade Cunningham | +4.33 | 572 |
| 10 | Scottie Barnes | +4.12 | 273 |
| 11 | James Harden | +3.96 | 672 |
| 12 | Shai Gilgeous-Alexander | +3.80 | 544 |
| 13 | Nikola Jokić | +3.49 | 237 |
| 14 | Joel Embiid | +3.43 | 233 |
| 15 | Alex Caruso | +3.41 | 353 |
| 16 | Jalen Green | +3.39 | 151 |
| 17 | Naz Reid | +3.14 | 323 |
| 18 | Paolo Banchero | +3.12 | 273 |
| 19 | Mike Conley | +2.95 | 168 |
| 20 | Victor Wembanyama | +2.93 | 750 |
| 21 | Sam Merrill | +2.86 | 338 |
| 22 | Duncan Robinson | +2.80 | 383 |
| 23 | Donovan Mitchell | +2.78 | 652 |
| 24 | Paul George | +2.74 | 394 |
| 25 | OG Anunoby | +2.73 | 586 |
| 26 | Chet Holmgren | +2.47 | 459 |
| 27 | Amen Thompson | +2.36 | 264 |
| 28 | Jarrett Allen | +2.23 | 529 |
| 29 | Tim Hardaway Jr. | +2.22 | 140 |
| 30 | Ayo Dosunmu | +2.21 | 292 |
| 31 | Dylan Harper | +2.13 | 615 |
| 32 | Tari Eason | +2.12 | 195 |
| 33 | Desmond Bane | +2.09 | 253 |
| 34 | Devin Booker | +2.06 | 153 |
| 35 | Jrue Holiday | +2.04 | 192 |
| 36 | Julian Champagnie | +2.01 | 705 |
| 37 | Jose Alvarado | +1.82 | 170 |
| 38 | Austin Reaves | +1.78 | 221 |
| 39 | Miles McBride | +1.75 | 334 |
| 40 | Onyeka Okongwu | +1.70 | 199 |
| 41 | Jalen Johnson | +1.69 | 214 |
| 42 | Wendell Carter Jr. | +1.65 | 237 |
| 43 | Jared McCain | +1.51 | 258 |
| 44 | LeBron James | +1.48 | 384 |
| 45 | Dean Wade | +1.45 | 407 |
| 46 | Caris LeVert | +1.43 | 216 |
| 47 | Tobias Harris | +1.41 | 485 |
| 48 | Cameron Johnson | +1.33 | 186 |
| 49 | Collin Murray-Boyles | +1.27 | 191 |
| 50 | Isaiah Joe | +1.25 | 143 |
