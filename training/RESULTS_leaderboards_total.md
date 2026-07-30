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
| total | 2013-14 | Playoffs | 99 | +0.398 | +0.274 | 25/30 | 12.0 |
| total | 2013-14 | Regular season | 247 | +0.366 | +0.334 | 22/30 | 14.0 |
| total | 2014-15 | Playoffs | 99 | +0.421 | +0.290 | 21/30 | 11.9 |
| total | 2014-15 | Regular season | 247 | +0.623 | +0.388 | 22/30 | 13.9 |

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
