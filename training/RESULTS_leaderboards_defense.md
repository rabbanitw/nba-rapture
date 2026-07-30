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
| defense | stage2-0 / huber | blend | 649 | 1.008 | +0.657 | +0.819 |

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
| defense | 2013-14 | Playoffs | 99 | +0.310 | +0.105 | 20/30 | 18.9 |
| defense | 2013-14 | Regular season | 247 | +0.407 | +0.268 | 19/30 | 18.5 |
| defense | 2014-15 | Playoffs | 99 | +0.356 | +0.263 | 19/30 | 16.0 |
| defense | 2014-15 | Regular season | 247 | +0.600 | +0.462 | 22/30 | 16.7 |

## 2013-14 Playoffs — defense, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.310 &nbsp;·&nbsp; hits@30 20/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Danny Green | +4.01 | +3.50 | 19 | +18 | Draymond Green | +8.00 |
| 2 | Pero Antic | +3.87 | +6.50 | 3 | +1 | Paul Millsap | +7.60 |
| 3 | Joakim Noah | +3.73 | +5.30 | 8 | +5 | Pero Antic | +6.50 |
| 4 | Greivis Vasquez | +3.64 | +6.00 | 6 | +2 | Nick Collison | +6.10 |
| 5 | Kawhi Leonard | +3.50 | +4.40 | 12 | +7 | Andray Blatche | +6.10 |
| 6 | Marcin Gortat | +3.42 | +3.50 | 20 | +14 | Greivis Vasquez | +6.00 |
| 7 | Trevor Ariza | +3.24 | +2.40 | 26 | +19 | Chris Andersen | +5.40 |
| 8 | Paul Millsap | +2.97 | +7.60 | 2 | -6 | Joakim Noah | +5.30 |
| 9 | Kyle Korver | +2.96 | -0.40 | 62 | +53 | Vince Carter | +5.00 |
| 10 | Tiago Splitter | +2.90 | +5.00 | 10 | +0 | Tiago Splitter | +5.00 |
| 11 | Andray Blatche | +2.81 | +6.10 | 5 | -6 | Rashard Lewis | +4.90 |
| 12 | John Wall | +2.75 | +0.80 | 41 | +29 | Kawhi Leonard | +4.40 |
| 13 | Paul Pierce | +2.64 | +1.50 | 34 | +21 | Serge Ibaka | +4.20 |
| 14 | Mike Conley | +2.54 | -1.30 | 75 | +61 | Ian Mahinmi | +4.20 |
| 15 | Serge Ibaka | +2.45 | +4.20 | 13 | -2 | Zach Randolph | +4.20 |
| 16 | Kevin Garnett | +2.39 | +0.40 | 51 | +35 | Marc Gasol | +4.10 |
| 17 | Manu Ginobili | +2.36 | +3.40 | 21 | +4 | Deron Williams | +3.80 |
| 18 | George Hill | +2.34 | +2.80 | 23 | +5 | Patty Mills | +3.70 |
| 19 | David West | +2.32 | +1.00 | 39 | +20 | Danny Green | +3.50 |
| 20 | Vince Carter | +2.27 | +5.00 | 9 | -11 | Marcin Gortat | +3.50 |
| 21 | LeBron James | +2.24 | +0.80 | 42 | +21 | Manu Ginobili | +3.40 |
| 22 | Tony Allen | +2.20 | +1.80 | 31 | +9 | LaMarcus Aldridge | +2.90 |
| 23 | Chris Andersen | +2.20 | +5.40 | 7 | -16 | George Hill | +2.80 |
| 24 | Zach Randolph | +2.18 | +4.20 | 15 | -9 | Chris Paul | +2.70 |
| 25 | Patty Mills | +2.16 | +3.70 | 18 | -7 | Bradley Beal | +2.70 |
| 26 | Patrick Patterson | +2.09 | -0.80 | 64 | +38 | Trevor Ariza | +2.40 |
| 27 | Rashard Lewis | +1.95 | +4.90 | 11 | -16 | Nicolas Batum | +2.20 |
| 28 | Kevin Durant | +1.95 | +0.50 | 48 | +20 | James Harden | +1.90 |
| 29 | Draymond Green | +1.87 | +8.00 | 1 | -28 | Tim Duncan | +1.90 |
| 30 | Nene | +1.73 | +1.80 | 30 | +0 | Nene | +1.80 |
| 31 | Nick Collison | +1.70 | +6.10 | 4 | -27 | Tony Allen | +1.80 |
| 32 | Chris Paul | +1.61 | +2.70 | 24 | -8 | Kendrick Perkins | +1.80 |
| 33 | Alan Anderson | +1.61 | +0.50 | 47 | +14 | JJ Redick | +1.70 |
| 34 | Deron Williams | +1.48 | +3.80 | 17 | -17 | Paul Pierce | +1.50 |
| 35 | David Lee | +1.44 | -2.00 | 87 | +52 | Kirk Hinrich | +1.40 |
| 36 | Bradley Beal | +1.29 | +2.70 | 25 | -11 | Roy Hibbert | +1.30 |
| 37 | DeMarre Carroll | +1.28 | -2.50 | 89 | +52 | Thabo Sefolosha | +1.30 |
| 38 | Jeremy Lin | +1.26 | -1.00 | 72 | +34 | Stephen Curry | +1.20 |
| 39 | Lance Stephenson | +1.23 | -0.80 | 66 | +27 | David West | +1.00 |
| 40 | Dwyane Wade | +1.23 | -1.80 | 81 | +41 | DeAndre Jordan | +0.90 |
| 41 | Dwight Howard | +1.22 | +0.40 | 49 | +8 | John Wall | +0.80 |
| 42 | Derek Fisher | +1.22 | -1.30 | 74 | +32 | LeBron James | +0.80 |
| 43 | Tim Duncan | +1.16 | +1.90 | 29 | -14 | Boris Diaw | +0.80 |
| 44 | Kendrick Perkins | +1.09 | +1.80 | 32 | -12 | Blake Griffin | +0.80 |
| 45 | DeAndre Jordan | +1.04 | +0.90 | 40 | -5 | Reggie Jackson | +0.70 |
| 46 | Marc Gasol | +0.93 | +4.10 | 16 | -30 | Kyle Lowry | +0.50 |
| 47 | Joe Johnson | +0.86 | +0.40 | 50 | +3 | Alan Anderson | +0.50 |
| 48 | Matt Barnes | +0.86 | -2.20 | 88 | +40 | Kevin Durant | +0.50 |
| 49 | Mario Chalmers | +0.72 | -1.80 | 82 | +33 | Dwight Howard | +0.40 |
| 50 | Kyle Lowry | +0.71 | +0.50 | 46 | -4 | Joe Johnson | +0.40 |

## 2013-14 Regular season — defense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.407 &nbsp;·&nbsp; hits@30 19/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Andrew Bogut | +4.64 | +4.40 | 4 | +3 | Kawhi Leonard | +5.00 |
| 2 | Draymond Green | +4.10 | +4.60 | 2 | +0 | Draymond Green | +4.60 |
| 3 | Tiago Splitter | +3.87 | +4.20 | 6 | +3 | Joakim Noah | +4.50 |
| 4 | Kevin Garnett | +3.79 | +3.50 | 11 | +7 | Andrew Bogut | +4.40 |
| 5 | Paul George | +3.77 | +2.90 | 21 | +16 | Michael KiddGilchrist | +4.40 |
| 6 | Kawhi Leonard | +3.51 | +5.00 | 1 | -5 | Tiago Splitter | +4.20 |
| 7 | Nene | +3.41 | +3.80 | 9 | +2 | Danny Green | +4.00 |
| 8 | Andre Iguodala | +3.09 | +2.60 | 25 | +17 | Chris Paul | +3.90 |
| 9 | Anderson Varejao | +3.06 | +3.60 | 10 | +1 | Nene | +3.80 |
| 10 | Joakim Noah | +3.05 | +4.50 | 3 | -7 | Anderson Varejao | +3.60 |
| 11 | Tony Allen | +3.01 | +2.40 | 31 | +20 | Kevin Garnett | +3.50 |
| 12 | Jae Crowder | +2.99 | +3.00 | 19 | +7 | Nick Calathes | +3.50 |
| 13 | Danny Green | +2.99 | +4.00 | 7 | -6 | Ian Mahinmi | +3.50 |
| 14 | Anthony Davis | +2.92 | +2.30 | 34 | +20 | Jimmy Butler | +3.40 |
| 15 | CJ Watson | +2.92 | +3.20 | 17 | +2 | Roy Hibbert | +3.40 |
| 16 | Paul Pierce | +2.87 | +1.80 | 50 | +34 | DeMarcus Cousins | +3.30 |
| 17 | Ian Mahinmi | +2.79 | +3.50 | 13 | -4 | CJ Watson | +3.20 |
| 18 | LaMarcus Aldridge | +2.76 | +2.20 | 37 | +19 | Tim Duncan | +3.00 |
| 19 | Marcin Gortat | +2.76 | +2.50 | 29 | +10 | Jae Crowder | +3.00 |
| 20 | Iman Shumpert | +2.70 | +1.00 | 73 | +53 | Kris Humphries | +3.00 |
| 21 | Paul Millsap | +2.68 | +2.70 | 23 | +2 | Paul George | +2.90 |
| 22 | Michael KiddGilchrist | +2.64 | +4.40 | 5 | -17 | Marc Gasol | +2.80 |
| 23 | Chris Bosh | +2.55 | +1.00 | 75 | +52 | Paul Millsap | +2.70 |
| 24 | Derek Fisher | +2.53 | +2.00 | 43 | +19 | Shane Battier | +2.70 |
| 25 | DeMarcus Cousins | +2.50 | +3.30 | 16 | -9 | Andre Iguodala | +2.60 |
| 26 | Gerald Wallace | +2.39 | +1.60 | 52 | +26 | DeMarre Carroll | +2.60 |
| 27 | Tim Duncan | +2.37 | +3.00 | 18 | -9 | Mario Chalmers | +2.50 |
| 28 | Darrell Arthur | +2.35 | +1.90 | 45 | +17 | Samuel Dalembert | +2.50 |
| 29 | David West | +2.34 | +1.50 | 57 | +28 | Marcin Gortat | +2.50 |
| 30 | DeAndre Jordan | +2.29 | +1.10 | 65 | +35 | Victor Oladipo | +2.40 |
| 31 | DeMarre Carroll | +2.28 | +2.60 | 26 | -5 | Tony Allen | +2.40 |
| 32 | Jimmy Butler | +2.27 | +3.40 | 14 | -18 | Dwight Howard | +2.40 |
| 33 | Nicolas Batum | +2.20 | +0.00 | 128 | +95 | Serge Ibaka | +2.30 |
| 34 | Chris Andersen | +2.18 | +1.60 | 55 | +21 | Anthony Davis | +2.30 |
| 35 | Roy Hibbert | +2.15 | +3.40 | 15 | -20 | Kemba Walker | +2.30 |
| 36 | Kirk Hinrich | +2.11 | +2.10 | 42 | +6 | Thabo Sefolosha | +2.30 |
| 37 | Shaun Livingston | +2.10 | +0.90 | 77 | +40 | LaMarcus Aldridge | +2.20 |
| 38 | Al Jefferson | +2.09 | +1.80 | 47 | +9 | Nikola Pekovic | +2.20 |
| 39 | Thabo Sefolosha | +2.05 | +2.30 | 36 | -3 | Eric Bledsoe | +2.20 |
| 40 | Shane Battier | +1.99 | +2.70 | 24 | -16 | George Hill | +2.10 |
| 41 | Manu Ginobili | +1.95 | +1.10 | 64 | +23 | Kosta Koufos | +2.10 |
| 42 | Kemba Walker | +1.93 | +2.30 | 35 | -7 | Kirk Hinrich | +2.10 |
| 43 | Robin Lopez | +1.93 | +2.00 | 44 | +1 | Derek Fisher | +2.00 |
| 44 | Chris Paul | +1.93 | +3.90 | 8 | -36 | Robin Lopez | +2.00 |
| 45 | Marc Gasol | +1.89 | +2.80 | 22 | -23 | Darrell Arthur | +1.90 |
| 46 | Amir Johnson | +1.88 | +1.20 | 63 | +17 | Patrick Beverley | +1.90 |
| 47 | Nick Collison | +1.88 | +0.70 | 88 | +41 | Al Jefferson | +1.80 |
| 48 | Mario Chalmers | +1.87 | +2.50 | 27 | -21 | Jeremy Lin | +1.80 |
| 49 | PJ Tucker | +1.86 | +0.90 | 82 | +33 | Ricky Rubio | +1.80 |
| 50 | Ersan Ilyasova | +1.85 | +0.70 | 90 | +40 | Paul Pierce | +1.80 |

## 2014-15 Playoffs — defense, top 50

> pool 99 players &nbsp;·&nbsp; tau(true30) +0.356 &nbsp;·&nbsp; hits@30 19/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | DeAndre Jordan | +5.23 | +2.10 | 29 | +28 | Jarrett Jack | +7.50 |
| 2 | Jarrett Jack | +5.22 | +7.50 | 1 | -1 | Anthony Davis | +7.20 |
| 3 | Pau Gasol | +5.04 | +3.50 | 16 | +13 | Timofey Mozgov | +6.90 |
| 4 | Tony Allen | +4.44 | +5.00 | 10 | +6 | Otto Porter Jr. | +6.30 |
| 5 | Otto Porter Jr. | +4.08 | +6.30 | 4 | -1 | Trevor Ariza | +6.10 |
| 6 | Jimmy Butler | +3.93 | +3.70 | 13 | +7 | AlFarouq Aminu | +5.80 |
| 7 | Timofey Mozgov | +3.89 | +6.90 | 3 | -4 | Dwight Howard | +5.70 |
| 8 | Al Horford | +3.79 | +4.40 | 12 | +4 | Danny Green | +5.50 |
| 9 | Dwight Howard | +3.69 | +5.70 | 7 | -2 | Marc Gasol | +5.30 |
| 10 | Tim Duncan | +3.69 | +2.70 | 23 | +13 | Tony Allen | +5.00 |
| 11 | Alan Anderson | +3.61 | +2.80 | 20 | +9 | Nene | +4.70 |
| 12 | Danny Green | +3.25 | +5.50 | 8 | -4 | Al Horford | +4.40 |
| 13 | Kyle Korver | +3.13 | +1.10 | 42 | +29 | Jimmy Butler | +3.70 |
| 14 | Nene | +3.06 | +4.70 | 11 | -3 | Blake Griffin | +3.70 |
| 15 | AlFarouq Aminu | +3.04 | +5.80 | 6 | -9 | Ramon Sessions | +3.50 |
| 16 | Andrew Bogut | +3.01 | +1.90 | 35 | +19 | Pau Gasol | +3.50 |
| 17 | Brook Lopez | +3.00 | +2.00 | 34 | +17 | Matt Barnes | +3.40 |
| 18 | Anthony Davis | +2.93 | +7.20 | 2 | -16 | Stephen Curry | +3.00 |
| 19 | Joakim Noah | +2.88 | +2.10 | 33 | +14 | Derrick Rose | +3.00 |
| 20 | Harrison Barnes | +2.85 | +1.00 | 43 | +23 | Alan Anderson | +2.80 |
| 21 | Matt Barnes | +2.72 | +3.40 | 17 | -4 | Avery Bradley | +2.80 |
| 22 | Marc Gasol | +2.67 | +5.30 | 9 | -13 | Manu Ginobili | +2.70 |
| 23 | Stephen Curry | +2.58 | +3.00 | 18 | -5 | Tim Duncan | +2.70 |
| 24 | Mike Dunleavy | +2.46 | +0.60 | 49 | +25 | Matthew Dellavedova | +2.50 |
| 25 | Mike Conley | +2.22 | +1.70 | 39 | +14 | Tristan Thompson | +2.50 |
| 26 | Marcin Gortat | +2.17 | +0.30 | 53 | +27 | Chris Paul | +2.50 |
| 27 | Blake Griffin | +1.91 | +3.70 | 14 | -13 | Thaddeus Young | +2.30 |
| 28 | Iman Shumpert | +1.88 | +0.20 | 55 | +27 | John Henson | +2.30 |
| 29 | DeMarre Carroll | +1.87 | -0.80 | 66 | +37 | DeAndre Jordan | +2.10 |
| 30 | Andre Iguodala | +1.86 | +0.70 | 48 | +18 | Festus Ezeli | +2.10 |
| 31 | Paul Millsap | +1.62 | +2.10 | 32 | +1 | JJ Barea | +2.10 |
| 32 | John Henson | +1.60 | +2.30 | 28 | -4 | Paul Millsap | +2.10 |
| 33 | Ramon Sessions | +1.59 | +3.50 | 15 | -18 | Joakim Noah | +2.10 |
| 34 | LeBron James | +1.56 | +1.10 | 41 | +7 | Brook Lopez | +2.00 |
| 35 | Drew Gooden | +1.55 | -1.30 | 72 | +37 | Andrew Bogut | +1.90 |
| 36 | Tony Snell | +1.45 | -2.80 | 83 | +47 | Pero Antic | +1.90 |
| 37 | Pablo Prigioni | +1.44 | -1.60 | 75 | +38 | Kent Bazemore | +1.80 |
| 38 | Trevor Ariza | +1.27 | +6.10 | 5 | -33 | Bradley Beal | +1.70 |
| 39 | Kent Bazemore | +1.21 | +1.80 | 37 | -2 | Mike Conley | +1.70 |
| 40 | JJ Barea | +1.17 | +2.10 | 31 | -9 | Kawhi Leonard | +1.40 |
| 41 | OJ Mayo | +1.17 | -0.90 | 67 | +26 | LeBron James | +1.10 |
| 42 | Nikola Mirotic | +1.14 | -2.00 | 78 | +36 | Kyle Korver | +1.10 |
| 43 | Giannis Antetokounmpo | +1.13 | +0.60 | 50 | +7 | Harrison Barnes | +1.00 |
| 44 | Pero Antic | +0.97 | +1.90 | 36 | -8 | Shaun Livingston | +0.90 |
| 45 | Matthew Dellavedova | +0.94 | +2.50 | 24 | -21 | John Wall | +0.90 |
| 46 | Tristan Thompson | +0.81 | +2.50 | 25 | -21 | Nicolas Batum | +0.80 |
| 47 | Derrick Rose | +0.78 | +3.00 | 19 | -28 | Bojan Bogdanovic | +0.70 |
| 48 | Courtney Lee | +0.78 | -0.70 | 63 | +15 | Andre Iguodala | +0.70 |
| 49 | JR Smith | +0.77 | -0.40 | 61 | +12 | Mike Dunleavy | +0.60 |
| 50 | Thaddeus Young | +0.74 | +2.30 | 27 | -23 | Giannis Antetokounmpo | +0.60 |

## 2014-15 Regular season — defense, top 50

> pool 247 players &nbsp;·&nbsp; tau(true30) +0.600 &nbsp;·&nbsp; hits@30 22/30

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Tony Allen | +4.96 | +4.80 | 4 | +3 | Kawhi Leonard | +5.20 |
| 2 | Draymond Green | +4.91 | +5.10 | 2 | +0 | Draymond Green | +5.10 |
| 3 | Andrew Bogut | +4.68 | +4.70 | 5 | +2 | Rudy Gobert | +4.80 |
| 4 | Rudy Gobert | +4.20 | +4.80 | 3 | -1 | Tony Allen | +4.80 |
| 5 | Kawhi Leonard | +4.20 | +5.20 | 1 | -4 | Andrew Bogut | +4.70 |
| 6 | Anthony Davis | +3.64 | +4.50 | 6 | +0 | Anthony Davis | +4.50 |
| 7 | Andre Roberson | +3.41 | +3.40 | 10 | +3 | DeMarcus Cousins | +4.40 |
| 8 | Nerlens Noel | +3.37 | +2.70 | 19 | +11 | Marcin Gortat | +3.60 |
| 9 | Nene | +3.28 | +2.80 | 18 | +9 | Tim Duncan | +3.50 |
| 10 | Michael KiddGilchrist | +3.08 | +3.00 | 14 | +4 | Andre Roberson | +3.40 |
| 11 | DeMarcus Cousins | +3.06 | +4.40 | 7 | -4 | Kosta Koufos | +3.30 |
| 12 | Iman Shumpert | +2.93 | +2.30 | 32 | +20 | Zaza Pachulia | +3.20 |
| 13 | Zaza Pachulia | +2.90 | +3.20 | 12 | -1 | Khris Middleton | +3.10 |
| 14 | AlFarouq Aminu | +2.90 | +2.60 | 24 | +10 | Michael KiddGilchrist | +3.00 |
| 15 | Tim Duncan | +2.90 | +3.50 | 9 | -6 | Serge Ibaka | +3.00 |
| 16 | Danny Green | +2.88 | +3.00 | 16 | +0 | Danny Green | +3.00 |
| 17 | Kosta Koufos | +2.87 | +3.30 | 11 | -6 | Jonas Jerebko | +2.80 |
| 18 | Marcus Smart | +2.77 | +1.80 | 43 | +25 | Nene | +2.80 |
| 19 | Tyson Chandler | +2.61 | +2.60 | 20 | +1 | Nerlens Noel | +2.70 |
| 20 | Marcin Gortat | +2.56 | +3.60 | 8 | -12 | Tyson Chandler | +2.60 |
| 21 | Jared Dudley | +2.55 | +1.80 | 44 | +23 | Marc Gasol | +2.60 |
| 22 | Khris Middleton | +2.46 | +3.10 | 13 | -9 | Joakim Noah | +2.60 |
| 23 | Nikola Mirotic | +2.46 | +2.20 | 33 | +10 | Josh Smith | +2.60 |
| 24 | Andre Iguodala | +2.43 | +1.60 | 47 | +23 | AlFarouq Aminu | +2.60 |
| 25 | Josh Smith | +2.42 | +2.60 | 23 | -2 | Alex Len | +2.50 |
| 26 | Jonas Jerebko | +2.39 | +2.80 | 17 | -9 | Paul Millsap | +2.40 |
| 27 | Greg Monroe | +2.38 | +0.20 | 111 | +84 | Omer Asik | +2.40 |
| 28 | Wesley Matthews | +2.32 | +1.30 | 56 | +28 | Timofey Mozgov | +2.40 |
| 29 | Derrick Favors | +2.21 | +2.10 | 34 | +5 | Darren Collison | +2.30 |
| 30 | Timofey Mozgov | +2.19 | +2.40 | 28 | -2 | Luc Mbah a Moute | +2.30 |
| 31 | Dwight Howard | +2.13 | +1.90 | 41 | +10 | Michael CarterWilliams | +2.30 |
| 32 | Michael CarterWilliams | +2.02 | +2.30 | 31 | -1 | Iman Shumpert | +2.30 |
| 33 | Paul Millsap | +2.00 | +2.40 | 26 | -7 | Nikola Mirotic | +2.20 |
| 34 | Kelly Olynyk | +1.99 | +2.10 | 36 | +2 | Derrick Favors | +2.10 |
| 35 | Manu Ginobili | +1.99 | +1.40 | 52 | +17 | Chris Paul | +2.10 |
| 36 | Jimmy Butler | +1.98 | -0.20 | 140 | +104 | Kelly Olynyk | +2.10 |
| 37 | DeAndre Jordan | +1.91 | +1.20 | 63 | +26 | Cody Zeller | +2.10 |
| 38 | PJ Tucker | +1.90 | +1.30 | 54 | +16 | Roy Hibbert | +2.00 |
| 39 | Zach Randolph | +1.88 | +1.30 | 59 | +20 | Steven Adams | +2.00 |
| 40 | Omer Asik | +1.84 | +2.40 | 27 | -13 | LaMarcus Aldridge | +1.90 |
| 41 | Joakim Noah | +1.78 | +2.60 | 22 | -19 | Dwight Howard | +1.90 |
| 42 | Cory Joseph | +1.76 | +0.50 | 85 | +43 | Pablo Prigioni | +1.80 |
| 43 | John Henson | +1.76 | +0.50 | 88 | +45 | Marcus Smart | +1.80 |
| 44 | Ersan Ilyasova | +1.75 | +0.40 | 100 | +56 | Jared Dudley | +1.80 |
| 45 | Monta Ellis | +1.74 | +0.80 | 77 | +32 | George Hill | +1.70 |
| 46 | Pau Gasol | +1.74 | +0.50 | 90 | +44 | Al Horford | +1.60 |
| 47 | Andre Drummond | +1.74 | +0.40 | 98 | +51 | Andre Iguodala | +1.60 |
| 48 | Brandan Wright | +1.72 | +1.30 | 58 | +10 | Kevin Love | +1.60 |
| 49 | Mario Chalmers | +1.70 | +1.50 | 49 | +0 | Mario Chalmers | +1.50 |
| 50 | Al Horford | +1.68 | +1.60 | 46 | -4 | Kris Humphries | +1.50 |

## 2023-24 Regular season — defense, top 50 (projected, no truth)

> pool 248 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +3.53 | 2593 |
| 2 | Isaiah Hartenstein | +3.06 | 1896 |
| 3 | Dean Wade | +3.05 | 1108 |
| 4 | Joel Embiid | +3.05 | 1309 |
| 5 | Alex Caruso | +3.00 | 2040 |
| 6 | Victor Wembanyama | +2.71 | 2106 |
| 7 | Kristaps Porzingis | +2.66 | 1690 |
| 8 | Matisse Thybulle | +2.65 | 1487 |
| 9 | Jusuf Nurkic | +2.60 | 2078 |
| 10 | Brook Lopez | +2.59 | 2411 |
| 11 | Nic Claxton | +2.42 | 2116 |
| 12 | Chet Holmgren | +2.22 | 2413 |
| 13 | Larry Nance Jr. | +2.19 | 1216 |
| 14 | Andre Drummond | +2.16 | 1351 |
| 15 | Draymond Green | +2.15 | 1490 |
| 16 | Nikola Jokic | +2.13 | 2737 |
| 17 | Toumani Camara | +2.05 | 1739 |
| 18 | Evan Mobley | +2.04 | 1532 |
| 19 | Ausar Thompson | +1.97 | 1583 |
| 20 | Amen Thompson | +1.95 | 1388 |
| 21 | Anthony Davis | +1.94 | 2700 |
| 22 | Ivica Zubac | +1.91 | 1795 |
| 23 | Paul Reed | +1.89 | 1590 |
| 24 | Derrick White | +1.85 | 2381 |
| 25 | Jarrett Allen | +1.85 | 2442 |
| 26 | Clint Capela | +1.83 | 1883 |
| 27 | Paul George | +1.71 | 2502 |
| 28 | Derrick Jones Jr. | +1.66 | 1783 |
| 29 | Kawhi Leonard | +1.64 | 2330 |
| 30 | Franz Wagner | +1.62 | 2337 |
| 31 | Walker Kessler | +1.62 | 1493 |
| 32 | Vince Williams Jr. | +1.62 | 1436 |
| 33 | Aaron Nesmith | +1.61 | 1995 |
| 34 | Wendell Carter Jr. | +1.59 | 1406 |
| 35 | Myles Turner | +1.58 | 2077 |
| 36 | Jakob Poeltl | +1.57 | 1319 |
| 37 | OG Anunoby | +1.55 | 1702 |
| 38 | Nickeil Alexander-Walker | +1.54 | 1921 |
| 39 | Naz Reid | +1.52 | 1964 |
| 40 | Bam Adebayo | +1.48 | 2416 |
| 41 | Isaiah Joe | +1.47 | 1445 |
| 42 | Daniel Gafford | +1.46 | 1814 |
| 43 | Jalen Suggs | +1.40 | 2025 |
| 44 | Luguentz Dort | +1.34 | 2246 |
| 45 | Dyson Daniels | +1.29 | 1358 |
| 46 | John Konchar | +1.28 | 1173 |
| 47 | Peyton Watson | +1.25 | 1488 |
| 48 | Herbert Jones | +1.15 | 2321 |
| 49 | Moses Moody | +1.13 | 1156 |
| 50 | Jalen Johnson | +1.12 | 1889 |

## 2023-24 Playoffs — defense, top 50 (projected, no truth)

> pool 103 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Justin Holiday | +4.73 | 150 |
| 2 | Kristaps Porzingis | +4.72 | 165 |
| 3 | Chet Holmgren | +3.59 | 345 |
| 4 | Paolo Banchero | +3.47 | 262 |
| 5 | Joel Embiid | +3.46 | 248 |
| 6 | Dereck Lively II | +3.23 | 462 |
| 7 | Aaron Wiggins | +3.06 | 157 |
| 8 | Evan Mobley | +3.03 | 422 |
| 9 | Wendell Carter Jr. | +2.80 | 185 |
| 10 | Al Horford | +2.67 | 575 |
| 11 | Cason Wallace | +2.33 | 198 |
| 12 | Jonathan Isaac | +2.29 | 147 |
| 13 | Ivica Zubac | +2.28 | 192 |
| 14 | Brandon Ingram | +2.26 | 145 |
| 15 | Jalen Suggs | +2.26 | 232 |
| 16 | Jalen Williams | +2.25 | 377 |
| 17 | Rudy Gobert | +2.24 | 512 |
| 18 | Franz Wagner | +1.92 | 259 |
| 19 | Luka Doncic | +1.90 | 900 |
| 20 | Luguentz Dort | +1.85 | 350 |
| 21 | Isaac Okoro | +1.84 | 263 |
| 22 | Derrick White | +1.63 | 676 |
| 23 | Gary Harris | +1.60 | 159 |
| 24 | Jrue Holiday | +1.59 | 720 |
| 25 | Josh Giddey | +1.55 | 181 |
| 26 | Josh Green | +1.40 | 399 |
| 27 | Kelly Oubre Jr. | +1.30 | 224 |
| 28 | Sam Hauser | +1.22 | 283 |
| 29 | Mike Conley | +1.16 | 474 |
| 30 | Bobby Portis | +1.09 | 187 |
| 31 | Christian Braun | +1.04 | 204 |
| 32 | Daniel Gafford | +1.00 | 445 |
| 33 | Kentavious Caldwell-Pope | +0.99 | 420 |
| 34 | Bam Adebayo | +0.85 | 192 |
| 35 | Anthony Edwards | +0.64 | 649 |
| 36 | Jayson Tatum | +0.58 | 768 |
| 37 | Shai Gilgeous-Alexander | +0.52 | 399 |
| 38 | Nikola Jokic | +0.48 | 482 |
| 39 | Jaylen Brown | +0.42 | 707 |
| 40 | Donovan Mitchell | +0.41 | 382 |
| 41 | Isaiah Joe | +0.41 | 173 |
| 42 | LeBron James | +0.36 | 204 |
| 43 | Trey Murphy III | +0.34 | 168 |
| 44 | Tyrese Haliburton | +0.33 | 522 |
| 45 | Jaden McDaniels | +0.25 | 537 |
| 46 | OG Anunoby | +0.18 | 324 |
| 47 | Isaiah Jackson | +0.17 | 154 |
| 48 | Derrick Jones Jr. | +0.15 | 647 |
| 49 | P.J. Washington | +0.13 | 785 |
| 50 | Paul George | +0.12 | 222 |

## 2024-25 Regular season — defense, top 50 (projected, no truth)

> pool 257 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.39 | 2388 |
| 2 | Victor Wembanyama | +3.31 | 1527 |
| 3 | Luke Kornet | +3.26 | 1361 |
| 4 | Alperen Sengun | +3.15 | 2394 |
| 5 | Ivica Zubac | +3.01 | 2624 |
| 6 | Toumani Camara | +2.89 | 2548 |
| 7 | Jaxson Hayes | +2.82 | 1093 |
| 8 | Luguentz Dort | +2.65 | 2073 |
| 9 | Kris Dunn | +2.60 | 1783 |
| 10 | Jarrett Allen | +2.54 | 2296 |
| 11 | Ausar Thompson | +2.52 | 1328 |
| 12 | Donovan Clingan | +2.50 | 1324 |
| 13 | Kristaps Porziņģis | +2.50 | 1210 |
| 14 | Dyson Daniels | +2.38 | 2571 |
| 15 | Evan Mobley | +2.36 | 2167 |
| 16 | Isaiah Hartenstein | +2.36 | 1590 |
| 17 | Draymond Green | +2.33 | 1983 |
| 18 | Brandin Podziemski | +2.11 | 1716 |
| 19 | Nicolas Batum | +2.08 | 1367 |
| 20 | Brandon Clarke | +2.06 | 1207 |
| 21 | Kevon Looney | +1.90 | 1142 |
| 22 | Isaiah Stewart | +1.88 | 1434 |
| 23 | Amen Thompson | +1.87 | 2225 |
| 24 | Jonathan Isaac | +1.86 | 1090 |
| 25 | Jaden McDaniels | +1.84 | 2614 |
| 26 | Walker Kessler | +1.68 | 1740 |
| 27 | Daniel Gafford | +1.66 | 1226 |
| 28 | Keon Ellis | +1.65 | 1948 |
| 29 | Jaren Jackson Jr. | +1.58 | 2207 |
| 30 | Scotty Pippen Jr. | +1.57 | 1683 |
| 31 | Cody Martin | +1.57 | 1173 |
| 32 | Sam Merrill | +1.56 | 1401 |
| 33 | Shai Gilgeous-Alexander | +1.53 | 2598 |
| 34 | Kentavious Caldwell-Pope | +1.53 | 2279 |
| 35 | Derrick White | +1.51 | 2574 |
| 36 | Anthony Davis | +1.49 | 1706 |
| 37 | Donte DiVincenzo | +1.48 | 1606 |
| 38 | Mike Conley | +1.48 | 1756 |
| 39 | Luka Dončić | +1.47 | 1769 |
| 40 | Goga Bitadze | +1.46 | 1430 |
| 41 | Dean Wade | +1.38 | 1252 |
| 42 | Cason Wallace | +1.37 | 1876 |
| 43 | Myles Turner | +1.36 | 2174 |
| 44 | Wendell Carter Jr. | +1.35 | 1758 |
| 45 | P.J. Washington | +1.34 | 1835 |
| 46 | Jalen Williams | +1.32 | 2237 |
| 47 | Jrue Holiday | +1.27 | 1896 |
| 48 | Jalen Johnson | +1.27 | 1284 |
| 49 | Paul George | +1.26 | 1334 |
| 50 | Tari Eason | +1.22 | 1420 |

## 2024-25 Playoffs — defense, top 50 (projected, no truth)

> pool 109 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.40 | 411 |
| 2 | Steven Adams | +3.99 | 155 |
| 3 | Kenrich Williams | +3.77 | 137 |
| 4 | Alperen Sengun | +3.70 | 256 |
| 5 | Alex Caruso | +3.66 | 562 |
| 6 | Ausar Thompson | +3.61 | 135 |
| 7 | Draymond Green | +3.58 | 389 |
| 8 | Jayson Tatum | +3.54 | 322 |
| 9 | Nikola Jokić | +3.49 | 563 |
| 10 | Jarrett Allen | +3.33 | 261 |
| 11 | Jaden McDaniels | +3.24 | 497 |
| 12 | Jaylin Williams | +3.22 | 141 |
| 13 | Chet Holmgren | +3.22 | 686 |
| 14 | Cason Wallace | +2.80 | 516 |
| 15 | Luke Kornet | +2.75 | 180 |
| 16 | Mitchell Robinson | +2.60 | 370 |
| 17 | Luguentz Dort | +2.57 | 666 |
| 18 | Gary Trent Jr. | +2.20 | 171 |
| 19 | Pascal Siakam | +2.19 | 771 |
| 20 | Brandin Podziemski | +2.10 | 385 |
| 21 | Tari Eason | +2.10 | 132 |
| 22 | Aaron Nesmith | +2.08 | 650 |
| 23 | Cade Cunningham | +1.98 | 248 |
| 24 | Kentavious Caldwell-Pope | +1.71 | 163 |
| 25 | OG Anunoby | +1.68 | 705 |
| 26 | Aaron Wiggins | +1.54 | 303 |
| 27 | Christian Braun | +1.52 | 544 |
| 28 | Isaiah Hartenstein | +1.41 | 516 |
| 29 | Mike Conley | +1.21 | 356 |
| 30 | Bobby Portis | +1.20 | 158 |
| 31 | Kristaps Porziņģis | +1.20 | 231 |
| 32 | Jaylen Brown | +1.15 | 402 |
| 33 | Buddy Hield | +1.12 | 327 |
| 34 | Al Horford | +1.02 | 348 |
| 35 | Quinten Post | +0.98 | 146 |
| 36 | Jrue Holiday | +0.94 | 264 |
| 37 | Andrew Nembhard | +0.83 | 769 |
| 38 | Peyton Watson | +0.75 | 199 |
| 39 | Jamal Murray | +0.74 | 578 |
| 40 | Karl-Anthony Towns | +0.73 | 639 |
| 41 | Anthony Edwards | +0.71 | 585 |
| 42 | Jabari Smith Jr. | +0.66 | 143 |
| 43 | Amen Thompson | +0.54 | 231 |
| 44 | Shai Gilgeous-Alexander | +0.48 | 851 |
| 45 | Nicolas Batum | +0.48 | 172 |
| 46 | Josh Hart | +0.47 | 642 |
| 47 | Franz Wagner | +0.46 | 195 |
| 48 | Derrick White | +0.43 | 415 |
| 49 | Isaiah Joe | +0.31 | 211 |
| 50 | Ivica Zubac | +0.31 | 256 |

## 2025-26 Regular season — defense, top 50 (projected, no truth)

> pool 269 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +5.40 | 1866 |
| 2 | Neemias Queta | +4.02 | 1926 |
| 3 | Chet Holmgren | +3.92 | 1997 |
| 4 | Isaiah Hartenstein | +3.55 | 1137 |
| 5 | Rudy Gobert | +3.09 | 2380 |
| 6 | Ausar Thompson | +3.01 | 1896 |
| 7 | Derrick White | +2.91 | 2625 |
| 8 | Hugo González | +2.90 | 1084 |
| 9 | Cason Wallace | +2.88 | 2046 |
| 10 | Dyson Daniels | +2.76 | 2520 |
| 11 | Ronald Holland II | +2.66 | 1550 |
| 12 | Ajay Mitchell | +2.44 | 1473 |
| 13 | Javonte Green | +2.30 | 1446 |
| 14 | Dru Smith | +2.16 | 1141 |
| 15 | Toumani Camara | +2.12 | 2731 |
| 16 | Jarrett Allen | +2.06 | 1519 |
| 17 | Baylor Scheierman | +2.06 | 1429 |
| 18 | Ryan Kalkbrenner | +2.02 | 1479 |
| 19 | Luke Kornet | +1.95 | 1430 |
| 20 | Jordan Goodwin | +1.94 | 1572 |
| 21 | Marcus Smart | +1.93 | 1769 |
| 22 | Jamal Shead | +1.91 | 1852 |
| 23 | John Konchar | +1.91 | 1115 |
| 24 | Ryan Dunn | +1.90 | 1355 |
| 25 | Jaylin Williams | +1.89 | 1277 |
| 26 | Donte DiVincenzo | +1.88 | 2494 |
| 27 | Sidy Cissoko | +1.86 | 1435 |
| 28 | Paul George | +1.83 | 1135 |
| 29 | Jalen Suggs | +1.83 | 1574 |
| 30 | Evan Mobley | +1.80 | 2074 |
| 31 | Wendell Carter Jr. | +1.80 | 2288 |
| 32 | Jusuf Nurkić | +1.69 | 1083 |
| 33 | Keon Ellis | +1.66 | 1479 |
| 34 | Sion James | +1.60 | 1843 |
| 35 | Scottie Barnes | +1.56 | 2681 |
| 36 | Collin Gillespie | +1.55 | 2282 |
| 37 | Bam Adebayo | +1.50 | 2365 |
| 38 | Mitchell Robinson | +1.47 | 1175 |
| 39 | Kris Murray | +1.46 | 1333 |
| 40 | Naz Reid | +1.45 | 2007 |
| 41 | Collin Murray-Boyles | +1.43 | 1246 |
| 42 | Mouhamed Gueye | +1.43 | 1179 |
| 43 | Josh Okogie | +1.42 | 1354 |
| 44 | OG Anunoby | +1.42 | 2224 |
| 45 | Oso Ighodaro | +1.40 | 1808 |
| 46 | Luguentz Dort | +1.39 | 1849 |
| 47 | Myles Turner | +1.31 | 1912 |
| 48 | Ziaire Williams | +1.25 | 1281 |
| 49 | Day'Ron Sharpe | +1.20 | 1160 |
| 50 | Dominick Barlow | +1.15 | 1689 |

## 2025-26 Playoffs — defense, top 50 (projected, no truth)

> pool 112 players above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +4.79 | 750 |
| 2 | Jose Alvarado | +4.36 | 170 |
| 3 | Neemias Queta | +4.21 | 152 |
| 4 | Tari Eason | +4.18 | 195 |
| 5 | Amen Thompson | +3.90 | 264 |
| 6 | Alex Caruso | +3.74 | 353 |
| 7 | Karl-Anthony Towns | +3.73 | 578 |
| 8 | Ajay Mitchell | +3.50 | 317 |
| 9 | Cason Wallace | +3.27 | 374 |
| 10 | Alperen Sengun | +3.27 | 232 |
| 11 | Ausar Thompson | +3.16 | 427 |
| 12 | Josh Hart | +2.83 | 614 |
| 13 | Toumani Camara | +2.63 | 165 |
| 14 | Javonte Green | +2.62 | 132 |
| 15 | Jarrett Allen | +2.61 | 529 |
| 16 | Jaylin Williams | +2.18 | 240 |
| 17 | Rudy Gobert | +2.14 | 372 |
| 18 | OG Anunoby | +2.11 | 586 |
| 19 | Isaiah Stewart | +2.06 | 165 |
| 20 | Jakob Poeltl | +2.02 | 134 |
| 21 | Dean Wade | +1.87 | 407 |
| 22 | Anthony Black | +1.74 | 196 |
| 23 | Dylan Harper | +1.71 | 615 |
| 24 | Mikal Bridges | +1.66 | 608 |
| 25 | Mitchell Robinson | +1.62 | 251 |
| 26 | Devin Vassell | +1.61 | 801 |
| 27 | Jaylen Brown | +1.47 | 249 |
| 28 | Christian Braun | +1.41 | 187 |
| 29 | De'Aaron Fox | +1.33 | 704 |
| 30 | Chet Holmgren | +1.28 | 459 |
| 31 | Marcus Smart | +1.27 | 345 |
| 32 | VJ Edgecombe | +1.21 | 407 |
| 33 | Julian Champagnie | +1.12 | 705 |
| 34 | Jabari Smith Jr. | +1.03 | 252 |
| 35 | Ja'Kobe Walter | +0.98 | 224 |
| 36 | Isaiah Hartenstein | +0.97 | 350 |
| 37 | Paolo Banchero | +0.95 | 273 |
| 38 | Jaxson Hayes | +0.92 | 163 |
| 39 | Dyson Daniels | +0.88 | 166 |
| 40 | Luke Kornet | +0.83 | 296 |
| 41 | Jamal Shead | +0.67 | 224 |
| 42 | Mike Conley | +0.67 | 168 |
| 43 | Jaden McDaniels | +0.65 | 406 |
| 44 | Evan Mobley | +0.61 | 640 |
| 45 | Derrick White | +0.58 | 251 |
| 46 | Collin Murray-Boyles | +0.48 | 191 |
| 47 | Jayson Tatum | +0.46 | 218 |
| 48 | James Harden | +0.36 | 672 |
| 49 | Nikola Jokić | +0.27 | 237 |
| 50 | Reed Sheppard | +0.15 | 192 |
