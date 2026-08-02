# Leaderboards from the component-architecture models

## The model

**Offense** copies RAPTOR's own structure (RESULTS_raptor_research.md): a box
model and an on/off model trained against 538's published component labels
(`rap_box_o`, `rap_onoff_o`), combined by a ridge with log-minutes interaction
terms. components: box 459 feats + on/off 689 feats, combiner weights box=0.950 onoff=0.186. The learned box-heavy weighting matches
538's stated design; output is in RAPTOR points because the combiner's target
is the blended rating.

**Defense** stays a direct model plus within-cell z-scores of rate stats —
direct + 12 within-cell z-score features (components rejected: box R2 +0.71 on defense). The defensive box component cannot be reproduced
without nearest-defender and positional-matchup data, which no feed we scrape
carries; that is the known ceiling on defensive ordering.

Both choices were selected on held-out 2013-14/2014-15 regular seasons and
replicated on disjoint seeds. Regular season only, trained and projected.

## Held-out test boards (truth available)

### 2013-14 regular season — offense, top 50

> pool 247 · tau(true30) +0.775 · hits@30 25/30 · dev@10 1.20 · MAE 0.607

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +6.65 | +7.10 | 2 | +1 | Kevin Durant | +7.60 |
| 2 | Kevin Durant | +6.63 | +7.60 | 1 | -1 | Chris Paul | +7.10 |
| 3 | LeBron James | +6.42 | +5.80 | 4 | +1 | James Harden | +6.30 |
| 4 | James Harden | +5.96 | +6.30 | 3 | -1 | LeBron James | +5.80 |
| 5 | Kevin Love | +4.50 | +5.70 | 5 | +0 | Kevin Love | +5.70 |
| 6 | Goran Dragic | +4.28 | +4.80 | 6 | +0 | Goran Dragic | +4.80 |
| 7 | Kyle Lowry | +4.18 | +4.40 | 7 | +0 | Kyle Lowry | +4.40 |
| 8 | Manu Ginobili | +3.99 | +4.00 | 10 | +2 | Dirk Nowitzki | +4.40 |
| 9 | Damian Lillard | +3.98 | +3.60 | 12 | +3 | Carmelo Anthony | +4.20 |
| 10 | Isaiah Thomas | +3.92 | +3.50 | 14 | +4 | Manu Ginobili | +4.00 |
| 11 | Russell Westbrook | +3.83 | +3.30 | 15 | +4 | Patty Mills | +3.90 |
| 12 | Carmelo Anthony | +3.48 | +4.20 | 9 | -3 | Damian Lillard | +3.60 |
| 13 | Dirk Nowitzki | +3.16 | +4.40 | 8 | -5 | Mike Conley | +3.50 |
| 14 | Mike Conley | +3.08 | +3.50 | 13 | -1 | Isaiah Thomas | +3.50 |
| 15 | Blake Griffin | +2.93 | +2.90 | 17 | +2 | Russell Westbrook | +3.30 |
| 16 | Kyrie Irving | +2.60 | +2.30 | 27 | +11 | Ty Lawson | +3.20 |
| 17 | Patty Mills | +2.60 | +3.90 | 11 | -6 | Blake Griffin | +2.90 |
| 18 | Jamal Crawford | +2.46 | +2.80 | 19 | +1 | Marco Belinelli | +2.80 |
| 19 | Ty Lawson | +2.42 | +3.20 | 16 | -3 | Jamal Crawford | +2.80 |
| 20 | Paul George | +2.39 | +2.60 | 22 | +2 | Wesley Matthews | +2.80 |
| 21 | DJ Augustin | +2.18 | +2.10 | 32 | +11 | Joe Johnson | +2.70 |
| 22 | Ricky Rubio | +2.09 | +1.90 | 39 | +17 | Paul George | +2.60 |
| 23 | Deron Williams | +1.98 | +2.60 | 23 | +0 | Deron Williams | +2.60 |
| 24 | Joe Johnson | +1.97 | +2.70 | 21 | -3 | Chandler Parsons | +2.60 |
| 25 | Pablo Prigioni | +1.92 | +1.70 | 43 | +18 | Nick Young | +2.40 |
| 26 | Wesley Matthews | +1.87 | +2.80 | 20 | -6 | Vince Carter | +2.40 |
| 27 | Jrue Holiday | +1.87 | +2.20 | 28 | +1 | Kyrie Irving | +2.30 |
| 28 | John Wall | +1.68 | +1.90 | 37 | +9 | Jrue Holiday | +2.20 |
| 29 | Chandler Parsons | +1.55 | +2.60 | 24 | -5 | Patrick Beverley | +2.20 |
| 30 | Andre Iguodala | +1.53 | +1.20 | 62 | +32 | Brandon Jennings | +2.20 |
| 31 | Vince Carter | +1.53 | +2.40 | 26 | -5 | Randy Foye | +2.10 |
| 32 | Kawhi Leonard | +1.49 | +1.70 | 44 | +12 | DJ Augustin | +2.10 |
| 33 | Klay Thompson | +1.41 | +2.10 | 33 | +0 | Klay Thompson | +2.10 |
| 34 | Kemba Walker | +1.39 | +1.40 | 52 | +18 | Josh McRoberts | +2.00 |
| 35 | Nicolas Batum | +1.38 | +1.90 | 38 | +3 | Channing Frye | +2.00 |
| 36 | DeMar DeRozan | +1.36 | +1.70 | 42 | +6 | Kyle Korver | +1.90 |
| 37 | George Hill | +1.33 | +0.30 | 120 | +83 | John Wall | +1.90 |
| 38 | Marco Belinelli | +1.29 | +2.80 | 18 | -20 | Nicolas Batum | +1.90 |
| 39 | Darren Collison | +1.26 | +1.10 | 65 | +26 | Ricky Rubio | +1.90 |
| 40 | Nikola Pekovic | +1.25 | +1.10 | 68 | +28 | JR Smith | +1.80 |
| 41 | Jose Calderon | +1.22 | +1.60 | 47 | +6 | Brandan Wright | +1.70 |
| 42 | JR Smith | +1.20 | +1.80 | 40 | -2 | DeMar DeRozan | +1.70 |
| 43 | Randy Foye | +1.14 | +2.10 | 31 | -12 | Pablo Prigioni | +1.70 |
| 44 | Brandan Wright | +1.09 | +1.70 | 41 | -3 | Kawhi Leonard | +1.70 |
| 45 | Eric Bledsoe | +1.09 | +1.50 | 48 | +3 | DeMarcus Cousins | +1.70 |
| 46 | Gerald Green | +1.09 | +1.30 | 57 | +11 | Mirza Teletovic | +1.60 |
| 47 | Patrick Beverley | +1.08 | +2.20 | 29 | -18 | Jose Calderon | +1.60 |
| 48 | Nick Young | +1.08 | +2.40 | 25 | -23 | Eric Bledsoe | +1.50 |
| 49 | LaMarcus Aldridge | +1.08 | +1.10 | 67 | +18 | Dwyane Wade | +1.50 |
| 50 | Greivis Vasquez | +1.06 | +0.30 | 113 | +63 | Mario Chalmers | +1.50 |

### 2014-15 regular season — offense, top 50

> pool 247 · tau(true30) +0.651 · hits@30 25/30 · dev@10 1.20 · MAE 0.607

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Chris Paul | +7.99 | +8.50 | 1 | +0 | Chris Paul | +8.50 |
| 2 | James Harden | +6.66 | +7.70 | 2 | +0 | James Harden | +7.70 |
| 3 | Russell Westbrook | +6.03 | +6.10 | 3 | +0 | Russell Westbrook | +6.10 |
| 4 | LeBron James | +5.67 | +5.30 | 5 | +1 | Kyrie Irving | +5.50 |
| 5 | Kyrie Irving | +5.33 | +5.50 | 4 | -1 | LeBron James | +5.30 |
| 6 | Isaiah Thomas | +5.27 | +4.50 | 8 | +2 | Lou Williams | +5.20 |
| 7 | Lou Williams | +4.44 | +5.20 | 6 | -1 | Kyle Korver | +4.60 |
| 8 | Damian Lillard | +4.10 | +4.00 | 11 | +3 | Isaiah Thomas | +4.50 |
| 9 | George Hill | +3.78 | +3.90 | 12 | +3 | Anthony Davis | +4.30 |
| 10 | Klay Thompson | +3.67 | +4.30 | 10 | +0 | Klay Thompson | +4.30 |
| 11 | Blake Griffin | +3.35 | +3.20 | 22 | +11 | Damian Lillard | +4.00 |
| 12 | Mike Conley | +3.15 | +2.40 | 32 | +20 | George Hill | +3.90 |
| 13 | Gordon Hayward | +3.07 | +3.20 | 21 | +8 | Carmelo Anthony | +3.80 |
| 14 | Jimmy Butler | +3.03 | +3.20 | 20 | +6 | Ty Lawson | +3.80 |
| 15 | Kyle Korver | +2.85 | +4.60 | 7 | -8 | Kawhi Leonard | +3.70 |
| 16 | Ty Lawson | +2.84 | +3.80 | 14 | -2 | Rudy Gay | +3.50 |
| 17 | Brandon Jennings | +2.70 | +3.10 | 23 | +6 | DeAndre Jordan | +3.40 |
| 18 | Carmelo Anthony | +2.69 | +3.80 | 13 | -5 | Kyle Lowry | +3.30 |
| 19 | Anthony Davis | +2.67 | +4.30 | 9 | -10 | Jrue Holiday | +3.30 |
| 20 | Jrue Holiday | +2.63 | +3.30 | 19 | -1 | Jimmy Butler | +3.20 |
| 21 | Kawhi Leonard | +2.62 | +3.70 | 15 | -6 | Gordon Hayward | +3.20 |
| 22 | Kyle Lowry | +2.59 | +3.30 | 18 | -4 | Blake Griffin | +3.20 |
| 23 | JJ Redick | +2.37 | +2.50 | 29 | +6 | Brandon Jennings | +3.10 |
| 24 | Dirk Nowitzki | +2.36 | +2.20 | 34 | +10 | Danny Green | +3.10 |
| 25 | Dwyane Wade | +2.28 | +2.00 | 42 | +17 | Danilo Gallinari | +2.80 |
| 26 | Danilo Gallinari | +2.25 | +2.80 | 25 | -1 | Anthony Morrow | +2.70 |
| 27 | Jeff Teague | +2.18 | +2.20 | 35 | +8 | Chandler Parsons | +2.60 |
| 28 | Reggie Jackson | +1.96 | +2.00 | 41 | +13 | Tyreke Evans | +2.60 |
| 29 | Danny Green | +1.88 | +3.10 | 24 | -5 | JJ Redick | +2.50 |
| 30 | Rudy Gay | +1.83 | +3.50 | 16 | -14 | LaMarcus Aldridge | +2.40 |
| 31 | Anthony Morrow | +1.81 | +2.70 | 26 | -5 | Patrick Patterson | +2.40 |
| 32 | John Wall | +1.79 | +2.10 | 39 | +7 | Mike Conley | +2.40 |
| 33 | Aaron Brooks | +1.73 | +1.60 | 56 | +23 | Wesley Matthews | +2.30 |
| 34 | LaMarcus Aldridge | +1.69 | +2.40 | 30 | -4 | Dirk Nowitzki | +2.20 |
| 35 | Gerald Green | +1.65 | +2.20 | 36 | +1 | Jeff Teague | +2.20 |
| 36 | JJ Barea | +1.54 | +1.90 | 46 | +10 | Gerald Green | +2.20 |
| 37 | Khris Middleton | +1.53 | +1.70 | 51 | +14 | Devin Harris | +2.10 |
| 38 | Darren Collison | +1.50 | +1.70 | 55 | +17 | JR Smith | +2.10 |
| 39 | Tyreke Evans | +1.45 | +2.60 | 28 | -11 | John Wall | +2.10 |
| 40 | Chandler Parsons | +1.43 | +2.60 | 27 | -13 | Ersan Ilyasova | +2.10 |
| 41 | Marc Gasol | +1.32 | +1.40 | 60 | +19 | Reggie Jackson | +2.00 |
| 42 | Manu Ginobili | +1.31 | +1.70 | 54 | +12 | Dwyane Wade | +2.00 |
| 43 | Kevin Love | +1.21 | +1.70 | 52 | +9 | DeMarre Carroll | +1.90 |
| 44 | Devin Harris | +1.17 | +2.10 | 37 | -7 | Nikola Mirotic | +1.90 |
| 45 | Goran Dragic | +1.13 | +1.90 | 45 | +0 | Goran Dragic | +1.90 |
| 46 | Patrick Patterson | +1.13 | +2.40 | 31 | -15 | JJ Barea | +1.90 |
| 47 | Jamal Crawford | +1.13 | +1.10 | 69 | +22 | Joe Johnson | +1.80 |
| 48 | Wesley Matthews | +1.12 | +2.30 | 33 | -15 | Luol Deng | +1.80 |
| 49 | JR Smith | +1.10 | +2.10 | 38 | -11 | Jae Crowder | +1.80 |
| 50 | Bradley Beal | +1.06 | +0.80 | 78 | +28 | Eric Bledsoe | +1.70 |

### 2013-14 regular season — defense, top 50

> pool 247 · tau(true30) +0.407 · hits@30 21/30 · dev@10 5.00 · MAE 0.696

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Andrew Bogut | +4.50 | +4.40 | 4 | +3 | Kawhi Leonard | +5.00 |
| 2 | Draymond Green | +4.12 | +4.60 | 2 | +0 | Draymond Green | +4.60 |
| 3 | Kevin Garnett | +3.93 | +3.50 | 11 | +8 | Joakim Noah | +4.50 |
| 4 | Tiago Splitter | +3.79 | +4.20 | 6 | +2 | Andrew Bogut | +4.40 |
| 5 | Paul George | +3.68 | +2.90 | 21 | +16 | Michael KiddGilchrist | +4.40 |
| 6 | Anderson Varejao | +3.63 | +3.60 | 10 | +4 | Tiago Splitter | +4.20 |
| 7 | Andre Iguodala | +3.41 | +2.60 | 25 | +18 | Danny Green | +4.00 |
| 8 | Kawhi Leonard | +3.34 | +5.00 | 1 | -7 | Chris Paul | +3.90 |
| 9 | Nene | +3.32 | +3.80 | 9 | +0 | Nene | +3.80 |
| 10 | Joakim Noah | +3.17 | +4.50 | 3 | -7 | Anderson Varejao | +3.60 |
| 11 | CJ Watson | +3.12 | +3.20 | 17 | +6 | Kevin Garnett | +3.50 |
| 12 | Marcin Gortat | +2.99 | +2.50 | 29 | +17 | Nick Calathes | +3.50 |
| 13 | Danny Green | +2.96 | +4.00 | 7 | -6 | Ian Mahinmi | +3.50 |
| 14 | Jae Crowder | +2.89 | +3.00 | 19 | +5 | Jimmy Butler | +3.40 |
| 15 | Tony Allen | +2.85 | +2.40 | 31 | +16 | Roy Hibbert | +3.40 |
| 16 | Paul Pierce | +2.84 | +1.80 | 50 | +34 | DeMarcus Cousins | +3.30 |
| 17 | Ian Mahinmi | +2.82 | +3.50 | 13 | -4 | CJ Watson | +3.20 |
| 18 | Derek Fisher | +2.78 | +2.00 | 43 | +25 | Tim Duncan | +3.00 |
| 19 | Jimmy Butler | +2.68 | +3.40 | 14 | -5 | Jae Crowder | +3.00 |
| 20 | Paul Millsap | +2.66 | +2.70 | 23 | +3 | Kris Humphries | +3.00 |
| 21 | Chris Bosh | +2.65 | +1.00 | 75 | +54 | Paul George | +2.90 |
| 22 | Iman Shumpert | +2.59 | +1.00 | 73 | +51 | Marc Gasol | +2.80 |
| 23 | David West | +2.57 | +1.50 | 57 | +34 | Paul Millsap | +2.70 |
| 24 | Kirk Hinrich | +2.57 | +2.10 | 42 | +18 | Shane Battier | +2.70 |
| 25 | Anthony Davis | +2.55 | +2.30 | 34 | +9 | Andre Iguodala | +2.60 |
| 26 | Gerald Wallace | +2.47 | +1.60 | 52 | +26 | DeMarre Carroll | +2.60 |
| 27 | Michael KiddGilchrist | +2.46 | +4.40 | 5 | -22 | Mario Chalmers | +2.50 |
| 28 | Tim Duncan | +2.45 | +3.00 | 18 | -10 | Samuel Dalembert | +2.50 |
| 29 | Roy Hibbert | +2.43 | +3.40 | 15 | -14 | Marcin Gortat | +2.50 |
| 30 | DeMarcus Cousins | +2.37 | +3.30 | 16 | -14 | Victor Oladipo | +2.40 |
| 31 | DeMarre Carroll | +2.32 | +2.60 | 26 | -5 | Tony Allen | +2.40 |
| 32 | Darrell Arthur | +2.30 | +1.90 | 45 | +13 | Dwight Howard | +2.40 |
| 33 | Al Jefferson | +2.29 | +1.80 | 47 | +14 | Serge Ibaka | +2.30 |
| 34 | DeAndre Jordan | +2.29 | +1.10 | 65 | +31 | Anthony Davis | +2.30 |
| 35 | Kyle OQuinn | +2.29 | +1.60 | 53 | +18 | Kemba Walker | +2.30 |
| 36 | Chris Paul | +2.28 | +3.90 | 8 | -28 | Thabo Sefolosha | +2.30 |
| 37 | LaMarcus Aldridge | +2.26 | +2.20 | 37 | +0 | LaMarcus Aldridge | +2.20 |
| 38 | Chris Andersen | +2.22 | +1.60 | 55 | +17 | Nikola Pekovic | +2.20 |
| 39 | Manu Ginobili | +2.17 | +1.10 | 64 | +25 | Eric Bledsoe | +2.20 |
| 40 | Thabo Sefolosha | +2.11 | +2.30 | 36 | -4 | George Hill | +2.10 |
| 41 | Kemba Walker | +2.06 | +2.30 | 35 | -6 | Kosta Koufos | +2.10 |
| 42 | Marc Gasol | +2.05 | +2.80 | 22 | -20 | Kirk Hinrich | +2.10 |
| 43 | Ersan Ilyasova | +2.04 | +0.70 | 90 | +47 | Derek Fisher | +2.00 |
| 44 | Shane Battier | +2.01 | +2.70 | 24 | -20 | Robin Lopez | +2.00 |
| 45 | Amir Johnson | +2.00 | +1.20 | 63 | +18 | Darrell Arthur | +1.90 |
| 46 | George Hill | +2.00 | +2.10 | 40 | -6 | Patrick Beverley | +1.90 |
| 47 | Shaun Livingston | +1.97 | +0.90 | 77 | +30 | Al Jefferson | +1.80 |
| 48 | Robin Lopez | +1.88 | +2.00 | 44 | -4 | Jeremy Lin | +1.80 |
| 49 | Nick Collison | +1.84 | +0.70 | 88 | +39 | Ricky Rubio | +1.80 |
| 50 | Blake Griffin | +1.83 | +0.30 | 113 | +63 | Paul Pierce | +1.80 |

### 2014-15 regular season — defense, top 50

> pool 247 · tau(true30) +0.614 · hits@30 21/30 · dev@10 5.00 · MAE 0.696

| pos | projected | est | true | true rank | Δrank | actual at pos | true |
|---:|---|---:|---:|---:|---:|---|---:|
| 1 | Draymond Green | +5.11 | +5.10 | 2 | +1 | Kawhi Leonard | +5.20 |
| 2 | Tony Allen | +4.81 | +4.80 | 4 | +2 | Draymond Green | +5.10 |
| 3 | Rudy Gobert | +4.51 | +4.80 | 3 | +0 | Rudy Gobert | +4.80 |
| 4 | Andrew Bogut | +4.40 | +4.70 | 5 | +1 | Tony Allen | +4.80 |
| 5 | Kawhi Leonard | +3.80 | +5.20 | 1 | -4 | Andrew Bogut | +4.70 |
| 6 | Nerlens Noel | +3.56 | +2.70 | 19 | +13 | Anthony Davis | +4.50 |
| 7 | Andre Roberson | +3.48 | +3.40 | 10 | +3 | DeMarcus Cousins | +4.40 |
| 8 | Michael KiddGilchrist | +3.39 | +3.00 | 14 | +6 | Marcin Gortat | +3.60 |
| 9 | Anthony Davis | +3.29 | +4.50 | 6 | -3 | Tim Duncan | +3.50 |
| 10 | Zaza Pachulia | +3.17 | +3.20 | 12 | +2 | Andre Roberson | +3.40 |
| 11 | Nene | +3.14 | +2.80 | 18 | +7 | Kosta Koufos | +3.30 |
| 12 | DeMarcus Cousins | +3.09 | +4.40 | 7 | -5 | Zaza Pachulia | +3.20 |
| 13 | Kosta Koufos | +2.91 | +3.30 | 11 | -2 | Khris Middleton | +3.10 |
| 14 | Khris Middleton | +2.90 | +3.10 | 13 | -1 | Michael KiddGilchrist | +3.00 |
| 15 | Marcus Smart | +2.86 | +1.80 | 43 | +28 | Serge Ibaka | +3.00 |
| 16 | AlFarouq Aminu | +2.85 | +2.60 | 24 | +8 | Danny Green | +3.00 |
| 17 | Jared Dudley | +2.77 | +1.80 | 44 | +27 | Jonas Jerebko | +2.80 |
| 18 | Danny Green | +2.72 | +3.00 | 16 | -2 | Nene | +2.80 |
| 19 | Tim Duncan | +2.70 | +3.50 | 9 | -10 | Nerlens Noel | +2.70 |
| 20 | Iman Shumpert | +2.64 | +2.30 | 32 | +12 | Tyson Chandler | +2.60 |
| 21 | Marcin Gortat | +2.54 | +3.60 | 8 | -13 | Marc Gasol | +2.60 |
| 22 | Michael CarterWilliams | +2.52 | +2.30 | 31 | +9 | Joakim Noah | +2.60 |
| 23 | Jonas Jerebko | +2.51 | +2.80 | 17 | -6 | Josh Smith | +2.60 |
| 24 | Tyson Chandler | +2.51 | +2.60 | 20 | -4 | AlFarouq Aminu | +2.60 |
| 25 | Josh Smith | +2.47 | +2.60 | 23 | -2 | Alex Len | +2.50 |
| 26 | Ersan Ilyasova | +2.41 | +0.40 | 100 | +74 | Paul Millsap | +2.40 |
| 27 | Andre Iguodala | +2.31 | +1.60 | 47 | +20 | Omer Asik | +2.40 |
| 28 | Dwight Howard | +2.31 | +1.90 | 41 | +13 | Timofey Mozgov | +2.40 |
| 29 | Wesley Matthews | +2.29 | +1.30 | 56 | +27 | Darren Collison | +2.30 |
| 30 | Greg Monroe | +2.27 | +0.20 | 111 | +81 | Luc Mbah a Moute | +2.30 |
| 31 | Nikola Mirotic | +2.24 | +2.20 | 33 | +2 | Michael CarterWilliams | +2.30 |
| 32 | Kelly Olynyk | +2.13 | +2.10 | 36 | +4 | Iman Shumpert | +2.30 |
| 33 | Derrick Favors | +2.12 | +2.10 | 34 | +1 | Nikola Mirotic | +2.20 |
| 34 | Manu Ginobili | +2.06 | +1.40 | 52 | +18 | Derrick Favors | +2.10 |
| 35 | Timofey Mozgov | +2.06 | +2.40 | 28 | -7 | Chris Paul | +2.10 |
| 36 | Jimmy Butler | +2.04 | -0.20 | 140 | +104 | Kelly Olynyk | +2.10 |
| 37 | John Henson | +1.96 | +0.50 | 88 | +51 | Cody Zeller | +2.10 |
| 38 | Paul Millsap | +1.90 | +2.40 | 26 | -12 | Roy Hibbert | +2.00 |
| 39 | Joakim Noah | +1.89 | +2.60 | 22 | -17 | Steven Adams | +2.00 |
| 40 | Omer Asik | +1.85 | +2.40 | 27 | -13 | LaMarcus Aldridge | +1.90 |
| 41 | PJ Tucker | +1.82 | +1.30 | 54 | +13 | Dwight Howard | +1.90 |
| 42 | Zach Randolph | +1.80 | +1.30 | 59 | +17 | Pablo Prigioni | +1.80 |
| 43 | Luc Mbah a Moute | +1.80 | +2.30 | 30 | -13 | Marcus Smart | +1.80 |
| 44 | Pau Gasol | +1.74 | +0.50 | 90 | +46 | Jared Dudley | +1.80 |
| 45 | Luis Scola | +1.70 | +0.60 | 84 | +39 | George Hill | +1.70 |
| 46 | George Hill | +1.69 | +1.70 | 45 | -1 | Al Horford | +1.60 |
| 47 | Cory Joseph | +1.69 | +0.50 | 85 | +38 | Andre Iguodala | +1.60 |
| 48 | Donatas Motiejunas | +1.65 | +0.80 | 78 | +30 | Kevin Love | +1.60 |
| 49 | Trevor Ariza | +1.63 | +0.30 | 110 | +61 | Mario Chalmers | +1.50 |
| 50 | Jae Crowder | +1.62 | +0.30 | 102 | +52 | Kris Humphries | +1.50 |

## Projected boards, 2023-26 regular seasons (no truth exists)

### 2023-24 regular season — offense, top 50 (projected)

> pool 248 above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokic | +9.21 | 2737 |
| 2 | Luka Doncic | +8.55 | 2624 |
| 3 | Jalen Brunson | +7.13 | 2726 |
| 4 | Shai Gilgeous-Alexander | +6.94 | 2553 |
| 5 | Tyrese Haliburton | +6.71 | 2224 |
| 6 | Stephen Curry | +6.11 | 2421 |
| 7 | Damian Lillard | +5.97 | 2579 |
| 8 | Donovan Mitchell | +5.80 | 1943 |
| 9 | Devin Booker | +5.79 | 2447 |
| 10 | LeBron James | +5.70 | 2504 |
| 11 | Giannis Antetokounmpo | +5.66 | 2567 |
| 12 | Trae Young | +5.62 | 1942 |
| 13 | Joel Embiid | +5.46 | 1309 |
| 14 | Jamal Murray | +5.33 | 1861 |
| 15 | Kyrie Irving | +5.30 | 2030 |
| 16 | Jayson Tatum | +5.09 | 2645 |
| 17 | James Harden | +5.06 | 2470 |
| 18 | Anthony Edwards | +4.40 | 2770 |
| 19 | De'Aaron Fox | +4.40 | 2659 |
| 20 | Tyrese Maxey | +4.38 | 2626 |
| 21 | Collin Sexton | +4.24 | 2075 |
| 22 | Lauri Markkanen | +4.07 | 1820 |
| 23 | Kawhi Leonard | +4.05 | 2330 |
| 24 | Paul George | +4.02 | 2502 |
| 25 | Jimmy Butler | +3.83 | 2042 |
| 26 | Fred VanVleet | +3.83 | 2684 |
| 27 | Kevin Durant | +3.82 | 2791 |
| 28 | T.J. McConnell | +3.80 | 1291 |
| 29 | DeMar DeRozan | +3.61 | 2989 |
| 30 | Payton Pritchard | +3.50 | 1826 |
| 31 | D'Angelo Russell | +3.45 | 2484 |
| 32 | Desmond Bane | +3.32 | 1443 |
| 33 | CJ McCollum | +3.31 | 2159 |
| 34 | Zion Williamson | +3.17 | 2207 |
| 35 | Donte DiVincenzo | +3.16 | 2360 |
| 36 | Malcolm Brogdon | +2.94 | 1121 |
| 37 | Anfernee Simons | +2.91 | 1582 |
| 38 | Julius Randle | +2.88 | 1630 |
| 39 | Pascal Siakam | +2.79 | 2658 |
| 40 | Mike Conley | +2.76 | 2193 |
| 41 | Bogdan Bogdanovic | +2.73 | 2401 |
| 42 | Khris Middleton | +2.70 | 1487 |
| 43 | Dejounte Murray | +2.67 | 2783 |
| 44 | Derrick White | +2.54 | 2381 |
| 45 | Domantas Sabonis | +2.44 | 2928 |
| 46 | Sam Merrill | +2.43 | 1069 |
| 47 | Terry Rozier | +2.42 | 2040 |
| 48 | Immanuel Quickley | +2.41 | 1985 |
| 49 | Anthony Davis | +2.32 | 2700 |
| 50 | Malik Monk | +2.31 | 1872 |

### 2024-25 regular season — offense, top 50 (projected)

> pool 257 above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.61 | 2571 |
| 2 | Shai Gilgeous-Alexander | +8.17 | 2598 |
| 3 | Stephen Curry | +7.20 | 2252 |
| 4 | Luka Dončić | +6.53 | 1769 |
| 5 | Tyrese Haliburton | +6.12 | 2451 |
| 6 | Jalen Brunson | +5.89 | 2301 |
| 7 | Damian Lillard | +5.85 | 2093 |
| 8 | LaMelo Ball | +5.81 | 1505 |
| 9 | Donovan Mitchell | +5.80 | 2232 |
| 10 | Giannis Antetokounmpo | +5.75 | 2289 |
| 11 | Ty Jerome | +5.47 | 1393 |
| 12 | James Harden | +5.43 | 2789 |
| 13 | Jayson Tatum | +5.19 | 2624 |
| 14 | Trae Young | +5.16 | 2739 |
| 15 | Darius Garland | +5.07 | 2301 |
| 16 | Tyler Herro | +4.95 | 2725 |
| 17 | Cade Cunningham | +4.54 | 2452 |
| 18 | Jamal Murray | +4.49 | 2418 |
| 19 | Anthony Edwards | +4.27 | 2871 |
| 20 | Austin Reaves | +4.06 | 2550 |
| 21 | Ja Morant | +4.05 | 1519 |
| 22 | Jimmy Butler | +3.91 | 1746 |
| 23 | Payton Pritchard | +3.80 | 2271 |
| 24 | Devin Booker | +3.79 | 2795 |
| 25 | Tyrese Maxey | +3.44 | 1960 |
| 26 | LeBron James | +3.38 | 2444 |
| 27 | Kyrie Irving | +3.37 | 1804 |
| 28 | Karl-Anthony Towns | +3.23 | 2517 |
| 29 | Franz Wagner | +3.20 | 2023 |
| 30 | Kevin Durant | +3.17 | 2265 |
| 31 | Norman Powell | +2.97 | 1958 |
| 32 | Christian Braun | +2.94 | 2675 |
| 33 | DeMar DeRozan | +2.90 | 2768 |
| 34 | Isaiah Joe | +2.89 | 1604 |
| 35 | Domantas Sabonis | +2.81 | 2429 |
| 36 | Malik Beasley | +2.74 | 2283 |
| 37 | Jalen Green | +2.68 | 2697 |
| 38 | Paolo Banchero | +2.66 | 1582 |
| 39 | Cameron Johnson | +2.65 | 1800 |
| 40 | Desmond Bane | +2.63 | 2205 |
| 41 | Derrick White | +2.56 | 2574 |
| 42 | Cameron Payne | +2.55 | 1090 |
| 43 | Kawhi Leonard | +2.49 | 1180 |
| 44 | Aaron Gordon | +2.49 | 1447 |
| 45 | Jaylen Brown | +2.39 | 2158 |
| 46 | Daniel Gafford | +2.36 | 1226 |
| 47 | Luke Kennard | +2.31 | 1472 |
| 48 | Chris Paul | +2.28 | 2292 |
| 49 | Jordan Poole | +2.26 | 2001 |
| 50 | Deni Avdija | +2.26 | 2161 |

### 2025-26 regular season — offense, top 50 (projected)

> pool 269 above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Nikola Jokić | +9.19 | 2265 |
| 2 | Luka Dončić | +8.27 | 2289 |
| 3 | Shai Gilgeous-Alexander | +8.09 | 2259 |
| 4 | Donovan Mitchell | +6.85 | 2342 |
| 5 | Kawhi Leonard | +6.77 | 2085 |
| 6 | Jamal Murray | +6.59 | 2652 |
| 7 | Stephen Curry | +6.46 | 1329 |
| 8 | James Harden | +6.44 | 2438 |
| 9 | LaMelo Ball | +6.34 | 2017 |
| 10 | Jalen Brunson | +5.82 | 2590 |
| 11 | Cade Cunningham | +5.33 | 2172 |
| 12 | Jimmy Butler III | +5.29 | 1182 |
| 13 | Tyrese Maxey | +5.08 | 2661 |
| 14 | Anthony Edwards | +4.91 | 2137 |
| 15 | Devin Booker | +4.77 | 2146 |
| 16 | Deni Avdija | +4.59 | 2199 |
| 17 | Kevin Durant | +4.36 | 2840 |
| 18 | Austin Reaves | +4.35 | 1762 |
| 19 | Jaylen Brown | +4.14 | 2443 |
| 20 | Jalen Duren | +4.11 | 1976 |
| 21 | Victor Wembanyama | +4.09 | 1866 |
| 22 | Payton Pritchard | +4.06 | 2556 |
| 23 | Joel Embiid | +4.06 | 1201 |
| 24 | Coby White | +3.95 | 1250 |
| 25 | Michael Porter Jr. | +3.87 | 1689 |
| 26 | Duncan Robinson | +3.57 | 2113 |
| 27 | Cam Spencer | +3.53 | 1714 |
| 28 | Jrue Holiday | +3.51 | 1560 |
| 29 | Kon Knueppel | +3.42 | 2551 |
| 30 | Lauri Markkanen | +3.41 | 1443 |
| 31 | Keyonte George | +3.41 | 1786 |
| 32 | Isaiah Joe | +3.24 | 1507 |
| 33 | Luka Garza | +3.23 | 1118 |
| 34 | De'Aaron Fox | +3.20 | 2231 |
| 35 | Grayson Allen | +3.15 | 1467 |
| 36 | Collin Gillespie | +3.13 | 2282 |
| 37 | Anfernee Simons | +2.97 | 1372 |
| 38 | Bones Hyland | +2.94 | 1177 |
| 39 | Darius Garland | +2.92 | 1344 |
| 40 | Alperen Sengun | +2.91 | 2398 |
| 41 | Julius Randle | +2.89 | 2610 |
| 42 | Immanuel Quickley | +2.88 | 2231 |
| 43 | Reed Sheppard | +2.83 | 2147 |
| 44 | Paul George | +2.79 | 1135 |
| 45 | Brandon Miller | +2.77 | 1968 |
| 46 | Nickeil Alexander-Walker | +2.74 | 2603 |
| 47 | Sam Merrill | +2.70 | 1377 |
| 48 | Norman Powell | +2.68 | 1717 |
| 49 | Miles McBride | +2.61 | 1080 |
| 50 | Trey Murphy III | +2.58 | 2341 |

### 2023-24 regular season — defense, top 50 (projected)

> pool 248 above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +3.77 | 2593 |
| 2 | Joel Embiid | +3.36 | 1309 |
| 3 | Alex Caruso | +3.33 | 2040 |
| 4 | Victor Wembanyama | +3.28 | 2106 |
| 5 | Isaiah Hartenstein | +3.21 | 1896 |
| 6 | Kristaps Porzingis | +3.17 | 1690 |
| 7 | Dean Wade | +3.15 | 1108 |
| 8 | Jusuf Nurkic | +3.02 | 2078 |
| 9 | Nic Claxton | +2.90 | 2116 |
| 10 | Nikola Jokic | +2.80 | 2737 |
| 11 | Brook Lopez | +2.72 | 2411 |
| 12 | Draymond Green | +2.60 | 1490 |
| 13 | Matisse Thybulle | +2.59 | 1487 |
| 14 | Toumani Camara | +2.57 | 1739 |
| 15 | Chet Holmgren | +2.56 | 2413 |
| 16 | Evan Mobley | +2.52 | 1532 |
| 17 | Anthony Davis | +2.41 | 2700 |
| 18 | Larry Nance Jr. | +2.40 | 1216 |
| 19 | Ivica Zubac | +2.35 | 1795 |
| 20 | Andre Drummond | +2.34 | 1351 |
| 21 | Amen Thompson | +2.33 | 1388 |
| 22 | Paul George | +2.25 | 2502 |
| 23 | Vince Williams Jr. | +2.21 | 1436 |
| 24 | Paul Reed | +2.17 | 1590 |
| 25 | Jarrett Allen | +2.15 | 2442 |
| 26 | Ausar Thompson | +2.14 | 1583 |
| 27 | Kawhi Leonard | +2.11 | 2330 |
| 28 | Derrick White | +2.05 | 2381 |
| 29 | Derrick Jones Jr. | +1.99 | 1783 |
| 30 | Wendell Carter Jr. | +1.98 | 1406 |
| 31 | Luguentz Dort | +1.97 | 2246 |
| 32 | Bam Adebayo | +1.94 | 2416 |
| 33 | Jalen Suggs | +1.94 | 2025 |
| 34 | OG Anunoby | +1.93 | 1702 |
| 35 | Clint Capela | +1.90 | 1883 |
| 36 | Franz Wagner | +1.88 | 2337 |
| 37 | Aaron Nesmith | +1.87 | 1995 |
| 38 | Daniel Gafford | +1.86 | 1814 |
| 39 | Dyson Daniels | +1.85 | 1358 |
| 40 | Walker Kessler | +1.72 | 1493 |
| 41 | Myles Turner | +1.71 | 2077 |
| 42 | Peyton Watson | +1.68 | 1488 |
| 43 | Jakob Poeltl | +1.68 | 1319 |
| 44 | Nickeil Alexander-Walker | +1.62 | 1921 |
| 45 | John Konchar | +1.57 | 1173 |
| 46 | Isaiah Joe | +1.55 | 1445 |
| 47 | Naz Reid | +1.47 | 1964 |
| 48 | Herbert Jones | +1.43 | 2321 |
| 49 | Shai Gilgeous-Alexander | +1.42 | 2553 |
| 50 | Brandin Podziemski | +1.41 | 1968 |

### 2024-25 regular season — defense, top 50 (projected)

> pool 257 above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.23 | 2388 |
| 2 | Victor Wembanyama | +3.80 | 1527 |
| 3 | Toumani Camara | +3.22 | 2548 |
| 4 | Ivica Zubac | +3.22 | 2624 |
| 5 | Luke Kornet | +3.16 | 1361 |
| 6 | Alperen Sengun | +3.06 | 2394 |
| 7 | Jarrett Allen | +2.98 | 2296 |
| 8 | Donovan Clingan | +2.94 | 1324 |
| 9 | Ausar Thompson | +2.94 | 1328 |
| 10 | Jaxson Hayes | +2.94 | 1093 |
| 11 | Kris Dunn | +2.83 | 1783 |
| 12 | Luguentz Dort | +2.72 | 2073 |
| 13 | Evan Mobley | +2.72 | 2167 |
| 14 | Isaiah Hartenstein | +2.64 | 1590 |
| 15 | Kristaps Porziņģis | +2.56 | 1210 |
| 16 | Dyson Daniels | +2.51 | 2571 |
| 17 | Brandin Podziemski | +2.35 | 1716 |
| 18 | Draymond Green | +2.34 | 1983 |
| 19 | Jonathan Isaac | +2.29 | 1090 |
| 20 | Brandon Clarke | +2.24 | 1207 |
| 21 | Isaiah Stewart | +2.20 | 1434 |
| 22 | Jaden McDaniels | +2.17 | 2614 |
| 23 | Kevon Looney | +2.14 | 1142 |
| 24 | Nicolas Batum | +2.12 | 1367 |
| 25 | Amen Thompson | +2.04 | 2225 |
| 26 | Daniel Gafford | +1.91 | 1226 |
| 27 | Myles Turner | +1.90 | 2174 |
| 28 | Wendell Carter Jr. | +1.90 | 1758 |
| 29 | Walker Kessler | +1.86 | 1740 |
| 30 | Jaren Jackson Jr. | +1.83 | 2207 |
| 31 | Cason Wallace | +1.82 | 1876 |
| 32 | Sam Merrill | +1.80 | 1401 |
| 33 | Anthony Davis | +1.79 | 1706 |
| 34 | Cody Martin | +1.77 | 1173 |
| 35 | Dean Wade | +1.74 | 1252 |
| 36 | Kentavious Caldwell-Pope | +1.71 | 2279 |
| 37 | Scotty Pippen Jr. | +1.69 | 1683 |
| 38 | Keon Ellis | +1.69 | 1948 |
| 39 | Luka Dončić | +1.67 | 1769 |
| 40 | Goga Bitadze | +1.66 | 1430 |
| 41 | Donte DiVincenzo | +1.65 | 1606 |
| 42 | Paul George | +1.60 | 1334 |
| 43 | Jalen Johnson | +1.59 | 1284 |
| 44 | Derrick White | +1.56 | 2574 |
| 45 | Shai Gilgeous-Alexander | +1.55 | 2598 |
| 46 | Jakob Poeltl | +1.53 | 1686 |
| 47 | Haywood Highsmith | +1.50 | 1818 |
| 48 | Kenrich Williams | +1.49 | 1132 |
| 49 | Tari Eason | +1.46 | 1420 |
| 50 | Jrue Holiday | +1.44 | 1896 |

### 2025-26 regular season — defense, top 50 (projected)

> pool 269 above the minutes floor

| pos | projected | est | mp |
|---:|---|---:|---:|
| 1 | Victor Wembanyama | +5.55 | 1866 |
| 2 | Neemias Queta | +4.10 | 1926 |
| 3 | Chet Holmgren | +4.03 | 1997 |
| 4 | Isaiah Hartenstein | +3.49 | 1137 |
| 5 | Rudy Gobert | +3.31 | 2380 |
| 6 | Ausar Thompson | +3.29 | 1896 |
| 7 | Cason Wallace | +3.17 | 2046 |
| 8 | Derrick White | +3.13 | 2625 |
| 9 | Ronald Holland II | +3.09 | 1550 |
| 10 | Hugo González | +2.97 | 1084 |
| 11 | Dyson Daniels | +2.82 | 2520 |
| 12 | Javonte Green | +2.73 | 1446 |
| 13 | Ajay Mitchell | +2.70 | 1473 |
| 14 | Baylor Scheierman | +2.55 | 1429 |
| 15 | Marcus Smart | +2.53 | 1769 |
| 16 | Luke Kornet | +2.49 | 1430 |
| 17 | Dru Smith | +2.44 | 1141 |
| 18 | John Konchar | +2.41 | 1115 |
| 19 | Toumani Camara | +2.39 | 2731 |
| 20 | Jarrett Allen | +2.32 | 1519 |
| 21 | Sidy Cissoko | +2.32 | 1435 |
| 22 | Ryan Kalkbrenner | +2.30 | 1479 |
| 23 | Paul George | +2.21 | 1135 |
| 24 | Jalen Suggs | +2.20 | 1574 |
| 25 | Evan Mobley | +2.17 | 2074 |
| 26 | Jamal Shead | +2.15 | 1852 |
| 27 | Jordan Goodwin | +2.15 | 1572 |
| 28 | Keon Ellis | +2.10 | 1479 |
| 29 | Bam Adebayo | +2.08 | 2365 |
| 30 | Jaylin Williams | +2.05 | 1277 |
| 31 | Wendell Carter Jr. | +2.02 | 2288 |
| 32 | Ryan Dunn | +2.01 | 1355 |
| 33 | Collin Murray-Boyles | +1.97 | 1246 |
| 34 | Mitchell Robinson | +1.97 | 1175 |
| 35 | Sion James | +1.96 | 1843 |
| 36 | Donte DiVincenzo | +1.95 | 2494 |
| 37 | Naz Reid | +1.90 | 2007 |
| 38 | OG Anunoby | +1.87 | 2224 |
| 39 | Scottie Barnes | +1.86 | 2681 |
| 40 | Josh Okogie | +1.84 | 1354 |
| 41 | Mouhamed Gueye | +1.80 | 1179 |
| 42 | Kris Murray | +1.78 | 1333 |
| 43 | Collin Gillespie | +1.74 | 2282 |
| 44 | Jusuf Nurkić | +1.72 | 1083 |
| 45 | Luguentz Dort | +1.63 | 1849 |
| 46 | Jalen Duren | +1.63 | 1976 |
| 47 | Oso Ighodaro | +1.59 | 1808 |
| 48 | Dominick Barlow | +1.58 | 1689 |
| 49 | Landry Shamet | +1.55 | 1171 |
| 50 | Brandon Miller | +1.53 | 1968 |
