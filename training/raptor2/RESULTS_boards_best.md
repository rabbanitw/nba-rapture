# Best-model projected leaderboards vs actual (season-held-out)

Every row is an out-of-sample projection: the model never saw the season
it is ranking. Configs: **offense** = gbm + struct hats + linear-wide hats
(hats4-both) with playoff rows pooled into training (promoted 3/3 seed
sets, median dev@10 1.10 vs 1.45/1.55/1.55); **defense** = gbm + struct
hats (hats3) -- every attempted upgrade (component-hat swaps, unions,
playoff pooling, RAPM hat, historical box-D hat) regressed it.
Pools are >=1065 minutes. `dev` = |actual rank - projected rank|.

## offense

CV: median dev@10 **1.20**, mean 1.33, median tau@10 +0.733, hits@10 93/100.

| season | dev@10 | tau@10 | hits@10 |
|---|---:|---:|---:|
| 2013-14 | 1.00 | +0.733 | 10/10 |
| 2014-15 | 1.20 | +0.733 | 9/10 |
| 2015-16 | 1.00 | +0.778 | 10/10 |
| 2016-17 | 1.90 | +0.333 | 9/10 |
| 2017-18 | 1.60 | +0.689 | 9/10 |
| 2018-19 | 2.70 | +0.289 | 8/10 |
| 2019-20 | 0.60 | +0.867 | 10/10 |
| 2020-21 | 1.20 | +0.867 | 9/10 |
| 2021-22 | 1.20 | +0.733 | 10/10 |
| 2022-23 | 0.90 | +0.822 | 9/10 |

### 2013-14 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Kevin Durant | +7.54 | +7.60 | 1 | 0 |
| 2 | Chris Paul | +6.77 | +7.10 | 2 | 0 |
| 3 | LeBron James | +6.13 | +5.80 | 4 | 1 |
| 4 | James Harden | +5.68 | +6.30 | 3 | 1 |
| 5 | Kevin Love | +5.63 | +5.70 | 5 | 0 |
| 6 | Goran Dragic | +4.70 | +4.80 | 6 | 0 |
| 7 | Carmelo Anthony | +4.59 | +4.20 | 9 | 2 |
| 8 | Manu Ginobili | +4.19 | +4.00 | 10 | 2 |
| 9 | Dirk Nowitzki | +4.13 | +4.40 | 8 | 1 |
| 10 | Kyle Lowry | +4.01 | +4.40 | 7 | 3 |
| 11 | Blake Griffin | +3.73 | +2.90 | 17 | 6 |
| 12 | Isaiah Thomas | +3.53 | +3.50 | 14 | 2 |
| 13 | Damian Lillard | +3.48 | +3.60 | 12 | 1 |
| 14 | Mike Conley | +3.37 | +3.50 | 13 | 1 |
| 15 | Russell Westbrook | +3.22 | +3.30 | 15 | 0 |

### 2014-15 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +9.34 | +8.50 | 1 | 0 |
| 2 | James Harden | +8.21 | +7.70 | 2 | 0 |
| 3 | LeBron James | +6.12 | +5.30 | 5 | 2 |
| 4 | Russell Westbrook | +5.84 | +6.10 | 3 | 1 |
| 5 | Lou Williams | +5.48 | +5.20 | 6 | 1 |
| 6 | Kyrie Irving | +5.06 | +5.50 | 4 | 2 |
| 7 | Isaiah Thomas | +4.69 | +4.50 | 8 | 1 |
| 8 | Klay Thompson | +4.06 | +4.30 | 10 | 2 |
| 9 | Kyle Korver | +4.03 | +4.60 | 7 | 2 |
| 10 | Damian Lillard | +3.95 | +4.00 | 11 | 1 |
| 11 | George Hill | +3.86 | +3.90 | 12 | 1 |
| 12 | Blake Griffin | +3.83 | +3.20 | 22 | 10 |
| 13 | Carmelo Anthony | +3.72 | +3.80 | 13 | 0 |
| 14 | Gordon Hayward | +3.49 | +3.20 | 21 | 7 |
| 15 | Anthony Davis | +3.38 | +4.30 | 9 | 6 |

### 2015-16 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +8.19 | +7.50 | 1 | 0 |
| 2 | Kevin Durant | +6.50 | +6.20 | 3 | 1 |
| 3 | LeBron James | +6.29 | +6.10 | 4 | 1 |
| 4 | James Harden | +6.19 | +5.70 | 5 | 1 |
| 5 | Russell Westbrook | +5.76 | +6.40 | 2 | 3 |
| 6 | Kyle Lowry | +5.56 | +5.50 | 6 | 0 |
| 7 | Damian Lillard | +5.36 | +5.00 | 8 | 1 |
| 8 | Isaiah Thomas | +4.45 | +4.50 | 9 | 1 |
| 9 | Kawhi Leonard | +4.17 | +5.00 | 7 | 2 |
| 10 | Draymond Green | +3.78 | +4.20 | 10 | 0 |
| 11 | Reggie Jackson | +3.73 | +3.20 | 17 | 6 |
| 12 | Kemba Walker | +3.66 | +3.40 | 13 | 1 |
| 13 | JJ Redick | +3.24 | +3.10 | 18 | 5 |
| 14 | Kyrie Irving | +3.24 | +2.60 | 23 | 9 |
| 15 | Paul George | +3.17 | +2.70 | 22 | 7 |

### 2016-17 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Isaiah Thomas | +8.64 | +8.30 | 1 | 0 |
| 2 | Chris Paul | +8.12 | +7.50 | 4 | 2 |
| 3 | Russell Westbrook | +7.74 | +7.80 | 3 | 0 |
| 4 | James Harden | +7.38 | +7.90 | 2 | 2 |
| 5 | LeBron James | +6.72 | +6.20 | 10 | 5 |
| 6 | Damian Lillard | +6.32 | +6.20 | 9 | 3 |
| 7 | Kawhi Leonard | +6.27 | +6.60 | 6 | 1 |
| 8 | Kyrie Irving | +6.10 | +6.30 | 8 | 0 |
| 9 | Kyle Lowry | +6.07 | +6.70 | 5 | 4 |
| 10 | Mike Conley | +5.87 | +5.30 | 12 | 2 |
| 11 | Kemba Walker | +5.53 | +5.00 | 14 | 3 |
| 12 | Nikola Jokic | +5.27 | +6.40 | 7 | 5 |
| 13 | Kevin Durant | +5.24 | +5.20 | 13 | 0 |
| 14 | Bradley Beal | +4.95 | +4.90 | 15 | 1 |
| 15 | Jimmy Butler | +4.95 | +4.80 | 16 | 1 |

### 2017-18 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Stephen Curry | +8.81 | +9.20 | 1 | 0 |
| 2 | Chris Paul | +8.04 | +7.60 | 2 | 0 |
| 3 | Damian Lillard | +6.36 | +6.60 | 4 | 1 |
| 4 | LeBron James | +6.23 | +7.00 | 3 | 1 |
| 5 | Kevin Durant | +5.82 | +5.60 | 6 | 1 |
| 6 | Kemba Walker | +5.59 | +5.40 | 8 | 2 |
| 7 | Lou Williams | +5.33 | +5.20 | 10 | 3 |
| 8 | Kyrie Irving | +5.03 | +6.00 | 5 | 3 |
| 9 | Russell Westbrook | +4.95 | +4.90 | 11 | 2 |
| 10 | Jimmy Butler | +4.85 | +5.50 | 7 | 3 |
| 11 | Nikola Jokic | +4.76 | +5.20 | 9 | 2 |
| 12 | KarlAnthony Towns | +4.56 | +4.30 | 14 | 2 |
| 13 | Tyreke Evans | +4.54 | +4.40 | 13 | 0 |
| 14 | Kyle Lowry | +4.40 | +4.80 | 12 | 2 |
| 15 | DeMar DeRozan | +3.60 | +3.90 | 15 | 0 |

### 2018-19 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Stephen Curry | +8.36 | +7.50 | 2 | 1 |
| 2 | James Harden | +8.15 | +9.80 | 1 | 1 |
| 3 | Damian Lillard | +7.55 | +6.80 | 3 | 0 |
| 4 | Kevin Durant | +6.25 | +5.00 | 9 | 5 |
| 5 | Paul George | +5.63 | +5.30 | 7 | 2 |
| 6 | Kemba Walker | +5.62 | +5.00 | 10 | 4 |
| 7 | Kawhi Leonard | +5.19 | +4.70 | 12 | 5 |
| 8 | Kyrie Irving | +5.14 | +5.50 | 5 | 3 |
| 9 | Lou Williams | +4.95 | +5.10 | 8 | 1 |
| 10 | Giannis Antetokounmpo | +4.94 | +4.20 | 15 | 5 |
| 11 | Nikola Jokic | +4.92 | +5.60 | 4 | 7 |
| 12 | LeBron James | +4.89 | +5.40 | 6 | 6 |
| 13 | Danilo Gallinari | +4.33 | +4.70 | 11 | 2 |
| 14 | Mike Conley | +3.94 | +4.60 | 13 | 1 |
| 15 | Anthony Davis | +3.91 | +4.10 | 17 | 2 |

### 2019-20 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Damian Lillard | +8.17 | +8.60 | 1 | 0 |
| 2 | Luka Doncic | +7.11 | +7.70 | 2 | 0 |
| 3 | Trae Young | +7.10 | +7.10 | 3 | 0 |
| 4 | KarlAnthony Towns | +5.87 | +5.80 | 5 | 1 |
| 5 | LeBron James | +5.70 | +6.30 | 4 | 1 |
| 6 | Bradley Beal | +5.38 | +5.40 | 7 | 1 |
| 7 | Giannis Antetokounmpo | +4.92 | +5.50 | 6 | 1 |
| 8 | Nikola Jokic | +4.61 | +4.40 | 9 | 1 |
| 9 | Devin Booker | +4.56 | +4.80 | 8 | 1 |
| 10 | Khris Middleton | +3.50 | +3.50 | 10 | 0 |
| 11 | Spencer Dinwiddie | +3.28 | +3.40 | 11 | 0 |
| 12 | Jayson Tatum | +3.19 | +3.40 | 12 | 0 |
| 13 | George Hill | +3.08 | +3.00 | 14 | 1 |
| 14 | CJ McCollum | +2.61 | +2.60 | 18 | 4 |
| 15 | Patty Mills | +2.57 | +3.30 | 13 | 2 |

### 2020-21 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokic | +8.65 | +8.70 | 1 | 0 |
| 2 | Stephen Curry | +8.30 | +8.70 | 2 | 0 |
| 3 | Damian Lillard | +8.02 | +7.00 | 3 | 0 |
| 4 | James Harden | +6.86 | +6.50 | 5 | 1 |
| 5 | Luka Doncic | +6.34 | +6.80 | 4 | 1 |
| 6 | Trae Young | +6.31 | +5.80 | 7 | 1 |
| 7 | Kawhi Leonard | +5.83 | +6.40 | 6 | 1 |
| 8 | Kyrie Irving | +5.35 | +5.60 | 8 | 0 |
| 9 | LeBron James | +4.97 | +5.30 | 10 | 1 |
| 10 | Joel Embiid | +4.86 | +3.90 | 17 | 7 |
| 11 | Bradley Beal | +4.59 | +5.50 | 9 | 2 |
| 12 | Jimmy Butler | +4.48 | +4.40 | 14 | 2 |
| 13 | Donovan Mitchell | +4.42 | +4.70 | 12 | 1 |
| 14 | Jayson Tatum | +4.24 | +4.10 | 15 | 1 |
| 15 | Shai Gilgeous-Alexander | +4.17 | +3.70 | 22 | 7 |

### 2021-22 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Trae Young | +7.91 | +7.60 | 2 | 1 |
| 2 | Nikola Jokic | +7.82 | +9.00 | 1 | 1 |
| 3 | Luka Doncic | +5.58 | +5.80 | 3 | 0 |
| 4 | Stephen Curry | +5.27 | +5.70 | 5 | 1 |
| 5 | Giannis Antetokounmpo | +5.26 | +5.70 | 4 | 1 |
| 6 | Donovan Mitchell | +4.65 | +4.60 | 8 | 2 |
| 7 | Kevin Durant | +4.62 | +5.00 | 6 | 1 |
| 8 | LeBron James | +4.50 | +4.60 | 9 | 1 |
| 9 | Ja Morant | +4.49 | +4.60 | 10 | 1 |
| 10 | Jayson Tatum | +4.42 | +4.80 | 7 | 3 |
| 11 | Joel Embiid | +4.16 | +4.30 | 12 | 1 |
| 12 | Jrue Holiday | +4.05 | +4.00 | 15 | 3 |
| 13 | Darius Garland | +4.02 | +4.50 | 11 | 2 |
| 14 | Devin Booker | +4.01 | +4.30 | 13 | 1 |
| 15 | Karl-Anthony Towns | +3.97 | +3.40 | 19 | 4 |

### 2022-23 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokic | +9.10 | +10.00 | 1 | 0 |
| 2 | Damian Lillard | +8.35 | +9.30 | 2 | 0 |
| 3 | Luka Doncic | +7.84 | +8.30 | 3 | 0 |
| 4 | Stephen Curry | +7.66 | +7.50 | 4 | 0 |
| 5 | Tyrese Haliburton | +6.96 | +6.90 | 5 | 0 |
| 6 | Donovan Mitchell | +5.88 | +5.90 | 7 | 1 |
| 7 | Jimmy Butler | +5.65 | +6.10 | 6 | 1 |
| 8 | Devin Booker | +5.44 | +5.30 | 10 | 2 |
| 9 | James Harden | +5.37 | +5.50 | 9 | 0 |
| 10 | Kawhi Leonard | +5.05 | +4.70 | 15 | 5 |
| 11 | Shai Gilgeous-Alexander | +4.91 | +5.00 | 12 | 1 |
| 12 | Trae Young | +4.83 | +5.20 | 11 | 1 |
| 13 | Kyrie Irving | +4.68 | +5.60 | 8 | 5 |
| 14 | Jalen Brunson | +4.63 | +5.00 | 13 | 1 |
| 15 | De'Aaron Fox | +4.50 | +4.50 | 18 | 3 |

## defense

CV: median dev@10 **4.15**, mean 4.11, median tau@10 +0.533, hits@10 73/100.

| season | dev@10 | tau@10 | hits@10 |
|---|---:|---:|---:|
| 2013-14 | 5.50 | +0.333 | 6/10 |
| 2014-15 | 2.50 | +0.511 | 8/10 |
| 2015-16 | 4.30 | +0.644 | 8/10 |
| 2016-17 | 3.10 | +0.733 | 7/10 |
| 2017-18 | 4.60 | +0.600 | 7/10 |
| 2018-19 | 6.30 | +0.422 | 7/10 |
| 2019-20 | 4.20 | +0.378 | 8/10 |
| 2020-21 | 3.50 | +0.556 | 8/10 |
| 2021-22 | 3.00 | +0.644 | 7/10 |
| 2022-23 | 4.10 | +0.422 | 7/10 |

### 2013-14 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Tiago Splitter | +4.44 | +4.20 | 6 | 5 |
| 2 | Andrew Bogut | +4.02 | +4.40 | 4 | 2 |
| 3 | Joakim Noah | +3.63 | +4.50 | 3 | 0 |
| 4 | Draymond Green | +3.62 | +4.60 | 2 | 2 |
| 5 | Kawhi Leonard | +3.61 | +5.00 | 1 | 4 |
| 6 | Roy Hibbert | +3.56 | +3.40 | 15 | 9 |
| 7 | Ian Mahinmi | +3.56 | +3.50 | 13 | 6 |
| 8 | Marcin Gortat | +3.29 | +2.50 | 29 | 21 |
| 9 | Danny Green | +3.15 | +4.00 | 7 | 2 |
| 10 | Jimmy Butler | +3.03 | +3.40 | 14 | 4 |
| 11 | Paul George | +3.02 | +2.90 | 21 | 10 |
| 12 | Nene | +2.79 | +3.80 | 9 | 3 |
| 13 | Tony Allen | +2.76 | +2.40 | 31 | 18 |
| 14 | Kevin Garnett | +2.71 | +3.50 | 11 | 3 |
| 15 | Andre Iguodala | +2.63 | +2.60 | 25 | 10 |

### 2014-15 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Andrew Bogut | +4.98 | +4.70 | 5 | 4 |
| 2 | Draymond Green | +4.91 | +5.10 | 2 | 0 |
| 3 | Rudy Gobert | +4.66 | +4.80 | 3 | 0 |
| 4 | Tony Allen | +4.03 | +4.80 | 4 | 0 |
| 5 | DeMarcus Cousins | +3.90 | +4.40 | 7 | 2 |
| 6 | Anthony Davis | +3.71 | +4.50 | 6 | 0 |
| 7 | Kawhi Leonard | +3.34 | +5.20 | 1 | 6 |
| 8 | Marcin Gortat | +3.18 | +3.60 | 8 | 0 |
| 9 | Nerlens Noel | +3.12 | +2.70 | 19 | 10 |
| 10 | Khris Middleton | +2.93 | +3.10 | 13 | 3 |
| 11 | Andre Roberson | +2.78 | +3.40 | 10 | 1 |
| 12 | Kosta Koufos | +2.78 | +3.30 | 11 | 1 |
| 13 | Jonas Jerebko | +2.75 | +2.80 | 17 | 4 |
| 14 | Danny Green | +2.50 | +3.00 | 16 | 2 |
| 15 | Marcus Smart | +2.49 | +1.80 | 43 | 28 |

### 2015-16 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Tim Duncan | +4.52 | +5.30 | 1 | 0 |
| 2 | Draymond Green | +4.35 | +5.20 | 3 | 1 |
| 3 | Kawhi Leonard | +3.45 | +5.10 | 4 | 1 |
| 4 | Andrew Bogut | +3.34 | +2.90 | 12 | 8 |
| 5 | Nikola Jokic | +3.31 | +4.50 | 5 | 0 |
| 6 | Danny Green | +3.23 | +2.20 | 28 | 22 |
| 7 | Rudy Gobert | +3.15 | +3.50 | 7 | 0 |
| 8 | Luc Mbah a Moute | +3.11 | +3.50 | 8 | 0 |
| 9 | Steven Adams | +3.09 | +5.30 | 2 | 7 |
| 10 | DeMarcus Cousins | +2.97 | +4.10 | 6 | 4 |
| 11 | Clint Capela | +2.89 | +1.70 | 47 | 36 |
| 12 | Andre Drummond | +2.76 | +1.60 | 48 | 36 |
| 13 | Ronnie Price | +2.73 | +1.50 | 60 | 47 |
| 14 | Wesley Johnson | +2.65 | +1.80 | 41 | 27 |
| 15 | Ian Mahinmi | +2.45 | +2.90 | 13 | 2 |

### 2016-17 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +5.00 | +5.60 | 2 | 1 |
| 2 | Draymond Green | +4.43 | +6.20 | 1 | 1 |
| 3 | Dewayne Dedmon | +4.17 | +4.10 | 4 | 1 |
| 4 | Anthony Davis | +4.07 | +4.40 | 3 | 1 |
| 5 | Luc Mbah a Moute | +3.45 | +3.80 | 7 | 2 |
| 6 | Robert Covington | +3.14 | +3.80 | 6 | 0 |
| 7 | Amir Johnson | +2.95 | +3.60 | 8 | 1 |
| 8 | Myles Turner | +2.88 | +2.20 | 23 | 15 |
| 9 | James Johnson | +2.84 | +2.40 | 15 | 6 |
| 10 | Danny Green | +2.67 | +2.60 | 13 | 3 |
| 11 | Chris Paul | +2.61 | +1.40 | 55 | 44 |
| 12 | Zaza Pachulia | +2.56 | +0.90 | 74 | 62 |
| 13 | DeAndre Jordan | +2.52 | +1.60 | 45 | 32 |
| 14 | Andre Roberson | +2.52 | +4.00 | 5 | 9 |
| 15 | Nene | +2.43 | +3.50 | 9 | 6 |

### 2017-18 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +4.81 | +4.00 | 1 | 0 |
| 2 | Anthony Davis | +4.57 | +3.80 | 2 | 0 |
| 3 | Joel Embiid | +4.28 | +3.40 | 6 | 3 |
| 4 | Jusuf Nurkic | +3.73 | +2.60 | 17 | 13 |
| 5 | Larry Nance Jr. | +3.55 | +2.60 | 18 | 13 |
| 6 | Victor Oladipo | +3.41 | +3.50 | 5 | 1 |
| 7 | Aron Baynes | +3.23 | +3.70 | 3 | 4 |
| 8 | Clint Capela | +3.18 | +2.80 | 14 | 6 |
| 9 | Robert Covington | +3.10 | +3.60 | 4 | 5 |
| 10 | Dejounte Murray | +2.85 | +3.10 | 9 | 1 |
| 11 | Amir Johnson | +2.85 | +3.20 | 8 | 3 |
| 12 | Fred VanVleet | +2.84 | +1.90 | 33 | 21 |
| 13 | Delon Wright | +2.80 | +1.70 | 41 | 28 |
| 14 | Pau Gasol | +2.79 | +1.10 | 69 | 55 |
| 15 | Giannis Antetokounmpo | +2.73 | +2.50 | 21 | 6 |

### 2018-19 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +4.11 | +5.10 | 2 | 1 |
| 2 | Joel Embiid | +3.74 | +3.60 | 6 | 4 |
| 3 | Jusuf Nurkic | +3.66 | +5.20 | 1 | 2 |
| 4 | Hassan Whiteside | +3.64 | +3.80 | 4 | 0 |
| 5 | Robert Covington | +3.46 | +3.30 | 10 | 5 |
| 6 | Myles Turner | +3.44 | +1.90 | 33 | 27 |
| 7 | Andre Drummond | +3.32 | +3.10 | 12 | 5 |
| 8 | Marc Gasol | +3.29 | +3.90 | 3 | 5 |
| 9 | Giannis Antetokounmpo | +3.22 | +2.60 | 20 | 11 |
| 10 | Draymond Green | +3.18 | +3.50 | 7 | 3 |
| 11 | Nikola Vucevic | +2.97 | +3.50 | 8 | 3 |
| 12 | Paul George | +2.92 | +3.80 | 5 | 7 |
| 13 | Kyle Anderson | +2.91 | +2.20 | 28 | 15 |
| 14 | Derrick Favors | +2.88 | +3.20 | 11 | 3 |
| 15 | Anthony Davis | +2.82 | +3.30 | 9 | 6 |

### 2019-20 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Giannis Antetokounmpo | +4.49 | +3.90 | 5 | 4 |
| 2 | Rudy Gobert | +3.77 | +6.60 | 1 | 1 |
| 3 | Joel Embiid | +3.71 | +3.60 | 8 | 5 |
| 4 | Marc Gasol | +3.27 | +4.10 | 3 | 1 |
| 5 | Brook Lopez | +3.06 | +3.90 | 6 | 1 |
| 6 | Kris Dunn | +2.61 | +4.10 | 4 | 2 |
| 7 | Clint Capela | +2.52 | +4.20 | 2 | 5 |
| 8 | Patrick Beverley | +2.51 | +3.10 | 9 | 1 |
| 9 | Kyle Anderson | +2.46 | +1.80 | 30 | 21 |
| 10 | Jakob Poeltl | +2.43 | +3.00 | 11 | 1 |
| 11 | Ben Simmons | +2.38 | +2.40 | 14 | 3 |
| 12 | Myles Turner | +2.31 | +1.70 | 32 | 20 |
| 13 | OG Anunoby | +2.26 | +1.80 | 26 | 13 |
| 14 | Steven Adams | +2.25 | +1.70 | 34 | 20 |
| 15 | Marcus Smart | +2.22 | +2.00 | 20 | 5 |

### 2020-21 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +5.71 | +8.20 | 1 | 0 |
| 2 | Clint Capela | +3.92 | +5.30 | 2 | 0 |
| 3 | Jakob Poeltl | +3.68 | +4.60 | 4 | 1 |
| 4 | Mike Conley | +3.66 | +4.00 | 8 | 4 |
| 5 | Alex Caruso | +3.50 | +4.60 | 6 | 1 |
| 6 | Kent Bazemore | +3.48 | +2.60 | 21 | 15 |
| 7 | Joel Embiid | +3.47 | +4.60 | 5 | 2 |
| 8 | Matisse Thybulle | +3.23 | +4.40 | 7 | 1 |
| 9 | Jimmy Butler | +3.09 | +2.90 | 13 | 4 |
| 10 | Myles Turner | +3.00 | +4.70 | 3 | 7 |
| 11 | Isaiah Stewart | +2.69 | +3.00 | 12 | 1 |
| 12 | Larry Nance Jr. | +2.65 | +2.80 | 14 | 2 |
| 13 | Draymond Green | +2.54 | +3.20 | 10 | 3 |
| 14 | Ivica Zubac | +2.53 | +1.70 | 48 | 34 |
| 15 | Rudy Gay | +2.40 | +2.80 | 15 | 0 |

### 2021-22 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +4.34 | +6.90 | 1 | 0 |
| 2 | Nikola Jokic | +3.98 | +5.90 | 2 | 0 |
| 3 | Draymond Green | +3.47 | +4.30 | 4 | 1 |
| 4 | Joel Embiid | +3.41 | +3.60 | 12 | 8 |
| 5 | Matisse Thybulle | +3.12 | +4.20 | 6 | 1 |
| 6 | Maxi Kleber | +3.04 | +2.80 | 16 | 10 |
| 7 | Jarrett Allen | +2.86 | +4.20 | 7 | 0 |
| 8 | Kenrich Williams | +2.84 | +3.80 | 11 | 3 |
| 9 | Alex Caruso | +2.79 | +4.70 | 3 | 6 |
| 10 | Bam Adebayo | +2.74 | +3.90 | 9 | 1 |
| 11 | Robert Williams III | +2.74 | +2.70 | 21 | 10 |
| 12 | Myles Turner | +2.68 | +2.40 | 28 | 16 |
| 13 | Paul George | +2.67 | +3.90 | 8 | 5 |
| 14 | Gary Payton II | +2.66 | +4.30 | 5 | 9 |
| 15 | Isaiah Hartenstein | +2.64 | +1.60 | 52 | 37 |

### 2022-23 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Alex Caruso | +3.85 | +6.10 | 1 | 0 |
| 2 | Anthony Davis | +3.83 | +4.60 | 3 | 1 |
| 3 | Steven Adams | +3.53 | +3.50 | 11 | 8 |
| 4 | Draymond Green | +3.42 | +3.50 | 9 | 5 |
| 5 | Brook Lopez | +3.25 | +5.00 | 2 | 3 |
| 6 | Jaren Jackson Jr. | +3.03 | +2.80 | 17 | 11 |
| 7 | Joel Embiid | +3.00 | +3.70 | 6 | 1 |
| 8 | Derrick White | +2.98 | +3.10 | 14 | 6 |
| 9 | Rudy Gobert | +2.85 | +4.10 | 5 | 4 |
| 10 | Dennis Smith Jr. | +2.57 | +3.50 | 8 | 2 |
| 11 | John Konchar | +2.50 | +3.30 | 12 | 1 |
| 12 | Jarrett Allen | +2.34 | +1.80 | 38 | 26 |
| 13 | Nikola Jokic | +2.33 | +4.10 | 4 | 9 |
| 14 | Isaiah Hartenstein | +2.30 | +2.60 | 21 | 7 |
| 15 | Immanuel Quickley | +2.23 | +2.20 | 28 | 13 |
