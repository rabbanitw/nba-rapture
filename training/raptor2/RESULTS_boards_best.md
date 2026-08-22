# Best-model projected leaderboards vs actual (season-held-out)

Regenerated after the label-name fix (RESULTS_def_outliers.md): 136
corrupted rows corrected, mp-derived features rebuilt. Configs:
**offense** = gbm + struct + linear-wide hats (hats4-both) with playoff
rows pooled; **defense** = gbm + struct hats (hats3, huber). Pools are
>=1065 minutes. `dev` = |actual rank - projected rank|.

## offense

CV: median dev@10 **1.20**, mean 1.24, median tau@10 +0.756, hits@10 94/100.

| season | dev@10 | tau@10 | hits@10 |
|---|---:|---:|---:|
| 2013-14 | 1.00 | +0.733 | 10/10 |
| 2014-15 | 1.20 | +0.733 | 9/10 |
| 2015-16 | 1.30 | +0.778 | 9/10 |
| 2016-17 | 1.90 | +0.378 | 9/10 |
| 2017-18 | 1.40 | +0.689 | 10/10 |
| 2018-19 | 2.80 | +0.333 | 9/10 |
| 2019-20 | 0.60 | +0.867 | 10/10 |
| 2020-21 | 1.20 | +0.867 | 9/10 |
| 2021-22 | 0.40 | +0.911 | 10/10 |
| 2022-23 | 0.60 | +0.867 | 9/10 |

### 2013-14 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Kevin Durant | +7.54 | +7.60 | 1 | 0 |
| 2 | Chris Paul | +6.75 | +7.10 | 2 | 0 |
| 3 | LeBron James | +6.16 | +5.80 | 4 | 1 |
| 4 | James Harden | +5.85 | +6.30 | 3 | 1 |
| 5 | Kevin Love | +5.72 | +5.70 | 5 | 0 |
| 6 | Goran Dragic | +4.70 | +4.80 | 6 | 0 |
| 7 | Carmelo Anthony | +4.28 | +4.20 | 9 | 2 |
| 8 | Manu Ginobili | +4.21 | +4.00 | 10 | 2 |
| 9 | Dirk Nowitzki | +4.15 | +4.40 | 8 | 1 |
| 10 | Kyle Lowry | +3.95 | +4.40 | 7 | 3 |
| 11 | Isaiah Thomas | +3.66 | +3.50 | 14 | 3 |
| 12 | Blake Griffin | +3.53 | +2.90 | 17 | 5 |
| 13 | Damian Lillard | +3.42 | +3.60 | 12 | 1 |
| 14 | Russell Westbrook | +3.41 | +3.30 | 15 | 1 |
| 15 | Mike Conley | +3.30 | +3.50 | 13 | 2 |

### 2014-15 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +9.38 | +8.50 | 1 | 0 |
| 2 | James Harden | +8.09 | +7.70 | 2 | 0 |
| 3 | LeBron James | +5.88 | +5.30 | 5 | 2 |
| 4 | Russell Westbrook | +5.82 | +6.10 | 3 | 1 |
| 5 | Lou Williams | +5.49 | +5.20 | 6 | 1 |
| 6 | Kyrie Irving | +5.21 | +5.50 | 4 | 2 |
| 7 | Isaiah Thomas | +4.82 | +4.50 | 8 | 1 |
| 8 | Klay Thompson | +4.05 | +4.30 | 10 | 2 |
| 9 | Kyle Korver | +4.00 | +4.60 | 7 | 2 |
| 10 | Damian Lillard | +4.00 | +4.00 | 11 | 1 |
| 11 | Blake Griffin | +3.89 | +3.20 | 22 | 11 |
| 12 | George Hill | +3.82 | +3.90 | 12 | 0 |
| 13 | Carmelo Anthony | +3.69 | +3.80 | 13 | 0 |
| 14 | Gordon Hayward | +3.58 | +3.20 | 21 | 7 |
| 15 | Anthony Davis | +3.52 | +4.30 | 9 | 6 |

### 2015-16 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Chris Paul | +8.28 | +7.50 | 1 | 0 |
| 2 | Kevin Durant | +6.54 | +6.20 | 3 | 1 |
| 3 | LeBron James | +6.12 | +6.10 | 4 | 1 |
| 4 | James Harden | +5.96 | +5.70 | 5 | 1 |
| 5 | Russell Westbrook | +5.81 | +6.40 | 2 | 3 |
| 6 | Kyle Lowry | +5.57 | +5.50 | 6 | 0 |
| 7 | Damian Lillard | +5.40 | +5.00 | 8 | 1 |
| 8 | Isaiah Thomas | +4.51 | +4.50 | 9 | 1 |
| 9 | Kawhi Leonard | +3.88 | +5.00 | 7 | 2 |
| 10 | Kemba Walker | +3.57 | +3.40 | 13 | 3 |
| 11 | Reggie Jackson | +3.56 | +3.20 | 17 | 6 |
| 12 | Kyrie Irving | +3.37 | +2.60 | 23 | 11 |
| 13 | Paul George | +3.22 | +2.70 | 22 | 9 |
| 14 | Jrue Holiday | +3.17 | +3.30 | 14 | 0 |
| 15 | Draymond Green | +3.13 | +4.20 | 10 | 5 |

### 2016-17 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Isaiah Thomas | +8.33 | +8.30 | 1 | 0 |
| 2 | Chris Paul | +7.97 | +7.50 | 4 | 2 |
| 3 | James Harden | +7.59 | +7.90 | 2 | 1 |
| 4 | Russell Westbrook | +7.52 | +7.80 | 3 | 1 |
| 5 | LeBron James | +6.75 | +6.20 | 10 | 5 |
| 6 | Damian Lillard | +6.41 | +6.20 | 9 | 3 |
| 7 | Kawhi Leonard | +6.35 | +6.60 | 6 | 1 |
| 8 | Kyrie Irving | +6.22 | +6.30 | 8 | 0 |
| 9 | Kyle Lowry | +6.21 | +6.70 | 5 | 4 |
| 10 | Mike Conley | +5.93 | +5.30 | 12 | 2 |
| 11 | Kemba Walker | +5.59 | +5.00 | 14 | 3 |
| 12 | Nikola Jokic | +5.40 | +6.40 | 7 | 5 |
| 13 | Kevin Durant | +5.35 | +5.20 | 13 | 0 |
| 14 | Bradley Beal | +5.06 | +4.90 | 15 | 1 |
| 15 | Jimmy Butler | +4.76 | +4.80 | 16 | 1 |

### 2017-18 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Stephen Curry | +8.90 | +9.20 | 1 | 0 |
| 2 | Chris Paul | +8.14 | +7.60 | 2 | 0 |
| 3 | Damian Lillard | +6.54 | +6.60 | 4 | 1 |
| 4 | LeBron James | +6.45 | +7.00 | 3 | 1 |
| 5 | Kevin Durant | +5.85 | +5.60 | 6 | 1 |
| 6 | Kemba Walker | +5.49 | +5.40 | 8 | 2 |
| 7 | Lou Williams | +5.44 | +5.20 | 10 | 3 |
| 8 | Kyrie Irving | +5.18 | +6.00 | 5 | 3 |
| 9 | Jimmy Butler | +5.04 | +5.50 | 7 | 2 |
| 10 | Nikola Jokic | +4.98 | +5.20 | 9 | 1 |
| 11 | Russell Westbrook | +4.84 | +4.90 | 11 | 0 |
| 12 | Tyreke Evans | +4.51 | +4.40 | 13 | 1 |
| 13 | Kyle Lowry | +4.43 | +4.80 | 12 | 1 |
| 14 | KarlAnthony Towns | +4.42 | +4.30 | 14 | 0 |
| 15 | DeMar DeRozan | +3.66 | +3.90 | 15 | 0 |

### 2018-19 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Stephen Curry | +8.50 | +7.50 | 2 | 1 |
| 2 | James Harden | +8.17 | +9.80 | 1 | 1 |
| 3 | Damian Lillard | +7.48 | +6.80 | 3 | 0 |
| 4 | Kevin Durant | +6.12 | +5.00 | 9 | 5 |
| 5 | Paul George | +5.82 | +5.30 | 7 | 2 |
| 6 | Kemba Walker | +5.55 | +5.00 | 10 | 4 |
| 7 | Kawhi Leonard | +5.12 | +4.70 | 12 | 5 |
| 8 | Kyrie Irving | +5.05 | +5.50 | 5 | 3 |
| 9 | Nikola Jokic | +4.98 | +5.60 | 4 | 5 |
| 10 | Lou Williams | +4.95 | +5.10 | 8 | 2 |
| 11 | Giannis Antetokounmpo | +4.87 | +4.20 | 15 | 4 |
| 12 | LeBron James | +4.84 | +5.40 | 6 | 6 |
| 13 | Danilo Gallinari | +4.40 | +4.70 | 11 | 2 |
| 14 | Anthony Davis | +4.13 | +4.10 | 17 | 3 |
| 15 | Jrue Holiday | +4.05 | +4.10 | 16 | 1 |

### 2019-20 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Damian Lillard | +8.25 | +8.60 | 1 | 0 |
| 2 | Luka Doncic | +7.38 | +7.70 | 2 | 0 |
| 3 | Trae Young | +6.80 | +7.10 | 3 | 0 |
| 4 | KarlAnthony Towns | +5.80 | +5.80 | 5 | 1 |
| 5 | LeBron James | +5.67 | +6.30 | 4 | 1 |
| 6 | Bradley Beal | +5.40 | +5.40 | 7 | 1 |
| 7 | Giannis Antetokounmpo | +4.85 | +5.50 | 6 | 1 |
| 8 | Nikola Jokic | +4.63 | +4.40 | 9 | 1 |
| 9 | Devin Booker | +4.58 | +4.80 | 8 | 1 |
| 10 | Khris Middleton | +3.60 | +3.50 | 10 | 0 |
| 11 | Spencer Dinwiddie | +3.40 | +3.40 | 11 | 0 |
| 12 | Jayson Tatum | +3.25 | +3.40 | 12 | 0 |
| 13 | George Hill | +3.18 | +3.00 | 14 | 1 |
| 14 | DeAaron Fox | +2.61 | +2.50 | 19 | 5 |
| 15 | Donovan Mitchell | +2.56 | +2.40 | 20 | 5 |

### 2020-21 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokic | +8.57 | +8.70 | 1 | 0 |
| 2 | Stephen Curry | +8.36 | +8.70 | 2 | 0 |
| 3 | Damian Lillard | +8.05 | +7.00 | 3 | 0 |
| 4 | Luka Doncic | +6.44 | +6.80 | 4 | 0 |
| 5 | Trae Young | +6.10 | +5.80 | 7 | 2 |
| 6 | James Harden | +6.08 | +6.50 | 5 | 1 |
| 7 | Kawhi Leonard | +6.03 | +6.40 | 6 | 1 |
| 8 | Kyrie Irving | +5.58 | +5.60 | 8 | 0 |
| 9 | Joel Embiid | +4.87 | +3.90 | 17 | 8 |
| 10 | LeBron James | +4.86 | +5.30 | 10 | 0 |
| 11 | Jimmy Butler | +4.72 | +4.40 | 14 | 3 |
| 12 | Bradley Beal | +4.64 | +5.50 | 9 | 3 |
| 13 | Jayson Tatum | +4.46 | +4.10 | 15 | 2 |
| 14 | Donovan Mitchell | +4.33 | +4.70 | 12 | 2 |
| 15 | Paul George | +4.19 | +4.80 | 11 | 4 |

### 2021-22 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokic | +7.96 | +9.00 | 1 | 0 |
| 2 | Trae Young | +7.87 | +7.60 | 2 | 0 |
| 3 | Giannis Antetokounmpo | +5.35 | +5.70 | 4 | 1 |
| 4 | Luka Doncic | +5.24 | +5.80 | 3 | 1 |
| 5 | Stephen Curry | +5.22 | +5.70 | 5 | 0 |
| 6 | Kevin Durant | +4.70 | +5.00 | 6 | 0 |
| 7 | Donovan Mitchell | +4.58 | +4.60 | 8 | 1 |
| 8 | Jayson Tatum | +4.42 | +4.80 | 7 | 1 |
| 9 | Ja Morant | +4.38 | +4.60 | 9 | 0 |
| 10 | LeBron James | +4.27 | +4.60 | 10 | 0 |
| 11 | DeMar DeRozan | +4.01 | +3.90 | 16 | 5 |
| 12 | Devin Booker | +4.01 | +4.30 | 13 | 1 |
| 13 | Joel Embiid | +3.98 | +4.30 | 12 | 1 |
| 14 | Darius Garland | +3.88 | +4.50 | 11 | 3 |
| 15 | James Harden | +3.83 | +3.50 | 18 | 3 |

### 2022-23 — offense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokic | +8.99 | +10.00 | 1 | 0 |
| 2 | Damian Lillard | +8.45 | +9.30 | 2 | 0 |
| 3 | Luka Doncic | +7.81 | +8.30 | 3 | 0 |
| 4 | Stephen Curry | +7.71 | +7.50 | 4 | 0 |
| 5 | Tyrese Haliburton | +7.09 | +6.90 | 5 | 0 |
| 6 | Donovan Mitchell | +5.75 | +5.90 | 7 | 1 |
| 7 | Jimmy Butler | +5.66 | +6.10 | 6 | 1 |
| 8 | James Harden | +5.36 | +5.50 | 9 | 1 |
| 9 | Devin Booker | +5.35 | +5.30 | 10 | 1 |
| 10 | Shai Gilgeous-Alexander | +4.93 | +5.00 | 12 | 2 |
| 11 | Kawhi Leonard | +4.88 | +4.70 | 15 | 4 |
| 12 | De'Aaron Fox | +4.80 | +4.50 | 18 | 6 |
| 13 | Kyrie Irving | +4.75 | +5.60 | 8 | 5 |
| 14 | Trae Young | +4.72 | +5.20 | 11 | 3 |
| 15 | Jalen Brunson | +4.70 | +5.00 | 13 | 2 |

## defense

CV: median dev@10 **4.65**, mean 4.57, median tau@10 +0.444, hits@10 72/100.

| season | dev@10 | tau@10 | hits@10 |
|---|---:|---:|---:|
| 2013-14 | 6.80 | +0.333 | 6/10 |
| 2014-15 | 2.70 | +0.467 | 8/10 |
| 2015-16 | 4.70 | +0.689 | 8/10 |
| 2016-17 | 6.30 | +0.689 | 7/10 |
| 2017-18 | 4.60 | +0.600 | 7/10 |
| 2018-19 | 6.50 | +0.333 | 7/10 |
| 2019-20 | 4.80 | +0.333 | 8/10 |
| 2020-21 | 3.20 | +0.511 | 7/10 |
| 2021-22 | 3.20 | +0.422 | 7/10 |
| 2022-23 | 2.90 | +0.289 | 7/10 |

### 2013-14 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Tiago Splitter | +4.55 | +4.20 | 6 | 5 |
| 2 | Andrew Bogut | +4.10 | +4.40 | 4 | 2 |
| 3 | Joakim Noah | +3.73 | +4.50 | 3 | 0 |
| 4 | Roy Hibbert | +3.70 | +3.40 | 15 | 11 |
| 5 | Kawhi Leonard | +3.65 | +5.00 | 1 | 4 |
| 6 | Ian Mahinmi | +3.60 | +3.50 | 13 | 7 |
| 7 | Draymond Green | +3.48 | +4.60 | 2 | 5 |
| 8 | Marcin Gortat | +3.25 | +2.50 | 29 | 21 |
| 9 | Danny Green | +3.10 | +4.00 | 7 | 2 |
| 10 | Paul George | +3.00 | +2.90 | 21 | 11 |
| 11 | Nene | +2.79 | +3.80 | 9 | 2 |
| 12 | Jae Crowder | +2.74 | +3.00 | 19 | 7 |
| 13 | Jimmy Butler | +2.73 | +3.40 | 14 | 1 |
| 14 | Andre Iguodala | +2.68 | +2.60 | 25 | 11 |
| 15 | Tony Allen | +2.68 | +2.40 | 31 | 16 |

### 2014-15 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Draymond Green | +4.88 | +5.10 | 2 | 1 |
| 2 | Andrew Bogut | +4.73 | +4.70 | 5 | 3 |
| 3 | Rudy Gobert | +4.58 | +4.80 | 3 | 0 |
| 4 | Tony Allen | +4.24 | +4.80 | 4 | 0 |
| 5 | DeMarcus Cousins | +3.75 | +4.40 | 7 | 2 |
| 6 | Marcin Gortat | +3.37 | +3.60 | 8 | 2 |
| 7 | Anthony Davis | +3.37 | +4.50 | 6 | 1 |
| 8 | Kawhi Leonard | +3.21 | +5.20 | 1 | 7 |
| 9 | Nerlens Noel | +3.14 | +2.70 | 19 | 10 |
| 10 | Kosta Koufos | +2.92 | +3.30 | 11 | 1 |
| 11 | Khris Middleton | +2.92 | +3.10 | 13 | 2 |
| 12 | Jonas Jerebko | +2.80 | +2.80 | 17 | 5 |
| 13 | Andre Roberson | +2.70 | +3.40 | 10 | 3 |
| 14 | Danny Green | +2.53 | +3.00 | 16 | 2 |
| 15 | Marcus Smart | +2.45 | +1.80 | 43 | 28 |

### 2015-16 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Draymond Green | +4.52 | +5.20 | 3 | 2 |
| 2 | Tim Duncan | +4.48 | +5.30 | 1 | 1 |
| 3 | Andrew Bogut | +3.61 | +2.90 | 12 | 9 |
| 4 | Kawhi Leonard | +3.57 | +5.10 | 4 | 0 |
| 5 | Danny Green | +3.30 | +2.20 | 28 | 23 |
| 6 | Nikola Jokic | +3.27 | +4.50 | 5 | 1 |
| 7 | Steven Adams | +3.23 | +5.30 | 2 | 5 |
| 8 | Rudy Gobert | +3.16 | +3.50 | 7 | 1 |
| 9 | Luc Mbah a Moute | +3.10 | +3.50 | 8 | 1 |
| 10 | DeMarcus Cousins | +2.97 | +4.10 | 6 | 4 |
| 11 | Andre Drummond | +2.95 | +1.60 | 48 | 37 |
| 12 | Clint Capela | +2.76 | +1.70 | 47 | 35 |
| 13 | Ronnie Price | +2.61 | +1.50 | 60 | 47 |
| 14 | Wesley Johnson | +2.51 | +1.80 | 41 | 27 |
| 15 | Andre Roberson | +2.43 | +1.30 | 67 | 52 |

### 2016-17 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +4.96 | +5.60 | 2 | 1 |
| 2 | Draymond Green | +4.43 | +6.20 | 1 | 1 |
| 3 | Dewayne Dedmon | +4.03 | +4.10 | 4 | 1 |
| 4 | Anthony Davis | +3.92 | +4.40 | 3 | 1 |
| 5 | Luc Mbah a Moute | +3.33 | +3.80 | 7 | 2 |
| 6 | Robert Covington | +3.14 | +3.80 | 6 | 0 |
| 7 | Amir Johnson | +2.82 | +3.60 | 8 | 1 |
| 8 | James Johnson | +2.81 | +2.40 | 15 | 7 |
| 9 | Myles Turner | +2.69 | +2.20 | 23 | 14 |
| 10 | DeAndre Jordan | +2.63 | +1.60 | 45 | 35 |
| 11 | Nene | +2.59 | +3.50 | 9 | 2 |
| 12 | Zaza Pachulia | +2.54 | +0.90 | 74 | 62 |
| 13 | Andre Roberson | +2.54 | +4.00 | 5 | 8 |
| 14 | Danny Green | +2.47 | +2.60 | 13 | 1 |
| 15 | Chris Paul | +2.46 | +1.40 | 55 | 40 |

### 2017-18 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +4.92 | +4.00 | 1 | 0 |
| 2 | Anthony Davis | +4.50 | +3.80 | 2 | 0 |
| 3 | Joel Embiid | +4.19 | +3.40 | 6 | 3 |
| 4 | Jusuf Nurkic | +3.77 | +2.60 | 17 | 13 |
| 5 | Larry Nance Jr. | +3.54 | +2.60 | 18 | 13 |
| 6 | Victor Oladipo | +3.20 | +3.50 | 5 | 1 |
| 7 | Aron Baynes | +3.17 | +3.70 | 3 | 4 |
| 8 | Clint Capela | +3.13 | +2.80 | 14 | 6 |
| 9 | Robert Covington | +3.10 | +3.60 | 4 | 5 |
| 10 | Dejounte Murray | +2.91 | +3.10 | 9 | 1 |
| 11 | Amir Johnson | +2.87 | +3.20 | 8 | 3 |
| 12 | Pau Gasol | +2.84 | +1.10 | 69 | 57 |
| 13 | Fred VanVleet | +2.73 | +1.90 | 33 | 20 |
| 14 | Giannis Antetokounmpo | +2.73 | +2.50 | 21 | 7 |
| 15 | Delon Wright | +2.66 | +1.70 | 41 | 26 |

### 2018-19 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +4.09 | +5.10 | 2 | 1 |
| 2 | Jusuf Nurkic | +3.60 | +5.20 | 1 | 1 |
| 3 | Joel Embiid | +3.56 | +3.60 | 6 | 3 |
| 4 | Robert Covington | +3.44 | +3.30 | 10 | 6 |
| 5 | Hassan Whiteside | +3.38 | +3.80 | 4 | 1 |
| 6 | Andre Drummond | +3.27 | +3.10 | 12 | 6 |
| 7 | Giannis Antetokounmpo | +3.27 | +2.60 | 20 | 13 |
| 8 | Myles Turner | +3.20 | +1.90 | 33 | 25 |
| 9 | Draymond Green | +3.15 | +3.50 | 7 | 2 |
| 10 | Marc Gasol | +3.15 | +3.90 | 3 | 7 |
| 11 | Nikola Vucevic | +3.04 | +3.50 | 8 | 3 |
| 12 | Derrick Favors | +2.92 | +3.20 | 11 | 1 |
| 13 | Anthony Davis | +2.77 | +3.30 | 9 | 4 |
| 14 | Paul George | +2.76 | +3.80 | 5 | 9 |
| 15 | Derrick Jones Jr. | +2.71 | +2.20 | 24 | 9 |

### 2019-20 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Giannis Antetokounmpo | +4.59 | +3.90 | 5 | 4 |
| 2 | Rudy Gobert | +3.77 | +6.60 | 1 | 1 |
| 3 | Joel Embiid | +3.60 | +3.60 | 8 | 5 |
| 4 | Marc Gasol | +3.28 | +4.10 | 3 | 1 |
| 5 | Brook Lopez | +3.04 | +3.90 | 6 | 1 |
| 6 | Kris Dunn | +2.44 | +4.10 | 4 | 2 |
| 7 | Patrick Beverley | +2.42 | +3.10 | 9 | 2 |
| 8 | Kyle Anderson | +2.42 | +1.80 | 30 | 22 |
| 9 | Jakob Poeltl | +2.39 | +3.00 | 11 | 2 |
| 10 | Clint Capela | +2.39 | +4.20 | 2 | 8 |
| 11 | Ben Simmons | +2.35 | +2.40 | 14 | 3 |
| 12 | Bam Adebayo | +2.25 | +2.10 | 18 | 6 |
| 13 | Steven Adams | +2.20 | +1.70 | 34 | 21 |
| 14 | Myles Turner | +2.18 | +1.70 | 32 | 18 |
| 15 | Draymond Green | +2.15 | +2.30 | 15 | 0 |

### 2020-21 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +5.76 | +8.20 | 1 | 0 |
| 2 | Clint Capela | +3.80 | +5.30 | 2 | 0 |
| 3 | Jakob Poeltl | +3.70 | +4.60 | 4 | 1 |
| 4 | Mike Conley | +3.69 | +4.00 | 8 | 4 |
| 5 | Alex Caruso | +3.52 | +4.60 | 6 | 1 |
| 6 | Kent Bazemore | +3.34 | +2.60 | 21 | 15 |
| 7 | Matisse Thybulle | +3.32 | +4.40 | 7 | 0 |
| 8 | Joel Embiid | +3.09 | +4.60 | 5 | 3 |
| 9 | Jimmy Butler | +2.98 | +2.90 | 13 | 4 |
| 10 | Larry Nance Jr. | +2.79 | +2.80 | 14 | 4 |
| 11 | Myles Turner | +2.74 | +4.70 | 3 | 8 |
| 12 | Isaiah Stewart | +2.63 | +3.00 | 12 | 0 |
| 13 | Draymond Green | +2.49 | +3.20 | 10 | 3 |
| 14 | Brook Lopez | +2.45 | +1.40 | 55 | 41 |
| 15 | Ivica Zubac | +2.37 | +1.70 | 48 | 33 |

### 2021-22 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | +4.26 | +6.90 | 1 | 0 |
| 2 | Nikola Jokic | +3.91 | +5.90 | 2 | 0 |
| 3 | Draymond Green | +3.49 | +4.30 | 5 | 2 |
| 4 | Joel Embiid | +3.42 | +3.60 | 12 | 8 |
| 5 | Matisse Thybulle | +3.15 | +4.20 | 6 | 1 |
| 6 | Maxi Kleber | +2.90 | +2.80 | 15 | 9 |
| 7 | Jarrett Allen | +2.79 | +4.20 | 7 | 0 |
| 8 | Robert Williams III | +2.77 | +2.70 | 17 | 9 |
| 9 | Kenrich Williams | +2.75 | +3.80 | 10 | 1 |
| 10 | Bam Adebayo | +2.66 | +3.90 | 8 | 2 |
| 11 | Gary Payton II | +2.64 | +4.30 | 4 | 7 |
| 12 | Myles Turner | +2.61 | +2.40 | 25 | 13 |
| 13 | Alex Caruso | +2.59 | +4.70 | 3 | 10 |
| 14 | Isaiah Hartenstein | +2.51 | +1.60 | 53 | 39 |
| 15 | Al Horford | +2.47 | +3.80 | 11 | 4 |

### 2022-23 — defense top 15 (projected vs actual)

| # | player | est | actual | actual rank | dev |
|---:|---|---:|---:|---:|---:|
| 1 | Anthony Davis | +3.92 | +4.60 | 3 | 2 |
| 2 | Alex Caruso | +3.92 | +6.10 | 1 | 1 |
| 3 | Draymond Green | +3.57 | +3.50 | 10 | 7 |
| 4 | Brook Lopez | +3.32 | +5.00 | 2 | 2 |
| 5 | Steven Adams | +3.29 | +3.60 | 8 | 3 |
| 6 | Joel Embiid | +3.05 | +3.70 | 6 | 0 |
| 7 | Rudy Gobert | +3.00 | +4.10 | 5 | 2 |
| 8 | Derrick White | +2.95 | +3.10 | 13 | 5 |
| 9 | Jaren Jackson Jr. | +2.93 | +2.80 | 15 | 6 |
| 10 | Dennis Smith Jr. | +2.69 | +3.50 | 11 | 1 |
| 11 | Jarrett Allen | +2.55 | +1.80 | 36 | 25 |
| 12 | Nikola Jokic | +2.45 | +4.10 | 4 | 8 |
| 13 | John Konchar | +2.31 | +3.20 | 12 | 1 |
| 14 | Isaiah Hartenstein | +2.28 | +2.60 | 20 | 6 |
| 15 | Immanuel Quickley | +2.23 | +2.20 | 26 | 11 |
