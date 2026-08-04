# Pairwise ranking (who is better?) vs regression

## offense

| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| direct | 1.45 | 3.38 | +0.667 | +0.737 | 0.488 | 17/20 | 32/40 |
| pairwise | 1.50 | 3.10 | +0.778 | +0.774 | 1.404 | 17/20 | 34/40 |
| rank-avg(d,p) | 1.05 | 3.38 | +0.733 | +0.774 | 123.119 | 17/20 | 33/40 |

## defense

| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| direct | 3.35 | 7.58 | +0.489 | +0.553 | 0.652 | 15/20 | 30/40 |
| pairwise | 4.60 | 6.32 | +0.533 | +0.474 | 1.336 | 14/20 | 32/40 |
| rank-avg(d,p) | 3.55 | 5.97 | +0.511 | +0.568 | 123.087 | 14/20 | 31/40 |

## 2013-14|Regular season — offense tournament (>= 1000 min), top 10

| pos | player | win % | true rank in pool |
|---:|---|---:|---:|
| 1 | Kevin Durant | 99.3% | 1 |
| 2 | Chris Paul | 99.1% | 2 |
| 3 | LeBron James | 98.8% | 4 |
| 4 | James Harden | 98.8% | 3 |
| 5 | Kevin Love | 97.9% | 5 |
| 6 | Goran Dragic | 97.3% | 6 |
| 7 | Kyle Lowry | 96.2% | 7 |
| 8 | Damian Lillard | 96.0% | 12 |
| 9 | Manu Ginobili | 95.4% | 10 |
| 10 | Dirk Nowitzki | 94.6% | 8 |

## 2014-15|Regular season — offense tournament (>= 1000 min), top 10

| pos | player | win % | true rank in pool |
|---:|---|---:|---:|
| 1 | Chris Paul | 99.9% | 1 |
| 2 | James Harden | 99.4% | 2 |
| 3 | LeBron James | 98.3% | 5 |
| 4 | Kyrie Irving | 98.1% | 4 |
| 5 | Russell Westbrook | 98.0% | 3 |
| 6 | Lou Williams | 97.0% | 6 |
| 7 | Damian Lillard | 96.4% | 11 |
| 8 | Isaiah Thomas | 96.3% | 8 |
| 9 | Klay Thompson | 96.1% | 10 |
| 10 | Blake Griffin | 94.3% | 22 |

## 2013-14|Regular season — defense tournament (>= 1000 min), top 10

| pos | player | win % | true rank in pool |
|---:|---|---:|---:|
| 1 | Draymond Green | 96.6% | 2 |
| 2 | Andrew Bogut | 96.5% | 4 |
| 3 | Joakim Noah | 94.3% | 3 |
| 4 | Kawhi Leonard | 93.7% | 1 |
| 5 | Tiago Splitter | 93.4% | 6 |
| 6 | Paul George | 93.1% | 21 |
| 7 | Nene | 93.0% | 9 |
| 8 | Roy Hibbert | 92.4% | 15 |
| 9 | Marcin Gortat | 91.8% | 29 |
| 10 | Ian Mahinmi | 91.2% | 13 |

## 2014-15|Regular season — defense tournament (>= 1000 min), top 10

| pos | player | win % | true rank in pool |
|---:|---|---:|---:|
| 1 | Draymond Green | 98.4% | 2 |
| 2 | Andrew Bogut | 97.4% | 5 |
| 3 | Tony Allen | 97.0% | 4 |
| 4 | Rudy Gobert | 95.6% | 3 |
| 5 | DeMarcus Cousins | 95.0% | 7 |
| 6 | Nerlens Noel | 94.3% | 19 |
| 7 | Zaza Pachulia | 93.1% | 12 |
| 8 | Marcin Gortat | 92.9% | 8 |
| 9 | Kawhi Leonard | 92.9% | 1 |
| 10 | Anthony Davis | 92.7% | 6 |
