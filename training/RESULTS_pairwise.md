# Pairwise ranking (who is better?) vs regression

## offense

| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| direct | 1.40 | 3.32 | +0.644 | +0.737 | 0.488 | 17/20 | 32/40 |
| pairwise | 1.50 | 2.92 | +0.733 | +0.747 | 1.404 | 17/20 | 35/40 |
| rank-avg(d,p) | 1.50 | 3.50 | +0.733 | +0.758 | 123.119 | 17/20 | 33/40 |

## defense

| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |
|---|---:|---:|---:|---:|---:|---:|---:|
| direct | 3.35 | 7.65 | +0.489 | +0.542 | 0.652 | 15/20 | 30/40 |
| pairwise | 4.65 | 6.95 | +0.511 | +0.468 | 1.337 | 14/20 | 31/40 |
| rank-avg(d,p) | 3.70 | 5.88 | +0.489 | +0.547 | 123.087 | 14/20 | 31/40 |

## 2013-14|Regular season — offense tournament (>= 1000 min), top 10

| pos | player | win % | true rank in pool |
|---:|---|---:|---:|
| 1 | Kevin Durant | 99.2% | 1 |
| 2 | Chris Paul | 99.0% | 2 |
| 3 | LeBron James | 98.9% | 4 |
| 4 | James Harden | 98.9% | 3 |
| 5 | Kevin Love | 98.0% | 5 |
| 6 | Goran Dragic | 97.3% | 6 |
| 7 | Kyle Lowry | 96.1% | 8 |
| 8 | Damian Lillard | 95.6% | 12 |
| 9 | Manu Ginobili | 95.3% | 10 |
| 10 | Carmelo Anthony | 94.5% | 9 |

## 2014-15|Regular season — offense tournament (>= 1000 min), top 10

| pos | player | win % | true rank in pool |
|---:|---|---:|---:|
| 1 | Chris Paul | 99.8% | 1 |
| 2 | James Harden | 99.4% | 2 |
| 3 | LeBron James | 98.3% | 5 |
| 4 | Russell Westbrook | 98.1% | 3 |
| 5 | Kyrie Irving | 97.9% | 4 |
| 6 | Lou Williams | 96.8% | 6 |
| 7 | Damian Lillard | 96.2% | 11 |
| 8 | Klay Thompson | 96.1% | 10 |
| 9 | Isaiah Thomas | 96.1% | 8 |
| 10 | Blake Griffin | 94.6% | 20 |

## 2013-14|Regular season — defense tournament (>= 1000 min), top 10

| pos | player | win % | true rank in pool |
|---:|---|---:|---:|
| 1 | Draymond Green | 96.8% | 2 |
| 2 | Andrew Bogut | 95.8% | 4 |
| 3 | Joakim Noah | 94.2% | 3 |
| 4 | Tiago Splitter | 93.8% | 6 |
| 5 | Paul George | 93.6% | 21 |
| 6 | Kawhi Leonard | 93.4% | 1 |
| 7 | Nene | 93.1% | 9 |
| 8 | Roy Hibbert | 91.6% | 14 |
| 9 | Andre Iguodala | 91.4% | 25 |
| 10 | Jimmy Butler | 91.4% | 15 |

## 2014-15|Regular season — defense tournament (>= 1000 min), top 10

| pos | player | win % | true rank in pool |
|---:|---|---:|---:|
| 1 | Draymond Green | 98.4% | 2 |
| 2 | Andrew Bogut | 97.1% | 5 |
| 3 | Tony Allen | 96.9% | 4 |
| 4 | Rudy Gobert | 95.0% | 3 |
| 5 | DeMarcus Cousins | 94.6% | 7 |
| 6 | Nerlens Noel | 94.1% | 19 |
| 7 | Zaza Pachulia | 93.0% | 12 |
| 8 | Marcin Gortat | 93.0% | 8 |
| 9 | Kawhi Leonard | 92.2% | 1 |
| 10 | Anthony Davis | 92.1% | 6 |
