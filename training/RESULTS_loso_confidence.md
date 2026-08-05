# LOSO confidence: rank intervals at every position, calibrated

Uncertainty = seed-ensemble spread + leave-fold-out residual bootstrap (minutes-bucketed), 2000 Monte Carlo re-rankings per board.

## Calibration -- do the intervals mean what they say?

| target | 90% CI coverage, all positions | coverage, top-30 positions |
|---|---:|---:|
| offense | 93.7% (n=2242) | 94.0% (n=300) |
| defense | 92.9% (n=2242) | 96.0% (n=300) |

## 2013-14 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Andrew Bogut | +4.19 | 4 | 1–17 | 88% | 99% |
| 2 | Tiago Splitter | +3.93 | 6 | 1–25 | 79% | 98% |
| 3 | Kawhi Leonard | +3.87 | 1 | 1–22 | 76% | 98% |
| 4 | Draymond Green | +3.71 | 2 | 1–26 | 70% | 97% |
| 5 | Kevin Garnett | +3.31 | 11 | 2–51 | 50% | 85% |
| 6 | Paul George | +3.25 | 21 | 2–47 | 52% | 87% |
| 7 | Joakim Noah | +3.23 | 3 | 2–42 | 46% | 89% |
| 8 | Ian Mahinmi | +3.07 | 13 | 2–55 | 39% | 80% |
| 9 | Nene | +2.92 | 9 | 3–59 | 30% | 75% |
| 10 | Roy Hibbert | +2.88 | 15 | 3–55 | 29% | 79% |
| 11 | Danny Green | +2.80 | 7 | 4–64 | 25% | 71% |
| 12 | Marcin Gortat | +2.79 | 29 | 4–57 | 27% | 75% |
| 13 | Anderson Varejao | +2.74 | 10 | 3–65 | 26% | 69% |
| 14 | Tony Allen | +2.73 | 31 | 4–74 | 24% | 69% |
| 15 | Andre Iguodala | +2.64 | 25 | 4–69 | 21% | 65% |
| 16 | Jimmy Butler | +2.63 | 14 | 5–61 | 18% | 69% |
| 17 | CJ Watson | +2.62 | 17 | 5–76 | 19% | 63% |
| 18 | Paul Millsap | +2.58 | 23 | 5–67 | 18% | 65% |
| 19 | DeMarcus Cousins | +2.50 | 16 | 5–72 | 16% | 59% |
| 20 | Paul Pierce | +2.46 | 50 | 6–72 | 13% | 57% |

## 2014-15 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Draymond Green | +5.00 | 2 | 1–7 | 98% | 100% |
| 2 | Tony Allen | +4.64 | 4 | 1–12 | 93% | 99% |
| 3 | Andrew Bogut | +4.40 | 5 | 1–16 | 88% | 99% |
| 4 | Rudy Gobert | +4.21 | 3 | 1–16 | 85% | 99% |
| 5 | Kawhi Leonard | +3.71 | 1 ⚠ | 2–25 | 67% | 97% |
| 6 | DeMarcus Cousins | +3.56 | 7 | 2–28 | 62% | 96% |
| 7 | Anthony Davis | +3.32 | 6 | 4–34 | 47% | 93% |
| 8 | Nerlens Noel | +3.26 | 19 | 4–36 | 45% | 92% |
| 9 | Zaza Pachulia | +3.26 | 12 | 3–36 | 46% | 91% |
| 10 | Andre Roberson | +3.10 | 10 | 4–52 | 37% | 83% |
| 11 | Marcin Gortat | +3.01 | 8 | 4–46 | 32% | 84% |
| 12 | Jonas Jerebko | +2.74 | 17 | 5–61 | 22% | 73% |
| 13 | Danny Green | +2.71 | 16 | 6–63 | 15% | 75% |
| 14 | Josh Smith | +2.71 | 23 | 6–60 | 16% | 72% |
| 15 | Kosta Koufos | +2.67 | 11 | 6–66 | 19% | 68% |
| 16 | Tim Duncan | +2.65 | 9 | 6–62 | 18% | 72% |
| 17 | AlFarouq Aminu | +2.61 | 24 | 6–68 | 17% | 67% |
| 18 | Nene | +2.59 | 18 | 6–60 | 15% | 68% |
| 19 | Khris Middleton | +2.43 | 13 | 8–73 | 11% | 61% |
| 20 | Marcus Smart | +2.41 | 43 | 8–71 | 11% | 60% |

## 2015-16 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Draymond Green | +4.41 | 3 | 1–11 | 94% | 100% |
| 2 | Tim Duncan | +3.99 | 1 | 1–20 | 84% | 98% |
| 3 | Kawhi Leonard | +3.93 | 4 | 1–18 | 85% | 99% |
| 4 | Rudy Gobert | +3.72 | 7 | 1–23 | 78% | 97% |
| 5 | Andrew Bogut | +3.35 | 12 | 2–41 | 59% | 90% |
| 6 | Steven Adams | +3.24 | 2 | 2–38 | 55% | 91% |
| 7 | Danny Green | +2.98 | 28 | 3–49 | 42% | 85% |
| 8 | Andre Roberson | +2.95 | 67 ⚠ | 3–49 | 41% | 83% |
| 9 | Andre Drummond | +2.71 | 48 | 5–59 | 25% | 78% |
| 10 | Nikola Jokic | +2.60 | 5 | 4–67 | 26% | 70% |
| 11 | Clint Capela | +2.59 | 47 | 4–72 | 24% | 68% |
| 12 | Luc Mbah a Moute | +2.57 | 8 | 5–71 | 22% | 69% |
| 13 | Ed Davis | +2.54 | 32 | 4–68 | 25% | 70% |
| 14 | Jeremy Lamb | +2.51 | 20 | 4–72 | 22% | 66% |
| 15 | DeMarcus Cousins | +2.48 | 6 | 6–69 | 18% | 67% |
| 16 | Marc Gasol | +2.43 | 16 | 5–71 | 19% | 63% |
| 17 | DeAndre Jordan | +2.33 | 38 | 8–75 | 11% | 60% |
| 18 | Kelly Olynyk | +2.31 | 33 | 6–86 | 15% | 58% |
| 19 | Marcus Smart | +2.29 | 37 | 6–82 | 15% | 56% |
| 20 | Kent Bazemore | +2.26 | 65 | 5–89 | 16% | 57% |

## 2016-17 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Rudy Gobert | +5.19 | 2 | 1–4 | 100% | 100% |
| 2 | Draymond Green | +4.11 | 1 | 1–12 | 92% | 99% |
| 3 | Dewayne Dedmon | +3.54 | 4 | 2–25 | 76% | 97% |
| 4 | Anthony Davis | +3.54 | 3 | 2–25 | 74% | 97% |
| 5 | Andre Roberson | +3.29 | 5 | 2–30 | 63% | 95% |
| 6 | Myles Turner | +2.98 | 23 | 3–42 | 46% | 89% |
| 7 | Luc Mbah a Moute | +2.92 | 7 | 3–42 | 44% | 88% |
| 8 | Michael KiddGilchrist | +2.71 | 11 | 4–56 | 33% | 81% |
| 9 | Robert Covington | +2.69 | 6 | 4–52 | 34% | 82% |
| 10 | Hassan Whiteside | +2.65 | 38 | 4–58 | 31% | 77% |
| 11 | DeAndre Jordan | +2.63 | 45 | 5–52 | 27% | 79% |
| 12 | Amir Johnson | +2.48 | 8 | 4–64 | 23% | 72% |
| 13 | Patrick Beverley | +2.25 | 14 | 6–75 | 14% | 60% |
| 14 | Cody Zeller | +2.21 | 54 | 6–72 | 16% | 60% |
| 15 | PJ Tucker | +2.20 | 46 | 6–71 | 13% | 62% |
| 16 | James Johnson | +2.20 | 15 | 5–75 | 15% | 59% |
| 17 | Derrick Favors | +2.15 | 93 ⚠ | 6–83 | 14% | 54% |
| 18 | AlFarouq Aminu | +2.13 | 10 | 7–76 | 11% | 55% |
| 19 | Marcus Smart | +2.09 | 25 | 7–78 | 10% | 54% |
| 20 | Nene | +2.09 | 9 | 7–82 | 12% | 54% |

## 2017-18 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.97 | 1 | 1–6 | 99% | 100% |
| 2 | Anthony Davis | +4.31 | 2 | 1–13 | 93% | 100% |
| 3 | Joel Embiid | +4.24 | 6 | 1–15 | 90% | 99% |
| 4 | Jusuf Nurkic | +3.89 | 17 | 2–23 | 80% | 98% |
| 5 | Clint Capela | +3.80 | 14 | 1–37 | 71% | 93% |
| 6 | Larry Nance Jr. | +3.32 | 18 | 3–45 | 53% | 88% |
| 7 | Dejounte Murray | +2.82 | 9 | 4–61 | 33% | 76% |
| 8 | Robert Covington | +2.74 | 4 ⚠ | 5–60 | 27% | 76% |
| 9 | Draymond Green | +2.68 | 24 | 5–59 | 21% | 71% |
| 10 | Myles Turner | +2.67 | 153 ⚠ | 5–64 | 23% | 70% |
| 11 | Victor Oladipo | +2.66 | 5 | 5–61 | 22% | 72% |
| 12 | Ersan Ilyasova | +2.62 | 31 | 5–61 | 21% | 69% |
| 13 | Aron Baynes | +2.60 | 3 ⚠ | 5–70 | 21% | 65% |
| 14 | Luc Mbah a Moute | +2.54 | 19 | 5–77 | 21% | 62% |
| 15 | Dwight Powell | +2.50 | 11 | 6–63 | 18% | 64% |
| 16 | Amir Johnson | +2.48 | 8 | 5–76 | 19% | 62% |
| 17 | Thaddeus Young | +2.44 | 30 | 7–72 | 13% | 60% |
| 18 | Giannis Antetokounmpo | +2.42 | 21 | 7–70 | 13% | 58% |
| 19 | Hassan Whiteside | +2.40 | 22 | 6–85 | 14% | 56% |
| 20 | Kristaps Porzingis | +2.40 | 32 | 6–82 | 14% | 55% |

## 2018-19 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.39 | 2 | 1–12 | 92% | 100% |
| 2 | Jusuf Nurkic | +4.08 | 1 | 1–16 | 86% | 99% |
| 3 | Joel Embiid | +3.98 | 6 | 1–21 | 81% | 99% |
| 4 | Hassan Whiteside | +3.81 | 4 | 1–31 | 69% | 95% |
| 5 | Robert Covington | +3.41 | 10 | 2–34 | 55% | 93% |
| 6 | Draymond Green | +3.23 | 7 | 2–38 | 46% | 90% |
| 7 | Myles Turner | +3.21 | 33 | 3–39 | 45% | 89% |
| 8 | Giannis Antetokounmpo | +3.21 | 20 | 3–38 | 44% | 90% |
| 9 | Marc Gasol | +3.08 | 3 | 3–41 | 39% | 89% |
| 10 | Ed Davis | +3.02 | 46 | 3–53 | 35% | 83% |
| 11 | Derrick Favors | +2.88 | 11 | 3–53 | 31% | 78% |
| 12 | Nikola Vucevic | +2.84 | 8 | 5–48 | 25% | 82% |
| 13 | Andre Drummond | +2.79 | 12 | 4–56 | 24% | 77% |
| 14 | Nikola Jokic | +2.76 | 37 | 4–57 | 26% | 76% |
| 15 | Paul George | +2.68 | 5 | 5–56 | 19% | 74% |
| 16 | Anthony Davis | +2.56 | 9 | 5–59 | 15% | 65% |
| 17 | Brook Lopez | +2.52 | 77 ⚠ | 5–72 | 19% | 64% |
| 18 | Derrick Jones Jr. | +2.47 | 24 | 6–69 | 15% | 60% |
| 19 | Clint Capela | +2.43 | 29 | 6–69 | 14% | 62% |
| 20 | Josh Hart | +2.41 | 14 | 7–64 | 10% | 60% |

## 2019-20 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Rudy Gobert | +5.38 | 1 | 1–4 | 100% | 100% |
| 2 | Giannis Antetokounmpo | +4.29 | 5 | 1–9 | 96% | 100% |
| 3 | Joel Embiid | +4.07 | 8 | 1–13 | 92% | 100% |
| 4 | Marc Gasol | +3.56 | 3 | 2–24 | 74% | 98% |
| 5 | Brook Lopez | +3.29 | 6 | 2–27 | 68% | 97% |
| 6 | Clint Capela | +3.26 | 2 | 2–31 | 63% | 95% |
| 7 | Kris Dunn | +2.92 | 4 | 3–32 | 48% | 94% |
| 8 | Jakob Poeltl | +2.87 | 11 | 3–32 | 44% | 94% |
| 9 | Kyle Anderson | +2.64 | 30 | 4–37 | 36% | 90% |
| 10 | Ben Simmons | +2.63 | 14 | 4–34 | 35% | 93% |
| 11 | Myles Turner | +2.62 | 32 | 4–34 | 36% | 92% |
| 12 | Patrick Beverley | +2.48 | 9 | 5–40 | 29% | 86% |
| 13 | Andre Drummond | +2.44 | 25 | 5–42 | 27% | 86% |
| 14 | Steven Adams | +2.34 | 34 | 6–39 | 23% | 85% |
| 15 | PJ Tucker | +2.31 | 17 | 6–43 | 21% | 83% |
| 16 | OG Anunoby | +2.23 | 26 | 7–43 | 16% | 82% |
| 17 | Bam Adebayo | +2.21 | 18 | 6–45 | 19% | 80% |
| 18 | Paul Millsap | +2.19 | 27 | 5–45 | 21% | 77% |
| 19 | Draymond Green | +2.19 | 15 | 6–48 | 17% | 77% |
| 20 | Nikola Jokic | +2.12 | 50 ⚠ | 6–44 | 15% | 79% |

## 2020-21 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Rudy Gobert | +5.51 | 1 | 1–3 | 100% | 100% |
| 2 | Clint Capela | +3.56 | 2 | 2–17 | 84% | 99% |
| 3 | Alex Caruso | +3.51 | 6 | 2–22 | 78% | 98% |
| 4 | Matisse Thybulle | +3.49 | 7 | 2–29 | 73% | 96% |
| 5 | Kent Bazemore | +3.34 | 21 | 2–26 | 73% | 97% |
| 6 | Mike Conley | +3.15 | 8 | 2–26 | 66% | 97% |
| 7 | Joel Embiid | +2.94 | 5 | 2–38 | 52% | 92% |
| 8 | Jakob Poeltl | +2.90 | 4 | 3–34 | 53% | 94% |
| 9 | Larry Nance Jr. | +2.80 | 14 | 3–42 | 46% | 90% |
| 10 | Myles Turner | +2.70 | 3 | 3–44 | 42% | 89% |
| 11 | Draymond Green | +2.54 | 10 | 4–47 | 34% | 84% |
| 12 | Jimmy Butler | +2.53 | 13 | 4–45 | 34% | 86% |
| 13 | Ben Simmons | +2.35 | 29 | 5–55 | 24% | 80% |
| 14 | OG Anunoby | +2.11 | 28 | 6–64 | 15% | 66% |
| 15 | Ivica Zubac | +2.06 | 48 | 6–65 | 14% | 65% |
| 16 | Giannis Antetokounmpo | +2.06 | 23 | 6–74 | 15% | 67% |
| 17 | Brook Lopez | +1.97 | 55 | 7–72 | 12% | 62% |
| 18 | Facundo Campazzo | +1.96 | 16 | 7–81 | 12% | 58% |
| 19 | Isaiah Stewart | +1.95 | 12 | 7–72 | 11% | 58% |
| 20 | Nicolas Batum | +1.93 | 17 | 8–71 | 9% | 60% |

## 2021-22 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Rudy Gobert | +4.75 | 1 | 1–9 | 96% | 100% |
| 2 | Joel Embiid | +4.00 | 12 | 1–22 | 81% | 98% |
| 3 | Nikola Jokic | +3.96 | 2 | 1–22 | 78% | 98% |
| 4 | Jarrett Allen | +3.65 | 7 | 2–29 | 67% | 95% |
| 5 | Matisse Thybulle | +3.61 | 6 | 2–31 | 63% | 95% |
| 6 | Draymond Green | +3.42 | 4 | 2–41 | 50% | 90% |
| 7 | Paul George | +3.15 | 8 | 3–48 | 39% | 85% |
| 8 | Alex Caruso | +3.14 | 3 | 3–50 | 36% | 84% |
| 9 | Kenrich Williams | +3.08 | 11 | 3–50 | 36% | 82% |
| 10 | Isaiah Hartenstein | +3.08 | 52 | 3–58 | 37% | 78% |
| 11 | Gary Payton II | +2.98 | 5 | 3–56 | 33% | 79% |
| 12 | Myles Turner | +2.87 | 28 | 4–63 | 27% | 74% |
| 13 | Bam Adebayo | +2.87 | 9 | 4–51 | 28% | 77% |
| 14 | Robert Covington | +2.81 | 20 | 4–59 | 25% | 74% |
| 15 | Jusuf Nurkic | +2.80 | 13 | 4–57 | 22% | 74% |
| 16 | Robert Williams III | +2.80 | 21 | 3–65 | 27% | 70% |
| 17 | Maxi Kleber | +2.67 | 16 | 4–73 | 21% | 64% |
| 18 | Derrick White | +2.60 | 14 | 7–62 | 13% | 65% |
| 19 | JaVale McGee | +2.58 | 26 | 6–74 | 15% | 61% |
| 20 | Giannis Antetokounmpo | +2.57 | 36 | 7–68 | 13% | 62% |

## 2022-23 — defense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Alex Caruso | +4.23 | 1 | 1–12 | 94% | 100% |
| 2 | Draymond Green | +3.57 | 9 | 1–19 | 83% | 99% |
| 3 | Brook Lopez | +3.49 | 2 | 1–23 | 77% | 98% |
| 4 | Jaren Jackson Jr. | +3.42 | 17 | 1–22 | 75% | 98% |
| 5 | Anthony Davis | +3.38 | 3 | 1–26 | 72% | 97% |
| 6 | Steven Adams | +3.24 | 11 | 1–30 | 67% | 95% |
| 7 | Joel Embiid | +2.94 | 6 | 2–34 | 53% | 93% |
| 8 | Derrick White | +2.74 | 14 | 3–40 | 41% | 89% |
| 9 | Jarrett Allen | +2.70 | 38 | 3–40 | 37% | 88% |
| 10 | John Konchar | +2.66 | 12 | 3–45 | 39% | 85% |
| 11 | Josh Okogie | +2.62 | 7 | 3–49 | 36% | 84% |
| 12 | Delon Wright | +2.58 | 10 | 3–52 | 33% | 81% |
| 13 | Isaiah Hartenstein | +2.44 | 21 | 4–57 | 29% | 78% |
| 14 | Matisse Thybulle | +2.43 | 23 | 3–65 | 30% | 73% |
| 15 | Dennis Smith Jr. | +2.40 | 8 | 4–60 | 25% | 76% |
| 16 | Rudy Gobert | +2.30 | 5 | 5–57 | 20% | 73% |
| 17 | Nikola Jokic | +2.09 | 4 ⚠ | 7–64 | 12% | 67% |
| 18 | Giannis Antetokounmpo | +2.07 | 25 | 6–64 | 16% | 65% |
| 19 | Bam Adebayo | +2.02 | 19 | 7–68 | 13% | 64% |
| 20 | Jusuf Nurkic | +1.91 | 31 | 8–78 | 11% | 56% |

## 2013-14 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Kevin Durant | +6.80 | 1 | 1–4 | 100% | 100% |
| 2 | Chris Paul | +6.77 | 2 | 1–4 | 100% | 100% |
| 3 | LeBron James | +6.65 | 4 | 1–4 | 100% | 100% |
| 4 | James Harden | +6.39 | 3 | 1–5 | 100% | 100% |
| 5 | Kevin Love | +4.60 | 5 | 5–12 | 89% | 100% |
| 6 | Kyle Lowry | +4.47 | 7 | 5–13 | 83% | 100% |
| 7 | Goran Dragic | +4.32 | 6 | 5–14 | 76% | 100% |
| 8 | Manu Ginobili | +4.14 | 10 | 5–14 | 70% | 100% |
| 9 | Damian Lillard | +4.12 | 12 | 5–15 | 64% | 100% |
| 10 | Isaiah Thomas | +4.03 | 14 | 5–16 | 57% | 100% |
| 11 | Russell Westbrook | +3.86 | 15 | 5–16 | 50% | 100% |
| 12 | Carmelo Anthony | +3.81 | 9 | 6–18 | 42% | 100% |
| 13 | Dirk Nowitzki | +3.43 | 8 | 8–23 | 18% | 99% |
| 14 | Mike Conley | +3.19 | 13 | 9–27 | 11% | 97% |
| 15 | Blake Griffin | +3.07 | 17 | 10–28 | 7% | 96% |
| 16 | Paul George | +2.68 | 22 | 12–37 | 3% | 89% |
| 17 | Patty Mills | +2.67 | 11 ⚠ | 12–34 | 3% | 91% |
| 18 | Ty Lawson | +2.61 | 16 | 12–38 | 2% | 89% |
| 19 | Kyrie Irving | +2.59 | 27 | 13–41 | 2% | 85% |
| 20 | Jamal Crawford | +2.55 | 19 | 12–41 | 2% | 86% |

## 2014-15 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Chris Paul | +8.12 | 1 | 1–2 | 100% | 100% |
| 2 | James Harden | +6.75 | 2 | 2–4 | 100% | 100% |
| 3 | Russell Westbrook | +5.96 | 3 | 2–6 | 100% | 100% |
| 4 | LeBron James | +5.81 | 5 | 2–7 | 100% | 100% |
| 5 | Kyrie Irving | +5.47 | 4 | 3–8 | 99% | 100% |
| 6 | Isaiah Thomas | +5.25 | 8 | 3–8 | 99% | 100% |
| 7 | Lou Williams | +4.56 | 6 | 5–11 | 93% | 100% |
| 8 | Damian Lillard | +4.12 | 11 | 6–16 | 72% | 100% |
| 9 | George Hill | +3.76 | 12 | 7–19 | 50% | 100% |
| 10 | Klay Thompson | +3.75 | 10 | 7–20 | 46% | 99% |
| 11 | Blake Griffin | +3.47 | 22 | 8–24 | 26% | 99% |
| 12 | Mike Conley | +3.23 | 32 ⚠ | 9–28 | 14% | 97% |
| 13 | Ty Lawson | +3.16 | 14 | 9–29 | 12% | 96% |
| 14 | Gordon Hayward | +3.14 | 21 | 9–28 | 12% | 97% |
| 15 | Jimmy Butler | +3.09 | 20 | 10–31 | 9% | 95% |
| 16 | Kyle Lowry | +3.03 | 18 | 10–32 | 8% | 94% |
| 17 | Kyle Korver | +2.99 | 7 ⚠ | 10–30 | 8% | 95% |
| 18 | Brandon Jennings | +2.91 | 23 | 9–31 | 10% | 94% |
| 19 | Carmelo Anthony | +2.86 | 13 | 10–33 | 7% | 92% |
| 20 | Jrue Holiday | +2.73 | 19 | 10–35 | 5% | 88% |

## 2015-16 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Chris Paul | +7.49 | 1 | 1–3 | 100% | 100% |
| 2 | Kevin Durant | +6.70 | 3 | 1–5 | 100% | 100% |
| 3 | LeBron James | +6.43 | 4 | 1–6 | 100% | 100% |
| 4 | James Harden | +6.42 | 5 | 1–6 | 100% | 100% |
| 5 | Russell Westbrook | +5.52 | 2 ⚠ | 3–8 | 100% | 100% |
| 6 | Kyle Lowry | +5.38 | 6 | 3–9 | 99% | 100% |
| 7 | Damian Lillard | +5.30 | 8 | 4–9 | 99% | 100% |
| 8 | Isaiah Thomas | +4.92 | 9 | 5–10 | 97% | 100% |
| 9 | Kawhi Leonard | +4.11 | 7 | 7–14 | 71% | 100% |
| 10 | Kyrie Irving | +3.35 | 23 | 9–23 | 26% | 99% |
| 11 | JJ Redick | +3.31 | 18 | 9–22 | 22% | 99% |
| 12 | Kemba Walker | +3.23 | 13 | 9–23 | 15% | 99% |
| 13 | Klay Thompson | +3.12 | 16 | 10–25 | 12% | 98% |
| 14 | Draymond Green | +3.06 | 10 | 10–26 | 11% | 98% |
| 15 | Paul George | +3.05 | 22 | 10–26 | 10% | 98% |
| 16 | Reggie Jackson | +2.98 | 17 | 10–27 | 8% | 98% |
| 17 | Nikola Jokic | +2.68 | 11 | 11–32 | 5% | 93% |
| 18 | Mike Conley | +2.59 | 30 | 11–35 | 3% | 91% |
| 19 | Jrue Holiday | +2.44 | 14 | 12–36 | 2% | 87% |
| 20 | Jimmy Butler | +2.26 | 25 | 14–43 | 2% | 78% |

## 2016-17 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Chris Paul | +8.00 | 4 | 1–4 | 100% | 100% |
| 2 | Isaiah Thomas | +7.69 | 1 | 1–5 | 100% | 100% |
| 3 | James Harden | +7.60 | 2 | 1–6 | 99% | 100% |
| 4 | Russell Westbrook | +6.83 | 3 | 2–11 | 95% | 100% |
| 5 | Damian Lillard | +6.70 | 9 | 2–12 | 90% | 100% |
| 6 | Kawhi Leonard | +6.41 | 6 | 3–13 | 81% | 100% |
| 7 | Kyle Lowry | +6.33 | 5 | 4–14 | 76% | 100% |
| 8 | Mike Conley | +6.32 | 12 | 4–13 | 77% | 100% |
| 9 | LeBron James | +6.13 | 10 | 4–14 | 61% | 100% |
| 10 | Kyrie Irving | +6.13 | 8 | 4–14 | 61% | 100% |
| 11 | Nikola Jokic | +6.12 | 7 | 4–14 | 68% | 100% |
| 12 | Kevin Durant | +5.92 | 13 | 5–15 | 50% | 100% |
| 13 | Jimmy Butler | +5.46 | 16 | 8–17 | 19% | 100% |
| 14 | Bradley Beal | +5.18 | 15 | 10–18 | 8% | 100% |
| 15 | Kemba Walker | +5.12 | 14 | 9–18 | 8% | 100% |
| 16 | John Wall | +4.58 | 18 | 12–23 | 2% | 99% |
| 17 | Blake Griffin | +3.98 | 17 | 15–29 | 0% | 96% |
| 18 | Lou Williams | +3.93 | 11 ⚠ | 15–29 | 0% | 97% |
| 19 | Gordon Hayward | +3.88 | 24 | 15–30 | 1% | 95% |
| 20 | Paul George | +3.82 | 30 | 16–30 | 0% | 95% |

## 2017-18 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Stephen Curry | +9.31 | 1 | 1–1 | 100% | 100% |
| 2 | Damian Lillard | +7.04 | 4 | 2–6 | 100% | 100% |
| 3 | Chris Paul | +6.80 | 2 | 2–6 | 100% | 100% |
| 4 | LeBron James | +6.34 | 3 | 2–9 | 98% | 100% |
| 5 | Kyrie Irving | +6.20 | 5 | 2–9 | 98% | 100% |
| 6 | Kevin Durant | +5.77 | 6 | 4–12 | 89% | 100% |
| 7 | Kemba Walker | +5.69 | 8 | 4–12 | 86% | 100% |
| 8 | Nikola Jokic | +5.60 | 9 | 4–12 | 83% | 100% |
| 9 | Jimmy Butler | +5.35 | 7 | 5–14 | 70% | 100% |
| 10 | Russell Westbrook | +5.14 | 11 | 6–14 | 56% | 100% |
| 11 | Lou Williams | +5.00 | 10 | 6–15 | 47% | 100% |
| 12 | Tyreke Evans | +4.74 | 13 | 6–16 | 33% | 100% |
| 13 | Kyle Lowry | +4.44 | 12 | 9–19 | 16% | 100% |
| 14 | Giannis Antetokounmpo | +3.98 | 19 | 11–25 | 4% | 98% |
| 15 | JJ Barea | +3.75 | 16 | 11–26 | 4% | 98% |
| 16 | Victor Oladipo | +3.66 | 17 | 12–29 | 2% | 96% |
| 17 | Eric Bledsoe | +3.49 | 24 | 13–33 | 1% | 93% |
| 18 | Eric Gordon | +3.46 | 22 | 13–32 | 1% | 94% |
| 19 | DeMar DeRozan | +3.41 | 15 | 14–35 | 1% | 92% |
| 20 | KarlAnthony Towns | +3.39 | 14 | 14–34 | 1% | 92% |

## 2018-19 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | James Harden | +7.56 | 1 | 1–3 | 100% | 100% |
| 2 | Stephen Curry | +7.06 | 2 | 1–4 | 100% | 100% |
| 3 | Damian Lillard | +6.56 | 3 | 1–5 | 100% | 100% |
| 4 | Kevin Durant | +5.46 | 9 | 3–12 | 89% | 100% |
| 5 | Nikola Jokic | +5.37 | 4 | 3–13 | 87% | 100% |
| 6 | Giannis Antetokounmpo | +5.15 | 15 ⚠ | 4–14 | 79% | 100% |
| 7 | Lou Williams | +5.13 | 8 | 4–13 | 80% | 100% |
| 8 | Kyrie Irving | +5.12 | 5 | 4–14 | 77% | 100% |
| 9 | LeBron James | +4.96 | 6 | 4–14 | 70% | 100% |
| 10 | Paul George | +4.93 | 7 | 4–15 | 65% | 100% |
| 11 | Kawhi Leonard | +4.78 | 12 | 5–16 | 57% | 100% |
| 12 | Kemba Walker | +4.68 | 10 | 5–17 | 46% | 100% |
| 13 | Devin Booker | +4.05 | 19 | 9–22 | 13% | 99% |
| 14 | Blake Griffin | +4.03 | 18 | 8–22 | 12% | 99% |
| 15 | Anthony Davis | +3.64 | 17 | 11–26 | 4% | 99% |
| 16 | Danilo Gallinari | +3.61 | 11 | 11–26 | 4% | 99% |
| 17 | Mike Conley | +3.57 | 13 | 12–26 | 3% | 98% |
| 18 | Jrue Holiday | +3.39 | 16 | 13–29 | 2% | 96% |
| 19 | Bradley Beal | +3.25 | 14 | 14–32 | 1% | 94% |
| 20 | Eric Bledsoe | +3.18 | 21 | 14–33 | 1% | 92% |

## 2019-20 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Damian Lillard | +8.06 | 1 | 1–3 | 100% | 100% |
| 2 | Luka Doncic | +7.23 | 2 | 1–4 | 100% | 100% |
| 3 | Trae Young | +7.03 | 3 | 1–5 | 100% | 100% |
| 4 | Bradley Beal | +6.13 | 7 | 3–8 | 100% | 100% |
| 5 | LeBron James | +5.77 | 4 | 3–8 | 99% | 100% |
| 6 | KarlAnthony Towns | +5.68 | 5 | 3–9 | 100% | 100% |
| 7 | Giannis Antetokounmpo | +5.54 | 6 | 4–9 | 100% | 100% |
| 8 | Nikola Jokic | +5.52 | 9 | 4–9 | 99% | 100% |
| 9 | Devin Booker | +4.59 | 8 | 7–12 | 86% | 100% |
| 10 | Khris Middleton | +3.89 | 10 | 9–16 | 49% | 100% |
| 11 | Kyle Lowry | +3.49 | 15 | 9–20 | 18% | 99% |
| 12 | Donovan Mitchell | +3.29 | 20 | 10–22 | 10% | 99% |
| 13 | Spencer Dinwiddie | +3.29 | 11 | 10–22 | 11% | 99% |
| 14 | Lou Williams | +3.05 | 23 | 10–25 | 7% | 99% |
| 15 | Jayson Tatum | +2.90 | 12 | 11–27 | 3% | 97% |
| 16 | CJ McCollum | +2.69 | 18 | 12–32 | 2% | 94% |
| 17 | George Hill | +2.63 | 14 | 11–31 | 3% | 94% |
| 18 | Jordan Clarkson | +2.59 | 17 | 12–32 | 1% | 92% |
| 19 | DeMar DeRozan | +2.58 | 16 | 12–35 | 1% | 92% |
| 20 | Patty Mills | +2.55 | 13 | 12–33 | 1% | 92% |

## 2020-21 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Nikola Jokic | +8.74 | 1 | 1–3 | 100% | 100% |
| 2 | Damian Lillard | +8.23 | 3 | 1–4 | 100% | 100% |
| 3 | Stephen Curry | +8.21 | 2 | 1–4 | 100% | 100% |
| 4 | Luka Doncic | +7.26 | 4 | 2–7 | 100% | 100% |
| 5 | Trae Young | +7.22 | 7 | 3–7 | 99% | 100% |
| 6 | Kawhi Leonard | +6.23 | 6 | 5–10 | 97% | 100% |
| 7 | James Harden | +6.02 | 5 | 5–11 | 93% | 100% |
| 8 | Kyrie Irving | +5.96 | 8 | 5–12 | 90% | 100% |
| 9 | LeBron James | +5.09 | 10 | 7–18 | 45% | 100% |
| 10 | Jimmy Butler | +5.09 | 14 | 7–19 | 45% | 100% |
| 11 | Donovan Mitchell | +5.04 | 12 | 8–19 | 35% | 100% |
| 12 | Chris Paul | +4.76 | 21 | 8–23 | 21% | 99% |
| 13 | Bradley Beal | +4.72 | 9 | 9–23 | 18% | 99% |
| 14 | Giannis Antetokounmpo | +4.52 | 13 | 9–26 | 11% | 98% |
| 15 | Joel Embiid | +4.43 | 17 | 10–27 | 10% | 98% |
| 16 | CJ McCollum | +4.31 | 18 | 10–28 | 6% | 97% |
| 17 | Jayson Tatum | +4.30 | 15 | 10–29 | 6% | 97% |
| 18 | Paul George | +4.12 | 11 | 11–30 | 2% | 95% |
| 19 | Zach LaVine | +3.91 | 16 | 12–35 | 2% | 88% |
| 20 | Mike Conley | +3.79 | 25 | 13–35 | 1% | 88% |

## 2021-22 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Trae Young | +7.46 | 2 | 1–3 | 100% | 100% |
| 2 | Nikola Jokic | +7.42 | 1 | 1–3 | 100% | 100% |
| 3 | Stephen Curry | +6.31 | 5 | 2–6 | 100% | 100% |
| 4 | Luka Doncic | +5.97 | 3 | 2–8 | 97% | 100% |
| 5 | Giannis Antetokounmpo | +5.37 | 4 | 3–12 | 89% | 100% |
| 6 | Kevin Durant | +5.21 | 6 | 4–13 | 85% | 100% |
| 7 | Ja Morant | +4.97 | 10 | 4–15 | 75% | 100% |
| 8 | Donovan Mitchell | +4.97 | 8 | 5–15 | 72% | 100% |
| 9 | James Harden | +4.72 | 18 ⚠ | 5–17 | 53% | 100% |
| 10 | DeMar DeRozan | +4.59 | 16 | 6–18 | 43% | 100% |
| 11 | LeBron James | +4.57 | 9 | 6–18 | 44% | 100% |
| 12 | Jrue Holiday | +4.36 | 15 | 6–20 | 28% | 99% |
| 13 | Darius Garland | +4.31 | 11 | 7–20 | 29% | 100% |
| 14 | Jayson Tatum | +4.23 | 7 | 7–21 | 22% | 100% |
| 15 | Devin Booker | +4.19 | 13 | 7–21 | 20% | 100% |
| 16 | Jimmy Butler | +4.07 | 20 | 8–23 | 20% | 100% |
| 17 | Joel Embiid | +3.49 | 12 | 12–31 | 3% | 95% |
| 18 | Chris Paul | +3.39 | 14 | 13–33 | 2% | 93% |
| 19 | Karl-Anthony Towns | +3.23 | 19 | 14–36 | 2% | 88% |
| 20 | LaMelo Ball | +3.17 | 30 | 14–38 | 1% | 86% |

## 2022-23 — offense, top 20 with confidence

| pos | player | est | true rank | 90% CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Damian Lillard | +9.19 | 2 | 1–2 | 100% | 100% |
| 2 | Nikola Jokic | +8.14 | 1 | 1–4 | 100% | 100% |
| 3 | Stephen Curry | +7.75 | 4 | 2–5 | 100% | 100% |
| 4 | Luka Doncic | +7.55 | 3 | 2–5 | 100% | 100% |
| 5 | Tyrese Haliburton | +6.39 | 5 | 4–11 | 93% | 100% |
| 6 | James Harden | +6.14 | 9 | 5–14 | 83% | 100% |
| 7 | Devin Booker | +5.93 | 10 | 5–16 | 74% | 100% |
| 8 | Jimmy Butler | +5.89 | 6 | 5–17 | 68% | 100% |
| 9 | Trae Young | +5.75 | 11 | 5–17 | 56% | 100% |
| 10 | Donovan Mitchell | +5.63 | 7 | 6–19 | 44% | 100% |
| 11 | Kyrie Irving | +5.40 | 8 | 7–21 | 31% | 100% |
| 12 | Shai Gilgeous-Alexander | +5.30 | 12 | 7–22 | 24% | 100% |
| 13 | LeBron James | +5.21 | 20 | 7–22 | 26% | 100% |
| 14 | Jalen Brunson | +5.19 | 13 | 7–23 | 20% | 99% |
| 15 | De'Aaron Fox | +5.12 | 18 | 8–24 | 15% | 99% |
| 16 | Giannis Antetokounmpo | +5.10 | 23 | 7–23 | 21% | 100% |
| 17 | Ja Morant | +5.02 | 16 | 8–25 | 14% | 99% |
| 18 | Kawhi Leonard | +4.93 | 15 | 8–25 | 13% | 99% |
| 19 | Kevin Durant | +4.82 | 14 | 9–26 | 10% | 99% |
| 20 | Jrue Holiday | +4.43 | 24 | 13–31 | 1% | 93% |
