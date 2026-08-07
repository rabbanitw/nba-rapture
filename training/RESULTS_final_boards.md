# Rapture 2023-26 projections — final validated stack

**Offense** = the components architecture (538's own two-part structure) with
the opponent on/off block. **Defense** = matched-regime LightGBM with
nearest-defender features. Both selected by ten-fold leave-one-season-out
validation ([RESULTS_loso.md](RESULTS_loso.md)): offense median dev@10 1.50
(hits@10 88/100), defense median dev@10 5.00 (hits@10 69/100).

**No truth exists for these seasons** — 538 shut down. What can be stated:
these exact fitted models score, on the held-out 2013-14/2014-15 cells:

| target | dev@10 | tau@10 | MAE | hits@20 |
|---|---:|---:|---:|---:|
| offense | 1.10 | +0.800 | 0.628 | 35/40 |
| defense | 3.80 | +0.511 | 0.625 | 30/40 |

The 90% rank intervals use the LOSO-calibrated machinery (measured coverage
92–94% against truth across ten seasons). `win%` on offense boards is the
share of eligible opponents beaten head-to-head by the independent pairwise
tournament model (LOSO-equivalent quality, no shared structure).
Eligibility: ≥1065 regular-season minutes, 538's own floor.

## 2023-24 — defense, top 30 (pool 248)

| pos | player | est | mp | 90% rank CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Rudy Gobert | +3.64 | 2593 | 1–20 | 83% | 98% |
| 2 | Victor Wembanyama | +3.56 | 2106 | 1–21 | 81% | 99% |
| 3 | Isaiah Hartenstein | +3.38 | 1896 | 1–28 | 70% | 96% |
| 4 | Jusuf Nurkic | +3.23 | 2078 | 1–28 | 64% | 96% |
| 5 | Alex Caruso | +3.10 | 2040 | 2–33 | 56% | 93% |
| 6 | Anthony Davis | +3.04 | 2700 | 2–36 | 53% | 92% |
| 7 | Joel Embiid | +3.02 | 1309 | 2–44 | 49% | 88% |
| 8 | Draymond Green | +2.97 | 1490 | 2–46 | 48% | 86% |
| 9 | Dean Wade | +2.91 | 1108 | 2–46 | 46% | 86% |
| 10 | Kristaps Porzingis | +2.76 | 1690 | 2–44 | 38% | 85% |
| 11 | Nikola Jokic | +2.60 | 2737 | 3–52 | 28% | 78% |
| 12 | Bam Adebayo | +2.59 | 2416 | 4–54 | 29% | 78% |
| 13 | Ivica Zubac | +2.47 | 1795 | 4–61 | 27% | 73% |
| 14 | Evan Mobley | +2.44 | 1532 | 4–68 | 24% | 69% |
| 15 | Nickeil Alexander-Walker | +2.27 | 1921 | 5–69 | 19% | 61% |
| 16 | Ausar Thompson | +2.22 | 1583 | 5–78 | 18% | 59% |
| 17 | OG Anunoby | +2.16 | 1702 | 6–72 | 14% | 57% |
| 18 | Toumani Camara | +2.13 | 1739 | 6–73 | 12% | 53% |
| 19 | Kawhi Leonard | +2.12 | 2330 | 7–73 | 10% | 55% |
| 20 | Brook Lopez | +2.09 | 2411 | 7–77 | 10% | 52% |
| 21 | Paul George | +2.07 | 2502 | 8–76 | 9% | 53% |
| 22 | Max Strus | +2.04 | 2239 | 7–78 | 9% | 51% |
| 23 | Amen Thompson | +2.04 | 1388 | 6–86 | 12% | 50% |
| 24 | Walker Kessler | +2.03 | 1493 | 7–87 | 12% | 50% |
| 25 | Larry Nance Jr. | +2.00 | 1216 | 7–89 | 10% | 46% |
| 26 | Chet Holmgren | +1.93 | 2413 | 9–82 | 7% | 45% |
| 27 | Franz Wagner | +1.91 | 2337 | 10–82 | 6% | 44% |
| 28 | Dyson Daniels | +1.89 | 1358 | 7–91 | 9% | 42% |
| 29 | Nic Claxton | +1.85 | 2116 | 10–85 | 5% | 40% |
| 30 | Clint Capela | +1.81 | 1883 | 10–91 | 6% | 40% |

## 2023-24 — offense, top 30 (pool 248)

| pos | player | est | mp | 90% rank CI | P(top-10) | P(top-30)  win% | |
|---:|---|---:|---:|---|---:|---:|---:|
| 1 | Nikola Jokic | +9.12 | 2737 | 1–2 | 100% | 100% | 99.4% |
| 2 | Luka Doncic | +8.70 | 2624 | 1–3 | 100% | 100% | 99.3% |
| 3 | Jalen Brunson | +7.25 | 2726 | 3–7 | 99% | 100% | 98.8% |
| 4 | Shai Gilgeous-Alexander | +6.96 | 2553 | 3–9 | 97% | 100% | 98.1% |
| 5 | Tyrese Haliburton | +6.80 | 2224 | 3–11 | 95% | 100% | 97.6% |
| 6 | Damian Lillard | +6.21 | 2579 | 4–15 | 76% | 100% | 94.8% |
| 7 | Stephen Curry | +6.08 | 2421 | 4–16 | 70% | 100% | 94.2% |
| 8 | Devin Booker | +5.85 | 2447 | 5–17 | 52% | 100% | 94.7% |
| 9 | Trae Young | +5.82 | 1942 | 5–17 | 59% | 100% | 94.8% |
| 10 | LeBron James | +5.77 | 2504 | 5–18 | 47% | 100% | 94.4% |
| 11 | Giannis Antetokounmpo | +5.74 | 2567 | 5–18 | 45% | 100% | 94.1% |
| 12 | Donovan Mitchell | +5.74 | 1943 | 5–18 | 50% | 100% | 94.0% |
| 13 | Jamal Murray | +5.45 | 1861 | 6–20 | 32% | 100% | 94.3% |
| 14 | Joel Embiid | +5.37 | 1309 | 6–20 | 27% | 100% | 91.7% |
| 15 | Kyrie Irving | +5.24 | 2030 | 8–21 | 18% | 100% | 94.0% |
| 16 | Jayson Tatum | +5.11 | 2645 | 9–23 | 9% | 99% | 91.0% |
| 17 | James Harden | +4.99 | 2470 | 9–25 | 7% | 99% | 95.4% |
| 18 | Tyrese Maxey | +4.70 | 2626 | 12–28 | 3% | 97% | 93.2% |
| 19 | Anthony Edwards | +4.48 | 2770 | 13–32 | 2% | 94% | 85.8% |
| 20 | De'Aaron Fox | +4.40 | 2659 | 14–33 | 1% | 93% | 92.3% |
| 21 | Collin Sexton | +4.17 | 2075 | 15–35 | 0% | 86% | 89.9% |
| 22 | Paul George | +4.00 | 2502 | 17–39 | 0% | 77% | 89.9% |
| 23 | Lauri Markkanen | +3.99 | 1820 | 17–39 | 0% | 79% | 89.5% |
| 24 | Kawhi Leonard | +3.96 | 2330 | 17–39 | 1% | 76% | 90.8% |
| 25 | T.J. McConnell | +3.94 | 1291 | 16–38 | 1% | 79% | 84.9% |
| 26 | Fred VanVleet | +3.81 | 2684 | 18–42 | 0% | 67% | 88.2% |
| 27 | Kevin Durant | +3.80 | 2791 | 18–42 | 1% | 66% | 86.1% |
| 28 | Jimmy Butler | +3.74 | 2042 | 18–43 | 0% | 67% | 88.5% |
| 29 | DeMar DeRozan | +3.61 | 2989 | 20–48 | 0% | 53% | 83.3% |
| 30 | Payton Pritchard | +3.44 | 1826 | 21–49 | 0% | 48% | 86.4% |

## 2024-25 — defense, top 30 (pool 257)

| pos | player | est | mp | 90% rank CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Victor Wembanyama | +3.80 | 1527 | 1–17 | 86% | 99% |
| 2 | Ivica Zubac | +3.35 | 2624 | 1–28 | 69% | 96% |
| 3 | Rudy Gobert | +3.29 | 2388 | 1–28 | 66% | 96% |
| 4 | Luke Kornet | +3.25 | 1361 | 1–37 | 61% | 92% |
| 5 | Ausar Thompson | +3.07 | 1328 | 1–43 | 54% | 89% |
| 6 | Jaxson Hayes | +3.05 | 1093 | 2–45 | 50% | 88% |
| 7 | Kris Dunn | +3.00 | 1783 | 2–40 | 50% | 89% |
| 8 | Donovan Clingan | +2.93 | 1324 | 2–46 | 45% | 85% |
| 9 | Alperen Sengun | +2.93 | 2394 | 2–42 | 45% | 89% |
| 10 | Draymond Green | +2.72 | 1983 | 2–51 | 37% | 82% |
| 11 | Evan Mobley | +2.64 | 2167 | 4–55 | 29% | 77% |
| 12 | Kevon Looney | +2.61 | 1142 | 3–61 | 31% | 73% |
| 13 | Anthony Davis | +2.44 | 1706 | 4–65 | 24% | 67% |
| 14 | Brandon Clarke | +2.43 | 1207 | 4–72 | 24% | 65% |
| 15 | Jaren Jackson Jr. | +2.34 | 2207 | 6–70 | 15% | 62% |
| 16 | Jonathan Isaac | +2.22 | 1090 | 5–80 | 16% | 55% |
| 17 | Toumani Camara | +2.21 | 2548 | 6–77 | 12% | 56% |
| 18 | Kristaps Porziņģis | +2.20 | 1210 | 5–81 | 16% | 54% |
| 19 | Paul George | +2.16 | 1334 | 5–87 | 14% | 52% |
| 20 | Goga Bitadze | +2.15 | 1430 | 6–86 | 13% | 51% |
| 21 | Luguentz Dort | +2.13 | 2073 | 7–76 | 10% | 52% |
| 22 | Amen Thompson | +2.12 | 2225 | 8–80 | 9% | 49% |
| 23 | Isaiah Hartenstein | +2.06 | 1590 | 8–84 | 11% | 49% |
| 24 | Brandin Podziemski | +2.03 | 1716 | 7–81 | 9% | 49% |
| 25 | Sam Merrill | +2.02 | 1401 | 8–96 | 9% | 44% |
| 26 | Isaiah Stewart | +2.01 | 1434 | 7–95 | 11% | 47% |
| 27 | Nicolas Batum | +2.00 | 1367 | 7–90 | 10% | 46% |
| 28 | Scotty Pippen Jr. | +1.97 | 1683 | 9–87 | 7% | 41% |
| 29 | Wendell Carter Jr. | +1.93 | 1758 | 8–87 | 8% | 42% |
| 30 | Dyson Daniels | +1.93 | 2571 | 9–91 | 7% | 43% |

## 2024-25 — offense, top 30 (pool 257)

| pos | player | est | mp | 90% rank CI | P(top-10) | P(top-30)  win% ||
|---:|---|---:|---:|---|---:|---:|---:|
| 1 | Nikola Jokić | +9.70 | 2571 | 1–2 | 100% | 100% | 99.8% |
| 2 | Shai Gilgeous-Alexander | +8.15 | 2598 | 2–3 | 100% | 100% | 99.5% |
| 3 | Stephen Curry | +7.12 | 2252 | 2–7 | 99% | 100% | 98.1% |
| 4 | Luka Dončić | +6.59 | 1769 | 3–10 | 96% | 100% | 97.1% |
| 5 | Tyrese Haliburton | +6.15 | 2451 | 3–14 | 83% | 100% | 97.0% |
| 6 | LaMelo Ball | +6.06 | 1505 | 3–14 | 82% | 100% | 95.0% |
| 7 | Damian Lillard | +6.04 | 2093 | 4–14 | 77% | 100% | 94.9% |
| 8 | Jalen Brunson | +5.91 | 2301 | 4–15 | 70% | 100% | 96.5% |
| 9 | Giannis Antetokounmpo | +5.84 | 2289 | 4–16 | 64% | 100% | 95.0% |
| 10 | Donovan Mitchell | +5.83 | 2232 | 4–16 | 63% | 100% | 95.3% |
| 11 | Jayson Tatum | +5.45 | 2624 | 6–19 | 35% | 100% | 92.1% |
| 12 | Ty Jerome | +5.38 | 1393 | 5–18 | 36% | 100% | 94.8% |
| 13 | James Harden | +5.23 | 2789 | 7–20 | 23% | 100% | 94.7% |
| 14 | Darius Garland | +5.20 | 2301 | 7–20 | 22% | 100% | 95.8% |
| 15 | Trae Young | +5.20 | 2739 | 7–20 | 19% | 100% | 94.8% |
| 16 | Tyler Herro | +4.87 | 2725 | 9–23 | 10% | 99% | 91.7% |
| 17 | Cade Cunningham | +4.54 | 2452 | 11–26 | 3% | 98% | 88.5% |
| 18 | Jamal Murray | +4.50 | 2418 | 12–28 | 3% | 98% | 93.4% |
| 19 | Anthony Edwards | +4.34 | 2871 | 13–29 | 2% | 96% | 88.5% |
| 20 | Austin Reaves | +3.99 | 2550 | 16–36 | 1% | 88% | 91.5% |
| 21 | Devin Booker | +3.93 | 2795 | 15–37 | 1% | 85% | 89.3% |
| 22 | Ja Morant | +3.92 | 1519 | 14–34 | 1% | 89% | 82.5% |
| 23 | Jimmy Butler | +3.81 | 1746 | 16–37 | 0% | 83% | 89.4% |
| 24 | Payton Pritchard | +3.76 | 2271 | 17–40 | 1% | 79% | 92.0% |
| 25 | Tyrese Maxey | +3.50 | 1960 | 18–45 | 0% | 68% | 88.5% |
| 26 | Kyrie Irving | +3.50 | 1804 | 18–45 | 0% | 68% | 90.6% |
| 27 | LeBron James | +3.41 | 2444 | 19–49 | 0% | 54% | 84.6% |
| 28 | Franz Wagner | +3.33 | 2023 | 19–50 | 0% | 56% | 85.1% |
| 29 | Karl-Anthony Towns | +3.19 | 2517 | 21–54 | 0% | 40% | 85.3% |
| 30 | Kevin Durant | +3.16 | 2265 | 21–57 | 0% | 39% | 80.7% |

## 2025-26 — defense, top 30 (pool 269)

| pos | player | est | mp | 90% rank CI | P(top-10) | P(top-30) |
|---:|---|---:|---:|---|---:|---:|
| 1 | Victor Wembanyama | +4.93 | 1866 | 1–6 | 99% | 100% |
| 2 | Rudy Gobert | +4.04 | 2380 | 1–15 | 88% | 100% |
| 3 | Neemias Queta | +4.01 | 1926 | 1–17 | 87% | 99% |
| 4 | Chet Holmgren | +3.87 | 1997 | 1–17 | 84% | 99% |
| 5 | Isaiah Hartenstein | +3.78 | 1137 | 1–24 | 75% | 97% |
| 6 | Derrick White | +3.10 | 2625 | 3–37 | 46% | 92% |
| 7 | Ajay Mitchell | +3.09 | 1473 | 3–38 | 47% | 91% |
| 8 | Cason Wallace | +3.02 | 2046 | 3–41 | 41% | 90% |
| 9 | Hugo González | +2.94 | 1084 | 3–51 | 40% | 85% |
| 10 | Ronald Holland II | +2.93 | 1550 | 4–45 | 36% | 86% |
| 11 | Marcus Smart | +2.68 | 1769 | 5–53 | 24% | 76% |
| 12 | Javonte Green | +2.57 | 1446 | 5–60 | 22% | 74% |
| 13 | Dru Smith | +2.56 | 1141 | 5–68 | 23% | 69% |
| 14 | Jalen Suggs | +2.52 | 1574 | 5–64 | 21% | 70% |
| 15 | Dyson Daniels | +2.43 | 2520 | 7–64 | 13% | 65% |
| 16 | Oso Ighodaro | +2.42 | 1808 | 6–70 | 18% | 65% |
| 17 | John Konchar | +2.35 | 1115 | 6–80 | 15% | 62% |
| 18 | Ausar Thompson | +2.35 | 1896 | 6–73 | 14% | 60% |
| 19 | Jaylin Williams | +2.33 | 1277 | 7–83 | 15% | 58% |
| 20 | Mitchell Robinson | +2.23 | 1175 | 7–84 | 12% | 53% |
| 21 | Baylor Scheierman | +2.23 | 1429 | 6–84 | 13% | 55% |
| 22 | Evan Mobley | +2.19 | 2074 | 9–76 | 8% | 54% |
| 23 | Bam Adebayo | +2.16 | 2365 | 9–82 | 7% | 52% |
| 24 | Jordan Goodwin | +2.07 | 1572 | 8–89 | 8% | 47% |
| 25 | Josh Okogie | +1.96 | 1354 | 9–103 | 7% | 40% |
| 26 | Ryan Dunn | +1.94 | 1355 | 9–101 | 6% | 40% |
| 27 | Day'Ron Sharpe | +1.87 | 1160 | 11–109 | 5% | 38% |
| 28 | Jaren Jackson Jr. | +1.81 | 1455 | 12–103 | 4% | 33% |
| 29 | Keon Ellis | +1.79 | 1479 | 12–102 | 4% | 34% |
| 30 | Brook Lopez | +1.76 | 1635 | 13–104 | 4% | 32% |

## 2025-26 — offense, top 30 (pool 269)

| pos | player | est | mp | 90% rank CI | P(top-10) | P(top-30)  win% ||
|---:|---|---:|---:|---|---:|---:|---:|
| 1 | Nikola Jokić | +9.23 | 2265 | 1–2 | 100% | 100% | 99.6% |
| 2 | Luka Dončić | +8.30 | 2289 | 1–4 | 100% | 100% | 98.9% |
| 3 | Shai Gilgeous-Alexander | +8.11 | 2259 | 1–4 | 100% | 100% | 99.2% |
| 4 | Donovan Mitchell | +6.80 | 2342 | 3–10 | 95% | 100% | 97.2% |
| 5 | Kawhi Leonard | +6.74 | 2085 | 4–11 | 95% | 100% | 98.0% |
| 6 | Jamal Murray | +6.58 | 2652 | 4–11 | 91% | 100% | 97.8% |
| 7 | LaMelo Ball | +6.54 | 2017 | 4–12 | 87% | 100% | 98.2% |
| 8 | Stephen Curry | +6.47 | 1329 | 4–11 | 92% | 100% | 94.6% |
| 9 | James Harden | +6.43 | 2438 | 4–12 | 87% | 100% | 96.4% |
| 10 | Jalen Brunson | +6.02 | 2590 | 5–14 | 66% | 100% | 96.5% |
| 11 | Cade Cunningham | +5.51 | 2172 | 7–18 | 29% | 100% | 95.8% |
| 12 | Jimmy Butler III | +5.27 | 1182 | 8–19 | 22% | 100% | 94.4% |
| 13 | Tyrese Maxey | +5.05 | 2661 | 10–23 | 9% | 99% | 93.7% |
| 14 | Anthony Edwards | +4.92 | 2137 | 10–25 | 6% | 98% | 92.4% |
| 15 | Devin Booker | +4.70 | 2146 | 11–28 | 3% | 97% | 89.8% |
| 16 | Deni Avdija | +4.68 | 2199 | 11–28 | 3% | 97% | 89.8% |
| 17 | Kevin Durant | +4.39 | 2840 | 12–33 | 1% | 93% | 90.8% |
| 18 | Austin Reaves | +4.38 | 1762 | 12–32 | 1% | 93% | 90.0% |
| 19 | Jalen Duren | +4.18 | 1976 | 14–37 | 1% | 87% | 88.9% |
| 20 | Coby White | +4.15 | 1250 | 13–35 | 1% | 89% | 83.5% |
| 21 | Jaylen Brown | +4.13 | 2443 | 14–38 | 1% | 84% | 87.3% |
| 22 | Victor Wembanyama | +4.11 | 1866 | 14–37 | 0% | 86% | 90.7% |
| 23 | Joel Embiid | +3.99 | 1201 | 14–38 | 1% | 81% | 85.3% |
| 24 | Payton Pritchard | +3.95 | 2556 | 15–41 | 0% | 77% | 92.5% |
| 25 | Michael Porter Jr. | +3.81 | 1689 | 16–42 | 0% | 74% | 84.9% |
| 26 | Duncan Robinson | +3.61 | 2113 | 18–50 | 0% | 52% | 87.3% |
| 27 | Lauri Markkanen | +3.55 | 1443 | 18–51 | 0% | 53% | 82.0% |
| 28 | Cam Spencer | +3.52 | 1714 | 18–51 | 0% | 53% | 88.9% |
| 29 | Keyonte George | +3.50 | 1786 | 18–52 | 0% | 51% | 84.4% |
| 30 | Jrue Holiday | +3.44 | 1560 | 20–56 | 0% | 45% | 87.8% |
