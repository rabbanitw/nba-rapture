# Estimated RAPTOR for 2023-24, 2024-25 and 2025-26

538 stopped publishing RAPTOR, so these three seasons have no ground truth.
Everything below is our combined model's **prediction**, not a measurement.

The model is the production combined one from
[RESULTS_stride_transfer.md](RESULTS_stride_transfer.md): the combined block set
(pbp + 14 tracking tables + wowy on/off/diff) at `--modern-stride 6`, blended
0.75 LightGBM / 0.25 RidgeCV, one fit per target, trained on every labeled row
except the held-out 2013-14 and 2014-15 seasons. One deliberate change: three
identifier columns are dropped (see caveats), so it is refit rather than reused,
and rescored below rather than cited.

## How accurate should you expect this to be?

Measured, not cited. Dropping the identifier columns (below) makes this not
quite the model RESULTS_top100.md scored, so the same fit that produced the
tables below was also scored on the held-out 2013-14 and 2014-15 rows it never
saw. For reference, the published numbers for the version that kept those
columns were total +0.751 / offense +0.821 / defense +0.635 R².

| target | R² | RMSE | MAE | ρ |
|---|---|---|---|---|
| total | +0.744 | 1.741 | 1.297 | +0.875 |
| offense | +0.832 | 0.990 | 0.756 | +0.917 |
| defense | +0.631 | 1.415 | 1.043 | +0.815 |

Those are 2013-14 and 2014-15. **Treat them as an optimistic bound here.**
These seasons are nine to twelve years further out than anything the model was
fit on, across a period in which three-point rate, pace and defensive rules all
moved. Nothing in this report measures that drift, because measuring it would
require the labels that do not exist.

## Caveats that affect the numbers

- **Defense is the weak target.** R² +0.631 against +0.832 for offense. The defensive top-100s below should
  be read as considerably softer than the offensive ones.
- **Three identifier columns were dropped**: `pbp|EntityId`, `pbp|RowId`,
  `pbp|TeamId`. `coverage.ID_FIELDS` never excluded them, so they had been
  training as ordinary features. EntityId is the NBA player id and RowId
  duplicates it; both run 708 to 1,630,572, and ids are issued in debut order,
  so every player in these seasons sits above the range the model was fit on.
  Removing them costs nothing measurable on the held-out seasons — total R²
  +0.744 against a published +0.751, defense +0.631 against +0.635 — and helps
  offense, +0.832 against +0.821. **This affects the existing models too**, not
  just this report.
- **Two scrape bugs were found and fixed while producing this.** The tracking
  percentages were on the API's 0-1 scale where the collection stores 0-100
  (41 columns across 12 tables), and the passing table's last three columns sit
  one place left of their headers. Uncorrected, these inflated the predicted
  offensive top end from a realistic +7.9 to +24.5. Both are repaired in Mongo
  by `scraping/migrate_tracking_v2.py`; anything read from those documents
  before this report was written is wrong.
- **No position data.** 538 supplied it; nothing we scrape does. Positions are
  carried over by name where 538 ever listed the player (1261/2386) and imputed otherwise. Rerunning
  with every position zeroed moves the offensive top-100 by 7 places and the defensive by 5,
  out of 1200 — the position one-hot is 0.06% of model gain, and it shows.
- **Minutes floor.** Regular season: 1065 min, Playoffs: 131 min. Derived, not chosen: the lowest minutes total 538 itself ever rated in that
  split, so the pool composition matches the one the model was trained against.
- **Three source columns are absent** from the new scrape — `3SecondViolations`,
  `HeaveAttempts`, `HeaveMakes` — because pbpstats omits an event type nobody
  recorded. They arrive as NaN, which LightGBM handles natively.

## 2023-24

### 2023-24 Regular season — top 100 offense

> pool 572 players, 248 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokic | 2737 | +7.92 | +2.07 | +9.66 |
| 2 | Luka Doncic | 2624 | +6.74 | -0.72 | +7.12 |
| 3 | Jalen Brunson | 2726 | +5.79 | -0.95 | +4.19 |
| 4 | Shai Gilgeous-Alexander | 2553 | +5.63 | +0.77 | +6.27 |
| 5 | Stephen Curry | 2421 | +5.52 | -1.85 | +3.81 |
| 6 | Tyrese Haliburton | 2224 | +5.45 | -1.20 | +4.48 |
| 7 | LeBron James | 2504 | +5.34 | -1.42 | +4.61 |
| 8 | Donovan Mitchell | 1943 | +4.99 | +0.16 | +4.91 |
| 9 | Damian Lillard | 2579 | +4.90 | -1.71 | +3.19 |
| 10 | Giannis Antetokounmpo | 2567 | +4.88 | +0.80 | +5.34 |
| 11 | Kyrie Irving | 2030 | +4.87 | -0.55 | +4.69 |
| 12 | Devin Booker | 2447 | +4.83 | -1.52 | +3.04 |
| 13 | Jamal Murray | 1861 | +4.70 | -1.11 | +3.95 |
| 14 | Trae Young | 1942 | +4.68 | -2.58 | +0.94 |
| 15 | Joel Embiid | 1309 | +4.50 | +2.64 | +7.11 |
| 16 | James Harden | 2470 | +4.49 | -1.33 | +3.36 |
| 17 | Jayson Tatum | 2645 | +4.29 | -0.30 | +4.71 |
| 18 | Tyrese Maxey | 2626 | +4.07 | -1.81 | +2.35 |
| 19 | Kevin Durant | 2791 | +3.86 | -0.32 | +3.74 |
| 20 | Paul George | 2502 | +3.78 | +1.59 | +5.60 |
| 21 | De'Aaron Fox | 2659 | +3.66 | +0.20 | +3.82 |
| 22 | Jimmy Butler | 2042 | +3.51 | +0.16 | +3.56 |
| 23 | Kawhi Leonard | 2330 | +3.45 | +1.66 | +5.64 |
| 24 | Zion Williamson | 2207 | +3.23 | -0.75 | +1.37 |
| 25 | Anthony Edwards | 2770 | +3.19 | -1.38 | +2.22 |
| 26 | Fred VanVleet | 2684 | +3.15 | +0.87 | +3.63 |
| 27 | D'Angelo Russell | 2484 | +3.11 | -1.16 | +1.13 |
| 28 | DeMar DeRozan | 2989 | +3.10 | -2.06 | +0.92 |
| 29 | T.J. McConnell | 1291 | +2.94 | +0.30 | +2.52 |
| 30 | CJ McCollum | 2159 | +2.83 | -1.31 | +1.41 |
| 31 | Alperen Sengun | 2046 | +2.79 | +1.23 | +3.73 |
| 32 | Desmond Bane | 1443 | +2.58 | -1.67 | +0.42 |
| 33 | Mike Conley | 2193 | +2.56 | +0.93 | +3.27 |
| 34 | Collin Sexton | 2075 | +2.46 | -2.11 | -0.43 |
| 35 | Brandon Ingram | 2103 | +2.44 | -1.22 | +0.91 |
| 36 | Pascal Siakam | 2658 | +2.41 | -0.77 | +1.39 |
| 37 | Derrick White | 2381 | +2.37 | +1.91 | +3.41 |
| 38 | Dejounte Murray | 2783 | +2.29 | -1.46 | -0.41 |
| 39 | Payton Pritchard | 1826 | +2.29 | -1.52 | +1.11 |
| 40 | Immanuel Quickley | 1985 | +2.17 | -1.43 | +0.08 |
| 41 | Khris Middleton | 1487 | +2.12 | -0.44 | +1.77 |
| 42 | Anfernee Simons | 1582 | +1.97 | -3.59 | -1.38 |
| 43 | Jalen Williams | 2223 | +1.96 | +0.22 | +2.42 |
| 44 | Malcolm Brogdon | 1121 | +1.95 | -1.95 | -0.22 |
| 45 | Julius Randle | 1630 | +1.93 | -0.74 | +0.94 |
| 46 | Malik Monk | 1872 | +1.90 | -1.79 | -0.01 |
| 47 | Terry Rozier | 2040 | +1.88 | -0.48 | +0.20 |
| 48 | Anthony Davis | 2700 | +1.88 | +2.37 | +4.74 |
| 49 | Jalen Green | 2602 | +1.85 | -2.19 | +0.15 |
| 50 | Austin Reaves | 2629 | +1.85 | -1.06 | +0.26 |
| 51 | Bradley Beal | 1767 | +1.84 | -0.92 | +0.79 |
| 52 | Lauri Markkanen | 1820 | +1.81 | -0.19 | +1.84 |
| 53 | Donte DiVincenzo | 2360 | +1.79 | +0.00 | +1.79 |
| 54 | Jaylen Brown | 2343 | +1.77 | -0.24 | +1.95 |
| 55 | Michael Porter Jr. | 2565 | +1.75 | -0.74 | +0.35 |
| 56 | Scottie Barnes | 2094 | +1.74 | +0.22 | +1.48 |
| 57 | Bogdan Bogdanovic | 2401 | +1.62 | -0.13 | +2.09 |
| 58 | Grayson Allen | 2513 | +1.61 | -0.22 | +0.70 |
| 59 | Sam Merrill | 1069 | +1.59 | -0.69 | +0.76 |
| 60 | Tyler Herro | 1407 | +1.54 | -2.19 | -0.36 |
| 61 | Trey Murphy III | 1690 | +1.45 | +0.59 | +2.38 |
| 62 | Cade Cunningham | 2074 | +1.44 | -2.65 | -1.86 |
| 63 | Coby White | 2881 | +1.43 | -0.75 | +0.81 |
| 64 | Darius Garland | 1901 | +1.40 | -1.32 | +0.40 |
| 65 | Chris Paul | 1531 | +1.36 | +0.20 | +1.74 |
| 66 | Kristaps Porzingis | 1690 | +1.33 | +1.90 | +3.71 |
| 67 | Joe Ingles | 1169 | +1.32 | -0.76 | +1.05 |
| 68 | Tre Jones | 2138 | +1.25 | +0.14 | +1.16 |
| 69 | Cam Thomas | 2075 | +1.22 | -2.17 | -1.73 |
| 70 | Cameron Payne | 1304 | +1.22 | -1.09 | -0.39 |
| 71 | Dante Exum | 1088 | +1.16 | +0.38 | +1.13 |
| 72 | Jarrett Allen | 2442 | +1.14 | +1.73 | +2.27 |
| 73 | Jrue Holiday | 2263 | +1.07 | +0.43 | +1.41 |
| 74 | Domantas Sabonis | 2928 | +1.04 | +0.17 | +1.61 |
| 75 | Buddy Hield | 2160 | +1.04 | -1.87 | -0.18 |
| 76 | Isaiah Joe | 1445 | +1.03 | +1.32 | +2.07 |
| 77 | Klay Thompson | 2284 | +0.98 | -1.89 | -1.35 |
| 78 | Franz Wagner | 2337 | +0.98 | +1.12 | +1.42 |
| 79 | Kelly Olynyk | 1759 | +0.96 | +0.25 | +0.91 |
| 80 | Sam Hauser | 1741 | +0.95 | +0.62 | +1.61 |
| 81 | Miles McBride | 1328 | +0.94 | +0.02 | +0.46 |
| 82 | Brandin Podziemski | 1968 | +0.94 | +0.65 | +2.17 |
| 83 | Kentavious Caldwell-Pope | 2402 | +0.92 | +0.30 | +1.04 |
| 84 | Josh Giddey | 2012 | +0.92 | -0.72 | +0.70 |
| 85 | Aaron Gordon | 2297 | +0.91 | +0.68 | +1.38 |
| 86 | Alec Burks | 1211 | +0.90 | -1.72 | -0.96 |
| 87 | Duncan Robinson | 1906 | +0.89 | -1.07 | -0.32 |
| 88 | Tyus Jones | 1933 | +0.89 | -2.17 | -1.93 |
| 89 | Spencer Dinwiddie | 2152 | +0.83 | -1.77 | -0.45 |
| 90 | Tobias Harris | 2368 | +0.78 | -0.37 | +0.39 |
| 91 | Paolo Banchero | 2799 | +0.78 | -1.15 | -0.73 |
| 92 | Moses Moody | 1156 | +0.76 | +0.75 | +1.94 |
| 93 | Isaiah Hartenstein | 1896 | +0.75 | +2.67 | +3.54 |
| 94 | Devin Vassell | 2248 | +0.74 | -1.41 | -0.51 |
| 95 | Malik Beasley | 2337 | +0.72 | -0.59 | +0.93 |
| 96 | Karl-Anthony Towns | 2026 | +0.72 | -0.06 | +2.02 |
| 97 | Max Strus | 2239 | +0.71 | +0.47 | +1.34 |
| 98 | Nicolas Batum | 1529 | +0.69 | +0.99 | +1.51 |
| 99 | Alex Caruso | 2040 | +0.68 | +3.23 | +3.33 |
| 100 | Caris LeVert | 1958 | +0.68 | -0.06 | +1.24 |

### 2023-24 Regular season — top 100 defense

> pool 572 players, 248 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | 2593 | -0.64 | +3.27 | +2.95 |
| 2 | Alex Caruso | 2040 | +0.68 | +3.23 | +3.33 |
| 3 | Victor Wembanyama | 2106 | +0.54 | +2.92 | +3.09 |
| 4 | Jusuf Nurkic | 2078 | -0.04 | +2.77 | +3.41 |
| 5 | Nic Claxton | 2116 | -2.50 | +2.69 | +0.53 |
| 6 | Isaiah Hartenstein | 1896 | +0.75 | +2.67 | +3.54 |
| 7 | Joel Embiid | 1309 | +4.50 | +2.64 | +7.11 |
| 8 | Dean Wade | 1108 | -0.87 | +2.50 | +1.55 |
| 9 | Anthony Davis | 2700 | +1.88 | +2.37 | +4.74 |
| 10 | Brook Lopez | 2411 | -0.63 | +2.31 | +1.45 |
| 11 | Chet Holmgren | 2413 | +0.51 | +2.19 | +3.31 |
| 12 | Ivica Zubac | 1795 | +0.03 | +2.12 | +1.76 |
| 13 | Paul Reed | 1590 | +0.31 | +2.11 | +2.26 |
| 14 | Clint Capela | 1883 | +0.55 | +2.08 | +2.65 |
| 15 | Nikola Jokic | 2737 | +7.92 | +2.07 | +9.66 |
| 16 | Luguentz Dort | 2246 | -0.24 | +2.05 | +1.53 |
| 17 | Andre Drummond | 1351 | -0.14 | +2.05 | +2.13 |
| 18 | Draymond Green | 1490 | +0.03 | +1.95 | +2.76 |
| 19 | Myles Turner | 2077 | -0.48 | +1.94 | +1.78 |
| 20 | Ausar Thompson | 1583 | -1.54 | +1.92 | -0.02 |
| 21 | Derrick White | 2381 | +2.37 | +1.91 | +3.41 |
| 22 | Kristaps Porzingis | 1690 | +1.33 | +1.90 | +3.71 |
| 23 | Walker Kessler | 1493 | -1.53 | +1.89 | -0.14 |
| 24 | Matisse Thybulle | 1487 | -1.49 | +1.86 | +0.34 |
| 25 | Evan Mobley | 1532 | -0.73 | +1.80 | +1.02 |
| 26 | Larry Nance Jr. | 1216 | -0.80 | +1.77 | +1.41 |
| 27 | Daniel Gafford | 1814 | +0.32 | +1.74 | +1.59 |
| 28 | Jarrett Allen | 2442 | +1.14 | +1.73 | +2.27 |
| 29 | Amen Thompson | 1388 | -0.01 | +1.69 | +1.59 |
| 30 | Kawhi Leonard | 2330 | +3.45 | +1.66 | +5.64 |
| 31 | Jakob Poeltl | 1319 | -0.91 | +1.66 | +0.66 |
| 32 | Paul George | 2502 | +3.78 | +1.59 | +5.60 |
| 33 | Wendell Carter Jr. | 1406 | -1.50 | +1.58 | +0.64 |
| 34 | Dyson Daniels | 1358 | -0.63 | +1.58 | +1.29 |
| 35 | Dereck Lively II | 1294 | -0.09 | +1.55 | +1.25 |
| 36 | Vince Williams Jr. | 1436 | -0.24 | +1.53 | +1.05 |
| 37 | Nickeil Alexander-Walker | 1921 | -0.51 | +1.48 | +0.43 |
| 38 | Bam Adebayo | 2416 | -0.25 | +1.46 | +0.92 |
| 39 | Aaron Nesmith | 1995 | -0.34 | +1.43 | +0.82 |
| 40 | Naz Reid | 1964 | -0.50 | +1.42 | +1.10 |
| 41 | Toumani Camara | 1739 | -1.49 | +1.40 | -0.08 |
| 42 | Isaiah Joe | 1445 | +1.03 | +1.32 | +2.07 |
| 43 | Jalen Suggs | 2025 | +0.03 | +1.31 | +1.56 |
| 44 | Derrick Jones Jr. | 1783 | -1.29 | +1.26 | +0.01 |
| 45 | Alperen Sengun | 2046 | +2.79 | +1.23 | +3.73 |
| 46 | Kyle Anderson | 1782 | -1.12 | +1.16 | +0.16 |
| 47 | OG Anunoby | 1702 | -0.50 | +1.15 | +0.51 |
| 48 | Franz Wagner | 2337 | +0.98 | +1.12 | +1.42 |
| 49 | Jalen Johnson | 1889 | -0.17 | +1.12 | +0.64 |
| 50 | Jonas Valanciunas | 1925 | +0.49 | +1.10 | +1.86 |
| 51 | Herbert Jones | 2321 | +0.42 | +1.08 | +1.78 |
| 52 | Al Horford | 1740 | -0.18 | +1.06 | +1.23 |
| 53 | Peyton Watson | 1488 | -2.42 | +1.00 | -1.70 |
| 54 | Nicolas Batum | 1529 | +0.69 | +0.99 | +1.51 |
| 55 | John Konchar | 1173 | -1.62 | +0.98 | -1.03 |
| 56 | Mike Conley | 2193 | +2.56 | +0.93 | +3.27 |
| 57 | Trayce Jackson-Davis | 1130 | +0.16 | +0.92 | +1.46 |
| 58 | Naji Marshall | 1257 | -0.24 | +0.90 | +0.82 |
| 59 | Fred VanVleet | 2684 | +3.15 | +0.87 | +3.63 |
| 60 | Giannis Antetokounmpo | 2567 | +4.88 | +0.80 | +5.34 |
| 61 | Shai Gilgeous-Alexander | 2553 | +5.63 | +0.77 | +6.27 |
| 62 | Kris Dunn | 1249 | -0.24 | +0.77 | +0.35 |
| 63 | Haywood Highsmith | 1366 | -0.55 | +0.75 | -0.28 |
| 64 | Moses Moody | 1156 | +0.76 | +0.75 | +1.94 |
| 65 | Aaron Gordon | 2297 | +0.91 | +0.68 | +1.38 |
| 66 | Royce O'Neale | 1954 | +0.17 | +0.67 | +0.81 |
| 67 | Brandin Podziemski | 1968 | +0.94 | +0.65 | +2.17 |
| 68 | Sam Hauser | 1741 | +0.95 | +0.62 | +1.61 |
| 69 | Josh Hart | 2707 | -0.10 | +0.60 | +1.00 |
| 70 | Anthony Black | 1164 | -2.28 | +0.60 | -1.90 |
| 71 | Trey Murphy III | 1690 | +1.45 | +0.59 | +2.38 |
| 72 | Jaren Jackson Jr. | 2124 | -0.42 | +0.58 | -0.67 |
| 73 | Kyle Lowry | 1690 | +0.23 | +0.54 | +0.54 |
| 74 | Precious Achiuwa | 1624 | -1.21 | +0.54 | -0.59 |
| 75 | Kevon Looney | 1195 | -0.68 | +0.51 | -0.02 |
| 76 | Caleb Martin | 1756 | -0.54 | +0.50 | +0.15 |
| 77 | Aaron Wiggins | 1228 | +0.02 | +0.47 | +0.64 |
| 78 | Max Strus | 2239 | +0.71 | +0.47 | +1.34 |
| 79 | Isaiah Stewart | 1423 | -1.64 | +0.46 | -2.10 |
| 80 | Kris Murray | 1348 | -1.80 | +0.45 | -1.80 |
| 81 | Jrue Holiday | 2263 | +1.07 | +0.43 | +1.41 |
| 82 | Nikola Vucevic | 2610 | +0.14 | +0.39 | -0.17 |
| 83 | Dante Exum | 1088 | +1.16 | +0.38 | +1.13 |
| 84 | Cason Wallace | 1692 | -0.89 | +0.34 | +0.03 |
| 85 | Jalen Duren | 1778 | -0.76 | +0.34 | -0.66 |
| 86 | Isaac Okoro | 1887 | -0.44 | +0.33 | +0.18 |
| 87 | Keegan Murray | 2589 | +0.65 | +0.33 | +1.21 |
| 88 | Christian Braun | 1656 | -1.00 | +0.31 | -0.94 |
| 89 | Kentavious Caldwell-Pope | 2402 | +0.92 | +0.30 | +1.04 |
| 90 | T.J. McConnell | 1291 | +2.94 | +0.30 | +2.52 |
| 91 | Kelly Olynyk | 1759 | +0.96 | +0.25 | +0.91 |
| 92 | Jalen Williams | 2223 | +1.96 | +0.22 | +2.42 |
| 93 | Scottie Barnes | 2094 | +1.74 | +0.22 | +1.48 |
| 94 | De'Aaron Fox | 2659 | +3.66 | +0.20 | +3.82 |
| 95 | Chris Paul | 1531 | +1.36 | +0.20 | +1.74 |
| 96 | Domantas Sabonis | 2928 | +1.04 | +0.17 | +1.61 |
| 97 | Jimmy Butler | 2042 | +3.51 | +0.16 | +3.56 |
| 98 | Donovan Mitchell | 1943 | +4.99 | +0.16 | +4.91 |
| 99 | Tre Jones | 2138 | +1.25 | +0.14 | +1.16 |
| 100 | Deandre Ayton | 1784 | -1.16 | +0.08 | -0.07 |

### 2023-24 Playoffs — top 100 offense

> pool 214 players, 103 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokic | 482 | +7.43 | +0.96 | +8.04 |
| 2 | Joel Embiid | 248 | +6.27 | +2.59 | +8.32 |
| 3 | Luka Doncic | 900 | +5.60 | +0.56 | +6.63 |
| 4 | LeBron James | 204 | +5.08 | +0.37 | +4.76 |
| 5 | Tyrese Haliburton | 522 | +5.03 | -0.13 | +5.41 |
| 6 | Damian Lillard | 156 | +4.82 | -2.99 | +1.55 |
| 7 | Devin Booker | 166 | +4.66 | +0.02 | +2.62 |
| 8 | Kevin Durant | 168 | +4.65 | -2.75 | +0.50 |
| 9 | Jalen Brunson | 518 | +4.56 | -1.59 | +3.50 |
| 10 | Shai Gilgeous-Alexander | 399 | +4.50 | +0.78 | +5.55 |
| 11 | Tyrese Maxey | 267 | +4.24 | -4.29 | +0.55 |
| 12 | Kyrie Irving | 879 | +4.23 | -1.62 | +2.52 |
| 13 | Austin Reaves | 174 | +4.11 | -1.00 | +3.14 |
| 14 | Khris Middleton | 230 | +3.96 | -1.42 | +3.10 |
| 15 | Donovan Mitchell | 382 | +3.95 | -0.51 | +4.60 |
| 16 | Jayson Tatum | 768 | +3.90 | +0.46 | +4.94 |
| 17 | Anthony Edwards | 649 | +3.86 | +0.38 | +5.40 |
| 18 | T.J. McConnell | 348 | +3.28 | -0.09 | +2.58 |
| 19 | Andrew Nembhard | 554 | +3.01 | -1.95 | +0.79 |
| 20 | Paul George | 222 | +2.91 | +0.84 | +3.57 |
| 21 | James Harden | 242 | +2.89 | -2.13 | +1.19 |
| 22 | Pascal Siakam | 603 | +2.83 | -0.79 | +2.64 |
| 23 | Mike Conley | 474 | +2.67 | +0.92 | +3.96 |
| 24 | Anthony Davis | 208 | +2.59 | +0.02 | +2.50 |
| 25 | Derrick White | 676 | +2.50 | +1.57 | +3.32 |
| 26 | Jaylen Brown | 707 | +2.36 | +0.74 | +3.50 |
| 27 | Jalen Williams | 377 | +2.15 | +2.36 | +3.83 |
| 28 | Jamal Murray | 462 | +1.85 | -3.65 | -2.39 |
| 29 | Michael Porter Jr. | 443 | +1.84 | -2.11 | -0.42 |
| 30 | Donte DiVincenzo | 466 | +1.77 | -1.84 | -0.73 |
| 31 | Sam Hauser | 283 | +1.71 | +1.00 | +2.35 |
| 32 | Jaden McDaniels | 537 | +1.64 | +0.06 | +0.99 |
| 33 | Jrue Holiday | 720 | +1.59 | +1.43 | +2.48 |
| 34 | Myles Turner | 550 | +1.52 | -0.04 | +1.59 |
| 35 | Kristaps Porzingis | 165 | +1.50 | +3.41 | +4.70 |
| 36 | Chet Holmgren | 345 | +1.46 | +3.10 | +3.90 |
| 37 | Bobby Portis | 187 | +1.37 | +1.07 | +2.32 |
| 38 | Al Horford | 575 | +1.35 | +2.18 | +3.42 |
| 39 | Paolo Banchero | 262 | +1.27 | +2.72 | +3.68 |
| 40 | Karl-Anthony Towns | 522 | +1.20 | -0.14 | +1.55 |
| 41 | Rudy Gobert | 512 | +1.18 | +2.45 | +3.50 |
| 42 | Kyle Lowry | 175 | +1.17 | +0.05 | +2.07 |
| 43 | Aaron Gordon | 445 | +1.16 | -1.10 | -0.62 |
| 44 | Ivica Zubac | 192 | +1.09 | +2.19 | +4.31 |
| 45 | Bam Adebayo | 192 | +0.90 | +0.41 | -0.14 |
| 46 | Kelly Oubre Jr. | 224 | +0.90 | +0.86 | +1.49 |
| 47 | Dereck Lively II | 462 | +0.77 | +3.04 | +3.00 |
| 48 | Aaron Wiggins | 157 | +0.75 | +2.49 | +3.16 |
| 49 | Brook Lopez | 200 | +0.73 | -2.46 | -1.02 |
| 50 | Norman Powell | 179 | +0.72 | -0.93 | -0.36 |
| 51 | Miles McBride | 347 | +0.72 | -1.45 | -1.75 |
| 52 | Franz Wagner | 259 | +0.64 | +0.91 | +1.69 |
| 53 | Patrick Beverley | 210 | +0.62 | -1.43 | +0.15 |
| 54 | Isaiah Joe | 173 | +0.55 | +1.00 | +1.28 |
| 55 | Josh Green | 399 | +0.46 | +1.03 | +1.24 |
| 56 | Kentavious Caldwell-Pope | 420 | +0.42 | +1.19 | +2.06 |
| 57 | Kyle Anderson | 231 | +0.35 | +0.13 | +0.08 |
| 58 | Tyler Herro | 185 | +0.22 | -0.11 | -0.59 |
| 59 | OG Anunoby | 324 | +0.21 | +0.48 | +1.00 |
| 60 | Bradley Beal | 154 | +0.16 | -3.00 | -3.43 |
| 61 | Derrick Jones Jr. | 647 | +0.15 | +0.03 | +0.63 |
| 62 | Terance Mann | 187 | +0.12 | -2.93 | -2.54 |
| 63 | Obi Toppin | 343 | +0.06 | -0.77 | -1.14 |
| 64 | Jalen Suggs | 232 | +0.04 | +1.25 | +2.23 |
| 65 | Payton Pritchard | 355 | -0.01 | -2.18 | -2.13 |
| 66 | CJ McCollum | 148 | -0.03 | -2.29 | -4.07 |
| 67 | D'Angelo Russell | 185 | -0.05 | -1.02 | -0.79 |
| 68 | Luguentz Dort | 350 | -0.10 | +1.41 | +1.94 |
| 69 | Ben Sheppard | 335 | -0.13 | -1.61 | +0.36 |
| 70 | Darius Garland | 432 | -0.20 | -1.63 | -2.21 |
| 71 | Max Strus | 434 | -0.21 | -1.86 | -1.09 |
| 72 | Josh Hart | 548 | -0.28 | -0.08 | -0.59 |
| 73 | Malik Beasley | 131 | -0.33 | -1.14 | -1.62 |
| 74 | P.J. Washington | 785 | -0.38 | +0.05 | -0.03 |
| 75 | Aaron Nesmith | 559 | -0.38 | +0.19 | -0.11 |
| 76 | Caleb Martin | 176 | -0.41 | -2.34 | -3.62 |
| 77 | Cason Wallace | 198 | -0.50 | +2.57 | +2.09 |
| 78 | Isaiah Hartenstein | 388 | -0.58 | -1.52 | -2.35 |
| 79 | Precious Achiuwa | 184 | -0.58 | -1.07 | -2.02 |
| 80 | Naz Reid | 361 | -0.64 | -1.22 | -1.15 |
| 81 | Nickeil Alexander-Walker | 378 | -0.71 | -0.68 | -1.11 |
| 82 | Daniel Gafford | 445 | -0.74 | +0.51 | -0.76 |
| 83 | Isaiah Jackson | 154 | -0.76 | +0.29 | -0.18 |
| 84 | Josh Giddey | 181 | -0.79 | +1.17 | +0.32 |
| 85 | Tim Hardaway Jr. | 178 | -0.97 | -2.35 | -3.29 |
| 86 | Maxi Kleber | 219 | -0.98 | -0.21 | -1.10 |
| 87 | Caris LeVert | 278 | -0.99 | -0.99 | -0.74 |
| 88 | Luke Kornet | 133 | -1.01 | -1.62 | -2.40 |
| 89 | Trey Murphy III | 168 | -1.03 | -0.13 | -0.60 |
| 90 | Wendell Carter Jr. | 185 | -1.10 | +1.39 | +1.55 |
| 91 | Tobias Harris | 218 | -1.12 | -0.20 | -2.13 |
| 92 | Jonathan Isaac | 147 | -1.13 | +1.36 | +0.94 |
| 93 | Nicolas Batum | 170 | -1.18 | -0.94 | -2.33 |
| 94 | Marcus Morris Sr. | 138 | -1.24 | -2.48 | -3.98 |
| 95 | Evan Mobley | 422 | -1.49 | +2.84 | +0.57 |
| 96 | Gary Harris | 159 | -1.84 | +0.79 | -1.06 |
| 97 | Justin Holiday | 150 | -2.04 | +4.36 | +2.70 |
| 98 | Dante Exum | 144 | -2.27 | -0.20 | -2.94 |
| 99 | Herbert Jones | 141 | -2.32 | -0.14 | -1.41 |
| 100 | Christian Braun | 204 | -2.57 | +0.22 | -1.90 |

### 2023-24 Playoffs — top 100 defense

> pool 214 players, 103 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Justin Holiday | 150 | -2.04 | +4.36 | +2.70 |
| 2 | Kristaps Porzingis | 165 | +1.50 | +3.41 | +4.70 |
| 3 | Chet Holmgren | 345 | +1.46 | +3.10 | +3.90 |
| 4 | Dereck Lively II | 462 | +0.77 | +3.04 | +3.00 |
| 5 | Evan Mobley | 422 | -1.49 | +2.84 | +0.57 |
| 6 | Paolo Banchero | 262 | +1.27 | +2.72 | +3.68 |
| 7 | Joel Embiid | 248 | +6.27 | +2.59 | +8.32 |
| 8 | Cason Wallace | 198 | -0.50 | +2.57 | +2.09 |
| 9 | Aaron Wiggins | 157 | +0.75 | +2.49 | +3.16 |
| 10 | Rudy Gobert | 512 | +1.18 | +2.45 | +3.50 |
| 11 | Jalen Williams | 377 | +2.15 | +2.36 | +3.83 |
| 12 | Ivica Zubac | 192 | +1.09 | +2.19 | +4.31 |
| 13 | Al Horford | 575 | +1.35 | +2.18 | +3.42 |
| 14 | Derrick White | 676 | +2.50 | +1.57 | +3.32 |
| 15 | Isaac Okoro | 263 | -3.90 | +1.50 | -1.69 |
| 16 | Brandon Ingram | 145 | -3.33 | +1.43 | -0.69 |
| 17 | Jrue Holiday | 720 | +1.59 | +1.43 | +2.48 |
| 18 | Luguentz Dort | 350 | -0.10 | +1.41 | +1.94 |
| 19 | Wendell Carter Jr. | 185 | -1.10 | +1.39 | +1.55 |
| 20 | Jonathan Isaac | 147 | -1.13 | +1.36 | +0.94 |
| 21 | Jalen Suggs | 232 | +0.04 | +1.25 | +2.23 |
| 22 | Kentavious Caldwell-Pope | 420 | +0.42 | +1.19 | +2.06 |
| 23 | Josh Giddey | 181 | -0.79 | +1.17 | +0.32 |
| 24 | Bobby Portis | 187 | +1.37 | +1.07 | +2.32 |
| 25 | Josh Green | 399 | +0.46 | +1.03 | +1.24 |
| 26 | Isaiah Joe | 173 | +0.55 | +1.00 | +1.28 |
| 27 | Sam Hauser | 283 | +1.71 | +1.00 | +2.35 |
| 28 | Nikola Jokic | 482 | +7.43 | +0.96 | +8.04 |
| 29 | Mike Conley | 474 | +2.67 | +0.92 | +3.96 |
| 30 | Franz Wagner | 259 | +0.64 | +0.91 | +1.69 |
| 31 | Kelly Oubre Jr. | 224 | +0.90 | +0.86 | +1.49 |
| 32 | Paul George | 222 | +2.91 | +0.84 | +3.57 |
| 33 | Gary Harris | 159 | -1.84 | +0.79 | -1.06 |
| 34 | Shai Gilgeous-Alexander | 399 | +4.50 | +0.78 | +5.55 |
| 35 | Jaylen Brown | 707 | +2.36 | +0.74 | +3.50 |
| 36 | Luka Doncic | 900 | +5.60 | +0.56 | +6.63 |
| 37 | Daniel Gafford | 445 | -0.74 | +0.51 | -0.76 |
| 38 | OG Anunoby | 324 | +0.21 | +0.48 | +1.00 |
| 39 | Jayson Tatum | 768 | +3.90 | +0.46 | +4.94 |
| 40 | Bam Adebayo | 192 | +0.90 | +0.41 | -0.14 |
| 41 | Anthony Edwards | 649 | +3.86 | +0.38 | +5.40 |
| 42 | LeBron James | 204 | +5.08 | +0.37 | +4.76 |
| 43 | Isaiah Jackson | 154 | -0.76 | +0.29 | -0.18 |
| 44 | Christian Braun | 204 | -2.57 | +0.22 | -1.90 |
| 45 | Aaron Nesmith | 559 | -0.38 | +0.19 | -0.11 |
| 46 | Kyle Anderson | 231 | +0.35 | +0.13 | +0.08 |
| 47 | Jaden McDaniels | 537 | +1.64 | +0.06 | +0.99 |
| 48 | P.J. Washington | 785 | -0.38 | +0.05 | -0.03 |
| 49 | Kyle Lowry | 175 | +1.17 | +0.05 | +2.07 |
| 50 | Derrick Jones Jr. | 647 | +0.15 | +0.03 | +0.63 |
| 51 | Anthony Davis | 208 | +2.59 | +0.02 | +2.50 |
| 52 | Devin Booker | 166 | +4.66 | +0.02 | +2.62 |
| 53 | Myles Turner | 550 | +1.52 | -0.04 | +1.59 |
| 54 | Josh Hart | 548 | -0.28 | -0.08 | -0.59 |
| 55 | T.J. McConnell | 348 | +3.28 | -0.09 | +2.58 |
| 56 | Tyler Herro | 185 | +0.22 | -0.11 | -0.59 |
| 57 | Tyrese Haliburton | 522 | +5.03 | -0.13 | +5.41 |
| 58 | Trey Murphy III | 168 | -1.03 | -0.13 | -0.60 |
| 59 | Herbert Jones | 141 | -2.32 | -0.14 | -1.41 |
| 60 | Karl-Anthony Towns | 522 | +1.20 | -0.14 | +1.55 |
| 61 | Tobias Harris | 218 | -1.12 | -0.20 | -2.13 |
| 62 | Dante Exum | 144 | -2.27 | -0.20 | -2.94 |
| 63 | Maxi Kleber | 219 | -0.98 | -0.21 | -1.10 |
| 64 | Donovan Mitchell | 382 | +3.95 | -0.51 | +4.60 |
| 65 | Nickeil Alexander-Walker | 378 | -0.71 | -0.68 | -1.11 |
| 66 | Obi Toppin | 343 | +0.06 | -0.77 | -1.14 |
| 67 | Pascal Siakam | 603 | +2.83 | -0.79 | +2.64 |
| 68 | Norman Powell | 179 | +0.72 | -0.93 | -0.36 |
| 69 | Nicolas Batum | 170 | -1.18 | -0.94 | -2.33 |
| 70 | Caris LeVert | 278 | -0.99 | -0.99 | -0.74 |
| 71 | Austin Reaves | 174 | +4.11 | -1.00 | +3.14 |
| 72 | D'Angelo Russell | 185 | -0.05 | -1.02 | -0.79 |
| 73 | Precious Achiuwa | 184 | -0.58 | -1.07 | -2.02 |
| 74 | Aaron Gordon | 445 | +1.16 | -1.10 | -0.62 |
| 75 | Malik Beasley | 131 | -0.33 | -1.14 | -1.62 |
| 76 | Naz Reid | 361 | -0.64 | -1.22 | -1.15 |
| 77 | Khris Middleton | 230 | +3.96 | -1.42 | +3.10 |
| 78 | Patrick Beverley | 210 | +0.62 | -1.43 | +0.15 |
| 79 | Miles McBride | 347 | +0.72 | -1.45 | -1.75 |
| 80 | Isaiah Hartenstein | 388 | -0.58 | -1.52 | -2.35 |
| 81 | Jalen Brunson | 518 | +4.56 | -1.59 | +3.50 |
| 82 | Ben Sheppard | 335 | -0.13 | -1.61 | +0.36 |
| 83 | Luke Kornet | 133 | -1.01 | -1.62 | -2.40 |
| 84 | Kyrie Irving | 879 | +4.23 | -1.62 | +2.52 |
| 85 | Darius Garland | 432 | -0.20 | -1.63 | -2.21 |
| 86 | Donte DiVincenzo | 466 | +1.77 | -1.84 | -0.73 |
| 87 | Max Strus | 434 | -0.21 | -1.86 | -1.09 |
| 88 | Andrew Nembhard | 554 | +3.01 | -1.95 | +0.79 |
| 89 | Michael Porter Jr. | 443 | +1.84 | -2.11 | -0.42 |
| 90 | James Harden | 242 | +2.89 | -2.13 | +1.19 |
| 91 | Payton Pritchard | 355 | -0.01 | -2.18 | -2.13 |
| 92 | Rui Hachimura | 152 | -3.47 | -2.27 | -4.55 |
| 93 | CJ McCollum | 148 | -0.03 | -2.29 | -4.07 |
| 94 | Caleb Martin | 176 | -0.41 | -2.34 | -3.62 |
| 95 | Tim Hardaway Jr. | 178 | -0.97 | -2.35 | -3.29 |
| 96 | Brook Lopez | 200 | +0.73 | -2.46 | -1.02 |
| 97 | Marcus Morris Sr. | 138 | -1.24 | -2.48 | -3.98 |
| 98 | Kevin Durant | 168 | +4.65 | -2.75 | +0.50 |
| 99 | Terance Mann | 187 | +0.12 | -2.93 | -2.54 |
| 100 | Damian Lillard | 156 | +4.82 | -2.99 | +1.55 |

## 2024-25

### 2024-25 Regular season — top 100 offense

> pool 569 players, 257 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokić | 2571 | +8.30 | +0.85 | +8.74 |
| 2 | Stephen Curry | 2252 | +6.90 | -2.16 | +4.66 |
| 3 | Shai Gilgeous-Alexander | 2598 | +6.18 | +0.87 | +6.76 |
| 4 | Luka Dončić | 1769 | +5.79 | +0.69 | +6.78 |
| 5 | Damian Lillard | 2093 | +5.74 | -2.56 | +3.21 |
| 6 | James Harden | 2789 | +5.69 | -0.83 | +5.28 |
| 7 | Giannis Antetokounmpo | 2289 | +5.00 | +1.07 | +5.37 |
| 8 | Tyrese Haliburton | 2451 | +4.75 | -0.79 | +4.39 |
| 9 | Donovan Mitchell | 2232 | +4.69 | -0.74 | +4.37 |
| 10 | LaMelo Ball | 1505 | +4.63 | -2.67 | +1.22 |
| 11 | Jayson Tatum | 2624 | +4.62 | -0.58 | +4.63 |
| 12 | Jalen Brunson | 2301 | +4.59 | -2.18 | +2.06 |
| 13 | Trae Young | 2739 | +4.43 | -3.39 | +0.75 |
| 14 | Darius Garland | 2301 | +4.42 | -1.11 | +3.66 |
| 15 | Tyler Herro | 2725 | +4.23 | -1.85 | +2.99 |
| 16 | Ty Jerome | 1393 | +4.17 | -0.59 | +3.15 |
| 17 | Jamal Murray | 2418 | +3.77 | -0.98 | +2.87 |
| 18 | LeBron James | 2444 | +3.76 | -1.74 | +0.83 |
| 19 | Devin Booker | 2795 | +3.72 | -1.59 | +1.24 |
| 20 | Cade Cunningham | 2452 | +3.48 | -1.82 | +1.93 |
| 21 | Anthony Edwards | 2871 | +3.44 | -1.27 | +2.47 |
| 22 | Austin Reaves | 2550 | +3.41 | -0.65 | +2.90 |
| 23 | Kyrie Irving | 1804 | +3.24 | -0.92 | +2.61 |
| 24 | Tyrese Maxey | 1960 | +2.94 | -0.99 | +0.59 |
| 25 | Kevin Durant | 2265 | +2.89 | -0.81 | +2.29 |
| 26 | Ja Morant | 1519 | +2.84 | -1.47 | +1.31 |
| 27 | Jimmy Butler | 1746 | +2.71 | +0.80 | +3.82 |
| 28 | Franz Wagner | 2023 | +2.58 | +0.44 | +2.77 |
| 29 | DeMar DeRozan | 2768 | +2.55 | -1.52 | +1.22 |
| 30 | Kawhi Leonard | 1180 | +2.48 | +0.85 | +4.21 |
| 31 | Payton Pritchard | 2271 | +2.47 | -0.93 | +2.17 |
| 32 | Norman Powell | 1958 | +2.46 | -0.61 | +2.76 |
| 33 | Karl-Anthony Towns | 2517 | +2.41 | +0.31 | +3.52 |
| 34 | Derrick White | 2574 | +2.36 | +1.28 | +3.49 |
| 35 | Jaylen Brown | 2158 | +2.27 | -0.47 | +1.75 |
| 36 | Cameron Payne | 1090 | +2.27 | +0.32 | +2.29 |
| 37 | Paolo Banchero | 1582 | +2.26 | -1.10 | +0.46 |
| 38 | Desmond Bane | 2205 | +2.19 | -1.34 | +0.71 |
| 39 | Chris Paul | 2292 | +2.17 | -0.74 | +0.86 |
| 40 | Christian Braun | 2675 | +2.16 | -0.63 | +1.19 |
| 41 | Anfernee Simons | 2292 | +2.07 | -2.66 | -1.44 |
| 42 | Zach LaVine | 2603 | +2.01 | -2.67 | -0.50 |
| 43 | Coby White | 2450 | +1.97 | -1.95 | -0.34 |
| 44 | Isaiah Joe | 1604 | +1.91 | +0.45 | +2.28 |
| 45 | Victor Wembanyama | 1527 | +1.88 | +3.18 | +5.45 |
| 46 | Ivica Zubac | 2624 | +1.81 | +3.51 | +4.98 |
| 47 | Michael Porter Jr. | 2593 | +1.81 | -1.30 | +0.19 |
| 48 | Jalen Williams | 2237 | +1.77 | +0.79 | +2.37 |
| 49 | De'Aaron Fox | 2241 | +1.75 | +0.10 | +1.23 |
| 50 | Anthony Davis | 1706 | +1.73 | +2.14 | +3.92 |
| 51 | Malik Monk | 2054 | +1.70 | -0.68 | +1.19 |
| 52 | Jalen Green | 2697 | +1.64 | -2.17 | +0.16 |
| 53 | Aaron Gordon | 1447 | +1.63 | -1.08 | +0.21 |
| 54 | Collin Sexton | 1758 | +1.61 | -2.44 | -0.97 |
| 55 | Domantas Sabonis | 2429 | +1.57 | +0.56 | +2.78 |
| 56 | Jordan Poole | 2001 | +1.55 | -1.40 | -0.42 |
| 57 | Josh Giddey | 2117 | +1.52 | -0.66 | +0.22 |
| 58 | Fred VanVleet | 2113 | +1.47 | +0.90 | +2.46 |
| 59 | D'Angelo Russell | 1481 | +1.46 | -0.02 | +1.32 |
| 60 | CJ McCollum | 1832 | +1.43 | -2.56 | -0.85 |
| 61 | Julius Randle | 2226 | +1.41 | -1.21 | -0.07 |
| 62 | Cameron Johnson | 1800 | +1.35 | -1.52 | +0.22 |
| 63 | Tyus Jones | 2174 | +1.33 | -1.18 | -0.11 |
| 64 | Harrison Barnes | 2228 | +1.29 | -1.25 | -0.44 |
| 65 | Josh Hart | 2897 | +1.28 | +0.28 | +1.96 |
| 66 | Sam Hauser | 1541 | +1.28 | -0.34 | +1.33 |
| 67 | Daniel Gafford | 1226 | +1.25 | +1.67 | +2.78 |
| 68 | Deni Avdija | 2161 | +1.25 | -0.55 | +1.86 |
| 69 | Nikola Vučević | 2278 | +1.24 | +0.05 | +0.69 |
| 70 | Luke Kennard | 1472 | +1.23 | -1.97 | -0.68 |
| 71 | Pascal Siakam | 2548 | +1.22 | +0.83 | +2.01 |
| 72 | Malik Beasley | 2283 | +1.21 | -1.58 | +0.32 |
| 73 | Brandin Podziemski | 1716 | +1.20 | +1.34 | +2.62 |
| 74 | Donte DiVincenzo | 1606 | +1.19 | +0.98 | +2.31 |
| 75 | Aaron Nesmith | 1123 | +1.16 | +0.65 | +1.47 |
| 76 | Mikal Bridges | 3036 | +1.13 | -0.72 | +0.21 |
| 77 | Gary Trent Jr. | 1893 | +1.12 | -1.25 | +0.13 |
| 78 | Mike Conley | 1756 | +1.09 | +1.70 | +2.96 |
| 79 | Luke Kornet | 1361 | +1.05 | +2.42 | +3.42 |
| 80 | Alperen Sengun | 2394 | +1.05 | +2.58 | +3.41 |
| 81 | Jalen Duren | 2035 | +0.98 | +0.24 | +1.77 |
| 82 | Jaren Jackson Jr. | 2207 | +0.95 | +1.12 | +1.75 |
| 83 | Grayson Allen | 1544 | +0.92 | -0.80 | -0.06 |
| 84 | Max Strus | 1273 | +0.90 | -0.29 | +0.51 |
| 85 | RJ Barrett | 1869 | +0.90 | -1.11 | -0.97 |
| 86 | Aaron Wiggins | 1744 | +0.90 | -0.16 | +1.27 |
| 87 | Santi Aldama | 1660 | +0.88 | -0.42 | +0.55 |
| 88 | Dennis Schröder | 2106 | +0.85 | -0.35 | +0.29 |
| 89 | Buddy Hield | 1864 | +0.84 | -1.14 | +0.59 |
| 90 | Jarrett Allen | 2296 | +0.79 | +2.24 | +3.05 |
| 91 | Kristaps Porziņģis | 1210 | +0.76 | +1.93 | +2.57 |
| 92 | De'Andre Hunter | 1740 | +0.72 | -0.89 | -0.11 |
| 93 | Shaedon Sharpe | 2252 | +0.71 | -1.83 | -1.13 |
| 94 | Duncan Robinson | 1786 | +0.67 | -0.83 | +0.09 |
| 95 | Andrew Wiggins | 1842 | +0.65 | -0.13 | -0.04 |
| 96 | Naz Reid | 2200 | +0.61 | +1.28 | +1.81 |
| 97 | John Collins | 1220 | +0.60 | +0.57 | +1.13 |
| 98 | Sam Merrill | 1401 | +0.58 | +1.24 | +1.49 |
| 99 | Lauri Markkanen | 1476 | +0.57 | -0.74 | -0.61 |
| 100 | Klay Thompson | 1965 | +0.56 | -1.62 | -1.31 |

### 2024-25 Regular season — top 100 defense

> pool 569 players, 257 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | 2388 | +0.05 | +3.66 | +3.37 |
| 2 | Ivica Zubac | 2624 | +1.81 | +3.51 | +4.98 |
| 3 | Victor Wembanyama | 1527 | +1.88 | +3.18 | +5.45 |
| 4 | Toumani Camara | 2548 | -0.27 | +2.81 | +2.15 |
| 5 | Kris Dunn | 1783 | -0.76 | +2.80 | +2.23 |
| 6 | Luguentz Dort | 2073 | +0.42 | +2.66 | +2.50 |
| 7 | Alperen Sengun | 2394 | +1.05 | +2.58 | +3.41 |
| 8 | Luke Kornet | 1361 | +1.05 | +2.42 | +3.42 |
| 9 | Evan Mobley | 2167 | +0.21 | +2.29 | +2.78 |
| 10 | Jarrett Allen | 2296 | +0.79 | +2.24 | +3.05 |
| 11 | Donovan Clingan | 1324 | -0.97 | +2.17 | +1.19 |
| 12 | Anthony Davis | 1706 | +1.73 | +2.14 | +3.92 |
| 13 | Jaxson Hayes | 1093 | -1.99 | +2.08 | +0.34 |
| 14 | Ausar Thompson | 1328 | -0.31 | +2.06 | +1.79 |
| 15 | Draymond Green | 1983 | +0.20 | +2.06 | +2.82 |
| 16 | Walker Kessler | 1740 | -0.25 | +2.02 | +1.44 |
| 17 | Kevon Looney | 1142 | +0.14 | +1.97 | +1.57 |
| 18 | Isaiah Hartenstein | 1590 | -0.00 | +1.96 | +1.82 |
| 19 | Dyson Daniels | 2571 | +0.24 | +1.96 | +1.73 |
| 20 | Kristaps Porziņģis | 1210 | +0.76 | +1.93 | +2.57 |
| 21 | Brandon Clarke | 1207 | +0.01 | +1.89 | +2.22 |
| 22 | Nicolas Batum | 1367 | -0.35 | +1.79 | +1.16 |
| 23 | Amen Thompson | 2225 | +0.21 | +1.75 | +1.89 |
| 24 | Jaden McDaniels | 2614 | -0.76 | +1.74 | +0.79 |
| 25 | Mike Conley | 1756 | +1.09 | +1.70 | +2.96 |
| 26 | Myles Turner | 2174 | -0.56 | +1.67 | +1.40 |
| 27 | Daniel Gafford | 1226 | +1.25 | +1.67 | +2.78 |
| 28 | Keon Ellis | 1948 | +0.40 | +1.59 | +2.07 |
| 29 | P.J. Washington | 1835 | -0.50 | +1.54 | +1.48 |
| 30 | Jonathan Isaac | 1090 | -1.25 | +1.53 | +0.20 |
| 31 | Mason Plumlee | 1300 | -0.83 | +1.48 | +0.73 |
| 32 | Isaiah Stewart | 1434 | -2.28 | +1.48 | -0.77 |
| 33 | Goga Bitadze | 1430 | -0.40 | +1.48 | +1.24 |
| 34 | Tari Eason | 1420 | +0.38 | +1.47 | +1.61 |
| 35 | Jakob Poeltl | 1686 | +0.25 | +1.44 | +1.53 |
| 36 | Kelly Oubre Jr. | 2078 | -0.96 | +1.43 | -0.11 |
| 37 | Al Horford | 1659 | -0.28 | +1.39 | +1.14 |
| 38 | Brandin Podziemski | 1716 | +1.20 | +1.34 | +2.62 |
| 39 | Kentavious Caldwell-Pope | 2279 | -0.63 | +1.32 | +0.52 |
| 40 | Naz Reid | 2200 | +0.61 | +1.28 | +1.81 |
| 41 | Derrick White | 2574 | +2.36 | +1.28 | +3.49 |
| 42 | Scotty Pippen Jr. | 1683 | +0.34 | +1.26 | +1.84 |
| 43 | Sam Merrill | 1401 | +0.58 | +1.24 | +1.49 |
| 44 | Wendell Carter Jr. | 1758 | -1.77 | +1.23 | -0.22 |
| 45 | Cody Martin | 1173 | -1.35 | +1.18 | +0.06 |
| 46 | Jalen Johnson | 1284 | +0.03 | +1.18 | +1.29 |
| 47 | Paul George | 1334 | -0.22 | +1.16 | +1.03 |
| 48 | Jaren Jackson Jr. | 2207 | +0.95 | +1.12 | +1.75 |
| 49 | Zach Edey | 1416 | -0.22 | +1.11 | +0.80 |
| 50 | Giannis Antetokounmpo | 2289 | +5.00 | +1.07 | +5.37 |
| 51 | Clint Capela | 1176 | -1.15 | +1.07 | -0.75 |
| 52 | Kel'el Ware | 1422 | -0.92 | +1.05 | +0.80 |
| 53 | Jrue Holiday | 1896 | +0.29 | +1.00 | +0.97 |
| 54 | Dean Wade | 1252 | -0.81 | +1.00 | +0.08 |
| 55 | Cason Wallace | 1876 | -0.02 | +1.00 | +1.57 |
| 56 | Donte DiVincenzo | 1606 | +1.19 | +0.98 | +2.31 |
| 57 | Brook Lopez | 2546 | -0.35 | +0.97 | +1.10 |
| 58 | Haywood Highsmith | 1818 | -0.57 | +0.93 | +0.76 |
| 59 | Moussa Diabaté | 1241 | -0.49 | +0.91 | +0.16 |
| 60 | Davion Mitchell | 2027 | +0.14 | +0.91 | +0.45 |
| 61 | Fred VanVleet | 2113 | +1.47 | +0.90 | +2.46 |
| 62 | Jabari Smith Jr. | 1716 | -0.51 | +0.89 | +0.13 |
| 63 | Dorian Finney-Smith | 1821 | +0.32 | +0.88 | +0.58 |
| 64 | Keegan Murray | 2610 | -0.53 | +0.87 | +0.62 |
| 65 | Shai Gilgeous-Alexander | 2598 | +6.18 | +0.87 | +6.76 |
| 66 | Scottie Barnes | 2134 | +0.44 | +0.86 | +0.33 |
| 67 | Nikola Jokić | 2571 | +8.30 | +0.85 | +8.74 |
| 68 | Kenrich Williams | 1132 | +0.28 | +0.85 | +1.77 |
| 69 | Kawhi Leonard | 1180 | +2.48 | +0.85 | +4.21 |
| 70 | Javonte Green | 1258 | -1.86 | +0.84 | -1.18 |
| 71 | Pascal Siakam | 2548 | +1.22 | +0.83 | +2.01 |
| 72 | Jimmy Butler | 1746 | +2.71 | +0.80 | +3.82 |
| 73 | Caleb Martin | 1218 | -2.40 | +0.80 | -1.73 |
| 74 | Derrick Jones Jr. | 1873 | +0.27 | +0.79 | +1.69 |
| 75 | Jalen Williams | 2237 | +1.77 | +0.79 | +2.37 |
| 76 | Nickeil Alexander-Walker | 2073 | -0.01 | +0.77 | +0.56 |
| 77 | Miles McBride | 1593 | +0.47 | +0.76 | +1.16 |
| 78 | Ben Simmons | 1120 | -1.18 | +0.72 | -1.47 |
| 79 | Luka Dončić | 1769 | +5.79 | +0.69 | +6.78 |
| 80 | Bobby Portis | 1244 | +0.00 | +0.69 | +0.85 |
| 81 | Jake LaRavia | 1349 | -0.20 | +0.67 | +0.91 |
| 82 | Aaron Nesmith | 1123 | +1.16 | +0.65 | +1.47 |
| 83 | Bam Adebayo | 2674 | -0.36 | +0.62 | +0.65 |
| 84 | OG Anunoby | 2706 | +0.40 | +0.59 | +1.43 |
| 85 | John Collins | 1220 | +0.60 | +0.57 | +1.13 |
| 86 | Domantas Sabonis | 2429 | +1.57 | +0.56 | +2.78 |
| 87 | Andrew Nembhard | 1881 | +0.34 | +0.56 | +1.50 |
| 88 | Deandre Ayton | 1206 | -1.12 | +0.51 | -0.45 |
| 89 | Precious Achiuwa | 1170 | -2.21 | +0.50 | -1.77 |
| 90 | Moses Moody | 1649 | +0.31 | +0.46 | +1.05 |
| 91 | Isaiah Joe | 1604 | +1.91 | +0.45 | +2.28 |
| 92 | Franz Wagner | 2023 | +2.58 | +0.44 | +2.77 |
| 93 | Anthony Black | 1887 | -1.75 | +0.43 | -0.78 |
| 94 | Gabe Vincent | 1525 | -1.13 | +0.43 | -0.27 |
| 95 | Onyeka Okongwu | 2064 | +0.54 | +0.40 | +1.01 |
| 96 | Josh Green | 1887 | -2.18 | +0.35 | -2.05 |
| 97 | Cameron Payne | 1090 | +2.27 | +0.32 | +2.29 |
| 98 | Rui Hachimura | 1869 | -0.49 | +0.32 | +0.06 |
| 99 | Nic Claxton | 1880 | -2.18 | +0.31 | -1.96 |
| 100 | Karl-Anthony Towns | 2517 | +2.41 | +0.31 | +3.52 |

### 2024-25 Playoffs — top 100 offense

> pool 219 players, 109 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Giannis Antetokounmpo | 188 | +6.61 | -0.78 | +5.09 |
| 2 | Jalen Brunson | 680 | +6.04 | -2.97 | +2.72 |
| 3 | LeBron James | 204 | +5.86 | +0.83 | +4.55 |
| 4 | Luka Dončić | 208 | +5.86 | -1.89 | +4.49 |
| 5 | Stephen Curry | 281 | +5.58 | -1.55 | +4.75 |
| 6 | Shai Gilgeous-Alexander | 851 | +4.86 | -0.23 | +4.54 |
| 7 | Nikola Jokić | 563 | +4.81 | +3.32 | +7.32 |
| 8 | Donovan Mitchell | 288 | +4.79 | -1.30 | +5.49 |
| 9 | Fred VanVleet | 280 | +4.76 | -0.57 | +3.28 |
| 10 | Tyrese Haliburton | 772 | +4.70 | +0.05 | +4.78 |
| 11 | Jayson Tatum | 322 | +4.48 | +2.94 | +7.42 |
| 12 | Kawhi Leonard | 265 | +4.14 | -1.79 | +2.74 |
| 13 | Anthony Edwards | 585 | +3.57 | +0.57 | +4.31 |
| 14 | Max Strus | 253 | +3.54 | +0.20 | +3.55 |
| 15 | Paolo Banchero | 197 | +3.49 | -0.47 | +2.09 |
| 16 | Payton Pritchard | 302 | +3.43 | -2.12 | +1.56 |
| 17 | Isaiah Joe | 211 | +3.17 | +0.02 | +2.59 |
| 18 | Julius Randle | 533 | +2.94 | -0.62 | +2.90 |
| 19 | Gary Trent Jr. | 171 | +2.91 | +1.62 | +3.12 |
| 20 | Jamal Murray | 578 | +2.90 | +0.28 | +3.49 |
| 21 | James Harden | 276 | +2.63 | -3.20 | -0.58 |
| 22 | Jimmy Butler III | 397 | +2.60 | -1.31 | +1.44 |
| 23 | Dennis Schröder | 164 | +2.56 | -0.46 | +1.93 |
| 24 | Darius Garland | 148 | +2.54 | -2.38 | -1.07 |
| 25 | Davion Mitchell | 142 | +2.54 | -5.01 | -1.07 |
| 26 | Jalen Williams | 796 | +2.50 | -1.08 | +1.75 |
| 27 | Aaron Nesmith | 650 | +2.47 | +1.68 | +3.60 |
| 28 | Alperen Sengun | 256 | +2.44 | +4.08 | +6.57 |
| 29 | Amen Thompson | 231 | +2.42 | +0.39 | +2.99 |
| 30 | Ty Jerome | 191 | +2.37 | -1.24 | +1.75 |
| 31 | Buddy Hield | 327 | +2.36 | +0.83 | +2.02 |
| 32 | AJ Green | 135 | +2.33 | -1.69 | +1.75 |
| 33 | Franz Wagner | 195 | +2.27 | +0.96 | +1.97 |
| 34 | Aaron Gordon | 522 | +2.19 | -0.84 | +2.06 |
| 35 | Derrick White | 415 | +2.10 | +0.01 | +1.87 |
| 36 | Pascal Siakam | 771 | +2.10 | +1.84 | +3.47 |
| 37 | Jaden McDaniels | 497 | +1.96 | +2.55 | +4.16 |
| 38 | Cade Cunningham | 248 | +1.91 | +1.79 | +4.55 |
| 39 | Sam Merrill | 159 | +1.77 | -0.36 | +1.80 |
| 40 | Andrew Nembhard | 769 | +1.69 | +0.85 | +2.99 |
| 41 | Evan Mobley | 257 | +1.61 | +0.26 | +2.21 |
| 42 | Steven Adams | 155 | +1.59 | +3.25 | +4.68 |
| 43 | Luke Kornet | 180 | +1.50 | +2.22 | +3.59 |
| 44 | Ivica Zubac | 256 | +1.49 | +0.51 | +2.06 |
| 45 | Jarrett Allen | 261 | +1.46 | +2.49 | +4.48 |
| 46 | T.J. McConnell | 402 | +1.43 | -1.11 | +0.04 |
| 47 | Jalen Duren | 203 | +1.24 | -0.65 | +0.67 |
| 48 | Dillon Brooks | 206 | +1.05 | -1.15 | +0.11 |
| 49 | Alex Caruso | 562 | +1.02 | +2.95 | +4.48 |
| 50 | De'Andre Hunter | 185 | +1.01 | -0.09 | +1.11 |
| 51 | Isaiah Hartenstein | 516 | +0.98 | +1.32 | +2.92 |
| 52 | Jaylen Brown | 402 | +0.74 | +0.46 | +1.99 |
| 53 | Tobias Harris | 233 | +0.69 | +0.69 | +0.83 |
| 54 | Mike Conley | 356 | +0.54 | +1.19 | +1.94 |
| 55 | Brandin Podziemski | 385 | +0.50 | +2.04 | +2.35 |
| 56 | Karl-Anthony Towns | 639 | +0.48 | +1.13 | +2.73 |
| 57 | Miles McBride | 341 | +0.47 | -0.54 | -0.29 |
| 58 | Jalen Green | 219 | +0.46 | -0.52 | +0.74 |
| 59 | Nicolas Batum | 172 | +0.45 | +0.84 | +1.79 |
| 60 | Bobby Portis | 158 | +0.43 | +1.00 | +0.74 |
| 61 | Rudy Gobert | 411 | +0.41 | +4.49 | +3.84 |
| 62 | Chet Holmgren | 686 | +0.36 | +2.26 | +2.09 |
| 63 | Bam Adebayo | 153 | +0.34 | -0.16 | +1.06 |
| 64 | Bennedict Mathurin | 385 | +0.34 | -1.77 | -1.15 |
| 65 | Naz Reid | 375 | +0.29 | +0.03 | +0.89 |
| 66 | Aaron Wiggins | 303 | +0.22 | +0.53 | +1.00 |
| 67 | Al Horford | 348 | +0.20 | +0.70 | +0.34 |
| 68 | Myles Turner | 675 | +0.17 | +0.06 | +1.24 |
| 69 | Jrue Holiday | 264 | +0.05 | +1.43 | +1.19 |
| 70 | Dorian Finney-Smith | 170 | +0.02 | -0.30 | -0.33 |
| 71 | Christian Braun | 544 | -0.02 | +1.48 | +1.28 |
| 72 | Jonathan Kuminga | 187 | -0.08 | -3.30 | -3.38 |
| 73 | Cason Wallace | 516 | -0.15 | +2.29 | +2.40 |
| 74 | Ben Sheppard | 293 | -0.16 | -1.88 | -2.10 |
| 75 | Josh Hart | 642 | -0.17 | +0.72 | +1.19 |
| 76 | Mitchell Robinson | 370 | -0.19 | +2.30 | +2.59 |
| 77 | Tyler Herro | 144 | -0.25 | -4.21 | -4.58 |
| 78 | Tim Hardaway Jr. | 188 | -0.29 | -2.32 | -1.43 |
| 79 | Obi Toppin | 440 | -0.37 | -1.40 | -1.15 |
| 80 | Kris Dunn | 153 | -0.40 | -1.33 | -0.04 |
| 81 | Ausar Thompson | 135 | -0.58 | +2.29 | +3.00 |
| 82 | Kevin Porter Jr. | 151 | -0.60 | -1.43 | -0.47 |
| 83 | Moses Moody | 193 | -0.70 | -0.76 | -2.30 |
| 84 | Malik Beasley | 163 | -0.71 | -1.88 | -2.75 |
| 85 | Donte DiVincenzo | 377 | -0.74 | -0.67 | -0.83 |
| 86 | Luguentz Dort | 666 | -0.80 | +1.97 | +0.95 |
| 87 | Nickeil Alexander-Walker | 310 | -0.93 | -0.75 | -1.88 |
| 88 | Norman Powell | 238 | -0.97 | -2.19 | -1.72 |
| 89 | OG Anunoby | 705 | -1.16 | +1.35 | -0.12 |
| 90 | Russell Westbrook | 313 | -1.17 | -2.07 | -2.46 |
| 91 | Mikal Bridges | 706 | -1.25 | -0.64 | -2.08 |
| 92 | Michael Porter Jr. | 435 | -1.28 | -1.75 | -2.83 |
| 93 | Jabari Smith Jr. | 143 | -1.32 | +0.43 | -1.38 |
| 94 | Gary Payton II | 180 | -1.38 | -0.04 | -1.32 |
| 95 | Austin Reaves | 196 | -1.44 | -1.28 | -2.49 |
| 96 | Jaren Jackson Jr. | 137 | -1.50 | -3.32 | -3.74 |
| 97 | Rui Hachimura | 182 | -1.52 | -2.99 | -3.80 |
| 98 | Kristaps Porziņģis | 231 | -1.54 | +0.94 | +0.50 |
| 99 | Desmond Bane | 139 | -1.63 | -2.69 | -4.00 |
| 100 | Quinten Post | 146 | -1.84 | +0.31 | -0.90 |

### 2024-25 Playoffs — top 100 defense

> pool 219 players, 109 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Rudy Gobert | 411 | +0.41 | +4.49 | +3.84 |
| 2 | Alperen Sengun | 256 | +2.44 | +4.08 | +6.57 |
| 3 | Nikola Jokić | 563 | +4.81 | +3.32 | +7.32 |
| 4 | Steven Adams | 155 | +1.59 | +3.25 | +4.68 |
| 5 | Kenrich Williams | 137 | -2.01 | +3.06 | +2.63 |
| 6 | Alex Caruso | 562 | +1.02 | +2.95 | +4.48 |
| 7 | Jayson Tatum | 322 | +4.48 | +2.94 | +7.42 |
| 8 | Jaylin Williams | 141 | -3.09 | +2.91 | +0.31 |
| 9 | Draymond Green | 389 | -2.92 | +2.78 | +0.94 |
| 10 | Jaden McDaniels | 497 | +1.96 | +2.55 | +4.16 |
| 11 | Jarrett Allen | 261 | +1.46 | +2.49 | +4.48 |
| 12 | Mitchell Robinson | 370 | -0.19 | +2.30 | +2.59 |
| 13 | Ausar Thompson | 135 | -0.58 | +2.29 | +3.00 |
| 14 | Cason Wallace | 516 | -0.15 | +2.29 | +2.40 |
| 15 | Chet Holmgren | 686 | +0.36 | +2.26 | +2.09 |
| 16 | Luke Kornet | 180 | +1.50 | +2.22 | +3.59 |
| 17 | Brandin Podziemski | 385 | +0.50 | +2.04 | +2.35 |
| 18 | Luguentz Dort | 666 | -0.80 | +1.97 | +0.95 |
| 19 | Pascal Siakam | 771 | +2.10 | +1.84 | +3.47 |
| 20 | Cade Cunningham | 248 | +1.91 | +1.79 | +4.55 |
| 21 | Kentavious Caldwell-Pope | 163 | -2.06 | +1.71 | -0.54 |
| 22 | Aaron Nesmith | 650 | +2.47 | +1.68 | +3.60 |
| 23 | Gary Trent Jr. | 171 | +2.91 | +1.62 | +3.12 |
| 24 | Christian Braun | 544 | -0.02 | +1.48 | +1.28 |
| 25 | Jrue Holiday | 264 | +0.05 | +1.43 | +1.19 |
| 26 | Tari Eason | 132 | -2.03 | +1.43 | +0.34 |
| 27 | OG Anunoby | 705 | -1.16 | +1.35 | -0.12 |
| 28 | Isaiah Hartenstein | 516 | +0.98 | +1.32 | +2.92 |
| 29 | Mike Conley | 356 | +0.54 | +1.19 | +1.94 |
| 30 | Karl-Anthony Towns | 639 | +0.48 | +1.13 | +2.73 |
| 31 | Bobby Portis | 158 | +0.43 | +1.00 | +0.74 |
| 32 | Franz Wagner | 195 | +2.27 | +0.96 | +1.97 |
| 33 | Kristaps Porziņģis | 231 | -1.54 | +0.94 | +0.50 |
| 34 | Andrew Nembhard | 769 | +1.69 | +0.85 | +2.99 |
| 35 | Nicolas Batum | 172 | +0.45 | +0.84 | +1.79 |
| 36 | LeBron James | 204 | +5.86 | +0.83 | +4.55 |
| 37 | Buddy Hield | 327 | +2.36 | +0.83 | +2.02 |
| 38 | Peyton Watson | 199 | -3.02 | +0.77 | -2.94 |
| 39 | Josh Hart | 642 | -0.17 | +0.72 | +1.19 |
| 40 | Al Horford | 348 | +0.20 | +0.70 | +0.34 |
| 41 | Tobias Harris | 233 | +0.69 | +0.69 | +0.83 |
| 42 | Anthony Edwards | 585 | +3.57 | +0.57 | +4.31 |
| 43 | Aaron Wiggins | 303 | +0.22 | +0.53 | +1.00 |
| 44 | Ivica Zubac | 256 | +1.49 | +0.51 | +2.06 |
| 45 | Jaylen Brown | 402 | +0.74 | +0.46 | +1.99 |
| 46 | Dean Wade | 142 | -1.87 | +0.46 | -1.06 |
| 47 | Jabari Smith Jr. | 143 | -1.32 | +0.43 | -1.38 |
| 48 | Amen Thompson | 231 | +2.42 | +0.39 | +2.99 |
| 49 | Quinten Post | 146 | -1.84 | +0.31 | -0.90 |
| 50 | Jamal Murray | 578 | +2.90 | +0.28 | +3.49 |
| 51 | Evan Mobley | 257 | +1.61 | +0.26 | +2.21 |
| 52 | Max Strus | 253 | +3.54 | +0.20 | +3.55 |
| 53 | Myles Turner | 675 | +0.17 | +0.06 | +1.24 |
| 54 | Tyrese Haliburton | 772 | +4.70 | +0.05 | +4.78 |
| 55 | Naz Reid | 375 | +0.29 | +0.03 | +0.89 |
| 56 | Isaiah Joe | 211 | +3.17 | +0.02 | +2.59 |
| 57 | Derrick White | 415 | +2.10 | +0.01 | +1.87 |
| 58 | Gary Payton II | 180 | -1.38 | -0.04 | -1.32 |
| 59 | De'Andre Hunter | 185 | +1.01 | -0.09 | +1.11 |
| 60 | Thomas Bryant | 167 | -2.96 | -0.14 | -3.96 |
| 61 | Bam Adebayo | 153 | +0.34 | -0.16 | +1.06 |
| 62 | Shai Gilgeous-Alexander | 851 | +4.86 | -0.23 | +4.54 |
| 63 | Dorian Finney-Smith | 170 | +0.02 | -0.30 | -0.33 |
| 64 | Sam Merrill | 159 | +1.77 | -0.36 | +1.80 |
| 65 | Dennis Schröder | 164 | +2.56 | -0.46 | +1.93 |
| 66 | Paolo Banchero | 197 | +3.49 | -0.47 | +2.09 |
| 67 | Jalen Green | 219 | +0.46 | -0.52 | +0.74 |
| 68 | Miles McBride | 341 | +0.47 | -0.54 | -0.29 |
| 69 | Fred VanVleet | 280 | +4.76 | -0.57 | +3.28 |
| 70 | Julius Randle | 533 | +2.94 | -0.62 | +2.90 |
| 71 | Mikal Bridges | 706 | -1.25 | -0.64 | -2.08 |
| 72 | Jalen Duren | 203 | +1.24 | -0.65 | +0.67 |
| 73 | Donte DiVincenzo | 377 | -0.74 | -0.67 | -0.83 |
| 74 | Nickeil Alexander-Walker | 310 | -0.93 | -0.75 | -1.88 |
| 75 | Moses Moody | 193 | -0.70 | -0.76 | -2.30 |
| 76 | Giannis Antetokounmpo | 188 | +6.61 | -0.78 | +5.09 |
| 77 | Aaron Gordon | 522 | +2.19 | -0.84 | +2.06 |
| 78 | Jalen Williams | 796 | +2.50 | -1.08 | +1.75 |
| 79 | T.J. McConnell | 402 | +1.43 | -1.11 | +0.04 |
| 80 | Dillon Brooks | 206 | +1.05 | -1.15 | +0.11 |
| 81 | Wendell Carter Jr. | 162 | -1.86 | -1.17 | -2.32 |
| 82 | Ty Jerome | 191 | +2.37 | -1.24 | +1.75 |
| 83 | Austin Reaves | 196 | -1.44 | -1.28 | -2.49 |
| 84 | Donovan Mitchell | 288 | +4.79 | -1.30 | +5.49 |
| 85 | Jimmy Butler III | 397 | +2.60 | -1.31 | +1.44 |
| 86 | Kris Dunn | 153 | -0.40 | -1.33 | -0.04 |
| 87 | Obi Toppin | 440 | -0.37 | -1.40 | -1.15 |
| 88 | Kevin Porter Jr. | 151 | -0.60 | -1.43 | -0.47 |
| 89 | Stephen Curry | 281 | +5.58 | -1.55 | +4.75 |
| 90 | AJ Green | 135 | +2.33 | -1.69 | +1.75 |
| 91 | Michael Porter Jr. | 435 | -1.28 | -1.75 | -2.83 |
| 92 | Bennedict Mathurin | 385 | +0.34 | -1.77 | -1.15 |
| 93 | Kawhi Leonard | 265 | +4.14 | -1.79 | +2.74 |
| 94 | Malik Beasley | 163 | -0.71 | -1.88 | -2.75 |
| 95 | Ben Sheppard | 293 | -0.16 | -1.88 | -2.10 |
| 96 | Luka Dončić | 208 | +5.86 | -1.89 | +4.49 |
| 97 | Russell Westbrook | 313 | -1.17 | -2.07 | -2.46 |
| 98 | Payton Pritchard | 302 | +3.43 | -2.12 | +1.56 |
| 99 | Norman Powell | 238 | -0.97 | -2.19 | -1.72 |
| 100 | Tim Hardaway Jr. | 188 | -0.29 | -2.32 | -1.43 |

## 2025-26

### 2025-26 Regular season — top 100 offense

> pool 582 players, 269 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Nikola Jokić | 2265 | +7.57 | +1.20 | +8.38 |
| 2 | Shai Gilgeous-Alexander | 2259 | +6.51 | +0.48 | +6.16 |
| 3 | James Harden | 2438 | +6.21 | -2.60 | +3.08 |
| 4 | Luka Dončić | 2289 | +6.16 | -0.34 | +7.15 |
| 5 | Kawhi Leonard | 2085 | +5.81 | +0.93 | +7.61 |
| 6 | Jamal Murray | 2652 | +5.80 | -1.01 | +4.46 |
| 7 | Stephen Curry | 1329 | +5.63 | -1.80 | +4.36 |
| 8 | Donovan Mitchell | 2342 | +5.51 | -1.28 | +4.96 |
| 9 | LaMelo Ball | 2017 | +4.87 | -1.06 | +4.24 |
| 10 | Jalen Brunson | 2590 | +4.68 | -1.73 | +3.21 |
| 11 | Cade Cunningham | 2172 | +4.32 | -0.51 | +3.34 |
| 12 | Tyrese Maxey | 2661 | +4.27 | +0.07 | +3.82 |
| 13 | Austin Reaves | 1762 | +3.88 | -0.30 | +3.56 |
| 14 | Coby White | 1250 | +3.81 | -2.07 | +1.64 |
| 15 | Kevin Durant | 2840 | +3.74 | -1.28 | +2.55 |
| 16 | Jimmy Butler III | 1182 | +3.44 | +1.04 | +4.80 |
| 17 | Joel Embiid | 1201 | +3.43 | +0.13 | +3.51 |
| 18 | Deni Avdija | 2199 | +3.38 | -1.71 | +2.19 |
| 19 | Devin Booker | 2146 | +3.37 | -1.32 | +2.22 |
| 20 | Anthony Edwards | 2137 | +3.06 | -2.37 | +1.93 |
| 21 | Jrue Holiday | 1560 | +3.02 | -0.18 | +3.10 |
| 22 | LeBron James | 1989 | +3.00 | -0.04 | +2.49 |
| 23 | Jaylen Brown | 2443 | +2.81 | -1.21 | +2.36 |
| 24 | Michael Porter Jr. | 1689 | +2.79 | -1.70 | +1.11 |
| 25 | Payton Pritchard | 2556 | +2.75 | -1.74 | +1.92 |
| 26 | Keyonte George | 1786 | +2.74 | -2.69 | -0.57 |
| 27 | Julius Randle | 2610 | +2.72 | -1.32 | +1.18 |
| 28 | De'Aaron Fox | 2231 | +2.69 | -0.04 | +2.14 |
| 29 | Lauri Markkanen | 1443 | +2.57 | -0.35 | +1.70 |
| 30 | Collin Gillespie | 2282 | +2.50 | +1.16 | +3.70 |
| 31 | Darius Garland | 1344 | +2.50 | -2.34 | -0.07 |
| 32 | Ryan Rollins | 2375 | +2.47 | -0.37 | +1.00 |
| 33 | Alperen Sengun | 2398 | +2.42 | +0.37 | +2.73 |
| 34 | Victor Wembanyama | 1866 | +2.36 | +4.33 | +7.60 |
| 35 | Bones Hyland | 1177 | +2.34 | -0.42 | +1.68 |
| 36 | CJ McCollum | 2263 | +2.33 | -1.37 | +1.02 |
| 37 | Duncan Robinson | 2113 | +2.27 | -0.72 | +1.57 |
| 38 | Reed Sheppard | 2147 | +2.03 | +0.19 | +2.95 |
| 39 | Jalen Duren | 1976 | +2.03 | +1.07 | +3.20 |
| 40 | Cam Spencer | 1714 | +1.97 | -1.91 | +0.31 |
| 41 | Jalen Johnson | 2532 | +1.96 | +0.21 | +2.13 |
| 42 | Kon Knueppel | 2551 | +1.93 | -0.94 | +1.47 |
| 43 | Immanuel Quickley | 2231 | +1.86 | -0.46 | +2.01 |
| 44 | Anfernee Simons | 1372 | +1.83 | -2.58 | -0.66 |
| 45 | Derrick White | 2625 | +1.81 | +2.31 | +4.24 |
| 46 | Paul George | 1135 | +1.72 | +1.46 | +3.23 |
| 47 | Nickeil Alexander-Walker | 2603 | +1.69 | +1.18 | +2.48 |
| 48 | Miles McBride | 1080 | +1.68 | -0.45 | +0.90 |
| 49 | Isaiah Joe | 1507 | +1.62 | +0.71 | +2.30 |
| 50 | Ajay Mitchell | 1473 | +1.62 | +1.91 | +2.86 |
| 51 | Josh Giddey | 1731 | +1.56 | -1.41 | -0.56 |
| 52 | Kevin Porter Jr. | 1261 | +1.56 | +0.17 | +1.19 |
| 53 | Trey Murphy III | 2341 | +1.56 | -0.63 | +1.02 |
| 54 | Pascal Siakam | 2057 | +1.55 | -1.06 | +0.19 |
| 55 | Grayson Allen | 1467 | +1.53 | -0.48 | +1.60 |
| 56 | Luka Garza | 1118 | +1.49 | +0.03 | +1.72 |
| 57 | Dylan Harper | 1558 | +1.46 | -0.17 | +1.05 |
| 58 | Zion Williamson | 1841 | +1.44 | -0.77 | +0.90 |
| 59 | Scottie Barnes | 2681 | +1.43 | +1.32 | +2.59 |
| 60 | Norman Powell | 1717 | +1.42 | -1.42 | +1.16 |
| 61 | Tim Hardaway Jr. | 2127 | +1.42 | -0.72 | +1.11 |
| 62 | DeMar DeRozan | 2406 | +1.41 | -2.10 | -1.43 |
| 63 | Karl-Anthony Towns | 2322 | +1.41 | +1.23 | +3.73 |
| 64 | Ayo Dosunmu | 1881 | +1.39 | -1.41 | -0.29 |
| 65 | Devin Vassell | 2044 | +1.36 | +0.09 | +1.22 |
| 66 | Desmond Bane | 2756 | +1.32 | -0.66 | +1.11 |
| 67 | Brandon Miller | 1968 | +1.31 | +0.34 | +2.33 |
| 68 | Sam Merrill | 1377 | +1.30 | -0.66 | +0.98 |
| 69 | Donte DiVincenzo | 2494 | +1.29 | +1.65 | +2.91 |
| 70 | Jordan Miller | 1325 | +1.23 | -1.06 | -0.15 |
| 71 | Julian Champagnie | 2266 | +1.23 | +0.59 | +1.78 |
| 72 | AJ Green | 2270 | +1.22 | -1.28 | -0.27 |
| 73 | Tre Jones | 1752 | +1.21 | -0.31 | +0.61 |
| 74 | Amen Thompson | 2953 | +1.19 | +0.86 | +2.32 |
| 75 | Collin Sexton | 1613 | +1.16 | -0.43 | +0.29 |
| 76 | Brandon Williams | 1463 | +1.15 | -1.13 | -0.30 |
| 77 | Sam Hauser | 1934 | +1.14 | +0.27 | +1.98 |
| 78 | Dyson Daniels | 2520 | +1.10 | +2.28 | +3.11 |
| 79 | Keldon Johnson | 1911 | +1.09 | -0.16 | +1.01 |
| 80 | Jerami Grant | 1695 | +1.07 | -1.90 | -0.44 |
| 81 | RJ Barrett | 1726 | +1.04 | -0.68 | +0.89 |
| 82 | Brandin Podziemski | 2333 | +0.99 | +0.52 | +1.72 |
| 83 | Jaime Jaquez Jr. | 2121 | +0.98 | -0.72 | +0.25 |
| 84 | Josh Hart | 1994 | +0.96 | +0.56 | +1.54 |
| 85 | Paolo Banchero | 2502 | +0.94 | -0.93 | -0.57 |
| 86 | Mikal Bridges | 2692 | +0.92 | +0.35 | +2.07 |
| 87 | Stephon Castle | 2038 | +0.91 | +0.21 | +1.64 |
| 88 | VJ Edgecombe | 2623 | +0.91 | -0.21 | +0.73 |
| 89 | Jalen Suggs | 1574 | +0.86 | +1.59 | +2.85 |
| 90 | Chet Holmgren | 1997 | +0.84 | +3.80 | +4.52 |
| 91 | Simone Fontecchio | 1172 | +0.83 | -1.09 | -0.43 |
| 92 | Donovan Clingan | 2094 | +0.82 | +1.70 | +2.83 |
| 93 | Luke Kennard | 1680 | +0.81 | -1.30 | -0.03 |
| 94 | Cooper Flagg | 2344 | +0.78 | -1.06 | -1.43 |
| 95 | Andrew Nembhard | 1786 | +0.76 | -2.64 | -2.63 |
| 96 | OG Anunoby | 2224 | +0.74 | +1.45 | +2.12 |
| 97 | Bobby Portis | 1619 | +0.74 | -1.31 | -0.87 |
| 98 | Nikola Vučević | 1818 | +0.73 | +0.41 | +0.76 |
| 99 | Mitchell Robinson | 1175 | +0.72 | +1.86 | +2.69 |
| 100 | Cameron Johnson | 1647 | +0.71 | -0.08 | +0.84 |

### 2025-26 Regular season — top 100 defense

> pool 582 players, 269 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Victor Wembanyama | 1866 | +2.36 | +4.33 | +7.60 |
| 2 | Neemias Queta | 1926 | -0.18 | +3.84 | +3.59 |
| 3 | Chet Holmgren | 1997 | +0.84 | +3.80 | +4.52 |
| 4 | Isaiah Hartenstein | 1137 | -0.21 | +3.38 | +2.68 |
| 5 | Rudy Gobert | 2380 | -0.46 | +3.15 | +2.88 |
| 6 | Cason Wallace | 2046 | -0.14 | +2.63 | +2.12 |
| 7 | Hugo González | 1084 | -1.03 | +2.36 | +1.23 |
| 8 | Derrick White | 2625 | +1.81 | +2.31 | +4.24 |
| 9 | Ausar Thompson | 1896 | -0.31 | +2.28 | +2.32 |
| 10 | Dyson Daniels | 2520 | +1.10 | +2.28 | +3.11 |
| 11 | Ronald Holland II | 1550 | -1.91 | +2.22 | +1.22 |
| 12 | Javonte Green | 1446 | +0.06 | +2.02 | +1.98 |
| 13 | Baylor Scheierman | 1429 | +0.13 | +2.00 | +2.09 |
| 14 | Jarrett Allen | 1519 | +0.67 | +1.97 | +2.46 |
| 15 | Marcus Smart | 1769 | -0.26 | +1.97 | +2.31 |
| 16 | Jaylin Williams | 1277 | -0.29 | +1.92 | +1.17 |
| 17 | Ajay Mitchell | 1473 | +1.62 | +1.91 | +2.86 |
| 18 | Keon Ellis | 1479 | -1.19 | +1.89 | +0.27 |
| 19 | Evan Mobley | 2074 | +0.04 | +1.88 | +1.96 |
| 20 | Toumani Camara | 2731 | +0.67 | +1.86 | +2.57 |
| 21 | Mitchell Robinson | 1175 | +0.72 | +1.86 | +2.69 |
| 22 | Jamal Shead | 1852 | +0.08 | +1.75 | +2.05 |
| 23 | Dru Smith | 1141 | -0.45 | +1.74 | +1.08 |
| 24 | Ryan Kalkbrenner | 1479 | -0.62 | +1.73 | +1.23 |
| 25 | John Konchar | 1115 | -0.47 | +1.73 | +1.36 |
| 26 | Donovan Clingan | 2094 | +0.82 | +1.70 | +2.83 |
| 27 | Jusuf Nurkić | 1083 | -0.73 | +1.69 | +0.13 |
| 28 | Jordan Goodwin | 1572 | +0.15 | +1.69 | +1.99 |
| 29 | Wendell Carter Jr. | 2288 | -1.06 | +1.69 | +1.05 |
| 30 | Naz Reid | 2007 | -0.02 | +1.67 | +1.96 |
| 31 | Donte DiVincenzo | 2494 | +1.29 | +1.65 | +2.91 |
| 32 | Josh Okogie | 1354 | -0.78 | +1.65 | +0.42 |
| 33 | Luke Kornet | 1430 | -0.41 | +1.64 | +1.41 |
| 34 | Sidy Cissoko | 1435 | -1.66 | +1.62 | -0.15 |
| 35 | Jalen Suggs | 1574 | +0.86 | +1.59 | +2.85 |
| 36 | Collin Murray-Boyles | 1246 | -1.80 | +1.49 | -0.35 |
| 37 | Paul George | 1135 | +1.72 | +1.46 | +3.23 |
| 38 | OG Anunoby | 2224 | +0.74 | +1.45 | +2.12 |
| 39 | Ryan Dunn | 1355 | -1.71 | +1.44 | -1.42 |
| 40 | Landry Shamet | 1171 | +0.09 | +1.38 | +0.90 |
| 41 | Luguentz Dort | 1849 | -1.14 | +1.37 | +0.06 |
| 42 | Scottie Barnes | 2681 | +1.43 | +1.32 | +2.59 |
| 43 | Bam Adebayo | 2365 | +0.62 | +1.32 | +2.08 |
| 44 | Mouhamed Gueye | 1179 | -1.01 | +1.24 | +0.35 |
| 45 | Myles Turner | 1912 | -0.88 | +1.23 | -0.31 |
| 46 | Karl-Anthony Towns | 2322 | +1.41 | +1.23 | +3.73 |
| 47 | Brook Lopez | 1635 | -1.17 | +1.20 | +0.03 |
| 48 | Nikola Jokić | 2265 | +7.57 | +1.20 | +8.38 |
| 49 | Nickeil Alexander-Walker | 2603 | +1.69 | +1.18 | +2.48 |
| 50 | Collin Gillespie | 2282 | +2.50 | +1.16 | +3.70 |
| 51 | Craig Porter Jr. | 1148 | +0.17 | +1.12 | +1.53 |
| 52 | Kris Murray | 1333 | -1.51 | +1.11 | -0.56 |
| 53 | Jakob Poeltl | 1149 | -0.73 | +1.09 | +0.75 |
| 54 | Jalen Duren | 1976 | +2.03 | +1.07 | +3.20 |
| 55 | Jalen Smith | 1095 | +0.24 | +1.05 | +1.58 |
| 56 | Oso Ighodaro | 1808 | -1.06 | +1.05 | +0.28 |
| 57 | Jimmy Butler III | 1182 | +3.44 | +1.04 | +4.80 |
| 58 | Day'Ron Sharpe | 1160 | +0.08 | +1.00 | +0.83 |
| 59 | Sion James | 1843 | -1.41 | +0.99 | -0.70 |
| 60 | De'Anthony Melton | 1125 | +0.14 | +0.99 | +1.31 |
| 61 | Kris Dunn | 2228 | -0.36 | +0.99 | +0.60 |
| 62 | Quinten Post | 1159 | -0.38 | +0.94 | +0.17 |
| 63 | Kawhi Leonard | 2085 | +5.81 | +0.93 | +7.61 |
| 64 | Dean Wade | 1318 | -0.53 | +0.90 | +0.24 |
| 65 | Moses Moody | 1540 | +0.70 | +0.89 | +1.16 |
| 66 | Spencer Jones | 1417 | -1.37 | +0.89 | -0.79 |
| 67 | Jordan Walsh | 1212 | -0.56 | +0.87 | +0.56 |
| 68 | Ivica Zubac | 1447 | +0.29 | +0.87 | +0.79 |
| 69 | Amen Thompson | 2953 | +1.19 | +0.86 | +2.32 |
| 70 | Jaxson Hayes | 1207 | -0.78 | +0.86 | +0.82 |
| 71 | Onyeka Okongwu | 2297 | -0.66 | +0.84 | +0.40 |
| 72 | Jaren Jackson Jr. | 1455 | -1.27 | +0.83 | -0.89 |
| 73 | Deandre Ayton | 1958 | -0.50 | +0.83 | -0.15 |
| 74 | Will Richard | 1377 | -1.09 | +0.80 | -0.15 |
| 75 | Moussa Diabaté | 1899 | +0.66 | +0.79 | +2.15 |
| 76 | Ja'Kobe Walter | 1474 | -0.51 | +0.78 | -0.31 |
| 77 | Derrick Jones Jr. | 1350 | -0.15 | +0.78 | +0.92 |
| 78 | Jarred Vanderbilt | 1128 | -1.68 | +0.78 | -1.49 |
| 79 | Jake LaRavia | 2061 | -0.40 | +0.76 | -0.39 |
| 80 | Herbert Jones | 1588 | -1.66 | +0.76 | -0.46 |
| 81 | Dominick Barlow | 1689 | -1.26 | +0.75 | -0.14 |
| 82 | Daniel Gafford | 1194 | -0.27 | +0.74 | +0.04 |
| 83 | Tari Eason | 1549 | -0.84 | +0.72 | +0.00 |
| 84 | Isaiah Joe | 1507 | +1.62 | +0.71 | +2.30 |
| 85 | Draymond Green | 1869 | -0.99 | +0.67 | -1.33 |
| 86 | Adem Bona | 1234 | -1.35 | +0.61 | -0.89 |
| 87 | Jay Huff | 1719 | -1.28 | +0.59 | -0.87 |
| 88 | Julian Champagnie | 2266 | +1.23 | +0.59 | +1.78 |
| 89 | Davion Mitchell | 2000 | +0.39 | +0.58 | +1.10 |
| 90 | Josh Hart | 1994 | +0.96 | +0.56 | +1.54 |
| 91 | Gui Santos | 1395 | -0.23 | +0.53 | +0.99 |
| 92 | Kel'el Ware | 1704 | -0.23 | +0.52 | +0.60 |
| 93 | Brandin Podziemski | 2333 | +0.99 | +0.52 | +1.72 |
| 94 | Pelle Larsson | 1849 | +0.37 | +0.52 | +1.01 |
| 95 | Ziaire Williams | 1281 | -1.49 | +0.48 | -0.74 |
| 96 | Shai Gilgeous-Alexander | 2259 | +6.51 | +0.48 | +6.16 |
| 97 | Andre Drummond | 1231 | -1.00 | +0.47 | -0.83 |
| 98 | Yves Missi | 1297 | -1.00 | +0.45 | -0.79 |
| 99 | Precious Achiuwa | 1745 | -0.60 | +0.45 | -0.42 |
| 100 | Alex Sarr | 1305 | -1.61 | +0.43 | -1.64 |

### 2025-26 Playoffs — top 100 offense

> pool 230 players, 112 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Jayson Tatum | 218 | +6.15 | +0.47 | +6.93 |
| 2 | Dillon Brooks | 149 | +5.11 | -2.20 | +2.47 |
| 3 | Jalen Brunson | 702 | +4.67 | -0.96 | +2.82 |
| 4 | Shai Gilgeous-Alexander | 544 | +4.48 | -1.92 | +2.36 |
| 5 | Ajay Mitchell | 317 | +3.86 | +3.16 | +6.03 |
| 6 | Karl-Anthony Towns | 578 | +3.63 | +4.22 | +7.26 |
| 7 | Tyrese Maxey | 437 | +3.54 | -2.52 | +0.85 |
| 8 | Joel Embiid | 233 | +3.44 | -1.98 | +0.69 |
| 9 | Scottie Barnes | 273 | +3.34 | -0.22 | +2.29 |
| 10 | Payton Pritchard | 231 | +3.22 | -2.22 | +1.50 |
| 11 | RJ Barrett | 271 | +3.21 | -0.91 | +2.29 |
| 12 | Jalen Green | 151 | +2.97 | -0.53 | +0.83 |
| 13 | Paolo Banchero | 273 | +2.75 | +1.18 | +3.37 |
| 14 | James Harden | 672 | +2.75 | -0.00 | +3.80 |
| 15 | Donovan Mitchell | 652 | +2.74 | -1.11 | +0.66 |
| 16 | Cade Cunningham | 572 | +2.71 | -1.05 | +1.85 |
| 17 | OG Anunoby | 586 | +2.64 | +1.67 | +4.75 |
| 18 | Alex Caruso | 353 | +2.54 | +3.45 | +6.49 |
| 19 | Mike Conley | 168 | +2.50 | +0.55 | +2.62 |
| 20 | Victor Wembanyama | 750 | +2.46 | +4.08 | +6.02 |
| 21 | Sam Merrill | 338 | +2.45 | -1.23 | +0.91 |
| 22 | Nikola Jokić | 237 | +2.41 | +0.75 | +2.92 |
| 23 | Paul George | 394 | +2.37 | +0.14 | +0.81 |
| 24 | Naz Reid | 323 | +2.36 | -0.71 | +0.86 |
| 25 | Jose Alvarado | 170 | +2.36 | +4.04 | +4.65 |
| 26 | Duncan Robinson | 383 | +2.23 | -0.66 | +1.93 |
| 27 | Chet Holmgren | 459 | +2.04 | +1.57 | +4.48 |
| 28 | Tim Hardaway Jr. | 140 | +1.90 | -1.53 | +1.19 |
| 29 | Jarrett Allen | 529 | +1.83 | +2.41 | +4.33 |
| 30 | Tari Eason | 195 | +1.73 | +3.39 | +4.11 |
| 31 | Julian Champagnie | 705 | +1.70 | +1.08 | +3.34 |
| 32 | Austin Reaves | 221 | +1.66 | -4.15 | -3.05 |
| 33 | Jrue Holiday | 192 | +1.63 | -2.05 | -0.18 |
| 34 | LeBron James | 384 | +1.56 | -1.14 | +1.16 |
| 35 | Dylan Harper | 615 | +1.54 | +1.22 | +2.56 |
| 36 | Dean Wade | 407 | +1.45 | +1.29 | +2.30 |
| 37 | Mikal Bridges | 608 | +1.45 | +1.73 | +3.39 |
| 38 | Caris LeVert | 216 | +1.44 | -0.90 | +0.13 |
| 39 | Amen Thompson | 264 | +1.39 | +3.55 | +4.98 |
| 40 | Jared McCain | 258 | +1.37 | -1.83 | -0.29 |
| 41 | Miles McBride | 334 | +1.32 | +0.13 | +0.71 |
| 42 | De'Aaron Fox | 704 | +1.30 | +1.40 | +2.40 |
| 43 | Ayo Dosunmu | 292 | +1.25 | -2.09 | -0.04 |
| 44 | Collin Murray-Boyles | 191 | +1.23 | +1.15 | +2.22 |
| 45 | Landry Shamet | 310 | +1.18 | -0.87 | -0.02 |
| 46 | Isaiah Joe | 143 | +1.01 | -0.42 | -0.40 |
| 47 | Alperen Sengun | 232 | +0.90 | +3.32 | +5.00 |
| 48 | Stephon Castle | 760 | +0.90 | -0.91 | +0.41 |
| 49 | Devin Vassell | 801 | +0.89 | +1.42 | +3.29 |
| 50 | Devin Booker | 153 | +0.88 | -3.86 | -1.51 |
| 51 | Tobias Harris | 485 | +0.86 | -0.79 | -0.17 |
| 52 | Desmond Bane | 253 | +0.84 | -0.66 | -0.04 |
| 53 | Andre Drummond | 142 | +0.79 | -0.28 | +0.13 |
| 54 | Cason Wallace | 374 | +0.76 | +3.00 | +4.23 |
| 55 | Cameron Johnson | 186 | +0.76 | -0.05 | +1.44 |
| 56 | Isaiah Hartenstein | 350 | +0.72 | +1.35 | +2.90 |
| 57 | Luke Kennard | 326 | +0.72 | -0.07 | +0.82 |
| 58 | Rui Hachimura | 386 | +0.65 | -1.54 | -0.93 |
| 59 | Jamal Murray | 238 | +0.57 | -1.80 | -2.11 |
| 60 | VJ Edgecombe | 407 | +0.48 | +0.82 | +1.68 |
| 61 | Jaylen Brown | 249 | +0.46 | +1.08 | +0.29 |
| 62 | Terrence Shannon Jr. | 203 | +0.46 | -1.22 | -0.93 |
| 63 | Scoot Henderson | 145 | +0.46 | -1.28 | -0.30 |
| 64 | Marcus Smart | 345 | +0.41 | +1.05 | +2.46 |
| 65 | Jordan Clarkson | 195 | +0.38 | -0.61 | +0.95 |
| 66 | Deni Avdija | 174 | +0.37 | -3.05 | -1.41 |
| 67 | Max Strus | 481 | +0.35 | -1.19 | -0.64 |
| 68 | Wendell Carter Jr. | 237 | +0.32 | -0.25 | +0.29 |
| 69 | Onyeka Okongwu | 199 | +0.30 | -0.59 | -0.32 |
| 70 | CJ McCollum | 192 | +0.29 | -2.47 | -2.18 |
| 71 | Sam Hauser | 164 | +0.24 | -0.51 | -0.60 |
| 72 | Mitchell Robinson | 251 | +0.19 | +1.57 | +2.37 |
| 73 | Jabari Smith Jr. | 252 | +0.09 | +1.30 | +1.26 |
| 74 | Jalen Johnson | 214 | +0.07 | -1.49 | -1.13 |
| 75 | Kelly Oubre Jr. | 364 | +0.06 | -0.74 | -0.70 |
| 76 | Daniss Jenkins | 318 | +0.03 | -1.88 | -1.96 |
| 77 | Jamal Shead | 224 | -0.03 | +1.57 | +1.56 |
| 78 | Quentin Grimes | 243 | -0.20 | -3.48 | -3.74 |
| 79 | Jaylin Williams | 240 | -0.28 | +2.31 | +2.21 |
| 80 | Josh Hart | 614 | -0.40 | +2.70 | +3.26 |
| 81 | Spencer Jones | 145 | -0.43 | +0.60 | -0.21 |
| 82 | Anthony Edwards | 324 | -0.57 | -2.08 | -3.11 |
| 83 | Evan Mobley | 640 | -0.60 | +1.02 | +0.32 |
| 84 | Ausar Thompson | 427 | -0.64 | +3.25 | +2.64 |
| 85 | Jaden McDaniels | 406 | -0.81 | +0.54 | -1.03 |
| 86 | Deandre Ayton | 285 | -0.86 | +0.50 | -0.30 |
| 87 | Harrison Barnes | 183 | -0.86 | -1.20 | -0.60 |
| 88 | Luke Kornet | 296 | -0.91 | +1.15 | +0.47 |
| 89 | Jakob Poeltl | 134 | -1.00 | +2.57 | +1.33 |
| 90 | Jonathan Kuminga | 156 | -1.13 | -1.93 | -3.06 |
| 91 | Reed Sheppard | 192 | -1.15 | +0.66 | -0.05 |
| 92 | Luguentz Dort | 335 | -1.18 | -1.46 | -2.15 |
| 93 | Jaxson Hayes | 163 | -1.18 | +0.32 | -0.64 |
| 94 | Jaylon Tyson | 216 | -1.24 | -1.18 | -2.58 |
| 95 | Keldon Johnson | 411 | -1.26 | -0.88 | -1.00 |
| 96 | Dennis Schröder | 271 | -1.46 | -0.54 | -2.16 |
| 97 | Derrick White | 251 | -1.50 | +0.63 | -1.61 |
| 98 | Dyson Daniels | 166 | -1.50 | +1.22 | -0.28 |
| 99 | Jalen Duren | 422 | -1.58 | -0.70 | -1.65 |
| 100 | Jalen Suggs | 248 | -1.65 | +0.23 | -1.46 |

### 2025-26 Playoffs — top 100 defense

> pool 230 players, 112 above the minutes floor

| # | player | mp | est. offense | est. defense | est. total |
|---:|---|---:|---:|---:|---:|
| 1 | Neemias Queta | 152 | -1.85 | +4.23 | +1.75 |
| 2 | Karl-Anthony Towns | 578 | +3.63 | +4.22 | +7.26 |
| 3 | Victor Wembanyama | 750 | +2.46 | +4.08 | +6.02 |
| 4 | Jose Alvarado | 170 | +2.36 | +4.04 | +4.65 |
| 5 | Amen Thompson | 264 | +1.39 | +3.55 | +4.98 |
| 6 | Alex Caruso | 353 | +2.54 | +3.45 | +6.49 |
| 7 | Tari Eason | 195 | +1.73 | +3.39 | +4.11 |
| 8 | Alperen Sengun | 232 | +0.90 | +3.32 | +5.00 |
| 9 | Ausar Thompson | 427 | -0.64 | +3.25 | +2.64 |
| 10 | Ajay Mitchell | 317 | +3.86 | +3.16 | +6.03 |
| 11 | Cason Wallace | 374 | +0.76 | +3.00 | +4.23 |
| 12 | Josh Hart | 614 | -0.40 | +2.70 | +3.26 |
| 13 | Jakob Poeltl | 134 | -1.00 | +2.57 | +1.33 |
| 14 | Toumani Camara | 165 | -1.84 | +2.42 | -0.01 |
| 15 | Jarrett Allen | 529 | +1.83 | +2.41 | +4.33 |
| 16 | Jaylin Williams | 240 | -0.28 | +2.31 | +2.21 |
| 17 | Rudy Gobert | 372 | -1.89 | +2.04 | +0.11 |
| 18 | Javonte Green | 132 | -2.80 | +2.03 | +0.43 |
| 19 | Mikal Bridges | 608 | +1.45 | +1.73 | +3.39 |
| 20 | OG Anunoby | 586 | +2.64 | +1.67 | +4.75 |
| 21 | Chet Holmgren | 459 | +2.04 | +1.57 | +4.48 |
| 22 | Jamal Shead | 224 | -0.03 | +1.57 | +1.56 |
| 23 | Mitchell Robinson | 251 | +0.19 | +1.57 | +2.37 |
| 24 | Isaiah Stewart | 165 | -2.32 | +1.54 | -0.94 |
| 25 | Devin Vassell | 801 | +0.89 | +1.42 | +3.29 |
| 26 | De'Aaron Fox | 704 | +1.30 | +1.40 | +2.40 |
| 27 | Isaiah Hartenstein | 350 | +0.72 | +1.35 | +2.90 |
| 28 | Jabari Smith Jr. | 252 | +0.09 | +1.30 | +1.26 |
| 29 | Dean Wade | 407 | +1.45 | +1.29 | +2.30 |
| 30 | Dyson Daniels | 166 | -1.50 | +1.22 | -0.28 |
| 31 | Dylan Harper | 615 | +1.54 | +1.22 | +2.56 |
| 32 | Paolo Banchero | 273 | +2.75 | +1.18 | +3.37 |
| 33 | Ja'Kobe Walter | 224 | -2.34 | +1.18 | -0.84 |
| 34 | Collin Murray-Boyles | 191 | +1.23 | +1.15 | +2.22 |
| 35 | Luke Kornet | 296 | -0.91 | +1.15 | +0.47 |
| 36 | Julian Champagnie | 705 | +1.70 | +1.08 | +3.34 |
| 37 | Jaylen Brown | 249 | +0.46 | +1.08 | +0.29 |
| 38 | Marcus Smart | 345 | +0.41 | +1.05 | +2.46 |
| 39 | Evan Mobley | 640 | -0.60 | +1.02 | +0.32 |
| 40 | Christian Braun | 187 | -2.45 | +0.97 | -1.17 |
| 41 | Anthony Black | 196 | -1.97 | +0.94 | -0.78 |
| 42 | VJ Edgecombe | 407 | +0.48 | +0.82 | +1.68 |
| 43 | Nikola Jokić | 237 | +2.41 | +0.75 | +2.92 |
| 44 | Reed Sheppard | 192 | -1.15 | +0.66 | -0.05 |
| 45 | Derrick White | 251 | -1.50 | +0.63 | -1.61 |
| 46 | Spencer Jones | 145 | -0.43 | +0.60 | -0.21 |
| 47 | Mike Conley | 168 | +2.50 | +0.55 | +2.62 |
| 48 | Jaden McDaniels | 406 | -0.81 | +0.54 | -1.03 |
| 49 | Deandre Ayton | 285 | -0.86 | +0.50 | -0.30 |
| 50 | Jayson Tatum | 218 | +6.15 | +0.47 | +6.93 |
| 51 | Jaxson Hayes | 163 | -1.18 | +0.32 | -0.64 |
| 52 | Jalen Suggs | 248 | -1.65 | +0.23 | -1.46 |
| 53 | Paul George | 394 | +2.37 | +0.14 | +0.81 |
| 54 | Miles McBride | 334 | +1.32 | +0.13 | +0.71 |
| 55 | James Harden | 672 | +2.75 | -0.00 | +3.80 |
| 56 | Cameron Johnson | 186 | +0.76 | -0.05 | +1.44 |
| 57 | Luke Kennard | 326 | +0.72 | -0.07 | +0.82 |
| 58 | Scottie Barnes | 273 | +3.34 | -0.22 | +2.29 |
| 59 | Wendell Carter Jr. | 237 | +0.32 | -0.25 | +0.29 |
| 60 | Andre Drummond | 142 | +0.79 | -0.28 | +0.13 |
| 61 | Isaiah Joe | 143 | +1.01 | -0.42 | -0.40 |
| 62 | Sam Hauser | 164 | +0.24 | -0.51 | -0.60 |
| 63 | Carter Bryant | 187 | -1.72 | -0.51 | -2.57 |
| 64 | Jalen Green | 151 | +2.97 | -0.53 | +0.83 |
| 65 | Dennis Schröder | 271 | -1.46 | -0.54 | -2.16 |
| 66 | Onyeka Okongwu | 199 | +0.30 | -0.59 | -0.32 |
| 67 | Jordan Clarkson | 195 | +0.38 | -0.61 | +0.95 |
| 68 | Desmond Bane | 253 | +0.84 | -0.66 | -0.04 |
| 69 | Duncan Robinson | 383 | +2.23 | -0.66 | +1.93 |
| 70 | Jalen Duren | 422 | -1.58 | -0.70 | -1.65 |
| 71 | Naz Reid | 323 | +2.36 | -0.71 | +0.86 |
| 72 | Kelly Oubre Jr. | 364 | +0.06 | -0.74 | -0.70 |
| 73 | Tobias Harris | 485 | +0.86 | -0.79 | -0.17 |
| 74 | Landry Shamet | 310 | +1.18 | -0.87 | -0.02 |
| 75 | Keldon Johnson | 411 | -1.26 | -0.88 | -1.00 |
| 76 | Caris LeVert | 216 | +1.44 | -0.90 | +0.13 |
| 77 | Stephon Castle | 760 | +0.90 | -0.91 | +0.41 |
| 78 | RJ Barrett | 271 | +3.21 | -0.91 | +2.29 |
| 79 | Jalen Brunson | 702 | +4.67 | -0.96 | +2.82 |
| 80 | Cade Cunningham | 572 | +2.71 | -1.05 | +1.85 |
| 81 | Donovan Mitchell | 652 | +2.74 | -1.11 | +0.66 |
| 82 | LeBron James | 384 | +1.56 | -1.14 | +1.16 |
| 83 | Jaylon Tyson | 216 | -1.24 | -1.18 | -2.58 |
| 84 | Max Strus | 481 | +0.35 | -1.19 | -0.64 |
| 85 | Harrison Barnes | 183 | -0.86 | -1.20 | -0.60 |
| 86 | Terrence Shannon Jr. | 203 | +0.46 | -1.22 | -0.93 |
| 87 | Sam Merrill | 338 | +2.45 | -1.23 | +0.91 |
| 88 | Scoot Henderson | 145 | +0.46 | -1.28 | -0.30 |
| 89 | Luguentz Dort | 335 | -1.18 | -1.46 | -2.15 |
| 90 | Jalen Johnson | 214 | +0.07 | -1.49 | -1.13 |
| 91 | Tim Hardaway Jr. | 140 | +1.90 | -1.53 | +1.19 |
| 92 | Rui Hachimura | 386 | +0.65 | -1.54 | -0.93 |
| 93 | Julius Randle | 400 | -3.22 | -1.61 | -4.31 |
| 94 | Jamal Murray | 238 | +0.57 | -1.80 | -2.11 |
| 95 | Jared McCain | 258 | +1.37 | -1.83 | -0.29 |
| 96 | Daniss Jenkins | 318 | +0.03 | -1.88 | -1.96 |
| 97 | Shai Gilgeous-Alexander | 544 | +4.48 | -1.92 | +2.36 |
| 98 | Jonathan Kuminga | 156 | -1.13 | -1.93 | -3.06 |
| 99 | Joel Embiid | 233 | +3.44 | -1.98 | +0.69 |
| 100 | Jrue Holiday | 192 | +1.63 | -2.05 | -0.18 |
