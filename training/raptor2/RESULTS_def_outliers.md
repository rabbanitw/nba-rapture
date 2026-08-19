# Defense outliers: the extremes were mostly corrupted labels, now fixed

Task: analyze the extreme outliers in the defense projection, study their
features, and execute elimination strategies. Outcome: the dominant cause
was a **data-identity bug**, not the model. Fixing it corrected 136 labeled
rows and four downstream feature artifacts; the model's worst "misses"
turned out to be cases where it was right and the label belonged to a
different player.

## 1. Diagnosis (diag_def_outliers.py)

Production defense config, all 2,242 eval rows, rank-based extreme
definition (actual top-15 projected outside 25, or vice versa, or |resid|
>= 2.5): 55 extremes. The worst looked impossible rather than unlucky —
e.g. *Kemba Walker 2022-23, actual +3.10 at 1,703 mp* (he played ~250
minutes that season and was never a plus defender; the model said -2.75).

## 2. Root cause: fuzzy-name label collisions

The 538 label docs were standardized with the old fuzzy name matcher.
Players missing from its roster — mostly 2021+ rookies — were mapped onto
existing names, and labels.py joins by that standardized name:

| matrix row | actually carried the label of |
|---|---|
| Kemba Walker 2022-23 | **Walker Kessler** (+3.1 D, 1,703 mp) |
| Austin Rivers 2021-22/2022-23 | **Austin Reaves** |
| Jalen Smith 2021-22/2022-23 | **Jalen Suggs** |
| Josh Hart 2021-22 | (another player; archive says Hart was -0.09, ours +2.7) |
| Julius Randle 2021-22 | someone at 584 mp (Randle: 2,544) |

Because the join keeps the first doc per name, the rookie's row also
*displaced* the veteran's true label. Audit across all 10 whole-season
cells (the only cells the CV protocol trains and tests on): **0 corrupted
rows 2013-14..2019-20, 1 in 2020-21, 25 in 2021-22, 77/361 (21%) in
2022-23.** The true identity survives on every doc (`name`/`data_key`), so
`migrate_label_names.py` re-joins by true name and corrected **136 rows**
(y, y_off, y_def, mp; component labels in components.npz) with 0 labels
lost. Backups: `data_fixed/*.npz.pre_namefix`.

## 3. Second-order poison: label-side mp inside frozen features

`mp` in combined.npz comes from the label doc, and four frozen artifacts
had baked per-36 features scaled by it: defend.npz, shotdash.npz,
hustle.npz, wowyopp.npz. Signature case: *CJ Elleby 2021-22* — old label
mp 2 gave `d2_value36 = 1537` and a struct box_d hat of **+20.85**, and
the blend emitted est -24.4. All four artifacts rebuilt on the fixed
matrix; Elleby now est -1.59 vs actual -2.5 with sane hats.

## 4. Re-baselines on honest data (everything downstream of this is new)

| metric | corrupt labels | fixed data |
|---|---|---|
| offense hats3 CV | 1.60 / 1.69 | **1.50 / 1.63** |
| offense production boards (hats4-both + pooling) | 1.20, 93/100 | **1.20 / 1.24, 94/100** |
| defense hats3 CV s0 | 4.15 / 4.11 | **4.65 / 4.57** |
| defense seed spread (s0/s10/s20 medians) | — | 4.65 / 5.00 / 5.05 |

The old 4.15 is not comparable: its 2020-23 folds were scored *against
corrupted labels*. On honest data the three most recent seasons become the
three **best** defense folds (2020-21: 3.2, 2021-22: 3.2, 2022-23: 2.9) —
the corruption had been sitting exactly where the eval was weakest. The
apparent worsening of some early folds vs the old number is inside the
seed-noise band the defense stack has shown all cycle (2017-18 swung
4.6 -> 7.4 across seeds in earlier tests).

Extreme-outlier count: 55 -> 46, and every impossible-looking case is gone.

## 5. Remaining outliers are genuine, and the robust strategies are null

What's left, per the contribution study: (a) top-tail compression — every
Gobert #1 season underestimated by ~2.7 but still ranked 1-2; (b) chronic
box-invisible effort defenders (Kidd-Gilchrist, Dort, Kenrich Williams);
(c) center on/off mirages in both directions (Jarrett Allen, Kanter,
Tristan Thompson). Strategies executed against the new baseline
(cv_def_robust.py):

| arm | result | verdict |
|---|---|---|
| train-floor mp>=250 | 5.00/4.74, 3W3T4L | null (tiny-mp labels like +15.4 @ 30 mp are down-weighted enough by sqrt(mp)) |
| winsorize labels ±6 | 4.80/5.12, 3W4T3L | null-to-worse (the heavy tail is signal — clipping Gobert hurts) |
| huber objective | identical, 0W10T0L | **already production**: tuned defense params are huber alpha=2.0 (offense is l2) |

So label-side robustness was already in the stack, and the remaining
extremes are the E3 story again: single-season defensive circumstance the
features cannot see. The un-executed ideas with any theoretical bite are
label-side (multi-season-averaged defense targets) — a metric change, not
an outlier fix.

## Caveats

- In-season snapshot cells share the fuzzy-name defect but train nothing
  in the raptor2 protocol; left as-is.
- Rookie rows displaced at build time (Kessler, Reaves, Suggs feature rows)
  are absent from the matrix entirely; recovering them needs a
  build_dataset rerun with fixed standardization — worth doing whenever the
  matrix is next rebuilt.
- All stored CV JSONs from before this fix were measured against partly
  corrupted 2020-23 labels. Cross-era comparisons should use only the
  re-baselined numbers above; within-era promotion verdicts (offense
  hats4-both, pooling) were relative comparisons on identical labels and
  the production offense config re-verifies on clean data (1.20, 94/100).
