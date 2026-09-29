# Defense wing-vs-big bias: position-relative features + per-group GBMs

Follow-up to RESULTS_def_rank_outliers.md (under-ranked suppression wings,
over-ranked contest-volume bigs). Position was already a GBM input
(`ctx|pos_*` multi-hot), so the arms change structure, not inputs.
`cv_def_wingbig.py` runs production defense (gbm + exact NaN hats3, huber,
3-seed blend + ridge) with one change per arm; `diag_wingbig.py` reads the
per-row OOF predictions. Group: bigness = mean multi-hot position index
(PG=1..C=5), **big = bigness >= 4** (PF, PF/C, C; ~32% of labeled rows).

The base arm reproduces RESULTS_hats3_cv.json exactly (0W10T0L) on the
rebuilt container's package versions (numpy 2.5, lightgbm 4.7, sklearn 1.9).

## Winner: `prelf_sb` — promotes 3/3 seed sets

`prelf` = production features + the 12 struct DB columns and 8 hustle /
matchup columns standardized **within cell x group** (a center's blocks
vs other bigs that season). `prelf_sb` = 0.5 * prelf GBM + 0.5 * (same
features, separate GBM+ridge blends for bigs and non-bigs).

Seed-matched vs the base arm (dev@10 median / mean; H2H over 10 folds):

| seed | base | prelf_sb | H2H | dev@20 base -> prelf_sb |
|---|---|---|---|---|
| s0 | 4.65 / 4.57 | **3.55 / 3.85** | 7W 1T 2L, net +7.2 | 9.46 -> 8.60 |
| s10 | 5.00 / 5.00 | **4.30 / 4.74** | 8W 0T 2L, net +2.6 | 9.46 -> 8.75 |
| s20 | 5.05 / 5.33 | **3.65 / 4.20** | 9W 0T 1L, net +11.3 | 9.38 -> 8.68 |

Median, mean, dev@20 and head-to-head all improve on every seed set — the
gate the hustle `er` arm failed (its H2H was never positive). Not one fold:
2016-17 is the biggest gain (+3.0 to +7.1; baseline there was seed-unstable
6.1-9.0, prelf_sb is 1.9-3.1), but 2017-18 (+1.0/+1.0/+1.3) and 2018-19
(+1.1/+1.1/+0.9) also win every seed, and the pre-hustle folds 2013-15 do
not regress. Weak spots: 2020-21 -0.3 to -0.5 every seed; 2021-22 -3.6 on
s10 (the per-group component's recurring 2021-22 miss, see splitblend).

### The bias it targets shrinks

Rank-error by group on the >=1065 pool (projected rank - actual rank;
+ = under-ranked), s0/s10/s20:

| | base | prelf_sb |
|---|---|---|
| actual top-30, bigs | +6.0 / +6.0 / +5.8 | +6.8 / +6.7 / +6.8 |
| actual top-30, non-bigs | **+7.7 / +7.5 / +7.8** | **+6.4 / +6.4 / +6.2** |
| projected top-30, bigs | **-7.8 / -8.4 / -7.6** | **-7.3 / -7.6 / -7.0** |
| projected top-30, non-bigs | -4.8 / -4.6 / -4.6 | -5.5 / -5.9 / -5.8 |
| over-ranked (proj top-20, 20+ worse) | 30 / 30 / 29 | 26 / 29 / 28 |
| ... of which bigs | 20 / 19 / 20 | 16 / 19 / 17 |

The wing/big asymmetry in the actual top-30 flips from 1.7 ranks against
non-bigs to ~0.4 against bigs; the big over-ranking gap in the projected
top-30 roughly halves. Named cases (proj rank / actual, stable across
seeds): Horford 2017-18 34 -> 19, Derrick White 2019-20 47 -> 36, Pachulia
2016-17 11 -> 29 (actual 74). **Not fixed:** Ntilikina (~100 / 20), Dort
(~50 / 9), Brook Lopez 2018-19 (~16 / 77), Drummond 2015-16 (~12 / 48);
Kenrich Williams 2022-23 gets worse (42 -> 55 / 14). The multi-season audit
(RESULTS_def_multi.md) already classed these as persistent skill the
features cannot see — the structural fix removes the *group-level* bias,
not the invisible-input misses.

## Close alternative: `prelfx_sb`

Same recipe plus position-relative defend-dash (8) and on/off-d (3)
columns. Most seed-stable and best on the broader board, slightly behind
on dev@10:

| seed | median / mean | H2H | dev@20 | over-ranked (bigs) |
|---|---|---|---|---|
| s0 | 3.80 / 4.42 | 4W 2T 4L, +1.5 | 8.40 | 25 (16) |
| s10 | 3.85 / 4.41 | 8W 0T 2L, +5.9 | 8.11 | 24 (15) |
| s20 | 3.75 / 4.38 | 8W 1T 1L, +9.5 | 8.54 | 25 (16) |

Prefer it if the target is the top-20 board / bias counts rather than the
dev@10 gate.

## Everything else

| arm | s0 | s10 | s20 | verdict |
|---|---|---|---|---|
| ghat_bd (box_d hat per group) | 5.70 / 5.99 | — | — | worse |
| ghat (box_d + onoff_d hats per group) | 5.95 / 5.91 | — | — | worse |
| prelhat (box_d hat on position-rel DB) | 5.75 / 5.56 | 5.25 / 5.70 | 5.40 / 5.54 | worse 3/3 |
| prelf (features only) | 4.05 / 4.57 | 3.90 / 4.61 | 4.10 / 4.62 | better median; H2H 4-5-5 / 5-2-3 / 5-0-5 |
| split (per-group GBMs only) | 4.75 / 5.14 | — | — | worse; under-ranks 16 -> 24 |
| splitblend (0.5 base + 0.5 split) | 4.45 / 4.64 | 4.55 / 4.66 | 4.65 / 4.66 | small; 2021-22 -4 every seed |
| prel3 (guard / wing / big) | 4.15 / 4.71 | — | — | below prelf |
| prelc (continuous bigness adjustment) | 4.60 / 5.22 | — | — | worse |
| prelc_sb | 5.05 / 5.09 | — | — | worse |
| prelfx | 3.70 / 4.50 | — | — | ~prelf |
| cov_full (hustle-era-only GBM, 50/50) | 3.45 / 4.39 | 3.55 / 4.57 | 3.65 / 4.43 | good dev@10, dev@20 worse (9.53-9.66); no bias change |
| cov_slim (~80 focused cols) | 5.75 / 5.20 | — | — | worse |
| psb_cov (prelf_sb + cov_full) | 3.85 / 4.38 | 3.85 / 4.41 | 3.85 / 4.41 | no gain over prelf_sb; dev@20 worse |

Readings:

- **Hat-level fixes fail again** (ghat, prelhat): consistent with every
  prior defense hat change. Per-group ridge hats are noisier (half the rows)
  and the GBM is sensitive to hat quality.
- **Position-relative features and per-group models are complementary.**
  Relative features let the model see "average blocks *for a big*";
  separate models let the stat -> defense mapping differ by archetype. Alone,
  split starves each model (bigs ~940 rows) and over-fits; blended with the
  global model it regularizes, and with relative features it wins.
- A hard 2-group split beats both 3 groups and a continuous bigness
  adjustment — the PF/C boundary is where the bias lives.
- The hustle-era-only model (cov_full) lifts the top-10 but not the top-20
  or the bias; it's a different effect and doesn't stack with prelf_sb.

## Verdict

**prelf_sb passes the defense promotion gate on 3/3 seed sets** — the first
defense change to do so since hats3. Not yet wired into production
(`boards_best.py --side defense` still runs hats3). Artifacts:
RESULTS_cv_wingbig_s{0,10,20}_*.json (per-season dev@10/20, tau, bias
counts), DIAG_wingbig_s*_*.npz (per-row OOF predictions per arm).
