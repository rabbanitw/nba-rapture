# Multi-season defense targets: tried, rejected — and why the ceiling didn't move

The E3 story suggested the last defense lever was label-side: the
tracking-era defense label is only r=.631 stable year-over-year, so score
against (and train on) a *multi-season* skill estimate instead.
cv_def_multi.py executes it: target = minutes-weighted mean of
raptor_defense over {t-1, t, t+1}; adjacent labels from the fixed matrix
plus 2012-13 from the 538 archive; and for the fold holding out season S,
**every training row's window drops S** (a training target never contains
the held-out season's label realization). Eval truth for S uses the full
window. Each fold trains two GBMs (single-target = production, and
multi-target) and scores both against both truths.

## The 2x2 (s0, medians; base|single reproduces the stored baseline exactly)

| | vs single-season truth | vs multi-season truth |
|---|---:|---:|
| trained on single (production) | **4.65** / 4.57 | 7.95 / 8.64 |
| trained on multi | 6.15 / 7.19 | 8.55 / 9.45 |

Soft blends interpolate monotonically — target = (1-b)·single + b·multi:
b=0.15 -> 5.20/4.84, b=0.30 -> 5.55/5.37. No regularization sweet spot;
the 2022-23 fold (no t+1 exists — 538 shut down) is hurt at every blend
(2.9 -> 5.6). Margins are ~1.5 dev@10, far outside the seed-noise band,
so no seed gates were run.

## Why this fails — the honest correction to the E3 interpretation

1. **The single-season "circumstance" is partly feature-visible.** The
   stack's features include the same season's on-court DefRtg, opponent
   context and on/off blocks — they encode this season's circumstance.
   Training on smoothed targets removes signal the features can predict,
   so multi|single collapses (12.4 dev@10 folds in 2015-18).
2. **Changing the truth moves the disagreement, it doesn't remove it.**
   Against the multi truth both models get *worse* (7.95/8.55), because
   ranking career skill from one season's circumstance-contaminated
   features is harder than ranking that season's label. Signature case:
   Kawhi Leonard 2016-17 — multi-truth rank 5, single label rank 42,
   production model rank **114**. The E3 bound is therefore not pure
   label noise; part of the "non-repeating variance" is circumstance the
   model already (correctly) chases.

## What the multi truth is good for: adjudicating the wing/big axis

Re-ranking the RESULTS_def_rank_outliers.md cases under the stable truth:

| case | single rank | multi rank | model est | verdict |
|---|---:|---:|---:|---|
| Ntilikina 2017-18 | 20 | 16 | 106 | persistent skill — model truly wrong |
| Kenrich Williams 2022-23 | 14 | 10 | 42 | persistent — model wrong |
| Derrick White 2019-20 | 16 | 8 | 47 | persistent — model wrong |
| Kidd-Gilchrist 2017-18 | 12 | 10 | 70 | persistent — model wrong |
| Dort 2020-21 | 9 | 19 | 49 | mostly persistent |
| Al Horford 2017-18 | 7 | 41 | 34 | single-season circumstance — model was right |
| Kidd-Gilchrist 2013-14 | 5 | 26 | 32 | circumstance — model right |
| Brook Lopez 2020-21 | 55 (est 14) | 12 | 14 | label was the noise — model right |
| Brook Lopez 2018-19 | 77 (est 17) | 44 | 17 | real bias — model wrong |
| Drummond 2015-16 | 48 (est 11) | 85 | 11 | real bias — model wrong |

The suppression-wing misses are real, persistent skill the box-D channel
cannot see; the big-premium bias is mostly real too; and a minority of
apparent misses were the label's own noise, where the model's skepticism
was validated.

## Verdict

**Rejected as a target; retained as an audit instrument.** Production
defense stays single-season-trained (gbm + hats3, huber). The label-side
route E3 pointed to is closed: dev@10's floor is not removable by label
smoothing because the features carry the same single-season circumstance
as the label. The remaining honest improvements are input-side
(full-history matchup/suppression data, which does not exist pre-2017) —
or accepting the floor. Artifacts: RESULTS_cv_def_multi.json,
_b0.15/_b0.3 variants, DIAG_def_multi_rows.json (per-row ranks under
both truths, both models, for any future audit).
