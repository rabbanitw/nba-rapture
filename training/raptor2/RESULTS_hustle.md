# Hustle / defend-dash / matchup scrape: real signal, no promotion

The local `scrape_hustle.py` run landed 41,485 docs in Atlas (defend
dashboards 2013-14+ RS+PO, hustle 2015-16+, defender-side season matchups
2017-18+ RS). `extract_hustle.py` aligns them to combined.npz rows —
95-100% match at the 10 season-end stamps, which is exactly where the CV
protocol's training AND test rows live, so coverage is not a confound.
23 engineered defense rates (deflections/charges/contested per36, defended
FG% and plus-minus at rim/overall/3pt, opponent scoring per 100 partial
poss while guarding) + 51 raw columns -> data_fixed/hustle.npz.

## The features are real

box_d — the weakest defense component (rho .865), short exactly these
inputs — improves when they're added to its linear-wide form:
season-held-out rho **.8650 -> .8766**, up in 9/10 seasons, biggest gains
in the tracking-rich late seasons (2021-22 .871->.885, 2022-23 .812->.837).

## The stack still doesn't promote (cv_hustle.py, 3 seed sets)

Protocol: cv_resid_pools verbatim (exact NaN-propagating hats, 3-seed
blend), defense side, arms = extra GBM feature columns.

| arm | s0 | s10 | s20 | verdict |
|---|---|---|---|---|
| +engineered (e) | 4.20, 3W3T4L | 5.25, 2W1T7L | 4.45, 3W2T5L | null/worse |
| +engineered cell-rel (ecr) | 4.40, 2W2T6L | 4.45, 3W1T6L | 4.55, 3W1T6L | worse |
| +engineered+raw (er) | **3.90**, 3W3T4L | **3.90**, 2W4T4L | **4.00**, 2W4T4L | below |
| box_d hat swap (wide+hustle, imputed) | 5.05 mean 6.24, 3W0T7L | — | — | rejected |

**er is the first defense arm ever to beat the 4.15 baseline median, and it
does so on all 3 seed sets.** But it fails the promotion gate: head-to-head
never positive (net -1, -2, -2), mean worse on 2/3 sets, the two pre-hustle
folds consistently regress (2013-14: -1.1 to -1.3 — test rows route down
missing branches the training rows don't take), and 2017-18 is seed-unstable
(4.60 / 6.80 / 7.40). The wins are real but small and localized: 2015-16
(+0.4 to +0.8 all seeds) and 2020-21 (+0.5 on 2/3).

The hat-swap arm confirms the known failure mode from the other direction:
a wide hat with 21%-of-matrix-coverage columns cannot use the all-finite
NaN convention, and the imputed version blows up exactly like every
imputed defense hat (13.4 and 12.3 dev@10 folds).

## Verdict

**Defense config unchanged: gbm + struct hats (hats3).** This closes the
last defense route flagged in RESULTS_defense_threads.md, and the outcome
matches the E3 era analysis: the component improves (+.012 rho — the
features do carry defensive information), yet ~37% of year-adjacent defense
label variance is non-repeating circumstance, and a 6% median improvement
that can't survive seed noise is what running into that floor looks like.
The er arm is the natural restart point if the label side ever changes
(e.g. multi-season-averaged defense targets), where the non-repeating
variance shrinks by construction.
