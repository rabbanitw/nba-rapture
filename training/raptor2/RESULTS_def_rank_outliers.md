# Defense rank outliers in the top 20: a wing-vs-big axis

Follow-up to RESULTS_def_outliers.md, on the name-fixed data. Question:
who are the players the defense projection puts *dozens of ranks* from
their true placement — especially the true top-20 — and what do their
features have in common?

Definitions (per season, >=1065-minute pool, from DIAG_def_outliers.json):
**underranked** = actual top-20, projected 20+ ranks worse (n=16);
**overranked** = projected top-20, actually 20+ ranks worse (n=30);
**control** = actual top-20, projected within 10 ranks (n=150).

## The rosters split cleanly by position archetype

Underranked — mostly guards/wings:

| season | player | true -> est | act | est |
|---|---|---|---:|---:|
| 2017-18 | Frank Ntilikina | 20 -> 106 | +2.50 | +0.36 |
| 2017-18 | Michael Kidd-Gilchrist | 12 -> 70 | +2.90 | +1.10 |
| 2013-14 | Kris Humphries | 20 -> 77 | +3.00 | +1.02 |
| 2020-21 | Luguentz Dort | 9 -> 49 | +3.80 | +1.15 |
| 2019-20 | Derrick White | 16 -> 47 | +2.20 | +0.78 |
| 2022-23 | Kenrich Williams | 14 -> 42 | +2.90 | +1.10 |
| 2013-14 | Michael Kidd-Gilchrist | 5 -> 32 | +4.40 | +1.99 |
| 2017-18 | Al Horford | 7 -> 34 | +3.30 | +2.05 |

(+ Rubio, Crowder, Richardson, Danny Green, Nurkic, Noel, Varejao,
Brook Lopez 2015-16.)

Overranked — overwhelmingly centers: Brook Lopez ×3 (2018-19: est 17,
true 77), Pachulia (est 12, true 74), Bosh, Gasol, Drummond, Capela,
DeAndre Jordan, Zubac, Gortat, Adams, Looney, Hartenstein, Ed Davis,
Okongwu, Robin Lopez, Myles Turner, Jarrett Allen … plus a few
reputation guards (Chris Paul 2016-17 est 15 / true 55, Jrue Holiday
2020-21, Marcus Smart 2014-15).

## Where the estimate goes wrong: the box-D channel

Label decomposition (538 components) and our hat values, group means:

| group | actual | label box_d | label onoff_d | hat box_d | hat onoff_d | hat err box | hat err onoff |
|---|---:|---:|---:|---:|---:|---:|---:|
| control | +3.70 | +3.58 | +3.27 | +2.73 | +3.34 | -0.81 | +0.07 |
| underranked | +2.96 | +2.97 | +2.02 | **+0.84** | +1.46 | **-2.02** | -0.56 |
| overranked | +1.59 | +1.44 | +1.83 | +2.55 | +2.69 | **+1.11** | **+0.86** |

The underranked miss is concentrated in the box-D hat: their 538 box-D
component is high (+2.97) but the structural box formula sees almost none
of it (+0.84). The on/off hat is roughly fine. The overranked bigs get
overshoot from *both* hats.

## What their features look like

Group means (matrix + defend-dash + hustle/matchup; hustle 2015-16+,
matchups 2017-18+):

| feature | control | underranked | overranked |
|---|---:|---:|---:|
| blocks (volume) | 75.7 | **55.6** | 77.1 |
| def rebounds | 4.8 | 3.7 | 4.4 |
| charges drawn (pbp) | 6.1 | **8.3** | 4.2 |
| hustle charges /36 | 0.09 | **0.18** | 0.04 |
| deflections /36 | 2.58 | 2.41 | **1.90** |
| contested 2s /36 | 7.9 | 6.5 | **9.0** |
| defended FGA /36 | 16.5 | 15.0 | **17.2** |
| rim dFG% | .55 | .59 | .56 |
| **matchup pts allowed /100** | **26.3** | **23.4** | **28.8** |
| on-court DefRtg | 105.7 | 108.5 | 106.5 |

Per-player, the pattern is stark: Crowder 19.7 / Richardson 19.1 /
Ntilikina 19.0 / Kenrich 21.9 matchup points allowed per 100 (elite),
Kenrich 0.78 charges/36 (~8x control); versus Brook Lopez 2020-21 at
36.9, Hartenstein 37.9, Looney 31.9 matchup points allowed — the drop
bigs bleed in space and the label knows it.

**Summary of the axis:** the structural box-D formula prices rim
protection volume (blocks, contests, defended FGA) and under-prices ball
pressure (deflections, charges) and matchup difficulty/suppression. True
top-20 wings whose 538 box-D is earned through those channels get pushed
30-90 ranks down; interior-volume bigs without them get pulled into the
top-20. This is the concrete face of the box_d component gap (rho .865,
.877 with hustle inputs) — 538's tracking-era box-D uses the
defended-shot / hustle / matchup data we only recently scraped.

## Executed fix attempt: targeted features, fixed labels — still null

All earlier hustle-feature CV predated the label fix, so re-run on honest
labels, including a **slim arm** = only the four discriminating columns
(charges/36, deflections/36, matchup pts/100, matchup eFG allowed):

| arm | s0 | s10 | s20 |
|---|---|---|---|
| baseline (fixed) | 4.65 / 4.57 | — | — |
| + engineered (23 cols) | 4.95, 4W0T6L | — | — |
| + slim (4 cols) | 5.20, 1W2T7L | 5.15, 4W1T5L | 4.70, 2W4T4L |

(RESULTS_cv_hustle_e-slim.json, _s10_slim.json, _s20_slim.json.)

The features that *identify* the misses cross-sectionally do not fix them
as GBM inputs: coverage starts 2015-16/2017-18 (early folds regress),
and within covered folds the gains don't survive seed noise. Same verdict
as the full hustle integration, now confirmed on clean labels.

## Conclusion

The top-20 rank outliers are not random error: they are a systematic
positional bias inherited from the box-D channel's inputs, identifiable
by charges/deflections/matchup-suppression on one side and
contest/defended-FGA volume without suppression on the other. The
correction signal exists in the scraped hustle/matchup features but is
too thin and too late-era to clear the promotion gate under this label
regime — consistent with the E3 bound. The two routes that could move it:
full-history matchup-type data (not scrapeable pre-2017), or a label-side
change (multi-season defense targets) that raises the ceiling the gate
measures against.
