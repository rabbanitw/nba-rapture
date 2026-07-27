# RAPTOR prediction pipeline

Predicts FiveThirtyEight RAPTOR totals from the scraped stats in MongoDB.
Seasons **2013-14 and 2014-15 are held out as test** and are scored exactly once.

```
python training/discover_schema.py     # survey the collection
python training/coverage.py            # measure per-field population rates
python training/build_dataset.py --model all   # -> training/data/{box,onoff,combined}.npz
python training/train_rapture.py --model all   # -> training/models/ (total, offense, defense)
python training/mp_sweep.py            # minimum-minutes threshold sweep
python training/experiment_combined.py # -> training/RESULTS_combined.md
python training/estimated_raptor.py && python training/compare_estimated_raptor.py  # external baseline
python training/leaderboards.py --top-n 100              # -> training/RESULTS_top100.md
python training/leaderboards.py --top-n 100 --no-min-mp # -> ..._nofilter.md
python training/starter_models.py      # -> training/RESULTS_starters.md
```

Credentials go in `credentials.txt` at the repo root (username line 1, password
line 2). It is gitignored.

## Architecture

Three models. The first two mirror how 538 defines the metric; the third ignores
that split deliberately. Per their
[data dictionary](https://github.com/fivethirtyeight/data/tree/master/nba-raptor),
`raptor_box_*` is "based only on box score estimate" and `raptor_onoff_*` is
"based only on plus-minus data", both in **points above average per 100 possessions**.

| model | features | label |
|---|---|---|
| `box` | `pbp` + the 14 `nba-tracking` blocks | `rap_box` |
| `onoff` | `wowy` on, `wowy` off, and **on − off** | `rap_onoff` |
| `combined` | all of the above at once | `rap` |

RAPTOR is *Robust Algorithm using Player Tracking and On/Off Ratings*, so the
first two rows mirror how the metric is built: feeding on/off data to the box
model (or vice versa) would predict a component from inputs 538 did not use for it.

`combined` drops that separation on purpose. It predicts 538's **blended** RAPTOR
(`rap` / `rap_o` / `rap_d`, "using both box and on-off components") from every
source at once, which is the number you actually want if you are reviving the
public-facing metric rather than reproducing its internals.

**LightGBM** is the primary learner, with a Ridge baseline and a 75/25 blend.
This is tabular data — 462 to 1,143 heterogeneous columns, a few thousand independent
player-seasons, heavy skew, and genuine missingness that LightGBM splits on
natively. Gradient-boosted trees are the right default there, and the gain
rankings are worth having for a metric-revival project. The blend wins on nearly
every target because Ridge and LightGBM make uncorrelated errors.

Every model is also fit against its offense and defense halves — see
*Offense and defense* below, where summing two part-models turns out to beat
predicting the total directly for on/off but not for box or combined.

## What the data actually looks like

The collection is 4.17M docs across four sources. Two things drive the design:

**1. Six synthetic full-season snapshots.** Timestamps `YYYY0715000000` (2014–2018)
plus `20201101000000` hold one row per player for a whole finished season:

| snapshot | season | role |
|---|---|---|
| `20140715000000` | 2013-14 | **test** |
| `20150715000000` | 2014-15 | **test** |
| `20160715000000` | 2015-16 | validation |
| `20170715000000` | 2016-17 | validation |
| `20180715000000` | 2017-18 | validation |
| `20201101000000` | 2019-20 | validation |

There are **no timestamps inside the 2013-14 or 2014-15 season windows** — those
seasons exist only as their offseason snapshot, split into `Regular season`
(250 players) and `Playoffs` (100 players). Everything else is 2020-21 onward,
scraped roughly daily *during* the season, so those rows are season-to-date.

Because the four other full-season snapshots have the same shape as the test set,
they make a distribution-matched **validation set**. Row filters and boosting
rounds are chosen there; the test seasons are never consulted during selection.

**2. Duplicate documents, including wrong-season contamination.** Some keys have
up to 10 docs. They are not harmless copies — at `20140715000000`, DJ Augustin's
tracking rows include HOU and LAL (2021-22) alongside the correct CHI row.
The correct row is the most recently inserted one:

| rule | median \|MIN − 538 mp\| / mp | within 5% of 538 mp |
|---|---|---|
| **max `_id` (latest insert)** | **0.0000** | **93.8%** |
| max `MIN` | 0.0000 | 81.2% |
| min `_id` (earliest) | 0.1835 | 37.5% |

So `pick_doc` takes `max(_id)`, validated against 538's own minutes column.
Byte-identical snapshots of the same player-season are then collapsed
(`dedupe`), which removes ~1.5k rows from the on/off set.

## Feature selection

Fields are chosen by **measured population rate**, not by truncation. Absence is
mostly *"this player recorded zero of these"* — bench players never draw a charge —
not *"never scraped"*. So a field is kept when it appears in **both** the historical
and modern eras (≥5%), and sparse values are left as NaN for LightGBM to split on.
Only true schema changes are dropped: `BlockingFouls`, `BlockingFoulsDrawn`,
`Clear Path Fouls` (gone from the modern feed) and `Transition Take Fouls[ Drawn]`
(absent from the historical one).

That yields 454 raw stat fields for each component model and 908 for `combined`.
With derived features the matrices are **462 / 689 / 1,143 columns**:

| model | stat fields | derived | context | **total** |
|---|---|---|---|---|
| box | 240 `pbp` + 214 tracking (14 blocks) | — | 8 | **462** |
| on/off | 227 `wowy_on` + 227 `wowy_off` | 227 `on − off` | 8 | **689** |
| combined | 240 `pbp` + 214 tracking + 454 wowy | 227 `on − off` | 8 | **1,143** |

Context is 5 position one-hots + `mp` + `season_progress` + `is_playoffs`.

### Dataset size

| model | fit rows | validation | **train total** | test |
|---|---|---|---|---|
| box | 15,227 | 1,347 | **16,574** | 696 |
| on/off | 15,940 | 1,250 | **17,190** | 696 |
| combined | 14,235 | 1,241 | **15,476** | 692 |

`combined` is slightly smaller because it needs `pbp`, `wowy_on` and `wowy_off`
all present on the same row — 270 timestamps carry all four sources, including
every one of the six full-season snapshots.

Row counts overstate the information available. Those ~17k rows cover only
**2,700 (box) / 2,531 (on/off) distinct player-season-split groups** across 671
players and 7 seasons, because a modern player-season recurs at many snapshot
dates. That redundancy is why validation is grouped by full season rather than
sampled at random.

**Counting stats are normalized to rates.** RAPTOR is per-100-possessions, but the
scraped values are season-to-date totals, so a January snapshot and an April
snapshot of the same player look completely different while carrying nearly the
same label. Each block has its own denominator (`TotalPoss` for pbp/wowy, `MIN`
for tracking); a column is divided by it when it is non-negative and correlates
≥0.75 with it, unless its name says it is already a rate. That normalizes 88/454
columns for box (the tracking feeds are already mostly percentages) and 217/454
for on/off; the classification is written to `models/*_normalization.json`.

## Results (held-out 2013-14 + 2014-15, n≈696 each)

| model | | RMSE | MAE | R² | Spearman |
|---|---|---|---|---|---|
| **box** | mean baseline | 3.242 | 2.461 | −0.078 | — |
| | Ridge | 2.179 | 1.534 | +0.513 | +0.781 |
| | LightGBM | 1.818 | 1.346 | +0.661 | +0.823 |
| | **blend** | **1.745** | **1.286** | **+0.687** | **+0.835** |
| **onoff** | mean baseline | 6.382 | 4.460 | −0.014 | — |
| | Ridge | 3.652 | 2.520 | +0.668 | +0.825 |
| | LightGBM | 3.204 | 2.084 | +0.744 | +0.895 |
| | **blend** | **3.124** | **2.038** | **+0.757** | **+0.900** |

By slice (LightGBM). Playoff rows are harder — small samples make the label itself
noisy, and on/off RMSE there is ~3× the regular-season figure:

| slice | n | box R² | box ρ | onoff R² | onoff ρ |
|---|---|---|---|---|---|
| 2013-14 Regular season | 248 | +0.716 | +0.838 | +0.804 | +0.912 |
| 2013-14 Playoffs | 100 | +0.495 | +0.695 | +0.730 | +0.892 |
| 2014-15 Regular season | 248 | +0.738 | +0.851 | +0.777 | +0.889 |
| 2014-15 Playoffs | 100 | +0.619 | +0.840 | +0.726 | +0.894 |

Top features are the ones the metric is actually about: box leans on `pbp|EfgPct`,
`pbp|OnOffRtg`, `pbp|PlusMinus`, `pbp|TsPct`; on/off leans on `wowy_diff|Points`,
`wowy_on|PlusMinus`, `wowy_diff|OpponentPoints`. Predicted 2013-14 leaders are
Curry / Durant / LeBron / Harden for box and Iguodala / Thompson / Korver /
Ginóbili for on/off — the right names for that era.

`ctx|mp` ranks high but is not leakage; it comes from the label document but is
minutes played, not a RAPTOR value, and is near-redundant with `pbp|Minutes`.
Dropping it moves box R² 0.660 → 0.639 and on/off 0.738 → 0.741.

## Offense and defense

`train_rapture.py --targets total offense defense` also fits the two halves
separately (`rap_*_o`, `rap_*_d`). The total is `o + d` up to the scrape's
1-decimal rounding (max deviation 0.101), so summing two part-models is a real
alternative to predicting the total directly.

| model | target | RMSE | MAE | R² | Spearman |
|---|---|---|---|---|---|
| **box** | total | 1.745 | 1.286 | +0.687 | +0.835 |
| | offense | 1.033 | 0.818 | +0.790 | +0.902 |
| | defense | 1.630 | 1.232 | +0.481 | +0.742 |
| **on/off** | total | 3.124 | 2.038 | +0.757 | +0.900 |
| | offense | 1.958 | 1.396 | +0.772 | +0.891 |
| | defense | 2.068 | 1.360 | +0.756 | +0.897 |

(Best of LightGBM / blend per row. R² is not comparable *across* targets — the
three labels have different variance — but is comparable within a target.)

**Box defense is the weak spot at R² +0.481**, roughly 0.3 below box offense,
while on/off defense reaches +0.756 from the same held-out players. That is the
argument for RAPTOR's two-component design restated as a measurement: play-by-play
and tracking stats capture offensive production well and defensive impact poorly,
and plus-minus data recovers exactly what the box score misses. Box defense is
also where Ridge collapses entirely (R² −0.014 vs LightGBM's +0.481) — the signal
there is interaction-heavy, not linear. Box offense is the opposite case, the one
target where Ridge alone (+0.804) beats LightGBM (+0.752).

### The combined model

`combined` predicts the blended RAPTOR (`rap_o` / `rap_d` / `rap`) from every
source at once — 1,143 features, no box/on-off separation:

| target | | RMSE | MAE | R² | Spearman |
|---|---|---|---|---|---|
| **offense** | LightGBM | 1.100 | 0.846 | +0.793 | +0.891 |
| | **blend** | **1.024** | **0.783** | **+0.820** | **+0.908** |
| **defense** | LightGBM | 1.450 | 1.071 | +0.613 | +0.804 |
| | **blend** | **1.409** | **1.041** | **+0.634** | **+0.818** |
| **total** | LightGBM | 1.794 | 1.341 | +0.728 | +0.876 |
| | **blend** | **1.715** | **1.280** | **+0.751** | **+0.886** |

By slice:

| slice | n | off R² | off ρ | def R² | def ρ | total R² | total ρ |
|---|---|---|---|---|---|---|---|
| 2013-14 Regular season | 247 | +0.822 | +0.897 | +0.692 | +0.857 | +0.760 | +0.880 |
| 2013-14 Playoffs | 99 | +0.800 | +0.891 | +0.489 | +0.696 | +0.666 | +0.841 |
| 2014-15 Regular season | 247 | +0.825 | +0.912 | +0.706 | +0.851 | +0.774 | +0.889 |
| 2014-15 Playoffs | 99 | +0.707 | +0.889 | +0.564 | +0.758 | +0.699 | +0.873 |

Offense is the easy half everywhere (R² +0.820 vs defense's +0.634). But the
useful comparison is against the box-only defense model: **+0.481 → +0.634 once
`wowy` features are in the pool.** That is the same finding as above from the
other direction — plus-minus carries defensive signal that play-by-play and
tracking do not — and it is visible in the gain rankings, where combined defense
leans on `pbp|OnDefRtg`, `wowy_diff|OpponentPoints`, `pbp|Steals` and
`track:defensive-impact|DFG%` together. Across the total model's top 30 features,
20 are `pbp`, 7 are `wowy`, 2 are tracking.

Note the labels differ: `rap_*` is 538's blended rating while `rap_box_*` and
`rap_onoff_*` are its components, so R² is **not** comparable across the three
models — only within a target. The defense comparison above is directional, not
a like-for-like delta.

One quirk: `rap_o` / `rap_d` / `rap` are stored rounded to one decimal (the
component labels are not), which puts a ~0.029 noise floor on those targets.
Negligible against label sds of 2.3–3.4.

### Sum of parts vs. direct total

Both predict the same `total` label, so these are directly comparable:

| model | | RMSE | MAE | R² | Spearman |
|---|---|---|---|---|---|
| box | direct total | **1.745** | **1.286** | **+0.687** | **+0.835** |
| | offense + defense | 1.834 | 1.340 | +0.655 | +0.824 |
| on/off | direct total | 3.124 | 2.038 | +0.757 | +0.900 |
| | **offense + defense** | **2.893** | **1.925** | **+0.792** | **+0.910** |
| combined | direct total | **1.715** | **1.280** | **+0.751** | **+0.886** |
| | offense + defense | 1.739 | 1.284 | +0.744 | +0.878 |

The answer differs by model. For **on/off, predict the parts and sum them** — that
is +0.035 R² and a 0.23 RMSE improvement for free, because offense and defense are
both well-determined there and splitting lets each head use different features. For
**box and combined, predict the total directly**: summing inherits the weaker
defense model's error instead of letting the total model route around it.

## Minimum-minutes threshold

`mp_sweep.py` sweeps a minimum-minutes cutoff for training rows. Thresholds are
season-type aware — a whole playoff run is ~300 minutes, so a flat cutoff would
delete the playoff pool rather than clean it.

The conclusion is that **filtering low-minute players does not help.** Above a
token cutoff, accuracy falls monotonically on both validation and test (box):

| RS / PO min | n fit | val RMSE | test R² | test ρ |
|---|---|---|---|---|
| 0 / 0 | 16,956 | 2.219 | +0.660 | +0.820 |
| **50 / 10** | **15,227** | **2.183** | **+0.661** | **+0.823** |
| 250 / 40 | 11,353 | 2.263 | +0.625 | +0.808 |
| 500 / 80 | 8,748 | 2.371 | +0.620 | +0.805 |
| 1000 / 160 | 5,647 | 2.437 | +0.605 | +0.796 |
| 1500 / 250 | 2,903 | 2.407 | +0.558 | +0.754 |

On/off behaves the same way (test R² +0.751 at 50/10, falling to +0.607 at
1500/250). Validation picks 50/10 for both models, which is what `MIN_MP` is set
to: just enough to drop degenerate rows (a 20-minute season carries no signal)
while leaving every validation and test row untouched. The effect on the headline
blend is within noise — box +0.691 → +0.687, on/off +0.760 → +0.757 — so this is
a "confirmed not worth doing" result rather than an improvement.

Two reasons filtering backfires. The test set is *already* high-minutes: 538 only
rated the top ~250 players per historical season, so the regular-season 5th
percentile is 1,197 minutes and a 1,000-minute cutoff removes **zero** test rows.
And low-minute rows are 83% of the training pool, so cutting them trades a large
amount of data for noise the model can already condition on via `ctx|mp` and
`ctx|season_progress`.

The sweep also prints a `kept-test` column, which is a trap worth naming: at
1500/250 the on/off model scores RMSE 3.031 on the 492 surviving test rows — the
best-looking number in the table — while the same model scores 3.974 on the full
696. Filtering the evaluation set makes the task easier, not the model better.

## Reports

| file | what it covers |
|---|---|
| [RESULTS_combined.md](RESULTS_combined.md) | position one-hot × minimum-minutes threshold for the combined model |
| [RESULTS_estimated_raptor.md](RESULTS_estimated_raptor.md) | Neil Paine's Estimated RAPTOR recreated and scored head-to-head |
| [RESULTS_top20.md](RESULTS_top20.md) | top-20 total/offense/defense leaderboards, predicted vs true |
| [RESULTS_top100.md](RESULTS_top100.md) | top-100 leaderboards with precision@K (regular season only) |
| [RESULTS_top20_nofilter.md](RESULTS_top20_nofilter.md), [RESULTS_top100_nofilter.md](RESULTS_top100_nofilter.md) | same, with every minutes filter removed |
| [RESULTS_stride.md](RESULTS_stride.md) | ablation over `--modern-stride`, MPG ≥ 5 training floor |
| [RESULTS_stride_transfer.md](RESULTS_stride_transfer.md) | why the sweep's stride-3 pick does not transfer; stride 6 stays |
| [RESULTS_starters.md](RESULTS_starters.md) | training only on rotation players, near-zero labels dropped |

## Known limitations

- **2018-19 is missing entirely** — there is no `20190715000000` snapshot, so no
  full-season row exists for it.
- Training rows are dominated by modern season-to-date snapshots (fit label
  sd ≈ 11.8 vs test 6.3). Rate normalization absorbs most of this, but the fit
  pool is still not distributionally identical to the test set, and the row
  filters that would narrow the gap all cost more accuracy than they buy.
- Squared loss regresses the extremes: Chris Paul's +11.3 box RAPTOR in 2013-14
  is predicted at +4.8. Rank order holds up far better than magnitude, which is
  why Spearman (~0.83–0.90) is well above R².
- `--modern-stride` was swept in [RESULTS_stride.md](RESULTS_stride.md). **Stride 1
  is the worst setting tested**, not the best: more near-duplicate modern snapshots
  re-weight the loss toward the in-season distribution and away from the
  full-season one the test set is drawn from. The sweep's finer recommendation
  (stride 3) does **not** transfer to the production pipeline — see
  [RESULTS_stride_transfer.md](RESULTS_stride_transfer.md). **Stride 6 stays the
  default.**
- Traded players get one team's row rather than a minutes-weighted combination.
  Validation says this is right ~94% of the time; the remainder is unfixed.
