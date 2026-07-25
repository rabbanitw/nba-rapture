# RAPTOR prediction pipeline

Predicts FiveThirtyEight RAPTOR totals from the scraped stats in MongoDB.
Seasons **2013-14 and 2014-15 are held out as test** and are scored exactly once.

```
python training/discover_schema.py     # survey the collection
python training/coverage.py            # measure per-field population rates
python training/build_dataset.py       # -> training/data/{box,onoff}.npz
python training/train_rapture.py       # -> training/models/
```

Credentials go in `credentials.txt` at the repo root (username line 1, password
line 2). It is gitignored.

## Architecture

Two independent models, because that is how 538 defines the metric. Per their
[data dictionary](https://github.com/fivethirtyeight/data/tree/master/nba-raptor),
`raptor_box_*` is "based only on box score estimate" and `raptor_onoff_*` is
"based only on plus-minus data", both in **points above average per 100 possessions**.

| model   | features                                   | label       |
|---------|--------------------------------------------|-------------|
| `box`   | `pbp` + the 14 `nba-tracking` blocks       | `rap_box`   |
| `onoff` | `wowy` on, `wowy` off, and **on − off**    | `rap_onoff` |

RAPTOR is *Robust Algorithm using Player Tracking and On/Off Ratings*, so the
source→target mapping above is the one the metric was built on. Feeding on/off
data to the box model (or vice versa) would predict a component from inputs 538
did not use for it.

**LightGBM** is the primary learner, with a Ridge baseline and a 75/25 blend.
This is tabular data — ~450 heterogeneous columns, a few thousand independent
player-seasons, heavy skew, and genuine missingness that LightGBM splits on
natively. Gradient-boosted trees are the right default there, and the gain
rankings are worth having for a metric-revival project. The blend wins on both
targets because Ridge and LightGBM make uncorrelated errors.

Offense and defense are not modelled separately: `rap_box`/`rap_onoff` are the
totals, and per the scrape they are not exactly `o + d` anyway (see *Label quirk*).

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

That yields **454 stat fields per model** (box: 240 pbp + 214 tracking; on/off:
227 on + 227 off), plus position one-hot, minutes, season progress, a playoff
flag, and — for on/off — 227 `on − off` differentials.

**Counting stats are normalized to rates.** RAPTOR is per-100-possessions, but the
scraped values are season-to-date totals, so a January snapshot and an April
snapshot of the same player look completely different while carrying nearly the
same label. Each block has its own denominator (`TotalPoss` for pbp/wowy, `MIN`
for tracking); a column is divided by it when it is non-negative and correlates
≥0.75 with it, unless its name says it is already a rate. 217/454 columns get
normalized; the classification is written to `models/*_normalization.json`.

## Results (held-out 2013-14 + 2014-15, n≈696 each)

| model | | RMSE | MAE | R² | Spearman |
|---|---|---|---|---|---|
| **box** | mean baseline | 3.462 | 2.627 | −0.229 | — |
| | Ridge | 2.048 | 1.569 | +0.570 | +0.764 |
| | LightGBM | 1.821 | 1.354 | +0.660 | +0.820 |
| | **blend** | **1.734** | **1.282** | **+0.691** | **+0.833** |
| **onoff** | mean baseline | 6.550 | 4.598 | −0.068 | — |
| | Ridge | 3.254 | 2.289 | +0.736 | +0.854 |
| | LightGBM | 3.243 | 2.111 | +0.738 | +0.884 |
| | **blend** | **3.104** | **2.047** | **+0.760** | **+0.895** |

By slice (LightGBM). Playoff rows are harder — small samples make the label itself
noisy, and on/off RMSE there is ~3× the regular-season figure:

| slice | n | box R² | box ρ | onoff R² | onoff ρ |
|---|---|---|---|---|---|
| 2013-14 Regular season | 248 | +0.715 | +0.846 | +0.803 | +0.907 |
| 2013-14 Playoffs | 100 | +0.516 | +0.709 | +0.754 | +0.900 |
| 2014-15 Regular season | 248 | +0.721 | +0.836 | +0.767 | +0.883 |
| 2014-15 Playoffs | 100 | +0.617 | +0.845 | +0.693 | +0.846 |

Top features are the ones the metric is actually about: box leans on `pbp|EfgPct`,
`pbp|OnOffRtg`, `pbp|PlusMinus`, `pbp|TsPct`; on/off leans on `wowy_diff|Points`,
`wowy_on|PlusMinus`, `wowy_diff|OpponentPoints`. Predicted 2013-14 leaders are
Curry / Durant / LeBron / Harden for box and Iguodala / Thompson / Korver /
Ginóbili for on/off — the right names for that era.

`ctx|mp` ranks high but is not leakage; it comes from the label document but is
minutes played, not a RAPTOR value, and is near-redundant with `pbp|Minutes`.
Dropping it moves box R² 0.660 → 0.639 and on/off 0.738 → 0.741.

## Known limitations

- **2018-19 is missing entirely** — there is no `20190715000000` snapshot, so no
  full-season row exists for it.
- Training rows are dominated by modern season-to-date snapshots (fit label
  sd ≈ 11.8 vs test 6.3). Rate normalization plus the `progress>=0.5` filter
  (selected on validation for on/off) absorbs most of this, but the fit pool is
  still not distributionally identical to the test set.
- Squared loss regresses the extremes: Chris Paul's +11.3 box RAPTOR in 2013-14
  is predicted at +4.8. Rank order holds up far better than magnitude, which is
  why Spearman (~0.83–0.90) is well above R².
- `--modern-stride 6` keeps every 6th modern snapshot. Those snapshots are near-daily
  and highly redundant, but nothing verifies that stride 1 would not help.
- Traded players get one team's row rather than a minutes-weighted combination.
  Validation says this is right ~94% of the time; the remainder is unfixed.
