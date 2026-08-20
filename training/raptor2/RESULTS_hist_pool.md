# Historical rows as training data: real calibration, already subsumed

Answer to "are the 18k historical rows in training?": they weren't — E1-E4
used them for analysis only, and the one training use (E4 defense transfer
hat) was rejected. This cycle tests them as training data proper. All label
data was already local (hist538/ = the fivethirtyeight/data nba-raptor
archive + our bbref 1977-2023 scrape, 100% id join); no external fetch
needed.

## Design (cv_hist_pool.py)

Per fold, a **calibrated hat**: ridge in the shared basketball-reference
feature space (48 cols, >=90% coverage; all-finite training rows; hat NaN
where alignment/features incomplete — exact-hats convention) trained on

- 9,687 player-seasons 1978-2013 with 538's raptor_offense/defense labels
  (box-only era + box+RAPM era; predate every fold), **plus**
- the fold's modern training rows labeled with the rap_box_o / rap_box_d
  component labels (16,162/16,951 modern rows align by name x season).

The pooled fit is what distinguishes this from E4's rejected transfer hat:
36 seasons of extra label mass, calibrated onto the modern component scale.
Added as one hat column to the stack. Arms: defense, offense, and E4's
offense hat (never previously tested).

## Results

hats3 stack, 3 seed sets (10-fold season-held-out, dev@10):

| arm | s0 | s10 | s20 |
|---|---|---|---|
| hats3 offense baseline | 1.60 / 1.69 | — | — |
| **off-cal** | **1.30**, 7W2T1L | **1.30**, 6W2T2L | **1.30**, 8W1T1L |
| off-e4 | 1.55, 7W2T1L | 1.40, 8W1T1L | 1.45, 6W3T1L |
| def-cal | 5.40, 2W2T6L | — | — |

**off-cal promotes 3/3 on the hats3 stack** — median 1.30 vs 1.60 on every
seed set, the strongest offense hat addition ever measured there (and it
dominates the pure-transfer E4 form, so the calibration pooling is doing
the work). Defense rejects it like every added defense hat.

Production-config gate (cv_prod_hist.py: hats4-both + playoff pooling +
cal hat, vs stored production CV):

| seed set | prod+cal | production | head-to-head |
|---|---|---|---|
| s0 | 1.25 / 1.30 | 1.10 / 1.24 | 2W 4T 4L |
| s10 | 1.10 / 1.27 | 1.10 / 1.28 | 3W 5T 2L |
| s20 | 1.25 / 1.32 | 1.10 / 1.29 | 1W 6T 3L |

Null-to-slightly-worse. Whatever the 36 historical seasons teach about the
box formula, the linear-wide hats + playoff pooling already know.

## Verdict

**Production configs unchanged** (offense hats4-both + pooling; defense
hats3). The historical archive's training value is real but conditional:
it lifts a weaker stack (hats3 offense -19% median) and vanishes against
the production one. Worth revisiting if the stack is ever slimmed for
speed, or for a pre-2014 Rapture extension — where E1 showed the same
bbref space reproduces box-era RAPTOR at rho .96-.98 and these pooled fits
would be the backbone.
