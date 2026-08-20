# Defensive inaccuracy: four threads, one construction bug, two promotions

Continuation of the defense investigation. Everything below is the branch's
10-fold season-held-out CV, >=1065 pools, dev@10 against published truth.
Baseline: defense hats3 4.15 median / 4.11 mean; offense hats4-both
1.45 / 1.42.

## 0. First, the bug that was masquerading as three results

Every experiment bolting new hats into the stack had been building the
production struct hats with median-imputed predictions on rows whose
structural features are incomplete. Production (`cv_resid_pools.make_hats`)
leaves those hats **NaN** so the GBM routes them down the missing branch.
The difference is nothing on offense and catastrophic on defense — 2016-17
fold: 3.10 (exact) vs 12.70 (imputed). Diagnosed, all harnesses patched to
the exact construction, and the defense boards now reproduce the stored
production CV bit-for-bit (median 4.15 / mean 4.11). Every defense verdict
below was (re)measured with the exact construction; the defense-union
rejection from the previous round was confounded and is re-verdicted below.

## 1. Scrapeable features still available (the missing box-D inputs)

stats.nba.com is **unreachable from the devcontainer** (datacenter block —
requests hang; re-verified). basketball-reference and api.pbpstats.com are
reachable. The three endpoints that carry 538's missing defensive
ingredients are alive on the NBA side and now have a ready scraper,
`scraping/scrape_hustle.py` (mirrors scrape_shotdash conventions, Mongo or
--raw-dir JSONL; **must run from a home connection**):

| endpoint | coverage | what it adds |
|---|---|---|
| leaguehustlestatsplayer | 2015-16+ | deflections, loose balls, charges drawn (real count), contested 2s/3s |
| leaguedashptdefend | 2013-14+ | opponent FGA/FG% at rim / overall / 3pt as closest defender |
| leagueseasonmatchups | 2017-18+ | matchup partial possessions + opponent points while guarding — the closest live stand-in for 538's positional-opponent variables (our posmatch v1/v2 reconstructions were null) |

This is the highest-expected-value defense route left: box_d is the weakest
component (rho .864) precisely for want of these inputs, and the era
analysis below shows the defense label contains real signal a box model
structurally cannot reach.

## 2. Playoff pooling: promoted for offense, rejected for defense

778 labeled playoff rows exist at the same stamps and were unused by the
raptor2 protocol. Pooling them into training (component hats + final GBM;
playoff rows are their own cells so cell-relative context is preserved;
held-out season's playoffs excluded):

| side | pooled | baseline | head-to-head |
|---|---|---|---|
| offense s0 | **1.10 / 1.24** | 1.45 / 1.42 | 5W 1T 4L |
| offense s10 | **1.10 / 1.28** | 1.55 / 1.58 | 7W 1T 2L |
| offense s20 | **1.10 / 1.29** | 1.55 / 1.50 | 5W 1T 4L |
| defense s0 (exact hats) | 4.70 / 5.92 | 4.15 / 4.11 | 1W 1T 8L |

**Offense promotes 3/3 disjoint seed sets** — the second offense promotion
of this cycle, and the production offense config is now: gbm + struct hats +
linear-wide hats + playoff pooling (boards CV: median dev@10 1.20, hits@10
93/100). **Defense rejects pooling** even on the corrected construction:
playoff defensive contexts (scheme-targeting, compressed rotations,
opponent-quality skew) inject training noise that regular-season top-10
ordering pays for. The offense/defense asymmetry is consistent with
everything else on this branch: offensive skill expression transfers across
contexts; defensive results are context entangled.

## 3. Calibration against long-term RAPM

Prior work already settled the strong form: a pooled long-term RAPM-D hat
(recreated from 1.42M possessions) was **exactly null** over 30 paired folds
(18W-12L, 5.91 vs 5.91 — RESULTS_rapm_hat_verdict.md), and rank metrics are
invariant to magnitude recalibration, so post-hoc calibration cannot move
dev@10. The remaining angle — RAPM as *training signal* rather than feature
— was tested through the historical archive (below): a defense hat trained
on 13 seasons of 538's own box+RAPM labels, i.e. their regularized RAPM mix,
carried onto our rows. It also fails (3W-0T-7L). The RAPM family is now
comprehensively closed for the defense stack.

## 4. The historical archive (fivethirtyeight/data nba-raptor)

`hist538/` (committed) + basketball-reference per-100 + advanced tables
1977-2023 (`scrape_bbref_hist.py`, 19,700 player-seasons; bbref ids join the
538 CSVs exactly — 100% match, 15,704 rows at mp>=250).

**E1 — the box-only era (1978-2000) is a solved formula.** Season-held-out,
raw box rates + derived metrics: offense rho .979 / R² .964, defense rho
.959 / R² .927. Our kind of box model reproduces pre-tracking RAPTOR almost
exactly — the "box-only RAPTOR" of the archive is recoverable, so a Rapture
extension to 1977-2000 is essentially free.

**E2 — what the RAPM mix added (2001-2013), i.e. the RAPM regularization.**
The era-1 model transferred: offense rho .966 — single-year RAPM barely
changes offensive ordering. Defense rho .815 (refit on-era: .851, R² .763)
— roughly a quarter of defensive label variance is plus-minus signal a box
model structurally cannot see. Residual variance by possession quintile:
offense flat [.39 .34 .31 .32 .34]; defense humped [.64 .73 .76 .72 .65] —
538's shrinkage gives low-possession players ~box-only ratings, weights
RAPM most in the mid-possession bulk, and high-possession players' RAPM is
precise enough to re-tighten. That is the regularization curve to imitate if
we ever blend a plus-minus term by sample size.

**E3 — year-over-year stability falls as the label gets richer.**

| era | offense r(t,t+1) | defense r(t,t+1) |
|---|---:|---:|
| 1978-2000 box | +0.811 | +0.779 |
| 2001-2013 box+RAPM | +0.783 | +0.700 |
| 2014-2022 tracking | +0.727 | **+0.631** |

Box formulas are stable because box rates are stable. Each measurement era
adds real single-season, non-repeating signal — most of all on defense. The
modern defensive label is substantially season-specific circumstance, which
is the honest reason defense dev@10 has a floor no architecture change has
broken: the components bake in on/off noise that nothing in a season's box
or tracking stats predicts.

**E4 — historical transfer hat: rejected.** Hats trained on 1978-2013
labels (offense rho .886 / defense .602 vs 2014+ labels, aligned to
16,922/16,951 rows, fold-independent by construction) added to the defense
stack: 4.50 / 5.51, 3W-0T-7L vs hats3. Rejected, same pattern as every
defense hat.

**Defense union, re-verdicted (exact hats):** 5.90 / 6.39, **0W-1T-9L** —
the confound is removed and the rejection is now clean: the defense final
model is genuinely made worse by additional hat columns, however good.

## Where this leaves defense

Config unchanged: **defense = gbm + struct hats (hats3), regular season
only.** Rejected this cycle with clean measurement: component-hat upgrades
(swap and union), playoff pooling, long-term RAPM hat, historical box+RAPM
hat. The two live routes, in order: (1) run `scrape_hustle.py` locally and
integrate deflections/matchup data — new *inputs*, which is what box_d
lacks by construction (E2); (2) accept that E3 bounds the metric: ~37% of
year-adjacent defense label variance is non-repeating, so dev@10 gains past
the current 4.15 median will be small and hard-won.

Boards: RESULTS_boards_best.md — offense (pooled hats4-both) median dev@10
1.20, hits@10 93/100; defense (hats3) median 4.15, hits@10 73/100; per-season
top-25 vs actual with rank deviations in the JSONs.
