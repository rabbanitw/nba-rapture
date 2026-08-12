# Defensive RAPM-hat: final verdict after three disjoint-seed CV replications

The hat = out-of-fold prediction of pooled long-term RAPM-D (recreated per the
Davis tutorial from 1.42M possessions; per-fold pools refit excluding the
held-out season) appended as a feature to the production defense model
(matched-training + defend, 3-seed members + ridge).

Test seasons looked promising (3.60 vs 3.80 on the production stack). Ten-fold
season-held-out CV, three disjoint seed sets:

| seed set | base median / mean | hat median / mean | head-to-head | hits@10 |
|---|---|---|---|---|
| 0-2   | 5.95 / 6.15 | **5.65 / 5.41** | **8W-0T-2L** | 69 vs 69 |
| 10-12 | 5.95 / 5.92 | 5.75 / 5.79 | 5W-0T-5L | 67 vs 66 |
| 20-22 | **5.80 / 5.65** | 6.20 / 6.52 | 5W-0T-5L | 69 vs 66 |

Pooled over 30 paired folds: 18W-12L, mean dev@10 **5.91 vs 5.91 — exactly
even**. The first run's 8-0-2 was seed luck; seed-to-seed variance dwarfs any
effect.

**Verdict: NOT promoted.** The production defense model stands unchanged.
Offense was rejected earlier without needing replication (hat degrades the
components stack: 1.10 -> 1.35-1.95).

Artifacts: RESULTS_loso_rapm_hat{,_rep,_rep2}.json,
RESULTS_rapm_calib_production.json, data_fixed/rapm_recreated.npz.
