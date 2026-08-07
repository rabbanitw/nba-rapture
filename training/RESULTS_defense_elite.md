# Forensic study: the true top-30 defenders — what identifies them, what orders them

Three rounds (`study_defense_elite.py`, `study_defense_elite2.py`,
`study_defense_elite3.py`, `loso_elite_weight.py`), run over the ten labeled
whole-season cells, ≥1065-minute pools, with permutation nulls and 10-fold LOSO
adjudication of every intervention the findings licensed.

## 1. Who the true elite are (in 538's labels)

**Position.** The true top-30 skews heavily toward bigs in every season: 53–77%
C/PF vs 39–50% in the eligible pool. "Elite defender" is structurally a
rim-protector category in these labels.

**Label self-consistency.** Year-over-year Spearman of `raptor_defense` for
repeat players: **+0.641** full pool, but only **+0.422 within the prior-year
elite**. The label itself reorders its own top band nearly half-way each season.
Any model's within-elite ordering is chasing a target with limited
self-consistency.

## 2. Feature relevance (empirical, per full feature profile)

**Membership** (AUC separating true top-30 from pool, mean of 10 seasons, all
sign-consistent 10/10):

| feature | direction | AUC strength |
|---|---|---:|
| wowy_diff\|OpponentPoints | lower = elite | 0.81 |
| pbp\|OnDefRtg (+ cell-relative) | lower = elite | 0.80 |
| defend\|d2_value36 (rim 1.05·miss−0.33·make /36) | higher = elite | 0.78 |
| defend\|d2_pct_pm (defended 2pt% vs expected) | lower = elite | 0.78 |
| pbp\|Blocks / RecoveredBlocks / Blocked2s | higher = elite | 0.75–0.76 |
| track:defensive-impact\|DFG% | lower = elite | 0.74 |
| defend\|rim_pct_pm | lower = elite | 0.74 |

**Ordering within the true top-30** — the question that matters for dev@10, and
a genuinely different one. Permutation null (400 perms, rank-matmul, 1,135
usable features): family-wise 95% threshold 0.229. **53 features beat it; 261
beat the per-feature null (~28 expected by chance). The signal is real but
modest** — the best single feature reaches |rho| ≈ 0.40:

| feature | rho within elite |
|---|---:|
| pbp\|OpponentPoints | −0.401 |
| pbp\|OnDefRtg (+ cellrel) | −0.398 |
| defend\|d2_pct_pm | −0.386 |
| wowy_on\|OpponentPoints | −0.376 |
| defend\|d2_value36 | +0.360 |
| wowy_diff\|OpponentPoints | −0.346 |
| wowy_on\|DefRebounds / DefTwoPtRebounds | +0.33–0.34 |
| pbp\|PlusMinus | +0.311 |

**The structural finding:** membership and ordering are different problems.
Blocks and DFG% identify *who is elite* but fall away for *ordering* the elite;
ordering is carried by on-court opponent scoring, rim-FG% suppression, and
defensive-rebounding presence — i.e., team-defense-on-court + nearest-defender
rim data.

**Model-level permutation importance on elite ordering** (production model,
groups permuted at test time, Δ vs base tau@30 +0.538 / dev@20 6.97):

| group permuted | cols | Δ tau@30 | Δ dev@20 |
|---|---:|---:|---:|
| defend-engineered | 8 | **−0.223** | **+12.1** |
| pbp-defensive | 53 | −0.156 | +15.3 |
| wowy (all) | 681 | −0.107 | +10.3 |
| cell-relative | 12 | −0.067 | +3.7 |
| pbp-offensive/rest | 184 | −0.049 | +3.0 |
| track-defense | 22 | **+0.005** | +0.8 |

The 8 engineered `defend|` columns are by far the most load-bearing per column.
The 22 raw tracking-defense columns contribute **nothing** to elite ordering —
their information arrives via the engineered defend features instead.

## 3. Interventions tested — all lost to the base model

| arm | dev@10 (test cells) | verdict |
|---|---:|---|
| base (matched+defend blend) | 3.80 | production |
| top-15/40/100 ordering-feature models | 15.75 / 10.95 / 9.85 | rejected — full profile required |
| union-60 (membership ∪ ordering feats) | 12.10 | rejected |
| two-stage elite specialist re-rank | 5.95 | rejected |
| two-stage rank-average | 4.85 | rejected |
| elite-weighted loss sigmoid ×3 | 3.70 | **LOSO-rejected**: median 6.10 vs 5.95, 3W-6L |
| elite-weighted loss sigmoid ×8 | 4.00 | LOSO-rejected: mean 7.74, one 24.2 blowup |

The sigmoid×3 arm illustrates exactly why LOSO is the only trusted selector
here: it looked better on the two test cells and lost 6 of 10 LOSO folds.

## 4. The ceiling diagnostic

The base model's Spearman **within the true top-30** on the test cells:
**+0.586 (2013-14), +0.828 (2014-15)** — above the best single feature (0.40)
and above the label's own year-over-year self-consistency (0.42). The model is
already extracting more within-elite ordering than any individual feature
carries, against a label whose elite band is itself substantially reshuffled
season to season (538's own defensive component fit six-year RAPM at R² ≈ 0.6).

## 5. Conclusions for model type and loss

1. **Model type: keep the full-profile GBM blend.** Every restriction
   (feature pruning, specialist staging) and every loss reweighting lost.
   Membership and ordering need different features, and the GBM already
   consumes both sets jointly — splitting them out empirically hurts.
2. **Loss: uniform.** Elite-emphasis reweighting trades global shape for tail
   fit and loses on LOSO. The remaining gap is not a loss-shape problem.
3. **The one under-exploited direction is data, not architecture:** the raw
   tracking-defense block is dead weight for elite ordering while the 8
   engineered defend columns are the most valuable block in the model —
   engineering more nearest-defender-style features (and acquiring
   matchup-level data we don't yet scrape) is where the evidence says
   headroom lives, if any.
