# nba-rapture

A revival of FiveThirtyEight's discontinued [RAPTOR player rating](https://projects.fivethirtyeight.com/nba-player-ratings/):
a data platform that collects the public inputs RAPTOR was built from, a
feature matrix aligned to the published RAPTOR labels, and a set of model
architectures trained to reproduce and project the ratings.

Repository layout:

```
scraping/    collectors (pbpstats.com + stats.nba.com -> MongoDB), rosters, migrations
training/    dataset build, labels, models, experiments, validation, reports
training/raptor2/   structural reproduction of the published RAPTOR methodology
training/data_fixed/  row-aligned feature/label artifacts (npz)
training/rapm_public/ third-party single-season RAPM (MIT, basketball-analytics)
```

Start with [`RAPTOR_REPRODUCTION.md`](RAPTOR_REPRODUCTION.md), the canonical
methodology, architecture-selection, hyperparameter, and Neil Paine comparison
report. Supporting references are `training/raptor_methodology_fulltext.txt`
(recovered methodology explainer), `training/RESULTS_raptor_coverage.md`
(section-by-section mapping), and the `training/RESULTS_*.md/json` files produced
by individual experiments.

### Current validated architecture

Ten-season leave-one-season-out validation selects separate heads: offense is a
three-seed LightGBM/Ridge blend over the full matrix plus opponent context and
four structural RAPTOR hats. Defense is a 60/40 ensemble of the matched
LightGBM/Ridge head and a second head augmented by defensive box/on-off hats and
their published 0.85/0.21 combination. Total is offense + defense. On 2,238
1,065-minute player-seasons common to Neil Paine's published file, the stack
scores RMSE **1.051**, R² **0.854**, and Spearman **0.913**, versus Paine's
Estimated RAPTOR at 1.299 / 0.777 / 0.862. See
`training/RESULTS_final_architecture.md` for the canonical table and confidence
intervals.

---

## 1. Data platform

### Cells and snapshots

Every collected table is keyed by **cell** = (timestamp, season type). A
timestamp is a whole-season stamp (e.g. `20220715000000` for 2021-22) or an
in-season snapshot date. Coverage: 13 seasons (2013-14 through 2025-26),
regular season and playoffs, at whole-season stamps, plus 38 in-season
snapshot cells (`scraping/defend_snapshots.json`) for sources added late.
`scraping/season_dates.py` holds the season calendar (`SNAPSHOTS`,
`KNOWN_BOUNDS`, `cells()`).

### Sources (MongoDB, one document per player per cell per table)

| source | endpoint | contents |
|---|---|---|
| `pbp` | pbpstats.com totals | full box/play-by-play derived stats (~340 fields): zone shooting (AtRim/ShortMidRange/LongMidRange/Corner3/Arc3 FGA/FGM/accuracy/assisted%/blocked%), assists by zone, turnover taxonomy (bad-pass/lost-ball/travels/step-OOB/dead-ball/live-ball), foul taxonomy (shooting/non-shooting/charge/loose-ball/penalty variants, drawn and committed), rebounds (incl. self-OREB, putbacks), on-court ratings, penalty/second-chance splits, usage, heaves |
| `wowy` | pbpstats get-wowy-stats | the same stat family split by player **on** vs **off** court |
| `wowy-opp` | get-wowy-stats `Type=Opponent` | opponent stat profile while the player is on vs off court |
| `nba-tracking` | stats.nba.com tracking tables | passing (incl. PASSES MADE/RECEIVED, FT_AST, potential AST), drives, touches (elbow/paint/post), catch-shoot, pull-up, speed-distance (off/def miles and speeds), rebounding (contested/deferred chances), defensive impact |
| `nba-defend` | leaguedashptdefend | nearest-defender defended shots, 6 categories (overall/2pt/3pt/<6ft/<10ft/>15ft), verbatim API columns |
| `nba-shotdash` | leaguedashplayerptshot + leaguedashptstats | own shots split by closest-defender distance (0-2/2-4/4-6/6+ ft: FGA/FGM/FG3A/FG3M/frequencies) and the possessions table (TIME_OF_POSS, AVG_SEC_PER_TOUCH, AVG_DRIB_PER_TOUCH, touches) |

Upsert keys per source are defined in `scraping/mongo_sink.py::KEY_FIELDS`
(timestamp, season type, standard name, plus `on_or_off` or `data_type` where
applicable). Player identity is resolved by **ID join** against roster files
(`scraping/rosters/roster_<ts>_<split>.json`, produced by
`scrape_pbp_totals.py`), never by fuzzy name.

### Scraping scripts

| script | role |
|---|---|
| `pbpstats_client.py` | retrying pbpstats HTTP client (0.35 s pacing, jittered backoff, transient-error taxonomy) |
| `scrape_pbp_totals.py` | `pbp` source; merges league-wide (500-row-capped) and per-team queries; writes roster files |
| `scrape_wowy.py` | `wowy` and (with `--opponent`) `wowy-opp`; per-cell isolation, resume via existing-key scan |
| `scrape_nba_tracking.py` | `nba-tracking`; FIELD_MAP schema with legacy column handling |
| `scrape_defend.py` | `nba-defend`; 6 DefenseCategory tables per cell; `--snapshots` mode |
| `scrape_shotdash.py` | `nba-shotdash`; 4 defender-distance tables + possessions; `--snapshots` mode |
| `season_dates.py` | season calendar and cell enumeration |
| `mongo_sink.py` | Mongo connection, upsert writer, `RawSink` JSONL fallback (`--raw-dir`) |
| `load_raw.py` | loads RawSink JSONL files into Mongo |
| `verify_scrape.py` | per-cell row-count/coverage verification |
| `purge_cell.py` | deletes a cell for re-scrape |
| `migrate_tracking_v2.py`, `migrate_ft_ast.py`, `migrate_label_season.py` | one-time schema/label migrations |
| `wb_scrape.py`, `pbp_scrape.py`, `wowy_scrape.py`, `nba_tracking_scrape.py`, `data_saver.py`, `database.py`, `fuzzydict.py`, `nba_teams.py` | phase-1/legacy collectors and helpers |

Operational notes: stats.nba.com endpoints refuse datacenter IPs and are run
from a residential connection; pbpstats endpoints run from anywhere.
Credentials: `credentials.txt` at repo root (gitignored), username line 1,
password line 2, or `MONGO_URI` env var.

---

## 2. Feature matrix and artifacts

`training/build_dataset.py` assembles `training/data_fixed/combined.npz`:
**16,951 rows** (player × cell), **909 base columns**, with labels and row
metadata (player, timestamp, season type, season, minutes, position). Rows are
label-aligned via `label_season` stamping (`training/labels.py`,
`training/seasons.py`).

Feature blocks as consumed by models (`experiment_combined.prepare()` and
per-experiment extensions):

| block | columns | construction |
|---|---|---|
| `pbp\|*` | ~340 | pbpstats totals; 306 columns rate-normalized per possession |
| `wowy_on\|*`, `wowy_off\|*` | ~2×220 | on/off splits |
| `wowy_diff\|*` | 227 | on-minus-off differentials, added in `prepare()` |
| `track:*` | ~130 | tracking tables, verbatim columns |
| `ctx\|*` | 8 | position dummies (PG/SG/SF/PF/C), minutes, season progress, playoff flag |
| `cellrel` (Z) | 12 | cell-relative z-scores of selected columns (`experiment_components.RELATIVE_COLS`) |
| defend `E` | 8 | engineered nearest-defender features (`experiment_defend.py`): `d2_value36` = (1.05·miss − 0.33·make)/36min, `d2_pct_pm`, `rim_pct_pm`, `d3a36`, `dfga36`, category plus-minus rates |
| defend `F` | 48 | raw defend category columns |
| `Eopp` | ~20 | engineered opponent-WOWY features (`experiment_oppdef.engineered`): luck-adjusted opponent ratings, on/off opponent profile deltas |
| `Bopp` | ~690 | opponent-WOWY per-100 on / off / diff blocks (`per100`) |
| shotdash `R` | 75 | raw defender-distance + possessions columns (`sd:*`) |
| shotdash `E` | 9 | engineered: `covered3pa` (1.00/0.80/0.57/0.31 weights), covered share, tight/wide-open 3 shares, wide-open 3P%, time-of-poss per 36, sec/touch, dribbles/touch, touches per 36 |
| posmatch `PM` | 3 | probabilistic positional-matchup accumulations from possession lineups: `posopp_pts100`, `posopp_oreb100`, `posopp_dreb100` (2013-19 window) |
| structural `OB/DB/OO` | 20/12/8 | the methodology's regression variables (see §4) |

Identifier columns (`pbp|EntityId`, `pbp|RowId`, `pbp|TeamId`) are excluded
from all models (`predict_seasons.DROP_FEATURES`).

Artifacts in `training/data_fixed/`:

| file | contents |
|---|---|
| `combined.npz` | base matrix + labels + row metadata |
| `components.npz` | 538's component labels row-aligned: `rap_box_o/d`, `rap_onoff_o/d`, `rap_o/d` |
| `defend.npz` | defend `E` (8) and `F` (48) blocks |
| `wowyopp.npz` | opponent-WOWY raw on/off arrays (229 fields) |
| `shotdash.npz` | shotdash `R` (75) and `E` (9) blocks |
| `rapm_recreated.npz` | pooled ridge RAPM (offense/defense) 2013-19 and 2015-19, per player, with possession counts |
| `nas_direct_preds.npz`, `nas_pairwise2_preds.npz` | row-aligned neural predictions |
| `../pairwise_gbm_{offense,defense}.npy` | row-aligned pairwise-GBM tournament scores (48k pairs/cell run) |
| `../raptor2/structural_vars.npz`, `structural2_hats.npz`, `posmatch.npz` | structural variables, component hats, positional-matchup variables |

`training/rapm_public/` holds per-season O/D RAPM JSON (1996-97 to 2018-19,
RS + playoffs) from gitlab.com/basketball-analytics/rapm-data (MIT).

### Labels and splits

Labels: FiveThirtyEight full-history RAPTOR, separate `raptor_offense` and
`raptor_defense` per player-season (plus the box/on-off component labels).
Ten labeled whole-season cells: 2013-14 through 2022-23.

Split convention used by every experiment: **test** = 2013-14 and 2014-15
regular-season cells; **train** = remaining labeled rows (≥50 min RS / ≥10 min
PO); four whole-season **validation** cells (2015-16, 2016-17, 2017-18,
2019-20) where an experiment needs internal selection. Cross-validation =
ten-fold season-held-out (train on nine labeled seasons, score the tenth).
Leaderboard eligibility floor: 1,065 minutes.

Metrics (`experiment_topk_rank.score_cells`): dev@k (mean absolute true-rank
error of the projected top-k), Kendall tau@k, hits@k (top-k set overlap), MAE,
Spearman.

---

## 3. Model architectures tested

Descriptions only; per-experiment outcomes are in the referenced
`RESULTS_*` files.

**Gradient-boosted family**
- *Direct blend* (`experiment_combined.py`, `experiment_oppdef.blend`):
  per-target LightGBM (tuned via `tune_lgbm.py` → `tuned_params.json`),
  seed-averaged ×3, blended 0.75/0.25 with a standardized RidgeCV on the same
  features.
- *Components architecture* (`experiment_components.py`): separate box model
  (box-mask features → `rap_box_*` label) and on-off model (WOWY-mask →
  `rap_onoff_*` label), combined by a minutes-aware ridge on
  `combiner_design` (box, on-off, log-minutes z, interactions); opponent-WOWY
  ensemble members variant (`experiment_comp_opp.py`, `final_boards.py`).
- *Matched-regime defense* (`experiment_defend.py`): training restricted to
  whole-season rows; defend blocks appended; full-matrix and matched-training
  variants; CatBoost comparison.
- *Random forest / extra-trees baselines* (`benchmark_models.py`).

**Loss / label / weighting variants**
- Huber and L2 objectives per target (`tuned_params.json`).
- Sign-penalty and asymmetric weighting for positive-RAPTOR rows
  (`experiment_arch_weight.py`, `weight_seed_study.py`).
- Elite-emphasis sample weights, sigmoid and hard-threshold
  (`study_defense_elite3.py`, `loso_elite_weight.py`).
- Rank-transformed and percentile labels, lambdarank, top-slice training
  (`experiment_ranking.py`, `experiment_labels.py`, `experiment_topk_rank.py`,
  `experiment_trainonly.py`).
- Stability-target feature selection: features ranked by GBM fits against
  adjacent-season labels, top-K restriction arms
  (`experiment_stability_select.py`).

**Pairwise (player-vs-player) family**
- *Pairwise GBM* (`experiment_pairwise.py`): LightGBM binary classifier on
  within-cell feature differences (ties |Δy| < 0.05 dropped), round-robin
  tournament aggregation with antisymmetrized win probabilities; pair-budget
  scaling 6k-48k pairs/cell.
- *All-pairs RankNet* (`pairwise_gpu.py`): MLP scorer s(x), BCE on score
  differences, streamed all within-cell pairs, GPU.
- *Pairwise NAS rounds 1-2* (`nas_pairwise.py`, `nas_pairwise2.py`):
  successive-halving search over three formulations (Bradley-Terry scorer,
  antisymmetrized difference net, two-tower interaction encoder), residual
  trunks, schedules; solo winner and top-3 ensemble evaluation.

**Neural direct-rating family**
- Fixed MLP regressors and Siamese variants (`siamese_model.py`,
  `siamese_total.py`, `experiment_explore.py`).
- *Direct-rating NAS* (`nas_direct.py`): successive-halving over four
  families (plain MLP, residual MLP, self-normalizing SELU, bottleneck
  compressor) × width/depth/activation/norm/dropout/lr/wd/batch/schedule,
  minute-weighted smooth-L1, seed-averaged winner.

**RAPM calibration family**
- Recreated pooled long-term RAPM (`build_rapm.py`): possession parsing per
  the rd11490 tutorial over 2013-19 play-by-play with reconstructed lineups;
  ridge on ±1 offense/defense indicators; 6-year and 4-year pools.
- Auxiliary "rapm-hat" feature: GBM mapping features → pooled RAPM,
  player-grouped OOF, appended to direct and production stacks
  (`experiment_rapm_calibration.py`, `experiment_rapm_calib_production.py`,
  `loso_rapm_hat*.py`).

**Structural reproduction and hybrids** (`training/raptor2/`)
- *Structural model* (`variables.py`, `variables2.py`, `structural.py`,
  `structural2.py`): the methodology's ~32 regression variables with its
  published constants (shot-EV table, covered-3PA weights, iso-turnover
  classes, fast-break credits, defended-shot values), cell-relative
  minute-weighted standardization, four component ridges fit on the component
  labels, fixed 0.85/0.21 and learned blends; v2 pre-adjusts on-off ratings
  for 3-point luck and competition.
- *Hybrids* (`hybrid.py`, `cv_hybrid.py`, `cv_bar.py`): structural hat + GBM
  on residual; full matrix + four component hats as features; season-held-out
  CV with per-fold hat refits and disjoint-seed replications.
- *Positional matchups* (`parse_attrib.py`, `posmatch.py`): possession
  re-parse with scorer/rebounder attribution; events distributed over the five
  opponents by position-vector overlap; per-player-season matchup variables.
- *Courtmate chain* (`courtmate_chain.py`): the methodology's own-on,
  courtmates-without, and courtmates'-courtmates ratings from possession
  lineups, with the published shared×apart weighting.
- *Post-processing* (`postprocess.py`): exact 0.85/0.21 blend, score-effect
  coefficients, team reconciliation constraint, replacement level, and WAR
  formula. Inputs whose fitted coefficients were never published remain
  explicit rather than guessed.

**Validation and reporting machinery**
- Season-held-out CV harnesses (`season_cv.py`, `loso_confidence.py`,
  experiment-specific `loso_*.py` / `cv_*.py`), disjoint-seed replication
  protocol.
- Rank confidence intervals (`confidence_report.py`): seed-member spread +
  leave-fold-out residual bootstrap in minutes buckets, 2,000 Monte-Carlo
  re-rankings → 90% rank CIs and P(top-k).
- Projection boards (`predict_seasons.py`, `final_boards.py`,
  `leaderboards.py`, `leaderboard_*.py`): unlabeled 2023-26 matrix build,
  position carry-over/imputation, eligibility filtering, board generation
  with CIs and pairwise win%.
- External baseline (`estimated_raptor.py`, `compare_estimated_raptor.py`):
  Neil Paine's Estimated RAPTOR recreated and scored on shared pools.
- NAS report (`build_nas_report_data.py`, `gen_nas_report.py` →
  `report_nas/nas_report.pdf`): LaTeX/PDF write-up of the NAS campaigns.
- Studies: defensive elite forensics (`study_defense_elite*.py`), stat
  polarity classification (`stat_polarity.py`), stride/feature ablations
  (`stride_ablation.py`, `experiment_defense2.py`,
  `experiment_final_explore.py`), minutes-floor sweep (`mp_sweep.py`),
  schema/coverage surveys (`discover_schema.py`, `coverage.py`).

---

## 4. Typical pipeline

```bash
# 1. collect (stats.nba.com steps on a residential connection)
python scraping/scrape_pbp_totals.py
python scraping/scrape_wowy.py           # and --opponent
python scraping/scrape_nba_tracking.py
python scraping/scrape_defend.py          # and --snapshots
python scraping/scrape_shotdash.py        # and --snapshots
python scraping/verify_scrape.py

# 2. build
python training/build_dataset.py --model all
python training/extract_components.py
python training/extract_wowyopp.py
python training/experiment_defend.py      # writes defend.npz
python training/extract_shotdash.py

# 3. model / evaluate (examples)
python training/experiment_combined.py
python training/loso_confidence.py
python training/final_boards.py

# canonical architecture + Neil Paine comparison (10-season LOSO)
python training/benchmark_final_architecture.py --paine-repo C:/tmp/NBA-elo

# defense-first ranking, component, and exact published-loss research
python training/experiment_defense_deep.py --stage screen
python training/experiment_defense_deep.py --stage confirm
python training/experiment_defense_deep.py --stage loss
python training/select_defense_architecture.py

# 4. structural reproduction branch
python training/raptor2/structural2.py
python training/raptor2/cv_hybrid.py --seedbase 0

# deterministic formula, courtmate-chain, and identity-join tests
python -m unittest discover -s training -p "test_*.py" -v
```

Environment: Python 3.12; lightgbm, xgboost, catboost, scikit-learn, torch
(CPU in-container; CUDA 12.8 wheels for GPU scripts), pymongo, pandas, scipy.
The canonical CPU environment is pinned in `requirements.txt`;
`training/db.py` provides the Mongo connection and `REPO_ROOT`.
