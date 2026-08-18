# Per-component bake-off: box_o, box_d, onoff_o, onoff_d each get their own architecture

The four most granular published labels each got an architecture search under
the branch's 10-fold season-held-out CV (train 9 full-season stamp cells, test
the held-out one; sqrt-minutes weights; metrics against the component label:
spearman rho over the full labeled cell, dev@10/tau@10/hits@10 over the
>=1065-minute pool ranked by the true component). `cv_components.py`, seed 0,
tuned side params at rounds//3.

Arms: **ridge-struct** (the production hats3 ridge on the structural block —
baseline), **ridge-wide** (standardized RidgeCV on the wide masked matrix:
box-masked X + shotdash + Z, box_d adds the defend block; on/off gets
onoff-masked X + opponent per-100 + courtmate + onoff2), **gbm-struct**,
**gbm-wide**, **gbm-wide+hat** (wide + struct + OOF ridge-hat column,
stacking), **blend** (0.5 ridge-struct + 0.5 stack), **mlp-struct** (torch,
2 seeds), **gbm-aug** (stack trained with mid-season labeled cells of train
seasons added, downweighted by snapshots-per-season).

## Results (rho med | dev@10 med | dev@10 mean | tau@10 med | hits@10 /100)

### box_o (rap_box_o)

| arm | rho | dev@10 | dev mean | tau@10 | hits |
|---|---:|---:|---:|---:|---:|
| ridge-struct (production) | +0.744 | 6.05 | 8.79 | +0.444 | 67 |
| **ridge-wide** | **+0.962** | **1.30** | **1.25** | **+0.778** | 87 |
| gbm-struct | +0.772 | 4.75 | 5.98 | +0.489 | 70 |
| gbm-wide | +0.933 | 1.80 | 2.03 | +0.689 | 84 |
| gbm-wide+hat | +0.938 | 1.55 | 1.64 | +0.711 | **89** |
| blend | +0.885 | 2.35 | 2.86 | +0.578 | 83 |
| gbm-aug | +0.935 | 1.75 | 1.76 | +0.689 | 84 |
| mlp-struct | +0.765 | 5.10 | 6.88 | +0.578 | 69 |

### box_d (rap_box_d)

| arm | rho | dev@10 | dev mean | tau@10 | hits |
|---|---:|---:|---:|---:|---:|
| ridge-struct (production) | +0.786 | 13.00 | 11.92 | +0.444 | 60 |
| ridge-wide | +0.841 | 9.15 | 10.83 | +0.467 | 64 |
| gbm-struct | +0.782 | 11.85 | 13.70 | +0.378 | 54 |
| gbm-wide | +0.855 | 6.45 | 6.61 | +0.444 | 71 |
| **gbm-wide+hat** | **+0.864** | **6.35** | **6.51** | +0.422 | 72 |
| blend | +0.844 | 8.85 | 8.83 | +0.489 | 68 |
| gbm-aug | +0.869 | 8.10 | 6.94 | +0.400 | **73** |
| mlp-struct | +0.798 | 13.70 | 13.98 | +0.378 | 57 |

### onoff_o (rap_onoff_o)

| arm | rho | dev@10 | dev mean | tau@10 | hits |
|---|---:|---:|---:|---:|---:|
| ridge-struct (production, courtmate chain) | +0.926 | 4.55 | 4.52 | +0.622 | 71 |
| ridge-wide | +0.954 | 2.75 | 2.77 | +0.533 | 80 |
| gbm-struct | +0.917 | 5.30 | 5.34 | +0.400 | 70 |
| gbm-wide | +0.936 | 3.45 | 3.94 | +0.511 | 74 |
| **gbm-wide+hat** | **+0.965** | **2.60** | 2.99 | +0.667 | **81** |
| blend | +0.957 | 3.15 | 3.29 | +0.689 | 77 |
| gbm-aug | +0.960 | 3.15 | 3.54 | +0.622 | 79 |
| mlp-struct | +0.924 | 3.85 | 4.08 | +0.556 | 74 |

### onoff_d (rap_onoff_d)

| arm | rho | dev@10 | dev mean | tau@10 | hits |
|---|---:|---:|---:|---:|---:|
| ridge-struct (production, courtmate chain) | +0.921 | 5.95 | 7.48 | +0.533 | 66 |
| ridge-wide | +0.949 | 3.45 | 5.11 | +0.622 | 75 |
| gbm-struct | +0.898 | 8.05 | 8.00 | +0.444 | 57 |
| **gbm-wide+hat** | **+0.961** | **2.65** | **3.28** | +0.667 | 74 |
| blend | +0.953 | 3.00 | 5.89 | +0.533 | 72 |
| gbm-wide | +0.937 | 5.60 | 7.21 | +0.578 | 66 |
| gbm-aug | +0.960 | 2.95 | 3.07 | +0.667 | **75** |
| mlp-struct | +0.920 | 6.90 | 8.72 | +0.489 | 64 |

## Reading

- **Every production hat is beaten decisively, and the winning architecture
  differs by component** — the premise of the experiment. box_o wants a wide
  *linear* model; the other three want the GBM stack.
- **box_o is (almost) a linear recipe we already carry.** 538's box component
  is itself a linear regression on box/tracking rates, so a standardized ridge
  over 547 raw columns reproduces it at rho .96 / dev@10 1.25 — better than
  any tree model, which has to approximate a hyperplane with axis-aligned
  splits. The 20-variable structural block was never the binding constraint;
  its *width* was (rho .74 → .96).
- **box_d remains the weakest label** (best rho .864). The methodology's two
  positional-matchup variables and its enhanced-rebound weighting are the
  missing inputs; posmatch v1/v2 already came up null on this branch.
- **On/off components reward stacking**: wide GBM + courtmate-chain block +
  its ridge hat as a feature. The chain features stay the single most
  important input (gbm-wide without them: rho .936/.937) but nonlinear
  residual learning on top is worth +.025-.03 rho and ~2 dev@10 points.
- **Mid-season augmentation (gbm-aug) is real but small** — best hits@10 on
  box_d/onoff_d, otherwise within noise of the stack. Worth keeping for
  defense-side hats.
- **mlp-struct ≈ ridge-struct everywhere**: on 3-20 curated inputs there is
  nothing nonlinear to find; the information is in the wide matrices.

## Cross-arm blends (from saved fold predictions)

z-scored averages of fold predictions (`cv_components_preds.npz`):

| comp | best single | best blend | blend numbers |
|---|---|---|---|
| box_o | ridge-wide 1.25 dev mean | rank-avg(rw, stack) | dev med 1.20, rho mean +0.942 |
| box_d | stack 6.51 dev mean | none better | avg3 rho +0.849 but dev 8.21 |
| onoff_o | stack 2.99 dev mean | **avg(rw, aug)** | **dev 2.40 med / 2.52 mean, hits 82** |
| onoff_d | stack 3.28 dev mean | **avg3(rw, stack, aug)** | **dev 2.60 med / 2.78 mean, hits 76** |

## Stack integration (cv_hats4.py) — the "so what"

Swapping/adding hats inside the production gbm+hats stack, cv_resid_pools
protocol (blend, 3 seeds, rounds//3), baseline = stored hats3 CV:

| variant | offense med/mean | H2H | defense med/mean | H2H |
|---|---|---|---|---|
| hats3 (production) | 1.60 / 1.69 | — | 4.15 / 4.11 | — |
| hats4-linear (swap: 4 ridge-wide hats) | 1.60 / 1.54 | 5W2T3L | 5.45 / 6.32 | 2W0T8L |
| hats4-winner (swap: rw + 3 OOF GBM stacks) | 1.60 / 1.62 | 3W4T3L | 5.05 / 6.20 | 2W2T6L |
| **hats4-both (union: struct hats + 4 ridge-wide hats)** | **1.45 / 1.42** | **6W2T2L** | 5.25 / 6.22 | 1W1T8L |

**Offense promotion gate — PASSED 3/3 disjoint seed sets** (hats4-both vs
in-protocol hats3, per-season head-to-head):

| seed set | hats3 med/mean | hats4-both med/mean | H2H |
|---|---|---|---|
| s0 | 1.60 / 1.69 | 1.45 / 1.42 | 6W 2T 2L |
| s10 | 1.55 / 1.70 | 1.55 / 1.58 | 6W 1T 3L |
| s20 | 1.65 / 1.70 | 1.55 / 1.50 | 8W 0T 2L |

**Defense rejects every hat upgrade**, including the pure union — adding
better-rho hat columns makes its dev@10 *worse* (blow-up folds 2015-16,
2022-23). Two mechanisms fit the evidence: (1) the defense top-10 is already
noise-limited, so extra correlated features reshuffle tree structure without
adding orderable signal; (2) for OOF-stacked hats, train rows carry OOF-model
values while test rows carry full-fit values, and the final GBM's split
thresholds don't transfer across that distribution shift (the in-sample
struct ridge hats never had this problem). The defense stack keeps hats3.

## Verdict

- **Promote for offense**: final offense model = gbm + struct hats + four
  ridge-wide component hats (hats4-both). Boards should be regenerated with
  this configuration.
- **Keep hats3 for defense.** Better defense components do not currently
  transfer; the binding constraint is the final model's noise floor, not
  component quality. The route to better defense boards is new *inputs*
  (positional matchups, enhanced rebounds), not better component fits.
- Best standalone component models (for component leaderboards or a future
  structural mode): box_o ridge-wide; box_d gbm-wide+hat; onoff_o
  avg(ridge-wide, gbm-aug); onoff_d avg(ridge-wide, stack, gbm-aug).
