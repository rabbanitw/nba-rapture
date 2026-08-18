# Player-v-player classification, round 2: both players' features, all ordered pairs, eight targets

Spec: submit BOTH players' feature vectors, predict who has the higher
published value (win probability), train on ALL ordered pairs within each
training cell — both orientations of every pair, so label balance is exactly
0.5 and no "higher player on top" shortcut exists. Ties |dy| < 0.05 dropped.
690k-953k training pairs per fold. 10-fold season-held-out (branch standard),
full round-robin tournament scoring on the held-out cell with antisymmetrized
win probabilities; a player's score = mean win probability against the field.

Eight targets, each its own model on its structurally-matched per-player
block (+ minutes): off (rap_o), def (rap_d), box (rap_box_o + rap_box_d),
onoff (rap_onoff_o + rap_onoff_d), box_o, box_d, onoff_o, onoff_d.

Arms: **pair-concat** ([x_a, x_b] — the literal spec), **pair-diff**
(x_a − x_b, the old agent's input, at all-pairs scale), **pair-mlp** (torch on
the concat), **reg-ridge** / **reg-gbm** (regression on the same block,
ranked — the controls). `pairwise2.py`, seed 0.

## Results (acc = ordered-pair accuracy, full cell; dev/hits over >=1065 pool)

| target | arm | acc | acc@pool | rho med | dev@10 med | dev mean | hits |
|---|---|---:|---:|---:|---:|---:|---:|
| off | pair-concat | 0.851 | 0.856 | +0.877 | 3.15 | 3.06 | 79 |
| off | **pair-diff** | **0.863** | **0.868** | +0.897 | **2.20** | **2.39** | 82 |
| off | pair-mlp | 0.865 | 0.866 | +0.897 | 2.90 | 3.11 | 80 |
| off | reg-ridge | 0.855 | 0.857 | +0.885 | 2.50 | 2.57 | 84 |
| off | reg-gbm | 0.857 | 0.860 | +0.880 | 3.00 | 2.76 | 81 |
| def | pair-concat | 0.825 | 0.836 | +0.847 | 11.85 | 10.68 | 62 |
| def | **pair-diff** | **0.837** | **0.847** | +0.867 | 11.00 | 10.67 | 62 |
| def | pair-mlp | 0.832 | 0.840 | +0.866 | **10.40** | **9.21** | 64 |
| def | reg-ridge | 0.825 | 0.832 | +0.840 | 10.55 | 9.30 | 64 |
| def | reg-gbm | 0.824 | 0.832 | +0.841 | 13.60 | 12.87 | 56 |
| box | pair-concat | 0.787 | 0.790 | +0.771 | 6.90 | 7.68 | 62 |
| box | **pair-diff** | 0.805 | 0.808 | +0.805 | **4.50** | **6.49** | **68** |
| box | pair-mlp | **0.809** | **0.811** | +0.828 | 6.15 | 9.41 | 64 |
| box | reg-ridge | 0.791 | 0.790 | +0.782 | 9.85 | 12.74 | 61 |
| box | reg-gbm | 0.794 | 0.797 | +0.788 | 6.40 | 7.93 | 60 |
| onoff | pair-concat | 0.874 | 0.880 | +0.917 | 4.05 | 4.46 | 71 |
| onoff | **pair-diff** | **0.889** | **0.896** | +0.933 | **3.20** | **3.71** | 76 |
| onoff | pair-mlp | 0.889 | 0.895 | +0.936 | 3.35 | 4.07 | 75 |
| onoff | reg-ridge | 0.887 | 0.893 | +0.935 | 3.35 | 3.83 | 76 |
| onoff | reg-gbm | 0.878 | 0.883 | +0.923 | 3.90 | 4.46 | 74 |
| box_o | pair-concat | 0.803 | 0.810 | +0.794 | 5.00 | 5.84 | 67 |
| box_o | pair-diff | 0.815 | 0.823 | +0.821 | 8.40 | 8.95 | 70 |
| box_o | **pair-mlp** | **0.816** | 0.820 | +0.821 | 4.85 | 7.19 | 67 |
| box_o | reg-ridge | 0.793 | 0.801 | +0.771 | 8.60 | 9.70 | 66 |
| box_o | reg-gbm | 0.807 | 0.814 | +0.804 | **4.55** | **5.56** | **73** |
| box_d | pair-concat | 0.797 | 0.809 | +0.806 | 12.25 | 13.07 | 55 |
| box_d | **pair-diff** | **0.810** | **0.822** | +0.821 | 12.35 | **10.72** | 58 |
| box_d | pair-mlp | 0.806 | 0.814 | +0.825 | **10.40** | 10.89 | 62 |
| box_d | reg-ridge | 0.794 | 0.801 | +0.798 | 12.40 | 11.31 | 63 |
| box_d | reg-gbm | 0.798 | 0.809 | +0.798 | 11.15 | 12.70 | 55 |
| onoff_o | pair-concat | 0.883 | 0.883 | +0.914 | 4.80 | 4.87 | 69 |
| onoff_o | **pair-diff** | **0.892** | **0.893** | +0.927 | **3.50** | **3.92** | 71 |
| onoff_o | pair-mlp | 0.892 | 0.893 | +0.927 | 4.20 | 4.40 | 71 |
| onoff_o | reg-ridge | 0.888 | 0.890 | +0.925 | 4.45 | 4.07 | 71 |
| onoff_o | reg-gbm | 0.877 | 0.876 | +0.906 | 5.05 | 5.21 | 69 |
| onoff_d | pair-concat | 0.879 | 0.885 | +0.926 | 5.85 | 8.35 | 63 |
| onoff_d | **pair-diff** | **0.886** | **0.893** | +0.935 | **5.05** | **6.41** | **67** |
| onoff_d | pair-mlp | 0.883 | 0.890 | +0.931 | 6.30 | 7.26 | 67 |
| onoff_d | reg-ridge | 0.883 | 0.888 | +0.930 | 5.95 | 7.74 | 64 |
| onoff_d | reg-gbm | 0.868 | 0.872 | +0.907 | 8.90 | 10.55 | 57 |

## Wide-feature pairwise (pairwise3.py)

The confound above is width: compact blocks lose to cv_components' wide
regressions regardless of loss. So the box components got a pair-diff model on
the SAME wide matrices as the Part-A winners (concat at that width does not
fit in 7GB; diff won every pairwise2 target anyway):

| target | model | acc | rho med | dev@10 med/mean | hits |
|---|---|---:|---:|---|---:|
| box_o | pair-diff-wide | **0.908** | +0.952 | 1.25 / 1.35 | **90/100** |
| box_o | ridge-wide regression (Part A) | — | +0.962 | 1.30 / 1.25 | 87/100 |
| box_d | pair-diff-wide | **0.843** | **+0.869** | 6.70 / 7.14 | 70/100 |
| box_d | gbm-wide+hat regression (Part A) | — | +0.864 | 6.35 / 6.51 | 72/100 |

## Reading

- **The classification framing wins the classification metric everywhere.**
  pair-diff beats both regression controls on ordered-pair accuracy on all
  eight targets (+0.3 to +1.6 points), exactly as the loss-function argument
  predicts: every pair contributes equal cross-entropy, so the model spends
  capacity ordering neighbours instead of fitting magnitudes.
- **Concat — the literal "submit both players" spec — is the weakest pair
  arm on every target.** Antisymmetry (P(a,b) = 1 − P(b,a)) has to be learned
  from data that diff features get for free; the ~1-point accuracy tax never
  disappears even with all 900k ordered pairs and both orientations. The MLP
  on concat closes most (not all) of the gap: with shared first-layer weights
  it can synthesize its own differences.
- **Pairwise's dev@10 advantage is real but target-dependent** — biggest on
  `box` (4.50 vs 6.40) and `off` (2.20 vs 2.50), parity on the noisy on/off
  totals where a 3-variable linear model is already at ceiling.
- **Width dominates loss.** No compact-block model, pairwise or regression,
  approaches the wide-feature Part-A winners. At equal (wide) features the
  two framings converge: pair-diff-wide matches wide regression on box_o
  (hits 90 — the best box_o hits of any model in this project) and box_d
  (best accuracy and rho, slightly worse dev). The pairwise loss buys
  ordering accuracy; it does not extract signal the features don't carry.
- Practical: tournament scores are within-cell ranks (win% ~ percentile), so
  they slot into rank-blends but cannot feed the stack as magnitude features
  without a calibration map. Given hats4-both already promoted on offense,
  the marginal integration candidate is a box_o/box wide pair-diff score as a
  rank feature — future work, noted not run.

## Verdict

Pairwise classification is confirmed as the better *ordering* loss on equal
features (all 8 targets), and diff is confirmed as the better input encoding
than concat. But the deciding variable for component quality was feature
width, which regression exploits more cheaply (no quadratic pair blow-up, no
tournament at inference). Keep: wide regression hats for production (see
RESULTS_cv_components.md), pair-diff-wide as the box_o hits@10 champion and
as the strongest candidate for a future rank-blend on the box side.
