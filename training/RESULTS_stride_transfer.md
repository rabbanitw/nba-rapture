# Does the stride ablation's recommendation transfer? No.

[RESULTS_stride.md](RESULTS_stride.md) swept `--modern-stride` and validation
picked **stride 3** (total, defense) and **stride 12** (offense). This is the
test of whether that recommendation survives being moved into the production
pipeline. It does not.

## Top-100, regular season, 200 slots

| target | run | R² | RMSE | ρ | hits@100 |
|---|---|---|---|---|---|
| **total** | **stride 6 (production)** | **+0.751** | **1.719** | **+0.886** | **168/200** |
| | stride 3 (production) | +0.743 | 1.746 | +0.881 | 164/200 |
| | stride-tuned (ablation subset) | +0.734 | 1.776 | +0.869 | 163/200 |
| | Paine | +0.683 | 1.938 | +0.846 | 165/200 |
| **offense** | **stride 6 (production)** | **+0.821** | 1.022 | +0.908 | **171/200** |
| | stride 3 (production) | +0.818 | 1.031 | +0.904 | 171/200 |
| | stride-tuned (ablation subset) | +0.825 | 1.012 | +0.917 | 170/200 |
| | Paine | +0.707 | 1.309 | +0.825 | 160/200 |
| **defense** | **stride 6 (production)** | **+0.635** | **1.409** | **+0.818** | **165/200** |
| | stride 3 (production) | +0.583 | 1.506 | +0.793 | 158/200 |
| | stride-tuned (ablation subset) | +0.638 | 1.403 | +0.795 | 158/200 |
| | Paine | +0.504 | 1.642 | +0.727 | 149/200 |

**Stride 6 stays the best production setting.** Stride 3 is a wash on offense and
costs 0.008 R² on total and 0.052 on defense.

## Why the ablation pointed the wrong way

The sweep built once at stride 1 and subset in memory, which reorders one step:

| | production pipeline | ablation |
|---|---|---|
| 1 | select timestamps at stride k | build **all** stride-1 rows |
| 2 | build rows for those timestamps | **dedupe** over the whole stride-1 set |
| 3 | dedupe over those rows | select timestamps at stride k |

`dedupe()` keeps the latest of each byte-identical `(player, season, split)`
group. Run over a near-daily snapshot set it removes far more — 25,805 of 94,732
rows, versus 903 of 33,641 in a direct stride-3 build — and the survivors are
skewed toward late-season snapshots. So the ablation's "stride 6" carries 10,933
fit rows where a direct stride-6 build carries 14,235, and they are not the same
rows.

That makes the sweep **internally valid but not transferable**: every rung shares
one pipeline, so their ordering is meaningful, but the pipeline itself is
slightly lossier than production and the fine-grained optimum shifts.

## What the ablation does establish

The coarse result is robust and was the actual question: **more snapshots make
things worse.** Stride 1 was the worst rung on all three targets (total +0.689,
offense +0.772, defense +0.543), and the degradation from stride 3 down to
stride 1 is monotonic and large — far bigger than the 3-vs-6 difference that
failed to transfer. Near-duplicate in-season snapshots add no independent
information while re-weighting the loss toward a distribution the test set is not
drawn from.

## Corrections this supersedes

- "A smaller stride is more likely to help than hurt" — wrong; stride 1 is worst.
- "I'd change the default to `--modern-stride 3`" — wrong; stride 6 wins when the
  comparison is run the production way.
- The sweep's original claim that subsetting stride 1 is "exactly equivalent to
  rebuilding per stride" — wrong, for the dedupe-ordering reason above.
