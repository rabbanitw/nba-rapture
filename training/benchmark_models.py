"""System-level comparison: our model against Neil Paine's Estimated RAPTOR.

Accuracy is covered elsewhere (RESULTS_leaderboards_*.md). This measures what the two
cost to build, hold, and run.

They are not the same kind of object. Paine's is a linear formula with published
weights -- 13 coefficients per component, applied to 12 box-score and plus-minus
inputs, plus a position adjustment. Ours is 1,149 boosted trees over 1,140 features
blended with a ridge regression. The interesting question is what the extra machinery
buys and what it costs.

Measured here:
  parameters and serialized size
  training wall time and peak memory
  inference latency, batched and single-row, over repeated trials
  throughput at scale
  peak memory during inference

Feature acquisition is reported separately at the bottom, from the actual scrape
logs, because it dominates everything else and is invisible to a timing harness.

Run:  python training/benchmark_models.py
"""

import argparse
import gc
import json
import resource
import time
import tracemalloc
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from estimated_raptor import W_DEF, W_OFF
from experiment_combined import prepare, splits
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
REPEATS = 20


def peak_rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def timeit(fn, repeats=REPEATS):
    """-> (median seconds, min seconds). Median resists a stray scheduling hiccup."""
    fn()                                   # warm
    ts = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    return float(np.median(ts)), float(np.min(ts))


def paine_predict(F):
    """Paine's linear form. F is a dict of the 12 named inputs, each an array."""
    off = np.full(len(next(iter(F.values()))), W_OFF["(Intercept)"])
    dfn = np.full(len(next(iter(F.values()))), W_DEF["(Intercept)"])
    for k, v in F.items():
        off = off + W_OFF[k] * v
        dfn = dfn + W_DEF[k] * v
    return off, dfn, off + dfn


def fake_paine_inputs(n, rng):
    """Paine's inputs, at plausible scales. Only shapes and dtypes matter for timing."""
    names = [k for k in W_OFF if k != "(Intercept)"]
    return {k: rng.normal(10, 5, n).astype(np.float64) for k in names}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_benchmark.md"))
    args = ap.parse_args()

    rng = np.random.default_rng(0)
    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X = X[:, keep]
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    y = d[TARGETS["total"]]
    tuned = json.loads(Path(args.tuned).read_text())
    params = dict(tuned["total"]["params"], verbose=-1)
    rounds = tuned["total"]["rounds"]
    Xtr, Xte = X[tr], X[test]
    print(f"train={Xtr.shape} test={Xte.shape}")

    res = {"n_train": int(tr.sum()), "n_test": int(test.sum()),
           "n_features_ours": int(X.shape[1]),
           "n_features_paine": len(W_OFF) - 1}

    # ---------------------------------------------------------------- training
    gc.collect()
    base_rss = peak_rss_mb()
    tracemalloc.start()
    t0 = time.perf_counter()
    bst = lgb.train(params, lgb.Dataset(Xtr, y[tr]), num_boost_round=rounds)
    lgb_train_s = time.perf_counter() - t0
    _, lgb_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    med = np.nanmedian(Xtr, axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    An, Bn = (A - mu) / sd, (B - mu) / sd
    t0 = time.perf_counter()
    ridge = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit(An, y[tr])
    ridge_train_s = time.perf_counter() - t0

    res["train_seconds_lgbm"] = lgb_train_s
    res["train_seconds_ridge"] = ridge_train_s
    res["train_peak_mb_lgbm"] = lgb_peak / 1e6
    res["train_rss_mb_after"] = peak_rss_mb() - base_rss
    # Paine does no training: the weights are published constants.
    res["train_seconds_paine"] = 0.0

    # ------------------------------------------------------------ model size
    model_str = bst.model_to_string()
    res["ours_trees"] = bst.num_trees()
    res["ours_leaves_total"] = sum(
        bst.dump_model()["tree_info"][i]["num_leaves"] for i in range(bst.num_trees()))
    res["ours_lgbm_bytes"] = len(model_str.encode())
    res["ours_ridge_params"] = int(ridge.coef_.size) + 1
    # normalisation vectors have to ship with the ridge for it to be usable
    res["ours_ridge_bytes"] = int(ridge.coef_.nbytes + mu.nbytes + sd.nbytes
                                  + med.nbytes)
    res["paine_params"] = len(W_OFF) + len(W_DEF)
    res["paine_bytes"] = len(json.dumps({"off": W_OFF, "def": W_DEF}).encode())

    # -------------------------------------------------------------- inference
    F_test = fake_paine_inputs(int(test.sum()), rng)
    res["infer_batch_ours_lgbm"] = timeit(lambda: bst.predict(Xte))[0]
    res["infer_batch_ours_ridge"] = timeit(lambda: ridge.predict(Bn))[0]
    res["infer_batch_paine"] = timeit(lambda: paine_predict(F_test))[0]

    one = Xte[:1]
    one_n = Bn[:1]
    F_one = fake_paine_inputs(1, rng)
    res["infer_single_ours_lgbm"] = timeit(lambda: bst.predict(one), 200)[0]
    res["infer_single_ours_ridge"] = timeit(lambda: ridge.predict(one_n), 200)[0]
    res["infer_single_paine"] = timeit(lambda: paine_predict(F_one), 200)[0]

    # throughput at a size neither model would ever really see, to separate
    # per-call overhead from per-row cost
    N = 100_000
    big = np.repeat(Xte, int(np.ceil(N / len(Xte))), axis=0)[:N]
    big_n = np.repeat(Bn, int(np.ceil(N / len(Bn))), axis=0)[:N]
    F_big = fake_paine_inputs(N, rng)
    t_ours = timeit(lambda: bst.predict(big), 5)[0] + timeit(
        lambda: ridge.predict(big_n), 5)[0]
    t_paine = timeit(lambda: paine_predict(F_big), 5)[0]
    res["throughput_ours"] = N / t_ours
    res["throughput_paine"] = N / t_paine

    gc.collect()
    tracemalloc.start()
    bst.predict(Xte)
    ridge.predict(Bn)
    _, infer_peak_ours = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    tracemalloc.start()
    paine_predict(F_test)
    _, infer_peak_paine = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    res["infer_peak_kb_ours"] = infer_peak_ours / 1e3
    res["infer_peak_kb_paine"] = infer_peak_paine / 1e3

    # feature matrix a batch needs resident
    res["input_mb_ours"] = Xte.nbytes / 1e6
    res["input_mb_paine"] = sum(v.nbytes for v in F_test.values()) / 1e6

    for k, v in res.items():
        print(f"  {k:<26} {v}")
    Path(args.out).with_suffix(".json").write_text(json.dumps(res, indent=1))
    write_report(res, args.out)
    print(f"\nwrote {args.out}")


def write_report(r, out):
    def x(a, b):
        return f"{a / b:,.0f}x" if b else "n/a"
    L = []
    A = L.append
    A("# System comparison: ours vs Neil Paine's Estimated RAPTOR")
    A("")
    A("Accuracy is in RESULTS_leaderboards_*.md. This is what the two cost to build,")
    A("store and run. They are different kinds of object: Paine's is a published")
    A("linear formula over 12 inputs; ours is 1,149 boosted trees over 1,140 features")
    A("blended with a ridge regression.")
    A("")
    A("## Size")
    A("")
    A("| | ours | Paine | ratio |")
    A("|---|---:|---:|---:|")
    A(f"| input features | {r['n_features_ours']:,} | {r['n_features_paine']} | "
      f"{x(r['n_features_ours'], r['n_features_paine'])} |")
    A(f"| parameters | {r['ours_leaves_total']:,} leaves + "
      f"{r['ours_ridge_params']:,} ridge | {r['paine_params']} | "
      f"{x(r['ours_leaves_total'] + r['ours_ridge_params'], r['paine_params'])} |")
    A(f"| serialized | {(r['ours_lgbm_bytes'] + r['ours_ridge_bytes']) / 1e6:.2f} MB | "
      f"{r['paine_bytes']} B | "
      f"{x(r['ours_lgbm_bytes'] + r['ours_ridge_bytes'], r['paine_bytes'])} |")
    A("")
    A("## Training")
    A("")
    A("| | ours | Paine |")
    A("|---|---:|---:|")
    A(f"| wall time | {r['train_seconds_lgbm']:.1f}s LightGBM + "
      f"{r['train_seconds_ridge']:.1f}s ridge | 0s (published constants) |")
    A(f"| peak allocation | {r['train_peak_mb_lgbm']:,.0f} MB | 0 |")
    A("")
    A("## Inference")
    A("")
    A(f"| | ours | Paine | ratio |")
    A("|---|---:|---:|---:|")
    A(f"| batch of {r['n_test']} (one season) | "
      f"{(r['infer_batch_ours_lgbm'] + r['infer_batch_ours_ridge']) * 1e3:.2f} ms | "
      f"{r['infer_batch_paine'] * 1e3:.3f} ms | "
      f"{x(r['infer_batch_ours_lgbm'] + r['infer_batch_ours_ridge'], r['infer_batch_paine'])} |")
    A(f"| single row | "
      f"{(r['infer_single_ours_lgbm'] + r['infer_single_ours_ridge']) * 1e6:.0f} us | "
      f"{r['infer_single_paine'] * 1e6:.1f} us | "
      f"{x(r['infer_single_ours_lgbm'] + r['infer_single_ours_ridge'], r['infer_single_paine'])} |")
    A(f"| throughput | {r['throughput_ours']:,.0f} rows/s | "
      f"{r['throughput_paine']:,.0f} rows/s | "
      f"{x(r['throughput_paine'], r['throughput_ours'])} in Paine's favour |")
    A(f"| peak alloc, one batch | {r['infer_peak_kb_ours']:,.0f} KB | "
      f"{r['infer_peak_kb_paine']:,.0f} KB | |")
    A(f"| input matrix resident | {r['input_mb_ours']:.2f} MB | "
      f"{r['input_mb_paine']:.3f} MB | "
      f"{x(r['input_mb_ours'], r['input_mb_paine'])} |")
    A("")
    A("## The cost that dominates: getting the features")
    A("")
    A("Everything above is microseconds against a data pipeline measured in hours.")
    A("Per season, from this project's actual scrape logs:")
    A("")
    A("| | ours | Paine |")
    A("|---|---|---|")
    A("| pbp box score | 62 API calls | 62 API calls |")
    A("| wowy on/off | ~1,560 calls, ~2h | ~1,560 calls, ~2h |")
    A("| player tracking | 28 calls, **needs a residential IP** | not needed |")
    A("| total | ~1,650 calls | ~1,622 calls |")
    A("")
    A("Both need the same two expensive feeds, because Paine's OnCourt and OnOff terms")
    A("come from the same wowy scrape ours does. The real operational difference is the")
    A("14 tracking tables: only 28 requests, but stats.nba.com refuses datacenter IPs,")
    A("so that one feed forces a residential connection into the pipeline. Paine's model")
    A("has no such dependency.")
    Path(out).write_text("\n".join(L))


if __name__ == "__main__":
    main()
