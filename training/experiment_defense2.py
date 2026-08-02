"""Defense-focused optimization round, driven by the recovered RAPTOR spec.

The full methodology (training/raptor_methodology_fulltext.txt) gives the actual
defensive regression: steals at +1.49, offensive fouls drawn at +2.28, defended
2-point misses +1.05 / makes -0.33 with 3-point *results* deliberately excluded as
noise, contested defensive rebounds weighted far above uncontested, distance
travelled for perimeter defenders only, and -- notably -- **no blocks at all** (zero
predictive power once defended field goals are accounted for).

Arms, all on top of the replicated defense production model (direct + cell-relative):

  base            direct + cell-relative, as shipped
  +538-linear     engineered features transcribing the published defensive
                  coefficients onto our columns: weighted steals, weighted offensive
                  fouls drawn, rim-defense value 1.05*(DFGA-DFGM)-0.33*DFGM,
                  contested DREB, perimeter-gated defensive distance. A tree can in
                  principle find these shapes; handing them over as single columns
                  costs nothing and injects six years of 538's regression work.
  +comp-feats     the four out-of-fold component predictions (box/on-off, both ends)
                  appended as features. The defense combiner failed because box_d is
                  weakly learnable; as *features* the tree can lean on them only
                  where they help -- including the offensive components, since
                  offensive load plausibly informs defensive rating.
  +prior          cell-relative plus the previous-season prediction feature (best
                  dev@10 in the last round, unreplicated then, combined here).
  kitchen-sink    all of the above at once.

Offense is included only for the arms that could plausibly transfer (538-linear has
an offensive analog via shot-type expected values; comp-feats applies as-is).

Run:  python training/experiment_defense2.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import (COMPONENT_LABELS, RELATIVE_COLS, cell_relative,
                                   masks_for)
from experiment_topk_rank import score_cells
from predict_seasons import DROP_FEATURES
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10
SEEDS = (0, 1, 2)


def blend(Xtr, t, Xte, med, params, rounds, seeds=SEEDS, ridge_w=0.25):
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    pr = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, t).predict(
        (B - mu) / sd)
    ps = [lgb.train(dict(params, seed=s, bagging_seed=s, feature_fraction_seed=s),
                    lgb.Dataset(Xtr, t), num_boost_round=rounds).predict(Xte)
          for s in seeds]
    return (1 - ridge_w) * np.mean(ps, axis=0) + ridge_w * pr


def col(X, feat, name):
    idx = {n: i for i, n in enumerate(feat)}
    return X[:, idx[name]].astype(np.float64) if name in idx else None


def raptor_linear_features(X, feat):
    """Transcribe the published coefficients onto our columns. NaNs pass through."""
    g = lambda n: col(X, feat, n)
    out, names = [], []

    def add(name, v):
        if v is not None:
            out.append(v)
            names.append(name)

    steals = g("pbp|Steals")
    offoul = g("pbp|Offensive Fouls Drawn")
    dfgm, dfga = g("track:defensive-impact|DFGM"), g("track:defensive-impact|DFGA")
    cdreb = g("track:defensive-rebounding|CONTESTED\nDREB")
    miles_d = g("track:speed-distance|DIST. MILES DEF")
    shoot_f = g("pbp|ShootingFouls")
    add("r538|steals_w", 1.49 * steals if steals is not None else None)
    add("r538|offoul_w", 2.28 * offoul if offoul is not None else None)
    if dfgm is not None and dfga is not None:
        add("r538|rim_def_value", 1.05 * (dfga - dfgm) - 0.33 * dfgm)
        with np.errstate(invalid="ignore", divide="ignore"):
            add("r538|rim_dfg_pct", np.where(dfga > 0, dfgm / dfga, np.nan))
    add("r538|contested_dreb", cdreb)
    # perimeter gate: guards and wings by the position one-hots
    per = None
    for c in ("ctx|pos_PG", "ctx|pos_SG", "ctx|pos_SF"):
        v = g(c)
        per = v if per is None else per + v
    if miles_d is not None and per is not None:
        add("r538|perimeter_miles_def", miles_d * (per > 0))
    add("r538|shooting_fouls_neg", -0.19 * shoot_f if shoot_f is not None else None)

    # offensive analog: shot-mix expected value and assisted-shot deduction
    ev = {"pbp|AtRimFGA": 1.16, "pbp|ShortMidRangeFGA": 0.82,
          "pbp|LongMidRangeFGA": 0.80, "pbp|Corner3FGA": 1.16, "pbp|Arc3FGA": 1.05}
    mix = None
    for n, w in ev.items():
        v = g(n)
        if v is not None:
            mix = w * v if mix is None else mix + w * v
    add("r538|shot_mix_ev", mix)
    ast2, atrim_ast = g("pbp|Assisted2sPct"), g("pbp|AtRimPctAssisted")
    add("r538|assisted2_pct_neg", -ast2 if ast2 is not None else None)
    add("r538|atrim_assisted_neg", -atrim_ast if atrim_ast is not None else None)
    return (np.column_stack(out).astype(np.float32), names) if out else (None, [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_defense2.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(Path(args.datadir) / "components.npz")
    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    groups = np.array([f"{p}|{s}" for p, s in
                       zip(d["player"][tr], d["season"][tr])])
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    box_mask, onoff_mask = masks_for(feat)
    tuned = json.loads(Path(args.tuned).read_text())
    print(f"X={X.shape} train={tr.sum()} test={test.sum()}", flush=True)

    # shared building blocks -------------------------------------------------
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    R, rnames = raptor_linear_features(X, feat)
    print(f"cell-relative feats: {Z.shape[1]}  538-linear feats: {len(rnames)} "
          f"({rnames})", flush=True)

    # OOF component predictions, once, shared by both targets ---------------
    print("building OOF component predictions ...", flush=True)
    oof_cols, oof_te_cols = [], []
    for tgt in ("offense", "defense"):
        params_t = dict(tuned[tgt]["params"], verbose=-1)
        rounds_t = tuned[tgt]["rounds"]
        for lab_name, mask in zip(COMPONENT_LABELS[tgt], (box_mask, onoff_mask)):
            lab = comp[lab_name]
            Xs, ms = X[:, mask], med[mask]
            oof = np.full(int(tr.sum()), np.nan)
            Xs_tr, lab_tr = Xs[tr], lab[tr]
            for tri, vai in GroupKFold(n_splits=4).split(Xs_tr, lab_tr, groups=groups):
                oof[vai] = blend(Xs_tr[tri], lab_tr[tri], Xs_tr[vai], ms,
                                 params_t, rounds_t, seeds=(0,))
            oof_cols.append(oof)
            oof_te_cols.append(blend(Xs_tr, lab_tr, Xs[test], ms, params_t,
                                     rounds_t, seeds=(0,)))
            print(f"  {lab_name} done", flush=True)
    C_tr = np.column_stack(oof_cols).astype(np.float32)
    C_te = np.column_stack(oof_te_cols).astype(np.float32)

    # prior-season feature (shared machinery, per target) --------------------
    seasons = np.array([str(s) for s in d["season"]])

    def prior_feature(target):
        y = d[TARGETS[target]]
        params_t = dict(tuned[target]["params"], verbose=-1)
        rounds_t = tuned[target]["rounds"]
        prior = np.full(len(y), np.nan)
        by_ps = {}
        for s in sorted(set(seasons[tr])):
            m_fit = tr & (seasons != s)
            mdl = lgb.train(dict(params_t, seed=0), lgb.Dataset(X[m_fit], y[m_fit]),
                            num_boost_round=rounds_t)
            held = tr & (seasons == s)
            for pl, v in zip(d["player"][held], mdl.predict(X[held])):
                by_ps.setdefault((str(pl), s), []).append(v)
        full = lgb.train(dict(params_t, seed=0), lgb.Dataset(X[tr], y[tr]),
                         num_boost_round=rounds_t)
        h14 = test & (seasons == "2013-14")
        for pl, v in zip(d["player"][h14], full.predict(X[h14])):
            by_ps.setdefault((str(pl), "2013-14"), []).append(v)
        for i in np.where(tr | test)[0]:
            y0 = int(seasons[i][:4])
            k = (str(d["player"][i]), f"{y0 - 1}-{str(y0)[2:]}")
            if k in by_ps:
                prior[i] = float(np.mean(by_ps[k]))
        return np.column_stack([prior, np.isfinite(prior)]).astype(np.float32)

    rows = []

    def record(target, name, p):
        y = d[TARGETS[target]]
        s = score_cells(y[test], p, cells_te)
        rows.append({"target": target, "arm": name, **s})
        print(f"  {name:<15} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40", flush=True)

    for target in ("defense", "offense"):
        y = d[TARGETS[target]]
        params_t = dict(tuned[target]["params"], verbose=-1)
        rounds_t = tuned[target]["rounds"]
        print(f"\n=== {target} ===", flush=True)
        P = prior_feature(target)

        variants = {
            "base": [Z],
            "+538-linear": [Z, R],
            "+comp-feats": [Z, "COMP"],
            "+prior": [Z, P],
            "kitchen-sink": [Z, R, "COMP", P],
        }
        for name, parts in variants.items():
            Xtr_parts, Xte_parts = [X[tr]], [X[test]]
            for part in parts:
                if isinstance(part, str) and part == "COMP":
                    Xtr_parts.append(C_tr)
                    Xte_parts.append(C_te)
                else:
                    Xtr_parts.append(part[tr])
                    Xte_parts.append(part[test])
            Xtr_v = np.hstack(Xtr_parts)
            Xte_v = np.hstack(Xte_parts)
            med_v = np.concatenate([med, np.zeros(Xtr_v.shape[1] - X.shape[1])])
            record(target, name,
                   blend(Xtr_v, y[tr], Xte_v, med_v, params_t, rounds_t))

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    L = ["# Defense-focused round two (spec-informed)", ""]
    for target in ("defense", "offense"):
        L += [f"## {target}", "",
              "| arm | dev@10 | dev@20 | tau@10 | tau@20 | MAE | hits@10 | hits@20 |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for r in sorted([x for x in rows if x["target"] == target],
                        key=lambda r: r["dev@10"]):
            L.append(f"| {r['arm']} | {r['dev@10']:.2f} | {r['dev@20']:.2f} | "
                     f"{r['tau@10']:+.3f} | {r['tau@20']:+.3f} | {r['mae']:.3f} | "
                     f"{r['hits@10']}/20 | {r['hits@20']}/40 |")
        L.append("")
    Path(args.out).write_text("\n".join(L))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
