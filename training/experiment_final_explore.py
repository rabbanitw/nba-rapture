"""Final exploration round on the full data stack (defend + opponent + FT_AST).

The one retry that new data justifies: the defense COMPONENTS architecture. It was
rejected earlier because rap_box_d was barely learnable (R^2 +0.71) -- 538's
defensive box inputs were missing. The defend scrape supplies exactly those inputs,
so the box_d model gets a second chance with them included. If its R^2 rises, the
architecture that transformed offense may now transfer.

Defense arms (full regime; base = production candidate X + cell-relative + defend-eng):

  base-defend       the replicated production candidate
  +feat-pack        engineered interactions: FREQ x quality, defend value per 100
                    defensive possessions (possession-normalized, not minutes),
                    defend features z-scored within cell (era context) and within
                    cell x bigs/guards (a rim protector's PLUSMINUS means something
                    different than a guard's)
  +opp-eng          opponent features retried WITH defend present -- null alone, but
                    they may be complementary to player-attributed defense
  def-components    box_d = box features + defend-eng -> rap_box_d;
                    onoff_d = wowy features + opponent block -> rap_onoff_d;
                    minutes-aware ridge combiner. Reports component R^2.
  ens(full,matched) average of the full-regime and matched-regime models -- both
                    replicated, trained on different row distributions
  catboost          architecture check on the winning feature set

Offense arms (brief -- the components+opp stack is already strong):

  components+opp    current best, as baseline
  rank-ens          rank-average with the direct+opp model: membership vs ordering
                    complementarity, shown earlier for cell_pct

Run:  python training/experiment_final_explore.py
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import GroupKFold

from db import REPO_ROOT
from experiment_combined import prepare, splits
from experiment_components import (COMPONENT_LABELS, RELATIVE_COLS, cell_relative,
                                   combiner_design, masks_for)
from experiment_oppdef import blend, engineered, per100
from experiment_topk_rank import ranks, score_cells
from predict_seasons import DROP_FEATURES
from seasons import FULL_SEASON_SNAPSHOTS
from train_rapture import TARGETS

RS_MIN, PO_MIN = 50, 10


def feat_pack(X, feat, E, enames, F, fcols, cells, ctx_pos_big):
    """Interactions and context-normalized versions of the defend features."""
    idx = {n: i for i, n in enumerate(feat)}
    fci = {c: i for i, c in enumerate(fcols)}
    ei = {n: i for i, n in enumerate(enames)}
    cols, names = [], []

    freq = E[:, ei["freq"]].astype(np.float64)
    ovpm = E[:, ei["ov_pct_pm"]].astype(np.float64)
    cols.append(freq * ovpm)
    names.append("fp|freq_x_pm")

    # possession-normalized 2pt defense value (per 100 defensive possessions)
    f2m = F[:, fci["defend-2pt|FG2M"]].astype(np.float64)
    f2a = F[:, fci["defend-2pt|FG2A"]].astype(np.float64)
    defposs = X[:, idx["pbp|DefPoss"]].astype(np.float64)
    with np.errstate(invalid="ignore", divide="ignore"):
        cols.append(np.where(defposs > 0,
                             100.0 * (1.05 * (f2a - f2m) - 0.33 * f2m) / defposs,
                             np.nan))
    names.append("fp|d2_value100")

    # cell-z and cell-x-position-z of the engineered defend columns
    for base_name in ("d2_value36", "rim_pct_pm", "ov_pct_pm", "freq"):
        v = E[:, ei[base_name]].astype(np.float64)
        z = np.full(len(v), np.nan)
        zp = np.full(len(v), np.nan)
        for c in np.unique(cells):
            m = cells == c
            mu, sd = np.nanmean(v[m]), np.nanstd(v[m])
            z[m] = (v[m] - mu) / (sd or 1.0)
            for grp in (0, 1):
                mg = m & (ctx_pos_big == grp)
                if mg.sum() > 5:
                    mu2, sd2 = np.nanmean(v[mg]), np.nanstd(v[mg])
                    zp[mg] = (v[mg] - mu2) / (sd2 or 1.0)
        cols += [z, zp]
        names += [f"fp|{base_name}_cellz", f"fp|{base_name}_posz"]

    return np.column_stack(cols).astype(np.float32), names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--tuned", default=str(REPO_ROOT / "training" / "tuned_params.json"))
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_final_explore.md"))
    args = ap.parse_args()

    X, feat, d = prepare(args.datadir)
    keep = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    X, feat = X[:, keep], [feat[i] for i in keep]
    comp = np.load(Path(args.datadir) / "components.npz")
    dfz = np.load(Path(args.datadir) / "defend.npz", allow_pickle=True)
    E, F = dfz["E"], dfz["F"]
    enames = [str(n) for n in dfz["enames"]]
    fcols = [str(c) for c in dfz["fcols"]]
    opp = np.load(Path(args.datadir) / "wowyopp.npz", allow_pickle=True)
    on_raw, off_raw = opp["on_X"], opp["off_X"]
    ofields = [str(f) for f in opp["fields"]]

    fit, val, test = splits(d, RS_MIN, PO_MIN)
    rs = d["season_type"] == "Regular season"
    tr, test = (fit | val) & rs, test & rs
    isfull = np.array([t in FULL_SEASON_SNAPSHOTS for t in d["timestamp"]])
    tr_m = tr & isfull
    cells_all = np.array([f"{t}|{s}" for t, s in
                          zip(d["timestamp"], d["season_type"])])
    cells_te = np.array([f"{s}|{t}" for s, t in
                         zip(d["season"][test], d["season_type"][test])])
    groups = np.array([f"{p}|{s}" for p, s in
                       zip(d["player"][tr], d["season"][tr])])
    mp = d["mp"].astype(np.float64)
    med = np.nanmedian(X[tr], axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    box_mask, onoff_mask = masks_for(feat)
    tuned = json.loads(Path(args.tuned).read_text())
    Z = cell_relative(X, feat, cells_all, RELATIVE_COLS)
    Eopp, _ = engineered(on_raw, off_raw, ofields, cells_all)
    on100, off100 = per100(on_raw, ofields), per100(off_raw, ofields)
    Bopp = np.hstack([on100, off100, on100 - off100]).astype(np.float32)
    idxf = {n: i for i, n in enumerate(feat)}
    pos_big = ((X[:, idxf["ctx|pos_C"]] > 0) |
               (X[:, idxf["ctx|pos_PF"]] > 0)).astype(int)
    FP, fpnames = feat_pack(X, feat, E, enames, F, fcols, cells_all, pos_big)
    print(f"X={X.shape}  feat-pack {FP.shape[1]} cols", flush=True)

    rows = []

    def record(target, name, p, extra=""):
        y = d[TARGETS[target]]
        s = score_cells(y[test], p, cells_te)
        rows.append({"target": target, "arm": name, **s})
        print(f"  {name:<18} dev@10={s['dev@10']:5.2f} dev@20={s['dev@20']:5.2f} "
              f"tau@10={s['tau@10']:+.3f} tau@20={s['tau@20']:+.3f} "
              f"MAE={s['mae']:.3f} hits@10={s['hits@10']}/20 "
              f"hits@20={s['hits@20']}/40  {extra}", flush=True)

    # ---------------- defense ------------------------------------------------
    y = d[TARGETS["defense"]]
    params = dict(tuned["defense"]["params"], verbose=-1)
    rounds = tuned["defense"]["rounds"]
    print("\n=== defense ===", flush=True)

    def fitset(parts, trmask, r):
        Xtr = np.hstack([X[trmask]] + [p[trmask] for p in parts])
        Xte = np.hstack([X[test]] + [p[test] for p in parts])
        mv = np.concatenate([med, np.zeros(Xtr.shape[1] - X.shape[1])])
        return blend(Xtr, y[trmask], Xte, mv, params, r)

    p_full = fitset([Z, E], tr, rounds)
    record("defense", "base-defend", p_full)
    record("defense", "+feat-pack", fitset([Z, E, FP], tr, rounds))
    record("defense", "+opp-eng", fitset([Z, E, Eopp], tr, rounds))

    p_matched = fitset([Z, E], tr_m, max(rounds // 3, 150))
    record("defense", "matched-defend", p_matched)
    record("defense", "ens(full,matched)", 0.5 * p_full + 0.5 * p_matched)

    # defense components, with the new inputs in each part's natural home
    Xb = np.hstack([X[:, box_mask], E])
    Xo = np.hstack([X[:, onoff_mask], Bopp])
    box_lab, onoff_lab = (comp[c] for c in COMPONENT_LABELS["defense"])
    parts_pred = {}
    for tag, Xs, lab in (("box_d+defend", Xb, box_lab),
                         ("onoff_d+opp", Xo, onoff_lab)):
        ms = np.nanmedian(Xs[tr], axis=0)
        ms = np.where(np.isfinite(ms), ms, 0.0)
        oof = np.full(int(tr.sum()), np.nan)
        Xs_tr, lab_tr = Xs[tr], lab[tr]
        for tri, vai in GroupKFold(n_splits=4).split(Xs_tr, lab_tr, groups=groups):
            oof[vai] = blend(Xs_tr[tri], lab_tr[tri], Xs_tr[vai], ms, params,
                             rounds, seeds=(0,))
        te = blend(Xs_tr, lab_tr, Xs[test], ms, params, rounds)
        r2 = 1 - np.nanvar(lab[test] - te) / np.nanvar(lab[test])
        print(f"  [{tag}] R2={r2:+.3f}", flush=True)
        parts_pred[tag] = (oof, te)
    cb = Ridge(alpha=1.0).fit(
        combiner_design(parts_pred["box_d+defend"][0],
                        parts_pred["onoff_d+opp"][0], mp[tr]), y[tr])
    record("defense", "def-components",
           cb.predict(combiner_design(parts_pred["box_d+defend"][1],
                                      parts_pred["onoff_d+opp"][1], mp[test])),
           extra=f"w={np.round(cb.coef_[:2], 3)}")

    from catboost import CatBoostRegressor
    Xtr_c = np.hstack([X[tr], Z[tr], E[tr]])
    Xte_c = np.hstack([X[test], Z[test], E[test]])
    ps = []
    for s in range(3):
        m = CatBoostRegressor(iterations=1500, learning_rate=0.03, depth=6,
                              l2_leaf_reg=5.0, random_seed=s, verbose=False,
                              allow_writing_files=False)
        m.fit(Xtr_c, y[tr])
        ps.append(m.predict(Xte_c))
    record("defense", "catboost", np.mean(ps, axis=0))

    # ---------------- offense ------------------------------------------------
    yo = d[TARGETS["offense"]]
    params_o = dict(tuned["offense"]["params"], verbose=-1)
    rounds_o = tuned["offense"]["rounds"]
    print("\n=== offense ===", flush=True)
    box_lab, onoff_lab = (comp[c] for c in COMPONENT_LABELS["offense"])
    Xb_o = X[:, box_mask]
    Xo_o = np.hstack([X[:, onoff_mask], Bopp])
    parts_o = {}
    for tag, Xs, lab in (("box_o", Xb_o, box_lab), ("onoff_o+opp", Xo_o, onoff_lab)):
        ms = np.nanmedian(Xs[tr], axis=0)
        ms = np.where(np.isfinite(ms), ms, 0.0)
        oof = np.full(int(tr.sum()), np.nan)
        Xs_tr, lab_tr = Xs[tr], lab[tr]
        for tri, vai in GroupKFold(n_splits=4).split(Xs_tr, lab_tr, groups=groups):
            oof[vai] = blend(Xs_tr[tri], lab_tr[tri], Xs_tr[vai], ms, params_o,
                             rounds_o, seeds=(0,))
        parts_o[tag] = (oof, blend(Xs_tr, lab_tr, Xs[test], ms, params_o, rounds_o))
    cbo = Ridge(alpha=1.0).fit(
        combiner_design(parts_o["box_o"][0], parts_o["onoff_o+opp"][0], mp[tr]),
        yo[tr])
    p_comp = cbo.predict(combiner_design(parts_o["box_o"][1],
                                         parts_o["onoff_o+opp"][1], mp[test]))
    record("offense", "components+opp", p_comp)

    Xtr_d = np.hstack([X[tr], Z[tr], Eopp[tr]])
    Xte_d = np.hstack([X[test], Z[test], Eopp[test]])
    mv = np.concatenate([med, np.zeros(Xtr_d.shape[1] - X.shape[1])])
    p_direct = blend(Xtr_d, yo[tr], Xte_d, mv, params_o, rounds_o)
    record("offense", "direct+opp", p_direct)
    rb = np.empty(len(yo[test]))
    for c in np.unique(cells_te):
        m = cells_te == c
        rb[m] = -(ranks(p_comp[m]) + ranks(p_direct[m])) / 2.0
    record("offense", "rank-ens", rb)

    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
    L = ["# Final exploration on the complete data stack", ""]
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
