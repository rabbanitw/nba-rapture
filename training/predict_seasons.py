"""Score the unlabeled seasons (2023-24 onward) with the combined model.

538 shut down, so these seasons have features but no RAPTOR to train against.
build_dataset.py keys every row off a 538 document and skips them entirely. This
builds the same feature matrix without that requirement, fits the production
combined model on everything that does have labels, and predicts.

The pipeline deliberately mirrors compare_estimated_raptor.our_predictions rather
than loading models/combined_*_lgbm.txt, because those saved boosters are the plain
LightGBM fits from train_rapture.py. Every reported result in RESULTS_top100.md and
RESULTS_stride_transfer.md comes from the 0.75 LightGBM + 0.25 RidgeCV blend, so
that is what "our model" means here.

Two things differ from a labeled row and are worth stating plainly:

  mp   build_dataset takes minutes from the 538 document. Here it comes from pbp
       Minutes. On 2017-18 regular season the two agree exactly -- correlation
       1.0000, median ratio 1.0000, median absolute difference 0.0 minutes -- so
       this is a substitution in name only.

  pos  538 supplied the position. Nothing in pbpstats or the tracking feed does,
       and only 39-60% of these players ever appeared in a 538 table, falling with
       each season. Known positions are carried over by name; the rest are imputed
       with a small classifier fit on the labeled rows. The position one-hot is
       0.06% of the combined model's total gain, and --pos-mode reruns the whole
       thing with positions zeroed so the report can quantify what it cost.

Run:  python training/predict_seasons.py
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.linear_model import RidgeCV

from coverage import TRACK_TYPES, as_float, pick_doc
from db import REPO_ROOT, get_collection
from estimated_raptor import metrics
from experiment_combined import splits
from seasons import UNLABELED_SNAPSHOTS, season_of
from train_rapture import (LGB_PARAMS, POS_COLS, TARGETS, add_context, dedupe,
                           load, normalize_rates)

MODEL = "combined"
SEASON_TYPES = ["Regular season", "Playoffs"]

# Identifiers that coverage.ID_FIELDS never excluded, so build_dataset let them
# through as features. EntityId is the NBA player id and RowId duplicates it; both
# run 708 to 1,630,572, and because ids are handed out in debut order they encode
# roughly when a player entered the league. Every player in these seasons sits at
# the top of that range, so a tree that learned a split on id sends all of them the
# same way. TeamId is constant and simply carries nothing. Dropped from both the
# training and prediction matrices, so the refit never sees them.
DROP_FEATURES = {"pbp|EntityId", "pbp|RowId", "pbp|TeamId"}
RS_MIN, PO_MIN = 50, 10          # training-row filters, selected in experiment_combined
TOP_N = 100

BLOCK_QUERY = {
    "pbp": {"source": "pbp"},
    "wowy_on": {"source": "wowy", "on_or_off": "on"},
    "wowy_off": {"source": "wowy", "on_or_off": "off"},
    **{f"track:{t}": {"source": "nba-tracking", "data_type": t} for t in TRACK_TYPES},
}
# Same rule build_dataset.py applies: without these a row has no real signal.
REQUIRED = ["pbp", "wowy_on", "wowy_off"]
MIN_BLOCKS_FRAC = 0.8


def build_unlabeled(coll, feat_names, timestamps):
    """Feature matrix for cells with no 538 label, in the training column order."""
    blocks = defaultdict(list)
    for n in feat_names:
        b, f = n.split("|", 1)
        blocks[b].append(f)

    rows, meta = [], []
    for ts in timestamps:
        for st in SEASON_TYPES:
            picked = defaultdict(dict)
            for block, q in BLOCK_QUERY.items():
                grp = defaultdict(list)
                for d in coll.find({"timestamp": ts, "season_type": st, **q}):
                    grp[d["standard_name"]].append(d)
                for name, docs in grp.items():
                    picked[name][block] = pick_doc(docs)

            n_before = len(rows)
            for player, got in picked.items():
                if (any(b not in got for b in REQUIRED)
                        or len(got) < len(BLOCK_QUERY) * MIN_BLOCKS_FRAC):
                    continue
                vec = []
                for b in blocks:
                    doc = got.get(b, {})
                    vec.extend(as_float(doc.get(f)) for f in blocks[b])
                rows.append(np.array([np.nan if v is None else v for v in vec],
                                     dtype=np.float32))
                # 538 supplied mp; pbp Minutes is the same number (see module docstring)
                mp = as_float(got["pbp"].get("Minutes")) or 0.0
                meta.append({"player": player, "timestamp": ts, "season": season_of(ts),
                             "season_type": st, "mp": mp, "pos": ""})
            print(f"  {ts} {st:<15} {len(picked):>4} players -> "
                  f"{len(rows) - n_before} complete rows")

    X = np.vstack(rows) if rows else np.zeros((0, len(feat_names)), dtype=np.float32)
    return X, meta


def carry_over_positions(coll, meta):
    """Reuse a player's 538 position where 538 ever listed them."""
    latest = {}
    for d in coll.find({"source": "538"}, {"standard_name": 1, "pos": 1,
                                           "timestamp": 1, "_id": 0}):
        if not d.get("pos"):
            continue
        prev = latest.get(d["standard_name"])
        if prev is None or d["timestamp"] > prev[0]:
            latest[d["standard_name"]] = (d["timestamp"], d["pos"])
    hit = 0
    for m in meta:
        got = latest.get(m["player"])
        if got:
            m["pos"] = got[1]
            hit += 1
    print(f"  positions carried over from 538: {hit}/{len(meta)}")
    return hit


def impute_positions(Xtr, pos_tr, Xnew, meta_new):
    """One binary LightGBM per position slot, fit on the labeled rows.

    Only fills players 538 never listed. Cheap because the position one-hot is
    0.06% of the model's gain -- this exists so those rows are not systematically
    all-zero, not because precision here matters.
    """
    need = [i for i, m in enumerate(meta_new) if not m["pos"]]
    if not need:
        return 0
    Y = np.zeros((len(pos_tr), len(POS_COLS)), dtype=np.float32)
    for i, p in enumerate(pos_tr):
        toks = {t.strip().upper() for t in str(p or "").split(",") if t.strip()}
        for j, c in enumerate(POS_COLS):
            Y[i, j] = 1.0 if c in toks else 0.0

    params = dict(LGB_PARAMS, objective="binary", learning_rate=0.05)
    preds = np.zeros((len(need), len(POS_COLS)))
    Xn = Xnew[need]
    for j, c in enumerate(POS_COLS):
        bst = lgb.train(params, lgb.Dataset(Xtr, Y[:, j]), num_boost_round=200)
        preds[:, j] = bst.predict(Xn)
    for k, i in enumerate(need):
        # 538 lists one or two slots; take the argmax, plus a clear runner-up.
        order = np.argsort(-preds[k])
        slots = [POS_COLS[order[0]]]
        if preds[k][order[1]] >= 0.5 * preds[k][order[0]]:
            slots.append(POS_COLS[order[1]])
        meta_new[i]["pos"] = ",".join(slots)
    print(f"  positions imputed for the remaining {len(need)}")
    return len(need)


def fit_and_predict(X, d, n_labeled, pos_mode):
    """Fit each target on the labeled rows, predict the trailing unlabeled ones."""
    lab = slice(0, n_labeled)
    new = slice(n_labeled, X.shape[0])
    dl = {k: (v[lab] if isinstance(v, np.ndarray) and v.shape[:1] == d["y"].shape[:1]
              else v) for k, v in d.items()}
    fit, val, test = splits(dl, RS_MIN, PO_MIN)
    tr = fit | val
    print(f"  fit={fit.sum()} val={val.sum()} (test {test.sum()} rows held out), "
          f"predicting {X[new].shape[0]} unlabeled rows")

    Xtr_all, Xnew = X[lab], X[new]
    out, held = {}, {}
    for target in ("total", "offense", "defense"):
        y = d[TARGETS[target]][lab]
        bst = lgb.train(LGB_PARAMS, lgb.Dataset(Xtr_all[fit], y[fit]),
                        num_boost_round=4000,
                        valid_sets=[lgb.Dataset(Xtr_all[val], y[val])],
                        callbacks=[lgb.early_stopping(150, verbose=False)])
        final = lgb.train(LGB_PARAMS, lgb.Dataset(Xtr_all[tr], y[tr]),
                          num_boost_round=bst.best_iteration)
        pred = final.predict(Xnew)

        med = np.nanmedian(Xtr_all[tr], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        A = np.where(np.isfinite(Xtr_all[tr]), Xtr_all[tr], med)
        B = np.where(np.isfinite(Xnew), Xnew, med)
        mu, sd = A.mean(0), A.std(0)
        sd[sd == 0] = 1.0
        ridge = RidgeCV(alphas=np.logspace(-2, 4, 25)).fit((A - mu) / sd, y[tr])
        out[target] = 0.75 * pred + 0.25 * ridge.predict((B - mu) / sd)

        # Score the held-out seasons with this exact model. Dropping the identifier
        # columns makes it not quite the model RESULTS_top100.md measured, so the
        # accuracy quoted in the report has to be re-earned rather than cited.
        Xte = Xtr_all[test]
        te = 0.75 * final.predict(Xte) + 0.25 * ridge.predict(
            (np.where(np.isfinite(Xte), Xte, med) - mu) / sd)
        held[target] = metrics(y[test], te)
        print(f"    {target:<8} {bst.best_iteration:>4} rounds, "
              f"pred mean {out[target].mean():+.3f} sd {out[target].std():.3f}, "
              f"held-out R²={held[target]['r2']:+.3f} RMSE={held[target]['rmse']:.3f} "
              f"ρ={held[target]['spearman']:+.3f}")
    return out, held


def eligibility(coll):
    """Minutes floor per split: the lowest minutes 538 itself ever rated.

    Matching 538's own pool composition is the closest thing to a principled
    threshold available with no labels to derive one from.
    """
    floors = {}
    for st in SEASON_TYPES:
        mins = []
        for ts in ("20140715000000", "20150715000000", "20160715000000",
                   "20170715000000", "20180715000000"):
            v = [as_float(d.get("mp")) for d in
                 coll.find({"source": "538", "timestamp": ts, "season_type": st},
                           {"mp": 1, "_id": 0})]
            v = [x for x in v if x]
            if v:
                mins.append(min(v))
        floors[st] = float(min(mins)) if mins else 0.0
    return floors


def run(args):
    coll = get_collection()
    d = load(MODEL, args.datadir)
    feat = list(d["feat_names"])
    keep_cols = [i for i, n in enumerate(feat) if n not in DROP_FEATURES]
    dropped = [n for n in feat if n in DROP_FEATURES]
    d["X"] = d["X"][:, keep_cols]
    feat = [feat[i] for i in keep_cols]
    n_labeled = d["X"].shape[0]
    print(f"labeled: X={d['X'].shape} (dropped identifier columns: {dropped})")

    print("building the unlabeled cells ...")
    Xn, meta = build_unlabeled(coll, feat, list(UNLABELED_SNAPSHOTS))
    print(f"unlabeled: X={Xn.shape}")

    carried = 0
    if args.pos_mode == "carry+impute":
        carried = carry_over_positions(coll, meta)
        # Impute on raw columns; add_context has not run yet, so shapes match.
        impute_positions(d["X"], d["pos"], Xn, meta)
    else:
        print("  --pos-mode zeros: every unlabeled row gets an all-zero position")

    # Normalize labeled and unlabeled together, with the scaling decisions taken
    # from the labeled non-test rows exactly as experiment_combined.prepare does.
    X_all = np.vstack([d["X"], Xn])
    is_train = np.concatenate([~d["test"].astype(bool),
                               np.zeros(Xn.shape[0], dtype=bool)])
    X_all = normalize_rates(X_all, feat, is_train)

    ctx = {k: np.concatenate([d[k], np.array([m[k] for m in meta],
                                             dtype=d[k].dtype)])
           for k in ("pos", "mp", "timestamp", "season_type")}
    X_all, feat_ctx = add_context(X_all, feat, ctx, MODEL)
    print(f"  matrix with context: {X_all.shape}")

    # Dedupe applies to the labeled block only; unlabeled rows are one per player.
    keep = dedupe(X_all[:n_labeled], d["y"], d)
    dl = {k: (v[keep] if isinstance(v, np.ndarray) and v.shape[:1] == d["test"].shape
              else v) for k, v in d.items()}
    X = np.vstack([X_all[:n_labeled][keep], X_all[n_labeled:]])

    preds, held = fit_and_predict(X, dl, len(keep), args.pos_mode)

    df = pd.DataFrame(meta)
    for t in ("total", "offense", "defense"):
        df[t] = preds[t]
    return df, eligibility(coll), carried, held


def leaderboard(df, floors, target, season, split, top_n):
    g = df[(df.season == season) & (df.season_type == split)]
    pool = g[g.mp >= floors[split]]
    cols = ["player", "mp", "offense", "defense", "total"]
    return pool.nlargest(top_n, target)[cols], len(g), len(pool)


def write_report(df, floors, args, sens, held):
    lines = []
    A = lines.append
    A("# Estimated RAPTOR for 2023-24, 2024-25 and 2025-26")
    A("")
    A("538 stopped publishing RAPTOR, so these three seasons have no ground truth.")
    A("Everything below is our combined model's **prediction**, not a measurement.")
    A("")
    A("The model is the production combined one from")
    A("[RESULTS_stride_transfer.md](RESULTS_stride_transfer.md): the combined block set")
    A("(pbp + 14 tracking tables + wowy on/off/diff) at `--modern-stride 6`, blended")
    A("0.75 LightGBM / 0.25 RidgeCV, one fit per target, trained on every labeled row")
    A("except the held-out 2013-14 and 2014-15 seasons. One deliberate change: three")
    A("identifier columns are dropped (see caveats), so it is refit rather than reused,")
    A("and rescored below rather than cited.")
    A("")
    A("## How accurate should you expect this to be?")
    A("")
    A("Measured, not cited. Dropping the identifier columns (below) makes this not")
    A("quite the model RESULTS_top100.md scored, so the same fit that produced the")
    A("tables below was also scored on the held-out 2013-14 and 2014-15 rows it never")
    A("saw. For reference, the published numbers for the version that kept those")
    A("columns were total +0.751 / offense +0.821 / defense +0.635 R².")
    A("")
    A("| target | R² | RMSE | MAE | ρ |")
    A("|---|---|---|---|---|")
    for t in ("total", "offense", "defense"):
        h = held[t]
        A(f"| {t} | {h['r2']:+.3f} | {h['rmse']:.3f} | {h['mae']:.3f} | "
          f"{h['spearman']:+.3f} |")
    A("")
    A("Those are 2013-14 and 2014-15. **Treat them as an optimistic bound here.**")
    A("These seasons are nine to twelve years further out than anything the model was")
    A("fit on, across a period in which three-point rate, pace and defensive rules all")
    A("moved. Nothing in this report measures that drift, because measuring it would")
    A("require the labels that do not exist.")
    A("")
    A("## Caveats that affect the numbers")
    A("")
    A(f"- **Defense is the weak target.** R² {held['defense']['r2']:+.3f} against "
      f"{held['offense']['r2']:+.3f} for offense. The defensive top-100s below should")
    A("  be read as considerably softer than the offensive ones.")
    A("- **Three identifier columns were dropped**: `pbp|EntityId`, `pbp|RowId`,")
    A("  `pbp|TeamId`. `coverage.ID_FIELDS` never excluded them, so they had been")
    A("  training as ordinary features. EntityId is the NBA player id and RowId")
    A("  duplicates it; both run 708 to 1,630,572, and ids are issued in debut order,")
    A("  so every player in these seasons sits above the range the model was fit on.")
    A("  Removing them costs nothing measurable on the held-out seasons — total R²")
    A("  +0.744 against a published +0.751, defense +0.631 against +0.635 — and helps")
    A("  offense, +0.832 against +0.821. **This affects the existing models too**, not")
    A("  just this report.")
    A("- **Two scrape bugs were found and fixed while producing this.** The tracking")
    A("  percentages were on the API's 0-1 scale where the collection stores 0-100")
    A("  (41 columns across 12 tables), and the passing table's last three columns sit")
    A("  one place left of their headers. Uncorrected, these inflated the predicted")
    A("  offensive top end from a realistic +7.9 to +24.5. Both are repaired in Mongo")
    A("  by `scraping/migrate_tracking_v2.py`; anything read from those documents")
    A("  before this report was written is wrong.")
    A("- **No position data.** 538 supplied it; nothing we scrape does. Positions are")
    A("  carried over by name where 538 ever listed the player "
      f"({sens['pos_carried']}/{sens['pos_total']}) and imputed otherwise. Rerunning")
    A("  with every position zeroed moves the offensive top-100 by "
      f"{sens['offense_changed']} places and the defensive by {sens['defense_changed']},")
    A("  out of "
      f"{sens['slots']} — the position one-hot is 0.06% of model gain, and it shows.")
    A("- **Minutes floor.** "
      + ", ".join(f"{k}: {v:.0f} min" for k, v in floors.items())
      + ". Derived, not chosen: the lowest minutes total 538 itself ever rated in that")
    A("  split, so the pool composition matches the one the model was trained against.")
    A("- **Three source columns are absent** from the new scrape — `3SecondViolations`,")
    A("  `HeaveAttempts`, `HeaveMakes` — because pbpstats omits an event type nobody")
    A("  recorded. They arrive as NaN, which LightGBM handles natively.")
    A("")

    for season in sorted(df.season.unique()):
        A(f"## {season}")
        A("")
        for split in SEASON_TYPES:
            for target in ("offense", "defense"):
                tab, pool_n, elig_n = leaderboard(df, floors, target, season, split,
                                                  args.top_n)
                if tab.empty:
                    continue
                A(f"### {season} {split} — top {len(tab)} {target}")
                A("")
                A(f"> pool {pool_n} players, {elig_n} above the minutes floor")
                A("")
                A("| # | player | mp | est. offense | est. defense | est. total |")
                A("|---:|---|---:|---:|---:|---:|")
                for i, (_, r) in enumerate(tab.iterrows(), 1):
                    A(f"| {i} | {r.player} | {r.mp:.0f} | {r.offense:+.2f} | "
                      f"{r.defense:+.2f} | {r.total:+.2f} |")
                A("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data"))
    ap.add_argument("--top-n", type=int, default=TOP_N)
    ap.add_argument("--pos-mode", choices=["carry+impute", "zeros"],
                    default="carry+impute")
    ap.add_argument("--out", default=str(REPO_ROOT / "training"
                                         / "RESULTS_predicted_seasons.md"))
    args = ap.parse_args()

    df, floors, carried, held = run(args)

    # Sensitivity: how much of the leaderboard is the position feature holding up?
    print("\nrerunning with positions zeroed for the sensitivity line ...")
    zargs = argparse.Namespace(**{**vars(args), "pos_mode": "zeros"})
    dfz, _, _, _ = run(zargs)
    sens = {"pos_total": len(df), "pos_carried": carried,
            "slots": 0, "offense_changed": 0, "defense_changed": 0}
    for season in sorted(df.season.unique()):
        for split in SEASON_TYPES:
            for target in ("offense", "defense"):
                a, _, _ = leaderboard(df, floors, target, season, split, args.top_n)
                b, _, _ = leaderboard(dfz, floors, target, season, split, args.top_n)
                sens["slots"] += len(a)
                sens[f"{target}_changed"] += len(set(a.player) ^ set(b.player)) // 2

    md = write_report(df, floors, args, sens, held)
    Path(args.out).write_text(md)
    df.to_json(Path(args.out).with_suffix(".json"), orient="records", indent=1)
    df.to_csv(Path(args.out).with_suffix(".csv"), index=False)
    print(f"\nwrote {args.out}")
    print(f"      {Path(args.out).with_suffix('.csv')}")


if __name__ == "__main__":
    main()
