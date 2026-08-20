"""Historical-RAPTOR experiments against the 538 nba-raptor archive.

The archive (hist538/) carries three label regimes: box-only RAPTOR
1977-2000, box + single-year regularized plus-minus 2001-2013, full
player-tracking RAPTOR 2014+. Features: basketball-reference per-100 +
advanced season tables (bbref_hist.jsonl, ids join the CSVs exactly).

  E1  era-1 fit: can a plain box model reproduce the box-only era?
      Ridge + GBM on 1978-2000, season-held-out rho/R2, offense and defense,
      two feature sets (raw box rates; + derived metrics WS/BPM).
  E2  the RAPM gap: apply the era-1 model to 2001-2013. The residual IS the
      single-year-RAPM component 538 mixed in. Quantifies how much defense
      escapes the box by construction, and the shrinkage curve: residual
      variance as a function of possessions = how 538 regularized RAPM
      toward the box prior.
  E3  year-over-year stability of raptor_defense (and offense) by era --
      how much repeatable signal each measurement regime adds.
  E4  transfer hat: train box-D (and box-O) on 1978-2013 bbref features
      against historical labels, predict every 2014-2023 player-season, and
      align to combined.npz rows by normalized name x season. The hat's
      training data predates every CV fold and uses an external label source
      -- a fold-independent feature. Saved to hist_hat.npz.

Run:  python training/raptor2/hist_experiments.py
"""

import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scipy.stats import spearmanr
from sklearn.linear_model import RidgeCV

from db import REPO_ROOT
from estimated_raptor import norm_name
from experiment_combined import prepare

TD = REPO_ROOT / "training"
HERE = TD / "raptor2"

RAW_EXCLUDE = {"ranker", "awards", "name_display", "team_name_abbr", "pos"}
DERIVED = {"per", "ows", "dws", "ws", "ws_per_48", "obpm", "dbpm", "bpm",
           "vorp"}
GBM_PARAMS = dict(objective="l2", learning_rate=0.05, num_leaves=31,
                  min_data_in_leaf=60, feature_fraction=0.7,
                  bagging_fraction=0.8, bagging_freq=1, lambda_l2=5.0,
                  verbose=-1, num_threads=0, seed=0)


def load_bbref():
    import gzip
    fp = HERE / "bbref_hist.jsonl"
    text = fp.read_text() if fp.exists() else gzip.open(
        HERE / "bbref_hist.jsonl.gz", "rt").read()
    recs = {}
    for line in text.splitlines():
        rec = json.loads(line)
        for pid, cells in rec["rows"].items():
            recs.setdefault((pid, rec["season"]), {}).update(cells)
    rows = []
    for (pid, season), cells in recs.items():
        row = {"pid": pid, "season": season,
               "name": cells.get("name_display", "")}
        for k, v in cells.items():
            if k in RAW_EXCLUDE:
                continue
            try:
                row[k] = float(v)
            except (TypeError, ValueError):
                pass
        rows.append(row)
    return pd.DataFrame(rows)


def feature_sets(df):
    num = [c for c in df.columns if c not in
           ("pid", "season", "name") and df[c].dtype != object]
    raw = [c for c in num if c not in DERIVED]
    return raw, num          # (raw, raw+derived)


def ridge_fit(Xtr, ytr, wtr, Xte):
    med = np.nanmedian(Xtr, axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    A = np.where(np.isfinite(Xtr), Xtr, med)
    B = np.where(np.isfinite(Xte), Xte, med)
    mu, sd = A.mean(0), A.std(0)
    sd[sd == 0] = 1.0
    m = RidgeCV(alphas=np.logspace(-3, 5, 33)).fit(
        (A - mu) / sd, ytr, sample_weight=wtr)
    return m.predict((B - mu) / sd)


def gbm_fit(Xtr, ytr, wtr, Xte, rounds=600):
    m = lgb.train(GBM_PARAMS, lgb.Dataset(Xtr, ytr, weight=wtr),
                  num_boost_round=rounds)
    return m.predict(Xte)


def season_cv(df, cols, ycol, seasons, model):
    rho, r2 = [], []
    X = df[cols].values.astype(np.float64)
    y = df[ycol].values.astype(np.float64)
    w = np.sqrt(np.maximum(df["poss"].values.astype(np.float64), 1.0) / 100)
    for s in seasons:
        te = df["season"].values == s
        tr = np.isin(df["season"].values, seasons) & ~te
        p = model(X[tr], y[tr], w[tr], X[te])
        rho.append(spearmanr(y[te], p).statistic)
        r2.append(1 - np.average((y[te] - p) ** 2, weights=w[te])
                  / np.average((y[te] - np.average(y[te], weights=w[te]))
                               ** 2, weights=w[te]))
    return float(np.median(rho)), float(np.median(r2))


def main():
    bb = load_bbref()
    hist = pd.read_csv(HERE / "hist538" / "historical_RAPTOR_by_player.csv")
    df = bb.merge(hist, left_on=["pid", "season"],
                  right_on=["player_id", "season"], how="inner",
                  suffixes=("", "_538"))
    print(f"bbref rows {len(bb)}, joined {len(df)} "
          f"({len(df)/len(hist[hist.season>=1977]):.1%} of 538 rows)",
          flush=True)
    df = df[df["mp"].fillna(0) >= 250].reset_index(drop=True)
    raw, rawd = feature_sets(bb)
    raw = [c for c in raw if c in df.columns]
    rawd = [c for c in rawd if c in df.columns]
    print(f"features: raw {len(raw)}, +derived {len(rawd)}; "
          f"rows with mp>=250: {len(df)}", flush=True)

    era1 = list(range(1978, 2001))
    era2 = list(range(2001, 2014))
    era3 = list(range(2014, 2023))
    out = {}

    # ---- E1: box-only era fit --------------------------------------------
    print("\n== E1: 1978-2000 (box-only labels), season-held-out ==",
          flush=True)
    d1 = df[df["season"].isin(era1)]
    for ycol in ("raptor_offense", "raptor_defense"):
        for cols, tag in ((raw, "raw"), (rawd, "raw+derived")):
            for model, mtag in ((ridge_fit, "ridge"), (gbm_fit, "gbm")):
                rho, r2 = season_cv(d1, cols, ycol, era1, model)
                out[f"E1|{ycol}|{tag}|{mtag}"] = {"rho": round(rho, 3),
                                                  "r2": round(r2, 3)}
                print(f"  {ycol:<16} {tag:<12} {mtag:<6} rho {rho:+.3f} "
                      f"R2 {r2:+.3f}", flush=True)

    # ---- E2: the RAPM gap -------------------------------------------------
    print("\n== E2: era-1 model applied to 2001-2013 (box+RAPM labels) ==",
          flush=True)
    d2 = df[df["season"].isin(era2)].reset_index(drop=True)
    for ycol in ("raptor_offense", "raptor_defense"):
        Xtr = d1[rawd].values.astype(np.float64)
        ytr = d1[ycol].values.astype(np.float64)
        wtr = np.sqrt(np.maximum(d1["poss"].values, 1.0) / 100)
        p2 = gbm_fit(Xtr, ytr, wtr, d2[rawd].values.astype(np.float64))
        y2 = d2[ycol].values.astype(np.float64)
        rho = spearmanr(y2, p2).statistic
        # refit ON era 2 (so the gap is not distribution shift):
        rho_in, r2_in = season_cv(d2, rawd, ycol, era2, gbm_fit)
        resid = y2 - p2
        poss = d2["poss"].values.astype(np.float64)
        qs = np.quantile(poss, [0, .2, .4, .6, .8, 1.0])
        var_by_bin = [float(np.var(resid[(poss >= a) & (poss <= b)]))
                      for a, b in zip(qs[:-1], qs[1:])]
        out[f"E2|{ycol}"] = {
            "rho_transfer": round(float(rho), 3),
            "rho_refit_cv": round(rho_in, 3), "r2_refit_cv": round(r2_in, 3),
            "resid_var_by_poss_quintile": [round(v, 2) for v in var_by_bin]}
        print(f"  {ycol:<16} transfer rho {rho:+.3f} | era2-refit CV rho "
              f"{rho_in:+.3f} R2 {r2_in:+.3f}", flush=True)
        print(f"    residual variance by poss quintile: "
              f"{[round(v,2) for v in var_by_bin]}", flush=True)

    # ---- E3: year-over-year stability by era ------------------------------
    print("\n== E3: YoY stability (poss>=1500 both years) ==", flush=True)
    st = df[df["poss"].fillna(0) >= 1500]
    idx = {(r.pid, r.season): i for i, r in
           zip(st.index, st.itertuples())}
    for ycol in ("raptor_offense", "raptor_defense"):
        y = st[ycol].values
        for era, tag in ((era1, "1978-2000 box"),
                         (era2, "2001-2013 box+RAPM"),
                         (era3, "2014-2022 tracking")):
            a, b = [], []
            for (pid, s), i in idx.items():
                j = idx.get((pid, s + 1))
                if j is not None and s in era and s + 1 in era:
                    a.append(st.loc[i, ycol])
                    b.append(st.loc[j, ycol])
            r = float(np.corrcoef(a, b)[0, 1])
            out[f"E3|{ycol}|{tag}"] = {"yoy_r": round(r, 3), "n": len(a)}
            print(f"  {ycol:<16} {tag:<20} r(t,t+1) {r:+.3f} (n={len(a)})",
                  flush=True)

    # ---- E4: transfer hat for the current pipeline ------------------------
    print("\n== E4: 1978-2013-trained hats predicted onto 2014-2023 ==",
          flush=True)
    dtr = df[df["season"].isin(era1 + era2)]
    dte = bb[bb["season"].isin(range(2014, 2024))].reset_index(drop=True)
    hats = {}
    for ycol in ("raptor_offense", "raptor_defense"):
        Xtr = dtr[rawd].values.astype(np.float64)
        ytr = dtr[ycol].values.astype(np.float64)
        wtr = np.sqrt(np.maximum(dtr["poss"].values, 1.0) / 100)
        Xte = dte[[c for c in rawd]].values.astype(np.float64)
        hats[ycol] = 0.5 * gbm_fit(Xtr, ytr, wtr, Xte) \
            + 0.5 * ridge_fit(Xtr, ytr, wtr, Xte)
        # sanity: rho vs the 2014+ historical labels where present
        chk = dte.merge(hist, left_on=["pid", "season"],
                        right_on=["player_id", "season"], how="left",
                        suffixes=("", "_538"))
        m = np.isfinite(chk[ycol].values)
        print(f"  {ycol}: rho vs 2014+ labels {spearmanr(chk[ycol].values[m], hats[ycol][m]).statistic:+.3f} "
              f"(n={int(m.sum())})", flush=True)

    # align to combined.npz rows by normalized name + season end-year
    X, feat, d = prepare(str(TD / "data_fixed"))
    names = np.array([norm_name(str(p)) for p in d["player"]])
    end_year = np.array([int(str(s)[:4]) + 1 for s in d["season"]])
    key = {}
    dupes = set()
    for i, r in dte.iterrows():
        k = (norm_name(str(r["name"])), int(r["season"]))
        if k in key:
            dupes.add(k)
        key[k] = i
    H = np.full((len(names), 2), np.nan)
    hit = 0
    for i, (nm, yr) in enumerate(zip(names, end_year)):
        k = (nm, yr)
        if k in key and k not in dupes:
            j = key[k]
            H[i, 0] = hats["raptor_offense"][j]
            H[i, 1] = hats["raptor_defense"][j]
            hit += 1
    print(f"  aligned {hit}/{len(names)} combined.npz rows "
          f"({len(dupes)} ambiguous name-seasons dropped)", flush=True)
    np.savez_compressed(HERE / "hist_hat.npz", H=H,
                        names=np.array(["hist_boxo_hat", "hist_boxd_hat"]))
    (HERE / "RESULTS_hist_experiments.json").write_text(
        json.dumps(out, indent=1))
    print("wrote raptor2/hist_hat.npz + RESULTS_hist_experiments.json",
          flush=True)


if __name__ == "__main__":
    main()
