"""Neil Paine's Estimated RAPTOR, recreated and scored on our held-out seasons.

Source: https://github.com/Neil-Paine-1/NBA-elo  (Paine built RAPTOR at 538).
Estimated RAPTOR is a linear model over per-100-possession box score actions plus
on-court / on-off plus-minus, with published weights, a position adjustment and a
team adjustment.

This module does two things:
  1. joins Paine's *published* eRO/eRD/eRT to our test rows, and
  2. recreates the formula from our own Mongo features and checks it against his
     published values before scoring it.

IMPORTANT: Paine fit these weights on full RAPTOR from 2014-2023, which contains
both of our test seasons. His numbers are in-sample here; ours are not.

Run:  python training/estimated_raptor.py --repo /tmp/NBA-elo
"""

import argparse
import json
import difflib
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from coverage import as_float, pick_doc
from db import REPO_ROOT, get_collection

# season -> (our wayback snapshot, Paine's Year)
TEST_SEASONS = {"2013-14": ("20140715000000", 2014), "2014-15": ("20150715000000", 2015)}
SPLITS = {"Regular season": "RS", "Playoffs": "PO"}

# Published weights, post-1997 version (the one with plus-minus inputs).
W_OFF = {"(Intercept)": -3.88704, "MPG": 0.026112, "PTS": 0.662784, "TSA": -0.51622,
         "AST": 0.430454, "TOV": -0.893465, "ORB": 0.303023, "DRB": -0.085637,
         "STL": 0.418092, "BLK": -0.230734, "PF": -0.108369,
         "OnCourt": 0.018381, "OnOff": 0.032054}
W_DEF = {"(Intercept)": -3.079144, "MPG": 0.033637, "PTS": -0.081412, "TSA": 0.025422,
         "AST": -0.025109, "TOV": -0.055809, "ORB": -0.099034, "DRB": 0.191569,
         "STL": 1.150891, "BLK": 0.611107, "PF": 0.010649,
         "OnCourt": 0.089391, "OnOff": 0.021717}

# Minute-weighted leaguewide targets each position must average after adjustment.
POS_TARGETS = {"PG": (0.3, -0.3), "SG": (0.2, -0.2), "SF": (0.0, 0.0),
               "PF": (-0.2, 0.2), "C": (-0.5, 0.5)}
POS_COLS = ["PG", "SG", "SF", "PF", "C"]


def norm_name(s):
    """Alphanumeric-only key.

    Paine's CSV has lost its diacritics to literal '?' ("Bojan Bogdanovi?",
    "Ersan ?lyasova") and our standard_name drops hyphens entirely
    ("Kentavious CaldwellPope"), so anything but letters and digits is noise.
    """
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s.lower())
    return re.sub(r"[^a-z0-9]", "", s)


def match_rows(ours, paine):
    """Exact key match, then difflib fallback within the same (year, split)."""
    lookup = defaultdict(dict)
    for i, r in paine.iterrows():
        lookup[(r["Year"], r["Type"])][r["key"]] = i
    idx, how = [], []
    for _, r in ours.iterrows():
        pool = lookup.get((r["year"], r["ptype"]), {})
        if r["key"] in pool:
            idx.append(pool[r["key"]]); how.append("exact"); continue
        close = difflib.get_close_matches(r["key"], list(pool), n=1, cutoff=0.85)
        if close:
            idx.append(pool[close[0]]); how.append("fuzzy")
        else:
            idx.append(-1); how.append("none")
    return np.array(idx), np.array(how)


# ---------------------------------------------------------------- our labels
def load_our_test_rows(coll):
    """538 labels for the four held-out cells, plus the raw stats the formula needs."""
    need_pbp = ["Points", "Assists", "Turnovers", "Steals", "Blocks", "Fouls",
                "OffRebounds", "DefRebounds", "Minutes", "GamesPlayed",
                "OffPoss", "DefPoss", "TotalPoss", "FG2A", "FG3A", "FTA"]
    rows = []
    for season, (ts, year) in TEST_SEASONS.items():
        for split, ptype in SPLITS.items():
            labels = {}
            for d in coll.find({"timestamp": ts, "source": "538", "season_type": split}):
                labels.setdefault(d["standard_name"], d)

            blocks = {}
            for key, q in (("pbp", {"source": "pbp"}),
                           ("on", {"source": "wowy", "on_or_off": "on"}),
                           ("off", {"source": "wowy", "on_or_off": "off"})):
                grp = defaultdict(list)
                for d in coll.find({"timestamp": ts, "season_type": split, **q}):
                    if d["standard_name"] in labels:
                        grp[d["standard_name"]].append(d)
                blocks[key] = {n: pick_doc(v) for n, v in grp.items()}

            for name, lab in labels.items():
                r = {"player": name, "season": season, "year": year,
                     "split": split, "ptype": ptype,
                     "team": lab.get("team"), "pos": lab.get("pos", ""),
                     "mp": as_float(lab.get("mp")) or 0.0,
                     "rap_o": as_float(lab.get("rap_o")),
                     "rap_d": as_float(lab.get("rap_d")),
                     "rap": as_float(lab.get("rap"))}
                p = blocks["pbp"].get(name)
                if p:
                    for f in need_pbp:
                        r[f] = as_float(p.get(f))
                for side in ("on", "off"):
                    w = blocks[side].get(name)
                    if w:
                        r[f"{side}_pm"] = as_float(w.get("PlusMinus"))
                        r[f"{side}_poss"] = as_float(w.get("TotalPoss"))
                rows.append(r)
    return pd.DataFrame(rows)


# ------------------------------------------------------- formula recreation
def build_inputs(df, per100="side"):
    """Per-100-possession inputs. `per100` picks the denominator convention:
    'side'  -> offensive stats per 100 OffPoss, defensive per 100 DefPoss
    'total' -> everything per 100 TotalPoss
    """
    x = pd.DataFrame(index=df.index)
    # An absent counting field means the player recorded none of that action
    # (no 3PA -> no FG3A key), so zero-fill counts. Denominators stay NaN.
    counts = ["Points", "Assists", "Turnovers", "Steals", "Blocks", "Fouls",
              "OffRebounds", "DefRebounds", "FG2A", "FG3A", "FTA"]
    df = df.copy()
    for c in counts:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    off = df["OffPoss"] if per100 == "side" else df["TotalPoss"]
    dfn = df["DefPoss"] if per100 == "side" else df["TotalPoss"]
    x["MPG"] = df["Minutes"] / df["GamesPlayed"].replace(0, np.nan)
    x["PTS"] = df["Points"] / off * 100
    x["TSA"] = (df["FG2A"] + df["FG3A"] + 0.44 * df["FTA"]) / off * 100
    x["AST"] = df["Assists"] / off * 100
    x["TOV"] = df["Turnovers"] / off * 100
    x["ORB"] = df["OffRebounds"] / off * 100
    x["DRB"] = df["DefRebounds"] / dfn * 100
    x["STL"] = df["Steals"] / dfn * 100
    x["BLK"] = df["Blocks"] / dfn * 100
    x["PF"] = df["Fouls"] / dfn * 100
    on_rtg = df["on_pm"] / df["on_poss"].replace(0, np.nan) * 100
    off_rtg = df["off_pm"] / df["off_poss"].replace(0, np.nan) * 100
    x["OnCourt"] = on_rtg
    x["OnOff"] = on_rtg - off_rtg
    return x


def apply_weights(x, w):
    out = np.full(len(x), w["(Intercept)"], dtype=float)
    for k, v in w.items():
        if k == "(Intercept)":
            continue
        out = out + v * x[k].to_numpy(dtype=float)
    return out


def pos_shares(pos_str):
    toks = [p.strip().upper() for p in str(pos_str or "").split(",") if p.strip()]
    toks = [t for t in toks if t in POS_COLS]
    if not toks:
        return {c: 0.0 for c in POS_COLS}
    return {c: (1.0 / len(toks) if c in toks else 0.0) for c in POS_COLS}


def position_adjust(df, ero, erd):
    """Shift ratings so each position's minute-weighted league average hits target."""
    shares = pd.DataFrame([pos_shares(p) for p in df["pos"]], index=df.index)
    mp = df["mp"].to_numpy(dtype=float)
    ero, erd = ero.copy(), erd.copy()
    finite = np.isfinite(ero) & np.isfinite(erd) & np.isfinite(mp)
    for c in POS_COLS:
        w = shares[c].to_numpy() * mp * finite
        if not np.isfinite(w).any() or np.nansum(w) <= 0:
            continue
        # average over finite rows only -- one NaN would otherwise poison every row
        cur_o = np.average(ero[finite], weights=w[finite])
        cur_d = np.average(erd[finite], weights=w[finite])
        tgt_o, tgt_d = POS_TARGETS[c]
        # move each player toward the target in proportion to their position share
        ero = ero + shares[c].to_numpy() * (tgt_o - cur_o)
        erd = erd + shares[c].to_numpy() * (tgt_d - cur_d)
    return ero, erd


# ------------------------------------------------------------------ scoring
def metrics(y, p):
    m = np.isfinite(y) & np.isfinite(p)
    y, p = np.asarray(y)[m], np.asarray(p)[m]
    if len(y) < 3:
        return {"n": int(len(y))}
    return {"n": int(len(y)),
            "rmse": float(np.sqrt(np.mean((y - p) ** 2))),
            "mae": float(np.mean(np.abs(y - p))),
            "r2": float(1 - np.sum((y - p) ** 2) / np.sum((y - y.mean()) ** 2)),
            "pearson": float(np.corrcoef(y, p)[0, 1]),
            "spearman": float(pd.Series(y).corr(pd.Series(p), method="spearman"))}


def load_paine(repo):
    df = pd.read_csv(Path(repo) / "nba_estimated_RAPTOR.csv", encoding="latin-1")
    df = df[df.Year.isin([2014, 2015]) & df.Type.isin(["RS", "PO"])].copy()
    df["key"] = df.Player.map(norm_name)
    # traded players have one row per team; collapse minute-weighted
    def agg(g):
        w = g["MP"].to_numpy(dtype=float)
        w = w if w.sum() > 0 else np.ones(len(g))
        return pd.Series({"MP": g["MP"].sum(),
                          "eRO": np.average(g["eRO"], weights=w),
                          "eRD": np.average(g["eRD"], weights=w),
                          "eRT": np.average(g["eRT"], weights=w)})
    return df.groupby(["key", "Year", "Type"], as_index=False).apply(
        agg, include_groups=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="/tmp/NBA-elo")
    ap.add_argument("--out", default=str(REPO_ROOT / "training" / "RESULTS_estimated_raptor.md"))
    args = ap.parse_args()

    print("loading our held-out rows from Mongo ...")
    ours = load_our_test_rows(get_collection())
    ours["key"] = ours.player.map(norm_name)
    print(f"  {len(ours)} rows  ({ours.groupby(['season','split']).size().to_dict()})")

    paine = load_paine(args.repo)
    idx, how = match_rows(ours, paine)
    merged = ours.reset_index(drop=True).copy()
    for c in ("eRO", "eRD", "eRT"):
        merged[c] = [paine.loc[i, c] if i >= 0 else np.nan for i in idx]
    merged["match"] = how
    matched = merged.eRT.notna().to_numpy()
    print(f"  matched to Paine's published values: {matched.sum()}/{len(merged)} "
          f"({(how=='exact').sum()} exact, {(how=='fuzzy').sum()} fuzzy)")
    unmatched = sorted(merged.loc[~matched, "player"].unique())
    if unmatched:
        print(f"  unmatched ({len(unmatched)}): {unmatched[:15]}")

    # ---- recreate the formula, pick the per-100 convention by fidelity -----
    print("\nrecreating the formula from our Mongo features:")
    best_conv, best_corr = None, -2
    for conv in ("side", "total"):
        x = build_inputs(merged, per100=conv)
        ero_raw, erd_raw = apply_weights(x, W_OFF), apply_weights(x, W_DEF)
        ero, erd = position_adjust(merged, ero_raw, erd_raw)
        ok = (matched & np.isfinite(ero) & np.isfinite(erd)
              & merged.eRO.notna().to_numpy())
        if ok.sum() < 10:
            print(f"  per-100 convention '{conv}': too few usable rows ({ok.sum()})")
            continue
        c = (np.corrcoef(ero[ok], merged.loc[ok, "eRO"])[0, 1]
             + np.corrcoef(erd[ok], merged.loc[ok, "eRD"])[0, 1]) / 2
        print(f"  per-100 convention '{conv}': mean corr vs published "
              f"eRO/eRD = {c:.4f}")
        if c > best_corr:
            best_conv, best_corr = conv, c
            merged["my_eRO"], merged["my_eRD"] = ero, erd
    if best_conv is None:
        raise SystemExit("no usable per-100 convention -- check the input columns")
    merged["my_eRT"] = merged.my_eRO + merged.my_eRD
    print(f"  -> using '{best_conv}'")

    fid = {t: metrics(merged.loc[matched, f"e{t}"], merged.loc[matched, f"my_e{t}"])
           for t in ("RO", "RD", "RT")}
    print("  recreation vs Paine's published values:")
    for t, m in fid.items():
        print(f"    e{t}: n={m['n']} rmse={m['rmse']:.3f} pearson={m['pearson']:.4f}")

    # ---- score both against our 538 labels --------------------------------
    print("\nscoring against 538 RAPTOR on our held-out seasons:")
    results = {}
    for label, pred_col, truth in (("published_eRO", "eRO", "rap_o"),
                                   ("published_eRD", "eRD", "rap_d"),
                                   ("published_eRT", "eRT", "rap"),
                                   ("recreated_eRO", "my_eRO", "rap_o"),
                                   ("recreated_eRD", "my_eRD", "rap_d"),
                                   ("recreated_eRT", "my_eRT", "rap")):
        sub = merged[matched]
        results[label] = {"all": metrics(sub[truth], sub[pred_col])}
        for (season, split), g in sub.groupby(["season", "split"]):
            results[label][f"{season} {split}"] = metrics(g[truth], g[pred_col])
        m = results[label]["all"]
        print(f"  {label:<16} n={m['n']:<4} rmse={m['rmse']:6.3f}  r2={m['r2']:+.3f}  "
              f"pearson={m['pearson']:+.3f}  spearman={m['spearman']:+.3f}")

    merged.to_csv(Path(args.out).with_suffix(".csv"), index=False)
    json.dump({"convention": best_conv, "fidelity": fid, "results": results,
               "n_matched": int(matched.sum()), "n_rows": int(len(merged))},
              open(Path(args.out).with_suffix(".json"), "w"), indent=2)
    print(f"\nwrote {Path(args.out).with_suffix('.json')}")
    return merged, results, fid, best_conv


if __name__ == "__main__":
    main()
