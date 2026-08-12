"""Recreate 538's long-term RAPM target following Ryan Davis's tutorial.

Pipeline (per rd11490/NBA_Tutorials, adapted to run fully in-container):
  data      stats.nba.com play-by-play for 2013-14..2018-19 regular seasons via
            the shufinskiy/nba_data GitHub mirror (stats.nba.com itself refuses
            this container's IP)
  lineups   nba_on_court.players_on_court() reconstructs the ten players on the
            floor for every event (same author ecosystem the tutorial links)
  possess.  Davis's rules verbatim: a possession ends on turnover, defensive
            rebound, made shot that is not an and-1, made last free throw
            (excluding away-from-play/loose-ball/inbound-foul 1-of-1s), or
            period end; points counted for the possession team only
  ridge     his rapm.py exactly: X = [offense +1 | defense -1] sparse, y =
            100 * points per possession, RidgeCV over lambdas {.01,.05,.1}
            scaled by n (lambda_to_alpha), 5-fold, with intercept

538's two adjustments on top: no extra mean-reversion (plain ridge already),
and the pooled estimate re-centered to zero (possession-weighted).

Outputs data_fixed/rapm_recreated.npz:
  ids, names, orapm6/drapm6 (2013-14..2018-19 pooled -- 538's exact window),
  orapm4/drapm4 (2015-16..2018-19 pooled -- leak-free for scoring on the
  2013-14/2014-15 test seasons), poss6/poss4.

Run:  python training/build_rapm.py            (~30-60 min)
"""

import json
import math
import sys
import time
from pathlib import Path

import numpy as np

if not hasattr(np, "in1d"):
    np.in1d = np.isin   # numpy 2.x dropped the alias nba_on_court still uses

import pandas as pd
from scipy import sparse
from sklearn.linear_model import RidgeCV

sys.path.insert(0, "/tmp/NBA_Tutorials/play_by_play_parser")
import play_by_play_utils as U          # Davis's predicates, verbatim
import nba_on_court as noc
from nba_on_court import nba_on_court as noc_mod


class _Offline:
    """stats.nba.com is unreachable from this container; nba_on_court falls
    back to a boxscore request when a period's lineup can't be inferred from
    play-by-play alone. Fail that path instantly (no 5x10s timeouts) so the
    caller can salvage the game period by period."""

    def __init__(self, *a, **k):
        raise ConnectionError("offline container")


noc_mod.boxscoretraditionalv2.BoxScoreTraditionalV2 = _Offline
SKIPPED_PERIODS = [0]
SKIPPED_GAMES = [0]


def lineups_offline(g):
    """players_on_court with per-period salvage; drops unresolvable periods."""
    try:
        return noc.players_on_court(g)
    except (ConnectionError, ValueError):
        pass
    parts = []
    for per in sorted(g["PERIOD"].unique()):
        gp = g[g["PERIOD"] == per].reset_index(drop=True)
        try:
            parts.append(noc.players_on_court(gp))
        except (ConnectionError, ValueError):
            SKIPPED_PERIODS[0] += 1
    if not parts:
        SKIPPED_GAMES[0] += 1
        return None
    return pd.concat(parts, ignore_index=True)

from db import REPO_ROOT

TD = REPO_ROOT / "training"
BUILD = Path("/tmp/rapm_build")
BUILD.mkdir(exist_ok=True)
SEASONS6 = [2013, 2014, 2015, 2016, 2017, 2018]     # start years, RS
SEASONS4 = [2015, 2016, 2017, 2018]
LAMBDAS = [0.01, 0.05, 0.1]
HP = [f"HOME_PLAYER{i}" for i in range(1, 6)]
AP = [f"AWAY_PLAYER{i}" for i in range(1, 6)]


def add_time_elapsed(g):
    def elapsed(row):
        try:
            m, s = str(row["PCTIMESTRING"]).split(":")
            m, s = int(m), int(s)
        except (ValueError, AttributeError):
            return np.nan
        period = row["PERIOD"]
        maxm = 12 if period < 5 else 5
        in_period = ((maxm - m - 1) * 60) + (60 - s)
        base = (min(period, 5) - 1) * 720 + max(period - 5, 0) * 300
        return base + in_period
    g[U.time_elapsed] = g.apply(elapsed, axis=1)
    return g


def team_ids(g):
    """(home_id, away_id) from which side's description carries made shots."""
    made = g[g["EVENTMSGTYPE"] == 1]
    h = made[made["HOMEDESCRIPTION"].astype(str).str.len() > 0]
    a = made[made["VISITORDESCRIPTION"].astype(str).str.len() > 0]
    if not len(h) or not len(a):
        return None, None
    return (int(h["PLAYER1_TEAM_ID"].mode().iloc[0]),
            int(a["PLAYER1_TEAM_ID"].mode().iloc[0]))


def parse_game(g):
    """-> list of (off5, def5, points) for one game."""
    g = g.sort_values("EVENTNUM").reset_index(drop=True)
    g["HOMEDESCRIPTION"] = g["HOMEDESCRIPTION"].fillna("")
    g["VISITORDESCRIPTION"] = g["VISITORDESCRIPTION"].fillna("")
    g["NEUTRALDESCRIPTION"] = g["NEUTRALDESCRIPTION"].fillna("")
    g = add_time_elapsed(g)
    hid, aid = team_ids(g)
    if hid is None:
        return []
    g = lineups_offline(g)
    if g is None:
        return []
    rows = list(g.iterrows())

    out = []
    cur = []
    for ind, row in rows:
        if not U.is_substitution(row) and not U.is_end_of_period(row):
            cur.append(row)
        try:
            end = (U.is_turnover(row)
                   or U.is_last_free_throw_made(ind, row, rows)
                   or U.is_defensive_rebound(ind, row, rows)
                   or U.is_make_and_not_and_1(ind, row, rows)
                   or U.is_end_of_period(row))
        except (IndexError, KeyError):
            end = False
        if end and cur:
            last = cur[-1]
            team = _possession_team(last, hid, aid)
            if team is not None:
                pts = 0
                for p in cur:
                    if (U.is_made_shot(p) or (U.is_free_throw(p)
                                              and not U.is_miss(p))):
                        if p["PLAYER1_TEAM_ID"] == team:
                            pts += _points(p)
                home5 = [int(last[c]) for c in HP]
                away5 = [int(last[c]) for c in AP]
                off5, def5 = (home5, away5) if team == hid else (away5, home5)
                out.append((off5, def5, pts))
            cur = []
    return out


def _points(p):
    if U.is_free_throw(p) and not U.is_miss(p):
        return 1
    if U.is_made_shot(p):
        return 3 if U.is_three(p) else 2
    return 0


def _possession_team(p, hid, aid):
    try:
        if U.is_made_shot(p) or U.is_free_throw(p):
            return int(p["PLAYER1_TEAM_ID"])
        if U.is_rebound(p):
            reb = int(p["PLAYER1_ID"]) if U.is_team_rebound(p) \
                else int(p["PLAYER1_TEAM_ID"])
            return aid if reb == hid else hid
        if U.is_turnover(p):
            return int(p["PLAYER1_ID"]) if U.is_team_turnover(p) \
                else int(p["PLAYER1_TEAM_ID"])
        t = p["PLAYER1_TEAM_ID"]
        if isinstance(t, float) and math.isnan(t):
            t = p["PLAYER1_ID"]
        t = int(t)
        return t if t in (hid, aid) else None
    except (ValueError, TypeError):
        return None


def build_possessions(season):
    cache = BUILD / f"poss_{season}.csv"
    if cache.exists():
        return pd.read_csv(cache)
    csv = BUILD / f"nbastats_{season}.csv"
    if not csv.exists():
        noc.load_nba_data(path=BUILD, seasons=season, data="nbastats",
                          seasontype="rg", untar=True)
    df = pd.read_csv(csv)
    recs = []
    t0 = time.time()
    for k, (gid, g) in enumerate(df.groupby("GAME_ID", sort=True)):
        for off5, def5, pts in parse_game(g):
            recs.append(off5 + def5 + [pts])
        if (k + 1) % 200 == 0:
            print(f"  [{season}] {k+1} games, {len(recs):,} poss "
                  f"({time.time()-t0:.0f}s)", flush=True)
    cols = [f"o{i}" for i in range(5)] + [f"d{i}" for i in range(5)] + ["pts"]
    out = pd.DataFrame(recs, columns=cols)
    out["season"] = season
    out.to_csv(cache, index=False)
    print(f"[{season}] {len(out):,} possessions "
          f"(mean pts {out.pts.mean():.3f}; skipped periods so far "
          f"{SKIPPED_PERIODS[0]}, games {SKIPPED_GAMES[0]})", flush=True)
    return out


def fit_rapm(poss, tag):
    players = sorted(set(np.unique(poss[[f"o{i}" for i in range(5)]].values))
                     | set(np.unique(poss[[f"d{i}" for i in range(5)]].values)))
    pidx = {p: i for i, p in enumerate(players)}
    P, n = len(players), len(poss)
    rows = np.repeat(np.arange(n), 10)
    cols = np.empty(n * 10, dtype=np.int64)
    vals = np.empty(n * 10, dtype=np.float64)
    O = poss[[f"o{i}" for i in range(5)]].values
    D = poss[[f"d{i}" for i in range(5)]].values
    for j in range(5):
        cols[j::10] = [pidx[p] for p in O[:, j]]
        vals[j::10] = 1.0
        cols[5 + j::10] = [pidx[p] + P for p in D[:, j]]
        vals[5 + j::10] = -1.0
    X = sparse.csr_matrix((vals, (rows, cols)), shape=(n, 2 * P))
    y = 100.0 * poss["pts"].values.astype(np.float64)
    alphas = [(l * n) / 2.0 for l in LAMBDAS]
    clf = RidgeCV(alphas=alphas, cv=5, fit_intercept=True)
    model = clf.fit(X, y)
    lam = (model.alpha_ * 2.0) / n
    o = model.coef_[:P]
    d = model.coef_[P:]
    # possession-weighted recentering (538 re-zeroed their pooled target)
    cnt_o = np.bincount(cols[np.tile(np.arange(10) < 5, n)], minlength=2 * P)[:P]
    cnt_d = np.bincount(cols[np.tile(np.arange(10) >= 5, n)] - P,
                        minlength=P)
    o = o - np.average(o, weights=np.maximum(cnt_o, 1))
    d = d - np.average(d, weights=np.maximum(cnt_d, 1))
    print(f"[{tag}] {n:,} poss, {P} players, lambda={lam:.3f}, "
          f"intercept={model.intercept_:.2f}", flush=True)
    return players, o, d, cnt_o + cnt_d


def main():
    frames = [build_possessions(s) for s in SEASONS6]
    poss6 = pd.concat(frames, ignore_index=True)
    poss4 = poss6[poss6["season"].isin(SEASONS4)]

    p6, o6, d6, c6 = fit_rapm(poss6, "6y 2013-19")
    p4, o4, d4, c4 = fit_rapm(poss4, "4y 2015-19")
    m4 = {p: (o, d, c) for p, o, d, c in zip(p4, o4, d4, c4)}

    # id -> standard name via the roster files
    names = {}
    rd = REPO_ROOT / "scraping" / "rosters"
    for f in rd.glob("roster_*_Regular_season.json"):
        for pid, rec in json.loads(f.read_text())["players"].items():
            names.setdefault(pid, rec["name"])
    out_names = [names.get(str(p), f"id{p}") for p in p6]
    unnamed = sum(1 for n in out_names if n.startswith("id"))
    print(f"names resolved for {len(out_names)-unnamed}/{len(out_names)}",
          flush=True)
    np.savez_compressed(
        TD / "data_fixed" / "rapm_recreated.npz",
        ids=np.array([str(p) for p in p6]), names=np.array(out_names),
        orapm6=o6, drapm6=d6, poss6=c6,
        orapm4=np.array([m4.get(p, (np.nan,) * 3)[0] for p in p6]),
        drapm4=np.array([m4.get(p, (np.nan,) * 3)[1] for p in p6]),
        poss4=np.array([m4.get(p, (np.nan, np.nan, 0))[2] for p in p6]))
    top = np.argsort(-(o6 + d6))[:10]
    for i in top:
        print(f"  {out_names[i]:<24} O {o6[i]:+.2f}  D {d6[i]:+.2f}  "
              f"T {o6[i]+d6[i]:+.2f}", flush=True)
    print("wrote data_fixed/rapm_recreated.npz", flush=True)


if __name__ == "__main__":
    main()
