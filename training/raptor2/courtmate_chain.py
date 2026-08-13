"""Build RAPTOR's three-level on/off courtmate chain from possession lineups.

The methodology uses, for each side of the ball:

1. the player's own on-court rating;
2. courtmates' ratings when they were not sharing the floor with the player,
   weighted by ``shared possessions * apart possessions``;
3. those courtmates' other-courtmate ratings (one additional graph step).

The checked-in ``wowy`` tables contain (1) and a team-without-player proxy for
(2), but not the pair-level relationships.  ``build_rapm.py`` can produce the
required ``poss_<year>.csv`` files.  This module turns one such possession file
into the missing chain without O(rotation²) API calls.

FiveThirtyEight did not publish the exact third-step normalization.  We use the
literal graph interpretation: compute step 2 for every player, then average the
step-2 values of the focal player's courtmates with the same published
shared×apart weights.  Opponent quality is emitted as a fourth column instead
of silently folding in an undocumented competition-adjustment coefficient.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _side_features(lineups, points, higher_is_better=True):
    lineups = np.asarray(lineups)
    points = np.asarray(points, dtype=float)
    if lineups.ndim != 2 or lineups.shape[1] != 5:
        raise ValueError("lineups must have shape (possessions, 5)")
    if len(points) != len(lineups):
        raise ValueError("points and lineups must have the same number of rows")
    players = np.unique(lineups)
    pidx = {p: i for i, p in enumerate(players)}
    n = len(players)
    poss = np.zeros(n, dtype=float)
    pts = np.zeros(n, dtype=float)
    shared_poss = np.zeros((n, n), dtype=float)
    shared_pts = np.zeros((n, n), dtype=float)

    for lineup, value in zip(lineups, points):
        ids = np.array([pidx[p] for p in lineup], dtype=int)
        poss[ids] += 1.0
        pts[ids] += value
        shared_poss[np.ix_(ids, ids)] += 1.0
        shared_pts[np.ix_(ids, ids)] += value

    sign = 1.0 if higher_is_better else -1.0
    own = sign * np.divide(100.0 * pts, poss,
                           out=np.full(n, np.nan), where=poss > 0)
    step2 = np.full(n, np.nan)
    weights = np.zeros((n, n), dtype=float)
    without = np.full((n, n), np.nan)
    for i in range(n):
        apart = poss - shared_poss[i]
        apart_pts = pts - shared_pts[i]
        ok = (np.arange(n) != i) & (shared_poss[i] > 0) & (apart > 0)
        without[i, ok] = sign * 100.0 * apart_pts[ok] / apart[ok]
        weights[i, ok] = shared_poss[i, ok] * apart[ok]
        valid = ok & np.isfinite(without[i])
        if weights[i, valid].sum() > 0:
            step2[i] = np.average(without[i, valid], weights=weights[i, valid])

    # One and only one further graph iteration, matching the article's finding
    # that additional courtmate levels added no predictive information.
    step3 = np.full(n, np.nan)
    for i in range(n):
        ok = (weights[i] > 0) & np.isfinite(step2)
        if weights[i, ok].sum() > 0:
            step3[i] = np.average(step2[ok], weights=weights[i, ok])
    return players, own, step2, step3, poss


def courtmate_chain_features(offense_lineups, defense_lineups, points):
    """Return one row per player with offense/defense chain and competition.

    Defensive ratings are negated points allowed per 100 so that larger values
    consistently mean better performance.  ``*_opp_quality`` is the possession-
    weighted mean raw rating of the five opponents faced and is deliberately a
    separate regression input.
    """
    offense_lineups = np.asarray(offense_lineups)
    defense_lineups = np.asarray(defense_lineups)
    if offense_lineups.shape != defense_lineups.shape:
        raise ValueError("offense and defense lineups must have equal shape")
    po, oo, o2, o3, oposs = _side_features(offense_lineups, points, True)
    pd_, dd, d2, d3, dposs = _side_features(defense_lineups, points, False)
    players = np.unique(np.concatenate([po, pd_]))
    oi, di = {p: i for i, p in enumerate(po)}, {p: i for i, p in enumerate(pd_)}

    # Raw opponent quality faced.  The final component regression determines
    # the adjustment sign/magnitude rather than baking in an unpublished value.
    opp_o = {p: [0.0, 0.0] for p in players}
    opp_d = {p: [0.0, 0.0] for p in players}
    for off, deff in zip(offense_lineups, defense_lineups):
        mean_def = np.nanmean([dd[di[p]] for p in deff])
        mean_off = np.nanmean([oo[oi[p]] for p in off])
        for p in off:
            opp_o[p][0] += mean_def
            opp_o[p][1] += 1
        for p in deff:
            opp_d[p][0] += mean_off
            opp_d[p][1] += 1

    records = []
    for p in players:
        a, b = oi.get(p), di.get(p)
        records.append({
            "player_id": p,
            "off_on": oo[a] if a is not None else np.nan,
            "off_courtmates_without": o2[a] if a is not None else np.nan,
            "off_courtmates_courtmates": o3[a] if a is not None else np.nan,
            "off_opp_quality": opp_o[p][0] / opp_o[p][1] if opp_o[p][1] else np.nan,
            "def_on": dd[b] if b is not None else np.nan,
            "def_courtmates_without": d2[b] if b is not None else np.nan,
            "def_courtmates_courtmates": d3[b] if b is not None else np.nan,
            "def_opp_quality": opp_d[p][0] / opp_d[p][1] if opp_d[p][1] else np.nan,
            "off_poss": oposs[a] if a is not None else 0.0,
            "def_poss": dposs[b] if b is not None else 0.0,
        })
    return pd.DataFrame(records)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("possessions", help="poss_<year>.csv from build_rapm.py")
    ap.add_argument("--out", help="output CSV; defaults beside the input")
    args = ap.parse_args()
    p = Path(args.possessions)
    df = pd.read_csv(p)
    offense = df[[f"o{i}" for i in range(5)]].to_numpy()
    defense = df[[f"d{i}" for i in range(5)]].to_numpy()
    out = courtmate_chain_features(offense, defense, df["pts"].to_numpy())
    target = Path(args.out) if args.out else p.with_name(p.stem + "_courtchain.csv")
    out.to_csv(target, index=False)
    print(f"wrote {target} ({len(out)} players)")


if __name__ == "__main__":
    main()
