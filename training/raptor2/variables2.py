"""On-off block v2: pre-adjusted ratings, per the document's actual pipeline.

v1 fed raw on/without ratings plus a naive own-3P% column into the ridge and
got sign flips (collinearity) and a defensive fit of rho 0.026. 538 adjust the
RATINGS first -- luck and strength of opposition -- then regress three clean
ratings. v2 reproduces that:

  luck adjustment      rating impact of 3P% deviation from the cell mean:
                       3 * (pct - cell_pct) * (3PA per 100 poss). Own shooting
                       for the offensive ratings; OPPONENT shooting (from the
                       opponent-WOWY block) for the defensive ratings -- the
                       DRAYMOND finding that opponent 3P results are noise.
  competition faced    opponent scoring rate while the player is on court
                       (defense) -- an explicit adjuster variable.

Variables per side: [on_adj, without_adj, diff_adj, competition].
"""

import numpy as np

OO2_NAMES_OFF = ["on_ortg_adj", "without_ortg_adj", "diff_ortg_adj",
                 "opp_pace_on"]
OO2_NAMES_DEF = ["on_drtg_adj", "without_drtg_adj", "diff_drtg_adj",
                 "comp_off_faced"]


def _per(v, denom, scale=100.0):
    out = np.full(len(denom), np.nan)
    ok = np.isfinite(v) & np.isfinite(denom) & (denom > 0)
    out[ok] = v[ok] * scale / denom[ok]
    return out


def _cell_mean(v, w, cells):
    out = np.full(len(v), np.nan)
    for c in np.unique(cells):
        m = cells == c
        ok = m & np.isfinite(v) & np.isfinite(w)
        if ok.sum() >= 20:
            out[m] = np.average(v[ok], weights=w[ok])
    return out


def build_onoff2(X, feat, opp_on, opp_off, ofields, cells, mp):
    cols = {n: i for i, n in enumerate(feat)}
    oc = {n: i for i, n in enumerate(ofields)}
    n = X.shape[0]

    def g(name):
        j = cols.get(name)
        return X[:, j].astype(np.float64) if j is not None \
            else np.full(n, np.nan)

    def go(block, name):
        j = oc.get(name)
        return block[:, j].astype(np.float64) if j is not None \
            else np.full(n, np.nan)

    w = np.sqrt(np.maximum(mp.astype(np.float64), 1.0))

    # ---------- offense: own scoring, own 3P luck ----------
    OFF = np.full((n, 4), np.nan)
    for k, pref in enumerate(("wowy_on", "wowy_off")):
        pts = g(f"{pref}|Points")
        poss = g(f"{pref}|OffPoss")
        rtg = _per(pts, poss)
        p3 = g(f"{pref}|Fg3Pct")
        a3 = _per(g(f"{pref}|FG3A"), poss)
        p3 = np.where(p3 > 1.5, p3 / 100.0, p3)
        lg = _cell_mean(p3, w, cells)
        luck = 3.0 * (p3 - lg) * a3
        OFF[:, k] = rtg - np.where(np.isfinite(luck), luck, 0.0)
    OFF[:, 2] = OFF[:, 0] - OFF[:, 1]
    # opponent pace faced on court (context var; offense-side competition
    # quality is not directly observable in our sources)
    OFF[:, 3] = _per(go(opp_on, "OffPoss"), g("wowy_on|MinutesOnCourt")
                     if "wowy_on|MinutesOnCourt" in cols else
                     g("wowy_on|Minutes"), 36.0)

    # ---------- defense: opponent scoring, OPPONENT 3P luck ----------
    DEF = np.full((n, 4), np.nan)
    for k, (pref, blk) in enumerate((("wowy_on", opp_on),
                                     ("wowy_off", opp_off))):
        opts = g(f"{pref}|OpponentPoints")
        dposs = g(f"{pref}|DefPoss")
        rtg = _per(opts, dposs)
        o3m = go(blk, "FG3M")
        o3a = go(blk, "FG3A")
        p3 = np.where(o3a > 0, o3m / np.where(o3a > 0, o3a, 1), np.nan)
        a3 = _per(o3a, dposs)
        lg = _cell_mean(p3, w, cells)
        luck = 3.0 * (p3 - lg) * a3
        # DRtg with opponent 3P luck removed; NEGATED so higher = better D
        DEF[:, k] = -(rtg - np.where(np.isfinite(luck), luck, 0.0))
    DEF[:, 2] = DEF[:, 0] - DEF[:, 1]
    DEF[:, 3] = _per(go(opp_on, "Points"), go(opp_on, "OffPoss"))

    return OFF, DEF
