"""Faithful construction of 538's RAPTOR regression variables, one per section.

This is the ground-up rebuild: instead of feeding ~1,100 raw columns to a
learner, construct the ~35 variables the methodology document actually
describes, with its published constants, and let the component regressions fit
their coefficients. Every variable cites its section.

Data limits, stated once (details in RESULTS_raptor_coverage.md):
  no dunk split            rim shots use the layup EV (1.16); dunks (1.83) fold in
  no positional matchups   the two positional-opponent variables cannot be built
  no courtmate chain       on-off step 2 uses team-without-player (wowy_off),
                           step 3 (courtmates' courtmates) is absent
  no score effects         possession-level margins not collected

All rate variables are per-100 offensive/defensive possessions where the doc
implies rates ("adjusted relative to league average" happens downstream, per
cell, in structural.py).

Outputs (build_variables()): dict with
  OB  offense box variables      (n x len(OB_NAMES))
  DB  defense box variables      (n x len(DB_NAMES))
  OO  on-off ratings per side    (n x len(OO_NAMES))
row-aligned with data_fixed/combined.npz.
"""

import numpy as np

# --- 538 constants, verbatim from the methodology text ----------------------
SHOT_EV = {"AtRim": 1.16, "ShortMidRange": 0.82, "LongMidRange": 0.80,
           "Corner3": 1.16, "Arc3": 1.05}          # heaves 0.08 handled apart
HEAVE_EV = 0.08
COVER_W = {"sd:shots-def0-2|FG3A": 1.00, "sd:shots-def2-4|FG3A": 0.80,
           "sd:shots-def4-6|FG3A": 0.57, "sd:shots-def6plus|FG3A": 0.31}
ISO_FULL_W, ISO_PART_W = 1.00, 0.75                # iso-turnover class weights
FB_STEAL, FB_BLOCK = 0.20, 0.11                    # fast-break start credits
NONSHOOT_DRAWN = 0.16
PENALTY_FOUL = 0.04
D2_MISS, D2_MAKE, D3_ATT = 1.05, -0.33, 0.17       # nearest-defender weights
OPP_FTM = -0.19
BOX_W, ONOFF_W = 0.85, 0.21                        # published blend

OB_NAMES = [
    "pts100",            # Points
    "usage100",          # Usage rate (heave-discounted)
    "top36",             # Time of possession per 36
    "sec_per_touch",     # possession style (doc: value decays with clock)
    "assisted_ev100",    # Assisted FGs: EV-weighted assisted makes (deduction)
    "unassisted_ev100",  # complement: EV-weighted unassisted makes
    "ast_ev100",         # Enhanced assists: EV-weighted assists thrown
    "ft_ast100",         # free-throw assists (partial credit fitted)
    "net_passes100",     # Net passes
    "oreb_c100",         # Enhanced OREB: contested
    "oreb_u100",         #   uncontested (doc: any OREB has value)
    "self_oreb_pct",     # Team OREB on missed shots (shooter effect proxy)
    "putback_pts100",    # rim-pressure complement
    "covered3pa100",     # Defended 3PA: 100/80/57/31 weights
    "iso_to100",         # Isolation turnovers (1.0/0.75 classes)
    "pass_to100",        # bad-pass turnovers (contrast class)
    "fb_start100",       # Fast-break starts: .2*steal + .11*recovered block
    "nonshoot_drawn100", # Nonshooting defensive fouls drawn
    "pen_drawn100",      # Penalty fouls drawn
    "ft_pts100",         # FT scoring (usage counterpart of fouls drawn)
]

DB_NAMES = [
    "steals100",         # Steals
    "charges100",        # Offensive fouls drawn (charge component)
    "d2_value100",       # Opp FG: 1.05*miss - 0.33*make (nearest defender)
    "d3a100",            # defended 3PA, attempts only (results = noise)
    "dfga100",           # defended FGA volume (defensive usage exposure)
    "dreb_c100",         # Enhanced DREB: contested
    "dreb_u100",         #   uncontested (doc: little value)
    "perim_dist36",      # Distance traveled, perimeter defenders only (gated)
    "shoot_fouls100",    # Opp FTM on own fouls (shooting-foul proxy)
    "liveball_to100",    # Fastbreak turnovers committed (own live-ball TOs)
    "pen_committed100",  # Penalty fouls committed
    "opp_efg_on",        # Opponents' offensive quality faced (on-court, proxy)
]

OO_NAMES = [
    "on_rtg",            # own on-court rating (points for/against per 100)
    "without_rtg",       # team-without-player rating (their step 2, approx)
    "onoff_diff",        # on minus without
    "luck3_on",          # opponent 3P% luck while on (defense-luck ingredient)
]


def _get(cols, X, name):
    j = cols.get(name)
    return X[:, j].astype(np.float64) if j is not None else None


def _per(v, denom, scale=100.0):
    out = np.full(len(denom), np.nan)
    ok = np.isfinite(v) & np.isfinite(denom) & (denom > 0)
    out[ok] = v[ok] * scale / denom[ok]
    return out


def build_variables(X, feat, sdR, sdRnames, defE, defEnames, mp):
    cols = {n: i for i, n in enumerate(feat)}
    sdc = {n: i for i, n in enumerate(sdRnames)}
    sde = {n: i for i, n in enumerate(defEnames)}
    sdE = defE
    n = X.shape[0]

    def g(name):
        v = _get(cols, X, name)
        return v if v is not None else np.full(n, np.nan)

    def gs(name):
        j = sdc.get(name)
        return sdR[:, j].astype(np.float64) if j is not None \
            else np.full(n, np.nan)

    op = g("pbp|OffPoss")
    dp = g("pbp|DefPoss")
    mp = mp.astype(np.float64)

    # ---------------- offense box ----------------
    OB = np.full((n, len(OB_NAMES)), np.nan)
    ob = {k: i for i, k in enumerate(OB_NAMES)}
    OB[:, ob["pts100"]] = _per(g("pbp|Points"), op)
    heaves = np.nan_to_num(g("pbp|HeaveAttempts"))
    usages = (np.nan_to_num(g("pbp|FG2A")) + np.nan_to_num(g("pbp|FG3A"))
              - heaves * (1 - HEAVE_EV / 1.0)
              + np.nan_to_num(g("pbp|Turnovers"))
              + 0.44 * np.nan_to_num(g("pbp|FTA")))
    OB[:, ob["usage100"]] = _per(usages, op)
    OB[:, ob["top36"]] = _per(gs("sd:possessions|TIME_OF_POSS"), mp, 36.0)
    OB[:, ob["sec_per_touch"]] = gs("sd:possessions|AVG_SEC_PER_TOUCH")

    ev_assisted = np.zeros(n)
    ev_unassisted = np.zeros(n)
    ev_ast = np.zeros(n)
    for zone, ev in SHOT_EV.items():
        fgm = np.nan_to_num(g(f"pbp|{zone}FGM"))
        pa = g(f"pbp|{zone}PctAssisted")
        pa = np.where(np.isfinite(pa), pa, 0.0)
        pa = np.where(pa > 1.5, pa / 100.0, pa)     # percent vs fraction guard
        ev_assisted += fgm * pa * ev
        ev_unassisted += fgm * (1 - pa) * ev
        ast_z = g(f"pbp|{zone}Assists")
        if np.isfinite(ast_z).any():
            ev_ast += np.nan_to_num(ast_z) * ev
    OB[:, ob["assisted_ev100"]] = _per(ev_assisted, op)
    OB[:, ob["unassisted_ev100"]] = _per(ev_unassisted, op)
    # zone assists exist only for rim/corner/arc: add generic 2pt assists rest
    two_ast = np.nan_to_num(g("pbp|Assisted2sPct")) * 0  # placeholder no-op
    OB[:, ob["ast_ev100"]] = _per(ev_ast, op)
    OB[:, ob["ft_ast100"]] = _per(g("track:passing|FT_AST"), op)
    pm = g("track:passing|PASSES\nMADE")
    pr = g("track:passing|PASSES\nRECEIVED")
    OB[:, ob["net_passes100"]] = _per(pm - pr, op)
    creb = g("track:rebounding|CONTESTED\nOREB")
    if not np.isfinite(creb).any():
        creb = g("track:offensive-rebounding|CONTESTED\nOREB")
    oreb = g("pbp|OffRebounds")
    OB[:, ob["oreb_c100"]] = _per(creb, op)
    OB[:, ob["oreb_u100"]] = _per(oreb - np.nan_to_num(creb), op)
    OB[:, ob["self_oreb_pct"]] = g("pbp|SelfORebPct")
    OB[:, ob["putback_pts100"]] = _per(g("pbp|PtsPutbacks"), op)
    cov = np.zeros(n)
    got = np.zeros(n, bool)
    for cname, w in COVER_W.items():
        v = gs(cname)
        m = np.isfinite(v)
        cov[m] += w * v[m]
        got |= m
    cov[~got] = np.nan
    OB[:, ob["covered3pa100"]] = _per(cov, op)
    iso = (ISO_FULL_W * (np.nan_to_num(g("pbp|Travels"))
                         + np.nan_to_num(g("pbp|Charge Fouls")))
           + ISO_PART_W * (np.nan_to_num(g("pbp|LostBallTurnovers"))
                           + np.nan_to_num(g("pbp|LostBallOutOfBoundsTurnovers"))
                           + np.nan_to_num(g("pbp|StepOutOfBoundsTurnovers"))))
    OB[:, ob["iso_to100"]] = _per(iso, op)
    OB[:, ob["pass_to100"]] = _per(
        np.nan_to_num(g("pbp|BadPassTurnovers"))
        + np.nan_to_num(g("pbp|BadPassOutOfBoundsTurnovers")), op)
    OB[:, ob["fb_start100"]] = _per(
        FB_STEAL * np.nan_to_num(g("pbp|Steals"))
        + FB_BLOCK * np.nan_to_num(g("pbp|RecoveredBlocks")), dp)
    OB[:, ob["nonshoot_drawn100"]] = _per(g("pbp|NonShootingFoulsDrawn"), op)
    OB[:, ob["pen_drawn100"]] = _per(
        g("pbp|NonShootingPenaltyNonTakeFoulsDrawn"), op)
    OB[:, ob["ft_pts100"]] = _per(g("pbp|FtPoints"), op)

    # ---------------- defense box ----------------
    DB = np.full((n, len(DB_NAMES)), np.nan)
    db = {k: i for i, k in enumerate(DB_NAMES)}
    DB[:, db["steals100"]] = _per(g("pbp|Steals"), dp)
    DB[:, db["charges100"]] = _per(g("pbp|Charge Fouls Drawn"), dp)
    d2m = sdE_col(sdE, sde, "d2_value36")
    # d2_value36 is per-36; convert to per-100 def poss via minutes->poss
    DB[:, db["d2_value100"]] = _per(d2m * mp / 36.0, dp)
    DB[:, db["d3a100"]] = _per(sdE_col(sdE, sde, "d3a36") * mp / 36.0, dp)
    DB[:, db["dfga100"]] = _per(sdE_col(sdE, sde, "dfga36") * mp / 36.0, dp)
    cdreb = g("track:defensive-rebounding|CONTESTED\nDREB")
    dreb = g("pbp|DefRebounds")
    DB[:, db["dreb_c100"]] = _per(cdreb, dp)
    DB[:, db["dreb_u100"]] = _per(dreb - np.nan_to_num(cdreb), dp)
    miles_def = g("track:speed-distance|DIST. MILES DEF")
    d3a = sdE_col(sdE, sde, "d3a36")
    d2a = sdE_col(sdE, sde, "dfga36") - np.nan_to_num(d3a)
    perim = d3a / np.where(d2a > 0, d2a, np.nan)
    gate = perim > np.nanmedian(perim)          # perimeter defenders only
    pd36 = _per(miles_def, mp, 36.0)
    DB[:, db["perim_dist36"]] = np.where(gate, pd36, 0.0)
    DB[:, db["shoot_fouls100"]] = _per(g("pbp|ShootingFouls"), dp)
    DB[:, db["liveball_to100"]] = _per(g("pbp|LiveBallTurnovers"), op)
    DB[:, db["pen_committed100"]] = _per(
        g("pbp|NonShootingPenaltyNonTakeFouls"), dp)
    DB[:, db["opp_efg_on"]] = g("pbp|OnDefRtg")

    # ---------------- on-off ratings (per side handled in structural) -------
    OO = np.full((n, len(OO_NAMES) * 2), np.nan)
    names_oo = []
    for k, side in enumerate(("off", "def")):
        base = k * len(OO_NAMES)
        if side == "off":
            on = g("wowy_on|Points")
            onp = g("wowy_on|OffPoss")
            wo = g("wowy_off|Points")
            wop = g("wowy_off|OffPoss")
        else:
            on = g("wowy_on|OpponentPoints")
            onp = g("wowy_on|DefPoss")
            wo = g("wowy_off|OpponentPoints")
            wop = g("wowy_off|DefPoss")
        on_r = _per(on, onp)
        wo_r = _per(wo, wop)
        OO[:, base + 0] = on_r
        OO[:, base + 1] = wo_r
        OO[:, base + 2] = on_r - wo_r
        opp3 = g("wowy_on|Arc3Accuracy") if side == "def" else \
            g("wowy_on|Fg3Pct")
        OO[:, base + 3] = opp3
        names_oo += [f"{side}_{nm}" for nm in OO_NAMES]

    return {"OB": OB, "OB_NAMES": OB_NAMES, "DB": DB, "DB_NAMES": DB_NAMES,
            "OO": OO, "OO_NAMES": names_oo}


def sdE_col(sdE, sde, name):
    j = sde.get(name)
    return sdE[:, j].astype(np.float64) if j is not None \
        else np.full(sdE.shape[0], np.nan)
