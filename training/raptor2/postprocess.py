"""Published RAPTOR combination, score-effect, team, pace, and WAR formulas.

FiveThirtyEight published enough information to reproduce some post-model
steps exactly and only described others qualitatively.  This module keeps that
boundary explicit:

* ``combine_components`` and ``raptor_war`` are exact published formulas.
* ``score_effect`` uses the exact published period/split coefficients.  It
  returns the *observed scoring-margin effect*; subtract it from an observed
  rating to express the rating in a tied-game context.
* ``reconcile_team_rating`` exactly enforces the stated team constraint while
  allocating the residual monotonically by usage.  FiveThirtyEight did not
  publish its precise usage transform, so callers must supply that transform.
* Individual Pace Impact itself cannot be reconstructed from the article: its
  switcher-only regression coefficients were never published.  A supplied
  pace-impact estimate can nevertheless be carried through the exact WAR
  formula.

All ratings are points per 100 possessions.  Array-like arguments broadcast
under NumPy's normal rules.
"""

from __future__ import annotations

import numpy as np


BOX_WEIGHT = 0.85
ONOFF_WEIGHT = 0.21
REPLACEMENT_LEVEL = -2.75
WAR_MULTIPLIER_RS = 0.0005102
WAR_MULTIPLIER_PO = 0.0005262

# Effect on scoring margin per 100 possessions for each 10 points of lead.
# Period 5 and later (overtime) use the fourth-quarter coefficient.
SCORE_EFFECT_RS = np.array([-1.1, -1.7, -2.3, -2.9], dtype=float)
SCORE_EFFECT_PO = np.array([-0.6, -0.9, -1.2, -1.5], dtype=float)

POSITION_REPLACEMENT = {
    "PG": (-1.10, -1.65),
    "SG": (-1.50, -1.25),
    "SF": (-1.90, -0.85),
    "PF": (-2.30, -0.45),
    "C": (-2.70, -0.05),
}


def combine_components(box, onoff):
    """Return descriptive RAPTOR before score/team effects.

    The weights intentionally sum to 1.06 because the two inputs contain
    non-redundant information.
    """
    return BOX_WEIGHT * np.asarray(box, dtype=float) + \
        ONOFF_WEIGHT * np.asarray(onoff, dtype=float)


def score_effect(lead, period, playoffs=False):
    """Observed scoring-margin effect for a lead and game period.

    ``lead`` is from the rated team's perspective.  Positive values therefore
    yield a negative effect (teams coast with a lead); negative values yield a
    positive effect.  ``period`` is one-indexed, with overtime mapped to period
    four as in the article.
    """
    lead, period = np.broadcast_arrays(np.asarray(lead, dtype=float),
                                       np.asarray(period, dtype=int))
    if np.any(period < 1):
        raise ValueError("period must be one-indexed (1, 2, 3, 4/OT)")
    coeff = SCORE_EFFECT_PO if playoffs else SCORE_EFFECT_RS
    return coeff[np.minimum(period, 4) - 1] * lead / 10.0


def tied_game_rating(observed_rating, mean_score_effect):
    """Remove a possession-weighted score effect from an observed rating."""
    return np.asarray(observed_rating, dtype=float) - \
        np.asarray(mean_score_effect, dtype=float)


def reconcile_team_rating(ratings, minutes, usage_weights, team_rating,
                          players_on_court=5.0):
    """Allocate a team residual by usage while enforcing the team total.

    The constraint is::

        players_on_court * minute_weighted_mean(adjusted ratings) = team_rating

    ``usage_weights`` controls only allocation.  Use offensive usage for
    offense.  For defense the article defines usage from induced turnovers,
    shooting fouls, and nearest-defender field-goal attempts.  The exact
    nonlinear transform was not published; this routine deliberately does not
    invent one.
    """
    r, m, u = np.broadcast_arrays(np.asarray(ratings, dtype=float),
                                  np.asarray(minutes, dtype=float),
                                  np.asarray(usage_weights, dtype=float))
    ok = np.isfinite(r) & np.isfinite(m) & np.isfinite(u) & (m >= 0) & (u >= 0)
    if not np.any(ok) or m[ok].sum() <= 0:
        raise ValueError("at least one finite, non-negative minute/usage row is required")
    if players_on_court <= 0:
        raise ValueError("players_on_court must be positive")

    current = players_on_court * np.average(r[ok], weights=m[ok])
    residual = float(team_rating) - current
    denom = players_on_court * np.sum(m[ok] * u[ok]) / np.sum(m[ok])
    out = r.copy()
    if denom == 0:
        # With no usage information, a uniform player adjustment is the only
        # constraint-preserving fallback.
        out[ok] += residual / players_on_court
    else:
        out[ok] += (residual / denom) * u[ok]
    return out


def raptor_war(raptor, minutes, league_pace, pace_impact=0.0, playoffs=False):
    """FiveThirtyEight's exact descriptive RAPTOR-to-WAR conversion."""
    pace = np.asarray(league_pace, dtype=float)
    if np.any(~np.isfinite(pace)) or np.any(pace <= 0):
        raise ValueError("league_pace must be finite and positive")
    mult = WAR_MULTIPLIER_PO if playoffs else WAR_MULTIPLIER_RS
    return ((np.asarray(raptor, dtype=float) - REPLACEMENT_LEVEL)
            * np.asarray(minutes, dtype=float)
            * (pace + np.asarray(pace_impact, dtype=float)) / pace
            * mult)

