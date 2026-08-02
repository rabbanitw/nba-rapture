
---

# Addendum: full methodology recovered via Wayback (curl, snapshot 2019-12-03)

Full text: training/raptor_methodology_fulltext.txt. Corrections and additions to
the summary above, now from the article itself rather than search-index quotes:

## The combination — exact weights

**Overall RAPTOR = 0.85 × Box + 0.21 × On-Off.** Deliberately sums past 100%
(the components carry non-redundant information). Our ridge combiner, fit on our
own data with no knowledge of this, learned 0.95 / 0.184–0.194.

## Box defense — the exact variable list

- Steals (+1.49 per 100 in the underlying RAPM regression)
- Offensive fouls drawn (+2.28) — the single heaviest defensive weight
- Defended 2-pt shots as nearest defender: missed +1.05, made −0.33
- Defended 3-pt shots: **attempts only (+0.17); results excluded as pure noise**
- **No blocks.** Zero predictive power once defended FG are accounted for
- Enhanced defensive rebounds: contested ≫ uncontested, weighted by the
  offensive-rebound rate of the preceding shot type
- Positional opponents' points and offensive rebounds (probabilistic matchups)
- Distance travelled per 100 defensive possessions, **perimeter defenders only**
- Opponents' FT made on own fouls (−0.19); penalty fouls (−0.04); fastbreak
  turnovers committed; opponents' offensive rating as competition adjustment
- Their defensive R² against RAPM: ~0.6, vs ~0.3 for traditional defensive stats

## Box offense — key structure

- Seven shot categories with expected values (dunks 1.83, layups 1.16, paint 0.82,
  midrange 0.80, corner 3 1.16, arc 3 1.05, heaves 0.08)
- Assisted-shot deduction proportional to the shot's expected value; enhanced
  assists credited the same way; net passes (made − received) positively weighted
- Usage, time of possession (negative), isolation turnovers (weighted subtypes),
  defended 3PA as the spacing/gravity measure (coverage-weighted: within 2ft =
  100%, 2-4ft = 80%, 4-6ft = 57%, 6ft+ = 31%)
- Fast-break starts (+0.2 per steal, +0.11 per rebounded block), non-shooting
  fouls drawn (+0.16), penalty fouls drawn (+0.04), opponents' defensive rating

## After combination — two adjustments we cannot replicate from season aggregates

- **Score effects**: per 10 points of lead, efficiency drops −1.1/−1.7/−2.3/−2.9
  per 100 by quarter (regular season; roughly half that in playoffs), computed on
  the player's own on-court possessions. Requires possession-level score state.
- **Team effects**: player ratings reconciled so minutes-weighted sums match the
  team's adjusted rating, with the correction allocated by offensive/defensive
  usage (defensive usage = induced TOs + shooting fouls + nearest-defender FGAs).
  Dropped in the predictive version (PREDATOR) — it did not help out of sample.

## Other recovered facts

- Descriptive RAPTOR uses **no priors** (no height/age/draft — those are PREDATOR).
- On/off: identical coefficients for offense and defense; three terms (own on-court
  rating, courtmates-without weighted, courtmates' courtmates); further iterations
  add nothing; this construction predicted out-of-sample RAPM as well as RAPM itself.
- Replacement level −2.75 per 100, from two-way-contract players.
- Individual pace impact fit only on team-switchers; ~half of team pace is players.
