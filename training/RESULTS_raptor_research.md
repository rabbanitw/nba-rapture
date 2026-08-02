# How RAPTOR actually worked — research notes

Assembled 2026-08 from FiveThirtyEight's methodology article (as quoted in search
indexes — the original now redirects to ABC News and archive.org is unreachable from
this environment), 538's data repositories, Neil Paine's Estimated RAPTOR, and
secondary write-ups. Where a claim comes from a quoted passage rather than a page I
could read end-to-end, it is marked *(quoted)*.

## The architecture

RAPTOR = **two separate models, combined**:

1. **Box/tracking component.** A regression in the BPM tradition: inputs are
   traditional box score stats *plus play-by-play and player-tracking stats*; the
   regression target is **six-year RAPM** (2013-14 through 2018-19, Ryan Davis's
   RAPM with adjustments) *(quoted)*. Variables were selected by out-of-sample
   validation on two 3-year halves of the RAPM data, with emphasis on players who
   changed teams — a variable that fit in-sample but failed across halves was judged
   luck and dropped *(quoted)*.
2. **On/off component.** Team performance with the player and combinations of
   teammates on/off the floor, adjusted for teammate and opponent quality
   (RAPM-like), with **luck adjustments** — replacing observed teammate/opponent
   shooting (3P%, FT%) with expected shooting so a player is not credited for
   variance he does not control.
3. **Combination**: box weighted more heavily than on/off *(quoted, weight values
   unpublished)*. Weighting is sample-size aware in the projection system: the
   prior's weight relative to current-season minutes varies with age and experience.

## Inputs 538 names that we have

- Play-by-play shot-location detail, per-100 rates, shot quality — our `pbp` block.
- Drives, touches, passes, speed/distance, rebounding-chance data — our 14 tracking
  tables.
- On/off splits — our `wowy` block.
- Contested vs uncontested rebounds, weighted by the shot type that produced them —
  partially ours (`CONTESTED\nDREB` etc.).

## Inputs 538 names that we lack entirely

| input | notes |
|---|---|
| **Positional matchup data** | points and rebounds scored by opponents at the defender's position; matchup assignment is probabilistic *(quoted)* |
| **Nearest-defender shot data** | how often the player was nearest defender, and opponent FG% on those shots *(quoted)*; `leaguedashptdefend` carries this |
| **Height, age, draft position, awards** | used by RAPTOR/its projections; we carry no biographical features at all |
| **Luck-adjusted on/off** | our wowy is raw; no teammate-shooting correction |
| **Six-year RAPM target** | our labels are 538's single-season outputs; the underlying multi-year RAPM is what made the box weights stable |
| Hustle stats (deflections, screen assists, box outs) | not in the article quotes, but standard tracking-era defensive signal we scan as absent |

## What this implies for our models

1. **Architecture**: we predict the blended `rap_o`/`rap_d` directly. RAPTOR built
   `rap_box` and `rap_onoff` separately and combined them — and 538 *published both
   components*, which our 538 documents carry. Copying the decomposition is testable
   today with no new data (experiment_components.py).
2. **Defense is where the missing inputs concentrate.** Matchup and nearest-defender
   data are the defensive box inputs; our defensive columns are blocks, steals,
   rebounds, fouls and one DFG%. This matches defense being our weakest target and
   Paine's weakest correlation (0.784 vs 0.913 offensive against full RAPTOR).
3. **Bio features are cheap.** Age/experience proxies are derivable from data we
   hold (first season seen, career minutes to date); height/draft need one static
   table scraped once.
4. **Cell-relative context**: 538's regressions are fit across a fixed six-year
   window; our training cells span eras with different league environments, and
   trees cannot see cell context — within-cell standardization is the cheap fix.

## Sources

- [How Our RAPTOR Metric Works — FiveThirtyEight](https://fivethirtyeight.com/features/how-our-raptor-metric-works/) (redirects to ABC News; content recovered via search-index quotes)
- [Introducing RAPTOR — FiveThirtyEight](https://fivethirtyeight.com/features/introducing-raptor-our-new-metric-for-the-modern-nba/)
- [538 nba-raptor data repo](https://github.com/fivethirtyeight/data/tree/master/nba-raptor)
- [538 nba-player-advanced-metrics repo](https://github.com/fivethirtyeight/nba-player-advanced-metrics)
- [Neil Paine, NBA Estimated RAPTOR ratings](https://neilpaine.substack.com/p/nba-estimated-raptor-player-ratings)
- [Neil-Paine-1/NBA-elo repo](https://github.com/Neil-Paine-1/NBA-elo)
- [RAPTOR Explained — NBAstuffer](https://www.nbastuffer.com/analytics101/raptor/)
- [APBRmetrics forum on 538 projections](https://apbr.org/metrics/viewtopic.php?t=9814)
- [Positive Residual, Estimated Contributions in the WNBA](https://www.positiveresidual.com/post/estimated-contributions-in-the-wnba/) (RAPTOR-style luck adjustment description)
- [How Our NBA Predictions Work — FiveThirtyEight](https://fivethirtyeight.com/methodology/how-our-nba-predictions-work/)
