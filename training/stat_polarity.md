# Stat polarity

908 features: **685 offense**, **107 defense**, **116 neutral**.

The offense model uses offense+neutral, the defense model defense+neutral, and total uses everything.

| block | offense | defense | neutral |
|---|---:|---:|---:|
| pbp | 186 | 35 | 19 |
| track:catch-shoot | 8 | 0 | 2 |
| track:defensive-impact | 0 | 6 | 4 |
| track:defensive-rebounding | 0 | 8 | 4 |
| track:drives | 17 | 0 | 4 |
| track:elbow-touch | 18 | 0 | 4 |
| track:offensive-rebounding | 8 | 0 | 4 |
| track:paint-touch | 18 | 0 | 4 |
| track:passing | 9 | 0 | 4 |
| track:pullup | 8 | 0 | 4 |
| track:rebounding | 0 | 0 | 12 |
| track:shooting-efficiency | 14 | 0 | 4 |
| track:speed-distance | 2 | 2 | 7 |
| track:touches | 13 | 0 | 4 |
| track:tracking-post-ups | 18 | 0 | 4 |
| wowy_off | 183 | 28 | 16 |
| wowy_on | 183 | 28 | 16 |

## offense

```
pbp|2pt And 1 Free Throw Trips
pbp|3SecondViolations
pbp|3pt And 1 Free Throw Trips
pbp|Arc3Accuracy
pbp|Arc3Assists
pbp|Arc3FGA
pbp|Arc3FGM
pbp|Arc3Frequency
pbp|Arc3PctAssisted
pbp|Arc3PctBlocked
pbp|AssistPoints
pbp|Assisted2sPct
pbp|Assisted3sPct
pbp|Assists
pbp|AtRimAccuracy
pbp|AtRimAssists
pbp|AtRimFG3AFrequency
pbp|AtRimFGA
pbp|AtRimFGM
pbp|AtRimFrequency
pbp|AtRimOffReboundedPct
pbp|AtRimPctAssisted
pbp|AtRimPctBlocked
pbp|Avg2ptShotDistance
pbp|Avg3ptShotDistance
pbp|BadPassOutOfBoundsTurnovers
pbp|BadPassSteals
pbp|BadPassTurnovers
pbp|Blocked2s
pbp|Blocked3s
pbp|BlockedArc3
pbp|BlockedAtRim
pbp|BlockedCorner3
pbp|BlockedLongMidRange
pbp|BlockedShortMidRange
pbp|Charge Fouls
pbp|Corner3Accuracy
pbp|Corner3Assists
pbp|Corner3FGA
pbp|Corner3FGM
pbp|Corner3Frequency
pbp|Corner3PctAssisted
pbp|Corner3PctBlocked
pbp|DeadBallTurnovers
pbp|EfgPct
pbp|FG2A
pbp|FG2APctBlocked
pbp|FG2M
pbp|FG3A
pbp|FG3APct
pbp|FG3APctBlocked
pbp|FG3M
pbp|FTA
pbp|FTOffRebounds
pbp|Fg2Pct
pbp|Fg2aBlocked
pbp|Fg3Pct
pbp|Fg3aBlocked
pbp|FirstChancePoints
pbp|FoulsDrawn
pbp|FtPoints
pbp|HeaveAttempts
pbp|LiveBallTurnoverPct
pbp|LiveBallTurnovers
pbp|LongMidRangeAccuracy
pbp|LongMidRangeAssists
pbp|LongMidRangeFGA
pbp|LongMidRangeFGM
pbp|LongMidRangeFrequency
pbp|LongMidRangeOffReboundedPct
pbp|LongMidRangePctAssisted
pbp|LongMidRangePctBlocked
pbp|LostBallOutOfBoundsTurnovers
pbp|LostBallSteals
pbp|LostBallTurnovers
pbp|NonHeaveArc3Accuracy
pbp|NonHeaveArc3FGA
pbp|NonHeaveArc3FGM
pbp|NonHeaveFg3Pct
pbp|NonPutbacksAssisted2sPct
pbp|NonShootingFoulsDrawn
pbp|NonShootingPenaltyNonTakeFoulsDrawn
pbp|OffArc3ReboundPct
pbp|OffAtRimReboundPct
pbp|OffCorner3ReboundPct
pbp|OffFGReboundPct
pbp|OffFTReboundPct
pbp|OffLongMidRangeReboundPct
pbp|OffRebounds
pbp|OffShortMidRangeReboundPct
pbp|OffThreePtReboundPct
pbp|OffThreePtRebounds
pbp|OffTwoPtReboundPct
pbp|OffTwoPtRebounds
pbp|Offensive Fouls
pbp|OffensiveGoaltends
pbp|OnOffRtg
pbp|PenaltyArc3Accuracy
pbp|PenaltyArc3FGA
pbp|PenaltyArc3FGM
pbp|PenaltyArc3Frequency
pbp|PenaltyAtRimAccuracy
pbp|PenaltyAtRimFGA
pbp|PenaltyAtRimFGM
pbp|PenaltyAtRimFrequency
pbp|PenaltyCorner3Accuracy
pbp|PenaltyCorner3FGA
pbp|PenaltyCorner3FGM
pbp|PenaltyCorner3Frequency
pbp|PenaltyEfgPct
pbp|PenaltyFG2A
pbp|PenaltyFG2M
pbp|PenaltyFG3A
pbp|PenaltyFG3M
pbp|PenaltyFg2Pct
pbp|PenaltyFg3Pct
pbp|PenaltyFtPoints
pbp|PenaltyPoints
pbp|PenaltyPointsExcludingTakeFouls
pbp|PenaltyPointsPct
pbp|PenaltyShotQualityAvg
pbp|PenaltyTsPct
pbp|PenaltyTurnovers
pbp|Points
pbp|PtsAssisted2s
pbp|PtsAssisted3s
pbp|PtsPutbacks
pbp|PtsUnassisted2s
pbp|PtsUnassisted3s
pbp|SecondChanceArc3Accuracy
pbp|SecondChanceArc3FGA
pbp|SecondChanceArc3FGM
pbp|SecondChanceArc3Frequency
pbp|SecondChanceArc3PctAssisted
pbp|SecondChanceAtRimAccuracy
pbp|SecondChanceAtRimFGA
pbp|SecondChanceAtRimFGM
pbp|SecondChanceAtRimFrequency
pbp|SecondChanceAtRimPctAssisted
pbp|SecondChanceCorner3Accuracy
pbp|SecondChanceCorner3FGA
pbp|SecondChanceCorner3FGM
pbp|SecondChanceCorner3Frequency
pbp|SecondChanceCorner3PctAssisted
pbp|SecondChanceEfgPct
pbp|SecondChanceFG2A
pbp|SecondChanceFG2M
pbp|SecondChanceFG3A
pbp|SecondChanceFG3M
pbp|SecondChanceFg2Pct
pbp|SecondChanceFg3Pct
pbp|SecondChanceFtPoints
pbp|SecondChancePoints
pbp|SecondChancePointsPct
pbp|SecondChanceShotQualityAvg
pbp|SecondChanceTsPct
pbp|SecondChanceTurnovers
pbp|SelfOReb
pbp|SelfORebPct
pbp|ShootingFoulsDrawnPct
pbp|ShortMidRangeAccuracy
pbp|ShortMidRangeAssists
pbp|ShortMidRangeFGA
pbp|ShortMidRangeFGM
pbp|ShortMidRangeFrequency
pbp|ShortMidRangeOffReboundedPct
pbp|ShortMidRangePctAssisted
pbp|ShortMidRangePctBlocked
pbp|ShotQualityAvg
pbp|StepOutOfBoundsTurnovers
pbp|ThreePtAssists
pbp|ThreePtOffReboundedPct
pbp|ThreePtShootingFoulsDrawn
pbp|ThreePtShootingFoulsDrawnPct
pbp|Travels
pbp|TsPct
pbp|Turnovers
pbp|TwoPtAssists
pbp|TwoPtShootingFoulsDrawn
pbp|TwoPtShootingFoulsDrawnPct
pbp|UnblockedArc3Accuracy
pbp|UnblockedAtRimAccuracy
pbp|UnblockedCorner3Accuracy
pbp|UnblockedLongMidRangeAccuracy
pbp|UnblockedShortMidRangeAccuracy
pbp|Usage
track:catch-shoot|3P%
track:catch-shoot|3PA
track:catch-shoot|3PM
track:catch-shoot|EFG%
track:catch-shoot|FG%
track:catch-shoot|FGA
track:catch-shoot|FGM
track:catch-shoot|PTS
track:drives|AST
track:drives|AST%
track:drives|DRIVES
track:drives|FG%
track:drives|FGA
track:drives|FGM
track:drives|FT%
track:drives|FTA
track:drives|FTM
track:drives|PASS
track:drives|PASS%
track:drives|PF
track:drives|PF%
track:drives|PTS
track:drives|PTS%
track:drives|TO
track:drives|TOV%
track:elbow-touch|AST
track:elbow-touch|AST%
track:elbow-touch|ELBOW
TOUCHES
track:elbow-touch|FG%
track:elbow-touch|FGA
track:elbow-touch|FGM
track:elbow-touch|FT%
track:elbow-touch|FTA
track:elbow-touch|FTM
track:elbow-touch|PASS
track:elbow-touch|PASS%
track:elbow-touch|PF
track:elbow-touch|PF%
track:elbow-touch|PTS
track:elbow-touch|PTS%
track:elbow-touch|TO
track:elbow-touch|TOUCHES
track:elbow-touch|TOV%
track:offensive-rebounding|ADJUSTED
OREB CHANCE%
track:offensive-rebounding|AVG OREB
DISTANCE
track:offensive-rebounding|CONTESTED
OREB
track:offensive-rebounding|CONTESTED
OREB%
track:offensive-rebounding|DEFERRED
OREB CHANCES
track:offensive-rebounding|OREB
track:offensive-rebounding|OREB
CHANCE%
track:offensive-rebounding|OREB
CHANCES
track:paint-touch|AST
track:paint-touch|AST%
track:paint-touch|FG%
track:paint-touch|FGA
track:paint-touch|FGM
track:paint-touch|FT%
track:paint-touch|FTA
track:paint-touch|FTM
track:paint-touch|PAINT
TOUCHES
track:paint-touch|PASS
track:paint-touch|PASS%
track:paint-touch|PF
track:paint-touch|PF%
track:paint-touch|PTS
track:paint-touch|PTS%
track:paint-touch|TO
track:paint-touch|TOUCHES
track:paint-touch|TOV%
track:passing|
track:passing|AST
track:passing|AST
ADJ
track:passing|AST PTS
CREATED
track:passing|AST TO
PASS%
track:passing|PASSES
MADE
track:passing|PASSES
RECEIVED
track:passing|POTENTIAL
AST
track:passing|SECONDARY
AST
track:pullup|3P%
track:pullup|3PA
track:pullup|3PM
track:pullup|EFG%
track:pullup|FG%
track:pullup|FGA
track:pullup|FGM
track:pullup|PTS
track:shooting-efficiency|C&S
FG%
track:shooting-efficiency|C&S
PTS
track:shooting-efficiency|DRIVE
FG%
track:shooting-efficiency|DRIVE
PTS
track:shooting-efficiency|EFG%
track:shooting-efficiency|ELBOW
TOUCH FG%
track:shooting-efficiency|ELBOW
TOUCH PTS
track:shooting-efficiency|PAINT
TOUCH FG%
track:shooting-efficiency|PAINT
TOUCH PTS
track:shooting-efficiency|POST
TOUCH FG%
track:shooting-efficiency|POST
TOUCH PTS
track:shooting-efficiency|PTS
track:shooting-efficiency|PULL UP
FG%
track:shooting-efficiency|PULL UP
PTS
track:speed-distance|AVG SPEED OFF
track:speed-distance|DIST. MILES OFF
track:touches|AVG DRIB PER
TOUCH
track:touches|AVG SEC PER
TOUCH
track:touches|ELBOW
TOUCHES
track:touches|FRONT CT
TOUCHES
track:touches|PAINT
TOUCHES
track:touches|POST
UPS
track:touches|PTS
track:touches|PTS PER
ELBOW TOUCH
track:touches|PTS PER
PAINT TOUCH
track:touches|PTS PER
POST TOUCH
track:touches|PTS PER
TOUCH
track:touches|TIME OF
POSS
track:touches|TOUCHES
track:tracking-post-ups|AST
track:tracking-post-ups|AST%
track:tracking-post-ups|FG%
track:tracking-post-ups|FGA
track:tracking-post-ups|FGM
track:tracking-post-ups|FT%
track:tracking-post-ups|FTA
track:tracking-post-ups|FTM
track:tracking-post-ups|PASS
track:tracking-post-ups|PASS%
track:tracking-post-ups|PF
track:tracking-post-ups|PF%
track:tracking-post-ups|POST
UPS
track:tracking-post-ups|PTS
track:tracking-post-ups|PTS%
track:tracking-post-ups|TO
track:tracking-post-ups|TOUCHES
track:tracking-post-ups|TOV%
wowy_off|2pt And 1 Free Throw Trips
wowy_off|3SecondViolations
wowy_off|3pt And 1 Free Throw Trips
wowy_off|Arc3Accuracy
wowy_off|Arc3Assists
wowy_off|Arc3FGA
wowy_off|Arc3FGM
wowy_off|Arc3Frequency
wowy_off|Arc3PctAssisted
wowy_off|Arc3PctBlocked
wowy_off|AssistPoints
wowy_off|Assisted2sPct
wowy_off|Assisted3sPct
wowy_off|Assists
wowy_off|AtRimAccuracy
wowy_off|AtRimAssists
wowy_off|AtRimFG3AFrequency
wowy_off|AtRimFGA
wowy_off|AtRimFGM
wowy_off|AtRimFrequency
wowy_off|AtRimPctAssisted
wowy_off|AtRimPctBlocked
wowy_off|Avg2ptShotDistance
wowy_off|Avg3ptShotDistance
wowy_off|BadPassOutOfBoundsTurnovers
wowy_off|BadPassSteals
wowy_off|BadPassTurnovers
wowy_off|Blocked2s
wowy_off|Blocked3s
wowy_off|BlockedArc3
wowy_off|BlockedAtRim
wowy_off|BlockedCorner3
wowy_off|BlockedLongMidRange
wowy_off|BlockedShortMidRange
wowy_off|Charge Fouls
wowy_off|Corner3Accuracy
wowy_off|Corner3Assists
wowy_off|Corner3FGA
wowy_off|Corner3FGM
wowy_off|Corner3Frequency
wowy_off|Corner3PctAssisted
wowy_off|Corner3PctBlocked
wowy_off|DeadBallTurnovers
wowy_off|EfgPct
wowy_off|FG2A
wowy_off|FG2APctBlocked
wowy_off|FG2M
wowy_off|FG3A
wowy_off|FG3APct
wowy_off|FG3APctBlocked
wowy_off|FG3M
wowy_off|FTA
wowy_off|FTOffRebounds
wowy_off|Fg2Pct
wowy_off|Fg2aBlocked
wowy_off|Fg3Pct
wowy_off|Fg3aBlocked
wowy_off|FirstChancePoints
wowy_off|FoulsDrawn
wowy_off|FtPoints
wowy_off|HeaveAttempts
wowy_off|HeaveMakes
wowy_off|LiveBallTurnoverPct
wowy_off|LiveBallTurnovers
wowy_off|LongMidRangeAccuracy
wowy_off|LongMidRangeAssists
wowy_off|LongMidRangeFGA
wowy_off|LongMidRangeFGM
wowy_off|LongMidRangeFrequency
wowy_off|LongMidRangePctAssisted
wowy_off|LongMidRangePctBlocked
wowy_off|LostBallOutOfBoundsTurnovers
wowy_off|LostBallSteals
wowy_off|LostBallTurnovers
wowy_off|NonHeaveArc3Accuracy
wowy_off|NonHeaveArc3FGA
wowy_off|NonHeaveArc3FGM
wowy_off|NonHeaveFg3Pct
wowy_off|NonPutbacksAssisted2sPct
wowy_off|NonShootingFoulsDrawn
wowy_off|NonShootingPenaltyNonTakeFoulsDrawn
wowy_off|OffArc3ReboundPct
wowy_off|OffAtRimReboundPct
wowy_off|OffCorner3ReboundPct
wowy_off|OffFGReboundPct
wowy_off|OffFTReboundPct
wowy_off|OffLongMidRangeReboundPct
wowy_off|OffRebounds
wowy_off|OffShortMidRangeReboundPct
wowy_off|OffThreePtReboundPct
wowy_off|OffThreePtRebounds
wowy_off|OffTwoPtReboundPct
wowy_off|OffTwoPtRebounds
wowy_off|Offensive Fouls
wowy_off|OffensiveGoaltends
wowy_off|PenaltyArc3Accuracy
wowy_off|PenaltyArc3FGA
wowy_off|PenaltyArc3FGM
wowy_off|PenaltyArc3Frequency
wowy_off|PenaltyAtRimAccuracy
wowy_off|PenaltyAtRimFGA
wowy_off|PenaltyAtRimFGM
wowy_off|PenaltyAtRimFrequency
wowy_off|PenaltyCorner3Accuracy
wowy_off|PenaltyCorner3FGA
wowy_off|PenaltyCorner3FGM
wowy_off|PenaltyCorner3Frequency
wowy_off|PenaltyEfgPct
wowy_off|PenaltyFG2A
wowy_off|PenaltyFG2M
wowy_off|PenaltyFG3A
wowy_off|PenaltyFG3M
wowy_off|PenaltyFg2Pct
wowy_off|PenaltyFg3Pct
wowy_off|PenaltyFtPoints
wowy_off|PenaltyPoints
wowy_off|PenaltyPointsExcludingTakeFouls
wowy_off|PenaltyPointsPct
wowy_off|PenaltyShotQualityAvg
wowy_off|PenaltyTsPct
wowy_off|PenaltyTurnovers
wowy_off|Points
wowy_off|PtsAssisted2s
wowy_off|PtsAssisted3s
wowy_off|PtsPutbacks
wowy_off|PtsUnassisted2s
wowy_off|PtsUnassisted3s
wowy_off|SecondChanceArc3Accuracy
wowy_off|SecondChanceArc3FGA
wowy_off|SecondChanceArc3FGM
wowy_off|SecondChanceArc3Frequency
wowy_off|SecondChanceArc3PctAssisted
wowy_off|SecondChanceAtRimAccuracy
wowy_off|SecondChanceAtRimFGA
wowy_off|SecondChanceAtRimFGM
wowy_off|SecondChanceAtRimFrequency
wowy_off|SecondChanceAtRimPctAssisted
wowy_off|SecondChanceCorner3Accuracy
wowy_off|SecondChanceCorner3FGA
wowy_off|SecondChanceCorner3FGM
wowy_off|SecondChanceCorner3Frequency
wowy_off|SecondChanceCorner3PctAssisted
wowy_off|SecondChanceEfgPct
wowy_off|SecondChanceFG2A
wowy_off|SecondChanceFG2M
wowy_off|SecondChanceFG3A
wowy_off|SecondChanceFG3M
wowy_off|SecondChanceFg2Pct
wowy_off|SecondChanceFg3Pct
wowy_off|SecondChanceFtPoints
wowy_off|SecondChancePoints
wowy_off|SecondChancePointsPct
wowy_off|SecondChanceShotQualityAvg
wowy_off|SecondChanceTsPct
wowy_off|SecondChanceTurnovers
wowy_off|SecondsExcludingORebsPerPossOff
wowy_off|SecondsPerPossOff
wowy_off|SelfOReb
wowy_off|SelfORebPct
wowy_off|ShootingFoulsDrawnPct
wowy_off|ShortMidRangeAccuracy
wowy_off|ShortMidRangeAssists
wowy_off|ShortMidRangeFGA
wowy_off|ShortMidRangeFGM
wowy_off|ShortMidRangeFrequency
wowy_off|ShortMidRangePctAssisted
wowy_off|ShortMidRangePctBlocked
wowy_off|ShotQualityAvg
wowy_off|StepOutOfBoundsTurnovers
wowy_off|ThreePtAssists
wowy_off|ThreePtShootingFoulsDrawn
wowy_off|ThreePtShootingFoulsDrawnPct
wowy_off|Travels
wowy_off|TsPct
wowy_off|Turnovers
wowy_off|TwoPtAssists
wowy_off|TwoPtShootingFoulsDrawn
wowy_off|TwoPtShootingFoulsDrawnPct
wowy_off|UnblockedArc3Accuracy
wowy_off|UnblockedAtRimAccuracy
wowy_off|UnblockedCorner3Accuracy
wowy_off|UnblockedLongMidRangeAccuracy
wowy_off|UnblockedShortMidRangeAccuracy
wowy_on|2pt And 1 Free Throw Trips
wowy_on|3SecondViolations
wowy_on|3pt And 1 Free Throw Trips
wowy_on|Arc3Accuracy
wowy_on|Arc3Assists
wowy_on|Arc3FGA
wowy_on|Arc3FGM
wowy_on|Arc3Frequency
wowy_on|Arc3PctAssisted
wowy_on|Arc3PctBlocked
wowy_on|AssistPoints
wowy_on|Assisted2sPct
wowy_on|Assisted3sPct
wowy_on|Assists
wowy_on|AtRimAccuracy
wowy_on|AtRimAssists
wowy_on|AtRimFG3AFrequency
wowy_on|AtRimFGA
wowy_on|AtRimFGM
wowy_on|AtRimFrequency
wowy_on|AtRimPctAssisted
wowy_on|AtRimPctBlocked
wowy_on|Avg2ptShotDistance
wowy_on|Avg3ptShotDistance
wowy_on|BadPassOutOfBoundsTurnovers
wowy_on|BadPassSteals
wowy_on|BadPassTurnovers
wowy_on|Blocked2s
wowy_on|Blocked3s
wowy_on|BlockedArc3
wowy_on|BlockedAtRim
wowy_on|BlockedCorner3
wowy_on|BlockedLongMidRange
wowy_on|BlockedShortMidRange
wowy_on|Charge Fouls
wowy_on|Corner3Accuracy
wowy_on|Corner3Assists
wowy_on|Corner3FGA
wowy_on|Corner3FGM
wowy_on|Corner3Frequency
wowy_on|Corner3PctAssisted
wowy_on|Corner3PctBlocked
wowy_on|DeadBallTurnovers
wowy_on|EfgPct
wowy_on|FG2A
wowy_on|FG2APctBlocked
wowy_on|FG2M
wowy_on|FG3A
wowy_on|FG3APct
wowy_on|FG3APctBlocked
wowy_on|FG3M
wowy_on|FTA
wowy_on|FTOffRebounds
wowy_on|Fg2Pct
wowy_on|Fg2aBlocked
wowy_on|Fg3Pct
wowy_on|Fg3aBlocked
wowy_on|FirstChancePoints
wowy_on|FoulsDrawn
wowy_on|FtPoints
wowy_on|HeaveAttempts
wowy_on|HeaveMakes
wowy_on|LiveBallTurnoverPct
wowy_on|LiveBallTurnovers
wowy_on|LongMidRangeAccuracy
wowy_on|LongMidRangeAssists
wowy_on|LongMidRangeFGA
wowy_on|LongMidRangeFGM
wowy_on|LongMidRangeFrequency
wowy_on|LongMidRangePctAssisted
wowy_on|LongMidRangePctBlocked
wowy_on|LostBallOutOfBoundsTurnovers
wowy_on|LostBallSteals
wowy_on|LostBallTurnovers
wowy_on|NonHeaveArc3Accuracy
wowy_on|NonHeaveArc3FGA
wowy_on|NonHeaveArc3FGM
wowy_on|NonHeaveFg3Pct
wowy_on|NonPutbacksAssisted2sPct
wowy_on|NonShootingFoulsDrawn
wowy_on|NonShootingPenaltyNonTakeFoulsDrawn
wowy_on|OffArc3ReboundPct
wowy_on|OffAtRimReboundPct
wowy_on|OffCorner3ReboundPct
wowy_on|OffFGReboundPct
wowy_on|OffFTReboundPct
wowy_on|OffLongMidRangeReboundPct
wowy_on|OffRebounds
wowy_on|OffShortMidRangeReboundPct
wowy_on|OffThreePtReboundPct
wowy_on|OffThreePtRebounds
wowy_on|OffTwoPtReboundPct
wowy_on|OffTwoPtRebounds
wowy_on|Offensive Fouls
wowy_on|OffensiveGoaltends
wowy_on|PenaltyArc3Accuracy
wowy_on|PenaltyArc3FGA
wowy_on|PenaltyArc3FGM
wowy_on|PenaltyArc3Frequency
wowy_on|PenaltyAtRimAccuracy
wowy_on|PenaltyAtRimFGA
wowy_on|PenaltyAtRimFGM
wowy_on|PenaltyAtRimFrequency
wowy_on|PenaltyCorner3Accuracy
wowy_on|PenaltyCorner3FGA
wowy_on|PenaltyCorner3FGM
wowy_on|PenaltyCorner3Frequency
wowy_on|PenaltyEfgPct
wowy_on|PenaltyFG2A
wowy_on|PenaltyFG2M
wowy_on|PenaltyFG3A
wowy_on|PenaltyFG3M
wowy_on|PenaltyFg2Pct
wowy_on|PenaltyFg3Pct
wowy_on|PenaltyFtPoints
wowy_on|PenaltyPoints
wowy_on|PenaltyPointsExcludingTakeFouls
wowy_on|PenaltyPointsPct
wowy_on|PenaltyShotQualityAvg
wowy_on|PenaltyTsPct
wowy_on|PenaltyTurnovers
wowy_on|Points
wowy_on|PtsAssisted2s
wowy_on|PtsAssisted3s
wowy_on|PtsPutbacks
wowy_on|PtsUnassisted2s
wowy_on|PtsUnassisted3s
wowy_on|SecondChanceArc3Accuracy
wowy_on|SecondChanceArc3FGA
wowy_on|SecondChanceArc3FGM
wowy_on|SecondChanceArc3Frequency
wowy_on|SecondChanceArc3PctAssisted
wowy_on|SecondChanceAtRimAccuracy
wowy_on|SecondChanceAtRimFGA
wowy_on|SecondChanceAtRimFGM
wowy_on|SecondChanceAtRimFrequency
wowy_on|SecondChanceAtRimPctAssisted
wowy_on|SecondChanceCorner3Accuracy
wowy_on|SecondChanceCorner3FGA
wowy_on|SecondChanceCorner3FGM
wowy_on|SecondChanceCorner3Frequency
wowy_on|SecondChanceCorner3PctAssisted
wowy_on|SecondChanceEfgPct
wowy_on|SecondChanceFG2A
wowy_on|SecondChanceFG2M
wowy_on|SecondChanceFG3A
wowy_on|SecondChanceFG3M
wowy_on|SecondChanceFg2Pct
wowy_on|SecondChanceFg3Pct
wowy_on|SecondChanceFtPoints
wowy_on|SecondChancePoints
wowy_on|SecondChancePointsPct
wowy_on|SecondChanceShotQualityAvg
wowy_on|SecondChanceTsPct
wowy_on|SecondChanceTurnovers
wowy_on|SecondsExcludingORebsPerPossOff
wowy_on|SecondsPerPossOff
wowy_on|SelfOReb
wowy_on|SelfORebPct
wowy_on|ShootingFoulsDrawnPct
wowy_on|ShortMidRangeAccuracy
wowy_on|ShortMidRangeAssists
wowy_on|ShortMidRangeFGA
wowy_on|ShortMidRangeFGM
wowy_on|ShortMidRangeFrequency
wowy_on|ShortMidRangePctAssisted
wowy_on|ShortMidRangePctBlocked
wowy_on|ShotQualityAvg
wowy_on|StepOutOfBoundsTurnovers
wowy_on|ThreePtAssists
wowy_on|ThreePtShootingFoulsDrawn
wowy_on|ThreePtShootingFoulsDrawnPct
wowy_on|Travels
wowy_on|TsPct
wowy_on|Turnovers
wowy_on|TwoPtAssists
wowy_on|TwoPtShootingFoulsDrawn
wowy_on|TwoPtShootingFoulsDrawnPct
wowy_on|UnblockedArc3Accuracy
wowy_on|UnblockedAtRimAccuracy
wowy_on|UnblockedCorner3Accuracy
wowy_on|UnblockedLongMidRangeAccuracy
wowy_on|UnblockedShortMidRangeAccuracy
```

## defense

```
pbp|Blocks
pbp|BlocksRecoveredPct
pbp|Charge Fouls Drawn
pbp|DefArc3ReboundPct
pbp|DefAtRimReboundPct
pbp|DefCorner3ReboundPct
pbp|DefFGReboundPct
pbp|DefFTReboundPct
pbp|DefLongMidRangeReboundPct
pbp|DefRebounds
pbp|DefShortMidRangeReboundPct
pbp|DefThreePtReboundPct
pbp|DefThreePtRebounds
pbp|DefTwoPtReboundPct
pbp|DefTwoPtRebounds
pbp|Defensive 3 Seconds Violations
pbp|DefensiveGoaltends
pbp|FTDefRebounds
pbp|Fouls
pbp|NonShootingPenaltyNonTakeFouls
pbp|Offensive Fouls Drawn
pbp|OnDefRtg
pbp|OpponentPoints
pbp|Period1Fouls2Minutes
pbp|Period2Fouls2Minutes
pbp|Period2Fouls3Minutes
pbp|Period3Fouls3Minutes
pbp|Period3Fouls4Minutes
pbp|Period4Fouls4Minutes
pbp|Period4Fouls5Minutes
pbp|PeriodOTFouls4Minutes
pbp|PeriodOTFouls5Minutes
pbp|RecoveredBlocks
pbp|ShootingFouls
pbp|Steals
track:defensive-impact|BLK
track:defensive-impact|DFG%
track:defensive-impact|DFGA
track:defensive-impact|DFGM
track:defensive-impact|DREB
track:defensive-impact|STL
track:defensive-rebounding|ADJUSTED
DREB CHANCE%
track:defensive-rebounding|AVG DREB
DISTANCE
track:defensive-rebounding|CONTESTED
DREB
track:defensive-rebounding|CONTESTED
DREB%
track:defensive-rebounding|DEFERRED
DREB CHANCES
track:defensive-rebounding|DREB
track:defensive-rebounding|DREB
CHANCE%
track:defensive-rebounding|DREB
CHANCES
track:speed-distance|AVG SPEED DEF
track:speed-distance|DIST. MILES DEF
wowy_off|Blocks
wowy_off|BlocksRecoveredPct
wowy_off|Charge Fouls Drawn
wowy_off|Clear Path Fouls
wowy_off|DefArc3ReboundPct
wowy_off|DefAtRimReboundPct
wowy_off|DefCorner3ReboundPct
wowy_off|DefFGReboundPct
wowy_off|DefFTReboundPct
wowy_off|DefLongMidRangeReboundPct
wowy_off|DefRebounds
wowy_off|DefShortMidRangeReboundPct
wowy_off|DefThreePtReboundPct
wowy_off|DefThreePtRebounds
wowy_off|DefTwoPtReboundPct
wowy_off|DefTwoPtRebounds
wowy_off|Defensive 3 Seconds Violations
wowy_off|DefensiveGoaltends
wowy_off|FTDefRebounds
wowy_off|Fouls
wowy_off|NonShootingPenaltyNonTakeFouls
wowy_off|Offensive Fouls Drawn
wowy_off|OpponentPoints
wowy_off|RecoveredBlocks
wowy_off|SecondsExcludingORebsPerPossDef
wowy_off|SecondsPerPossDef
wowy_off|ShootingFouls
wowy_off|Steals
wowy_on|Blocks
wowy_on|BlocksRecoveredPct
wowy_on|Charge Fouls Drawn
wowy_on|Clear Path Fouls
wowy_on|DefArc3ReboundPct
wowy_on|DefAtRimReboundPct
wowy_on|DefCorner3ReboundPct
wowy_on|DefFGReboundPct
wowy_on|DefFTReboundPct
wowy_on|DefLongMidRangeReboundPct
wowy_on|DefRebounds
wowy_on|DefShortMidRangeReboundPct
wowy_on|DefThreePtReboundPct
wowy_on|DefThreePtRebounds
wowy_on|DefTwoPtReboundPct
wowy_on|DefTwoPtRebounds
wowy_on|Defensive 3 Seconds Violations
wowy_on|DefensiveGoaltends
wowy_on|FTDefRebounds
wowy_on|Fouls
wowy_on|NonShootingPenaltyNonTakeFouls
wowy_on|Offensive Fouls Drawn
wowy_on|OpponentPoints
wowy_on|RecoveredBlocks
wowy_on|SecondsExcludingORebsPerPossDef
wowy_on|SecondsPerPossDef
wowy_on|ShootingFouls
wowy_on|Steals
```

## neutral

```
pbp|DefPoss
pbp|EntityId
pbp|GamesPlayed
pbp|Loose Ball Fouls
pbp|Loose Ball Fouls Drawn
pbp|Minutes
pbp|OffPoss
pbp|PenaltyDefPoss
pbp|PenaltyOffPoss
pbp|PenaltyOffPossExcludingTakeFouls
pbp|PenaltyOffPossPct
pbp|PlusMinus
pbp|Rebounds
pbp|RowId
pbp|SecondChanceOffPoss
pbp|SecondsPlayed
pbp|TeamId
pbp|Technical Free Throw Trips
pbp|TotalPoss
track:catch-shoot|GP
track:catch-shoot|MIN
track:defensive-impact|GP
track:defensive-impact|L
track:defensive-impact|MIN
track:defensive-impact|W
track:defensive-rebounding|GP
track:defensive-rebounding|L
track:defensive-rebounding|MIN
track:defensive-rebounding|W
track:drives|GP
track:drives|L
track:drives|MIN
track:drives|W
track:elbow-touch|GP
track:elbow-touch|L
track:elbow-touch|MIN
track:elbow-touch|W
track:offensive-rebounding|GP
track:offensive-rebounding|L
track:offensive-rebounding|MIN
track:offensive-rebounding|W
track:paint-touch|GP
track:paint-touch|L
track:paint-touch|MIN
track:paint-touch|W
track:passing|GP
track:passing|L
track:passing|MIN
track:passing|W
track:pullup|GP
track:pullup|L
track:pullup|MIN
track:pullup|W
track:rebounding|ADJUSTED
REB CHANCE%
track:rebounding|AVG REB
DISTANCE
track:rebounding|CONTESTED
REB
track:rebounding|CONTESTED
REB%
track:rebounding|DEFERRED
REB CHANCES
track:rebounding|GP
track:rebounding|L
track:rebounding|MIN
track:rebounding|REB
track:rebounding|REB
CHANCE%
track:rebounding|REB
CHANCES
track:rebounding|W
track:shooting-efficiency|GP
track:shooting-efficiency|L
track:shooting-efficiency|MIN
track:shooting-efficiency|W
track:speed-distance|AVG SPEED
track:speed-distance|DIST. FEET
track:speed-distance|DIST. MILES
track:speed-distance|GP
track:speed-distance|L
track:speed-distance|MIN
track:speed-distance|W
track:touches|GP
track:touches|L
track:touches|MIN
track:touches|W
track:tracking-post-ups|GP
track:tracking-post-ups|L
track:tracking-post-ups|MIN
track:tracking-post-ups|W
wowy_off|DefPoss
wowy_off|Loose Ball Fouls
wowy_off|Loose Ball Fouls Drawn
wowy_off|Minutes
wowy_off|OffPoss
wowy_off|Pace
wowy_off|PenaltyDefPoss
wowy_off|PenaltyOffPoss
wowy_off|PenaltyOffPossExcludingTakeFouls
wowy_off|PenaltyOffPossPct
wowy_off|PlusMinus
wowy_off|Rebounds
wowy_off|SecondChanceOffPoss
wowy_off|SecondsPlayed
wowy_off|Technical Free Throw Trips
wowy_off|TotalPoss
wowy_on|DefPoss
wowy_on|Loose Ball Fouls
wowy_on|Loose Ball Fouls Drawn
wowy_on|Minutes
wowy_on|OffPoss
wowy_on|Pace
wowy_on|PenaltyDefPoss
wowy_on|PenaltyOffPoss
wowy_on|PenaltyOffPossExcludingTakeFouls
wowy_on|PenaltyOffPossPct
wowy_on|PlusMinus
wowy_on|Rebounds
wowy_on|SecondChanceOffPoss
wowy_on|SecondsPlayed
wowy_on|Technical Free Throw Trips
wowy_on|TotalPoss
```