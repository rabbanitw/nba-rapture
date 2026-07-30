"""Split every feature into offence-centric, defence-centric, or neutral.

The point is to stop the offence model reading defensive columns and vice versa.
RAPTOR's own components are defined that way -- rap_o is what a player does to his
team's scoring, rap_d to the opponent's -- so a column that only describes the other
half of the floor is noise the model has to learn to ignore, and 908 columns against
13k rows leaves plenty of room to fit that noise instead.

Rules are ordered and first-match-wins, so the specific cases sit above the general
ones. The pairs that matter and are easy to get backwards:

  Blocks              blocks BY the player            -> defence
  Blocked2s, BlockedAtRim, ...  the player's OWN shot got blocked  -> offence
  Steals              steals BY the player            -> defence
  BadPassSteals, LostBallSteals   the player's turnover WAS a steal -> offence
  Fouls, ShootingFouls            committed                        -> defence
  FoulsDrawn, ...Drawn            drawn                            -> offence
  Offensive Fouls                 committed by him on offence      -> offence
  Offensive Fouls Drawn           he drew it, so he was defending  -> defence
  Charge Fouls / Charge Fouls Drawn        same inversion
  OnOffRtg / OnDefRtg             team rating while he is on
  OpponentPoints                  what the other team scored       -> defence

Exposure columns -- Minutes, possession counts, games, PlusMinus -- are neutral: they
size the sample rather than describe either end.

Whole tracking tables have a polarity of their own: drives, pull-ups, post-ups,
touches, passing and catch-and-shoot are offence by construction; defensive-impact
and defensive-rebounding are defence; the combined rebounding table and
speed-distance are neutral apart from their explicit OFF/DEF splits.

Run:  python training/stat_polarity.py            # print the classification
      python training/stat_polarity.py --dump      # write stat_polarity.md/.json
"""

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

from db import REPO_ROOT

OFF, DEF, NEU = "offense", "defense", "neutral"

# Tracking tables whose every stat belongs to one side.
BLOCK_POLARITY = {
    "track:catch-shoot": OFF,
    "track:pullup": OFF,
    "track:drives": OFF,
    "track:elbow-touch": OFF,
    "track:paint-touch": OFF,
    "track:tracking-post-ups": OFF,
    "track:touches": OFF,
    "track:passing": OFF,
    "track:shooting-efficiency": OFF,
    "track:offensive-rebounding": OFF,
    "track:defensive-impact": DEF,
    "track:defensive-rebounding": DEF,
    "track:rebounding": NEU,        # combined REB; the OREB/DREB tables carry the sides
    "track:speed-distance": NEU,    # effort, not a side -- bar the OFF/DEF splits below
}

# Ordered; first match wins. Applied to the stat name, block handled separately.
RULES = [
    # --- exposure and context -------------------------------------------------
    (NEU, r"^(GP|W|L|MIN|Minutes|SecondsPlayed|GamesPlayed|TotalPoss|PlusMinus)$"),
    (NEU, r"^(OffPoss|DefPoss|PenaltyOffPoss.*|PenaltyDefPoss|SecondChanceOffPoss)$"),
    (NEU, r"^(Rebounds|Technical Free Throw Trips|Pace)$"),
    (NEU, r"^Loose Ball Fouls"),                 # neither side owns a loose ball
    # Seconds per possession: the suffix says whose possession.
    (OFF, r"^Seconds(ExcludingORebs)?PerPossOff$"),
    (DEF, r"^Seconds(ExcludingORebs)?PerPossDef$"),
    # Points scored while in the penalty. "Fouls" appears in the name, so this has to
    # outrank the generic committed-fouls rule below or it lands on defence.
    (OFF, r"^Penalty(Points|FtPoints)"),
    (OFF, r"^(FtPoints|2pt And 1 Free Throw Trips|3pt And 1 Free Throw Trips)$"),
    (NEU, r"^(REB|CONTESTED\nREB|REB\nCHANCES|DEFERRED\nREB|ADJUSTED\nREB|AVG REB)"),

    # --- the inversions, before the general foul/block/steal rules ------------
    (DEF, r"^(Offensive Fouls Drawn|Charge Fouls Drawn)$"),
    (OFF, r"^(Offensive Fouls|Charge Fouls)$"),
    (OFF, r"Drawn"),                             # any *FoulsDrawn -> he was attacking
    (OFF, r"^(BadPassSteals|LostBallSteals)$"),  # his turnover, stolen
    (OFF, r"^Blocked|^Fg2aBlocked$|^Fg3aBlocked$|PctBlocked$"),  # his shot, blocked

    # --- explicit sides ------------------------------------------------------
    (OFF, r"^Off|^SelfOReb|OffRebounded|^FTOffRebounds$|^OnOffRtg$"),
    (DEF, r"^Def|^FTDefRebounds$|^OnDefRtg$|^OpponentPoints$"),
    (OFF, r"(?i)(^|\b)(DIST\. MILES OFF|AVG SPEED OFF)"),
    (DEF, r"(?i)(^|\b)(DIST\. MILES DEF|AVG SPEED DEF)"),

    # --- defensive production and fouls committed ----------------------------
    (DEF, r"^(Blocks|BlocksRecoveredPct|RecoveredBlocks|Steals|STL|BLK|DREB)$"),
    (DEF, r"^(DFGM|DFGA|DFG%)$"),
    (DEF, r"Fouls"),                             # committed; Drawn already claimed
    (DEF, r"^Period\d.*Fouls|^PeriodOTFouls"),   # foul-trouble timing
    (DEF, r"^Defensive "),

    # --- everything a player does with the ball ------------------------------
    (OFF, r"^(Points|Assists|Turnovers|Usage|TsPct|EfgPct|AssistPoints)$"),
    (OFF, r"^(FG2A|FG2M|FG3A|FG3M|FTA|Fg2Pct|Fg3Pct|FG3APct)$"),
    (OFF, r"^(OREB|CONTESTED\nOREB|OREB\nCHANCES|DEFERRED\nOREB|ADJUSTED\nOREB|AVG OREB)"),
    (OFF, r"(AtRim|Arc3|Corner3|MidRange|ThreePt|TwoPt|Heave)"),
    (OFF, r"^(Penalty|SecondChance|FirstChance|Pts|Assisted|NonPutbacks|Shot|Travel|"
          r"StepOutOfBounds|DeadBall|LiveBall|BadPass|LostBall|3SecondViolations|"
          r"OffensiveGoaltends|Avg\dpt)"),
    (OFF, r"^(TOUCHES|PASS|PTS|FGM|FGA|FG%|FTM|FT%|3PM|3PA|3P%|EFG%|AST|TO|TOV%|PF|"
          r"DRIVES|POST|PAINT|ELBOW|FRONT|TIME OF|AVG SEC|AVG DRIB|C&S|PULL UP|"
          r"PASSES|SECONDARY|POTENTIAL|AST )"),
]

COMPILED = [(pol, re.compile(pat)) for pol, pat in RULES]


def classify(feature_name):
    """-> (polarity, the rule index that decided it)."""
    block, stat = feature_name.split("|", 1)

    # An explicitly-sided stat wins even inside a one-sided table: speed-distance is
    # neutral overall but its DIST MILES OFF column is not.
    for i, (pol, rx) in enumerate(COMPILED):
        if rx.search(stat):
            bp = BLOCK_POLARITY.get(block)
            # Inside a one-sided tracking table, a neutral-looking stat name
            # (PTS, FGM) really is that side's -- the table says so.
            if bp in (OFF, DEF) and pol is NEU and not re.match(
                    r"^(GP|W|L|MIN)$", stat):
                return bp, f"block:{block}"
            return pol, f"rule{i}"

    bp = BLOCK_POLARITY.get(block)
    if bp:
        return bp, f"block:{block}"
    return NEU, "unmatched"


def classify_all(feat_names):
    out = {}
    why = {}
    for n in feat_names:
        pol, reason = classify(n)
        out[n] = pol
        why[n] = reason
    return out, why


def feature_mask(feat_names, target, polarity=None):
    """Boolean mask over feat_names for a target.

    offense -> offence + neutral, defense -> defence + neutral, total -> everything.
    Context columns added by add_context (ctx|*, wowy_diff|*) are always kept: they
    are exposure and differentials, not one side's production.
    """
    if polarity is None:
        polarity, _ = classify_all([n for n in feat_names if "|" in n])
    keep = []
    for n in feat_names:
        block, _, stat = n.partition("|")
        if block == "ctx":
            # position, minutes, season progress, playoff flag -- exposure, not a side
            keep.append(True)
            continue
        if block == "wowy_diff":
            # An on-minus-off differential is only as sided as the stat underneath it:
            # the team's points differential with a player on is an offence signal, so
            # a defence model has no more business with it than with wowy_on|Points.
            pol = polarity.get(f"wowy_on|{stat}", NEU)
        else:
            pol = polarity.get(n, NEU)
        if target == "offense":
            keep.append(pol in (OFF, NEU))
        elif target == "defense":
            keep.append(pol in (DEF, NEU))
        else:
            keep.append(True)
    return np.array(keep)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datadir", default=str(REPO_ROOT / "training" / "data_fixed"))
    ap.add_argument("--dump", action="store_true")
    args = ap.parse_args()

    d = np.load(Path(args.datadir) / "combined.npz", allow_pickle=True)
    feat = list(d["feat_names"])
    pol, why = classify_all(feat)

    counts = Counter(pol.values())
    print(f"{len(feat)} features: " + ", ".join(
        f"{k}={counts[k]}" for k in (OFF, DEF, NEU)))
    print(f"\noffense model would use {counts[OFF] + counts[NEU]} "
          f"({100*(counts[OFF]+counts[NEU])/len(feat):.0f}%), "
          f"defense model {counts[DEF] + counts[NEU]} "
          f"({100*(counts[DEF]+counts[NEU])/len(feat):.0f}%)")

    by_block = defaultdict(Counter)
    for n, p in pol.items():
        by_block[n.split("|", 1)[0]][p] += 1
    print(f"\n{'block':<28} {'off':>5} {'def':>5} {'neu':>5}")
    for b in sorted(by_block):
        c = by_block[b]
        print(f"{b:<28} {c[OFF]:>5} {c[DEF]:>5} {c[NEU]:>5}")

    unmatched = [n for n in feat if why[n] == "unmatched"]
    print(f"\nunmatched (defaulted to neutral): {len(unmatched)}")
    for n in unmatched[:25]:
        print(f"   {n}")

    if args.dump:
        Path(REPO_ROOT / "training" / "stat_polarity.json").write_text(
            json.dumps({"polarity": pol, "reason": why}, indent=1))
        lines = ["# Stat polarity", "",
                 f"{len(feat)} features: **{counts[OFF]} offense**, "
                 f"**{counts[DEF]} defense**, **{counts[NEU]} neutral**.", "",
                 "The offense model uses offense+neutral, the defense model "
                 "defense+neutral, and total uses everything.", "",
                 "| block | offense | defense | neutral |", "|---|---:|---:|---:|"]
        for b in sorted(by_block):
            c = by_block[b]
            lines.append(f"| {b} | {c[OFF]} | {c[DEF]} | {c[NEU]} |")
        for side in (OFF, DEF, NEU):
            lines += ["", f"## {side}", ""]
            names = sorted(n for n in feat if pol[n] == side)
            lines.append("```")
            lines += names
            lines.append("```")
        Path(REPO_ROOT / "training" / "stat_polarity.md").write_text("\n".join(lines))
        print("\nwrote stat_polarity.json and stat_polarity.md")


if __name__ == "__main__":
    main()
