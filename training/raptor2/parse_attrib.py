"""Re-parse possessions keeping scorer/rebounder attribution, for the
positional-matchup variables (538's "positional opponents' points/rebounds" --
the one defensive input with no counterpart anywhere in our matrix).

Reuses the validated build_rapm parser; adds, per possession, the attributed
events: who scored how many points, who took each offensive/defensive rebound.
Seasons 2013-2018 (the lineup data already on disk), regular season.

Outputs per season in /tmp/rapm_build:
  attrib_poss_<season>.csv    o0..o4, d0..d4, pts          (as before)
  attrib_events_<season>.csv  poss_idx, kind{score,oreb,dreb}, player_id, value

Run:  python training/raptor2/parse_attrib.py
"""

import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import build_rapm as BR          # noqa: E402  (parser + offline patches)
import pandas as pd              # noqa: E402
import play_by_play_utils as U   # noqa: E402

BUILD = Path("/tmp/rapm_build")
SEASONS = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]


def parse_game_attrib(g):
    g = g.sort_values("EVENTNUM").reset_index(drop=True)
    for c in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        g[c] = g[c].fillna("")
    g = BR.add_time_elapsed(g)
    hid, aid = BR.team_ids(g)
    if hid is None:
        return [], []
    g = BR.lineups_offline(g)
    if g is None:
        return [], []
    rows = list(g.iterrows())
    poss_out, ev_out = [], []
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
            team = BR._possession_team(last, hid, aid)
            if team is not None:
                pts = 0
                events = []
                for p in cur:
                    if (U.is_made_shot(p) or (U.is_free_throw(p)
                                              and not U.is_miss(p))):
                        if p["PLAYER1_TEAM_ID"] == team:
                            v = BR._points(p)
                            pts += v
                            events.append(("score", int(p["PLAYER1_ID"]), v))
                    elif U.is_rebound(p) and not U.is_team_rebound(p):
                        try:
                            rt = int(p["PLAYER1_TEAM_ID"])
                        except (ValueError, TypeError):
                            continue
                        kind = "oreb" if rt == team else "dreb"
                        events.append((kind, int(p["PLAYER1_ID"]), 1))
                home5 = [int(last[c]) for c in BR.HP]
                away5 = [int(last[c]) for c in BR.AP]
                off5, def5 = (home5, away5) if team == hid else (away5, home5)
                idx = len(poss_out)
                poss_out.append(off5 + def5 + [pts])
                ev_out += [(idx, k, pid, v) for k, pid, v in events]
            cur = []
    return poss_out, ev_out


def main():
    for season in SEASONS:
        pc = BUILD / f"attrib_poss_{season}.csv"
        if pc.exists():
            print(f"[{season}] cached", flush=True)
            continue
        df = pd.read_csv(BUILD / f"nbastats_{season}.csv")
        precs, erecs = [], []
        t0 = time.time()
        offset = 0
        for k, (gid, g) in enumerate(df.groupby("GAME_ID", sort=True)):
            p, e = parse_game_attrib(g)
            precs += p
            erecs += [(offset + i, kind, pid, v) for i, kind, pid, v in e]
            offset = len(precs)
            if (k + 1) % 300 == 0:
                print(f"  [{season}] {k+1} games ({time.time()-t0:.0f}s)",
                      flush=True)
        cols = [f"o{i}" for i in range(5)] + [f"d{i}" for i in range(5)] + ["pts"]
        pd.DataFrame(precs, columns=cols).to_csv(pc, index=False)
        pd.DataFrame(erecs, columns=["poss_idx", "kind", "player_id", "value"]
                     ).to_csv(BUILD / f"attrib_events_{season}.csv", index=False)
        print(f"[{season}] {len(precs):,} poss, {len(erecs):,} events",
              flush=True)
    print("done", flush=True)


if __name__ == "__main__":
    main()
