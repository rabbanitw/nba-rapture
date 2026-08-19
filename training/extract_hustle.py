"""Row-aligned features from the nba-hustle scrape -> data_fixed/hustle.npz.

Two blocks, aligned to combined.npz rows by (timestamp, season_type, player):

  R   raw verbatim columns from the five tables, prefixed hd:<table>|<col>
      (hustle 2015-16+, defend dashboards 2013-14+, matchups 2017-18+;
      earlier rows stay NaN)
  E   engineered defense-side rates:
        deflections/loose balls/charges/contested (per36, combined-mp denom)
        rim/overall/3pt defended volume per36 + defended pct + pct-plusminus
          (attempt-count guards on every pct)
        matchup partial-poss per36 + opponent scoring / ast / tov / sfl per
          100 partial poss + eFG allowed while guarding

Run:  python training/extract_hustle.py
"""

import numpy as np

from db import REPO_ROOT, get_collection
from coverage import as_float

TD = REPO_ROOT / "training"
TABLES = ("hustle", "defend-overall", "defend-rim", "defend-3pt", "matchups")
META = {"_id", "PLAYER", "name", "standard_name", "nba_player_id", "source",
        "data_type", "timestamp", "season_type"}

ENAMES = [
    "deflections_per36", "loose_balls_per36", "charges_per36",
    "contested2_per36", "contested3_per36", "screen_ast_per36",
    "def_boxouts_per36",
    "rim_dfga_per36", "rim_dfg_pct", "rim_plusminus",
    "ov_dfga_per36", "ov_dfg_pct", "ov_plusminus",
    "d3_fga_per36", "d3_pct", "d3_plusminus",
    "mu_poss_per36", "mu_pts_per100", "mu_ast_per100", "mu_tov_per100",
    "mu_sfl_per100", "mu_efg_allowed", "mu_fta_per100",
]
E_IDX = {nm: j for j, nm in enumerate(ENAMES)}


def main():
    d = np.load(TD / "data_fixed" / "combined.npz", allow_pickle=True)
    players, tss, sts = d["player"], d["timestamp"], d["season_type"]
    mp = d["mp"].astype(np.float64)
    n = len(players)
    coll = get_collection()

    docs = {}
    cols = {t: set() for t in TABLES}
    for doc in coll.find({"source": "nba-hustle"}):
        key = (str(doc["timestamp"]), str(doc["season_type"]),
               str(doc["standard_name"]))
        docs.setdefault(key, {})[doc["data_type"]] = doc
        cols[doc["data_type"]].update(k for k in doc if k not in META)
    print(f"{len(docs)} (cell, player) entries from Mongo", flush=True)

    names_r = [f"hd:{t}|{c}" for t in TABLES for c in sorted(cols[t])]
    slot = {nm: j for j, nm in enumerate(names_r)}
    R = np.full((n, len(names_r)), np.nan)
    E = np.full((n, len(ENAMES)), np.nan)

    def per36(i, name, v):
        if v is not None and mp[i] > 0:
            E[i, E_IDX[name]] = v * 36.0 / mp[i]

    hit = 0
    for i in range(n):
        entry = docs.get((str(tss[i]), str(sts[i]), str(players[i])))
        if not entry:
            continue
        hit += 1
        for t, doc in entry.items():
            for c in cols[t]:
                v = as_float(doc.get(c))
                if v is not None:
                    R[i, slot[f"hd:{t}|{c}"]] = v

        hu = entry.get("hustle")
        if hu is not None:
            for nm, c in (("deflections_per36", "DEFLECTIONS"),
                          ("loose_balls_per36", "LOOSE_BALLS_RECOVERED"),
                          ("charges_per36", "CHARGES_DRAWN"),
                          ("contested2_per36", "CONTESTED_SHOTS_2PT"),
                          ("contested3_per36", "CONTESTED_SHOTS_3PT"),
                          ("screen_ast_per36", "SCREEN_ASSISTS"),
                          ("def_boxouts_per36", "DEF_BOXOUTS")):
                per36(i, nm, as_float(hu.get(c)))

        for t, fga_c, pct_c, pm_c, pre, guard in (
                ("defend-rim", "FGA_LT_06", "LT_06_PCT", "PLUSMINUS",
                 "rim", 50),
                ("defend-overall", "D_FGA", "D_FG_PCT", "PCT_PLUSMINUS",
                 "ov", 100),
                ("defend-3pt", "FG3A", "FG3_PCT", "PLUSMINUS", "d3", 50)):
            doc = entry.get(t)
            if doc is None:
                continue
            fga = as_float(doc.get(fga_c))
            per36(i, f"{pre}_{'dfga' if pre != 'd3' else 'fga'}_per36", fga)
            if fga is not None and fga >= guard:
                for nm, c in ((f"{pre}_{'dfg_pct' if pre != 'd3' else 'pct'}",
                               pct_c), (f"{pre}_plusminus", pm_c)):
                    v = as_float(doc.get(c))
                    if v is not None:
                        E[i, E_IDX[nm]] = v

        mu = entry.get("matchups")
        if mu is not None:
            poss = as_float(mu.get("PARTIAL_POSS"))
            per36(i, "mu_poss_per36", poss)
            if poss is not None and poss >= 300:
                for nm, c in (("mu_pts_per100", "PLAYER_PTS"),
                              ("mu_ast_per100", "MATCHUP_AST"),
                              ("mu_tov_per100", "MATCHUP_TOV"),
                              ("mu_sfl_per100", "SFL"),
                              ("mu_fta_per100", "MATCHUP_FTA")):
                    v = as_float(mu.get(c))
                    if v is not None:
                        E[i, E_IDX[nm]] = v * 100.0 / poss
                fga = as_float(mu.get("MATCHUP_FGA"))
                if fga is not None and fga >= 100:
                    fgm = as_float(mu.get("MATCHUP_FGM")) or 0.0
                    fg3m = as_float(mu.get("MATCHUP_FG3M")) or 0.0
                    E[i, E_IDX["mu_efg_allowed"]] = (fgm + 0.5 * fg3m) / fga

    print(f"matched {hit}/{n} rows; R {R.shape}, E {E.shape}", flush=True)
    for j, nm in enumerate(ENAMES):
        print(f"  {nm:<20} coverage {np.isfinite(E[:, j]).mean():.2f}")
    np.savez_compressed(TD / "data_fixed" / "hustle.npz",
                        R=R, rnames=np.array(names_r),
                        E=E, enames=np.array(ENAMES))
    print("wrote data_fixed/hustle.npz", flush=True)


if __name__ == "__main__":
    main()
