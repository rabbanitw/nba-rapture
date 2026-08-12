"""Row-aligned features from the nba-shotdash scrape -> data_fixed/shotdash.npz.

Two blocks, aligned to combined.npz rows by (timestamp, season_type, player):

  R   raw verbatim columns from the five tables, prefixed sd:<table>|<col>
      (4 defender-distance buckets of the player's own shots + possessions)
  E   engineered per 538's methodology text:
        covered3pa      1.00*FG3A(0-2ft) + 0.80*(2-4ft) + 0.57*(4-6ft) + 0.31*(6+ft)
                        -- their exact "covered" weights for the spacing variable
        covered3_per36, covered_share3 (covered3pa / total 3PA)
        tight3_share    (0-2 + 2-4 buckets) / total 3PA
        wideopen3_pct   FG3_PCT in the 6+ ft bucket
        time_poss_per36, sec_per_touch, drib_per_touch, touches_per36

Run:  python training/extract_shotdash.py
"""

import numpy as np

from db import REPO_ROOT, get_collection
from coverage import as_float

TD = REPO_ROOT / "training"
TABLES = ("shots-def0-2", "shots-def2-4", "shots-def4-6", "shots-def6plus",
          "possessions")
META = {"_id", "PLAYER", "name", "standard_name", "nba_player_id", "source",
        "data_type", "timestamp", "season_type"}
COVER_W = {"shots-def0-2": 1.00, "shots-def2-4": 0.80,
           "shots-def4-6": 0.57, "shots-def6plus": 0.31}


def main():
    d = np.load(TD / "data_fixed" / "combined.npz", allow_pickle=True)
    players, tss, sts = d["player"], d["timestamp"], d["season_type"]
    mp = d["mp"].astype(np.float64)
    n = len(players)
    coll = get_collection()

    docs = {}          # (ts, st, name) -> {table: doc}
    cols = {t: set() for t in TABLES}
    for doc in coll.find({"source": "nba-shotdash"}):
        key = (str(doc["timestamp"]), str(doc["season_type"]),
               str(doc["standard_name"]))
        docs.setdefault(key, {})[doc["data_type"]] = doc
        cols[doc["data_type"]].update(k for k in doc if k not in META)
    print(f"{len(docs)} (cell, player) entries from Mongo", flush=True)

    names_r = [f"sd:{t}|{c}" for t in TABLES for c in sorted(cols[t])]
    slot = {nm: j for j, nm in enumerate(names_r)}
    R = np.full((n, len(names_r)), np.nan)
    enames = ["covered3pa", "covered3_per36", "covered_share3", "tight3_share",
              "wideopen3_pct", "time_poss_per36", "sec_per_touch",
              "drib_per_touch", "touches_per36"]
    E = np.full((n, len(enames)), np.nan)

    hit = 0
    for i in range(n):
        entry = docs.get((str(tss[i]), str(sts[i]), str(players[i])))
        if not entry:
            continue
        hit += 1
        f3a = {}
        for t, doc in entry.items():
            for c in cols[t]:
                v = as_float(doc.get(c))
                if v is not None:
                    R[i, slot[f"sd:{t}|{c}"]] = v
            if t in COVER_W:
                f3a[t] = as_float(doc.get("FG3A")) or 0.0
        if f3a:
            cov = sum(COVER_W[t] * f3a.get(t, 0.0) for t in COVER_W)
            tot3 = sum(f3a.values())
            E[i, 0] = cov
            if mp[i] > 0:
                E[i, 1] = cov * 36.0 / mp[i]
            if tot3 >= 10:
                E[i, 2] = cov / tot3
                E[i, 3] = (f3a.get("shots-def0-2", 0.0)
                           + f3a.get("shots-def2-4", 0.0)) / tot3
            wo = entry.get("shots-def6plus")
            if wo is not None:
                v = as_float(wo.get("FG3_PCT"))
                if v is not None and (as_float(wo.get("FG3A")) or 0) >= 20:
                    E[i, 4] = v
        po = entry.get("possessions")
        if po is not None:
            top = as_float(po.get("TIME_OF_POSS"))
            if top is not None and mp[i] > 0:
                E[i, 5] = top * 36.0 / mp[i]
            for j, c in ((6, "AVG_SEC_PER_TOUCH"), (7, "AVG_DRIB_PER_TOUCH")):
                v = as_float(po.get(c))
                if v is not None:
                    E[i, j] = v
            v = as_float(po.get("TOUCHES"))
            if v is not None and mp[i] > 0:
                E[i, 8] = v * 36.0 / mp[i]

    print(f"matched {hit}/{n} rows; R {R.shape}, E {E.shape}", flush=True)
    cov = np.isfinite(R).mean(0)
    print(f"raw col coverage: median {np.median(cov):.2f}", flush=True)
    np.savez_compressed(TD / "data_fixed" / "shotdash.npz",
                        R=R, rnames=np.array(names_r),
                        E=E, enames=np.array(enames))
    print("wrote data_fixed/shotdash.npz", flush=True)


if __name__ == "__main__":
    main()
