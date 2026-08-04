"""Row-align the opponent on/off documents with data_fixed/combined.npz.

For each row (player, timestamp, Regular season) pulls the wowy-opp on and off
documents and lays their 229 stat fields out as two matrices. Playoff rows get NaN
throughout -- the opponent scrape was regular-season only, as is production.

Output: data_fixed/wowyopp.npz with on_X, off_X (n_rows x n_fields), fields.
"""

import sys
import numpy as np

sys.path.insert(0, "..")
from coverage import as_float
from db import get_collection

KEYS = {"_id", "name", "standard_name", "team", "n_stints", "source",
        "timestamp", "season_type", "on_or_off"}

d = np.load("data_fixed/combined.npz", allow_pickle=True)
players, tss, sts = d["player"], d["timestamp"], d["season_type"]
coll = get_collection()

# field universe from a sample of docs
fields = set()
for doc in coll.find({"source": "wowy-opp"}).limit(300):
    fields.update(k for k in doc if k not in KEYS)
fields = sorted(fields)
fi = {f: i for i, f in enumerate(fields)}
print(f"{len(fields)} opponent stat fields")

on_X = np.full((len(players), len(fields)), np.nan, dtype=np.float32)
off_X = np.full((len(players), len(fields)), np.nan, dtype=np.float32)

cache = {}
for i, (p, ts, st) in enumerate(zip(players, tss, sts)):
    key = (str(ts), str(st))
    if key not in cache:
        cell = {}
        for doc in coll.find({"source": "wowy-opp", "timestamp": str(ts),
                              "season_type": str(st)}):
            cell[(doc["standard_name"], doc["on_or_off"])] = doc
        cache[key] = cell
        if cell:
            print(f"  cell {key}: {len(cell)} docs", flush=True)
    for side, M in (("on", on_X), ("off", off_X)):
        doc = cache[key].get((str(p), side))
        if doc:
            for k, v in doc.items():
                if k in fi:
                    x = as_float(v)
                    if x is not None:
                        M[i, fi[k]] = x

n_on = np.isfinite(on_X).any(axis=1).sum()
print(f"rows with opponent data: {n_on}/{len(players)}")
np.savez_compressed("data_fixed/wowyopp.npz", on_X=on_X, off_X=off_X,
                    fields=np.array(fields, dtype=object))
print("wrote data_fixed/wowyopp.npz")
