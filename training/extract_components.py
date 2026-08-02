"""Attach 538's component labels (rap_box_o/d, rap_onoff_o/d) to the built rows.

RAPTOR is two models combined -- a box/tracking component and an on/off component --
and 538 published both components per player, which our 538 documents carry. The
direct models have been predicting the blended rap_o/rap_d from everything at once;
mimicking the real architecture needs the intermediate labels row-aligned with
data_fixed/combined.npz.
"""
import sys, json
import numpy as np
sys.path.insert(0, '..')
import utils
from db import get_collection
from labels import labels_for
from coverage import as_float

d = np.load('data_fixed/combined.npz', allow_pickle=True)
players, tss, sts = d['player'], d['timestamp'], d['season_type']
coll = get_collection()

cols = ['rap_box_o', 'rap_box_d', 'rap_onoff_o', 'rap_onoff_d', 'rap_o', 'rap_d']
out = {c: np.full(len(players), np.nan) for c in cols}
cache = {}
for i, (p, ts, st) in enumerate(zip(players, tss, sts)):
    key = (str(ts), str(st))
    if key not in cache:
        cache[key] = labels_for(coll, str(ts), str(st), utils.get_season(str(ts)))
        print(f"  cell {key}: {len(cache[key])} labels", flush=True)
    doc = cache[key].get(str(p))
    if doc:
        for c in cols:
            v = as_float(doc.get(c))
            if v is not None:
                out[c][i] = v

for c in cols:
    n = np.isfinite(out[c]).sum()
    print(f"{c}: {n}/{len(players)} rows matched")
np.savez_compressed('data_fixed/components.npz', **out)
print("wrote data_fixed/components.npz")
