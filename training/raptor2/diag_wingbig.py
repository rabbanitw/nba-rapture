"""Group-level bias readout for cv_def_wingbig OOF predictions.

For each arm in a DIAG_wingbig_*.npz: on the >=1065 pool per season,
mean signed rank error (projected rank - actual rank; + = under-ranked)
for bigs vs non-bigs among the actual top-30, and the same among the
projected top-30 (- = over-ranked), plus the named outlier cases.

Run:  python training/raptor2/diag_wingbig.py DIAG_wingbig_s0_....npz
"""

import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
FLOOR = 1065
CASES = [("Frank Ntilikina", "2017-18"), ("Michael Kidd-Gilchrist", "2017-18"),
         ("Luguentz Dort", "2020-21"), ("Derrick White", "2019-20"),
         ("Kenrich Williams", "2022-23"), ("Al Horford", "2017-18"),
         ("Brook Lopez", "2018-19"), ("Zaza Pachulia", "2016-17"),
         ("Andre Drummond", "2015-16"), ("Chris Paul", "2016-17")]


def ranks(v):
    r = np.empty(len(v), dtype=int)
    r[np.argsort(-v)] = np.arange(1, len(v) + 1)
    return r


def main():
    z = np.load(HERE / sys.argv[1])
    c = np.load(HERE.parent / "data_fixed" / "combined.npz", allow_pickle=True)
    player, season, mp = c["player"], c["season"], c["mp"].astype(float)
    y, big, lab = z["y"], z["big"], z["labeled"]
    arms = [k[2:] for k in z.files if k.startswith("p_")]
    ev = lab & (mp >= FLOOR)
    seasons = sorted(set(season[ev]))
    print(f"{'arm':12s} {'act30 big':>9s} {'act30 non':>9s} "
          f"{'prj30 big':>9s} {'prj30 non':>9s}  cases (proj rank / act)")
    for a in arms:
        p = z[f"p_{a}"]
        acc = {k: [] for k in ("ab", "an", "pb", "pn")}
        case_rk = {}
        for s in seasons:
            m = ev & (season == s)
            ra, rp = ranks(y[m]), ranks(p[m])
            err, bg = rp - ra, big[m]
            acc["ab"] += list(err[(ra <= 30) & bg])
            acc["an"] += list(err[(ra <= 30) & ~bg])
            acc["pb"] += list(err[(rp <= 30) & bg])
            acc["pn"] += list(err[(rp <= 30) & ~bg])
            for nm, ss in CASES:
                if ss == s:
                    hit = np.where(player[m] == nm)[0]
                    if len(hit):
                        case_rk[nm.split()[-1]] = f"{rp[hit[0]]}/{ra[hit[0]]}"
        print(f"{a:12s} {np.mean(acc['ab']):+9.1f} {np.mean(acc['an']):+9.1f} "
              f"{np.mean(acc['pb']):+9.1f} {np.mean(acc['pn']):+9.1f}  "
              + " ".join(f"{k}:{v}" for k, v in case_rk.items()))


if __name__ == "__main__":
    main()
