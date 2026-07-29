"""Pick the 538 label rows that belong with a given cell's features.

The pipeline used to take every 538 document at a timestamp as that cell's labels.
That is wrong wherever the archived page was showing a season other than the one the
stats were scraped for, which is most of the modern range:

  268/444 snapshots  label season == feature season
  174 snapshots      hold two seasons at once (the completed table plus the one in
                     progress), so the correct rows are present but mixed with stale ones
    1 snapshot       20201101000000: 2018-19 labels against 2019-20 features
    1 snapshot       20210801000000: 2019-20 labels against 2020-21 features

migrate_label_season.py wrote the true season onto every 538 document as
label_season, so selection is now explicit rather than assumed.

Two lookup modes, because the two kinds of cell mean different things:

  same-timestamp  An in-season snapshot is a season-to-date observation, and its
                  label has to be the season-to-date RAPTOR from that same capture.
                  Only documents at this timestamp are eligible.

  season-wide     A whole-season cell wants the finished season's table, and 538
                  published that on a page archived after the season ended, under
                  some later timestamp. So for these, the best complete table for
                  the season is used wherever it lives. This is what recovers
                  2018-19, 2019-20 and 2020-21 as usable labeled seasons.
"""

from collections import defaultdict

# Feature season -> the whole-season cell that should carry it. Their features and
# their labels sit under different timestamps; see the module docstring.
SEASON_WIDE = {
    "20140715000000": "2013-14",
    "20150715000000": "2014-15",
    "20160715000000": "2015-16",
    "20170715000000": "2016-17",
    "20180715000000": "2017-18",
    "20190715000000": "2018-19",
    "20201101000000": "2019-20",
    "20210801000000": "2020-21",
}

_cache = {}


def best_season_table(coll, season, season_type):
    """The most complete 538 table for a finished season, from whichever snapshot.

    538 reissued the same finished-season table under many later captures; they
    agree, so 'most rows' just picks the one that was fully scraped.
    """
    key = (season, season_type)
    if key in _cache:
        return _cache[key]
    by_ts = defaultdict(dict)
    for d in coll.find({"source": "538", "label_season": season,
                        "season_type": season_type}):
        by_ts[d["timestamp"]].setdefault(d["standard_name"], d)
    if not by_ts:
        _cache[key] = {}
        return {}

    # Total minutes, not row count. 538 republished the same season under hundreds
    # of captures, some taken while it was still being played -- those can carry
    # plenty of rows but partial minutes. The finished table is the one where the
    # season's minutes are fully accumulated. Picking by row count instead left the
    # 2020-21 cell 8.4% short against its own features; by minutes it is exact.
    def total_minutes(table):
        tot = 0.0
        for d in table.values():
            v = str(d.get("mp") or "").replace(",", "")
            try:
                tot += float(v)
            except ValueError:
                pass
        return tot

    best = max(by_ts.values(), key=lambda t: (total_minutes(t), len(t)))
    _cache[key] = best
    return best


def labels_for(coll, ts, season_type, feature_season):
    """-> {standard_name: 538 doc} for this cell, or {} if there is no honest match."""
    if SEASON_WIDE.get(ts):
        return best_season_table(coll, SEASON_WIDE[ts], season_type)

    out = {}
    for d in coll.find({"source": "538", "timestamp": ts,
                        "season_type": season_type,
                        "label_season": feature_season}):
        out.setdefault(d["standard_name"], d)
    return out
