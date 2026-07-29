"""Season / phase logic for wayback timestamps (YYYYMMDDhhmmss).

Follows the repo convention in utils.get_season: a season spans from its
regular-season start up to the next season's regular-season start, so an
offseason snapshot belongs to the season that just finished.
"""

# season -> (regular_season_start, playoffs_start, playoffs_end)
SEASONS = [
    ("2013-14", "2013-10-29", "2014-04-19", "2014-06-15"),
    ("2014-15", "2014-10-28", "2015-04-18", "2015-06-16"),
    ("2015-16", "2015-10-27", "2016-04-16", "2016-06-19"),
    ("2016-17", "2016-10-25", "2017-04-15", "2017-06-12"),
    ("2017-18", "2017-10-17", "2018-04-14", "2018-06-08"),
    ("2018-19", "2018-10-16", "2019-04-13", "2019-06-13"),
    ("2019-20", "2019-10-22", "2020-08-17", "2020-10-11"),
    ("2020-21", "2020-12-22", "2021-05-22", "2021-07-20"),
    ("2021-22", "2021-10-19", "2022-04-16", "2022-06-16"),
    ("2022-23", "2022-10-18", "2023-04-15", "2023-06-12"),
    ("2023-24", "2023-10-24", "2024-04-20", "2024-06-17"),
    ("2024-25", "2024-10-22", "2025-04-19", "2025-06-22"),
    ("2025-26", "2025-10-21", "2026-04-18", "2026-06-13"),
]

TEST_SEASONS = ("2013-14", "2014-15")

# Synthetic full-season snapshots (one row per player for a whole season). The last
# three have stats but no RAPTOR label -- 538 shut down, so they are inference cells,
# not training cells. build_dataset.py keys every row off a 538 document and will
# skip them; leaderboards.py and estimated_raptor.py are what read them.
# Keyed by the season the FEATURES at that timestamp cover, which is what the row
# actually is. The labels for the last three of the labeled cells live under other
# timestamps entirely -- labels.py resolves that.
FULL_SEASON_SNAPSHOTS = {
    "20140715000000": "2013-14",
    "20150715000000": "2014-15",
    "20160715000000": "2015-16",
    "20170715000000": "2016-17",
    "20180715000000": "2017-18",
    # 2018-19 was absent from the collection entirely. Its 538 labels turned out to
    # be sitting at 20201101000000; the features are scraped under this stamp.
    "20190715000000": "2018-19",
    "20201101000000": "2019-20",
    # A seventh whole-season cell that was never registered here: 2020-21 features,
    # with 2019-20 labels wrongly attached to them.
    "20210801000000": "2020-21",
    "20240715000000": "2023-24",
    "20250715000000": "2024-25",
    "20260715000000": "2025-26",
}

# The cells with features but no label, kept separate so a caller can ask for
# "everything trainable" or "everything scoreable" without hardcoding stamps.
UNLABELED_SNAPSHOTS = ("20240715000000", "20250715000000", "20260715000000")


def ws(date):
    return date.replace("-", "") + "000000"


def season_of(ts):
    """Season a timestamp belongs to, or None if before the first tracked season."""
    if ts in FULL_SEASON_SNAPSHOTS:
        return FULL_SEASON_SNAPSHOTS[ts]
    cur = None
    for name, rs, _, _ in SEASONS:
        if ts >= ws(rs):
            cur = name
    return cur


def phase_of(ts):
    """'regular', 'playoffs', or 'offseason' for the season the timestamp belongs to."""
    if ts in FULL_SEASON_SNAPSHOTS:
        return "offseason"
    for _, rs, ps, pe in SEASONS:
        if ws(rs) <= ts < ws(ps):
            return "regular"
        if ws(ps) <= ts <= ws(pe):
            return "playoffs"
    return "offseason"


def season_progress(ts):
    """0..1 fraction of the regular season elapsed at this timestamp.

    Full-season snapshots and anything at/after the playoffs count as 1.0. This
    tells the model how much of a sample the stats are drawn from -- an early
    January snapshot is a far noisier observation than an April one.
    """
    if ts in FULL_SEASON_SNAPSHOTS:
        return 1.0
    season = season_of(ts)
    for name, rs, ps, _ in SEASONS:
        if name == season:
            lo, hi = ws(rs), ws(ps)
            if ts <= lo:
                return 0.0
            if ts >= hi:
                return 1.0
            return (int(ts[:8]) - int(lo[:8])) / max(int(hi[:8]) - int(lo[:8]), 1)
    return 1.0


def is_test(ts):
    return season_of(ts) in TEST_SEASONS
