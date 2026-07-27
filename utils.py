"""Season lookup and date helpers for the scrapers.

Everything here hangs off one SEASONS table. The previous version spelled the same
dates out three times -- a prose table in a docstring, a chain of elif branches in
get_season, and another chain in get_date_range_extended -- which is how the
following got in:

  * The table stopped at 2022-23 and get_season raised on anything past 2025-10-21,
    so no season after 2024-25 could be scraped at all.
  * The 2024-25 finals end date was a guess ("2025-06-19  # Estimated Finals end").
    The finals actually ended 2025-06-22, so a playoff snapshot taken between those
    dates fell through every branch and returned None.
  * get_date_range_extended compared season_type against "Regular season" for
    2021-22 onward but "Regular Season" for 2020-21 and earlier. Every caller passes
    the lowercase spelling, so for 2013-14 through 2020-21 the regular-season branch
    was unreachable and those scrapes used the else branch's full-season range
    instead. It happened to be harmless because pbpstats also filters on its own
    SeasonType parameter, but nothing about the code said so.
  * wowy_scrape.py and nba_tracking_scrape.py both call utils.get_date_range(), which
    did not exist -- both raised AttributeError on their first date lookup.

Dates for 2023-24 onward come from pbpstats' schedule rather than being typed in;
see scraping/season_dates.py, which is the authority for new scrapes. This table is
what the older scripts read, and the two agree.
"""

from datetime import datetime
import re

# season -> (regular season start, regular season end,
#            playoffs start, playoffs end)
# Play-in games sit in the gap between regular season end and playoffs start.
SEASONS = [
    ("2013-14", "2013-10-29", "2014-04-16", "2014-04-19", "2014-06-15"),
    ("2014-15", "2014-10-28", "2015-04-15", "2015-04-18", "2015-06-16"),
    ("2015-16", "2015-10-27", "2016-04-13", "2016-04-16", "2016-06-19"),
    ("2016-17", "2016-10-25", "2017-04-12", "2017-04-15", "2017-06-12"),
    ("2017-18", "2017-10-17", "2018-04-11", "2018-04-14", "2018-06-08"),
    ("2018-19", "2018-10-16", "2019-04-10", "2019-04-13", "2019-06-13"),
    ("2019-20", "2019-10-22", "2020-03-11", "2020-08-17", "2020-10-11"),
    ("2020-21", "2020-12-22", "2021-05-16", "2021-05-22", "2021-07-20"),
    ("2021-22", "2021-10-19", "2022-04-10", "2022-04-16", "2022-06-16"),
    ("2022-23", "2022-10-18", "2023-04-09", "2023-04-15", "2023-06-12"),
    # From here down the dates are pbpstats' first and last game of each split.
    ("2023-24", "2023-10-24", "2024-04-14", "2024-04-20", "2024-06-17"),
    ("2024-25", "2024-10-22", "2025-04-13", "2025-04-19", "2025-06-22"),
    ("2025-26", "2025-10-21", "2026-04-12", "2026-04-18", "2026-06-13"),
]

PLAYOFFS = "playoffs"
REGULAR = "regular"


def _season_type_kind(season_type):
    """Map any of the spellings floating around the scrapers onto one token.

    Callers pass 'Regular season', 'Regular Season', 'Playoffs', 'Play in', 'All',
    'Full season'. Anything that isn't clearly one split is treated as the whole
    season, which is what the old else branch did.
    """
    s = str(season_type or "").strip().lower()
    if s == "playoffs":
        return PLAYOFFS
    if s in ("regular season", "regular"):
        return REGULAR
    return None


def get_season(waystamp):
    """Season a wayback timestamp (YYYYMMDDhhmmss) belongs to.

    A season runs from its opening night up to the next season's opening night, so
    an offseason snapshot belongs to the season that just finished.
    """
    date = str(waystamp)[:8]
    current = None
    for name, rs_start, _, _, _ in SEASONS:
        if date >= rs_start.replace("-", ""):
            current = name
    if current is None:
        raise ValueError(f"timestamp {waystamp} predates the {SEASONS[0][0]} season")
    return current


def season_bounds(season):
    for row in SEASONS:
        if row[0] == season:
            return row[1:]
    raise ValueError(f"unknown season {season!r}")


def get_date_range(waystamp, season_type):
    """[start, end] of the split this snapshot observes, as YYYY-MM-DD.

    The end is the snapshot's own date, clamped to the end of the split -- a
    snapshot taken in March sees the season to date, one taken in the offseason
    sees the whole thing.

    Returns None when the snapshot predates the split it asks about (a January
    snapshot has no playoff data yet), which is the caller's cue to skip it.
    """
    season = get_season(waystamp)
    rs_start, rs_end, po_start, po_end = season_bounds(season)
    kind = _season_type_kind(season_type)
    d = str(waystamp)[:8]
    on = f"{d[:4]}-{d[4:6]}-{d[6:8]}"   # the snapshot's own date

    if kind == PLAYOFFS:
        if on < po_start:
            return None
        return [po_start, min(on, po_end)]
    if kind == REGULAR:
        return [rs_start, min(on, rs_end)]
    return [rs_start, min(on, po_end)]


# Kept because pbp_scrape.py imports this name.
def get_date_range_extended(waystamp, season_type):
    rng = get_date_range(waystamp, season_type)
    if rng is None:
        raise ValueError(f"{season_type} had not started at {waystamp} "
                         f"(season {get_season(waystamp)})")
    return rng


def inside_range(timestamp, end):
    return timestamp < wayback_time(end)


def regular_time(waystamp):
    """Wayback stamp YYYYMMDDhhmmss -> YYYY-MM-DD."""
    return datetime.strptime(str(waystamp), "%Y%m%d%H%M%S").strftime("%Y-%m-%d")


def wayback_time(date):
    """YYYY-MM-DD -> wayback stamp YYYYMMDDhhmmss."""
    return datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d%H%M%S")


def remove_numbers_and_apostrophes(string: str) -> str:
    return re.sub(r"[\d\'\-.]+", "", string)


def reformat_date(timestamp):
    """YYYY-MM-DD -> MM/DD/YYYY, which is what stats.nba.com wants."""
    return datetime.strptime(timestamp, "%Y-%m-%d").strftime("%m/%d/%Y")
