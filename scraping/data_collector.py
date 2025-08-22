import json
import pymongo
from pymongo import MongoClient
from typing import Iterable, Dict, Any, Optional
import os
import pickle
from datetime import datetime
import re


username = 'nbarapture'
password = 'cdTMM9n3Awh4ntQw'

MONGO_URI = (
  f"mongodb+srv://{username}:{password}@nba-rapture-2.qnfzf.mongodb.net/"
  "?retryWrites=true&w=majority&appName=nba-rapture-2"
)
client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000000, socketTimeoutMS=3000000)
db = client["nba_rapture"]
coll = db["nba_rapture"]


def fetch_538(season_type: str, timestamp: str, standard_name: str,
                    *,
                    projection: dict = None,
                    sort: list = None):
    """
    Return the *first* document where
        source        == '538'
        standard_name == player_name
        timestamp     == timestamp   (if provided)

    Parameters
    ----------
    player_name : str
    timestamp   : str | None  – exact match (leave None to ignore)
    projection  : dict | None – same rules as MongoDB projection
    sort        : list | None – e.g. [('timestamp', -1)] to pick newest first

    Returns
    -------
    dict | None
        The matching document, or None if no match.
    """
    query = {"source": "538", "standard_name": standard_name, "timestamp": timestamp,
             "season_type": season_type}

    data = coll.find_one(query, projection=projection, sort=sort)
    if not data:
      return False

    path = os.path.join(
        "/content/drive/MyDrive/nba-ml/Docs/nba-rapture/",
        "Data", season_type, timestamp, standard_name, "538"
    )
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "538.pkl"), "wb") as f:
        pickle.dump(data, f)

    return data


def collect_pbp(season_type: str, timestamp: str, name: str, *,
                     projection: dict = None, sort: list = None,
                    verbose = 0):
    with open("PBP_KEYS.json", "r", encoding="utf-8") as f:
        PBP_KEYS = set(json.load(f))

    BAD_KEYS = {"name", "timestamp", "season_type",
     "source", "standard_name", "_id", "data_type", "ShortName"}

    LABEL_FIELDS = {"source", "standard_name", "season_type", "timestamp"}

    # Build one query for all categories
    query = {
        "source": "pbp",
        "standard_name": name,
        "season_type": season_type,   # remove these two lines if your docs don't have them
        "timestamp": timestamp,
    }

    exclude_keys = BAD_KEYS - LABEL_FIELDS

    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        # allow caller overrides, but never allow removing data_type
        proj.update({k: v for k, v in projection.items()})

    cursor = coll.find(query, projection=proj, batch_size=64)
    docs = list(cursor)
    full_stats = []

    if not docs:
          #print(f"PBP data missing for {name}!")
          return False

    d = docs[0]
    # drop label fields before collecting values
    for k in list(d.keys()):
        if (k in LABEL_FIELDS) or (k not in PBP_KEYS):
            d.pop(k, None)
    full_stats.extend(d.values())

    path = os.path.join(
        "/content/drive/MyDrive/nba-ml/Docs/nba-rapture/",
        "Data", season_type, timestamp, name, "pbp"
    )
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "pbp.pkl"), "wb") as f:
        pickle.dump(full_stats, f)

    #print("Done collecting pbp!")
    return full_stats


def collect_wowy_off(season_type: str, timestamp: str, name: str, *,
                     projection: dict = None, sort: list = None,
                    verbose = 0):
    with open("WOWY_OFF_KEYS.json", "r", encoding="utf-8") as f:
        WOWY_OFF_KEYS = set(json.load(f))

    BAD_KEYS = {"name", "timestamp", "season_type",
     "source", "standard_name", "_id", "data_type"}

    LABEL_FIELDS = {"on_or_off", "source", "standard_name", "season_type", "timestamp"}

    # Build one query for all categories
    query = {
        "source": "wowy",
        "standard_name": name,
        "on_or_off": "off",
        "season_type": season_type,   # remove these two lines if your docs don't have them
        "timestamp": timestamp,
    }

    exclude_keys = BAD_KEYS - LABEL_FIELDS

    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        # allow caller overrides
        proj.update({k: v for k, v in projection.items()})

    cursor = coll.find(query, projection=proj, batch_size=64)
    docs = list(cursor)
    full_stats = []

    if not docs:
          #print(f"Wowy OFF data for {name} missing!")
          return False

    d = docs[0]
    # drop label fields before collecting values
    for k in list(d.keys()):
        if (k in LABEL_FIELDS) or (k not in WOWY_OFF_KEYS):
            d.pop(k, None)
    full_stats.extend(d.values())

    path = os.path.join(
        "/content/drive/MyDrive/nba-ml/Docs/nba-rapture/",
        "Data", season_type, timestamp, name, "wowy_off"
    )
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "wowy_off.pkl"), "wb") as f:
        pickle.dump(full_stats, f)

    #print("Done collecting wowy-off!")
    return True

def collect_wowy_on(season_type: str, timestamp: str, name: str, *,
                     projection: dict = None, sort: list = None,
                    verbose = 0):
    with open("WOWY_ON_KEYS.json", "r", encoding="utf-8") as f:
        WOWY_ON_KEYS = set(json.load(f))

    BAD_KEYS = {"name", "timestamp", "season_type",
     "source", "standard_name", "_id", "data_type"}

    LABEL_FIELDS = {"on_or_off", "source", "standard_name", "season_type", "timestamp"}

    # Build one query for all categories
    query = {
        "source": "wowy",
        "standard_name": name,
        "on_or_off": "on",
        "season_type": season_type,   # remove these two lines if your docs don't have them
        "timestamp": timestamp,
    }

    exclude_keys = BAD_KEYS - LABEL_FIELDS

    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        # allow caller overrides, but never allow removing data_type
        proj.update({k: v for k, v in projection.items()})

    cursor = coll.find(query, projection=proj, batch_size=64)
    docs = list(cursor)
    full_stats = []

    if not docs:
          #print(f"Wowy ON data for {name} missing!")
          return False

    d = docs[0]
    # drop label fields before collecting values
    for k in list(d.keys()):
        if k in LABEL_FIELDS:
            d.pop(k, None)
    full_stats.extend(d.values())

    path = os.path.join(
        "/content/drive/MyDrive/nba-ml/Docs/nba-rapture/",
        "Data", season_type, timestamp, name, "wowy_on"
    )
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "wowy_on.pkl"), "wb") as f:
        pickle.dump(full_stats, f)

    #print("Done collecting wowy-on!")
    return True


def collect_tracking(season_type: str, timestamp: str, name: str, *,
                     projection: dict = None, sort: list = None):
    with open("BAD_TRACKING_KEYS.json", "r", encoding="utf-8") as f:
        bad_keys = set(json.load(f))

    TRACK_TYPES = [
      'catch-shoot','defensive-impact','defensive-rebounding','drives',
      'elbow-touch','offensive-rebounding','paint-touch','passing','pullup',
      'rebounding','shooting-efficiency','speed-distance','touches','tracking-post-ups'
    ]

    LABEL_FIELDS = {"data_type", "source", "standard_name", "season_type", "timestamp"}

    # Build one query for all categories
    query = {
        "source": "nba-tracking",
        "standard_name": name,
        "data_type": {"$in": TRACK_TYPES},
        "season_type": season_type,   # remove these two lines if your docs don't have them
        "timestamp": timestamp,
    }

    # Exclude BAD_KEYS but ALWAYS keep label fields
    exclude_keys = bad_keys - LABEL_FIELDS
    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        # allow caller overrides, but never allow removing data_type
        proj.update({k: v for k, v in projection.items() if k != "data_type"})

    cursor = coll.find(query, projection=proj, batch_size=64)
    docs = list(cursor)

    if not docs:
        #print(f"NBA Tracking missing for {name}!")
        return False

    # Sanity check to catch projection issues early
    if "data_type" not in docs[0]:
        raise RuntimeError(
            "Projection removed 'data_type'. Ensure LABEL_FIELDS are not excluded "
            "and BAD_TRACKING_KEYS.json doesn't get applied to them."
        )

    by_cat = {d["data_type"]: d for d in docs}

    full_stats = []
    missing = []
    for cat in TRACK_TYPES:
        d = by_cat.get(cat)
        if not d:
            missing.append(cat)
            continue
        # drop label fields before collecting values
        for k in list(d.keys()):
            if k in LABEL_FIELDS:
                d.pop(k, None)
        full_stats.extend(d.values())

    if missing:
        #print("Missing categories:", ", ".join(missing))
        return False

    path = os.path.join(
        "/content/drive/MyDrive/nba-ml/Docs/nba-rapture/",
        "Data", season_type, timestamp, name, "tracking"
    )
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "nba-tracking.pkl"), "wb") as f:
        pickle.dump(full_stats, f)

    #print("Done collecting NBA-tracking!")
    return True


def get_timestamps(source: str):
    distinct_timestamps = coll.distinct(
        'timestamp',
        {"source": source}
    )
    return distinct_timestamps


'''
### NBA Regular Season and Postseason Dates (2013–2023)

| Season  | Regular Season Start | Regular Season End | Postseason Start | Postseason End |
|---------|-----------------------|--------------------|------------------|----------------|
| 2013-14 | 2013-10-29           | 2014-04-16        | 2014-04-19       | 2014-06-15     |
| 2014-15 | 2014-10-28           | 2015-04-15        | 2015-04-18       | 2015-06-16     |
| 2015-16 | 2015-10-27           | 2016-04-13        | 2016-04-16       | 2016-06-19     |
| 2016-17 | 2016-10-25           | 2017-04-12        | 2017-04-15       | 2017-06-12     |
| 2017-18 | 2017-10-17           | 2018-04-11        | 2018-04-14       | 2018-06-08     |
| 2018-19 | 2018-10-16           | 2019-04-10        | 2019-04-13       | 2019-06-13     |
| 2019-20 | 2019-10-22           | 2020-03-11        | 2020-08-17       | 2020-10-11     |
| 2020-21 | 2020-12-22           | 2021-05-16        | 2021-05-22       | 2021-07-20     |
| 2021-22 | 2021-10-19           | 2022-04-10        | 2022-04-16       | 2022-06-16     |
| 2022-23 | 2022-10-18           | 2023-04-09        | 2023-04-15       | 2023-06-12     |
'''

def get_season(waystamp):

  if waystamp >= wayback_time("2013-10-29") and waystamp < wayback_time("2014-10-28"):
    return '2013-14'
  elif waystamp >= wayback_time("2014-10-28") and waystamp < wayback_time("2015-10-27"):
    return '2014-15'
  elif waystamp >= wayback_time("2015-10-27") and waystamp < wayback_time("2016-10-25"):
    return '2015-16'
  elif waystamp >= wayback_time("2016-10-25") and waystamp < wayback_time("2017-10-17"):
    return '2016-17'
  elif waystamp >= wayback_time("2017-10-17") and waystamp < wayback_time("2018-10-16"):
    return '2017-18'
  elif waystamp >= wayback_time("2019-10-22") and waystamp < wayback_time("2020-12-22"):
    return '2019-20'
  elif waystamp >= wayback_time("2020-12-22") and waystamp < wayback_time("2021-10-19"):
    return '2020-21'
  elif waystamp >= wayback_time("2021-10-19") and waystamp < wayback_time("2022-10-18"):
    return '2021-22'
  elif waystamp >= wayback_time("2022-10-18") and waystamp <= wayback_time("2023-06-12"):
    return '2022-23'

def inside_range(timestamp, end):
  return timestamp < wayback_time(end)


def get_date_range(timestamp, season_type):
    season = get_season(timestamp)

    if season == '2020-21':
        if season_type == "Playoffs":
            if inside_range(timestamp, '2021-07-20'):
                return ['2021-05-22', regular_time(timestamp)]
        elif season_type == "Regular Season":
            if inside_range(timestamp, '2021-05-22'):
                return ['2020-12-22', regular_time(timestamp)]
        else:
            return ['2020-12-22', regular_time(timestamp)]

    elif season == '2021-22':
        if season_type == "Playoffs":
            if inside_range(timestamp, '2022-06-16'):
                return ['2022-04-16', regular_time(timestamp)]
        elif season_type == "Regular season":
            if inside_range(timestamp, '2022-04-16'):
                return ['2021-10-19', regular_time(timestamp)]
        else:
            return ['2021-10-19', regular_time(timestamp)]

    elif season == '2022-23':
        if season_type == "Playoffs":
            if inside_range(timestamp, '2023-06-12'):
                return ['2023-04-15', regular_time(timestamp)]
        elif season_type == "Regular season":
            if inside_range(timestamp, '2023-04-15'):
                return ['2022-10-18', regular_time(timestamp)]
        else:
            return ['2022-10-18', regular_time(timestamp)]

    # no matching branch
    raise ValueError(
        f"No date-range rule for season={season} season_type={season_type}"
    )



def regular_time(waystamp):

  #Wayback time format YYYYMMDDhhmmss
  date_object = datetime.strptime(waystamp, "%Y%m%d%H%M%S")
  convert_date = date_object.strftime("%Y-%m-%d")
  return convert_date

def wayback_time(date):

  #PBP date format
  date_object = datetime.strptime(date, "%Y-%m-%d")

  #Turn into wayback timestamp
  convert_date = date_object.strftime("%Y%m%d%H%M%S")

  return convert_date


def remove_numbers_and_apostrophes(string: str) -> str:
  return re.sub(r'[\d\'\-.]+', '', string)


def reformat_date(timestamp):
  date_object = datetime.strptime(timestamp, "%Y-%m-%d")
  return date_object.strftime("%m/%d/%Y")


# Map each season to its (PlayoffsStart, PlayoffsEnd), inclusive
PLAYOFF_WINDOWS = {
    '2013-14': ('2014-04-19', '2014-06-15'),
    '2014-15': ('2015-04-18', '2015-06-16'),
    '2015-16': ('2016-04-16', '2016-06-19'),
    '2016-17': ('2017-04-15', '2017-06-12'),
    '2017-18': ('2018-04-14', '2018-06-08'),
    '2018-19': ('2019-04-13', '2019-06-13'),
    '2019-20': ('2020-08-17', '2020-10-11'),  # Bubble year
    '2020-21': ('2021-05-22', '2021-07-20'),
    '2021-22': ('2022-04-16', '2022-06-16'),
    '2022-23': ('2023-04-15', '2023-06-12'),
}

def is_legit_playoff_timestamp(waystamp: str) -> bool:
    """
    Return True iff `waystamp` (YYYYMMDDhhmmss) falls within the official
    Playoffs window for its season (inclusive of both endpoints).
    Relies on your `get_season`, `wayback_time`.
    """
    season = get_season(waystamp)
    if not season:
        return False
    window = PLAYOFF_WINDOWS.get(season)
    if not window:
        # Season not covered by our table or get_season didn't map it
        return False

    start_date, end_date = window
    start_ts = wayback_time(start_date)  # YYYYMMDD000000
    end_ts   = wayback_time(end_date)    # YYYYMMDD000000

    # Inclusive check (covers any hhmmss during the dates)
    return start_ts <= waystamp <= end_ts

def playoff_window_for(waystamp: str):
    """
    Convenience: returns (season, start_date, end_date) if within playoffs,
    else returns None.
    """
    season = get_season(waystamp)
    if season in PLAYOFF_WINDOWS:
        start_date, end_date = PLAYOFF_WINDOWS[season]
        if wayback_time(start_date) <= waystamp <= wayback_time(end_date):
            return season, start_date, end_date
    return None


def get_names(timestamp):
    distinct_names = coll.distinct(
        'standard_name',
        {"timestamp": timestamp, "source": '538'}
    )
    return distinct_names


def process_data():
  LOG = {'538':[], 'pbp':[], 'wowy_on':[], 'wowy_off':[], 'track':[]}

  TS_LIST = get_timestamps('538')
  season_types = ['Regular season', 'Playoffs', 'Full']
  for ts in TS_LIST:
    path = os.path.join("Data")
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "LOG.pkl"), "wb") as f:
      pickle.dump(LOG, f)
    names = get_names(ts)
    print(f"Trying to process {ts}...")
    for player in names:
      #print(player)
      MP_REGULAR = 0
      MP_PLAYOFFS = 0
      TRACKING_REG = ""
      TRACKING_PLAYOFFS = ""
      for season in season_types:
        if season == "Playoffs":
          valid = is_legit_playoff_timestamp(ts)
          if not valid:
            #This isn't a real playoff timestamp!
            continue
        if season == "Full":
          if MP_PLAYOFFS:
            WEIGHT = MP_REGULAR + MP_PLAYOFFS
            tracking = MP_REGULAR/WEIGHT*TRACKING_REG+MP_PLAYOFFS/WEIGHT*TRACKING_PLAYOFFS
          else:
            # No playoff data? Don't process any further -- in this case Full =
            # Regular, which is redundant data.
            continue
        core_info = fetch_538(season, ts, player)
        if core_info:
          if season == 'Regular season':
            MP_REGULAR = core_info['mp']
          elif season == 'Playoffs':
            MP_PLAYOFFS = core_info['mp']
        else:
          LOG['538'].append(ts+'-'+season+'-'+'538'+'-'+player)
          continue
        if not collect_pbp(season, ts, player):
          LOG['pbp'].append(ts+'-'+season+'-'+'pbp'+'-'+player)
          continue
        if not collect_wowy_on(season, ts, player):
          LOG['wowy_on'].append(ts+'-'+season+'-'+'wowy_on'+'-'+player)
          continue
        if not collect_wowy_off(season, ts, player):
          LOG['wowy_off'].append(ts+'-'+season+'-'+'wowy_off'+'-'+player)
          continue
        #Separate handling for full -- it has no tracking data! Depends on the existence of reg + playoffs.
        if season == "Full":
          #We have special handling for full season.
          continue
        else:
          tracking = collect_tracking(season, ts, player)
        if tracking:
          if season == "Regular season":
            TRACKING_REG = tracking
          elif season == "Playoffs":
            TRACKING_PLAYOFFS = tracking
        else:
          LOG['track'].append(ts+'-'+season+'-'+'tracking'+'-'+player)
          continue
    print(ts, " complete!")