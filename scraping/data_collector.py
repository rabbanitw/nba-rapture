import asyncio
import json
import os
import pickle
import re
from datetime import datetime
from typing import Optional

import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient


# ==== Mongo setup (async) ====
username = 'nbarapture'
password = 'cdTMM9n3Awh4ntQw'
MONGO_URI = (
    f"mongodb+srv://{username}:{password}@nba-rapture-2.qnfzf.mongodb.net/"
    "?retryWrites=true&w=majority&appName=nba-rapture-2"
)
client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=3000000, socketTimeoutMS=3000000)
db = client["nba_rapture"]
coll = db["nba_rapture"]

# ==== Preload key files (sync load is fine at startup; tiny files) ====
with open("PBP_KEYS.json", "r", encoding="utf-8") as f:
    PBP_KEYS = set(json.load(f))
with open("WOWY_OFF_KEYS.json", "r", encoding="utf-8") as f:
    WOWY_OFF_KEYS = set(json.load(f))
with open("WOWY_ON_KEYS.json", "r", encoding="utf-8") as f:
    WOWY_ON_KEYS = set(json.load(f))
with open("BAD_TRACKING_KEYS.json", "r", encoding="utf-8") as f:
    BAD_TRACKING_KEYS = set(json.load(f))

# ==== Helpers ====
async def dump_pickle(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = pickle.dumps(obj)
    async with aiofiles.open(path, "wb") as f:
        await f.write(data)

def wayback_time(date: str) -> str:
    # date: "YYYY-MM-DD" -> "YYYYMMDDhhmmss" (00:00:00)
    date_object = datetime.strptime(date, "%Y-%m-%d")
    return date_object.strftime("%Y%m%d%H%M%S")

def regular_time(waystamp: str) -> str:
    # wayback ts "YYYYMMDDhhmmss" -> "YYYY-MM-DD"
    date_object = datetime.strptime(waystamp, "%Y%m%d%H%M%S")
    return date_object.strftime("%Y-%m-%d")

def inside_range(timestamp: str, end: str) -> bool:
    return timestamp < wayback_time(end)

def remove_numbers_and_apostrophes(string: str) -> str:
    return re.sub(r'[\d\'\-.]+', '', string)

def reformat_date(timestamp: str) -> str:
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
    '2019-20': ('2020-08-17', '2020-10-11'),
    '2020-21': ('2021-05-22', '2021-07-20'),
    '2021-22': ('2022-04-16', '2022-06-16'),
    '2022-23': ('2023-04-15', '2023-06-12'),
}

def get_season(waystamp: str) -> Optional[str]:
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
    return None

def is_legit_playoff_timestamp(waystamp: str) -> bool:
    season = get_season(waystamp)
    if not season:
        return False
    window = PLAYOFF_WINDOWS.get(season)
    if not window:
        return False
    start_date, end_date = window
    return wayback_time(start_date) <= waystamp <= wayback_time(end_date)

def playoff_window_for(waystamp: str):
    season = get_season(waystamp)
    if season in PLAYOFF_WINDOWS:
        start_date, end_date = PLAYOFF_WINDOWS[season]
        if wayback_time(start_date) <= waystamp <= wayback_time(end_date):
            return season, start_date, end_date
    return None

def get_date_range(timestamp: str, season_type: str):
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

    raise ValueError(f"No date-range rule for season={season} season_type={season_type}")


# ==== Async DB/file ops ====
async def fetch_538(season_type: str, timestamp: str, standard_name: str, *, projection: dict = None, sort: list = None):
    query = {"source": "538", "standard_name": standard_name, "timestamp": timestamp, "season_type": season_type}
    data = await coll.find_one(query, projection=projection, sort=sort)
    if not data:
        return False

    path = os.path.join("Data", season_type, timestamp, standard_name, "538")
    await dump_pickle(os.path.join(path, "538.pkl"), data)
    return data

async def collect_pbp(season_type: str, timestamp: str, name: str, *, projection: dict = None, sort: list = None, verbose=0):
    BAD_KEYS = {"name", "timestamp", "season_type", "source", "standard_name", "_id", "data_type", "ShortName"}
    LABEL_FIELDS = {"source", "standard_name", "season_type", "timestamp"}

    query = {
        "source": "pbp",
        "standard_name": name,
        "season_type": season_type,
        "timestamp": timestamp,
    }

    exclude_keys = BAD_KEYS - LABEL_FIELDS
    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        proj.update({k: v for k, v in projection.items()})

    cursor = coll.find(query, projection=proj, batch_size=64)
    docs = await cursor.to_list(None)
    if not docs:
        return False

    d = docs[0]
    for k in list(d.keys()):
        if (k in LABEL_FIELDS) or (k not in PBP_KEYS):
            d.pop(k, None)
    full_stats = list(d.values())

    path = os.path.join("Data", season_type, timestamp, name, "pbp")
    await dump_pickle(os.path.join(path, "pbp.pkl"), full_stats)
    return full_stats

async def collect_wowy_off(season_type: str, timestamp: str, name: str, *, projection: dict = None, sort: list = None, verbose=0):
    BAD_KEYS = {"name", "timestamp", "season_type", "source", "standard_name", "_id", "data_type"}
    LABEL_FIELDS = {"on_or_off", "source", "standard_name", "season_type", "timestamp"}

    query = {
        "source": "wowy",
        "standard_name": name,
        "on_or_off": "off",
        "season_type": season_type,
        "timestamp": timestamp,
    }

    exclude_keys = BAD_KEYS - LABEL_FIELDS
    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        proj.update({k: v for k, v in projection.items()})

    cursor = coll.find(query, projection=proj, batch_size=64)
    docs = await cursor.to_list(None)
    if not docs:
        return False

    d = docs[0]
    for k in list(d.keys()):
        if (k in LABEL_FIELDS) or (k not in WOWY_OFF_KEYS):
            d.pop(k, None)
    full_stats = list(d.values())

    path = os.path.join("Data", season_type, timestamp, name, "wowy_off")
    await dump_pickle(os.path.join(path, "wowy_off.pkl"), full_stats)
    return True

async def collect_wowy_on(season_type: str, timestamp: str, name: str, *, projection: dict = None, sort: list = None, verbose=0):
    BAD_KEYS = {"name", "timestamp", "season_type", "source", "standard_name", "_id", "data_type"}
    LABEL_FIELDS = {"on_or_off", "source", "standard_name", "season_type", "timestamp"}

    query = {
        "source": "wowy",
        "standard_name": name,
        "on_or_off": "on",
        "season_type": season_type,
        "timestamp": timestamp,
    }

    exclude_keys = BAD_KEYS - LABEL_FIELDS
    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        proj.update({k: v for k, v in projection.items()})

    cursor = coll.find(query, projection=proj, batch_size=64)
    docs = await cursor.to_list(None)
    if not docs:
        return False

    d = docs[0]
    for k in list(d.keys()):
        if k in LABEL_FIELDS:
            d.pop(k, None)
    full_stats = list(d.values())

    path = os.path.join("Data", season_type, timestamp, name, "wowy_on")
    await dump_pickle(os.path.join(path, "wowy_on.pkl"), full_stats)
    return True

async def collect_tracking(season_type: str, timestamp: str, name: str, *, projection: dict = None, sort: list = None):
    TRACK_TYPES = [
        'catch-shoot','defensive-impact','defensive-rebounding','drives',
        'elbow-touch','offensive-rebounding','paint-touch','passing','pullup',
        'rebounding','shooting-efficiency','speed-distance','touches','tracking-post-ups'
    ]
    LABEL_FIELDS = {"data_type", "source", "standard_name", "season_type", "timestamp"}

    query = {
        "source": "nba-tracking",
        "standard_name": name,
        "data_type": {"$in": TRACK_TYPES},
        "season_type": season_type,
        "timestamp": timestamp,
    }

    exclude_keys = BAD_TRACKING_KEYS - LABEL_FIELDS
    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        proj.update({k: v for k, v in projection.items() if k != "data_type"})

    cursor = coll.find(query, projection=proj, batch_size=64)
    docs = await cursor.to_list(None)
    if not docs:
        return False

    if "data_type" not in docs[0]:
        raise RuntimeError("Projection removed 'data_type'.")

    by_cat = {d["data_type"]: d for d in docs}
    full_stats = []
    missing = []
    for cat in TRACK_TYPES:
        d = by_cat.get(cat)
        if not d:
            missing.append(cat)
            continue
        for k in list(d.keys()):
            if k in LABEL_FIELDS:
                d.pop(k, None)
        full_stats.extend(d.values())

    if missing:
        return False

    path = os.path.join("Data", season_type, timestamp, name, "tracking")
    await dump_pickle(os.path.join(path, "nba-tracking.pkl"), full_stats)
    return True

# ==== Distinct helpers ====
async def get_timestamps(source: str):
    return await coll.distinct('timestamp', {"source": source})

async def get_names(timestamp: str):
    return await coll.distinct('standard_name', {"timestamp": timestamp, "source": '538'})

# ==== Orchestration ====
MAX_CONCURRENCY = 20  # tune as you like

async def process_player(ts: str, player: str, LOG: dict, log_lock: asyncio.Lock):
    season_types = ['Regular season', 'Playoffs', 'Full']

    MP_REGULAR = 0
    MP_PLAYOFFS = 0
    TRACKING_REG = ""
    TRACKING_PLAYOFFS = ""

    for season in season_types:
        if season == "Playoffs":
            if not is_legit_playoff_timestamp(ts):
                # Not a playoff timestamp; skip this season
                continue
        if season == "Full":
            # Depends on reg + playoffs existing; logic kept as in original
            if MP_PLAYOFFS:
                WEIGHT = MP_REGULAR + MP_PLAYOFFS
                # Note: original code mixes lists/booleans here; left unchanged
                _tracking = MP_REGULAR/WEIGHT*TRACKING_REG + MP_PLAYOFFS/WEIGHT*TRACKING_PLAYOFFS  # noqa: F841
            else:
                continue

        core_info = await fetch_538(season, ts, player)
        if core_info:
            if season == 'Regular season':
                MP_REGULAR = core_info.get('mp', 0)
            elif season == 'Playoffs':
                MP_PLAYOFFS = core_info.get('mp', 0)
        else:
            async with log_lock:
                LOG['538'].append(ts+'-'+season+'-'+'538'+'-'+player)
            continue

        ok_pbp = await collect_pbp(season, ts, player)
        if not ok_pbp:
            async with log_lock:
                LOG['pbp'].append(ts+'-'+season+'-'+'pbp'+'-'+player)
            continue

        ok_on = await collect_wowy_on(season, ts, player)
        if not ok_on:
            async with log_lock:
                LOG['wowy_on'].append(ts+'-'+season+'-'+'wowy_on'+'-'+player)
            continue

        ok_off = await collect_wowy_off(season, ts, player)
        if not ok_off:
            async with log_lock:
                LOG['wowy_off'].append(ts+'-'+season+'-'+'wowy_off'+'-'+player)
            continue

        if season == "Full":
            # Special handling: no tracking for Full
            continue
        else:
            tracking = await collect_tracking(season, ts, player)

        if tracking:
            if season == "Regular season":
                TRACKING_REG = tracking
            elif season == "Playoffs":
                TRACKING_PLAYOFFS = tracking
        else:
            async with log_lock:
                LOG['track'].append(ts+'-'+season+'-'+'tracking'+'-'+player)
            continue

async def process_timestamp(ts: str, LOG: dict, log_lock: asyncio.Lock, sem: asyncio.Semaphore):
    # Persist LOG periodically like original
    os.makedirs("Data", exist_ok=True)
    await dump_pickle(os.path.join("Data", "LOG.pkl"), LOG)

    names = await get_names(ts)
    print(f"Trying to process {ts}...")

    async def run_player(p):
        async with sem:
            await process_player(ts, p, LOG, log_lock)

    await asyncio.gather(*(run_player(p) for p in names))
    print(ts, " complete!")

async def process_data():
    LOG = {'538': [], 'pbp': [], 'wowy_on': [], 'wowy_off': [], 'track': []}
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    log_lock = asyncio.Lock()

    TS_LIST = await get_timestamps('538')
    for ts in TS_LIST:
        await process_timestamp(ts, LOG, log_lock, sem)

    # final writeout of LOG
    await dump_pickle(os.path.join("Data", "LOG.pkl"), LOG)

if __name__ == "__main__":
    asyncio.run(process_data())
