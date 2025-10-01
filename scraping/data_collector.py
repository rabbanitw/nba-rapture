import asyncio
import json
import os
import pickle
import re
import time
from datetime import datetime
from typing import Optional
import utils

import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient

OUTPUT_DIR = "nba_data_v2"

# ==== Mongo setup (async) ====
username = 'nbarapture'
password = 'fAY8cOij4S9NA8Bx'
MONGO_URI = (
    f"mongodb+srv://{username}:{password}@nba-rapture-2.qnfzf.mongodb.net/"
    "?retryWrites=true&w=majority&appName=nba-rapture-2"
)
client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=3000000, socketTimeoutMS=3000000)
db = client["nba_rapture"]
coll = db["nba_rapture"]

# Global tracking for key counts
current_wowy_on_keys = None
current_wowy_off_keys = None
wowy_on_key_changes = []
wowy_off_key_changes = []

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


async def dump_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(obj, indent=2))


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


def is_legit_playoff_timestamp(waystamp: str) -> bool:
    season = utils.get_season(waystamp)
    if not season:
        return False
    window = PLAYOFF_WINDOWS.get(season)
    if not window:
        return False
    start_date, end_date = window
    return wayback_time(start_date) <= waystamp <= wayback_time(end_date)


def playoff_window_for(waystamp: str):
    season = utils.get_season(waystamp)
    if season in PLAYOFF_WINDOWS:
        start_date, end_date = PLAYOFF_WINDOWS[season]
        if wayback_time(start_date) <= waystamp <= wayback_time(end_date):
            return season, start_date, end_date
    return None


# ==== Async DB/file ops ====
async def fetch_538(season_type: str, timestamp: str, standard_name: str, *, projection: dict = None,
                    sort: list = None):
    query = {"source": "538", "standard_name": standard_name, "timestamp": timestamp, "season_type": season_type}
    data = await coll.find_one(query, projection=projection, sort=sort)
    if not data:
        return False

    path = os.path.join(OUTPUT_DIR, season_type, timestamp, standard_name, "538")
    await dump_pickle(os.path.join(path, "538.pkl"), data)
    return data


async def collect_pbp(season_type: str, timestamp: str, name: str, *, projection: dict = None, sort: list = None,
                      verbose=0):
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

    path = os.path.join(OUTPUT_DIR, season_type, timestamp, name, "pbp")
    await dump_pickle(os.path.join(path, "pbp.pkl"), full_stats)
    return full_stats


async def collect_wowy_off(season_type: str, timestamp: str, name: str, *, projection: dict = None, sort: list = None,
                           verbose=0, key_lock: asyncio.Lock = None):
    global current_wowy_off_keys, wowy_off_key_changes

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

    # Count keys before filtering
    num_keys = len([k for k in d.keys() if k not in LABEL_FIELDS])

    # Track key count changes
    if key_lock:
        async with key_lock:
            if current_wowy_off_keys is None:
                current_wowy_off_keys = num_keys
                wowy_off_key_changes.append({
                    "timestamp": timestamp,
                    "season_type": season_type,
                    "player": name,
                    "num_keys": num_keys,
                    "previous_keys": None,
                    "change": "initial"
                })
            elif num_keys != current_wowy_off_keys:
                wowy_off_key_changes.append({
                    "timestamp": timestamp,
                    "season_type": season_type,
                    "player": name,
                    "num_keys": num_keys,
                    "previous_keys": current_wowy_off_keys,
                    "change": num_keys - current_wowy_off_keys
                })
                current_wowy_off_keys = num_keys

    for k in list(d.keys()):
        if (k in LABEL_FIELDS) or (k not in WOWY_OFF_KEYS):
            d.pop(k, None)
    full_stats = list(d.values())

    path = os.path.join(OUTPUT_DIR, season_type, timestamp, name, "wowy_off")
    await dump_pickle(os.path.join(path, "wowy_off.pkl"), full_stats)
    return True


async def collect_wowy_on(season_type: str, timestamp: str, name: str, *, projection: dict = None, sort: list = None,
                          verbose=0, key_lock: asyncio.Lock = None):
    global current_wowy_on_keys, wowy_on_key_changes

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

    # Count keys before filtering
    num_keys = len([k for k in d.keys() if k not in LABEL_FIELDS])

    # Track key count changes
    if key_lock:
        async with key_lock:
            if current_wowy_on_keys is None:
                current_wowy_on_keys = num_keys
                wowy_on_key_changes.append({
                    "timestamp": timestamp,
                    "season_type": season_type,
                    "player": name,
                    "num_keys": num_keys,
                    "previous_keys": None,
                    "change": "initial"
                })
            elif num_keys != current_wowy_on_keys:
                wowy_on_key_changes.append({
                    "timestamp": timestamp,
                    "season_type": season_type,
                    "player": name,
                    "num_keys": num_keys,
                    "previous_keys": current_wowy_on_keys,
                    "change": num_keys - current_wowy_on_keys
                })
                current_wowy_on_keys = num_keys

    for k in list(d.keys()):
        if (k in LABEL_FIELDS) or (k not in WOWY_ON_KEYS):
            d.pop(k, None)
    full_stats = list(d.values())

    path = os.path.join(OUTPUT_DIR, season_type, timestamp, name, "wowy_on")
    await dump_pickle(os.path.join(path, "wowy_on.pkl"), full_stats)
    return True


async def collect_tracking(season_type: str, timestamp: str, name: str, *, projection: dict = None, sort: list = None):
    TRACK_TYPES = [
        'catch-shoot', 'defensive-impact', 'defensive-rebounding', 'drives',
        'elbow-touch', 'offensive-rebounding', 'paint-touch', 'passing', 'pullup',
        'rebounding', 'shooting-efficiency', 'speed-distance', 'touches', 'tracking-post-ups'
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

    path = os.path.join(OUTPUT_DIR, season_type, timestamp, name, "tracking")
    await dump_pickle(os.path.join(path, "tracking.pkl"), full_stats)
    return True


# ==== Distinct helpers ====
async def get_timestamps(source: str):
    return await coll.distinct('timestamp', {"source": source})


async def get_names(timestamp: str):
    return await coll.distinct('standard_name', {"timestamp": timestamp, "source": '538'})


# ==== Orchestration ====
MAX_CONCURRENCY = 20  # tune as you like


async def process_player(ts: str, player: str, LOG: dict, log_lock: asyncio.Lock, key_lock: asyncio.Lock):
    season_types = ['Regular season', 'Playoffs', 'Full']

    # First, collect what we can for Regular and Playoffs
    reg_data = {}
    playoff_data = {}

    # Process Regular season
    season = 'Regular season'
    core_info = await fetch_538(season, ts, player)
    if core_info:
        reg_data['538'] = core_info
        # Convert mp to float, defaulting to 0 if not present or not convertible
        try:
            reg_data['mp'] = float(core_info.get('mp', 0))
        except (ValueError, TypeError):
            reg_data['mp'] = 0

        # Try to collect other data types, but don't stop if one fails
        pbp = await collect_pbp(season, ts, player)
        if pbp:
            reg_data['pbp'] = pbp
        else:
            async with log_lock:
                LOG['pbp'].append(ts + '-' + season + '-' + 'pbp' + '-' + player)

        wowy_on = await collect_wowy_on(season, ts, player, key_lock=key_lock)
        if wowy_on:
            reg_data['wowy_on'] = wowy_on
        else:
            async with log_lock:
                LOG['wowy_on'].append(ts + '-' + season + '-' + 'wowy_on' + '-' + player)

        wowy_off = await collect_wowy_off(season, ts, player, key_lock=key_lock)
        if wowy_off:
            reg_data['wowy_off'] = wowy_off
        else:
            async with log_lock:
                LOG['wowy_off'].append(ts + '-' + season + '-' + 'wowy_off' + '-' + player)

        tracking = await collect_tracking(season, ts, player)
        if tracking:
            reg_data['tracking'] = tracking
        else:
            async with log_lock:
                LOG['track'].append(ts + '-' + season + '-' + 'tracking' + '-' + player)
    else:
        async with log_lock:
            LOG['538'].append(ts + '-' + season + '-' + '538' + '-' + player)

    # Process Playoffs
    season = 'Playoffs'
    core_info = await fetch_538(season, ts, player)
    if core_info:
        playoff_data['538'] = core_info
        # Convert mp to float, defaulting to 0 if not present or not convertible
        try:
            playoff_data['mp'] = float(core_info.get('mp', 0))
        except (ValueError, TypeError):
            playoff_data['mp'] = 0

        # Collect other data types similarly
        pbp = await collect_pbp(season, ts, player)
        if pbp:
            playoff_data['pbp'] = pbp
        else:
            async with log_lock:
                LOG['pbp'].append(ts + '-' + season + '-' + 'pbp' + '-' + player)

        wowy_on = await collect_wowy_on(season, ts, player, key_lock=key_lock)
        if wowy_on:
            playoff_data['wowy_on'] = wowy_on
        else:
            async with log_lock:
                LOG['wowy_on'].append(ts + '-' + season + '-' + 'wowy_on' + '-' + player)

        wowy_off = await collect_wowy_off(season, ts, player, key_lock=key_lock)
        if wowy_off:
            playoff_data['wowy_off'] = wowy_off
        else:
            async with log_lock:
                LOG['wowy_off'].append(ts + '-' + season + '-' + 'wowy_off' + '-' + player)

        tracking = await collect_tracking(season, ts, player)
        if tracking:
            playoff_data['tracking'] = tracking
        else:
            async with log_lock:
                LOG['track'].append(ts + '-' + season + '-' + 'tracking' + '-' + player)
    else:
        async with log_lock:
            LOG['538'].append(ts + '-' + season + '-' + '538' + '-' + player)

    # Process Full season if we have both regular and playoff data
    season = 'Full'
    if reg_data.get('mp', 0) > 0 and playoff_data.get('mp', 0) > 0:
        # For Full season, you need to decide how to combine the data
        # Currently, the code just fetches Full season data separately
        # which might be the intended behavior

        core_info = await fetch_538(season, ts, player)
        if core_info:
            pbp = await collect_pbp(season, ts, player)
            if not pbp:
                async with log_lock:
                    LOG['pbp'].append(ts + '-' + season + '-' + 'pbp' + '-' + player)

            wowy_on = await collect_wowy_on(season, ts, player, key_lock=key_lock)
            if not wowy_on:
                async with log_lock:
                    LOG['wowy_on'].append(ts + '-' + season + '-' + 'wowy_on' + '-' + player)

            wowy_off = await collect_wowy_off(season, ts, player, key_lock=key_lock)
            if not wowy_off:
                async with log_lock:
                    LOG['wowy_off'].append(ts + '-' + season + '-' + 'wowy_off' + '-' + player)

            # Note: Original code skips tracking for Full season
        else:
            async with log_lock:
                LOG['538'].append(ts + '-' + season + '-' + '538' + '-' + player)


async def process_timestamp(ts: str, LOG: dict, log_lock: asyncio.Lock, key_lock: asyncio.Lock, sem: asyncio.Semaphore):
    ts_start_time = time.time()

    # Persist LOG periodically like original
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    await dump_pickle(os.path.join(OUTPUT_DIR, "LOG.pkl"), LOG)

    names = await get_names(ts)
    print(f"Trying to process {ts} ({len(names)} players)...")

    async def run_player(p):
        async with sem:
            await process_player(ts, p, LOG, log_lock, key_lock)

    await asyncio.gather(*(run_player(p) for p in names))

    ts_elapsed = time.time() - ts_start_time
    print(f"{ts} complete! ({ts_elapsed:.2f} seconds)")


def format_duration(seconds):
    """Convert seconds to human-readable format"""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"


async def process_data():
    print("=== NBA Data Collection Started ===")
    start_time = time.time()

    LOG = {'538': [], 'pbp': [], 'wowy_on': [], 'wowy_off': [], 'track': []}
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    log_lock = asyncio.Lock()
    key_lock = asyncio.Lock()  # New lock for key tracking

    print("Getting timestamp list...")
    timestamp_fetch_start = time.time()
    TS_LIST = await get_timestamps('538')
    # print(TS_LIST)
    # raise Exception('hello')
    timestamp_fetch_elapsed = time.time() - timestamp_fetch_start
    print(f"Found {len(TS_LIST)} timestamps to process ({timestamp_fetch_elapsed:.2f} seconds)")

    processing_start_time = time.time()
    for i, ts in enumerate(TS_LIST, 1):
        print(f"\n--- Processing timestamp {i}/{len(TS_LIST)} ---")
        await process_timestamp(ts, LOG, log_lock, key_lock, sem)

    processing_elapsed = time.time() - processing_start_time
    print(f"\nAll timestamps processed in {format_duration(processing_elapsed)}")

    # final writeout of LOG
    final_log_start = time.time()
    await dump_pickle(os.path.join(OUTPUT_DIR, "LOG.pkl"), LOG)

    # Write out the key change tracking files
    await dump_json(os.path.join(OUTPUT_DIR, "wowy_on_key_changes.json"), wowy_on_key_changes)
    await dump_json(os.path.join(OUTPUT_DIR, "wowy_off_key_changes.json"), wowy_off_key_changes)

    final_log_elapsed = time.time() - final_log_start

    total_elapsed = time.time() - start_time

    print("\n=== NBA Data Collection Complete ===")
    print(f"Total execution time: {format_duration(total_elapsed)}")
    print(f"Final log save: {final_log_elapsed:.2f} seconds")
    print(f"Timestamps processed: {len(TS_LIST)}")
    print(f"WOWY ON key changes detected: {len(wowy_on_key_changes)}")
    print(f"WOWY OFF key changes detected: {len(wowy_off_key_changes)}")

    # Show error summary from LOG
    total_errors = sum(len(v) for v in LOG.values())
    if total_errors > 0:
        print(f"Total errors encountered: {total_errors}")
        for error_type, errors in LOG.items():
            if errors:
                print(f"  {error_type}: {len(errors)} errors")


if __name__ == "__main__":
    print("Starting NBA data collection with timing...")
    asyncio.run(process_data())