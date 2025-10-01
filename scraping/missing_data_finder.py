import pickle
import os
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Iterable, Dict, Any, Optional
import json
import asyncio
from datetime import datetime
import time
import re
import utils
from tqdm.asyncio import tqdm

username = 'nbarapture'
password = 'cdTMM9n3Awh4ntQw'

MONGO_URI = (
    f"mongodb+srv://{username}:{password}@nba-rapture-2.qnfzf.mongodb.net/"
    "?retryWrites=true&w=majority&appName=nba-rapture-2"
)

# Use Motor for async MongoDB operations
client = None  # Initialize as None, will be set in main()
db = None
coll = None
MISSING_DATA_FINDER_DIRECTORY = "missing_data_finder"


async def initialize_db():
    """Initialize database connection"""
    global client, db, coll
    client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=3000000, socketTimeoutMS=3000000)
    db = client["nba_rapture"]
    coll = db["nba_rapture"]


async def close_db():
    """Close database connection safely"""
    global client
    if client is not None:
        client.close()
        client = None


# Semaphore to limit concurrent operations
DB_SEMAPHORE = asyncio.Semaphore(10)  # Adjust based on your needs
FILE_SEMAPHORE = asyncio.Semaphore(20)  # For file operations


async def get_timestamps(source: str):
    async with DB_SEMAPHORE:
        distinct_timestamps = await coll.distinct(
            'timestamp',
            {"source": source}
        )
        return distinct_timestamps


async def get_names(timestamp):
    async with DB_SEMAPHORE:
        distinct_names = await coll.distinct(
            'standard_name',
            {"timestamp": timestamp, "source": '538'}
        )
        return distinct_names


async def async_write_pickle(path: str, filename: str, data):
    """Async wrapper for pickle writing"""
    async with FILE_SEMAPHORE:
        loop = asyncio.get_event_loop()
        os.makedirs(path, exist_ok=True)
        filepath = os.path.join(path, filename)
        # Run the blocking pickle operation in a thread pool
        await loop.run_in_executor(None, lambda: _write_pickle_sync(filepath, data))


def _write_pickle_sync(filepath: str, data):
    """Synchronous pickle writing helper"""
    with open(filepath, "wb") as f:
        pickle.dump(data, f)


async def fetch_538(season_type: str, timestamp: str, standard_name: str,
                    *,
                    projection: dict | None = None,
                    sort: list | None = None):
    """
    Return the *first* document where
        source        == '538'
        standard_name == player_name
        timestamp     == timestamp   (if provided)

    Parameters
    ----------
    standard_name : str
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

    async with DB_SEMAPHORE:
        data = await coll.find_one(query, projection=projection, sort=sort)

    if not data:
        return False

    path = os.path.join(
        MISSING_DATA_FINDER_DIRECTORY, season_type, timestamp, standard_name, "538"
    )
    await async_write_pickle(path, "538.pkl", data)

    return data


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
    season = utils.get_season(waystamp)
    if not season:
        return False
    window = PLAYOFF_WINDOWS.get(season)
    if not window:
        # Season not covered by our table or get_season didn't map it
        return False

    start_date, end_date = window
    start_ts = utils.wayback_time(start_date)  # YYYYMMDD000000
    end_ts = utils.wayback_time(end_date)  # YYYYMMDD000000

    # Inclusive check (covers any hhmmss during the dates)
    return start_ts <= waystamp <= end_ts


def playoff_window_for(waystamp: str):
    """
    Convenience: returns (season, start_date, end_date) if within playoffs,
    else returns None.
    """
    season = utils.get_season(waystamp)
    if season in PLAYOFF_WINDOWS:
        start_date, end_date = PLAYOFF_WINDOWS[season]
        if utils.wayback_time(start_date) <= waystamp <= utils.wayback_time(end_date):
            return season, start_date, end_date
    return None


async def load_json_keys(filename: str) -> set:
    """Async helper to load JSON keys"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: _load_json_sync(filename))


def _load_json_sync(filename: str) -> set:
    """Synchronous JSON loading helper"""
    with open(filename, "r", encoding="utf-8") as f:
        return set(json.load(f))


async def collect_pbp(season_type: str, timestamp: str, name: str, *,
                      projection: dict | None = None, sort: list | None = None,
                      verbose=0):
    PBP_KEYS = await load_json_keys("PBP_KEYS.json")

    BAD_KEYS = {"name", "timestamp", "season_type",
                "source", "standard_name", "_id", "data_type", "ShortName"}

    LABEL_FIELDS = {"source", "standard_name", "season_type", "timestamp"}

    # Build one query for all categories
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

    async with DB_SEMAPHORE:
        cursor = coll.find(query, projection=proj, batch_size=64)
        docs = await cursor.to_list(length=None)

    full_stats = []

    if not docs:
        return False

    d = docs[0]
    # drop label fields before collecting values
    for k in list(d.keys()):
        if (k in LABEL_FIELDS) or (k not in PBP_KEYS):
            d.pop(k, None)

    full_stats.extend(d.values())

    path = os.path.join(
        MISSING_DATA_FINDER_DIRECTORY, season_type, timestamp, name, "pbp"
    )
    await async_write_pickle(path, "pbp.pkl", full_stats)

    return full_stats


async def collect_wowy_on(season_type: str, timestamp: str, name: str, *,
                          projection: dict | None = None, sort: list | None = None,
                          verbose=0):
    WOWY_ON_KEYS = await load_json_keys("WOWY_ON_KEYS.json")

    BAD_KEYS = {"name", "timestamp", "season_type",
                "source", "standard_name", "_id", "data_type"}

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

    async with DB_SEMAPHORE:
        cursor = coll.find(query, projection=proj, batch_size=64)
        docs = await cursor.to_list(length=None)

    full_stats = []

    if not docs:
        return False

    d = docs[0]
    for k in list(d.keys()):
        if k in LABEL_FIELDS:
            d.pop(k, None)
    full_stats.extend(d.values())

    path = os.path.join(
        MISSING_DATA_FINDER_DIRECTORY, season_type, timestamp, name, "wowy_on"
    )
    await async_write_pickle(path, "wowy_on.pkl", full_stats)

    return True


async def collect_wowy_off(season_type: str, timestamp: str, name: str, *,
                           projection: dict | None = None, sort: list | None = None,
                           verbose=0):
    WOWY_OFF_KEYS = await load_json_keys("WOWY_OFF_KEYS.json")

    BAD_KEYS = {"name", "timestamp", "season_type",
                "source", "standard_name", "_id", "data_type"}

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

    async with DB_SEMAPHORE:
        cursor = coll.find(query, projection=proj, batch_size=64)
        docs = await cursor.to_list(length=None)

    full_stats = []

    if not docs:
        return False

    d = docs[0]
    for k in list(d.keys()):
        if (k in LABEL_FIELDS) or (k not in WOWY_OFF_KEYS):
            d.pop(k, None)
    full_stats.extend(d.values())

    path = os.path.join(
        MISSING_DATA_FINDER_DIRECTORY, season_type, timestamp, name, "wowy_off"
    )
    await async_write_pickle(path, "wowy_off.pkl", full_stats)

    return True


async def collect_tracking(season_type: str, timestamp: str, name: str, *,
                           projection: dict | None = None, sort: list | None = None):
    bad_keys = await load_json_keys("BAD_TRACKING_KEYS.json")

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

    exclude_keys = bad_keys - LABEL_FIELDS
    proj = {"_id": 0, **{k: 0 for k in exclude_keys}}
    if projection:
        proj.update({k: v for k, v in projection.items() if k != "data_type"})

    async with DB_SEMAPHORE:
        cursor = coll.find(query, projection=proj, batch_size=64)
        docs = await cursor.to_list(length=None)

    if not docs:
        return False

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
        for k in list(d.keys()):
            if k in LABEL_FIELDS:
                d.pop(k, None)
        full_stats.extend(d.values())

    if missing:
        return False

    path = os.path.join(
        MISSING_DATA_FINDER_DIRECTORY, season_type, timestamp, name, "tracking"
    )
    await async_write_pickle(path, "tracking.pkl", full_stats)

    return True


async def process_player_season(player: str, season: str, ts: str, LOG: dict,
                                MP_REGULAR: int, MP_PLAYOFFS: int,
                                TRACKING_REG, TRACKING_PLAYOFFS,
                                progress_callback=None):
    """Process a single player-season combination"""
    complete_player = True

    if season == "Playoffs":
        valid = is_legit_playoff_timestamp(ts)
        if not valid:
            return None, None, None

    if season == "Full":
        if MP_PLAYOFFS:
            WEIGHT = MP_REGULAR + MP_PLAYOFFS
            tracking = MP_REGULAR / WEIGHT * TRACKING_REG + MP_PLAYOFFS / WEIGHT * TRACKING_PLAYOFFS
        else:
            return None, None, None

    # Create tasks for all data collection operations
    tasks = []

    # 538 data
    core_info_task = asyncio.create_task(fetch_538(season, ts, player))
    tasks.append(('538', core_info_task))

    # PBP data
    pbp_task = asyncio.create_task(collect_pbp(season, ts, player))
    tasks.append(('pbp', pbp_task))

    # WOWY data
    wowy_on_task = asyncio.create_task(collect_wowy_on(season, ts, player))
    tasks.append(('wowy_on', wowy_on_task))

    wowy_off_task = asyncio.create_task(collect_wowy_off(season, ts, player))
    tasks.append(('wowy_off', wowy_off_task))

    # Tracking data (only for non-Full seasons)
    if season != "Full":
        tracking_task = asyncio.create_task(collect_tracking(season, ts, player))
        tasks.append(('tracking', tracking_task))

    # Wait for all tasks to complete
    results = {}
    for task_name, task in tasks:
        try:
            result = await task
            results[task_name] = result
            if progress_callback:
                await progress_callback()
        except Exception as e:
            print(f"\nError processing {task_name} for {player} in {season}: {e}")
            results[task_name] = False
            if progress_callback:
                await progress_callback()

    # Process results and update LOG
    core_info = results.get('538', False)
    mp_current = 0
    tracking_current = None

    if core_info:
        if season == 'Regular season':
            mp_current = core_info['mp']
        elif season == 'Playoffs':
            mp_current = core_info['mp']
    else:
        LOG['538'].append(f"{ts}-{season}-538-{player}")

    if not results.get('pbp', False):
        LOG['pbp'].append(f"{ts}-{season}-pbp-{player}")

    if not results.get('wowy_on', False):
        LOG['wowy_on'].append(f"{ts}-{season}-wowy_on-{player}")

    if not results.get('wowy_off', False):
        LOG['wowy_off'].append(f"{ts}-{season}-wowy_off-{player}")

    if season != "Full":
        tracking_current = results.get('tracking', False)
        if not tracking_current:
            LOG['track'].append(f"{ts}-{season}-tracking-{player}")

    return mp_current, tracking_current, LOG


async def process_player(player: str, ts: str, LOG: dict, progress_callback=None):
    """Process a single player across all seasons"""
    season_types = ['Regular season', 'Playoffs', 'Full']

    MP_REGULAR = 0
    MP_PLAYOFFS = 0
    TRACKING_REG = ""
    TRACKING_PLAYOFFS = ""

    for season in season_types:
        mp_current, tracking_current, updated_log = await process_player_season(
            player, season, ts, LOG, MP_REGULAR, MP_PLAYOFFS,
            TRACKING_REG, TRACKING_PLAYOFFS, progress_callback
        )

        if mp_current is not None:
            if season == 'Regular season':
                MP_REGULAR = mp_current
                if tracking_current:
                    TRACKING_REG = tracking_current
            elif season == 'Playoffs':
                MP_PLAYOFFS = mp_current
                if tracking_current:
                    TRACKING_PLAYOFFS = tracking_current

        if updated_log is not None:
            LOG.update(updated_log)


async def process_timestamp(ts: str, LOG: dict, timestamp_pbar=None):
    """Process all players for a given timestamp"""
    names = await get_names(ts)

    if timestamp_pbar:
        timestamp_pbar.set_description(f"Processing {ts} ({len(names)} players)")

    print(f"\nProcessing {ts} with {len(names)} players...")

    # Calculate total operations for this timestamp
    # Each player has 3 seasons, each season has ~5 operations
    total_operations = len(names) * 3 * 5  # Rough estimate

    # Create progress bar for this timestamp's operations
    with tqdm(total=total_operations, desc=f"Operations for {ts}",
              leave=False, position=1, colour='green') as op_pbar:

        async def progress_callback():
            op_pbar.update(1)

        # Create semaphore to limit concurrent players being processed
        player_semaphore = asyncio.Semaphore(5)  # Adjust based on your system

        async def process_player_with_semaphore(player):
            async with player_semaphore:
                await process_player(player, ts, LOG, progress_callback)

        # Process players concurrently
        player_tasks = [process_player_with_semaphore(player) for player in names]

        # Use tqdm.asyncio.gather for concurrent execution with progress
        await tqdm.gather(*player_tasks, desc=f"Players in {ts}",
                          leave=False, position=2, colour='blue')

    # Save LOG periodically
    path = os.path.join("missing_data_finder")
    await async_write_pickle(path, "LOG.pkl", LOG)

    if timestamp_pbar:
        timestamp_pbar.update(1)


async def process_data():
    LOG = {'538': [], 'pbp': [], 'wowy_on': [], 'wowy_off': [], 'track': []}

    print("Getting timestamps...")
    TS_LIST = await get_timestamps('538')
    valid_times = []
    for timestamp_str in TS_LIST:  # Changed variable name from 'time' to 'timestamp_str'
        # if int(timestamp_str) > 20210115003233:
        valid_times.append(timestamp_str)

    print(f"Found {len(valid_times)} valid timestamps to process")

    # Create main progress bar for timestamps
    start_time = time.time()
    with tqdm(total=len(valid_times), desc="Overall Progress",
              position=0, colour='red') as timestamp_pbar:

        # Process timestamps sequentially to avoid overwhelming the system
        for i, ts in enumerate(valid_times):
            try:
                current_time = time.time()
                elapsed = current_time - start_time
                avg_time_per_ts = elapsed / (i + 1) if i > 0 else 0
                remaining_ts = len(valid_times) - i - 1
                eta = avg_time_per_ts * remaining_ts

                timestamp_pbar.set_postfix({
                    'ETA': f"{eta / 60:.1f}m" if eta > 60 else f"{eta:.1f}s",
                    'Avg/TS': f"{avg_time_per_ts:.1f}s",
                    'Errors': sum(len(v) for v in LOG.values())
                })

                await process_timestamp(ts, LOG, timestamp_pbar)
                print(f"✓ {ts} complete! ({i + 1}/{len(valid_times)})")

            except Exception as e:
                print(f"\n❌ Error processing timestamp {ts}: {e}")
                timestamp_pbar.update(1)  # Still update progress bar
                continue

    # Final LOG save
    path = os.path.join("missing_data_finder")
    await async_write_pickle(path, "LOG.pkl", LOG)

    # Print summary
    total_time = time.time() - start_time
    print(f"\n🎉 Processing complete!")
    print(f"⏰ Total time: {total_time / 60:.1f} minutes")
    print(f"📊 Error summary:")
    for source, errors in LOG.items():
        if errors:
            print(f"   {source}: {len(errors)} errors")

    total_errors = sum(len(v) for v in LOG.values())
    print(f"   Total errors: {total_errors}")


async def main():
    print("🏀 NBA Data Processing Tool")
    print("=" * 50)
    print("Initializing database connection...")

    try:
        await initialize_db()
        print("✅ Database connected successfully")
        print("Finding missing data...")
        await process_data()
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("🔌 Closing database connection...")
        await close_db()
        print("✅ Done!")


if __name__ == "__main__":
    asyncio.run(main())