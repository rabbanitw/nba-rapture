import csv
import os
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, List, Set, Tuple
import asyncio
from datetime import datetime
import time
from tqdm.asyncio import tqdm
import utils  # Assuming this module exists with get_season and wayback_time functions

# MongoDB credentials
username = 'nbarapture'
password = 'fAY8cOij4S9NA8Bx'

MONGO_URI = (
    f"mongodb+srv://{username}:{password}@nba-rapture-2.qnfzf.mongodb.net/"
    "?retryWrites=true&w=majority&appName=nba-rapture-2"
)

# Global database variables
client = None
db = None
coll = None

# Semaphore to limit concurrent operations
DB_SEMAPHORE = asyncio.Semaphore(10)

# Output directory
OUTPUT_DIR = "missing_data_csvs"

# Tracking data types
TRACKING_TYPES = [
    'catch-shoot', 'defensive-impact', 'defensive-rebounding', 'drives',
    'elbow-touch', 'offensive-rebounding', 'paint-touch', 'passing', 'pullup',
    'rebounding', 'shooting-efficiency', 'speed-distance', 'touches', 'tracking-post-ups'
]

# Playoff windows for validation
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


async def initialize_db():
    """Initialize database connection"""
    global client, db, coll
    client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=3000000, socketTimeoutMS=3000000)
    db = client["nba_rapture"]
    coll = db["nba_rapture"]
    print("✅ Database connected successfully")


async def close_db():
    """Close database connection safely"""
    global client
    if client is not None:
        client.close()
        client = None
        print("✅ Database connection closed")


def is_legit_playoff_timestamp(waystamp: str) -> bool:
    """Check if timestamp falls within legitimate playoff window"""
    season = utils.get_season(waystamp)
    if not season:
        return False
    window = PLAYOFF_WINDOWS.get(season)
    if not window:
        return False

    start_date, end_date = window
    start_ts = utils.wayback_time(start_date)
    end_ts = utils.wayback_time(end_date)

    return start_ts <= waystamp <= end_ts


async def get_timestamps_from_538() -> List[str]:
    """Get all distinct timestamps from 538 source"""
    async with DB_SEMAPHORE:
        timestamps = await coll.distinct('timestamp', {"source": "538"})
    return sorted(timestamps)


async def get_players_for_timestamp(timestamp: str) -> List[str]:
    """Get all distinct player names for a given timestamp from 538"""
    async with DB_SEMAPHORE:
        players = await coll.distinct('standard_name', {
            "timestamp": timestamp,
            "source": "538"
        })
    return sorted(players)


async def check_538_exists(timestamp: str, player: str, season_type: str) -> bool:
    """Check if 538 data exists for a player-timestamp-season combination"""
    async with DB_SEMAPHORE:
        doc = await coll.find_one({
            "source": "538",
            "timestamp": timestamp,
            "standard_name": player,
            "season_type": season_type
        }, {"_id": 1})
    return doc is not None


async def check_pbp_exists(timestamp: str, player: str, season_type: str) -> bool:
    """Check if PBP data exists"""
    async with DB_SEMAPHORE:
        doc = await coll.find_one({
            "source": "pbp",
            "timestamp": timestamp,
            "standard_name": player,
            "season_type": season_type
        }, {"_id": 1})
    return doc is not None


async def check_wowy_exists(timestamp: str, player: str, season_type: str, on_or_off: str) -> bool:
    """Check if WOWY data exists"""
    async with DB_SEMAPHORE:
        doc = await coll.find_one({
            "source": "wowy",
            "timestamp": timestamp,
            "standard_name": player,
            "season_type": season_type,
            "on_or_off": on_or_off
        }, {"_id": 1})
    return doc is not None


async def check_tracking_exists(timestamp: str, player: str, season_type: str, data_type: str) -> bool:
    """Check if specific tracking data type exists"""
    async with DB_SEMAPHORE:
        doc = await coll.find_one({
            "source": "nba-tracking",
            "timestamp": timestamp,
            "standard_name": player,
            "season_type": season_type,
            "data_type": data_type
        }, {"_id": 1})
    return doc is not None


async def find_missing_for_player_timestamp(
    timestamp: str,
    player: str,
    season_type: str,
    progress_callback=None
) -> Dict[str, List]:
    """Find all missing data for a player-timestamp-season combination"""
    missing = {
        'pbp': [],
        'wowy_on': [],
        'wowy_off': [],
        'tracking': []
    }

    # Skip playoff check for invalid timestamps
    if season_type == "Playoffs" and not is_legit_playoff_timestamp(timestamp):
        if progress_callback:
            # Update progress for skipped checks
            await progress_callback(1 + 1 + 1 + len(TRACKING_TYPES))
        return missing

    # For Full season, check if both Regular and Playoffs exist
    if season_type == "Full":
        has_regular = await check_538_exists(timestamp, player, "Regular season")
        has_playoffs = await check_538_exists(timestamp, player, "Playoffs")

        if not (has_regular and has_playoffs and is_legit_playoff_timestamp(timestamp)):
            if progress_callback:
                await progress_callback(1 + 1 + 1 + len(TRACKING_TYPES))
            return missing

    # Check PBP
    if not await check_pbp_exists(timestamp, player, season_type):
        missing['pbp'].append({
            'timestamp': timestamp,
            'standard_name': player,
            'season_type': season_type
        })
    if progress_callback:
        await progress_callback(1)

    # Check WOWY ON
    if not await check_wowy_exists(timestamp, player, season_type, 'on'):
        missing['wowy_on'].append({
            'timestamp': timestamp,
            'standard_name': player,
            'season_type': season_type
        })
    if progress_callback:
        await progress_callback(1)

    # Check WOWY OFF
    if not await check_wowy_exists(timestamp, player, season_type, 'off'):
        missing['wowy_off'].append({
            'timestamp': timestamp,
            'standard_name': player,
            'season_type': season_type
        })
    if progress_callback:
        await progress_callback(1)

    # Check tracking data (not for Full season)
    if season_type != "Full":
        for data_type in TRACKING_TYPES:
            if not await check_tracking_exists(timestamp, player, season_type, data_type):
                missing['tracking'].append({
                    'timestamp': timestamp,
                    'standard_name': player,
                    'season_type': season_type,
                    'data_type': data_type
                })
            if progress_callback:
                await progress_callback(1)
    else:
        # Update progress for skipped tracking checks
        if progress_callback:
            await progress_callback(len(TRACKING_TYPES))

    return missing


async def process_timestamp(timestamp: str, all_missing: Dict[str, List], pbar: tqdm):
    """Process all players for a given timestamp"""
    players = await get_players_for_timestamp(timestamp)
    season_types = ['Regular season', 'Playoffs', 'Full']

    # Calculate total operations for progress tracking
    ops_per_combo = 3 + len(TRACKING_TYPES)  # pbp, wowy_on, wowy_off, tracking types
    total_ops = len(players) * len(season_types) * ops_per_combo

    with tqdm(total=total_ops, desc=f"Checking {timestamp}", leave=False, position=1) as ops_pbar:
        async def progress_callback(ops_count):
            ops_pbar.update(ops_count)

        # Process all player-season combinations
        tasks = []
        for player in players:
            for season_type in season_types:
                task = find_missing_for_player_timestamp(
                    timestamp, player, season_type, progress_callback
                )
                tasks.append(task)

        # Run all checks concurrently
        results = await asyncio.gather(*tasks)

        # Aggregate results
        for result in results:
            for source, items in result.items():
                all_missing[source].extend(items)

    pbar.update(1)


def write_csv_files(all_missing: Dict[str, List]):
    """Write missing data to CSV files"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Write PBP CSV
    if all_missing['pbp']:
        pbp_file = os.path.join(OUTPUT_DIR, 'missing_pbp.csv')
        with open(pbp_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'standard_name', 'season_type'])
            writer.writeheader()
            writer.writerows(all_missing['pbp'])
        print(f"✅ Wrote {len(all_missing['pbp'])} missing PBP entries to {pbp_file}")

    # Write WOWY_ON CSV
    if all_missing['wowy_on']:
        wowy_on_file = os.path.join(OUTPUT_DIR, 'missing_wowy_on.csv')
        with open(wowy_on_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'standard_name', 'season_type'])
            writer.writeheader()
            writer.writerows(all_missing['wowy_on'])
        print(f"✅ Wrote {len(all_missing['wowy_on'])} missing WOWY_ON entries to {wowy_on_file}")

    # Write WOWY_OFF CSV
    if all_missing['wowy_off']:
        wowy_off_file = os.path.join(OUTPUT_DIR, 'missing_wowy_off.csv')
        with open(wowy_off_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'standard_name', 'season_type'])
            writer.writeheader()
            writer.writerows(all_missing['wowy_off'])
        print(f"✅ Wrote {len(all_missing['wowy_off'])} missing WOWY_OFF entries to {wowy_off_file}")

    # Write Tracking CSV
    if all_missing['tracking']:
        tracking_file = os.path.join(OUTPUT_DIR, 'missing_tracking.csv')
        with open(tracking_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['timestamp', 'standard_name', 'season_type', 'data_type'])
            writer.writeheader()
            writer.writerows(all_missing['tracking'])
        print(f"✅ Wrote {len(all_missing['tracking'])} missing tracking entries to {tracking_file}")


async def main():
    """Main function to find and save missing data"""
    print("🏀 NBA Missing Data Finder")
    print("=" * 50)

    try:
        # Initialize database
        await initialize_db()

        # Get all timestamps from 538
        print("\n📊 Getting timestamps from 538 source...")
        timestamps = await get_timestamps_from_538()
        print(f"Found {len(timestamps)} timestamps to check")

        # Initialize storage for all missing data
        all_missing = {
            'pbp': [],
            'wowy_on': [],
            'wowy_off': [],
            'tracking': []
        }

        # Process each timestamp
        start_time = time.time()
        with tqdm(total=len(timestamps), desc="Overall Progress", position=0) as pbar:
            for i, timestamp in enumerate(timestamps):
                # Update ETA
                if i > 0:
                    elapsed = time.time() - start_time
                    avg_time = elapsed / i
                    remaining = len(timestamps) - i
                    eta = avg_time * remaining
                    pbar.set_postfix({'ETA': f"{eta / 60:.1f}m" if eta > 60 else f"{eta:.1f}s"})

                await process_timestamp(timestamp, all_missing, pbar)

        # Write results to CSV files
        print("\n📝 Writing CSV files...")
        write_csv_files(all_missing)

        # Print summary
        total_time = time.time() - start_time
        print(f"\n🎉 Processing complete!")
        print(f"⏰ Total time: {total_time / 60:.1f} minutes")
        print(f"\n📊 Missing data summary:")
        print(f"   PBP: {len(all_missing['pbp'])} missing entries")
        print(f"   WOWY_ON: {len(all_missing['wowy_on'])} missing entries")
        print(f"   WOWY_OFF: {len(all_missing['wowy_off'])} missing entries")
        print(f"   Tracking: {len(all_missing['tracking'])} missing entries")

    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())