"""
NBA Data Fetcher
Processes missing_*.csv files and fetches data from various sources
"""

import csv
import os
import time
import random
import traceback
import asyncio
from datetime import datetime
from pathlib import Path
from functools import lru_cache
from typing import Dict, Tuple, Optional, List
import logging
from collections import defaultdict

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ReadTimeout
from urllib3.util.retry import Retry

from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import PlayerGameLog, CommonPlayerInfo
from nba_api.library.http import NBAHTTP

# Import your custom modules
import wowy_scrape
import pbp_scrape
import nba_tracking_scrape

# ============================================================================
# CONFIGURATION
# ============================================================================

# Directories
INPUT_DIR = "missing_data_csvs"  # Where missing_*.csv files are located
OUTPUT_DIR = "missing_data_finder"  # Where scraped data will be saved

# Rate limiting
DEFAULT_INTERVAL = 2.0  # seconds between API calls
WOWY_INTERVAL = 3.0  # selenium needs more time
TRACKING_INTERVAL = 1.0
PBP_INTERVAL = 3.0

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_fetcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Error tracking files
ERROR_LOGS = {
    'wowy': 'wowy_errors.csv',
    'pbp': 'pbp_errors.csv',
    'tracking': 'tracking_errors.csv'
}

# Skip tracking files
SKIP_FILES = {
    'wowy': 'wowy_skipped.txt',
    'pbp': 'pbp_skipped.txt',
    'tracking': 'tracking_skipped.txt'
}

# ============================================================================
# NBA API SETUP
# ============================================================================

# Team lookups
_ALL_TEAMS = teams.get_teams()
ID_BY_ABBR = {t["abbreviation"]: t["id"] for t in _ALL_TEAMS}
NAME_BY_ID = {t["id"]: t["full_name"] for t in _ALL_TEAMS}

# Season type mappings
SEASON_TYPE_MAPPINGS = {
    'pbp': {
        'Regular season': 'Regular Season',
        'Playoffs': 'Playoffs',
        'Full': 'All'
    },
    'wowy': {
        'Regular season': 'Regular Season',
        'Playoffs': 'Playoffs',
        'Full': 'All'
    },
    'tracking': {
        'Regular season': 'Regular season',
        'Playoffs': 'Playoffs'
        # Note: No 'Full' for tracking
    }
}


def harden_nba_api(timeout=60, max_retries=5):
    """Configure NBA API with robust retry logic and headers"""
    s = NBAHTTP._session or requests.Session()

    retries = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        backoff_factor=1.5,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/138.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://stats.nba.com",
        "Origin": "https://www.nba.com",
        "Connection": "keep-alive",
    })

    NBAHTTP._session = s
    NBAHTTP._timeout = timeout


# Initialize hardened NBA API
harden_nba_api(timeout=60, max_retries=5)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def load_skip_list(source: str) -> set:
    """Load list of entries to skip for a given source"""
    skip_file = SKIP_FILES.get(source)
    if skip_file and os.path.exists(skip_file):
        with open(skip_file, encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()


def init_error_log(source: str):
    """Initialize error log CSV for a source if it doesn't exist"""
    error_file = ERROR_LOGS.get(source)
    if error_file and not os.path.exists(error_file):
        headers = get_error_headers(source)
        with open(error_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def get_error_headers(source: str) -> list:
    """Get appropriate error log headers for each source"""
    if source == 'wowy':
        return ["timestamp", "player_name", "team_name", "season_type",
                "on_or_off", "output_path", "error", "error_time"]
    elif source == 'pbp':
        return ["timestamp", "player_name", "season_type", "output_path",
                "error", "error_time"]
    elif source == 'tracking':
        return ["timestamp", "player_name", "season_type", "data_type",
                "output_path", "error", "error_time"]
    return ["timestamp", "player_name", "error", "error_time"]


def log_error(source: str, error_data: dict):
    """Log an error to the appropriate error CSV"""
    error_file = ERROR_LOGS.get(source)
    if error_file:
        error_data['error_time'] = datetime.now().isoformat()
        with open(error_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=get_error_headers(source))
            writer.writerow(error_data)


def safe_player_game_log(pid: int, season: str, attempts: int = 4):
    """Safely fetch player game log with retries"""
    delay = 1.0
    for i in range(attempts):
        try:
            time.sleep(0.15 + random.random() * 0.15)
            return PlayerGameLog(player_id=pid, season=season)
        except (ReadTimeout, Exception) as e:
            if i == attempts - 1:
                raise
            logger.warning(f"PlayerGameLog retry {i + 1}/{attempts} for {pid} {season}: {e}")
            time.sleep(delay)
            delay *= 2


@lru_cache(maxsize=5000)
def get_team_at_timestamp(player_name: str, timestamp: str, max_retries: int = 5) -> Tuple[int, str]:
    """Get team ID and name for a player at a specific timestamp"""
    # Find player
    matches = players.find_players_by_full_name(player_name)
    if not matches:
        raise ValueError(f"No NBA player found for '{player_name}'")
    pid = matches[0]["id"]

    # Parse season
    dt = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    start_year = dt.year if dt.month >= 10 else dt.year - 1
    season = f"{start_year}-{str(start_year + 1)[-2:]}"

    # Fetch game log with retries
    for attempt in range(1, max_retries + 1):
        try:
            gl = safe_player_game_log(pid=pid, season=season)
            break
        except ReadTimeout:
            logger.warning(f"Timeout on PlayerGameLog for {player_name}, retry {attempt}/{max_retries}")
            time.sleep(2 ** attempt)
    else:
        raise RuntimeError(f"Failed to fetch PlayerGameLog for {player_name} after {max_retries} retries")

    df = gl.get_data_frames()[0]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    # Find most recent game
    played = df[df["GAME_DATE"] <= dt]
    if not played.empty:
        row = played.iloc[0]
        if "TEAM_ID" in row:
            team_id = int(row["TEAM_ID"])
        elif "TEAM_ABBREVIATION" in row:
            team_id = ID_BY_ABBR[row["TEAM_ABBREVIATION"]]
        else:
            abbr = row["MATCHUP"].split()[0].replace("@", "").replace("vs.", "").strip().upper()
            team_id = ID_BY_ABBR.get(abbr)
            if team_id is None:
                raise ValueError(f"Unknown team abbreviation '{abbr}'")
    else:
        # No games yet - use current roster
        info = CommonPlayerInfo(player_id=pid)
        info_df = info.get_data_frames()[0]
        team_id = int(info_df.loc[0, "TEAM_ID"])

    team_name = NAME_BY_ID.get(team_id, "Unknown Team")
    return team_id, team_name


# ============================================================================
# DATA HANDLERS
# ============================================================================

async def handle_pbp_batch(timestamp: str, season_type_csv: str, player_names: List[str], skip_set: set) -> bool:
    """Handle PBP data fetching for a batch of players with the same timestamp"""
    output_file = f"pbp_stats_{timestamp}.csv"
    output_path = os.path.join(OUTPUT_DIR, season_type_csv, timestamp, output_file)

    # Check if file already exists first - before any other processing
    if os.path.exists(output_path):
        logger.info(f"File already exists, skipping: {output_path}")
        return True

    if output_path in skip_set:
        logger.info(f"Skipping known entry: {output_path}")
        return True

    # Get API season type
    season_type_api = SEASON_TYPE_MAPPINGS['pbp'].get(season_type_csv)
    if not season_type_api:
        logger.warning(f"Unknown season type for PBP: {season_type_csv}")
        return False

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        # Run blocking scrape in thread - only once for all players with this timestamp
        await asyncio.to_thread(
            pbp_scrape.scrape_and_save_without_async,
            timestamp, season_type_api, season_type_csv, output_path
        )
        logger.info(f"✅ PBP fetched: {timestamp} ({season_type_csv}) for {len(player_names)} players: {', '.join(player_names[:3])}{'...' if len(player_names) > 3 else ''}")
        return True

    except Exception as e:
        logger.error(f"Failed to fetch PBP for {timestamp}: {e}")
        # Log error for each player in the batch
        for player_name in player_names:
            log_error('pbp', {
                'timestamp': timestamp,
                'player_name': player_name,
                'season_type': season_type_csv,
                'output_path': output_path,
                'error': str(e)
            })
        return False


async def handle_tracking(row: dict, skip_set: set, completed_timestamps: set) -> bool:
    """Handle tracking data fetching"""
    timestamp = row["timestamp"]
    player_name = row["standard_name"]
    season_type_csv = row["season_type"]
    data_type = row["data_type"]

    # Skip 'Full' season for tracking
    if season_type_csv == 'Full':
        return True

    output_path = os.path.join(OUTPUT_DIR, season_type_csv, timestamp, player_name, "tracking", f"{data_type}.csv")

    # Check if file already exists first - before any other processing
    if os.path.exists(output_path):
        logger.info(f"File already exists, skipping: {output_path}")
        cache_key = f"{timestamp}-{season_type_csv}-{data_type}"
        completed_timestamps.add(cache_key)
        return True

    # Check if already completed for this timestamp
    cache_key = f"{timestamp}-{season_type_csv}-{data_type}"
    if cache_key in completed_timestamps:
        logger.info(f"Already fetched tracking: {cache_key}")
        return True

    if output_path in skip_set:
        logger.info(f"Skipping known entry: {output_path}")
        completed_timestamps.add(cache_key)
        return True

    try:
        # Run blocking scrape in thread
        await asyncio.to_thread(
            nba_tracking_scrape.retrieve_from_nba_api,
            timestamp=timestamp,
            season_type=season_type_csv,
            stat_type=data_type,
            player_name=player_name,
            custom_root_dir=OUTPUT_DIR
        )

        completed_timestamps.add(cache_key)
        logger.info(f"✅ Tracking fetched: {player_name} @ {timestamp} ({data_type})")
        return True

    except Exception as e:
        logger.error(f"Failed to fetch tracking for {player_name} @ {timestamp} ({data_type}): {e}")
        log_error('tracking', {
            'timestamp': timestamp,
            'player_name': player_name,
            'season_type': season_type_csv,
            'data_type': data_type,
            'output_path': output_path,
            'error': str(e)
        })
        return False


async def handle_wowy(row: dict, skip_set: set, is_on: bool) -> bool:
    """Handle WOWY data fetching (on or off)"""
    timestamp = row["timestamp"]
    player_name = row["standard_name"]
    season_type_csv = row["season_type"]

    on_off_str = "on" if is_on else "off"
    output_file = f"wowy_{timestamp}_{player_name}_{on_off_str}.csv"
    output_path = os.path.join(OUTPUT_DIR, season_type_csv, timestamp, player_name, "wowy", output_file)

    # Check if file already exists first - before any other processing
    if os.path.exists(output_path):
        logger.info(f"File already exists, skipping: {output_path}")
        return True

    if output_path in skip_set:
        logger.info(f"Skipping known entry: {output_path}")
        return True

    # Get API season type
    season_type_api = SEASON_TYPE_MAPPINGS['wowy'].get(season_type_csv)
    if not season_type_api:
        logger.warning(f"Unknown season type for WOWY: {season_type_csv}")
        return False

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        # Get team info
        team_id, team_name = get_team_at_timestamp(player_name, timestamp)

        # Run selenium scrape in thread (blocking)
        await asyncio.to_thread(
            wowy_scrape.retrieve_from_wowy_via_selenium,
            player_name=player_name,
            team_name=team_name,
            date_str=timestamp,
            season_type_key=season_type_api,
            season_type_value=season_type_csv,
            is_on=is_on,
            output_path=output_path,
            team_id=team_id
        )

        logger.info(f"✅ WOWY {on_off_str} fetched: {player_name} @ {timestamp} ({season_type_csv})")
        return True

    except Exception as e:
        logger.error(f"Failed to fetch WOWY {on_off_str} for {player_name} @ {timestamp}: {e}")
        log_error('wowy', {
            'timestamp': timestamp,
            'player_name': player_name,
            'team_name': team_name if 'team_name' in locals() else 'Unknown',
            'season_type': season_type_csv,
            'on_or_off': on_off_str,
            'output_path': output_path,
            'error': str(e)
        })
        return False


# ============================================================================
# MAIN PROCESSING
# ============================================================================

async def process_pbp_csv(csv_path: Path, interval: float):
    """Process PBP CSV file with batching by timestamp"""
    if not csv_path.exists():
        logger.warning(f"File not found: {csv_path}")
        return

    # Load skip list and initialize error log
    skip_set = load_skip_list('pbp')
    init_error_log('pbp')

    # Read CSV and group by timestamp + season_type
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Group rows by (timestamp, season_type)
    grouped_rows = defaultdict(list)
    for row in rows:
        key = (row["timestamp"], row["season_type"])
        grouped_rows[key].append(row["standard_name"])

    logger.info(f"Processing {len(rows)} rows from {csv_path.name}, grouped into {len(grouped_rows)} unique timestamp/season_type combinations")

    # Process each unique timestamp/season_type combination
    tasks = []
    for i, ((timestamp, season_type), player_names) in enumerate(grouped_rows.items()):
        task = asyncio.create_task(handle_pbp_batch(timestamp, season_type, player_names, skip_set))
        tasks.append(task)

        # Rate limiting between different timestamp requests
        if i < len(grouped_rows) - 1:
            await asyncio.sleep(interval)

        # Log progress
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i + 1}/{len(grouped_rows)} timestamp groups scheduled")

    # Wait for all tasks to complete
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception) or r is False)
        logger.info(f"Completed {csv_path.name}: {success_count} success, {error_count} errors")


async def process_csv_file(csv_path: Path, source: str, interval: float):
    """Process a single CSV file with rate limiting (for non-PBP sources)"""
    if not csv_path.exists():
        logger.warning(f"File not found: {csv_path}")
        return

    # Load skip list and initialize error log
    skip_set = load_skip_list(source)
    init_error_log(source)

    # Track completed items (for caching)
    completed_timestamps = set()

    # Read CSV and process rows
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    logger.info(f"Processing {len(rows)} rows from {csv_path.name}")

    # Process rows with rate limiting
    tasks = []
    for i, row in enumerate(rows):
        # Create appropriate task based on source
        if source == 'tracking':
            task = asyncio.create_task(handle_tracking(row, skip_set, completed_timestamps))
        elif source == 'wowy_on':
            task = asyncio.create_task(handle_wowy(row, skip_set, is_on=True))
        elif source == 'wowy_off':
            task = asyncio.create_task(handle_wowy(row, skip_set, is_on=False))
        else:
            logger.warning(f"Unknown source: {source}")
            continue

        tasks.append(task)

        # Rate limiting
        if i < len(rows) - 1:  # Don't wait after last item
            await asyncio.sleep(interval)

        # Log progress every 100 rows
        if (i + 1) % 100 == 0:
            logger.info(f"Progress: {i + 1}/{len(rows)} rows scheduled")

    # Wait for all tasks to complete
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception) or r is False)
        logger.info(f"Completed {csv_path.name}: {success_count} success, {error_count} errors")


async def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("NBA Data Fetcher Started")
    logger.info("=" * 60)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Process PBP CSV with special batching logic
    pbp_csv_path = Path(INPUT_DIR) / "missing_pbp.csv"
    if pbp_csv_path.exists():
        logger.info(f"\n📂 Processing missing_pbp.csv with {PBP_INTERVAL}s interval (batched by timestamp)")
        await process_pbp_csv(pbp_csv_path, PBP_INTERVAL)
    else:
        logger.warning(f"⚠️  missing_pbp.csv not found in {INPUT_DIR}")

    # Process other CSV files normally
    other_csv_configs = [
        ("missing_tracking.csv", "tracking", TRACKING_INTERVAL),
        ("missing_wowy_on.csv", "wowy_on", WOWY_INTERVAL),
        ("missing_wowy_off.csv", "wowy_off", WOWY_INTERVAL),
    ]

    for csv_name, source, interval in other_csv_configs:
        csv_path = Path(INPUT_DIR) / csv_name
        if csv_path.exists():
            logger.info(f"\n📂 Processing {csv_name} with {interval}s interval")
            await process_csv_file(csv_path, source, interval)
        else:
            logger.warning(f"⚠️  {csv_name} not found in {INPUT_DIR}")

    logger.info("\n" + "=" * 60)
    logger.info("NBA Data Fetcher Completed")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️  Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        traceback.print_exc()