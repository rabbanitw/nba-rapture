import csv
import glob
import os
import time

import wowy_scrape
import pbp_scrape
from datetime import datetime
import pandas as pd
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import PlayerGameLog, CommonPlayerInfo
import nba_tracking_scrape
import traceback
from requests.exceptions import ReadTimeout
import asyncio
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from nba_api.library.http import NBAHTTP
import requests_cache
import time, random
from requests.exceptions import ReadTimeout
from nba_api.stats.endpoints import PlayerGameLog
from functools import lru_cache




_ALL_TEAMS = teams.get_teams()
ID_BY_ABBR    = { t["abbreviation"]: t["id"]        for t in _ALL_TEAMS }
NAME_BY_ID    = { t["id"]:           t["full_name"] for t in _ALL_TEAMS }


completed_nba_tracking_timestamps = []



CSV_PATH = Path("missing_wowy.csv")
INTERVAL = 2.0  # seconds between calls


if os.path.exists("wowy_skipped.txt"):
    with open("wowy_skipped.txt", encoding="utf-8") as f:
        SKIPPED_OUTPUTS = set(line.strip() for line in f)
else:
    SKIPPED_OUTPUTS = set()


ERROR_LOG = "wowy_errors.csv"
if not os.path.exists(ERROR_LOG):
    with open(ERROR_LOG, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "player_name",
            "team_name",
            "date_str",
            "season_type_key",
            "season_type_value",
            "is_on",
            "output_path",
            "team_id",
            "error"
        ])




def harden_nba_api(timeout=60, max_retries=5):
    # reuse or create the session that nba_api uses under the hood
    s = NBAHTTP._session or requests.Session()

    # retry on common transient failures & throttling
    retries = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        backoff_factor=1.5,                 # 0 -> 1.5 -> 3 -> 4.5 -> ...
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    # robust headers help avoid “botty” fingerprints
    s.headers.update({
        "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/138.0.0.0 Safari/537.36"),
        "Accept":          "application/json, text/plain, */*",
        "Referer":         "https://stats.nba.com",
        "Origin":          "https://www.nba.com",
        "Connection":      "keep-alive",   # or try 'close' if resets persist
    })

    NBAHTTP._session = s
    NBAHTTP._timeout = timeout

harden_nba_api(timeout=60, max_retries=5)


def safe_player_game_log(pid, season, attempts=4):
    delay = 1.0
    for i in range(attempts):
        try:
            # small jitter helps with thundering herd
            time.sleep(0.15 + random.random() * 0.15)
            return PlayerGameLog(player_id=pid, season=season)
        except (ReadTimeout, Exception) as e:
            if i == attempts - 1:
                raise
            print(f"⏳ PlayerGameLog retry {i+1}/{attempts} for {pid} {season}: {e}")
            time.sleep(delay)
            delay *= 2


# ——— Your per-source handlers ———
def handle_pbp(row):
    """
    Called for each row in missing_pbp.csv
    row is a dict, e.g. {"timestamp": "...", "name": "..."}
    """
    # TODO: implement your pbp-specific logic here
    name = row["standard_name"]
    timestamp = row["timestamp"]

    season_types = [
        {'Regular Season': 'Regular season'},
        {'Playoffs': 'Playoffs'},
        # {'PlayIn': 'Play in'},
        {'All': 'Full'},
        # {'Full': 'Full'}
    ]
    for season_type in season_types:
        for season_type_key, season_type_value in season_type.items():
            folder_path = 'missing_data'
            output_file = f"pbp_stats_{timestamp}.csv"
            output_path = os.path.join(folder_path, season_type_value, output_file)

            os.makedirs(season_type_value, exist_ok=True)
            if os.path.exists(output_path):
                print("File exists!")
                continue

            pbp_scrape.scrape_and_save_without_async(timestamp, season_type_key, season_type_value, output_path)
    print(f"[PBP] {row['timestamp']} – {row['standard_name']}")

def handle_nba_tracking(row):
    """
    Called for each row in missing_nba-tracking.csv
    row is a dict, e.g. {"timestamp": "...", "name": "...", "data_type": "..."}
    """
    # TODO: implement your nba-tracking logic here
    timestamp = row['timestamp']

    data_type = row['data_type']
    season_types = [
        {'Regular Season': 'Regular season'},
        {'Playoffs': 'Playoffs'},
        # {'PlayIn': 'Play in'},
        {'All': 'Full'},
        # {'Full': 'Full'}
    ]
    if timestamp not in completed_nba_tracking_timestamps:
        for season_type in season_types:
            for season_type_key, season_type_value in season_type.items():
                if season_type_value != 'Full season':
                    nba_tracking_scrape.retrieve_from_nba_api(timestamp=timestamp, season_type=season_type_value, stat_type=data_type)
                    completed_nba_tracking_timestamps.append(timestamp)

    print(f"[NBA-TRACKING] {row['timestamp']} – {row['standard_name']} ({row.get('data_type','')})")

def handle_wowy(row):
    """
    Called for each row in missing_wowy.csv
    row is a dict, e.g. {"timestamp": "...", "name": "...", "on_or_off": "..."}
    """
    # TODO: implement your wowy-specific logic here
    name = row["standard_name"]
    timestamp = row["timestamp"]

    team_id, team_name = get_team_at_timestamp(name, timestamp)
    season_types = [
        {'Regular Season': 'Regular season'},
        {'Playoffs': 'Playoffs'},
        # {'PlayIn': 'Play in'},
        {'All': 'All'},
    ]

    for season_type in season_types:
        for season_type_key, season_type_value in season_type.items():
            folder_path = 'missing_data'
            on_or_off = row["on_or_off"]
            output_file = f"wowy_{timestamp}_{name}_{on_or_off}.csv"
            output_path = os.path.join(folder_path, season_type_value, output_file)
            print("output_path is:", output_path)
            if os.path.exists(output_path):
                print("WOWY file exists!")
                continue
            if output_path in SKIPPED_OUTPUTS:
                print(f"⏭️ Skipping known-empty {output_path}")
                continue
            try:
                wowy_scrape.retrieve_from_wowy_via_selenium(
                    player_name=name,
                    team_name=team_name,
                    date_str=timestamp,
                    season_type_key=season_type_key,
                    season_type_value=season_type_value,
                    is_on=True if row["on_or_off"]=="on" else False,
                    output_path = output_path,
                    team_id=team_id,
                )
            except Exception as e:
                print(f"Error fetching WOWY for {name}@{timestamp}: {e}")

                # 2) append a row to the CSV with all your params + the exception text
                with open(ERROR_LOG, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        timestamp,
                        name,
                        team_name,
                        timestamp,  # date_str is the same as timestamp here
                        season_type_key,
                        season_type_value,
                        row["on_or_off"],  # or use is_on
                        output_path,
                        team_id,
                        str(e)  # exception message
                    ])
    print(f"[WOWY] {row['timestamp']} – {row['standard_name']} ({row.get('on_or_off','')})")

# ——— Dispatch map from source → handler ———
HANDLERS = {
    # "pbp": handle_pbp,
    # "nba-tracking": handle_nba_tracking,
    "wowy": handle_wowy,
}

def process_file(path):
    # derive source from filename: missing_{source}.csv
    filename = os.path.basename(path)
    source = filename.removeprefix("missing_").removesuffix(".csv")
    handler = HANDLERS.get(source)
    if handler is None:
        print(f"⚠️  No handler registered for source '{source}', skipping {filename}")
        return

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                handler(row)
            except Exception as e:
                print(f"Error handling row {row['standard_name']} and {row['timestamp']}", e)
                # 1) print which file/row triggered the error
                print(f"\n❌ ERROR in {filename} on row:")
                for k, v in row.items():
                    # !r will reveal hidden codepoints
                    print(f"  {k!r}: {v!r}")
                # 2) full traceback
                traceback.print_exc()



# def main():
#     for path in glob.glob("missing_*.csv"):
#         print(f"\nProcessing {path}")
#         process_file(path)


@lru_cache(maxsize=5000)
def get_team_at_timestamp(player_name: str, timestamp: str, max_retries=5) -> tuple[int,str]:
    # 1) look up player
    matches = players.find_players_by_full_name(player_name)
    if not matches:
        raise ValueError(f"No NBA player found for '{player_name}'")
    pid = matches[0]["id"]

    # 2) parse season string
    dt = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    start_year = dt.year if dt.month >= 10 else dt.year - 1
    season = f"{start_year}-{str(start_year+1)[-2:]}"

    # 3) fetch game log

    for attempt in range(1, max_retries + 1):
        try:
            # gl = PlayerGameLog(player_id=pid, season=season)
            gl = safe_player_game_log(pid=pid, season=season)
            break  # success
        except ReadTimeout:
            print(f"⏳ Timeout on PlayerGameLog for {player_name}, retrying ({attempt}/{max_retries})...")
            time.sleep(2 ** attempt)  # exponential backoff
    else:
        raise RuntimeError(f"❌ Failed to fetch PlayerGameLog for {player_name} after {max_retries} retries.")

    df = gl.get_data_frames()[0]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    # 4) pick the most recent game on or before timestamp
    played = df[df["GAME_DATE"] <= dt]
    if not played.empty:
        row = played.iloc[0]
        # try direct TEAM_ID
        if "TEAM_ID" in row:
            team_id = int(row["TEAM_ID"])
        # try TEAM_ABBREVIATION
        elif "TEAM_ABBREVIATION" in row:
            team_id = ID_BY_ABBR[row["TEAM_ABBREVIATION"]]
        # fallback: parse MATCHUP
        else:
            # e.g. "CLE vs. UTA" or "PHI @ MIL"
            abbr = row["MATCHUP"].split()[0].replace("@","").replace("vs.","").strip().upper()
            team_id = ID_BY_ABBR.get(abbr)
            if team_id is None:
                raise ValueError(f"Unknown team abbreviation '{abbr}' in MATCHUP")
    else:
        # no games yet: fallback to current roster via CommonPlayerInfo
        info = CommonPlayerInfo(player_id=pid)
        info_df = info.get_data_frames()[0]
        team_id = int(info_df.loc[0, "TEAM_ID"])

    team_name = NAME_BY_ID.get(team_id, "Unknown Team")
    return team_id, team_name


async def run_handler_async(row):
    try:
        # `handle_wowy` is sync; run it in a thread
        await asyncio.to_thread(handle_wowy, row)
    except Exception:
        print(f"\n❌ ERROR in missing_wowy.csv on row:")
        for k, v in row.items():
            print(f"  {k!r}: {v!r}")
        traceback.print_exc()


async def schedule_wowy(interval: float):
    """
    Read just missing_wowy.csv, and for each row:
      • spawn handle_wowy(row)
      • wait `interval` seconds
    """
    if not CSV_PATH.exists():
        print(f"❌ File not found: {CSV_PATH}")
        return

    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # dispatch the work
            asyncio.create_task(run_handler_async(row))
            # throttle
            await asyncio.sleep(interval)

    print(f"✅ Scheduled all rows in {CSV_PATH} at {interval}s intervals.")



async def main():
    await schedule_wowy(INTERVAL)
    # give a bit of time for in-flight tasks to finish:
    await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())

