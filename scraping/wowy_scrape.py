import requests
import utils
import database
import time
import traceback
from fuzzydict import FuzzyDict
import asyncio
import os, csv, json
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json
import time
from urllib.parse import urlencode
from data import nba_player_ids, historical_checkbox_ids, get_final_timestamp_for_season

PROCESSED_FILES_LOG = "processed_files_wowy.log"
# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
# }
headers = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/138.0.7204.97 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.pbpstats.com/",
    "Origin": "https://www.pbpstats.com",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

db = database.get_database()

lock = asyncio.Lock()
processed_count = 0

nba_team_ids = {
    "Hawks": 1610612737,
    "Celtics": 1610612738,
    "Nets": 1610612751,
    "Hornets": 1610612766,
    "Bulls": 1610612741,
    "Cavaliers": 1610612739,
    "Mavericks": 1610612742,
    "Nuggets": 1610612743,
    "Pistons": 1610612765,
    "Warriors": 1610612744,
    "Rockets": 1610612745,
    "Pacers": 1610612754,
    "Clippers": 1610612746,
    "Lakers": 1610612747,
    "Grizzlies": 1610612763,
    "Heat": 1610612748,
    "Bucks": 1610612749,
    "Timberwolves": 1610612750,
    "Pelicans": 1610612740,
    "Knicks": 1610612752,
    "Thunder": 1610612760,
    "Magic": 1610612753,
    "76ers": 1610612755,
    "Suns": 1610612756,
    "Trail Blazers": 1610612757,
    "Kings": 1610612758,
    "Spurs": 1610612759,
    "Raptors": 1610612761,
    "Jazz": 1610612762,
    "Wizards": 1610612764
}

fuzzy_nba_player_ids = FuzzyDict(threshold=80)
fuzzy_nba_player_ids.update(nba_player_ids)

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=options)

timestamps_to_scrape = [
    '20140715000000',
    '20150715000000',
    '20160715000000',
    '20170715000000',
    '20180715000000',
    '20201101000000'
]


def is_after_last_timestamp(timestamp):
    return timestamp >= '20220107000332'



def retrieve_from_wowy_via_selenium(player_name, team_name, date_str, season_type_key, season_type_value, is_on,
                                    output_path=None, team_id=None):
    try:
        start_date, end_date = utils.get_date_range_extended(date_str, season_type_value)
    except TypeError as e:
        print(f"[SKIP] {e}")
        return

    # 2. Construct the final URL yourself
    params = {
        "Season": utils.get_season(date_str),
        "SeasonType": season_type_key,
        "Type": "Team",
        "FromDate": start_date,
        "ToDate": end_date,
        "TeamId": team_id or nba_team_ids[team_name],
    }
    if is_on:
        params['0Exactly1OnFloor'] = fuzzy_nba_player_ids.get(player_name)
    else:
        params['0Exactly0OnFloor'] = fuzzy_nba_player_ids.get(player_name)
    print("now processing params:", params)

    url = f"https://api.pbpstats.com/get-wowy-stats/nba?{urlencode(params)}"
    print("url?", url)

    # 3. Load the page
    driver.get(url)
    time.sleep(3)  # give it time to load

    # 4. Extract the raw JSON text
    raw_text = driver.find_element("tag name", "pre").text
    data = json.loads(raw_text)

    # 5. Done
    parsed_rows = data.get("single_row_table_data", [])
    print("✅ Parsed rows:", len(parsed_rows))

    if parsed_rows:
        save_local_wowy_data(parsed_rows, output_path)
        # write_wowy_data(stats, player_name, date_str, season_type_value, is_on)
    else:
        print("No data to write")
        with open("wowy_skipped.txt", "a", encoding="utf-8") as f:
            f.write(output_path + "\n")


def load_processed_files() -> set:
    if not os.path.exists(PROCESSED_FILES_LOG):
        return set()
    with open(PROCESSED_FILES_LOG, "r") as f:
        processed = {line.strip() for line in f if line.strip()}
    return processed


def mark_file_processed(file_name: str):
    with open(PROCESSED_FILES_LOG, "a") as f:
        f.write(file_name + "\n")


async def robust_get_wowy_data(player_name, team_name, date_str, season_type_key, season_type_value, is_on,
                               output_path):
    attempt = 0
    max_delay = 600  # 10 minutes in seconds

    while True:
        try:
            retrieve_from_wowy(player_name, team_name, date_str, season_type_key, season_type_value, is_on, output_path)
            return
        except requests.exceptions.RequestException as e:
            attempt += 1
            delay = min(2 ** (attempt - 1), max_delay)
            print(
                f"[Attempt {attempt}] Failed to save to database: {e}\n"
                f"Retrying in {delay} seconds..."
            )
            await asyncio.sleep(delay)


async def robust_get_wowy_data_limited(player_name, team_name, date_str, season_type_key, season_type_value, on_or_off,
                                       sem, output_path):
    global processed_count
    async with sem:
        await robust_get_wowy_data(player_name, team_name, date_str, season_type_key, season_type_value, on_or_off,
                                   output_path)
    async with lock:
        processed_count += 1
        print(f"Processed {processed_count} tasks so far.")
    return True


def write_wowy_data(wowy_data, player_name, timestamp, season_type, is_on):
    wowy_data["name"] = player_name
    wowy_data["timestamp"] = timestamp
    wowy_data["season_type"] = season_type
    wowy_data["on_or_off"] = "on" if is_on else "off"
    wowy_data["source"] = "wowy"
    database.create_document(db, wowy_data)
    print(f"Saved [{player_name}], [{timestamp}], [{'on' if is_on else 'off'}] to database!")


def ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data):
    ensure_parent(path)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"❌ Failed to write JSON {path}: {e}")


def save_local_wowy_data(wowy_data, output_path):
    """
       Writes a single-row CSV, guaranteed to handle Unicode
       """
    path = Path(output_path)
    ensure_parent(path)

    if path.exists():
        print(f"⚠️  File already exists: {path}")
        return
    if output_path is None:
        print("output_path is None. Skipping.")
        return
    # if os.path.exists(output_path):
    #     print("File exists!")

    else:
        print("writing to file", path)
        # with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
        #     writer = csv.DictWriter(csvfile, fieldnames=wowy_data.keys())
        #     writer.writeheader()
        #     writer.writerow(wowy_data)
        with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=wowy_data.keys())
            writer.writeheader()

            try:
                writer.writerow(wowy_data)
            except UnicodeEncodeError as e:
                # pinpoint the offending field
                print(f"\n🚨 UnicodeEncodeError when writing {output_path}: {e}")
                for k, v in wowy_data.items():
                    # show repr to expose the exact character
                    print(f"  {k!r}: {v!r}")
                # re-raise so you still get the stack trace
                raise


def retrieve_from_wowy(player_name, team_name, date_str, season_type_key, season_type_value, is_on, output_path=None,
                       team_id=None):
    url = "https://api.pbpstats.com/get-wowy-stats/nba"
    try:
        start_date, end_date = utils.get_date_range_extended(date_str, season_type_value)
    except TypeError as e:
        print(f"[SKIP] {e}")
        return

    params = {
        "Season": utils.get_season(date_str),
        "SeasonType": season_type_key,
        "Type": "Team",
        "FromDate": start_date,
        "ToDate": end_date,
        "TeamId": team_id or nba_team_ids[team_name],
    }
    if is_on:
        params['0Exactly1OnFloor'] = fuzzy_nba_player_ids.get(player_name)
    else:
        params['0Exactly0OnFloor'] = fuzzy_nba_player_ids.get(player_name)
    print("now processing params:", params)

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    stats = response_json["single_row_table_data"]

    print("after making requests")

    if stats:
        save_local_wowy_data(stats, output_path)
        # write_wowy_data(stats, player_name, date_str, season_type_value, is_on)
    else:
        print("No data to write")


async def process_file(full_file_path, filename, season_type_key, season_type_value, season_type_folder, filter_folder,
                       sem):
    tasks = []
    output_base = 'wowy'  # Base output directory

    with open(full_file_path, 'r') as file:
        c = csv.DictReader(file)
        try:
            header = next(c)
        except StopIteration:
            print(f"Warning: Empty CSV file: {full_file_path}")
            return
        for row in c:
            player_name = utils.remove_numbers_and_apostrophes(row['name'])
            team_name = row['team']
            date_str = os.path.splitext(filename)[0]

            if filter_folder in historical_checkbox_ids:
                date_str = get_final_timestamp_for_season(filter_folder)


            # Create output directory structure
            output_dir = os.path.join(output_base, season_type_folder, filter_folder)
            os.makedirs(output_dir, exist_ok=True)

            # Process "on" data
            output_file_on = f"wowy_{date_str}_{player_name}_on.csv"
            output_path_on = os.path.join(output_dir, output_file_on)

            tasks.append(asyncio.create_task(
                robust_get_wowy_data_limited(player_name, team_name, date_str,
                                             season_type_key, season_type_value,
                                             True, sem, output_path_on)
            ))

            # Process "off" data
            output_file_off = f"wowy_{date_str}_{player_name}_off.csv"
            output_path_off = os.path.join(output_dir, output_file_off)

            tasks.append(asyncio.create_task(
                robust_get_wowy_data_limited(player_name, team_name, date_str,
                                             season_type_key, season_type_value,
                                             False, sem, output_path_off)
            ))

    # Gather all tasks for this file
    await asyncio.gather(*tasks)
    print(f"Done processing file {filename}!")


async def main():
    processed_files = load_processed_files()

    # Folder-name -> utils.get_date_range() "season_type_value"
    season_type_mapping = {
        'Regular season': 'Regular season',
        'Playoffs': 'Playoffs',
        'Full season': 'All'
    }

    # Folder-name -> API "SeasonType" value
    season_type_keys = {
        'Regular season': 'Regular Season',
        'Playoffs': 'Playoffs',
        'Full season': 'All'
    }

    base_folder = '538'
    if not os.path.exists(base_folder):
        print(f"Error: {base_folder} folder not found!")
        return

    # Concurrency cap
    sem = asyncio.Semaphore(3)

    # Iterate the explicit lists instead of scanning the directory tree
    for season_type_folder, season_type_key in season_type_keys.items():
        # Skip unknown folders just in case
        if season_type_folder not in season_type_mapping:
            continue

        season_type_value = season_type_mapping[season_type_folder]
        print(f"Processing season type: {season_type_folder} ({season_type_key})")

        for timestamp in timestamps_to_scrape:
            # Basic guards
            if not isinstance(timestamp, str) or not timestamp.isdigit():
                print(f"  Skipping invalid timestamp: {timestamp!r}")
                continue
            if is_after_last_timestamp(timestamp):
                print(f"  Skipping because it's after the last timestamp: {timestamp}")
                continue

            # Derive filter folder from the year in the timestamp
            year = timestamp[:4] if timestamp[:4] != '2020' else '2019'
            filter_folder = f"filter-{year}"
            filename = f"{timestamp}.csv"
            filepath = os.path.join(base_folder, season_type_folder, filter_folder, filename)

            # Unique id for processed-files tracking
            file_identifier = f"{season_type_folder}/{filter_folder}/{filename}"

            if file_identifier in processed_files:
                print(f"  Skipping already processed file: {file_identifier}")
                continue

            # Make sure the CSV exists
            if not os.path.isfile(filepath):
                print(f"  Missing CSV: {filepath} — skipping")
                continue

            print(f"  Processing: {file_identifier}")
            await process_file(
                filepath,
                filename,
                season_type_key,
                season_type_value,
                season_type_folder,
                filter_folder,
                sem
            )

            # mark_file_processed(file_identifier)

    print("wowee we're done!")


if __name__ == "__main__":
    asyncio.run(main())
    driver.quit()