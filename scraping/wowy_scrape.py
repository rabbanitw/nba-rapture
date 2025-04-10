import requests
import csv
import os
import utils
import database
import uids
import time
import traceback
from fuzzydict import FuzzyDict
import asyncio


PROCESSED_FILES_LOG = "processed_files_wowy.log"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

db = database.get_database()

SEM = asyncio.Semaphore(10)
lock = asyncio.Lock()
processed_count = 0

fuzzy_nba_player_ids = FuzzyDict()
fuzzy_nba_player_ids.update(uids.nba_player_ids)

def load_processed_files() -> set:
    if not os.path.exists(PROCESSED_FILES_LOG):
        return set()
    with open(PROCESSED_FILES_LOG, "r") as f:
        processed = {line.strip() for line in f if line.strip()}
    return processed


def mark_file_processed(file_name: str):
    with open(PROCESSED_FILES_LOG, "a") as f:
        f.write(file_name + "\n")


async def robust_get_wowy_data(player_name, team_name, date_str, season_type, is_on):
    attempt = 0
    max_delay = 600  # 10 minutes in seconds

    while True:
        try:
            retrieve_from_wowy(player_name, team_name, date_str, season_type, is_on)
            return
        except requests.exceptions.RequestException as e:
            attempt += 1
            delay = min(2 ** (attempt - 1), max_delay)
            print(
                f"[Attempt {attempt}] Failed to save to database: {e}\n"
                f"Retrying in {delay} seconds..."
            )
            await asyncio.sleep(delay)


async def robust_get_wowy_data_limited(player_name, team_name, date_str, season_type, on_or_off, sem):
    global processed_count
    async with sem:
        await robust_get_wowy_data(player_name, team_name, date_str, season_type, on_or_off)
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


def save_local_wowy_data(wowy_data, output_path):
    if os.path.exists(output_path):
        print("File exists!")
    else:
        with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=wowy_data.keys())
            writer.writeheader()
            writer.writerow(wowy_data)


def retrieve_from_wowy(player_name, team_name, date_str, season_type, is_on):
    url = "https://api.pbpstats.com/get-wowy-stats/nba"
    start_date, end_date = utils.get_date_range(date_str, season_type)

    params = {
        "Season": utils.get_season(date_str),
        "SeasonType": season_type,
        "Type": "Team",
        "FromDate": start_date,
        "ToDate": end_date,
        "TeamId": uids.nba_team_ids[team_name],
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

    is_on_string = "on" if is_on else "off"
    output_file = f"pbp_wowy_{player_name}_{is_on_string}_{date_str}.csv"
    output_path = os.path.join("nba-ml", season_type_value, output_file)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if stats:
        save_local_wowy_data(stats, output_path)
        # write_wowy_data(stats, player_name, date_str, season_type_value, is_on)
    else:
        print("No data to write")


async def process_file(full_file_path, filename, season_type, sem):
    print(full_file_path)
    print(filename)
    print(season_type)
    print(sem)
    tasks = []
    with open(full_file_path, 'r') as file:
        c = csv.DictReader(file)
        header = next(c)
        for row in c:
            player_name = utils.remove_numbers_and_apostrophes(row['name'])
            team_name = row['team']
            date_str = os.path.splitext(filename)[0]

            tasks.append(asyncio.create_task(
                robust_get_wowy_data_limited(player_name, team_name, date_str,
                                             season_type, True, sem)
            ))
            tasks.append(asyncio.create_task(
                robust_get_wowy_data_limited(player_name, team_name, date_str,
                                             season_type, False, sem)
            ))

    # Gather all tasks for this file
    await asyncio.gather(*tasks)
    print(f"Done processing file {filename}!")


async def main():
    # processed_files = load_processed_files()
    season_types = ['RegularSeason', 'Playoffs', 'PlayIn', 'All']

    # concurrency limit to 10 tasks at a time (tweak as needed)
    sem = asyncio.Semaphore(10)

    for st in season_types:
        folder_path = 'nba-ml'
        files = os.listdir(f"{folder_path}/{st}")
        filename = "test.csv"

    # logic kinda weird here if there are no files...processed log doesn't work?
    # what do these files look like...
        # for filename in files:
            # if filename in processed_files:
            #     print(f"Skipping already processed file: {filename}")
            #     continue

            # name, extension = os.path.splitext(filename)
            # if not name.isnumeric():
            #     print(f"Skipping {filename}; not a valid timestamp")
            #     continue

        full_file_path = os.path.join(folder_path, st, filename)

            # Process one file at a time (could also queue up tasks for many files if desired)
        await process_file(full_file_path, filename, st, sem)

        mark_file_processed(filename)

    print("wowee we're done!")


if __name__ == "__main__":
    asyncio.run(main())
