from datetime import datetime
import re
import asyncio, functools, os
from time import sleep

_failed_log_lock = asyncio.Lock()  # one lock for the whole module


historical_checkbox_ids = [
                    'filter-2014',
                    'filter-2015',
                    'filter-2016',
                    'filter-2017',
                    'filter-2018',
                    'filter-2019',
                    'filter-2020',
                ]


async def log_failed(timestamp: str, season_type: str, reason: str):
    """Append a line to failed_timestamps.csv in a thread‑safe way."""
    line = f"{timestamp},{season_type},{reason}\n"
    async with _failed_log_lock:
        # run the blocking file‑write in a thread so we don't block the loop
        await asyncio.to_thread(
            functools.partial(
                open("failed_timestamps.csv", "a", encoding="utf-8").write,
                line
            )
        )


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

    # 2020-21 season (COVID-affected)
    if season == '2020-21':
        if season_type == "Playoffs":
            if inside_range(timestamp, '2021-07-20'):
                return ['2021-05-22', regular_time(timestamp)]
        elif season_type == "Regular Season":
            if inside_range(timestamp, '2021-05-22'):
                return ['2020-12-22', regular_time(timestamp)]
        else:
            return ['2020-12-22', regular_time(timestamp)]

    # 2021-22 season
    elif season == '2021-22':
        if season_type == "Playoffs":
            if inside_range(timestamp, '2022-06-16'):
                return ['2022-04-16', regular_time(timestamp)]
        elif season_type == "Regular season":
            if inside_range(timestamp, '2022-04-16'):
                return ['2021-10-19', regular_time(timestamp)]
        else:
            return ['2021-10-19', regular_time(timestamp)]

    # 2022-23 season
    elif season == '2022-23':
        if season_type == "Playoffs":
            if inside_range(timestamp, '2023-06-12'):
                return ['2023-04-15', regular_time(timestamp)]
        elif season_type == "Regular season":
            if inside_range(timestamp, '2023-04-15'):
                return ['2022-10-18', regular_time(timestamp)]
        else:
            return ['2022-10-18', regular_time(timestamp)]

    # 2019-20 season (COVID-affected)
    elif season == '2019-20':
        if season_type == "Playoffs":
            return ['2020-08-17', '2020-10-11']  # Playoffs resumed in bubble
        elif season_type == "Regular Season":
            return ['2019-10-22', '2020-08-17']
        else:
            return ['2019-10-22', '2020-10-11']

    # 2018-19 season
    elif season == '2018-19':
        if season_type == "Playoffs":
            return ['2019-04-13', '2019-06-13']
        elif season_type == "Regular Season":
            return ['2018-10-16', '2019-04-13']
        else:
            return ['2018-10-16', '2019-06-13']

    # 2017-18 season
    elif season == '2017-18':
        if season_type == "Playoffs":
            return ['2018-04-14', '2018-06-08']
        elif season_type == "Regular Season":
            return ['2017-10-17', '2018-04-14']
        else:
            return ['2017-10-17', '2018-06-08']

    # 2016-17 season
    elif season == '2016-17':
        if season_type == "Playoffs":
            return ['2017-04-15', '2017-06-12']
        elif season_type == "Regular Season":
            return ['2016-10-25', '2017-04-15']
        else:
            return ['2016-10-25', '2017-06-12']

    # 2015-16 season
    elif season == '2015-16':
        if season_type == "Playoffs":
            return ['2016-04-16', '2016-06-19']
        elif season_type == "Regular Season":
            return ['2015-10-27', '2016-04-16']
        else:
            return ['2015-10-27', '2016-06-19']

    # 2014-15 season
    elif season == '2014-15':
        if season_type == "Playoffs":
            return ['2015-04-18', '2015-06-16']
        elif season_type == "Regular Season":
            return ['2014-10-28', '2015-04-18']
        else:
            return ['2014-10-28', '2015-06-16']

    # 2013-14 season
    elif season == '2013-14':
        if season_type == "Playoffs":
            return ['2014-04-19', '2014-06-15']
        elif season_type == "Regular Season":
            return ['2013-10-29', '2014-04-19']
        else:
            return ['2013-10-29', '2014-06-15']

    # No matching branch
    raise ValueError(
        f"No date-range rule for season={season} season_type={season_type}"
    )


def regular_time(waystamp):
    # Wayback time format YYYYMMDDhhmmss
    date_object = datetime.strptime(waystamp, "%Y%m%d%H%M%S")
    convert_date = date_object.strftime("%Y-%m-%d")
    return convert_date


def wayback_time(date):
    # PBP date format
    date_object = datetime.strptime(date, "%Y-%m-%d")

    # Turn into wayback timestamp
    convert_date = date_object.strftime("%Y%m%d%H%M%S")

    return convert_date


def remove_numbers_and_apostrophes(string: str) -> str:
    return re.sub(r'[\d\'\-]+$', '', string)


# PBP API caller

import requests
import csv
import os
import asyncio
import glob


# timestamp = utils.get_timestamp()
def scrape_and_save_without_async(date_str, season_type_key, season_type_value, output_path):
    print(f"now processing {output_path}")
    url = "https://api.pbpstats.com/get-totals/nba"
    try:
        start_date, end_date = get_date_range(date_str, season_type_value)
    except ValueError as e:
        print(f"[SKIP] {e}")
        return

    params = {
        "Season": get_season(date_str),
        "SeasonType": season_type_key,
        "Type": "Player",
        "FromDate": start_date,
        "ToDate": end_date,
        "StartType": "All",
        "StatType": "Per100Possessions"
    }
    attempt = 0
    max_delay = 600  # 10 minutes in seconds

    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()

            response_json = response.json()
            player_stats = response_json["multi_row_table_data"]

            # Collect all fieldnames
            all_keys = set()
            for row in player_stats:
                all_keys.update(row.keys())

            # Write to CSV
            with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=all_keys)
                writer.writeheader()
                writer.writerows(player_stats)

            print(f"Data has been written to {output_path}")
            return  # Exit the function if successful

        except requests.exceptions.RequestException as e:
            attempt += 1
            # Exponential backoff: 2^(attempt-1), but capped at 600 seconds
            delay = min(2 ** (attempt - 1), max_delay)
            print(
                f"[Attempt {attempt}] Failed to write {output_path}: {e}\n"
                f"Retrying in {delay} seconds..."
            )
            sleep(delay)


def get_final_timestamp_for_season(checkbox_id):
    if checkbox_id == "filter-2014":
        return "20140715000000"
    elif checkbox_id == "filter-2015":
        return "20150715000000"
    elif checkbox_id == "filter-2016":
        return "20160715000000"
    elif checkbox_id == "filter-2017":
        return "20170715000000"
    elif checkbox_id == "filter-2018":
        return "20180715000000"
    elif checkbox_id == "filter-2019":
        return "20201101000000"
    elif checkbox_id == "filter-2020":
        return "20210801000000"





async def scrape_and_save(date_str, season_type_key, season_type_value, output_path):
    print(f"now processing {output_path}")
    url = "https://api.pbpstats.com/get-totals/nba"
    try:
        start_date, end_date = get_date_range(date_str, season_type_value)
    except ValueError as e:
        await log_failed(date_str, season_type_value, str(e))
        print(f"[SKIP] {e}")
        return

    params = {
        "Season": get_season(date_str),
        "SeasonType": season_type_key,
        "Type": "Player",
        "FromDate": start_date,
        "ToDate": end_date,
        "StartType": "All",
        "StatType": "Per100Possessions"
    }
    attempt = 0
    max_delay = 600  # 10 minutes in seconds

    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()

            response_json = response.json()
            player_stats = response_json["multi_row_table_data"]

            # Collect all fieldnames
            all_keys = set()
            for row in player_stats:
                all_keys.update(row.keys())

            # Write to CSV
            with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=all_keys)
                writer.writeheader()
                writer.writerows(player_stats)

            print(f"Data has been written to {output_path}")
            return  # Exit the function if successful

        except requests.exceptions.RequestException as e:
            attempt += 1
            # Exponential backoff: 2^(attempt-1), but capped at 600 seconds
            delay = min(2 ** (attempt - 1), max_delay)
            print(
                f"[Attempt {attempt}] Failed to write {output_path}: {e}\n"
                f"Retrying in {delay} seconds..."
            )
            await asyncio.sleep(delay)


async def new_retrieve_from_pbp():
    tasks = []

    # Define the mapping between folder names and API season types
    season_type_mapping = {
        'Regular season': 'Regular season',
        'Playoffs': 'Playoffs',
        'Full season': 'Full season'
    }

    # Define the mapping between folder names and API keys
    season_type_keys = {
        'Regular season': 'Regular Season',
        'Playoffs': 'Playoffs',
        'Full season': 'All'
    }

    base_folder = '538'
    output_base = 'pbp'

    # Check if 538 folder exists
    if not os.path.exists(base_folder):
        print(f"Error: {base_folder} folder not found!")
        return

    # Iterate through season type folders
    for season_type_folder in os.listdir(base_folder):
        season_type_path = os.path.join(base_folder, season_type_folder)

        # Skip if not a directory or not in our mapping
        if not os.path.isdir(season_type_path) or season_type_folder not in season_type_mapping:
            continue

        season_type_value = season_type_mapping[season_type_folder]
        season_type_key = season_type_keys[season_type_folder]

        print(f"Processing season type: {season_type_folder}")

        # Iterate through filter folders (filter-2014, filter-2015, etc.)
        for filter_folder in os.listdir(season_type_path):
            filter_path = os.path.join(season_type_path, filter_folder)

            # Skip if not a directory or doesn't start with 'filter-'
            if not os.path.isdir(filter_path) or not filter_folder.startswith('filter-'):
                continue

            print(f"  Processing filter: {filter_folder}")

            # Iterate through files in the filter folder
            for filename in os.listdir(filter_path):
                filepath = os.path.join(filter_path, filename)

                # Skip if not a file
                if not os.path.isfile(filepath):
                    continue

                name, extension = os.path.splitext(filename)

                # Check if filename (without extension) is numeric (timestamp)
                if name.isnumeric():
                    if filter_folder in historical_checkbox_ids:
                        date_str = get_final_timestamp_for_season(filter_folder)
                    else:
                        date_str = name
                    output_file = f"pbp_stats_{date_str}.csv"

                    # Create output path in pbp directory
                    output_dir = os.path.join(output_base, season_type_folder, filter_folder)
                    output_path = os.path.join(output_dir, output_file)

                    # Create output directory if it doesn't exist
                    os.makedirs(output_dir, exist_ok=True)

                    # Skip if output file already exists
                    if os.path.exists(output_path):
                        print(f"    File exists: {output_path}")
                        continue

                    # Create task for async processing
                    tasks.append(asyncio.create_task(
                        scrape_and_save(date_str, season_type_key, season_type_value, output_path)
                    ))

                else:
                    print(f"    Skipping non-numeric file: {name}")

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
        print("All tasks in new retrieve from pbp have completed!")
    else:
        print("No tasks to process!")


if __name__ == "__main__":
    asyncio.run(new_retrieve_from_pbp())