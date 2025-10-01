from pathlib import Path
import requests
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from urllib.parse import urlencode
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import Select
import os
import utils
from datetime import datetime
from typing import Any
# Modern style (3.10+) – Callable lives in collections.abc
from collections.abc import Callable
import re
import tempfile
import shutil
import undetected_chromedriver as uc
import traceback
from data import nba_player_ids, historical_checkbox_ids, get_final_timestamp_for_season
import csv
import sys

ROOT_DIR = Path("nba_api")  # ← Output directory for NBA API data
SOURCE_DIR = Path("538")  # ← Source directory to read timestamps from
SUBFOLDERS = ["Playoffs", "Regular season", "Full season"]
HISTORICAL_YEARS = ['filter-2014', 'filter-2015', 'filter-2016', 'filter-2017', 'filter-2018', 'filter-2019']

# Increase the connection and read timeouts (in seconds)
os.environ['NBA_API_CONNECTION_TIMEOUT'] = '60'
os.environ['NBA_API_READ_TIMEOUT'] = '60'
Path.mkdir(ROOT_DIR, exist_ok=True)


def get_number_or_zero(value):
    try:
        return float(value)
    except:
        return 0


def write_to_file(data, output_path):
    try:
        print(f"writing to file: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(data, file)
    except Exception as e:
        print(f"Failed to write {output_path}", e)


def convert_to_nba_api_season(season_type_value):
    return {
        'Regular season': 'Regular Season',
        'Playoffs': 'Playoffs',
        'Play in': 'PlayIn',
        'Full season': 'Full Season'  # Default to Regular Season for full season
    }.get(season_type_value)


def to_float(x: str) -> float:
    return float(x or 0)


_num = re.compile(r"""
    ^\s*
    [+-]?
    (?:
        (?:\d{1,3}(?:,\d{3})*|\d+)?
        (?:\.\d*)?
        |\.\d+
    )
    \s*%?\s*$
""", re.VERBOSE)


def to_float_if_num(value) -> Any:
    """
    Convert the incoming value to float when it looks numeric,
    otherwise return it unchanged.
    """
    if isinstance(value, str) and _num.match(value):
        # strip commas and trailing percent, then cast
        clean = value.replace(",", "").rstrip("%").strip()
        # empty string after stripping? -> leave unchanged
        if clean:
            try:
                return float(clean)
            except ValueError:
                pass  # fall through to return original
    return value


def retrieve_from_nba_api(timestamp: str, season_type: str, stat_type: str, player_name: str = None) -> None:
    """
    Modified to optionally filter for a specific player and save to a unique filename.
    """
    url = f"https://www.nba.com/stats/players/{stat_type}"
    date_range = utils.get_date_range(timestamp, season_type)
    if not date_range or len(date_range) != 2:
        print(f"No valid date range for {timestamp!r} / {season_type!r}. Skipping.")
        return

    start_date, end_date = date_range

    if datetime.strptime(start_date, "%Y-%m-%d") > datetime.strptime(end_date, "%Y-%m-%d"):
        print(f"Invalid date range: {start_date} > {end_date}. Skipping.")
        return

    nba_api_season = convert_to_nba_api_season(season_type)
    if nba_api_season == 'Unknown':
        print(f"Unknown season type: {season_type}")
        return

    season_str = utils.get_season(timestamp)
    start_date = utils.reformat_date(start_date)
    end_date = utils.reformat_date(end_date)

    params = {
        "Season": season_str,
        "PlayerOrTeam": "Player",
        "DateFrom": start_date,
        "DateTo": end_date,
        # "MeasureType": "SpeedDistance",
        "SeasonType": nba_api_season,
        "PerMode": "Totals"
    }

    # Create filename based on player name if provided
    if player_name:
        # Sanitize player name for filename
        safe_player_name = re.sub(r'[^\w\s-]', '', player_name).strip().replace(' ', '_')
        output_file = f"nba_api_{stat_type}_{timestamp}_{safe_player_name}.json"
    else:
        output_file = f"nba_api_{stat_type}_{timestamp}.json"

    # Determine filter folder from timestamp (year-based)
    year = timestamp[:4]
    filter_folder = f"filter-{year}"

    # Create directory structure: nba_api/season_type/filter_folder/file
    output_dir = ROOT_DIR / season_type / filter_folder
    output_path = output_dir / output_file

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        print(f"{output_path} already exists – skipping.")
        return

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/530.36")

    tmp_profile = tempfile.mkdtemp(prefix="chrome-scrape-")
    chrome_options.add_argument(f"--user-data-dir={tmp_profile}")

    print(f"Starting Selenium for {player_name or 'all players'}, {stat_type}, {timestamp}...")

    driver = webdriver.Chrome(options=chrome_options)
    final_url = f"{url}?{urlencode(params)}"

    try:
        print("Getting page...")
        print(f"final url? {final_url}")

        driver.get(final_url)
        time.sleep(5)

        wait = WebDriverWait(driver, 30)

        settings_div = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR,
                                            "div.Crom_cromSettings__ak6Hd"))
        )
        page_select = settings_div.find_element(By.CSS_SELECTOR,
                                                "select.DropDown_select__4pIg9")
        Select(page_select).select_by_index(0)

        time.sleep(5)  # naive approach

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'Crom_table__')]"))
        )
        table = driver.find_element(By.XPATH, "//table[contains(@class, 'Crom_table')]")

        raw_headers = [th.text.strip() for th in table.find_elements(By.CSS_SELECTOR, "thead tr th")]
        # the first header is blank/"#"; insert PLAYER after it
        if raw_headers[0] in ("", "#"):
            raw_headers[0] = "RANK"  # or "#" if you want the rank
            raw_headers.insert(1, "PLAYER")  # now PLAYER is in the list
        headers = raw_headers

        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

        data = {}
        for row in rows:
            cells = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]
            record = dict(zip(headers, cells))
            record = {k: to_float_if_num(v) for k, v in record.items()}
            player = record["PLAYER"]  # use the PLAYER field

            # If we're looking for a specific player, only add them
            if player_name:
                # Check if this is the player we're looking for
                # Handle cases where player name might have team abbreviation
                if player_name in player or player in player_name:
                    data[player] = record
                    break  # Found the player, no need to continue
            else:
                data[player] = record  # key the dict by name

        if player_name and not data:
            print(f"Warning: Player {player_name} not found in the data")

        write_to_file(data, output_path)
        print(f"Successfully saved {len(data)} players to {output_path}")

    except TimeoutException:
        print("Timeout! Table element could not be located.")
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

        driver.save_screenshot("debug_screen.png")
        return
    except requests.exceptions.RequestException as e:
        print(f"Failed to write {output_path}: {e}")
        return
    except requests.exceptions.ReadTimeout as e:
        print(f"Failed to write {output_path} due to timeout: {e}")
        return
    except NoSuchElementException as e:
        print(f"Could not find element: {e}")
        return
    except Exception as e:
        print(f"Unknown exception: {e}")
        traceback.print_exc()
        return
    finally:
        driver.quit()
        shutil.rmtree(tmp_profile, ignore_errors=True)


def process_csv_file(csv_path: str, season_types: list = None):
    """
    Process a CSV file with columns: timestamp, standard_name, data_type

    Args:
        csv_path: Path to the CSV file
        season_types: List of season types to process (default: ['Regular season', 'Playoffs'])
    """
    if season_types is None:
        season_types = ['Regular season', 'Playoffs']

    # Read the CSV file
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        # Group by timestamp and data_type for efficiency
        requests_to_process = {}

        for row in reader:
            timestamp = row['timestamp'].strip()
            player_name = row['standard_name'].strip()
            data_type = row['data_type'].strip()

            # Create a key for grouping
            key = (timestamp, data_type)

            if key not in requests_to_process:
                requests_to_process[key] = []

            requests_to_process[key].append(player_name)

    # Process each unique combination
    total_requests = len(requests_to_process)
    current = 0

    for (timestamp, data_type), players in requests_to_process.items():
        current += 1
        print(f"\n[{current}/{total_requests}] Processing timestamp: {timestamp}, data_type: {data_type}")
        print(f"  Players to fetch: {', '.join(players)}")

        # Process for each season type
        for season_type in season_types:
            print(f"  Season type: {season_type}")

            # Since we want data for specific players, we might want to fetch all data once
            # and then filter, or fetch individually. For efficiency, let's fetch once per
            # timestamp/data_type/season_type combination

            # Fetch the data once for all players at this timestamp
            try:
                # We'll modify the function to return data instead of just saving
                # For now, let's call it once without a player filter
                retrieve_from_nba_api(timestamp, season_type, data_type)

                # Note: If you want individual files per player, uncomment below:
                # for player_name in players:
                #     retrieve_from_nba_api(timestamp, season_type, data_type, player_name)

            except Exception as e:
                print(f"    Error processing: {e}")
                continue


def main(csv_file = None) -> None:
    """
    Main function that can either process from CSV or use the original folder-based approach
    """


    # Check if a CSV file was provided as command-line argument
    if csv_file:
        if Path(csv_file).exists():
            print(f"Processing from CSV file: {csv_file}")
            process_csv_file(csv_file)
        else:
            print(f"CSV file not found: {csv_file}")
            return
    else:
        # Fall back to the original behavior
        print("No CSV file provided. Using original folder-based processing...")

        stat_types = [
            "drives",
            "defensive-impact",
            "catch-shoot",
            "passing",
            "touches",
            "pullup",
            "rebounding",
            "offensive-rebounding",
            "defensive-rebounding",
            "shooting-efficiency",
            "speed-distance",
            "elbow-touch",
            "tracking-post-ups",
            "paint-touch"
        ]

        # Check if source directory exists
        if not SOURCE_DIR.exists():
            print(f"Source directory {SOURCE_DIR} does not exist!")
            return

        # Iterate through season type folders in the 538 directory
        for season_type_folder in SOURCE_DIR.iterdir():
            if not season_type_folder.is_dir():
                continue

            season_type = season_type_folder.name
            if season_type not in SUBFOLDERS:
                continue

            print(f"Processing season type: {season_type}")

            # Skip Full season to avoid duplication (as mentioned in original script)
            if season_type == 'Full season':
                print(f"Skipping {season_type} to avoid duplication")
                continue

            # Iterate through filter folders (filter-2014, filter-2015, etc.)
            for filter_folder in season_type_folder.iterdir():
                if not filter_folder.is_dir() or not filter_folder.name.startswith('filter-'):
                    continue
                if filter_folder.name not in HISTORICAL_YEARS:
                    continue

                filter_name = filter_folder.name
                print(f"  Processing filter: {filter_name}")

                # Look for timestamp files in this filter folder
                for item in filter_folder.iterdir():
                    if item.is_file() and item.stem.isnumeric():
                        timestamp = item.stem

                        if filter_folder in historical_checkbox_ids:
                            old_timestamp = timestamp
                            timestamp = get_final_timestamp_for_season(filter_folder)
                            print(f"Changed {old_timestamp} to {timestamp}")
                        else:
                            print(f"We're keeping {timestamp} as it is!")

                        print(f"    Processing timestamp: {timestamp}")

                        # Process each stat type for this timestamp
                        for stat_type in stat_types:
                            try:
                                # FIX: Don't pass filter_name as player_name
                                # Just call with the 3 required parameters
                                retrieve_from_nba_api(timestamp, season_type, stat_type)
                            except Exception as e:
                                print(f"    Error processing {stat_type} for {timestamp}: {e}")
                                continue

    print("Finished processing all NBA API data!")


if __name__ == "__main__":
    # main("missing_nba-tracking.csv")
    main()