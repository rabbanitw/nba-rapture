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
from typing import Any, Optional
import re
import tempfile
import shutil
import traceback
from data import nba_player_ids, historical_checkbox_ids, get_final_timestamp_for_season
import csv
import sys
import atexit

ROOT_DIR = Path("nba_api")
SOURCE_DIR = Path("538")
SUBFOLDERS = ["Playoffs", "Regular season", "Full season"]
HISTORICAL_YEARS = ['filter-2014', 'filter-2015', 'filter-2016', 'filter-2017', 'filter-2018', 'filter-2019']

os.environ['NBA_API_CONNECTION_TIMEOUT'] = '60'
os.environ['NBA_API_READ_TIMEOUT'] = '60'
Path.mkdir(ROOT_DIR, exist_ok=True)

# Global browser instance
_global_driver = None
_tmp_profile = None


def _cleanup_driver():
    """Clean up the global driver on exit"""
    global _global_driver, _tmp_profile
    if _global_driver:
        try:
            _global_driver.quit()
        except:
            pass
        _global_driver = None
    if _tmp_profile and os.path.exists(_tmp_profile):
        try:
            shutil.rmtree(_tmp_profile, ignore_errors=True)
        except:
            pass
        _tmp_profile = None


# Register cleanup function
atexit.register(_cleanup_driver)


def _get_or_create_driver():
    """Get existing driver or create a new one"""
    global _global_driver, _tmp_profile

    if _global_driver is not None:
        try:
            # Test if driver is still alive
            _ = _global_driver.current_url
            return _global_driver
        except:
            # Driver is dead, clean it up
            _cleanup_driver()

    # Create new driver
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-logging')
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/530.36")

    _tmp_profile = tempfile.mkdtemp(prefix="chrome-scrape-")
    chrome_options.add_argument(f"--user-data-dir={_tmp_profile}")

    _global_driver = webdriver.Chrome(options=chrome_options)
    return _global_driver


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
        'Full season': 'Full Season'
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
    """Convert the incoming value to float when it looks numeric"""
    if isinstance(value, str) and _num.match(value):
        clean = value.replace(",", "").rstrip("%").strip()
        if clean:
            try:
                return float(clean)
            except ValueError:
                pass
    return value


def retrieve_from_nba_api(timestamp: str, season_type: str, stat_type: str, player_name: str = None,
                          custom_root_dir: str = None) -> None:
    """
    Modified to reuse a single Chrome instance for all requests.
    """
    url = f"https://www.nba.com/stats/players/{stat_type}"
    date_range = utils.get_date_range_extended(timestamp, season_type)
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
        "SeasonType": nba_api_season,
        "PerMode": "Totals"
    }

    if custom_root_dir:
        base_dir = Path(custom_root_dir)
    else:
        base_dir = ROOT_DIR

    if player_name:
        safe_player_name = re.sub(r'[^\w\s-]', '', player_name).strip().replace(' ', '_')
        output_dir = base_dir / season_type / timestamp / player_name / "tracking"
        output_file = f"{stat_type}.csv"
    else:
        output_file = f"nba_api_{stat_type}_{timestamp}.json"
        year = timestamp[:4]
        filter_folder = f"filter-{year}"
        output_dir = base_dir / season_type / filter_folder

    output_path = output_dir / output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        print(f"{output_path} already exists – skipping.")
        return

    print(f"Fetching {player_name or 'all players'}, {stat_type}, {timestamp}...")

    # Use shared driver instead of creating a new one
    driver = _get_or_create_driver()
    final_url = f"{url}?{urlencode(params)}"

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            driver.get(final_url)
            time.sleep(5)

            wait = WebDriverWait(driver, 30)

            settings_div = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.Crom_cromSettings__ak6Hd"))
            )
            page_select = settings_div.find_element(By.CSS_SELECTOR, "select.DropDown_select__4pIg9")
            Select(page_select).select_by_index(0)

            time.sleep(5)

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'Crom_table__')]"))
            )

            # Wait for table to stabilize
            time.sleep(3)

            # Extract headers
            table = driver.find_element(By.XPATH, "//table[contains(@class, 'Crom_table')]")
            raw_headers = [th.text.strip() for th in table.find_elements(By.CSS_SELECTOR, "thead tr th")]
            if raw_headers[0] in ("", "#"):
                raw_headers[0] = "RANK"
                raw_headers.insert(1, "PLAYER")
            headers = raw_headers

            # NEW APPROACH: Use JavaScript to extract all data at once
            # This avoids stale element issues by getting everything in one shot
            script = """
            const table = document.querySelector('table.Crom_table__p1iZz, table[class*="Crom_table"]');
            if (!table) return [];

            const rows = table.querySelectorAll('tbody tr');
            const data = [];

            rows.forEach(row => {
                const cells = Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim());
                if (cells.length > 0) {
                    data.push(cells);
                }
            });

            return data;
            """

            all_rows_data = driver.execute_script(script)

            if not all_rows_data:
                print("Warning: No data extracted from table")
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"Failed after {max_retries} retries")
                    return
                time.sleep(2)
                continue

            # Process the extracted data
            data = {}
            for cells in all_rows_data:
                if len(cells) != len(headers):
                    # Handle header mismatch
                    continue

                record = dict(zip(headers, cells))
                record = {k: to_float_if_num(v) for k, v in record.items()}

                if "PLAYER" not in record:
                    continue

                player = record["PLAYER"]

                if not player:  # Skip empty player names
                    continue

                if player_name:
                    if player_name in player or player in player_name:
                        data[player] = record
                        break
                else:
                    data[player] = record

            if player_name and not data:
                print(f"Warning: Player {player_name} not found in the data")

            if player_name:
                if data:
                    player_data = list(data.values())[0]
                    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=player_data.keys())
                        writer.writeheader()
                        writer.writerow(player_data)
                    print(f"Successfully saved player data to {output_path}")
                else:
                    print(f"No data found for player {player_name}")
            else:
                write_to_file(data, output_path)
                print(f"Successfully saved {len(data)} players to {output_path}")

            # Success - break out of retry loop
            break

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
            retry_count += 1
            if retry_count >= max_retries:
                print(f"Failed after {max_retries} retries")
                return
            print(f"Retrying... ({retry_count}/{max_retries})")
            time.sleep(2)
        except Exception as e:
            print(f"Unknown exception: {e}")
            traceback.print_exc()
            retry_count += 1
            if retry_count >= max_retries:
                print(f"Failed after {max_retries} retries")
                return
            print(f"Retrying... ({retry_count}/{max_retries})")
            time.sleep(2)


def process_csv_file(csv_path: str, season_types: list = None):
    """Process a CSV file with columns: timestamp, standard_name, data_type"""
    if season_types is None:
        season_types = ['Regular season', 'Playoffs']

    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        requests_to_process = {}

        for row in reader:
            timestamp = row['timestamp'].strip()
            player_name = row['standard_name'].strip()
            data_type = row['data_type'].strip()
            key = (timestamp, data_type)

            if key not in requests_to_process:
                requests_to_process[key] = []
            requests_to_process[key].append(player_name)

    total_requests = len(requests_to_process)
    current = 0

    for (timestamp, data_type), players in requests_to_process.items():
        current += 1
        print(f"\n[{current}/{total_requests}] Processing timestamp: {timestamp}, data_type: {data_type}")
        print(f"  Players to fetch: {', '.join(players)}")

        for season_type in season_types:
            print(f"  Season type: {season_type}")
            try:
                retrieve_from_nba_api(timestamp, season_type, data_type)
            except Exception as e:
                print(f"    Error processing: {e}")
                continue


def main(csv_file=None) -> None:
    """Main function that can either process from CSV or use the original folder-based approach"""
    try:
        if csv_file:
            if Path(csv_file).exists():
                print(f"Processing from CSV file: {csv_file}")
                process_csv_file(csv_file)
            else:
                print(f"CSV file not found: {csv_file}")
                return
        else:
            print("No CSV file provided. Using original folder-based processing...")

            stat_types = [
                "drives", "defensive-impact", "catch-shoot", "passing", "touches",
                "pullup", "rebounding", "offensive-rebounding", "defensive-rebounding",
                "shooting-efficiency", "speed-distance", "elbow-touch", "tracking-post-ups",
                "paint-touch"
            ]

            if not SOURCE_DIR.exists():
                print(f"Source directory {SOURCE_DIR} does not exist!")
                return

            for season_type_folder in SOURCE_DIR.iterdir():
                if not season_type_folder.is_dir():
                    continue

                season_type = season_type_folder.name
                if season_type not in SUBFOLDERS:
                    continue

                print(f"Processing season type: {season_type}")

                if season_type == 'Full season':
                    print(f"Skipping {season_type} to avoid duplication")
                    continue

                for filter_folder in season_type_folder.iterdir():
                    if not filter_folder.is_dir() or not filter_folder.name.startswith('filter-'):
                        continue
                    if filter_folder.name not in HISTORICAL_YEARS:
                        continue

                    filter_name = filter_folder.name
                    print(f"  Processing filter: {filter_name}")

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

                            for stat_type in stat_types:
                                try:
                                    retrieve_from_nba_api(timestamp, season_type, stat_type)
                                except Exception as e:
                                    print(f"    Error processing {stat_type} for {timestamp}: {e}")
                                    continue

        print("Finished processing all NBA API data!")
    finally:
        # Clean up driver when done
        _cleanup_driver()


if __name__ == "__main__":
    # main("missing_nba-tracking.csv")
    main()