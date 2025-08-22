import tempfile, uuid, shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
import os
import requests
from time import sleep
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    StaleElementReferenceException,
)
import csv
from pathlib import Path
from selenium.webdriver.common.action_chains import ActionChains
import time



from scraping.data_saver import timestamp_already_processed

# Specify the output CSV file
output_file = "player_data.csv"


def new_driver() -> webdriver.Chrome:
    """Return a fresh, headless Chrome with its own throw‑away profile."""
    opts = Options()
    opts.add_argument("--headless=new")  # headless Chrome ≥115
    opts.add_argument("--no-sandbox")  # good practice on EC2
    opts.add_argument("--disable-dev-shm-usage")  # avoid /dev/shm issues

    # each run gets a unique profile so the 'user‑data‑dir already in use'
    # lock files can never collide
    profile_dir = tempfile.mkdtemp(prefix="chrome-")
    opts.add_argument(f"--user-data-dir={profile_dir}")

    driver = webdriver.Chrome(options=opts)
    driver._profile_dir = profile_dir  # stash so we can delete it
    return driver


def close_driver(driver: webdriver.Chrome):
    """Quit Chrome and clean up the temporary profile directory."""
    try:
        driver.quit()
    finally:
        shutil.rmtree(getattr(driver, "_profile_dir", ""), ignore_errors=True)


# Write data to the CSV
def save_data(timestamp, player_data, season, checkbox_id):

    base_dir = Path(__file__).resolve().parent
    dir_538 = base_dir / "538"
    new_dir = dir_538 / season / checkbox_id
    new_dir.mkdir(parents=True, exist_ok=True)
    new_path = os.path.join(new_dir, f"{timestamp}.csv")

    if os.path.exists(new_path):
        print(f"File {new_path} exists!")
        return

    with open(new_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write the header (keys from the first dictionary in player_data)
        if player_data:  # Check if the list is not empty
            header = player_data[0].keys()
            writer.writerow(header)

            # Write each player's data (values)
            for player in player_data:
                writer.writerow(player.values())
    print(f"Timestamp {timestamp} and season {season} saved!")


def unselect_all_checkboxes(driver):
    js_unselect_all = """
    var checkboxes = document.querySelectorAll('.year-checkbox');
    checkboxes.forEach(function(checkbox) {
        if (checkbox.checked) {
            checkbox.checked = false;
            checkbox.dispatchEvent(new Event('change', { bubbles: true }));
            checkbox.dispatchEvent(new Event('click', { bubbles: true }));
        }
    });
    """
    driver.execute_script(js_unselect_all)
    print("Unselected all checkboxes")


# Function to select a specific checkbox by ID
def select_checkbox_by_id(driver, checkbox_id):
    js_select = f"""
    var checkbox = document.getElementById('{checkbox_id}');
    if (checkbox && !checkbox.checked) {{
        checkbox.checked = true;
        checkbox.dispatchEvent(new Event('change', {{ bubbles: true }}));
        checkbox.dispatchEvent(new Event('click', {{ bubbles: true }}));
    }}
    """
    driver.execute_script(js_select)
    print(f"Selected checkbox: {checkbox_id}")


def fetch_and_save_data(driver):
    player_data = []
    rows = driver.find_elements(By.XPATH, "//tr[@data-key]")

    upper = ['row_num', 'name', 'team', 'pos', 'mp', 'rap_box_o', 'rap_box_d', 'rap_box', 'rap_onoff_o', 'rap_onoff_d',
             'rap_onoff', 'rap_o', 'rap_d', 'rap', 'war']
    # Loop through each row and gather all data
    for row in rows:
        player = {}
        # Extract key attributes
        player['data_key'] = row.get_attribute('data-key')
        player['id'] = row.get_attribute('id')

        # Extract all columns, even if empty
        columns_td = row.find_elements(By.TAG_NAME, 'td')
        for idx, col in enumerate(columns_td):
            text = col.text.strip()  # Extract text content
            if not text:  # Handle empty cells
                text = col.get_attribute('data-val') or ''  # Try to fetch 'data-val' if available
            player[upper[idx]] = text

        # Add the player data to the list
        player_data.append(player)
    return player_data


def scrape(driver, url, timestamp, season, checkbox_id):
    print(f"url: {url}")
    driver.get(url)
    slider = driver.find_element(By.ID, 'filter-slider')

    # Use JavaScript to set the slider value and dispatch input and change events
    desired_value = 1
    driver.execute_script("""
      var slider = arguments[0];
      slider.value = arguments[1];
      slider.dispatchEvent(new Event('input'));
      slider.dispatchEvent(new Event('change'));
  """, slider, desired_value)

    # change the dropdown to Regular season
    wait = WebDriverWait(driver, 10)
    dropdown_element = wait.until(
        EC.presence_of_element_located((By.ID, "filter-season-type"))
    )

    # Create a Selenium Select object based on the dropdown
    select_dropdown = Select(dropdown_element)

    # Select by visible text
    select_dropdown.select_by_visible_text(season)

    # (Optional) Print out the newly selected option
    selected_option = select_dropdown.first_selected_option.text
    print("Selected option:", selected_option)

    # Wait for the checkboxes to be present
    wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "year-checkbox")))

    # Array of checkbox IDs to iterate through
    print(f"\n--- Processing {checkbox_id} ---")

    # Step 1: Unselect all checkboxes to ensure only one is selected
    unselect_all_checkboxes(driver)

    # Small delay to let the page process
    time.sleep(1)

    # Step 2: Select the current checkbox
    select_checkbox_by_id(driver, checkbox_id)

    # Small delay to let the page process the selection
    time.sleep(4)

    # Step 3: Call download()
    player_data = fetch_and_save_data(driver)
    save_data(timestamp, player_data, season, checkbox_id)


def fetch_wayback_snapshots(url):
    """
    Fetch a list of snapshots for the given URL using
    the Wayback Machine CDX server API. Returns a list
    of dictionaries with timestamp and the complete
    archived URL.
    """
    cdx_url = (
        "https://web.archive.org/cdx/search/cdx"
        "?url={url}&output=json"
        "&fl=timestamp,original,statuscode"
        "&filter=statuscode:200"
    ).format(url=url)

    try:
        response = requests.get(cdx_url)
        response.raise_for_status()

        data = response.json()

        # The first item in `data` is a list of headers (e.g., ["timestamp", "original", "statuscode"])
        if len(data) < 2:
            print("No snapshots found or no data returned.")
            return []

        headers = data[0]  # e.g. ["timestamp", "original", "statuscode"]
        snapshot_rows = data[1:]

        snapshots_list = []

        print(snapshot_rows)

        for row in snapshot_rows:
            snapshot_info = dict(zip(headers, row))
            timestamp = snapshot_info["timestamp"]
            # if timestamp not in missing_timestamps:
            if timestamp_already_processed(timestamp, "538"):
                print(f"Skipping already processed timestamp: {timestamp}")
                continue
            else:
                print(f"Processing timestamp: {timestamp}")
            original_url = snapshot_info["original"]

            # Construct the Wayback Machine archived URL
            archived_url = f"https://web.archive.org/web/{timestamp}/{original_url}"

            # Add to our list
            snapshots_list.append({
                "timestamp": timestamp,
                "archived_url": archived_url
            })

        return snapshots_list

    except requests.exceptions.RequestException as e:
        print(f"Error fetching snapshots: {e}")
        return []


def main():
    target_url = "https://projects.fivethirtyeight.com/nba-player-ratings/"
    snapshots = fetch_wayback_snapshots(target_url)
    print(f"snapshots length: {len(snapshots)}")
    seen_timestamps = set()
    unique_snapshots = []

    for snapshot in snapshots:
        if snapshot["timestamp"] not in seen_timestamps:
            seen_timestamps.add(snapshot["timestamp"])
            unique_snapshots.append(snapshot)

    snapshots = unique_snapshots
    print(f"new snapshots length: {len(snapshots)}")

    failed = []

    base_dir = Path(__file__).resolve().parent
    dir_538 = base_dir / "538"

    for snap in snapshots:
        for season in ["Full season", "Regular season", "Playoffs"]:
            checkbox_ids = [
                # 'filter-2014',
                # 'filter-2015',
                # 'filter-2016',
                # 'filter-2017',
                # 'filter-2018',
                # 'filter-2019',
                # 'filter-2020',
                'filter-2021',
                'filter-2022'
            ]
            for checkbox_id in checkbox_ids:
                new_dir = dir_538 / season
                new_dir.mkdir(parents=True, exist_ok=True)
                new_dir = new_dir / checkbox_id
                new_dir.mkdir(parents=True, exist_ok=True)

                timestamp = snap['timestamp']
                outfile = f"{timestamp}.csv"
                file_path = os.path.join(new_dir, outfile)
                if os.path.exists(file_path):
                    print(f"Skipping already processed file_path: {file_path}")
                    continue

                driver = new_driver()  # <-- create
                try:
                    scrape(driver, snap['archived_url'], timestamp, season, checkbox_id)
                except Exception as e:
                    print(f"[{timestamp}] failed: {e}")
                    failed.append(timestamp)
                finally:
                    close_driver(driver)  # <-- destroy

    print("Failed timestamps:", failed)


if __name__ == "__main__":
    main()
