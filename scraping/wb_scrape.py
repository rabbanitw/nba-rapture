import os
import csv
import json
import time
# import utils
import asyncio
import requests
import argparse
import concurrent.futures

from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# list of missing timestamps in another file
from missing_timestamps import *

target_url = "https://projects.fivethirtyeight.com/nba-player-ratings/"
headers = ['data_key', 'id', 'row_num', 'name', 'team', 'pos', 'mp',
           'rap_box_o', 'rap_box_d', 'rap_box', 'rap_onoff_o', 'rap_onoff_d',
           'rap_onoff', 'rap_o', 'rap_d', 'rap', 'war']

# Write data to the CSV
def save_data(timestamp, player_data, season):

  print(f"Saving data")

  os.makedirs(season, exist_ok = True)
  output_file = f"{timestamp}-{season}.csv"
  output_path = os.path.join(season, output_file)

  with open(output_path, mode='w', newline='', encoding='utf-8') as file:
      writer = csv.writer(file)

      # Write the header (keys from the first dictionary in player_data)
      if player_data:  # Check if the list is not empty
          writer.writerow(headers)

          # Write each player's data (values)
          for player in player_data:
              writer.writerow(player)

  print(f"{timestamp} saved!")
  return

def scrape(input_parameters):
    timestamp = input_parameters[0]
    season = input_parameters[1]

    """
    Selenium driver actions to retrieve the necessary data/tables
    """
    url = f"https://web.archive.org/web/{timestamp}/{target_url}"

    print(f"Scraping {timestamp} for {season}: {url}")

    driver = drivers()
    sel_start = time.time()
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

  # change the dropdown to season
    dropdown_element = WebDriverWait(driver, 5).until(
      EC.presence_of_element_located((By.ID, "filter-season-type"))
    )

  # Create a Selenium Select object based on the dropdown
    select_dropdown = Select(dropdown_element)

  # Select by value
    match season:
        case "Regular season":
            shorthand = "RS"
        case "Full season":
            shorthand = "TOT"
        case "Playoffs":
            shorthand = "PO"
    select_dropdown.select_by_value(shorthand)
    print(f"Selected option: {select_dropdown.first_selected_option.text}")

    sel_end = time.time()
    sel_elapsed = sel_end - sel_start
    print(f"Selenium took {sel_elapsed:.3f} seconds")
    print(f"Soupin' now")
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()
    del driver
    print(f"Scraping {timestamp} complete!")
    parse_html(soup, season, timestamp)

def parse_html(html, season, timestamp):
    """
    Use beautifulsoup to parse HTML from Selenium driver
    """

    print(f"Parsin'")

    table = html.find('table')
    table_body = table.find('tbody')
    rows = table_body.find_all('tr')
    player_data = []

    for row in rows:
          cols = row.find_all('td')
          cols = [ele.text.strip() for ele in cols]
          cols.insert(0, row.attrs.get('data-key'))
          cols.insert(1, row.attrs.get('id'))

          # Add the player data to the list
          player_data.append(cols)

    parse_end = time.time()

    save_data(timestamp, player_data, season)

def fetch_wayback_snapshots(url):
    """
    Fetch a list of snapshots for the given URL using
    the Wayback Machine CDX server API. Returns a list
    of dictionaries with timestamp and the complete
    archived URL.
    """
    # CDX API endpoint with JSON output
    cdx_url = (
        "https://web.archive.org/cdx/search/cdx"
        "?url={url}&output=json"
        "&fl=timestamp,original,statuscode"
        "&filter=statuscode:200"
    ).format(url=url)

    print("Fetching URLs")

    try:
        response = requests.get(cdx_url)
        response.raise_for_status()
        data = response.json()

        # The first item in `data` is a list of headers (e.g., ["timestamp", "original", "statuscode"])
        if len(data) < 2:
            print("No snapshots found or no data returned.")
            return []

        headers = data[0]       # e.g. ["timestamp", "original", "statuscode"]
        snapshot_rows = data[1:]
        snapshots_dict = {}

        for row in snapshot_rows:
            snapshot_info = dict(zip(headers, row))
            timestamp = snapshot_info["timestamp"]
            original_url = snapshot_info["original"]

            # Construct the Wayback Machine archived URL
            archived_url = f"https://web.archive.org/web/{timestamp}/{original_url}"

            # ignore any timestamps after 20230621145310 since EOL
            if timestamp < '202306211453110':
                snapshots_dict[timestamp] = archived_url

        return snapshots_dict

    except requests.exceptions.RequestException as e:
        print(f"Error fetching snapshots: {e}")
        return []

def drivers():
    """
    Initializes Selenium webdriver
    remote connects to a Selenium Grid instance running locally via Docker
    this allows multiple drivers to be spun up and run simultaneously

    otherwise a driver would have to be run one by one
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless=new")
    options.add_experimental_option(
        "prefs", {
            # block image loading
            "profile.managed_default_content_settings.images": 2,
        }
    )
    driver = webdriver.Remote(
            command_executor="http://localhost:4444", options=options
            )

    return driver

def get_processed_files(directory):
    """
    Get a list of files in a directory and returns them as a list of timestamps for processing
    """
    generated_timestamps = []

    for file in directory:
        generated_timestamps.append(Path(file).stem)
    return generated_timestamps


def get_empty_files(directory):
    """
    Check a given directory for empty files

    Returns them as a list of timestamps for processing
    """
    timestamps = [Path(file).stem for file in directory
            if os.path.getsize(f"./scraping/all_files/Playoffs/{file}") == 0]
    return timestamps

def main(seasons):
    failed_timestamps = []

    snapshots = fetch_wayback_snapshots(target_url)

    try:
        map_args = []
        for snapshot in snapshots:
            # either or all of ["Full season", "Regular season", "Playoffs"]:
            for season in seasons:
                output_path = os.path.join(season, f"{snapshot}-{season}.csv")
                if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                    print(f"{output_path} already exists!")
                else:
                    map_args.append([snapshot, season])

        # submitting all the jobs together regardless of season
        # each will spin up a separate instance of selenium and go through the flow
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results_futures = list(map(lambda x: executor.submit(scrape, x), map_args))
            results = [f.result() for f in concurrent.futures.as_completed(results_futures)]


    except Exception as e:
        print(e)

    print("Failed timestamps", failed_timestamps)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--fetch', help='fetch available timestamps, otherwise provide necessary timestamps')
    parser.add_argument('-g', '--grid', help='use selenium grid for parallelism')
    parser.add_argument('-s', '--seasons', help='choose between full, regular, playoffs, or all', nargs="+", choices=['Full season', 'Regular season', 'Playoffs', 'all'])
    parser.add_argument('-o', '--overwrite', help='overwrite any processed files')
    args = parser.parse_args()

    main(args.seasons)
