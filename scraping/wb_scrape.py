import os
import csv
import json
import asyncio
import requests

from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

headers = ['data_key', 'id', 'row_num', 'name', 'team', 'pos', 'mp',
           'rap_box_o', 'rap_box_d', 'rap_box', 'rap_onoff_o', 'rap_onoff_d',
           'rap_onoff', 'rap_o', 'rap_d', 'rap', 'war']

target_url = "https://projects.fivethirtyeight.com/nba-player-ratings/"

# Write data to the CSV
def save_data(timestamp, player_data, dir):

  os.makedirs(dir, exist_ok = True)
  output_file = f"{timestamp}-bs4.csv"
  output_path = os.path.join(dir, output_file)
  if os.path.exists(output_path):
    print("File exists!")
    return

  with open(output_path, mode='w', newline='', encoding='utf-8') as file:
      writer = csv.writer(file)

      print(player_data)
      # Write the header (keys from the first dictionary in player_data)
      if player_data:  # Check if the list is not empty
          writer.writerow(headers)

          # Write each player's data (values)
          for player in player_data:
              writer.writerow(player)

  print(timestamp, " saved!")
  return

async def scrape(url, timestamp, season):

  driver = await drivers()
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
  wait = WebDriverWait(driver, 5)
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

  soup = BeautifulSoup(driver.page_source, 'html.parser')
  driver.quit()
  del driver

  parse_html(soup, timestamp, season)

def parse_html(html, timestamp, season):

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

    dir = season
    save_data(timestamp, player_data, season)

# update this to also accept a list of snapshots dates
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

            snapshots_dict[timestamp] = archived_url

        return snapshots_dict

    except requests.exceptions.RequestException as e:
        print(f"Error fetching snapshots: {e}")
        return []

async def scrape_setup(timestamp, url):
    failed_timestamps = []
    tasks = []
    try:
        for season in ["Full season", "Regular season", "Playoffs"]:
            tasks.append(await asyncio.create_task(scrape(url, timestamp, season)))
    except Exception as e:
        print(f"error getting data for timestamp {timestamp}")
        print(e)
        failed_timestamps.append(timestamp)

async def drivers():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)
    return driver

async def main(target_url):
    # snapshots = fetch_wayback_snapshots(target_url)
    snapshots = {'20250306182230': 'https://web.archive.org/web/20250306182230/https://projects.fivethirtyeight.com/nba-player-ratings/'}

    # Print a few sample entries
    for timestamp, url in snapshots.items():
        await scrape_setup(timestamp, url)
    # print("Failed timestamps", failed_timestamps)

if __name__ == "__main__":
    asyncio.run(main(target_url))
