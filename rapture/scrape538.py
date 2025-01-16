from selenium import webdriver
from selenium.webdriver.common.by import By
import os
import requests
import json
from time import sleep
#'https://web.archive.org/web/20210210150139/https://projects.fivethirtyeight.com/nba-player-ratings/'
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import csv

driver = webdriver.Chrome()

# Specify the output CSV file
output_file = "player_data.csv"

# Write data to the CSV
def save_data(timestamp, player_data, dir):
  output_file = timestamp + ".csv"
  output_path = os.path.join(dir,output_file)
  if os.path.exists(output_path):
    print("File exists!")
    return

  with open(output_path, mode='w', newline='', encoding='utf-8') as file:
      writer = csv.writer(file)

      # Write the header (keys from the first dictionary in player_data)
      if player_data:  # Check if the list is not empty
          header = player_data[0].keys()
          writer.writerow(header)

          # Write each player's data (values)
          for player in player_data:
              writer.writerow(player.values())
  print(timestamp, " saved!")
  return


def scrape(driver, url, timestamp, season):
  print(f"url: {url}")
  driver.get(url)
  slider = driver.find_element(By.ID, 'filter-slider')
  #slider = WebDriverWait(driver, 15).until(
  #      EC.visibility_of_all_elements_located((By.ID, 'filter-slider'))
  #  )

  # Use JavaScript to set the slider value and dispatch input and change events
  desired_value = 1
  driver.execute_script("""
      var slider = arguments[0];
      slider.value = arguments[1];
      slider.dispatchEvent(new Event('input'));
      slider.dispatchEvent(new Event('change'));
  """, slider, desired_value)


  # checkboxes = driver.find_elements(By.CLASS_NAME, "year-checkbox")
  # for checkbox in checkboxes:
  #   if not checkbox.is_selected():
  #       checkbox.click()
  # for checkbox in checkboxes:
  #   print(f"Checkbox for {checkbox.get_attribute('year')} is {'checked' if checkbox.is_selected() else 'unchecked'}.")

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



  # Wait for the table to update (use WebDriverWait if necessary for AJAX loads)
  #WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.XPATH, "//tr[@data-key]")))

  # Locate all rows in the table
  rows = driver.find_elements(By.XPATH, "//tr[@data-key]")

  upper = ['row_num', 'name', 'team', 'pos', 'mp','rap_box_o', 'rap_box_d', 'rap_box', 'rap_onoff_o', 'rap_onoff_d', 'rap_onoff', 'rap_o', 'rap_d', 'rap', 'war']
  # Initialize an empty list to store player data
  player_data = []

  ids = []

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

  dir = season
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

        snapshots_list = []

        for row in snapshot_rows:
            snapshot_info = dict(zip(headers, row))
            timestamp = snapshot_info["timestamp"]
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

if __name__ == "__main__":
    target_url = "https://projects.fivethirtyeight.com/nba-player-ratings/"
    snapshots = fetch_wayback_snapshots(target_url)
    failed_timestamps = []



    # Print a few sample entries
    for s in snapshots:  # Show only the first 10 for brevity
        for season in ["Full season", "Regular season", "Playoffs"]:
          output_file = f"{s['timestamp']}-{season}" + ".csv"
          if os.path.exists(output_file):
            print("File exists!")
            continue
          print(f"url: {s['archived_url']}")
          sleep(10)
          try:
              scrape(driver, s['archived_url'], s['timestamp'], season)
          except Exception as e:
              print(f"error getting data for timestamp {s['timestamp']}")
              print(e)
              failed_timestamps.append(s['timestamp'])
          print(s)
    print("Failed timestamps", failed_timestamps)

def main():

    target_url = "https://projects.fivethirtyeight.com/nba-player-ratings/"
    snapshots = fetch_wayback_snapshots(target_url)
    failed_timestamps = []

    # Print a few sample entries
    for s in snapshots:  # Show only the first 10 for brevity
        for season in ["Full season", "Regular season", "Playoffs"]:
          output_file = f"{s['timestamp']}.csv"
          if os.path.exists(os.path.join(season,output_file)):
            print("File exists!")
            continue
          print(f"url: {s['archived_url']}")
          sleep(10)
          try:
              scrape(driver, s['archived_url'], s['timestamp'], season)
          except Exception as e:
              print(f"error getting data for timestamp {s['timestamp']}")
              print(e)
              failed_timestamps.append(s['timestamp'])
          print(s)
    print("Failed timestamps", failed_timestamps)


if __name__ == "__main__":
    main()
