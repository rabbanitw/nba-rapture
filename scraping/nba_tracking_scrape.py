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

ROOT_DIR = Path("/home/ec2-user/nba-rapture/wb_scraping/all_files")        # ← change to your real path
SUBFOLDERS = ["Playoffs", "Regular season", "Full season"]

# Increase the connection and read timeouts (in seconds)
os.environ['NBA_API_CONNECTION_TIMEOUT'] = '60'
os.environ['NBA_API_READ_TIMEOUT'] = '60'


def get_number_or_zero(value):
  try:
    return float(value)
  except:
    return 0


def parse_line(line: str) -> (str, dict):
  """
  Parses a single line of data and returns (player_name, stats_dict).
  """
  tokens = line.split()
  # The last 12 tokens are known columns (TEAM, GP, W, L, etc.)
  if len(tokens) < 13:
    raise ValueError(f"Line doesn't have enough tokens: {line}")

  # Slice out the last 12 tokens
  data_cols = tokens[-12:]  # e.g. ["DEN", "10", "6", "4", "272", ...]
  # Everything else (start to -12) is the player's name
  name_tokens = tokens[:-12]  # e.g. ["Aaron", "Gordon"]
  player_name = " ".join(name_tokens)  # "Aaron Gordon"

  # Convert each of the 12 columns to either strings or numeric
  # For example: TEAM is a string, GP, W, L, MIN, DIST_FEET are ints, the rest are floats.
  team = data_cols[0]
  gp = int(data_cols[1])
  w = int(data_cols[2])
  l = int(data_cols[3])
  minutes = int(data_cols[4])
  dist_feet = int(get_number_or_zero(data_cols[5]))
  dist_miles = float(get_number_or_zero(data_cols[6]))
  dist_miles_off = float(get_number_or_zero(data_cols[7]))
  dist_miles_def = float(get_number_or_zero(data_cols[8]))
  avg_speed = float(get_number_or_zero(data_cols[9]))
  avg_speed_off = float(get_number_or_zero(data_cols[10]))
  avg_speed_def = float(get_number_or_zero(data_cols[11]))

  # Construct a dictionary for the stats
  stats_dict = {
    "TEAM": team,
    "GP": gp,
    "W": w,
    "L": l,
    "MIN": minutes,
    "DIST_FEET": dist_feet,
    "DIST_MILES": dist_miles,
    "DIST_MILES_OFF": dist_miles_off,
    "DIST_MILES_DEF": dist_miles_def,
    "AVG_SPEED": avg_speed,
    "AVG_SPEED_OFF": avg_speed_off,
    "AVG_SPEED_DEF": avg_speed_def
  }
  return player_name, stats_dict


def parse_all_lines(lines):
  """
  lines is a list of strings, e.g.
  [
    "PLAYER TEAM GP W L MIN DIST. FEET DIST. MILES DIST. MILES OFF ...",
    "Aaron Gordon DEN 10 6 4 272 105023 19.90 11.10 8.80 4.15 4.60 3.70",
    ...
  ]
  """
  # First line is the header; skip or verify
  header_line = lines[0]
  # e.g. "PLAYER TEAM GP W L MIN ..."

  # Initialize a dictionary keyed by player
  data_by_player = {}
  for line in lines[1:]:  # skip header line
    # skip empty lines
    if not line.strip():
      continue
    name, stats = parse_line(line)
    data_by_player[name] = stats

  return data_by_player



def write_to_file(data, output_path):
  try:
    print(f"writing to file: {output_path}")
    with open(output_path, 'w') as file:
      json.dump(data, file)
  except:
    print(f"Failed to write {output_path}")


def convert_to_nba_api_season(season_type_value):
  match season_type_value:
    case 'Regular season':
      return 'Regular Season'
    case 'Playoffs':
      return 'Playoffs'
    case 'Play in':
      return 'PlayIn'
    case _:
      return 'Unknown'


def retrieve_from_nba_api(timestamp: str, season_type: str) -> None:
  url = "https://www.nba.com/stats/players/speed-distance"
  date_range = utils.get_date_range(timestamp, season_type)
  if not date_range or len(date_range) != 2:
    print(f"⚠️  No valid date range for {timestamp!r} / {season_type!r}.  Skipping.")
    return

  start_date, end_date = date_range

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
    "MeasureType": "SpeedDistance",
    "SeasonType": nba_api_season,
    "PerMode": "Totals"
  }

  output_file = f"nba_api_{timestamp}.json"
  output_path = ROOT_DIR / Path(season_type) / output_file

  if output_path.exists():
    print(f"✅  {output_path} already exists – skipping.")
    return

  chrome_options = Options()
  chrome_options.add_argument('--headless')
  chrome_options.add_argument('--no-sandbox')
  chrome_options.add_argument('--disable-dev-shm-usage')
  chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

  print("Starting Selenium...")

  driver = webdriver.Chrome(options=chrome_options)
  final_url = f"{url}?{urlencode(params)}"

  all_data = {}

  try:
    print("Getting page...")

    print(f"final url? {final_url}")

    driver.get(final_url)
    time.sleep(5)

    wait = WebDriverWait(driver, 30)
    settings_div = driver.find_element(By.CSS_SELECTOR, "div.Crom_cromSettings__ak6Hd")
    page_select_dropdown = settings_div.find_element(By.CSS_SELECTOR, "select.DropDown_select__4pIg9")
    dropdown = Select(page_select_dropdown)
    dropdown.select_by_index(0)

    time.sleep(5)  # naive approach

    WebDriverWait(driver, 60).until(
      EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'Crom_table__')]"))
    )
    table2 = driver.find_element(By.XPATH, "//table[contains(@class, 'Crom_table')]")
    rows = table2.find_elements(By.CSS_SELECTOR, "tr")
    row_texts = [row.text for row in rows]
    data_dict = parse_all_lines(row_texts)
    all_data = all_data | data_dict

    write_to_file(all_data, output_path)

  except TimeoutException:
    print("Timeout! Could not find the table element. Trying a different locator...")
    try:
      # If the original XPath fails, try a more general one
      WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
      )
      print("Found the table using a different locator!")
    except TimeoutException:
      print("Timeout again! Table element could not be located.")
      driver.quit()  # Close the browser to avoid resource leaks
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
  finally:
    driver.quit()


def main() -> None:
  for sub in SUBFOLDERS:
    folder = ROOT_DIR / sub

    if not folder.is_dir():
      print(f"⚠️  Skipping missing folder: {folder}")
      continue

    for item in folder.iterdir():
      if item.is_file() and item.stem.isnumeric():
        retrieve_from_nba_api(item.stem, sub)


if __name__ == "__main__":
  main()
