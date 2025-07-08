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


ROOT_DIR = Path("latest_data")        # ← change to your real path
SUBFOLDERS = ["Playoffs", "Regular season", "Full season"]

# Increase the connection and read timeouts (in seconds)
os.environ['NBA_API_CONNECTION_TIMEOUT'] = '60'
os.environ['NBA_API_READ_TIMEOUT'] = '60'


def get_number_or_zero(value):
  try:
    return float(value)
  except:
    return 0




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

def to_float_if_num(value: str | Any) -> Any:
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
                pass            # fall through to return original
    return value

def retrieve_from_nba_api(timestamp: str, season_type: str, stat_type: str) -> None:
  url = f"https://www.nba.com/stats/players/{stat_type}"
  date_range = utils.get_date_range(timestamp, season_type)
  if not date_range or len(date_range) != 2:
    print(f"⚠️  No valid date range for {timestamp!r} / {season_type!r}.  Skipping.")
    return

  start_date, end_date = date_range

  if datetime.strptime(start_date, "%Y-%m-%d") > datetime.strptime(end_date, "%Y-%m-%d"):
    print(f"⚠️  Invalid date range: {start_date} > {end_date}. Skipping.")
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

  output_file = f"nba_api_{stat_type}_{timestamp}.json"
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

  try:
    print("Getting page...")

    print(f"final url? {final_url}")

    driver.get(final_url)
    time.sleep(5)

    wait = WebDriverWait(driver, 10)

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
    # the first header is blank/“#”; insert PLAYER after it
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
      player = record["PLAYER"]  # ➋ use the PLAYER field
      data[player] = record  # ➌ key the dict by name

    write_to_file(data, output_path)

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

    for item in folder.iterdir():
      if item.is_file() and item.stem.isnumeric():
        for stat_type in stat_types:
          retrieve_from_nba_api(item.stem, sub, stat_type)


if __name__ == "__main__":
  main()
