from datetime import datetime
import re
import asyncio, functools, os



_failed_log_lock = asyncio.Lock()          # one lock for the whole module

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

  match season:
    case '2020-21':
      if season_type == "Playoffs":
        if inside_range(timestamp,'2021-07-20'):
          return ['2021-05-22',regular_time(timestamp)]
      elif season_type == "Regular Season":
        if inside_range(timestamp,'2021-05-22'):
          return ['2020-12-22',regular_time(timestamp)]
      else:
        return ['2020-12-22',regular_time(timestamp)]
    case '2021-22':
      if season_type == "Playoffs":
        if inside_range(timestamp,'2022-06-16'):
          return ['2022-04-16',regular_time(timestamp)]
      elif season_type == "Regular season":
        if inside_range(timestamp, "2022-04-16"):
          return ["2021-10-19",regular_time(timestamp)]
      else:
        return ["2021-10-19",regular_time(timestamp)]
    case '2022-23':
      if season_type == "Playoffs":
        if inside_range(timestamp, "2023-06-12"):
          return ["2023-04-15",regular_time(timestamp)]
      elif season_type == "Regular season":
        if inside_range(timestamp,"2023-04-15"):
          return ['2022-10-18', regular_time(timestamp)]
      else:
          return ['2022-10-18', regular_time(timestamp)]
  raise ValueError(
      f"No date‑range rule for season={get_season(timestamp)} "
      f"season_type={season_type}"
  )

def regular_time(waystamp):

  #Wayback time format YYYYMMDDhhmmss
  date_object = datetime.strptime(waystamp, "%Y%m%d%H%M%S")
  convert_date = date_object.strftime("%Y-%m-%d")
  return convert_date

def wayback_time(date):

  #PBP date format
  date_object = datetime.strptime(date, "%Y-%m-%d")

  #Turn into wayback timestamp
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
  season_types = [
    {'Regular Season': 'Regular season'},
    {'Playoffs': 'Playoffs'},
    # {'PlayIn': 'Play in'},
    {'All': 'Full'},
    # {'Full': 'Full'}
  ]
  for season_type in season_types:
    for season_type_key, season_type_value in season_type.items():
      folder_path = 'all_files'
      # List all files in the folder
      files = os.listdir(os.path.join(folder_path, season_type_value))
      for filename in files:
        name, extension = os.path.splitext(filename)
        # print(name)
        if name.isnumeric():
          date_str = name
          output_file = f"pbp_stats_{date_str}.csv"
          output_path = os.path.join(season_type_value, output_file)

          os.makedirs(season_type_value, exist_ok=True)
          if os.path.exists(output_path):
            print("File exists!")
            continue

          tasks.append(asyncio.create_task(scrape_and_save(date_str, season_type_key, season_type_value, output_path)))
        else:
          print(f"Skipping file: {name}")
  await asyncio.gather(*tasks, return_exceptions=True)
  print("All tasks in new retrieve from pbp have completed!")


if __name__ == "__main__":
    asyncio.run(new_retrieve_from_pbp())