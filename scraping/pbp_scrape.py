from datetime import datetime
import re
import asyncio, functools, os
import utils


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
      start_date, end_date = utils.get_date_range_extended(date_str, season_type_value)
  except ValueError as e:
      await log_failed(date_str, season_type_value, str(e))
      print(f"[SKIP] {e}")
      return

  params = {
      "Season": utils.get_season(date_str),
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