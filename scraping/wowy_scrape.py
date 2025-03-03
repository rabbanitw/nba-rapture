import requests
import csv
import os
import utils
import database
import time
import traceback

PROCESSED_FILES_LOG = "processed_files_wowy.log"

db = database.get_database()

nba_team_ids = {
    "Hawks": 1610612737,
    "Celtics": 1610612738,
    "Nets": 1610612751,
    "Hornets": 1610612766,
    "Bulls": 1610612741,
    "Cavaliers": 1610612739,
    "Mavericks": 1610612742,
    "Nuggets": 1610612743,
    "Pistons": 1610612765,
    "Warriors": 1610612744,
    "Rockets": 1610612745,
    "Pacers": 1610612754,
    "Clippers": 1610612746,
    "Lakers": 1610612747,
    "Grizzlies": 1610612763,
    "Heat": 1610612748,
    "Bucks": 1610612749,
    "Timberwolves": 1610612750,
    "Pelicans": 1610612740,
    "Knicks": 1610612752,
    "Thunder": 1610612760,
    "Magic": 1610612753,
    "76ers": 1610612755,
    "Suns": 1610612756,
    "Trail Blazers": 1610612757,
    "Kings": 1610612758,
    "Spurs": 1610612759,
    "Raptors": 1610612761,
    "Jazz": 1610612762,
    "Wizards": 1610612764
}


def load_processed_files() -> set:
    if not os.path.exists(PROCESSED_FILES_LOG):
        return set()
    with open(PROCESSED_FILES_LOG, "r") as f:
        processed = {line.strip() for line in f if line.strip()}
    return processed


def mark_file_processed(file_name: str):
    with open(PROCESSED_FILES_LOG, "a") as f:
        f.write(file_name + "\n")


def robust_get_wowy_data(player_name, team_name, date_str, season_type_key, season_type_value,
                         is_on, nba_player_ids):
    max_retries = 5
    base_sleep = 3
    attempt = 1
    while True:
        try:
            retrieve_from_wowy(player_name, team_name, date_str, season_type_key, season_type_value, is_on,
                               nba_player_ids)
            # If we succeed, break out of the loop
            break

        except Exception as e:
            print(
                f"[ERROR] Failed to process {player_name} on {team_name} at {date_str} with type {"ON" if is_on else "OFF"} (attempt {attempt}/{max_retries}).")
            traceback.print_exc()

            if attempt >= max_retries:
                print(f"[CRITICAL] Exceeded max retries for {player_name} on {team_name} at {date_str} with type {"ON" if is_on else "OFF"}.")
                raise  # Re-raise the error or handle as appropriate

            # Exponential-ish backoff
            sleep_time = base_sleep * (2 ** (attempt - 1))
            print(f"[INFO] Retrying {player_name} on {team_name} at {date_str} with type {"ON" if is_on else "OFF"} in {sleep_time} seconds...")
            time.sleep(sleep_time)
            attempt += 1


def get_all_players():
    players_response = requests.get("https://api.pbpstats.com/get-all-players-for-league/nba")
    players = players_response.json()
    nba_player_ids = {
        utils.remove_numbers_and_apostrophes(player_name): player_id
        for player_id, player_name in players["players"].items()
    }
    return nba_player_ids


def write_wowy_data(wowy_data, player_name, timestamp, season_type, is_on):
    wowy_data["name"] = player_name
    wowy_data["timestamp"] = timestamp
    wowy_data["season_type"] = season_type
    wowy_data["on_or_off"] = "on" if is_on else "off"
    wowy_data["source"] = "wowy"
    database.create_document(db, wowy_data)


def save_local_wowy_data(wowy_data, output_path):
    if os.path.exists(output_path):
        print("File exists!")
    else:
        with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=wowy_data.keys())
            writer.writeheader()
            writer.writerow(wowy_data)


def retrieve_from_wowy(player_name, team_name, date_str, season_type_key, season_type_value, is_on, nba_player_ids):
    url = "https://api.pbpstats.com/get-wowy-stats/nba"
    start_date, end_date = utils.get_date_range(date_str, season_type_value)
    print(f"start_date: {start_date}")
    print(f"end_date: {end_date}")

    params = {
        "Season": utils.get_season(date_str),
        "SeasonType": season_type_key,
        "Type": "Team",
        "FromDate": start_date,
        "ToDate": end_date,
        "TeamId": nba_team_ids[team_name],
    }
    if is_on:
        params['0Exactly1OnFloor'] = nba_player_ids[player_name]
    else:
        params['0Exactly0OnFloor'] = nba_player_ids[player_name]
    print("params:", params)

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        response_json = response.json()
        stats = response_json["single_row_table_data"]

        print(stats)
        # print(type(stats))

        is_on_string = "on" if is_on else "off"
        output_file = f"pbp_wowy_{player_name}_{is_on_string}_{date_str}.csv"
        output_path = os.path.join(season_type_value, output_file)

        if stats:
            # save_local_wowy_data(stats, output_path)
            write_wowy_data(stats, player_name, date_str, season_type_value, is_on)
            print(f"Data has been saved")
        else:
            print("No data to write")
    except requests.exceptions.RequestException as e:
        print(f"Failed to save data due to problem with server: {e}")
    except Exception as e:
        print(f"Unknown error saving data for {player_name}, {team_name}, {date_str}, {season_type_value}", e)


def main():
    processed_files = load_processed_files()
    nba_player_ids = get_all_players()
    print(nba_player_ids)
    season_types = [
        {'Regular Season': 'Regular season'},
        {'Playoffs': 'Playoffs'},
        {'PlayIn': 'Play in'},
        {'All': 'All'}
    ]
    for season_type in season_types:
        for season_type_key, season_type_value in season_type.items():
            folder_path = '/content/drive/MyDrive/nba-ml'
            files = os.listdir(f"{folder_path}/{season_type_value}")
            for filename in files:
                if filename in processed_files:
                    print(f"Skipping already processed file: {filename}")
                    continue
                name, extension = os.path.splitext(filename)
                if name.isnumeric():
                    full_file_path = os.path.join(folder_path, season_type_value, filename)
                    with open(full_file_path, 'r') as file:
                        c = csv.DictReader(file)
                        header = next(c)
                        for row in c:
                            # print(row)
                            player_name = utils.remove_numbers_and_apostrophes(row['name'])
                            team_name = row['team']
                            date_str = name
                            robust_get_wowy_data(player_name, team_name, date_str, season_type_key, season_type_value,
                                                 True, nba_player_ids)
                            robust_get_wowy_data(player_name, team_name, date_str, season_type_key, season_type_value,
                                                 False, nba_player_ids)
                else:
                    print(f"we're not processing this file lmao {name}")
                mark_file_processed(filename)
    print("wowee we're done!")


if __name__ == "__main__":
    main()
