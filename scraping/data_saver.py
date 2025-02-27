import os
import json
import csv
import utils
import database

db = database.get_database()


def process_pbp(timestamp: str, file_path: str):
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row_dict in reader:
                player_name = utils.remove_numbers_and_apostrophes(row_dict.get("Name"))
                print(f"Now processing {player_name} and timestamp {timestamp} from 538")
                row_dict["Name"] = player_name
                row_dict["timestamp"] = timestamp
                row_dict["source"] = "pbp"
                database.create_document(db, row_dict)
        print(f"Finished processing PBP")
    else:
        print(f"File not found: {file_path}")


def process_nba(timestamp: str, file_path: str):
    if os.path.isfile(file_path):
        with open(file_path, "r") as file:
            data = json.load(file)
            for player_name, row_dict in data.items():
                print(f"Now processing {player_name} and timestamp {timestamp} from nba tracking data")
                row_dict["NAME"] = utils.remove_numbers_and_apostrophes(player_name)
                row_dict["timestamp"] = timestamp
                row_dict["source"] = "nba-tracking"
                database.create_document(db, row_dict)
        print(f"Finished processing NBA tracking data")
    else:
        print(f"File not found: {file_path}")


def process_538(timestamp: str, file_path: str):
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row_dict in reader:
                player_name = utils.remove_numbers_and_apostrophes(row_dict.get("name"))
                print(f"Now processing {player_name} and timestamp {timestamp} from 538")
                row_dict["name"] = player_name
                row_dict["timestamp"] = timestamp
                row_dict["source"] = "538"
                database.create_document(db, row_dict)
        print(f"Finished processing 538")
    else:
        print(f"File not found: {file_path}")


def save_data():
    season_types = [
        {'Regular Season': 'Regular season'},
        {'Playoffs': 'Playoffs'},
        {'PlayIn': 'Play in'},
        {'All': 'All'}
    ]
    for season_type in season_types:
        for season_type_key, season_type_value in season_type.items():
            folder_path = '/nba-ml'
            directory = season_type_value
            files = os.listdir(f"{folder_path}/{directory}")
            for filename in files:
                name, extension = os.path.splitext(filename)
                if name.startswith('pbp_stats_'):  # PBP API data
                    timestamp = name.replace('pbp_stats_', '')
                    file_path = os.path.join(folder_path, season_type_value, f"{name}.csv")
                    process_pbp(timestamp, file_path)
                elif name.startswith('nba_api_'):  # NBA tracking data
                    timestamp = name.replace('nba_api_', '')
                    file_path = os.path.join(folder_path, season_type_value, f"{name}.json")
                    process_nba(timestamp, file_path)
                elif name.isnumeric():  # 538 raptor
                    timestamp = name
                    file_path = os.path.join(folder_path, season_type_value, f"{name}.csv")
                    process_538(timestamp, file_path)
                else:
                    print(f"Skipping file: {name}")
    print(f"We are done!")


if __name__ == "__main__":
    save_data()
