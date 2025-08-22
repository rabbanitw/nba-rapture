import os
import json
import csv
import utils
import database
import re
from typing import Tuple, Union, Any

db = database.get_database()


def extract_parts_nba_api(filename: str) -> Tuple[str, str]:
    print('filename?', filename)
    match = re.match(r"nba_api_(.+?)_(\d{14})", filename)
    if match:
        something = match.group(1)
        timestamp = match.group(2)
        return something, timestamp
    else:
        raise ValueError("Filename does not match expected format")


def extract_parts_wowy(filename: str):
    print('filename?', filename)
    m = re.fullmatch(
        r'(?P<source>[^_]+)_(?P<ts>\d{14})_(?P<player>[^_]+)_(?P<state>on|off)',
        filename,
        flags=re.IGNORECASE,
    )
    if not m:
        raise ValueError(f"Filename not in expected format: {filename!r}")
    d = m.groupdict()
    return d["source"], d["ts"], d["player"], d["state"].lower()


def already_processed(player_name: str, timestamp: str, season_type: str, source: str, data_type: str = None, on_or_off: str = None):
    query = {
        "name": player_name,
        "timestamp": timestamp,
        "season_type": season_type,
        "source": source
    }
    if data_type is not None:
        query["data_type"] = data_type
    return database.document_exists(db, query)


def process_pbp(timestamp: str, file_path: str, season_type: str):
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row_dict in reader:
                player_name = utils.remove_numbers_and_apostrophes(row_dict.get("Name"))
                if not already_processed(player_name, timestamp, season_type, "pbp"):
                    print(f"Now processing {player_name} and timestamp {timestamp} from 538")
                    row_dict["name"] = player_name
                    row_dict["timestamp"] = timestamp
                    row_dict["season_type"] = season_type
                    row_dict["source"] = "pbp"
                    database.create_document(db, row_dict)
        print(f"Finished processing PBP")
    else:
        print(f"File not found: {file_path}")


def process_nba(timestamp: str, file_path: str, season_type: str, data_type: str):
    if os.path.isfile(file_path):
        with open(file_path, "r") as file:
            data = json.load(file)
            for player_name, row_dict in data.items():
                if not already_processed(player_name, timestamp, season_type, "nba-tracking", data_type):
                    print(f"Now processing {player_name}, timestamp {timestamp}, and data_type {data_type} from nba tracking data")
                    row_dict["name"] = utils.remove_numbers_and_apostrophes(player_name)
                    row_dict["timestamp"] = timestamp
                    row_dict["season_type"] = season_type
                    row_dict["data_type"] = data_type
                    row_dict["source"] = "nba-tracking"
                    print("row_dict?", row_dict)
                    database.create_document(db, row_dict)
        print(f"Finished processing NBA tracking data")
    else:
        print(f"File not found: {file_path}")



def _coerce_number(s: str):
    """Best-effort numeric coercion: int if clean int, else float, else raw string/None."""
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    # int?
    if re.fullmatch(r"-?\d+", s):
        try:
            return int(s)
        except ValueError:
            pass
    # float?
    try:
        return float(s)
    except ValueError:
        return s


def process_wowy(timestamp: str, file_path: str, season_type: str, on_or_off: str, player_name: str):
    if os.path.isfile(file_path):
        if not already_processed(player_name, timestamp, season_type, "wowy", on_or_off = on_or_off):
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                row = next(reader, None)
                if row is None:
                    raise ValueError(f"No data rows found in {file_path!r}")
                second = next(reader, None)
                if second and any((v or "").strip() for v in second.values()):
                    raise ValueError(f"Expected exactly 1 data row in {file_path!r}, found more.")

            row = {k: _coerce_number(v) for k, v in row.items()}

            doc = {
                "source": "wowy",
                "timestamp": timestamp,
                "season_type": season_type,
                "on_or_off": on_or_off,
                "name": player_name,
                **row
            }
            database.create_document(db, doc)
            print(f"✅ Saved [{player_name}], [{timestamp}], [{on_or_off}] to database!")
        else:
            print(f"⏭️ Skipping [{player_name}], [{timestamp}], [{on_or_off}]. Already saved to DB.")
    else:
        print(f"File not found: {file_path}")



def process_538(timestamp: str, file_path: str, season_type: str):
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row_dict in reader:
                player_name = utils.remove_numbers_and_apostrophes(row_dict.get("name"))
                if not already_processed(player_name, timestamp, season_type, "538"):
                    print(f"Now processing {player_name} and timestamp {timestamp} from 538")
                    row_dict["name"] = player_name
                    row_dict["timestamp"] = timestamp
                    row_dict["season_type"] = season_type
                    row_dict["source"] = "538"
                    database.create_document(db, row_dict)
        print(f"Finished processing 538")
    else:
        print(f"File not found: {file_path}")


def save_data():
    season_types = [
        {'Regular Season': 'Regular season'},
        {'Playoffs': 'Playoffs'},
        # {'PlayIn': 'Play in'},
        {'All': 'All'},
        {'Full Season': 'Full'},
    ]
    for season_type in season_types:
        for season_type_key, season_type_value in season_type.items():
            folder_path = 'missing_data'
            directory = season_type_value
            files = os.listdir(f"{folder_path}/{directory}")
            for filename in files:
                name, extension = os.path.splitext(filename)
                if name.startswith('pbp_stats_'):  # PBP API data
                    timestamp = name.replace('pbp_stats_', '')
                    file_path = os.path.join(folder_path, season_type_value, f"{name}.csv")
                    process_pbp(timestamp, file_path, season_type_value)
                    # print("We're not processing PBP this time")
                elif name.startswith('nba_api_'):  # NBA tracking data
                    data_type, timestamp = extract_parts_nba_api(name)
                    file_path = os.path.join(folder_path, season_type_value, f"{name}.json")
                    process_nba(timestamp, file_path, season_type_value, data_type)
                elif name.startswith('wowy_'):  # NBA tracking data
                    source, timestamp, player, on_or_off = extract_parts_wowy(name)
                    file_path = os.path.join(folder_path, season_type_value, f"{name}.csv")
                    process_wowy(timestamp, file_path, season_type_value, on_or_off, player)
                elif name.isnumeric():  # 538 raptor
                    # timestamp = name
                    # file_path = os.path.join(folder_path, season_type_value, f"{name}.csv")
                    # process_538(timestamp, file_path, season_type_value)
                    print("We're not processing 538 this time")
                else:
                    print(f"Skipping file: {name}")
    print(f"We are done!")


if __name__ == "__main__":
    save_data()
