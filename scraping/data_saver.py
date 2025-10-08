import os
import json
import csv
import utils
import database
import re
from typing import Tuple, Union, Any
import data

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


def already_processed(player_name: str, timestamp: str, season_type: str, source: str, data_type: str = None,
                      on_or_off: str = None):
    query = {
        "name": player_name,
        "timestamp": timestamp,
        "season_type": season_type,
        "source": source
    }
    if data_type is not None:
        query["data_type"] = data_type
    if on_or_off is not None:
        query["on_or_off"] = on_or_off
    return database.document_exists(db, query)


def timestamp_already_processed(timestamp: str, source: str):
    query = {
        "timestamp": timestamp,
        "source": source
    }
    return database.document_exists(db, query)


def process_pbp(timestamp: str, file_path: str, season_type: str):
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row_dict in reader:
                player_name = utils.remove_numbers_and_apostrophes(row_dict.get("Name"))
                if not already_processed(player_name, timestamp, season_type, "pbp"):
                    print(f"Now processing {player_name} and timestamp {timestamp} from pbp")
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
            data_json = json.load(file)
            for player_name, row_dict in data_json.items():
                if not already_processed(player_name, timestamp, season_type, "nba-tracking", data_type):
                    print(
                        f"Now processing {player_name}, timestamp {timestamp}, and data_type {data_type} from nba tracking data")
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
        if not already_processed(player_name, timestamp, season_type, "wowy", on_or_off=on_or_off):
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                row = next(reader, None)
                if row is None:
                    raise ValueError(f"No data rows found in {file_path!r}")
                second = next(reader, None)
                if second and any((v or "").strip() for v in second.values()):  # noqa: SIM103
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
            print(f"Saved [{player_name}], [{timestamp}], [{on_or_off}] to database!")
        else:
            print(f"Skipping [{player_name}], [{timestamp}], [{on_or_off}]. Already saved to DB.")
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


def get_season_type_from_folder(folder_name: str) -> str:
    """Convert folder names to consistent season type values."""
    folder_mapping = {
        'Regular season': 'Regular season',
        'Playoffs': 'Playoffs',
        'Full season': 'Full season'
    }
    return folder_mapping.get(folder_name, folder_name)


def _looks_like_ts_folder(name: str) -> bool:
    return bool(re.fullmatch(r"\d{14}", name))


def process_data_source_folder(base_folder: str, source_name: str):
    """
    Process a data source folder.

    NEW for PBP:
      missing_data_finder/{season_type}/{timestamp}/pbp_stats_YYYYMMDDHHMMSS.csv
    """
    print(f"Processing {source_name} folder...")

    if not os.path.exists(base_folder):
        print(f"Folder not found: {base_folder}")
        return

    if source_name == "538":
        # (unchanged) 538 has season_type/filter-year/files structure
        for season_type_folder in os.listdir(base_folder):
            season_type_path = os.path.join(base_folder, season_type_folder)
            if not os.path.isdir(season_type_path):
                continue

            season_type = get_season_type_from_folder(season_type_folder)
            print(f"  Processing {season_type_folder} season...")

            for filter_folder in os.listdir(season_type_path):
                filter_path = os.path.join(season_type_path, filter_folder)
                if not os.path.isdir(filter_path) or not filter_folder.startswith('filter-'):
                    continue

                print(f"    Processing {filter_folder}...")
                for filename in os.listdir(filter_path):
                    if not filename.endswith('.csv'):
                        continue
                    name = os.path.splitext(filename)[0]
                    if name.isnumeric():
                        timestamp = name
                        file_path = os.path.join(filter_path, filename)
                        process_538(timestamp, file_path, season_type)
                    else:
                        print(f"      Skipping non-numeric file: {filename}")

    elif source_name == "pbp":
        # NEW STRUCTURE: season_type / timestamp / pbp_stats_YYYYMMDDHHMMSS.csv
        for season_type_folder in os.listdir(base_folder):
            season_type_path = os.path.join(base_folder, season_type_folder)
            if not os.path.isdir(season_type_path):
                continue

            season_type = get_season_type_from_folder(season_type_folder)
            print(f"  Processing {season_type_folder} season...")

            for ts_folder in os.listdir(season_type_path):
                ts_path = os.path.join(season_type_path, ts_folder)
                if not os.path.isdir(ts_path):
                    continue
                if not _looks_like_ts_folder(ts_folder):
                    print(f"    Skipping non-timestamp folder: {ts_folder}")
                    continue

                timestamp = ts_folder
                print(f"    Processing timestamp {timestamp}...")

                # Expect files like pbp_stats_YYYYMMDDHHMMSS.csv in this folder
                for filename in os.listdir(ts_path):
                    if not filename.endswith(".csv"):
                        continue
                    if not filename.startswith("pbp_stats_"):
                        print(f"      Skipping non-pbp file: {filename}")
                        continue

                    # If filename timestamp disagrees with folder, prefer folder name.
                    name_no_ext = os.path.splitext(filename)[0]
                    fn_ts_match = re.fullmatch(r"pbp_stats_(\d{14})", name_no_ext)
                    if fn_ts_match and fn_ts_match.group(1) != timestamp:
                        print(f"      Warning: filename ts {fn_ts_match.group(1)} != folder ts {timestamp}; using folder ts.")

                    file_path = os.path.join(ts_path, filename)
                    try:
                        process_pbp(timestamp, file_path, season_type)
                    except Exception as e:
                        print(f"      Error processing {filename}: {e}")

    elif source_name in ["wowy", "nba_api"]:
        # (unchanged) legacy structure for these sources
        for season_type_folder in os.listdir(base_folder):
            season_type_path = os.path.join(base_folder, season_type_folder)
            if not os.path.isdir(season_type_path):
                continue

            season_type = get_season_type_from_folder(season_type_folder)
            print(f"  Processing {season_type_folder} season...")

            for filter_folder in os.listdir(season_type_path):
                filter_path = os.path.join(season_type_path, filter_folder)
                if not os.path.isdir(filter_path) or not filter_folder.startswith('filter-'):
                    continue

                print(f"    Processing {filter_folder}...")

                for filename in os.listdir(filter_path):
                    file_path = os.path.join(filter_path, filename)
                    name = os.path.splitext(filename)[0]
                    try:
                        if source_name == "wowy" and name.startswith('wowy_'):
                            source, timestamp, player, on_or_off = extract_parts_wowy(name)
                            process_wowy(timestamp, file_path, season_type, on_or_off, player)
                        elif source_name == "nba_api" and name.startswith('nba_api_'):
                            data_type, timestamp = extract_parts_nba_api(name)
                            process_nba(timestamp, file_path, season_type, data_type)
                        else:
                            print(f"      Skipping file: {filename}")
                    except Exception as e:
                        print(f"      Error processing {filename}: {e}")


def save_data():
    """Main function to process all data source folders."""
    # Adjust as needed; you mentioned pbp with the new layout.
    data_sources = ["pbp"]
    base_folder = "missing_data_finder"

    for source in data_sources:
        if os.path.exists(base_folder):
            process_data_source_folder(base_folder, source)
        else:
            print(f"Skipping {source} - folder not found")

    print("We are done!")


if __name__ == "__main__":
    save_data()
