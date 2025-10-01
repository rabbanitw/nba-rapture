# Local PyCharm version - run directly without Jupyter

import os
import pickle
import numpy as np
from typing import Dict, Any, Optional, List
import gc
import argparse
import name_utils

POS_COLS = ["PG", "SG", "SF", "PF", "C"]


def one_hot_pos(pos_string: str, pos_cols: List[str] = POS_COLS) -> np.ndarray:
    """Convert position string to one-hot encoding."""
    pos_tokens = [p.strip().upper() for p in pos_string.split(",") if p.strip()]
    return np.array([1 if col in pos_tokens else 0 for col in pos_cols], dtype=np.float32)


def _clean_data_iterable(data_iter) -> np.ndarray:
    """Clean and convert data to float32 array."""
    data_clean = []
    for x in data_iter:
        if x == '':
            data_clean.append(0.0)
        elif isinstance(x, (float, np.floating)):
            data_clean.append(float(x))
        elif isinstance(x, (int, np.integer)):
            data_clean.append(float(x))  # FIX: Convert int to float, not 0.0
        else:
            try:
                # Try to convert to float
                data_clean.append(float(x))
            except (ValueError, TypeError):
                print(f"Non-numeric value encountered: {x!r} (type={type(x).__name__})")
                data_clean.append(-1000.0)
    return np.asarray(data_clean, dtype=np.float32)


def process_single_timestamp(
    base_dir: str,
    season_type: str,
    timestamp: str,
    standard_name: str,
    sources: List[str],
) -> Optional[Dict[str, Any]]:
    """
    Process a single timestamp. Returns entry dict or None.
    """
    path = os.path.join(base_dir, season_type, timestamp, standard_name)
    path_538 = os.path.join(path, '538', '538.pkl')

    # Read 538 data
    try:
        with open(path_538, 'rb') as f:
            dic_538 = pickle.load(f)
    except Exception as e:
        return None

    pos = dic_538.get('pos')
    if pos is None:
        return None

    # Start with one-hot position
    one_hot = one_hot_pos(str(pos))
    data_parts = [one_hot]

    # DEBUG: Print lengths
    print(f"\nDEBUG for {standard_name} at {timestamp}:")
    print(f"  One-hot position length: {len(one_hot)}")

    # Load each source
    for source in sources:
        file_path = os.path.join(path, source, source + '.pkl')
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)

            # DEBUG: Check what we loaded
            print(f"  {source} - type: {type(data)}, length: {len(data) if hasattr(data, '__len__') else 'N/A'}")
            if isinstance(data, dict):
                print(f"    Keys: {list(data.keys())}")

            # Clean and convert immediately
            np_data = _clean_data_iterable(data)
            print(f"  {source} after cleaning - length: {len(np_data)}")
            data_parts.append(np_data)

            # Release memory immediately
            del data, np_data

        except Exception as e:
            print(f"Failed to load {source} for {timestamp}: {e}")
            return None

    # Concatenate all parts
    combined = np.concatenate(data_parts)
    print(f"  Total combined length: {len(combined)}")

    del data_parts

    entry = {'data': combined, 'label': dic_538}
    return entry


def process_player(
    standard_name: str,
    season_type: str,
    model_type: str = "box",
    base_dir: str = "nba_data",
    print_every: int = 10,
) -> str:
    """
    Process player data and save to pickle file.

    Args:
        standard_name: Player name
        season_type: Season type
        model_type: "box" or "onoff"
        base_dir: Base directory
        print_every: Print progress every N timestamps

    Returns:
        Path to saved tensor.pkl
    """
    if model_type not in ("box", "onoff"):
        raise ValueError(f"Unknown model_type: {model_type}")

    directory_path = os.path.join(base_dir, season_type)

    # Get list of timestamps
    if not os.path.isdir(directory_path):
        raise FileNotFoundError(f"Missing directory: {directory_path}")

    timestamps = [item for item in os.listdir(directory_path)
                  if os.path.isdir(os.path.join(directory_path, item))]

    if not timestamps:
        out_dir = os.path.join(base_dir, season_type, model_type, standard_name)
        out_path = os.path.join(out_dir, 'tensor.pkl')
        os.makedirs(out_dir, exist_ok=True)
        with open(out_path, 'wb') as f:
            pickle.dump({}, f)
        print(f"No timestamps found. Saved empty tensor.")
        return out_path

    # Determine sources
    sources = ['pbp', 'tracking'] if model_type == 'box' else ['wowy_on', 'wowy_off']

    total = len(timestamps)
    print(f"Processing {total} timestamps for {standard_name}...")
    print(f"Model type: {model_type}, Sources: {sources}")

    player_dic: Dict[str, Dict[str, Any]] = {}

    # Track data lengths to verify consistency
    data_lengths = []

    # Process each timestamp
    for idx, ts in enumerate(timestamps):
        entry = process_single_timestamp(base_dir, season_type, ts, standard_name, sources)

        # if entry['data'] =

        if entry is not None:
            player_dic[ts] = entry
            data_lengths.append(len(entry['data']))
            del entry

        # Garbage collect periodically
        if (idx + 1) % 10 == 0:
            gc.collect()

        # Print progress
        if (idx + 1) % print_every == 0 or idx == total - 1:
            print(f"Progress: {idx + 1}/{total} ({len(player_dic)} valid)")

    # Check for length consistency
    if data_lengths:
        unique_lengths = set(data_lengths)
        if len(unique_lengths) > 1:
            print(f"WARNING: Inconsistent data lengths for {standard_name}")
            print(f"  Unique lengths found: {unique_lengths}")
            from collections import Counter
            length_counts = Counter(data_lengths)
            print(f"  Length distribution: {dict(length_counts)}")

    # Save final result
    out_dir = os.path.join(base_dir, season_type, model_type, standard_name)
    out_path = os.path.join(out_dir, 'tensor.pkl')
    os.makedirs(out_dir, exist_ok=True)

    print(f"Saving {len(player_dic)} timestamps to {out_path}...")
    with open(out_path, 'wb') as f:
        pickle.dump(player_dic, f)

    del player_dic
    gc.collect()

    print(f"✓ Successfully saved to: {out_path}")
    return out_path


def batch_process_players(
    player_names: List[str],
    season_type: str,
    model_type: str = "box",
    base_dir: str = "nba_data",
) -> None:
    """
    Process multiple players in sequence.

    Args:
        player_names: List of player names to process
        season_type: Season type
        model_type: "box" or "onoff"
        base_dir: Base directory
    """
    total_players = len(player_names)
    print(f"\n{'=' * 60}")
    print(f"Batch processing {total_players} players")
    print(f"{'=' * 60}\n")

    for idx, player_name in enumerate(player_names, 1):
        print(f"\n[{idx}/{total_players}] Processing: {player_name}")
        print("-" * 60)

        try:
            result = process_player(player_name, season_type, model_type, base_dir)
            print(f"✓ Completed: {player_name}\n")
        except Exception as e:
            print(f"✗ Failed: {player_name}")
            print(f"  Error: {e}\n")

        # Garbage collect between players
        gc.collect()

    print(f"\n{'=' * 60}")
    print(f"Batch processing complete!")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    all_names = name_utils.get_or_load_names()
    batch_process_players(all_names, season_type="Regular season", model_type="onoff", base_dir='nba_data_v2')
    # process_player(standard_name='Alan Anderson', season_type="Regular season", model_type='box', base_dir='nba_data')