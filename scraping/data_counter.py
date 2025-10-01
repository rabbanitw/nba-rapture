import os
from pathlib import Path
from collections import defaultdict
import json


def analyze_player_data(base_path):
    """
    Analyze player data completeness for each season type and timestamp.

    Args:
        base_path: The root directory path containing the data_collector folder

    Returns:
        A dictionary with analysis results
    """
    # Required data types for a complete set
    required_data_types = {'538', 'pbp', 'wowy_off', 'wowy_on', 'tracking'}

    # Store results
    results = defaultdict(lambda: defaultdict(lambda: {
        'complete_players': [],
        'incomplete_players': defaultdict(list),
        'total_complete': 0,
        'total_incomplete': 0
    }))

    data_collector_path = Path(base_path) / 'data_collector'

    if not data_collector_path.exists():
        print(f"Error: Path {data_collector_path} does not exist!")
        return results

    # Iterate through season types (Playoffs, Regular season, etc.)
    for season_type_dir in data_collector_path.iterdir():
        if not season_type_dir.is_dir():
            continue

        season_type = season_type_dir.name

        # Iterate through timestamps
        for timestamp_dir in season_type_dir.iterdir():
            if not timestamp_dir.is_dir():
                continue

            timestamp = timestamp_dir.name

            # Track player data for this timestamp
            player_data = defaultdict(set)

            # Iterate through player directories
            for player_dir in timestamp_dir.iterdir():
                if not player_dir.is_dir():
                    continue

                player_name = player_dir.name

                # Check which data types exist for this player
                for data_item in player_dir.iterdir():
                    if data_item.is_dir() and data_item.name in required_data_types:
                        player_data[player_name].add(data_item.name)

            # Analyze completeness for each player
            for player_name, data_types in player_data.items():
                missing_types = required_data_types - data_types

                if not missing_types:
                    # Player has complete data
                    results[season_type][timestamp]['complete_players'].append(player_name)
                    results[season_type][timestamp]['total_complete'] += 1
                else:
                    # Player has incomplete data
                    results[season_type][timestamp]['incomplete_players'][player_name] = list(missing_types)
                    results[season_type][timestamp]['total_incomplete'] += 1

    return results


def print_analysis_report(results):
    """
    Print a formatted analysis report.

    Args:
        results: The analysis results dictionary
    """
    print("\n" + "=" * 80)
    print("PLAYER DATA COMPLETENESS ANALYSIS REPORT")
    print("=" * 80)

    if not results:
        print("No data found to analyze.")
        return

    for season_type in sorted(results.keys()):
        print(f"\n{'=' * 40}")
        print(f"SEASON TYPE: {season_type}")
        print(f"{'=' * 40}")

        for timestamp in sorted(results[season_type].keys()):
            timestamp_data = results[season_type][timestamp]

            print(f"\n  Timestamp: {timestamp}")
            print(f"  {'-' * 36}")

            # Complete players
            total_complete = timestamp_data['total_complete']
            print(f"  ✓ Complete Data Sets: {total_complete} players")

            if timestamp_data['complete_players']:
                print(f"    Players with complete data:")
                for player in sorted(timestamp_data['complete_players']):
                    print(f"      • {player}")

            # Incomplete players
            total_incomplete = timestamp_data['total_incomplete']
            if total_incomplete > 0:
                print(f"\n  ✗ Incomplete Data Sets: {total_incomplete} players")
                print(f"    Players with missing data:")
                for player, missing in sorted(timestamp_data['incomplete_players'].items()):
                    missing_str = ', '.join(sorted(missing))
                    print(f"      • {player}: missing [{missing_str}]")

            # Summary
            total_players = total_complete + total_incomplete
            if total_players > 0:
                completeness_rate = (total_complete / total_players) * 100
                print(f"\n  Summary: {total_complete}/{total_players} players complete ({completeness_rate:.1f}%)")


def save_results_to_file(results, output_path):
    """
    Save analysis results to a JSON file.

    Args:
        results: The analysis results dictionary
        output_path: Path where to save the results
    """
    # Convert defaultdict to regular dict for JSON serialization
    serializable_results = {}
    for season_type, timestamps in results.items():
        serializable_results[season_type] = {}
        for timestamp, data in timestamps.items():
            serializable_results[season_type][timestamp] = {
                'complete_players': data['complete_players'],
                'incomplete_players': dict(data['incomplete_players']),
                'total_complete': data['total_complete'],
                'total_incomplete': data['total_incomplete']
            }

    with open(output_path, 'w') as f:
        json.dump(serializable_results, f, indent=2)
    print(f"\n✓ Results saved to: {output_path}")


def main():
    """
    Main function to run the analysis.
    """
    # Get the base path from user or use current directory
    print("Player Data Completeness Analyzer")
    print("-" * 40)

    # You can modify this path to match your actual directory structure
    # For PyCharm, you might want to use the project root directory
    base_path = input("Enter the base path (press Enter for current directory): ").strip()

    if not base_path:
        base_path = os.getcwd()

    print(f"\nAnalyzing data in: {base_path}")
    print("Looking for: data_collector/[Season Type]/[Timestamp]/[Player Name]/[Data Type]")
    print("\nRequired data types for completeness: 538, pbp, wowy_off, wowy_on, tracking")

    # Run analysis
    results = analyze_player_data(base_path)

    # Print report
    print_analysis_report(results)

    # Optionally save to file
    save_option = input("\n\nDo you want to save the results to a JSON file? (y/n): ").strip().lower()
    if save_option == 'y':
        output_filename = input("Enter output filename (default: player_data_analysis.json): ").strip()
        if not output_filename:
            output_filename = "player_data_analysis.json"
        if not output_filename.endswith('.json'):
            output_filename += '.json'

        save_results_to_file(results, output_filename)

    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()