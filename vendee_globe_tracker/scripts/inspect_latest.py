# scripts/inspect_latest.py
import json
from pathlib import Path
import pprint

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

def get_latest_file():
    files = sorted(RAW_DIR.glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)
    return files[0] if files else None

def inspect_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)

    print(f"\nLoaded file: {file_path.name}")
    print(f"Type of root object: {type(data).__name__}")

    if "latestdata" in data:
        latest = data["latestdata"]
        print(f"\nLast update time: {latest.get('lastUpdate')}")
        race_data = latest.get("data", [])
        print(f"Number of entries in 'data': {len(race_data)}")

        if race_data:
            print("\nSample first entry (truncated):")
            pp = pprint.PrettyPrinter(indent=2, depth=3, width=120)
            pp.pprint(race_data[0])
        else:
            print("No race data found.")
    else:
        print("Unexpected JSON format.")
        pp = pprint.PrettyPrinter(indent=2, depth=2, width=100)
        pp.pprint(data)

if __name__ == "__main__":
    latest_file = get_latest_file()
    if not latest_file:
        print("No JSON files found in data/raw/")
    else:
        inspect_json(latest_file)
