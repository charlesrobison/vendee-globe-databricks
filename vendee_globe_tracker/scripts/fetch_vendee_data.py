from pathlib import Path
from datetime import datetime
import os
import requests
import json
from dotenv import load_dotenv


# Paths
BASE_DIR = Path(__file__).resolve().parents[1]  # one level up from 'scripts'
RAW_PATH = BASE_DIR / "data" / "raw"

# Make sure raw data folder exists
RAW_PATH.mkdir(parents=True, exist_ok=True)

# API settings
load_dotenv()  # load environment variables from .env file
API_KEY = os.getenv("VENDEE_API_KEY")  # stored in .env
API_URL = f"https://www.vendeeglobeapi.com/api/vgdata?apikey={API_KEY}"

def fetch_vendee_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    # timestamped file
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_PATH / f"vendee_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved data to {file_path}")

if __name__ == "__main__":
    fetch_vendee_data()
