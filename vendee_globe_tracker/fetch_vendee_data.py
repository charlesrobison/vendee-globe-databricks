import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("VENDEE_API_KEY")
if not API_KEY:
    raise ValueError("API key not found. Please set VENDEE_API_KEY in your .env file.")

API_URL = f"https://www.vendeeglobeapi.com/api/vgdata?apikey={API_KEY}"

# Create data/raw directory if it doesn't exist
os.makedirs("data/raw", exist_ok=True)

def fetch_vendee_data():
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return

    # Save JSON with timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = f"data/raw/vendee_{timestamp}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved data to {file_path}")

if __name__ == "__main__":
    fetch_vendee_data()
