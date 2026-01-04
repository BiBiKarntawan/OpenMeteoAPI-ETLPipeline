import json
import os
from datetime import datetime, timezone
import requests

RAW_DIR = "data/raw"

def fetch_open_meteo(latitude: float, longitude: float, timezone_name: str = "Australia/Melbourne"):
    print(f"FETCHING DATA URL...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,relativehumidity_2m,precipitation,windspeed_10m",
        "past_days" : 7,
        "forecast_days": 2,
        "timezone": timezone_name,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    #Melbourne
    lat, lon = -37.8136, 144.9631
    data = fetch_open_meteo(lat, lon)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    out_path = os.path.join(RAW_DIR, f"openmeteo_melbourne_{ts}.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"SAVED RAW DATA TO : {out_path}")

if __name__== "__main__":
    main()