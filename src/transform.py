import os
import glob
import json
import pandas as pd

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def latest_raw_file() -> str:
    files = sorted(glob.glob(os.path.join(RAW_DIR, "openmeteo_*.json")))
    if not files:
        raise FileNotFoundError("No raw files found.")
    return files[-1]

def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    raw_path =latest_raw_file()
    with open(raw_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])

    df = pd.DataFrame({
        "time": pd.to_datetime(times),
        "temperature_2m": hourly.get("temperature_2m", []),
        "relative_humidity_2m": hourly.get("relativehumidity_2m", []),
        "precipitation": hourly.get("precipitation", []),
        "windspeed_10m": hourly.get("windspeed_10m", []),
    })

    #ADD Metadata columns
    df["latitude"] = payload.get("latitude")
    df["longitude"] = payload.get("longitude")
    df["timezone"] = payload.get("timezone")

    #cleaning
    df = df.drop_duplicates(subset=["time"]).sort_values("time")
    df = df[df["time"].notna()].reset_index(drop=True)

    #clean bad values to NaN
    for col in ["temperature_2m", "relative_humidity_2m", "precipitation", "windspeed_10m"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    out_path = os.path.join(PROCESSED_DIR, "hourly_weather.parquet")
    df.to_parquet(out_path, index=False)

    print(f" Read raw: {raw_path}")
    print(f" Wrote processed: {out_path}")
    print(f"Rows: {len(df)} | Columns: {list(df.columns)}")

if __name__ == "__main__":
    main()