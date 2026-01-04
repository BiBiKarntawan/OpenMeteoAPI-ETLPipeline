# src/analytics.py
import os
import pandas as pd
from sqlalchemy import create_engine

DB_PATH = "data/warehouse.db"
TABLE = "fact_hourly_weather"
REPORT_DIR = "reports"

def main():
    os.makedirs(REPORT_DIR, exist_ok=True)
    engine = create_engine(f"sqlite:///{DB_PATH}")

    # Daily summary
    query = f"""
    SELECT
      date(time) AS day,
      AVG(temperature_2m) AS avg_temp,
      MAX(temperature_2m) AS max_temp,
      MIN(temperature_2m) AS min_temp,
      SUM(precipitation) AS total_precip,
      MAX(windspeed_10m) AS max_windspeed
    FROM {TABLE}
    GROUP BY date(time)
    ORDER BY day;
    """

    df = pd.read_sql_query(query, con=engine)
    out_path = os.path.join(REPORT_DIR, "daily_weather_summary.csv")
    df.to_csv(out_path, index=False)

    print(f" Wrote analytics report: {out_path}")
    print(df.head())

if __name__ == "__main__":
    main()
