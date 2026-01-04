import os
import pandas as pd
from sqlalchemy import create_engine

PROCESSED_FILE = "data/processed/hourly_weather.parquet"
DB_PATH = "data/warehouse.db"
TABLE_NAME = "fact_hourly_weather"

def main():
    if not os.path.exists(PROCESSED_FILE):
        raise FileNotFoundError()
    
    os.makedirs("data", exist_ok=True)

    df = pd.read_parquet(PROCESSED_FILE)

    engine = create_engine(f"sqlite:///{DB_PATH}")
    #replace on each run 
    df.to_sql(TABLE_NAME, con=engine, if_exists="replace", index=False)
    
    print(f" Loaded {len(df)} rows into {DB_PATH} table: {TABLE_NAME}")

if __name__ == "__main__":
    main()