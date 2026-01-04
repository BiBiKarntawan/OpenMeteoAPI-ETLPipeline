import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///data/warehouse.db")
df = pd.read_sql("SELECT * FROM fact_hourly_weather LIMIT 5", con=engine)
print(df)
