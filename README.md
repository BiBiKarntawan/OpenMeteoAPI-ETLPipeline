# Data Quality & ETL Pipeline

## Overview

    This project implements an end-to-end pipeline that ingests weather data from the Open-Meteo public API, 
	performs data cleaning and transformation, enforces data quality checks using Great Expectations (GX Core), 
	loads validated data into a SQLite data warehouse, and produces an analytics summary.
    The pipeline is fully reproducible, and demonstrates real-world ETL + data quality gate patterns.

		Open-Meteo API
		      v
		Extract (JSON)
		      v
		Transform (Cleaned Parquet)
		      v
		Data Quality Validation (GX Core)
		      v
		SQLite Warehouse (fact_hourly_weather)
		      v
		Analytics Output (CSV reports)

## Tech Stack

		    Python 3
		    Pandas
		    Requests
		    Great Expectations (GX Core)
		    SQLite + SQLAlchemy
		    Parquet (PyArrow)

## Data Quality Rules (Great Expectations)

		    The pipeline enforces a data quality gate before loading data:
		    time column must exist and be not null
		    time values must be unique
		    temperature_2m must be between -10 and 50
		    relative_humidity_2m must be between 0 and 100
		    precipitation must be >= 0
		    windspeed_10m must be >= 0
		    If any critical rule fails, the pipeline stops automatically.

## How to Run
1) Create virtual environment
```bash	
python -m venv .venv
source .venv/bin/activate   # macOS / Linux
```			
2) Install dependencie
```bash
pip install -r requirements.txt
```
3) Run the entire pipeline
```bash
python src/run_pipeline.py
```
## Data Warehouse
    SQLite table: fact_hourly_weather
    Location: data/warehouse.db

## Data Quality Report
    reports/gx_validation_<timestamp>.json  
## Analytics Output
    reports/daily_weather_summary.csv
    daily average, min, max temperature
    total precipitation
    max wind speed

## Key Learnings
    Designed a complete ETL pipeline with clear data stages (raw → processed → warehouse)
    Implemented data quality validation as a first-class step
    Used GX Core programmatic validation (no CLI dependency)
    Applied warehouse-style fact table design
    Built a reproducible, single-command pipeline

## Future Improvements
    Incremental loading (append new hours only)
    Orchestration with Apache Airflow
    Cloud deployment (AWS S3 + Athena)
    CI validation on pull requests

Author
Bibi
