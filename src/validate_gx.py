# src/validate_gx.py
import sys
import pandas as pd
import great_expectations as gx
import json
import os
from datetime import datetime

PARQUET_PATH = "data/processed/hourly_weather.parquet"
REPORT_DIR = "reports"


def main():
    # 1) Load processed data
    df = pd.read_parquet(PARQUET_PATH)

    # 2) Create a GX context
    context = gx.get_context()

    # 3) Connect to data
    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="hourly_weather_df")

    batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_df")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # 4) Build an Expectation 
    suite_name = "hourly_weather_suite"
    suite = context.suites.add(gx.core.expectation_suite.ExpectationSuite(name=suite_name))

    # --- Key expectations ---
    # time column must exist & not be null & unique
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="time", severity="critical"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="time", severity="critical"))

    # ranges / validity checks
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="temperature_2m", min_value=-10, max_value=50, severity="warning"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="relative_humidity_2m", min_value=0, max_value=100, severity="critical"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="precipitation", min_value=0, severity="critical"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="windspeed_10m", min_value=0, severity="critical"
        )
    )

    # 5) Validate
    validation_results = batch.validate(suite)
    os.makedirs(REPORT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")
    report_path = os.path.join(REPORT_DIR, f"gx_validation_{ts}.json")

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(validation_results, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n Saved validation report to: {report_path}")
    
    print(validation_results)

    # 6) Fail the pipeline if validation fails
    if not validation_results["success"]:
        print("\n Data quality validation FAILED. Stopping pipeline.")
        sys.exit(1)

    print("\n Data quality validation PASSED.")

if __name__ == "__main__":
    main()
