"""
Country codes ingestion entry point.

Uses the shared CSV ingestion function to load
the raw country codes dataset into the Bronze layer.
"""

import sys

from ingestion.csv_ingestion import (
    DATASETS,
    ingest_csv,
)


def run_country_codes_ingestion() -> bool:
    """Ingest the country codes dataset and report its status."""

    source_file = DATASETS["country_codes"]

    result = ingest_csv(
        dataset_name="country_codes",
        source_file=source_file,
    )

    if result["status"] == "SUCCESS":
        print("\nCountry codes ingestion completed successfully.")
        print(f"Rows: {result['row_count']}")
        print(f"Columns: {result['column_count']}")
        print(f"Output: {result['output']}")
        return True

    print("\nCountry codes ingestion failed.")
    print(f"Error: {result['error']}")
    return False


if __name__ == "__main__":
    success = run_country_codes_ingestion()

    if not success:
        sys.exit(1)

