"""
Chart data ingestion entry point.

Uses the shared CSV ingestion function to load
the raw chart dataset into the Bronze layer.
"""

import sys

from ingestion.csv_ingestion import (
    DATASETS,
    ingest_csv,
)


def run_chart_ingestion() -> bool:
    """Ingest the charts dataset and report its status."""

    source_file = DATASETS["charts"]

    result = ingest_csv(
        dataset_name="charts",
        source_file=source_file,
    )

    if result["status"] == "SUCCESS":
        print("\nChart ingestion completed successfully.")
        print(f"Rows: {result['row_count']}")
        print(f"Columns: {result['column_count']}")
        print(f"Output: {result['output']}")
        return True

    print("\nChart ingestion failed.")
    print(f"Error: {result['error']}")
    return False


if __name__ == "__main__":
    success = run_chart_ingestion()

    if not success:
        sys.exit(1)

