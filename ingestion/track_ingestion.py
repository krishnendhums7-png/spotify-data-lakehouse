"""
Track data ingestion entry point.

Uses the shared CSV ingestion function to load
the raw track dataset into the Bronze layer.
"""

import sys

from ingestion.csv_ingestion import (
    DATASETS,
    ingest_csv,
)


def run_track_ingestion() -> bool:
    """Ingest the tracks dataset and report its status."""

    source_file = DATASETS["tracks"]

    result = ingest_csv(
        dataset_name="tracks",
        source_file=source_file,
    )

    if result["status"] == "SUCCESS":
        print("\nTrack ingestion completed successfully.")
        print(f"Rows: {result['row_count']}")
        print(f"Columns: {result['column_count']}")
        print(f"Output: {result['output']}")
        return True

    print("\nTrack ingestion failed.")
    print(f"Error: {result['error']}")
    return False


if __name__ == "__main__":
    success = run_track_ingestion()

    if not success:
        sys.exit(1)