import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


# ==================================================
# 1. PROJECT PATHS
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
LOG_DIR = PROJECT_ROOT / "data" / "logs"

LOG_FILE = LOG_DIR / "ingestion.log"


# ==================================================
# 2. DATASET CONFIGURATION
# ==================================================

DATASETS = {
    "tracks": RAW_DIR / "tracks.csv",
    "charts": RAW_DIR / "charts_sample.csv",
    "country_codes": RAW_DIR / "country_codes.csv",
}


# ==================================================
# 3. LOGGING CONFIGURATION
# ==================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("csv_ingestion")
logger.setLevel(logging.INFO)
logger.handlers.clear()

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

file_handler = logging.FileHandler(
    LOG_FILE,
    encoding="utf-8"
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


# ==================================================
# 4. CSV HEADER VALIDATION
# ==================================================

def validate_csv_header(source_file: Path) -> list[str]:
    """Validate CSV headers and return the original headers."""

    with source_file.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:
        reader = csv.reader(file)

        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV file is empty.")

    if not header:
        raise ValueError("CSV file has no column headers.")

    # Permit an empty first header because the source may
    # contain an accidentally exported index column.
    # Empty headers elsewhere are not allowed.
    if any(not str(name).strip() for name in header[1:]):
        raise ValueError("CSV contains an empty column name.")

    # Detect duplicate non-empty column names.
    normalized_names = [
        str(name).strip().casefold()
        for name in header
        if str(name).strip()
    ]

    if len(normalized_names) != len(set(normalized_names)):
        raise ValueError("Duplicate column names found in CSV.")

    if not normalized_names:
        raise ValueError("CSV contains no valid column names.")

    if not str(header[0]).strip():
        logger.info(
            "Detected an empty first header in %s.",
            source_file.name
        )

    return header


# ==================================================
# 5. REMOVE ACCIDENTAL INDEX COLUMN
# ==================================================

def remove_accidental_index_column(
    df: pd.DataFrame,
    source_file: Path
) -> pd.DataFrame:
    """
    Remove the first column only if the original header was empty
    and the values are sequential integers starting at zero.
    """

    if len(df.columns) == 0:
        return df

    first_column_name = str(df.columns[0]).strip()

    # Pandas usually renames an empty header to 'Unnamed: 0'.
    if first_column_name not in ("", "Unnamed: 0"):
        raise ValueError(
            "Unexpected column name for the suspected index column."
        )

    first_column = df.iloc[:, 0]

    # Confirm that the column contains no missing values.
    if first_column.isna().any():
        raise ValueError(
            "The suspected index column contains missing values."
        )

    # Convert the column to numeric values for validation.
    numeric_values = pd.to_numeric(
        first_column,
        errors="coerce"
    )

    if numeric_values.isna().any():
        raise ValueError(
            "The suspected index column contains non-numeric values."
        )

    # Remove only if the values are exactly 0, 1, 2, 3, ...
    expected_values = list(range(len(df)))

    if numeric_values.tolist() != expected_values:
        raise ValueError(
            "The suspected index column is not sequential. "
            "Manual review is required."
        )

    if not numeric_values.is_unique:
        raise ValueError(
            "The suspected index column contains duplicate values."
        )

    # Remove the verified accidental index column.
    df = df.iloc[:, 1:].copy()

    logger.info(
        "Removed accidental index column from %s.",
        source_file.name
    )

    return df


# ==================================================
# 6. INGEST ONE CSV DATASET
# ==================================================

def ingest_csv(
    dataset_name: str,
    source_file: Path
) -> dict:
    """Validate a CSV and save it as a Bronze Parquet file."""

    started_at = datetime.now(timezone.utc)

    result = {
        "dataset": dataset_name,
        "status": "FAILED",
        "rows": 0,
        "row_count": 0,
        "columns": 0,
        "column_count": 0,
        "output": None,
        "error": None,
        "started_at": started_at.isoformat(),
        "finished_at": None,
    }

    try:
        logger.info(
            "Starting ingestion for %s",
            dataset_name
        )

        # Check whether the source file exists.
        if not source_file.exists():
            raise FileNotFoundError(
                f"Source file not found: {source_file}"
            )

        # Validate the original CSV header.
        header = validate_csv_header(source_file)

        # Read the CSV without modifying the original file.
        df = pd.read_csv(
            source_file,
            encoding="utf-8-sig"
        )

        # Detect duplicate column names after loading.
        normalized_columns = [
            str(column).strip().casefold()
            for column in df.columns
        ]

        if len(normalized_columns) != len(set(normalized_columns)):
            raise ValueError(
                "Duplicate column names found after loading CSV."
            )

        # FIX: Check the original header, not the pandas column name.
        # When the original header is empty, pandas normally renames
        # it to 'Unnamed: 0'. Pass the DataFrame to the removal
        # function so that the column can be checked and removed.
        if header and not str(header[0]).strip():
            df = remove_accidental_index_column(
                df,
                source_file
            )

        # Ensure useful columns remain.
        if len(df.columns) == 0:
            raise ValueError(
                "No columns remain after CSV validation."
            )

        # Create the destination directory.
        output_dir = BRONZE_DIR / dataset_name
        output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        output_file = output_dir / "data.parquet"
        temporary_file = output_dir / "data.tmp.parquet"

        try:
            # Write to a temporary file first.
            df.to_parquet(
                temporary_file,
                index=False
            )

            # Replace the final output after a successful write.
            temporary_file.replace(output_file)

        finally:
            # Remove any leftover temporary file.
            if temporary_file.exists():
                temporary_file.unlink()

        # Update the result with both old and new field names
        # to remain compatible with the existing unit tests.
        result.update({
            "status": "SUCCESS",
            "rows": len(df),
            "row_count": len(df),
            "columns": len(df.columns),
            "column_count": len(df.columns),
            "output": str(output_file),
        })

        logger.info(
            "Successfully ingested %s: rows=%s, columns=%s, output=%s",
            dataset_name,
            len(df),
            len(df.columns),
            output_file
        )

    except Exception as exc:
        result["error"] = str(exc)

        logger.exception(
            "Ingestion failed for %s: %s",
            dataset_name,
            exc
        )

    finally:
        result["finished_at"] = datetime.now(
            timezone.utc
        ).isoformat()

    return result


# ==================================================
# 7. RUN ALL DATASETS
# ==================================================

def run_ingestion() -> list[dict]:
    """Ingest all configured datasets and print a summary."""

    logger.info("=" * 60)
    logger.info("CSV INGESTION PIPELINE STARTED")
    logger.info("=" * 60)

    results = []

    for dataset_name, source_file in DATASETS.items():
        result = ingest_csv(
            dataset_name,
            source_file
        )
        results.append(result)

    successful = sum(
        result["status"] == "SUCCESS"
        for result in results
    )

    failed = len(results) - successful

    logger.info("=" * 60)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 60)

    for result in results:
        logger.info(
            "%s | status=%s | rows=%s | columns=%s",
            result["dataset"],
            result["status"],
            result["row_count"],
            result["column_count"]
        )

        if result["error"]:
            logger.error(
                "%s error: %s",
                result["dataset"],
                result["error"]
            )

    logger.info(
        "Total datasets: %s | Successful: %s | Failed: %s",
        len(results),
        successful,
        failed
    )

    logger.info("CSV INGESTION PIPELINE FINISHED")

    return results


# ==================================================
# 8. SCRIPT ENTRY POINT
# ==================================================

if __name__ == "__main__":
    ingestion_results = run_ingestion()

    if any(
        result["status"] != "SUCCESS"
        for result in ingestion_results
    ):
        sys.exit(1)

