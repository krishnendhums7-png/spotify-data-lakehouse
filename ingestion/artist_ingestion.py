"""
Artist Ingestion Module

Extracts unique artist names from the tracks dataset
and stores them as a separate Parquet dataset.
"""

import logging
from pathlib import Path

import pandas as pd


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "tracks.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze" / "artists"
OUTPUT_FILE = OUTPUT_DIR / "data.parquet"
LOG_FILE = PROJECT_ROOT / "data" / "logs" / "ingestion.log"


# Configure logging
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
    force=True,
)

logger = logging.getLogger(__name__)


def ingest_artists():
    """Extract unique artist names and save them as Parquet."""

    logger.info("=" * 50)
    logger.info("ARTIST INGESTION STARTED")

    try:
        # Check whether the source dataset exists
        if not RAW_FILE.exists():
            raise FileNotFoundError(
                f"Source dataset not found: {RAW_FILE}"
            )

        # Read only the artist column to reduce memory usage
        df = pd.read_csv(
            RAW_FILE,
            usecols=["artists"],
            dtype={"artists": "string"},
        )

        # Clean artist names
        df["artists"] = df["artists"].str.strip()

        # Remove missing and empty values
        df = df.dropna(subset=["artists"])
        df = df[df["artists"] != ""]

        # Split cells containing multiple artist names.
        # This dataset commonly separates artists using semicolons.
        df["artist_name"] = df["artists"].str.split(";")
        df = df.explode("artist_name")

        # Clean names after splitting
        df["artist_name"] = df["artist_name"].str.strip()
        df = df.dropna(subset=["artist_name"])
        df = df[df["artist_name"] != ""]

        # Keep each artist name only once
        artists_df = (
            df[["artist_name"]]
            .drop_duplicates()
            .sort_values("artist_name")
            .reset_index(drop=True)
        )

        # Create output directory
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # Save as Parquet
        artists_df.to_parquet(OUTPUT_FILE, index=False)

        logger.info("Artist ingestion completed successfully")
        logger.info("Unique artist names: %s", len(artists_df))
        logger.info("Output file: %s", OUTPUT_FILE)

        return artists_df

    except Exception:
        logger.exception("Artist ingestion failed")
        raise


if __name__ == "__main__":
    result = ingest_artists()
    print("\nArtist ingestion completed.")
    print(f"Unique artist names: {len(result)}")
    print(f"Output file: {OUTPUT_FILE}")

