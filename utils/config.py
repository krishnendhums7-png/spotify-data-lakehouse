"""Configuration for the Spotify Lakehouse pipeline (Member 2 module).

Everything that might change between machines (paths, Spark sizing) lives here
and can be overridden with environment variables or CLI flags. Business rules
(valid ranges, region aliases) are also kept here so they are documented in one
place and are easy to unit-test.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Project root is computed from this file's location -> no machine specific paths.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# --------------------------------------------------------------------------- #
# Source datasets: file name + the exact header we expect (checked in Bronze)
# --------------------------------------------------------------------------- #
# NOTE: the first header cell of tracks.csv is an EMPTY string (a pandas-exported
# index). Spark names such a column "_c0", so we use that name everywhere.
SOURCE_DATASETS: dict[str, dict] = {
    "tracks": {
        "file": "tracks.csv",
        "columns": ["_c0", "track_id", "artists", "album_name", "track_name", "popularity",
                    "duration_ms", "explicit", "danceability", "energy", "key", "loudness",
                    "mode", "speechiness", "acousticness", "instrumentalness", "liveness",
                    "valence", "tempo", "time_signature", "track_genre"],
        "csv_header": ["", "track_id", "artists", "album_name", "track_name", "popularity",
                       "duration_ms", "explicit", "danceability", "energy", "key", "loudness",
                       "mode", "speechiness", "acousticness", "instrumentalness", "liveness",
                       "valence", "tempo", "time_signature", "track_genre"],
    },
    "charts": {
        "file": "charts_sample.csv",
        "columns": ["title", "rank", "date", "artist", "url", "region", "chart", "trend", "streams"],
        "csv_header": ["title", "rank", "date", "artist", "url", "region", "chart", "trend", "streams"],
    },
    "countries": {
        "file": "country_codes.csv",
        "columns": ["name", "alpha-2", "alpha-3", "country-code", "iso_3166-2", "region",
                    "sub-region", "intermediate-region", "region-code", "sub-region-code",
                    "intermediate-region-code"],
        "csv_header": ["name", "alpha-2", "alpha-3", "country-code", "iso_3166-2", "region",
                       "sub-region", "intermediate-region", "region-code", "sub-region-code",
                       "intermediate-region-code"],
    },
}

# --------------------------------------------------------------------------- #
# Business / validation rules (documented in docs/transformations.md)
# --------------------------------------------------------------------------- #
TRACK_ID_REGEX = r"^[A-Za-z0-9]{22}$"            # Spotify base-62 track id
SPOTIFY_TRACK_URL_REGEX = r"open\.spotify\.com/(?:intl-[a-zA-Z-]+/)?track/([A-Za-z0-9]{22})"

POPULARITY_RANGE = (0, 100)
UNIT_INTERVAL_FEATURES = ["danceability", "energy", "speechiness", "acousticness",
                          "instrumentalness", "liveness", "valence"]   # must be in [0, 1]
LOUDNESS_RANGE_DB = (-60.0, 10.0)                 # Spotify documents roughly -60..0 dB
KEY_RANGE = (0, 11)                               # pitch class
VALID_MODES = (0, 1)
TEMPO_RANGE_EXCLUSIVE_LOW = 0.0                   # tempo must be > 0 ...
TEMPO_MAX = 300.0                                 # ... and <= 300 BPM, otherwise set to NULL
TIME_SIGNATURE_RANGE = (1, 7)                     # 0 is a "not detected" sentinel -> NULL

CHART_MAX_RANK = {"top200": 200, "viral50": 50}
MIN_CHART_DATE = "2010-01-01"                     # Spotify charts do not pre-date this
VALID_CHART_TYPES = tuple(CHART_MAX_RANK)

# Chart "region" values that are written differently from the ISO country table.
# Each alias was verified against country_codes.csv; the pipeline warns if a target is missing.
REGION_ALIASES: dict[str, str] = {
    "Bolivia": "Bolivia, Plurinational State of",
    "Czech Republic": "Czechia",
    "Netherlands": "Netherlands, Kingdom of the",
    "Taiwan": "Taiwan, Province of China",
    "Turkey": "Türkiye",
    "United Kingdom": "United Kingdom of Great Britain and Northern Ireland",
    "United States": "United States of America",
}
# Regions that are not countries; they are kept in the data but never joined to countries.
NON_COUNTRY_REGIONS = ("Global",)

# country_codes.csv has no UN M49 geography for Taiwan (blank region). We fill it from the
# UN M49 standard and FLAG it (geography_source = 'manual_override') so it is never hidden.
GEOGRAPHY_OVERRIDES: dict[str, dict] = {
    "TW": {"region": "Asia", "sub_region": "Eastern Asia",
           "region_code": 142, "sub_region_code": 30},
}

# chart "artist" is a comma separated credit list; these real names contain commas.
PROTECTED_ARTIST_NAMES = ("Earth, Wind & Fire", "Tyler, The Creator")


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved configuration for one pipeline run."""

    data_dir: Path
    run_id: str
    shuffle_partitions: int = 8
    driver_memory: str = "2g"
    spark_master: str = "local[*]"
    log_level: str = "INFO"

    # ---- directories ------------------------------------------------------
    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.silver_dir / "_quarantine"

    @property
    def logs_dir(self) -> Path:
        return PROJECT_ROOT / "logs"

    @property
    def reports_dir(self) -> Path:
        return PROJECT_ROOT / "reports"

    # ---- helpers ----------------------------------------------------------
    def raw_file(self, dataset: str) -> Path:
        return self.raw_dir / SOURCE_DATASETS[dataset]["file"]

    def bronze_path(self, dataset: str) -> Path:
        return self.bronze_dir / dataset

    def silver_path(self, dataset: str) -> Path:
        return self.silver_dir / dataset

    def gold_path(self, dataset: str) -> Path:
        return self.gold_dir / dataset

    def ensure_directories(self) -> None:
        for d in (self.raw_dir, self.bronze_dir, self.silver_dir, self.gold_dir,
                  self.quarantine_dir, self.logs_dir, self.reports_dir):
            d.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls, data_dir: str | os.PathLike | None = None, **overrides) -> "PipelineConfig":
        """Priority: explicit argument > SPOTIFY_DATA_DIR env var > <project>/data."""
        chosen = data_dir or os.environ.get("SPOTIFY_DATA_DIR") or (PROJECT_ROOT / "data")
        return cls(
            data_dir=Path(chosen).expanduser().resolve(),
            run_id=overrides.pop("run_id", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")),
            shuffle_partitions=int(overrides.pop("shuffle_partitions",
                                                 os.environ.get("SPARK_SHUFFLE_PARTITIONS", 8))),
            driver_memory=overrides.pop("driver_memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g")),
            spark_master=overrides.pop("spark_master", os.environ.get("SPARK_MASTER", "local[*]")),
            log_level=overrides.pop("log_level", os.environ.get("LOG_LEVEL", "INFO")),
        )
