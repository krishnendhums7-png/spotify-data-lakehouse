"""Declarative Expectation Suite definitions for Spotify Lakehouse (Data Quality).

Covers all 11 suites across Bronze, Silver, Gold, and Marts layers.
Ensures strict expectation mapping; unsupported expectation types NEVER silently pass.
"""
from __future__ import annotations

from typing import Any

# =========================================================================== #
# Domain Constants & Business Rules
# =========================================================================== #

VALID_AUDIO_FEATURES = [
    "danceability", "energy", "speechiness", "acousticness",
    "instrumentalness", "liveness", "valence",
]

POPULARITY_RANGE = {"min_value": 0, "max_value": 100}
AUDIO_FEATURE_RANGE = {"min_value": 0.0, "max_value": 1.0}
LOUDNESS_RANGE = {"min_value": -60.0, "max_value": 10.0}
KEY_RANGE = {"min_value": 0, "max_value": 11}
VALID_MODES = [0, 1]
TEMPO_RANGE = {"min_value": 0.0, "max_value": 300.0}
TIME_SIGNATURE_RANGE = {"min_value": 1, "max_value": 7}
DURATION_MIN_MS = 1000

CHART_MAX_RANKS = {"top200": 200, "viral50": 50}
VALID_TRENDS = ["SAME_POSITION", "MOVE_UP", "MOVE_DOWN", "NEW_ENTRY"]
VALID_SEASONS = ["Winter", "Spring", "Summer", "Autumn"]
DATE_REGEX = r"^\d{4}-\d{2}-\d{2}$"
TRACK_ID_REGEX = r"^[A-Za-z0-9]{22}$"

# =========================================================================== #
# Supported Expectation Registry (Unsupported expectations fail fast)
# =========================================================================== #

SUPPORTED_EXPECTATION_TYPES = {
    "expect_table_columns_to_match_set",
    "expect_table_row_count_to_be_between",
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_unique",
    "expect_compound_columns_to_be_unique",
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_match_regex",
    "expect_column_value_lengths_to_equal",
    "expect_foreign_keys_to_exist",
    "expect_bridge_weights_to_sum_to_one",
    "expect_quarantine_records_to_have_reasons",
    "expect_quarantine_error_rate_to_be_below_threshold",
}

# =========================================================================== #
# Bronze Layer Expectations
# =========================================================================== #

BRONZE_TRACKS_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": [
                "_c0", "track_id", "artists", "album_name", "track_name", "popularity",
                "duration_ms", "explicit", "danceability", "energy", "key", "loudness",
                "mode", "speechiness", "acousticness", "instrumentalness", "liveness",
                "valence", "tempo", "time_signature", "track_genre",
                "_source_file", "_ingested_at", "_run_id",
            ],
            "exact_match": False,
        },
        "meta": {"dimension": "schema", "description": "Bronze tracks must contain raw source columns and lineage attributes"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "track_id"},
        "meta": {"dimension": "nulls", "description": "track_id must not be null in Bronze tracks"},
    },
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1, "max_value": 1000000},
        "meta": {"dimension": "volume", "description": "Bronze tracks must not be empty"},
    },
]

BRONZE_CHARTS_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": [
                "title", "rank", "date", "artist", "url", "region", "chart", "trend", "streams",
                "_source_file", "_ingested_at", "_run_id",
            ],
            "exact_match": False,
        },
        "meta": {"dimension": "schema", "description": "Bronze charts must contain raw source columns and lineage attributes"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "rank"},
        "meta": {"dimension": "nulls", "description": "rank must not be null in Bronze charts"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "date"},
        "meta": {"dimension": "nulls", "description": "date must not be null in Bronze charts"},
    },
]

BRONZE_COUNTRIES_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": [
                "name", "alpha-2", "alpha-3", "country-code", "iso_3166-2", "region",
                "sub-region", "intermediate-region", "region-code", "sub-region-code",
                "intermediate-region-code", "_source_file", "_ingested_at", "_run_id",
            ],
            "exact_match": False,
        },
        "meta": {"dimension": "schema", "description": "Bronze countries must contain ISO and UN M49 columns"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "alpha-3"},
        "meta": {"dimension": "nulls", "description": "alpha-3 country code must not be null in Bronze countries"},
    },
]

# =========================================================================== #
# Silver Layer Expectations
# =========================================================================== #

SILVER_TRACKS_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": [
                "track_id", "track_name", "album_name", "artists", "popularity", "duration_ms",
                "explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness",
                "acousticness", "instrumentalness", "liveness", "valence", "tempo",
                "time_signature", "genres", "primary_genre", "genre_count",
            ],
            "exact_match": False,
        },
        "meta": {"dimension": "schema", "description": "Silver tracks schema must include typed audio features and genre arrays"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "track_id"},
        "meta": {"dimension": "nulls", "description": "track_id is primary key and must never be null"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "track_name"},
        "meta": {"dimension": "nulls", "description": "track_name must be present in Silver (blank names quarantined)"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "artists"},
        "meta": {"dimension": "nulls", "description": "artists string must not be null"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {"column": "track_id"},
        "meta": {"dimension": "duplicates", "description": "track_id must be completely unique after deduplication"},
    },
    {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {"column": "track_id", "regex": TRACK_ID_REGEX},
        "meta": {"dimension": "format", "description": "track_id must be exactly 22 alphanumeric characters"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "popularity", "min_value": 0, "max_value": 100},
        "meta": {"dimension": "ranges", "description": "popularity must be between 0 and 100"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "duration_ms", "min_value": 1000, "max_value": 7200000},
        "meta": {"dimension": "ranges", "description": "duration_ms must be positive (at least 1s)"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "danceability", "min_value": 0.0, "max_value": 1.0},
        "meta": {"dimension": "ranges", "description": "danceability unit interval [0.0, 1.0]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "energy", "min_value": 0.0, "max_value": 1.0},
        "meta": {"dimension": "ranges", "description": "energy unit interval [0.0, 1.0]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "speechiness", "min_value": 0.0, "max_value": 1.0},
        "meta": {"dimension": "ranges", "description": "speechiness unit interval [0.0, 1.0]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "acousticness", "min_value": 0.0, "max_value": 1.0},
        "meta": {"dimension": "ranges", "description": "acousticness unit interval [0.0, 1.0]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "instrumentalness", "min_value": 0.0, "max_value": 1.0},
        "meta": {"dimension": "ranges", "description": "instrumentalness unit interval [0.0, 1.0]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "liveness", "min_value": 0.0, "max_value": 1.0},
        "meta": {"dimension": "ranges", "description": "liveness unit interval [0.0, 1.0]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "valence", "min_value": 0.0, "max_value": 1.0},
        "meta": {"dimension": "ranges", "description": "valence unit interval [0.0, 1.0]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "loudness", "min_value": -60.0, "max_value": 10.0},
        "meta": {"dimension": "ranges", "description": "loudness in valid dB range [-60.0, 10.0]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "key", "min_value": 0, "max_value": 11},
        "meta": {"dimension": "ranges", "description": "pitch key must be in pitch class [0, 11]"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "mode", "value_set": [0, 1]},
        "meta": {"dimension": "ranges", "description": "mode must be 0 (minor) or 1 (major)"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "tempo", "min_value": 0.0, "max_value": 300.0, "allow_null": True},
        "meta": {"dimension": "ranges", "description": "tempo must be > 0 and <= 300 BPM (0 sentinels set to null)"},
    },
]

SILVER_CHARTS_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_table_columns_to_match_set",
        "kwargs": {
            "column_set": [
                "track_title", "chart_rank", "chart_date", "chart_artist", "track_url",
                "region", "chart_type", "trend", "streams", "track_id", "has_streams",
            ],
            "exact_match": False,
        },
        "meta": {"dimension": "schema", "description": "Silver charts must contain standardized column names and extracted track_id"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "chart_date"},
        "meta": {"dimension": "nulls", "description": "chart_date must not be null"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "chart_rank"},
        "meta": {"dimension": "nulls", "description": "chart_rank must not be null"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "region"},
        "meta": {"dimension": "nulls", "description": "chart region must not be null"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "chart_type"},
        "meta": {"dimension": "nulls", "description": "chart_type must not be null"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "track_id"},
        "meta": {"dimension": "nulls", "description": "track_id parsed from URL must not be null"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "chart_rank", "min_value": 1, "max_value": 200},
        "meta": {"dimension": "chart_positions", "description": "chart rank must be between 1 and 200"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "chart_type", "value_set": ["top200", "viral50"]},
        "meta": {"dimension": "chart_positions", "description": "chart_type must be top200 or viral50"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "trend", "value_set": VALID_TRENDS},
        "meta": {"dimension": "chart_positions", "description": "trend must be one of the 4 Spotify trend categories"},
    },
    {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {"column": "chart_date", "regex": DATE_REGEX},
        "meta": {"dimension": "dates", "description": "chart_date must conform to ISO 8601 YYYY-MM-DD"},
    },
    {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "kwargs": {"column_list": ["chart_date", "region", "chart_type", "chart_rank"]},
        "meta": {"dimension": "duplicates", "description": "Primary grain (date, region, chart_type, rank) must be unique"},
    },
]

SILVER_COUNTRIES_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "iso_alpha2"},
        "meta": {"dimension": "nulls", "description": "iso_alpha2 must not be null (Namibia 'NA' recovered)"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "iso_alpha3"},
        "meta": {"dimension": "nulls", "description": "iso_alpha3 must not be null"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {"column": "iso_alpha3"},
        "meta": {"dimension": "duplicates", "description": "iso_alpha3 must be unique primary key"},
    },
    {
        "expectation_type": "expect_column_value_lengths_to_equal",
        "kwargs": {"column": "iso_alpha2", "value": 2},
        "meta": {"dimension": "format", "description": "iso_alpha2 must be exactly 2 characters"},
    },
    {
        "expectation_type": "expect_column_value_lengths_to_equal",
        "kwargs": {"column": "iso_alpha3", "value": 3},
        "meta": {"dimension": "format", "description": "iso_alpha3 must be exactly 3 characters"},
    },
]

SILVER_TRACK_GENRES_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "track_id"},
        "meta": {"dimension": "nulls", "description": "track_id must not be null in genre bridge"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "genre"},
        "meta": {"dimension": "nulls", "description": "genre must not be null in genre bridge"},
    },
    {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "kwargs": {"column_list": ["track_id", "genre"]},
        "meta": {"dimension": "duplicates", "description": "(track_id, genre) pairs must be unique"},
    },
]

# =========================================================================== #
# Gold Layer Expectations
# =========================================================================== #

GOLD_ENRICHED_CHARTS_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "chart_record_id"},
        "meta": {"dimension": "nulls", "description": "chart_record_id SHA-256 key must not be null"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {"column": "chart_record_id"},
        "meta": {"dimension": "duplicates", "description": "chart_record_id must be unique across all chart entries"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "chart_year", "min_value": 2010, "max_value": 2030},
        "meta": {"dimension": "dates", "description": "chart_year must be within 2010..2030"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "chart_month", "min_value": 1, "max_value": 12},
        "meta": {"dimension": "dates", "description": "chart_month must be 1..12"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "chart_season", "value_set": VALID_SEASONS},
        "meta": {"dimension": "dates", "description": "chart_season must be winter, spring, summer, or autumn"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "chart_rank", "min_value": 1, "max_value": 200},
        "meta": {"dimension": "chart_positions", "description": "chart_rank in Gold must be 1..200"},
    },
]

GOLD_COUNTRY_ANALYTICS_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "kwargs": {"column_list": ["region", "chart_type"]},
        "meta": {"dimension": "duplicates", "description": "Country analytics grain (region x chart_type) must be unique"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "chart_entries", "min_value": 1, "max_value": 500000},
        "meta": {"dimension": "ranges", "description": "chart_entries count must be positive"},
    },
]

# =========================================================================== #
# Referential Integrity Expectations (Star Schema)
# =========================================================================== #

REFERENTIAL_INTEGRITY_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_foreign_keys_to_exist",
        "kwargs": {
            "child_dataset": "fact_chart",
            "child_column": "track_key",
            "parent_dataset": "dim_track",
            "parent_column": "track_key",
        },
        "meta": {"dimension": "referential_integrity", "description": "Every track_key in fact_chart must exist in dim_track"},
    },
    {
        "expectation_type": "expect_foreign_keys_to_exist",
        "kwargs": {
            "child_dataset": "fact_chart",
            "child_column": "country_key",
            "parent_dataset": "dim_country",
            "parent_column": "country_key",
        },
        "meta": {"dimension": "referential_integrity", "description": "Every country_key in fact_chart must exist in dim_country"},
    },
    {
        "expectation_type": "expect_foreign_keys_to_exist",
        "kwargs": {
            "child_dataset": "bridge_track_artist",
            "child_column": "track_key",
            "parent_dataset": "dim_track",
            "parent_column": "track_key",
        },
        "meta": {"dimension": "referential_integrity", "description": "Every track_key in bridge_track_artist must exist in dim_track"},
    },
    {
        "expectation_type": "expect_bridge_weights_to_sum_to_one",
        "kwargs": {
            "dataset": "bridge_track_artist",
            "group_column": "track_key",
            "weight_column": "credit_weight",
        },
        "meta": {"dimension": "referential_integrity", "description": "Artist bridge credit_weight must sum strictly to 1.0 per track"},
    },
    {
        "expectation_type": "expect_bridge_weights_to_sum_to_one",
        "kwargs": {
            "dataset": "bridge_track_genre",
            "group_column": "track_key",
            "weight_column": "weighting_factor",
        },
        "meta": {"dimension": "referential_integrity", "description": "Genre bridge weighting_factor must sum strictly to 1.0 per track"},
    },
]

# =========================================================================== #
# Quarantine Isolation Expectations
# =========================================================================== #

QUARANTINE_ISOLATION_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_quarantine_records_to_have_reasons",
        "kwargs": {
            "dataset": "tracks_quarantine",
            "reason_column": "reject_reasons",
        },
        "meta": {"dimension": "quarantine", "description": "Quarantined tracks must document non-empty reject reasons"},
    },
    {
        "expectation_type": "expect_quarantine_error_rate_to_be_below_threshold",
        "kwargs": {
            "dataset": "tracks_quarantine",
            "total_dataset": "silver_tracks",
            "threshold_pct": 0.1,
        },
        "meta": {"dimension": "quarantine", "description": "Quarantine rate must not breach error budget (0.1%)"},
    },
]

ALL_SUITES: dict[str, list[dict[str, Any]]] = {
    "bronze_tracks_suite": BRONZE_TRACKS_EXPECTATIONS,
    "bronze_charts_suite": BRONZE_CHARTS_EXPECTATIONS,
    "bronze_countries_suite": BRONZE_COUNTRIES_EXPECTATIONS,
    "silver_tracks_suite": SILVER_TRACKS_EXPECTATIONS,
    "silver_charts_suite": SILVER_CHARTS_EXPECTATIONS,
    "silver_countries_suite": SILVER_COUNTRIES_EXPECTATIONS,
    "silver_track_genres_suite": SILVER_TRACK_GENRES_EXPECTATIONS,
    "gold_enriched_charts_suite": GOLD_ENRICHED_CHARTS_EXPECTATIONS,
    "gold_country_analytics_suite": GOLD_COUNTRY_ANALYTICS_EXPECTATIONS,
    "referential_integrity_suite": REFERENTIAL_INTEGRITY_EXPECTATIONS,
    "quarantine_isolation_suite": QUARANTINE_ISOLATION_EXPECTATIONS,
}
