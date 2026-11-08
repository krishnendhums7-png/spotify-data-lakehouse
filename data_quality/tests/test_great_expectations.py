"""Isolated unit and integration tests for Data Quality Great Expectations validation engine.

Covers:
- All 11 suites execution
- Valid datasets pass
- Invalid datasets fail
- Missing datasets fail explicitly (never silent pass)
- Missing columns detected
- Invalid numerical values detected
- Invalid dates detected
- Referential integrity and bridge weight sum checks
- Unsupported expectations fail fast in strict mode
"""
from __future__ import annotations

import unittest
import pandas as pd

from data_quality.quality.expectations_definitions import (
    ALL_SUITES,
    SUPPORTED_EXPECTATION_TYPES,
)
from data_quality.quality.validator import (
    GreatExpectationsValidator,
    UnsupportedExpectationError,
)


class TestDataQualityGreatExpectations(unittest.TestCase):
    def setUp(self):
        self.validator = GreatExpectationsValidator()

    def test_all_11_suites_registered(self):
        expected_suites = [
            "bronze_tracks_suite", "bronze_charts_suite", "bronze_countries_suite",
            "silver_tracks_suite", "silver_charts_suite", "silver_countries_suite",
            "silver_track_genres_suite", "gold_enriched_charts_suite",
            "gold_country_analytics_suite", "referential_integrity_suite",
            "quarantine_isolation_suite",
        ]
        self.assertEqual(len(ALL_SUITES), 11)
        for s in expected_suites:
            self.assertIn(s, ALL_SUITES, f"Suite '{s}' missing from ALL_SUITES registry")

    def test_clean_silver_tracks_validation(self):
        clean_df = pd.DataFrame({
            "track_id": ["4iV5W9uYEdYUVa79Axb7Rh", "1301WleyT98MSxVHPZCA6M"],
            "track_name": ["Song A", "Song B"],
            "album_name": ["Album A", "Album B"],
            "artists": ["Artist A", "Artist B"],
            "popularity": [85, 92],
            "duration_ms": [215000, 185000],
            "explicit": [False, True],
            "danceability": [0.75, 0.60],
            "energy": [0.82, 0.70],
            "key": [5, 11],
            "loudness": [-5.2, -4.1],
            "mode": [1, 0],
            "speechiness": [0.04, 0.12],
            "acousticness": [0.15, 0.65],
            "instrumentalness": [0.0, 0.05],
            "liveness": [0.10, 0.22],
            "valence": [0.55, 0.38],
            "tempo": [124.0, 95.5],
            "time_signature": [4, 4],
            "genres": [["pop"], ["dance"]],
            "primary_genre": ["pop", "dance"],
            "genre_count": [1, 1],
        })
        res = self.validator.validate_suite("silver_tracks_suite", clean_df, "silver", "tracks")
        self.assertTrue(res.success, f"Expected clean data to pass: {[e.description for e in res.evaluations if not e.success]}")
        self.assertEqual(res.failed_expectations, 0)
        self.assertEqual(res.status, "PASSED")

    def test_missing_dataset_produces_failed_status(self):
        """CRITICAL FIX: Missing datasets must produce failed status, never silent pass."""
        res = self.validator.validate_suite("silver_tracks_suite", None, "silver", "tracks")
        self.assertFalse(res.success, "Missing dataset must produce failed validation status")
        self.assertEqual(res.status, "MISSING_DATASET")
        self.assertGreater(res.failed_expectations, 0)
        self.assertIn("not found on disk", res.evaluations[0].details.get("error", ""))

    def test_missing_columns_detected(self):
        """Missing required columns must cause validation failure."""
        incomplete_df = pd.DataFrame({
            "track_id": ["4iV5W9uYEdYUVa79Axb7Rh"],
            # popularity and other audio features missing
        })
        res = self.validator.validate_suite("silver_tracks_suite", incomplete_df, "silver", "tracks")
        self.assertFalse(res.success)
        self.assertGreater(res.failed_expectations, 0)

    def test_invalid_numerical_values_detected(self):
        """Out of range numerical metrics (popularity > 100, danceability > 1.0) must fail."""
        invalid_nums_df = pd.DataFrame({
            "track_id": ["4iV5W9uYEdYUVa79Axb7Rh"],
            "popularity": [999],  # > 100
            "danceability": [2.5],  # > 1.0
            "loudness": [100.0],  # > 10.0
            "mode": [9],  # not in {0, 1}
        })
        fake_suite = [
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "popularity", "min_value": 0, "max_value": 100}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "danceability", "min_value": 0.0, "max_value": 1.0}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "mode", "value_set": [0, 1]}},
        ]
        evals = self.validator._execute_native_gx_suite(invalid_nums_df, "test_suite", fake_suite)
        for e in evals:
            self.assertFalse(e.success, f"Expected {e.expectation_type} to fail on invalid numerical value")

    def test_invalid_dates_detected(self):
        """Regex date check must fail when date format is invalid."""
        bad_dates_df = pd.DataFrame({
            "chart_date": ["not-a-date", "2023/05/01", "2023-13-40"],
        })
        date_exp = {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {"column": "chart_date", "regex": r"^\d{4}-\d{2}-\d{2}$"},
        }
        res = self.validator.evaluate_expectation(bad_dates_df, date_exp)
        self.assertFalse(res.success, "Invalid date format must fail regex expectation")

    def test_unsupported_expectation_fails_fast(self):
        """CRITICAL FIX: Unsupported expectations must NEVER silently pass."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        fake_exp = {
            "expectation_type": "expect_column_values_to_be_unicorns",
            "kwargs": {"column": "col"},
            "meta": {"dimension": "fake", "description": "Fake expectation"},
        }
        # In strict mode, it raises UnsupportedExpectationError
        with self.assertRaises(UnsupportedExpectationError):
            self.validator.evaluate_expectation(df, fake_exp)

        # In non-strict mode, it records success=False, NEVER success=True!
        lenient_validator = GreatExpectationsValidator(strict_unsupported=False)
        eval_res = lenient_validator.evaluate_expectation(df, fake_exp)
        self.assertFalse(eval_res.success, "Unsupported expectation must not silently pass!")
        self.assertEqual(eval_res.observed_value, "UNSUPPORTED_EXPECTATION_TYPE")

    def test_referential_integrity_and_bridge_weights(self):
        """Custom integration validation: foreign keys and bridge weights."""
        dim_track = pd.DataFrame({"track_key": [101, 102, 103]})
        dim_country = pd.DataFrame({"country_key": [1, 2]})
        fact_chart = pd.DataFrame({
            "chart_record_id": ["c1", "c2"],
            "track_key": [101, 102],
            "country_key": [1, 2],
        })
        bridge_artist = pd.DataFrame({
            "track_key": [101, 101, 102],
            "artist_key": [1, 2, 1],
            "credit_weight": [0.5, 0.5, 1.0],  # Valid: sums to 1.0 per track
        })
        bridge_genre = pd.DataFrame({
            "track_key": [101, 102],
            "genre_key": [1, 2],
            "weighting_factor": [1.0, 1.0],  # Valid
        })

        ctx = {
            "fact_chart": fact_chart,
            "dim_track": dim_track,
            "dim_country": dim_country,
            "bridge_track_artist": bridge_artist,
            "bridge_track_genre": bridge_genre,
        }

        res = self.validator.validate_suite("referential_integrity_suite", fact_chart, "marts", "fact_chart", context_dfs=ctx)
        self.assertTrue(res.success, f"Expected referential integrity to pass: {[e.description for e in res.evaluations if not e.success]}")
        self.assertEqual(res.failed_expectations, 0)
        self.assertEqual(res.engine, "CustomIntegrationValidator")

        # Test foreign key violation
        corrupt_fact = pd.DataFrame({
            "chart_record_id": ["c1", "c2"],
            "track_key": [101, 9999],  # 9999 does not exist in dim_track
            "country_key": [1, 2],
        })
        bad_ctx = dict(ctx)
        bad_ctx["fact_chart"] = corrupt_fact
        bad_res = self.validator.validate_suite("referential_integrity_suite", corrupt_fact, "marts", "fact_chart", context_dfs=bad_ctx)
        self.assertFalse(bad_res.success, "Orphan foreign key must trigger validation failure")

        # Test bridge weight sum violation
        bad_bridge = pd.DataFrame({
            "track_key": [101, 101],
            "artist_key": [1, 2],
            "credit_weight": [0.4, 0.3],  # Sum is 0.7 != 1.0
        })
        bad_bridge_ctx = dict(ctx)
        bad_bridge_ctx["bridge_track_artist"] = bad_bridge
        bad_bridge_res = self.validator.validate_suite("referential_integrity_suite", fact_chart, "marts", "fact_chart", context_dfs=bad_bridge_ctx)
        self.assertFalse(bad_bridge_res.success, "Bridge weights not summing to 1.0 must fail")

    def test_quarantine_isolation_and_budget(self):
        clean_tracks = pd.DataFrame({"track_id": [f"track_{i:04d}" for i in range(10000)]})
        # 5 quarantined rows out of 10,000 = 0.05% <= 0.1% threshold
        quarantine_df = pd.DataFrame({
            "track_id": [f"bad_{i}" for i in range(5)],
            "reject_reasons": ["NULL_ID;OUT_OF_BOUNDS"] * 5,
        })
        ctx = {"silver_tracks": clean_tracks, "tracks_quarantine": quarantine_df}
        res = self.validator.validate_suite("quarantine_isolation_suite", quarantine_df, "silver", "tracks_quarantine", context_dfs=ctx)
        self.assertTrue(res.success)

        # Breach threshold: 25 out of 10,000 = 0.25% > 0.1% threshold
        breach_df = pd.DataFrame({
            "track_id": [f"bad_{i}" for i in range(25)],
            "reject_reasons": ["NULL_ID"] * 25,
        })
        breach_ctx = {"silver_tracks": clean_tracks, "tracks_quarantine": breach_df}
        breach_res = self.validator.validate_suite("quarantine_isolation_suite", breach_df, "silver", "tracks_quarantine", context_dfs=breach_ctx)
        self.assertFalse(breach_res.success, "Quarantine budget breach must fail suite")


if __name__ == "__main__":
    unittest.main()
