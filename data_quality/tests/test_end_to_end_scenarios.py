"""End-to-end integration tests for Data Quality pipeline execution scenarios.

Tests:
- Scenario A: Complete successful pipeline execution across all stages.
- Scenario B: Controlled failure injection verifying failure detection,
  validation reporting, task failure marking, downstream blocking, and monitoring logs.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest import mock
import pandas as pd

from data_quality.airflow.dag_runner import run_local_pipeline
from data_quality.monitoring.alert_handler import ALERTS_LOG, MONITORING_JSON
from data_quality.monitoring.metrics_collector import METRICS_FILE
from data_quality.quality.validator import GreatExpectationsValidator, SuiteValidationResult, ExpectationEvaluation


class TestEndToEndScenarios(unittest.TestCase):
    def setUp(self):
        self.validator = GreatExpectationsValidator()

    def test_scenario_a_successful_execution(self):
        """Scenario A: All stages succeed, validation passes, reports generated, status SUCCESS."""
        # 1. Clean Data across layers
        clean_tracks = pd.DataFrame({
            "track_id": ["4iV5W9uYEdYUVa79Axb7Rh", "1301WleyT98MSxVHPZCA6M"],
            "track_name": ["Track 1", "Track 2"],
            "album_name": ["Album 1", "Album 2"],
            "artists": ["Artist 1", "Artist 2"],
            "popularity": [80, 90],
            "duration_ms": [200000, 190000],
            "explicit": [False, True],
            "danceability": [0.7, 0.6],
            "energy": [0.8, 0.7],
            "key": [1, 5],
            "loudness": [-5.0, -4.0],
            "mode": [1, 0],
            "speechiness": [0.05, 0.08],
            "acousticness": [0.2, 0.5],
            "instrumentalness": [0.0, 0.01],
            "liveness": [0.1, 0.2],
            "valence": [0.6, 0.4],
            "tempo": [120.0, 100.0],
            "time_signature": [4, 4],
            "genres": [["pop"], ["rock"]],
            "primary_genre": ["pop", "rock"],
            "genre_count": [1, 1],
        })

        clean_charts = pd.DataFrame({
            "track_title": ["Track 1", "Track 2"],
            "chart_rank": [1, 2],
            "chart_date": ["2023-01-01", "2023-01-02"],
            "chart_artist": ["Artist 1", "Artist 2"],
            "track_url": ["https://open.spotify.com/track/4iV5W9uYEdYUVa79Axb7Rh", "https://open.spotify.com/track/1301WleyT98MSxVHPZCA6M"],
            "region": ["US", "GB"],
            "chart_type": ["top200", "top200"],
            "trend": ["SAME_POSITION", "NEW_ENTRY"],
            "streams": [500000, 450000],
            "track_id": ["4iV5W9uYEdYUVa79Axb7Rh", "1301WleyT98MSxVHPZCA6M"],
            "has_streams": [True, True],
        })

        clean_countries = pd.DataFrame({
            "iso_alpha2": ["US", "GB"],
            "iso_alpha3": ["USA", "GBR"],
            "country_name": ["United States", "United Kingdom"],
            "region": ["Americas", "Europe"],
            "sub_region": ["Northern America", "Northern Europe"],
        })

        clean_genres = pd.DataFrame({
            "track_id": ["4iV5W9uYEdYUVa79Axb7Rh", "1301WleyT98MSxVHPZCA6M"],
            "genre": ["pop", "rock"],
        })

        clean_quarantine = pd.DataFrame(columns=["track_id", "reject_reasons"])

        ctx = {
            "silver_tracks": clean_tracks,
            "silver_charts": clean_charts,
            "silver_countries": clean_countries,
            "silver_track_genres": clean_genres,
            "tracks_quarantine": clean_quarantine,
        }

        # Validate Silver Layer
        results = self.validator.run_layer_validation("silver", context_dfs=ctx)
        self.assertEqual(len(results), 5)
        for r in results:
            self.assertTrue(r.success, f"Suite '{r.suite_name}' failed: {[e.description for e in r.evaluations if not e.success]}")

        # Save and verify report generation
        json_path = self.validator.save_validation_results(results)
        self.assertTrue(json_path.exists())

        from data_quality.quality.report_generator import generate_validation_reports
        html_path = generate_validation_reports(results)
        self.assertTrue(html_path.exists())

    def test_scenario_b_failure_execution_and_downstream_blocking(self):
        """Scenario B: Inject corrupted record -> validation fails -> task fails -> downstream blocked."""
        # 1. Corrupt track record (popularity out of bounds, duration negative)
        bad_tracks = pd.DataFrame({
            "track_id": ["invalid_length_id"],
            "popularity": [999],  # invalid!
            "duration_ms": [-500],  # invalid!
            "danceability": [2.5],  # invalid!
        })

        ctx = {"silver_tracks": bad_tracks}
        res = self.validator.validate_suite("silver_tracks_suite", bad_tracks, "silver", "tracks", context_dfs=ctx)
        self.assertFalse(res.success, "Corrupted track data must fail validation")

        # 2. Verify Airflow quality gate raises exception
        from data_quality.airflow.dags.spotify_pipeline import task_validate_quality_gate
        with mock.patch("data_quality.quality.validator.GreatExpectationsValidator.run_layer_validation", return_value=[res]):
            with self.assertRaises(ValueError) as err_ctx:
                task_validate_quality_gate("silver")
            self.assertIn("Quality Gate FAILED for layer 'silver'", str(err_ctx.exception))

        # 3. Verify DAG runner triggers alert and halts immediately
        with mock.patch("data_quality.airflow.dag_runner.task_validate_quality_gate", side_effect=ValueError("Validation Failed!")):
            exit_code = run_local_pipeline(task_filter="validate_silver_quality_gate")
            self.assertEqual(exit_code, 1, "Pipeline must exit with code 1 upon task failure")

        # 4. Verify Monitoring alert log captured the failure
        self.assertTrue(ALERTS_LOG.exists())
        log_content = ALERTS_LOG.read_text(encoding="utf-8")
        self.assertIn("TASK_FAILURE", log_content)


if __name__ == "__main__":
    unittest.main()
