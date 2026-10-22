"""Tests for the artist ingestion module."""

import pandas as pd
import pytest

from ingestion import artist_ingestion


def test_artist_ingestion_creates_parquet_file(tmp_path, monkeypatch):
    """Check that artist names are extracted and saved."""

    source_file = tmp_path / "tracks.csv"
    output_dir = tmp_path / "artists"

    pd.DataFrame({
        "artists": [
            "Artist A",
            "Artist B",
            "Artist A",
            "Artist C",
        ]
    }).to_csv(source_file, index=False)

    monkeypatch.setattr(artist_ingestion, "RAW_FILE", source_file)
    monkeypatch.setattr(artist_ingestion, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(
        artist_ingestion,
        "OUTPUT_FILE",
        output_dir / "data.parquet",
    )

    result = artist_ingestion.ingest_artists()

    assert (output_dir / "data.parquet").exists()
    assert len(result) == 3
    assert result["artist_name"].is_unique
    assert set(result["artist_name"]) == {
        "Artist A",
        "Artist B",
        "Artist C",
    }


def test_artist_ingestion_splits_multiple_artists(tmp_path, monkeypatch):
    """Check that semicolon-separated names are split."""

    source_file = tmp_path / "tracks.csv"
    output_dir = tmp_path / "artists"

    pd.DataFrame({
        "artists": [
            "Artist A;Artist B",
            "Artist C",
        ]
    }).to_csv(source_file, index=False)

    monkeypatch.setattr(artist_ingestion, "RAW_FILE", source_file)
    monkeypatch.setattr(artist_ingestion, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(
        artist_ingestion,
        "OUTPUT_FILE",
        output_dir / "data.parquet",
    )

    result = artist_ingestion.ingest_artists()

    assert set(result["artist_name"]) == {
        "Artist A",
        "Artist B",
        "Artist C",
    }


def test_artist_ingestion_fails_when_source_is_missing(
    tmp_path, monkeypatch
):
    """Check that a missing source file raises an error."""

    source_file = tmp_path / "missing_tracks.csv"
    output_dir = tmp_path / "artists"

    monkeypatch.setattr(artist_ingestion, "RAW_FILE", source_file)
    monkeypatch.setattr(artist_ingestion, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(
        artist_ingestion,
        "OUTPUT_FILE",
        output_dir / "data.parquet",
    )

    with pytest.raises(FileNotFoundError):
        artist_ingestion.ingest_artists()

