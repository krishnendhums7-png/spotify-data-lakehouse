import pandas as pd
from ingestion import csv_ingestion


def test_ingest_csv_success(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir()

    source_file = raw_dir / "tracks.csv"

    pd.DataFrame({
        "track_id": ["T001", "T002"],
        "track_name": ["Song A", "Song B"],
    }).to_csv(source_file, index=False)

    monkeypatch.setattr(
        csv_ingestion, "BRONZE_DIR", bronze_dir
    )

    result = csv_ingestion.ingest_csv(
        "tracks_test", source_file
    )

    assert result["status"] == "SUCCESS"
    assert result["row_count"] == 2
    assert result["column_count"] == 2

    output_file = bronze_dir / "tracks_test" / "data.parquet"
    assert output_file.exists()

    output_df = pd.read_parquet(output_file)

    assert len(output_df) == 2
    assert list(output_df.columns) == ["track_id", "track_name"]


def test_missing_source_file(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"

    monkeypatch.setattr(
        csv_ingestion, "BRONZE_DIR", bronze_dir
    )

    missing_file = tmp_path / "missing.csv"

    result = csv_ingestion.ingest_csv(
        "missing_test", missing_file
    )

    assert result["status"] == "FAILED"
    assert "Source file not found" in result["error"]


def test_duplicate_column_names(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"

    monkeypatch.setattr(
        csv_ingestion, "BRONZE_DIR", bronze_dir
    )

    source_file = tmp_path / "duplicate.csv"

    source_file.write_text(
        "track_id,track_id\nT001,T002\n",
        encoding="utf-8",
    )

    result = csv_ingestion.ingest_csv(
        "duplicate_test", source_file
    )

    assert result["status"] == "FAILED"
    assert "Duplicate column names" in result["error"]


def test_empty_csv(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"

    monkeypatch.setattr(
        csv_ingestion, "BRONZE_DIR", bronze_dir
    )

    source_file = tmp_path / "empty.csv"
    source_file.write_text("", encoding="utf-8")

    result = csv_ingestion.ingest_csv(
        "empty_test", source_file
    )

    assert result["status"] == "FAILED"
    assert result["error"] is not None