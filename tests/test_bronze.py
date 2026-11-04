"""Bronze layer tests: values are preserved, lineage is added, nothing is lost."""
import csv
import json

import pytest
from pyspark.sql.types import StringType

from spark.bronze_ingestion import (METADATA_COLUMNS, add_ingestion_metadata, ingest_dataset,
                                    read_source_csv, string_schema)
from spark.common import PipelineError
from utils.config import SOURCE_DATASETS, PipelineConfig


def test_read_source_csv_preserves_original_values(spark, tmp_path):
    """Quoted commas, 'NA', scientific notation and leading zeros must come through untouched."""
    p = tmp_path / "x.csv"
    p.write_text('id,name,val\n001,"Smith, J",1.01e-06\n002,NA,\n', encoding="utf-8")
    df = read_source_csv(spark, p, ["id", "name", "val"])
    rows = {r["id"]: r for r in df.collect()}
    assert rows["001"]["name"] == "Smith, J"
    assert rows["001"]["val"] == "1.01e-06"
    assert rows["002"]["name"] == "NA"        # NOT turned into NULL
    assert rows["002"]["val"] is None         # an empty CSV cell is NULL
    assert all(isinstance(f.dataType, StringType) for f in df.schema.fields)


def test_read_source_csv_failfast_on_malformed_row(spark, tmp_path):
    p = tmp_path / "bad.csv"
    p.write_text("a,b\n1,2,3,4\n", encoding="utf-8")
    with pytest.raises(Exception):
        read_source_csv(spark, p, ["a", "b"]).collect()


def test_add_ingestion_metadata_keeps_source_columns(spark, tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")
    df = add_ingestion_metadata(read_source_csv(spark, p, ["a", "b"]), "RUN1")
    assert df.columns == ["a", "b"] + METADATA_COLUMNS
    row = df.first()
    assert (row["a"], row["b"], row["_source_file"], row["_run_id"]) == ("1", "2", "x.csv", "RUN1")


def test_header_drift_fails_loudly(spark, tmp_path):
    cfg = PipelineConfig.from_env(tmp_path)
    cfg.ensure_directories()
    # charts file with a wrong header
    (cfg.raw_dir / "charts_sample.csv").write_text("title,rank,WRONG\n", encoding="utf-8")
    with pytest.raises(PipelineError, match="header drift"):
        ingest_dataset(spark, cfg, "charts")


def test_string_schema_is_all_strings():
    assert all(isinstance(f.dataType, StringType) for f in string_schema(["a", "b"]).fields)


# ------------------------------ integration (real data) ------------------------------
@pytest.mark.integration
def test_bronze_row_counts_match_independent_csv_count(pipeline_run):
    cfg, _ = pipeline_run
    for name, spec in SOURCE_DATASETS.items():
        with (cfg.raw_dir / spec["file"]).open(newline="", encoding="utf-8") as fh:
            expected = sum(1 for _ in csv.reader(fh)) - 1
        m = json.loads((cfg.bronze_dir / "_manifest.json").read_text())[name]
        assert m["source_records"] == m["bronze_records"] == expected


@pytest.mark.integration
def test_bronze_parquet_has_source_and_lineage_columns(pipeline_run, spark):
    cfg, _ = pipeline_run
    for name, spec in SOURCE_DATASETS.items():
        df = spark.read.parquet(str(cfg.bronze_path(name)))
        assert df.columns == spec["columns"] + METADATA_COLUMNS
        assert df.select("_source_file").distinct().first()[0] == spec["file"]


@pytest.mark.integration
def test_bronze_manifest_has_checksums(pipeline_run):
    cfg, _ = pipeline_run
    manifest = json.loads((cfg.bronze_dir / "_manifest.json").read_text())
    assert set(manifest) == set(SOURCE_DATASETS)
    assert all(len(m["source_sha256"]) == 64 for m in manifest.values())
