"""BRONZE layer: raw CSV  ->  Parquet, without changing any source value.

Design decisions
----------------
* Every source column is read as STRING (explicit schema, no inference). Inference could
  silently change values (e.g. "1.01e-06" -> double, "NA" -> null, "0012" -> 12). Bronze must
  mirror the source; typing happens in Silver where it is validated and audited.
* The header of each CSV is checked against the expected header (schema drift = hard failure).
* Reading uses FAILFAST: a malformed row stops the job instead of being silently dropped.
* Three metadata columns are *added* (never replacing source data):
  _source_file, _ingested_at, _run_id.
* Record counts are verified twice with two independent parsers: Python's csv module
  (source of truth) vs Spark reading the Bronze Parquet back.
* A manifest (_manifest.json) records file size, SHA-256 and counts for traceability.
"""
from __future__ import annotations

import csv
import hashlib
import json
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark.common import PipelineError, StageResult, TableStat, write_parquet
from utils.config import PROJECT_ROOT, SOURCE_DATASETS, PipelineConfig
from utils.logger import get_logger

log = get_logger(__name__)

METADATA_COLUMNS = ["_source_file", "_ingested_at", "_run_id"]


def stage_raw_data(config: PipelineConfig) -> None:
    """Make sure the three CSVs are in data/raw. If not, extract them from Dataset.zip."""
    missing = [d for d in SOURCE_DATASETS if not config.raw_file(d).exists()]
    if not missing:
        return
    candidates = [config.raw_dir / "Dataset.zip", config.data_dir / "Dataset.zip",
                  PROJECT_ROOT / "Dataset.zip"]
    zip_path = next((p for p in candidates if p.exists()), None)
    if zip_path is None:
        raise PipelineError(
            f"Missing raw files {[SOURCE_DATASETS[m]['file'] for m in missing]} in {config.raw_dir}. "
            f"Copy the CSVs there, or place Dataset.zip next to the project / in data/raw."
        )
    log.info("Extracting raw CSVs from %s", zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        wanted = {SOURCE_DATASETS[m]["file"] for m in missing}
        for member in zf.namelist():
            name = Path(member).name           # flatten any folder inside the zip (and avoids path tricks)
            if name in wanted:
                (config.raw_dir / name).write_bytes(zf.read(member))
    still_missing = [d for d in SOURCE_DATASETS if not config.raw_file(d).exists()]
    if still_missing:
        raise PipelineError(f"Dataset.zip did not contain: {still_missing}")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _read_header_and_count(path: Path) -> tuple[list[str], int]:
    """Independent check using Python's csv module: header + number of data records."""
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        n = sum(1 for _ in reader)
    return header, n


def string_schema(columns: list[str]) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in columns])


def read_source_csv(spark: SparkSession, path: Path, columns: list[str]) -> DataFrame:
    """Read a CSV with an explicit all-string schema, keeping every value exactly as written."""
    return (
        spark.read.format("csv")
        .option("header", True)                 # first line is the header and is skipped
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')                  # RFC-4180: a quote inside a quoted field is doubled
        .option("mode", "FAILFAST")
        .schema(string_schema(columns))
        .load(str(path))
    )


def add_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Append lineage columns. Source columns are untouched."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )


def ingest_dataset(spark: SparkSession, config: PipelineConfig, name: str) -> tuple[TableStat, dict]:
    spec = SOURCE_DATASETS[name]
    src = config.raw_file(name)
    log.info("[bronze:%s] reading %s (%.1f MB)", name, src.name, src.stat().st_size / 1e6)

    header, expected_rows = _read_header_and_count(src)
    if header != spec["csv_header"]:
        raise PipelineError(f"[bronze:{name}] header drift. expected={spec['csv_header']} found={header}")

    df = add_ingestion_metadata(read_source_csv(spark, src, spec["columns"]), config.run_id)
    out = config.bronze_path(name)
    write_parquet(df, out, num_files=1)

    # Read back from disk: proves the Parquet is valid and complete.
    bronze_rows = spark.read.parquet(str(out)).count()
    if bronze_rows != expected_rows:
        raise PipelineError(f"[bronze:{name}] row loss! source={expected_rows} bronze={bronze_rows}")
    log.info("[bronze:%s] source records=%s -> bronze records=%s  OK", name, f"{expected_rows:,}", f"{bronze_rows:,}")

    manifest = {
        "source_file": src.name,
        "source_size_bytes": src.stat().st_size,
        "source_sha256": _sha256(src),
        "source_records": expected_rows,
        "bronze_records": bronze_rows,
        "columns": spec["columns"] + METADATA_COLUMNS,
        "bronze_path": str(out.relative_to(config.data_dir)),
        "run_id": config.run_id,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    stat = TableStat("bronze", name, rows_in=expected_rows, rows_out=bronze_rows, path=str(out),
                     note="all columns stored as string; + lineage columns")
    return stat, manifest


def run_bronze(spark: SparkSession, config: PipelineConfig) -> StageResult:
    t0 = time.time()
    log.info("=== BRONZE: raw CSV -> Parquet ===")
    stage_raw_data(config)
    result = StageResult("bronze")
    manifest: dict[str, dict] = {}
    for name in SOURCE_DATASETS:
        try:
            stat, m = ingest_dataset(spark, config, name)
        except PipelineError:
            raise
        except Exception as exc:  # wrap unexpected Spark/IO errors with context
            raise PipelineError(f"[bronze:{name}] ingestion failed: {exc}") from exc
        result.tables.append(stat)
        manifest[name] = m
    config.bronze_dir.mkdir(parents=True, exist_ok=True)
    (config.bronze_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    result.seconds = time.time() - t0
    return result
