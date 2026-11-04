"""Small helpers shared by all pipeline stages."""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from utils.logger import get_logger

log = get_logger(__name__)


class PipelineError(RuntimeError):
    """Raised when a stage cannot complete or a reconciliation check fails."""


@dataclass
class TableStat:
    """Row accounting for one output dataset."""
    layer: str
    dataset: str
    rows_in: int | None
    rows_out: int
    rows_rejected: int = 0
    rows_dedup_removed: int = 0
    path: str = ""
    note: str = ""


@dataclass
class StageResult:
    stage: str
    seconds: float = 0.0
    tables: list[TableStat] = field(default_factory=list)
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"stage": self.stage, "seconds": round(self.seconds, 2),
                "tables": [asdict(t) for t in self.tables], "details": self.details}


def to_snake_case(name: str) -> str:
    """'sub-region' -> 'sub_region', 'Unnamed: 0' -> 'unnamed_0'."""
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_")
    return name.lower()


def blank_to_null(col: Column) -> Column:
    """Trim a string column and turn empty strings into NULL."""
    trimmed = F.trim(col)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def write_parquet(df: DataFrame, path: Path, partition_by: list[str] | None = None,
                  num_files: int | None = 1) -> None:
    """Write a DataFrame as Parquet (overwrite = idempotent re-runs).

    ``coalesce`` is a *narrow* operation (no shuffle) used only to avoid dozens of tiny files
    on this small data set. For big data, raise/remove ``num_files`` and let Spark decide.
    """
    out = df.coalesce(num_files) if num_files else df
    writer = out.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(str(path))
    log.info("Wrote Parquet -> %s", path)


def reason_array(*conditions: Column) -> Column:
    """Build an array<string> of failure reasons, dropping the NULL (= rule passed) entries."""
    return F.filter(F.array(*conditions), lambda x: x.isNotNull())


def cast_with_audit(df: DataFrame, mapping: dict[str, str], out_col: str = "_cast_errors") -> DataFrame:
    """Cast columns to the target types and record which values could not be parsed.

    A value that is non-NULL in the source but NULL after the cast is an *unparseable* value.
    We keep a list of those columns per row so bad data is rejected visibly, not silently nulled.
    """
    checks = [
        F.when(F.col(c).isNotNull() & F.col(c).cast(t).isNull(), F.lit(f"unparseable_{c}"))
        for c, t in mapping.items()
    ]
    df = df.withColumn(out_col, reason_array(*checks))
    for c, t in mapping.items():
        df = df.withColumn(c, F.col(c).cast(t))
    return df
