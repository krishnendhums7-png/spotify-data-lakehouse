"""Single entry point:   python -m spark.run_pipeline

Runs  raw CSV -> Bronze -> Silver -> Gold -> Verify  and prints a row-count summary.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from spark.bronze_ingestion import run_bronze
from spark.bronze_to_silver import run_silver
from spark.common import PipelineError, StageResult
from spark.silver_to_gold import run_gold
from spark.spark_session import get_spark
from spark.verify_outputs import run_verify
from utils.config import PipelineConfig
from utils.logger import get_logger, setup_logging

log = get_logger(__name__)

STAGES = {"bronze": run_bronze, "silver": run_silver, "gold": run_gold, "verify": run_verify}


def format_summary(results: list[StageResult]) -> str:
    lines = ["", "=" * 112, f"{'LAYER':<7}{'DATASET':<24}{'ROWS IN':>10}{'ROWS OUT':>11}{'QUARANTINED':>13}{'DUPS REMOVED':>14}  NOTE",
             "-" * 112]
    for r in results:
        for t in r.tables:
            rin = "-" if t.rows_in is None else f"{t.rows_in:,}"
            lines.append(f"{t.layer:<7}{t.dataset:<24}{rin:>10}{t.rows_out:>11,}{t.rows_rejected:>13,}{t.rows_dedup_removed:>14,}  {t.note[:46]}")
    lines.append("-" * 112)
    lines.append("Stage times: " + ", ".join(f"{r.stage}={r.seconds:.1f}s" for r in results))
    lines.append("=" * 112)
    return "\n".join(lines)


def write_reports(config: PipelineConfig, results: list[StageResult], status: str, total: float) -> None:
    config.reports_dir.mkdir(parents=True, exist_ok=True)
    payload = {"run_id": config.run_id, "status": status, "total_seconds": round(total, 1),
               "finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "stages": [r.to_dict() for r in results]}
    (config.reports_dir / "run_summary.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md = [f"# Pipeline run summary\n", f"- run_id: `{config.run_id}`", f"- status: **{status}**",
          f"- total time: {total:.1f}s\n", "```", format_summary(results).strip(), "```"]
    (config.reports_dir / "run_summary.md").write_text("\n".join(md), encoding="utf-8")


def run_pipeline(config: PipelineConfig, stages: list[str] | None = None,
                 spark: SparkSession | None = None) -> list[StageResult]:
    """Run the requested stages in order. A caller-supplied SparkSession is left running."""
    stages = stages or list(STAGES)
    config.ensure_directories()
    owns_spark = spark is None
    spark = spark or get_spark(config)
    results: list[StageResult] = []
    t0 = time.time()
    status = "FAILED"
    try:
        for name in stages:
            results.append(STAGES[name](spark, config))
            log.info("Stage '%s' finished in %.1fs", name, results[-1].seconds)
        status = "SUCCESS"
        return results
    except Exception:
        log.exception("Pipeline failed during stage %d/%d", len(results) + 1, len(stages))
        raise
    finally:
        total = time.time() - t0
        print(format_summary(results))
        write_reports(config, results, status, total)
        if owns_spark:
            spark.stop()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Spotify lakehouse pipeline (Member 2): raw -> bronze -> silver -> gold")
    p.add_argument("--data-dir", help="root data folder (default: ./data or $SPOTIFY_DATA_DIR)")
    p.add_argument("--stages", nargs="+", choices=list(STAGES), default=list(STAGES),
                   help="stages to run, in the order given (default: all)")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--shuffle-partitions", type=int, default=None, help="spark.sql.shuffle.partitions (default 8)")
    args = p.parse_args(argv)

    overrides = {"log_level": args.log_level}
    if args.shuffle_partitions:
        overrides["shuffle_partitions"] = args.shuffle_partitions
    config = PipelineConfig.from_env(args.data_dir, **overrides)
    log_file = setup_logging(config.logs_dir, config.log_level, config.run_id)
    log.info("Run %s | data dir: %s | log file: %s", config.run_id, config.data_dir, log_file)
    try:
        run_pipeline(config, args.stages)
    except PipelineError as exc:
        log.error("PIPELINE FAILED: %s", exc)
        return 1
    except Exception as exc:
        log.error("UNEXPECTED FAILURE: %s", exc)
        return 2
    log.info("PIPELINE SUCCEEDED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
