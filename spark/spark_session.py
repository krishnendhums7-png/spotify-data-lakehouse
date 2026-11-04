"""SparkSession factory.

All Spark tuning lives here so every stage (and the tests) get an identical session.
"""
from __future__ import annotations

import os
import shutil
import sys

from pyspark.sql import SparkSession

from utils.config import PipelineConfig
from utils.logger import get_logger

log = get_logger(__name__)


def _check_java() -> None:
    """Fail early with a helpful message instead of a cryptic Py4J error."""
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        raise RuntimeError(
            "Java was not found. Spark needs a JDK (17 or 21). Install it and make sure "
            "`java -version` works (or set JAVA_HOME). See README -> Installation."
        )


def get_spark(config: PipelineConfig, app_name: str = "spotify-lakehouse") -> SparkSession:
    """Create (or reuse) a local SparkSession configured for this project."""
    _check_java()
    # Make the Python workers use the *same* interpreter as the driver (important on Windows).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName(app_name)
        .master(config.spark_master)
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.driver.bindAddress", "127.0.0.1")   # avoids hostname problems on WSL/VPN
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")   # keeps log files readable
        # Data is small: the default 200 shuffle partitions would create 200 tiny tasks/files.
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")       # AQE coalesces small shuffle partitions
        # Lenient casts: an unparseable value becomes NULL (we detect and report it ourselves)
        # instead of crashing the job. Also makes behaviour identical on Spark 3.5 and 4.x.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark %s started (master=%s, shuffle_partitions=%s)",
             spark.version, config.spark_master, config.shuffle_partitions)
    return spark
