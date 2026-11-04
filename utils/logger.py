"""Central logging set-up.

One call to ``setup_logging`` at the start of the pipeline configures a console
handler and (optionally) a timestamped log file. Every module then just does
``log = get_logger(__name__)``.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
ROOT_LOGGER_NAME = "spotify_pipeline"


def setup_logging(log_dir: Path | None = None, level: str = "INFO",
                  run_id: str = "run") -> Path | None:
    """Configure console (+ file) logging. Returns the log file path if one was created."""
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()  # makes the function safe to call twice (tests, notebooks)
    logger.propagate = False

    formatter = logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    log_file = None
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"pipeline_{run_id}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return log_file


def get_logger(name: str) -> logging.Logger:
    """Return a child logger of the pipeline root logger."""
    short = name.split(".")[-1] if name else "main"
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{short}")
