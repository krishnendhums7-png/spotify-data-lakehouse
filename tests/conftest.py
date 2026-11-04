"""Shared fixtures.

* ``spark``        - one small local SparkSession for the whole test run.
* ``pipeline_run`` - runs the FULL pipeline once (real CSVs from data/raw) into a temp folder;
                     integration tests read its outputs. Skipped if the CSVs are not present.
"""
from __future__ import annotations

import shutil

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from utils.config import PROJECT_ROOT, SOURCE_DATASETS, PipelineConfig


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from spark.spark_session import get_spark
    cfg = PipelineConfig.from_env(tmp_path_factory.mktemp("sparkcfg"), shuffle_partitions=2, spark_master="local[2]")
    session = get_spark(cfg, app_name="spotify-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def pipeline_run(spark, tmp_path_factory):
    raw_src = PROJECT_ROOT / "data" / "raw"
    if not all((raw_src / v["file"]).exists() for v in SOURCE_DATASETS.values()):
        pytest.skip("real CSVs not found in data/raw")
    from spark.run_pipeline import run_pipeline
    cfg = PipelineConfig.from_env(tmp_path_factory.mktemp("lake"), shuffle_partitions=4, spark_master="local[2]")
    cfg.ensure_directories()
    for v in SOURCE_DATASETS.values():
        shutil.copy(raw_src / v["file"], cfg.raw_dir / v["file"])
    results = run_pipeline(cfg, spark=spark)
    return cfg, {r.stage: r for r in results}


# ---------- helpers to build small *Bronze-shaped* DataFrames (all strings) ----------
def string_df(spark, columns, rows):
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(rows, schema)


TRACK_DEFAULTS = {
    "_c0": "0", "track_id": "A" * 22, "artists": "X;Y", "album_name": "Alb", "track_name": "Song",
    "popularity": "50", "duration_ms": "200000", "explicit": "False", "danceability": "0.5", "energy": "0.5",
    "key": "1", "loudness": "-6.0", "mode": "1", "speechiness": "0.05", "acousticness": "0.1",
    "instrumentalness": "0.0", "liveness": "0.1", "valence": "0.5", "tempo": "120.0",
    "time_signature": "4", "track_genre": "pop",
}
CHART_DEFAULTS = {
    "title": "Song", "rank": "1", "date": "2017-01-01", "artist": "X", "region": "Argentina",
    "url": "https://open.spotify.com/track/" + "A" * 22, "chart": "top200", "trend": "SAME_POSITION",
    "streams": "1000.0",
}
COUNTRY_DEFAULTS = {
    "name": "Testland", "alpha-2": "TL", "alpha-3": "TST", "country-code": "999", "iso_3166-2": "ISO 3166-2:TL",
    "region": "Europe", "sub-region": "Northern Europe", "intermediate-region": None, "region-code": "150.0",
    "sub-region-code": "154.0", "intermediate-region-code": None,
}


def make_bronze(spark, dataset, defaults, rows):
    cols = SOURCE_DATASETS[dataset]["columns"]
    data = [tuple({**defaults, **r}.get(c) for c in cols) for r in rows]
    return string_df(spark, cols, data)
