"""GOLD layer: curated, analytics-ready datasets built from Silver.

Scope note: Member 3 owns the final star schema (fact/dimension tables, surrogate keys, dbt).
These Gold tables are denormalised, well-named *inputs* for that work. They carry the natural
keys Member 3 needs (chart_record_id, track_id, country_iso_alpha3, chart_date_id, ...).

Outputs (data/gold/):
  enriched_chart_data   all chart rows + track + country attributes + date parts (full fidelity)
  music_analytics       curated wide table for BI (one row per chart entry)
  country_analytics     grain: region x chart_type
  artist_analytics      grain: artist
  genre_analytics       grain: genre (catalogue metrics + chart metrics)
  monthly_analytics     grain: year-month x chart_type
"""
from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common import PipelineError, StageResult, TableStat, write_parquet
from utils import config as C
from utils.config import PipelineConfig
from utils.logger import get_logger

log = get_logger(__name__)

AUDIO_FEATURES = ["danceability", "energy", "speechiness", "acousticness", "instrumentalness",
                  "liveness", "valence", "loudness", "tempo"]


# =========================================================================== #
# Reusable building blocks
# =========================================================================== #
def season_expr(month_col):
    """Northern-hemisphere meteorological season (documented assumption)."""
    return (F.when(month_col.isin(12, 1, 2), F.lit("Winter"))
            .when(month_col.isin(3, 4, 5), F.lit("Spring"))
            .when(month_col.isin(6, 7, 8), F.lit("Summer"))
            .otherwise(F.lit("Autumn")))


def add_date_attributes(df: DataFrame, date_col: str = "chart_date") -> DataFrame:
    d = F.col(date_col)
    return (df.withColumn("chart_date_id", F.date_format(d, "yyyyMMdd").cast("int"))
            .withColumn("chart_year", F.year(d))
            .withColumn("chart_quarter", F.quarter(d))
            .withColumn("chart_month", F.month(d))
            .withColumn("chart_year_month", F.date_format(d, "yyyy-MM"))
            .withColumn("chart_day", F.dayofmonth(d))
            .withColumn("chart_day_of_week", F.date_format(d, "EEEE"))
            .withColumn("chart_season", season_expr(F.month(d))))


def add_chart_record_id(df: DataFrame) -> DataFrame:
    """Deterministic natural-key fingerprint of one chart entry (candidate Fact_Chart.chart_id)."""
    return df.withColumn("chart_record_id", F.sha2(F.concat_ws(
        "|", F.date_format("chart_date", "yyyy-MM-dd"), F.col("region"), F.col("chart_type"),
        F.col("chart_rank").cast("string")), 256))


def audio_avgs(prefix: str = "avg_") -> list:
    return [F.round(F.avg(c), 4).alias(f"{prefix}{c}") for c in AUDIO_FEATURES]


def split_artists(df: DataFrame, col: str = "chart_artist") -> DataFrame:
    """One output row per credited artist.

    chart_artist is a ", "-separated credit list. A few real names contain ", " themselves
    (config.PROTECTED_ARTIST_NAMES); their comma is temporarily replaced by '@@' so the split
    does not cut them in two, then restored.
    """
    s = F.col(col)
    for name in C.PROTECTED_ARTIST_NAMES:
        s = F.replace(s, F.lit(name), F.lit(name.replace(", ", "@@")))   # literal (non-regex) replace
    names = F.transform(F.split(s, r",\s*"), lambda x: F.regexp_replace(F.trim(x), "@@", ", "))
    return df.withColumn("artist_name", F.explode(names)).filter(F.col("artist_name") != "")


# =========================================================================== #
# Gold datasets
# =========================================================================== #
def build_gold_enriched(silver_enriched: DataFrame) -> DataFrame:
    return add_chart_record_id(add_date_attributes(silver_enriched))


def build_music_analytics(gold_enriched: DataFrame) -> DataFrame:
    """Curated BI table: one row per chart entry, analysis columns only (no URL / raw noise)."""
    return gold_enriched.select(
        "chart_record_id", "chart_date", "chart_date_id", "chart_year", "chart_quarter", "chart_month",
        "chart_year_month", "chart_season", "chart_type", "chart_rank", "streams", "trend",
        "region", "is_global_region", "country_iso_alpha2", "country_iso_alpha3", "country_continent",
        "country_region", "country_sub_region", "region_match_status",
        "track_id", F.coalesce("track_name", "track_title").alias("track_name"),
        "chart_artist", "track_album_name", "primary_genre", "genres", "track_popularity",
        "track_duration_ms", "track_explicit", *AUDIO_FEATURES, "key", "mode", "time_signature",
        "is_track_matched")


def build_country_analytics(gold_enriched: DataFrame) -> DataFrame:
    g = gold_enriched.groupBy("region", "chart_type").agg(
        F.first("country_iso_alpha2", ignorenulls=True).alias("country_iso_alpha2"),
        F.first("country_iso_alpha3", ignorenulls=True).alias("country_iso_alpha3"),
        F.first("country_continent", ignorenulls=True).alias("country_continent"),
        F.first("country_sub_region", ignorenulls=True).alias("country_sub_region"),
        F.first("is_global_region").alias("is_global_region"),
        F.count(F.lit(1)).alias("chart_entries"),
        F.countDistinct("chart_date").alias("distinct_chart_dates"),
        F.countDistinct("track_id").alias("distinct_tracks"),
        F.countDistinct("chart_artist").alias("distinct_artist_credits"),
        F.min("chart_date").alias("first_chart_date"),
        F.max("chart_date").alias("last_chart_date"),
        F.sum("streams").alias("total_streams"),
        F.round(F.avg("streams"), 2).alias("avg_streams"),
        F.round(F.avg("chart_rank"), 3).alias("avg_chart_rank"),
        F.sum(F.col("is_track_matched").cast("int")).alias("entries_with_track_attributes"),
        F.round(F.avg(F.col("is_track_matched").cast("double")), 4).alias("track_attribute_coverage"),
        F.round(F.avg("track_popularity"), 3).alias("avg_track_popularity"),
        *audio_avgs())
    return g


def build_artist_analytics(gold_enriched: DataFrame) -> DataFrame:
    exploded = split_artists(gold_enriched.select("chart_artist", "chart_date", "chart_type", "chart_rank",
                                                  "streams", "track_id", "region", "is_global_region"))
    return (exploded.withColumn("artist_key", F.lower("artist_name"))
            .groupBy("artist_key").agg(
        F.min("artist_name").alias("artist_name"),
        F.count(F.lit(1)).alias("chart_entries"),
        F.countDistinct("track_id").alias("distinct_tracks"),
        F.countDistinct("chart_date").alias("distinct_chart_dates"),
        F.countDistinct(F.when(~F.col("is_global_region"), F.col("region"))).alias("distinct_countries_charted"),
        F.min("chart_date").alias("first_chart_date"),
        F.max("chart_date").alias("last_chart_date"),
        F.datediff(F.max("chart_date"), F.min("chart_date")).alias("chart_span_days"),
        F.min("chart_rank").alias("best_chart_rank"),
        F.round(F.avg("chart_rank"), 3).alias("avg_chart_rank"),
        F.sum(F.when(F.col("chart_rank") == 1, 1).otherwise(0)).alias("times_ranked_first"),
        F.sum("streams").alias("total_streams"),
        F.round(F.avg("streams"), 2).alias("avg_streams")))


def build_genre_analytics(silver_tracks: DataFrame, silver_bridge: DataFrame,
                          gold_enriched: DataFrame) -> DataFrame:
    """Catalogue metrics for ALL genres (from tracks) + chart metrics for tracks that charted.

    A track with several genres is counted once in each of its genres, so chart_entries must not
    be summed across genres (documented in the data dictionary)."""
    catalogue = (silver_bridge.join(silver_tracks, "track_id", "inner")
                 .groupBy("genre").agg(
        F.countDistinct("track_id").alias("catalogue_tracks"),
        F.round(F.avg("popularity"), 3).alias("avg_popularity"),
        F.round(F.avg(F.col("explicit").cast("double")), 4).alias("explicit_share"),
        F.round(F.avg("duration_ms") / 60000.0, 3).alias("avg_duration_min"),
        *audio_avgs("catalogue_avg_")))
    charted = (gold_enriched.filter("is_track_matched").select("track_id", "chart_rank", "streams", "region", "chart_date")
               .join(silver_bridge, "track_id", "inner")
               .groupBy("genre").agg(
        F.count(F.lit(1)).alias("chart_entries"),
        F.countDistinct("track_id").alias("charted_tracks"),
        F.countDistinct("region").alias("distinct_regions"),
        F.round(F.avg("chart_rank"), 3).alias("avg_chart_rank"),
        F.sum("streams").alias("total_streams")))
    out = catalogue.join(charted, "genre", "left")
    for c in ("chart_entries", "charted_tracks", "distinct_regions"):
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0)))      # a genre with no charted track = real 0
    return out


def build_monthly_analytics(gold_enriched: DataFrame) -> DataFrame:
    return (gold_enriched.groupBy("chart_year", "chart_month", "chart_year_month", "chart_season", "chart_type")
            .agg(F.count(F.lit(1)).alias("chart_entries"),
                 F.countDistinct("chart_date").alias("distinct_chart_dates"),
                 F.countDistinct("track_id").alias("distinct_tracks"),
                 F.countDistinct("region").alias("distinct_regions"),
                 F.sum("streams").alias("total_streams"),
                 F.round(F.avg("streams"), 2).alias("avg_streams"),
                 F.round(F.avg(F.col("is_track_matched").cast("double")), 4).alias("track_attribute_coverage"),
                 *audio_avgs()))


# =========================================================================== #
# Stage runner
# =========================================================================== #
def run_gold(spark: SparkSession, config: PipelineConfig) -> StageResult:
    t0 = time.time()
    log.info("=== GOLD: curated analytical datasets ===")
    res = StageResult("gold")
    s_enriched = spark.read.parquet(str(config.silver_path("enriched_chart_data")))
    s_tracks = spark.read.parquet(str(config.silver_path("tracks")))
    s_bridge = spark.read.parquet(str(config.silver_path("track_genres")))
    silver_n = s_enriched.count()

    g_enriched = build_gold_enriched(s_enriched).cache()
    gn = g_enriched.count()
    if gn != silver_n:
        raise PipelineError(f"[gold:enriched] rows changed: silver={silver_n} gold={gn}")
    # partitioned by year: 4 partitions here; lets engines prune by year. coalesce(1) keeps 1 file/partition.
    write_parquet(g_enriched, config.gold_path("enriched_chart_data"), partition_by=["chart_year"])
    res.tables.append(TableStat("gold", "enriched_chart_data", silver_n, gn, 0, 0, str(config.gold_path("enriched_chart_data")),
                                "partitioned by chart_year; + date parts + chart_record_id"))

    outputs = {
        "music_analytics": (build_music_analytics(g_enriched), ["chart_year"], "curated wide BI table; one row per chart entry"),
        "country_analytics": (build_country_analytics(g_enriched), None, "grain: region x chart_type"),
        "artist_analytics": (build_artist_analytics(g_enriched), None, "grain: credited artist (comma-split)"),
        "genre_analytics": (build_genre_analytics(s_tracks, s_bridge, g_enriched), None, "grain: genre; catalogue + chart metrics"),
        "monthly_analytics": (build_monthly_analytics(g_enriched), None, "grain: year-month x chart_type"),
    }
    for name, (df, part, note) in outputs.items():
        write_parquet(df, config.gold_path(name), partition_by=part)
        n = spark.read.parquet(str(config.gold_path(name))).count()    # read back
        rows_in = silver_n if name == "music_analytics" else None
        if name == "music_analytics" and n != silver_n:
            raise PipelineError(f"[gold:{name}] rows changed: silver={silver_n} gold={n}")
        log.info("[gold:%s] %s rows  (%s)", name, f"{n:,}", note)
        res.tables.append(TableStat("gold", name, rows_in, n, 0, 0, str(config.gold_path(name)), note))
    g_enriched.unpersist()
    res.seconds = time.time() - t0
    return res
