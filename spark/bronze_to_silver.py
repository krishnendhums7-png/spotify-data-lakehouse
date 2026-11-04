"""SILVER layer: clean, type, validate, de-duplicate and integrate the Bronze data.

Every transformation is a small pure function (DataFrame -> DataFrame) so it can be unit tested.
Bad records are never silently dropped: they go to data/silver/_quarantine/<dataset> together
with the original raw values and a ``reject_reasons`` column.

Row accounting identity enforced for each dataset:
    bronze rows = silver rows + quarantined rows + duplicates removed
"""
from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark.common import (PipelineError, StageResult, TableStat, blank_to_null, cast_with_audit,
                          reason_array, write_parquet)
from spark.bronze_ingestion import METADATA_COLUMNS
from utils import config as C
from utils.config import PipelineConfig, SOURCE_DATASETS
from utils.logger import get_logger

log = get_logger(__name__)


# =========================================================================== #
# Shared helpers
# =========================================================================== #
def _with_raw_struct(bronze: DataFrame, dataset: str) -> DataFrame:
    """Keep the original (string) record in a struct column so quarantine can show raw values."""
    cols = SOURCE_DATASETS[dataset]["columns"]
    return bronze.select(F.struct(*[F.col(f"`{c}`") for c in cols]).alias("_raw"),
                         *[F.col(f"`{c}`") for c in cols])


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Rows with an empty ``reject_reasons`` array are valid, the rest are quarantined."""
    valid = df.filter(F.size("reject_reasons") == 0)
    rejected = (df.filter(F.size("reject_reasons") > 0)
                .select("_raw.*", F.concat_ws(";", "reject_reasons").alias("reject_reasons")))
    return valid, rejected


def _between_or_null(col, lo, hi, reason: str):
    """Reason if value is NULL or outside [lo, hi]; NULL (= pass) otherwise."""
    return F.when(col.isNull() | ~col.between(lo, hi), F.lit(reason))


# =========================================================================== #
# TRACKS
# =========================================================================== #
def confirm_artificial_index(df: DataFrame, col: str = "_c0") -> bool:
    """True if ``col`` is an artificial 0..n-1 row counter (unique, contiguous)."""
    r = df.select(F.count(F.lit(1)).alias("n"),
                  F.countDistinct(col).alias("d"),
                  F.min(F.col(col).cast("long")).alias("mn"),
                  F.max(F.col(col).cast("long")).alias("mx")).first()
    return r["n"] == r["d"] and r["mn"] == 0 and r["mx"] == r["n"] - 1


def standardize_tracks(bronze: DataFrame, drop_index: bool = True) -> DataFrame:
    """Trim, blank->NULL, rename and cast the tracks columns (no rows removed here)."""
    df = _with_raw_struct(bronze, "tracks")
    text_cols = ["track_id", "artists", "album_name", "track_name", "track_genre", "explicit"]
    num_cols = ["popularity", "duration_ms", "key", "mode", "time_signature", "danceability",
                "energy", "loudness", "speechiness", "acousticness", "instrumentalness",
                "liveness", "valence", "tempo"]
    for c in text_cols + num_cols:
        df = df.withColumn(c, blank_to_null(F.col(c)))
    if drop_index:
        df = df.drop("_c0")
    else:
        df = df.withColumnRenamed("_c0", "source_row_id")

    # explicit: "True"/"False" -> boolean
    df = df.withColumn("explicit", F.when(F.lower("explicit").isin("true", "1"), True)
                       .when(F.lower("explicit").isin("false", "0"), False))
    df = cast_with_audit(df, {
        "popularity": "int", "duration_ms": "long", "key": "int", "mode": "int",
        "time_signature": "int", "danceability": "double", "energy": "double",
        "loudness": "double", "speechiness": "double", "acousticness": "double",
        "instrumentalness": "double", "liveness": "double", "valence": "double", "tempo": "double",
    })
    return df


def validate_tracks(df: DataFrame) -> DataFrame:
    """Add ``reject_reasons`` (array<string>). Empty array = record is valid."""
    lo_p, hi_p = C.POPULARITY_RANGE
    rules = [
        F.when(F.col("track_id").isNull() | ~F.col("track_id").rlike(C.TRACK_ID_REGEX), F.lit("invalid_track_id")),
        F.when(F.col("track_name").isNull(), F.lit("missing_track_name")),
        _between_or_null(F.col("popularity"), lo_p, hi_p, "invalid_popularity"),
        F.when(F.col("duration_ms").isNull() | (F.col("duration_ms") <= 0), F.lit("invalid_duration_ms")),
        _between_or_null(F.col("loudness"), *C.LOUDNESS_RANGE_DB, "out_of_range_loudness"),
        # key / mode may be NULL, but if present they must be valid
        F.when(F.col("key").isNotNull() & ~F.col("key").between(*C.KEY_RANGE), F.lit("invalid_key")),
        F.when(F.col("mode").isNotNull() & ~F.col("mode").isin(*C.VALID_MODES), F.lit("invalid_mode")),
    ] + [_between_or_null(F.col(c), 0.0, 1.0, f"out_of_range_{c}") for c in C.UNIT_INTERVAL_FEATURES]
    return df.withColumn("reject_reasons", F.concat(F.col("_cast_errors"), reason_array(*rules))).drop("_cast_errors")


def nullify_invalid_rhythm(df: DataFrame) -> DataFrame:
    """tempo = 0 / time_signature = 0 are 'analysis failed' sentinels, not real measurements.

    We set them to NULL (never impute) and keep boolean flags so analysts can filter on them.
    """
    tempo_ok = (F.col("tempo") > C.TEMPO_RANGE_EXCLUSIVE_LOW) & (F.col("tempo") <= C.TEMPO_MAX)
    ts_ok = F.col("time_signature").between(*C.TIME_SIGNATURE_RANGE)
    return (df.withColumn("has_valid_tempo", F.coalesce(tempo_ok, F.lit(False)))
            .withColumn("has_valid_time_signature", F.coalesce(ts_ok, F.lit(False)))
            .withColumn("tempo", F.when(F.col("has_valid_tempo"), F.col("tempo")))
            .withColumn("time_signature", F.when(F.col("has_valid_time_signature"), F.col("time_signature"))))


def deduplicate_tracks(df: DataFrame) -> DataFrame:
    """One row per track_id.

    The source repeats a track once per genre (same audio features, sometimes a different
    popularity). We keep ONE representative row (highest popularity, then alphabetical genre,
    so the choice is deterministic) and keep the information that would be lost:
      genres (all genre labels), genre_count, source_row_count, popularity_conflict.
    """
    pick = Window.partitionBy("track_id").orderBy(F.col("popularity").desc(), F.col("track_genre").asc_nulls_last())
    whole = Window.partitionBy("track_id")
    return (df.withColumn("_rn", F.row_number().over(pick))
            .withColumn("genres", F.array_sort(F.collect_set("track_genre").over(whole)))
            .withColumn("source_row_count", F.count(F.lit(1)).over(whole))
            .withColumn("popularity_conflict", F.min("popularity").over(whole) != F.max("popularity").over(whole))
            .filter(F.col("_rn") == 1).drop("_rn")
            .withColumn("genre_count", F.size("genres"))
            .withColumnRenamed("track_genre", "primary_genre"))


def add_artist_columns(df: DataFrame) -> DataFrame:
    """tracks.artists is ';' separated -> array + primary artist + count."""
    arr = F.transform(F.split(F.col("artists"), ";"), lambda x: F.trim(x))
    return (df.withColumn("artist_list", F.when(F.col("artists").isNotNull(), arr))
            .withColumn("primary_artist", F.element_at("artist_list", 1))
            .withColumn("artist_count", F.size("artist_list")))


TRACK_SILVER_COLUMNS = [
    "track_id", "track_name", "artists", "artist_list", "primary_artist", "artist_count", "album_name",
    "popularity", "duration_ms", "explicit", "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature",
    "has_valid_tempo", "has_valid_time_signature", "primary_genre", "genres", "genre_count",
    "source_row_count", "popularity_conflict",
]


def build_silver_tracks(bronze: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, dict]:
    """Returns (tracks, track_genres bridge, quarantine, stats)."""
    drop_index = confirm_artificial_index(bronze)
    log.info("[silver:tracks] '_c0' (blank-named first column) is an artificial 0..n-1 index: %s", drop_index)
    validated = validate_tracks(standardize_tracks(bronze, drop_index=drop_index)).cache()
    try:
        valid, rejected = split_valid_rejected(validated)
        valid = nullify_invalid_rhythm(valid)
        bridge = (valid.select("track_id", F.col("track_genre").alias("genre"))
                  .where(F.col("genre").isNotNull()).dropDuplicates())
        tracks = add_artist_columns(deduplicate_tracks(valid)).select(*TRACK_SILVER_COLUMNS)
        stats = {"index_column_dropped": drop_index}
        # materialise before the cache is released
        tracks = tracks.cache(); bridge = bridge.cache(); rejected = rejected.cache()
        stats.update(tracks_rows=tracks.count(), bridge_rows=bridge.count(), rejected_rows=rejected.count(),
                     valid_rows_before_dedup=valid.count())
    finally:
        validated.unpersist()   # tracks/bridge/rejected are already materialised in their own caches
    return tracks, bridge, rejected, stats


# =========================================================================== #
# CHARTS
# =========================================================================== #
def extract_track_id(url_col):
    """Spotify track id (22 base-62 chars) from an open.spotify.com URL; NULL if absent."""
    extracted = F.regexp_extract(url_col, C.SPOTIFY_TRACK_URL_REGEX, 1)
    return F.when(extracted == "", F.lit(None)).otherwise(extracted)


def region_country_name(region_col):
    """Standardised region -> official country name used in country_codes (via REGION_ALIASES)."""
    alias_map = F.create_map(*[x for kv in C.REGION_ALIASES.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    resolved = F.coalesce(F.element_at(alias_map, region_col), region_col)
    return F.when(region_col.isin(*C.NON_COUNTRY_REGIONS), F.lit(None)).otherwise(resolved)


def standardize_charts(bronze: DataFrame) -> DataFrame:
    df = _with_raw_struct(bronze, "charts")
    for c in ["title", "rank", "date", "artist", "url", "region", "chart", "trend", "streams"]:
        df = df.withColumn(c, blank_to_null(F.col(c)))
    df = cast_with_audit(df, {"rank": "int", "date": "date", "streams": "double"})
    region = F.regexp_replace(F.col("region"), r"\s+", " ")          # collapse inner whitespace
    return (df.select(
        "_raw", "_cast_errors",
        F.col("date").alias("chart_date"),
        F.lower("chart").alias("chart_type"),
        region.alias("region"),
        F.col("rank").alias("chart_rank"),
        extract_track_id(F.col("url")).alias("track_id"),
        F.col("title").alias("track_title"),
        F.col("artist").alias("chart_artist"),
        F.upper("trend").alias("trend"),
        F.col("streams").cast("long").alias("streams"),
        F.col("streams").isNotNull().alias("has_streams"),
        F.col("url").alias("track_url"),
    ).withColumn("region_country_name", region_country_name(F.col("region")))
     .withColumn("is_global_region", F.col("region").isin(*C.NON_COUNTRY_REGIONS)))


def validate_charts(df: DataFrame) -> DataFrame:
    max_rank = F.create_map(*[x for kv in C.CHART_MAX_RANK.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    rules = [
        F.when(F.col("chart_date").isNull(), F.lit("missing_date")),
        F.when(F.col("chart_date") > F.current_date(), F.lit("future_date")),
        F.when(F.col("chart_date") < F.lit(C.MIN_CHART_DATE).cast("date"), F.lit("date_too_old")),
        F.when(F.col("chart_type").isNull() | ~F.col("chart_type").isin(*C.VALID_CHART_TYPES), F.lit("unknown_chart_type")),
        F.when(F.col("chart_rank").isNull() | (F.col("chart_rank") < 1)
               | (F.col("chart_rank") > F.coalesce(F.element_at(max_rank, F.col("chart_type")), F.lit(200))),
               F.lit("invalid_rank")),
        F.when(F.col("track_id").isNull(), F.lit("missing_track_id_in_url")),
        F.when(F.col("region").isNull(), F.lit("missing_region")),
        F.when(F.col("streams") < 0, F.lit("negative_streams")),
    ]
    return df.withColumn("reject_reasons", F.concat(F.col("_cast_errors"), reason_array(*rules))).drop("_cast_errors")


def deduplicate_charts(df: DataFrame) -> DataFrame:
    """Two composite keys:
       A) (chart_date, region, chart_type, chart_rank)  - one entry per position per chart-day
       B) (chart_date, region, chart_type, track_id)    - a track appears once per chart-day
    Which duplicate survives is deterministic (highest streams / best rank)."""
    w_a = Window.partitionBy("chart_date", "region", "chart_type", "chart_rank").orderBy(
        F.col("streams").desc_nulls_last(), F.col("track_id"))
    a = df.withColumn("_rn", F.row_number().over(w_a))
    kept_a = a.filter(F.col("_rn") == 1).drop("_rn")
    w_b = Window.partitionBy("chart_date", "region", "chart_type", "track_id").orderBy(
        F.col("chart_rank").asc(), F.col("streams").desc_nulls_last())
    b = kept_a.withColumn("_rn", F.row_number().over(w_b))
    kept_b = b.filter(F.col("_rn") == 1).drop("_rn")
    return kept_b


CHART_SILVER_COLUMNS = ["chart_date", "chart_type", "region", "region_country_name", "is_global_region",
                        "chart_rank", "track_id", "track_title", "chart_artist", "trend", "streams",
                        "has_streams", "track_url"]


def build_silver_charts(bronze: DataFrame) -> tuple[DataFrame, DataFrame, dict]:
    validated = validate_charts(standardize_charts(bronze)).cache()
    valid, rejected = split_valid_rejected(validated)
    valid_n = valid.count()
    charts = deduplicate_charts(valid).select(*CHART_SILVER_COLUMNS).cache()
    rejected = rejected.cache()
    stats = {"valid_rows_before_dedup": valid_n, "charts_rows": charts.count(), "rejected_rows": rejected.count()}
    stats["duplicates_removed"] = valid_n - stats["charts_rows"]
    return charts, rejected, stats


# =========================================================================== #
# COUNTRIES
# =========================================================================== #
def standardize_countries(bronze: DataFrame) -> DataFrame:
    """Rename to snake_case, trim, blank->NULL, cast numeric codes (no rows removed here)."""
    df = _with_raw_struct(bronze, "countries")
    b = lambda name: blank_to_null(F.col(f"`{name}`"))
    df = df.select("_raw",
                   b("name").alias("country_name"),
                   F.upper(b("alpha-2")).alias("iso_alpha2"),   # NB: blank for Namibia in the SOURCE file (see below)
                   F.upper(b("alpha-3")).alias("iso_alpha3"),
                   b("country-code").alias("iso_numeric_code"),
                   b("iso_3166-2").alias("iso_3166_2"),
                   b("region").alias("region"),
                   b("sub-region").alias("sub_region"),
                   b("intermediate-region").alias("intermediate_region"),
                   b("region-code").alias("region_code"),
                   b("sub-region-code").alias("sub_region_code"),
                   b("intermediate-region-code").alias("intermediate_region_code"))
    df = cast_with_audit(df, {"iso_numeric_code": "int", "region_code": "int",
                              "sub_region_code": "int", "intermediate_region_code": "int"})
    return recover_iso_alpha2(df)


def recover_iso_alpha2(df: DataFrame) -> DataFrame:
    """Fill a blank alpha-2 code from the same row's iso_3166_2 value ('ISO 3166-2:NA' -> 'NA').

    Profiling showed country_codes.csv has a blank alpha-2 for Namibia (its code 'NA' was most
    likely read as 'missing' by whatever tool exported the file) while iso_3166_2 still holds it.
    We only use evidence from the same record, and flag every recovered value.
    """
    from_iso = F.regexp_extract(F.col("iso_3166_2"), r"^ISO 3166-2:([A-Z]{2})$", 1)
    from_iso = F.when(from_iso == "", F.lit(None)).otherwise(from_iso)
    return (df.withColumn("iso_alpha2_recovered", F.col("iso_alpha2").isNull() & from_iso.isNotNull())
            .withColumn("iso_alpha2", F.coalesce(F.col("iso_alpha2"), from_iso)))


def apply_geography_overrides(df: DataFrame) -> DataFrame:
    """Fill the UN geography for countries whose region is blank in the source (config-driven, flagged).

    The "is the source blank?" flag is computed ONCE, before any column is overwritten; otherwise
    the first override (region) would make the condition false for the remaining columns.
    """
    out = (df.withColumn("_src_blank", F.col("region").isNull())
           .withColumn("geography_source", F.when(F.col("_src_blank"), F.lit("missing")).otherwise(F.lit("source"))))
    for iso2, vals in C.GEOGRAPHY_OVERRIDES.items():
        cond = (F.col("iso_alpha2") == iso2) & F.col("_src_blank")
        out = out.withColumn("geography_source",
                             F.when(cond, F.lit("manual_override")).otherwise(F.col("geography_source")))
        for col, val in vals.items():
            out = out.withColumn(col, F.when(cond, F.lit(val)).otherwise(F.col(col)))
    # 'continent' = UN M49 top-level region (Africa, Americas, Asia, Europe, Oceania)
    return out.drop("_src_blank").withColumn("continent", F.col("region"))


def validate_countries(df: DataFrame) -> DataFrame:
    rules = [
        F.when(F.col("country_name").isNull(), F.lit("missing_country_name")),
        F.when(F.col("iso_alpha3").isNull() | ~F.col("iso_alpha3").rlike("^[A-Z]{3}$"), F.lit("invalid_iso_alpha3")),
        F.when(F.col("iso_alpha2").isNotNull() & ~F.col("iso_alpha2").rlike("^[A-Z]{2}$"), F.lit("invalid_iso_alpha2")),
    ]
    return df.withColumn("reject_reasons", F.concat(F.col("_cast_errors"), reason_array(*rules))).drop("_cast_errors")


COUNTRY_SILVER_COLUMNS = ["country_name", "iso_alpha2", "iso_alpha3", "iso_numeric_code", "iso_3166_2",
                          "continent", "region", "sub_region", "intermediate_region", "region_code",
                          "sub_region_code", "intermediate_region_code", "geography_source", "has_geography", "iso_alpha2_recovered"]


def build_silver_countries(bronze: DataFrame) -> tuple[DataFrame, DataFrame, dict]:
    validated = validate_countries(standardize_countries(bronze))
    valid, rejected = split_valid_rejected(validated)
    valid = apply_geography_overrides(valid)
    w = Window.partitionBy("iso_alpha3").orderBy(F.col("country_name"))
    deduped = (valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
               .withColumn("has_geography", F.col("region").isNotNull())
               .select(*COUNTRY_SILVER_COLUMNS)).cache()
    rejected = rejected.cache()
    valid_n = valid.count()
    stats = {"valid_rows_before_dedup": valid_n, "countries_rows": deduped.count(),
             "rejected_rows": rejected.count()}
    stats["duplicates_removed"] = valid_n - stats["countries_rows"]
    stats["without_geography"] = deduped.filter(~F.col("has_geography")).count()
    stats["iso_alpha2_recovered"] = deduped.filter("iso_alpha2_recovered").count()
    stats["iso_alpha2_still_missing"] = deduped.filter(F.col("iso_alpha2").isNull()).count()
    return deduped, rejected, stats


# =========================================================================== #
# INTEGRATION  (charts  LEFT JOIN tracks  LEFT JOIN countries)
# =========================================================================== #
TRACK_JOIN_COLUMNS = {
    "track_name": "track_name", "artists": "track_artists", "album_name": "track_album_name",
    "popularity": "track_popularity", "duration_ms": "track_duration_ms", "explicit": "track_explicit",
    "primary_genre": "primary_genre", "genres": "genres", "genre_count": "genre_count",
    "danceability": "danceability", "energy": "energy", "key": "key", "loudness": "loudness",
    "mode": "mode", "speechiness": "speechiness", "acousticness": "acousticness",
    "instrumentalness": "instrumentalness", "liveness": "liveness", "valence": "valence",
    "tempo": "tempo", "time_signature": "time_signature",
}
COUNTRY_JOIN_COLUMNS = {
    "iso_alpha2": "country_iso_alpha2", "iso_alpha3": "country_iso_alpha3",
    "continent": "country_continent", "region": "country_region", "sub_region": "country_sub_region",
    "intermediate_region": "country_intermediate_region", "geography_source": "country_geography_source",
}


def build_enriched_charts(charts: DataFrame, tracks: DataFrame, countries: DataFrame) -> DataFrame:
    """LEFT JOINs keep every chart row. Dimension columns are renamed up-front so there are
    no duplicate / ambiguous column names after the join. Both dimensions are small, so they
    are broadcast (no shuffle of the larger charts table)."""
    t = tracks.select(F.col("track_id").alias("_t_id"),
                      *[F.col(k).alias(v) for k, v in TRACK_JOIN_COLUMNS.items()]
                      ).withColumn("is_track_matched", F.lit(True))
    c = countries.select(F.col("country_name").alias("_c_name"),
                         *[F.col(k).alias(v) for k, v in COUNTRY_JOIN_COLUMNS.items()]
                         ).withColumn("_c_matched", F.lit(True))
    joined = (charts
              .join(F.broadcast(t), charts["track_id"] == t["_t_id"], "left")
              .join(F.broadcast(c), F.lower(charts["region_country_name"]) == F.lower(c["_c_name"]), "left")
              .drop("_t_id", "_c_name"))
    return (joined
            .withColumn("is_track_matched", F.coalesce(F.col("is_track_matched"), F.lit(False)))
            .withColumn("region_match_status",
                        F.when(F.col("is_global_region"), F.lit("global_aggregate"))
                        .when(F.col("_c_matched"), F.lit("matched"))
                        .otherwise(F.lit("unmatched")))
            .drop("_c_matched"))


# =========================================================================== #
# STAGE RUNNER
# =========================================================================== #
def _reconcile(name: str, bronze_n: int, silver_n: int, rejected_n: int, dup_n: int) -> None:
    if bronze_n != silver_n + rejected_n + dup_n:
        raise PipelineError(f"[silver:{name}] row accounting mismatch: bronze={bronze_n} != "
                            f"silver={silver_n} + quarantine={rejected_n} + duplicates={dup_n}")


def run_silver(spark: SparkSession, config: PipelineConfig) -> StageResult:
    t0 = time.time()
    log.info("=== SILVER: clean / validate / integrate ===")
    res = StageResult("silver")
    rd = lambda n: spark.read.parquet(str(config.bronze_path(n)))
    q = lambda n: config.quarantine_dir / n

    # ---------- tracks ----------
    b_tracks = rd("tracks"); bt = b_tracks.count()
    tracks, bridge, rej_t, st = build_silver_tracks(b_tracks)
    dup_t = st["valid_rows_before_dedup"] - st["tracks_rows"]
    _reconcile("tracks", bt, st["tracks_rows"], st["rejected_rows"], dup_t)
    write_parquet(tracks, config.silver_path("tracks"))
    write_parquet(bridge, config.silver_path("track_genres"))
    write_parquet(rej_t, q("tracks"))
    nconf = tracks.filter("popularity_conflict").count()
    log.info("[silver:tracks] bronze=%s valid=%s quarantined=%s duplicates_removed=%s -> %s unique tracks "
             "(%s had conflicting popularity across genres)", f"{bt:,}", f"{st['valid_rows_before_dedup']:,}",
             st["rejected_rows"], f"{dup_t:,}", f"{st['tracks_rows']:,}", nconf)
    res.tables.append(TableStat("silver", "tracks", bt, st["tracks_rows"], st["rejected_rows"], dup_t,
                                str(config.silver_path("tracks")), "one row per track_id; genres kept in array"))
    res.tables.append(TableStat("silver", "track_genres", st["valid_rows_before_dedup"], st["bridge_rows"], 0, 0,
                                str(config.silver_path("track_genres")), "bridge: track_id x genre"))
    res.details["tracks"] = {**st, "duplicates_removed": dup_t, "popularity_conflicts": nconf,
                             "invalid_tempo_set_null": tracks.filter(~F.col("has_valid_tempo")).count(),
                             "invalid_time_signature_set_null": tracks.filter(~F.col("has_valid_time_signature")).count()}

    # ---------- charts ----------
    b_charts = rd("charts"); bc = b_charts.count()
    charts, rej_c, sc = build_silver_charts(b_charts)
    _reconcile("charts", bc, sc["charts_rows"], sc["rejected_rows"], sc["duplicates_removed"])
    write_parquet(charts, config.silver_path("charts"), partition_by=None)
    write_parquet(rej_c, q("charts"))
    null_streams = charts.filter(~F.col("has_streams")).count()
    log.info("[silver:charts] bronze=%s valid=%s quarantined=%s duplicates_removed=%s -> %s rows; "
             "missing streams kept as NULL: %s", f"{bc:,}", f"{sc['valid_rows_before_dedup']:,}", sc["rejected_rows"],
             sc["duplicates_removed"], f"{sc['charts_rows']:,}", null_streams)
    res.tables.append(TableStat("silver", "charts", bc, sc["charts_rows"], sc["rejected_rows"], sc["duplicates_removed"],
                                str(config.silver_path("charts")), "typed, validated, track_id extracted"))
    res.details["charts"] = {**sc, "null_streams": null_streams}

    # ---------- countries ----------
    b_ctry = rd("countries"); bn = b_ctry.count()
    countries, rej_k, sk = build_silver_countries(b_ctry)
    _reconcile("countries", bn, sk["countries_rows"], sk["rejected_rows"], sk["duplicates_removed"])
    write_parquet(countries, config.silver_path("countries"))
    write_parquet(rej_k, q("countries"))
    log.info("[silver:countries] bronze=%s -> %s rows (quarantined=%s, duplicates=%s, still without geography=%s, "
             "iso_alpha2 recovered from iso_3166_2=%s, still missing=%s)",
             bn, sk["countries_rows"], sk["rejected_rows"], sk["duplicates_removed"], sk["without_geography"],
             sk["iso_alpha2_recovered"], sk["iso_alpha2_still_missing"])
    res.tables.append(TableStat("silver", "countries", bn, sk["countries_rows"], sk["rejected_rows"],
                                sk["duplicates_removed"], str(config.silver_path("countries")), "ISO codes preserved"))
    res.details["countries"] = sk

    # warn if a configured alias target does not exist in the country table
    names = {r[0].lower() for r in countries.select("country_name").collect()}   # 249 rows: safe collect
    bad = {k: v for k, v in C.REGION_ALIASES.items() if v.lower() not in names}
    if bad:
        log.warning("[silver:enriched] REGION_ALIASES targets missing from country table: %s", bad)

    # ---------- integration ----------
    enriched = build_enriched_charts(charts, tracks, countries).cache()
    en = enriched.count()
    if en != sc["charts_rows"]:
        raise PipelineError(f"[silver:enriched] chart rows lost in join! charts={sc['charts_rows']} enriched={en}")
    write_parquet(enriched, config.silver_path("enriched_chart_data"))
    matched = enriched.filter("is_track_matched").count()
    status = {r["region_match_status"]: r["count"] for r in enriched.groupBy("region_match_status").count().collect()}
    title_ok = enriched.filter("is_track_matched").filter(
        F.lower(F.trim("track_title")) == F.lower(F.trim("track_name"))).count()
    unmatched_regions = [r[0] for r in enriched.filter("region_match_status = 'unmatched'")
                         .select("region").distinct().collect()]
    distinct_ids = charts.select("track_id").distinct().count()
    matched_ids = enriched.filter("is_track_matched").select("track_id").distinct().count()
    log.info("[silver:enriched] chart rows in=%s out=%s (no rows lost). Track match: %s rows (%.2f%%), %s of %s distinct "
             "chart tracks. Region match status: %s. Title agreement on matched rows: %s/%s. Unmatched regions: %s",
             f"{sc['charts_rows']:,}", f"{en:,}", f"{matched:,}", 100.0 * matched / en, matched_ids, distinct_ids,
             status, title_ok, matched, unmatched_regions)
    res.tables.append(TableStat("silver", "enriched_chart_data", sc["charts_rows"], en, 0, 0,
                                str(config.silver_path("enriched_chart_data")),
                                f"LEFT JOIN tracks+countries; track match {100.0*matched/en:.2f}%"))
    res.details["enriched"] = {"rows": en, "track_matched_rows": matched,
                               "track_match_rate_pct": round(100.0 * matched / en, 2),
                               "distinct_chart_tracks": distinct_ids, "distinct_tracks_matched": matched_ids,
                               "region_match_status": status, "unmatched_regions": unmatched_regions,
                               "title_agreement_on_matched_rows": title_ok}
    for df in (tracks, bridge, rej_t, charts, rej_c, countries, rej_k, enriched):
        df.unpersist()
    res.seconds = time.time() - t0
    return res
