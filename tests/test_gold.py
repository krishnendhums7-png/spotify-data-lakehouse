"""Gold layer tests."""
from datetime import date

import pytest
from pyspark.sql import functions as F

from spark import silver_to_gold as G


def test_season_and_date_attributes(spark):
    df = spark.createDataFrame([(date(2017, 1, 1),), (date(2017, 4, 15),), (date(2018, 8, 9),), (date(2018, 11, 30),)],
                               "chart_date date")
    out = {str(r["chart_date"]): r for r in G.add_date_attributes(df).collect()}
    assert out["2017-01-01"]["chart_season"] == "Winter" and out["2017-01-01"]["chart_date_id"] == 20170101
    assert out["2017-04-15"]["chart_season"] == "Spring" and out["2017-04-15"]["chart_quarter"] == 2
    assert out["2018-08-09"]["chart_season"] == "Summer" and out["2018-08-09"]["chart_year_month"] == "2018-08"
    assert out["2018-11-30"]["chart_season"] == "Autumn"


def test_split_artists_respects_protected_names(spark):
    df = spark.createDataFrame([("J Balvin, Pharrell Williams, BIA",), ("Earth, Wind & Fire",),
                                ("Tyler, The Creator, Kali Uchis",)], "chart_artist string")
    got = sorted(r["artist_name"] for r in G.split_artists(df).collect())
    assert got == sorted(["J Balvin", "Pharrell Williams", "BIA", "Earth, Wind & Fire", "Tyler, The Creator", "Kali Uchis"])


def test_chart_record_id_is_deterministic_and_distinguishes_positions(spark):
    df = spark.createDataFrame([(date(2017, 1, 1), "Chile", "top200", 1), (date(2017, 1, 1), "Chile", "top200", 2)],
                               "chart_date date, region string, chart_type string, chart_rank int")
    a = [r[0] for r in G.add_chart_record_id(df).select("chart_record_id").collect()]
    b = [r[0] for r in G.add_chart_record_id(df).select("chart_record_id").collect()]
    assert a == b and len(set(a)) == 2 and all(len(x) == 64 for x in a)


def test_genre_analytics_zero_fills_genres_without_chart_activity(spark):
    tracks = spark.createDataFrame([("T1", 80, True, 180000, 0.5, 0.5, 0.1, 0.1, 0.0, 0.1, 0.5, -5.0, 120.0),
                                    ("T2", 20, False, 240000, 0.4, 0.4, 0.1, 0.1, 0.0, 0.1, 0.5, -7.0, 100.0)],
                                   "track_id string, popularity int, explicit boolean, duration_ms long, danceability double, "
                                   "energy double, speechiness double, acousticness double, instrumentalness double, "
                                   "liveness double, valence double, loudness double, tempo double")
    bridge = spark.createDataFrame([("T1", "pop"), ("T2", "jazz")], "track_id string, genre string")
    enriched = spark.createDataFrame([("T1", 1, 100, "Chile", date(2017, 1, 1), True)],
                                     "track_id string, chart_rank int, streams long, region string, chart_date date, is_track_matched boolean")
    out = {r["genre"]: r for r in G.build_genre_analytics(tracks, bridge, enriched).collect()}
    assert out["pop"]["chart_entries"] == 1 and out["pop"]["charted_tracks"] == 1
    assert out["jazz"]["chart_entries"] == 0 and out["jazz"]["catalogue_tracks"] == 1


@pytest.mark.integration
def test_gold_outputs_exist_and_keep_every_chart_row(pipeline_run, spark):
    cfg, res = pipeline_run
    silver = spark.read.parquet(str(cfg.silver_path("charts"))).count()
    for name in ("enriched_chart_data", "music_analytics"):
        assert spark.read.parquet(str(cfg.gold_path(name))).count() == silver
    for name in ("country_analytics", "artist_analytics", "genre_analytics", "monthly_analytics"):
        assert spark.read.parquet(str(cfg.gold_path(name))).count() > 0


@pytest.mark.integration
def test_gold_aggregates_reconcile_with_enriched(pipeline_run, spark):
    cfg, _ = pipeline_run
    enriched = spark.read.parquet(str(cfg.gold_path("enriched_chart_data")))
    total = enriched.count()
    assert spark.read.parquet(str(cfg.gold_path("country_analytics"))).agg(F.sum("chart_entries")).first()[0] == total
    assert spark.read.parquet(str(cfg.gold_path("monthly_analytics"))).agg(F.sum("chart_entries")).first()[0] == total
    assert enriched.select("chart_record_id").distinct().count() == total
    # genre table covers every genre in the track catalogue
    genres = spark.read.parquet(str(cfg.silver_path("track_genres"))).select("genre").distinct().count()
    assert spark.read.parquet(str(cfg.gold_path("genre_analytics"))).count() == genres


@pytest.mark.integration
def test_gold_music_analytics_columns_for_downstream(pipeline_run, spark):
    cfg, _ = pipeline_run
    cols = set(spark.read.parquet(str(cfg.gold_path("music_analytics"))).columns)
    assert {"chart_record_id", "chart_date_id", "track_id", "country_iso_alpha3", "primary_genre", "danceability",
            "chart_rank", "streams", "chart_season", "is_track_matched"} <= cols
