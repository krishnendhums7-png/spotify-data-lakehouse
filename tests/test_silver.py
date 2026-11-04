"""Silver layer tests: each transformation rule on small, hand-made Bronze-shaped inputs."""
import pytest
from pyspark.sql import functions as F

from spark import bronze_to_silver as S
from tests.conftest import CHART_DEFAULTS, COUNTRY_DEFAULTS, TRACK_DEFAULTS, make_bronze, string_df


def _tracks(spark, rows):
    return make_bronze(spark, "tracks", TRACK_DEFAULTS, rows)


def _charts(spark, rows):
    return make_bronze(spark, "charts", CHART_DEFAULTS, rows)


# ----------------------------------- tracks -----------------------------------
def test_artificial_index_detection(spark):
    good = string_df(spark, ["_c0"], [("0",), ("1",), ("2",)])
    bad = string_df(spark, ["_c0"], [("0",), ("5",), ("2",)])
    assert S.confirm_artificial_index(good) is True
    assert S.confirm_artificial_index(bad) is False


def test_tracks_types_and_index_dropped(spark):
    df = S.standardize_tracks(_tracks(spark, [{"explicit": "True"}]))
    r = df.first()
    assert "_c0" not in df.columns
    assert r["explicit"] is True and r["popularity"] == 50 and r["duration_ms"] == 200000
    assert dict(df.dtypes)["danceability"] == "double"


@pytest.mark.parametrize("override,reason", [
    ({"popularity": "150"}, "invalid_popularity"),
    ({"popularity": "abc"}, "unparseable_popularity"),
    ({"danceability": "1.5"}, "out_of_range_danceability"),
    ({"energy": "-0.1"}, "out_of_range_energy"),
    ({"duration_ms": "0"}, "invalid_duration_ms"),
    ({"track_name": None}, "missing_track_name"),
    ({"track_id": "short"}, "invalid_track_id"),
    ({"loudness": "-99"}, "out_of_range_loudness"),
    ({"mode": "2"}, "invalid_mode"),
])
def test_invalid_tracks_are_rejected_with_reason(spark, override, reason):
    df = S.validate_tracks(S.standardize_tracks(_tracks(spark, [override])))
    assert reason in df.first()["reject_reasons"]


def test_valid_track_has_no_reject_reasons(spark):
    assert S.validate_tracks(S.standardize_tracks(_tracks(spark, [{}]))).first()["reject_reasons"] == []


def test_zero_tempo_and_time_signature_become_null_with_flags(spark):
    df = S.standardize_tracks(_tracks(spark, [{"tempo": "0.0", "time_signature": "0"}, {}]))
    out = {r["has_valid_tempo"]: r for r in S.nullify_invalid_rhythm(df).collect()}
    assert out[False]["tempo"] is None and out[False]["time_signature"] is None
    assert out[True]["tempo"] == 120.0 and out[True]["time_signature"] == 4


def test_dedup_keeps_highest_popularity_and_all_genres(spark):
    rows = [{"track_genre": "rock", "popularity": "40"},
            {"track_genre": "pop", "popularity": "60"},
            {"track_genre": "indie", "popularity": "60"},
            {"track_id": "B" * 22, "track_genre": "jazz", "popularity": "10"}]
    df = S.standardize_tracks(_tracks(spark, rows))
    out = {r["track_id"]: r for r in S.deduplicate_tracks(df).collect()}
    a = out["A" * 22]
    assert len(out) == 2
    assert a["popularity"] == 60 and a["primary_genre"] == "indie"      # tie -> alphabetical genre
    assert a["genres"] == ["indie", "pop", "rock"] and a["genre_count"] == 3
    assert a["source_row_count"] == 3 and a["popularity_conflict"] is True
    assert out["B" * 22]["popularity_conflict"] is False


def test_artist_columns_split_on_semicolon(spark):
    df = S.add_artist_columns(S.standardize_tracks(_tracks(spark, [{"artists": "Ingrid Michaelson;ZAYN"}])))
    r = df.first()
    assert r["artist_list"] == ["Ingrid Michaelson", "ZAYN"] and r["primary_artist"] == "Ingrid Michaelson"
    assert r["artist_count"] == 2


def test_build_silver_tracks_accounting(spark):
    rows = [{"track_genre": "a"}, {"track_genre": "b"},                      # duplicate id -> 1 removed
            {"track_id": "B" * 22, "popularity": "999"}]                     # invalid -> quarantine
    bronze = _tracks(spark, [{**r, "_c0": str(i)} for i, r in enumerate(rows)])
    tracks, bridge, rejected, st = S.build_silver_tracks(bronze)
    assert (tracks.count(), rejected.count(), bridge.count()) == (1, 1, 2)
    assert st["valid_rows_before_dedup"] == 2
    assert "invalid_popularity" in rejected.first()["reject_reasons"]
    assert "_raw" not in rejected.columns and "popularity" in rejected.columns   # raw values kept for review


# ----------------------------------- charts -----------------------------------
@pytest.mark.parametrize("url,expected", [
    ("https://open.spotify.com/track/6mICuAdrwEjh6Y6lroV2Kg", "6mICuAdrwEjh6Y6lroV2Kg"),
    ("https://open.spotify.com/intl-de/track/6mICuAdrwEjh6Y6lroV2Kg?si=abc", "6mICuAdrwEjh6Y6lroV2Kg"),
    ("https://open.spotify.com/album/6mICuAdrwEjh6Y6lroV2Kg", None),
    ("not a url", None),
])
def test_extract_track_id(spark, url, expected):
    df = spark.createDataFrame([(url,)], "url string")
    assert df.select(S.extract_track_id(F.col("url"))).first()[0] == expected


def test_region_standardisation_and_aliases(spark):
    df = spark.createDataFrame([("United States",), ("Turkey",), ("Argentina",), ("Global",)], "r string")
    out = {r["r"]: r["c"] for r in df.select("r", S.region_country_name(F.col("r")).alias("c")).collect()}
    assert out == {"United States": "United States of America", "Turkey": "Türkiye",
                   "Argentina": "Argentina", "Global": None}


def test_chart_types_and_missing_streams_stay_null(spark):
    rows = [{"streams": None, "chart": "VIRAL50", "region": "  Costa   Rica "}]
    r = S.standardize_charts(_charts(spark, rows)).first()
    assert r["streams"] is None and r["has_streams"] is False          # never invent a value
    assert r["chart_type"] == "viral50" and r["region"] == "Costa Rica"
    assert dict(S.standardize_charts(_charts(spark, rows)).dtypes)["chart_date"] == "date"
    assert dict(S.standardize_charts(_charts(spark, rows)).dtypes)["streams"] == "bigint"


@pytest.mark.parametrize("override,reason", [
    ({"rank": "201"}, "invalid_rank"), ({"rank": "0"}, "invalid_rank"),
    ({"chart": "viral50", "rank": "51"}, "invalid_rank"),
    ({"date": "2999-01-01"}, "future_date"), ({"date": "2001-01-01"}, "date_too_old"),
    ({"date": "not-a-date"}, "unparseable_date"), ({"url": "nope"}, "missing_track_id_in_url"),
    ({"chart": "weekly"}, "unknown_chart_type"), ({"streams": "-5"}, "negative_streams"),
])
def test_invalid_charts_are_rejected(spark, override, reason):
    df = S.validate_charts(S.standardize_charts(_charts(spark, [override])))
    assert reason in df.first()["reject_reasons"]


def test_chart_dedup_on_composite_keys(spark):
    rows = [{"rank": "1", "streams": "10"}, {"rank": "1", "streams": "99"},                 # same position twice
            {"rank": "2", "streams": "5"},                                                   # same track again, worse rank
            {"rank": "3", "date": "2017-01-02"}]                                             # different day -> keep
    df = S.validate_charts(S.standardize_charts(_charts(spark, rows)))
    out = S.deduplicate_charts(df.filter(F.size("reject_reasons") == 0)).collect()
    assert len(out) == 2                                                                      # A) drop 1, B) drop 1
    kept = {(str(r["chart_date"]), r["chart_rank"]): r["streams"] for r in out}
    assert kept == {("2017-01-01", 1): 99, ("2017-01-02", 3): 1000}


# ---------------------------------- countries ----------------------------------
def _countries(spark, rows):
    return make_bronze(spark, "countries", COUNTRY_DEFAULTS, rows)


def test_countries_snake_case_and_codes_preserved(spark):
    df = S.standardize_countries(_countries(spark, [{}]))
    assert {"country_name", "iso_alpha2", "iso_alpha3", "sub_region", "intermediate_region"} <= set(df.columns)
    r = df.first()
    assert (r["iso_alpha2"], r["iso_alpha3"], r["iso_numeric_code"], r["region_code"]) == ("TL", "TST", 999, 150)


def test_missing_alpha2_is_recovered_from_iso_3166_2_and_flagged(spark):
    df = S.standardize_countries(_countries(spark, [{"name": "Namibia", "alpha-2": None, "alpha-3": "NAM",
                                                     "iso_3166-2": "ISO 3166-2:NA"}]))
    r = df.first()
    assert r["iso_alpha2"] == "NA" and r["iso_alpha2_recovered"] is True


def test_geography_override_fills_all_blank_columns(spark):
    """Regression test: an earlier version filled only `region` and left sub_region NULL."""
    rows = [{"name": "Taiwan, Province of China", "alpha-2": "TW", "alpha-3": "TWN", "region": None,
             "sub-region": None, "region-code": None, "sub-region-code": None},
            {"name": "Antarctica", "alpha-2": "AQ", "alpha-3": "ATA", "region": None, "sub-region": None,
             "region-code": None, "sub-region-code": None}]
    df = S.apply_geography_overrides(S.standardize_countries(_countries(spark, rows)))
    out = {r["iso_alpha2"]: r for r in df.collect()}
    tw, aq = out["TW"], out["AQ"]
    assert (tw["region"], tw["sub_region"], tw["region_code"], tw["sub_region_code"]) == ("Asia", "Eastern Asia", 142, 30)
    assert tw["geography_source"] == "manual_override" and tw["continent"] == "Asia"
    assert aq["region"] is None and aq["geography_source"] == "missing"       # not invented


def test_countries_dedup_on_iso_alpha3(spark):
    bronze = _countries(spark, [{}, {}, {"name": "Other", "alpha-2": "OT", "alpha-3": "OTH"}])
    countries, rejected, st = S.build_silver_countries(bronze)
    assert countries.count() == 2 and st["duplicates_removed"] == 1 and rejected.count() == 0


# ---------------------------------- integration (join) ----------------------------------
def test_left_join_keeps_unmatched_chart_rows_and_has_unique_columns(spark):
    charts = S.validate_charts(S.standardize_charts(_charts(spark, [
        {"rank": "1"},                                                              # matches track A..
        {"rank": "2", "url": "https://open.spotify.com/track/" + "Z" * 22},         # no such track
        {"rank": "3", "region": "Global"},
        {"rank": "4", "region": "Atlantis"}])))
    charts = charts.filter(F.size("reject_reasons") == 0).select(*S.CHART_SILVER_COLUMNS)
    tracks, _, _, _ = S.build_silver_tracks(_tracks(spark, [{}]))
    countries, _, _ = S.build_silver_countries(_countries(spark, [{"name": "Argentina", "alpha-2": "AR", "alpha-3": "ARG"}]))
    out = S.build_enriched_charts(charts, tracks, countries)
    assert out.count() == 4                                                         # nothing lost
    assert len(out.columns) == len(set(out.columns))                                # no duplicate names
    rows = {r["chart_rank"]: r for r in out.collect()}
    assert rows[1]["is_track_matched"] and rows[1]["region_match_status"] == "matched"
    assert not rows[2]["is_track_matched"] and rows[2]["danceability"] is None
    assert rows[3]["region_match_status"] == "global_aggregate"
    assert rows[4]["region_match_status"] == "unmatched" and rows[4]["country_iso_alpha3"] is None


@pytest.mark.integration
def test_silver_row_accounting_on_real_data(pipeline_run):
    _, res = pipeline_run
    for t in res["silver"].tables:
        if t.dataset in {"tracks", "charts", "countries"}:
            assert t.rows_in == t.rows_out + t.rows_rejected + t.rows_dedup_removed, t.dataset


@pytest.mark.integration
def test_silver_real_data_expectations(pipeline_run, spark):
    cfg, _ = pipeline_run
    tracks = spark.read.parquet(str(cfg.silver_path("tracks")))
    assert tracks.select("track_id").distinct().count() == tracks.count()
    assert tracks.filter("popularity < 0 OR popularity > 100").count() == 0
    assert tracks.filter("tempo IS NOT NULL AND tempo <= 0").count() == 0
    charts = spark.read.parquet(str(cfg.silver_path("charts")))
    assert charts.filter("chart_rank < 1 OR chart_rank > 200").count() == 0
    assert charts.filter("track_id IS NULL").count() == 0
    enriched = spark.read.parquet(str(cfg.silver_path("enriched_chart_data")))
    assert enriched.filter("region_match_status = 'unmatched'").count() == 0     # all 57 countries + Global resolved
