"""Builds docs/data_dictionary.md from the REAL Parquet schemas + a description catalogue.

    python scripts/generate_data_dictionary.py        (run after the pipeline)

Column names/types come from the files on disk, so the dictionary cannot drift from the data.
The script fails if a column has no description, forcing the catalogue to stay complete.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pyspark.sql import SparkSession  # noqa: E402

D = {
 # --- lineage / bronze
 "_source_file": "Name of the raw CSV the row came from (lineage).",
 "_ingested_at": "UTC timestamp when the Bronze load ran.",
 "_run_id": "Pipeline run identifier (UTC timestamp) of the Bronze load.",
 "_c0": "Blank-named first CSV column (artificial 0..n-1 pandas index in tracks.csv). Dropped in Silver.",
 # --- tracks
 "track_id": "Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts.",
 "artists": "Artist names as in the source, ';'-separated.", "album_name": "Album title.", "track_name": "Track title.",
 "popularity": "Spotify popularity 0-100 (for a track repeated across genres: the highest value).",
 "duration_ms": "Track length in milliseconds.", "explicit": "True if the track has explicit lyrics.",
 "danceability": "Audio feature 0-1: suitability for dancing.", "energy": "Audio feature 0-1: intensity/activity.",
 "key": "Pitch class 0-11 (0 = C).", "loudness": "Overall loudness in dB (typically -60..0).",
 "mode": "1 = major, 0 = minor.", "speechiness": "Audio feature 0-1: presence of spoken words.",
 "acousticness": "Audio feature 0-1: confidence the track is acoustic.",
 "instrumentalness": "Audio feature 0-1: likelihood of no vocals.", "liveness": "Audio feature 0-1: live-audience presence.",
 "valence": "Audio feature 0-1: musical positiveness.", "tempo": "Estimated tempo in BPM; NULL when the source value was the invalid 0/out-of-range.",
 "time_signature": "Estimated beats per bar (1-7); NULL when the source value was the invalid 0.",
 "track_genre": "Genre label of one source row (Bronze; a track repeats once per genre).",
 "artist_list": "artists split on ';' into an array.", "primary_artist": "First artist in artist_list.",
 "artist_count": "Number of artists credited on the track.",
 "has_valid_tempo": "False when the source tempo was 0 / out of range (tempo set to NULL).",
 "has_valid_time_signature": "False when the source time_signature was 0 (set to NULL).",
 "primary_genre": "Genre of the representative row kept by de-duplication (highest popularity, then alphabetical).",
 "genres": "Sorted array of ALL genres the track appears under in the source.", "genre_count": "Number of genres in `genres`.",
 "source_row_count": "How many Bronze rows were merged into this track.",
 "popularity_conflict": "True if the source rows of this track had different popularity values.",
 "genre": "Genre label.",
 # --- charts
 "chart_date": "Date of the chart snapshot.", "chart_type": "'top200' or 'viral50'.",
 "region": "Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent).",
 "region_country_name": "Official country name used to join country_codes (alias-resolved); NULL for 'Global'.",
 "is_global_region": "True for the worldwide 'Global' chart (not a country).",
 "chart_rank": "Position on the chart (1-200 for top200, 1-50 for viral50).",
 "track_title": "Track title as printed on the chart.", "chart_artist": "Artist credit string on the chart (', '-separated).",
 "trend": "Movement vs previous day: MOVE_UP, MOVE_DOWN, SAME_POSITION, NEW_ENTRY.",
 "streams": "Daily stream count; NULL when the source has none (never imputed).",
 "has_streams": "True when streams is not NULL.", "track_url": "Original Spotify URL from the source.",
 # --- countries
 "country_name": "Official country / territory name (ISO 3166 style).", "iso_alpha2": "ISO 3166-1 alpha-2 code.",
 "iso_alpha3": "ISO 3166-1 alpha-3 code. Primary key of silver/countries.", "iso_numeric_code": "ISO 3166-1 numeric code.",
 "iso_3166_2": "ISO 3166-2 reference string.", "continent": "UN M49 top-level region (Africa, Americas, Asia, Europe, Oceania); same value as `region`.",
 "sub_region": "UN M49 sub-region.", "intermediate_region": "UN M49 intermediate region (NULL where not applicable).",
 "region_code": "UN M49 region code.", "sub_region_code": "UN M49 sub-region code.",
 "intermediate_region_code": "UN M49 intermediate-region code.",
 "geography_source": "'source' | 'manual_override' (filled from config, e.g. Taiwan) | 'missing' (e.g. Antarctica).",
 "has_geography": "True when region information is available.", "iso_alpha2_recovered": "True if alpha-2 was blank in the source and was recovered from iso_3166_2 (Namibia).",
 # --- enriched
 "track_artists": "tracks.artists for the matched track.", "track_album_name": "tracks.album_name for the matched track.",
 "track_popularity": "tracks.popularity of the matched track.", "track_duration_ms": "tracks.duration_ms of the matched track.",
 "track_explicit": "tracks.explicit of the matched track.", "is_track_matched": "True when track_id exists in silver/tracks (else all track attributes are NULL).",
 "country_iso_alpha2": "ISO alpha-2 of the matched country.", "country_iso_alpha3": "ISO alpha-3 of the matched country.",
 "country_continent": "Continent (UN region) of the matched country.", "country_region": "UN region of the matched country.",
 "country_sub_region": "UN sub-region of the matched country.", "country_intermediate_region": "UN intermediate region.",
 "country_geography_source": "geography_source of the matched country.",
 "region_match_status": "'matched' | 'global_aggregate' | 'unmatched' (chart region vs country table).",
 # --- gold date / keys
 "chart_record_id": "SHA-256 of (date|region|chart_type|rank): deterministic natural key of a chart entry.",
 "chart_date_id": "Date as integer yyyymmdd (candidate Dim_Date key).", "chart_year": "Year of chart_date.",
 "chart_quarter": "Quarter 1-4.", "chart_month": "Month 1-12.", "chart_year_month": "'yyyy-MM'.",
 "chart_day": "Day of month.", "chart_day_of_week": "Weekday name.",
 "chart_season": "Northern-hemisphere meteorological season (Dec-Feb Winter, Mar-May Spring, Jun-Aug Summer, Sep-Nov Autumn).",
 # --- aggregates
 "chart_entries": "Number of chart rows in the group.", "distinct_chart_dates": "Distinct snapshot dates in the group.",
 "distinct_tracks": "Distinct track_ids in the group.", "distinct_artist_credits": "Distinct chart_artist strings.",
 "first_chart_date": "Earliest chart_date.", "last_chart_date": "Latest chart_date.",
 "total_streams": "Sum of streams (NULLs ignored; NULL if no stream data).", "avg_streams": "Average streams over rows that have streams.",
 "avg_chart_rank": "Average chart_rank.", "entries_with_track_attributes": "Chart rows whose track exists in tracks.csv.",
 "track_attribute_coverage": "Share (0-1) of chart rows with track attributes: how much to trust the audio averages.",
 "avg_track_popularity": "Average popularity over matched rows.",
 "artist_key": "Lower-cased artist name (grouping key).", "artist_name": "Credited artist (chart_artist split on ', ', protected names kept whole).",
 "distinct_countries_charted": "Distinct non-Global regions where the artist charted.", "chart_span_days": "last_chart_date - first_chart_date in days.",
 "best_chart_rank": "Lowest (best) rank reached.", "times_ranked_first": "Number of entries at rank 1.",
 "catalogue_tracks": "Distinct tracks in tracks.csv carrying this genre.", "avg_popularity": "Average popularity of catalogue tracks.",
 "explicit_share": "Share (0-1) of catalogue tracks that are explicit.", "avg_duration_min": "Average duration in minutes.",
 "charted_tracks": "Distinct catalogue tracks of this genre that appear in the chart sample.",
 "distinct_regions": "Distinct chart regions.",
}
for f in ["danceability", "energy", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "loudness", "tempo"]:
    D[f"avg_{f}"] = f"Average {f} over chart rows with track attributes."
    D[f"catalogue_avg_{f}"] = f"Average {f} over all catalogue tracks of the genre."

TABLES = [("bronze", "tracks"), ("bronze", "charts"), ("bronze", "countries"), ("silver", "tracks"), ("silver", "track_genres"),
          ("silver", "charts"), ("silver", "countries"), ("silver", "enriched_chart_data"), ("silver", "_quarantine/tracks"),
          ("gold", "enriched_chart_data"), ("gold", "music_analytics"), ("gold", "country_analytics"),
          ("gold", "artist_analytics"), ("gold", "genre_analytics"), ("gold", "monthly_analytics")]
GRAIN = {"silver/tracks": "one row per track_id", "silver/track_genres": "one row per (track_id, genre)",
         "silver/charts": "one row per (chart_date, region, chart_type, chart_rank)", "silver/countries": "one row per iso_alpha3",
         "silver/enriched_chart_data": "one row per chart entry (= silver/charts)", "gold/enriched_chart_data": "one row per chart entry",
         "gold/music_analytics": "one row per chart entry", "gold/country_analytics": "region x chart_type",
         "gold/artist_analytics": "credited artist", "gold/genre_analytics": "genre", "gold/monthly_analytics": "year-month x chart_type",
         "bronze/tracks": "one row per source CSV record", "bronze/charts": "one row per source CSV record",
         "bronze/countries": "one row per source CSV record", "silver/_quarantine/tracks": "rejected source records + reject_reasons"}
spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
out = ["# Data dictionary (generated from the Parquet files by `scripts/generate_data_dictionary.py`)\n",
       "Types are the Spark types of the columns actually on disk. See `docs/transformations.md` for the rules behind them.\n"]
missing = set()
for layer, name in TABLES:
    path = ROOT / "data" / layer / name
    df = spark.read.parquet(str(path)); n = df.count()
    out.append(f"## {layer}/{name}\n\n- Rows: **{n:,}**  - Grain: {GRAIN.get(f'{layer}/{name}', '')}\n")
    out.append("| Column | Type | Description |\n|---|---|---|")
    for fld in df.schema.fields:
        d = D.get(fld.name)
        if d is None and layer == "bronze":
            d = "Source column, stored as string exactly as in the CSV."
        if d is None and name == "_quarantine/tracks":
            d = "Original raw value (string) of the rejected record." if fld.name != "reject_reasons" else "';'-separated list of failed rules."
        if fld.name == "reject_reasons":
            d = "';'-separated list of failed validation rules."
        if d is None:
            missing.add(fld.name); d = "?"
        out.append(f"| {fld.name} | {fld.dataType.simpleString()} | {d} |")
    out.append("")
if missing:
    sys.exit(f"Descriptions missing for: {sorted(missing)}")
(ROOT / "docs" / "data_dictionary.md").write_text("\n".join(out), encoding="utf-8")
print("wrote docs/data_dictionary.md")
spark.stop()
