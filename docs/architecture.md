# Architecture (Member 2: Spark ETL & Data Lakehouse)

```
 data/raw/*.csv  (tracks 114,000 | charts 50,000 | country_codes 249)
        |   spark/bronze_ingestion.py
        v
 BRONZE  data/bronze/{tracks,charts,countries}/   Parquet, all columns string + lineage, _manifest.json
        |   spark/bronze_to_silver.py
        v
 SILVER  data/silver/{tracks,track_genres,charts,countries,enriched_chart_data}/
         data/silver/_quarantine/{tracks,charts,countries}/      (rejected rows + reasons)
        |   spark/silver_to_gold.py
        v
 GOLD    data/gold/{enriched_chart_data,music_analytics,country_analytics,
                    artist_analytics,genre_analytics,monthly_analytics}/
        |   spark/verify_outputs.py  (read-back + reconciliation, 28 checks)
        v
 hand-off ->  Member 3 (dbt / star schema)  ->  Member 4 (GE, Airflow)  ->  Member 5 (dashboard)
```

Orchestration of the four stages: `python -m spark.run_pipeline` (`spark/run_pipeline.py`).

## Purpose of each layer

| Layer | Contract | Mutability |
|---|---|---|
| Raw | Untouched source files | never edited |
| Bronze | Exact copy of the source values in Parquet (strings) + lineage. Lets us re-run Silver without re-reading CSVs and proves what was received | overwritten only by re-ingestion |
| Silver | One trustworthy, typed, de-duplicated version of each entity; bad rows quarantined, not hidden; datasets joined | rebuilt from Bronze |
| Gold | Business-shaped tables that answer the project's analytical questions | rebuilt from Silver |

Because each layer is rebuilt from the one before it, every stage is idempotent (`mode("overwrite")`) and any rule change can be replayed.

## Data model of the sources

```
charts_sample.csv --(url -> track_id)--> tracks.csv           match: 8,554 / 50,000 rows (17.11%)
charts_sample.csv --(region -> name)---> country_codes.csv     match: 48,800 rows + 1,200 'Global' (non-country)
tracks.csv --(track_id x track_genre)--> repeated once per genre -> silver/tracks + silver/track_genres
```

## Key engineering decisions

1. **Parquet baseline, Delta Lake not used.** Delta needs extra JARs downloaded from Maven; the build environment here could not reach Maven, so it could not be installed or tested. The code does not claim Delta support. Because Silver/Gold writes go through one helper (`common.write_parquet`), switching to Delta later means changing that function and adding `delta-spark` to the Spark session.
2. **Strings in Bronze, types in Silver** (fidelity first, validation second).
3. **Quarantine instead of silent drops**, plus a row-accounting identity per dataset checked in code.
4. **Left joins + broadcast** for the small dimensions; an assertion that no chart row is lost.
5. **Small-data tuning:** `shuffle.partitions = 8` (default 200 would create 200 tiny tasks), AQE on, `coalesce(1)` before writing (a narrow operation, no shuffle) to avoid many tiny files, `cache()` only for DataFrames reused for several counts/writes (released with `unpersist()`), no `collect()` on large data (only on the 249-row country table and a few aggregates), no Pandas conversion.
6. **Lenient casts** (`spark.sql.ansi.enabled=false`) combined with our own `cast_with_audit`: unparseable values are detected and rejected with a reason rather than crashing the job.
7. **Gold stays denormalised** and carries natural keys; dimensional modelling is left to Member 3.

## Interfaces for the other members

| Member | Use | Where |
|---|---|---|
| 1 | Drop new CSVs in `data/raw/`; Bronze can be replaced by their ingestion output if it keeps the same column names (header check enforces this) | `utils/config.SOURCE_DATASETS` |
| 3 | `music_analytics` / `enriched_chart_data` -> `Fact_Chart`; `silver/tracks` -> `Dim_Track`; `silver/countries` -> `Dim_Country`; `genre_analytics`/`track_genres` -> `Dim_Genre`; date columns -> `Dim_Date`; artists via `artist_analytics.artist_name` (no source id) | `docs/data_dictionary.md` |
| 4 | Quarantine folders and `reject_reasons`; the checks in `verify_outputs.py` are a starting point for Great Expectations suites; Airflow can call `python -m spark.run_pipeline --stages ...` and checks the exit code (0 ok, 1 pipeline error, 2 unexpected) | `data/silver/_quarantine` |
| 5 | Read Gold Parquet directly (`pandas.read_parquet`, Spark, DuckDB). `country_analytics` + `monthly_analytics` are ready for charts | `data/gold` |

## Known limitations
* Only 17.11% of chart rows have track attributes; audio/genre analysis is based on that subset.
* The chart sample has 25 snapshot dates (2017-2020, unevenly spread), so "monthly trends" are indicative only.
* No artist ids or artist country/genre in the sources -> `Dim_Artist.artist_country/artist_genre` cannot be filled.
* Runs in Spark local mode; not tested on a cluster.
