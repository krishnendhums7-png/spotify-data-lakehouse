# MEMBER 2 - Viva preparation (Spark ETL & Data Lakehouse)

Read `README.md` first, then this file. Everything marked "our result" comes from the real run (`reports/run_summary.md`).
Be honest in the viva about two things: **Delta Lake was not implemented**, and **only 17.11% of chart rows match a track**.

---
## Part 1 - Concepts in plain language

**What is Apache Spark?** An engine that processes data in parallel. It splits data into *partitions*, runs the same work on each partition on several CPU cores (or machines) and combines the results. Pandas works in one process on one machine's memory; Spark is built to scale beyond that.

**What is PySpark?** The Python API of Spark. You write Python; the work runs in the JVM (Java). That is why Java must be installed.

**What is ETL?** Extract (read CSV) -> Transform (clean, type, validate, join) -> Load (write Parquet). Ours is Raw CSV -> Bronze -> Silver -> Gold.

**What is a data lakehouse?** A data lake (cheap files, any format) with warehouse-like discipline: defined layers, schemas, quality rules and (with Delta/Iceberg) transactions. Ours is a lakehouse *without* a table format: layers + Parquet + checks.

**Bronze / Silver / Gold**
* Bronze: the source as received (we keep every value as text + lineage columns). Purpose: traceability and re-processing.
* Silver: cleaned, typed, de-duplicated, validated, joined. Purpose: one trustworthy version of each entity.
* Gold: curated tables for a question or a dashboard. Purpose: fast, easy analytics.

**Why Parquet?** Columnar (reads only needed columns), compressed (our 26 MB of CSV -> about 9-15 MB per layer), stores the schema, splittable for parallel reads, standard across Spark/dbt/DuckDB/pandas. CSV has no types and is slow to parse.

**What is schema inference?** Spark scans the data to guess column types. It is convenient but can guess wrongly (`NA`, `1.01e-06`, leading zeros) and costs an extra pass. We avoid it: Bronze uses an explicit all-string schema, Silver casts with validation.

**How does deduplication work?** Define the business key, rank the rows inside each key with a window function (`row_number() over (partition by key order by ...)`), keep rank 1. Ours: tracks by `track_id` (keep highest popularity, ties alphabetical genre), charts by (date, region, chart, rank) and (date, region, chart, track_id), countries by `iso_alpha3`.

**How did we handle missing values?** We never invent values. Empty strings become NULL. `streams` missing (22 viral50 rows) stays NULL with `has_streams=False`. Impossible values (tempo 0, time_signature 0) become NULL with a flag. A record that is unusable (no name, duration 0) goes to quarantine with the reason. Missing geography is left NULL (Antarctica) or filled from a documented, flagged config override (Taiwan). A blank ISO code (Namibia) is recovered from another column of the same row and flagged.

**How did we join the three datasets?**
1. charts -> tracks: extract the 22-character id from `url` with a regex, LEFT JOIN on `track_id`.
2. charts -> countries: standardise `region`, map 7 differently-spelled names with an alias table (e.g. United States -> United States of America), treat `Global` as a non-country, LEFT JOIN on lower-cased name.
3. Both joins are LEFT so no chart row is lost (50,000 in = 50,000 out, checked in code). Dimensions are small, so they are **broadcast**.

**What is a Spark DataFrame?** A distributed table with named, typed columns. Operations create a new DataFrame (they are immutable).

**What is lazy evaluation?** Transformations (`select`, `filter`, `join`, `withColumn`) only build a plan. Nothing runs until an *action* (`count`, `write`, `collect`, `show`). Spark can then optimise the whole plan (e.g. read only needed columns).

**Transformations vs actions.** Transformation = returns a DataFrame, lazy (`filter`, `join`, `groupBy.agg`). Action = triggers execution (`count()`, `write.parquet()`, `first()`). Narrow transformations (`filter`, `select`) need no data movement; wide ones (`join`, `groupBy`, `window`) need a *shuffle*.

**What is repartitioning?** Changing how many partitions the data has (`repartition(n)` shuffles; `coalesce(n)` only merges, no shuffle). We avoid `repartition`; we use `coalesce(1)` before writing so tiny data does not produce hundreds of tiny files, and we lower `spark.sql.shuffle.partitions` from 200 to 8.

**Parquet vs Delta Lake.** Parquet is a *file format*. Delta Lake is a *table format* stored as Parquet files + a transaction log: ACID transactions, updates/deletes/merge, time travel, schema enforcement. Parquet alone can be corrupted by a failed half-written overwrite and cannot update rows. We used Parquet; Delta would be the upgrade.

---
## Part 2 - Every script, in the order they run

| Script | What it does and why |
|---|---|
| `utils/config.py` | One place for paths (computed from the file location, so no machine paths), the expected CSV headers, all validation ranges, region aliases, protected artist names. Rules in config = easy to explain and test |
| `utils/logger.py` | Console + `logs/pipeline_<run_id>.log` |
| `spark/spark_session.py` | Builds the SparkSession: 8 shuffle partitions, adaptive execution on, UTC timezone, lenient casts (`ansi=false`) so a bad value becomes NULL instead of a crash (we detect those ourselves), Snappy Parquet |
| `spark/common.py` | `write_parquet` (overwrite, optional partitioning, coalesce), `cast_with_audit` (cast + list the values that could not be parsed), `reason_array`, result dataclasses |
| `spark/bronze_ingestion.py` | Extract zip if needed -> compare CSV header with expected -> read all columns as string (FAILFAST) -> add `_source_file/_ingested_at/_run_id` -> write Parquet -> compare Spark's row count with an *independent* Python `csv` count -> write `_manifest.json` (SHA-256, sizes, counts) |
| `spark/bronze_to_silver.py` | For each dataset: standardise names/types -> validate (rules produce a `reject_reasons` array) -> split valid / quarantined -> de-duplicate -> write. Then builds `enriched_chart_data`. Checks `bronze = silver + quarantine + duplicates` |
| `spark/silver_to_gold.py` | Adds date attributes (year, quarter, month, season, `chart_date_id`) and `chart_record_id`; builds music/country/artist/genre/monthly datasets |
| `spark/verify_outputs.py` | Re-reads every dataset from disk; 28 checks (columns, types, no row lost, unique keys, ISO codes complete) |
| `spark/run_pipeline.py` | Runs the stages in order, prints a summary table, saves `reports/run_summary.*`, returns exit code 0/1/2 |
| `tests/` | 46 unit tests on small hand-made inputs + 8 integration tests on the real CSVs |
| `scripts/profile_sources.py`, `generate_data_dictionary.py` | Produce `docs/data_profile.md` and `docs/data_dictionary.md` from the real files (no hand-typed numbers) |

---
## Part 3 - Our actual results

| Item | Result |
|---|---|
| Bronze rows | tracks 114,000, charts 50,000, countries 249 (equal to source counts; two parsers agree) |
| `tracks.csv` structure | 114,000 rows but only 89,741 distinct `track_id`: a track appears once per genre (up to 9 times). 114 genres. First column is an artificial index (0..113,999) -> dropped after checking |
| Tracks quarantined | 1: `1kR4gIb7nGxHPI3D2ifs59`, no name/artist/album, duration 0 |
| Tracks de-duplicated | 24,259 repeated rows merged -> **89,740** Silver tracks. Only `popularity` differs between copies (720 tracks) |
| Invalid tempo / time signature | 157 tracks tempo 0, 162 tracks time_signature 0 -> NULL + flags |
| Charts | 50,000 -> 50,000: no invalid rows, no duplicates found. 22 NULL streams, all `viral50` |
| Chart dates | 25 distinct dates between 2017-01-01 and 2020-06-01, unevenly spread (a sample) |
| Chart -> track match | **8,554 rows (17.11%)**; 372 of 6,038 distinct chart tracks. 96.3% of matched rows have the same title, which supports the id join being correct |
| Chart -> country match | 48,800 rows to 57 countries; 1,200 `Global`; 0 unmatched. Only 50 of 58 names matched exactly; 7 needed aliases (Bolivia, Czech Republic, Netherlands, Taiwan, Turkey, United Kingdom, United States) |
| Countries | 249 rows. Namibia's alpha-2 is blank *in the CSV* -> recovered "NA" from `iso_3166_2`. Antarctica has no region. Taiwan region blank -> filled (Asia / Eastern Asia) and flagged `manual_override` |
| Gold sizes | enriched 50,000; music 50,000; country 73; artist 3,363; genre 114; monthly 8 |
| Example insights | Most chart entries per artist: The Chainsmokers 1,081, Drake 975, Ed Sheeran 954, The Weeknd 910, Ozuna 902. Chart genres of matched tracks: house (2,953 entries, 47 tracks), dance, electro, edm lead. Track coverage per country ranges (e.g. Argentina 10%, Australia 19%) |
| Pipeline run | 85 s total on the author's machine; 28/28 verify checks passed; 54/54 tests passed |

Caution: the insights come from a sample with 17% track coverage; do not present them as global truths.

---
## Part 4 - 20 likely viva questions

**1. Why did you read everything as strings in Bronze?**
Bronze must mirror the source. Inference can change values (`NA`, `1.01e-06`, leading zeros). Typing and validation happen in Silver, where bad values are reported instead of silently altered.

**2. Why is Silver tracks smaller than Bronze (114,000 -> 89,740)?**
The source lists each track once per genre. 24,259 repeated rows were merged by `track_id`, and 1 invalid row was quarantined: 114,000 = 89,740 + 24,259 + 1. No genre information was lost: it is in the `genres` array and the `track_genres` table.

**3. Which duplicate row did you keep and why?**
The one with the highest popularity, ties broken by alphabetical genre so the result is the same every run. Only popularity differs between copies (720 tracks); all other fields are identical. This is documented as a business rule and flagged by `popularity_conflict`.

**4. Why is `track_id` the business key and not title + artist?**
Different Spotify tracks (re-releases, remasters) can share a title and artist. `track_id` is the unique Spotify identifier.

**5. How did you get the track id from the chart data?**
A regex on the `url` column: `open.spotify.com/(intl-xx/)?track/<22 alphanumeric>`. It worked for 50,000 of 50,000 rows.

**6. Why only 17.11% of chart rows match tracks?**
The two files are independent samples of Spotify data: the catalogue contains 89,740 tracks while the chart sample references 6,038 distinct tracks, of which only 372 are in the catalogue. It is a data property, not a join bug (96.3% of matched rows have the same title). Unmatched rows are kept with NULL track attributes, and Gold exposes `track_attribute_coverage`.

**7. Why LEFT JOIN?**
Charts are the fact data. An inner join would delete 82.89% of chart rows. A LEFT JOIN keeps all, and we assert output rows = input rows.

**8. How did you join charts to countries?**
By standardised country name. 50 of 58 region names matched directly; 7 were mapped with an alias table to the official names; `Global` is not a country and is kept with `region_match_status='global_aggregate'`. Result: 0 unmatched.

**9. What did you do with missing values?**
Nothing was imputed. Empty -> NULL; missing streams stay NULL (22 viral50 rows); impossible values (tempo 0, time_signature 0) -> NULL plus a flag; unusable records -> quarantine with reasons; ISO alpha-2 blank for Namibia -> recovered from the same row's `iso_3166_2` and flagged; Taiwan's blank region -> filled from a documented config entry and flagged `manual_override`.

**10. Why did you set Taiwan's region manually? Isn't that making up data?**
It is a transparent, configurable correction using the UN M49 classification the rest of the table follows, and every such row carries `geography_source='manual_override'`. Without it, Taiwan's 806 chart rows would have no continent. It can be switched off by emptying `GEOGRAPHY_OVERRIDES`. Antarctica, with no evidence, stays NULL.

**11. What is the quarantine and why use it?**
A folder with rejected rows, their original raw values and a `reject_reasons` text. Dropping bad rows silently hides data problems; quarantine lets Member 4 or an analyst inspect and fix them. Our only quarantined row: a track with no name and duration 0.

**12. How do you know no rows were lost?**
Three layers of proof: (a) Bronze count equals an independent Python CSV count, (b) per dataset `bronze = silver + quarantine + duplicates` is asserted in code, (c) chart rows are 50,000 at Silver, enriched and both Gold tables, checked in `verify_outputs.py` and in tests.

**13. What is lazy evaluation and where do you see it in your code?**
Spark only builds a plan until an action runs. In `bronze_to_silver.py` all `withColumn/filter/join` calls are lazy; the `count()` and `write.parquet()` calls trigger execution. That is why we `cache()` DataFrames used for several counts/writes, otherwise Spark would recompute them each time.

**14. Where did you optimise performance?**
Explicit schema (no inference pass); shuffle partitions 200 -> 8; adaptive query execution; broadcast joins for the two small dimension tables; `cache()` + `unpersist()` for reused DataFrames; `coalesce(1)` (no shuffle) to avoid tiny files; no `collect()` on large data and no Pandas conversion; Parquet columnar output; partitioning Gold enriched data by `chart_year`.

**15. What is the difference between `repartition` and `coalesce`?**
`repartition(n)` does a full shuffle and can increase or decrease partitions. `coalesce(n)` only merges existing partitions without a shuffle, so it is cheaper but can only decrease. We use `coalesce` for writing.

**16. Parquet vs Delta Lake - and did you use Delta?**
No, Delta was optional and could not be installed/tested in my build environment, so I did not claim it. Delta adds a transaction log to Parquet for ACID, updates/merge, time travel and schema enforcement. Our writes use one function (`write_parquet`), so migration is localised.

**17. Why is the pipeline idempotent?**
Each stage reads only from the previous layer and writes with `overwrite`, so re-running gives the same outputs (apart from `_ingested_at`/`_run_id` in Bronze).

**18. What data-quality rules did you apply to tracks?**
popularity 0-100; duration > 0; danceability, energy, speechiness, acousticness, instrumentalness, liveness, valence in [0,1]; loudness -60..10 dB; key 0-11; mode 0/1; valid 22-char id; name present. Tempo outside (0, 300] and time_signature outside 1-7 become NULL. Real data violated only the name/duration rule (1 row) and had 157/162 zero sentinels.

**19. What are the limitations of your Gold datasets?**
Audio/genre results cover only 17.11% of chart rows; the chart sample has 25 dates, so monthly/seasonal trends are indicative; no artist ids (artists split from a comma list, with protected names such as "Earth, Wind & Fire"); a multi-genre track is counted once per genre so genre entries are not additive; season assumes the northern hemisphere.

**20. How does your work connect to the other members?**
Gold tables carry natural keys (`chart_record_id`, `track_id`, `country_iso_alpha3`, `chart_date_id`, `genre`) for Member 3's star schema; quarantine and `verify_outputs.py` give Member 4 a starting point for Great Expectations; the exit code (0/1/2) lets Airflow detect failure; Gold Parquet feeds Member 5's dashboard. I deliberately did not build the star schema, dbt, GE, Airflow or the dashboard.

---
## Part 5 - Extra questions you might get
* *What would you change for 100x more data?* Remove `coalesce(1)`, partition Gold by date, raise shuffle partitions, drop `cache()` where memory is tight, run on a cluster, use Delta/Iceberg, and ingest incrementally.
* *Why `ansi.enabled=false`?* So an unparseable cast becomes NULL (and is caught by `cast_with_audit`) rather than throwing; it also makes behaviour the same in Spark 3.5 and 4.x.
* *Why a SHA-256 `chart_record_id`?* The source has no chart id; hashing (date, region, chart type, rank) gives a deterministic, unique natural key that Member 3 can map to a surrogate key.
* *What bugs did you hit?* (1) The verification step caught that Namibia's ISO code is blank in the CSV (my assumption was wrong) -> recovery rule. (2) Reviewing outputs found Taiwan's sub-region still NULL because the override condition re-read a column it had just filled -> fixed and a regression test added. (3) A PySpark lambda with default arguments broke the artist split -> rewritten.
