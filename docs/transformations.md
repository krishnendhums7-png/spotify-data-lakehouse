# Transformations and the reasons behind them

All numbers are from the real run (`reports/run_summary.md`, `logs/`). Profiling detail: `docs/data_profile.md`.
Rule constants live in `utils/config.py`; code in `spark/bronze_ingestion.py`, `bronze_to_silver.py`, `silver_to_gold.py`.

## Bronze  (raw CSV -> Parquet, nothing "fixed")

| Step | What | Why |
|---|---|---|
| Header check | Python `csv` reads the header and compares it to the expected list | Source schema drift must fail loudly |
| Explicit all-string schema | Every source column is read as `string`, no inference | Inference can silently change values (`1.01e-06`, `NA`, leading zeros). Typing belongs in Silver where it is validated |
| `FAILFAST` + RFC-4180 quoting | Malformed rows stop the job; `""` inside quotes handled | A dropped row is silent data loss |
| Lineage columns | `_source_file`, `_ingested_at`, `_run_id` added | Traceability back to the raw file |
| Manifest | `bronze/_manifest.json`: size, SHA-256, record counts | Proves which exact file was loaded |
| Count check | Python `csv` record count (independent parser) = rows read back from Parquet | Proves no loss at ingestion |

Result: tracks 114,000 / charts 50,000 / countries 249 records, all equal to the source counts.
The blank-named first column of `tracks.csv` is stored as `_c0` (Spark's name for an empty header).

## Silver - tracks  (114,000 -> 89,740)

| Rule | Detail | Actual effect |
|---|---|---|
| Drop index column | `_c0` is removed **only after verifying** it is unique and contiguous 0..n-1 | Confirmed artificial -> dropped |
| Types | popularity int; duration_ms long; explicit boolean; audio features double; key/mode/time_signature int | Unparseable values would be rejected with `unparseable_<col>` (none occurred) |
| Trim / blank -> NULL | all text and numeric-as-text columns | - |
| Validation (reject -> quarantine) | track_id 22 alnum chars; track_name present; popularity 0-100; duration_ms > 0; the 7 unit-interval features in [0,1]; loudness -60..10 dB; key 0-11; mode 0/1 | **1 row rejected**: `track_id 1kR4gIb7nGxHPI3D2ifs59` (k-pop) has no name/artist/album and duration 0 (`missing_track_name;invalid_duration_ms`) |
| Sentinel values -> NULL | tempo <= 0 or > 300, time_signature outside 1-7 | 157 tracks have tempo 0, 162 have time_signature 0. They look like "analysis failed" placeholders, so they become NULL (never imputed) and `has_valid_tempo` / `has_valid_time_signature` are set to False |
| Kept as-is | loudness > 0 dB (90 rows), duration > 1 hour (16), popularity 0 (16,020), time_signature 1 | Plausible real values, not errors |
| **Deduplication** | key = `track_id`. Source repeats a track once per genre: 24,259 extra rows. Same title/artist/audio features; only `popularity` differs (720 tracks). Keep the row with the highest popularity (ties: alphabetical genre -> deterministic) | 113,999 valid rows -> 89,740 tracks. Different track_ids with the same title (re-releases) are **kept**: they are different Spotify tracks |
| Information preserved | `genres` (sorted array of all genres), `genre_count`, `primary_genre`, `source_row_count`, `popularity_conflict` + bridge table `silver/track_genres` (113,549 track x genre rows) | Dedup must not lose the genre information that was the reason for the duplicates |
| Artists | `artists` (original `;` string) + `artist_list`, `primary_artist`, `artist_count` | Convenient for analysis; original kept |

Identity enforced in code: `114,000 = 89,740 + 1 quarantined + 24,259 duplicates`.

## Silver - charts  (50,000 -> 50,000)

| Rule | Detail | Actual effect |
|---|---|---|
| Rename | title->track_title, rank->chart_rank, date->chart_date, artist->chart_artist, chart->chart_type, url->track_url | - |
| Types | chart_date date, chart_rank int, streams long (via double: source is `253019.0`) | all parsed |
| `track_id` | regex on `url` (`open.spotify.com/(intl-xx/)?track/<22 chars>`) | extracted for 50,000/50,000 rows |
| Region standardisation | trim + collapse spaces; `region_country_name` = official name via `REGION_ALIASES` (7 aliases); `Global` is flagged `is_global_region` and left unmapped | 57 countries resolved, Global = 1,200 rows |
| Missing streams | kept NULL, `has_streams = False` | 22 rows, **all** `viral50` chart rows (that chart has no stream counts); no value invented |
| Validation | date not null / not future / >= 2010-01-01; chart_type in {top200, viral50}; rank 1-200 (top200) or 1-50 (viral50); track_id present; region present; streams >= 0 | 0 rejected |
| Deduplication | (A) `chart_date+region+chart_type+chart_rank`; (B) `chart_date+region+chart_type+track_id` (best surviving row = most streams / best rank) | 0 duplicates found, but the rules are active and unit-tested |

## Silver - countries  (249 -> 249)

| Rule | Detail | Actual effect |
|---|---|---|
| Rename | `alpha-2`->`iso_alpha2`, `sub-region`->`sub_region`, ... (snake_case) | - |
| Codes | iso_alpha2/3 upper-cased and validated; numeric codes cast to int | all valid |
| Blank alpha-2 | `Namibia,,NAM,...` has a **blank alpha-2 in the source file** (its code "NA" was lost by whatever exported it). The same row's `iso_3166_2` says `ISO 3166-2:NA`, so the code is recovered from that column and `iso_alpha2_recovered = True` | 1 recovered |
| Missing geography | Antarctica: no region at all -> stays NULL, `geography_source='missing'`. Taiwan: blank region in the source -> filled from the UN M49 classification (Asia / Eastern Asia, codes 142/30) via `GEOGRAPHY_OVERRIDES`, flagged `geography_source='manual_override'` | 1 override, 1 still missing |
| `continent` | alias of the UN top-level `region` (Africa, Americas, Asia, Europe, Oceania) | The source has no separate continent column |
| Dedup | key `iso_alpha3` | 0 duplicates |
| `intermediate_region` NULL (144 rows) | legitimately "not applicable", not an error | left NULL |

## Silver - integration  (`enriched_chart_data`)

`charts LEFT JOIN tracks ON track_id` and `LEFT JOIN countries ON lower(region_country_name) = lower(country_name)`.
Both dimensions are small, so they are **broadcast** (no shuffle of the chart table).
Dimension columns are renamed before joining (`track_*`, `country_*`) so there are no duplicate/ambiguous names.
Flags: `is_track_matched`, `region_match_status` (`matched` | `global_aggregate` | `unmatched`).

| Metric | Value |
|---|---|
| Chart rows in / out | 50,000 / 50,000 (checked in code: a mismatch raises an error) |
| Chart rows with track info | 8,554 (**17.11%**); 372 of 6,038 distinct chart tracks |
| Title agreement on matched rows (chart title == track_name) | 8,240 / 8,554 (96.3%) - a sanity check that the id join is correct; the rest differ in wording (e.g. "- Acoustic") |
| Regions | 48,800 matched to a country, 1,200 Global, 0 unmatched |

The low track coverage is a property of the supplied sample (`tracks.csv` is a 114k-track catalogue, `charts_sample.csv` a 50k-row slice of charts), not a join error. Unmatched rows are kept with NULL track attributes.

## Gold

| Dataset | Grain | Purpose |
|---|---|---|
| `enriched_chart_data` | chart entry (50,000) | Full-fidelity enriched table + date attributes (`chart_date_id`, year, quarter, month, season...) + `chart_record_id` (SHA-256 natural key). Partitioned by `chart_year` |
| `music_analytics` | chart entry (50,000) | Curated BI table: chart, artist, genre, country/region, date and audio features; drops raw/URL noise |
| `country_analytics` | region x chart_type (73) | entries, distinct tracks/artists, streams, avg rank, audio averages, `track_attribute_coverage` |
| `artist_analytics` | credited artist (3,363) | chart presence, span, best rank, countries, streams |
| `genre_analytics` | genre (114) | catalogue metrics for **all** genres + chart metrics for genres of charted tracks |
| `monthly_analytics` | year-month x chart_type (8) | trend table (only 8 months exist in the sample) |

Design notes
* Season = Northern-hemisphere meteorological season (assumption; the data has no hemisphere per track).
* Artists are split on `, ` with `PROTECTED_ARTIST_NAMES` (e.g. "Earth, Wind & Fire") kept whole. The source has no artist ids, so Member 3 must generate surrogate keys. Artist names that contain commas and are not in the protected list would be split incorrectly (none other was found in this sample).
* In `genre_analytics` a track with several genres is counted in each of them, so `chart_entries` must not be summed across genres.
* Audio-feature averages only cover matched rows; always read them with `track_attribute_coverage`.
* No star schema / surrogate keys here: that is Member 3's work. Natural keys provided: `chart_record_id`, `track_id`, `country_iso_alpha3`, `chart_date_id`, `genre`.
