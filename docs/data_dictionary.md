# Data dictionary (generated from the Parquet files by `scripts/generate_data_dictionary.py`)

Types are the Spark types of the columns actually on disk. See `docs/transformations.md` for the rules behind them.

## bronze/tracks

- Rows: **114,000**  - Grain: one row per source CSV record

| Column | Type | Description |
|---|---|---|
| _c0 | string | Blank-named first CSV column (artificial 0..n-1 pandas index in tracks.csv). Dropped in Silver. |
| track_id | string | Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts. |
| artists | string | Artist names as in the source, ';'-separated. |
| album_name | string | Album title. |
| track_name | string | Track title. |
| popularity | string | Spotify popularity 0-100 (for a track repeated across genres: the highest value). |
| duration_ms | string | Track length in milliseconds. |
| explicit | string | True if the track has explicit lyrics. |
| danceability | string | Audio feature 0-1: suitability for dancing. |
| energy | string | Audio feature 0-1: intensity/activity. |
| key | string | Pitch class 0-11 (0 = C). |
| loudness | string | Overall loudness in dB (typically -60..0). |
| mode | string | 1 = major, 0 = minor. |
| speechiness | string | Audio feature 0-1: presence of spoken words. |
| acousticness | string | Audio feature 0-1: confidence the track is acoustic. |
| instrumentalness | string | Audio feature 0-1: likelihood of no vocals. |
| liveness | string | Audio feature 0-1: live-audience presence. |
| valence | string | Audio feature 0-1: musical positiveness. |
| tempo | string | Estimated tempo in BPM; NULL when the source value was the invalid 0/out-of-range. |
| time_signature | string | Estimated beats per bar (1-7); NULL when the source value was the invalid 0. |
| track_genre | string | Genre label of one source row (Bronze; a track repeats once per genre). |
| _source_file | string | Name of the raw CSV the row came from (lineage). |
| _ingested_at | timestamp | UTC timestamp when the Bronze load ran. |
| _run_id | string | Pipeline run identifier (UTC timestamp) of the Bronze load. |

## bronze/charts

- Rows: **50,000**  - Grain: one row per source CSV record

| Column | Type | Description |
|---|---|---|
| title | string | Source column, stored as string exactly as in the CSV. |
| rank | string | Source column, stored as string exactly as in the CSV. |
| date | string | Source column, stored as string exactly as in the CSV. |
| artist | string | Source column, stored as string exactly as in the CSV. |
| url | string | Source column, stored as string exactly as in the CSV. |
| region | string | Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent). |
| chart | string | Source column, stored as string exactly as in the CSV. |
| trend | string | Movement vs previous day: MOVE_UP, MOVE_DOWN, SAME_POSITION, NEW_ENTRY. |
| streams | string | Daily stream count; NULL when the source has none (never imputed). |
| _source_file | string | Name of the raw CSV the row came from (lineage). |
| _ingested_at | timestamp | UTC timestamp when the Bronze load ran. |
| _run_id | string | Pipeline run identifier (UTC timestamp) of the Bronze load. |

## bronze/countries

- Rows: **249**  - Grain: one row per source CSV record

| Column | Type | Description |
|---|---|---|
| name | string | Source column, stored as string exactly as in the CSV. |
| alpha-2 | string | Source column, stored as string exactly as in the CSV. |
| alpha-3 | string | Source column, stored as string exactly as in the CSV. |
| country-code | string | Source column, stored as string exactly as in the CSV. |
| iso_3166-2 | string | Source column, stored as string exactly as in the CSV. |
| region | string | Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent). |
| sub-region | string | Source column, stored as string exactly as in the CSV. |
| intermediate-region | string | Source column, stored as string exactly as in the CSV. |
| region-code | string | Source column, stored as string exactly as in the CSV. |
| sub-region-code | string | Source column, stored as string exactly as in the CSV. |
| intermediate-region-code | string | Source column, stored as string exactly as in the CSV. |
| _source_file | string | Name of the raw CSV the row came from (lineage). |
| _ingested_at | timestamp | UTC timestamp when the Bronze load ran. |
| _run_id | string | Pipeline run identifier (UTC timestamp) of the Bronze load. |

## silver/tracks

- Rows: **89,740**  - Grain: one row per track_id

| Column | Type | Description |
|---|---|---|
| track_id | string | Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts. |
| track_name | string | Track title. |
| artists | string | Artist names as in the source, ';'-separated. |
| artist_list | array<string> | artists split on ';' into an array. |
| primary_artist | string | First artist in artist_list. |
| artist_count | int | Number of artists credited on the track. |
| album_name | string | Album title. |
| popularity | int | Spotify popularity 0-100 (for a track repeated across genres: the highest value). |
| duration_ms | bigint | Track length in milliseconds. |
| explicit | boolean | True if the track has explicit lyrics. |
| danceability | double | Audio feature 0-1: suitability for dancing. |
| energy | double | Audio feature 0-1: intensity/activity. |
| key | int | Pitch class 0-11 (0 = C). |
| loudness | double | Overall loudness in dB (typically -60..0). |
| mode | int | 1 = major, 0 = minor. |
| speechiness | double | Audio feature 0-1: presence of spoken words. |
| acousticness | double | Audio feature 0-1: confidence the track is acoustic. |
| instrumentalness | double | Audio feature 0-1: likelihood of no vocals. |
| liveness | double | Audio feature 0-1: live-audience presence. |
| valence | double | Audio feature 0-1: musical positiveness. |
| tempo | double | Estimated tempo in BPM; NULL when the source value was the invalid 0/out-of-range. |
| time_signature | int | Estimated beats per bar (1-7); NULL when the source value was the invalid 0. |
| has_valid_tempo | boolean | False when the source tempo was 0 / out of range (tempo set to NULL). |
| has_valid_time_signature | boolean | False when the source time_signature was 0 (set to NULL). |
| primary_genre | string | Genre of the representative row kept by de-duplication (highest popularity, then alphabetical). |
| genres | array<string> | Sorted array of ALL genres the track appears under in the source. |
| genre_count | int | Number of genres in `genres`. |
| source_row_count | bigint | How many Bronze rows were merged into this track. |
| popularity_conflict | boolean | True if the source rows of this track had different popularity values. |

## silver/track_genres

- Rows: **113,549**  - Grain: one row per (track_id, genre)

| Column | Type | Description |
|---|---|---|
| track_id | string | Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts. |
| genre | string | Genre label. |

## silver/charts

- Rows: **50,000**  - Grain: one row per (chart_date, region, chart_type, chart_rank)

| Column | Type | Description |
|---|---|---|
| chart_date | date | Date of the chart snapshot. |
| chart_type | string | 'top200' or 'viral50'. |
| region | string | Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent). |
| region_country_name | string | Official country name used to join country_codes (alias-resolved); NULL for 'Global'. |
| is_global_region | boolean | True for the worldwide 'Global' chart (not a country). |
| chart_rank | int | Position on the chart (1-200 for top200, 1-50 for viral50). |
| track_id | string | Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts. |
| track_title | string | Track title as printed on the chart. |
| chart_artist | string | Artist credit string on the chart (', '-separated). |
| trend | string | Movement vs previous day: MOVE_UP, MOVE_DOWN, SAME_POSITION, NEW_ENTRY. |
| streams | bigint | Daily stream count; NULL when the source has none (never imputed). |
| has_streams | boolean | True when streams is not NULL. |
| track_url | string | Original Spotify URL from the source. |

## silver/countries

- Rows: **249**  - Grain: one row per iso_alpha3

| Column | Type | Description |
|---|---|---|
| country_name | string | Official country / territory name (ISO 3166 style). |
| iso_alpha2 | string | ISO 3166-1 alpha-2 code. |
| iso_alpha3 | string | ISO 3166-1 alpha-3 code. Primary key of silver/countries. |
| iso_numeric_code | int | ISO 3166-1 numeric code. |
| iso_3166_2 | string | ISO 3166-2 reference string. |
| continent | string | UN M49 top-level region (Africa, Americas, Asia, Europe, Oceania); same value as `region`. |
| region | string | Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent). |
| sub_region | string | UN M49 sub-region. |
| intermediate_region | string | UN M49 intermediate region (NULL where not applicable). |
| region_code | int | UN M49 region code. |
| sub_region_code | int | UN M49 sub-region code. |
| intermediate_region_code | int | UN M49 intermediate-region code. |
| geography_source | string | 'source' | 'manual_override' (filled from config, e.g. Taiwan) | 'missing' (e.g. Antarctica). |
| has_geography | boolean | True when region information is available. |
| iso_alpha2_recovered | boolean | True if alpha-2 was blank in the source and was recovered from iso_3166_2 (Namibia). |

## silver/enriched_chart_data

- Rows: **50,000**  - Grain: one row per chart entry (= silver/charts)

| Column | Type | Description |
|---|---|---|
| chart_date | date | Date of the chart snapshot. |
| chart_type | string | 'top200' or 'viral50'. |
| region | string | Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent). |
| region_country_name | string | Official country name used to join country_codes (alias-resolved); NULL for 'Global'. |
| is_global_region | boolean | True for the worldwide 'Global' chart (not a country). |
| chart_rank | int | Position on the chart (1-200 for top200, 1-50 for viral50). |
| track_id | string | Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts. |
| track_title | string | Track title as printed on the chart. |
| chart_artist | string | Artist credit string on the chart (', '-separated). |
| trend | string | Movement vs previous day: MOVE_UP, MOVE_DOWN, SAME_POSITION, NEW_ENTRY. |
| streams | bigint | Daily stream count; NULL when the source has none (never imputed). |
| has_streams | boolean | True when streams is not NULL. |
| track_url | string | Original Spotify URL from the source. |
| track_name | string | Track title. |
| track_artists | string | tracks.artists for the matched track. |
| track_album_name | string | tracks.album_name for the matched track. |
| track_popularity | int | tracks.popularity of the matched track. |
| track_duration_ms | bigint | tracks.duration_ms of the matched track. |
| track_explicit | boolean | tracks.explicit of the matched track. |
| primary_genre | string | Genre of the representative row kept by de-duplication (highest popularity, then alphabetical). |
| genres | array<string> | Sorted array of ALL genres the track appears under in the source. |
| genre_count | int | Number of genres in `genres`. |
| danceability | double | Audio feature 0-1: suitability for dancing. |
| energy | double | Audio feature 0-1: intensity/activity. |
| key | int | Pitch class 0-11 (0 = C). |
| loudness | double | Overall loudness in dB (typically -60..0). |
| mode | int | 1 = major, 0 = minor. |
| speechiness | double | Audio feature 0-1: presence of spoken words. |
| acousticness | double | Audio feature 0-1: confidence the track is acoustic. |
| instrumentalness | double | Audio feature 0-1: likelihood of no vocals. |
| liveness | double | Audio feature 0-1: live-audience presence. |
| valence | double | Audio feature 0-1: musical positiveness. |
| tempo | double | Estimated tempo in BPM; NULL when the source value was the invalid 0/out-of-range. |
| time_signature | int | Estimated beats per bar (1-7); NULL when the source value was the invalid 0. |
| is_track_matched | boolean | True when track_id exists in silver/tracks (else all track attributes are NULL). |
| country_iso_alpha2 | string | ISO alpha-2 of the matched country. |
| country_iso_alpha3 | string | ISO alpha-3 of the matched country. |
| country_continent | string | Continent (UN region) of the matched country. |
| country_region | string | UN region of the matched country. |
| country_sub_region | string | UN sub-region of the matched country. |
| country_intermediate_region | string | UN intermediate region. |
| country_geography_source | string | geography_source of the matched country. |
| region_match_status | string | 'matched' | 'global_aggregate' | 'unmatched' (chart region vs country table). |

## silver/_quarantine/tracks

- Rows: **1**  - Grain: rejected source records + reject_reasons

| Column | Type | Description |
|---|---|---|
| _c0 | string | Blank-named first CSV column (artificial 0..n-1 pandas index in tracks.csv). Dropped in Silver. |
| track_id | string | Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts. |
| artists | string | Artist names as in the source, ';'-separated. |
| album_name | string | Album title. |
| track_name | string | Track title. |
| popularity | string | Spotify popularity 0-100 (for a track repeated across genres: the highest value). |
| duration_ms | string | Track length in milliseconds. |
| explicit | string | True if the track has explicit lyrics. |
| danceability | string | Audio feature 0-1: suitability for dancing. |
| energy | string | Audio feature 0-1: intensity/activity. |
| key | string | Pitch class 0-11 (0 = C). |
| loudness | string | Overall loudness in dB (typically -60..0). |
| mode | string | 1 = major, 0 = minor. |
| speechiness | string | Audio feature 0-1: presence of spoken words. |
| acousticness | string | Audio feature 0-1: confidence the track is acoustic. |
| instrumentalness | string | Audio feature 0-1: likelihood of no vocals. |
| liveness | string | Audio feature 0-1: live-audience presence. |
| valence | string | Audio feature 0-1: musical positiveness. |
| tempo | string | Estimated tempo in BPM; NULL when the source value was the invalid 0/out-of-range. |
| time_signature | string | Estimated beats per bar (1-7); NULL when the source value was the invalid 0. |
| track_genre | string | Genre label of one source row (Bronze; a track repeats once per genre). |
| reject_reasons | string | ';'-separated list of failed validation rules. |

## gold/enriched_chart_data

- Rows: **50,000**  - Grain: one row per chart entry

| Column | Type | Description |
|---|---|---|
| chart_date | date | Date of the chart snapshot. |
| chart_type | string | 'top200' or 'viral50'. |
| region | string | Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent). |
| region_country_name | string | Official country name used to join country_codes (alias-resolved); NULL for 'Global'. |
| is_global_region | boolean | True for the worldwide 'Global' chart (not a country). |
| chart_rank | int | Position on the chart (1-200 for top200, 1-50 for viral50). |
| track_id | string | Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts. |
| track_title | string | Track title as printed on the chart. |
| chart_artist | string | Artist credit string on the chart (', '-separated). |
| trend | string | Movement vs previous day: MOVE_UP, MOVE_DOWN, SAME_POSITION, NEW_ENTRY. |
| streams | bigint | Daily stream count; NULL when the source has none (never imputed). |
| has_streams | boolean | True when streams is not NULL. |
| track_url | string | Original Spotify URL from the source. |
| track_name | string | Track title. |
| track_artists | string | tracks.artists for the matched track. |
| track_album_name | string | tracks.album_name for the matched track. |
| track_popularity | int | tracks.popularity of the matched track. |
| track_duration_ms | bigint | tracks.duration_ms of the matched track. |
| track_explicit | boolean | tracks.explicit of the matched track. |
| primary_genre | string | Genre of the representative row kept by de-duplication (highest popularity, then alphabetical). |
| genres | array<string> | Sorted array of ALL genres the track appears under in the source. |
| genre_count | int | Number of genres in `genres`. |
| danceability | double | Audio feature 0-1: suitability for dancing. |
| energy | double | Audio feature 0-1: intensity/activity. |
| key | int | Pitch class 0-11 (0 = C). |
| loudness | double | Overall loudness in dB (typically -60..0). |
| mode | int | 1 = major, 0 = minor. |
| speechiness | double | Audio feature 0-1: presence of spoken words. |
| acousticness | double | Audio feature 0-1: confidence the track is acoustic. |
| instrumentalness | double | Audio feature 0-1: likelihood of no vocals. |
| liveness | double | Audio feature 0-1: live-audience presence. |
| valence | double | Audio feature 0-1: musical positiveness. |
| tempo | double | Estimated tempo in BPM; NULL when the source value was the invalid 0/out-of-range. |
| time_signature | int | Estimated beats per bar (1-7); NULL when the source value was the invalid 0. |
| is_track_matched | boolean | True when track_id exists in silver/tracks (else all track attributes are NULL). |
| country_iso_alpha2 | string | ISO alpha-2 of the matched country. |
| country_iso_alpha3 | string | ISO alpha-3 of the matched country. |
| country_continent | string | Continent (UN region) of the matched country. |
| country_region | string | UN region of the matched country. |
| country_sub_region | string | UN sub-region of the matched country. |
| country_intermediate_region | string | UN intermediate region. |
| country_geography_source | string | geography_source of the matched country. |
| region_match_status | string | 'matched' | 'global_aggregate' | 'unmatched' (chart region vs country table). |
| chart_date_id | int | Date as integer yyyymmdd (candidate Dim_Date key). |
| chart_quarter | int | Quarter 1-4. |
| chart_month | int | Month 1-12. |
| chart_year_month | string | 'yyyy-MM'. |
| chart_day | int | Day of month. |
| chart_day_of_week | string | Weekday name. |
| chart_season | string | Northern-hemisphere meteorological season (Dec-Feb Winter, Mar-May Spring, Jun-Aug Summer, Sep-Nov Autumn). |
| chart_record_id | string | SHA-256 of (date|region|chart_type|rank): deterministic natural key of a chart entry. |
| chart_year | int | Year of chart_date. |

## gold/music_analytics

- Rows: **50,000**  - Grain: one row per chart entry

| Column | Type | Description |
|---|---|---|
| chart_record_id | string | SHA-256 of (date|region|chart_type|rank): deterministic natural key of a chart entry. |
| chart_date | date | Date of the chart snapshot. |
| chart_date_id | int | Date as integer yyyymmdd (candidate Dim_Date key). |
| chart_quarter | int | Quarter 1-4. |
| chart_month | int | Month 1-12. |
| chart_year_month | string | 'yyyy-MM'. |
| chart_season | string | Northern-hemisphere meteorological season (Dec-Feb Winter, Mar-May Spring, Jun-Aug Summer, Sep-Nov Autumn). |
| chart_type | string | 'top200' or 'viral50'. |
| chart_rank | int | Position on the chart (1-200 for top200, 1-50 for viral50). |
| streams | bigint | Daily stream count; NULL when the source has none (never imputed). |
| trend | string | Movement vs previous day: MOVE_UP, MOVE_DOWN, SAME_POSITION, NEW_ENTRY. |
| region | string | Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent). |
| is_global_region | boolean | True for the worldwide 'Global' chart (not a country). |
| country_iso_alpha2 | string | ISO alpha-2 of the matched country. |
| country_iso_alpha3 | string | ISO alpha-3 of the matched country. |
| country_continent | string | Continent (UN region) of the matched country. |
| country_region | string | UN region of the matched country. |
| country_sub_region | string | UN sub-region of the matched country. |
| region_match_status | string | 'matched' | 'global_aggregate' | 'unmatched' (chart region vs country table). |
| track_id | string | Spotify track id (22 base-62 characters). Primary key of silver/tracks; join key to charts. |
| track_name | string | Track title. |
| chart_artist | string | Artist credit string on the chart (', '-separated). |
| track_album_name | string | tracks.album_name for the matched track. |
| primary_genre | string | Genre of the representative row kept by de-duplication (highest popularity, then alphabetical). |
| genres | array<string> | Sorted array of ALL genres the track appears under in the source. |
| track_popularity | int | tracks.popularity of the matched track. |
| track_duration_ms | bigint | tracks.duration_ms of the matched track. |
| track_explicit | boolean | tracks.explicit of the matched track. |
| danceability | double | Audio feature 0-1: suitability for dancing. |
| energy | double | Audio feature 0-1: intensity/activity. |
| speechiness | double | Audio feature 0-1: presence of spoken words. |
| acousticness | double | Audio feature 0-1: confidence the track is acoustic. |
| instrumentalness | double | Audio feature 0-1: likelihood of no vocals. |
| liveness | double | Audio feature 0-1: live-audience presence. |
| valence | double | Audio feature 0-1: musical positiveness. |
| loudness | double | Overall loudness in dB (typically -60..0). |
| tempo | double | Estimated tempo in BPM; NULL when the source value was the invalid 0/out-of-range. |
| key | int | Pitch class 0-11 (0 = C). |
| mode | int | 1 = major, 0 = minor. |
| time_signature | int | Estimated beats per bar (1-7); NULL when the source value was the invalid 0. |
| is_track_matched | boolean | True when track_id exists in silver/tracks (else all track attributes are NULL). |
| chart_year | int | Year of chart_date. |

## gold/country_analytics

- Rows: **73**  - Grain: region x chart_type

| Column | Type | Description |
|---|---|---|
| region | string | Chart region as written in the source (trimmed); for country tables: UN M49 top-level region (see continent). |
| chart_type | string | 'top200' or 'viral50'. |
| country_iso_alpha2 | string | ISO alpha-2 of the matched country. |
| country_iso_alpha3 | string | ISO alpha-3 of the matched country. |
| country_continent | string | Continent (UN region) of the matched country. |
| country_sub_region | string | UN sub-region of the matched country. |
| is_global_region | boolean | True for the worldwide 'Global' chart (not a country). |
| chart_entries | bigint | Number of chart rows in the group. |
| distinct_chart_dates | bigint | Distinct snapshot dates in the group. |
| distinct_tracks | bigint | Distinct track_ids in the group. |
| distinct_artist_credits | bigint | Distinct chart_artist strings. |
| first_chart_date | date | Earliest chart_date. |
| last_chart_date | date | Latest chart_date. |
| total_streams | bigint | Sum of streams (NULLs ignored; NULL if no stream data). |
| avg_streams | double | Average streams over rows that have streams. |
| avg_chart_rank | double | Average chart_rank. |
| entries_with_track_attributes | bigint | Chart rows whose track exists in tracks.csv. |
| track_attribute_coverage | double | Share (0-1) of chart rows with track attributes: how much to trust the audio averages. |
| avg_track_popularity | double | Average popularity over matched rows. |
| avg_danceability | double | Average danceability over chart rows with track attributes. |
| avg_energy | double | Average energy over chart rows with track attributes. |
| avg_speechiness | double | Average speechiness over chart rows with track attributes. |
| avg_acousticness | double | Average acousticness over chart rows with track attributes. |
| avg_instrumentalness | double | Average instrumentalness over chart rows with track attributes. |
| avg_liveness | double | Average liveness over chart rows with track attributes. |
| avg_valence | double | Average valence over chart rows with track attributes. |
| avg_loudness | double | Average loudness over chart rows with track attributes. |
| avg_tempo | double | Average tempo over chart rows with track attributes. |

## gold/artist_analytics

- Rows: **3,363**  - Grain: credited artist

| Column | Type | Description |
|---|---|---|
| artist_key | string | Lower-cased artist name (grouping key). |
| artist_name | string | Credited artist (chart_artist split on ', ', protected names kept whole). |
| chart_entries | bigint | Number of chart rows in the group. |
| distinct_tracks | bigint | Distinct track_ids in the group. |
| distinct_chart_dates | bigint | Distinct snapshot dates in the group. |
| distinct_countries_charted | bigint | Distinct non-Global regions where the artist charted. |
| first_chart_date | date | Earliest chart_date. |
| last_chart_date | date | Latest chart_date. |
| chart_span_days | int | last_chart_date - first_chart_date in days. |
| best_chart_rank | int | Lowest (best) rank reached. |
| avg_chart_rank | double | Average chart_rank. |
| times_ranked_first | bigint | Number of entries at rank 1. |
| total_streams | bigint | Sum of streams (NULLs ignored; NULL if no stream data). |
| avg_streams | double | Average streams over rows that have streams. |

## gold/genre_analytics

- Rows: **114**  - Grain: genre

| Column | Type | Description |
|---|---|---|
| genre | string | Genre label. |
| catalogue_tracks | bigint | Distinct tracks in tracks.csv carrying this genre. |
| avg_popularity | double | Average popularity of catalogue tracks. |
| explicit_share | double | Share (0-1) of catalogue tracks that are explicit. |
| avg_duration_min | double | Average duration in minutes. |
| catalogue_avg_danceability | double | Average danceability over all catalogue tracks of the genre. |
| catalogue_avg_energy | double | Average energy over all catalogue tracks of the genre. |
| catalogue_avg_speechiness | double | Average speechiness over all catalogue tracks of the genre. |
| catalogue_avg_acousticness | double | Average acousticness over all catalogue tracks of the genre. |
| catalogue_avg_instrumentalness | double | Average instrumentalness over all catalogue tracks of the genre. |
| catalogue_avg_liveness | double | Average liveness over all catalogue tracks of the genre. |
| catalogue_avg_valence | double | Average valence over all catalogue tracks of the genre. |
| catalogue_avg_loudness | double | Average loudness over all catalogue tracks of the genre. |
| catalogue_avg_tempo | double | Average tempo over all catalogue tracks of the genre. |
| chart_entries | bigint | Number of chart rows in the group. |
| charted_tracks | bigint | Distinct catalogue tracks of this genre that appear in the chart sample. |
| distinct_regions | bigint | Distinct chart regions. |
| avg_chart_rank | double | Average chart_rank. |
| total_streams | bigint | Sum of streams (NULLs ignored; NULL if no stream data). |

## gold/monthly_analytics

- Rows: **8**  - Grain: year-month x chart_type

| Column | Type | Description |
|---|---|---|
| chart_year | int | Year of chart_date. |
| chart_month | int | Month 1-12. |
| chart_year_month | string | 'yyyy-MM'. |
| chart_season | string | Northern-hemisphere meteorological season (Dec-Feb Winter, Mar-May Spring, Jun-Aug Summer, Sep-Nov Autumn). |
| chart_type | string | 'top200' or 'viral50'. |
| chart_entries | bigint | Number of chart rows in the group. |
| distinct_chart_dates | bigint | Distinct snapshot dates in the group. |
| distinct_tracks | bigint | Distinct track_ids in the group. |
| distinct_regions | bigint | Distinct chart regions. |
| total_streams | bigint | Sum of streams (NULLs ignored; NULL if no stream data). |
| avg_streams | double | Average streams over rows that have streams. |
| track_attribute_coverage | double | Share (0-1) of chart rows with track attributes: how much to trust the audio averages. |
| avg_danceability | double | Average danceability over chart rows with track attributes. |
| avg_energy | double | Average energy over chart rows with track attributes. |
| avg_speechiness | double | Average speechiness over chart rows with track attributes. |
| avg_acousticness | double | Average acousticness over chart rows with track attributes. |
| avg_instrumentalness | double | Average instrumentalness over chart rows with track attributes. |
| avg_liveness | double | Average liveness over chart rows with track attributes. |
| avg_valence | double | Average valence over chart rows with track attributes. |
| avg_loudness | double | Average loudness over chart rows with track attributes. |
| avg_tempo | double | Average tempo over chart rows with track attributes. |
