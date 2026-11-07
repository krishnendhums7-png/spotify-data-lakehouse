# spotify-data-lakehouse
https://drive.google.com/drive/folders/1Rw8fVXcbsJxaucyR34xoGPeC6LeSOwWn?usp=sharing


---

# Global Spotify Music Streaming Analytics - Data Lakehouse
### Member 2 module: Spark ETL & Data Lakehouse (Bronze -> Silver -> Gold)

## 1. Project overview
The capstone builds an end-to-end data-engineering pipeline for global Spotify chart and track data.
This repository contains **Member 2's part**: PySpark jobs that turn three raw CSV files into a
Bronze / Silver / Gold lakehouse stored as Parquet, ready for Member 3 (dbt/star schema), Member 4
(Great Expectations/Airflow) and Member 5 (dashboard).

Input data (`data/raw/`, from `Dataset.zip`)

| File | Rows | What it is |
|---|---|---|
| `tracks.csv` | 114,000 | Spotify track catalogue with audio features and genre (a track repeats once per genre) |
| `charts_sample.csv` | 50,000 | Daily chart entries (top200 / viral50) for 58 regions, 25 dates between 2017-01-01 and 2020-06-01 |
| `country_codes.csv` | 249 | ISO country codes with UN region / sub-region |

## 2. My responsibilities (Member 2) and where they are implemented
| Responsibility | Implemented in |
|---|---|
| Bronze-Silver-Gold design | `docs/architecture.md`, folder layout in `data/` |
| PySpark ETL jobs | `spark/bronze_ingestion.py`, `bronze_to_silver.py`, `silver_to_gold.py` |
| Clean data, missing values, duplicates | `bronze_to_silver.py` (rules in `docs/transformations.md`) |
| Join datasets, standardise fields/types | `build_enriched_charts`, `standardize_*` functions |
| Silver and Gold datasets | `data/silver`, `data/gold` |
| Parquet (Delta optional) | Parquet implemented and tested; **Delta Lake not implemented** (see limitations) |
| Spark optimisation | `spark/spark_session.py`, broadcast joins, `coalesce`, caching (see architecture.md) |

## 3. Technologies
Python 3.12, Apache Spark / PySpark 4.3.0, Parquet (Snappy), pytest, pandas (profiling script only). Needs Java 17 or 21.
Tested by the author on Ubuntu 24.04 (Python 3.12.3, OpenJDK 21). **Not tested on native Windows or on your WSL** - see section 5.

## 4. Folder structure
```
spotify-data-engineering/
├── data/
│   ├── raw/        input CSVs (copy of Dataset.zip contents)
│   ├── bronze/     tracks/ charts/ countries/ + _manifest.json
│   ├── silver/     tracks/ track_genres/ charts/ countries/ enriched_chart_data/ _quarantine/
│   └── gold/       enriched_chart_data/ music_analytics/ country_analytics/
│                   artist_analytics/ genre_analytics/ monthly_analytics/
├── spark/
│   ├── spark_session.py     SparkSession factory (all tuning in one place)
│   ├── common.py            shared helpers (write_parquet, cast_with_audit, result classes)
│   ├── bronze_ingestion.py  raw CSV -> Bronze
│   ├── bronze_to_silver.py  Bronze -> Silver (+ quarantine, + integration)
│   ├── silver_to_gold.py    Silver -> Gold
│   ├── verify_outputs.py    read-back and reconciliation checks
│   └── run_pipeline.py      single entry point
├── utils/   config.py (paths, rules, aliases) | logger.py
├── tests/   conftest.py | test_bronze.py | test_silver.py | test_gold.py
├── scripts/ profile_sources.py | generate_data_dictionary.py
├── docs/    architecture.md | transformations.md | data_dictionary.md | data_profile.md
├── logs/    pipeline_<run_id>.log (from the delivered run)
├── reports/ run_summary.md / run_summary.json (from the delivered run)
├── MEMBER_2_VIVA.md   viva preparation
├── requirements.txt | pytest.ini | .gitignore | README.md
```
(Improvements over the suggested layout: `spark/common.py`, `spark/verify_outputs.py`, `scripts/`, `reports/`, `logs/`, and `data_profile.md`.)

## 5. Installation

### Option A - WSL Ubuntu (recommended for Spark on Windows)
```bash
sudo apt update && sudo apt install -y openjdk-21-jdk python3-venv unzip   # Ubuntu 22.04: use openjdk-17-jdk
cd spotify-data-engineering
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
java -version        # must print 17 or 21
```

### Option B - Windows PowerShell
1. Install a JDK 17 or 21 (e.g. Temurin) and Python 3.12. Check: `java -version`, `py -3.12 --version`.
2. ```powershell
   Set-Location spotify-data-engineering
   py -3.12 -m venv .venv
   Set-ExecutionPolicy -Scope Process Bypass      # only if activation is blocked
   .\.venv\Scripts\Activate.ps1
   python -m pip install -r requirements.txt
   ```
3. Known Windows issue (not tested by me): Spark writing Parquet on native Windows may fail with `HADOOP_HOME`/`winutils.exe` errors. If that happens, install a matching `winutils.exe` and set `HADOOP_HOME`, or simply use WSL (Option A).

If your folder is in OneDrive or has spaces, move it to a short path such as `C:\dev\spotify-data-engineering` (or `~/spotify-data-engineering` in WSL).

## 6. Running the pipeline
Put `tracks.csv`, `charts_sample.csv`, `country_codes.csv` in `data/raw/` (or place `Dataset.zip` next to the project / in `data/raw/`; it is extracted automatically).
```bash
python -m spark.run_pipeline                 # raw -> bronze -> silver -> gold -> verify   (about 1.5 minutes)
```
Options
```bash
python -m spark.run_pipeline --stages silver gold verify     # re-run only some stages (needs earlier outputs)
python -m spark.run_pipeline --data-dir D:/lake              # other data folder (or env SPOTIFY_DATA_DIR)
python -m spark.run_pipeline --log-level DEBUG --shuffle-partitions 16
```
Exit code: `0` success, `1` pipeline/validation failure, `2` unexpected error.
Outputs: `logs/pipeline_<run_id>.log`, `reports/run_summary.md|json`, and the summary table printed to the console.
Generated documentation (optional): `python scripts/profile_sources.py`, `python scripts/generate_data_dictionary.py`.

## 7. What each script does
| Script | Role |
|---|---|
| `utils/config.py` | Paths (no hard-coded machine paths), expected CSV headers, validation ranges, region aliases, protected artist names |
| `utils/logger.py` | Console + file logging |
| `spark/spark_session.py` | Creates the local SparkSession (shuffle partitions, AQE, UTC, lenient casts) |
| `spark/common.py` | `write_parquet`, `cast_with_audit`, `reason_array`, `StageResult/TableStat` |
| `spark/bronze_ingestion.py` | Extract zip if needed, check headers, read CSV as strings, add lineage, write Parquet, verify counts, write manifest |
| `spark/bronze_to_silver.py` | Standardise, cast, validate, quarantine, de-duplicate, build the enriched chart table |
| `spark/silver_to_gold.py` | Date attributes, curated and aggregated Gold datasets |
| `spark/verify_outputs.py` | Re-reads every dataset from disk; checks columns, types, row accounting, uniqueness (28 checks) |
| `spark/run_pipeline.py` | Runs the stages in order, prints/saves the summary, returns an exit code |

## 8. Layer summaries (details: `docs/transformations.md`)
* **Bronze** - exact source values as strings + `_source_file/_ingested_at/_run_id`; 114,000 / 50,000 / 249 rows (equal to the source).
* **Silver** - typed, validated, de-duplicated. tracks 114,000 -> **89,740** (1 quarantined, 24,259 genre-duplicates merged; all genres kept in `genres` and `track_genres`); charts 50,000 -> 50,000; countries 249 -> 249 (Namibia's blank ISO code recovered, Taiwan's blank region filled from config and flagged). `enriched_chart_data` = charts LEFT JOIN tracks LEFT JOIN countries: 50,000 rows, **17.11% with track attributes**, all regions resolved (57 countries + Global).
* **Gold** - `enriched_chart_data` (50,000), `music_analytics` (50,000), `country_analytics` (73), `artist_analytics` (3,363), `genre_analytics` (114), `monthly_analytics` (8).

## 9. Testing
```bash
python -m pytest -q -m "not integration"     # 46 fast unit tests (~75 s, includes Spark start-up)
python -m pytest -q -m integration           # 8 tests: run the whole pipeline on the real CSVs in a temp folder (~100 s)
python -m pytest -q                          # everything
```
Delivered result: 46 unit + 8 integration tests passed on the author's machine; the final pipeline run passed all 28 verification checks.

## 10. Limitations and future work
* **Delta Lake is not implemented** (optional in the brief). The build environment could not download the Delta JARs, so it could not be tested; Parquet is the tested baseline. Next step: add `delta-spark`, enable the Delta extensions in `spark_session.py`, and make `write_parquet` write `format("delta")`.
* Only 17.11% of chart rows match `tracks.csv`; audio-feature and genre insights describe that subset (`track_attribute_coverage` columns make this visible).
* The chart file is a sample (25 dates); monthly/seasonal conclusions are indicative only.
* No artist ids in the sources; artist names are split from a comma-separated credit string.
* Local-mode Spark only. For larger data: remove `coalesce(1)`, raise shuffle partitions, partition by date, replace `cache()` where memory is limited.
* Great Expectations suites, Airflow DAG, dbt models and the dashboard belong to Members 3-5.


---

# Member 5 Module: Analytics, Visualization and Dashboard Deployment

## 11. Streamlit Analytics Dashboard

Member 5 developed an interactive Streamlit dashboard to visualize Spotify music streaming analytics using the processed analytical datasets.

**Live Dashboard:** https://spotify-data-lakehousegit-zhtkaewezyjamvejqjzdks.streamlit.app/

**GitHub Branch:** `member5-dashboard`

### 11.1 Dashboard Features

1. **Country Analytics:** Analyses country-wise chart performance and popularity.
2. **Artist Leaderboard:** Displays artist rankings, chart entries and attributed streams.
3. **Genre Performance:** Compares streaming performance across music genres.
4. **Monthly and Seasonal Trends:** Visualizes changes in music trends over time.
5. **Top Tracks by Country:** Displays popular tracks across selected countries.
6. **Track Longevity:** Analyses track chart presence and longevity.

### 11.2 Technologies Used

- Python
- Streamlit
- Pandas
- PyArrow
- Parquet

### 11.3 Running the Dashboard Locally

Install the required dependencies:

```bash
pip install -r requirements.txt
```

Run the dashboard:

```bash
python -m streamlit run dashboard/app.py
```

The dashboard will be available at `http://localhost:8501`.

### 11.4 Testing and Deployment

The dashboard was deployed using Streamlit Community Cloud.

The following checks were performed:

- Verified all eight analytical Parquet files.
- Successfully loaded the datasets using Pandas.
- Checked package dependencies.
- Verified Python syntax.
- Tested all six dashboard sections and interactive filters.
- Verified the live dashboard deployment.