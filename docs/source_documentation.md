# Global Spotify Music Streaming Analytics

## Data Source and Ingestion Documentation

### 1. Introduction

This document describes the datasets used in the Global Spotify Music Streaming Analytics project and the data acquisition and ingestion process implemented by Member 1.

The purpose of the ingestion modules is to load CSV datasets, validate their structure, convert them into Parquet format, and store the processed files in the bronze data layer.

### 2. Datasets

The project currently uses the following three raw datasets.

| Dataset       | Source File         | Expected Rows | Expected Columns |
| ------------- | ------------------- | ------------: | ---------------: |
| Tracks        | `tracks.csv`        |       114,000 |               20 |
| Charts        | `charts_sample.csv` |        50,000 |                9 |
| Country Codes | `country_codes.csv` |           249 |               11 |

The datasets provide track-related information, chart records, and country-code information for the music streaming analytics project.

**Artist data:** Artist names are extracted from the `artists` column of `tracks.csv`. The artist dataset is derived from the existing tracks dataset and is not currently collected from a separate artist source.

The original dataset providers, download references, and licence information must be recorded after they have been verified.

### 3. Data Acquisition

The raw source datasets are stored in:

`data/raw/`

Expected source files:

* `tracks.csv`
* `charts_sample.csv`
* `country_codes.csv`

These files provide the input data for the ingestion pipeline.

### 4. Ingestion Modules

The project contains the following ingestion scripts.

| Script                                 | Purpose                                                                         |
| -------------------------------------- | ------------------------------------------------------------------------------- |
| `ingestion/csv_ingestion.py`           | Ingests the three raw CSV datasets and converts them into Parquet format.       |
| `ingestion/track_ingestion.py`         | Separate track ingestion script.                                                |
| `ingestion/chart_ingestion.py`         | Separate chart ingestion script.                                                |
| `ingestion/country_codes_ingestion.py` | Separate country-code ingestion script.                                         |
| `ingestion/artist_ingestion.py`        | Extracts unique artist names from the tracks dataset and saves them separately. |

The main CSV ingestion pipeline performs these operations:

1. Reads the source CSV files.
2. Checks that the input files exist.
3. Validates dataset headers.
4. Detects and handles the empty first header in `tracks.csv`.
5. Converts the datasets into Parquet format.
6. Saves the output files in the bronze data layer.
7. Records ingestion activity and errors in a log file.

### 5. Artist Ingestion Process

The artist ingestion module is implemented in:

`ingestion/artist_ingestion.py`

It performs the following operations:

1. Reads the `artists` column from `data/raw/tracks.csv`.
2. Removes missing and empty artist values.
3. Removes leading and trailing whitespace.
4. Splits semicolon-separated artist names into separate entries.
5. Removes duplicate artist names.
6. Sorts the names alphabetically.
7. Saves the result as a Parquet file.

The output contains one column:

`artist_name`

The latest successful run extracted **29,858 unique artist-name entries** from the source dataset.

These are unique names, not necessarily verified individual artist identities. Artist IDs, countries, and biographies are not provided by this extraction process.

### 6. Output Storage

The main CSV ingestion pipeline generates these Parquet files:

* `data/bronze/tracks/data.parquet`
* `data/bronze/charts/data.parquet`
* `data/bronze/country_codes/data.parquet`

The artist ingestion module generates:

* `data/bronze/artists/data.parquet`

The ingestion log is stored at:

`data/logs/ingestion.log`

Parquet is used because it supports structured data storage and efficient analytical processing.

### 7. Data Validation

The existing ingestion tests cover:

* Successful CSV ingestion.
* Missing source files.
* Duplicate column names.
* Empty CSV files.

The artist ingestion tests cover:

* Successful extraction and Parquet output creation.
* Splitting multiple semicolon-separated artist names.
* Handling a missing source file.

### 8. Testing

The main ingestion tests are located in:

`tests/test_ingestion.py`

The artist ingestion tests are located in:

`tests/test_artist_ingestion.py`

Run the artist-specific tests from the project root:

`python -m pytest tests/test_artist_ingestion.py -v`

Run all ingestion tests:

`python -m pytest tests -v`

The latest verified results were:

* Artist ingestion tests: **3 passed**.
* All ingestion tests combined: **7 passed**.

### 9. Running the Ingestion Modules

Run the main CSV ingestion pipeline from the project root:

`python -m ingestion.csv_ingestion`

Run artist ingestion separately:

`python -m ingestion.artist_ingestion`

The artist module creates its output directory automatically if it does not already exist.

### 10. Expected Dataset Dimensions

The expected dimensions of the three original datasets are:

| Dataset       |    Rows | Columns |
| ------------- | ------: | ------: |
| Tracks        | 114,000 |      20 |
| Charts        |  50,000 |       9 |
| Country Codes |     249 |      11 |

The artist dataset contains one column, `artist_name`. Its row count depends on the unique artist names extracted from the tracks dataset. The latest verified output contained 29,858 rows.

These dimensions should be checked against the generated Parquet files after ingestion runs.

### 11. Storage and Version Control

The ingestion code, tests, documentation, and dependency files should be maintained in GitHub.

Raw datasets, generated Parquet files, local virtual environments, and runtime logs should normally be excluded from version control.

Dataset source references, licences, and download instructions should be documented so that team members can obtain the required inputs independently.

### 12. Member 1 Responsibilities

Member 1 is responsible for:

* Acquiring the required datasets.
* Organising the raw data directory.
* Implementing data ingestion scripts.
* Converting CSV datasets into Parquet format.
* Extracting artist names from the tracks dataset.
* Validating input files and dataset headers.
* Maintaining ingestion unit tests.
* Recording ingestion activity and errors.
* Documenting the ingestion workflow.
* Preparing the module for integration with the team's analytics pipeline.

### 13. Conclusion

The data acquisition and ingestion component provides the initial data-processing stage for the Global Spotify Music Streaming Analytics project.

The current pipeline converts three raw CSV datasets into Parquet files, performs basic validation, records processing activity, and includes automated tests.

The additional artist ingestion module extracts unique artist names from the existing tracks dataset and saves them as a separate Parquet dataset. The latest test run confirmed that all seven ingestion tests passed.
