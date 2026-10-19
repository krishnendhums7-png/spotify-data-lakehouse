# Global Spotify Music Streaming Analytics

## Data Source and Ingestion Documentation

### 1. Introduction

This document describes the datasets used in the Global Spotify Music Streaming Analytics project and the data acquisition and ingestion process implemented by Member 1.

The purpose of the ingestion module is to load CSV datasets, validate their structure, convert them into Parquet format, and store the processed files in the bronze data layer.

### 2. Datasets

The ingestion pipeline processes three datasets.

| Dataset       | Source File       | Expected Rows | Expected Columns |
| ------------- | ----------------- | ------------: | ---------------: |
| Tracks        | tracks.csv        |       114,000 |               20 |
| Charts        | charts_sample.csv |        50,000 |                9 |
| Country Codes | country_codes.csv |           249 |               11 |

The datasets provide track-related information, chart records, and country-code information for the music streaming analytics project.

The original source and licence information should be recorded here after verifying the dataset provider's details.

### 3. Data Acquisition

The source datasets are stored in the project's raw data directory.

Directory:

`data/raw/`

Expected source files:

* `tracks.csv`
* `charts_sample.csv`
* `country_codes.csv`

These files are used as inputs to the ingestion pipeline.

### 4. Ingestion Process

The ingestion module is implemented in:

`ingestion/csv_ingestion.py`

The pipeline performs the following operations:

1. Reads the source CSV files.
2. Checks that the input files exist.
3. Validates the dataset headers.
4. Detects and handles the empty first header in `tracks.csv`.
5. Converts the datasets into Parquet format.
6. Saves the processed files in the bronze data layer.
7. Records ingestion activity and errors in a log file.

### 5. Output Storage

The processed Parquet files are saved in the following locations:

* `data/bronze/tracks/data.parquet`
* `data/bronze/charts/data.parquet`
* `data/bronze/country_codes/data.parquet`

The ingestion log is saved at:

`data/logs/ingestion.log`

Parquet is used as the output format because it supports structured data storage and efficient analytical processing.

### 6. Data Validation

The ingestion module includes checks for:

* Missing source files.
* Dataset headers and column names.
* Empty CSV files.
* Duplicate column names.

The pipeline is tested to ensure that expected input problems are handled appropriately.

### 7. Testing

The ingestion module's tests are located in:

`tests/test_ingestion.py`

Run the tests from the project root directory using:

`python -m pytest tests/test_ingestion.py -v`

The previously verified test result was four passing tests.

### 8. Running the Ingestion Pipeline

From the project root directory, execute:

`python -m ingestion.csv_ingestion`

The pipeline reads the raw CSV files and generates the corresponding Parquet files in the bronze data layer.

### 9. Expected Dataset Dimensions

After successful ingestion, the expected dataset dimensions are:

| Dataset       |    Rows | Columns |
| ------------- | ------: | ------: |
| Tracks        | 114,000 |      20 |
| Charts        |  50,000 |       9 |
| Country Codes |     249 |      11 |

These dimensions should be checked against the generated Parquet files after each ingestion run.

### 10. Storage and Version Control

The ingestion code, tests, documentation, and dependency files should be maintained in GitHub.

Raw datasets, generated Parquet files, local virtual environments, and runtime logs should normally be excluded from version control.

Dataset source references, licences, and download instructions should be documented so team members can obtain the required inputs independently.

### 11. Member 1 Responsibilities

Member 1 is responsible for:

* Acquiring the required datasets.
* Organising the raw data directory.
* Implementing the CSV-to-Parquet ingestion pipeline.
* Validating input files and dataset headers.
* Maintaining ingestion unit tests.
* Documenting the ingestion workflow.
* Preparing the module for integration with the team's analytics pipeline.

### 12. Conclusion

The data ingestion module provides the initial data-processing stage for the Global Spotify Music Streaming Analytics project. It converts the three CSV datasets into Parquet files, performs basic validation, logs pipeline activity, and provides tests to verify its behaviour.
