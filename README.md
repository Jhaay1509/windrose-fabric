# Windrose — Weather Data Engineering with Microsoft Fabric

A personal data engineering project exploring the ingestion, transformation and analytical processing of weather data using Microsoft Fabric.

The project uses the Open-Meteo API as its data source and implements a Medallion architecture with Python, PySpark, OneLake and Delta Lake.

**Status:** In active development.

## Project objectives

- Ingest weather forecast data from a REST API.
- Preserve raw source data for traceability and reprocessing.
- Transform nested JSON into structured analytical datasets.
- Implement data quality checks and incremental upserts.
- Build reporting-ready Gold tables.
- Orchestrate and monitor the pipeline using Microsoft Fabric.

## Architecture

**Bronze — Raw ingestion**

A Python notebook requests hourly forecast data from Open-Meteo and stores the raw JSON response in OneLake Files.

**Silver — Cleaning and transformation**

A PySpark notebook:

- Reads raw Bronze JSON.
- Flattens nested hourly arrays using Spark transformations.
- Converts source fields to appropriate data types.
- Validates critical fields and checks for duplicate business keys.
- Uses Delta Lake MERGE to update existing records and insert new records.

The business key for hourly forecasts is the combination of `city` and `event_time`.

**Gold — Analytical layer (planned)**

Daily weather aggregations and reporting-ready Delta tables.

## Technologies

- Microsoft Fabric
- OneLake and Lakehouse
- Python and PySpark
- Delta Lake
- Dataflow Gen2 and Power Query
- Spark SQL
- GitHub

## Additional implementation

A separate Dataflow Gen2 exercise demonstrates ingestion and transformation using Power Query, with the resulting data loaded into a Lakehouse Delta table.

This provides a comparison between low-code and programmatic data engineering approaches.

## Data quality and testing

The Silver implementation has been tested for:

- Duplicate prevention using the `city` and `event_time` business key.
- Repeat processing of the same source batch.
- Updates to existing records.
- Insertion of new hourly records.

**Observed test results:**

- Initial ingestion: 48 hourly records.
- Repeat ingestion: 48 records, with no duplicate business keys.
- Separate MERGE test: an existing temperature updated from 9.0°C to 10.0°C.
- MERGE test record count increased from 48 to 49 following insertion of a new hourly record.

The MERGE tests were performed against a separate Delta table to protect the main Silver dataset.

## Repository structure

The `notebooks/` directory currently contains:

- `nb_windrose_bronze_ingest.ipynb`
- `nb_windrose_silver_transform.ipynb`

## Development roadmap

## Development roadmap

- [x] Raw API ingestion into OneLake
- [x] PySpark Silver transformations
- [x] Data quality validation
- [x] Delta Lake MERGE implementation
- [x] Gold-layer modelling and daily aggregation
- [x] Gold Delta table creation and validation
- [x] Parameterized Bronze and Silver notebooks
- [ ] Automated pipeline execution and testing
- [ ] Scheduling and monitoring
- [ ] CI/CD and deployment exercises

## Project context

This implementation forms part of my practical preparation for Microsoft Fabric Data Engineer certification (DP-700), applying established data engineering principles to the Fabric ecosystem.

The project is developed incrementally, with each stage demonstrating practical engineering concepts and design decisions.
