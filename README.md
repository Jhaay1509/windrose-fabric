# Windrose — Weather Data Engineering with Microsoft Fabric

A personal data engineering project exploring the ingestion, transformation, orchestration and analytical processing of weather data using Microsoft Fabric.

Windrose uses the Open-Meteo REST API as its data source and implements a Medallion Architecture with Python, PySpark, OneLake and Delta Lake.

The project focuses on practical data engineering principles, including incremental processing, idempotency, data quality, parameterised pipelines, Spark session management and workflow orchestration.

**Status:** Core Bronze, Silver and Gold pipeline implemented and successfully tested. Further development ongoing.

## Project Objectives

- Ingest weather forecast data from a REST API.
- Preserve raw source data for traceability and reprocessing.
- Transform nested JSON into structured analytical datasets.
- Implement data quality checks and incremental upserts.
- Build reporting-ready Gold tables.
- Orchestrate notebook execution using Microsoft Fabric Data Factory.
- Explore Spark performance, capacity management and deployment practices.

## Architecture

The current workflow follows:

**Fabric Data Factory → Orchestrator Notebook → Bronze → Silver → Gold**

The Data Factory pipeline triggers a parent notebook, which manages the execution of three processing notebooks with explicit dependencies.

### Bronze — Raw Ingestion

A Python notebook requests hourly weather forecasts for Edinburgh from the Open-Meteo API.

The implementation:

- Retrieves temperature and relative humidity forecasts using Python `requests`.
- Preserves the original JSON response in OneLake Files.
- Organises raw files using an ingestion-date folder structure.
- Supports a `run_date` parameter to identify the ingestion batch.

Example storage structure:

`Files/bronze/open_meteo/forecast/edinburgh/ingestion_date=2026-11-23/response.json`

Preserving the source response allows raw data to be inspected and reprocessed independently of downstream transformations.

### Silver — Cleaning and Transformation

A PySpark notebook reads the raw Bronze JSON and transforms the nested hourly arrays into structured records.

The implementation:

- Reads JSON using Spark's DataFrame API.
- Uses `arrays_zip()` and `explode()` to align and flatten hourly observations.
- Converts timestamps, temperature and humidity into appropriate data types.
- Validates critical fields and acceptable humidity ranges.
- Checks for duplicate business keys.
- Uses Delta Lake `MERGE` to update existing records and insert new records.

The business key for hourly forecasts is the combination of `city` and `event_time`.

The MERGE operation supports repeatable ingestion: matching forecast hours are updated, while previously unseen hours are inserted.

This avoids blindly appending duplicate records when a forecast period is processed again.

### Gold — Analytical Layer

A separate Spark notebook reads the validated Silver Delta table and produces daily weather summaries.

The Gold layer calculates:

- Average daily temperature.
- Minimum and maximum daily temperature.
- Average daily relative humidity.
- Total hourly readings for each city and forecast date.

The output is stored in the Delta table `dbo.weather_daily_gold`.

Spark SQL `MERGE` is used to update existing daily aggregates and insert new city/date combinations.

The current implementation recalculates daily aggregates from the Silver table before merging them into Gold.

## Pipeline Orchestration

### Initial Data Factory Implementation

The first orchestration design used three sequential Notebook activities:

**Bronze → Silver → Gold**

Each activity was configured with a success dependency, ensuring that downstream processing would only begin after the preceding activity completed successfully.

During testing, the Bronze activity succeeded, but Silver repeatedly failed with:

**HTTP 430 — TooManyRequestsForCapacity**

Fabric was unable to allocate the Spark session required to execute the Silver notebook. The failure occurred before the Silver transformation code started.

Troubleshooting included reviewing Spark pool configurations, high-concurrency settings, session tags and notebook compatibility.

### Shared-Session Notebook Orchestration

The orchestration design was revised to use `notebookutils.notebook.runMultiple()`.

A parent notebook, `nb_windrose_orchestrator`, defines the execution dependencies:

- Bronze executes first.
- Silver depends on Bronze.
- Gold depends on Silver.

The implementation uses a notebook DAG with `concurrency` set to `1`, ensuring sequential execution.

Child notebooks share the parent's Spark session, reducing the need for independently allocated Spark sessions during the workflow.

The `useRootDefaultLakehouse` execution argument ensures that child notebooks use the orchestrator's default Lakehouse.

This design successfully executed all three notebooks.

### Data Factory Integration

Following successful notebook-level testing, the Data Factory pipeline was updated to contain a single Notebook activity referencing `nb_windrose_orchestrator`.

The current execution architecture is:

**Data Factory → `nb_windrose_orchestrator` → Bronze → Silver → Gold**

The revised pipeline was successfully executed through Fabric Data Factory on 23 November 2026.

The successful pipeline run completed in approximately 5 minutes 50 seconds, including notebook and Spark execution overhead.

This exercise demonstrated an important distinction between orchestration dependencies, Spark session allocation and available compute capacity.

## Parameterisation

The pipeline accepts a String parameter named `run_date`.

Data Factory passes this parameter to the orchestrator notebook, which passes the effective date to the Bronze and Silver notebooks.

- **Bronze:** Uses `run_date` to determine where the raw JSON response is stored.
- **Silver:** Uses the same parameter to locate and process the corresponding Bronze file.
- **Gold:** Reads the Silver table and recalculates daily aggregates.

If `run_date` is empty, the orchestrator uses the current UTC date.

The parameter represents the ingestion batch date, not the actual forecast date. The API request retrieves the currently available forecast, rather than historical forecasts corresponding to the supplied `run_date`.

## Technologies

| Technology | Purpose |
|---|---|
| Microsoft Fabric | Data engineering and analytics platform |
| Fabric Data Factory | Pipeline execution and orchestration |
| OneLake | Storage for raw and processed data |
| Fabric Lakehouse | Lakehouse data organisation and Delta tables |
| Python | REST API ingestion and processing logic |
| PySpark | Distributed transformation and validation |
| Spark SQL | Aggregation and Delta MERGE operations |
| Delta Lake | Transactional storage and incremental upserts |
| Dataflow Gen2 / Power Query | Separate low-code ingestion exercise |
| GitHub | Version control and project documentation |

## Additional Implementation — Dataflow Gen2

A separate Dataflow Gen2 exercise demonstrates API ingestion and transformation using Power Query.

The resulting weather data is loaded into the Lakehouse Delta table `dbo.weather_hourly`.

This implementation is separate from the notebook-based Bronze, Silver and Gold pipeline.

It provides a practical comparison between low-code data preparation with Dataflow Gen2 and programmatic transformations using Python and PySpark.

## Data Quality and Testing

### Silver MERGE Testing

The Silver implementation has been tested for:

- Duplicate prevention using the `(city, event_time)` business key.
- Repeat processing of the same source batch.
- Updates to existing records.
- Insertion of new hourly records.
- Validation of required fields and humidity values.

Initial test results:

- Initial ingestion: 48 hourly records.
- Repeat ingestion: 48 records, with no duplicate business keys.
- An existing temperature was updated from 9.0°C to 10.0°C.
- A new hourly record was inserted, increasing the test-table count from 48 to 49.

The update and insertion tests were performed against a separate Delta table, `dbo.weather_forecast_silver_merge_test`, to protect the main Silver dataset.

### End-to-End Pipeline Validation

Following successful orchestration testing, the Silver and Gold tables were validated.

**Observed results — 23 November 2026:**

| Validation metric | Result |
|---|---:|
| Silver hourly records | 96 |
| Duplicate Silver business keys | 0 |
| Gold daily aggregate records | 4 |
| Total hourly readings represented in Gold | 96 |

The Silver dataset contained the following forecast dates:

| Forecast date | Hourly records |
|---|---:|
| 2026-11-07 | 24 |
| 2026-11-08 | 24 |
| 2026-11-23 | 24 |
| 2026-11-24 | 24 |
| **Total** | **96** |

Validation assertions confirmed that:

- No duplicate `(city, event_time)` business keys existed in Silver.
- The total hourly readings represented in Gold matched the number of Silver records.

These checks demonstrate successful data reconciliation between the Silver and Gold layers.

The figures represent a successful test run and will change as additional forecast periods are ingested.

## Spark Performance and Capacity Observations

Performance testing highlighted the difference between Spark session startup time and actual command execution.

An initial analytical query required approximately:

- 2 minutes 17 seconds for Spark session preparation.
- 23 seconds for command execution.

Repeating the same query using the active Spark session completed in approximately 11 seconds.

These observations demonstrate that Spark session startup and execution overhead can be significant relative to the processing requirements of a small dataset.

The project currently processes a limited number of hourly weather records. Spark is used to explore distributed processing and Fabric engineering capabilities rather than because the present data volume requires distributed compute.

## Repository Structure

The repository is organised into the following directories:

**`notebooks/`**

- `nb_windrose_bronze_ingest.ipynb` — Raw API ingestion.
- `nb_windrose_silver_transform.ipynb` — Data transformation, validation and incremental MERGE.
- `windrose_gold_transform.ipynb` — Daily aggregation and Gold MERGE.
- `nb_windrose_orchestrator.ipynb` — Notebook DAG orchestration using `runMultiple()`.

**`pipelines/`**

- `pl_windrose_weather_etl.json` — Fabric Data Factory pipeline definition.

**`README.md`**

- Project architecture, implementation details, testing and development progress.

## Engineering Considerations

**Idempotency**

Delta MERGE supports repeatable processing by updating matching business keys instead of inserting duplicate hourly records.

**Incremental Processing**

Forecast records are inserted or updated based on their city and event time. Existing records outside the incoming forecast window are preserved.

**Data Quality**

Validation checks are applied before loading Silver, with additional reconciliation between Silver and Gold.

**Separation of Responsibilities**

Individual notebooks handle ingestion, transformation and aggregation, while the orchestrator manages execution dependencies.

**Compute Management**

Shared-session notebook execution reduces Spark session allocation overhead for this workload on a limited Fabric trial capacity.

**Observability**

Fabric notebook execution snapshots, Spark job information and Data Factory monitoring are used to investigate failures and verify execution.

## Development Roadmap

### Completed

- [x] Raw API ingestion into OneLake.
- [x] PySpark Silver transformations.
- [x] Data quality validation.
- [x] Delta Lake MERGE implementation.
- [x] Isolated update and insertion testing.
- [x] Gold-layer modelling and daily aggregation.
- [x] Gold Delta table creation and validation.
- [x] Parameterised Bronze and Silver notebooks.
- [x] Notebook DAG orchestration using `runMultiple()`.
- [x] Data Factory integration.
- [x] Successful end-to-end pipeline execution.
- [x] Silver-to-Gold reconciliation.
- [x] Initial Spark performance and capacity investigation.

### Planned

- [ ] Pipeline scheduling and additional operational monitoring.
- [ ] Lakehouse SQL analytics endpoint exploration.
- [ ] Fabric Warehouse integration and T-SQL exercises.
- [ ] Larger-scale data processing and Spark performance analysis.
- [ ] Additional failure-handling and recovery scenarios.
- [ ] Git integration, deployment pipelines and CI/CD exercises.

## Project Context

This implementation forms part of my practical preparation for the Microsoft Fabric Data Engineer certification (DP-700), applying established data engineering principles to the Fabric ecosystem.

The project builds on prior experience with Python, SQL, cloud data platforms, ETL orchestration and analytical data modelling.

It is being developed incrementally, with each stage demonstrating practical engineering concepts, implementation choices, troubleshooting and validation.

Windrose is a learning and portfolio project, not a production-deployed weather data service.
