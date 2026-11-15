# Windrose Fabric Notebooks

This directory contains the PySpark notebooks used
to implement the Bronze, Silver and Gold layers
of the Windrose weather data pipeline.

## Current notebooks

- Bronze: API ingestion and raw JSON storage in OneLake.
- Silver: PySpark transformations, data quality
  validation and Delta Lake MERGE operations.

## Planned

- Gold: Daily weather aggregations for analytical reporting.
