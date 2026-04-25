<img width="1644" height="781" alt="image" src="https://github.com/user-attachments/assets/1a801508-77a7-4b23-899d-dbd7f5176e9a" />


**Formula 1 ETL Pipeline (Bronze → Silver → Gold)**

This project develops a comprehensive ETL pipeline for Formula 1 data, utilizing a modern data lakehouse architecture with Bronze, Silver, and Gold tiers. The pipeline collects raw F1 data, cleans and transforms it into datasets suitable for analysis, and generates curated tables for reporting and insights.

Architecture Summary

The pipeline consists of three main layers:

Bronze Layer —> Raw Data Collection

Collects raw Formula 1 datasets from source files

Maintains original schema and structure

Performs minimal transformations (schema validation, metadata management)

Includes datasets such as:

**Circuits,
Races,
Drivers,
Constructors,
Results,
Pit stops,
Lap times,
Qualifying data.**

Each dataset has a dedicated ingestion script for scalability and modularity.

Silver Layer —> Data Cleaning and Standardization

Cleans and standardizes Bronze data

Applies data quality checks and transformations

Normalizes schemas and enforces data types

Prepares datasets for analytical purposes

Silver tables are created separately for each entity (races, drivers, constructors, etc.), facilitating debugging and reprocessing.

Gold Layer —> Business Logic and Insights

Produces aggregated, curated datasets

Implements business rules for analytics and reporting

Creates final tables such as:

**Race results,
Driver standings,
Constructor standings.**

Gold tables are optimized for downstream consumption by dashboards or BI tools.
