<img width="1644" height="781" alt="image" src="https://github.com/user-attachments/assets/1a801508-77a7-4b23-899d-dbd7f5176e9a" />


**Formula 1 ETL Pipeline (Bronze → Silver → Gold)**

This project implements an end-to-end ETL pipeline for Formula 1 data, following a modern data lakehouse architecture with Bronze, Silver, and Gold layers. The pipeline ingests raw F1 data, cleans and transforms it into analytics-ready datasets, and produces curated tables for reporting and insights.

Architecture Overview

The pipeline is structured into three logical layers:

Bronze Layer —> Raw Ingestion

Ingests raw Formula 1 datasets from source files

Preserves original schema and structure

Minimal transformation (schema enforcement, metadata handling)

Ingested entities include:

**Circuits, 
Races, 
Drivers, 
Constructors, 
Results, 
Pit stops, 
Lap times, 
Qualifying data.**

Each dataset has its own ingestion script for modularity and scalability.

Silver Layer —> Data Preparation and Cleansing

Cleans and standardizes Bronze data

Applies data quality rules and transformations

Normalizes schemas and enforces data types

Prepares datasets for analytical use

Silver tables are created independently per entity (races, drivers, constructors, etc.), enabling easy debugging and reprocessing.

Gold Layer —> Business Logic and Analytics

Produces aggregated and curated datasets

Implements business logic for analytics and reporting

Creates final tables such as:

**Race results, 
Driver standings, 
Constructor standings.**

Gold tables are optimized for downstream consumption by dashboards or BI tools.
