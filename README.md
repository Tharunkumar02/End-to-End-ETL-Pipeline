<img width="1644" height="781" alt="image" src="https://github.com/user-attachments/assets/1a801508-77a7-4b23-899d-dbd7f5176e9a" />


# 🏎️ Formula 1 ETL Pipeline (Bronze → Silver → Gold)

An end-to-end **data engineering project** that builds a scalable ETL pipeline for Formula 1 data using a modern **lakehouse architecture**. The pipeline transforms raw race data into analytics-ready datasets and powers downstream reporting (e.g., Power BI dashboards).

---

## 🚀 Overview

This project ingests historical Formula 1 datasets and processes them through a **multi-layered architecture**:

* **Bronze** → Raw ingestion
* **Silver** → Cleaned and standardized data
* **Gold** → Business-ready, aggregated insights

The goal is to demonstrate **data pipeline design, transformation logic, and analytics modeling** at scale.

---

## 🏗️ Architecture

### 🥉 Bronze Layer — Raw Data Ingestion

* Ingests raw F1 datasets from source files
* Preserves original schema (schema-on-read)
* Performs minimal validation and metadata tracking
* Modular ingestion scripts for each dataset

**Datasets include:**

* Circuits
* Races
* Drivers
* Constructors
* Results
* Pit Stops
* Lap Times
* Qualifying

---

### 🥈 Silver Layer — Data Cleaning & Standardization

* Cleans and validates raw data (null handling, deduplication)
* Standardizes schemas and enforces data types
* Applies transformation logic for consistency
* Creates structured tables for each entity

➡️ Designed for **reusability, debugging, and incremental processing**

---

### 🥇 Gold Layer — Analytics & Business Logic

* Builds curated, aggregated datasets
* Applies domain-specific logic (standings, scoring, rankings)
* Optimized for BI tools and reporting

**Key outputs:**

* Driver Standings
* Constructor Standings
* Race Results

---

## 📊 Use Case

The Gold layer powers interactive dashboards (e.g., Power BI) to analyze:

* Driver and team performance trends
* Historical race statistics
* Global distribution of circuits

---

## ⚙️ Key Features

* Layered **Medallion Architecture (Bronze/Silver/Gold)**
* Modular and scalable pipeline design
* Data quality checks and schema enforcement
* Analytics-ready data modeling
* Integration-ready for BI tools

---

## 🧠 What This Demonstrates

* End-to-end **ETL pipeline development**
* Data modeling and transformation best practices
* Scalable data architecture design
* Turning raw data into actionable insights

---

Here's the downstream Power BI dashboard showing the actual performance of drivers and constructors for each race.
<img width="1404" height="746" alt="image" src="https://github.com/user-attachments/assets/110b0f52-6349-48d5-a465-7fb1e60e76cc" />
