# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, lit

# Schema
circuits_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

raw_base_path = "/Volumes/workspace/default/etl_volume/raw"
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/circuits"

# List folders
folders = dbutils.fs.ls(raw_base_path)
date_folders = sorted([f.name.rstrip("/") for f in folders if f.isDir()])

for v_file_date in date_folders:
    print(f"Processing folder: {v_file_date}")

    raw_path = f"{raw_base_path}/{v_file_date}"

    # Read
    circuits_df = spark.read.csv(
        f"{raw_path}/circuits.csv",
        header=True,
        schema=circuits_schema
    )

    # Transform
    circuits_final_df = circuits_df.select(
        col("circuitId").alias("circuit_id"),
        col("circuitRef").alias("circuit_ref"),
        col("name"),
        col("location"),
        col("country"),
        col("lat").alias("latitude"),
        col("lng").alias("longitude"),
        col("alt").alias("altitude")
    ).withColumn(
        "ingestion_date", current_timestamp()
    ).withColumn(
        "file_date", lit(v_file_date)
    )

    # Write (append)
    circuits_final_df.write \
        .mode("append") \
        .format("delta") \
        .save(bronze_path)

print("✅ All folders processed successfully")


# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_bronze;
# MAGIC
# MAGIC create table if not exists f1_bronze.circuits
# MAGIC using delta
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/etl_volume/bronze/circuits`;
# MAGIC
# MAGIC select * from f1_bronze.circuits;