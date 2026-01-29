# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, current_timestamp, concat, lit, try_to_timestamp
)

# Schema
races_df_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True)
])

raw_base_path = "/Volumes/workspace/default/etl_volume/raw"
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/races"

# List folders
folders = dbutils.fs.ls(raw_base_path)
date_folders = sorted([f.name.rstrip("/") for f in folders if f.isDir()])

for v_file_date in date_folders:
    print(f"Processing folder: {v_file_date}")

    raw_path = f"{raw_base_path}/{v_file_date}"

    # Read
    races_df = spark.read.csv(
        f"{raw_path}/races.csv",
        header=True,
        schema=races_df_schema
    )

    # Transform
    races_final_df = (
        races_df
        .withColumn("ingestion_date", current_timestamp())
        .withColumn(
            "race_timestamp",
            try_to_timestamp(
                concat(col("date"), lit(" "), col("time")),
                lit("yyyy-MM-dd HH:mm:ss")
            )
        )
        .withColumn("file_date", lit(v_file_date))
        .select(
            col("raceId").alias("race_id"),
            col("year").alias("race_year"),
            col("round"),
            col("circuitId").alias("circuit_id"),
            col("name"),
            col("ingestion_date"),
            col("race_timestamp"),
            col("file_date")
        )
    )

    # Write (append)
    races_final_df.write \
        .mode("append") \
        .format("delta") \
        .save(bronze_path)

print("✅ All folders processed successfully")


# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_bronze;
# MAGIC
# MAGIC create table if not exists f1_bronze.races
# MAGIC using delta
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/etl_volume/bronze/races`;
# MAGIC
# MAGIC select * from f1_bronze.races;