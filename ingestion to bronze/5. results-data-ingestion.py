# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, lit

# Schema definition
results_df_schema = StructType([
    StructField("resultId", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", DoubleType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", StringType(), True)
])

raw_base_path = "/Volumes/workspace/default/etl_volume/raw"
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/results"

# List folders
folders = dbutils.fs.ls(raw_base_path)
date_folders = sorted([f.name.rstrip("/") for f in folders if f.isDir()])

for v_file_date in date_folders:
    
    raw_path = f"{raw_base_path}/{v_file_date}/results.json"

    # Read raw data
    results_df = spark.read.schema(results_df_schema).json(raw_path)

    # Transform data and add file_date
    results_final_df = results_df.withColumn(
        "ingestion_date", current_timestamp()
    ).withColumn(
        "file_date", lit(v_file_date)
    )

    results_final_new_df = results_final_df.select(
        col("resultId").alias("result_id"),
        col("raceId").alias("race_id"),
        col("driverId").alias("driver_id"),
        col("constructorId").alias("constructor_id"),
        col("number"),
        col("grid"),
        col("position"),
        col("positionText").alias("position_text"),
        col("positionOrder").alias("position_order"),
        col("points"),
        col("laps"),
        col("time"),
        col("milliseconds"),
        col("fastestLap").alias("fastest_lap"),
        col("rank"),
        col("fastestLapTime").alias("fastest_lap_time"),
        col("fastestLapSpeed").alias("fastest_lap_speed"),
        col("file_date"),
        col("ingestion_date")
    )

    results_final_new_df.write \
        .mode("append") \
        .format("delta") \
        .save(bronze_path)

    print(f"✅ {v_file_date} processed successfully.")

print("✅ All folders processed successfully.")


# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_bronze;
# MAGIC
# MAGIC create table if not exists f1_bronze.results
# MAGIC using delta
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/etl_volume/bronze/results`;
# MAGIC
# MAGIC select * from f1_bronze.results;