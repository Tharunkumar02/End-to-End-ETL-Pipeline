# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, lit

# Schema definition
pitstops_df_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("duration", StringType(), False),
    StructField("milliseconds", IntegerType(), False)
])

raw_base_path = "/Volumes/workspace/default/etl_volume/raw"
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/pitstops"

# List folders
folders = dbutils.fs.ls(raw_base_path)
date_folders = sorted([f.name.rstrip("/") for f in folders if f.isDir()])

for v_file_date in date_folders:

    raw_path = f"{raw_base_path}/{v_file_date}/pit_stops.json"

    # Read raw data
    pitstops_df = spark.read.schema(pitstops_df_schema).json(raw_path)

    # Transform data
    pitstops_final_df = pitstops_df.withColumn(
        "ingestion_date", current_timestamp()
    )

    pitstops_final_new_df = pitstops_final_df.select(
        col("raceId").alias("race_id"),
        col("driverId").alias("driver_id"),
        col("stop"),
        col("lap"),
        col("time"),
        col("duration"),
        col("milliseconds"),
        col("ingestion_date")
    )

    # Write to the correct bronze path (append mode)
    pitstops_final_new_df.write \
        .mode("append") \
        .format("delta") \
        .save(bronze_path)

    print(f"✅ {v_file_date} processed successfully.")

print("✅ All folders processed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_bronze;
# MAGIC
# MAGIC create table if not exists f1_bronze.pitstops
# MAGIC using delta
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/etl_volume/bronze/pitstops`;
# MAGIC
# MAGIC select * from f1_bronze.pitstops;