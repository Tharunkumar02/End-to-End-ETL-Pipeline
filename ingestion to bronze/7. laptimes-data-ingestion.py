# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, lit

# Schema definition
laptimes_df_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

raw_base_path = "/Volumes/workspace/default/etl_volume/raw"
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/laptimes"

# List folders
folders = dbutils.fs.ls(raw_base_path)
date_folders = sorted([f.name.rstrip("/") for f in folders if f.isDir()])

for v_file_date in date_folders:

    raw_path = f"{raw_base_path}/{v_file_date}/lap_times/lap_times_split*.csv"

    # Read raw data
    laptimes_df = spark.read.schema(laptimes_df_schema).csv(raw_path)

    # Transform data
    laptimes_final_df = laptimes_df.withColumn(
        "ingestion_date", current_timestamp()
    )

    laptimes_final_new_df = laptimes_final_df.select(
        col("raceId").alias("race_id"),
        col("driverId").alias("driver_id"),
        col("lap"),
        col("position"),
        col("time"),
        col("milliseconds"),
        col("ingestion_date")
    )

    # Write to the same bronze path (append mode)
    laptimes_final_new_df.write \
        .mode("append") \
        .format("delta") \
        .save(bronze_path)

    print(f"✅ {v_file_date} processed successfully.")

print("✅ All folders processed successfully.")


# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_bronze;
# MAGIC
# MAGIC create table if not exists f1_bronze.laptimes
# MAGIC using delta
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/etl_volume/bronze/laptimes`;
# MAGIC
# MAGIC select * from f1_bronze.laptimes;