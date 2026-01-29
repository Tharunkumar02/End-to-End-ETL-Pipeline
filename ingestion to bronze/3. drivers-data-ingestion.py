# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, current_timestamp, lit, concat
)

# Nested schema
name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# Schema
drivers_df_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("name", name_schema),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

raw_base_path = "/Volumes/workspace/default/etl_volume/raw"
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/drivers"

# List folders
folders = dbutils.fs.ls(raw_base_path)
date_folders = sorted([f.name.rstrip("/") for f in folders if f.isDir()])

for v_file_date in date_folders:
    print(f"Processing folder: {v_file_date}")

    raw_path = f"{raw_base_path}/{v_file_date}"

    # Read
    drivers_df = spark.read.json(
        f"{raw_path}/drivers.json",
        schema=drivers_df_schema
    )

    # Transform
    drivers_final_df = (
        drivers_df
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("file_date", lit(v_file_date))
        .withColumn(
            "name",
            concat(col("name.forename"), lit(" "), col("name.surname"))
        )
        .select(
            col("driverId").alias("driver_id"),
            col("driverRef").alias("driver_ref"),
            col("number"),
            col("code"),
            col("name"),
            col("dob"),
            col("nationality"),
            col("ingestion_date"),
            col("file_date")
        )
    )

    # Write (append)
    drivers_final_df.write \
        .mode("append") \
        .format("delta") \
        .save(bronze_path)

print("✅ All folders processed successfully")


# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_bronze;
# MAGIC
# MAGIC create table if not exists f1_bronze.drivers
# MAGIC using delta
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/etl_volume/bronze/drivers`;
# MAGIC
# MAGIC select * from f1_bronze.drivers;