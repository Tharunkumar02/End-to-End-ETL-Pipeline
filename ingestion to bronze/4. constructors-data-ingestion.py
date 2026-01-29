# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, lit

# Schema
constructors_df_schema = StructType([
    StructField("constructorId", IntegerType(), True),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

raw_base_path = "/Volumes/workspace/default/etl_volume/raw"
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/constructors"

# List folders
folders = dbutils.fs.ls(raw_base_path)
date_folders = sorted([f.name.rstrip("/") for f in folders if f.isDir()])

for v_file_date in date_folders:
    print(f"Processing folder: {v_file_date}")

    raw_path = f"{raw_base_path}/{v_file_date}"

    # Read
    constructors_df = spark.read.json(
        f"{raw_path}/constructors.json",
        schema=constructors_df_schema
    )

    # Transform
    constructors_final_df = (
        constructors_df
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("file_date", lit(v_file_date))
        .select(
            col("constructorId").alias("constructor_id"),
            col("constructorRef").alias("constructor_ref"),
            col("name"),
            col("nationality"),
            col("ingestion_date"),
            col("file_date")
        )
    )

    # Write (append)
    constructors_final_df.write \
        .mode("append") \
        .format("delta") \
        .save(bronze_path)

print("✅ All folders processed successfully")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create database if not exists f1_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC use database f1_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_bronze.constructors
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/etl_volume/bronze/constructors`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc f1_bronze.constructors;
# MAGIC select * from f1_bronze.constructors