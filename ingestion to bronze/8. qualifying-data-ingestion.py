# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, lit

# Schema definition
qualifying_df_schema = StructType([
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

raw_base_path = "/Volumes/workspace/default/etl_volume/raw"
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/qualifying" 

# List folders
folders = dbutils.fs.ls(raw_base_path)
date_folders = sorted([f.name.rstrip("/") for f in folders if f.isDir()])

for v_file_date in date_folders:
    
    raw_path = f"{raw_base_path}/{v_file_date}/qualifying/qualifying_split*.json"

    # Read raw data
    qualifying_df = spark.read.schema(qualifying_df_schema).option("multiline", "true").json(raw_path)

    # Transform data
    qualifying_final_df = qualifying_df.withColumn(
        "ingestion_date", current_timestamp()
    )

    qualifying_final_new_df = qualifying_final_df.select(
        col("qualifyId").alias("qualify_id"),
        col("raceId").alias("race_id"),
        col("driverId").alias("driver_id"),
        col("constructorId").alias("constructor_id"),
        col("number"),
        col("position"),
        col("q1"),
        col("q2"),
        col("q3"),
        col("ingestion_date")
    )

    # Write to the same bronze path (append mode)
    qualifying_final_new_df.write \
        .mode("append") \
        .format("delta") \
        .save(bronze_path)

    print(f"✅ {v_file_date} processed successfully.")

print("✅ All folders processed successfully.")


# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_bronze;
# MAGIC
# MAGIC create table if not exists f1_bronze.qualifying
# MAGIC using delta
# MAGIC AS SELECT * FROM delta.`/Volumes/workspace/default/etl_volume/bronze/qualifying`;
# MAGIC
# MAGIC select * from f1_bronze.qualifying;