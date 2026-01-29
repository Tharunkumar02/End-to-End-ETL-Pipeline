# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_silver;
# MAGIC use f1_silver;

# COMMAND ----------

from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# PATHS
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/laptimes"
silver_path = "/Volumes/workspace/default/etl_volume/silver/laptimes"
silver_table = "f1_silver.laptimes"

# READ BRONZE
laptimes_bronze_df = spark.read.format("delta").load(bronze_path)

# DEDUPLICATE (keep latest ingestion_date per lap)
window_spec = (
    Window.partitionBy("race_id", "driver_id", "lap")
          .orderBy(col("ingestion_date").desc())
)

laptimes_dedup_df = (
    laptimes_bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# DATA QUALITY CHECKS
dq_failures = laptimes_dedup_df.filter(
    col("race_id").isNull() | col("driver_id").isNull() | col("lap").isNull()
)

if dq_failures.count() > 0:
    raise Exception("DQ failure: Null race_id, driver_id, or lap detected in laptimes silver load")

# ADD PROCESSING TIMESTAMP
silver_df = laptimes_dedup_df.withColumn("processed_at", current_timestamp())

# WRITE TO SILVER

(
    silver_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(silver_path)
)

# CREATE TABLE FIRST TIME OR MERGE
if not spark.catalog.tableExists(silver_table):

    silver_df.write \
        .format("delta") \
        .saveAsTable(silver_table)

else:

    silver_df.createOrReplaceTempView("temp_laptimes_silver")

    spark.sql("""
    MERGE INTO laptimes tgt
    USING temp_laptimes_silver src
    ON tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap

    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Silver laptimes table updated successfully")
