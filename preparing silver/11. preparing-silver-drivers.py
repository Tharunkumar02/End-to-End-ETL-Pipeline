# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_silver;
# MAGIC use f1_silver;

# COMMAND ----------

from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# PATHS
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/drivers"
silver_path = "/Volumes/workspace/default/etl_volume/silver/drivers"
silver_table = "f1_silver.drivers"

# READ BRONZE
silver_df = spark.read.format("delta").load(bronze_path)

# DEDUPLICATE (keep latest file_date per driver)
window_spec = (
    Window.partitionBy("driver_id")
          .orderBy(col("file_date").desc())
)

drivers_dedup_df = (
    silver_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# DATA QUALITY CHECKS
dq_failures = drivers_dedup_df.filter(col("driver_id").isNull())

if dq_failures.count() > 0:
    raise Exception("DQ failure: Null driver_id detected in drivers silver load")

# ADD PROCESSING TIMESTAMP
silver_df = drivers_dedup_df.withColumn("processed_at", current_timestamp())

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

    silver_df.createOrReplaceTempView("temp_drivers_silver")

    spark.sql("""
    MERGE INTO drivers tgt
    USING temp_drivers_silver src
    ON tgt.driver_id = src.driver_id

    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Silver drivers table updated successfully")