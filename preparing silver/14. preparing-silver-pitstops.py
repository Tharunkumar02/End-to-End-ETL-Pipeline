# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_silver;
# MAGIC use f1_silver;

# COMMAND ----------

from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# PATHS
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/pitstops"
silver_path = "/Volumes/workspace/default/etl_volume/silver/pitstops"
silver_table = "f1_silver.pitstops"

# READ BRONZE
pitstops_bronze_df = spark.read.format("delta").load(bronze_path)

# DEDUPLICATE (keep latest ingestion_date per pitstop)
window_spec = (
    Window.partitionBy("race_id", "driver_id", "stop")
          .orderBy(col("ingestion_date").desc())
)

pitstops_dedup_df = (
    pitstops_bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# DATA QUALITY CHECKS
dq_failures = pitstops_dedup_df.filter(
    col("race_id").isNull() | col("driver_id").isNull() | col("stop").isNull()
)

if dq_failures.count() > 0:
    raise Exception("DQ failure: Null race_id, driver_id, or stop detected in pitstops silver load")

# ADD PROCESSING TIMESTAMP
silver_df = pitstops_dedup_df.withColumn("processed_at", current_timestamp())

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

    silver_df.createOrReplaceTempView("temp_pitstops_silver")

    spark.sql("""
    MERGE INTO pitstops tgt
    USING temp_pitstops_silver src
    ON tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop

    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Silver pitstops table updated successfully")
