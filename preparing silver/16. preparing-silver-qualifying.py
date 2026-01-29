# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_silver;
# MAGIC use f1_silver;

# COMMAND ----------

from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# PATHS
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/qualifying"
silver_path = "/Volumes/workspace/default/etl_volume/silver/qualifying"
silver_table = "f1_silver.qualifying"

# READ BRONZE
qualifying_bronze_df = spark.read.format("delta").load(bronze_path)

# DEDUPLICATE (keep latest ingestion_date per qualifying entry)
window_spec = (
    Window.partitionBy("qualify_id")
          .orderBy(col("ingestion_date").desc())
)

qualifying_dedup_df = (
    qualifying_bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# DATA QUALITY CHECKS
dq_failures = qualifying_dedup_df.filter(col("qualify_id").isNull())

if dq_failures.count() > 0:
    raise Exception("DQ failure: Null qualify_id detected in qualifying silver load")

# ADD PROCESSING TIMESTAMP
silver_df = qualifying_dedup_df.withColumn("processed_at", current_timestamp())

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

    silver_df.createOrReplaceTempView("temp_qualifying_silver")

    spark.sql("""
    MERGE INTO qualifying tgt
    USING temp_qualifying_silver src
    ON tgt.qualify_id = src.qualify_id

    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Silver qualifying table updated successfully")
