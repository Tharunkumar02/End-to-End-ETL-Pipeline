# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_silver;
# MAGIC use f1_silver;

# COMMAND ----------

from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# PATHS
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/results"
silver_path = "/Volumes/workspace/default/etl_volume/silver/results"
silver_table = "f1_silver.results"

# READ BRONZE
results_bronze_df = spark.read.format("delta").load(bronze_path)

# DEDUPLICATE (keep latest file_date per result)
window_spec = (
    Window.partitionBy("result_id")
          .orderBy(col("file_date").desc())
)

results_dedup_df = (
    results_bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# DATA QUALITY CHECKS
dq_failures = results_dedup_df.filter(col("result_id").isNull())

if dq_failures.count() > 0:
    raise Exception("DQ failure: Null result_id detected in results silver load")

# ADD PROCESSING TIMESTAMP
silver_df = results_dedup_df.withColumn("processed_at", current_timestamp())

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

    silver_df.createOrReplaceTempView("temp_results_silver")

    spark.sql("""
    MERGE INTO results tgt
    USING temp_results_silver src
    ON tgt.result_id = src.result_id

    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Silver results table updated successfully")
