# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_silver;
# MAGIC use f1_silver;

# COMMAND ----------

from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# PATHS
bronze_path = "/Volumes/workspace/default/etl_volume/bronze/constructors"
silver_path = "/Volumes/workspace/default/etl_volume/silver/constructors"
silver_table = "f1_silver.constructors"

# READ BRONZE
constructors_bronze_df = spark.read.format("delta").load(bronze_path)

# DEDUPLICATE (keep latest file_date per constructor)
window_spec = (
    Window.partitionBy("constructor_id")
          .orderBy(col("file_date").desc())
)

constructors_dedup_df = (
    constructors_bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# DATA QUALITY CHECKS
dq_failures = constructors_dedup_df.filter(col("constructor_id").isNull())

if dq_failures.count() > 0:
    raise Exception("DQ failure: Null constructor_id detected in constructors silver load")

# ADD PROCESSING TIMESTAMP
silver_df = constructors_dedup_df.withColumn("processed_at", current_timestamp())

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

    silver_df.createOrReplaceTempView("temp_constructors_silver")

    spark.sql("""
    MERGE INTO constructors tgt
    USING temp_constructors_silver src
    ON tgt.constructor_id = src.constructor_id

    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Silver constructors table updated successfully")
