# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_silver;
# MAGIC use f1_silver;

# COMMAND ----------

from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window


# PATHS

bronze_path = "/Volumes/workspace/default/etl_volume/bronze/circuits"
silver_path = "/Volumes/workspace/default/etl_volume/silver/circuits"

# READ BRONZE

bronze_df = spark.read.format("delta").load(bronze_path)

# DEDUPLICATE

window_spec = (
    Window.partitionBy("circuit_id")
          .orderBy(col("file_date").desc())
)

dedup_df = (
    bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# DATA QUALITY CHECKS

dq_failures = dedup_df.filter(col("circuit_id").isNull())

if dq_failures.count() > 0:
    raise Exception("DQ failure: Null circuit_id detected in circuits silver load")

# ADD PROCESSING TIMESTAMP

silver_df = dedup_df.withColumn("processed_at", current_timestamp())

# WRITE TO SILVER

(
    silver_df
    .write
    .format("delta")
    .mode("append")
    .option("overwriteSchema", "true")
    .save(silver_path)
)

# CREATE TABLE FIRST TIME OR MERGE

if not spark.catalog.tableExists("f1_silver.circuits"):

    silver_df.write \
        .format("delta") \
        .saveAsTable("f1_silver.circuits")

else:

    silver_df.createOrReplaceTempView("temp_circuits_silver")

    spark.sql("""
    MERGE INTO circuits tgt
    USING temp_circuits_silver src
    ON tgt.circuit_id = src.circuit_id

    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

print("Silver circuits table updated successfully")
