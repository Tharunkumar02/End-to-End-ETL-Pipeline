# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_silver;
# MAGIC use f1_silver;

# COMMAND ----------

from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# PATHS

bronze_path = "/Volumes/workspace/default/etl_volume/bronze/races"
silver_path = "/Volumes/workspace/default/etl_volume/silver/races"

silver_table = "f1_silver.races"

# READ BRONZE

bronze_df = spark.read.format("delta").load(bronze_path)

# DEDUPLICATE

window_spec = (
    Window.partitionBy("race_id")
          .orderBy(col("file_date").desc())
)

dedup_df = (
    bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# DATA QUALITY CHECKS

dq_failures = dedup_df.filter(
    col("race_id").isNull() |
    col("race_year").isNull() |
    col("circuit_id").isNull()
)

if dq_failures.count() > 0:
    raise Exception("DQ failure: Null keys detected in races silver load")

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

# CREATE SILVER TABLE IF NOT EXISTS

if not spark.catalog.tableExists("f1_silver.races"):

    silver_df.write \
        .format("delta") \
        .partitionBy("race_year") \
        .saveAsTable("f1_silver.races")

else:

    silver_df.createOrReplaceTempView("temp_races_silver")

    spark.sql("""
    MERGE INTO races tgt
    USING temp_races_silver src
    ON tgt.race_id = src.race_id

    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# OPTIONAL OPTIMIZE

spark.sql("OPTIMIZE races ZORDER BY (race_id)")

print("Silver races table updated successfully")