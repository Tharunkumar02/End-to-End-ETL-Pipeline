# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_gold;
# MAGIC use f1_gold;

# COMMAND ----------

from pyspark.sql.functions import sum, desc, count, col, when, rank, current_timestamp, asc
from pyspark.sql.window import Window

# PATHS
raw_path = "/Volumes/workspace/default/etl_volume/gold/race_results"
gold_path = "/Volumes/workspace/default/etl_volume/gold/driver_standings"
gold_table = "f1_gold.driver_standings"

# READ GOLD RACE RESULTS
race_results_df = spark.read.format("delta").load(f"{raw_path}")

# CALCULATE DRIVER STANDINGS
driver_standing_df = (
    race_results_df
    .groupBy("race_year", "driver_nationality", "driver_name")
    .agg(
        sum("points").alias("total_points"),
        count(when(col("race_position") == 1, True)).alias("number_of_wins")
    )
)

# CALCULATE DRIVER RANKINGS
driver_window = Window.partitionBy("race_year").orderBy(desc("total_points"))
driver_standing_df = driver_standing_df.withColumn("rank", rank().over(driver_window))

# ADD PROCESSING TIMESTAMP
driver_standing_df = driver_standing_df.withColumn("gold_processed_at", current_timestamp())

# WRITE TO GOLD LAYER PATH
(
    driver_standing_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_path)
)

# WRITE TO GOLD LAYER
if not spark.catalog.tableExists(gold_table):
    driver_standing_df.write.mode("overwrite").format("delta").saveAsTable(gold_table)
else:
    driver_standing_df.createOrReplaceTempView("temp_driver_standings_gold")
    spark.sql(f"""
        MERGE INTO {gold_table} tgt
        USING temp_driver_standings_gold src
        ON tgt.race_year = src.race_year
           AND tgt.driver_name = src.driver_name
        WHEN MATCHED THEN UPDATE SET
            tgt.driver_nationality = src.driver_nationality,
            tgt.total_points = src.total_points,
            tgt.number_of_wins = src.number_of_wins,
            tgt.rank = src.rank,
            tgt.gold_processed_at = src.gold_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Gold driver_standings table updated successfully")
