# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_gold;
# MAGIC use f1_gold;

# COMMAND ----------

from pyspark.sql.functions import sum, desc, count, col, when, rank, current_timestamp, asc
from pyspark.sql.window import Window

# PATHS
raw_path = "/Volumes/workspace/default/etl_volume/gold/race_results"
gold_path = "/Volumes/workspace/default/etl_volume/gold/constructor_standings"
gold_table = "f1_gold.constructor_standings"

# READ GOLD RACE RESULTS
race_results_df = spark.read.format("delta").load(f"{raw_path}")

# CALCULATE CONSTRUCTOR STANDINGS
constructor_standing_df = (
    race_results_df
    .groupBy("race_year", "team")
    .agg(
        sum("points").alias("total_points"),
        count(when(col("race_position") == 1, True)).alias("number_of_wins")
    )
)

# CALCULATE CONSTRUCTOR RANKINGS
constructor_window = Window.partitionBy("race_year").orderBy(desc("total_points"))
constructor_standing_df = constructor_standing_df.withColumn("rank", rank().over(constructor_window))

# ADD PROCESSING TIMESTAMP
constructor_standing_df = constructor_standing_df.withColumn("gold_processed_at", current_timestamp())

# WRITE TO GOLD LAYER PATH
(
    constructor_standing_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_path)
)

# WRITE TO GOLD LAYER
if not spark.catalog.tableExists(gold_table):
    constructor_standing_df.write.mode("overwrite").format("delta").saveAsTable(gold_table)
else:
    constructor_standing_df.createOrReplaceTempView("temp_constructor_standings_gold")
    spark.sql(f"""
        MERGE INTO {gold_table} tgt
        USING temp_constructor_standings_gold src
        ON tgt.race_year = src.race_year
           AND tgt.team = src.team
        WHEN MATCHED THEN UPDATE SET
            tgt.total_points = src.total_points,
            tgt.number_of_wins = src.number_of_wins,
            tgt.rank = src.rank,
            tgt.gold_processed_at = src.gold_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Gold constructor_standings table updated successfully")
