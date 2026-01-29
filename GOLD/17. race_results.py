# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_gold;
# MAGIC use f1_gold;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql import Window
from pyspark.sql.functions import row_number

# PATHS
raw_path = "/Volumes/workspace/default/etl_volume/silver"
gold_path = "/Volumes/workspace/default/etl_volume/gold/race_results"
gold_table = "f1_gold.race_results"

# READ SILVER TABLES
race_df = (
    spark.read.format("delta").load(f"{raw_path}/races")
    .withColumnRenamed("name", "race_name")
    .withColumnRenamed("round", "race_round")
    .withColumnRenamed("race_timestamp", "race_date")
)

circuits_df = (
    spark.read.format("delta").load(f"{raw_path}/circuits")
    .withColumnRenamed("location", "circuit_location")
    .withColumnRenamed("name", "circuit_name")
    .withColumnRenamed("country", "circuit_country")
)

drivers_df = (
    spark.read.format("delta").load(f"{raw_path}/drivers")
    .withColumnRenamed("number", "driver_number")
    .withColumnRenamed("name", "driver_name")
    .withColumnRenamed("nationality", "driver_nationality")
)

constructors_df = (
    spark.read.format("delta").load(f"{raw_path}/constructors")
    .withColumnRenamed("name", "team")
)

results_df = (
    spark.read.format("delta").load(f"{raw_path}/results")
    .withColumnRenamed("time", "race_time")
    .withColumnRenamed("race_id", "result_race_id")
    .withColumnRenamed("driver_id", "result_driver_id")
    .withColumnRenamed("constructor_id", "result_constructor_id")
    .withColumnRenamed("position", "race_position")
)

# JOIN RACE WITH CIRCUITS
race_circuits_df = race_df.join(
    circuits_df, race_df.circuit_id == circuits_df.circuit_id, "inner"
).select(
    race_df.race_id,
    race_df.race_year,
    race_df.race_name,
    race_df.race_date,
    circuits_df.circuit_location,
    circuits_df.circuit_country
)

# JOIN WITH RESULTS, DRIVERS, CONSTRUCTORS
race_results_df = (
    results_df
    .join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id, "inner")
    .join(drivers_df, results_df.result_driver_id == drivers_df.driver_id, "inner")
    .join(constructors_df, results_df.result_constructor_id == constructors_df.constructor_id, "inner")
    .select(
        race_circuits_df.race_year,
        race_circuits_df.race_name,
        race_circuits_df.race_date,
        race_circuits_df.circuit_location,
        race_circuits_df.circuit_country,
        drivers_df.driver_name,
        drivers_df.driver_number,
        drivers_df.driver_nationality,
        constructors_df.team,
        results_df.grid,
        results_df.fastest_lap,
        results_df.race_position,
        results_df.race_time,
        results_df.points
    )
    .withColumn("gold_processed_at", current_timestamp())
)

# DEDUPLICATE SOURCE TO AVOID MERGE CONFLICTS
window_spec = Window.partitionBy("race_year", "race_name", "driver_name").orderBy("gold_processed_at")

race_results_dedup = race_results_df.withColumn(
    "row_num", row_number().over(window_spec)
).filter("row_num = 1").drop("row_num")

# WRITE TO GOLD LAYER (DELTA FILE)
(
    race_results_dedup
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)

# WRITE / MERGE INTO GOLD TABLE
if not spark.catalog.tableExists(gold_table):
    race_results_dedup.write.mode("overwrite").format("delta").saveAsTable(gold_table)
else:
    race_results_dedup.createOrReplaceTempView("temp_race_results_gold")
    spark.sql(f"""
        MERGE INTO {gold_table} tgt
        USING temp_race_results_gold src
        ON tgt.race_year = src.race_year
           AND tgt.race_name = src.race_name
           AND tgt.driver_name = src.driver_name
        WHEN MATCHED THEN UPDATE SET
            tgt.circuit_location = src.circuit_location,
            tgt.circuit_country = src.circuit_country,
            tgt.driver_number = src.driver_number,
            tgt.driver_nationality = src.driver_nationality,
            tgt.team = src.team,
            tgt.grid = src.grid,
            tgt.fastest_lap = src.fastest_lap,
            tgt.race_position = src.race_position,
            tgt.race_time = src.race_time,
            tgt.points = src.points,
            tgt.gold_processed_at = src.gold_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Gold race_results table updated successfully")
