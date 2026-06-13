# Databricks notebook source
# MAGIC %md
# MAGIC # Week 1 Real Data Extension - NYC Taxi Pipeline
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Apply all Week 1 Databricks, PySpark, Spark SQL and lakehouse skills to a real public dataset.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC NYC Taxi & Limousine Commission Yellow Taxi Trip Records.
# MAGIC
# MAGIC ## Files used
# MAGIC
# MAGIC - Yellow Taxi Trip Records - January 2024, Parquet
# MAGIC - Taxi Zone Lookup Table, CSV
# MAGIC
# MAGIC ## Skills practised
# MAGIC
# MAGIC - Reading Parquet
# MAGIC - Reading CSV
# MAGIC - Spark DataFrames
# MAGIC - PySpark transformations
# MAGIC - Spark SQL temporary views
# MAGIC - Null checks
# MAGIC - Invalid value checks
# MAGIC - Joins
# MAGIC - Bronze/Silver/Gold tables
# MAGIC - Row count checks
# MAGIC - Saving Databricks tables

# COMMAND ----------

taxi_trips_path = "/Volumes/workspace/default/week1_raw/yellow_tripdata_2024-01.parquet"
zone_lookup_path = "/Volumes/workspace/default/week1_raw/taxi_zone_lookup.csv"

print(taxi_trips_path)
print(zone_lookup_path)

# COMMAND ----------

volume_path = "/Volumes/workspace/default/week1_raw"

display(dbutils.fs.ls(volume_path))

# COMMAND ----------

bronze_trips_df = spark.read.parquet(taxi_trips_path)

display(bronze_trips_df.limit(10))

# COMMAND ----------

zone_lookup_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(zone_lookup_path)
)

display(zone_lookup_df)

# COMMAND ----------

bronze_trips_df.printSchema()

bronze_count = bronze_trips_df.count()

print(f"Bronze trip row count: {bronze_count}")

# COMMAND ----------

bronze_trips_df.write.mode("overwrite").saveAsTable("week1_real_bronze_taxi_trips")

# COMMAND ----------

zone_lookup_df.write.mode("overwrite").saveAsTable("week1_real_bronze_taxi_zones")

# COMMAND ----------

bronze_trips_df.createOrReplaceTempView("bronze_taxi_trips_view")
zone_lookup_df.createOrReplaceTempView("taxi_zone_lookup_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC from bronze_taxi_trips_view
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS row_count,
# MAGIC   MIN(tpep_pickup_datetime) AS earliest_pickup,
# MAGIC   MAX(tpep_pickup_datetime) AS latest_pickup,
# MAGIC   ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
# MAGIC   ROUND(AVG(total_amount), 2) AS avg_total_amount
# MAGIC FROM bronze_taxi_trips_view;

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_date,
    hour,
    unix_timestamp,
    round
)

# COMMAND ----------

silver_trips_df = (
    bronze_trips_df
    .select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount"
    )
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .withColumn(
        "trip_duration_minutes",
        round(
            (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60,
            2
        )
    )
    .withColumn("source_system", lit("nyc_tlc_yellow_taxi"))
    .withColumn("load_timestamp", current_timestamp())
)

display(silver_trips_df.limit(10))

# COMMAND ----------

silver_trips_df = (
    silver_trips_df
    .fillna({
        "passenger_count": 0,
        "trip_distance": 0.0,
        "fare_amount": 0.0,
        "tip_amount": 0.0,
        "total_amount": 0.0
    })
    .filter(col("trip_distance") > 0)
    .filter(col("total_amount") > 0)
    .filter(col("trip_duration_minutes") > 0)
    .filter(col("trip_duration_minutes") <= 240)
)

display(silver_trips_df.limit(10))

# COMMAND ----------

bronze_count = bronze_trips_df.count()
silver_count = silver_trips_df.count()

print(f"Bronze row count: {bronze_count}")
print(f"Silver row count after cleaning: {silver_count}")
print(f"Rows removed during cleaning: {bronze_count - silver_count}")

# COMMAND ----------

pickup_zones_df = (
    zone_lookup_df
    .withColumnRenamed("LocationID", "PULocationID")
    .withColumnRenamed("Borough", "pickup_borough")
    .withColumnRenamed("Zone", "pickup_zone")
    .withColumnRenamed("service_zone", "pickup_service_zone")
)

display(pickup_zones_df)

# COMMAND ----------

silver_trips_with_zones_df = (
    silver_trips_df
    .join(pickup_zones_df, on="PULocationID", how="left")
)

display(silver_trips_with_zones_df.limit(10))

# COMMAND ----------

missing_pickup_zone_count = (
    silver_trips_with_zones_df
    .filter(col("pickup_zone").isNull())
    .count()
)

print(f"Trips with missing pickup zone after join: {missing_pickup_zone_count}")

# COMMAND ----------

silver_trips_with_zones_df.write.mode("overwrite").saveAsTable("week1_real_silver_taxi_trips")

# COMMAND ----------

from pyspark.sql.functions import avg, count

# COMMAND ----------

gold_summary_df = (
    silver_trips_with_zones_df
    .groupBy("pickup_borough", "pickup_zone", "payment_type")
    .agg(
        count("*").alias("trip_count"),
        round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        round(avg("total_amount"), 2).alias("avg_total_amount"),
        round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
    )
)

display(gold_summary_df.orderBy(col("trip_count").desc()).limit(20))

# COMMAND ----------

gold_summary_df.write.mode("overwrite").saveAsTable("week1_real_gold_taxi_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM week1_real_gold_taxi_summary
# MAGIC ORDER BY trip_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, when

null_check_df = silver_trips_with_zones_df.select(
    spark_sum(when(col("pickup_borough").isNull(), 1).otherwise(0)).alias("null_pickup_borough"),
    spark_sum(when(col("pickup_zone").isNull(), 1).otherwise(0)).alias("null_pickup_zone"),
    spark_sum(when(col("trip_distance").isNull(), 1).otherwise(0)).alias("null_trip_distance"),
    spark_sum(when(col("total_amount").isNull(), 1).otherwise(0)).alias("null_total_amount"),
    spark_sum(when(col("trip_duration_minutes").isNull(), 1).otherwise(0)).alias("null_trip_duration_minutes")
)

display(null_check_df)

# COMMAND ----------

invalid_values_df = silver_trips_with_zones_df.filter(
    (col("trip_distance") <= 0) |
    (col("total_amount") <= 0) |
    (col("trip_duration_minutes") <= 0) |
    (col("trip_duration_minutes") > 240)
)

invalid_count = invalid_values_df.count()

print(f"Invalid value count after cleaning: {invalid_count}")

# COMMAND ----------

duplicates_df = (
    silver_trips_with_zones_df
    .groupBy(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "total_amount"
    )
    .count()
    .filter(col("count") > 1)
)

display(duplicates_df.limit(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES LIKE 'week1_real*';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE week1_real_gold_taxi_summary;

# COMMAND ----------

print("Week 1 real-data taxi pipeline completed.")
print(f"Bronze row count: {bronze_count}")
print(f"Silver row count after cleaning: {silver_count}")
print(f"Rows removed during cleaning: {bronze_count - silver_count}")
print(f"Trips with missing pickup zone after join: {missing_pickup_zone_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary
# MAGIC
# MAGIC In this notebook, I applied my Week 1 Databricks learning to a real public dataset from NYC TLC.
# MAGIC
# MAGIC ### Bronze
# MAGIC
# MAGIC The Bronze layer stores the raw taxi trip data and raw taxi zone lookup data.
# MAGIC
# MAGIC Tables created:
# MAGIC
# MAGIC - `week1_real_bronze_taxi_trips`
# MAGIC - `week1_real_bronze_taxi_zones`
# MAGIC
# MAGIC ### Silver
# MAGIC
# MAGIC The Silver layer cleans and enriches the taxi trip data by:
# MAGIC
# MAGIC - selecting useful columns
# MAGIC - creating `pickup_date`
# MAGIC - creating `pickup_hour`
# MAGIC - calculating `trip_duration_minutes`
# MAGIC - replacing missing numeric values
# MAGIC - filtering invalid distances, amounts and durations
# MAGIC - joining pickup borough and zone information
# MAGIC - adding audit columns
# MAGIC
# MAGIC Table created:
# MAGIC
# MAGIC - `week1_real_silver_taxi_trips`
# MAGIC
# MAGIC ### Gold
# MAGIC
# MAGIC The Gold layer summarises the cleaned taxi trips by pickup borough, pickup zone and payment type.
# MAGIC
# MAGIC Table created:
# MAGIC
# MAGIC - `week1_real_gold_taxi_summary`
# MAGIC
# MAGIC ### QA checks
# MAGIC
# MAGIC The notebook includes:
# MAGIC
# MAGIC - Bronze vs Silver row count comparison
# MAGIC - missing pickup zone check after join
# MAGIC - null checks
# MAGIC - invalid value checks
# MAGIC - duplicate-like record checks
# MAGIC - saved table verification
# MAGIC
# MAGIC ### Key learning
# MAGIC
# MAGIC This notebook shows how to load real Parquet and CSV data into Databricks, process it using PySpark, query it using Spark SQL, save Bronze/Silver/Gold tables, and apply basic data quality checks.

# COMMAND ----------

