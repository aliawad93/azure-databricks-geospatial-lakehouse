# Databricks notebook source
# MAGIC %md
# MAGIC # Mini Lakehouse Pipeline - Week 1 
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Create a simple raw to cleaned to summary data pipeline using PySpark and SQL
# MAGIC
# MAGIC ## Layers
# MAGIC
# MAGIC - Bronze: raw sample data
# MAGIC
# MAGIC - Silver: cleaned and standardised data
# MAGIC
# MAGIC - Gold: summary table for reporting

# COMMAND ----------

bronze_data = [
    ("EA001", "Flood Zone 2", "Yorkshire", "1200.5", "High"),
    ("EA002", "Flood Zone 3", "Yorkshire", "850.0", "Very High"),
    ("EA003", "River Buffer", "North West", "430.2", "Medium"),
    ("EA004", "Protected Area", "South East", "2100.7", "Low"),
    ("EA005", "Flood Zone 3", "North West", None, "Very High"),
    ("EA006", "Flood Zone 3", "Yorkshire", "900.0", "Very High")
]

columns = ["asset_id", "asset_type", "region", "area_sqm", "risk_level"]

bronze_df = spark.createDataFrame(bronze_data, columns)

display(bronze_df)

# COMMAND ----------

bronze_df.write.mode("overwrite").saveAsTable("week1_bronze_environment_assets")

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

silver_df = (
    bronze_df
    .withColumn("area_sqm", col("area_sqm").cast("double"))
    .fillna({"area_sqm": 0})
    .withColumn("source_system", lit("week1_training"))
    .withColumn("load_timestamp", current_timestamp())
)

display(silver_df)

# COMMAND ----------

silver_df.write.mode("overwrite").saveAsTable("week1_silver_environment_assets")

# COMMAND ----------

gold_df = (
    silver_df
    .groupBy("region", "risk_level")
    .count()
    .withColumnRenamed("count", "asset_count")
)

display(gold_df)

# COMMAND ----------

gold_df.write.mode("overwrite").saveAsTable("week1_gold_environment_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM week1_bronze_environment_assets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM week1_silver_environment_assets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM week1_gold_environment_summary;

# COMMAND ----------

bronze_count = bronze_df.count()
silver_count = silver_df.count()

print(f"Bronze count: {bronze_count}")
print(f"Silver count: {silver_count}")

if bronze_count == silver_count:
    print("PASS: Row counts match")
else:
    print("FAIL: Row counts do not match")

# COMMAND ----------

null_area_count = silver_df.filter(col("area_sqm").isNull()).count()

print(f"Null area count: {null_area_count}")

# COMMAND ----------

from pyspark.sql.functions import count

duplicates_df = (
    silver_df
    .groupBy("asset_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

display(duplicates_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I built
# MAGIC
# MAGIC I created a small Bronze, Silver and Gold pipeline.
# MAGIC
# MAGIC ## Bronze
# MAGIC
# MAGIC The Bronze table stores the raw data exactly as received.
# MAGIC
# MAGIC ## Silver
# MAGIC
# MAGIC The Silver table cleans the data by casting `area_sqm` to a numeric value, replacing nulls, and adding audit columns.
# MAGIC
# MAGIC ## Gold
# MAGIC
# MAGIC The Gold table summarises the cleaned data by region and risk level.
# MAGIC
# MAGIC ## QA checks
# MAGIC
# MAGIC I checked:
# MAGIC
# MAGIC - Bronze vs Silver row counts
# MAGIC - Null values in `area_sqm`
# MAGIC - Duplicate `asset_id` values
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC This is a small version of a real data engineering pipeline where raw data is ingested, cleaned, standardised, quality checked and summarised for reporting.

# COMMAND ----------

