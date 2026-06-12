# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL Basics
# MAGIC
# MAGIC Today I am practising how to query Spark DataFrames using SQL.
# MAGIC
# MAGIC I will practise:
# MAGIC
# MAGIC - SELECT
# MAGIC - WHERE
# MAGIC - GROUP BY
# MAGIC - ORDER BY
# MAGIC - IS NULL
# MAGIC - COALESCE
# MAGIC - TEMP VIEW

# COMMAND ----------

data = [
    ("EA001", "Flood Zone 2", "Yorkshire", 1200.5, "High"),
    ("EA002", "Flood Zone 3", "Yorkshire", 850.0, "Very High"),
    ("EA003", "River Buffer", "North West", 430.2, "Medium"),
    ("EA004", "Protected Area", "South East", 2100.7, "Low"),
    ("EA005", "Flood Zone 3", "North West", None, "Very High")
]

columns = ["asset_id", "asset_type", "region", "area_sqm", "risk_level"]

df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("environment_assets")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM environment_assets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM environment_assets
# MAGIC WHERE risk_level IN ('High', 'Very High');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   COUNT(*) AS record_count,
# MAGIC   SUM(area_sqm) AS total_area_sqm
# MAGIC FROM environment_assets
# MAGIC GROUP BY region
# MAGIC ORDER BY total_area_sqm DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM environment_assets
# MAGIC WHERE area_sqm IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW environment_assets_clean AS
# MAGIC SELECT
# MAGIC   asset_id,
# MAGIC   asset_type,
# MAGIC   region,
# MAGIC   COALESCE(area_sqm, 0) AS area_sqm,
# MAGIC   risk_level
# MAGIC FROM environment_assets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM environment_assets_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I learned
# MAGIC
# MAGIC - I queried a Spark DataFrame using SQL.
# MAGIC - I filtered records using WHERE.
# MAGIC - I grouped records using GROUP BY.
# MAGIC - I checked missing values using IS NULL.
# MAGIC - I replaced null values using COALESCE.
# MAGIC - I created a cleaned temporary SQL view.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Spark SQL is useful because I can apply SQL knowledge directly inside Databricks while working with Spark DataFrames.

# COMMAND ----------

