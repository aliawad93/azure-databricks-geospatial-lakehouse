# Databricks notebook source
# MAGIC %md
# MAGIC # First Databricks Table
# MAGIC
# MAGIC Today I am learning how to save a cleaned Spark DataFrame as a reusable Databricks table.
# MAGIC
# MAGIC I will practise:
# MAGIC
# MAGIC - creating a DataFrame
# MAGIC - cleaning null values
# MAGIC - adding an audit/source column
# MAGIC - saving the result as a table
# MAGIC - querying the table using SQL
# MAGIC - describing the table structure

# COMMAND ----------

from pyspark.sql.functions import lit

data = [
    ("EA001", "Flood Zone 2", "Yorkshire", 1200.5, "High"),
    ("EA002", "Flood Zone 3", "Yorkshire", 850.0, "Very High"),
    ("EA003", "River Buffer", "North West", 430.2, "Medium"),
    ("EA004", "Protected Area", "South East", 2100.7, "Low"),
    ("EA005", "Flood Zone 3", "North West", None, "Very High")
]

columns = ["asset_id", "asset_type", "region", "area_sqm", "risk_level"]

df = spark.createDataFrame(data, columns)

df_clean = (
    df
    .fillna({"area_sqm": 0})
    .withColumn("source_system", lit("training_sample"))
)

display(df_clean)

# COMMAND ----------

df_clean.write.mode("overwrite").saveAsTable("week1_environment_assets")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM week1_environment_assets;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE week1_environment_assets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count
# MAGIC FROM week1_environment_assets;

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook does
# MAGIC
# MAGIC 1. Creates a small environmental asset dataset.
# MAGIC 2. Handles null values in the `area_sqm` field.
# MAGIC 3. Adds a `source_system` audit column.
# MAGIC 4. Saves the cleaned data as a Databricks table.
# MAGIC 5. Queries the table using SQL.
# MAGIC 6. Checks the table structure and row count.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC In real data engineering projects, raw data is cleaned, standardised and saved as a reusable table for downstream analytics, reporting or further transformation.
# MAGIC
# MAGIC ## Key point
# MAGIC
# MAGIC A DataFrame exists while the notebook/session is running, but a saved table can be queried again later.