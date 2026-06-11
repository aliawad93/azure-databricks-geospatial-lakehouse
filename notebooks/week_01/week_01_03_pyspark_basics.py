# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark Basics
# MAGIC
# MAGIC Today I am practising common PySpark DataFrame operations:
# MAGIC
# MAGIC - select columns
# MAGIC - filter rows
# MAGIC - add columns
# MAGIC - handle null values
# MAGIC - group and aggregate data

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

display(df)

# COMMAND ----------

df_selected = df.select("asset_id", "asset_type", "region")

display(df_selected)

# COMMAND ----------

df_high_risk = df.filter(df.risk_level.isin("High", "Very High"))
display(df_high_risk)

# COMMAND ----------

from pyspark.sql.functions import lit
df_with_source = df.withColumn("source_system", lit("training_sample"))
display(df_with_source)

# COMMAND ----------

from pyspark.sql.functions import col

df_null_area = df.filter(col("area_sqm").isNull())

display(df_null_area)

# COMMAND ----------

df_clean = df.fillna({"area_sqm": 0})

display(df_clean)

# COMMAND ----------

df_region_summary = df_clean.groupBy("region").sum("area_sqm")

display(df_region_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I learned
# MAGIC
# MAGIC - `select()` chooses columns.
# MAGIC - `filter()` chooses rows.
# MAGIC - `withColumn()` adds or changes a column.
# MAGIC - `isNull()` helps find missing values.
# MAGIC - `fillna()` replaces missing values.
# MAGIC - `groupBy()` groups records for aggregation.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC These are the basic building blocks of data cleaning and transformation in PySpark.