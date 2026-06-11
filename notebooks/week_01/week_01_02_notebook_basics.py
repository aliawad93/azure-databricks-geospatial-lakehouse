# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Basics
# MAGIC
# MAGIC This notebook practises Python cells, SQL cells and Markdown documentation.
# MAGIC
# MAGIC ## Why notebooks matter
# MAGIC
# MAGIC Notebooks allow data engineers to combine code, results and explanation in one place.

# COMMAND ----------

project_name = "DEFRA-style Geospatial Lakehouse"
week = 1
role = "Geospatial Data Engineer"

print(project_name)
print(week)
print(role)

# COMMAND ----------

datasets = [
    {"name": "flood_zones", "source": "Environment Agency", "format": "GeoPackage"},
    {"name": "rivers", "source": "Open data", "format": "GeoJSON"},
    {"name": "local_authorities", "source": "ONS", "format": "Shapefile"}
]

datasets

# COMMAND ----------

df = spark.createDataFrame(datasets)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("datasets_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM datasets_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT format, COUNT(*) AS dataset_count
# MAGIC FROM datasets_view
# MAGIC GROUP BY format;

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I learned
# MAGIC
# MAGIC - I created Python variables.
# MAGIC - I created a small list of dictionaries.
# MAGIC - I converted Python data into a Spark DataFrame.
# MAGIC - I displayed the DataFrame.
# MAGIC - I created a temporary SQL view.
# MAGIC - I queried the view using SQL.
# MAGIC

# COMMAND ----------

