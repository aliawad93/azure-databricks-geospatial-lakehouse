# Azure Databricks Geospatial Lakehouse

This repository documents my learning journey towards Azure Databricks, PySpark, Spark SQL and lakehouse-style data engineering.

## Goal

Build practical skills in Databricks and data engineering, with a focus on geospatial, environmental and energy-style datasets.

## Week 1 Summary

Week 1 focused on Databricks fundamentals, notebooks, PySpark, Spark SQL, saved tables and a simple Bronze/Silver/Gold-style pipeline.

## What I Built

* Databricks orientation notebook
* Notebook basics notebook
* PySpark basics notebook
* Spark SQL basics notebook
* First saved Databricks table
* Mini Bronze/Silver/Gold pipeline

## Week 1 Notebooks

* `week_01_01_databricks_orientation.py`
* `week_01_02_notebook_basics.py`
* `week_01_03_pyspark_basics.py`
* `week_01_04_spark_sql_basics.py`
* `week_01_05_first_table.py`
* `week_01_06_mini_pipeline.py`

## Skills Practised

* Databricks notebooks
* Markdown documentation
* Python cells
* SQL cells
* Spark DataFrames
* PySpark transformations
* Temporary SQL views
* Spark SQL filtering and aggregation
* Null handling
* Saving Databricks tables
* Basic QA checks
* Bronze, Silver and Gold pipeline structure

## Week 1 Pipeline

The mini pipeline follows a simple medallion-style structure:

* Bronze: raw environmental asset data
* Silver: cleaned and standardised data
* Gold: summary table for reporting

## QA Checks Included

* Bronze and Silver row count comparison
* Null check on `area_sqm`
* Duplicate check on `asset_id`

## Key Learning

A DataFrame is temporary while working in a notebook, but a saved Databricks table can be queried again later. PySpark is the Python way of using Spark, while Spark SQL is the SQL way of using Spark.
