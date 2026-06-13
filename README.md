# Azure Databricks Geospatial Lakehouse

This repository documents my learning journey towards Azure Databricks, PySpark, Spark SQL and lakehouse-style data engineering.

## Goal

Build practical skills in Databricks and data engineering, with a focus on geospatial, environmental, energy and real-world open datasets.

## Week 1 Summary

Week 1 focused on Databricks fundamentals, notebooks, PySpark, Spark SQL, saved tables and a simple Bronze/Silver/Gold-style pipeline.

After completing the basic training notebooks, I applied the same concepts to a real public dataset: NYC TLC Yellow Taxi Trip Records.

## What I Built

* Databricks orientation notebook
* Notebook basics notebook
* PySpark basics notebook
* Spark SQL basics notebook
* First saved Databricks table
* Mini Bronze/Silver/Gold pipeline
* Real-data NYC taxi Bronze/Silver/Gold pipeline

## Week 1 Notebooks

* `week_01_01_databricks_orientation.py`
* `week_01_02_notebook_basics.py`
* `week_01_03_pyspark_basics.py`
* `week_01_04_spark_sql_basics.py`
* `week_01_05_first_table.py`
* `week_01_06_mini_pipeline.py`
* `week_01_07_real_data_taxi_pipeline.py`

## Skills Practised

* Databricks notebooks
* Markdown documentation
* Python cells
* SQL cells
* Spark DataFrames
* PySpark transformations
* Temporary SQL views
* Spark SQL filtering and aggregation
* Reading Parquet data
* Reading CSV data
* Databricks Volumes
* Null handling
* Invalid value filtering
* DataFrame joins
* Saving Databricks tables
* Basic QA checks
* Bronze, Silver and Gold pipeline structure

## Week 1 Mini Pipeline

The first mini pipeline uses a small environmental-style sample dataset and follows a simple medallion-style structure:

* Bronze: raw environmental asset data
* Silver: cleaned and standardised data
* Gold: summary table for reporting

## Week 1 Real Data Extension

The real-data extension applies the same Week 1 concepts to NYC TLC Yellow Taxi Trip Records.

This extension uses:

* Parquet taxi trip data
* CSV taxi zone lookup data
* Databricks Volumes
* Bronze, Silver and Gold tables
* PySpark transformations
* Spark SQL queries
* Row count checks
* Null checks
* Invalid value checks
* Duplicate-like record checks
* A lookup join to enrich trip records with pickup borough and zone names

The raw data files are not stored in this repository. They were downloaded from the official NYC TLC source and uploaded manually into Databricks.

## Real Data Pipeline Summary

The real-data taxi pipeline follows this structure:

### Bronze

The Bronze layer stores the raw taxi trip data and the raw taxi zone lookup data.

Tables created:

* `week1_real_bronze_taxi_trips`
* `week1_real_bronze_taxi_zones`

### Silver

The Silver layer cleans and enriches the taxi trip data by:

* selecting useful columns
* creating `pickup_date`
* creating `pickup_hour`
* calculating `trip_duration_minutes`
* replacing missing numeric values
* filtering invalid distances, amounts and durations
* joining pickup borough and pickup zone information
* adding audit columns such as `source_system` and `load_timestamp`

Table created:

* `week1_real_silver_taxi_trips`

### Gold

The Gold layer summarises the cleaned taxi trips by pickup borough, pickup zone and payment type.

Table created:

* `week1_real_gold_taxi_summary`

## QA Checks Included

The notebooks include the following data quality checks:

* Bronze and Silver row count comparison
* Rows removed during cleaning
* Null checks on important columns
* Invalid value checks after cleaning
* Duplicate-like record checks
* Missing pickup zone check after lookup join
* Saved table verification

## Week 1 Real Data Results

The real-data taxi notebook processed more than 2.9 million Bronze records.

Key results:

* Bronze row count: 2,964,624
* Silver row count after cleaning: 2,870,066
* Rows removed during cleaning: 94,558
* Trips with missing pickup zone after join: 0

## Key Learning

A DataFrame is temporary while working in a notebook, but a saved Databricks table can be queried again later.

PySpark is the Python way of using Spark, while Spark SQL is the SQL way of using Spark.

The Bronze/Silver/Gold pattern helps structure data pipelines by separating raw data, cleaned data and reporting-ready data.

## Repository Structure

```text
azure-databricks-geospatial-lakehouse/
│
├── data/
│   └── sample/
│       ├── .gitkeep
│       └── README.md
│
├── docs/
│   ├── glossary.md
│   └── week_01_summary.md
│
├── notebooks/
│   └── week_01/
│       ├── week_01_01_databricks_orientation.py
│       ├── week_01_02_notebook_basics.py
│       ├── week_01_03_pyspark_basics.py
│       ├── week_01_04_spark_sql_basics.py
│       ├── week_01_05_first_table.py
│       ├── week_01_06_mini_pipeline.py
│       └── week_01_07_real_data_taxi_pipeline.py
│
├── .gitignore
└── README.md
```

## Data Storage Note

Large raw data files are not committed to this repository.

The raw taxi files were stored in Databricks Volumes and processed from there. This keeps the GitHub repository clean and focused on code, documentation and reproducible learning outputs.

## Current Status

Week 1 is complete.

This repository now contains both basic learning notebooks and a real-data extension that applies the same concepts to a large public dataset.
