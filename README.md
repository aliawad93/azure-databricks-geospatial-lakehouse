# Azure Databricks Geospatial Lakehouse

This repository documents my learning journey towards Azure Databricks, PySpark, Spark SQL and lakehouse-style data engineering.

The project starts with basic Databricks training notebooks and progressively develops into a practical portfolio project focused on geospatial, environmental, energy and real-world open datasets.

## Goal

Build practical hands-on skills in Databricks and modern data engineering, with a focus on:

* Azure Databricks
* PySpark
* Spark SQL
* Delta/lakehouse-style tables
* Bronze, Silver and Gold pipeline structure
* Data quality checks
* Real-world open datasets
* Future geospatial processing using GeoParquet and Apache Sedona

## Why This Project Matters

This project demonstrates practical data engineering skills using Databricks, PySpark, Spark SQL and lakehouse-style architecture.

It starts with simple training notebooks and then applies the same concepts to a larger real-world public dataset. The aim is to build a portfolio that shows hands-on ability with data ingestion, transformation, table creation, quality checks and reporting-ready outputs.

The longer-term goal is to develop a strong geospatial data engineering portfolio using Databricks, Spark and lakehouse patterns.

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

| Layer  | Purpose                       |
| ------ | ----------------------------- |
| Bronze | Raw environmental asset data  |
| Silver | Cleaned and standardised data |
| Gold   | Summary table for reporting   |

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
* Potential duplicate checks
* A lookup join to enrich trip records with pickup borough and pickup zone names

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
* Potential duplicate checks
* Missing pickup zone check after lookup join
* Saved table verification

## Week 1 Real Data Results

The real-data taxi notebook processed more than 2.9 million Bronze records.

| Metric                                    |     Value |
| ----------------------------------------- | --------: |
| Bronze taxi trip records                  | 2,964,624 |
| Silver taxi trip records after cleaning   | 2,870,066 |
| Rows removed during cleaning              |    94,558 |
| Trips with missing pickup zone after join |         0 |

## Key Learning

A DataFrame is temporary while working in a notebook, but a saved Databricks table can be queried again later.

PySpark is the Python way of using Spark, while Spark SQL is the SQL way of using Spark.

The Bronze/Silver/Gold pattern helps structure data pipelines by separating raw data, cleaned data and reporting-ready data.

This is an early lakehouse data engineering pipeline. Later stages of the project will extend it with geospatial processing using taxi zone geometries, GeoParquet and Apache Sedona.

## Known Limitations

* The raw taxi files are not included in the repository because they are large.
* The current pipeline is batch-based and manually triggered.
* The current version focuses on pickup zone enrichment only.
* The project does not yet include automated scheduling, incremental loading or Unity Catalog governance.
* The current taxi pipeline is not yet fully geospatial because it does not use taxi zone boundary geometries.
* Geospatial processing will be added in later weeks using taxi zone geometries, GeoParquet and Apache Sedona.

## Next Steps

* Add stronger PySpark transformations and reusable functions.
* Practise joins, window functions and date/time analysis.
* Add more detailed data quality checks.
* Create incremental loading examples.
* Add workflow orchestration using Databricks Workflows.
* Introduce geospatial processing using taxi zones, GeoParquet and Apache Sedona.

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
