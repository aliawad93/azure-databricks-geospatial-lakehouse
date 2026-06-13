# Data Notes

Large raw data files are not stored in this GitHub repository.

The Week 1 real-data extension uses NYC Taxi & Limousine Commission Yellow Taxi Trip Record data and the Taxi Zone Lookup Table.

The source files used are:

- `yellow_tripdata_2024-01.parquet`
- `taxi_zone_lookup.csv`

These files were downloaded from the official NYC TLC Trip Record Data page and uploaded manually into Databricks Free Edition.

The raw data is stored in Databricks Volumes, while this repository stores only notebooks, documentation and reproducible instructions.

The optional taxi zone shapefile was downloaded for possible future geospatial work but was not used in the Week 1 real-data pipeline.
