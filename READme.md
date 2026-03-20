# Chicago Crime Data Analysis with Apache Spark

## Project Progress
- [x] **Step 1:** Initialized Spark Session with custom memory allocation (4GB).
- [x] **Step 2:** Defined a strict `StructType` schema for 22 columns to optimize ingestion.
- [x] **Step 3:** Successfully loaded 1.8GB+ of CSV data using relative pathing.
- [x] **Step 4:** Cleaned null values and cast 'Date' strings to `TimestampType`.
- [x] **Step 5-7:** Filtered for 2016-2026, excluded non-criminal categories, and merged 'Sex Offense' and 'Prostitution' types.