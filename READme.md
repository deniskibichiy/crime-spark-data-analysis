# Chicago Crime Data Analysis (PySpark)

An end-to-end Big Data pipeline designed to process, clean, and analyze the **Chicago Crime Dataset** (2001–Present). This project handles over **8.5 million rows** of data, utilizing Apache Spark's distributed computing capabilities to perform high-performance ETL and visualization.

---

## Architecture & Workflow
The project follows a standard Data Engineering lifecycle: **Ingestion → Cleaning → Transformation → Analysis → Visualization.**



### Key Technical Features:
* **Schema Enforcement:** Manual `StructType` definition to avoid the overhead of `inferSchema`.
* **Memory Management:** Configured with `spark.driver.memory = 4g` to handle large-scale CSV processing.
* **Headless Visualization:** Utilizes the Matplotlib `Agg` backend for generating charts in a Linux terminal environment.

---

## Tech Stack
* **Engine:** Apache Spark 3.5.x (PySpark)
* **Language:** Python 3.11
* **Environment:** Miniforge3 (Conda)
* **Libraries:** `pyspark`, `matplotlib`, `pandas`, `os`

---

## Project Structure
```text
.
├── data/               # Input: chicago_crimes.csv (git-ignored)
├── output/             # Output: top_10_crimes.png & analysis logs
├── src/
│   ├── main.py         # Primary ETL & Analysis pipeline
│   └── schema.py       # StructType schema definitions
└── README.md           # Documentation
```
## Data Pipeline Stages

### 1. Ingestion & Schema
We load **8.5M+ rows** using a **predefined schema** to ensure data integrity and improve processing speed.

### 2. Data Cleaning
- **Timestamp Conversion:** Converted string dates (e.g., `05/23/2016 11:00:00 PM`) into Spark `TimestampType`.
- **Null Handling:** Executed `.dropna()` to remove incomplete records for improved spatial and temporal accuracy.

### 3. Filtering & Logic
- **Timeline Filtering:** Restricted the dataset to records between **2016 and 2026**.
- **Category Exclusions:** Removed administrative or non-criminal categories (e.g., `STALKING`, `ARSON`, `NON-CRIMINAL`).
- **Category Merging:** Combined `SEX OFFENSE` and `PROSTITUTION` into a single analytical category.

### 4. Analysis
- **Trend Analysis:** Grouped incidents by **Year** to visualize crime frequency across the last decade.
- **Temporal Patterns:** Identified the **peak hour of the day** when most criminal incidents occur.
- **Visualization:** Generated and exported a **Bar Chart of the Top 10 Crime Types** to the `/output` directory.

---
## System Environment
- **OS:** Ubuntu 22.04 / 24.04 LTS
- **Shell:** Bash / Zsh
- **Java:** OpenJDK 11 or 17 (Required for Spark)
- **Memory:** 8GB RAM minimum (4GB allocated to Spark Driver)
---
## Dependencies
```bash
pyspark
matplotlib
pandas
```


