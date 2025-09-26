
# Olympics Data Engineering Project

## Overview
This project demonstrates an **end-to-end Azure Data Engineering pipeline** using:
- **Azure DevOps** for CI/CD and branch management (Feature → Main → Live).
- **Azure Data Factory (ADF)** for ingestion and orchestration.
- **Azure Databricks** (PySpark + Delta Lake) for transformations.
- **Unity Catalog & External Locations** for governance.
- **Delta Live Tables (DLT)** for curated Gold layer with CDC + expectations.
- **Power BI** for analytics.

It implements a modern **Lakehouse architecture (Bronze → Silver → Gold)** with production-grade governance and automation.

---

## Architecture
**Flow:**
1. Source data pulled from GitHub → Bronze container via ADF.  
2. Metadata-driven pipelines organize files dynamically.  
3. Silver layer notebooks in Databricks apply cleaning & transformations.  
4. Gold layer curated with DLT, CDC, and data quality rules.  
5. Gold schema exposed to Power BI for reporting.  

---

## DevOps (CI/CD)
- Used **Azure DevOps Repos** to maintain pipelines + notebooks.  
- Branching strategy:  
  - **Feature branch** for development.  
  - **Main branch** for tested code.  
  - **Live branch** for production promotion.  
- Screenshots of ADF pipelines and DevOps merges are included in `adf_pipelines/screenshots`.  

---

## Azure Data Factory Pipelines

### 1. GitHub JSON Ingestion
- Parameters: `fileName`, `relUrl`, `baseUrl`, `sinkFolder`, `sinkContainer`.  
- Activities:  
  - **ForEach + Copy Activity** → ingest JSON into Bronze.  

### 2. Metadata-Driven File Selection
- Activities:  
  - **Get Metadata** → fetch list of files in a container.  
  - **ForEach** → iterate over files.  
  - **If Condition** → match by filename and type:
    ```json
    @and(
      equals(item().filename,'nocs.csv'),
      equals(item().type,'File')
    )
    ```
  - **Else branch** → append unmatched files.  
  - **Set Variable** → capture unmatched list + file count (`@length()`).  

---

## Databricks Silver Layer

### Unity Catalog Setup
- Created **Unity Catalog** + **External Locations** (`bronze_ext`, `silver_ext`, `gold_ext`).  
- Purpose: central governance, no secrets/SAS tokens, cross-workspace access.  

### NOCs Dataset
- Read CSV from Bronze.  
- Dropped redundant columns.  
- Cleaned tag column (split by `-`, kept first token).  
- Wrote into Silver as Delta table `Olympics.Silver.NOCs`.  

### Athletes Dataset Transformations
- **Null Handling:** replaced nulls in birth/residence fields with `"Unknown"`.  
- **Filtering:** kept active athletes (`current = True`).  
- **Type Casting:** converted `height`, `weight` → `FloatType`.  
- **Sorting & Filtering:** sorted by height desc, weight asc; dropped `weight <= 0`.  
- **Value Replacement:** standardized nationality (`United States` → `US`).  
- **Duplicate Check:** grouped by `code` to validate uniqueness.  
- **Column Rename:** `code` → `athleteId`.  
- **Array Conversion:** split `occupation` column into arrays.  
- **Column Pruning:** selected relevant attributes.  
- **Window Function:** calculated cumulative weight per nationality.  
- **Write:** stored in Silver as Delta table `Olympics.Silver.Athletes`.  

### Parameterized Notebooks + Job Pipelines
- Used `dbutils.widgets.text()` for parameters:
  - `sourceContainer`, `sinkContainer`, `folder`.  
- Built lookup notebook:
  ```python
  myFiles = [
    {"sourceContainer":"bronze","sinkContainer":"silver","folder":"events"},
    {"sourceContainer":"bronze","sinkContainer":"silver","folder":"coaches"}
  ]
  dbutils.jobs.taskValues.set(key="output", value=myFiles)

  ## Databricks Gold Layer – Delta Live Tables (DLT)

The Gold layer provides **curated, analytics-ready datasets** with enforced data quality and CDC handling.

### Key Features
- **Streaming Tables & Views**: Ingest Silver tables into Gold schema with final cleanup.  
- **Data Quality Expectations**: Applied rules like `code IS NOT NULL`, `event IS NOT NULL`, `current = True`.  
- **CDC / Upserts**: Implemented SCD Type 1 using `dlt.apply_changes` for Athletes dataset.  
- **Unity Catalog Integration**: All Gold tables are governed, secure, and lineage-tracked.  

### Result
Curated entities (`Athletes`, `Coaches`, `NOCs`, `Events`) are stored in Gold schema, validated, and connected to **Power BI dashboards** for reporting and insights.

