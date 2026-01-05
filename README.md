# Stock Trading & Investment Performance Analytics

## Project Overview

This project implements an end-to-end **stock market analytics pipeline** using **Apache Airflow, Python (Pandas), Azure Databricks (PySpark), and Power BI**. The solution follows a **Medallion Architecture-inspired approach** (Bronze-Silver-Gold) to ingest raw stock and portfolio data, validate and transform it, perform scalable analytics, and present insights through interactive dashboards.

The pipeline demonstrates real-world data engineering concepts such as workflow orchestration, data quality validation, time-series analysis, distributed analytics, and business intelligence reporting.

---

## Business Objectives

* Ingest and process raw stock price and portfolio transaction data
* Perform time-series analysis and return calculations
* Validate data integrity, risk metrics, and transformations
* Automate analytics execution using Airflow and Databricks
* Visualize stock performance, portfolio health, and risk insights in Power BI

---

## Architecture Overview

```
Raw CSV Data
   ↓
Airflow DAG Orchestration
   ↓
ingestion_cleaning.py (Bronze + Silver)
   ↓
Cleaned & Validated CSV Outputs
   ↓
Databricks Gold Analytics (PySpark)
   ↓
Power BI Dashboards
```

---

## Project Directory Structure

```
CAPSTONE PROJECT
│
├── airflow
│   ├── dags
│   │   └── stock_analytics_databricks_pipeline.py
│   └── scripts
│       └── ingestion_cleaning.py
│
├── data
│   ├── raw
│   └── processed
│       ├── cleaned_stock_data.csv
│       └── cleaned_transactions.csv
│
├── notebooks
│   ├── bronze_ingestion.ipynb
│   ├── silver_transformations.ipynb
│   ├── gold_analytics.ipynb
│   └── stock_analytics_job.ipynb
│
├── outputs
│   ├── airflow
│   ├── analytics
│   ├── ingestion & transformations
│   └── powerbi
│
└── powerbi
    └── Capstone Project.pbix
```

---

## Data Processing Layers

### Bronze Layer (Raw Ingestion)

* Validates file availability and schema
* Preserve raw data structure for traceability
* Standardize file formats and storage location
* Serve as a stable input for Silver-layer transformations

### Silver Layer (Cleaning & Transformation)

* Handles missing values and forward filling
* Sorts data for time-series consistency
* Calculates daily returns and cumulative returns using Pandas
* Computes moving averages and normalized prices
* Performs sanity checks on prices, volume, and returns

> **Note:** Bronze and Silver layers are logically separated but **operationally combined** in a single Python script (`ingestion_cleaning.py`) executed by Airflow. This reduces orchestration complexity while maintaining clear processing responsibilities.

### Gold Layer (Analytics)

* Executed in Azure Databricks using PySpark
* Aggregations for portfolio value, profit/loss, volatility, and returns
* Sub-sector level performance and risk analysis
* Output tables optimized for BI consumption

---

## Airflow Workflow Orchestration

The Airflow DAG (`stock_analytics_databricks_pipeline.py`) performs:

* Scheduled and manual pipeline execution
* Execution of Bronze + Silver processing via `ingestion_cleaning.py`
* Triggering Databricks analytics notebooks using Databricks operators
* Retry handling, logging, and monitoring

The DAG graph and logs confirm successful orchestration and dependency management across pipeline stages.

---

## Databricks Analytics

Databricks notebooks perform:

* Distributed PySpark transformations
* Return and volatility validation checks
* Portfolio-level and sub-sector aggregations
* Gold-layer table generation for dashboards

Job execution is triggered programmatically by Airflow using the Databricks Submit Run API.

---

## Power BI Dashboards

The Power BI report (`Capstone Project.pbix`) contains multiple dashboards:

### Dashboard 1: Stock Market Performance Overview

* Normalized stock price trends
* Cumulative return growth
* Monthly opening price intensity
* Average daily return comparison
* Key KPIs such as last closing price and moving averages

### Dashboard 2: Portfolio & Trading Activity Analysis

* Portfolio allocation by stock
* Buy vs Sell distribution
* Investment value by stock
* Trading activity over time
* Transaction-level details

### Dashboard 3: Profitability & Portfolio Health

* Net profit or loss by stock
* Total trades executed
* Portfolio-level P&L
* Invested vs market value trends

### Dashboard 4: Risk & Sub-Sector Performance Insights

* Risk vs return analysis by sub-sector
* Average cumulative return by sub-sector
* Volatility indicators
* Last ETL snapshot status

---

## Validation & Testing

The project includes:

* File existence and schema validation
* Data integrity and null checks
* Price, volume, and return sanity checks
* Validation of extreme return thresholds
* Cross-verification between Pandas and PySpark outputs
* Dashboard accuracy validation against processed datasets

---

## Technologies Used

* Python (Pandas, NumPy)
* Apache Airflow
* Azure Databricks (PySpark)
* Power BI
* CSV-based data sources

---

## Key Learning Outcomes

* End-to-end data engineering pipeline design
* Time-series analysis and financial metrics computation
* Workflow automation and monitoring with Airflow
* Distributed analytics using PySpark
* Professional dashboard design using Power BI

---
