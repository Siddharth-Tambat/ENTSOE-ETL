# ⚡ ENTSO-E ETL Pipeline

Automated data pipeline to extract, transform, and load ENTSO‑E API data into Azure Postgres DB and Azure Data Lake Storage Gen2, orchestrated with Apache Airflow.

---

## Table of Contents
- Introduction
- Features
- Architecture
- Prerequisites
- Folder Structure
- Setup and Run
- Visuals

---

## 1. Introduction

[ENTSO-E (European Network of Transmission System Operators for Electricity)](https://www.entsoe.eu/) is the official body of European transmission operators.  
They operate the **Transparency Platform**, which provides open data on electricity markets, transmission, generation, consumption, balancing, and reserves.  

The Transparency Platform exposes this data via a **public API** (XML-based, IEC-CIM standard), which includes datasets like - Load, Generation, and Transmission data.
In this Project we'll be extracting data from the below two API for Germany region:

- Day-ahead and Intraday prices  
- Balancing data: including **Volumes and Prices of Contracted Reserves**

Important Info:
- 🔑 Access to the API requires an **ENTSO-E Transparency API Key** 
- 📜 [API Documentation](https://transparencyplatform.zendesk.com/hc/en-us/articles/15692855254548-Sitemap-for-Restful-API-Integration)
  
---
## 2. Features

- Scheduled, on‑demand and backfill runs with Apache Airflow.
- XML parsing tailored for ENTSO‑E API responses.
- Indexed Postgres schema for fast queries.
- Partitioned Parquet in Data Lake for analytics and ML forecasting.

---

## 3. Architecture

- **Orchestration**: [Apache Airflow](https://airflow.apache.org/)
- **Data Source**: ENTSO-E Transparency API (XML responses)  
- **Processing**: Python + Pandas (parsing XML into DataFrames)  
- **Data Store**: Azure Database for PostgreSQL  
- **Data Lake**: Azure Data Lake Storage Gen2 (ADLS2), storing **partitioned Parquet** files
- **Data Warehouse**: Azure Synapse Analytics
- **File Format**: Parquet (optimized for analytics; supported by Synapse, KQL, Databricks, Spark, Power BI)  
- **Deployment & CI/CD**: GitHub Actions syncing DAGs into the Airflow VM  

---

## 4. Prerequisites

- ENTOS-E API Key [(get *securityToken*)](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token)
- An Airflow deployment [(running in Docker)](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- Azure Postgres instance and credentials
- Azure Data Lake Storage Gen2 account and credentials

---

## 5. Folder Structure

```bash
entsoe-etl/
│
├── dags/
│   ├── germany_bnc_reserves.py    # BNC Airflow DAG
│   └── bnc_parser.py              # XML parsing logic
│   └── germany_energy_prices.py   # Energy Prices DAG
│   └── energy_prices_parser.py    # XML parsing logic
│   └── __init__.py    
│
├── .github/
│   └── workflows/
│       └── deploy_dags.yml        # CI/CD workflow for DAG sync
│
├── sql/
│   └── schema.sql                 # Postgres schema for germany_bnc_reserves
│
├── docs/
│   └── pipeline_architecture.png
│   └── airflow_dag_graph.png
│   └── airflow_calendar.png
│   └── synapse_analytics.png
│   └── postgres_db.png
│
└── README.md                       # Project documentation
```

 ---

 ## 6. 🚀 Setup and Run

1. Clone the repository:
```bash
git clone https://github.com/Siddharth-Tambat/entsoe-etl.git
cd entsoe-etl
```
2. Ensure Airflow is running in Docker and DAGs are mounted in the dags/ folder.
3. In Airflow:
 - Add Variable: entsoe-api-key = <your_api_key>
 - Add Connections:
   * entsoe_pg → Postgres
   * azure_data_lake → ADLS Gen2
4. Create the Postgres schemas using the sql/schema.sql
5. Trigger the DAG:
```bash
   airflow dags trigger germany_bnc_reserves
```
6. Backfill historical data:
```bash
docker exec -it airflow-airflow-scheduler-1 bash
airflow dags backfill \
  -s {from_date} \
  -e {to_date} \
  germany_bnc_reserves
```

---

## 7. Visuals

### Pipeline Architecture
![Pipeline architecture](docs/pipeline_architecture.png)

### Airflow Views
<p float="left">
  <img src="docs/airflow_dag_graph.png" alt="Airflow DAG graph" width="49%" />
  <img src="docs/airflow_calendar.png" alt="Airflow calendar" width="49%" />
</p>

### Postgres DB
![Postgres DB](docs/postgres_db.png)

### Storage and Analytics
<p float="left">
  <img src="docs/adls2.png" alt="ADLS2 layout" width="49%" />
  <img src="docs/synapse_analytics.png" alt="Synapse Analytics view" width="49%" />
</p>
