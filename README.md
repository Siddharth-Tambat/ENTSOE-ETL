# ⚡ ENTSO-E API ETL Pipeline

Automated data pipeline to extract, transform, and load ENTSO‑E API data into Azure Postgres DB and Azure Data Lake Storage Gen2, orchestrated with Apache Airflow.

---

## Table of Contents
- Introduction
- Architecture
- Project Setup
- Folder Structure

---

## 1. Introduction to ENTSO-E and its API

[ENTSO-E (European Network of Transmission System Operators for Electricity)](https://www.entsoe.eu/) is the official body of European transmission operators.  
They operate the **Transparency Platform**, which provides open data on electricity markets, transmission, generation, consumption, balancing, and reserves.  

The Transparency Platform exposes this data via a **public API** (XML-based, IEC-CIM standard), which includes datasets like:

- Load, Generation, and Transmission data  
- Day-ahead and Intraday prices  
- Balancing data: including **Volumes and Prices of Contracted Reserves**  

- 🔑 Access to the API requires an **ENTSO-E Transparency API Key** [(get *securityToken*)](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token)
- 📜 [API Documentation](https://transparencyplatform.zendesk.com/hc/en-us/articles/15692855254548-Sitemap-for-Restful-API-Integration)
---

## 2. Architecture Overview

- **Orchestration**: [Apache Airflow] [(running in Docker)](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- **Data Source**: ENTSO-E Transparency API (XML responses)  
- **Processing**: Python + Pandas (parsing XML into DataFrames)  
- **Data Store**: Azure Database for PostgreSQL  
- **Data Lake**: Azure Data Lake Storage Gen2 (ADLS2), storing **partitioned Parquet** files
- **Data Warehouse**: Azure Synapse Analytics
- **File Format**: Parquet (optimized for analytics; supported by Synapse, KQL, Databricks, Spark, Power BI)  
- **Deployment & CI/CD**: GitHub Actions syncing DAGs into the Airflow VM  

---

## 3. 🚀 How to Set Up and Run the ETL Pipeline



---

## 4. Folder Structure

```bash
entsoe-etl/
│
├── dags/
│   ├── germany_bnc_reserves.py    # Main Airflow DAG
│   └── bnc_parser.py              # XML parsing logic
│
├── .github/
│   └── workflows/
│       └── deploy_dags.yml        # CI/CD workflow for DAG sync
│
├── sql/
│   └── schema.sql                 # Postgres schema for germany_bnc_reserves
│
├── docs/
│   └── README.md                  # Project documentation
│
└── requirements.txt               # Python dependencies
```
