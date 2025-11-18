🛒 Ecommerce Data Engineering Platform
Modern End-to-End Data Engineering Project Using Airflow, Kafka, Postgres, Docker & Superset

This project demonstrates a production-grade data engineering platform designed to process, transform, and analyze ecommerce data. It includes batch ingestion, real-time streaming pipelines, orchestration, data quality checks, a dimensional data warehouse, and a Business Intelligence dashboard built using Apache Superset.

🚀 Features
✔ Batch Data Pipelines (Airflow)

Ingest raw CSV datasets (Olist ecommerce dataset)

Load into staging tables

Transform into a star schema (fact + dimensions)

Build aggregated DW tables and analytical views

✔ Real-time Streaming (Kafka)

Produce order & payment events using Kafka producers

Consume events into Postgres staging tables

Mimics real-time ecommerce activity

✔ Data Warehouse (Postgres)

fact_orders

dim_customers

dim_products

dim_sellers

(optional) dim_date, dim_geolocation

Optimized for BI and analytics workloads.

✔ Data Quality Checks (Airflow DQ DAG)

Row count validation

Null checks

Foreign key integrity checks

Automated DQ run after DW build using ExternalTaskSensor

✔ Business Intelligence Dashboard (Superset)

Revenue trends

Sales & order insights

Delivery performance

AOV (Average Order Value)

Executive-level KPIs

Exported YAML dashboard + chart definitions included



📁 Project Structure
```
ecommerce-data-engineering-platform/
│
├── dags/
│   ├── build_olist_dw.py
│   ├── ingest_olist_staging.py
│   ├── ingest_orders_csv.py
│   ├── kafka_produce_orders.py
│   ├── kafka_produce_payments.py
│   ├── kafka_consume_olist.py
│   ├── kafka_consume_olist_to_staging.py
│   ├── dq_olist_dw.py
│
├── sql/
│   ├── create_dw_views.sql
│   ├── create_stg_orders.sql
│
├── dashboards/
│   └── superset/
│       ├── metadata.yaml
│       ├── EXECUTIVE_DASHBOARD.yaml
│       ├── charts/
│           ├── Daily_Monthly_Revenue_Trend.yaml
│           ├── Total_Sales_and_Orders_Trend.yaml
│           ├── Delivery_Status_Breakdown.yaml
│           ├── Total_Revenue.yaml
│           ├── Average_Order_Value.yaml
│           ├── Monthly_Revenue_Trend.yaml
│
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md
```

🧱 Architecture Diagram
```
          +--------------------------+
          |     Raw CSV Files        |
          +------------+-------------+
                       |
                [Airflow Ingestion]
                       |
                       v
         +-------------+--------------+
         |        Postgres STG        |
         +-------------+--------------+
                       |
                [DW Build DAG]
                       |
                       v
       +---------------+----------------+ 
       |           Data Warehouse       |
       | (fact_orders, dim tables...)   |
       +---------------+----------------+
                       |
                [DQ DAG Runs]
                       |
                       v
         +-------------+--------------+
         |       Apache Superset      |
         |   BI Dashboard & Analytics |
         +----------------------------+

   Streaming Layer:
       Kafka Producers --> Kafka Topics --> Kafka Consumers --> STG
```
📊 Superset Dashboard (Screenshots)
Daily & Monthly Revenue Trend

<img width="477" height="331" alt="image" src="https://github.com/user-attachments/assets/758691dd-496f-4e20-91d2-8cf31ea929c9" />





