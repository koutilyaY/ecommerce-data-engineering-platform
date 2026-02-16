End-to-End Ecommerce Analytics & Data Platform

Production-style data platform that ingests batch and streaming ecommerce data, transforms it using an ELT architecture into an analytics-ready warehouse, enforces automated data quality validation, and delivers executive dashboards for business decision-making.

🎯 Business Problem
Challenges Faced by Ecommerce Organizations

Fragmented data across multiple transactional systems

Inconsistent KPI definitions across teams

Limited visibility into revenue and operational performance

Unvalidated data leading to unreliable reporting

Absence of near real-time analytics capabilities
Business Value Delivered

This platform addresses these challenges by:

Centralizing batch and streaming data into a unified warehouse

Standardizing KPI definitions through dimensional modeling

Enforcing automated data quality validation

Enabling executive-level revenue and operational dashboards

Supporting near real-time decision-making workflows


## 🚀 Features

- **Batch Data Pipelines (Airflow)**
  - Ingest raw CSV datasets (Olist ecommerce dataset)
  - Load into staging tables
  - Transform into a star schema (fact + dimensions)
  - Build aggregated DW tables and analytical views

- **Real-time Streaming (Kafka)**
  - Produce order & payment events using Kafka producers
  - Consume events into Postgres staging tables
  - Mimics real-time ecommerce activity

- **Data Warehouse (Postgres)**
  - fact_orders, dim_customers, dim_products, dim_sellers
  - (optional) dim_date, dim_geolocation

- **Data Quality Checks (Airflow DQ DAG)**
  - Row count, null checks, FK checks
  - `ExternalTaskSensor` so DQ runs after `build_olist_dw`

- **Business Intelligence Dashboard (Superset)**
  - Revenue trends, orders, delivery status, AOV, executive KPIs




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
CSV Files (Batch)         Kafka Producers (Streaming)
        │                           │
        ▼                           ▼
     Airflow DAGs            Kafka Topics
        │                           │
        ▼                           ▼
  PostgreSQL Staging  ←  Kafka Consumers
        │
        ▼
  Data Warehouse (Star Schema)
        │
        ▼
  Data Quality DAG
        │
        ▼
  Apache Superset Dashboards

```
📊 Superset Dashboard (Screenshots)
Daily & Monthly Revenue Trend

<img width="477" height="331" alt="image" src="https://github.com/user-attachments/assets/758691dd-496f-4e20-91d2-8cf31ea929c9" />

Total Sales & Orders Trend

<img width="484" height="328" alt="image" src="https://github.com/user-attachments/assets/a519e069-3916-451b-bc97-08ea9387a137" />

Delivery Status Breakdown

<img width="483" height="323" alt="image" src="https://github.com/user-attachments/assets/d4c82e3c-c341-423c-9093-53798302c0b4" />

Total Revenue

<img width="481" height="321" alt="image" src="https://github.com/user-attachments/assets/fa0e1517-032a-4f3f-974c-9549f9a3fe76" />

Average Order Value (AOV)

<img width="483" height="320" alt="image" src="https://github.com/user-attachments/assets/133ab35c-5525-4331-af3d-bfd40b6c4c4c" />

Monthly Revenue Trend

<img width="480" height="328" alt="image" src="https://github.com/user-attachments/assets/1bc65055-0aff-47ce-83f2-a7be6d980d5f" />


⚙️ How to Run the Project
1. Start Docker
   docker-compose up -d
2. Open Airflow
   http://localhost:8080
   
Trigger in this order:

ingest_orders_csv

ingest_olist_staging

build_olist_dw

dq_olist_dw

3. Open Superset
   http://localhost:8088
Settings → Import → Select metadata + dashboard YAML











