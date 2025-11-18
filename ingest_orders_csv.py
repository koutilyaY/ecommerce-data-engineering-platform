from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_PATH = "/opt/airflow/data/raw/olist_orders_dataset.csv"

def load_orders_from_csv():
    """
    Bulk load the Olist orders CSV into the stg_orders table in Postgres.
    We truncate the table first so the DAG is idempotent.
    """
    hook = PostgresHook(postgres_conn_id="dw_postgres")

    # 1) Make sure the table is empty before loading
    hook.run("TRUNCATE TABLE public.stg_orders;")

    # 2) Bulk copy from CSV
    copy_sql = """
        COPY public.stg_orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        FROM STDIN
        WITH (FORMAT csv, HEADER true);
    """

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        cursor.close()
        conn.close()

default_args = {
    "owner": "koutilya",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="ingest_orders_csv",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Load Olist orders CSV into Postgres staging table",
) as dag:

    create_table = PostgresOperator(
        task_id="create_stg_orders_table",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS public.stg_orders (
                order_id VARCHAR PRIMARY KEY,
                customer_id VARCHAR,
                order_status VARCHAR,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
            );
        """,
    )

    load_csv = PythonOperator(
        task_id="load_orders_from_csv",
        python_callable=load_orders_from_csv,
    )

    create_table >> load_csv
