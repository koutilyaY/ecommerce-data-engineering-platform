from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# All CSVs live in this folder inside the Airflow container
CSV_DIR = "/opt/airflow/data/raw"

def load_csv_to_table(table_name: str, csv_filename: str, columns: str) -> None:
    """
    Generic loader: truncates a table and bulk loads a CSV into it.
    columns is a comma-separated list of column names in the same order as the CSV.
    """
    hook = PostgresHook(postgres_conn_id="dw_postgres")

    # 1) Make sure the table is empty before loading (idempotent)
    hook.run(f"TRUNCATE TABLE public.{table_name};")

    copy_sql = f"""
        COPY public.{table_name} ({columns})
        FROM STDIN
        WITH (FORMAT csv, HEADER true);
    """

    csv_path = f"{CSV_DIR}/{csv_filename}"

    with open(csv_path, "r", encoding="utf-8") as f:
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
    dag_id="ingest_olist_staging",
    default_args=default_args,
    schedule_interval="@daily",  # later you can change to "@daily"
    catchup=False,
    description="Ingest Olist CSVs into Postgres staging tables",
) as dag:

    # ─────────────────────────────
    # ORDERS
    # ─────────────────────────────
    create_stg_orders = PostgresOperator(
        task_id="create_stg_orders",
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

    load_stg_orders = PythonOperator(
        task_id="load_stg_orders",
        python_callable=load_csv_to_table,
        op_kwargs={
            "table_name": "stg_orders",
            "csv_filename": "olist_orders_dataset.csv",
            "columns": (
                "order_id, "
                "customer_id, "
                "order_status, "
                "order_purchase_timestamp, "
                "order_approved_at, "
                "order_delivered_carrier_date, "
                "order_delivered_customer_date, "
                "order_estimated_delivery_date"
            ),
        },
    )

    # ─────────────────────────────
    # CUSTOMERS
    # ─────────────────────────────
    create_stg_customers = PostgresOperator(
        task_id="create_stg_customers",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS public.stg_customers (
                customer_id VARCHAR,
                customer_unique_id VARCHAR,
                customer_zip_code_prefix VARCHAR,
                customer_city VARCHAR,
                customer_state VARCHAR
            );
        """,
    )

    load_stg_customers = PythonOperator(
        task_id="load_stg_customers",
        python_callable=load_csv_to_table,
        op_kwargs={
            "table_name": "stg_customers",
            "csv_filename": "olist_customers_dataset.csv",
            "columns": (
                "customer_id, "
                "customer_unique_id, "
                "customer_zip_code_prefix, "
                "customer_city, "
                "customer_state"
            ),
        },
    )

    # ─────────────────────────────
    # ORDER ITEMS
    # ─────────────────────────────
    create_stg_order_items = PostgresOperator(
        task_id="create_stg_order_items",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS public.stg_order_items (
                order_id VARCHAR,
                order_item_id VARCHAR,
                product_id VARCHAR,
                seller_id VARCHAR,
                shipping_limit_date TIMESTAMP,
                price VARCHAR,
                freight_value VARCHAR
            );
        """,
    )

    load_stg_order_items = PythonOperator(
        task_id="load_stg_order_items",
        python_callable=load_csv_to_table,
        op_kwargs={
            "table_name": "stg_order_items",
            "csv_filename": "olist_order_items_dataset.csv",
            "columns": (
                "order_id, "
                "order_item_id, "
                "product_id, "
                "seller_id, "
                "shipping_limit_date, "
                "price, "
                "freight_value"
            ),
        },
    )

    # ─────────────────────────────
    # ORDER PAYMENTS
    # ─────────────────────────────
    create_stg_order_payments = PostgresOperator(
        task_id="create_stg_order_payments",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS public.stg_order_payments (
                order_id VARCHAR,
                payment_sequential VARCHAR,
                payment_type VARCHAR,
                payment_installments VARCHAR,
                payment_value VARCHAR
            );
        """,
    )

    load_stg_order_payments = PythonOperator(
        task_id="load_stg_order_payments",
        python_callable=load_csv_to_table,
        op_kwargs={
            "table_name": "stg_order_payments",
            "csv_filename": "olist_order_payments_dataset.csv",
            "columns": (
                "order_id, "
                "payment_sequential, "
                "payment_type, "
                "payment_installments, "
                "payment_value"
            ),
        },
    )

    # ─────────────────────────────
    # ORDER REVIEWS
    # ─────────────────────────────
    create_stg_order_reviews = PostgresOperator(
        task_id="create_stg_order_reviews",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS public.stg_order_reviews (
                review_id VARCHAR,
                order_id VARCHAR,
                review_score VARCHAR,
                review_comment_title VARCHAR,
                review_comment_message VARCHAR,
                review_creation_date TIMESTAMP,
                review_answer_timestamp TIMESTAMP
            );
        """,
    )

    load_stg_order_reviews = PythonOperator(
        task_id="load_stg_order_reviews",
        python_callable=load_csv_to_table,
        op_kwargs={
            "table_name": "stg_order_reviews",
            "csv_filename": "olist_order_reviews_dataset.csv",
            "columns": (
                "review_id, "
                "order_id, "
                "review_score, "
                "review_comment_title, "
                "review_comment_message, "
                "review_creation_date, "
                "review_answer_timestamp"
            ),
        },
    )

    # ─────────────────────────────
    # PRODUCTS
    # ─────────────────────────────
    create_stg_products = PostgresOperator(
        task_id="create_stg_products",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS public.stg_products (
                product_id VARCHAR,
                product_category_name VARCHAR,
                product_name_lenght VARCHAR,
                product_description_lenght VARCHAR,
                product_photos_qty VARCHAR,
                product_weight_g VARCHAR,
                product_length_cm VARCHAR,
                product_height_cm VARCHAR,
                product_width_cm VARCHAR
            );
        """,
    )

    load_stg_products = PythonOperator(
        task_id="load_stg_products",
        python_callable=load_csv_to_table,
        op_kwargs={
            "table_name": "stg_products",
            "csv_filename": "olist_products_dataset.csv",
            "columns": (
                "product_id, "
                "product_category_name, "
                "product_name_lenght, "
                "product_description_lenght, "
                "product_photos_qty, "
                "product_weight_g, "
                "product_length_cm, "
                "product_height_cm, "
                "product_width_cm"
            ),
        },
    )

    # ─────────────────────────────
    # SELLERS
    # ─────────────────────────────
    create_stg_sellers = PostgresOperator(
        task_id="create_stg_sellers",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS public.stg_sellers (
                seller_id VARCHAR,
                seller_zip_code_prefix VARCHAR,
                seller_city VARCHAR,
                seller_state VARCHAR
            );
        """,
    )

    load_stg_sellers = PythonOperator(
        task_id="load_stg_sellers",
        python_callable=load_csv_to_table,
        op_kwargs={
            "table_name": "stg_sellers",
            "csv_filename": "olist_sellers_dataset.csv",
            "columns": (
                "seller_id, "
                "seller_zip_code_prefix, "
                "seller_city, "
                "seller_state"
            ),
        },
    )

    # ─────────────────────────────
    # Dependencies: each create -> corresponding load
    # (All groups can run in parallel)
    # ─────────────────────────────
    create_stg_orders >> load_stg_orders
    create_stg_customers >> load_stg_customers
    create_stg_order_items >> load_stg_order_items
    create_stg_order_payments >> load_stg_order_payments
    create_stg_order_reviews >> load_stg_order_reviews
    create_stg_products >> load_stg_products
    create_stg_sellers >> load_stg_sellers
