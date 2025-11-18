from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "koutilya",
    "start_date": datetime(2025, 1, 1),
}


def dq_check_fact_orders():
    """
    Data quality checks on dw.fact_orders:

    1) Table has rows
    2) customer_sk NULL ratio is below a threshold
    3) No negative numeric values for totals
    4) No future order_purchase_timestamp
    5) Primary key (order_id) is unique
    6) customer_sk values actually exist in dw.dim_customers
    """

    hook = PostgresHook(postgres_conn_id="dw_postgres")

    # ---- CONFIGURABLE THRESHOLDS ----
    NULL_CUSTOMER_MAX_RATIO = 0.05   # e.g. allow up to 5% NULL customer_sk
    MAX_BAD_NUMERIC_ROWS = 0         # how many rows with bad numeric values allowed
    MAX_FUTURE_DATES = 0             # how many rows with future purchase timestamps allowed
    MAX_DUP_PK_ROWS = 0              # how many duplicate order_id rows allowed
    MAX_MISSING_DIM_ROWS = 0         # how many rows allowed with missing dim customers

    # 1) Check row count > 0
    row_count = hook.get_first("SELECT COUNT(*) FROM dw.fact_orders;")[0]
    if row_count < 1:
        raise ValueError("DQ check failed: dw.fact_orders has 0 rows.")

    # 2) Null ratio for customer_sk
    null_customer = hook.get_first(
        "SELECT COUNT(*) FROM dw.fact_orders WHERE customer_sk IS NULL;"
    )[0]

    customer_null_ratio = null_customer / row_count

    if customer_null_ratio > NULL_CUSTOMER_MAX_RATIO:
        raise ValueError(
            f"DQ check failed: customer_sk NULL ratio too high "
            f"({customer_null_ratio:.2%} > {NULL_CUSTOMER_MAX_RATIO:.2%})."
        )

    # 3) No negative numeric values for totals
    bad_numeric_rows = hook.get_first(
        """
        SELECT COUNT(*)
        FROM dw.fact_orders
        WHERE
            total_items < 0
            OR gross_value < 0
            OR freight_value < 0
            OR payment_value < 0;
        """
    )[0]

    if bad_numeric_rows > MAX_BAD_NUMERIC_ROWS:
        raise ValueError(
            f"DQ check failed: found {bad_numeric_rows} rows with "
            "negative total_items, gross_value, freight_value, or payment_value."
        )

    # 4) No future purchase timestamps
    future_dates = hook.get_first(
        """
        SELECT COUNT(*)
        FROM dw.fact_orders
        WHERE order_purchase_timestamp::date > CURRENT_DATE;
        """
    )[0]

    if future_dates > MAX_FUTURE_DATES:
        raise ValueError(
            f"DQ check failed: found {future_dates} rows with "
            "order_purchase_timestamp in the future."
        )

    # 5) Primary key uniqueness (order_id)
    duplicate_pk_rows = hook.get_first(
        """
        SELECT COUNT(*)
        FROM (
            SELECT order_id, COUNT(*) AS cnt
            FROM dw.fact_orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
        ) dup;
        """
    )[0]

    if duplicate_pk_rows > MAX_DUP_PK_ROWS:
        raise ValueError(
            f"DQ check failed: found {duplicate_pk_rows} order_id values with duplicates."
        )

    # 6) Basic referential integrity: customer_sk must exist in dim_customers
    missing_customers = hook.get_first(
        """
        SELECT COUNT(*)
        FROM dw.fact_orders fo
        LEFT JOIN dw.dim_customers dc
            ON fo.customer_sk = dc.customer_sk
        WHERE fo.customer_sk IS NOT NULL
          AND dc.customer_sk IS NULL;
        """
    )[0]

    if missing_customers > MAX_MISSING_DIM_ROWS:
        raise ValueError(
            f"DQ check failed: found {missing_customers} fact rows with "
            "customer_sk not present in dw.dim_customers."
        )

    # If we got here, all checks passed
    print("DQ checks passed successfully for dw.fact_orders.")


with DAG(
    dag_id="build_olist_dw",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Build Olist star schema in dw schema",
) as dag:

    # 0) WAIT FOR STAGING DAG
    wait_for_staging = ExternalTaskSensor(
        task_id="wait_for_ingest_olist_staging",
        external_dag_id="ingest_olist_staging",
        external_task_id=None,  # wait for whole DAG success
        mode="reschedule",
        poke_interval=60,       # check every 60s
        timeout=60 * 60,        # give up after 1h
    )

    # 1) Create DW schema
    create_dw_schema = PostgresOperator(
        task_id="create_dw_schema",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE SCHEMA IF NOT EXISTS dw;
        """,
    )

    # 2) DIM_CUSTOMERS
    dim_customers = PostgresOperator(
        task_id="build_dim_customers",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS dw.dim_customers (
                customer_sk SERIAL PRIMARY KEY,
                customer_id VARCHAR UNIQUE,
                customer_unique_id VARCHAR,
                customer_city VARCHAR,
                customer_state VARCHAR
            );

            TRUNCATE TABLE dw.dim_customers;

            INSERT INTO dw.dim_customers (
                customer_id,
                customer_unique_id,
                customer_city,
                customer_state
            )
            SELECT DISTINCT
                customer_id,
                customer_unique_id,
                customer_city,
                customer_state
            FROM public.stg_customers;
        """,
    )

    # 3) DIM_PRODUCTS
    dim_products = PostgresOperator(
        task_id="build_dim_products",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS dw.dim_products (
                product_sk SERIAL PRIMARY KEY,
                product_id VARCHAR UNIQUE,
                product_category_name VARCHAR,
                product_name_lenght VARCHAR,
                product_description_lenght VARCHAR,
                product_photos_qty VARCHAR,
                product_weight_g VARCHAR,
                product_length_cm VARCHAR,
                product_height_cm VARCHAR,
                product_width_cm VARCHAR
            );

            TRUNCATE TABLE dw.dim_products;

            INSERT INTO dw.dim_products (
                product_id,
                product_category_name,
                product_name_lenght,
                product_description_lenght,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm
            )
            SELECT DISTINCT
                product_id,
                product_category_name,
                product_name_lenght,
                product_description_lenght,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm
            FROM public.stg_products;
        """,
    )

    # 4) DIM_SELLERS
    dim_sellers = PostgresOperator(
        task_id="build_dim_sellers",
        postgres_conn_id="dw_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS dw.dim_sellers (
                seller_sk SERIAL PRIMARY KEY,
                seller_id VARCHAR UNIQUE,
                seller_zip_code_prefix VARCHAR,
                seller_city VARCHAR,
                seller_state VARCHAR
            );

            TRUNCATE TABLE dw.dim_sellers;

            INSERT INTO dw.dim_sellers (
                seller_id,
                seller_zip_code_prefix,
                seller_city,
                seller_state
            )
            SELECT DISTINCT
                seller_id,
                seller_zip_code_prefix,
                seller_city,
                seller_state
            FROM public.stg_sellers;
        """,
    )

    # 5) FACT_ORDERS
        # 5) FACT_ORDERS
    fact_orders = PostgresOperator(
        task_id="build_fact_orders",
        postgres_conn_id="dw_postgres",
        sql="""
            -- 1) Create the table once if it doesn't exist
            CREATE TABLE IF NOT EXISTS dw.fact_orders (
                order_id VARCHAR PRIMARY KEY,
                customer_sk INTEGER,
                order_status VARCHAR,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP,
                delivery_delay_days INTEGER,
                total_items INTEGER,
                gross_value NUMERIC(10,2),
                freight_value NUMERIC(10,2),
                payment_value NUMERIC(10,2),
                review_score INTEGER
            );

            -- 2) Empty the table before reloading (keeps views intact)
            TRUNCATE TABLE dw.fact_orders;

            -- 3) Re-populate with fresh data from staging + dim_customers
            INSERT INTO dw.fact_orders (
                order_id,
                customer_sk,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_customer_date,
                order_estimated_delivery_date,
                delivery_delay_days,
                total_items,
                gross_value,
                freight_value,
                payment_value,
                review_score
            )
            SELECT
                o.order_id,
                dc.customer_sk,
                o.order_status,
                o.order_purchase_timestamp,
                o.order_approved_at,
                o.order_delivered_customer_date,
                o.order_estimated_delivery_date,
                CASE
                    WHEN o.order_delivered_customer_date IS NOT NULL
                         AND o.order_estimated_delivery_date IS NOT NULL
                    THEN (o.order_delivered_customer_date::date - o.order_estimated_delivery_date::date)
                    ELSE NULL
                END AS delivery_delay_days,
                COALESCE(oi.total_items, 0) AS total_items,
                COALESCE(oi.gross_value, 0)::NUMERIC(10,2) AS gross_value,
                COALESCE(oi.freight_value, 0)::NUMERIC(10,2) AS freight_value,
                COALESCE(op.payment_value, 0)::NUMERIC(10,2) AS payment_value,
                COALESCE(orv.review_score, 0)::INT AS review_score
            FROM public.stg_orders o
            LEFT JOIN (
                SELECT
                    order_id,
                    COUNT(*) AS total_items,
                    SUM(price::NUMERIC) AS gross_value,
                    SUM(freight_value::NUMERIC) AS freight_value
                FROM public.stg_order_items
                GROUP BY order_id
            ) oi ON o.order_id = oi.order_id
            LEFT JOIN (
                SELECT
                    order_id,
                    SUM(payment_value::NUMERIC) AS payment_value
                FROM public.stg_order_payments
                GROUP BY order_id
            ) op ON o.order_id = op.order_id
            LEFT JOIN (
                SELECT
                    order_id,
                    AVG(review_score::NUMERIC)::INT AS review_score
                FROM public.stg_order_reviews
                GROUP BY order_id
            ) orv ON o.order_id = orv.order_id
            LEFT JOIN dw.dim_customers dc
                ON o.customer_id = dc.customer_id;
        """,
    )


    # 6) DATA QUALITY TASK
    dq_fact_orders = PythonOperator(
        task_id="dq_fact_orders",
        python_callable=dq_check_fact_orders,
    )

    # Dependencies: wait -> schema -> dims -> fact -> dq
    wait_for_staging >> create_dw_schema
    create_dw_schema >> [dim_customers, dim_products, dim_sellers]
    [dim_customers, dim_products, dim_sellers] >> fact_orders
    fact_orders >> dq_fact_orders
