from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

# Adjust if you used a different connection id
POSTGRES_CONN_ID = "dw_postgres"

# IMPORTANT: This must match the dag_id of your DW DAG
# (check inside build_olist_dw.py for dag_id)
BUILD_DW_DAG_ID = "build_olist_dw"

default_args = {
    "owner": "koutilya",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_row_counts(**context):
    """
    Ensure core DW tables are not empty (and have a reasonable minimum size).
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # table_name -> minimum_rows_expected
    tables_min_rows = {
        "dw.fact_orders": 1,
        "dw.dim_customers": 1,
        "dw.dim_products": 1,
        "dw.dim_sellers": 1,
    }

    for table, min_rows in tables_min_rows.items():
        records = hook.get_first(f"SELECT COUNT(*) FROM {table};")
        if records is None:
            raise AirflowException(f"Row count check failed: {table} returned no result")

        row_count = records[0]
        if row_count < min_rows:
            raise AirflowException(
                f"Row count check failed for {table}: "
                f"expected >= {min_rows}, got {row_count}"
            )


def check_nulls(**context):
    """
    Ensure key columns in DW tables do not contain NULLs.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Each item: (table, column)
    critical_columns = [
        ("dw.dim_customers", "customer_sk"),
        ("dw.dim_customers", "customer_id"),
        ("dw.dim_products", "product_sk"),
        ("dw.dim_products", "product_id"),
        ("dw.dim_sellers", "seller_sk"),
        ("dw.dim_sellers", "seller_id"),
        ("dw.fact_orders", "order_sk"),
        ("dw.fact_orders", "order_id"),
        ("dw.fact_orders", "customer_sk"),
        ("dw.fact_orders", "product_sk"),
        ("dw.fact_orders", "seller_sk"),
    ]

    for table, column in critical_columns:
        records = hook.get_first(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL;"
        )
        null_count = records[0]
        if null_count > 0:
            raise AirflowException(
                f"Null check failed: {table}.{column} has {null_count} NULL values"
            )


def check_fk_integrity(**context):
    """
    Ensure fact_orders has valid foreign keys to dimensions
    (no orphan fact rows).
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    fk_checks = {
        "customer_fk": """
            SELECT COUNT(*) 
            FROM dw.fact_orders f
            LEFT JOIN dw.dim_customers c ON f.customer_sk = c.customer_sk
            WHERE c.customer_sk IS NULL;
        """,
        "product_fk": """
            SELECT COUNT(*) 
            FROM dw.fact_orders f
            LEFT JOIN dw.dim_products p ON f.product_sk = p.product_sk
            WHERE p.product_sk IS NULL;
        """,
        "seller_fk": """
            SELECT COUNT(*) 
            FROM dw.fact_orders f
            LEFT JOIN dw.dim_sellers s ON f.seller_sk = s.seller_sk
            WHERE s.seller_sk IS NULL;
        """,
    }

    for name, query in fk_checks.items():
        records = hook.get_first(query)
        orphan_count = records[0]
        if orphan_count > 0:
            raise AirflowException(
                f"FK integrity check '{name}' failed: {orphan_count} orphan rows found"
            )


with DAG(
    dag_id="dq_olist_dw",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # should match or follow build_olist_dw frequency
    catchup=False,
    tags=["dq", "olist", "dw"],
    description="Data quality checks for Olist DW (row counts, nulls, FK integrity)",
) as dag:

    from airflow.utils.state import DagRunState

    wait_for_build_dw = ExternalTaskSensor(
    task_id="wait_for_build_olist_dw",
    external_dag_id=BUILD_DW_DAG_ID,   # must match build_olist_dw dag_id
    external_task_id=None,             # wait for the whole DAG run
    allowed_states=[DagRunState.SUCCESS],
    failed_states=[DagRunState.FAILED],
    poke_interval=60,                  # check every 60 seconds
    timeout=60 * 60 * 2,               # fail after 2 hours
    mode="reschedule",
)


    dq_row_counts = PythonOperator(
        task_id="dq_row_counts",
        python_callable=check_row_counts,
    )

    dq_nulls = PythonOperator(
        task_id="dq_nulls",
        python_callable=check_nulls,
    )

    dq_fk_integrity = PythonOperator(
        task_id="dq_fk_integrity",
        python_callable=check_fk_integrity,
    )

    # ✅ Dependency chain: only run DQ after build_olist_dw succeeded
    wait_for_build_dw >> dq_row_counts >> dq_nulls >> dq_fk_integrity
