from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

# Airflow connection id pointing to ecommerce_dw database
POSTGRES_CONN_ID = "dw_postgres"

# Must match the dag_id of your DW build DAG
BUILD_DW_DAG_ID = "build_olist_dw"

default_args = {
    "owner": "koutilya",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_row_counts(**context):
    """
    Ensure core DW tables are not empty.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

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
    Matches the actual schema of your DW.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Each item: (table, column)
    critical_columns = [
        # Dimension surrogate & business keys
        ("dw.dim_customers", "customer_sk"),
        ("dw.dim_customers", "customer_id"),
        ("dw.dim_products", "product_sk"),
        ("dw.dim_products", "product_id"),
        ("dw.dim_sellers", "seller_sk"),
        ("dw.dim_sellers", "seller_id"),

        # Fact table natural key + foreign key
        ("dw.fact_orders", "order_id"),
        ("dw.fact_orders", "customer_sk"),
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
    Ensure fact_orders has valid foreign keys to dimensions.
    For your current schema, only customer_sk is a FK in fact_orders.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Check: every customer_sk in fact_orders exists in dim_customers
    query_customer_fk = """
        SELECT COUNT(*) 
        FROM dw.fact_orders f
        LEFT JOIN dw.dim_customers c 
          ON f.customer_sk = c.customer_sk
        WHERE c.customer_sk IS NULL;
    """

    records = hook.get_first(query_customer_fk)
    orphan_count = records[0]
    if orphan_count > 0:
        raise AirflowException(
            f"FK integrity check failed for customer_sk: "
            f"{orphan_count} orphan rows found in dw.fact_orders"
        )


with DAG(
    dag_id="dq_olist_dw",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # or None if you want to trigger manually
    catchup=False,
    tags=["dq", "olist", "dw"],
    description="Data quality checks for Olist DW (row counts, nulls, FK integrity)",
) as dag:

    # Wait for the DW build DAG to complete successfully
    wait_for_build_dw = ExternalTaskSensor(
        task_id="wait_for_build_olist_dw",
        external_dag_id=BUILD_DW_DAG_ID,   # build_olist_dw dag_id
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

    # Dependency chain
    wait_for_build_dw >> dq_row_counts >> dq_nulls >> dq_fk_integrity
