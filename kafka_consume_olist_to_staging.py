from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from confluent_kafka import Consumer


# --- CONFIG ------------------------------------------------------------------

# Kafka inside Docker network
KAFKA_BOOTSTRAP = "kafka:9092"

# Topics (change if you used dotted names instead)
ORDERS_TOPIC = "olist_orders_raw"
PAYMENTS_TOPIC = "olist_payments_raw"

# Airflow connection id for Postgres
POSTGRES_CONN_ID = "dw_postgres"


# --- HELPERS -----------------------------------------------------------------

def _build_consumer(group_id: str, topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])
    return consumer


def _consume_orders_to_staging(**context):
    """
    Read messages from ORDERS_TOPIC and insert into stg_orders.
    Assumes JSON with the standard Olist orders schema.
    """
    consumer = _build_consumer("stg_orders_consumer", ORDERS_TOPIC)
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    rows = []
    batch_size = 1000
    idle_polls = 0
    max_idle_polls = 5

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            idle_polls += 1
            if idle_polls >= max_idle_polls:
                break
            continue

        if msg.error():
            raise Exception(msg.error())

        idle_polls = 0

        payload_str = msg.value().decode("utf-8")
        try:
            record = json.loads(payload_str)
        except json.JSONDecodeError:
            print(f"Skipping non-JSON message: {payload_str}")
            continue

        # Map JSON fields to the staging table columns
        rows.append(
            {
                "order_id": record.get("order_id"),
                "customer_id": record.get("customer_id"),
                "order_status": record.get("order_status"),
                "order_purchase_timestamp": record.get("order_purchase_timestamp"),
                "order_approved_at": record.get("order_approved_at"),
                "order_delivered_carrier_date": record.get(
                    "order_delivered_carrier_date"
                ),
                "order_delivered_customer_date": record.get(
                    "order_delivered_customer_date"
                ),
                "order_estimated_delivery_date": record.get(
                    "order_estimated_delivery_date"
                ),
            }
        )

        if len(rows) >= batch_size:
            _insert_orders(pg, rows)
            rows = []

    if rows:
        _insert_orders(pg, rows)

    consumer.close()


def _insert_orders(pg, rows):
    if not rows:
        return

    sql = """
        INSERT INTO public.stg_orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        VALUES (
            %(order_id)s,
            %(customer_id)s,
            %(order_status)s,
            NULLIF(%(order_purchase_timestamp)s, '')::timestamp,
            NULLIF(%(order_approved_at)s, '')::timestamp,
            NULLIF(%(order_delivered_carrier_date)s, '')::timestamp,
            NULLIF(%(order_delivered_customer_date)s, '')::timestamp,
            NULLIF(%(order_estimated_delivery_date)s, '')::timestamp
        )
        ON CONFLICT (order_id) DO UPDATE
        SET
            customer_id = EXCLUDED.customer_id,
            order_status = EXCLUDED.order_status,
            order_purchase_timestamp = EXCLUDED.order_purchase_timestamp,
            order_approved_at = EXCLUDED.order_approved_at,
            order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
            order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
            order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date;
    """

    # insert one row (dict) at a time
    for row in rows:
        pg.run(sql, parameters=row)




def _consume_payments_to_staging(**context):
    """
    Read messages from PAYMENTS_TOPIC and insert into stg_order_payments.
    Assumes JSON with the standard Olist payments schema.
    """
    consumer = _build_consumer("stg_payments_consumer", PAYMENTS_TOPIC)
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    rows = []
    batch_size = 1000
    idle_polls = 0
    max_idle_polls = 5

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            idle_polls += 1
            if idle_polls >= max_idle_polls:
                break
            continue

        if msg.error():
            raise Exception(msg.error())

        idle_polls = 0

        payload_str = msg.value().decode("utf-8")
        try:
            record = json.loads(payload_str)
        except json.JSONDecodeError:
            print(f"Skipping non-JSON message: {payload_str}")
            continue

        rows.append(
            {
                "order_id": record.get("order_id"),
                "payment_sequential": record.get("payment_sequential"),
                "payment_type": record.get("payment_type"),
                "payment_installments": record.get("payment_installments"),
                "payment_value": record.get("payment_value"),
            }
        )

        if len(rows) >= batch_size:
            _insert_payments(pg, rows)
            rows = []

    if rows:
        _insert_payments(pg, rows)

    consumer.close()


def _insert_payments(pg, rows):
    if not rows:
        return

    sql = """
        INSERT INTO public.stg_order_payments (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        )
        VALUES (
            %(order_id)s,
            %(payment_sequential)s,
            %(payment_type)s,
            %(payment_installments)s,
            %(payment_value)s
        )
        ON CONFLICT (order_id, payment_sequential) DO UPDATE
        SET
            payment_type = EXCLUDED.payment_type,
            payment_installments = EXCLUDED.payment_installments,
            payment_value = EXCLUDED.payment_value;
    """

    # ✅ insert one message at a time
    for row in rows:
        pg.run(sql, parameters=row)



# --- DAG DEFINITION ----------------------------------------------------------

with DAG(
    dag_id="kafka_consume_olist_to_staging",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    description="Consume Olist orders & payments from Kafka into staging tables",
    tags=["kafka", "olist", "staging"],
) as dag:

    # 1) Make sure staging tables exist
    create_stg_orders = PostgresOperator(
        task_id="create_stg_orders_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS stg_orders (
                order_id TEXT,
                customer_id TEXT,
                order_status TEXT,
                order_purchase_timestamp TEXT,
                order_approved_at TEXT,
                order_delivered_carrier_date TEXT,
                order_delivered_customer_date TEXT,
                order_estimated_delivery_date TEXT
            );
        """,
    )

    create_stg_order_payments = PostgresOperator(
        task_id="create_stg_order_payments_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS stg_order_payments (
                order_id TEXT,
                payment_sequential TEXT,
                payment_type TEXT,
                payment_installments TEXT,
                payment_value TEXT
            );
        """,
    )

    # 2) Consume from Kafka and load into staging
    consume_orders = PythonOperator(
        task_id="consume_orders_from_kafka",
        python_callable=_consume_orders_to_staging,
    )

    consume_payments = PythonOperator(
        task_id="consume_payments_from_kafka",
        python_callable=_consume_payments_to_staging,
    )

    # order: create tables -> consume orders -> consume payments
    create_stg_orders >> create_stg_order_payments >> consume_orders >> consume_payments
