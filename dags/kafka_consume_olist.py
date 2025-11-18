from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from confluent_kafka import Consumer


KAFKA_BOOTSTRAP = "kafka:9092"  # inside Docker network
ORDERS_TOPIC = "olist_orders_raw"
PAYMENTS_TOPIC = "olist_payments_raw"
POSTGRES_CONN_ID = "dw_postgres"


def _consume_topic_to_table(topic: str, table_name: str, **context):
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"{table_name}_loader",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    rows = []
    batch_size = 1000
    idle_polls = 0
    max_idle_polls = 5  # stop after ~5 seconds with no new messages

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

        value_str = msg.value().decode("utf-8")
        key_str = msg.key().decode("utf-8") if msg.key() else None

        # ensure payload is JSON so we can cast to jsonb
        try:
            json.loads(value_str)
        except json.JSONDecodeError:
            print(f"Skipping non-JSON message from {topic}: {value_str}")
            continue

        rows.append(
            {
                "order_id": key_str,
                "payload": value_str,
                "kafka_topic": msg.topic(),
                "kafka_partition": msg.partition(),
                "kafka_offset": msg.offset(),
            }
        )

        if len(rows) >= batch_size:
            _insert_rows(pg, table_name, rows)
            rows = []

    if rows:
        _insert_rows(pg, table_name, rows)

    consumer.close()


def _insert_rows(pg, table_name, rows):
    """
    Insert a list of dict rows into Postgres.

    `rows` should be a list like:
        [{"order_id": "...", "customer_id": "...", ...}, ...]
    """
    if not rows:
        return

    # Use the keys from the first row as columns
    cols = list(rows[0].keys())
    columns_sql = ", ".join(cols)
    placeholders_sql = ", ".join([f"%({c})s" for c in cols])

    insert_sql = f"""
        INSERT INTO {table_name} ({columns_sql})
        VALUES ({placeholders_sql})
    """

    # IMPORTANT: run one row (one dict) at a time
    for row in rows:
        pg.run(insert_sql, parameters=row)



def consume_orders(**context):
    _consume_topic_to_table(
        topic=ORDERS_TOPIC,
        table_name="raw_kafka.raw_kafka_orders",
        **context,
    )


def consume_payments(**context):
    _consume_topic_to_table(
        topic=PAYMENTS_TOPIC,
        table_name="raw_kafka.raw_kafka_payments",
        **context,
    )


with DAG(
    dag_id="kafka_consume_olist",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    description="Consume Olist orders & payments from Kafka into raw_kafka tables",
    tags=["kafka", "olist"],
) as dag:

    consume_orders_task = PythonOperator(
        task_id="consume_orders_from_kafka",
        python_callable=consume_orders,
    )

    consume_payments_task = PythonOperator(
        task_id="consume_payments_from_kafka",
        python_callable=consume_payments,
    )

    consume_orders_task >> consume_payments_task
