import csv
import json
from confluent_kafka import Producer

KAFKA_BROKER = "kafka:9092"
TOPIC = "olist_orders_raw"   # make sure this topic exists in Kafka


def delivery_report(err, msg):
    """Optional callback for delivery results."""
    if err is not None:
        print(f"❌ Delivery failed for {msg.key()}: {err}")
    # else:
    #     print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def main():
    p = Producer({"bootstrap.servers": KAFKA_BROKER})

    with open("data/raw/olist_orders_dataset.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=1):
            key = row.get("order_id") or str(i)
            value = json.dumps(row)

            while True:
                try:
                    p.produce(
                        TOPIC,
                        key=key,
                        value=value,
                        callback=delivery_report,
                    )
                    break  # produced successfully
                except BufferError:
                    # Local queue is full – let Kafka catch up
                    print("Queue full, waiting for deliveries to complete...")
                    p.poll(1.0)  # wait up to 1s for delivery callbacks

            # allow background delivery callbacks to run
            p.poll(0)

    print("Waiting for all messages to be delivered...")
    p.flush()
    print("✅ Finished sending all order records.")


if __name__ == "__main__":
    main()
