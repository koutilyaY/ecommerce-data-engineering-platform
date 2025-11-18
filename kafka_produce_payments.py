import csv
import json
import time
from confluent_kafka import Producer

KAFKA_BROKER = "kafka:9092"
TOPIC = "olist_payments_raw"  # or your final topic name


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for {msg.key()}: {err}")
    # else:
    #     print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def main():
    p = Producer({"bootstrap.servers": KAFKA_BROKER})

    with open("data/raw/olist_order_payments_dataset.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=1):
            key = row.get("order_id") or str(i)
            value = json.dumps(row)

            # keep trying if local queue is full
            while True:
                try:
                    p.produce(
                        TOPIC,
                        key=key,
                        value=value,
                        callback=delivery_report,
                    )
                    break  # produced OK -> move to next record
                except BufferError:
                    # queue full: serve delivery callbacks & wait a bit
                    print("Queue full, waiting for deliveries to complete...")
                    p.poll(1.0)  # wait up to 1s for Kafka to catch up

            # allow background delivery callbacks to run
            p.poll(0)

    print("Waiting for all messages to be delivered...")
    p.flush()
    print("✅ Finished sending all payment records.")


if __name__ == "__main__":
    main()
