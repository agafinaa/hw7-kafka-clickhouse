import json
import pandas as pd
from confluent_kafka import Producer

CSV_PATH = "data/train.csv"
TOPIC = "transactions"
BROKER = "localhost:9094"

COL_STATE = "us_state"
COL_CATEGORY = "cat_id"
COL_AMOUNT = "amount"


def delivery_report(err, msg):
    if err is not None:
        raise RuntimeError(f"Delivery failed: {err}")


def main():
    df = pd.read_csv(CSV_PATH)
    producer = Producer({"bootstrap.servers": BROKER})
    sent = 0
    for row in df[[COL_STATE, COL_CATEGORY, COL_AMOUNT]].itertuples(index=False):
        payload = {
            "state": str(row[0]),
            "category": str(row[1]),
            "amount": float(row[2]),
        }
        producer.produce(TOPIC,
                         value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                         callback=delivery_report)
        producer.poll(0)
        sent += 1
        if sent % 5000 == 0:
            producer.flush()
            print(f"sent: {sent}")

    producer.flush()
    print(f"done, sent: {sent}")


if __name__ == "__main__":
    main()
