"""
Sample 1 — Basic producer

Publishes 5 JSON records to topic `hello-kafka` (see config.py).

Kafka concepts in this file:
  - topic:   where records are stored
  - key:     optional; same key -> same partition (all 5 messages use "demo-key")
  - value:   your payload (here: JSON)
  - offset:  position of the record inside that partition (assigned by the broker)

Run samples/02_consumer.py in another terminal first, then run this script.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaProducer

from config import BOOTSTRAP_SERVERS, KAFKA_API_VERSION, TOPIC_HELLO

MESSAGE_KEY = "demo-key"  # Same key on every send -> same partition -> strict order for this key.


def main() -> None:
    print(f"Connecting to {BOOTSTRAP_SERVERS} ...")
    print(f"Topic: {TOPIC_HELLO!r}  |  key on every record: {MESSAGE_KEY!r}")
    print("(Same key routes all messages to one partition.)\n")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )
    try:
        for i in range(5):
            payload = {"seq": i, "message": f"hello from producer #{i}"}
            future = producer.send(TOPIC_HELLO, key=MESSAGE_KEY, value=payload)
            record_metadata = future.get(timeout=10)
            print(
                f"[{i}] APPENDED  topic={record_metadata.topic!r}  "
                f"partition={record_metadata.partition}  "
                f"offset={record_metadata.offset}  "                
                f"key={MESSAGE_KEY!r}"
            )
            time.sleep(0.3)
        producer.flush()
        print("\nDone. Records are durably on the broker; the consumer can read them.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
