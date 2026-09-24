"""
Sample 2 — Basic consumer

Subscribes to `hello-kafka` and prints each record with partition, offset, key, and value.

Kafka concepts in this file:
  - consumer group (group_id): Kafka tracks how far THIS group has read (offsets)
  - auto_offset_reset=earliest: if the group is new, start at the oldest available record
  - poll loop: for msg in consumer blocks until records arrive

Run this BEFORE samples/01_producer.py so you see messages as they are sent.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaConsumer

from config import BOOTSTRAP_SERVERS, KAFKA_API_VERSION, TOPIC_HELLO

GROUP_ID = "learning-group-hello"


def main() -> None:
    print(f"Bootstrap: {BOOTSTRAP_SERVERS}")
    print(f"Topic:     {TOPIC_HELLO!r}")
    print(f"Group:     {GROUP_ID!r}  (offsets are stored per group)")
    print("Offset reset for a NEW group: earliest (read from the start of the log)")
    print("\nWaiting for records ... run samples/01_producer.py in another terminal.\n")

    consumer = KafkaConsumer(
        TOPIC_HELLO,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )
    try:
        for msg in consumer:
            print(
                f"READ  partition={msg.partition}  offset={msg.offset}  "
                f"key={msg.key!r}  value={msg.value} timestamp = {msg.timestamp} "
            )
    except KeyboardInterrupt:
        print("\nStopped. Committed offsets for this group are kept on the broker.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
