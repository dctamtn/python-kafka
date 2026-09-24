"""
Sample 3a — Producer with many keys (for the two-worker consumer demo)

Publishes 12 "orders" to topic `orders-demo`. Keys are user-0 .. user-3 (4 distinct keys).

Kafka concept:
  - partitioner hashes the key -> one partition per key
  - same user_id always lands on the same partition (ordering per user)

Run AFTER two consumers are up:
  python samples/03_consumer_two_workers.py worker-a
  python samples/03_consumer_two_workers.py worker-b
"""

from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaProducer

from config import BOOTSTRAP_SERVERS, KAFKA_API_VERSION, TOPIC_ORDERS

DISTINCT_KEYS = 4  # user-0 .. user-3


def main() -> None:
    print(f"Topic: {TOPIC_ORDERS!r}")
    print(f"Sending 12 orders with keys user-0 .. user-{DISTINCT_KEYS - 1}")
    print("Watch: same key -> same partition in the lines below.\n")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )
    key_to_partition: dict[str, set[int]] = defaultdict(set)
    try:
        for i in range(12):
            user_id = f"user-{i % DISTINCT_KEYS}"
            payload = {"order_id": i, "user": user_id, "amount_cents": 100 * (i + 1)}
            future = producer.send(TOPIC_ORDERS, key=user_id, value=payload)
            meta = future.get(timeout=10)
            key_to_partition[user_id].add(meta.partition)
            print(
                f"[order {i:2d}]  key={user_id!r}  ->  "
                f"partition={meta.partition}  offset={meta.offset}"
            )
            time.sleep(0.15)
        producer.flush()
        print("\n--- Key -> partition map (each key should use exactly one partition) ---")
        for key in sorted(key_to_partition):
            parts = sorted(key_to_partition[key])
            print(f"  {key}: partition(s) {parts}")
        print(
            "\nNow check worker-a / worker-b terminals: each message appears on "
            "only ONE worker (same consumer group)."
        )
    finally:
        producer.close()


if __name__ == "__main__":
    main()
