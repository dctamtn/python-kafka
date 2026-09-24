"""
Sample 3b — Two consumers, one consumer group (load balancing)

Both processes use group_id='learning-group-orders'. Kafka assigns each partition
to at most one consumer in the group, so work is shared.

How to run:
  Terminal A: python samples/03_consumer_two_workers.py worker-a
  Terminal B: python samples/03_consumer_two_workers.py worker-b
  Terminal C: python samples/03_producer_many_keys.py

Pass a label (worker-a / worker-b) so you can tell which terminal handled each message.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from config import BOOTSTRAP_SERVERS, KAFKA_API_VERSION, TOPIC_ORDERS

GROUP_ID = "learning-group-orders"


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python samples/03_consumer_two_workers.py <worker-name>")
        print("Example: python samples/03_consumer_two_workers.py worker-a")
        sys.exit(1)

    name = sys.argv[1]
    print(f"Worker label: {name!r}")
    print(f"Topic:        {TOPIC_ORDERS!r}")
    print(f"Group:        {GROUP_ID!r}")
    print(
        "This process competes with other consumers in the SAME group for partitions.\n"
        "Waiting for messages ... run samples/03_producer_many_keys.py when both workers are up.\n"
    )

    try:
        consumer = KafkaConsumer(
            TOPIC_ORDERS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            api_version=KAFKA_API_VERSION,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
        )
    except NoBrokersAvailable:
        print("ERROR: Kafka broker is not reachable at localhost:9092.")
        print("Start/verify the stack: docker compose up -d ; docker compose ps")
        sys.exit(2)

    try:
        for msg in consumer:
            print(
                f"[{name}] HANDLED  partition={msg.partition}  offset={msg.offset}  "
                f"key={msg.key!r}  value={msg.value}"
            )
    except KeyboardInterrupt:
        print(f"\n[{name}] stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
