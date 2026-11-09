"""
Sample 3a — Producer with many keys (for the multi-worker consumer demo)

Publishes "orders" to topic `orders-demo`. One distinct key per partition
(user-0 .. user-{N-1}, where N = partition count of the topic), 3 orders per key.

Kafka concept:
  - the partitioner maps a key -> one partition; same key always -> same partition
    (ordering per user)
  - a consumer group can only spread work over as many partitions as exist:
    N partitions feed at most N workers

Why a custom partitioner here?
  Kafka's default partitioner hashes the key (murmur2). With only a handful of
  keys, several keys often hash to the SAME partition and other partitions stay
  empty — then the workers assigned those empty partitions never print anything.
  For this demo we map user-i -> partition i so EVERY partition (and therefore
  every worker, up to N) receives messages. Production code normally keeps the
  default partitioner and relies on many distinct keys instead.

Run AFTER the consumers are up (any number from 1 up to the partition count):
  python samples/03_consumer_two_workers.py worker-a
  python samples/03_consumer_two_workers.py worker-b
  python samples/03_consumer_two_workers.py worker-c
  ...
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

ORDERS_PER_KEY = 3


def user_partitioner(key: bytes | None, all_partitions: list[int], available: list[int]) -> int:
    """Map key b'user-<i>' -> partition i % N (deterministic, covers every partition).

    Called by KafkaProducer on every send() that has no explicit `partition=`.
    Signature is fixed by kafka-python:
      key            - the key AFTER key_serializer ran (bytes), or None if no key
      all_partitions - sorted ids of every partition of the topic, e.g. [0, 1, 2, 3]
      available      - subset of all_partitions that currently has a leader (unused here)
      returns        - the partition id to append the record to

    Examples (N = 4 partitions, all_partitions = [0, 1, 2, 3]):
      key=b"user-0",  -> 0        ("0"  % 4 = 0)
      key=b"user-1",  -> 1
      key=b"user-3",  -> 3
      key=b"user-4",  -> 0        wraps around: 4 % 4 = 0 (shares partition with user-0)
      key=b"user-10", -> 2        rsplit("-", 1) splits on the LAST hyphen -> "10"; 10 % 4 = 2
      key=None        -> 0        keyless record: always first partition

    Same key always returns the same partition, so per-user ordering is kept.
    Partition ids come from all_partitions, so the function also works for
    non-contiguous lists, e.g. all_partitions=[0, 2] and key=b"user-1" -> 2.
    """
    if key is None:
        return all_partitions[0]
    index = int(key.decode("utf-8").rsplit("-", 1)[1])
    return all_partitions[index % len(all_partitions)]


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        partitioner=user_partitioner,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )
    try:
        partitions = producer.partitions_for(TOPIC_ORDERS)
        print(f"[debug] partitions_for({TOPIC_ORDERS!r}) -> {partitions!r}")
        if not partitions:
            # Topic does not exist yet: the first send auto-creates it, then retry.
            producer.send(TOPIC_ORDERS, key="user-0", value={"warmup": True}).get(timeout=10)
            partitions = producer.partitions_for(TOPIC_ORDERS)
            print(f"[debug] partitions_for after warmup send -> {partitions!r}")
        num_partitions = len(partitions)
        total = num_partitions * ORDERS_PER_KEY

        print(f"Topic: {TOPIC_ORDERS!r}  partitions: {num_partitions}")
        print(
            f"Sending {total} orders with keys user-0 .. user-{num_partitions - 1} "
            f"({ORDERS_PER_KEY} orders per key, one key per partition)"
        )
        print("Watch: same key -> same partition in the lines below.\n")

        key_to_partition: dict[str, set[int]] = defaultdict(set)
        for i in range(total):
            user_id = f"user-{i % num_partitions}"
            payload = {"order_id": i, "user": user_id, "amount_cents": 100 * (i + 1)}
            meta = producer.send(TOPIC_ORDERS, key=user_id, value=payload).get(timeout=10)
            key_to_partition[user_id].add(meta.partition)
            print(
                f"[order {i:2d}]  key={user_id!r}  ->  "
                f"partition={meta.partition}  offset={meta.offset}"
            )
            time.sleep(0.15)
        producer.flush()

        print("\n--- Key -> partition map (each key should use exactly one partition) ---")
        for key in sorted(key_to_partition, key=lambda k: int(k.rsplit("-", 1)[1])):
            parts = sorted(key_to_partition[key])
            print(f"  {key}: partition(s) {parts}")
        print(
            "\nNow check the worker terminals: each message appears on only ONE worker "
            f"(same consumer group). Up to {num_partitions} workers can be busy at once - "
            "one per partition."
        )
    finally:
        producer.close()


if __name__ == "__main__":
    main()
