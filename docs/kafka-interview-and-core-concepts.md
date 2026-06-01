# Kafka: interview prep and core concepts

This note is two things in one: **what interviewers often expect you to say** (tight, accurate phrases) and **how Kafka actually fits together** (so the phrases make sense).

---

## Part A — What you need for a typical Kafka interview

### A1. One-sentence pitch

Kafka is a **distributed, durable append-only log** (event streaming platform). Producers write **records** to **topics**; consumers read them **in order per partition**; **consumer groups** scale reads and track **offsets**.

### A2. Vocabulary you should be able to define in under 30 seconds each

| Term | What to say |
|------|----------------|
| **Broker** | A Kafka server that stores data and serves clients. A cluster has many brokers. |
| **Topic** | A named stream of records; logical category (like a table name, not a queue name). |
| **Partition** | A topic is split into partitions; each is an ordered, immutable log. **Parallelism and ordering live here.** |
| **Offset** | Monotonic position of a record **inside a partition** (not global across partitions). |
| **Producer** | Client that publishes to a topic (optionally with a **key**). |
| **Consumer** | Client that reads from topic(s). |
| **Consumer group** | Consumers sharing the same `group.id` cooperate: **each partition is assigned to at most one consumer in the group** (when consumers ≤ partitions and stable). |
| **Consumer group coordinator / rebalance** | When membership changes, partitions are **reassigned** (rebalance). Can pause consumption briefly; tune/explain `partition.assignment.strategy`, static membership, etc., if senior. |
| **Replication** | Each partition has a **leader** and **followers** on other brokers for fault tolerance. |
| **ISR** | In-Sync Replicas: followers caught up enough; used for commit rules and leader election. |

### A3. Ordering: the answer interviewers want

- **Global order** across a whole topic: **not guaranteed** (multiple partitions interleave).
- **Order per key** (practical pattern): use the **same key** → same **partition** (hash of key % num partitions) → **strict order for that key** within the partition.

### A4. Delivery semantics (say the trade-offs)

| Semantic | Idea | Typical cost |
|----------|------|----------------|
| **At-most-once** | Send, no wait / commit carelessly → can **lose** messages. | Lowest latency, least safety. |
| **At-least-once** | Retry on failure; consumer may **reprocess** (duplicates possible) unless idempotent. | Common default story. |
| **Exactly-once** | End-to-end correctness (transactions / idempotent producer + read-process-write patterns). | Complexity, overhead, bounded scenarios; **be honest** about what “EOS” covers in your stack. |

Interview tip: pair semantics with **idempotent consumers** (dedupe key, natural keys, outbox pattern).

### A5. Retention vs deletion

- **Retention by time/size**: Kafka keeps logs even if no consumer is online (unlike many queues that drop after ack).
- **Compaction** (optional): keep **latest value per key** for changelog-style topics.

### A6. “Kafka vs message queue (e.g. RabbitMQ)”

- Kafka: **log / replay**, consumer-controlled offset, high throughput, partitioned ordering, long retention.
- Classic queue: often **per-message delete** after ack, push-style patterns, different routing (exchanges).

Avoid absolutes; say **“depends on workload”**.

### A7. Operational topics (mid/senior)

- **Under-replicated partitions / offline broker**: replication lag, ISR shrink, risk if leader dies.
- **Hot partition**: one key dominates → one partition bottleneck.
- **Too few partitions**: limits consumer parallelism; **too many**: more metadata, file handles, rebalance cost.
- **ZooKeeper vs KRaft**: modern direction is **KRaft** (Kafka metadata in Kafka); know that older stacks used ZooKeeper for coordination.

### A8. Things you can mention if you used Kafka in projects

- **Schema evolution** (Avro/Protobuf/JSON Schema + Schema Registry).
- **Dead-letter** handling (retry topic, poison pill).
- **Outbox pattern** (DB + Kafka consistency).
- **Monitoring**: consumer lag, under-replicated partitions, request latencies.

---

## Part B — Mental model: how Kafka works (understanding)

### B1. The core object is a partition log

Imagine a **single partition** as a growing array on disk:

```text
partition 2:  [0] [1] [2] [3] ... offsets
```

Producers **append**; they do not update rows in place (normal topics). Consumers read by **offset**.

### B2. Topics scale by splitting into partitions

A topic is **one or more partitions** stored across brokers:

- **Scale writes/reads**: many partitions → many disks/servers can work in parallel.
- **Ordering**: only **within** a partition.

### B3. Keys choose (sticky) routing

If you set a **key**, the default partitioner sends all records with that key to the **same partition** (stable ordering for that key). No key → round-robin-ish assignment (no key-based ordering).

### B4. Consumer groups are the scaling mechanism

Rules of thumb:

- Consumers in the **same group** **share** partitions (each partition → one consumer in the group).
- If **consumers > partitions**, extra consumers sit **idle** (no partitions to assign).
- Offsets are stored **per group + partition** (so a new group can read `earliest` or `latest` independently).

### B5. Why “replay” is natural

Because the log is retained, a new consumer group (or a reset offset in dev) can **read history** again. That’s powerful for new services, debugging, and reprocessing — and it’s why **disk retention policy** matters.

### B6. Replication (why clusters survive faults)

A partition is copied to multiple brokers. One replica is **leader** (handles reads/writes for clients); others follow. If the leader fails, a new leader is elected from **ISR** when possible.

### B7. What often confuses newcomers

1. **“Kafka guaranteed order”** — only **per partition**, not whole topic.
2. **Offsets vs keys** — offset is log position; key is routing metadata.
3. **Consumer lag** — how far behind the consumer group is from the log end; operational health signal.
4. **Rebalance** — not an error; it’s coordination. It becomes a problem if it happens too often (flapping consumers, slow processing).

---

## Part C — Quick self-check (can you answer these out loud?)

1. What guarantees ordering, and at what scope?
2. Why might two consumers in the **same** group both stay busy but **not** see duplicate partitions?
3. What happens if all consumers in a group use a **new** `group.id`?
4. Why can processing be **at-least-once** even if the broker never loses data?
5. Name one cause of **hot partition** and one mitigation.

If you can answer those in plain language, you are in solid shape for many Kafka screens.

---

## Part D — Tie-in to this repo’s samples

| Sample | Narrate this when you run it |
|--------|------------------------------|
| `01_producer.py` + `02_consumer.py` | **Append** to a log; **same key → same partition**; **offset** increases per partition; **group** remembers progress. |
| `03_producer_many_keys.py` | **Distinct keys** map to different partitions (see the key → partition summary). |
| `03_consumer_two_workers.py` | **Same group_id** → each partition handled by **one** worker; two terminals **split** work. |

Run the [README](../README.md) flow once. If you can explain every column in `partition=… offset=… key=…` from the terminal, you are using the right mental model for interviews.
