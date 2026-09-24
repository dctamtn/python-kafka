# Kafka debugging tools (this lab)

Tools and commands to **inspect topics**, **read messages**, **check consumer groups**, and **trace flow** while you debug Python samples against `localhost:9092`.

**Topics in this repo:** `hello-kafka`, `orders-demo` (see `config.py`)

---

## Quick pick: what to use when

| Goal | Tool | Section |
|------|------|---------|
| List topics / partitions | Kafka CLI in Docker | [§1](#1-built-in-cli-via-docker-no-extra-install) |
| Peek messages without your consumer | `kafka-console-consumer` | [§1](#1-built-in-cli-via-docker-no-extra-install) |
| See committed offsets & lag | `kafka-consumer-groups` | [§1](#1-built-in-cli-via-docker-no-extra-install) |
| Visual browser (topics, messages, groups) | Kafka UI (web) | [§2](#2-kafka-ui-web-recommended-for-visual-debugging) |
| Fast terminal produce/consume from Windows | kcat | [§3](#3-kcat-terminal-on-your-host) |
| Desktop GUI | Offset Explorer | [§4](#4-offset-explorer-desktop-gui) |
| Trace your Python scripts | Sample log output | [§5](#5-debugging-this-repos-python-samples) |

---

## 1. Built-in CLI (via Docker, no extra install)

Your `docker-compose.yml` uses Confluent’s `cp-kafka` image, which includes official Kafka command-line tools. Run them **inside** the `kafka` container.

### List topics

```powershell
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Describe a topic (partitions, leaders, offsets)

```powershell
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic hello-kafka
```

Example output columns:

| Column | Meaning |
|--------|---------|
| `Partition` | Partition id (0, 1, 2, …) |
| `Leader` | Broker handling reads/writes for that partition |
| `Replicas` / `Isr` | Replication (single broker in this lab) |
| `LogEndOffset` | Next offset to write (high watermark) |

### Read messages (without running your Python consumer)

From the **beginning** (good right after `01_producer.py`):

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic hello-kafka --from-beginning --property print.key=true --property key.separator=" | "
```

Only **new** messages (like `auto_offset_reset=latest`):

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic hello-kafka --property print.key=true
```

Orders demo:

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-demo --from-beginning --property print.key=true --property key.separator=" | "
```

Press **Ctrl+C** to stop.

### Send a test message manually

```powershell
docker compose exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic hello-kafka --property "parse.key=true" --property "key.separator=:"
```

Type one line per message, e.g. `my-key:{"test": true}`, then **Ctrl+C**.

### Consumer groups (offsets & lag)

After running `02_consumer.py`:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group learning-group-hello
```

After Step 4 (`03_consumer_two_workers.py`):

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group learning-group-orders
```

| Column | Meaning |
|--------|---------|
| `CURRENT-OFFSET` | Where the group has read to |
| `LOG-END-OFFSET` | Latest offset on the broker |
| `LAG` | `LOG-END-OFFSET - CURRENT-OFFSET` — **0** means caught up; **>0** means backlog |

### Reset a group offset (dev only)

Use when you want to re-read from the start **without** changing `group_id` in Python:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group learning-group-hello --reset-offsets --to-earliest --topic hello-kafka --execute
```

Stop your Python consumer first. For a clean slate, `docker compose down -v` wipes all broker data.

### Typical debug flow (Samples 1 & 2)

```text
1. docker compose ps                          → broker up?
2. kafka-topics --describe --topic hello-kafka   → partitions exist?
3. Run 01_producer.py                         → messages sent?
4. kafka-console-consumer --from-beginning    → messages on broker?
5. kafka-consumer-groups --describe --group learning-group-hello  → offsets moving?
```

If step 4 shows messages but your Python consumer does not → check `group_id`, consumer already committed past those offsets, or consumer not subscribed to the right topic.

---

## 2. Kafka UI (web, recommended for visual debugging)

**[UI for Apache Kafka](https://github.com/provectus/kafka-ui)** (Provectus) — free, open source. Browse topics, read messages, inspect consumer groups, and see partition layout in a browser.

### Option A — one-off Docker run (does not change this repo)

With Kafka already running via `docker compose up -d`:

```powershell
docker run -d --name kafka-ui -p 8080:8080 `
  -e KAFKA_CLUSTERS_0_NAME=local `
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.docker.internal:9092 `
  provectuslabs/kafka-ui:latest
```

Open **http://localhost:8080**

Stop when done:

```powershell
docker stop kafka-ui
docker rm kafka-ui
```

On Linux, replace `host.docker.internal:9092` with your host IP or add `extra_hosts` as needed.

### Option B — add to `docker-compose.yml` (persistent)

Add a service (example):

```yaml
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      - kafka
```

Then `docker compose up -d` and open **http://localhost:8080**.

### What to click while debugging

| UI area | Use for |
|---------|---------|
| **Topics** → `hello-kafka` / `orders-demo` | Partition count, message count |
| **Messages** | Read payload, key, partition, offset, timestamp |
| **Consumer groups** | `learning-group-hello`, `learning-group-orders` — lag per partition |
| **Brokers** | Confirm single broker healthy (this lab) |

---

## 3. kcat (terminal on your host)

**[kcat](https://github.com/edenhill/kcat)** (formerly kafkacat) — lightweight CLI on Windows/macOS/Linux. Good when you want quick reads without `docker exec`.

Install (examples):

- Windows: `choco install kcat` or download from releases
- macOS: `brew install kcat`

### Examples (host → `localhost:9092`)

List metadata:

```powershell
kcat -b localhost:9092 -L
```

Consume from beginning:

```powershell
kcat -b localhost:9092 -t hello-kafka -C -o beginning -f "partition=%p offset=%o key=%k value=%s\n"
```

Produce one message:

```powershell
echo user-0:{"order_id":99} | kcat -b localhost:9092 -t orders-demo -P -K:
```

(`-K:` means key is separated from value by `:`)

---

## 4. Offset Explorer (desktop GUI)

**[Offset Explorer](https://www.kafkatool.com/)** (formerly Kafka Tool) — desktop app for Windows/macOS/Linux.

- Connect to `localhost:9092` (no auth in this lab)
- Tree view: brokers → topics → partitions → messages
- Consumer group tab: offsets and lag

Useful if you prefer a native GUI over a browser. Free for single-broker clusters; paid features exist for larger setups.

**Other UIs (similar role):** [AKHQ](https://github.com/tchiotludo/akhq), [Kafdrop](https://github.com/obsidiandynamics/kafdrop) — also run as Docker containers pointing at `localhost:9092` or `kafka:29092`.

---

## 5. Debugging this repo’s Python samples

Your scripts already print the fields you need to correlate with CLI/UI:

```text
APPENDED / READ / HANDLED  partition=…  offset=…  key=…  value=…
```

### Checklist when something looks wrong

| Symptom | Check |
|---------|--------|
| Producer prints partition/offset but consumer silent | Consumer started? Same topic in `config.py`? Group already past those offsets? |
| All messages same partition | Expected for `demo-key` in Sample 1; unexpected if you expected many keys |
| Only one worker busy in Step 4 | `kafka-topics --describe` — topic created with 1 partition? Run `docker compose down -v` |
| `NoBrokersAvailable` | `docker compose ps`; wait after first start |
| Duplicate processing | At-least-once behavior; consumer crashed after read before commit |

### Extra logging in code (optional)

```python
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("kafka").setLevel(logging.DEBUG)
```

Shows broker requests in the terminal — noisy but useful for connection and metadata issues.

### Compare three views of the same flow

```text
Terminal A: python samples\02_consumer.py     → what your app reads
Terminal B: python samples\01_producer.py     → what your app writes
Terminal C: kafka-console-consumer OR Kafka UI → ground truth on the broker
```

If broker/UI shows messages but Python consumer does not → problem is likely **group/offset/subscription**, not produce path.

---

## 6. Production-oriented tools (reference)

Not required for this lab, but names you will see on real projects:

| Tool | Role |
|------|------|
| **Prometheus + Grafana** | Metrics dashboards |
| **Burrow** / **Kafka Lag Exporter** | Consumer lag monitoring |
| **Conduktor** / **Confluent Control Center** | Commercial ops UIs |
| **Schema Registry** | Avro/Protobuf schema debug (not used in this JSON lab) |

---

## 7. Cheat sheet (copy-paste)

```powershell
# Topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic hello-kafka

# Peek messages
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic hello-kafka --from-beginning --property print.key=true

# Consumer groups
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group learning-group-hello
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group learning-group-orders

# Broker logs
docker compose logs -f kafka
```

---

**See also:** [kafka-guide.md](kafka-guide.md) — concepts, labs, and troubleshooting.
