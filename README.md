# Kafka learning lab (Python)

Small Docker Compose stack plus Python scripts for **producer → topic → consumer** and **consumer groups**.

**Full guide (setup, diagrams, deep dives, interview prep):** [docs/kafka-guide.md](docs/kafka-guide.md)

---

## Quick start

```powershell
docker compose up -d
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Terminal A** — start consumer first:

```powershell
python samples\02_consumer.py
```

**Terminal B** — then producer:

```powershell
python samples\01_producer.py
```

For Step 4 (two workers + many keys), consumer groups, troubleshooting, and Kafka concepts, see **[docs/kafka-guide.md](docs/kafka-guide.md)**.

---

## Validate Kafka On Docker Desktop (Step By Step)

Use these steps to confirm Kafka is healthy and reachable on your machine.

### 1) Start Docker Desktop

- Open Docker Desktop and wait until the engine shows as running.

### 2) Start the Kafka stack

From the repo root:

```powershell
docker compose up -d
```

If Kafka image pull fails once (network timeout), run:

```powershell
docker pull confluentinc/cp-kafka:7.6.1
docker compose up -d
```

### 3) Confirm containers are up

```powershell
docker compose ps
```

You should see both services in `Up` state:

- `python_kafka-zookeeper-1`
- `python_kafka-kafka-1`

### 4) Check Kafka logs for startup success

```powershell
docker compose logs kafka --tail 100
```

Look for lines showing the broker started and is listening (port `9092`).

### 5) Verify with Python sample (real end-to-end test)

Terminal A:

```powershell
python samples\02_consumer.py
```

Terminal B:

```powershell
python samples\01_producer.py
```

Success means:

- Producer prints `partition` and `offset`
- Consumer prints `READ partition=... offset=...`

### 6) If Docker Desktop still looks empty

Check Docker context:

```powershell
docker context ls
docker context use desktop-linux
docker compose up -d
```

### 7) Clean reset (optional)

```powershell
docker compose down -v
docker compose up -d
```

This wipes old Kafka data and recreates containers and volumes.

---

## Repo layout

| Path | Purpose |
|------|---------|
| [docs/kafka-guide.md](docs/kafka-guide.md) | Complete learning guide |
| [docs/kafka-debugging-tools.md](docs/kafka-debugging-tools.md) | CLI, Kafka UI, and debug workflows |
| `samples/` | Producer and consumer scripts |
| `config.py` | Broker address and topic names |
| `docker-compose.yml` | ZooKeeper + Kafka on `localhost:9092` |
