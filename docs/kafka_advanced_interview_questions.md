# Advanced Kafka Interview Questions

## Table of Contents
1. [Message Duplication & Loss](#message-duplication--loss)
2. [Consumer Group Rebalancing](#consumer-group-rebalancing)
3. [Partitioning & Scalability](#partitioning--scalability)
4. [Exactly-Once Semantics (EOS)](#exactly-once-semantics-eos)
5. [Monitoring & Observability](#monitoring--observability)
6. [Failure Scenarios & Recovery](#failure-scenarios--recovery)
7. [Performance Tuning](#performance-tuning)
8. [Security & Multi-Tenancy](#security--multi-tenancy)

---

## Message Duplication & Loss

### Q1: How does Kafka handle message duplication, and what strategies can you implement to achieve idempotent processing?

**Answer:**
Kafka provides **idempotent producers** (enable.idempotence=true) which ensure exactly-once semantics per partition within a single producer session. The producer attaches a unique PID (Producer ID) and sequence number to each message.

**For idempotent processing at the consumer level:**
- Use **unique message IDs** and store processed IDs in an external store (Redis, database)
- Implement **deterministic idempotency keys** based on message content
- Use **Kafka Transactions** with consumer-producer patterns
- Store consumer offsets with business logic in the same database transaction

**Example pattern:**
```java
// Check if message was already processed
if (processedMessageStore.contains(messageId)) {
    return; // Skip duplicate
}
// Process message
database.transaction(() -> {
    process(message);
    processedMessageStore.add(messageId);
    consumer.commitSync();
});
```

---

### Q2: What are the scenarios where messages can be lost in Kafka, and how do you prevent each?

**Answer:**

| Scenario | Cause | Prevention |
|----------|-------|------------|
| **Producer-side loss** | Network failure, async send without callback | Use `acks=all`, enable retries, implement callback handlers |
| **Broker-side loss** | Unclean leader election, insufficient replication | `min.insync.replicas=2`, `replication.factor=3`, disable unclean election |
| **Consumer-side loss** | Auto-commit before processing | Use manual commits, process-then-commit pattern |
| **Log retention loss** | Messages expired before consumption | Tune `retention.ms`, `retention.bytes`, or use compacted topics |
| **Partition reassignment loss** | Improper handling during rebalancing | Implement rebalancing listeners, pause consumption during rebalance |

**Critical configurations:**
```properties
# Producer
acks=all
retries=2147483647
delivery.timeout.ms=120000
enable.idempotence=true

# Broker
min.insync.replicas=2
unclean.leader.election.enable=false

# Consumer
enable.auto.commit=false
max.poll.records=500
```

---

### Q3: Explain the "at-least-once", "at-most-once", and "exactly-once" semantics in Kafka. When would you choose each?

**Answer:**

| Semantic | Implementation | Use Case | Trade-off |
|----------|---------------|----------|-----------|
| **At-most-once** | Fire-and-forget, no retries, auto-commit | High-throughput, loss-tolerant (metrics, logs) | Fastest, may lose messages |
| **At-least-once** | Retries + acks, manual commit after processing | Most common, requires idempotent consumers | May duplicate, never loses |
| **Exactly-once** | Transactions, idempotent producers, transactional consumers | Financial transactions, inventory management | Higher latency, complexity |

**Exactly-Once implementation:**
```java
producer.initTransactions();

try {
    producer.beginTransaction();
    
    for (ConsumerRecord record : records) {
        producer.send(new ProducerRecord(outputTopic, process(record)));
    }
    
    producer.sendOffsetsToTransaction(
        consumer.position(consumer.assignment()), 
        consumer.groupMetadata()
    );
    
    producer.commitTransaction();
} catch (Exception e) {
    producer.abortTransaction();
    throw e;
}
```

---

### Q4: How do you handle duplicate messages when using Kafka Streams or Kafka Connect?

**Answer:**

**Kafka Streams:**
- Uses **log-compacted changelog topics** for state stores
- Automatic deduplication through stateful operations (`reduce`, `aggregate`)
- Use `suppress()` operator to emit only final results within windows

**Kafka Connect:**
- **Source connectors**: Implement custom `SourceTask` with offset tracking
- **Sink connectors**: Use idempotent writes (UPSERT) or external deduplication
- Enable `errors.tolerance=all` with dead letter queues for poison pills

**Custom deduplication in Streams:**
```java
KStream<String, Transaction> transactions = builder.stream("transactions");

transactions
    .groupByKey()
    .reduce((old, new) -> new.transactionId().equals(old.transactionId()) ? old : new)
    .toStream()
    .to("deduplicated-transactions");
```

---

## Consumer Group Rebalancing

### Q5: What happens during a consumer group rebalance, and how can you minimize its impact?

**Answer:**

**Rebalance triggers:**
- New consumer joins/leaves group
- Consumer fails heartbeat
- Consumer calls `unsubscribe()`
- Partition count changes
- `session.timeout.ms` expires

**Rebalance process (Eager vs Cooperative):**

| Protocol | Behavior | Impact |
|----------|----------|--------|
| **Eager** (default) | Stop all consumers, reassign all partitions | Stop-the-world pause |
| **Cooperative** (sticky) | Revoke only necessary partitions, incremental assignment | Minimal interruption |

**Mitigation strategies:**
```java
// Use cooperative rebalancing
props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
    CooperativeStickyAssignor.class.getName());

// Tune timeouts
props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

// Implement rebalance listener
consumer.subscribe(topics, new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // Flush state, commit offsets
        commitOffsets();
    }
    
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Restore state for new partitions
        initializeState(partitions);
    }
});
```

---

### Q6: What is the "rebalance storm" problem, and how do you solve it?

**Answer:**

**Rebalance Storm:**
Occurs when multiple consumers join/leave in quick succession, causing cascading rebalances that prevent steady consumption.

**Common causes:**
- Frequent consumer restarts (OOM, health checks)
- Network instability causing session timeouts
- Autoscaling groups scaling too aggressively
- Uneven partition distribution causing hot consumers

**Solutions:**

1. **Static membership** (Kafka 2.3+):
```java
props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "consumer-1-static");
// Survives short restarts without rebalance
```

2. **Consumer group metadata caching:**
   - Delay rejoin on transient failures
   - Exponential backoff for rejoin attempts

3. **Partition assignment strategy tuning:**
```java
// Sticky assignor maintains partition ownership
props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
    "org.apache.kafka.clients.consumer.StickyAssignor");
```

4. **Monitoring & alerting:**
   - Alert on rebalance rate > threshold
   - Track consumer lag during rebalances

---

## Partitioning & Scalability

### Q7: How do you choose the right number of partitions for a topic, and what are the trade-offs?

**Answer:**

**Factors to consider:**

| Factor | Guideline |
|--------|-----------|
| **Throughput** | Target: 10MB/s per partition for writes, 10-20MB/s for reads |
| **Consumer parallelism** | Partitions = max(desired consumer count) |
| **Key-based ordering** | Keys hash to partitions; more partitions = better distribution |
| **Operational overhead** | More partitions = more open files, more memory, longer recovery |

**Calculation formula:**
```
Partitions = max(
    RequiredThroughput / PartitionCapacity,
    ExpectedConsumerCount,
    MinimumForKeyDistribution
)
```

**Trade-offs:**

| More Partitions | Fewer Partitions |
|-----------------|------------------|
| Higher throughput | Lower latency (fewer seeks) |
| Better parallelism | Less memory per broker |
| Better key distribution | Faster rebalances |
| More consumer scaling | Easier operations |

**Increasing partitions:**
- Can only add, never remove
- Existing keys may move to new partitions (breaks ordering guarantees)
- Use `kafka-add-partitions.sh` or AdminClient

---

### Q8: Explain partition skew and how to detect and resolve it.

**Answer:**

**Partition Skew:**
Uneven distribution of messages across partitions, causing hot spots and uneven load.

**Causes:**
- Poor key selection (e.g., using `user_id` when 90% of traffic is from one user)
- Null keys with sticky partitioner
- Custom partitioner bugs

**Detection:**
```bash
# Check partition sizes
kafka-log-dirs.sh --describe --bootstrap-server localhost:9092 --topic my-topic

# Check consumer lag per partition
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group my-group --describe
```

**Resolution:**

1. **Salting keys:**
```java
// Add random suffix to hot keys
String saltedKey = hotKey + "_" + random.nextInt(10);
producer.send(new ProducerRecord<>(topic, saltedKey, value));
```

2. **Custom partitioner with awareness:**
```java
public class LoadAwarePartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        // Route based on current partition load
        return leastLoadedPartition(topic, cluster);
    }
}
```

3. **Sticky partitioner with batching** (Kafka 2.4+):
```java
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, 
    "org.apache.kafka.clients.producer.RoundRobinPartitioner");
```

---

## Exactly-Once Semantics (EOS)

### Q9: What are the limitations of Kafka's exactly-once semantics, and when might it fail?

**Answer:**

**EOS Guarantees:**
- Exactly-once per partition within a producer session
- Atomic multi-partition writes within a transaction
- Consumer isolation level controls read uncommitted vs committed

**Limitations & Failure Scenarios:**

| Scenario | Issue | Mitigation |
|----------|-------|------------|
| **Producer session expires** | PID changes, sequence resets | Short transaction duration, tune `transaction.timeout.ms` |
| **Cross-topic transactions** | All topics must be on same broker | Ensure co-location or use fewer brokers |
| **Consumer rebalance during transaction** | May cause abort | Short poll intervals, handle aborts |
| **Zombie producers** | Network partition causes duplicate PIDs | `transactional.id` fencing with epoch |
| **External system writes** | Non-transactional sinks | Use idempotent writes or two-phase commit |

**Transaction timeout tuning:**
```java
props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000); // 60s
props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
```

---

### Q10: How do you implement exactly-once when writing to external systems (databases, Elasticsearch)?

**Answer:**

**Approach 1: Idempotent Writes (Recommended)**
```java
// Database: UPSERT instead of INSERT
INSERT INTO events (id, data) VALUES (?, ?) 
ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;

// Elasticsearch: Use document ID based on Kafka message key
IndexRequest request = new IndexRequest("events")
    .id(message.key()) // Same key = idempotent
    .source(message.value());
```

**Approach 2: Store Offsets with Data**
```java
// Single transaction: data + offset
database.transaction(tx -> {
    insertEvent(tx, event);
    storeConsumerOffset(tx, topicPartition, offset);
});

// On restart: seek to stored offset
consumer.seek(topicPartition, storedOffset + 1);
```

**Approach 3: Two-Phase Commit (Complex)**
- Prepare phase: write to external system
- Commit phase: commit Kafka transaction
- Requires external system support for prepared transactions

---

## Monitoring & Observability

### Q11: What are the critical metrics to monitor in a production Kafka cluster?

**Answer:**

**Broker Metrics:**

| Metric | Alert Threshold | Action |
|--------|-----------------|--------|
| `kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec` | Baseline deviation > 50% | Check producer health |
| `kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec` | > 0 | Check broker logs |
| `kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs` | p99 > 1000ms | Disk I/O issue |
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | > 0 for > 60s | Broker failure |
| `kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs` | Spike in rate | Instability |

**Consumer Metrics:**

| Metric | Alert Threshold | Action |
|--------|-----------------|--------|
| `records-lag-max` | > threshold (business dependent) | Scale consumers |
| `records-consumed-rate` | Drop to 0 | Consumer failure |
| `fetch-latency-avg` | p99 > 500ms | Network or broker issue |
| `commit-latency-avg` | Increasing trend | Coordinator overload |

**Producer Metrics:**

| Metric | Alert Threshold | Action |
|--------|-----------------|--------|
| `record-error-rate` | > 0.1% | Check broker availability |
| `record-retry-rate` | > 10% | Network instability |
| `request-latency-avg` | p99 > 100ms | Broker overload |
| `buffer-available-bytes` | < 10% | Producer backpressure |

---

### Q12: How do you troubleshoot consumer lag that keeps increasing?

**Answer:**

**Diagnostic Steps:**

1. **Identify slow partition(s):**
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group my-group --describe
```

2. **Check for partition skew:**
```bash
kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic my-topic --time -1
```

3. **Analyze consumer behavior:**
   - Is `max.poll.records` too high?
   - Is processing time > `max.poll.interval.ms`?
   - Are commits happening frequently enough?

4. **Common causes & fixes:**

| Cause | Fix |
|-------|-----|
| Slow processing | Optimize code, add caching |
| Insufficient consumers | Add consumers (up to partition count) |
| Rebalance storms | Use static membership, tune timeouts |
| Large messages | Increase `fetch.max.bytes`, tune `max.partition.fetch.bytes` |
| GC pauses | Tune JVM, use G1GC or ZGC |

**Code optimization:**
```java
// Parallel processing within consumer
records.partitions().parallelStream().forEach(partition -> {
    List<ConsumerRecord> partitionRecords = records.records(partition);
    processBatch(partitionRecords);
});

// Batch database writes
List<Event> batch = new ArrayList<>();
for (ConsumerRecord record : records) {
    batch.add(deserialize(record));
    if (batch.size() >= BATCH_SIZE) {
        database.bulkInsert(batch);
        batch.clear();
    }
}
```

---

## Failure Scenarios & Recovery

### Q13: What happens when a Kafka broker dies, and how does the cluster recover?

**Answer:**

**Failure Detection:**
- ZooKeeper/KRaft detects missing heartbeats
- Controller marks broker as offline after `zookeeper.session.timeout.ms`

**Recovery Process:**

1. **Leader election:**
   - Controller selects new leader from ISR (In-Sync Replicas)
   - If no ISR available: unclean election (if enabled) or unavailable

2. **Producer impact:**
   - `acks=0/1`: Brief interruption, automatic retry
   - `acks=all`: Block until new leader elected

3. **Consumer impact:**
   - Rebalance triggered
   - Resume from last committed offset

**Recovery tuning:**
```properties
# Faster failure detection
zookeeper.session.timeout.ms=6000
replica.socket.timeout.ms=30000

# Leader election
controller.socket.timeout.ms=30000
unclean.leader.election.enable=false  # Safety over availability
```

**Manual recovery if data lost:**
```bash
# Check partition status
kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic my-topic

# If under-replicated, check replica status
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
    --topics-to-move-json-file topics.json --broker-list "0,1,2" --generate

# Move partitions back to preferred replica
kafka-leader-election.sh --bootstrap-server localhost:9092 \
    --election-type preferred --topic my-topic --partition 0
```

---

### Q14: Describe the scenario where Kafka loses messages despite `acks=all` and `replication.factor=3`.

**Answer:**

**The "Min ISR" Trap:**
Even with `acks=all`, if `min.insync.replicas` is not set, a producer will succeed with just 1 acknowledgment (the leader only).

**Scenario:**
1. Replication factor = 3
2. `min.insync.replicas` = 1 (default)
3. Leader acknowledges immediately
4. Leader crashes before replication
5. New leader elected from stale replica → data loss

**Configuration to prevent:**
```properties
# Must set this!
min.insync.replicas=2

# Producer
acks=all
retries=2147483647
enable.idempotence=true
```

**ISR shrinkage scenario:**
- Network partition isolates leader
- Followers fall behind
- ISR shrinks to just leader
- Leader crashes → data loss if unclean election enabled

**Prevention:**
```properties
# Never elect out-of-sync replica
unclean.leader.election.enable=false

# Control ISR shrinkage
replica.lag.time.max.ms=30000
replica.lag.max.messages=4000
```

---

### Q15: How do you recover from a "poison pill" message that crashes your consumer?

**Answer:**

**Poison Pill:**
A malformed message that causes deserialization/processing errors, crashing the consumer repeatedly.

**Recovery Strategies:**

**1. Dead Letter Queue (DLQ):**
```java
public void process(ConsumerRecord record) {
    try {
        processMessage(record);
    } catch (PoisonPillException e) {
        // Send to DLQ for manual inspection
        dlqProducer.send(new ProducerRecord("dlq-topic", record));
        
        // Skip this offset
        consumer.commitSync(Collections.singletonMap(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
        ));
    }
}
```

**2. Skip with logging:**
```java
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Skip to end (data loss!)

// Or manually seek
consumer.seek(new TopicPartition(topic, partition), 
    consumer.position(partition) + 1);
```

**3. Schema validation (prevention):**
```java
// Use Schema Registry
props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
props.put("schema.registry.url", "http://localhost:8081");
```

**4. Binary safe consumption:**
```java
// Use ByteArrayDeserializer, validate before parsing
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
    ByteArrayDeserializer.class.getName());

byte[] data = record.value();
if (isValidJson(data)) {
    processJson(data);
} else {
    sendToDlq(record);
}
```

---

## Performance Tuning

### Q16: How do you achieve sub-10ms latency in Kafka?

**Answer:**

**Producer optimizations:**
```properties
# Disable batching for low latency
linger.ms=0
batch.size=1

# Reduce buffering
buffer.memory=33554432
compression.type=none

# Fast acks
acks=1  # or 0 for fire-and-forget (lossy)
retries=0  # or handle in application
```

**Broker optimizations:**
```properties
# OS-level
dirty.ratio=0.3
num.io.threads=16
num.network.threads=8

# Disable unnecessary features
log.flush.interval.messages=10000
log.flush.interval.ms=1000
```

**Consumer optimizations:**
```properties
fetch.min.bytes=1
fetch.max.wait.ms=0
max.poll.records=1
```

**Infrastructure:**
- NVMe SSDs (not HDD)
- 10Gbps+ network
- Same datacenter, low RTT
- Dedicated brokers (no shared resources)

**Trade-offs:**
| Optimization | Throughput | Durability |
|--------------|-----------|------------|
| `acks=1` | Higher | Lower |
| `linger.ms=0` | Lower | Same |
| `compression=none` | Higher | Same |
| `retries=0` | Lower | Lower |

---

### Q17: What causes Kafka producer throughput to suddenly drop, and how do you diagnose it?

**Answer:**

**Common causes:**

| Symptom | Likely Cause | Diagnosis |
|---------|--------------|-----------|
| Steady degradation | Buffer exhaustion | `buffer-available-bytes` trending to 0 |
| Sudden drop | Broker unavailability | `connection-creation-rate` spike |
| Periodic drops | Log rolling/compaction | `log-flush-rate` spikes |
| Gradual increase | Network saturation | `request-latency` increasing |

**Diagnostic commands:**
```bash
# Producer metrics
kafka-producer-perf-test.sh --topic test --num-records 100000 \
    --record-size 1024 --throughput -1 \
    --producer-props bootstrap.servers=localhost:9092

# Check broker thread usage
kafka-topics.sh --describe --bootstrap-server localhost:9092

# Network analysis
iftop -i eth0
ss -tin | grep :9092
```

**Producer backpressure handling:**
```java
// Monitor buffer memory
ProducerMetrics metrics = producer.metrics();
double bufferAvail = metrics.get("buffer-available-bytes");

if (bufferAvail < BUFFER_THRESHOLD) {
    // Apply backpressure upstream
    pauseIngestion();
}

// Or use blocking send with timeout
try {
    producer.send(record).get(100, TimeUnit.MILLISECONDS);
} catch (TimeoutException e) {
    // Handle backpressure
}
```

---

### Q18: How do you tune Kafka for high-throughput batch processing (100MB/s+)?

**Answer:**

**Producer configuration:**
```properties
# Large batches
batch.size=524288  # 512KB
linger.ms=100
buffer.memory=134217728  # 128MB

# Compression
compression.type=lz4  # or zstd for better ratio

# Parallelism
max.in.flight.requests.per.connection=5
```

**Broker configuration:**
```properties
# Network threads
num.network.threads=8
num.io.threads=16

# Log settings
log.segment.bytes=1073741824  # 1GB segments
log.roll.hours=168
log.retention.hours=168

# OS-level optimizations
log.flush.interval.messages=10000
log.flush.interval.ms=1000
```

**Consumer configuration:**
```properties
fetch.min.bytes=1048576  # 1MB
fetch.max.bytes=52428800  # 50MB
max.poll.records=1000
```

**Hardware requirements:**
- 10Gbps network minimum
- NVMe SSDs in RAID 0
- 64GB+ RAM per broker
- Separate disks for log and OS

**Partition strategy:**
```java
// More partitions for parallelism
// Rule: partitions >= max(throughput/10MB/s, consumer_count)
// For 100MB/s: 10+ partitions

// Key-based routing for ordering
// Or round-robin for maximum throughput
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
    RoundRobinPartitioner.class.getName());
```

---

## Security & Multi-Tenancy

### Q19: How do you implement multi-tenancy in Kafka while maintaining isolation?

**Answer:**

**Isolation levels:**

| Approach | Isolation Level | Overhead |
|----------|-----------------|----------|
| **Separate clusters** | Complete | High (operational) |
| **Separate brokers (KRaft)** | Strong | Medium |
| **Separate topics + ACLs** | Logical | Low |
| **Topic prefixes + quotas** | Logical | Low |

**Implementation with ACLs:**
```bash
# Create tenant user
kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type users --entity-name tenant-a-user \
    --alter --add-config 'SCRAM-SHA-256=[password=secret]'

# Set ACLs for tenant A
kafka-acls.sh --bootstrap-server localhost:9092 \
    --add --allow-principal User:tenant-a-user \
    --operation Read --operation Write \
    --topic 'tenant-a-.*' --resource-pattern-type prefixed

# Set quotas
kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type users --entity-name tenant-a-user \
    --alter --add-config 'producer_byte_rate=10485760,consumer_byte_rate=20971520'
```

**Client configuration:**
```java
props.put("security.protocol", "SASL_SSL");
props.put("sasl.mechanism", "SCRAM-SHA-256");
props.put("sasl.jaas.config", 
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username='tenant-a-user' password='secret';");
```

---

### Q20: How do you secure Kafka in a zero-trust environment?

**Answer:**

**Defense in depth:**

```
┌─────────────────────────────────────────────────────────────┐
│                    Network Layer                            │
│  - TLS 1.3 for all connections                              │
│  - mTLS for inter-broker communication                      │
│  - Network segmentation (VLANs/NSGs)                          │
└─────────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────────┐
│                 Authentication Layer                        │
│  - SASL/SCRAM or OAuth (not PLAIN)                          │
│  - Certificate-based auth for services                      │
│  - Regular credential rotation                              │
└─────────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────────┐
│                  Authorization Layer                        │
│  - Fine-grained ACLs on topics, groups, clusters              │
│  - Prefix-based resource patterns                            │
│  - Regular ACL audits                                        │
└─────────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────────┐
│                    Encryption Layer                           │
│  - TLS in transit                                            │
│  - KMS integration for at-rest encryption                    │
│  - Key rotation policies                                     │
└─────────────────────────────────────────────────────────────┘
```

**Configuration:**
```properties
# Server properties
listeners=SASL_SSL://:9093
security.inter.broker.protocol=SASL_SSL
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
sasl.enabled.mechanisms=SCRAM-SHA-512

# SSL settings
ssl.keystore.location=/secure/kafka.keystore.jks
ssl.keystore.password=secure-password
ssl.key.password=secure-password
ssl.truststore.location=/secure/kafka.truststore.jks
ssl.truststore.password=secure-password
ssl.client.auth=required

# Encryption at rest
log.cleanup.policy=delete
```

**Audit logging:**
```properties
# Enable authorization logging
log4j.logger.kafka.authorizer.logger=INFO, authorizerAppender
log4j.additivity.kafka.authorizer.logger=false
```

---

## Advanced Scenarios

### Q21: How would you design a Kafka-based system for cross-region replication with RPO < 1 minute?

**Answer:**

**Architecture:**
```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│   Region A   │◄───────►│   Region B   │◄───────►│   Region C   │
│  (Primary)   │  MM2    │  (Standby)   │  MM2    │  (Disaster)  │
└──────────────┘         └──────────────┘         └──────────────┘
      │                         │                       │
      └─────────────────────────┴───────────────────────┘
                              │
                    ┌─────────────────┐
                    │  Observer Nodes  │
                    │  (KRaft-based)   │
                    └─────────────────┘
```

**MirrorMaker 2 configuration:**
```properties
# MM2 config
clusters=source, target
source.bootstrap.servers=region-a:9092
target.bootstrap.servers=region-b:9092

# Replication settings
source->target.enabled=true
source->target.topics=.*
source->target.replication.factor=3

# Low latency replication
source->target.refresh.topics.interval.seconds=10
source->target.sync.group.offsets.interval.seconds=10

# Exactly-once replication
source->target.producer.isolation.level=read_committed
```

**RPO optimization:**
```properties
# Producer (Region A)
acks=all
max.in.flight.requests.per.connection=1
enable.idempotence=true

# Broker (Region A)
min.insync.replicas=2
unclean.leader.election.enable=false

# MM2 Consumer
fetch.min.bytes=1
fetch.max.wait.ms=100
max.poll.records=100
```

**Monitoring RPO:**
```bash
# Check replication lag
kafka-consumer-groups.sh --bootstrap-server region-b:9092 \
    --group mm2-source-target --describe

# Alert if lag > 60 seconds
```

---

### Q22: Design a Kafka system that handles 10 million messages/second with 99.99% availability.

**Answer:**

**Architecture:**
```
                    ┌─────────────────────────────────────────┐
                    │           Load Balancers (L4)           │
                    └─────────────────────────────────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        │                              │                              │
   ┌────┴────┐                    ┌────┴────┐                    ┌────┴────┐
   │ Broker  │◄──────────────────►│ Broker  │◄──────────────────►│ Broker  │
   │ Rack 1  │      ISR           │ Rack 2  │      ISR           │ Rack 3  │
   └────┬────┘                    └────┬────┘                    └────┬────┘
        │                              │                              │
   ┌────┴────┐                    ┌────┴────┐                    ┌────┴────┐
   │ Broker  │                    │ Broker  │                    │ Broker  │
   │ Rack 1  │                    │ Rack 2  │                    │ Rack 3  │
   └─────────┘                    └─────────┘                    └─────────┘
        │                              │                              │
        └──────────────────────────────┼──────────────────────────────┘
                                       │
                    ┌─────────────────────────────────────────┐
                    │         ZooKeeper Ensemble (5 nodes)    │
                    └─────────────────────────────────────────┘
```

**Capacity planning:**
```
Target: 10M messages/sec
Message size: 1KB average
Throughput: 10GB/s

Partitions per topic: 1000
Replication factor: 3
Brokers needed: 30 (333K msg/s per broker)
Racks: 3 (10 brokers per rack)

Storage per day: 10GB/s × 86400s = 864TB
Retention: 7 days = 6PB raw, 18PB with replication
```

**Configuration:**
```properties
# Producer
batch.size=65536
linger.ms=5
compression.type=lz4
acks=1  # Availability over durability
retries=3
max.in.flight.requests.per.connection=5

# Broker
num.network.threads=16
num.io.threads=32
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400

num.partitions=1000
default.replication.factor=3
min.insync.replicas=2

# Rack awareness
broker.rack=rack-1  # (rack-2, rack-3 on other brokers)
```

**Availability calculation:**
```
Single broker failure: 100% available (ISR maintained)
Rack failure: 100% available (replicas in other racks)
Region failure: 0% (requires multi-region)

With 3 regions, 3 replicas per region:
- Can tolerate 2 region failures
- 99.99% = < 52 min downtime/year
```

**Failure domains:**
- Separate power feeds per rack
- Separate network switches
- Separate availability zones
- Separate regions (for DR)

---

## Summary Checklist

### Before Production

#### Configuration Verification
- [ ] `acks=all` for critical data
- [ ] `min.insync.replicas` >= 2
- [ ] `replication.factor` >= 3
- [ ] `unclean.leader.election.enable=false`
- [ ] `enable.idempotence=true` (producers)
- [ ] `enable.auto.commit=false` (consumers)
- [ ] `isolation.level=read_committed` (if using transactions)

#### Monitoring Setup
- [ ] Under-replicated partitions alert
- [ ] Consumer lag alerting
- [ ] Broker disk space alerting
- [ ] Producer error rate monitoring
- [ ] Rebalance rate tracking
- [ ] Log flush latency monitoring

#### Operational Readiness
- [ ] Runbook for broker failures
- [ ] Runbook for partition reassignment
- [ ] Backup and restore procedures
- [ ] Disaster recovery plan
- [ ] Security audit (ACLs, encryption)
- [ ] Load testing completed

---

## Quick Reference: Decision Matrix

| Scenario | Recommended Approach |
|----------|---------------------|
| Financial transactions | Exactly-once + idempotent consumers + external store |
| High-throughput metrics | At-most-once + batching + compression |
| User activity tracking | At-least-once + idempotent DB writes |
| Inventory management | Exactly-once + transactions |
| Log aggregation | At-most-once + high retention |
| Event sourcing | At-least-once + event store |

---

## Key Takeaways

1. **Message Loss Prevention**: Configure `acks=all` + `min.insync.replicas=2` + `unclean.leader.election.enable=false`

2. **Duplicate Handling**: Use idempotent producers + unique message IDs + deterministic processing

3. **Consumer Scaling**: Partition count limits parallelism; plan for growth

4. **Exactly-Once**: Only within Kafka; external systems need idempotent writes

5. **Rebalance Mitigation**: Static membership + cooperative rebalancing + proper timeout tuning

6. **Performance**: Trade-offs between latency, throughput, and durability

7. **Monitoring**: Lag, under-replication, and rebalance rate are critical metrics

8. **Security**: Defense in depth - network, auth, authorization, encryption

---

*Last Updated: 2024*
