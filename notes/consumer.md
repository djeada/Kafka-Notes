## KafkaConsumer from kafka-python

KafkaConsumer is the client that allows Python applications to consume messages from a Kafka cluster. It is essential to understand its key concepts and parameters for an effective integration.

```
+-------------------------------- Kafka Broker --------------------------------+
|                                                                              |
|  +------------------+    +------------------+    +------------------+        |
|  |     Topic A      |    |     Topic A      |    |     Topic B      |        |
|  |   Partition 0    |    |   Partition 1    |    |   Partition 0    |        |
|  +------------------+    +------------------+    +------------------+        |
|                                                                              |
|  +------------------+    +------------------+    +------------------+        |
|  |     Topic B      |    |     Topic C      |    |     Topic C      |        |
|  |   Partition 1    |    |   Partition 0    |    |   Partition 1    |        |
|  +------------------+    +------------------+    +------------------+        |
+------------------------------------------------------------------------------+
        |                                         |
        |                                         |
        V                                         V
+-----------------------------+      +-----------------------------+
|       Kafka Consumer 1      |      |       Kafka Consumer 2      |
|                             |      |                             |
|  Reads from Topic A,        |      |  Reads from Topic B,        |
|  Partition 0                |      |  Partition 1                |
|                             |      |                             |
+-----------------------------+      +-----------------------------+
```

groups
```
          |
          |
          V
+-----------------------------+
|    Kafka Consumer Group     |
|                             |
|  +-----------------------+  |
|  |   Consumer 1          |  |
|  |   Reads from          |  |
|  |   Topic A, Partition 0|  |
|  +-----------------------+  |
|                             |
|  +-----------------------+  |
|  |   Consumer 2          |  |
|  |   Reads from          |  |
|  |   Topic A, Partition 1|  |
|  +-----------------------+  |
```

rebalancing (consumer 3 joins the group)
```
BEFORE rebalance                          AFTER rebalance
+-------------------------------+         +-------------------------------+
|    Consumer Group "my-group"  |         |    Consumer Group "my-group"  |
|                               |         |                               |
|  +-----------+  +-----------+ |         | +---------+ +---------+      |
|  | Consumer 1|  | Consumer 2| |         | |Consumer1| |Consumer2|      |
|  | P0, P1    |  | P2, P3    | |         | | P0, P1  | | P2      |      |
|  +-----------+  +-----------+ |         | +---------+ +---------+      |
|                               |         |                               |
|                               |  --->   |           +---------+         |
|                               |         |           |Consumer3|         |
|                               |         |           | P3      |         |
|                               |         |           +---------+         |
+-------------------------------+         +-------------------------------+

BEFORE rebalance (consumer 2 leaves)      AFTER rebalance
+-------------------------------+         +-------------------------------+
|    Consumer Group "my-group"  |         |    Consumer Group "my-group"  |
|                               |         |                               |
| +---------+ +---------+      |         | +---------+ +---------+      |
| |Consumer1| |Consumer2|      |         | |Consumer1| |Consumer3|      |
| | P0, P1  | | P2      |      |         | | P0, P1  | | P2, P3  |      |
| +---------+ +---------+      |         | +---------+ +---------+      |
|                               |  --->   |                               |
|           +---------+         |         |                               |
|           |Consumer3|         |         |                               |
|           | P3      |         |         |                               |
|           +---------+         |         |                               |
+-------------------------------+         +-------------------------------+
```

### Important Concepts to Understand

1. **Consumer Groups**:
   - Kafka lets consumers organize into consumer groups for better scalability and manageability.
   - All consumers within a group share the same group ID.
   - Each message from a topic is delivered to only one consumer within the group, ensuring that no message is processed twice.

2. **Offsets**:
   - Every record in a Kafka partition has a unique sequential id called an offset.
   - The `KafkaConsumer` can begin reading messages from any specified offset, allowing for message reprocessing or skipping.

3. **Partition Assignment**:
   - Kafka topics are divided into partitions to enable parallel processing.
   - A consumer can either:
     - Be assigned specific partitions manually.
     - Subscribe to topics, leaving Kafka to manage the partition assignment.

4. **Polling**:
   - The consumer doesn't receive messages automatically. Instead, it continuously polls the Kafka server to fetch new messages.

5. **Deserializers**:
   - As Kafka messages are essentially byte arrays, deserializers help in converting these byte arrays into useful Python objects or other data types.

6. **Auto-commit**:
   - By default, the consumer commits the offsets of messages it has read automatically.
   - However, users can control this feature, enabling or disabling auto-commit as needed.

### Key Parameters of KafkaConsumer

- **bootstrap_servers**: 
  - A list of `host:port` pairs for establishing the initial connection to the Kafka cluster.

- **client_id**: 
  - Identifier for this consumer instance, useful for logging and metrics.

- **group_id**: 
  - Specifies the consumer group this consumer will be part of.

- **key_deserializer** & **value_deserializer**:
  - Functions to deserialize keys and values respectively.
  - Default is `None`, meaning no deserialization.

- **auto_offset_reset**: 
  - What to do when there's no initial offset or if the current offset is invalid. 
  - Choices: `earliest` (start from the beginning), `latest` (start from the most recent), or `none` (throw an exception).

- **enable_auto_commit**:
  - If set to `True`, the consumer's offset is committed in the background periodically.
  - Defaults to `True`.

- **auto_commit_interval_ms**: 
  - Specifies the interval (in milliseconds) at which offsets are committed to Kafka if `enable_auto_commit` is `True`.

- **max_poll_records**: 
  - Defines the maximum number of records that will be returned in one `poll()` call.

- **session_timeout_ms**: 
  - The time (in milliseconds) the broker waits for a consumer heartbeat before considering it dead and reassigning its partitions to other consumers.

- **isolation_level**:
  - Controls how transactional messages are read. Set to the string `'read_uncommitted'` (default) to return all messages, or `'read_committed'` to return only committed transactional messages.

- **partition_assignment_strategy**:
  - List of objects to use to distribute partition ownership among consumer group members. Defaults to `[RangePartitionAssignor, RoundRobinPartitionAssignor]`.

## KafkaConsumer Lifecycle

1. **Initialization**:
   - The consumer is created using the KafkaConsumer class with desired parameters.
   
2. **Subscription**:
   - The consumer subscribes to desired topics using the `subscribe()` method.

3. **Polling Loop**:
   - Continuously polls for new messages and processes them.

4. **Shutdown**:
   - It's essential to close the consumer gracefully using the `close()` method to ensure that offsets are committed and resources are released.

### Example Usage

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer('my-topic',
                         bootstrap_servers='localhost:9092',
                         group_id='my-group',
                         value_deserializer=lambda x: x.decode('utf-8'))

for message in consumer:
    print(message.value)
```

### Manual Offset Management

When `enable_auto_commit` is set to `False`, the application is responsible for committing offsets. This gives fine-grained control over when a message is considered "processed".

Key methods:

- **commit()** — Synchronously commits the latest consumed offsets (or specified offsets). Blocks until the broker confirms.
- **commit_async()** — Asynchronously commits offsets. Accepts an optional callback for success/failure notification.
- **seek(partition, offset)** — Manually set the fetch offset for a specific partition. The next `poll()` will start from this offset.
- **position(partition)** — Returns the next offset that will be fetched for the given partition.
- **committed(partition)** — Returns the last committed offset for the given partition.

```python
from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    enable_auto_commit=False,
    value_deserializer=lambda x: x.decode('utf-8')
)

for message in consumer:
    try:
        process(message.value)
        # Synchronous commit after each message
        consumer.commit()
    except Exception as e:
        print(f"Processing failed: {e}")
        # Do not commit — message will be redelivered
```

Batch commit example for higher throughput:

```python
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    enable_auto_commit=False,
    value_deserializer=lambda x: x.decode('utf-8')
)

batch_size = 100
count = 0

for message in consumer:
    process(message.value)
    count += 1

    if count % batch_size == 0:
        consumer.commit_async(callback=lambda offsets, err: (
            print(f"Commit failed: {err}") if err else None
        ))

# Final synchronous commit before shutdown
consumer.commit()
consumer.close()
```

Using `seek()` to replay messages from a specific offset:

```python
from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    enable_auto_commit=False
)

tp = TopicPartition('my-topic', 0)
consumer.assign([tp])

# Wait for assignment then seek
consumer.seek(tp, 42)

print(f"Next offset to fetch: {consumer.position(tp)}")
print(f"Last committed offset: {consumer.committed(tp)}")

for message in consumer:
    print(f"offset={message.offset} value={message.value}")
```

### Partition Assignment Strategies

When consumers use `subscribe()`, Kafka assigns partitions using a configurable strategy. The group coordinator triggers a rebalance when consumers join or leave.

1. **RangeAssignor** (default):
   - Assigns partitions on a per-topic basis.
   - Orders partitions numerically and consumers lexicographically, then divides partitions into contiguous ranges.
   - Can lead to uneven distribution when the partition count is not evenly divisible by the consumer count.

2. **RoundRobinAssignor**:
   - Assigns all partitions from all subscribed topics in a round-robin fashion.
   - Produces a more even distribution than Range when consumers subscribe to the same set of topics.

3. **StickyAssignor**:
   - Aims for a balanced assignment like RoundRobin but also minimizes partition movement during rebalances.
   - Consumers keep as many of their previously assigned partitions as possible.

4. **CooperativeStickyAssignor**:
   - Same balancing goals as Sticky but uses the cooperative rebalance protocol.
   - Instead of revoking all partitions at once (eager protocol), only partitions that need to move are revoked.
   - Reduces the "stop-the-world" effect during rebalances and is recommended for production use.

```python
from kafka import KafkaConsumer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    partition_assignment_strategy=[RoundRobinPartitionAssignor]
)
```

### Static Group Membership

By default, every time a consumer restarts it gets a new member ID and triggers a full rebalance. Static group membership solves this by assigning a persistent identity to each consumer via `group.instance.id`.

- When a consumer with a `group.instance.id` disconnects, the broker does **not** immediately trigger a rebalance. It waits for `session.timeout.ms` to expire.
- If the consumer reconnects before the timeout, it reclaims its previous partition assignment without any rebalance.
- This is especially useful for rolling deployments and transient restarts.

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    group_instance_id='consumer-host-1',
    session_timeout_ms=45000
)
```

When to use static membership:
- Rolling restarts or deployments where consumers cycle quickly.
- Environments where frequent rebalances are expensive (large state, many partitions).
- Kubernetes pods with stable identities (StatefulSets).

### Exactly-Once Consumption

Kafka supports exactly-once semantics (EOS) for consumers working alongside transactional producers. The key is using the `read_committed` isolation level.

- **read_uncommitted** (default): The consumer sees all messages, including those from aborted transactions.
- **read_committed**: The consumer only sees messages from committed transactions. Aborted transaction messages are filtered out.

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    isolation_level='read_committed',
    enable_auto_commit=False
)
```

For true exactly-once processing, the typical pattern is:
1. Consume messages with `isolation_level='read_committed'`.
2. Process and write results to an external system (e.g., a database) in a transaction.
3. Store the consumer offset alongside the output in the same transaction.
4. On restart, read the last committed offset from the external store and use `seek()` to resume.

```python
from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='eos-group',
    isolation_level='read_committed',
    enable_auto_commit=False
)

tp = TopicPartition('my-topic', 0)
consumer.assign([tp])

# Recover offset from external store (e.g., database)
saved_offset = get_offset_from_database(tp)
if saved_offset is not None:
    consumer.seek(tp, saved_offset + 1)

for message in consumer:
    with database_transaction() as txn:
        process_and_store(txn, message)
        save_offset_to_database(txn, tp, message.offset)
```

### Consumer Lag and Monitoring

Consumer lag is the difference between the latest offset in a partition (log-end offset) and the consumer group's last committed offset. It indicates how far behind a consumer is.

```
Partition Log:
  Offset:  0   1   2   3   4   5   6   7   8   9  10  11  12
           [---consumed---] [-------lag-------] [--not yet produced--]
                            ^                   ^
                    committed offset       log-end offset
                         (5)                  (10)
                            lag = 10 - 5 = 5
```

**What causes lag:**
- Slow message processing logic (CPU-bound or blocking I/O).
- Insufficient consumer instances relative to partition count.
- Frequent rebalances disrupting consumption.
- Network latency between consumers and brokers.
- Burst traffic producing messages faster than consumption rate.

**How to monitor lag:**
- Use the `kafka-consumer-groups.sh` CLI tool bundled with Kafka.
- Query the consumer offsets programmatically within your application.
- Export lag metrics to monitoring systems (Prometheus, Datadog, etc.).

```python
from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group'
)
consumer.subscribe(['my-topic'])
consumer.poll(0)  # trigger assignment

for tp in consumer.assignment():
    committed = consumer.committed(tp)
    end_offset = consumer.end_offsets([tp])[tp]
    lag = end_offset - (committed if committed else 0)
    print(f"{tp.topic}-{tp.partition}: committed={committed} "
          f"end={end_offset} lag={lag}")

consumer.close()
```

**Remediation strategies:**
- Scale out by adding more consumers (up to the number of partitions).
- Increase `max_poll_records` to fetch larger batches per poll.
- Offload heavy processing to a thread pool or worker queue.
- Increase partition count to allow more parallelism.

### Standalone Consumer (Manual Assignment)

Instead of using `subscribe()` and letting Kafka manage partition assignment, you can use `assign()` to manually control which partitions a consumer reads from. This is called a standalone or unmanaged consumer.

**assign() vs subscribe():**

| Feature                 | subscribe()             | assign()                |
|-------------------------|-------------------------|-------------------------|
| Group management        | Automatic               | None                    |
| Rebalancing             | Yes                     | No                      |
| Requires group_id       | Yes                     | Optional                |
| Partition control       | Kafka decides           | You decide              |
| Offset commits          | Group-coordinated       | Manual or self-managed  |

When to use `assign()`:
- You need deterministic partition-to-consumer mapping.
- Your application manages its own partition distribution (e.g., external orchestrator).
- You want to read from specific partitions without group coordination overhead.
- Building tooling that reads from a known partition (e.g., log tailer, offset inspector).

```python
from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    enable_auto_commit=False,
    value_deserializer=lambda x: x.decode('utf-8')
)

# Manually assign partitions 0 and 2 of 'my-topic'
partitions = [TopicPartition('my-topic', 0), TopicPartition('my-topic', 2)]
consumer.assign(partitions)

# Optionally seek to a specific starting point
for tp in partitions:
    consumer.seek_to_beginning(tp)

for message in consumer:
    print(f"partition={message.partition} offset={message.offset} "
          f"value={message.value}")
```

Important: `assign()` and `subscribe()` are mutually exclusive. Calling one clears the other. Do not mix them on the same consumer instance.

## Common Pitfalls & Troubleshooting

1. **Not Setting `group_id`**:
   - Problem: Without setting a `group_id`, every instance of the consumer will read every message in the topic, which can result in duplicated message processing.
   - Solution: Always specify a `group_id` when setting up a KafkaConsumer, especially if you intend to have multiple consumer instances.

2. **Consumer Lagging Behind**:
   - Problem: The consumer isn't processing messages as quickly as they arrive. This can be due to slow processing logic or misconfigurations.
   - Solution: Monitor the consumer lag, optimize message processing logic, or increase the number of consumer instances.

3. **Offset Management Issues**:
   - Problem: Mismanagement of offsets can lead to reprocessing of the same messages or even missing some.
   - Solution: Ensure `enable_auto_commit` is appropriately configured or manage offsets manually with caution.

4. **Message Deserialization Errors**:
   - Problem: Incorrect deserialization functions or corrupted messages can lead to exceptions during message consumption.
   - Solution: Ensure `key_deserializer` and `value_deserializer` are correctly set. Implement error handling to deal with corrupted messages gracefully.

5. **Connection Issues with Kafka Brokers**:
   - Problem: KafkaConsumer can't establish a connection due to network issues, misconfigurations, or broker unavailability.
   - Solution: Verify the `bootstrap_servers` parameter and ensure network connectivity. Check broker health and logs for more insights.

6. **Frequent Rebalances**:
   - Problem: Consumers repeatedly join and leave the group, causing partitions to be reassigned and temporarily halting consumption.
   - Solution: Increase `session_timeout_ms` and `max_poll_interval_ms`. Use static group membership (`group_instance_id`) for stable consumers. Ensure `poll()` is called frequently enough.

7. **Mixing assign() and subscribe()**:
   - Problem: Calling both `assign()` and `subscribe()` on the same consumer causes an `IllegalStateError`.
   - Solution: Choose one approach per consumer instance. Use `subscribe()` for group-managed consumers and `assign()` for standalone consumers.

## Best Practices

1. **Incorporate Error Handling**:
   - To ensure smooth consumption, implement error handling. It ensures that sporadic issues don't halt the entire consumption process.

2. **Commit Offsets After Processing**:
   - Committing the offset after successfully processing a message ensures that a consumer doesn't reprocess the same message upon restarts.

3. **Parallelize Processing**:
   - Use multiple threads or processes for CPU-bound tasks. It can significantly improve the message processing rate.

4. **Tune Consumer Configurations**:
   - Regularly review and adjust consumer configurations like `max_poll_records`, `session_timeout_ms`, etc., based on the changing workload and performance metrics.

5. **Monitor Consumer Health**:
   - Use tools and frameworks to monitor consumer health, throughput, and lag. Being proactive can help detect and rectify issues before they become critical.

6. **Graceful Shutdown**:
   - Ensure that the consumer shuts down gracefully, releasing resources, and committing offsets. It helps in smooth restarts and resource management.

7. **Use Static Membership for Stable Consumers**:
   - Assign `group_instance_id` to consumers that restart frequently (e.g., during deployments) to avoid unnecessary rebalances.

8. **Prefer Cooperative Rebalancing**:
   - Use `CooperativeStickyAssignor` to minimize disruption during rebalances. Consumers keep their existing partitions while only the necessary partitions are moved.

9. **Track Consumer Lag Continuously**:
   - Expose consumer lag as a metric in your monitoring system. Set alerts for when lag exceeds acceptable thresholds so you can respond before data freshness degrades.
