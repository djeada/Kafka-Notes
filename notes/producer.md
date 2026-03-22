## KafkaProducer from kafka-python

KafkaProducer is the client that allows Python applications to produce messages to a Kafka cluster. Understanding its key concepts and parameters is crucial for efficient integration and message publishing.

```
+-------------------------+
|     Kafka Producer      |
|                         |
|  Produces to Topic A    |
+-------------------------+
          |
          |
          V
+-------------------------+
|     Kafka Broker        |
|                         |
|  +------------------+   |
|  |     Topic A      |   |
|  |   Partition 0    |   |
|  +------------------+   |
|                         |
|  +------------------+   |
|  |     Topic A      |   |
|  |   Partition 1    |   |
|  +------------------+   |
|                         |
|  +------------------+   |
|  |     Topic B      |   |
|  |   Partition 0    |   |
|  +------------------+   |
|                         |
|  +------------------+   |
|  |     Topic B      |   |
|  |   Partition 1    |   |
|  +------------------+   |
+-------------------------+
```

### Multi-Broker Cluster with Replication

In production, Kafka runs as a cluster of brokers. Each partition has a leader
and one or more follower replicas spread across brokers. The producer always
writes to the partition leader; followers replicate asynchronously (or
synchronously when `acks=all`).

```
+-------------------------+
|     Kafka Producer      |
|  (PID=1, Epoch=0)       |
+-------------------------+
      |          |
      |          +----------------------------+
      V                                       V
+---------------------------+   +---------------------------+
|   Broker 1 (Leader)       |   |   Broker 2                |
|                           |   |                           |
|  +---------------------+  |   |  +---------------------+  |
|  | Topic A - P0 (Lead) |  |   |  | Topic A - P0 (Repl) |  |
|  +---------------------+  |   |  +---------------------+  |
|                           |   |                           |
|  +---------------------+  |   |  +---------------------+  |
|  | Topic A - P1 (Repl) |  |   |  | Topic A - P1 (Lead) |  |
|  +---------------------+  |   |  +---------------------+  |
+---------------------------+   +---------------------------+
                                          |
              replicates to               V
                              +---------------------------+
                              |   Broker 3                |
                              |                           |
                              |  +---------------------+  |
                              |  | Topic A - P0 (Repl) |  |
                              |  +---------------------+  |
                              |                           |
                              |  +---------------------+  |
                              |  | Topic A - P1 (Repl) |  |
                              |  +---------------------+  |
                              +---------------------------+

  Lead = Leader replica   Repl = Follower replica
  replication.factor = 3  min.insync.replicas = 2
```

### Important Concepts to Understand

1. **Message Key**:
   - The key associated with the message.
   - Can affect which partition a message goes to if a custom partitioner isn't specified.
   - Messages with the same key are always routed to the same partition, guaranteeing ordering per key.

2. **Serializers**:
   - Kafka messages are sent as byte arrays. Serializers help convert Python objects or other data types into byte arrays suitable for Kafka.

3. **Acknowledgements (`acks`)**:
   - Determines how many partition replicas must receive the message before the producer gets an acknowledgment.
   - It can affect message durability and producer latency.
   - `acks=0`: Fire and forget. Lowest latency, no durability guarantee.
   - `acks=1`: Leader writes to its local log before responding. Message can be lost if the leader fails before replication.
   - `acks=all` (`-1`): Leader waits for the full set of in-sync replicas (ISR) to acknowledge. Strongest durability guarantee.

4. **Message Batching**:
   - For efficiency, the producer can batch multiple messages together.
   - The size and time of these batches can be configured.

5. **Retries**:
   - If the producer receives an error, it can retry sending the message.
   - The number of retries and the delay between them can be configured.
   - Retries can cause message reordering unless idempotence is enabled or `max_in_flight_requests_per_connection` is set to `1`.

### Key Parameters of KafkaProducer

- **bootstrap_servers**:
  - A list of `host:port` pairs for establishing the initial connection to the Kafka cluster.

- **client_id**:
  - Identifier for this producer instance, useful for logging and metrics.

- **key_serializer** & **value_serializer**:
  - Functions to serialize keys and values respectively.
  - Default is `None`, meaning no serialization.

- **acks**:
  - Acknowledgment level: `0` (no acks), `1` (leader ack), `-1` or `all` (full replica ack).

- **compression_type**:
  - Algorithm to compress messages: `gzip`, `snappy`, `lz4`, or `zstd`.

- **batch_size**:
  - The number of bytes of messages to collect before sending to Kafka.

- **linger_ms**:
  - The time producer waits before sending a batch.

- **max_request_size**:
  - The maximum size of a request in bytes.

- **buffer_memory**:
  - Total bytes of memory the producer can use to buffer records waiting to be sent. Default is `33554432` (32 MB). If the buffer is full, `send()` will block up to `max_block_ms` before raising an error.

- **max_in_flight_requests_per_connection**:
  - Maximum number of unacknowledged requests the producer will send per connection before blocking. Set to `1` to guarantee strict ordering without idempotence. Default is `5`.

- **retries**:
  - Number of times to retry a failed send request. Combined with `retry_backoff_ms` to control retry behavior.

- **enable_idempotence**:
  - When `True`, the producer ensures exactly-once delivery per partition by assigning a producer ID and sequence number to each message. Requires `acks=all` and `retries > 0`.

- **transactional_id**:
  - A unique identifier for the transactional producer. Setting this enables the transactions API and implies `enable_idempotence=True`.

### KafkaProducer Lifecycle

1. **Initialization**:
   - The producer is instantiated with the desired parameters.

2. **Message Production**:
   - Messages are sent to topics using the `send()` method. IMPORTANT: ASYNC

3. **Flushing**:
   - Ensure all messages are sent with the `flush()` method.

4. **Shutdown**:
   - Close the producer gracefully using the `close()` method to ensure all messages are delivered and resources are released.

### Example Usage

```python
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: str(v).encode('utf-8'))

producer.send('my-topic', value='Hello, Kafka!')
producer.flush()
producer.close()
```

### Idempotent Producers

Idempotent delivery guarantees that retries do not result in duplicate messages
within a single partition. Under the hood, the broker assigns each producer a
unique **Producer ID (PID)** and tracks a **sequence number** per
topic-partition pair. If a retried batch carries a sequence number the broker
has already seen, the duplicate is silently discarded.

Requirements:
- `enable_idempotence=True`
- `acks` must be `all`
- `retries` must be greater than `0`
- `max_in_flight_requests_per_connection` must be `<= 5`

```
+---------------------------+         +---------------------------+
|   Idempotent Producer     |         |   Broker (Leader)         |
|   PID=42  Epoch=0         |         |                           |
+---------------------------+         |   Sequence Tracker:       |
   | send(key=A, seq=0)     |         |     PID=42, P0 -> seq=0  |
   |------------------------>|         |                           |
   |                         |  ack    |   Writes msg (seq=0)     |
   |<------------------------|         |                           |
   |                         |         |                           |
   | retry(key=A, seq=0)    |         |   seq=0 already seen     |
   |------------------------>|         |   -> duplicate discarded  |
   |                         |  ack    |                           |
   |<------------------------|         +---------------------------+
```

```python
from kafka import KafkaProducer

# Idempotent producer — no duplicates on retry
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=5,
    max_in_flight_requests_per_connection=5,
    enable_idempotence=True,
    value_serializer=lambda v: str(v).encode('utf-8'),
)

for i in range(100):
    producer.send('orders', key=b'order-key', value=f'order-{i}')

producer.flush()
producer.close()
```

### Transactional Producers

Transactions extend idempotence across multiple partitions and topics. A
transactional producer groups a set of sends into an atomic unit: either all
messages are committed and visible to consumers (with `isolation.level=
read_committed`) or none are.

Key methods:
- `init_transactions()` — registers the `transactional.id` with the broker and
  fences any previous producer instance sharing the same ID.
- `begin_transaction()` — starts a new transaction.
- `send()` — enqueues messages within the transaction boundary.
- `commit_transaction()` — commits all messages in the current transaction.
- `abort_transaction()` — rolls back the current transaction.

```
  begin_transaction()
        |
        V
  send(topic_a, msg1)
  send(topic_b, msg2)
        |
        +--- success? ---> commit_transaction()
        |
        +--- failure? ---> abort_transaction()
```

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    transactional_id='my-transactional-id',
    acks='all',
    value_serializer=lambda v: str(v).encode('utf-8'),
)

producer.init_transactions()

try:
    producer.begin_transaction()
    producer.send('debits', value='debit-100')
    producer.send('credits', value='credit-100')
    producer.commit_transaction()
except Exception:
    producer.abort_transaction()
finally:
    producer.close()
```

### Custom Partitioners

By default, kafka-python uses a **murmur2 hash** of the serialized key to
choose a partition:

```
partition = murmur2(key_bytes) % num_partitions
```

If the key is `None`, messages are distributed using a round-robin (or sticky)
strategy across available partitions.

You can supply a custom partitioner function to `KafkaProducer`. The function
receives the key, all partitions, and the currently available partitions, and
must return a single partition number.

```python
import random
from kafka import KafkaProducer

def priority_partitioner(key, all_partitions, available_partitions):
    """Route high-priority messages to partition 0, others randomly."""
    if key and key == b'high':
        return all_partitions[0]
    return random.choice(available_partitions)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    partitioner=priority_partitioner,
    value_serializer=lambda v: str(v).encode('utf-8'),
)

producer.send('events', key=b'high', value='critical event')
producer.send('events', key=b'low', value='normal event')
producer.flush()
producer.close()
```

### Callbacks and Error Handling

`send()` returns a `FutureRecordMetadata` object. You can handle results
either synchronously by calling `.get()` or asynchronously by registering
callback functions.

**Synchronous (blocking) approach** — `future.get()`:

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: str(v).encode('utf-8'),
)

future = producer.send('my-topic', value='important message')
try:
    record_metadata = future.get(timeout=10)
    print(f"Sent to {record_metadata.topic} "
          f"partition {record_metadata.partition} "
          f"offset {record_metadata.offset}")
except Exception as e:
    print(f"Send failed: {e}")

producer.close()
```

**Asynchronous (non-blocking) approach** — callbacks:

```python
from kafka import KafkaProducer

def on_send_success(record_metadata):
    print(f"Delivered to {record_metadata.topic} "
          f"[{record_metadata.partition}] @ offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Error producing message: {excp}")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: str(v).encode('utf-8'),
)

producer.send('my-topic', value='async message') \
    .add_callback(on_send_success) \
    .add_errback(on_send_error)

# flush ensures callbacks fire before the program exits
producer.flush()
producer.close()
```

### Performance Tuning Guidelines

Balancing latency and throughput is a core producer tuning exercise. The
following parameters interact closely:

| Parameter | Effect when increased | Trade-off |
|---|---|---|
| `batch_size` | Larger batches, better throughput | Higher per-message latency; more memory per partition |
| `linger_ms` | Producer waits longer to fill batches | Higher latency, better batching efficiency |
| `buffer_memory` | More room to buffer unsent records | Higher memory footprint |
| `compression_type` | Smaller payloads on the wire | CPU overhead for compression/decompression |
| `max_in_flight_requests_per_connection` | More pipelining, higher throughput | Risk of reordering if retries occur (mitigated by idempotence) |

**General guidelines**:

1. **Low latency** — keep `linger_ms=0` (default) and `batch_size` moderate.
   Messages are sent as soon as possible.
2. **High throughput** — increase `linger_ms` (e.g., `5`–`100`) so the
   producer has time to fill batches. Increase `batch_size` (e.g.,
   `65536`–`524288`). Enable `compression_type='lz4'` for a good
   compression-to-CPU ratio.
3. **Ordering guarantees** — set `max_in_flight_requests_per_connection=1` or
   enable idempotence (`enable_idempotence=True`) which safely allows up to 5
   in-flight requests while preserving order.
4. **Back-pressure** — if the producer generates messages faster than the
   broker can accept, `buffer_memory` will fill up. Increase it or slow down
   production. Monitor `buffer-available-bytes` metrics.

```python
from kafka import KafkaProducer

# High-throughput producer configuration
producer = KafkaProducer(
    bootstrap_servers=['broker1:9092', 'broker2:9092', 'broker3:9092'],
    acks='all',
    enable_idempotence=True,
    compression_type='lz4',
    batch_size=262144,       # 256 KB batches
    linger_ms=50,            # wait up to 50 ms to fill a batch
    buffer_memory=67108864,  # 64 MB send buffer
    max_in_flight_requests_per_connection=5,
    value_serializer=lambda v: str(v).encode('utf-8'),
)

for i in range(100000):
    producer.send('high-volume-topic', value=f'event-{i}')

producer.flush()
producer.close()
```

## KafkaProducer Common Pitfalls & Troubleshooting

1. **Message Not Delivered**:
   - **Problem**: Messages are not appearing in the Kafka topic.
   - **Solution**: Review the `acks` setting; if it's set to `0`, there are no guarantees of delivery. Check the broker logs and network connectivity.

2. **Serialization Issues**:
   - **Problem**: Errors arise during the serialization of messages.
   - **Solution**: Ensure that the `key_serializer` and `value_serializer` match the data types you're producing. Implement adequate logging to capture serialization exceptions.

3. **Performance Bottlenecks**:
   - **Problem**: Producer throughput is slower than expected.
   - **Solution**: Adjust batch settings (`batch_size`, `linger_ms`). Consider enabling message compression using the `compression_type` parameter.

4. **High Retry Rates**:
   - **Problem**: Frequent retries when sending messages.
   - **Solution**: Check the health and load of the Kafka brokers. Consider increasing the `retry.backoff.ms` to give more time between retries.

5. **Resource Leaks**:
   - **Problem**: Producer instances consuming more resources over time.
   - **Solution**: Always close the producer using the `close()` method after sending messages to free up resources.

6. **Duplicate Messages on Retry**:
   - **Problem**: Retries produce duplicate records in the topic.
   - **Solution**: Enable `enable_idempotence=True` so the broker deduplicates retried batches using the producer ID and sequence number.

7. **Out-of-Order Messages**:
   - **Problem**: Messages arrive at the broker in a different order than they were sent.
   - **Solution**: Set `max_in_flight_requests_per_connection=1` or enable idempotence, which safely handles up to 5 in-flight requests while preserving order.

## KafkaProducer Best Practices

1. **Tune for Throughput**:
   - Adjust batching settings (`batch_size`, `linger_ms`) and enable message compression (`compression_type`) to maximize throughput.

2. **Ensure Reliable Delivery**:
   - Set the `acks` parameter to `all` or `-1` to ensure messages are stored by all replicas before acknowledgment. Monitor the `send()` method's return value or use callbacks to handle potential errors.

3. **Handle Exceptions**:
   - Implement proper exception handling for potential serialization errors, timeouts, and broker connection issues.

4. **Use Callbacks for Asynchronous Producing**:
   - Instead of waiting for the `send()` method, use callbacks to handle successful message deliveries and possible failures, allowing for non-blocking message production.

5. **Monitor Metrics**:
   - Regularly monitor producer metrics like request rate, request latency, and batch size to detect and troubleshoot potential issues.

6. **Graceful Shutdown**:
   - Always terminate the producer instance with the `close()` method to ensure all buffered and in-flight messages are sent and to release all associated resources.

7. **Enable Idempotence by Default**:
   - Unless you have a specific reason not to, set `enable_idempotence=True`. It prevents duplicates on retry with negligible overhead.

8. **Use Transactions for Cross-Partition Atomicity**:
   - When your application must produce to multiple topics/partitions atomically (e.g., debit and credit), wrap sends in a transaction using `transactional_id`.
