
## Kafka Streams with kafka-python

*and Stream Processing Concepts*

Kafka Streams allows for the real-time processing of data within Kafka. This document covers the core concepts behind stream processing, the Kafka Streams architecture, windowing strategies, exactly-once semantics, and ksqlDB. It also includes practical implementation patterns using kafka-python.

**Note:** For production stream processing, consider using the native Java Kafka Streams library or ksqlDB, which provide built-in support for stateful operations, windowing, and exactly-once semantics. The patterns in the implementation section demonstrate how to approximate stream processing using kafka-python.

---

### Stream Processing Topology

A Kafka Streams application is modeled as a **topology** — a directed acyclic graph of source nodes, processor nodes, and sink nodes. Data flows from source topics through a chain of transformations and lands in sink topics.

```
                      +--------------------+
                      |   Source Topic(s)  |
                      +---------+----------+
                                |
                     consume records
                                |
                                v
                  +-------------+-------------+
                  |     Source Processor       |
                  |  (deserialize records)     |
                  +-------------+-------------+
                                |
                                v
                  +-------------+-------------+
                  |   Stream Processor Node    |
                  |  (filter, map, aggregate)  |
                  +------+------------+-------+
                         |            |
                         v            v
              +----------+--+   +----+---------+
              |  Processor  |   |  Processor   |
              |  (enrich)   |   |  (branch)    |
              +------+------+   +------+-------+
                     |                 |
                     v                 v
              +------+------+   +------+-------+
              | Sink        |   | Sink         |
              | Processor   |   | Processor    |
              | (serialize) |   | (serialize)  |
              +------+------+   +------+-------+
                     |                 |
                     v                 v
              +------+------+   +------+-------+
              | Sink Topic  |   | Sink Topic   |
              |    (A)      |   |    (B)       |
              +-------------+   +--------------+
```

Each processor node receives one record at a time, applies its logic, and forwards zero or more records downstream. This forms the basis of all Kafka Streams and ksqlDB operations.

---

### Stream Processing Concepts

1. **What is Stream Processing?**
   - Stream processing is the continuous, real-time computation over unbounded sequences of events. Unlike batch processing, which operates on finite datasets at scheduled intervals, stream processing handles each event as it arrives.
   - A stream is an ordered, replayable, and fault-tolerant sequence of immutable records. Each record consists of a key, a value, and a timestamp.

2. **Event Time vs Processing Time**
   - **Event time**: The timestamp embedded in the record when the event actually occurred at the source. This is the most meaningful time for business logic.
   - **Processing time**: The wall-clock time when the record is processed by the stream application. This can differ significantly from event time due to network delays, consumer lag, or reprocessing.
   - **Ingestion time**: The timestamp assigned by the Kafka broker when the record is appended to the partition log. This sits between event time and processing time.
   - Kafka Streams uses event time by default (via `TimestampExtractor`) for windowing and joins. Choosing the wrong time semantics can lead to incorrect aggregation results.

3. **At-Least-Once vs Exactly-Once Semantics**
   - **At-most-once**: Records may be lost but are never reprocessed. Achieved by committing offsets before processing.
   - **At-least-once**: Records are never lost but may be reprocessed on failure. Achieved by committing offsets after processing. This is the default for most Kafka consumer setups.
   - **Exactly-once**: Each record is processed exactly one time, even in the presence of failures. Kafka achieves this through a combination of idempotent producers, transactions, and consumer offset management. See the Exactly-Once Semantics section below.

---

### Kafka Streams Overview

Kafka Streams is a client library for building stream processing applications on top of Kafka. It runs inside your application process — no separate cluster required.

1. **KStream**
   - Represents an unbounded stream of records where each record is an independent event.
   - Think of it as an append-only log — a new record with the same key does not replace the old one.
   - Operations: `filter`, `map`, `flatMap`, `branch`, `merge`, `groupByKey`, `join`.

2. **KTable**
   - Represents a changelog stream where each record is an update to a key.
   - Only the latest value for each key is retained. A `null` value deletes the key (tombstone).
   - Internally backed by a **state store** that materializes the latest state.

3. **GlobalKTable**
   - Similar to KTable but replicated in full on every application instance.
   - Useful for small reference datasets (e.g., country codes, config lookups) where you need to join against the complete dataset regardless of partitioning.
   - Not partitioned — every instance has every key.

4. **Topology**
   - The directed graph of processors that defines your stream processing logic.
   - Built programmatically using the `StreamsBuilder` or the lower-level `Topology` API.
   - Kafka Streams partitions the topology across application instances for parallel processing.

5. **Processing Guarantees**
   - Configurable via `processing.guarantee`: `at_least_once` (default) or `exactly_once_v2`.
   - Exactly-once wraps each task's processing into a Kafka transaction that atomically commits output records and consumer offsets.

6. **State Stores**
   - Local key-value stores (backed by RocksDB by default) used by stateful operations like aggregations, joins, and windowing.
   - State is fault-tolerant: each state store has a corresponding **changelog topic** in Kafka that records every state mutation.
   - On failure, a new instance restores state by replaying the changelog topic.

7. **Changelog Topics**
   - Automatically created internal topics that back each state store.
   - Named `<application.id>-<store-name>-changelog`.
   - Compacted to retain only the latest value per key, keeping storage bounded.

8. **Interactive Queries**
   - Allow external applications to query the state stores of a running Kafka Streams instance.
   - Useful for building read-only APIs on top of materialized views without writing results to a separate database.
   - Queries can be local (same instance) or distributed across instances using metadata about which instance owns which key.

---

### Windowing Types

Windowing groups records into finite time buckets for aggregation. Kafka Streams supports four windowing strategies.

#### Tumbling Windows

Fixed-size, non-overlapping windows. Each record belongs to exactly one window.

```
Time  -->  0s     10s     20s     30s     40s
           |       |       |       |       |
           +-------+-------+-------+-------+
           | Win 1 | Win 2 | Win 3 | Win 4 |
           +-------+-------+-------+-------+

Window size = 10s
Record at t=7s  -> Win 1
Record at t=15s -> Win 2
Record at t=20s -> Win 3
```

- Best for periodic aggregations (e.g., count events per minute).
- No duplicate counting — windows do not overlap.

#### Hopping Windows

Fixed-size windows that advance by a configurable hop interval. Windows overlap, so a single record can belong to multiple windows.

```
Time  -->  0s    5s    10s   15s   20s   25s
           |     |     |     |     |     |
           +-----+-----+
           |   Win 1   |
           +-----+-----+-----+
                 |   Win 2   |
                 +-----+-----+-----+
                       |   Win 3   |
                       +-----+-----+-----+
                             |   Win 4   |
                             +-----+-----+

Window size = 10s, Hop = 5s
Record at t=7s  -> Win 1, Win 2
Record at t=12s -> Win 2, Win 3
```

- Useful for smoothed or overlapping aggregations (e.g., moving averages).
- When hop equals window size, it behaves like a tumbling window.

#### Sliding Windows

Used exclusively for join operations. A sliding window captures all records that fall within a specified time difference of each other.

```
                    join window
             |<---- grace ---->|
             |                 |
Record A: ---+---[t=100]------+---
             |   ^             |
             |   |  within     |
             |   |  window     |
Record B: ---+---+--[t=105]---+---
             |                 |
             90s             110s

Window = +/- 10s around each record
Record A at t=100 joins with Record B at t=105
because |100 - 105| = 5 <= 10
```

- Defined by a time difference rather than fixed boundaries.
- Commonly used in stream-stream joins.

#### Session Windows

Dynamic windows driven by activity. A session closes after a configurable **inactivity gap**. If a new record arrives within the gap, the session extends.

```
Time  -->  0s  3s  5s       15s       25s 27s 30s
           |   |   |         |         |   |   |
           +---+---+         +---------+---+---+
           | Sess 1|         |   Session 2     |
           +-------+         +---------+-------+
              ^                    ^
              |                    |
         gap < 10s            gap < 10s
         (3s, 2s)             (10s gap closes
                               Sess 1, then
                               new activity)

Inactivity gap = 10s
Records at t=0,3,5 -> Session 1 [0-5]
No records for 10s  -> Session 1 closes
Records at t=15,25,27,30 -> Session 2 [15-30]
```

- Ideal for user session tracking, clickstream analysis, and activity-based grouping.
- Window boundaries are determined by the data, not by fixed time intervals.

---

### Exactly-Once Semantics in Streams

Exactly-once semantics (EOS) ensures that each input record is processed and its effects are reflected in output topics and state stores exactly one time, even when failures occur.

1. **Configuration**
   - Set `processing.guarantee` to `exactly_once_v2` (recommended for Kafka 2.5+).
   - The older `exactly_once` setting uses one transactional producer per input partition; `exactly_once_v2` uses one per task, which is more efficient.

2. **How It Works**
   - **Idempotent producers**: Each producer is assigned a Producer ID (PID) and sequence number. The broker deduplicates writes with the same PID and sequence, preventing duplicates from retries.
   - **Transactions**: Kafka Streams wraps each processing step into a transaction. The transaction atomically writes output records to sink topics, updates changelog topics for state stores, and commits consumer offsets.
   - **Consumer offset commits**: Offsets are committed as part of the transaction rather than separately. If the transaction aborts, offsets are not committed, and the records will be reprocessed.

3. **The EOS Transaction Flow**

```
+------------------+     +-------------------+     +------------------+
|  1. Read from    | --> |  2. Process       | --> |  3. Begin        |
|  input topic     |     |  record and       |     |  transaction     |
|  (consumer)      |     |  update state     |     |                  |
+------------------+     +-------------------+     +--------+---------+
                                                            |
                         +----------------------------------+
                         |
                         v
+------------------+     +-------------------+     +------------------+
|  6. Commit       | <-- |  5. Commit        | <-- |  4. Write to     |
|  transaction     |     |  consumer offsets  |     |  output + state  |
|                  |     |  (in transaction)  |     |  changelog topic |
+------------------+     +-------------------+     +------------------+
```

4. **Requirements and Trade-offs**
   - Requires `min.insync.replicas >= 2` and `acks=all` for durability.
   - Adds latency due to transaction coordination overhead.
   - Output consumers must set `isolation.level=read_committed` to only see committed records.
   - EOS protects against duplicates within the Kafka ecosystem; side effects to external systems (databases, APIs) are not covered by the transaction.

---

### ksqlDB Overview

ksqlDB is a streaming database built on top of Kafka Streams. It provides a SQL interface for creating stream processing applications without writing application code.

1. **What is ksqlDB?**
   - A server that runs Kafka Streams topologies defined via SQL statements.
   - Supports creating streams and tables, running continuous queries, and serving pull queries against materialized views.
   - Manages state stores, changelog topics, and consumer groups internally.

2. **CREATE STREAM**
   - Defines a stream backed by a Kafka topic. Each record is an independent event.

```
CREATE STREAM page_views (
    user_id VARCHAR KEY,
    page VARCHAR,
    view_time BIGINT
) WITH (
    KAFKA_TOPIC = 'page_views_topic',
    VALUE_FORMAT = 'JSON'
);
```

3. **CREATE TABLE**
   - Defines a table backed by a compacted Kafka topic. Each key has exactly one current value.

```
CREATE TABLE user_profiles (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    email VARCHAR
) WITH (
    KAFKA_TOPIC = 'user_profiles_topic',
    VALUE_FORMAT = 'JSON'
);
```

4. **Persistent Queries vs Push Queries**
   - **Persistent queries**: Long-running server-side queries that continuously process data and write results to a new topic or materialized view. Created with `CREATE STREAM AS SELECT` or `CREATE TABLE AS SELECT`.
   - **Push queries**: Client-side queries that subscribe to a result stream and receive updates as they happen. Use `EMIT CHANGES` syntax.
   - **Pull queries**: Point-in-time lookups against materialized views, similar to a traditional database `SELECT`. Return immediately with the current state.

5. **Example Persistent Query**

```
CREATE TABLE page_view_counts AS
    SELECT user_id,
           COUNT(*) AS view_count
    FROM page_views
    WINDOW TUMBLING (SIZE 1 HOUR)
    GROUP BY user_id
    EMIT CHANGES;
```

6. **Example Push Query**

```
SELECT user_id, view_count
FROM page_view_counts
EMIT CHANGES;
```

7. **Example Pull Query**

```
SELECT user_id, view_count
FROM page_view_counts
WHERE user_id = 'user-42';
```

---

### Choosing Between Kafka Streams, ksqlDB, and kafka-python

| Criteria               | Kafka Streams                | ksqlDB                        | kafka-python                  |
|------------------------|------------------------------|-------------------------------|-------------------------------|
| **Language**           | JVM (Java/Kotlin/Scala)      | SQL                           | Python                        |
| **Deployment**         | Embedded in application      | Standalone server cluster     | Embedded in application       |
| **Exactly-once**       | Built-in                     | Built-in                      | Manual (requires transactions)|
| **State management**   | Built-in (RocksDB)           | Built-in (managed)            | Manual (in-memory or external)|
| **Windowing**          | Built-in (4 types)           | Built-in (SQL syntax)         | Manual implementation         |
| **Joins**              | Stream-stream, stream-table  | Stream-stream, stream-table   | Manual with local buffers     |
| **Interactive queries**| Supported                    | Pull queries                  | Not available                 |
| **Learning curve**     | Moderate (API knowledge)     | Low (SQL familiarity)         | Low (Python familiarity)      |
| **Flexibility**        | High (custom processors)     | Medium (SQL constraints)      | High (full Python ecosystem)  |
| **Best for**           | Complex stateful apps        | SQL-friendly teams, rapid dev | Lightweight transforms, glue  |

- **Choose Kafka Streams** when you need fine-grained control, complex stateful processing, custom serialization, or when your team already works in the JVM ecosystem.
- **Choose ksqlDB** when you want rapid development with SQL, need built-in materialized views, or want to empower analysts to build streaming pipelines.
- **Choose kafka-python** when your processing is simple (filter, transform, route), when Python is your primary language, or when you need to integrate with Python-specific libraries (ML models, data science tools).

---

### Implementation with kafka-python

The following sections demonstrate how to approximate common stream processing patterns using kafka-python. These are useful for lightweight processing but lack the built-in guarantees and state management of Kafka Streams and ksqlDB.

#### Stream Processing of Data in Kafka

1. **Stream Definition**:
   - In a Kafka context, a stream corresponds to a topic. You can process this stream of data using kafka-python by consuming messages and producing results back to another topic.

2. **Stateful Operations**:
   - While kafka-python does not natively support stateful operations, you can maintain state within your Python application and use kafka-python to read and write state changes to Kafka topics.

3. **Stateless Operations**:
   - Operations that process each record independently without needing to maintain any state. These are simpler to implement and scale.

#### Basic Operations

1. **Filter**: Exclude or include records based on certain conditions.
   
```python
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('input-topic', bootstrap_servers='localhost:9092')
producer = KafkaProducer(bootstrap_servers='localhost:9092')

for message in consumer:
   if "desired_string" in message.value.decode('utf-8'):
       producer.send('output-topic', value=message.value)
```

2.  **Map**: Transform a record's value.

```python
for message in consumer:
    transformed_value = message.value.decode('utf-8').upper().encode('utf-8')
    producer.send('output-topic', value=transformed_value)
```

3. **Aggregate**: For simple aggregates, you can maintain state within your Python application.

```python
count = 0
for message in consumer:
    count += 1
    producer.send('aggregate-topic', key=message.key, value=str(count).encode('utf-8'))
```

4. **Branch**: Route records to different topics based on conditions.

```python
for message in consumer:
    value = message.value.decode('utf-8')
    if value.startswith('ERROR'):
        producer.send('error-topic', value=message.value)
    else:
        producer.send('valid-topic', value=message.value)
```

#### Windowing and Join Operations

1. **Windowing**: Windowing is a bit more complex in a pure kafka-python setup as the library itself doesn't provide windowing utilities. However, you can use Python's data processing libraries like pandas to achieve this functionality by buffering data and processing them in batches.

```python
import time
from collections import defaultdict

window_duration = 60  # seconds
window = defaultdict(list)
window_start = time.time()

for message in consumer:
    window[message.key].append(message.value)

    if time.time() - window_start >= window_duration:
        for key, values in window.items():
            result = str(len(values)).encode('utf-8')
            producer.send('windowed-counts', key=key, value=result)
        window.clear()
        window_start = time.time()
```

2. **Join Operations**: For joining two streams, you can maintain local state for one stream and check against incoming records from another stream.

```python
buffer = {}
consumer1 = KafkaConsumer('topic1', bootstrap_servers='localhost:9092')
consumer2 = KafkaConsumer('topic2', bootstrap_servers='localhost:9092')

for message in consumer1:
    buffer[message.key] = message.value

for message in consumer2:
    if message.key in buffer:
        joined_value = buffer[message.key] + b" " + message.value
        producer.send('joined-topic', key=message.key, value=joined_value)
```

#### Error Handling in Stream Processing

When building stream processing pipelines, handling errors gracefully is essential to avoid data loss and maintain pipeline health.

```python
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('input-topic', bootstrap_servers='localhost:9092')
producer = KafkaProducer(bootstrap_servers='localhost:9092')

for message in consumer:
    try:
        result = message.value.decode('utf-8').upper().encode('utf-8')
        future = producer.send('output-topic', value=result)
        future.get(timeout=10)
    except Exception as e:
        print(f"Failed to process message at offset {message.offset}: {e}")
        producer.send('dead-letter-topic', value=message.value)
```

Using kafka-python for stream processing requires a more hands-on approach than Kafka Streams in Java. However, with the flexibility of Python and the right design patterns, you can replicate many of the streaming operations effectively.
