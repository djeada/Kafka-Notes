## Kafka Connect

Kafka Connect is a framework for streaming data between Apache Kafka and external systems. It provides a scalable and reliable way to move data without writing custom integration code.

### Change Data Capture (CDC) with Debezium

```
                            Change Data Capture (CDC) — Debezium + Kafka

      +---------------+
      |  Order Service|
      +-------+-------+
              | writes
      +-------v-------+
      |   MySQL DB    |
      +-------+-------+
              | binlog changes (INSERT/UPDATE/DELETE)
      +-------v-----------+
      |   MySQL Binlog    |
      +-------+-----------+
              |
   +----------v-----------+
   |     Kafka Connect     |
   |  +-----------------+  |
   |  | Debezium (MySQL)|  |  tails binlog -> change events
   |  +-----------------+  |
   |  +-----------------+  |
   |  | Router / SMTs   |  |  routes by table
   |  +-----------------+  |
   +----------+------------+
              |
   +----------v----------------------------------------+
   |               Apache Kafka                        |
   |  +-------------------------------------+          |
   |  |  topic: events.orders              |          |
   |  +-------------------------------------+          |
   |  +-------------------------------------+          |
   |  |  topic: events.inventory           |          |
   |  +-------------------------------------+          |
   +----------+--------------+--------------+----------+
              |              |              |
           reads          reads          reads
        +--------+     +----------+    +---------+
        |Shipping|     |Snowflake |    |  Redis  |
        | Service|     |(Warehouse)|   | (Cache) |
        +--------+     +----------+    +---------+
```

- Debezium converts row-level DML (INSERT/UPDATE/DELETE) into structured change events.
- Router/SMTs map tables to topics (e.g., orders to `events.orders`, inventory to `events.inventory`).
- Consumers subscribe and apply changes in near real time.

### Source and Sink Connectors

1. **Source Connectors**:
   - Pull data from an external system into Kafka topics.
   - Examples: database CDC (Debezium), file system, MQTT, JDBC.

2. **Sink Connectors**:
   - Push data from Kafka topics to an external system.
   - Examples: Elasticsearch, HDFS, S3, JDBC, Redis.

```
Source System --> Source Connector --> Kafka Topic --> Sink Connector --> Target System
```

### Standalone vs Distributed Mode

1. **Standalone Mode**:
   - Runs a single Connect worker process. Good for development and testing.
   - Configuration is provided via properties files.

2. **Distributed Mode**:
   - Runs multiple Connect workers as a cluster. The workload is balanced across workers automatically.
   - Connectors and their tasks are submitted via a REST API.
   - Provides fault tolerance: if a worker fails, its tasks are reassigned to other workers.

### Single Message Transforms (SMTs)

SMTs allow lightweight, per-record transformations without needing a full stream processing application.

Common transforms include:

- **InsertField**: Add a field with a static value or metadata (e.g., timestamp).
- **ReplaceField**: Rename, drop, or include specific fields.
- **MaskField**: Mask sensitive data with a fixed value.
- **TimestampRouter**: Route records to different topics based on a timestamp field.
- **RegexRouter**: Route records to topics matching a regular expression pattern.

### Dead Letter Queues

When a connector encounters a record it cannot process (e.g., deserialization error, schema mismatch), it can be configured to route the failed record to a dead letter queue (DLQ) topic instead of failing the entire task.

```
errors.tolerance = all
errors.deadletterqueue.topic.name = my-dlq-topic
errors.deadletterqueue.context.headers.enable = true
```

### Common Connector Configuration Properties

| Property                  | Description                                                   |
|---------------------------|---------------------------------------------------------------|
| `name`                    | Unique name for the connector instance                        |
| `connector.class`         | Fully qualified class name of the connector                   |
| `tasks.max`               | Maximum number of tasks to create for this connector          |
| `topics` / `topics.regex` | Topics to consume from (sink connectors)                      |
| `key.converter`           | Converter class for record keys (e.g., JSON, Avro)            |
| `value.converter`         | Converter class for record values                             |
