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

### REST API for Distributed Mode

In distributed mode, connectors are managed through a REST API (default port 8083).

| Method | Endpoint                            | Description                          |
|--------|-------------------------------------|--------------------------------------|
| GET    | `/connectors`                       | List all active connectors           |
| POST   | `/connectors`                       | Create a new connector               |
| GET    | `/connectors/{name}/status`         | Get current status of a connector    |
| PUT    | `/connectors/{name}/config`         | Update connector configuration       |
| POST   | `/connectors/{name}/restart`        | Restart a connector                  |
| DELETE | `/connectors/{name}`                | Delete a connector                   |

**Creating a connector:**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{"name": "jdbc-source-orders", "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://db:3306/shop",
    "table.whitelist": "orders",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "topic.prefix": "db.",
    "tasks.max": "1"
  }}'
```

**Status check and deletion:**

```bash
curl http://localhost:8083/connectors/jdbc-source-orders/status | jq .
curl -X DELETE http://localhost:8083/connectors/jdbc-source-orders
```

### Connector Lifecycle

Connectors and tasks move through states: **UNASSIGNED** → **RUNNING** → **PAUSED** or **FAILED**.

```
  +------------+     start     +---------+
  | UNASSIGNED +-------------->| RUNNING |
  +-----+------+               +----+----+
        ^                           |
        |    worker failure         | pause
        +---------------------------+------->+--------+
                                    |        | PAUSED |
                                    | error  +---+----+
                                    v        resume|
                                 +--+---+       |
                                 |FAILED|<------+
                                 +------+
```

- **UNASSIGNED**: Created but not yet assigned to a worker.
- **RUNNING**: Actively processing records.
- **PAUSED**: Paused via REST API; no records are read or written.
- **FAILED**: Unrecoverable error encountered.

Task states follow the same model. Each task transitions independently — a connector reports `RUNNING` even if one of its tasks has `FAILED`.

**Worker management:** Configurations are stored in `config.storage.topic`. The group leader assigns tasks across workers. If a worker leaves, a rebalance redistributes its tasks.

**Graceful shutdown:** Pause connectors (`POST /connectors/{name}/pause`) before stopping a worker. This flushes offsets and stops processing. Resume with `POST /connectors/{name}/resume`.

### Converters and Serialization

Converters control how Connect serializes keys and values when reading from or writing to Kafka.

| Converter              | Format      | Schema Support |
|------------------------|-------------|----------------|
| `JsonConverter`        | JSON        | Optional       |
| `AvroConverter`        | Avro binary | Yes            |
| `ProtobufConverter`    | Protobuf    | Yes            |
| `StringConverter`      | Plain text  | No             |
| `ByteArrayConverter`   | Raw bytes   | No             |

**JsonConverter** embeds the schema in every message by default. Disable inline schemas when using Schema Registry or when schemas are not needed:

```
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

**AvroConverter** and **ProtobufConverter** use Schema Registry to keep messages compact:

```
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081
```

**Choosing a converter:** Use `AvroConverter`/`ProtobufConverter` for production schema evolution; `JsonConverter` with `schemas.enable=false` for simple pipelines; `StringConverter` for plain text; `ByteArrayConverter` for opaque binary pass-through.

### Error Handling and Retries

Beyond dead letter queues, Kafka Connect provides fine-grained error control.

**`errors.tolerance`** controls behavior when a record fails:

- `none` (default): The task fails immediately on the first error.
- `all`: The task skips the bad record and continues processing.

**Retry settings:** `errors.retry.delay.max.ms` (default 60000) sets the maximum backoff between retries. `errors.retry.timeout` (default 0, disabled) sets the total retry window before giving up.

**Recommended production configuration** combining retries, DLQ, and error logging:

```
errors.tolerance                = all
errors.retry.timeout            = 300000
errors.retry.delay.max.ms       = 15000
errors.log.enable               = true
errors.log.include.messages     = true
errors.deadletterqueue.topic.name = connector-dlq
errors.deadletterqueue.topic.replication.factor = 3
errors.deadletterqueue.context.headers.enable = true
```

Failed records are routed to the DLQ with error context headers (exception, stage, connector name), and full details are written to the worker log for debugging.

### Monitoring Connectors

Kafka Connect exposes JMX metrics under the `kafka.connect` domain:

| Metric Group              | Key Metrics                                                |
|---------------------------|------------------------------------------------------------|
| `connect-worker-metrics`  | `connector-count`, `task-count`, `connector-startup-failure-total` |
| `task-metrics`            | `running-ratio`, `batch-size-avg`, `offset-commit-failure-percentage` |
| `sink-task-metrics`       | `partition-count`, `put-batch-avg-time-ms`                 |
| `source-task-metrics`     | `poll-batch-avg-time-ms`, `source-record-active-count`     |

**REST API health checks:**

Poll the status endpoint periodically to detect failures early:

```bash
# Check all connectors for FAILED tasks
for c in $(curl -s localhost:8083/connectors | jq -r '.[]'); do
  curl -s "localhost:8083/connectors/$c/status" \
    | jq '{name: .name, state: .connector.state, tasks: [.tasks[]|.state]}'
done
```

**Alerting guidelines:**

- Alert when any connector or task enters `FAILED`.
- Watch `offset-commit-failure-percentage` — rising values mean the connector cannot commit progress.
- Track `running-ratio` per task; below 1.0 means time spent in error recovery.
- Monitor consumer lag on sink connectors with standard consumer group tools.

### Common Pitfalls

**1. Schema evolution issues:**
- Adding a required field without a default breaks consumers. Changing a field type (e.g., `int` → `string`) is backward-incompatible.
- Use Schema Registry compatibility modes (`BACKWARD`, `FORWARD`, `FULL`) to catch breaking changes before deployment.

**2. Task rebalancing:**
- Rebalances occur when workers join/leave or connectors are created/deleted. All tasks pause briefly, causing a consumer lag spike.
- Use `scheduled.rebalance.max.delay.ms` to give departing workers time to rejoin before redistribution.

**3. Offset management in source connectors:**
- Source connectors track position in `offset.storage.topic`. Deleting and recreating a connector with the same name reuses old offsets, which may skip or reprocess data.
- To reset offsets, use a new connector name or manually clear entries from the offset topic.

**4. Connector versioning:**
- Connector JARs live in `plugin.path`. Running different versions across workers causes unpredictable behavior.
- Always perform a rolling restart of all workers after upgrading a connector plugin.
