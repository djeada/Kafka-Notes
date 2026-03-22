## Schema Registry

Schema Registry is a serving layer for schema metadata that provides a RESTful interface for storing, retrieving, and managing Avro, Protobuf, and JSON schemas. It ensures that producers and consumers use compatible schemas, enabling safe and controlled schema evolution in event-driven architectures.

### Architecture Overview

```
                          Schema Registry Architecture

  +------------------+         +----------------------+         +------------------+
  |                  |  POST   |                      |  GET    |                  |
  |  Kafka Producer  +-------->+   Schema Registry    +<--------+  Kafka Consumer  |
  |                  | register|                      | lookup  |                  |
  +--------+---------+ schema  +----------+-----------+ schema  +--------+---------+
           |                              |                              ^
           |  produce                     | stores schemas               | consume
           |  (schema ID + data)          | in _schemas topic            | (schema ID + data)
           V                              V                              |
  +--------+--------------------------------------------------------------+--------+
  |                              Apache Kafka Cluster                              |
  |                                                                                |
  |  +---------------------------+    +--------------------------------------+     |
  |  |  _schemas (internal)      |    |  user-topics (messages carry         |     |
  |  |  stores all registered    |    |  schema ID in first 5 bytes)        |     |
  |  |  schemas as log           |    +--------------------------------------+     |
  |  +---------------------------+                                                 |
  +--------------------------------------------------------------------------------+
```

- Producers register schemas before sending data. The registry returns a **schema ID**.
- Messages carry the schema ID in the first 5 bytes (1 magic byte + 4-byte ID).
- Consumers fetch the schema by ID from the registry to deserialize the message.
- The registry itself uses Kafka (`_schemas` topic) as its durable backend store.

### What is Schema Registry

1. **Purpose**: Enforce data contracts between producers and consumers so that schema changes do not break downstream systems.

2. **How it Works**:
   - Producers serialize data using a registered schema and embed the schema ID in the message.
   - The registry checks new schemas against compatibility rules before accepting them.
   - Consumers deserialize messages by fetching the writer schema from the registry using the embedded ID.

3. **Schema Subjects**: A subject is a scope under which schemas are registered, typically `<topic>-key` or `<topic>-value`. The subject strategy can be customized (TopicName, RecordName, TopicRecordName).

4. **Schema IDs**: Globally unique, monotonically increasing integers assigned by the registry. They are embedded in every serialized message.

5. **Confluent Schema Registry**: The most widely used implementation, open-source under the Confluent Community License. Alternatives include Apicurio Registry and AWS Glue Schema Registry.

### Supported Schema Formats

#### Avro (Default, Most Popular)

Avro is a row-oriented binary serialization format with a compact encoding and rich schema evolution support. It is the default and most mature format in Schema Registry.

```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

#### Protobuf

Protocol Buffers offer strong typing, code generation in many languages, and efficient binary encoding. Preferred when teams already use gRPC or need strict cross-language contracts.

```protobuf
syntax = "proto3";
package com.example;

message User {
  int64 id = 1;
  string name = 2;
  string email = 3;
  int64 created_at = 4;
}
```

#### JSON Schema

JSON Schema validates JSON payloads directly. Useful when human readability is important or when integrating with REST/HTTP systems that already use JSON.

```json
{
  "type": "object",
  "properties": {
    "id":   {"type": "integer"},
    "name": {"type": "string"},
    "email": {"type": "string"}
  },
  "required": ["id", "name"]
}
```

#### Format Comparison

| Feature                  | Avro             | Protobuf         | JSON Schema      |
|--------------------------|------------------|------------------|------------------|
| Schema evolution support | Excellent        | Excellent        | Limited          |
| Serialization size       | Compact (binary) | Compact (binary) | Large (text)     |
| Human readability        | Low              | Medium           | High             |
| Language support         | Wide (JVM-first) | Very wide        | Very wide        |
| Code generation          | Optional         | Required         | Optional         |
| Default in Schema Registry | Yes           | No               | No               |

### Compatibility Modes

Schema Registry enforces compatibility rules when a new version of a schema is registered. The compatibility type is configured per subject (or globally).

```
  Schema Evolution Example (BACKWARD compatible)

  v1 (writer)                v2 (writer + reader)
  +------------------+       +------------------+
  | id: long         |       | id: long         |
  | name: string     | ----> | name: string     |
  +------------------+       | email: string?   |  <-- new optional field
                              +------------------+
  v2 consumer CAN read v1 data (email defaults to null)
```

| Compatibility Mode       | Description                                         | When to Use                          |
|--------------------------|-----------------------------------------------------|--------------------------------------|
| **BACKWARD** (default)   | New schema can read data written by old schema      | Adding optional fields               |
| **FORWARD**              | Old schema can read data written by new schema      | Removing optional fields             |
| **FULL**                 | Both backward and forward compatible                | Strictest safe evolution             |
| **NONE**                 | No compatibility checking                           | Development/testing only             |
| **BACKWARD_TRANSITIVE**  | BACKWARD across all registered versions             | Long-lived consumers reading old data|
| **FORWARD_TRANSITIVE**   | FORWARD across all registered versions              | Rolling deployments with old readers |
| **FULL_TRANSITIVE**      | FULL across all registered versions                 | Maximum safety across all versions   |

- **BACKWARD**: Adding a field with a default value is allowed. Removing a field without a default is not.
- **FORWARD**: Removing a field with a default is allowed. Adding a required field is not.
- **FULL**: Only adding or removing optional fields (with defaults) is allowed.
- **TRANSITIVE** variants check compatibility against every previously registered version, not just the latest.

### REST API

Schema Registry exposes a RESTful API on port `8081` by default.

#### Key Endpoints

| Method | Endpoint                                                 | Description                          |
|--------|----------------------------------------------------------|--------------------------------------|
| GET    | `/subjects`                                              | List all subjects                    |
| GET    | `/subjects/{subject}/versions`                           | List versions under a subject        |
| GET    | `/subjects/{subject}/versions/{version}`                 | Get schema by subject and version    |
| POST   | `/subjects/{subject}/versions`                           | Register a new schema                |
| GET    | `/schemas/ids/{id}`                                      | Get schema by global ID              |
| POST   | `/compatibility/subjects/{subject}/versions/{version}`   | Test compatibility of a schema       |
| PUT    | `/config/{subject}`                                      | Update compatibility for a subject   |
| GET    | `/config/{subject}`                                      | Get compatibility for a subject      |

#### Register a New Schema

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}"
  }' \
  http://localhost:8081/subjects/users-value/versions

# Response: {"id": 1}
```

#### Check Compatibility

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  }' \
  http://localhost:8081/compatibility/subjects/users-value/versions/latest

# Response: {"is_compatible": true}
```

#### Set Compatibility Mode

```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "FULL"}' \
  http://localhost:8081/config/users-value
```

### Integration with Kafka Clients

#### Java Producer Configuration

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("schema.registry.url", "http://localhost:8081");

// Optional: auto-register schemas (true by default)
props.put("auto.register.schemas", true);

KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
```

#### Java Consumer Configuration

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "user-consumer-group");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
props.put("schema.registry.url", "http://localhost:8081");

// Use specific Avro classes instead of GenericRecord
props.put("specific.avro.reader", true);

KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
```

#### Python Example with confluent_kafka

```python
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

# Schema Registry client
schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro schema
schema_str = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"}
  ]
}
"""

# ---- Producer ----
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer_conf = {
    "bootstrap.servers": "localhost:9092",
    "value.serializer": avro_serializer,
}
producer = SerializingProducer(producer_conf)
producer.produce(topic="users", value={"id": 1, "name": "Alice"})
producer.flush()

# ---- Consumer ----
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "user-group",
    "value.deserializer": avro_deserializer,
    "auto.offset.reset": "earliest",
}
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(["users"])

msg = consumer.poll(1.0)
if msg is not None:
    user = msg.value()
    print(f"Consumed user: {user['name']}")
```

### Schema Evolution Strategies

1. **Adding Optional Fields**: Add new fields with default values. This is backward compatible — old consumers ignore the new field, new consumers use the default when reading old data.

2. **Removing Optional Fields**: Remove fields that have default values. This is forward compatible — old consumers use the default for the missing field.

3. **Renaming Fields**: Not supported natively in Avro. Use **aliases** to map old field names to new ones:
   ```json
   {"name": "user_name", "type": "string", "aliases": ["name"]}
   ```

4. **Changing Field Types**: Limited to promotions (e.g., `int` → `long`, `float` → `double`). Arbitrary type changes break compatibility.

5. **Versioning Strategies**:
   - **Topic-per-version**: Create a new topic for breaking changes (e.g., `users-v1`, `users-v2`). Simple but requires consumer migration.
   - **Subject naming strategies**: Use `TopicRecordNameStrategy` to allow multiple schemas per topic, enabling independent evolution per record type.
   - **Dual-write migration**: Write to both old and new topics during a transition period, then cut over consumers.

### Common Pitfalls

1. **Registering incompatible schemas**: Forgetting to set default values when adding fields causes compatibility check failures. Always add new fields as optional with defaults.

2. **Schema ID caching issues**: Clients cache schema IDs locally. If you delete and re-register a schema, the ID changes but clients may still use the stale cached ID. Avoid deleting schemas in production.

3. **Subject naming confusion**: The default `TopicNameStrategy` creates subjects as `<topic>-key` and `<topic>-value`. Misconfiguring the strategy causes schemas to register under wrong subjects.

4. **Setting compatibility to NONE in production**: Disabling compatibility checking removes the safety net entirely. A single bad schema deployment can break all consumers.

5. **Not testing compatibility before deploying**: Always use the `/compatibility` endpoint to test new schemas in CI/CD before registering them. A failed registration in production halts deployments.

6. **Oversized schemas**: Very large schemas (hundreds of fields) slow down serialization and increase registry load. Split large schemas into smaller, composable record types.

7. **Ignoring the `_schemas` topic**: The `_schemas` topic is the registry's backing store. If it is deleted or misconfigured (wrong replication factor, compaction disabled), all schemas are lost.

### Best Practices

1. **Use FULL or FULL_TRANSITIVE compatibility** for critical topics to prevent both backward and forward incompatibilities.

2. **Disable auto-registration in production**: Set `auto.register.schemas=false` in producer configs. Register schemas explicitly through CI/CD pipelines to maintain control over schema changes.

3. **Version schemas in source control**: Store `.avsc`, `.proto`, or `.json` schema files alongside application code and register them as part of the deployment pipeline.

4. **Use the compatibility check endpoint in CI/CD**: Before merging schema changes, validate compatibility against the registry as a build step.

5. **Set replication factor ≥ 3 for the `_schemas` topic**: This is your schema metadata — treat it with the same care as your most critical data.

6. **Monitor Schema Registry**: Track request latency, error rates, and the number of registered schemas. Use health check endpoint (`GET /`) and JMX metrics.

7. **Use specific Avro reader classes** (`specific.avro.reader=true`) in consumers for type safety and better performance instead of `GenericRecord`.

8. **Establish naming conventions early**: Decide on subject naming strategy (`TopicName`, `RecordName`, `TopicRecordName`) and enforce it across all teams before scaling out.

9. **Plan for schema registry high availability**: Run multiple Schema Registry instances behind a load balancer. Only one instance is the leader (handles writes), but all instances serve reads.
