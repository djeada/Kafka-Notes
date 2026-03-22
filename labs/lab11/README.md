## Task 11: Schema Management with Schema Registry (Avro/Protobuf/JSON)

**Objectives:**
- Set up Confluent Schema Registry (or similar) to manage data schemas.
- Produce and consume Avro messages with a Python client.
- Understand compatibility settings (backward, forward, full).

**Lab Steps:**

1. **Add Schema Registry to Docker Compose:**  
   ```yaml
   schema-registry:
     image: confluentinc/cp-schema-registry:7.3.0
     depends_on:
       - kafka
     environment:
       SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka:9092"
       SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
     ports:
       - "8081:8081"
   ```

2. **Use Avro in Python (with `confluent-kafka`):**
   ```bash
   pip install confluent-kafka[avro]
   ```
   - Example Avro Producer:
     ```python
     from confluent_kafka import avro
     from confluent_kafka.avro import AvroProducer

     value_schema_str = """
     {
       "type": "record",
       "name": "User",
       "fields" : [
         {"name": "name", "type": "string"},
         {"name": "age", "type": "int"}
       ]
     }
     """

     value_schema = avro.loads(value_schema_str)
     avro_producer = AvroProducer({
         'bootstrap.servers': 'localhost:9092',
         'schema.registry.url': 'http://localhost:8081'
     }, default_value_schema=value_schema)

     avro_producer.produce(topic='avro_topic', value={"name": "Alice", "age": 30})
     avro_producer.flush()
     ```

3. **Check Registered Schemas:**
   ```bash
   curl http://localhost:8081/subjects
   curl http://localhost:8081/subjects/avro_topic-value/versions/1
   ```

4. **Reflection:**  
   Discuss how Schema Registry ensures consistent data structures across producers and consumers, preventing “schema drift.” Note how Avro can reduce message size and simplify versioning.
