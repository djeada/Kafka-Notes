## Task 7: ksqlDB for Stream Processing (Optional Alternative to Streams)

**Objectives:**
- Explore ksqlDB as a SQL-based approach to stream processing on top of Kafka.
- Create streams and tables with ksqlDB CLI or UI.
- Filter, join, or aggregate streaming data in near real-time.

**Lab Steps:**

1. **Add a ksqlDB Service to Docker Compose (Confluent Image Example):**
   ```yaml
   ksqldb-server:
     image: confluentinc/ksqldb-server:latest
     depends_on:
       - kafka
     environment:
       KSQL_CONFIG_DIR: "/etc/ksqldb"
       KSQL_BOOTSTRAP_SERVERS: "kafka:9092"
       KSQL_LISTENERS: "http://0.0.0.0:8088"
     ports:
       - "8088:8088"
   ```
   Then `docker-compose up -d ksqldb-server`.

2. **Access ksqlDB CLI or REST API:**  
   ```bash
   docker exec -it ksqldb-server ksql http://localhost:8088
   # or
   curl http://localhost:8088/info
   ```

3. **Create a Stream and Query Data:**
   ```sql
   CREATE STREAM input_stream (
     message VARCHAR
   ) WITH (
     KAFKA_TOPIC='input_topic',
     VALUE_FORMAT='DELIMITED'
   );

   SELECT message FROM input_stream EMIT CHANGES;
   ```
   - Produce data to `input_topic` and watch it appear in ksqlDB queries.

4. **Transform with ksqlDB:**
   ```sql
   CREATE STREAM transformed_stream AS
   SELECT UCASE(message) AS upper_msg
   FROM input_stream
   EMIT CHANGES;
   ```
   - Check the new output topic in Kafka (`TRANSFORMED_STREAM` by default).

5. **Reflection:**  
   Compare the SQL-based approach of ksqlDB with the programmatic approach of Kafka Streams. Discuss ease of use vs. flexibility for complex transformations.
