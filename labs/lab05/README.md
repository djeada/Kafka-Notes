## Task 5: Kafka Connect for Data Ingestion

**Objectives:**
- Use Kafka Connect to ingest data from external systems (e.g., files, JDBC databases).
- Configure a connector, start it, and validate data flow.
- Explore how Kafka Connect differs from custom ingestion scripts.

**Lab Steps:**

1. **Docker Compose with Kafka Connect:**  
   Extend your docker-compose to include a Kafka Connect service (e.g., Confluent’s `cp-kafka-connect` image). Example snippet:
   ```yaml
   connect:
     image: confluentinc/cp-kafka-connect:7.3.0
     depends_on:
       - kafka
     environment:
       CONNECT_BOOTSTRAP_SERVERS: "kafka:9092"
       CONNECT_REST_ADVERTISED_HOST_NAME: "connect"
       CONNECT_GROUP_ID: "connect-cluster"
       CONNECT_CONFIG_STORAGE_TOPIC: "connect-configs"
       CONNECT_OFFSET_STORAGE_TOPIC: "connect-offsets"
       CONNECT_STATUS_STORAGE_TOPIC: "connect-status"
     ports:
       - "8083:8083"
   ```
   Then `docker-compose up -d connect`.

2. **Install or Enable a Connector Plugin (FileSource Example):**  
   Some images include the FileStream connector by default. Verify via:
   ```bash
   curl localhost:8083/connector-plugins
   ```

3. **Configure and Start a Connector:**  
   ```bash
   curl -X POST -H "Content-Type: application/json" \
   --data '{
     "name": "local-file-source",
     "config": {
       "connector.class": "FileStreamSource",
       "tasks.max": "1",
       "file": "/tmp/test-input.txt",
       "topic": "file_topic"
     }
   }' http://localhost:8083/connectors
   ```
   - This reads from `/tmp/test-input.txt` in the Connect container and sends lines to `file_topic`.

4. **Validate Data Flow:**  
   - Exec into the `connect` container: `docker exec -it connect bash`
   - `echo "Hello from file connector" >> /tmp/test-input.txt`
   - Consume from `file_topic`:
     ```bash
     kafka-console-consumer --bootstrap-server kafka:9092 --topic file_topic --from-beginning
     ```
   - Confirm that new lines appear in Kafka.

5. **Reflection:**  
   Compare Kafka Connect to custom Python scripts for data ingestion. Note the advantages (scalability, fault tolerance, community connectors) and complexities (configuration, version compatibility).
