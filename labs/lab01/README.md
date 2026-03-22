## Task 1: Environment Setup, Cluster Basics, and Verification

**Objectives:**
- Set up a local Kafka environment (via Docker or Docker Compose).
- Understand fundamental Kafka components: brokers, topics, partitions, and Zookeeper (or KIP-500 if using newer versions without Zookeeper).
- Verify installation by producing and consuming a test message.

**Lab Steps:**

1. **Docker Compose File for Kafka (Basic Setup):**  
   Create a `docker-compose.yml`:
   ```yaml
   version: '3'
   services:
     zookeeper:
       image: confluentinc/cp-zookeeper:7.3.0
       environment:
         ZOOKEEPER_CLIENT_PORT: 2181
         ZOOKEEPER_TICK_TIME: 2000
       ports:
         - "2181:2181"

     kafka:
       image: confluentinc/cp-kafka:7.3.0
       depends_on:
         - zookeeper
       ports:
         - "9092:9092"
       environment:
         KAFKA_BROKER_ID: 1
         KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
         KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
         KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
         KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
   ```
   Start the cluster:
   ```bash
   docker-compose up -d
   ```

2. **Verify Kafka Broker and Zookeeper:**  
   - Run `docker-compose ps` to ensure containers are up.  
   - (Optional) `docker logs kafka` to confirm the broker started successfully.

3. **Produce and Consume a Test Message (CLI):**  
   - **Create a topic** (inside the Kafka container):
     ```bash
     docker exec -it kafka bash
     kafka-topics --create --topic test_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
     ```
   - **Produce a message**:
     ```bash
     kafka-console-producer --broker-list localhost:9092 --topic test_topic
     # type a message like "Hello Kafka!"
     # press Ctrl+C to exit
     ```
   - **Consume the message**:
     ```bash
     kafka-console-consumer --bootstrap-server localhost:9092 --topic test_topic --from-beginning
     # you should see "Hello Kafka!"
     # press Ctrl+C to exit
     ```

4. **Reflection:**  
   In your lab notes, record how Kafka uses brokers, partitions, and Zookeeper (if using older versions). Summarize the core idea of “publish-subscribe” messaging.
