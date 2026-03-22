## Task 3: Managing Topics, Partitions, and Replication

**Objectives:**
- Create, list, and describe Kafka topics.
- Understand how partitions and replication factor affect scalability and fault tolerance.
- Practice adjusting partitions for an existing topic.

**Lab Steps:**

1. **Create a Topic with Multiple Partitions:**  
   ```bash
   kafka-topics --create --topic multi_part_topic --bootstrap-server localhost:9092 \
   --partitions 3 --replication-factor 1
   ```
   - List topics:
     ```bash
     kafka-topics --list --bootstrap-server localhost:9092
     ```
   - Describe the new topic:
     ```bash
     kafka-topics --describe --topic multi_part_topic --bootstrap-server localhost:9092
     ```

2. **Adjust Partitions on an Existing Topic (Optional in Dev Environments):**  
   ```bash
   kafka-topics --alter --topic multi_part_topic --partitions 5 --bootstrap-server localhost:9092
   ```
   - Note: This only increases partitions; it can’t decrease them.

3. **Replication Factor (Requires a Multi-Broker Setup):**  
   - If you have multiple brokers in your Docker Compose, create or alter a topic with higher replication factor.  
   - Verify which broker is leader for each partition.

4. **Python Script for Topic Management (Optional):**  
   Some Python libraries expose Admin Clients to create/describe topics. For instance, with `confluent-kafka`:
   ```python
   from confluent_kafka.admin import AdminClient, NewTopic

   admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

   new_topic = NewTopic('python_created_topic', num_partitions=3, replication_factor=1)
   admin.create_topics([new_topic])
   print("Topic created!")
   ```
   - Inspect the topic with Kafka CLI.

5. **Reflection:**  
   Explain how partitioning improves throughput and how replication ensures fault tolerance. Note the potential complexity when you alter partitions or replicate across multiple brokers.
