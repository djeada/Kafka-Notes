## Task 4: Consumer Groups, Offsets, and Rebalancing

**Objectives:**
- Understand consumer groups and how Kafka tracks offsets per group.
- Demonstrate rebalancing when new consumers join or leave a group.
- Explore manual offset commits vs. auto commits.

**Lab Steps:**

1. **Consumer Group with Multiple Consumers:**  
   - Start one consumer in terminal #1:
     ```bash
     kafka-console-consumer --bootstrap-server localhost:9092 --topic multi_part_topic \
     --group my_consumer_group
     ```
   - Start another consumer (terminal #2) with the same group ID:
     ```bash
     kafka-console-consumer --bootstrap-server localhost:9092 --topic multi_part_topic \
     --group my_consumer_group
     ```
   - Observe how partitions are distributed between the two consumers.

2. **Produce Messages to `multi_part_topic`:**  
   ```bash
   kafka-console-producer --broker-list localhost:9092 --topic multi_part_topic
   ```
   - Enter messages; see them consumed across both terminals.

3. **Group Management:**  
   - List consumer groups:
     ```bash
     kafka-consumer-groups --bootstrap-server localhost:9092 --list
     ```
   - Describe a group:
     ```bash
     kafka-consumer-groups --bootstrap-server localhost:9092 --group my_consumer_group --describe
     ```

4. **Python Consumer with Manual Offset Commit:**  
   Example with `kafka-python`:
   ```python
   from kafka import KafkaConsumer, TopicPartition

   consumer = KafkaConsumer(
       bootstrap_servers='localhost:9092',
       enable_auto_commit=False
   )
   consumer.assign([TopicPartition('multi_part_topic', 0)])

   for msg in consumer:
       print(f"Received: {msg.value.decode()}")
       # Manually commit offset
       consumer.commit()
   ```
   - This approach gives fine-grained control over offset commits.

5. **Reflection:**  
   Summarize how consumer groups handle parallel consumption and how offset tracking differs in automatic vs. manual commits. Document rebalancing behavior when you add or remove consumers.
