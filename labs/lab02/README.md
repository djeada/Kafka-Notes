## Task 2: Producing and Consuming with Python

**Objectives:**
- Use Python libraries (`kafka-python` or `confluent-kafka`) to produce and consume messages.
- Understand producer and consumer configurations (e.g., `bootstrap_servers`, `group_id`, offsets).
- Explore synchronous vs. asynchronous message sending.

**Lab Steps:**

1. **Install a Kafka Python Library:**  
   Inside your Python environment (local machine or container):
   ```bash
   pip install kafka-python
   ```
   or
   ```bash
   pip install confluent-kafka
   ```

2. **Python Producer Example (using `kafka-python`):**  
   ```python
   from kafka import KafkaProducer

   producer = KafkaProducer(bootstrap_servers='localhost:9092')

   for i in range(5):
       message = f"Message number {i}"
       producer.send('test_topic', value=message.encode('utf-8'))
       print(f"Sent: {message}")

   producer.flush()
   producer.close()
   ```
   - Run the script. Check logs in Docker or run a consumer to see the messages.

3. **Python Consumer Example:**  
   ```python
   from kafka import KafkaConsumer

   consumer = KafkaConsumer(
       'test_topic',
       bootstrap_servers='localhost:9092',
       auto_offset_reset='earliest',
       group_id='test_group'
   )

   print("Starting consumer...")
   for msg in consumer:
       print(f"Received: {msg.value.decode('utf-8')}")
   ```
   - Leave the script running, re-run your producer, and watch messages arrive in real-time.

4. **Reflection:**  
   Note how Python automation can quickly feed data into Kafka or retrieve it. Document the difference in APIs (synchronous vs. asynchronous calls, batching, etc.).
