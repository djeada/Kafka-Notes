## Task 6: Kafka Streams for Real-Time Processing

**Objectives:**
- Introduce the Kafka Streams library for stream processing in Java or an alternative (e.g., `ksqlDB`).
- Set up a basic Streams application to transform/aggregate data.
- Understand concepts like KStream vs. KTable, windowing, and state stores.

**Lab Steps:**

1. **Kafka Streams Quickstart (Optional if comfortable with Java):**  
   - Create a simple Maven project with `org.apache.kafka:kafka-streams` dependency.
   - Write a basic Kafka Streams app to read from one topic, process messages, and output to another topic.

2. **Example Processing Topology (Pseudo Code):**
   ```java
   StreamsBuilder builder = new StreamsBuilder();
   KStream<String, String> source = builder.stream("input_topic");
   
   KStream<String, String> transformed = source.mapValues(value -> value.toUpperCase());
   transformed.to("output_topic");

   KafkaStreams streams = new KafkaStreams(builder.build(), props);
   streams.start();
   ```
3. **Create Input and Output Topics:**  
   ```bash
   kafka-topics --create --topic input_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics --create --topic output_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

4. **Produce and Verify:**  
   - Send messages to `input_topic`, check that `output_topic` has the uppercase version.  
   - Optionally, consume `output_topic` from the command line or with Python to confirm transformation.

5. **Reflection:**  
   Describe how Kafka Streams is a library that runs in your app (vs. Connect or a separate cluster). Note how real-time transformations differ from batch-based processing.
