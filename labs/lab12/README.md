## Task 12: Advanced Setup, Final Project, and Best Practices

**Objectives:**
- Combine all learned skills into a final multi-topic, multi-application pipeline.
- Demonstrate ingestion (Kafka Connect), real-time processing (Kafka Streams or ksqlDB), and schema management (Schema Registry).
- Present best practices for monitoring, scaling, and security in a final design.

**Lab Steps (Example Project):**

1. **Design a Pipeline:**  
   - Use Kafka Connect to ingest data from a file or database source into `raw_topic`.
   - Use Kafka Streams or ksqlDB to process data into `processed_topic`.
   - Optionally produce Avro messages to `processed_topic` with a schema in the Registry.

2. **Build a Monitoring Dashboard:**  
   - Set up a minimal Prometheus & Grafana stack to visualize broker metrics, consumer lag, etc.

3. **Enable Basic Security:**  
   - If feasible, configure SSL or SASL for this final pipeline to secure data in transit.

4. **Final Demo and Documentation:**  
   - Produce sample data to `raw_topic` (manually or automatically).
   - Show the processed data in `processed_topic`.
   - Show logs and metrics indicating stable operation.

5. **Reflection:**  
   Write a short retrospective on the key concepts: high availability, scaling producers/consumers, schema evolution, and the synergy of Connect/Streams/ksqlDB. Highlight real-world deployment considerations: cluster sizing, multi-DC replication (MirrorMaker), etc.
