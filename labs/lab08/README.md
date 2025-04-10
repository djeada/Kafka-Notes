## Task 8: Monitoring and Logging (Metrics, JMX, Tools)

**Objectives:**
- Set up Kafka monitoring to capture broker/producer/consumer metrics (JMX, Grafana, Prometheus).
- Analyze key metrics (request rates, latency, consumer lag).
- Review logs for debugging broker or client issues.

**Lab Steps:**

1. **Enable JMX in Kafka:**  
   In your docker-compose Kafka service, set environment variables for JMX export, for example:
   ```yaml
   environment:
     KAFKA_JMX_PORT: 9999
     KAFKA_JMX_HOSTNAME: 0.0.0.0
     # Additional steps may be needed for Docker bridging
   ```

2. **Use a Prometheus JMX Exporter (Optional):**  
   - Add a JMX Exporter sidecar or run a container that scrapes JMX metrics and exposes them to Prometheus.

3. **Consumer Group Lag Monitoring:**  
   - Use `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group_name>` to measure offsets.  
   - Tools like Burrow or Grafana dashboards can visualize consumer lag over time.

4. **Log Inspection:**  
   - Check logs in `/var/log/kafka` (if you’re mounting volumes) or via `docker logs kafka`.  
   - Note major events: GC pauses, cluster rebalances, etc.

5. **Reflection:**  
   Document which metrics are most important (e.g., ingestion rate, consumer lag) for production stability. Summarize how to set up a complete monitoring stack (Kafka + JMX + Prometheus + Grafana).
