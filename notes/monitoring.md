## Kafka Monitoring and Logging

Monitoring and logging are crucial components for the smooth and reliable operation of a Kafka cluster. Ensuring that your Kafka cluster is healthy, performing optimally, and diagnosing issues promptly can prevent significant operational challenges.

### Monitoring Architecture Overview

```
+----------+   +----------+   +----------+
| Broker 1 |   | Broker 2 |   | Broker N |
+----+-----+   +----+-----+   +----+-----+
     |              |              |
     v              v              v
+----+-----+   +----+-----+   +----+-----+
| JMX      |   | JMX      |   | JMX      |
| Exporter |   | Exporter |   | Exporter |
+----+-----+   +----+-----+   +----+-----+
     +--------+-----+-----+--------+
              |           |
              v           v
      +-------+---+ +----+----------+
      |Prometheus +->| Alertmanager |
      +-------+---+ +----+-----+---+
              |        |       |
              v        v       v
      +-------+---+ +-+---+ +-+-----+
      |  Grafana  | |Pager| | Slack |
      +-----------+ |Duty | +-------+
                    +-----+
```

### Important Metrics to Monitor in a Kafka Cluster

1. **Broker Metrics**:
   - **Under Replicated Partitions** (`kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`): Number of partitions for which replication is lagging. Should always be zero in a healthy cluster.
   - **Active Controller Count** (`kafka.controller:type=KafkaController,name=ActiveControllerCount`): There should always be exactly one active controller across the cluster.
   - **Offline Partitions Count** (`kafka.controller:type=KafkaController,name=OfflinePartitionsCount`): Number of partitions that don't have an active leader and are hence not writable or readable. Ideally, this should be zero.
   - **Request Handler Idle Ratio** (`kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent`): Percentage of time request handler threads are idle. A low value indicates the broker is under heavy load.
   - **Network Processor Idle Ratio** (`kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent`): Similar to request handler idle ratio but for network threads.

2. **Producer Metrics**:
   - **Record Send Rate** (`kafka.producer:type=producer-metrics,client-id=*,name=record-send-rate`): The average number of records sent by the producer per second.
   - **Record Error Rate** (`kafka.producer:type=producer-metrics,client-id=*,name=record-error-rate`): The average per-second number of failed record sends.
   - **Request Latency Average** (`kafka.producer:type=producer-metrics,client-id=*,name=request-latency-avg`): Average time in ms for produce requests.

3. **Consumer Metrics**:
   - **Records Consumed Rate** (`kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*,name=records-consumed-rate`): The average number of records consumed per second.
   - **Consumer Lag** (`kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*,partition=*,name=records-lag`): The number of messages that remain to be consumed by the consumer. A constantly growing lag might indicate an issue.
   - **Commit Rate** (`kafka.consumer:type=consumer-coordinator-metrics,client-id=*,name=commit-rate`): Rate of offset commits.

4. **System Metrics**:
   - **CPU Utilization**: High CPU usage might indicate a need for more brokers or better hardware.
   - **Memory Utilization**: Sudden spikes can indicate a problem. Monitor JVM heap usage for brokers.
   - **Disk I/O**: High disk I/O can be a performance bottleneck. Monitor both read and write throughput.
   - **Disk Usage**: Ensure sufficient free disk space for log segments. Running out of disk space causes broker failures.

### Tools and Platforms for Monitoring

1. **Grafana**:
   - A popular open-source platform for monitoring and alerting. It allows visualizing your metrics from multiple sources in dashboards.
   - **Integration**: With the Kafka datasource plugin, Grafana can visualize metrics from Kafka seamlessly.

2. **Prometheus**:
   - An open-source monitoring solution. It scrapes metrics from configured targets at given intervals and then triggers alerts if some condition is observed.
   - **Integration**: Kafka has a JMX exporter that can be used with Prometheus to pull the metrics.

3. **JMX (Java Management Extensions)**:
   - Kafka brokers expose metrics via JMX. You can use tools like `jconsole` or `jmxtrans` to view these metrics.
   - Enable JMX by setting the `JMX_PORT` environment variable before starting the broker:
     ```
     export JMX_PORT=9999
     bin/kafka-server-start.sh config/server.properties
     ```
   - Use the `kafka.tools.JmxTool` utility to query specific MBeans from the command line:
     ```
     bin/kafka-run-class.sh kafka.tools.JmxTool \
       --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi \
       --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
     ```

4. **Confluent Control Center**:
   - Provides a comprehensive management and monitoring solution for Apache Kafka. It's a commercial solution provided by Confluent, the company founded by the creators of Kafka.

### Alerting Rules

Setting up alerts helps you detect and respond to issues before they become critical. Common alerting thresholds include:

| Metric                          | Condition                     | Severity |
|---------------------------------|-------------------------------|----------|
| Under Replicated Partitions     | > 0 for more than 5 minutes  | Critical |
| Offline Partitions Count        | > 0                           | Critical |
| Active Controller Count         | != 1                          | Critical |
| Consumer Lag                    | Growing for more than 10 min  | Warning  |
| Request Handler Idle Ratio      | < 20%                         | Warning  |
| Disk Usage                      | > 80%                         | Warning  |
| Producer Error Rate             | > 1% of send rate             | Warning  |

### Prometheus JMX Exporter Configuration

The JMX Exporter runs as a Java agent alongside each broker and exposes Kafka MBeans as Prometheus-compatible metrics on an HTTP endpoint.

1. **Attach the agent** to each broker:
   ```
   export KAFKA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx_exporter/jmx_exporter.yml"
   bin/kafka-server-start.sh config/server.properties
   ```
2. **Example `jmx_exporter.yml`**:
   ```yaml
   lowercaseOutputName: true
   rules:
     - pattern: "kafka.server<type=BrokerTopicMetrics, name=(MessagesInPerSec|BytesInPerSec|BytesOutPerSec)><>Count"
       name: "kafka_server_brokertopicmetrics_$1_total"
       type: COUNTER
     - pattern: "kafka.server<type=ReplicaManager, name=(UnderReplicatedPartitions|PartitionCount)><>Value"
       name: "kafka_server_replicamanager_$1"
       type: GAUGE
     - pattern: "kafka.controller<type=KafkaController, name=(ActiveControllerCount|OfflinePartitionsCount)><>Value"
       name: "kafka_controller_$1"
       type: GAUGE
   ```
3. **Prometheus scrape config** (`prometheus.yml`):
   ```yaml
   scrape_configs:
     - job_name: "kafka"
       static_configs:
         - targets: ["kafka-broker-1:7071","kafka-broker-2:7071","kafka-broker-3:7071"]
       scrape_interval: 15s
   ```

### Key Grafana Dashboards

1. **Broker Overview** — active controller count, under-replicated partitions, offline partitions.
   ```promql
   kafka_server_replicamanager_underreplicatedpartitions
   ```
2. **Topic Throughput** — messages in/sec and bytes in/out per topic.
   ```promql
   sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_total[5m])) by (topic)
   ```
3. **Consumer Lag** — per-group lag and lag growth rate.
   ```promql
   kafka_consumergroup_lag{consumergroup="my-group", topic="my-topic"}
   ```
4. **Producer Latency** — p50/p95/p99 produce-request latency and error rate.
   ```promql
   kafka_network_request_total_time_ms_mean{request="Produce"}
   ```

### Distributed Tracing

Metrics give aggregate views; distributed tracing provides visibility into the lifecycle of individual messages across producers, brokers, and consumers.

1. **Why Tracing Matters** — Kafka decouples producers from consumers, making it hard to follow a single message end-to-end. Tracing reveals latency bottlenecks, retry storms, and message loss at every hop.
2. **OpenTelemetry Integration** — the OTel Java agent auto-instruments Kafka clients:
   ```
   export JAVA_TOOL_OPTIONS="-javaagent:/opt/otel/opentelemetry-javaagent.jar"
   export OTEL_SERVICE_NAME="order-producer"
   export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4317"
   ```
   The agent creates spans for `KafkaProducer.send()` and `KafkaConsumer.poll()`, propagating trace context through record headers.
3. **Correlation IDs in Message Headers** — inject a unique ID into every record so logs, metrics, and traces can be joined:
   ```
   ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
   record.headers().add("correlation-id", UUID.randomUUID().toString().getBytes());
   producer.send(record);
   ```
   Consumers read the header and propagate the ID into their own log MDC and downstream calls.

### Cluster Health Checks

Automated health checks catch problems before users notice them.

1. **Broker API Version Check** — verify a broker is reachable:
   ```
   bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
   ```
   A successful response lists supported API keys. Timeouts indicate the broker is down.
2. **Cluster Metadata Inspection**:
   ```
   bin/kafka-metadata.sh --snapshot /var/kafka-logs/__cluster_metadata-0/00000000000000000000.log \
     --cluster-id <cluster-id>
   ```
3. **Under-Replicated Partition Troubleshooting**
   ```
   UnderReplicatedPartitions > 0?
     |yes
     +-- Broker down? --yes--> Restart / replace broker
     |no
     +-- Disk I/O saturated? --yes--> Add disks or move partitions
     |no
     +-- Network saturated? --yes--> Scale brokers or upgrade NIC
   ```
4. **Periodic Health Checks** — run on a cron schedule and push results to Prometheus via Pushgateway. Check controller election, ISR counts, and log-directory errors.

### Capacity Planning Metrics

1. **Bytes In/Out Per Broker** — identify hot-spot brokers:
   ```promql
   rate(kafka_server_brokertopicmetrics_bytesinpersec_total[5m])
   rate(kafka_server_brokertopicmetrics_bytesoutpersec_total[5m])
   ```
   Rebalance partitions with `kafka-reassign-partitions.sh` if traffic is skewed.
2. **Partition Count Per Broker** — each partition has a memory and FD cost:
   ```promql
   kafka_server_replicamanager_partitioncount
   ```
   Keep counts balanced; aim for fewer than 4,000 per broker (older versions) or up to 200,000 in KRaft mode.
3. **Network Utilization** — sustained usage above 70 % signals the need for more brokers:
   ```promql
   rate(node_network_transmit_bytes_total{device="eth0"}[5m]) * 8
   ```
4. **Disk Utilization Trends** — project when disks will fill:
   ```promql
   predict_linear(node_filesystem_free_bytes{mountpoint="/var/kafka-logs"}[7d], 30*24*3600)
   ```
5. **When to Add Brokers**:
   - CPU idle consistently below 30 % across all brokers.
   - Disk utilization above 70 % with retention already at minimum.
   - Network utilization above 70 % of NIC capacity.
   - Consumer lag growing despite healthy consumers.

### Logging Best Practices

1. **Log Retention**:
   - Set an appropriate log retention period. It helps in ensuring that you have sufficient historical logs for troubleshooting, but also that you are not consuming too much disk space.

2. **Log Levels**:
   - While DEBUG or TRACE levels provide detailed information, they also produce a significant volume of logs. Use them judiciously, preferably during debugging sessions.

3. **Structured Logging**:
   - Using structured log formats like JSON makes it easier to parse and analyze the logs.

4. **Centralized Logging**:
   - Use tools like ELK (Elasticsearch, Logstash, Kibana) or Splunk to centralize logs from all Kafka components. This centralization makes it easier to search, visualize, and analyze the logs.

5. **Alerting**:
   - Set up alerts based on log patterns. This can help in identifying and reacting to issues in real-time.

6. **Regular Review**:
   - Regularly review logs and alerts to identify recurring issues or patterns, which might indicate a deeper systemic problem.

Monitoring and logging are essential for maintaining the health and performance of your Kafka infrastructure. Proactively setting up proper tools and practices can save significant time and effort in diagnosing and rectifying issues.
