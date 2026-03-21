## Kafka Monitoring and Logging

Monitoring and logging are crucial components for the smooth and reliable operation of a Kafka cluster. Ensuring that your Kafka cluster is healthy, performing optimally, and diagnosing issues promptly can prevent significant operational challenges.

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
