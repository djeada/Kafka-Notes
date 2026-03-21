# Kafka Notes

Concepts and applications of Apache Kafka for real-time data streaming and distributed messaging systems. This repository includes a range of topics from introductory concepts to advanced use cases, covering producers, consumers, brokers, stream processing, security, schema management, monitoring, and performance tuning.

## Getting Started

1. Clone this repository.
2. Install Python dependencies: `pip install -r requirements.txt`
3. Start with the [notes](#notes) for conceptual understanding, then move to [labs](#labs) for hands-on practice.
4. Use the [flashcards](flashcards/intro.md) for study and review.

## References

- https://www.linkedin.com/pulse/apache-kafka-architecture-hussein-nasser-wpqcc/
- https://www.geeknarrator.com/blog/diskless-kafka-kip-1150
- https://bytebytego.com/guides/can-kafka-lose-messages/
- https://kafka.apache.org/documentation/
- https://developer.confluent.io/

## Notes

| #   | Title                                                                   | Link                                                                                                  |
|-----|-------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| 1   | KafkaProducer from kafka-python                                         | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/producer.md)                            |
| 2   | KafkaConsumer from kafka-python                                         | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/consumer.md)                            |
| 3   | Kafka Brokers                                                           | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/brokers.md)                             |
| 4   | Kafka Topics and Partitions                                             | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/partitions.md)                          |
| 5   | Kafka Connect                                                           | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/kafka_connect.md)                       |
| 6   | Kafka Streams and Stream Processing                                     | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/streams.md)                             |
| 7   | Monitoring and Logging                                                  | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/monitoring.md)                          |
| 8   | Why Is Kafka Fast                                                       | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/why_is_kafka_fast.md)                   |
| 9   | Kafka Security (SSL, SASL, ACLs)                                        | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/security.md)                            |
| 10  | Schema Registry                                                         | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/schema_registry.md)                     |

## Case Studies

| #   | Title                                                                   | Link                                                                                                  |
|-----|-------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| 1   | Optimizing Kafka Producer Writes (ext4 vs XFS)                          | [NOTES](https://github.com/djeada/Kafka-Notes/blob/main/notes/case_studies/optimizing_kafka_writes.md) |

## Labs

| #   | Title                                                                   | Link                                                                                                  |
|-----|-------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| 1   | Environment Setup, Cluster Basics, and Verification                     | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab01)                                     |
| 2   | Producing and Consuming with Python                                     | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab02)                                     |
| 3   | Managing Topics, Partitions, and Replication                            | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab03)                                     |
| 4   | Consumer Groups, Offsets, and Rebalancing                               | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab04)                                     |
| 5   | Kafka Connect for Data Ingestion                                        | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab05)                                     |
| 6   | Kafka Streams for Real-Time Processing                                  | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab06)                                     |
| 7   | ksqlDB for Stream Processing                                            | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab07)                                     |
| 8   | Monitoring and Logging (Metrics, JMX, Tools)                            | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab08)                                     |
| 9   | Kafka Security (SSL, SASL, ACLs)                                          | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab09)                                     |
| 10  | Tuning, Performance, and Reliability                                   | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab10)                                     |
| 11  | Schema Management with Schema Registry (Avro/Protobuf/JSON)              | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab11)                                     |
| 12  | Advanced Setup, Final Project, and Best Practices                        | [LAB](https://github.com/djeada/Kafka-Notes/tree/main/labs/lab12)                                     |

## Scripts

The repository also includes a few standalone helper scripts in `scripts/` for common Kafka administration and monitoring tasks. Each script now accepts command-line arguments, so you can reuse them against your own brokers, topics, and consumer groups without editing the source.

### Examples

```bash
python scripts/alter_partitions.py --broker localhost:9092 --topic orders --num-partitions 6
python scripts/broker_latency.py --brokers localhost:9092 localhost:9093 --monitor-duration 30 --ping-interval 5
python scripts/consumer_lag_alert.py --broker localhost:9092 --consumer-group analytics --topic orders --lag-threshold 250
python scripts/consumer_offset_reset.py --broker localhost:9092 --consumer-group analytics --topic orders --reset-to earliest
python scripts/metrics_visualization.py --broker localhost:9092 --consumer-group analytics --topic orders --monitor-duration 120 --poll-interval 10
```
