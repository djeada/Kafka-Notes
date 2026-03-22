# Project 07: Log Aggregation System

A log aggregation pipeline that collects logs from multiple simulated
applications, produces them to a single Kafka topic, and consumes them with
filtering by log level. This project demonstrates how Kafka serves as a
centralised log bus in microservice architectures.

## Why Kafka for Log Aggregation?

In production systems, dozens (or hundreds) of services generate log output
simultaneously. Shipping every log directly to a database or search engine
creates tight coupling and can overwhelm downstream storage during traffic
spikes. Kafka acts as a **durable buffer** between log producers and
consumers:

* **Decoupling** – producers write logs without knowing who reads them.
* **Fan-out** – multiple consumers (dashboards, alerting, long-term storage)
  can each read the same stream independently.
* **Back-pressure** – Kafka retains messages on disk, so a slow consumer
  never blocks a fast producer.
* **Replay** – new consumers can re-read historical logs from the beginning
  of the topic.

## Architecture

```
┌─────────────┐
│ web-server   │──┐
├─────────────┤  │
│ auth-service │──┤     ┌──────────────┐     ┌──────────────────┐
├─────────────┤  ├────▶│  Kafka Topic  │────▶│  Log Aggregator  │
│ payment-svc  │──┤     │  (app-logs)   │     │  (filter & file) │
├─────────────┤  │     └──────────────┘     └──────────────────┘
│ notif-svc    │──┘
└─────────────┘
```

The **producer** simulates all four applications in a single process (each
message carries an `app` field), while the **consumer** filters by minimum
log level and optionally writes matching entries to a file.

## Prerequisites

| Tool             | Version |
|------------------|---------|
| Docker           | 20+     |
| Docker Compose   | 2.0+    |
| Python           | 3.8+    |
| pip              | 21+     |

## Setup and Usage

### 1. Start the Kafka Broker

```bash
docker-compose up -d
```

Wait a few seconds for Kafka and Zookeeper to become healthy:

```bash
docker-compose ps
```

Both services should show a **healthy** status.

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Producer

Generate 100 simulated log entries (the default) across four applications:

```bash
python producer.py
```

Customise the run with command-line arguments:

```bash
python producer.py --messages 200 --interval 0.05 --apps web-server auth-service
```

| Argument      | Default                                                          | Description                      |
|---------------|------------------------------------------------------------------|----------------------------------|
| `--broker`    | `localhost:9092`                                                 | Kafka broker address             |
| `--topic`     | `app-logs`                                                       | Target topic                     |
| `--messages`  | `100`                                                            | Number of log entries to produce |
| `--interval`  | `0.1`                                                            | Seconds between messages         |
| `--apps`      | `web-server auth-service payment-service notification-service`   | Applications to simulate         |

### 4. Run the Consumer

Consume logs and display only those at **WARNING** level or above:

```bash
python consumer.py --min-level WARNING
```

Write matching logs to a file while also printing to stdout:

```bash
python consumer.py --min-level ERROR --output errors.log
```

| Argument      | Default          | Description                          |
|---------------|------------------|--------------------------------------|
| `--broker`    | `localhost:9092` | Kafka broker address                 |
| `--topic`     | `app-logs`       | Topic to consume from                |
| `--group`     | `log-aggregator` | Consumer group ID                    |
| `--min-level` | `INFO`           | Minimum log level (DEBUG–CRITICAL)   |
| `--output`    | *(none)*         | Optional file to append matched logs |
| `--timeout`   | `30`             | Consumer timeout in seconds          |

## Log Level Hierarchy

The consumer uses a numeric hierarchy to filter log entries:

```
DEBUG (0) < INFO (1) < WARNING (2) < ERROR (3) < CRITICAL (4)
```

Setting `--min-level WARNING` shows WARNING, ERROR, and CRITICAL entries.

## Expected Output

**Producer:**

```
2025-01-15 10:00:01,100 - INFO - Sent log 1/100: [INFO] auth-service - User login successful
2025-01-15 10:00:01,210 - INFO - Sent log 2/100: [ERROR] web-server - Connection timeout to upstream service
...
2025-01-15 10:00:11,500 - INFO - Successfully sent 100 log entries to topic 'app-logs'
```

**Consumer** (with `--min-level WARNING`):

```
2025-01-15 10:00:15,100 - INFO - Consuming from topic 'app-logs' (min level: WARNING) ...
2025-01-15 10:00:15,200 - INFO - [2025-01-15T10:00:01+00:00] ERROR    web-server: Connection timeout to upstream service
2025-01-15 10:00:15,205 - INFO - [2025-01-15T10:00:03+00:00] WARNING  payment-service: Duplicate transaction detected
...
2025-01-15 10:00:45,300 - INFO - --- Aggregation Summary ---
2025-01-15 10:00:45,300 - INFO - Total logs received : 100
2025-01-15 10:00:45,300 - INFO - Logs matching >=WARNING : 38
```

## Cleanup

Stop and remove the Kafka and Zookeeper containers:

```bash
docker-compose down
```

## Concepts Learned

- **Centralised log topic** – many producers write to one topic; Kafka
  partitions handle the throughput.
- **Structured logging** – JSON payloads let consumers parse, filter, and
  route log entries programmatically.
- **Level-based filtering** – consumers apply a numeric hierarchy to select
  only the severity they care about.
- **File sinks** – a consumer can persist filtered logs to disk, mimicking a
  lightweight log-shipping pipeline (Kafka → file → monitoring tool).
- **Multi-producer pattern** – a single topic can receive data from many
  independent sources, each identified by a metadata field (`app`).
