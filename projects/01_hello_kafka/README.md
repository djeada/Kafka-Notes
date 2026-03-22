# Project 01: Hello Kafka

A minimal Kafka example that demonstrates the fundamentals of producing and consuming
plain text messages. This is the simplest possible starting point for working with
Apache Kafka and Python.

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

Wait a few seconds for Kafka and Zookeeper to become healthy. You can verify
with:

```bash
docker-compose ps
```

Both services should show a **healthy** status.

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Producer

Send 10 messages (the default) to the `hello-kafka` topic:

```bash
python producer.py
```

You can customize the run with command-line arguments:

```bash
python producer.py --broker localhost:9092 --topic hello-kafka --messages 20
```

### 4. Run the Consumer

Consume and display all messages from the `hello-kafka` topic:

```bash
python consumer.py
```

The consumer will automatically exit after the timeout period (default 10 s)
when no new messages arrive.

```bash
python consumer.py --broker localhost:9092 --topic hello-kafka --timeout 15
```

## Expected Output

**Producer:**

```
2025-01-15 10:00:01,234 - INFO - Sent: Hello Kafka #1
2025-01-15 10:00:01,240 - INFO - Sent: Hello Kafka #2
...
2025-01-15 10:00:01,280 - INFO - Sent: Hello Kafka #10
2025-01-15 10:00:01,285 - INFO - Successfully sent 10 messages to topic 'hello-kafka'
```

**Consumer:**

```
2025-01-15 10:00:05,100 - INFO - Received: Hello Kafka #1
2025-01-15 10:00:05,102 - INFO - Received: Hello Kafka #2
...
2025-01-15 10:00:05,120 - INFO - Received: Hello Kafka #10
2025-01-15 10:00:15,125 - INFO - Total messages received: 10
```

## Cleanup

Stop and remove the Kafka and Zookeeper containers:

```bash
docker-compose down
```

## Concepts Learned

- **Kafka Producer** – sends (produces) messages to a named topic.
- **Kafka Consumer** – reads (consumes) messages from a topic starting at a
  configurable offset (`earliest` in this project).
- **Topics** – logical channels that decouple producers from consumers.
- **Serialization / Deserialization** – messages are transmitted as bytes;
  the producer encodes strings to UTF-8 and the consumer decodes them back.
- **Consumer Timeout** – `consumer_timeout_ms` controls how long the consumer
  waits for new messages before it stops iterating.
