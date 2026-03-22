# Project 02: Event Counter

A beginner Kafka project that produces random JSON events and consumes them
while maintaining running totals per event type. This builds on the Hello Kafka
example by introducing **JSON serialization** and basic stream aggregation.

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

Verify both services are healthy:

```bash
docker-compose ps
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Producer

Send 50 random events (the default) to the `event-counts` topic:

```bash
python producer.py
```

Customize with command-line arguments:

```bash
python producer.py --messages 100 --interval 0.2
```

| Argument     | Default        | Description                   |
|--------------|----------------|-------------------------------|
| `--broker`   | localhost:9092 | Kafka broker address          |
| `--topic`    | event-counts   | Target topic                  |
| `--messages` | 50             | Number of events to produce   |
| `--interval` | 0.5            | Seconds between messages      |

### 4. Run the Consumer

Consume events and watch the running totals:

```bash
python consumer.py
```

The consumer exits automatically after the timeout period when no new messages
arrive.

```bash
python consumer.py --timeout 60
```

## Expected Output

**Producer:**

```
2025-01-15 10:00:01,100 - INFO - Sent: {'event_id': 1, 'type': 'click'}
2025-01-15 10:00:01,600 - INFO - Sent: {'event_id': 2, 'type': 'view'}
2025-01-15 10:00:02,100 - INFO - Sent: {'event_id': 3, 'type': 'purchase'}
...
2025-01-15 10:00:25,600 - INFO - Successfully sent 50 events to topic 'event-counts'
```

**Consumer:**

```
2025-01-15 10:01:00,200 - INFO - Received event #1: type=click
2025-01-15 10:01:00,201 - INFO - Running totals: {'click': 1}
2025-01-15 10:01:00,205 - INFO - Received event #2: type=view
2025-01-15 10:01:00,206 - INFO - Running totals: {'click': 1, 'view': 1}
2025-01-15 10:01:00,210 - INFO - Received event #3: type=purchase
2025-01-15 10:01:00,211 - INFO - Running totals: {'click': 1, 'view': 1, 'purchase': 1}
...
2025-01-15 10:01:30,500 - INFO - Final totals after 50 events: {'click': 14, 'view': 12, 'purchase': 11, 'signup': 13}
```

## Cleanup

```bash
docker-compose down
```

## Concepts Learned

- **JSON Serialization** – `json.dumps()` converts Python dicts to JSON strings
  for the producer; `json.loads()` converts them back in the consumer.
- **Value Serializer / Deserializer** – Kafka messages are raw bytes. Custom
  serializer and deserializer lambdas handle the conversion transparently.
- **Stream Aggregation** – the consumer keeps a running `Counter` of event
  types, demonstrating a simple stateful stream processing pattern.
- **Throttled Production** – the `--interval` flag adds a delay between
  messages, simulating a more realistic event stream.
