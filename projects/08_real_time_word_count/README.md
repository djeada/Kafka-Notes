# Project 08: Real-Time Word Count

A real-time word frequency counter that streams text sentences into Kafka and
performs running word count analysis on the consumer side. Word count is
widely regarded as the **"Hello World" of stream processing** — simple enough
to understand immediately, yet it demonstrates the core concepts that
underpin every streaming application.

## Why Word Count Matters

Every stream processing framework (Kafka Streams, Apache Flink, Spark
Streaming) uses word count as its introductory example because it exercises
the fundamental building blocks:

1. **Ingestion** – reading an unbounded stream of data.
2. **Tokenisation** – splitting each record into smaller units.
3. **Stateful aggregation** – maintaining running counts across many records.
4. **Periodic reporting** – emitting results while the stream is still active.

These same patterns appear in click-stream analytics, log monitoring, IoT
sensor aggregation, and real-time dashboards.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
│   Producer    │────▶│  Kafka Topic │────▶│  Word Counter    │
│ (sentences)   │     │ (text-stream)│     │ (tokenise+count) │
└──────────────┘     └──────────────┘     └──────────────────┘
```

The **producer** sends sample sentences one at a time — simulating a live
text feed (e.g. chat messages, news headlines, social-media posts). The
**consumer** reads each sentence, normalises the text, and maintains a
running `Counter` of word frequencies, periodically printing the top N words.

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

Stream 50 sentences (the default) to the `text-stream` topic:

```bash
python producer.py
```

Customise the run:

```bash
python producer.py --messages 100 --interval 0.1
```

| Argument     | Default          | Description              |
|--------------|------------------|--------------------------|
| `--broker`   | `localhost:9092` | Kafka broker address     |
| `--topic`    | `text-stream`    | Target topic             |
| `--messages` | `50`             | Number of sentences      |
| `--interval` | `0.3`            | Seconds between messages |

### 4. Run the Consumer

Start the word counter and display the top 10 words:

```bash
python consumer.py
```

Show the top 20 words instead:

```bash
python consumer.py --top-n 20
```

| Argument    | Default          | Description                   |
|-------------|------------------|-------------------------------|
| `--broker`  | `localhost:9092` | Kafka broker address          |
| `--topic`   | `text-stream`    | Topic to consume from         |
| `--group`   | `word-counter`   | Consumer group ID             |
| `--top-n`   | `10`             | Number of top words to show   |
| `--timeout` | `20`             | Consumer timeout in seconds   |

## Expected Output

**Producer:**

```
2025-01-15 10:00:01,100 - INFO - Sent 1/50: Apache Kafka is a distributed streaming platform ...
2025-01-15 10:00:01,400 - INFO - Sent 2/50: Stream processing allows you to analyse data ...
...
2025-01-15 10:00:16,000 - INFO - Successfully sent 50 sentences to topic 'text-stream'
```

**Consumer:**

```
2025-01-15 10:00:20,100 - INFO - Consuming from topic 'text-stream' ...
2025-01-15 10:00:20,200 - INFO - Processed: Apache Kafka is a distributed streaming platform ...
...
2025-01-15 10:00:23,500 - INFO - --- Top 10 Words ---
2025-01-15 10:00:23,500 - INFO -    1. kafka                12
2025-01-15 10:00:23,500 - INFO -    2. data                 10
2025-01-15 10:00:23,500 - INFO -    3. streaming             9
2025-01-15 10:00:23,500 - INFO -    4. processing            8
2025-01-15 10:00:23,500 - INFO -    5. messages              7
...
2025-01-15 10:00:40,000 - INFO - --- Final Results (50 messages) ---
2025-01-15 10:00:40,000 - INFO - Total unique words: 142
```

## Cleanup

Stop and remove the Kafka and Zookeeper containers:

```bash
docker-compose down
```

## Concepts Learned

- **Stream processing** – processing unbounded data one record at a time,
  rather than collecting everything first and processing in batch.
- **Tokenisation** – splitting raw text into individual words and normalising
  them (lowercase, punctuation removal) for consistent counting.
- **Stateful aggregation** – the consumer maintains in-memory state (word
  counts) that grows with every message.
- **Periodic reporting** – emitting intermediate results while the stream is
  still active gives visibility into the running computation.
- **Exactly-once considerations** – in production, ensuring each word is
  counted exactly once requires Kafka transactions or idempotent consumers.
