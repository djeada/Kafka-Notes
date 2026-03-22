# Project 06 — Sensor Data Aggregator

**Difficulty:** Intermediate

Simulated IoT sensors publish readings to a **partitioned** Kafka topic.  A
consumer aggregates the data (min, max, average) per sensor in real time.

---

## Architecture

```
  sensor-01 ──┐                          ┌──────────────────────┐
  sensor-02 ──┤   ┌──────────────────┐   │  Consumer            │
  sensor-03 ──┼──▶│  "sensor-data"   │──▶│  (SensorAggregator)  │
  sensor-04 ──┤   │  3 partitions    │   │  min / max / avg     │
  sensor-05 ──┘   └──────────────────┘   └──────────────────────┘
```

**Key concepts demonstrated:**

* **Key-based partitioning** — every reading carries `sensor_id` as the
  message key, so all data from a single sensor lands on the **same
  partition**.  This guarantees per-sensor ordering.
* **Running aggregation** — the consumer tracks count, min, max, and a
  running sum per sensor, computing the average on the fly.
* **Multiple partitions** — the broker is configured with
  `KAFKA_NUM_PARTITIONS: 3` so the topic is automatically created with three
  partitions.

---

## Prerequisites

* Docker & Docker Compose
* Python 3.8+

---

## Quick Start

### 1. Start Kafka (3 default partitions)

```bash
docker compose up -d
```

Verify both containers are healthy:

```bash
docker compose ps
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the producer (Terminal 1)

```bash
python producer.py --sensors 5 --readings 100 --interval 0.2
```

Five simulated sensors (alternating temperature and humidity) will send
100 readings total.

### 4. Run the consumer (Terminal 2)

```bash
python consumer.py --timeout 20
```

The consumer prints an aggregation snapshot every 20 readings and a final
summary when the timeout expires.

Example output:

```
--- Aggregation snapshot (20 readings) ---
  sensor-01 : count=5  min=17.23  max=36.41  avg=27.10
  sensor-02 : count=3  min=32.50  max=88.20  avg=55.63
  ...
```

---

## Partitioning in Detail

Because the producer sets `key=sensor_id`, the default Kafka partitioner
hashes the key to choose a partition:

```
sensor-01 → hash → partition 0
sensor-02 → hash → partition 2
sensor-03 → hash → partition 1
...
```

This means:

1. All readings from **sensor-01** are always on the same partition →
   order is preserved per sensor.
2. Multiple consumers in the same group can each own different partitions,
   so each consumer handles a disjoint subset of sensors.

### Running multiple consumers

```bash
# Terminal 2
python consumer.py --group sensor-aggregator

# Terminal 3
python consumer.py --group sensor-aggregator
```

With 3 partitions and 2 consumers, one consumer gets 2 partitions and the
other gets 1.  Adding a third consumer gives each exactly one partition.

---

## CLI Reference

### producer.py

| Flag         | Default          | Description                       |
|--------------|------------------|-----------------------------------|
| `--broker`   | `localhost:9092` | Kafka broker address              |
| `--topic`    | `sensor-data`    | Target topic                      |
| `--sensors`  | `5`              | Number of simulated sensors       |
| `--readings` | `100`            | Total readings to send            |
| `--interval` | `0.2`            | Seconds between readings          |

### consumer.py

| Flag       | Default             | Description                      |
|------------|---------------------|----------------------------------|
| `--broker` | `localhost:9092`    | Kafka broker address             |
| `--topic`  | `sensor-data`       | Topic to consume from            |
| `--group`  | `sensor-aggregator` | Consumer group ID                |
| `--timeout`| `20`                | Poll timeout (seconds)           |

---

## Cleanup

```bash
docker compose down
```
