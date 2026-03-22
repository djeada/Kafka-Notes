# Project 04 — Multi-Consumer Notifications

**Difficulty:** Intermediate

Demonstrates Kafka **consumer groups** with a notification system.  A single
producer publishes randomised notifications (email, SMS, push) and multiple
consumers share or duplicate the work depending on how they are grouped.

---

## Architecture

```
                        ┌────────────────────┐
                        │     Producer        │
                        │  (notifications)    │
                        └────────┬───────────┘
                                 │
                        ┌────────▼───────────┐
                        │  Kafka Topic        │
                        │  "notifications"    │
                        └──┬──────┬──────┬───┘
                           │      │      │
              ┌────────────┘      │      └────────────┐
              ▼                   ▼                    ▼
     ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
     │  Consumer A     │  │  Consumer B     │  │  Consumer C     │
     │  (group-1)      │  │  (group-1)      │  │  (group-2)      │
     │  --filter email  │  │  --filter sms   │  │  (all types)    │
     └────────────────┘  └────────────────┘  └────────────────┘
```

* **Same group → load-balancing:** Consumers A and B split partitions.
* **Different group → fan-out:** Group-2 receives its own copy of every
  message, independent of group-1.

---

## Prerequisites

* Docker & Docker Compose
* Python 3.8+

---

## Quick Start

### 1. Start Kafka

```bash
docker compose up -d
```

Wait a few seconds for the health-checks to pass:

```bash
docker compose ps
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the producer (Terminal 1)

```bash
python producer.py --messages 30 --interval 0.3
```

This sends 30 random notifications (email / sms / push) to the
`notifications` topic.

### 4. Run a single consumer (Terminal 2)

```bash
python consumer.py
```

Receives **all** notification types.

---

## Consumer Group Scenarios

### Scenario A — Load Balancing (same group)

Open **two** terminals and run the same group:

```bash
# Terminal 2
python consumer.py --group notification-processors

# Terminal 3
python consumer.py --group notification-processors
```

Kafka distributes partitions between them — each message is delivered to
**exactly one** consumer in the group.

### Scenario B — Fan-Out (different groups)

```bash
# Terminal 2 — group A, only emails
python consumer.py --group email-team --filter-type email

# Terminal 3 — group B, only SMS
python consumer.py --group sms-team --filter-type sms

# Terminal 4 — group C, only push
python consumer.py --group push-team --filter-type push
```

Each group receives **every** message independently.  The `--filter-type`
flag causes the consumer to skip notifications that don't match, which is
logged at the end as a "skipped" count.

### Scenario C — Combined

```bash
# Two consumers sharing email work
python consumer.py --group email-team --filter-type email   # Terminal 2
python consumer.py --group email-team --filter-type email   # Terminal 3

# One consumer for all SMS
python consumer.py --group sms-team --filter-type sms       # Terminal 4
```

---

## CLI Reference

### producer.py

| Flag         | Default            | Description                          |
|--------------|--------------------|--------------------------------------|
| `--broker`   | `localhost:9092`   | Kafka broker address                 |
| `--topic`    | `notifications`    | Target topic                         |
| `--messages` | `30`               | Number of notifications to send      |
| `--interval` | `0.3`              | Seconds between messages             |

### consumer.py

| Flag            | Default                    | Description                        |
|-----------------|----------------------------|------------------------------------|
| `--broker`      | `localhost:9092`           | Kafka broker address               |
| `--topic`       | `notifications`            | Topic to consume from              |
| `--group`       | `notification-processors`  | Consumer group ID                  |
| `--filter-type` | *(none — all types)*       | `email`, `sms`, or `push`         |
| `--timeout`     | `30`                       | Poll timeout (seconds)             |

---

## Cleanup

```bash
docker compose down
```
