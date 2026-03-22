# Project 09: Fraud Detection Pipeline

A multi-topic, rule-based fraud detection system built on Apache Kafka. A
transaction producer generates simulated financial events. A fraud detector
consumes those events, applies configurable rules, and publishes alerts to a
separate topic. An alert consumer reads and logs every alert for monitoring.

## Architecture

```
┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐
│                     │         │                     │         │                     │
│ transaction_producer│──────▶  │   fraud_detector    │──────▶  │   alert_consumer    │
│                     │  topic: │                     │  topic: │                     │
│  Generates JSON     │ "trans- │  Consumes + applies │ "fraud- │  Reads and logs     │
│  transactions       │ actions"│  fraud rules, then  │ alerts" │  every alert        │
│  (~10% suspicious)  │         │  produces alerts    │         │                     │
│                     │         │  (consumer+producer)│         │                     │
└─────────────────────┘         └─────────────────────┘         └─────────────────────┘
```

### Detection Rules

| Rule | Description |
|------|-------------|
| **High Amount** | Transaction amount exceeds a configurable threshold (default 5 000) |
| **Rapid Transactions** | Same user sends ≥ 3 transactions within a 10-second window |
| **Suspicious Location** | Transaction originates from a known suspicious location |

## Prerequisites

| Tool | Version |
|------|---------|
| Docker | 20+ |
| Docker Compose | 2.0+ |
| Python | 3.8+ |

## Setup and Usage

### 1. Start Kafka and Zookeeper

```bash
cd projects/09_fraud_detection_pipeline
docker-compose up -d
```

Wait roughly 30 seconds for the broker to become healthy:

```bash
docker-compose ps   # both services should show "healthy"
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the transaction producer

In **terminal 1**, generate 100 transactions (default):

```bash
python transaction_producer.py
```

Custom options:

```bash
python transaction_producer.py --messages 200 --interval 0.1
```

### 4. Run the fraud detector

In **terminal 2**, start the detector. It reads from `transactions` and writes
alerts to `fraud-alerts`:

```bash
python fraud_detector.py
```

Adjust the amount threshold:

```bash
python fraud_detector.py --amount-threshold 3000
```

### 5. Run the alert consumer

In **terminal 3**, view the alerts:

```bash
python alert_consumer.py
```

## Expected Output

**transaction_producer.py**
```
2024-01-15 10:00:01 - INFO - Sent normal transaction 1/100: user=user_007 amount=42.50 merchant=Starbucks
2024-01-15 10:00:01 - INFO - Sent SUSPICIOUS transaction 2/100: user=user_003 amount=12340.99 merchant=CryptoExchange_XYZ
...
2024-01-15 10:00:21 - INFO - All 100 transactions sent to topic 'transactions'
```

**fraud_detector.py**
```
2024-01-15 10:00:25 - INFO - Processing transaction 3a8b... from user_003 (amount=12340.99)
2024-01-15 10:00:25 - WARNING - FRAUD ALERT: alert-3a8b... — High amount: 12340.99 exceeds threshold 5000.00
...
2024-01-15 10:00:30 - INFO - Detector finished: processed 100 transactions, sent 14 alerts
```

**alert_consumer.py**
```
2024-01-15 10:00:35 - WARNING - ALERT #1 | ID: alert-3a8b | Transaction: 3a8b... | User: user_003 | Amount: 12340.99 | Rule: High amount ...
...
2024-01-15 10:00:36 - INFO - Alert consumer finished: 14 alerts received
```

## Cleanup

```bash
docker-compose down
```

## Concepts Learned

- **Multi-topic pipelines** — routing events between distinct Kafka topics for
  separation of concerns
- **Consumer-producer pattern** — a single service that reads from one topic and
  writes to another, enabling stream processing without a full framework
- **Rule-based stream processing** — applying configurable business rules to
  streaming data in real time
- **Stateful detection** — maintaining per-user history to detect patterns such
  as rapid successive transactions
