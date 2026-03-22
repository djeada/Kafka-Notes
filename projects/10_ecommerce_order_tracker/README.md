# Project 10: E-Commerce Order Tracker

A multi-topic, stateful order-lifecycle management system built on Apache Kafka.
An order producer simulates customers placing orders. An order processor drives
each order through the fulfillment pipeline (payment → shipping → delivery) and
publishes status events. A status consumer tracks and displays the full
lifecycle of every order.

## Architecture

```
┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
│                  │ topic:  │                  │ topic:  │                  │
│  order_producer  │──────▶  │ order_processor  │──────▶  │ status_consumer  │
│                  │ "order- │                  │ "order- │                  │
│  Places customer │ placed" │  Validates,      │ status" │  Tracks order    │
│  orders with     │         │  processes       │         │  lifecycle and   │
│  random items    │         │  payment, ships, │         │  logs every      │
│                  │         │  delivers        │         │  transition      │
└──────────────────┘        └──────────────────┘        └──────────────────┘

Order Status Flow:
  PLACED → PAYMENT_PROCESSING → PAYMENT_CONFIRMED → SHIPPED → DELIVERED
```

## Prerequisites

| Tool | Version |
|------|---------|
| Docker | 20+ |
| Docker Compose | 2.0+ |
| Python | 3.8+ |

## Setup and Usage

### 1. Start Kafka and Zookeeper

```bash
cd projects/10_ecommerce_order_tracker
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

### 3. Run the order producer

In **terminal 1**, place 20 orders (default):

```bash
python order_producer.py
```

Generate more orders with a shorter interval:

```bash
python order_producer.py --orders 50 --interval 0.5
```

### 4. Run the order processor

In **terminal 2**, start the processor. It reads from `order-placed` and writes
status updates to `order-status`:

```bash
python order_processor.py
```

### 5. Run the status consumer

In **terminal 3**, track the lifecycle of all orders:

```bash
python status_consumer.py
```

## Expected Output

**order_producer.py**
```
2024-01-15 10:00:01 - INFO - Order 1/20 placed: id=a1b2c3d4 customer=cust_0012 total=194.98 items=[Mechanical Keyboard, Laptop Stand]
2024-01-15 10:00:02 - INFO - Order 2/20 placed: id=e5f6a7b8 customer=cust_0037 total=79.99 items=[Wireless Headphones]
...
2024-01-15 10:00:21 - INFO - All 20 orders sent to topic 'order-placed'
```

**order_processor.py**
```
2024-01-15 10:00:25 - INFO - Processing order a1b2c3d4 (customer=cust_0012, total=194.98)
2024-01-15 10:00:25 - INFO -   Order a1b2c3d4 → PLACED
2024-01-15 10:00:25 - INFO -   Order a1b2c3d4 → PAYMENT_PROCESSING
2024-01-15 10:00:26 - INFO -   Order a1b2c3d4 → PAYMENT_CONFIRMED
2024-01-15 10:00:26 - INFO -   Order a1b2c3d4 → SHIPPED
2024-01-15 10:00:27 - INFO -   Order a1b2c3d4 → DELIVERED
...
2024-01-15 10:00:50 - INFO - Processor finished: 20 orders fully processed
```

**status_consumer.py**
```
2024-01-15 10:00:55 - INFO - Order a1b2c3d4 → PLACED : Order received and queued for processing
2024-01-15 10:00:55 - INFO - Order a1b2c3d4 → PAYMENT_PROCESSING : Payment authorization in progress
2024-01-15 10:00:55 - INFO - Order a1b2c3d4 → PAYMENT_CONFIRMED : Payment successfully processed
2024-01-15 10:00:55 - INFO - Order a1b2c3d4 → SHIPPED : Package handed to carrier
2024-01-15 10:00:55 - INFO - Order a1b2c3d4 → DELIVERED : Package delivered to customer
...
2024-01-15 10:01:00 - INFO - === Order Lifecycle Summary ===
2024-01-15 10:01:00 - INFO -   Order a1b2c3d4: PLACED → PAYMENT_PROCESSING → PAYMENT_CONFIRMED → SHIPPED → DELIVERED
2024-01-15 10:01:00 - INFO - Total orders tracked: 20
```

## Cleanup

```bash
docker-compose down
```

## Concepts Learned

- **Event-driven order management** — modelling a business workflow as a stream
  of immutable status events rather than mutable database rows
- **Multi-topic architecture** — separating commands (order-placed) from events
  (order-status) for clear domain boundaries
- **Stateful stream processing** — maintaining in-memory state (order history)
  while consuming an unbounded event stream
- **Lifecycle patterns** — representing entity state machines through ordered
  Kafka messages that capture every transition
