                           Change Data Capture (CDC) — Debezium + Kafka

      ┌───────────────┐
      │  Order Service│
      └───────┬───────┘
              │ writes
      ┌───────▼───────┐
      │   MySQL DB    │
      └───────┬───────┘
              │ binlog changes (INSERT/UPDATE/DELETE)
      ┌───────▼───────────┐
      │   MySQL Binlog    │
      └───────┬───────────┘
              │
   ┌──────────▼───────────┐
   │     Kafka Connect     │
   │  ┌─────────────────┐  │
   │  │ Debezium (MySQL)│  │  tails binlog → change events
   │  └─────────────────┘  │
   │  ┌─────────────────┐  │
   │  │ Router / SMTs   │  │  routes by table
   │  └─────────────────┘  │
   └──────────┬────────────┘
              │
   ╔══════════▼════════════════════════════════╗
   ║               Apache Kafka                ║
   ║  ┌─────────────────────────────────────┐  ║
   ║  │  topic: events.orders              │  ║
   ║  └─────────────────────────────────────┘  ║
   ║  ┌─────────────────────────────────────┐  ║
   ║  │  topic: events.inventory           │  ║
   ║  └─────────────────────────────────────┘  ║
   ╚══════════┬═══════════════┬═══════════════╝
              │               │
           reads           reads                         reads
        ┌───▼────┐     ┌─────▼────┐                 ┌────▼────┐
        │Shipping│     │Snowflake │                 │  Redis  │
        │ Service│     │ (Warehouse)                │ (Cache) │
        └────────┘     └──────────┘                 └─────────┘


Notes:
- Debezium converts row-level DML (INSERT/UPDATE/DELETE) into structured change events.
- Router/SMTs map tables → topics (e.g., orders → events.orders, inventory → events.inventory).
- Consumers subscribe and apply changes in near real time.
