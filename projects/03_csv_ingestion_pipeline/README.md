# Project 03: CSV Ingestion Pipeline

A beginner-intermediate Kafka project that reads a CSV file, publishes each row
as a JSON message to a Kafka topic, and then consumes those messages to write
them back into a new CSV file. This demonstrates a simple **file → Kafka → file**
data pipeline.

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

### 3. Generate Sample Data

Create a CSV file with 100 rows of realistic user data:

```bash
python generate_sample_data.py
```

Customize the output:

```bash
python generate_sample_data.py --output my_data.csv --rows 500
```

This produces a file with columns: `id`, `name`, `email`, `age`, `city`.

### 4. Run the Producer

Ingest the CSV into the `csv-ingest` topic:

```bash
python producer.py
```

Or specify a different input file:

```bash
python producer.py --input my_data.csv
```

### 5. Run the Consumer

Consume the messages and write them to an output CSV:

```bash
python consumer.py
```

Customise the output path or timeout:

```bash
python consumer.py --output results.csv --timeout 20
```

### 6. Verify the Output

Compare the input and output files:

```bash
diff sample_data.csv output_data.csv
```

The files should be identical (same header and rows in the same order).

## Expected Output

**Data Generator:**

```
2025-01-15 10:00:00,100 - INFO - Generated 100 rows in 'sample_data.csv'
```

**Producer:**

```
2025-01-15 10:00:05,200 - INFO - Sent row: {'id': '1', 'name': 'Alice Smith', 'email': 'alice.smith@example.com', 'age': '34', 'city': 'Austin'}
2025-01-15 10:00:05,205 - INFO - Sent row: {'id': '2', 'name': 'Bob Johnson', 'email': 'bob.johnson@mail.com', 'age': '28', 'city': 'Seattle'}
...
2025-01-15 10:00:05,500 - INFO - Successfully sent 100 rows from 'sample_data.csv' to topic 'csv-ingest'
```

**Consumer:**

```
2025-01-15 10:00:10,300 - INFO - Written row 1: {'id': '1', 'name': 'Alice Smith', 'email': 'alice.smith@example.com', 'age': '34', 'city': 'Austin'}
...
2025-01-15 10:00:10,600 - INFO - Total rows written to 'output_data.csv': 100
```

## Cleanup

```bash
docker-compose down
```

Remove generated data files if desired:

```bash
rm -f sample_data.csv output_data.csv
```

## Concepts Learned

- **CSV to JSON Conversion** – `csv.DictReader` turns each CSV row into a
  Python dict which is then serialised to JSON for Kafka transmission.
- **File-based Pipelines** – a common integration pattern where Kafka acts as a
  buffer between a file source and a file sink.
- **End-to-end Data Integrity** – by comparing input and output files you can
  verify that no data was lost or corrupted during the pipeline.
- **Schema Awareness** – the consumer uses a fixed list of field names to write
  the output CSV header, ensuring a consistent schema regardless of message
  ordering.
