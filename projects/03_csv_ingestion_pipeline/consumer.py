import argparse
import csv
import json
import logging
from typing import Optional, Sequence

from kafka import KafkaConsumer

# Configuration
DEFAULT_BROKER = 'localhost:9092'
DEFAULT_TOPIC = 'csv-ingest'
DEFAULT_OUTPUT = 'output_data.csv'
DEFAULT_TIMEOUT = 15
FIELDNAMES = ['id', 'name', 'email', 'age', 'city']

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def consume_to_csv(broker: str, topic: str, output_path: str, timeout: int) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset='earliest',
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )
    row_count = 0
    try:
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
            writer.writeheader()
            for message in consumer:
                row = message.value
                writer.writerow(row)
                row_count += 1
                logger.info(f"Written row {row_count}: {row}")
    finally:
        consumer.close()
    logger.info(f"Total rows written to '{output_path}': {row_count}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume JSON messages from Kafka and write them to a CSV file.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume from")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV file path")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Consumer timeout in seconds")
    return parser


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)


def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    consume_to_csv(parsed_args.broker, parsed_args.topic, parsed_args.output, parsed_args.timeout)


if __name__ == "__main__":
    main()
