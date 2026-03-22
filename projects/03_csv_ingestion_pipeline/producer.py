import argparse
import csv
import json
import logging
from typing import Optional, Sequence

from kafka import KafkaProducer

# Configuration
DEFAULT_BROKER = 'localhost:9092'
DEFAULT_TOPIC = 'csv-ingest'
DEFAULT_INPUT = 'sample_data.csv'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def produce_csv(broker: str, topic: str, input_path: str) -> None:
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    row_count = 0
    try:
        with open(input_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                producer.send(topic, value=dict(row))
                logger.info(f"Sent row: {row}")
                row_count += 1
        producer.flush()
        logger.info(f"Successfully sent {row_count} rows from '{input_path}' to topic '{topic}'")
    finally:
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read a CSV file and produce each row as a JSON message to Kafka.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to produce to")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input CSV file path")
    return parser


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)


def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    produce_csv(parsed_args.broker, parsed_args.topic, getattr(parsed_args, 'input'))


if __name__ == "__main__":
    main()
