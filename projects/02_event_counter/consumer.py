import argparse
import json
import logging
from collections import Counter
from typing import Optional, Sequence

from kafka import KafkaConsumer

# Configuration
DEFAULT_BROKER = 'localhost:9092'
DEFAULT_TOPIC = 'event-counts'
DEFAULT_TIMEOUT = 30

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def consume_events(broker: str, topic: str, timeout: int) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset='earliest',
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )
    counts: Counter = Counter()
    message_count = 0
    try:
        for message in consumer:
            event = message.value
            event_type = event.get("type", "unknown")
            counts[event_type] += 1
            message_count += 1
            logger.info(f"Received event #{event.get('event_id')}: type={event_type}")
            logger.info(f"Running totals: {dict(counts)}")
    finally:
        consumer.close()
    logger.info(f"Final totals after {message_count} events: {dict(counts)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume JSON events and display running count totals.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume from")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Consumer timeout in seconds")
    return parser


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)


def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    consume_events(parsed_args.broker, parsed_args.topic, parsed_args.timeout)


if __name__ == "__main__":
    main()
