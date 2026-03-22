import argparse
import logging
from typing import Optional, Sequence

from kafka import KafkaConsumer

# Configuration
DEFAULT_BROKER = 'localhost:9092'
DEFAULT_TOPIC = 'hello-kafka'
DEFAULT_TIMEOUT = 10

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def consume_messages(broker: str, topic: str, timeout: int) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset='earliest',
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda v: v.decode('utf-8'),
    )
    message_count = 0
    try:
        for message in consumer:
            logger.info(f"Received: {message.value}")
            message_count += 1
    finally:
        consumer.close()
    logger.info(f"Total messages received: {message_count}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume plain text messages from a Kafka topic.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume from")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Consumer timeout in seconds")
    return parser


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)


def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    consume_messages(parsed_args.broker, parsed_args.topic, parsed_args.timeout)


if __name__ == "__main__":
    main()
