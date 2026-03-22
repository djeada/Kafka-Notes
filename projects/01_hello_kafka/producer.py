import argparse
import logging
from typing import Optional, Sequence

from kafka import KafkaProducer

# Configuration
DEFAULT_BROKER = 'localhost:9092'
DEFAULT_TOPIC = 'hello-kafka'
DEFAULT_MESSAGES = 10

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def send_messages(broker: str, topic: str, num_messages: int) -> None:
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: v.encode('utf-8'),
    )
    try:
        for i in range(1, num_messages + 1):
            message = f"Hello Kafka #{i}"
            producer.send(topic, value=message)
            logger.info(f"Sent: {message}")
        producer.flush()
        logger.info(f"Successfully sent {num_messages} messages to topic '{topic}'")
    finally:
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce plain text messages to a Kafka topic.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to produce to")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES, help="Number of messages to send")
    return parser


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)


def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    send_messages(parsed_args.broker, parsed_args.topic, parsed_args.messages)


if __name__ == "__main__":
    main()
