import argparse
import json
import logging
import random
import time
from typing import Optional, Sequence

from kafka import KafkaProducer

# Configuration
DEFAULT_BROKER = 'localhost:9092'
DEFAULT_TOPIC = 'event-counts'
DEFAULT_MESSAGES = 50
DEFAULT_INTERVAL = 0.5
EVENT_TYPES = ['click', 'view', 'purchase', 'signup']

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def send_events(broker: str, topic: str, num_messages: int, interval: float) -> None:
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    try:
        for i in range(1, num_messages + 1):
            event = {"event_id": i, "type": random.choice(EVENT_TYPES)}
            producer.send(topic, value=event)
            logger.info(f"Sent: {event}")
            time.sleep(interval)
        producer.flush()
        logger.info(f"Successfully sent {num_messages} events to topic '{topic}'")
    finally:
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce random JSON events to a Kafka topic.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to produce to")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES, help="Number of events to send")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="Seconds between messages")
    return parser


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)


def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    send_events(parsed_args.broker, parsed_args.topic, parsed_args.messages, parsed_args.interval)


if __name__ == "__main__":
    main()
