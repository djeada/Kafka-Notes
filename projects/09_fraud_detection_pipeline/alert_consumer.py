"""Alert consumer that reads fraud alerts and logs them.

Simple consumer that subscribes to the fraud-alerts topic and
prints each alert with full details for monitoring and audit.
"""

import argparse
import json
import logging
from typing import Optional, Sequence

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "fraud-alerts"
DEFAULT_GROUP = "alert-readers"
DEFAULT_TIMEOUT = 30


def consume_alerts(broker: str, topic: str, group: str, timeout: int) -> None:
    """Consume and log fraud alerts from the Kafka topic."""
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        group_id=group,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout * 1000,
    )
    consumer.subscribe([topic])

    alert_count = 0
    try:
        logger.info("Alert consumer started: listening on '%s'", topic)
        for message in consumer:
            alert = message.value
            alert_count += 1
            logger.warning(
                "ALERT #%d | ID: %s | Transaction: %s | User: %s | "
                "Amount: %.2f | Rule: %s | Time: %s",
                alert_count,
                alert.get("alert_id", "N/A"),
                alert.get("transaction_id", "N/A")[:8],
                alert.get("user_id", "N/A"),
                alert.get("amount", 0),
                alert.get("rule_triggered", "N/A"),
                alert.get("timestamp", "N/A"),
            )
        logger.info("Alert consumer finished: %d alerts received", alert_count)
    finally:
        consumer.close()


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="Consume and display fraud alerts from Kafka",
    )
    parser.add_argument(
        "--broker", default=DEFAULT_BROKER,
        help="Kafka broker address (default: %(default)s)",
    )
    parser.add_argument(
        "--topic", default=DEFAULT_TOPIC,
        help="Kafka topic to consume from (default: %(default)s)",
    )
    parser.add_argument(
        "--group", default=DEFAULT_GROUP,
        help="Consumer group ID (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT,
        help="Consumer timeout in seconds (default: %(default)s)",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    return build_parser().parse_args(argv)


def main() -> None:
    """Entry point."""
    args = parse_args()
    consume_alerts(args.broker, args.topic, args.group, args.timeout)


if __name__ == "__main__":
    main()
