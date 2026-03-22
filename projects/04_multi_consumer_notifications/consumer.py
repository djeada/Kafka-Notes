"""Consumer that reads notifications from Kafka, optionally filtering by type."""

import argparse
import json
import logging
from typing import Optional, Sequence

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "notifications"
DEFAULT_GROUP = "notification-processors"
DEFAULT_TIMEOUT = 30


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume notification messages from a Kafka topic."
    )
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume from")
    parser.add_argument("--group", default=DEFAULT_GROUP, help="Consumer group ID")
    parser.add_argument(
        "--filter-type",
        default=None,
        choices=["email", "sms", "push"],
        help="Only process notifications of this type",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Consumer poll timeout in seconds",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def consume_notifications(
    broker: str, topic: str, group: str, filter_type: Optional[str], timeout: int
) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset="earliest",
        group_id=group,
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    processed = 0
    skipped = 0

    try:
        logger.info(
            "Listening on topic '%s' (group=%s, filter=%s) …",
            topic,
            group,
            filter_type or "none",
        )

        for message in consumer:
            notification = message.value
            notif_type = notification.get("type")

            if filter_type and notif_type != filter_type:
                skipped += 1
                logger.debug("Skipped [%s] notification (id=%s)", notif_type, notification.get("notification_id"))
                continue

            processed += 1
            logger.info(
                "Processed [%s] id=%s  to=%s — %s",
                notif_type,
                notification.get("notification_id"),
                notification.get("recipient"),
                notification.get("message"),
            )
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        consumer.close()

    logger.info("Done. processed=%d  skipped=%d", processed, skipped)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    consume_notifications(args.broker, args.topic, args.group, args.filter_type, args.timeout)


if __name__ == "__main__":
    main()
