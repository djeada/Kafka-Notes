"""Status consumer that tracks the lifecycle of each order.

Consumes from the order-status topic, maintains an in-memory dictionary
mapping each order to its sequence of status updates, and logs every
transition.
"""

import argparse
import json
import logging
from typing import Dict, List, Optional, Sequence

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "order-status"
DEFAULT_GROUP = "status-trackers"
DEFAULT_TIMEOUT = 60


class OrderTracker:
    """Tracks order status transitions in memory."""

    def __init__(self) -> None:
        self._orders: Dict[str, List[dict]] = {}

    def record(self, event: dict) -> None:
        """Record a status event for an order."""
        order_id = event["order_id"]
        if order_id not in self._orders:
            self._orders[order_id] = []
        self._orders[order_id].append(event)

    def summary(self) -> None:
        """Log a summary of all tracked orders."""
        logger.info("=== Order Lifecycle Summary ===")
        for order_id, events in self._orders.items():
            statuses = " → ".join(e["status"] for e in events)
            logger.info("  Order %s: %s", order_id[:8], statuses)
        logger.info(
            "Total orders tracked: %d", len(self._orders),
        )


def consume_statuses(
    broker: str, topic: str, group: str, timeout: int,
) -> None:
    """Consume order-status events and track each order's lifecycle."""
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        group_id=group,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout * 1000,
    )
    consumer.subscribe([topic])

    tracker = OrderTracker()
    event_count = 0

    try:
        logger.info("Status consumer started: listening on '%s'", topic)
        for message in consumer:
            event = message.value
            event_count += 1
            tracker.record(event)
            logger.info(
                "Order %s → %s : %s",
                event["order_id"][:8],
                event["status"],
                event.get("details", ""),
            )

        logger.info(
            "Status consumer finished: %d events received", event_count,
        )
        tracker.summary()
    finally:
        consumer.close()


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="Track order lifecycle via status events from Kafka",
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
    consume_statuses(args.broker, args.topic, args.group, args.timeout)


if __name__ == "__main__":
    main()
