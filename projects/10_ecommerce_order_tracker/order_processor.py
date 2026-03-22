"""Order processor that drives orders through a lifecycle pipeline.

Consumes from the order-placed topic, simulates each processing stage
(validation, payment, shipping, delivery), and produces status updates
to the order-status topic.
"""

import argparse
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Optional, Sequence

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_INPUT_TOPIC = "order-placed"
DEFAULT_OUTPUT_TOPIC = "order-status"
DEFAULT_GROUP = "order-processors"
DEFAULT_TIMEOUT = 60

STATUS_PIPELINE = [
    ("PLACED", "Order received and queued for processing"),
    ("PAYMENT_PROCESSING", "Payment authorization in progress"),
    ("PAYMENT_CONFIRMED", "Payment successfully processed"),
    ("SHIPPED", "Package handed to carrier"),
    ("DELIVERED", "Package delivered to customer"),
]


def build_status_event(order_id: str, status: str, details: str) -> dict:
    """Create a status-update event."""
    return {
        "order_id": order_id,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "details": details,
    }


def process_orders(
    broker: str,
    input_topic: str,
    output_topic: str,
    group: str,
    timeout: int,
) -> None:
    """Consume orders and produce lifecycle status updates."""
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        group_id=group,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout * 1000,
    )
    consumer.subscribe([input_topic])

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    orders_processed = 0

    try:
        logger.info(
            "Order processor started: reading '%s', writing to '%s'",
            input_topic, output_topic,
        )
        for message in consumer:
            order = message.value
            order_id = order["order_id"]
            orders_processed += 1
            logger.info(
                "Processing order %s (customer=%s, total=%.2f)",
                order_id[:8], order["customer_id"], order["total_amount"],
            )

            for status, details in STATUS_PIPELINE:
                event = build_status_event(order_id, status, details)
                producer.send(output_topic, value=event)
                logger.info(
                    "  Order %s → %s", order_id[:8], status,
                )
                # Simulate processing delay between stages
                delay = random.uniform(0.1, 0.5)
                time.sleep(delay)

        producer.flush()
        logger.info(
            "Processor finished: %d orders fully processed", orders_processed,
        )
    finally:
        consumer.close()
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="Process orders through the fulfillment lifecycle",
    )
    parser.add_argument(
        "--broker", default=DEFAULT_BROKER,
        help="Kafka broker address (default: %(default)s)",
    )
    parser.add_argument(
        "--input-topic", default=DEFAULT_INPUT_TOPIC,
        help="Topic to consume orders from (default: %(default)s)",
    )
    parser.add_argument(
        "--output-topic", default=DEFAULT_OUTPUT_TOPIC,
        help="Topic to produce status updates to (default: %(default)s)",
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
    process_orders(
        broker=args.broker,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        group=args.group,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
