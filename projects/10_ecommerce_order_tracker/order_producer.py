"""Order producer that simulates customer orders being placed.

Sends JSON order events to the order-placed topic. Each order contains
a random selection of items from a product catalog.
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Sequence

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "order-placed"
DEFAULT_ORDERS = 20
DEFAULT_INTERVAL = 1.0

PRODUCT_CATALOG = [
    {"name": "Wireless Headphones", "price": 79.99},
    {"name": "USB-C Charging Cable", "price": 12.99},
    {"name": "Mechanical Keyboard", "price": 149.99},
    {"name": "Laptop Stand", "price": 45.00},
    {"name": "Webcam HD 1080p", "price": 64.99},
    {"name": "Mouse Pad XL", "price": 19.99},
    {"name": "Portable SSD 1TB", "price": 109.99},
    {"name": "Monitor Light Bar", "price": 54.99},
    {"name": "Bluetooth Speaker", "price": 39.99},
    {"name": "Phone Case", "price": 14.99},
]


def generate_order() -> dict:
    """Generate a single order with random items from the catalog."""
    num_items = random.randint(1, 4)
    items = random.sample(PRODUCT_CATALOG, num_items)
    total_amount = round(sum(item["price"] for item in items), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"cust_{random.randint(1, 50):04d}",
        "items": items,
        "total_amount": total_amount,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_orders(broker: str, topic: str, count: int, interval: float) -> None:
    """Produce order messages to the Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        for i in range(count):
            order = generate_order()
            producer.send(topic, value=order)
            item_names = ", ".join(item["name"] for item in order["items"])
            logger.info(
                "Order %d/%d placed: id=%s customer=%s total=%.2f items=[%s]",
                i + 1, count,
                order["order_id"][:8],
                order["customer_id"],
                order["total_amount"],
                item_names,
            )
            time.sleep(interval)
        producer.flush()
        logger.info("All %d orders sent to topic '%s'", count, topic)
    finally:
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="Simulate customer orders and send them to Kafka",
    )
    parser.add_argument(
        "--broker", default=DEFAULT_BROKER,
        help="Kafka broker address (default: %(default)s)",
    )
    parser.add_argument(
        "--topic", default=DEFAULT_TOPIC,
        help="Kafka topic to produce to (default: %(default)s)",
    )
    parser.add_argument(
        "--orders", type=int, default=DEFAULT_ORDERS,
        help="Number of orders to generate (default: %(default)s)",
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL,
        help="Seconds between orders (default: %(default)s)",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    return build_parser().parse_args(argv)


def main() -> None:
    """Entry point."""
    args = parse_args()
    send_orders(args.broker, args.topic, args.orders, args.interval)


if __name__ == "__main__":
    main()
