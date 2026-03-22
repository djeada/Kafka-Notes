"""Transaction producer that generates simulated financial transactions.

Approximately 10% of transactions are generated as suspicious
(high amounts, unusual locations) to test fraud detection rules.
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
DEFAULT_TOPIC = "transactions"
DEFAULT_MESSAGES = 100
DEFAULT_INTERVAL = 0.2

NORMAL_MERCHANTS = [
    "Amazon", "Walmart", "Target", "Starbucks", "McDonalds",
    "Shell Gas", "Netflix", "Spotify", "Uber", "Whole Foods",
]

SUSPICIOUS_MERCHANTS = [
    "CryptoExchange_XYZ", "OffshoreGaming_777", "UnknownVendor_999",
]

NORMAL_LOCATIONS = [
    "New York, US", "Los Angeles, US", "Chicago, US",
    "Houston, US", "Seattle, US", "Boston, US",
]

SUSPICIOUS_LOCATIONS = [
    "Unknown Location", "Offshore Territory", "Unregistered Zone",
]

CURRENCIES = ["USD", "EUR", "GBP"]


def generate_transaction(suspicious: bool = False) -> dict:
    """Generate a single transaction record."""
    if suspicious:
        amount = round(random.uniform(5000, 50000), 2)
        merchant = random.choice(SUSPICIOUS_MERCHANTS)
        location = random.choice(SUSPICIOUS_LOCATIONS)
    else:
        amount = round(random.uniform(1, 500), 2)
        merchant = random.choice(NORMAL_MERCHANTS)
        location = random.choice(NORMAL_LOCATIONS)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 20):03d}",
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "merchant": merchant,
        "location": location,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_transactions(broker: str, topic: str, count: int, interval: float) -> None:
    """Produce transaction messages to the Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        for i in range(count):
            suspicious = random.random() < 0.10
            transaction = generate_transaction(suspicious=suspicious)
            producer.send(topic, value=transaction)
            label = "SUSPICIOUS" if suspicious else "normal"
            logger.info(
                "Sent %s transaction %d/%d: user=%s amount=%.2f merchant=%s",
                label, i + 1, count,
                transaction["user_id"],
                transaction["amount"],
                transaction["merchant"],
            )
            time.sleep(interval)
        producer.flush()
        logger.info("All %d transactions sent to topic '%s'", count, topic)
    finally:
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="Generate simulated financial transactions to Kafka",
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
        "--messages", type=int, default=DEFAULT_MESSAGES,
        help="Number of transactions to generate (default: %(default)s)",
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL,
        help="Seconds between messages (default: %(default)s)",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    return build_parser().parse_args(argv)


def main() -> None:
    """Entry point."""
    args = parse_args()
    send_transactions(args.broker, args.topic, args.messages, args.interval)


if __name__ == "__main__":
    main()
